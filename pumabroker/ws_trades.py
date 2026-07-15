"""
ws_trades.py — Cliente Socket.IO para wss://trade.pumabroker.com/socket.io/

Usa python-socketio (AsyncClient) que cuida de TODO o protocolo
Engine.IO v4 + Socket.IO v4 automaticamente (handshake, ping/pong, reconexão).

Fluxo (reproduzindo fielmente o cliente oficial do navegador):
  1. Login REST -> accessToken (JWT)
  2. Conectar em https://trade.pumabroker.com com path /socket.io e transport websocket
  3. Aguardar pacote 0{sid...} (feito automaticamente pelo socketio)
  4. Enviar namespace /trades autenticando com {token: accessToken}
  5. Abrir namespace /otc
  6. Emitir subscribe para cada ativo: ["subscribe", {"symbol":"BETHUSDT","interval":60}]
  7. Receber eventos tick e candle
  8. Manter heartbeat conforme pingInterval informado pelo servidor

NOTA: O frame exato de abertura de ordem (COMPRA/VENDA) não foi capturado
pois o mercado estava fechado. O método place_order() documenta a estrutura
estimada e deve ser validado com mercado aberto.
"""

import asyncio
import logging
from typing import Any, Callable, Dict, List, Optional

import aiohttp
import socketio

from .config import config
from .models import TradeUpdate

logger = logging.getLogger(__name__)

EventHandler = Callable[[str, Any], None]


class TradesWebSocket:
    """
    Cliente Socket.IO para o namespace /trades e /otc da Puma Broker.

    Usa python-socketio AsyncClient que gerencia automaticamente:
    - Handshake Engine.IO v4 (pacote 0{sid...})
    - Ping/Pong automático (conforme pingInterval do servidor)
    - Reconexão com backoff
    - Abertura de namespaces com auth

    Fluxo de conexão:
      1. sio.connect(auth={"token": jwt}) → handshake + connect /trades
      2. On /trades connect → emit("subscribe", account_id)
      3. sio.connect(namespaces=["/otc"]) → connect /otc
      4. On /otc connect → emit("subscribe", {"symbol": X, "interval": Y}) per asset

    Uso:
        ws = TradesWebSocket(session_token="...", account_id="28318")
        ws.on("tradeUpdate", meu_handler)

        async with ws:
            await ws.place_order("EURUSD", "call", amount=2.0, duration=60)
            await ws.listen()
    """

    def __init__(self, session_token: str, account_id: str):
        self._token = session_token
        self._account_id = account_id
        self._running = False
        self._handlers: Dict[str, List[EventHandler]] = {}
        self._reconnect_count = 0

        # Socket.IO AsyncClient — gerencia TODO o protocolo Engine.IO + Socket.IO
        self._sio: Optional[socketio.AsyncClient] = None

        # Estado dos namespaces
        self._ns_trades_open = False
        self._ns_otc_open = False
        self._connected_event = asyncio.Event()

        # Assets para subscribe no /otc (preenchidos via set_assets)
        self._otc_assets: List[Dict[str, Any]] = []

        # HTTP session para headers customizados no handshake
        self._http_session: Optional[aiohttp.ClientSession] = None

    # ── Registro de handlers ──────────────────────────────────────────────────

    def on(self, event: str, handler: EventHandler) -> None:
        """
        Registra callback para eventos Socket.IO.

        Eventos conhecidos:
          "tradeUpdate"  — resultado de uma ordem
          "tick"         — atualização de preço (namespace /otc)
          "candle"       — candle fechado (namespace /otc)
          "*"            — todos os eventos

        Exemplo:
            ws.on("tradeUpdate", lambda ev, data: print(data))
        """
        self._handlers.setdefault(event, []).append(handler)

    def set_otc_assets(self, assets: List[Dict[str, Any]]) -> None:
        """
        Define os ativos para subscribe no /otc.

        Args:
            assets: Lista de dicts [{"symbol": "BETHUSDT", "interval": 60}, ...]
        """
        self._otc_assets = list(assets)

    # ── Conexão ───────────────────────────────────────────────────────────────

    async def connect(self) -> None:
        """
        Conecta ao Socket.IO server da Puma Broker.

        Fluxo reproduzindo o navegador:
          1. Abre sessão HTTP com headers customizados (Origin, User-Agent)
          2. Conecta Socket.IO em https://trade.pumabroker.com
             - auth: {token: jwt} (autenticação)
             - transport: websocket
             - namespaces: [/trades]
          3. handshake automático (pacote 0{sid...}) feito pelo socketio
          4. heartbeat automático conforme pingInterval do servidor
        """

        # Cria sessão HTTP com headers que o navegador envia
        self._http_session = aiohttp.ClientSession(
            headers={
                **config.DEFAULT_HEADERS,
                "Cookie": f"{config.SESSION_COOKIE}={self._token}",
            }
        )

        # Cria o AsyncClient Socket.IO
        self._sio = socketio.AsyncClient(
            http_session=self._http_session,
            logger=False,
            engineio_logger=False,
            reconnection=True,
            reconnection_attempts=config.WS_MAX_RECONNECT,
            reconnection_delay=config.WS_RECONNECT_DELAY,
            reconnection_delay_max=30,
        )

        # Registra handlers de eventos
        self._register_handlers()

        logger.info(
            "Conectando Socket.IO trades: %s (namespaces: /trades, /otc)",
            config.BASE_URL,
        )

        # Conecta ao namespace /trades com autenticação
        # O socketio cuida do handshake EIO (pacote 0{sid...}) automaticamente
        await self._sio.connect(
            config.BASE_URL,
            auth={"token": self._token},
            transports=["websocket"],
            namespaces=["/trades"],
        )

        logger.info("Socket.IO trades conectado. Account: %s", self._account_id)

    def _register_handlers(self) -> None:
        """Registra todos os handlers de eventos no Socket.IO client."""

        # ── Connect / Disconnect ──────────────────────────────────────────

        @self._sio.on("connect", namespace="/trades")
        async def on_trades_connect():
            self._ns_trades_open = True
            logger.info("Namespace /trades conectado")

            # Subscribe na conta (exatamente como o navegador)
            # Frame real: 42/trades,["subscribe","28318"]
            await self._sio.emit("subscribe", self._account_id, namespace="/trades")
            logger.info("→ Subscribe account_id=%s", self._account_id)

            # Conecta /otc após /trades estar pronto
            if "/otc" not in self._sio.namespaces:
                await self._sio.connect(
                    config.BASE_URL,
                    auth={"token": self._token},
                    transports=["websocket"],
                    namespaces=["/otc"],
                )

        @self._sio.on("connect", namespace="/otc")
        async def on_otc_connect():
            self._ns_otc_open = True
            logger.info("Namespace /otc conectado")

            # Subscribe em cada ativo (exatamente como o navegador)
            for asset in self._otc_assets:
                await self._sio.emit(
                    "subscribe",
                    {"symbol": asset["symbol"], "interval": asset.get("interval", 60)},
                    namespace="/otc",
                )
                logger.info(
                    "→ Subscribe %s interval=%s",
                    asset["symbol"],
                    asset.get("interval", 60),
                )

            # Marca como totalmente conectado
            self._connected_event.set()

        @self._sio.on("disconnect", namespace="/trades")
        def on_trades_disconnect():
            self._ns_trades_open = False
            logger.warning("Namespace /trades desconectado")

        @self._sio.on("disconnect", namespace="/otc")
        def on_otc_disconnect():
            self._ns_otc_open = False
            logger.warning("Namespace /otc desconectado")

        @self._sio.on("connect_error")
        def on_connect_error(data):
            logger.error("Socket.IO connect_error: %s", data)

        # ── Eventos de dados ──────────────────────────────────────────────

        @self._sio.on("tradeUpdate", namespace="/trades")
        def on_trade_update(data):
            logger.debug("← tradeUpdate: %s", str(data)[:120])
            self._dispatch_event("tradeUpdate", data)

        @self._sio.on("tick", namespace="/otc")
        def on_tick(data):
            logger.debug("← tick: %s", str(data)[:120])
            self._dispatch_event("tick", data)

        @self._sio.on("candle", namespace="/otc")
        def on_candle(data):
            logger.debug("← candle: %s", str(data)[:120])
            self._dispatch_event("candle", data)

        # Handler genérico para outros eventos
        @self._sio.on("*", namespace="/trades")
        def on_trades_any(event, data):
            logger.debug("← /trades [%s]: %s", event, str(data)[:120])
            self._dispatch_event(event, data)

        @self._sio.on("*", namespace="/otc")
        def on_otc_any(event, data):
            logger.debug("← /otc [%s]: %s", event, str(data)[:120])
            self._dispatch_event(event, data)

    def _dispatch_event(self, event_name: str, data: Any) -> None:
        """Despacha evento para handlers registrados."""

        # Converte para TradeUpdate quando possível
        if event_name == "tradeUpdate" and isinstance(data, dict):
            try:
                trade = TradeUpdate(**data)
                logger.info(
                    "Trade #%s %s %s $%.2f status=%s payout=%.0f%%",
                    trade.id,
                    trade.symbol,
                    trade.direction,
                    trade.amount,
                    trade.status,
                    trade.payout_percent(),
                )
                for handler in self._handlers.get("tradeUpdate", []):
                    self._call(handler, event_name, trade)
                for handler in self._handlers.get("*", []):
                    self._call(handler, event_name, trade)
                return
            except Exception as exc:
                logger.debug("Erro ao parsear tradeUpdate: %s", exc)

        # Despacha para handlers registrados
        for handler in self._handlers.get(event_name, []):
            self._call(handler, event_name, data)

        # Handler coringa
        for handler in self._handlers.get("*", []):
            self._call(handler, event_name, data)

    def _call(self, handler, event_name, data):
        try:
            if asyncio.iscoroutinefunction(handler):
                asyncio.ensure_future(handler(event_name, data))
            else:
                handler(event_name, data)
        except Exception as exc:
            logger.error("Erro no handler [%s]: %s", event_name, exc, exc_info=True)

    # ── Envio ─────────────────────────────────────────────────────────────────

    async def place_order(
        self,
        symbol: str,
        direction: str,
        amount: float,
        duration: int = 60,
        namespace: str = "otc",
    ) -> None:
        """
        Envia uma ordem de opção binária via Socket.IO.

        CONFIRMADO via DevTools (15/06/2026 22:38):
          Resposta do servidor após ordem:
          42/trades,["tradeUpdate",{
            "id":"5186325", "symbol":"BETHUSDT",
            "direction":"CALL",   <- MAIÚSCULAS confirmadas
            "amount":2, "entryPrice":"911.31632",
            "exitPrice":null, "profit":0, "payout":0.87,
            "status":"ACTIVE", "isDemo":false
          }]

        Args:
            symbol:    "BETHUSDT", "EURUSD", etc.
            direction: "CALL" | "PUT" (maiúsculas — confirmado no frame real)
            amount:    Valor (2 = R$2,00 — confirmado no frame real)
            duration:  Duração em segundos (60 = 1min)
            namespace: "otc" (padrão — candles via 42/otc confirmado)
        """
        from .models import OrderRequest

        order = OrderRequest(
            symbol=symbol,
            direction=direction.upper(),
            amount=amount,
            duration=duration,
            account_id=self._account_id,
        )

        payload = order.model_dump()

        # Envia via Socket.IO (python-socketio cuida do frame 42/namespace,[...])
        await self._sio.emit("trade", payload, namespace=f"/{namespace}")
        logger.info(
            "→ Ordem enviada: %s %s $%.2f %ds",
            symbol,
            direction.upper(),
            amount,
            duration,
        )

    # ── Loop principal ────────────────────────────────────────────────────────

    async def listen(self) -> None:
        """Loop de escuta — mantém a conexão Socket.IO ativa."""
        self._running = True

        try:
            # Aguarda a conexão completa (trades + otc)
            await asyncio.wait_for(self._connected_event.wait(), timeout=15.0)
            logger.info("Socket.IO totalmente conectado (/trades + /otc)")
        except asyncio.TimeoutError:
            logger.warning("Timeout aguardando conexão completa do Socket.IO")

        # Mantém o loop vivo enquanto o socket estiver conectado
        while self._running and self._sio and self._sio.connected:
            await asyncio.sleep(1)

        if self._running:
            logger.warning("Socket.IO desconectado — loop encerrado")

    async def disconnect(self) -> None:
        """Fecha a conexão Socket.IO."""
        self._running = False
        self._connected_event.clear()
        if self._sio and self._sio.connected:
            await self._sio.disconnect()
        if self._http_session:
            await self._http_session.close()
            self._http_session = None
        self._ns_trades_open = False
        self._ns_otc_open = False
        logger.info("Socket.IO trades desconectado.")

    # ── Context manager ───────────────────────────────────────────────────────

    async def __aenter__(self):
        await self.connect()
        return self

    async def __aexit__(self, *args):
        await self.disconnect()
