"""
ws_trades.py — Cliente Socket.IO para wss://trade.pumabroker.com/socket.io/

Protocolo Socket.IO v4 (Engine.IO v4) descoberto via DevTools:

  HANDSHAKE (recebido):
    0{"sid":"AAxrTo2uISdLGA9bA2vI","upgrades":[],"pingInterval":25000,"pingTimeout":20000}

  NAMESPACE /trades (enviado):
    40/trades,
    → Servidor responde: 40/trades,{"sid":"OFsHv2_qGNT3HyibA2vJ"}

  SUBSCRIBE (enviado após namespace aberto):
    42/trades,["subscribe","28318"]    ← 28318 = account_id

  NAMESPACE /otc (enviado):
    40/otc,
    → Servidor responde: 40/otc,{"sid":"UgJ02aQp6z6HKNdWA2vK"}

  PING/PONG Engine.IO:
    2   → ping (enviado pelo cliente a cada pingInterval=25s)
    3   ← pong (resposta do servidor)

  FORMATO Socket.IO v4:
    4X/namespace,["event", payload]
    onde X: 0=CONNECT, 2=EVENT, 3=ACK, 4=ERROR
    Prefixo Engine.IO: 4 = MESSAGE type

NOTA: O frame exato de abertura de ordem (COMPRA/VENDA) não foi capturado
pois o mercado estava fechado. O método place_order() documenta a estrutura
estimada e deve ser validado com mercado aberto.
"""

import asyncio
import json
import logging
import re
import time
from typing import Any, Callable, Dict, List, Optional

import websockets
from websockets.exceptions import ConnectionClosed

from .config import config
from .models import EIOHandshake, OrderRequest

logger = logging.getLogger(__name__)

EventHandler = Callable[[str, Any], None]


class TradesWebSocket:
    """
    Cliente Socket.IO para o namespace /trades e /otc da Puma Broker.

    Gerencia:
    - Handshake Engine.IO v4
    - Abertura de namespaces /trades e /otc
    - Subscribe na conta (account_id)
    - Ping/Pong automático (a cada 25s)
    - Envio e recepção de ordens

    Uso:
        ws = TradesWebSocket(session_token="...", account_id="28318")
        ws.on("order_result", meu_handler)

        async with ws:
            await ws.place_order("EURUSD", "call", amount=2.0, duration=60)
            await ws.listen()
    """

    def __init__(self, session_token: str, account_id: str):
        self._token      = session_token
        self._account_id = account_id
        self._ws         = None
        self._running    = False
        self._sid:       Optional[str] = None
        self._ping_interval: int = 25
        self._handlers:  Dict[str, List[EventHandler]] = {}
        self._reconnect_count = 0

        # Controle de namespaces abertos
        self._ns_trades_open = False
        self._ns_otc_open    = False

    # ── Registro de handlers ──────────────────────────────────────────────────

    def on(self, event: str, handler: EventHandler) -> None:
        """
        Registra callback para eventos Socket.IO.

        Eventos conhecidos:
          "order_result"  — resultado de uma ordem
          "balance"       — atualização de saldo
          "*"             — todos os eventos

        Exemplo:
            ws.on("order_result", lambda ev, data: print(data))
        """
        self._handlers.setdefault(event, []).append(handler)

    # ── Conexão ───────────────────────────────────────────────────────────────

    async def connect(self) -> None:
        headers = {
            "Cookie":  f"{config.SESSION_COOKIE}={self._token}",
            "Origin":  config.BASE_URL,
            **{k: v for k, v in config.DEFAULT_HEADERS.items()
               if k not in ("Accept", "Accept-Language")},
        }

        logger.info("Conectando Socket.IO trades: %s", config.WS_TRADES_URL)

        self._ws = await websockets.connect(
            config.WS_TRADES_URL,
            extra_headers=headers,
            ping_interval=None,   # ping manual via Engine.IO "2"
            close_timeout=5,
        )

        # Aguarda handshake Engine.IO
        raw = await self._ws.recv()
        await self._handle_eio_handshake(raw)

        # Abre namespaces
        await self._open_namespaces()
        logger.info("Socket.IO trades conectado. Account: %s", self._account_id)

    async def _handle_eio_handshake(self, raw: str) -> None:
        """
        Processa pacote Engine.IO tipo 0 (OPEN).
        Frame: 0{"sid":"...","pingInterval":25000,"pingTimeout":20000,...}
        """
        if not raw.startswith("0"):
            logger.warning("Handshake inesperado: %s", raw[:80])
            return

        try:
            data = json.loads(raw[1:])
            hs   = EIOHandshake(**data)
            self._sid            = hs.sid
            self._ping_interval  = hs.pingInterval // 1000
            logger.info("EIO Handshake OK. SID=%s pingInterval=%ds",
                        self._sid, self._ping_interval)
        except Exception as exc:
            logger.error("Erro no handshake: %s", exc)

    async def _open_namespaces(self) -> None:
        """
        Abre os namespaces /trades e /otc.
        Frame enviado: 40/trades,
        Frame esperado: 40/trades,{"sid":"..."}
        """
        # Abre /trades
        await self._send_raw("40/trades,")
        logger.debug("→ Namespace /trades aberto")

        # Abre /otc
        await self._send_raw("40/otc,")
        logger.debug("→ Namespace /otc aberto")

        # Aguarda confirmações (com timeout)
        deadline = time.time() + 5.0
        while time.time() < deadline:
            if self._ns_trades_open and self._ns_otc_open:
                break
            try:
                raw = await asyncio.wait_for(self._ws.recv(), timeout=1.0)
                await self._dispatch(raw)
            except asyncio.TimeoutError:
                pass

        # Subscribe na conta
        if self._ns_trades_open:
            await self._subscribe()

    async def _subscribe(self) -> None:
        """
        Subscreve aos eventos da conta.
        Frame real: 42/trades,["subscribe","28318"]
        """
        payload = json.dumps(["subscribe", self._account_id])
        await self._send_raw(f"42/trades,{payload}")
        logger.info("→ Subscribe account_id=%s", self._account_id)

    # ── Envio ─────────────────────────────────────────────────────────────────

    async def _send_raw(self, data: str) -> None:
        if not self._ws:
            raise RuntimeError("WebSocket não conectado")
        logger.debug("→ WS3 send: %s", data[:100])
        await self._ws.send(data)

    async def _send_sio(self, namespace: str, event: str, payload: Any) -> None:
        """
        Envia evento Socket.IO v4.
        Formato: 42/namespace,["event", payload]
        """
        data   = json.dumps([event, payload])
        frame  = f"42/{namespace},{data}"
        await self._send_raw(frame)

    # ── Ordens ────────────────────────────────────────────────────────────────

    async def place_order(
        self,
        symbol:    str,
        direction: str,
        amount:    float,
        duration:  int = 60,
        namespace: str = "otc",
    ) -> None:
        """
        Envia uma ordem de opção binária.

        CONFIRMADO via DevTools (15/06/2026 22:38):
          Resposta do servidor após ordem:
          42/trades,["tradeUpdate",{
            "id":"5186325", "symbol":"BETHUSDT",
            "direction":"CALL",   ← MAIÚSCULAS confirmadas
            "amount":2, "entryPrice":"911.31632",
            "exitPrice":null, "profit":0, "payout":0.87,
            "status":"ACTIVE", "isDemo":false
          }]

        O evento de ENVIO (cliente→servidor) ainda precisa ser capturado.
        Para capturar: no DevTools → WS3 → aba Messages → filtrar setas ↑
        (frames com seta para cima = enviados pelo cliente).

        Args:
            symbol:    "BETHUSDT", "EURUSD", etc. (confirme o formato exato)
            direction: "CALL" | "PUT" (maiúsculas — confirmado no frame real)
            amount:    Valor (2 = R$2,00 — confirmado no frame real)
            duration:  Duração em segundos (60 = 1min)
            namespace: "otc" (padrão — candles via 42/otc confirmado)
        """
        order = OrderRequest(
            symbol=symbol,
            direction=direction.upper(),  # confirmado: "CALL" / "PUT"
            amount=amount,
            duration=duration,
            account_id=self._account_id,
        )

        payload = order.model_dump()

        # ⚠️ O nome do evento de envio ainda precisa ser confirmado.
        # Candidatos mais prováveis baseados no padrão Socket.IO + resposta "tradeUpdate":
        # "trade", "placeTrade", "openTrade", "createTrade"
        # Para descobrir: filtrar frames ↑ no DevTools ao clicar COMPRA
        await self._send_sio(namespace, "trade", payload)
        logger.info(
            "→ Ordem enviada: %s %s $%.2f %ds",
            symbol, direction.upper(), amount, duration,
        )

    # ── Recepção e dispatch ───────────────────────────────────────────────────

    async def _dispatch(self, raw: str) -> None:
        logger.debug("← WS3 recv: %s", raw[:120])

        # Ping Engine.IO (2) → responde com Pong (3)
        if raw == "2":
            await self._send_raw("3")
            logger.debug("← ping → pong")
            return

        if raw == "3":
            logger.debug("← pong recebido")
            return

        # Namespace /trades confirmado: 40/trades,{"sid":"..."}
        if raw.startswith("40/trades,"):
            self._ns_trades_open = True
            logger.info("Namespace /trades confirmado")
            return

        # Namespace /otc confirmado: 40/otc,{"sid":"..."}
        if raw.startswith("40/otc,"):
            self._ns_otc_open = True
            logger.info("Namespace /otc confirmado")
            return

        # Eventos Socket.IO: 42/namespace,["event", data]
        if raw.startswith("42/"):
            await self._dispatch_sio_event(raw)
            return

        # Outros pacotes EIO (41=disconnect, 44=error, etc.)
        logger.debug("WS3 pacote EIO ignorado: %s", raw[:40])

    async def _dispatch_sio_event(self, raw: str) -> None:
        """
        Decodifica e despacha evento Socket.IO v4.
        Formato: 42/namespace,["event_name", payload]
        """
        # Extrai namespace e payload
        match = re.match(r"42/(\w+),(.*)", raw, re.DOTALL)
        if not match:
            logger.debug("Frame SIO não reconhecido: %s", raw[:80])
            return

        namespace = match.group(1)
        body_raw  = match.group(2)

        try:
            body = json.loads(body_raw)
        except json.JSONDecodeError:
            logger.debug("Body SIO inválido: %s", body_raw[:80])
            return

        if not isinstance(body, list) or len(body) < 1:
            return

        event_name = body[0]
        event_data = body[1] if len(body) > 1 else None

        logger.info("← SIO [/%s] %s: %s", namespace, event_name, str(event_data)[:120])

        # tradeUpdate — evento CONFIRMADO via DevTools (15/06/2026)
        # Converte para TradeUpdate model quando possível
        if event_name == "tradeUpdate" and isinstance(event_data, dict):
            try:
                from .models import TradeUpdate
                trade = TradeUpdate(**event_data)
                logger.info(
                    "Trade #%s %s %s $%.2f status=%s payout=%.0f%%",
                    trade.id, trade.symbol, trade.direction,
                    trade.amount, trade.status, trade.payout_percent(),
                )
                # Despacha como tradeUpdate com objeto tipado
                for handler in self._handlers.get("tradeUpdate", []):
                    self._call(handler, event_name, trade)
                for handler in self._handlers.get("*", []):
                    self._call(handler, event_name, trade)
                return
            except Exception as exc:
                logger.debug("Erro ao parsear tradeUpdate: %s", exc)

        # Despacha para handlers registrados
        for handler in self._handlers.get(event_name, []):
            self._call(handler, event_name, event_data)

        # Handler coringa
        for handler in self._handlers.get("*", []):
            self._call(handler, event_name, event_data)

    def _call(self, handler, event_name, data):
        try:
            if asyncio.iscoroutinefunction(handler):
                asyncio.ensure_future(handler(event_name, data))
            else:
                handler(event_name, data)
        except Exception as exc:
            logger.error("Erro no handler WS3 [%s]: %s", event_name, exc, exc_info=True)

    # ── Ping loop ─────────────────────────────────────────────────────────────

    async def _ping_loop(self) -> None:
        """
        Envia ping Engine.IO ("2") a cada pingInterval segundos.
        Descoberto: pingInterval=25000ms = 25s
        """
        while self._running:
            await asyncio.sleep(self._ping_interval)
            if self._ws and self._ws.open:
                try:
                    await self._send_raw("2")
                    logger.debug("→ EIO ping enviado")
                except Exception as exc:
                    logger.debug("Erro no ping: %s", exc)

    # ── Loop principal ────────────────────────────────────────────────────────

    async def listen(self) -> None:
        """Loop de escuta com reconexão automática."""
        self._running = True

        while self._running:
            try:
                ping_task = asyncio.create_task(self._ping_loop())

                async for raw in self._ws:
                    if isinstance(raw, bytes):
                        raw = raw.decode("utf-8")
                    await self._dispatch(raw)

            except ConnectionClosed as exc:
                logger.warning("WS3 desconectado: %s", exc)
            except Exception as exc:
                logger.error("WS3 erro: %s", exc, exc_info=True)
            finally:
                ping_task.cancel()
                self._ns_trades_open = False
                self._ns_otc_open    = False

            if not self._running:
                break

            delay = min(config.WS_RECONNECT_DELAY * (2 ** self._reconnect_count), 120)
            self._reconnect_count += 1
            logger.info("WS3 reconectando em %.0fs...", delay)
            await asyncio.sleep(delay)

            try:
                await self.connect()
                self._reconnect_count = 0
            except Exception as exc:
                logger.error("WS3 falha na reconexão: %s", exc)

    async def disconnect(self) -> None:
        self._running = False
        if self._ws:
            await self._ws.close()

    async def __aenter__(self):
        await self.connect()
        return self

    async def __aexit__(self, *args):
        await self.disconnect()
