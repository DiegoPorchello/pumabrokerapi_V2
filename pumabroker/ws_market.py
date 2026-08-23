"""
ws_market.py — Cliente WebSocket puro para wss://wsmt5.pumabroker.com/

Protocolo descoberto via DevTools → Socket → wsmt5.pumabroker.com → Messages:

  RECEBIDO (bar_update):
    {"type":"bar_update","symbol":"EURUSD","interval":"5",
     "bar":{"time":1781556900,"open":1.15884,"high":1.159,
            "low":1.15883,"close":1.15896,"volume":190.0},
     "last_bar":{...}}

  RECEBIDO (server_time):
    {"type":"server_time","timestamp":1781557272731}

  ENVIADO (heartbeat request):
    {"method":"server_time"}

  Sec-WebSocket-Extensions: permessage-deflate; server_max_window_bits=12
  Sec-WebSocket-Version: 13
"""

import asyncio
import json
import logging
import time
from typing import Callable, Dict, List, Optional

import websockets
from websockets.exceptions import ConnectionClosed

from .config import config
from .models import BarUpdateEvent, ServerTimeWS2

logger = logging.getLogger(__name__)

BarHandler = Callable[[BarUpdateEvent], None]


class MarketWebSocket:
    """
    Conecta ao feed de candles OHLCV da Puma Broker (wsmt5.pumabroker.com).

    Uso:
        ws = MarketWebSocket(session_token="cd0dc3ba...")
        ws.on_bar("EURUSD", "5", meu_handler)

        async with ws:
            await ws.listen()
    """

    DEAD_CONNECTION_TIMEOUT = 30  # segundos sem resposta → reconectar

    def __init__(self, session_token: str):
        self._token    = session_token
        self._ws       = None
        self._running  = False
        self._handlers: Dict[str, List[BarHandler]] = {}
        self._last_server_time: Optional[int] = None
        self._last_message_time: float = 0.0
        self._reconnect_count = 0
        # Subscriptions dinâmicas extraídas dos handlers registrados
        self._subscribed_symbols: List[str] = []
        self._subscribed_intervals: List[str] = []

    # ── Registro de handlers ──────────────────────────────────────────────────

    def on_bar(self, symbol: str, interval: str, handler: BarHandler) -> None:
        """
        Registra callback para bar_update de um ativo/intervalo específico.

        Args:
            symbol:   Ex: "EURUSD", "BTCUSD"
            interval: Em minutos: "1", "5", "15", "30", "60"
            handler:  Função(BarUpdateEvent) → None

        Exemplo:
            ws.on_bar("EURUSD", "5", lambda b: print(b.bar.close))
        """
        key = f"{symbol}:{interval}"
        self._handlers.setdefault(key, []).append(handler)

        # Rastreia subscriptions para envio dinâmico ao servidor
        sym_upper = symbol.upper()
        if sym_upper not in self._subscribed_symbols:
            self._subscribed_symbols.append(sym_upper)
        if interval not in self._subscribed_intervals:
            self._subscribed_intervals.append(interval)

        logger.debug("Handler registrado: %s interval=%s (total symbols=%d)",
                      symbol, interval, len(self._subscribed_symbols))

    def on_any_bar(self, handler: BarHandler) -> None:
        """Registra callback para todos os bar_updates independente de símbolo."""
        self._handlers.setdefault("*", []).append(handler)

    # ── Conexão ───────────────────────────────────────────────────────────────

    async def connect(self) -> None:
        headers = {
            "Cookie":  f"{config.SESSION_COOKIE}={self._token}",
            "Origin":  config.BASE_URL,
            **{k: v for k, v in config.DEFAULT_HEADERS.items()
               if k not in ("Accept", "Accept-Language")},
        }

        logger.info("Conectando ao feed de mercado: %s", config.WS_MARKET_URL)

        self._ws = await websockets.connect(
            config.WS_MARKET_URL,
            additional_headers=headers,
            # Servidor usa permessage-deflate
            compression="deflate",
            ping_interval=None,   # heartbeat manual via {"method":"server_time"}
            close_timeout=5,
        )
        self._running = True
        logger.info("Feed de mercado conectado.")

        # Envia pedido de inscrição após autenticação (WS2 puro)
        await self._subscribe()

    # ── Inscrição no feed de mercado (WS2) ──────────────────────────────────

    async def _subscribe(self) -> None:
        """
        Envia pedido de inscrição após autenticação.
        Frame real observado no DevTools:
        {"method":"subscribe","params":{"symbols":["EURUSD"], "intervals":["5"]}}

        Agora usa symbols/intervals dinâmicos baseados nos handlers registrados.
        """
        symbols = self._subscribed_symbols if self._subscribed_symbols else ["EURUSD"]
        intervals = self._subscribed_intervals if self._subscribed_intervals else ["1"]

        payload = {
            "method": "subscribe",
            "params": {
                "symbols": symbols,
                "intervals": intervals
            }
        }
        await self._ws.send(json.dumps(payload))
        logger.info("→ WS2 subscribe enviado: symbols=%s intervals=%s",
                     symbols, intervals)

    # ── Heartbeat manual ──────────────────────────────────────────────────────

    async def _heartbeat_loop(self) -> None:
        """
        Envia {"method":"server_time"} periodicamente.
        Observado nos frames: a cada ~10s o cliente envia esse payload.
        """
        while self._running:
            try:
                if self._ws and self._ws.open:
                    await self._ws.send(json.dumps({"method": "server_time"}))
                    logger.debug("→ WS2 heartbeat enviado")
            except Exception as exc:
                logger.debug("Erro no heartbeat WS2: %s", exc)
            await asyncio.sleep(10)

    # ── Recepção e dispatch ───────────────────────────────────────────────────

    async def _dispatch(self, raw: str) -> None:
        try:
            data = json.loads(raw)
        except json.JSONDecodeError:
            logger.debug("WS2 frame não-JSON ignorado: %s", raw[:80])
            return

        # Atualiza timestamp da última mensagem recebida
        import time as _time
        self._last_message_time = _time.time()

        msg_type = data.get("type") or data.get("method")
        logger.debug("← WS2 [%s]: %s", msg_type, raw[:120])

        if msg_type == "bar_update":
            try:
                event = BarUpdateEvent(**data)
            except Exception as exc:
                logger.warning("bar_update inválido: %s — %s", data, exc)
                return

            # Despacha para handlers específicos
            key = f"{event.symbol}:{event.interval}"
            for handler in self._handlers.get(key, []):
                self._call(handler, event)

            # Despacha para handlers globais
            for handler in self._handlers.get("*", []):
                self._call(handler, event)

        elif msg_type == "server_time":
            self._last_server_time = data.get("timestamp")
            logger.debug("WS2 server_time: %s", self._last_server_time)

    def _call(self, handler, event):
        try:
            if asyncio.iscoroutinefunction(handler):
                asyncio.ensure_future(handler(event))
            else:
                handler(event)
        except Exception as exc:
            logger.error("Erro no handler WS2: %s", exc, exc_info=True)

    # ── Loop principal ────────────────────────────────────────────────────────

    async def listen(self) -> None:
        """Loop de escuta com reconexão automática + detecção de dead connection."""
        self._running = True
        import time as _time
        self._last_message_time = _time.time()

        while self._running:
            try:
                # Inicia heartbeat em paralelo
                hb_task = asyncio.create_task(self._heartbeat_loop())
                # Inicia watchdog de dead connection
                watchdog_task = asyncio.create_task(self._dead_connection_watchdog())

                async for raw in self._ws:
                    if isinstance(raw, bytes):
                        raw = raw.decode("utf-8")
                    await self._dispatch(raw)

            except ConnectionClosed as exc:
                logger.warning("WS2 desconectado: %s", exc)
            except Exception as exc:
                logger.error("WS2 erro: %s", exc, exc_info=True)
            finally:
                hb_task.cancel()
                watchdog_task.cancel()

            if not self._running:
                break

            delay = min(config.WS_RECONNECT_DELAY * (2 ** self._reconnect_count), 120)
            self._reconnect_count += 1
            logger.info("WS2 reconectando em %.0fs...", delay)
            await asyncio.sleep(delay)

            try:
                await self.connect()
                self._reconnect_count = 0
                self._last_message_time = _time.time()
            except Exception as exc:
                logger.error("WS2 falha na reconexão: %s", exc)

    async def _dead_connection_watchdog(self) -> None:
        """Detecta conexão morta: sem nenhuma mensagem por DEAD_CONNECTION_TIMEOUT segundos."""
        while self._running:
            await asyncio.sleep(10)
            if not self._ws or not self._running:
                continue
            import time as _time
            elapsed = _time.time() - self._last_message_time
            if elapsed > self.DEAD_CONNECTION_TIMEOUT:
                logger.warning(
                    "WS2 DEAD CONNECTION: %.0fs sem mensagens — forçando reconexão",
                    elapsed,
                )
                try:
                    if self._ws:
                        await self._ws.close()
                except Exception:
                    pass
                break

    async def disconnect(self) -> None:
        self._running = False
        if self._ws:
            await self._ws.close()

    # ── Context manager ───────────────────────────────────────────────────────

    async def __aenter__(self):
        await self.connect()
        return self

    async def __aexit__(self, *args):
        await self.disconnect()

    @property
    def last_server_time(self) -> Optional[int]:
        return self._last_server_time
