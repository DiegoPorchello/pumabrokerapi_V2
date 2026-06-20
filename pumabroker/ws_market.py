"""
ws_market.py — Cliente WebSocket puro para wss://wsm5.pumabroker.com/

Protocolo descoberto via DevTools → Socket → wsm5.pumabroker.com → Messages:

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
    Conecta ao feed de candles OHLCV da Puma Broker (wsm5.pumabroker.com).

    Uso:
        ws = MarketWebSocket(session_token="cd0dc3ba...")
        ws.on_bar("EURUSD", "5", meu_handler)

        async with ws:
            await ws.listen()
    """

    def __init__(self, session_token: str):
        self._token    = session_token
        self._ws       = None
        self._running  = False
        self._handlers: Dict[str, List[BarHandler]] = {}
        self._last_server_time: Optional[int] = None
        self._reconnect_count = 0

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
        logger.debug("Handler registrado: %s interval=%s", symbol, interval)

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
            extra_headers=headers,
            # Servidor usa permessage-deflate
            compression="deflate",
            ping_interval=None,   # heartbeat manual via {"method":"server_time"}
            close_timeout=5,
        )
        self._running = True
        logger.info("Feed de mercado conectado.")

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
        """Loop de escuta com reconexão automática."""
        self._running = True

        while self._running:
            try:
                # Inicia heartbeat em paralelo
                hb_task = asyncio.create_task(self._heartbeat_loop())

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

            if not self._running:
                break

            delay = min(config.WS_RECONNECT_DELAY * (2 ** self._reconnect_count), 120)
            self._reconnect_count += 1
            logger.info("WS2 reconectando em %.0fs...", delay)
            await asyncio.sleep(delay)

            try:
                await self.connect()
                self._reconnect_count = 0
            except Exception as exc:
                logger.error("WS2 falha na reconexão: %s", exc)

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
