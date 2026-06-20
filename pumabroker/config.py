"""
config.py — Configuração central da biblioteca pumabroker.

Endpoints descobertos via DevTools → Network → Socket (15/06/2026):

  WS1: wss://trade.pumabroker.com/ws/?EIO=4&transport=websocket
       → Socket.IO v4, namespace raiz
       → Eventos: price, candle, serverTime

  WS2: wss://wsm5.pumabroker.com/
       → WebSocket puro (não Socket.IO)
       → Eventos: bar_update, server_time

  WS3: wss://trade.pumabroker.com/socket.io/?EIO=4&transport=websocket
       → Socket.IO v4, namespaces /trades e /otc
       → Eventos: subscribe, order (ordens e trades)

  Auth: Cookie server_name_session=<token>
"""

import os
from dataclasses import dataclass, field
from typing import Optional


@dataclass
class PumaBrokerConfig:
    # ── Domínio ───────────────────────────────────────────────────────────────
    TRADE_HOST:  str = "trade.pumabroker.com"
    MARKET_HOST: str = "wsm5.pumabroker.com"
    BASE_URL:    str = "https://trade.pumabroker.com"

    # ── REST endpoints (descobertos via XHR) ──────────────────────────────────
    ME_URL:       str = "https://trade.pumabroker.com/me"
    ACCOUNTS_URL: str = "https://trade.pumabroker.com/accounts"
    BALANCE_URL:  str = "https://trade.pumabroker.com/balance"
    SETTINGS_URL: str = "https://trade.pumabroker.com/settings"
    ACTIVE_URL:   str = "https://trade.pumabroker.com/active"

    # ── Endpoint de ordens (REST POST — confirmado 15/06/2026) ───────────────
    # Descoberto via DevTools → Fetch/XHR → "trades" → Headers → Request URL
    TRADES_URL:   str = "https://trade.pumabroker.com/trades"

    # ── WebSocket 1 — Socket.IO — preços tick ─────────────────────────────────
    # Frames observados: 42["price",{...}], 42["candle",{...}], 42["serverTime",{...}]
    WS_PRICE_URL: str = "wss://trade.pumabroker.com/ws/?EIO=4&transport=websocket"

    # ── WebSocket 2 — puro — candles OHLCV ───────────────────────────────────
    # Frames observados: {"type":"bar_update","symbol":"EURUSD","interval":"5",...}
    WS_MARKET_URL: str = "wss://wsm5.pumabroker.com/"

    # ── WebSocket 3 — Socket.IO — ordens/trades ───────────────────────────────
    # Namespaces: /trades, /otc
    # Frames observados: 40/trades, 42/trades,["subscribe","28318"]
    WS_TRADES_URL: str = "wss://trade.pumabroker.com/socket.io/?EIO=4&transport=websocket"

    # ── Autenticação ──────────────────────────────────────────────────────────
    # Cookie descoberto: server_name_session=cd0dc3ba351b950fc7621ef63b19d855
    # ── Login (confirmado 16/06/2026) ────────────────────────────────────────
    # POST /login → payload: {email, password} → response: {user: {..., token: "..."}}
    LOGIN_URL: str = "https://trade.pumabroker.com/login"

    SESSION_COOKIE: str = "server_name_session"
    SESSION_TOKEN: Optional[str] = field(
        default_factory=lambda: os.getenv("PUMA_SESSION")
    )

    # ── Socket.IO Engine.IO v4 ────────────────────────────────────────────────
    # Descoberto: pingInterval=25000, pingTimeout=20000
    EIO_PING_INTERVAL: int = 25   # segundos
    EIO_PING_TIMEOUT:  int = 20

    # ── Reconexão ─────────────────────────────────────────────────────────────
    WS_RECONNECT_DELAY: int = 5
    WS_MAX_RECONNECT:   int = 10

    # ── HTTP ──────────────────────────────────────────────────────────────────
    HTTP_TIMEOUT: int = 10

    # ── Headers padrão ────────────────────────────────────────────────────────
    DEFAULT_HEADERS: dict = field(default_factory=lambda: {
        "Accept":          "application/json, text/plain, */*",
        "Accept-Language": "pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7",
        "Origin":          "https://trade.pumabroker.com",
        "Referer":         "https://trade.pumabroker.com/",
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0.0.0 Safari/537.36"
        ),
    })

    # ── Logging ───────────────────────────────────────────────────────────────
    LOG_LEVEL: str = field(default_factory=lambda: os.getenv("LOG_LEVEL", "INFO"))


config = PumaBrokerConfig()
