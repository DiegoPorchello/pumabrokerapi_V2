"""
models.py — Modelos baseados nos frames REAIS capturados via DevTools.

WS1 frames reais:
  42["price",{"symbol":"EURUSD","price":1.15891,"open":1.15891,
    "high":1.15891,"low":1.15891,"close":1.15891,"volume":0,
    "timestamp":1781557139950,"klineStart":1781557200000}]
  42["candle",{...}]
  42["serverTime",{"time":1781557260423}]

WS2 frames reais:
  {"type":"bar_update","symbol":"EURUSD","interval":"5",
   "bar":{"time":1781556900,"open":1.15884,"high":1.159,
          "low":1.15883,"close":1.15896,"volume":190.0},
   "last_bar":{...}}
  {"method":"server_time"}
  {"type":"server_time","timestamp":1781557272731}

WS3 frames reais (Socket.IO namespaces /trades e /otc):
  0{"sid":"AAxrTo2u...","upgrades":[],"pingInterval":25000,"pingTimeout":20000}
  40/trades,
  40/otc,
  42/trades,["subscribe","28318"]   ← 28318 = account_id
  40/otc,{"sid":"UgJ02aQp..."}
  2  ← ping Engine.IO
  3  ← pong Engine.IO
"""

from __future__ import annotations
from typing import Any, Dict, List, Optional
from pydantic import BaseModel, Field


# ══════════════════════════════════════════════════════════════════════════════
# WS1 — Preços tick (wss://trade.pumabroker.com/ws/)
# ══════════════════════════════════════════════════════════════════════════════

class PriceEvent(BaseModel):
    """
    Frame: 42["price", <este objeto>]
    Recebido continuamente com o preço atual do ativo.
    """
    symbol:     str
    price:      float
    open:       float
    high:       float
    low:        float
    close:      float
    volume:     float
    timestamp:  int              # unix ms
    klineStart: Optional[int] = None
    klineClosed: Optional[bool] = None
    verify:     Optional[str]  = None


class CandleEvent(BaseModel):
    """
    Frame: 42["candle", <este objeto>]
    Vela fechada em tempo real.
    """
    symbol:  str
    time:    int
    open:    Optional[float] = None
    high:    Optional[float] = None
    low:     Optional[float] = None
    close:   Optional[float] = None
    volume:  Optional[float] = None


class ServerTimeWS1(BaseModel):
    """
    Frame: 42["serverTime", {"time": 1781557260423}]
    Timestamp do servidor para sincronização.
    """
    time: int


# ══════════════════════════════════════════════════════════════════════════════
# WS2 — Candles OHLCV (wss://wsmt5.pumabroker.com/)
# ══════════════════════════════════════════════════════════════════════════════

class Bar(BaseModel):
    """
    Estrutura de uma vela dentro do bar_update.
    """
    time:   int
    open:   float
    high:   float
    low:    float
    close:  float
    volume: float


class BarUpdateEvent(BaseModel):
    """
    Frame: {"type":"bar_update","symbol":"EURUSD","interval":"5",
            "bar":{...},"last_bar":{...}}

    interval: tamanho da vela em minutos (string: "1", "5", "15", etc.)
    """
    type:     str = "bar_update"
    symbol:   str
    interval: str           # "1", "5", "15", "30", "60", etc.
    bar:      Bar
    last_bar: Optional[Bar] = None


class ServerTimeWS2(BaseModel):
    """
    Frame: {"type":"server_time","timestamp":1781557272731}
    Heartbeat/sync do servidor WS2.
    """
    type:      str = "server_time"
    timestamp: int


class ServerTimePingWS2(BaseModel):
    """
    Frame enviado pelo cliente: {"method":"server_time"}
    Solicita o timestamp do servidor.
    """
    method: str = "server_time"


# ══════════════════════════════════════════════════════════════════════════════
# WS3 — Ordens / Trades (Socket.IO /trades e /otc)
# ══════════════════════════════════════════════════════════════════════════════

class EIOHandshake(BaseModel):
    """
    Pacote 0 do Engine.IO recebido na abertura da conexão.
    Frame: 0{"sid":"...","upgrades":[],"pingInterval":25000,"pingTimeout":20000}
    """
    sid:          str
    upgrades:     List[str] = Field(default_factory=list)
    pingInterval: int = 25000
    pingTimeout:  int = 20000
    maxPayload:   Optional[int] = None


class TradeSubscribePayload(BaseModel):
    """
    Frame enviado ao namespace /trades para receber eventos da conta.
    Frame real: 42/trades,["subscribe","28318"]
    O segundo elemento é o account_id como string.
    """
    event:      str = "subscribe"
    account_id: str   # ex: "28318"


class OrderRequest(BaseModel):
    """
    Payload REST REAL capturado via DevTools → Fetch/XHR → trades → Payload
    (15/06/2026 23:22)

    Endpoint confirmado:
      POST https://trade.pumabroker.com/trades
      Authorization: Bearer <jwt_token>
      Content-Type: application/json

    Payload completo real:
      {
        "userId":     "28318",
        "symbol":     "AUDUSD",
        "direction":  "CALL",
        "amount":     2,
        "duration":   530,
        "entryPrice": 0.70563,
        "mode":       "CANDLE_TIME",
        "payout":     0.85,
        "timeframe":  "M15",
        "verify":     "gAAAAABqMLMX8x8K...",
        "wallet":     "REAL"
      }

    Campos confirmados:
      - direction:  "CALL" | "PUT" (maiúsculas)
      - mode:       "CANDLE_TIME"  (fixo — vela inteira)
      - wallet:     "REAL" | "DEMO"
      - timeframe:  "M1" | "M5" | "M15" | "M30" | "H1"
      - verify:     token anti-fraude gerado pelo frontend (renovar por sessão)
      - duration:   segundos ate expiracao (530s para M15)
      - entryPrice: preco atual no momento do clique
      - payout:     percentual de retorno (0.85 = 85%)
    """
    userId:     str
    symbol:     str
    direction:  str
    amount:     float
    duration:   int
    entryPrice: float
    mode:       str   = "CANDLE_TIME"
    payout:     float = 0.85
    timeframe:  str   = "M1"
    verify:     str   = ""
    wallet:     str   = "REAL"


class TradeUpdate(BaseModel):
    """
    Evento recebido via 42/trades,["tradeUpdate", <este objeto>]

    Frame REAL capturado (15/06/2026 22:38:54):
      id:          "5186325"
      uid:         "364a4ac0f65720fa8b450f7b0b32cd"
      userId:      "28318"
      currency:    "BETHUSDT"
      symbol:      "BETHUSDT"
      direction:   "CALL"
      amount:      2
      entryPrice:  "911.31632"
      exitPrice:   null          → preenchido quando a ordem fecha
      profit:      0             → lucro final (0 enquanto ACTIVE)
      payout:      0.87          → 87% de retorno se ganhar
      status:      "ACTIVE"      → "ACTIVE" | "WIN" | "LOSS" | "DRAW"
      isDemo:      false
    """
    id:          str
    uid:         Optional[str]   = None
    userId:      Optional[str]   = None
    currency:    Optional[str]   = None
    symbol:      str
    direction:   str              # "CALL" | "PUT"
    amount:      float
    entryPrice:  Optional[str]   = None
    exitPrice:   Optional[str]   = None
    profit:      Optional[float] = None
    payout:      Optional[float] = None   # ex: 0.87 = 87%
    status:      str = "ACTIVE"           # "ACTIVE" | "WIN" | "LOSS" | "DRAW"
    isDemo:      Optional[bool]  = None

    def is_active(self) -> bool:
        return self.status == "ACTIVE"

    def is_win(self) -> bool:
        return self.status == "WIN"

    def is_loss(self) -> bool:
        return self.status == "LOSS"

    def payout_percent(self) -> float:
        """Retorna o payout em % (ex: 0.87 → 87.0)"""
        return (self.payout or 0) * 100


# Alias para compatibilidade
OrderResult = TradeUpdate


# ══════════════════════════════════════════════════════════════════════════════
# REST — Perfil e Conta
# ══════════════════════════════════════════════════════════════════════════════

class AccountInfo(BaseModel):
    """
    Resposta do endpoint GET /accounts
    Status 304 = cached (dados do usuário autenticado)
    """
    id:       Optional[int]   = None
    balance:  Optional[float] = None
    currency: Optional[str]   = None
    type:     Optional[str]   = None   # "real" | "demo"
    extra:    Dict[str, Any]  = Field(default_factory=dict)

    model_config = {"extra": "allow"}


class UserProfile(BaseModel):
    """
    Resposta do endpoint GET /me
    """
    id:    Optional[int]  = None
    email: Optional[str]  = None
    name:  Optional[str]  = None
    extra: Dict[str, Any] = Field(default_factory=dict)

    model_config = {"extra": "allow"}
