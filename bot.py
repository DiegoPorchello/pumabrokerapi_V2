"""
bot.py — Bot de trading completo para a Puma Broker (Python).

Estratégia: Cruzamento de Médias Móveis (EMA9 × EMA21)
  CALL quando EMA9 cruza EMA21 para cima
  PUT  quando EMA9 cruza EMA21 para baixo

Uso:
  pip install -r requirements.txt
  cp .env.example .env && edite as credenciais
  python bot.py
"""

import asyncio
import logging
import os
import sys
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

# ── Imports da biblioteca ─────────────────────────────────────────────────────
sys.path.insert(0, os.path.dirname(__file__))
from pumabroker import PumaBroker, BarUpdateEvent, TradeUpdate

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("bot")

# ── Configuração ──────────────────────────────────────────────────────────────
@dataclass
class BotConfig:
    symbol:          str   = os.getenv("SYMBOL",     "AUDUSD")
    timeframe:       str   = os.getenv("TIMEFRAME",  "M1")
    amount:          float = float(os.getenv("AMOUNT",    "2"))
    wallet:          str   = os.getenv("WALLET",     "DEMO")    # DEMO para testar!
    max_open_trades: int   = int(os.getenv("MAX_TRADES",  "1"))
    stop_loss_daily: float = float(os.getenv("STOP_LOSS",  "20"))
    cooldown_s:      float = float(os.getenv("COOLDOWN_MS","5000")) / 1000
    ema9_period:     int   = 9
    ema21_period:    int   = 21


@dataclass
class BotState:
    closes:      deque = field(default_factory=lambda: deque(maxlen=100))
    open_trades: int   = 0
    profit:      float = 0.0
    wins:        int   = 0
    losses:      int   = 0
    last_order:  float = 0.0
    stopped:     bool  = False


cfg   = BotConfig()
state = BotState()
pb:   PumaBroker = None  # será inicializado no main


# ── Cálculo de EMA ────────────────────────────────────────────────────────────
def calc_ema(prices: list, period: int) -> float:
    if len(prices) < period:
        return 0.0
    k   = 2 / (period + 1)
    ema = sum(prices[:period]) / period
    for p in prices[period:]:
        ema = p * k + ema * (1 - k)
    return ema


# ── Detecção de sinal ─────────────────────────────────────────────────────────
def check_signal(closes: list) -> str | None:
    if len(closes) < cfg.ema21_period + 2:
        return None

    prev = closes[:-1]
    curr = closes

    ema9_prev  = calc_ema(prev, cfg.ema9_period)
    ema21_prev = calc_ema(prev, cfg.ema21_period)
    ema9_curr  = calc_ema(curr, cfg.ema9_period)
    ema21_curr = calc_ema(curr, cfg.ema21_period)

    if ema9_prev <= ema21_prev and ema9_curr > ema21_curr:
        return "CALL"
    if ema9_prev >= ema21_prev and ema9_curr < ema21_curr:
        return "PUT"
    return None


# ── Handler de candle ─────────────────────────────────────────────────────────
async def on_bar(bar: BarUpdateEvent) -> None:
    global pb

    if state.stopped:
        return

    state.closes.append(bar.bar.close)
    closes = list(state.closes)

    signal = check_signal(closes)
    if not signal:
        return

    import time
    now = time.time()

    # Gestão de risco
    if now - state.last_order < cfg.cooldown_s:
        logger.debug("Cooldown ativo.")
        return
    if state.open_trades >= cfg.max_open_trades:
        logger.debug("Max trades abertos.")
        return
    if state.profit <= -cfg.stop_loss_daily:
        logger.warning(f"⛔ STOP DIÁRIO atingido: R${state.profit:.2f}")
        state.stopped = True
        return

    ema9  = calc_ema(closes, cfg.ema9_period)
    ema21 = calc_ema(closes, cfg.ema21_period)

    logger.info("━" * 55)
    logger.info(f"SINAL: {signal} | {bar.symbol} @ {bar.bar.close:.5f}")
    logger.info(f"EMA9: {ema9:.5f} | EMA21: {ema21:.5f}")

    state.open_trades += 1
    state.last_order   = now

    try:
        if signal == "CALL":
            result = pb.buy_call(
                symbol=cfg.symbol,
                amount=cfg.amount,
                timeframe=cfg.timeframe,
                entry_price=bar.bar.close,
            )
        else:
            result = pb.buy_put(
                symbol=cfg.symbol,
                amount=cfg.amount,
                timeframe=cfg.timeframe,
                entry_price=bar.bar.close,
            )
        logger.info(f"✅ Ordem enviada: {str(result)[:80]}")
    except Exception as e:
        state.open_trades = max(0, state.open_trades - 1)
        logger.error(f"❌ Erro na ordem: {e}")


# ── Handler de resultado ──────────────────────────────────────────────────────
def on_trade_result(event: str, trade: TradeUpdate) -> None:
    state.open_trades = max(0, state.open_trades - 1)
    payout = trade.payout or 0.85

    if trade.status == "WIN":
        profit = trade.amount * payout
        state.profit += profit
        state.wins   += 1
        logger.info(f"🏆 WIN  | +R${profit:.2f} | Total: R${state.profit:.2f}")
    elif trade.status == "LOSS":
        state.profit -= trade.amount
        state.losses += 1
        logger.info(f"💀 LOSS | -R${trade.amount:.2f} | Total: R${state.profit:.2f}")
    elif trade.status == "DRAW":
        logger.info(f"🤝 DRAW | R${trade.amount:.2f} devolvido")

    total = state.wins + state.losses
    wr    = (state.wins / total * 100) if total > 0 else 0
    logger.info(f"W:{state.wins} L:{state.losses} WR:{wr:.1f}% | P&L: R${state.profit:.2f}")


# ── Status periódico ─────────────────────────────────────────────────────────
async def status_loop() -> None:
    while not state.stopped:
        await asyncio.sleep(60)
        total = state.wins + state.losses
        wr    = (state.wins / total * 100) if total > 0 else 0
        status = "⛔ PARADO" if state.stopped else "✅ Rodando"
        logger.info(
            f"[STATUS] W:{state.wins} L:{state.losses} WR:{wr:.1f}% "
            f"P&L:R${state.profit:.2f} Open:{state.open_trades} {status}"
        )


# ── Main ──────────────────────────────────────────────────────────────────────
async def main() -> None:
    global pb

    print("╔══════════════════════════════════════════════════════╗")
    print("║         PUMA BROKER BOT — EMA 9×21 (Python)         ║")
    print("╚══════════════════════════════════════════════════════╝")
    print(f"Símbolo:   {cfg.symbol}")
    print(f"Timeframe: {cfg.timeframe}")
    print(f"Valor:     R${cfg.amount}")
    print(f"Wallet:    {cfg.wallet}")
    print(f"Stop:      R${cfg.stop_loss_daily}")
    print("──────────────────────────────────────────────────────")

    email    = os.getenv("PUMA_EMAIL",    "")
    password = os.getenv("PUMA_PASSWORD", "")

    if not email or not password:
        logger.error("Configure PUMA_EMAIL e PUMA_PASSWORD no .env")
        sys.exit(1)

    pb = PumaBroker(
        email=email,
        password=password,
        wallet=cfg.wallet,
    )

    await pb.connect()

    # Registra handlers
    interval = cfg.timeframe.replace("M", "").replace("H", "")
    pb.on_bar(cfg.symbol, interval, lambda bar: asyncio.create_task(on_bar(bar)))
    pb.on_event("tradeUpdate", on_trade_result)

    logger.info(f"Bot aguardando candles de {cfg.symbol} {cfg.timeframe}...")

    # Roda status loop e listen em paralelo
    await asyncio.gather(
        status_loop(),
        pb.listen(),
    )


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Bot encerrado pelo usuário.")
