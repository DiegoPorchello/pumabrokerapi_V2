"""
bot.py -- Bot de trading com logica de QUADRANTE para a Puma Broker (Python).

Estrategia: Hybrid Probability (Q1-Q4)
  - Rastreia sequencia de cores (GREEN/RED)
  - Calcula aceleracao do corpo do candle
  - Entra na vela 4 (ENTRADA)
  - Se perdeu na 4, entra na 5 (SOROSGALE)
  - Ciclo reinicia quando a cor muda
"""

import asyncio
import logging
import os
import sys
import time as _time
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional
from dotenv import load_dotenv

load_dotenv()

sys.path.insert(0, os.path.dirname(__file__))
from pumabroker import PumaBroker, BarUpdateEvent, TradeUpdate

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("bot")


TIMEFRAME_SECONDS = {
    "M1": 60, "M5": 300, "M15": 900, "M30": 1800,
    "H1": 3600, "H4": 14400, "D1": 86400,
}


@dataclass
class BotConfig:
    symbol:          str   = os.getenv("SYMBOL", "BETHUSDT")
    timeframe:       str   = os.getenv("TIMEFRAME", "M1")
    amount:          float = float(os.getenv("AMOUNT", "2"))
    wallet:          str   = os.getenv("WALLET", "DEMO")
    max_open_trades: int   = int(os.getenv("MAX_TRADES", "1"))
    stop_loss_daily: float = float(os.getenv("STOP_LOSS", "20"))


@dataclass
class Candle:
    open:  float
    close: float
    high:  float
    low:   float
    time:  float = 0.0

    @property
    def color(self) -> str:
        return "GREEN" if self.close >= self.open else "RED"

    @property
    def body(self) -> float:
        return abs(self.close - self.open)

    @property
    def range(self) -> float:
        return self.high - self.low


@dataclass
class CandleRecord:
    open:         float
    close:        float
    high:         float
    low:          float
    time:         float
    cycle_id:     int
    quadrant:     str
    step:         str
    direction:    str
    probability:  float
    color:        str
    accelerating: bool
    accel_ratio:  float


@dataclass
class CompletedCycle:
    cycle_id:    int
    color:       str
    quadrant:    str
    step:        str
    seq_len:     int
    result:      str
    num_candles: int


@dataclass
class ProbabilityTracker:
    q1_wins:  int = 0
    q1_total: int = 0
    q2_wins:  int = 0
    q2_total: int = 0
    q3_wins:  int = 0
    q3_total: int = 0
    q4_wins:  int = 0
    q4_total: int = 0
    recent_cycles: deque = field(default_factory=lambda: deque(maxlen=50))

    def register_cycle(self, cycle: CompletedCycle) -> None:
        self.recent_cycles.append(cycle)
        q = cycle.quadrant
        won = cycle.result == "WIN"
        if q == "Q1":
            self.q1_total += 1
            if won:
                self.q1_wins += 1
        elif q == "Q2":
            self.q2_total += 1
            if won:
                self.q2_wins += 1
        elif q == "Q3":
            self.q3_total += 1
            if won:
                self.q3_wins += 1
        elif q == "Q4":
            self.q4_total += 1
            if won:
                self.q4_wins += 1

    def get_probability(self, quadrant: str) -> float:
        if quadrant == "Q1":
            w, t = self.q1_wins, self.q1_total
        elif quadrant == "Q2":
            w, t = self.q2_wins, self.q2_total
        elif quadrant == "Q3":
            w, t = self.q3_wins, self.q3_total
        elif quadrant == "Q4":
            w, t = self.q4_wins, self.q4_total
        else:
            return 0.0
        if t < 3:
            return 0.0
        return (w / t) * 100

    def get_stats(self) -> dict:
        return {
            "Q1": f"{self.q1_wins}/{self.q1_total} ({self.get_probability('Q1'):.1f}%)",
            "Q2": f"{self.q2_wins}/{self.q2_total} ({self.get_probability('Q2'):.1f}%)",
            "Q3": f"{self.q3_wins}/{self.q3_total} ({self.get_probability('Q3'):.1f}%)",
            "Q4": f"{self.q4_wins}/{self.q4_total} ({self.get_probability('Q4'):.1f}%)",
            "total_cycles": len(self.recent_cycles),
        }

@dataclass
class BotState:
    candles:            deque = field(default_factory=lambda: deque(maxlen=200))
    open_trades:        int   = 0
    profit:             float = 0.0
    wins:               int   = 0
    losses:             int   = 0
    stopped:            bool  = False
    last_candle_time:   float = 0.0
    last_candle_open:   float = 0.0
    last_candle_close:  float = 0.0
    last_candle_high:   float = 0.0
    last_candle_low:    float = float("inf")
    candle_count:       int   = 0
    history_loaded:     bool  = False
    last_trade_result:  str   = None
    min_probability:    float = float(os.getenv("MIN_PROBABILITY", "70"))

    # Cycle tracking
    cycle_id:           int   = 0
    seq_len:            int   = 0
    seq_color:          str   = ""
    cycle_quadrant:     str   = ""
    prev_body:          float = 0.0
    entry_sent:         bool  = False
    sorosgale_sent:     bool  = False
    cycle_result:       str   = "PENDING"
    registered_cycles:  set   = field(default_factory=set)

    completed_cycles:   deque = field(default_factory=lambda: deque(maxlen=100))
    candle_records:     deque = field(default_factory=lambda: deque(maxlen=500))
    prob_tracker:       ProbabilityTracker = field(default_factory=ProbabilityTracker)

    # Sorosgale antecipado
    pending_sorosgale:  bool  = False  # Se tem sorosgale pendente (perda antecipada)
    sorosgale_direction: str = ""     # Direção do sorosgale pendente
    sorosgale_task:     object = None # Task do timer de verificação


cfg   = BotConfig()
state = BotState()
pb:   PumaBroker = None


def candle_color(c: Candle) -> str:
    return "GREEN" if c.close >= c.open else "RED"


def candle_body(c: Candle) -> float:
    return abs(c.close - c.open)


def compute_quadrant(seq_len: int, accelerating: bool) -> str:
    if seq_len <= 3 and accelerating:
        return "Q1"
    elif seq_len >= 4 and accelerating:
        return "Q2"
    elif seq_len <= 3 and not accelerating:
        return "Q3"
    else:
        return "Q4"


def compute_direction(quadrant: str, color: str) -> str:
    if quadrant == "Q4":
        return "PUT" if color == "GREEN" else "CALL"
    return "CALL" if color == "GREEN" else "PUT"


def compute_step(seq_len: int) -> str:
    if seq_len == 1:
        return "INICIA"
    elif seq_len == 2:
        return "CONTINUIDADE"
    elif seq_len == 3:
        return "PREPARANDO"
    elif seq_len == 4:
        return "ENTRADA"
    else:
        return "SOROSGALE"


def format_time(unix_ts: float) -> str:
    if not unix_ts:
        return "??:??"
    return datetime.fromtimestamp(unix_ts, tz=timezone.utc).strftime("%H:%M")


def _find_last_record(cycle_id: int) -> Optional[CandleRecord]:
    for rec in reversed(state.candle_records):
        if rec.cycle_id == cycle_id:
            return rec
    return None


def _count_records(cycle_id: int) -> int:
    return sum(1 for r in state.candle_records if r.cycle_id == cycle_id)


# ---------------------------------------------------------------------------
# Cycle Management
# ---------------------------------------------------------------------------
def lock_previous_cycle() -> None:
    if state.cycle_id == 0:
        return

    last_record = _find_last_record(state.cycle_id)
    if last_record is None:
        return

    num = _count_records(state.cycle_id)

    completed = CompletedCycle(
        cycle_id=state.cycle_id,
        color=state.seq_color,
        quadrant=last_record.quadrant,
        step=last_record.step,
        seq_len=state.seq_len,
        result=state.cycle_result,
        num_candles=num,
    )
    state.completed_cycles.append(completed)

    if state.cycle_result in ("WIN", "LOSS") and state.cycle_id not in state.registered_cycles:
        state.prob_tracker.register_cycle(completed)
        state.registered_cycles.add(state.cycle_id)


def _maybe_register_cycle() -> None:
    if state.cycle_id in state.registered_cycles:
        return
    if state.cycle_result not in ("WIN", "LOSS"):
        return

    last_record = _find_last_record(state.cycle_id)
    if last_record is None:
        return

    num = _count_records(state.cycle_id)
    completed = CompletedCycle(
        cycle_id=state.cycle_id,
        color=state.seq_color,
        quadrant=last_record.quadrant,
        step=last_record.step,
        seq_len=state.seq_len,
        result=state.cycle_result,
        num_candles=num,
    )

    for c in state.completed_cycles:
        if c.cycle_id == state.cycle_id:
            c.result = state.cycle_result
            break
    else:
        state.completed_cycles.append(completed)

    state.prob_tracker.register_cycle(completed)
    state.registered_cycles.add(state.cycle_id)


def process_candle(candle: Candle, silent: bool = False) -> CandleRecord:
    color = candle.color
    body = candle.body
    new_cycle = False

    if state.seq_len > 0 and color != state.seq_color:
        lock_previous_cycle()
        state.cycle_id += 1
        state.seq_len = 1
        state.seq_color = color
        state.entry_sent = False
        state.sorosgale_sent = False
        state.cycle_result = "PENDING"
        new_cycle = True
    elif state.seq_len == 0:
        state.cycle_id = 1
        state.seq_len = 1
        state.seq_color = color
        new_cycle = True
    else:
        state.seq_len += 1

    accelerating = False
    accel_ratio = 1.0
    if state.prev_body > 0:
        accel_ratio = body / state.prev_body
        accelerating = accel_ratio > 1.15

    quadrant    = compute_quadrant(state.seq_len, accelerating)
    direction   = compute_direction(quadrant, color)
    step        = compute_step(state.seq_len)
    probability = state.prob_tracker.get_probability(quadrant)

    # Atualiza quadrante do ciclo atual
    state.cycle_quadrant = quadrant

    record = CandleRecord(
        open=candle.open,
        close=candle.close,
        high=candle.high,
        low=candle.low,
        time=candle.time,
        cycle_id=state.cycle_id,
        quadrant=quadrant,
        step=step,
        direction=direction,
        probability=probability,
        color=color,
        accelerating=accelerating,
        accel_ratio=accel_ratio,
    )
    state.candle_records.append(record)
    state.prev_body = body

    if not silent:
        if new_cycle and state.cycle_id > 1:
            print_completed_block()
        print_candle_log(record)
        print_cycle_header(record)

    return record


# ---------------------------------------------------------------------------
# Display
# ---------------------------------------------------------------------------
def print_completed_block() -> None:
    recent = list(state.completed_cycles)[-5:]
    if not recent:
        return
    logger.info("=" * 60)
    logger.info("  QUADRANTES FINALIZADOS (travados)")
    logger.info("-" * 60)
    for c in reversed(recent):
        logger.info(
            f"  [C{c.cycle_id}] {c.quadrant} | Seq={c.seq_len} {c.color} | "
            f"{c.step} | {c.num_candles} velas | {c.result}"
        )
    logger.info("=" * 60)


def print_candle_log(record: CandleRecord) -> None:
    time_str = format_time(record.time)
    logger.info(
        f"[C{record.cycle_id}] {record.quadrant}|{record.step}|{record.direction}|"
        f"{record.probability:.1f}% | {record.color} "
        f"O={record.open:.5f} C={record.close:.5f} "
        f"H={record.high:.5f} L={record.low:.5f} @ {time_str}"
    )


def print_cycle_header(record: CandleRecord) -> None:
    accel_str = "SIM" if record.accelerating else "NAO"
    stats = state.prob_tracker.get_stats()
    logger.info(
        f"CICLO ATUAL #{state.cycle_id} | {record.quadrant} | "
        f"Seq={state.seq_len} {state.seq_color} | "
        f"Acel={accel_str} ({record.accel_ratio:.2f}) | "
        f"Prob={record.probability:.1f}% | "
        f"Min={state.min_probability}% | "
        f"W:{state.wins} L:{state.losses} | "
        f"P&L: R${state.profit:.2f}"
    )


async def execute_order(signal: str, price: float) -> bool:
    global pb

    if state.open_trades >= cfg.max_open_trades:
        logger.debug("Max trades abertos - ignorando sinal.")
        return False
    if state.profit <= -cfg.stop_loss_daily:
        logger.warning(f"STOP DIARIO atingido: R${state.profit:.2f}")
        state.stopped = True
        return False

    logger.info("=" * 55)
    logger.info(f"EXECUTANDO: {signal} | {cfg.symbol} @ {price:.5f}")
    logger.info(f"Ciclo: {state.cycle_id} | Seq: {state.seq_len} {state.seq_color}")

    state.open_trades += 1

    try:
        if signal == "CALL":
            result = pb.buy_call(
                symbol=cfg.symbol,
                amount=cfg.amount,
                timeframe=cfg.timeframe,
                entry_price=price,
            )
        else:
            result = pb.buy_put(
                symbol=cfg.symbol,
                amount=cfg.amount,
                timeframe=cfg.timeframe,
                entry_price=price,
            )
        logger.info(f"Ordem enviada: {str(result)[:80]}")
        return True
    except Exception as e:
        state.open_trades = max(0, state.open_trades - 1)
        logger.error(f"Erro na ordem: {e}")
        return False


# ---------------------------------------------------------------------------
# Sorosgale Antecipado — verifica 5s antes do candle fechar
# ---------------------------------------------------------------------------
async def _check_anticipated_loss(entry_price: float, direction: str, candle_open_time: float):
    """
    Timer que roda 5 segundos antes do candle fechar.
    Verifica se o preço está contra a operação e prepara sorosgale.
    """
    timeframe_s = TIMEFRAME_SECONDS.get(cfg.timeframe, 60)
    # Espera até faltar 5 segundos para o candle fechar
    wait_time = timeframe_s - 5
    if wait_time <= 0:
        wait_time = 5

    await asyncio.sleep(wait_time)

    if state.stopped:
        return

    # Pega preço atual
    current_price = state.last_candle_close

    # Verifica se está perdendo
    is_losing = False
    if direction == "CALL" and current_price < entry_price:
        is_losing = True
    elif direction == "PUT" and current_price > entry_price:
        is_losing = True

    if is_losing:
        logger.info(
            f"!! PERDA ANTECIPADA detectada !! Preco atual: {current_price:.5f} vs "
            f"entrada: {entry_price:.5f} | Preparando sorosgale para proximo candle"
        )
        state.pending_sorosgale = True
        state.sorosgale_direction = direction  # Mesma direção
    else:
        logger.info(
            f"Verificacao antecipada: Preco {current_price:.5f} vs entrada {entry_price:.5f} "
            f"- favoravel, sem sorosgale"
        )


def _start_anticipation(entry_price: float, direction: str, candle_open_time: float):
    """Inicia o timer de verificação antecipada."""
    # Cancela timer anterior se existir
    if state.sorosgale_task and not state.sorosgale_task.done():
        state.sorosgale_task.cancel()

    state.sorosgale_task = asyncio.ensure_future(
        _check_anticipated_loss(entry_price, direction, candle_open_time)
    )


async def on_bar(bar: BarUpdateEvent) -> None:
    if state.stopped:
        return

    bar_time = bar.bar.time
    is_new_candle = (bar_time != state.last_candle_time)

    if is_new_candle and state.last_candle_time > 0:
        state.candle_count += 1

        closed_candle = Candle(
            open=state.last_candle_open,
            close=state.last_candle_close,
            high=state.last_candle_high,
            low=state.last_candle_low,
            time=state.last_candle_time,
        )
        state.candles.append(closed_candle)

        # ── VERIFICA SOROSGALE PENDENTE (antecipado) ──
        if state.pending_sorosgale and not state.sorosgale_sent:
            probability = state.prob_tracker.get_probability(state.cycle_quadrant)
            entry_price = bar.bar.close
            logger.info(
                f">> SOROSGALE ANTECIPADO! Preco: {entry_price:.5f} | "
                f"Prob: {probability:.1f}%"
            )
            if probability >= state.min_probability:
                await execute_order(state.sorosgale_direction, entry_price)
                state.sorosgale_sent = True
                state.pending_sorosgale = False
            else:
                logger.info(
                    f">> SOROSGALE BLOQUEADO: Prob {probability:.1f}% < {state.min_probability}%"
                )
                state.pending_sorosgale = False

        record = process_candle(closed_candle)

        entry_price = bar.bar.close
        step = record.step
        probability = record.probability

        if step == "ENTRADA" and not state.entry_sent:
            if probability >= state.min_probability:
                logger.info(
                    f">> ENTRADA! prob {probability:.1f}% >= {state.min_probability}% "
                    f"- entrando @ {entry_price:.5f}"
                )
                await execute_order(record.direction, entry_price)
                state.entry_sent = True
                # Inicia timer de verificacao antecipada
                _start_anticipation(entry_price, record.direction, bar_time)
            else:
                logger.info(
                    f">> ENTRADA BLOQUEADA: prob {probability:.1f}% < {state.min_probability}%"
                )

        elif step == "SOROSGALE" and not state.sorosgale_sent:
            if state.last_trade_result == "LOSS":
                if probability >= state.min_probability:
                    logger.info(
                        f">> SOROSGALE! prob {probability:.1f}% >= {state.min_probability}% "
                        f"- entrando @ {entry_price:.5f}"
                    )
                    await execute_order(record.direction, entry_price)
                    state.sorosgale_sent = True
                else:
                    logger.info(
                        f">> SOROSGALE BLOQUEADO: prob {probability:.1f}% < {state.min_probability}%"
                    )
            else:
                if state.last_trade_result is not None:
                    logger.info("SOROSGALE ignorado (nao houve LOSS)")

        state.last_candle_time = bar_time

    elif is_new_candle and state.last_candle_time == 0:
        state.last_candle_time = bar_time
        logger.info(f"Primeiro candle recebido: time={bar_time}")

    state.last_candle_close = bar.bar.close
    if is_new_candle:
        state.last_candle_high = bar.bar.high
        state.last_candle_low = bar.bar.low
    else:
        if bar.bar.high > state.last_candle_high:
            state.last_candle_high = bar.bar.high
        if bar.bar.low < state.last_candle_low:
            state.last_candle_low = bar.bar.low
    state.last_candle_open = bar.bar.open


def on_trade_result(event: str, trade: TradeUpdate) -> None:
    state.open_trades = max(0, state.open_trades - 1)
    payout = trade.payout or 0.85

    if trade.status == "WIN":
        profit = trade.amount * payout
        state.profit += profit
        state.wins += 1
        state.last_trade_result = "WIN"
        state.cycle_result = "WIN"
        # Se tinha sorosgale pendente e ganhou, limpa
        if state.pending_sorosgale:
            state.pending_sorosgale = False
            logger.info("WIN - sorosgale pendente cancelado (operacao ganhou)")
        _maybe_register_cycle()
        logger.info(
            f"WIN  | +R${profit:.2f} | Total: R${state.profit:.2f} | "
            f"W:{state.wins} L:{state.losses}"
        )

    elif trade.status == "LOSS":
        state.profit -= trade.amount
        state.losses += 1
        state.last_trade_result = "LOSS"
        state.cycle_result = "LOSS"
        state.entry_sent = True
        # NÃO reseta sorosgale_sent aqui — o sorosgale já pode ter sido
        # enviado pelo timer antecipado. Se pending_sorosgale está ativo,
        # o timer já cuidou disso.
        if not state.pending_sorosgale:
            state.sorosgale_sent = False
        _maybe_register_cycle()
        logger.info(
            f"LOSS | -R${trade.amount:.2f} | Total: R${state.profit:.2f} | "
            f"W:{state.wins} L:{state.losses}"
        )

    elif trade.status == "DRAW":
        state.last_trade_result = "DRAW"
        state.cycle_result = "DRAW"
        logger.info(f"DRAW | R${trade.amount:.2f} devolvido")

    total = state.wins + state.losses
    wr = (state.wins / total * 100) if total > 0 else 0
    logger.info(f"W:{state.wins} L:{state.losses} WR:{wr:.1f}% | P&L: R${state.profit:.2f}")


async def load_history() -> None:
    global pb

    interval = cfg.timeframe.replace("M", "").replace("H", "")
    try:
        if pb._auth:
            pb._auth.get_access_token()
            import requests as _req
            url = "https://trade.pumabroker.com/api/v1/tradingview/history"
            params = {
                "symbol": cfg.symbol,
                "resolution": interval,
                "countback": 100,
            }
            headers = {
                "Authorization": f"Bearer {pb._trades_api._jwt}",
                "Accept": "application/json",
            }
            resp = _req.get(url, params=params, headers=headers, timeout=10)
            if resp.status_code == 200:
                data = resp.json()
                if data.get("s") == "ok" and "c" in data:
                    opens  = data.get("o", [])
                    highs  = data.get("h", [])
                    lows   = data.get("l", [])
                    closes = data.get("c", [])
                    times  = data.get("t", [])

                    for i in range(len(closes)):
                        if closes[i] and closes[i] > 0:
                            candle = Candle(
                                open=opens[i] if i < len(opens) else closes[i],
                                close=closes[i],
                                high=highs[i] if i < len(highs) else closes[i],
                                low=lows[i] if i < len(lows) else closes[i],
                                time=times[i] if i < len(times) else 0,
                            )
                            state.candles.append(candle)

                    for candle in state.candles:
                        process_candle(candle, silent=True)

                    logger.info(
                        f"Historico carregado: {len(state.candles)} candles, "
                        f"{len(state.completed_cycles)} ciclos para {cfg.symbol} {cfg.timeframe}"
                    )
                    state.history_loaded = True
                    return
        logger.warning("Historico nao disponivel - usando apenas candles em tempo real.")
    except Exception as e:
        logger.warning(f"Erro ao carregar historico: {e} - usando candles em tempo real.")


async def status_loop() -> None:
    while not state.stopped:
        await asyncio.sleep(60)
        total = state.wins + state.losses
        wr = (state.wins / total * 100) if total > 0 else 0
        stats = state.prob_tracker.get_stats()
        status = "PARADO" if state.stopped else "Rodando"
        logger.info(
            f"[STATUS] W:{state.wins} L:{state.losses} WR:{wr:.1f}% "
            f"P&L:R${state.profit:.2f} Open:{state.open_trades} "
            f"Ciclo:#{state.cycle_id} Seq:{state.seq_len} {state.seq_color} | "
            f"Prob Q1:{stats['Q1']} Q2:{stats['Q2']} Q3:{stats['Q3']} Q4:{stats['Q4']} | "
            f"Min:{state.min_probability}% {status}"
        )


async def main() -> None:
    global pb

    print("+============================================================+")
    print("|      PUMA BROKER BOT - QUADRANTE (Hybrid Probability)     |")
    print("+============================================================+")
    print(f"Simbolo:      {cfg.symbol}")
    print(f"Timeframe:    {cfg.timeframe}")
    print(f"Valor:        R${cfg.amount}")
    print(f"Wallet:       {cfg.wallet}")
    print(f"Stop:         R${cfg.stop_loss_daily}")
    print(f"Min Prob:     {state.min_probability}%")
    print("----------------------------------------------------------")

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

    await load_history()

    interval = cfg.timeframe.replace("M", "").replace("H", "")
    pb.on_bar(cfg.symbol, interval, lambda bar: asyncio.create_task(on_bar(bar)))
    pb.on_event("tradeUpdate", on_trade_result)

    logger.info(f"Bot aguardando candles de {cfg.symbol} {cfg.timeframe}...")
    logger.info(f"Buffer: {len(state.candles)} candles | Historico: {state.history_loaded}")

    await asyncio.gather(
        status_loop(),
        pb.listen(),
    )


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Bot encerrado pelo usuario.")
