"""Motor offline e seguro de sinais. Nunca envia ordens para a corretora."""
from __future__ import annotations
import argparse
import csv
import json
from dataclasses import asdict, dataclass
from pathlib import Path

@dataclass(frozen=True)
class Candle:
    time: int
    open: float
    high: float
    low: float
    close: float

    def validate(self) -> None:
        if self.time < 0 or min(self.open, self.high, self.low, self.close) <= 0:
            raise ValueError("Timestamp/preços inválidos")
        if self.high < max(self.open, self.close) or self.low > min(self.open, self.close):
            raise ValueError("OHLC inconsistente")

@dataclass(frozen=True)
class Trade:
    signal_time: int
    entry_time: int
    exit_time: int
    direction: str
    entry: float
    exit: float
    result: str
    pnl: float

def read_csv(path: str) -> list[Candle]:
    candles = []
    with open(path, newline="", encoding="utf-8-sig") as source:
        for row in csv.DictReader(source):
            candle = Candle(
                int(row["time"]), float(row["open"]), float(row["high"]),
                float(row["low"]), float(row["close"])
            )
            candle.validate()
            candles.append(candle)
    if any(b.time <= a.time for a, b in zip(candles, candles[1:])):
        raise ValueError("Candles precisam estar em ordem cronológica sem duplicatas")
    return candles

def signal(history: list[Candle], fast: int = 8, slow: int = 21) -> str | None:
    """Sinal decidido SOMENTE com velas encerradas; usa tendência e pullback."""
    if not 2 <= fast < slow:
        raise ValueError("Janelas fast/slow inválidas")
    if len(history) < slow + 3:
        return None
    closes = [c.close for c in history]
    ma_fast = sum(closes[-fast:]) / fast
    ma_slow = sum(closes[-slow:]) / slow
    prior_fast = sum(closes[-fast-1:-1]) / fast
    last, prev = history[-1], history[-2]
    if ma_fast > ma_slow and ma_fast > prior_fast and prev.close < prev.open and last.close > last.open and last.close > prev.high:
        return "CALL"
    if ma_fast < ma_slow and ma_fast < prior_fast and prev.close > prev.open and last.close < last.open and last.close < prev.low:
        return "PUT"
    return None

def backtest(candles: list[Candle], expiry: int = 1, payout: float = .85,
             stake: float = 1.0, fast: int = 8, slow: int = 21) -> dict:
    """Entrada na abertura da vela seguinte ao sinal; saída no fechamento após expiry velas.
    Não presume que a corretora aceita esse preço/tempo. É um teste de hipótese."""
    if expiry < 1 or stake <= 0 or not 0 < payout <= 1:
        raise ValueError("expiry, stake ou payout inválidos")
    trades: list[Trade] = []
    next_allowed = slow + 3
    for i in range(slow + 2, len(candles) - expiry):
        if i + 1 < next_allowed:
            continue
        direction = signal(candles[:i + 1], fast, slow)
        if direction is None:
            continue
        entry_candle = candles[i + 1]
        final_candle = candles[i + expiry]
        # expiry=1: abre na próxima vela e liquida no fechamento dessa vela.
        entry = entry_candle.open
        exit_price = final_candle.close
        won = exit_price > entry if direction == "CALL" else exit_price < entry
        # Empate tratado como perda por conservadorismo; conferir regra da corretora.
        pnl = round(stake * (payout if won else -1), 8)
        trades.append(Trade(candles[i].time, entry_candle.time, final_candle.time,
                            direction, entry, exit_price, "WIN" if won else "LOSS", pnl))
        next_allowed = i + expiry + 1
    wins = sum(t.result == "WIN" for t in trades)
    count = len(trades)
    return {
        "mode": "PAPER_OFFLINE_ONLY", "count": count, "wins": wins,
        "losses": count - wins, "win_rate": wins / count if count else None,
        "breakeven_rate": 1 / (1 + payout),
        "total_pnl": round(sum(t.pnl for t in trades), 8),
        "trades": [asdict(t) for t in trades],
        "assumptions": "Candles contínuos do mesmo intervalo, execução na abertura seguinte, sem latência, payout fixo; não reproduz fielmente liquidação na Puma."
    }

def main() -> None:
    parser = argparse.ArgumentParser(description="Backtest de sinais sem enviar ordens")
    parser.add_argument("--csv", required=True, help="CSV com time,open,high,low,close")
    parser.add_argument("--expiry", type=int, default=1, help="Duração em número de candles")
    parser.add_argument("--payout", type=float, default=.85)
    parser.add_argument("--stake", type=float, default=1.0)
    parser.add_argument("--output", default="", help="Salvar relatório JSON")
    args = parser.parse_args()
    report = backtest(read_csv(args.csv), args.expiry, args.payout, args.stake)
    body = json.dumps(report, ensure_ascii=False, indent=2)
    if args.output:
        Path(args.output).write_text(body, encoding="utf-8")
    print(body)

if __name__ == "__main__":
    main()
