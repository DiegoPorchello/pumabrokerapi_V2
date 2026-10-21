"""Coletor WS2 read-only pelo proxy LOCAL já existente (127.0.0.1:3002).

Nunca envia ordens. Exige proxy inicializado separadamente.
Deduplica updates de vela e só calcula sinais em velas FINALIZADAS.
"""
import argparse
import asyncio
import csv
import json
import logging
import time
from collections import deque
from pathlib import Path

from paper_probability_bot import Candle, signal

LOG = logging.getLogger("puma.paper.collector")
INTERVALS = {"1": 60, "2": 120, "3": 180, "5": 300, "15": 900}

class ClosedBarCollector:
    def __init__(self, symbol="BETHUSDT", interval="1", output="candles_live.csv"):
        if interval not in INTERVALS:
            raise ValueError("Intervalo inválido")
        self.symbol = symbol.upper()
        self.interval = interval
        self.seconds = INTERVALS[interval]
        self.output = Path(output)
        self.current = None
        self.history = deque(maxlen=2000)
        self.signals = []
        self.gap_count = 0

    def ingest(self, payload: dict):
        if payload.get("type") != "bar_update":
            return None
        if payload.get("symbol", "").upper() != self.symbol or str(payload.get("interval")) != self.interval:
            return None
        raw = payload.get("bar") or {}
        try:
            bar = Candle(time=int(raw["time"]), open=float(raw["open"]), high=float(raw["high"]),
                         low=float(raw["low"]), close=float(raw["close"]))
            bar.validate()
        except (KeyError, TypeError, ValueError) as exc:
            LOG.warning("Candle inválido ignorado: %s", exc)
            return None
        if bar.time % self.seconds != 0:
            LOG.warning("Timestamp fora de alinhamento: %s", bar.time)
            return None
        if self.current is None:
            self.current = bar
            return None
        if bar.time == self.current.time:
            self.current = bar  # update parcial do mesmo candle
            return None
        if bar.time < self.current.time:
            LOG.warning("Update antigo descartado: %s", bar.time)
            return None
        finished = self.current
        self.current = bar
        if bar.time - finished.time != self.seconds:
            self.gap_count += 1
            self.history.clear()
            LOG.warning("Lacuna entre candles; histórico da estratégia reiniciado")
            return None
        self.history.append(finished)
        self._save(finished)
        direction = signal(list(self.history))
        if direction:
            entry = {"signal_time": finished.time, "symbol": self.symbol,
                     "timeframe": self.interval, "direction": direction,
                     "mode": "SIGNAL_ONLY_NO_ORDERS"}
            self.signals.append(entry)
            LOG.info("SINAL (sem ordem): %s", entry)
            return entry
        return None

    def _save(self, bar: Candle):
        self.output.parent.mkdir(parents=True, exist_ok=True)
        fresh = not self.output.exists() or self.output.stat().st_size == 0
        with self.output.open("a", newline="", encoding="utf-8") as file:
            writer = csv.writer(file)
            if fresh:
                writer.writerow(["time", "open", "high", "low", "close"])
            writer.writerow([bar.time, bar.open, bar.high, bar.low, bar.close])

async def stream(collector: ClosedBarCollector, proxy_url: str):
    from urllib.parse import urlsplit
    from websockets.asyncio.client import connect
    parsed = urlsplit(proxy_url)
    if parsed.scheme != "ws" or parsed.hostname not in ("127.0.0.1", "localhost") or parsed.username or parsed.password:
        raise ValueError("Somente proxy WS2 local ws://127.0.0.1 é permitido")
    if parsed.path not in ("", "/") or parsed.query or parsed.fragment:
        raise ValueError("Não inclua credenciais/paths na URL local")
    reconnect = 1
    while True:
        try:
            async with connect(proxy_url, open_timeout=10, ping_interval=20) as ws:
                reconnect = 1
                await ws.send(json.dumps({"method": "subscribe", "params": {
                    "symbols": [collector.symbol], "intervals": [collector.interval]
                }}))
                LOG.info("Conectado ao proxy WS2 local; recebendo %s M%s", collector.symbol, collector.interval)
                async for msg in ws:
                    try:
                        data = json.loads(msg)
                        if isinstance(data, dict):
                            collector.ingest(data)
                    except (json.JSONDecodeError, TypeError):
                        LOG.warning("Frame inválido descartado")
        except (OSError, TimeoutError, Exception) as exc:
            # Reconexão não autoriza reabrir ordens; nunca há ordens aqui.
            LOG.warning("WS2 indisponível (%s). Reconectando.", type(exc).__name__)
            await asyncio.sleep(reconnect)
            reconnect = min(reconnect * 2, 30)

def main():
    parser = argparse.ArgumentParser(description="Puma WS2 read-only, sinais sem ordens")
    parser.add_argument("--symbol", default="BETHUSDT")
    parser.add_argument("--interval", default="1", choices=list(INTERVALS))
    parser.add_argument("--proxy", default="ws://127.0.0.1:3002")
    parser.add_argument("--output", default="candles_live.csv")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO)
    asyncio.run(stream(ClosedBarCollector(args.symbol, args.interval, args.output), args.proxy))

if __name__ == "__main__":
    main()
