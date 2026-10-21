"""Coletor do feed híbrido Puma: autenticação e sinais SOMENTE LEITURA.
Requer PUMA_MARKET_WS_KEY configurada localmente. Nunca envia ordens.
"""
import argparse
import asyncio
import csv
import json
import logging
import os
from pathlib import Path
from websockets.asyncio.client import connect
from paper_probability_bot import Candle, signal

LOG = logging.getLogger("puma.hybrid")
URL = "wss://market.pumabroker.com/ws"

class HybridCollector:
    def __init__(self, symbol="BETHUSDT", interval="1", output="candles_beth.csv"):
        self.symbol = symbol.upper()
        self.interval = str(interval)
        self.output = Path(output)
        self.last_time = -1
        self.history = []
        self.current = None

    def ingest(self, event):
        if not isinstance(event, dict):
            return None
        if event.get("symbol", "").upper() != self.symbol or str(event.get("interval")) != self.interval:
            return None
        if event.get("type") not in ("bar_closed", "bar_update"):
            return None
        raw = event.get("bar") or {}
        try:
            bar = Candle(int(raw["time"]), float(raw["open"]), float(raw["high"]),
                         float(raw["low"]), float(raw["close"]))
            bar.validate()
        except (KeyError, TypeError, ValueError):
            LOG.warning("Candle inválido")
            return None
        if event["type"] == "bar_update":
            if self.current is not None and bar.time > self.current.time:
                self._close(self.current)
            if bar.time > self.last_time:
                self.current = bar
            return None
        return self._close(bar)

    def _close(self, bar):
        if bar.time <= self.last_time:
            return None
        if self.last_time >= 0 and bar.time - self.last_time != int(self.interval) * 60:
            LOG.warning("Lacuna na sequência, reiniciando histórico da estratégia")
            self.history.clear()
        self.last_time = bar.time
        self.history.append(bar)
        if len(self.history) > 2000:
            self.history.pop(0)
        self.output.parent.mkdir(parents=True, exist_ok=True)
        new = not self.output.exists() or self.output.stat().st_size == 0
        with self.output.open("a", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            if new:
                writer.writerow(["time","open","high","low","close"])
            writer.writerow([bar.time,bar.open,bar.high,bar.low,bar.close])
        LOG.info("Candle fechado: %s close=%.5f", bar.time, bar.close)
        decision = signal(self.history)
        if decision:
            LOG.info("SINAL %s (SEM ENVIO DE ORDEM)", decision)
        return decision

async def run(collector):
    key = os.environ.get("PUMA_MARKET_WS_KEY", "").strip()
    if not key:
        raise SystemExit("Configure PUMA_MARKET_WS_KEY no terminal local antes de conectar.")
    delay = 1
    while True:
        try:
            async with connect(URL, open_timeout=15, ping_interval=20, origin="https://trade.pumabroker.com") as ws:
                await ws.send(json.dumps({"method":"AUTH","params":{"key":key}}))
                LOG.info("WS conectado; AUTH enviado (chave oculta)")
                delay = 1
                async for frame in ws:
                    try:
                        event = json.loads(frame)
                    except (ValueError, TypeError):
                        continue
                    if not isinstance(event, dict):
                        continue
                    kind = event.get("type")
                    if kind == "auth":
                        if event.get("status") != "ok":
                            LOG.error("AUTH recusado; encerrando sem assinatura")
                            return
                        await ws.send(json.dumps({"method":"SUBSCRIBE_TICKS"}))
                        await ws.send(json.dumps({"method":"SUBSCRIBE","params":{"symbol":collector.symbol,"interval":collector.interval}}))
                        LOG.info("AUTH OK, aguardando confirmação de assinatura")
                    elif kind == "subscribed":
                        LOG.info("ASSINATURA OK %s M%s", event.get("symbol"), event.get("interval"))
                    elif kind in ("bar_update","bar_closed"):
                        collector.ingest(event)
                    elif kind == "error":
                        LOG.warning("Servidor retornou erro de protocolo (conteúdo omitido)")
        except Exception as error:
            LOG.warning("Conexão interrompida: %s; reconectando", type(error).__name__)
            await asyncio.sleep(delay)
            delay = min(delay * 2, 30)

def main():
    parser=argparse.ArgumentParser()
    parser.add_argument("--symbol",default="BETHUSDT")
    parser.add_argument("--interval",default="1",choices=["1","2","3","5","15"])
    parser.add_argument("--output",default="candles_beth.csv")
    args=parser.parse_args()
    logging.basicConfig(level=logging.INFO)
    asyncio.run(run(HybridCollector(args.symbol,args.interval,args.output)))

if __name__=="__main__":
    main()
