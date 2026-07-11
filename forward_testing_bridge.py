"""
forward_testing_bridge.py
Bridge entre o motor TypeScript (Forward Testing) e a Puma Broker (Python).

Funciona como um serviço HTTP local que:
  1. Recebe comandos do engine TS (start, stop, status)
  2. Conecta à Puma Broker (conta DEMO)
  3. Escuta candles em tempo real (WebSocket wsm5)
  4. Executa ordens via REST (POST /trades)
  5. Escuta resultados (Socket.IO tradeUpdate)
  6. Registra operações no Supabase via REST API
  7. Retorna resultados para o engine TS

Uso:
  python forward_testing_bridge.py --email user@email.com --password 123

Endpoints HTTP:
  POST /start    → Inicia bot com config
  POST /stop     → Para o bot
  GET  /status   → Status atual do bot
  GET  /candles  → Últimos candles recebidos
  GET  /trades   → Últimas operações
"""

import asyncio
import json
import logging
import os
import sys
from datetime import datetime
from typing import Optional

# Adiciona o diretório ao path para importar pumabroker
sys.path.insert(0, os.path.dirname(__file__))

from dotenv import load_dotenv
from pumabroker import PumaBroker, BarUpdateEvent, TradeUpdate

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("forward_testing")


class ForwardTestingBridge:
    """
    Ponte entre o Supabase (TS) e a Puma Broker (Python).

    Gerencia:
    - Conexão WebSocket dupla (candles + trades)
    - Execução de ordens com base em sinais do Score Engine
    - Registro de resultados no Supabase
    """

    def __init__(
        self,
        email: str,
        password: str,
        wallet: str = "DEMO",
        supabase_url: Optional[str] = None,
        supabase_key: Optional[str] = None,
    ):
        self._email = email
        self._password = password
        self._wallet = wallet

        # Supabase config (opcional — registra resultados no banco)
        self._supabase_url = supabase_url or os.getenv("VITE_SUPABASE_URL", "")
        self._supabase_key = supabase_key or os.getenv("VITE_SUPABASE_ANON_KEY", "")

        # Estado
        self._broker: Optional[PumaBroker] = None
        self._running = False
        self._current_session_id: Optional[str] = None
        self._latest_candles: dict = {}
        self._open_trades: dict = {}

    # ── Ciclo de vida ─────────────────────────────────────────────────────────

    async def start(self, session_id: str, config: dict) -> dict:
        """
        Inicia a conexão com a Puma Broker e começa a escutar.

        Args:
            session_id: ID da sessão no Supabase
            config: {
                symbol: "AUDUSD",
                timeframe: "M1",
                amount: 2.0,
                wallet: "DEMO",
            }
        """
        self._current_session_id = session_id
        symbol = config.get("symbol", "AUDUSD")
        timeframe = config.get("timeframe", "M1")
        amount = config.get("amount", 2.0)

        logger.info(f"Iniciando Forward Testing: sessão={session_id} {symbol} {timeframe}")

        self._broker = PumaBroker(
            email=self._email,
            password=self._password,
            wallet=self._wallet,
        )

        await self._broker.connect()
        self._running = True

        # Registra handlers
        interval = timeframe.replace("M", "").replace("H", "")
        self._broker.on_bar(symbol, interval, self._on_bar)
        self._broker.on_event("tradeUpdate", self._on_trade_result)

        logger.info(f"Forward Testing rodando: {symbol} {timeframe} R${amount} {self._wallet}")
        return {"status": "running", "session_id": session_id}

    async def stop(self) -> dict:
        """Para o bot e desconecta."""
        self._running = False
        if self._broker:
            await self._broker.disconnect()
        logger.info("Forward Testing parado.")
        return {"status": "stopped"}

    def get_status(self) -> dict:
        """Retorna status atual."""
        return {
            "running": self._running,
            "session_id": self._current_session_id,
            "open_trades": len(self._open_trades),
            "latest_candles": {
                k: v.get("close") for k, v in self._latest_candles.items()
            },
        }

    # ── Handlers ──────────────────────────────────────────────────────────────

    def _on_bar(self, bar: BarUpdateEvent) -> None:
        """Callback de candle em tempo real."""
        key = f"{bar.symbol}:{bar.interval}"
        self._latest_candles[key] = {
            "close": bar.bar.close,
            "open": bar.bar.open,
            "high": bar.bar.high,
            "low": bar.bar.low,
            "volume": bar.bar.volume,
            "time": bar.bar.time,
        }

    def _on_trade_result(self, event: str, trade: TradeUpdate) -> None:
        """
        Callback de resultado de operação.
        Registra no Supabase e no estado local.
        """
        logger.info(
            f"Trade #{trade.id}: {trade.symbol} {trade.direction} "
            f"R${trade.amount} status={trade.status}"
        )

        # Se configurado com Supabase, registra resultado
        if self._supabase_url and self._supabase_key:
            self._register_trade(trade)

        # Gerencia estado local
        if trade.status == "ACTIVE":
            self._open_trades[trade.id] = trade
        elif trade.status in ("WIN", "LOSS", "DRAW"):
            self._open_trades.pop(trade.id, None)

    def _register_trade(self, trade: TradeUpdate) -> None:
        """
        Registra o resultado da operação no Supabase.
        Chamado pelo callback tradeUpdate.
        """
        import requests

        if trade.status == "ACTIVE":
            return  # Ainda não finalizou

        profit = 0
        result = "pending"

        if trade.status == "WIN":
            profit = trade.amount * (trade.payout or 0.85)
            result = "win"
        elif trade.status == "LOSS":
            profit = -trade.amount
            result = "loss"
        elif trade.status == "DRAW":
            profit = 0
            result = "draw"

        # Busca a operation pelo broker_order_id
        headers = {
            "apikey": self._supabase_key,
            "Authorization": f"Bearer {self._supabase_key}",
            "Content-Type": "application/json",
        }

        # Primeiro encontra a operation
        find_url = (
            f"{self._supabase_url}/rest/v1/operations"
            f"?broker_order_id=eq.{trade.id}&select=id"
        )
        resp = requests.get(find_url, headers=headers)

        if resp.status_code == 200 and len(resp.json()) > 0:
            op_id = resp.json()[0]["id"]
            # Atualiza o resultado
            update_url = f"{self._supabase_url}/rest/v1/operations?id=eq.{op_id}"
            payload = {
                "result": result,
                "profit": round(profit, 2),
                "exit_price": float(trade.exitPrice or 0),
                "broker_status": trade.status,
                "closed_at": datetime.utcnow().isoformat(),
            }
            requests.patch(update_url, json=payload, headers=headers)

    # ── Execução de ordens ───────────────────────────────────────────────────

    async def get_available_assets(self) -> list:
        """Retorna lista de ativos disponíveis/abertos para negociação."""
        if not self._broker:
            return []
        try:
            assets = self._broker.get_active_assets()
            return assets if isinstance(assets, list) else list(assets.keys())
        except Exception as e:
            logger.warning(f"Erro ao buscar ativos disponíveis: {e}")
            return []

    async def execute_signal(
        self,
        symbol: str,
        direction: str,
        amount: float,
        timeframe: str = "M1",
        entry_price: float = 0.0,
        payout: float = 0.85,
        session_id: Optional[str] = None,
    ) -> dict:
        """
        Executa uma ordem baseada em sinal do Score Engine.
        Chamado pelo motor TS via HTTP.

        Retorna o resultado da ordem para registro no Supabase.
        """
        if not self._broker or not self._running:
            raise RuntimeError("Bridge não está rodando.")

        # Valida se o ativo está disponível antes de executar
        if not self._broker.is_asset_available(symbol):
            raise RuntimeError(
                f"Ativo {symbol} não está disponível/aberto para negociação. "
                f"Ativos disponíveis: {await self.get_available_assets()}"
            )

        if direction.upper() == "CALL":
            result = self._broker.buy_call(
                symbol=symbol,
                amount=amount,
                timeframe=timeframe,
                entry_price=entry_price,
                payout=payout,
            )
        else:
            result = self._broker.buy_put(
                symbol=symbol,
                amount=amount,
                timeframe=timeframe,
                entry_price=entry_price,
                payout=payout,
            )

        # Registra a operação no Supabase com broker_order_id
        if self._supabase_url and self._supabase_key and session_id:
            import requests

            headers = {
                "apikey": self._supabase_key,
                "Authorization": f"Bearer {self._supabase_key}",
                "Content-Type": "application/json",
                "Prefer": "return=representation",
            }

            payload = {
                "session_id": session_id,
                "asset": symbol,
                "direction": direction.upper(),
                "amount": amount,
                "entry_price": entry_price or 0,
                "payout": payout,
                "timeframe": timeframe,
                "broker_order_id": result.get("id", ""),
                "result": "pending",
                "level": 0,
                "entry_number": 1,
            }

            resp = requests.post(
                f"{self._supabase_url}/rest/v1/operations",
                json=payload,
                headers=headers,
            )

            if resp.status_code == 201:
                logger.info(f"Operação registrada no Supabase: {resp.json().get('id')}")

        return result


# ══════════════════════════════════════════════════════════════════════════════
# CLI (para testes manuais)
# ══════════════════════════════════════════════════════════════════════════════
if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Forward Testing Bridge — Puma Broker")
    parser.add_argument("--email", required=True, help="Email da conta Puma Broker")
    parser.add_argument("--password", required=True, help="Senha da conta Puma Broker")
    parser.add_argument("--wallet", default="DEMO", choices=["DEMO", "REAL"])
    parser.add_argument("--symbol", default="AUDUSD")
    parser.add_argument("--timeframe", default="M1")
    parser.add_argument("--amount", type=float, default=2.0)

    args = parser.parse_args()

    async def main():
        bridge = ForwardTestingBridge(
            email=args.email,
            password=args.password,
            wallet=args.wallet,
        )

        await bridge.start("test-manual", {
            "symbol": args.symbol,
            "timeframe": args.timeframe,
            "amount": args.amount,
        })

        await bridge._broker.listen()

    asyncio.run(main())
