"""
example.py — Exemplo funcional da biblioteca pumabroker.

Como obter os tokens (DevTools):
  session_token -> Application -> Cookies -> server_name_session
  account_id    -> Network -> Socket -> 42/trades,["subscribe","ID"]
  jwt_token     -> Network -> Fetch/XHR -> qualquer request -> Headers -> Authorization
  verify_token  -> Network -> Fetch/XHR -> trades -> Payload -> campo "verify"
"""

import asyncio
import logging
from pumabroker import PumaBroker, TradeUpdate, BarUpdateEvent

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(message)s")

SESSION_TOKEN = "cd0dc3ba351b950fc7621ef63b19d855"
ACCOUNT_ID    = "28318"
JWT_TOKEN     = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9..."
VERIFY_TOKEN  = "gAAAAABqMLMX8x8KnN21gQRKFTfSa2FM..."

current_price = {}


async def main():
    pb = PumaBroker(
        session_token=SESSION_TOKEN,
        account_id=ACCOUNT_ID,
        jwt_token=JWT_TOKEN,
        verify_token=VERIFY_TOKEN,
        wallet="REAL",
    )

    def on_bar(bar: BarUpdateEvent):
        current_price[bar.symbol] = bar.bar.close
        print(f"[BAR] {bar.symbol} M{bar.interval} C:{bar.bar.close:.5f}")

    def on_trade(event: str, trade: TradeUpdate):
        icons = {"ACTIVE": "PENDENTE", "WIN": "GANHOU", "LOSS": "PERDEU", "DRAW": "EMPATE"}
        print(f"[{icons.get(trade.status, trade.status)}] {trade.symbol} "
              f"{trade.direction} ${trade.amount} payout={trade.payout_percent():.0f}%")

    async with pb:
        pb.on_bar("AUDUSD", "1", on_bar)
        pb.on_event("tradeUpdate", on_trade)

        await asyncio.sleep(3)

        price = current_price.get("AUDUSD", 0.70563)
        try:
            result = pb.buy_call(
                symbol="AUDUSD",
                amount=2.0,
                timeframe="M1",
                entry_price=price,
                payout=0.85,
            )
            print(f"[ORDEM] {result}")
        except Exception as e:
            print(f"[ERRO] {e}")

        await pb.listen()


if __name__ == "__main__":
    asyncio.run(main())
