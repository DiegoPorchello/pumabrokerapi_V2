"""
example.py — Exemplo funcional da biblioteca pumabroker.

IMPORTANTE: NUNCA hardcoded tokens neste arquivo.
Use o PumaBrokerAuth para obter tokens via login:

    from pumabroker.auth import PumaBrokerAuth
    auth = PumaBrokerAuth("email@gmail.com", "senha")
    session = auth.login()
    # session.token → JWT (accessToken)
    # session.user_id → "28318"

Os tokens abaixo são PLACEHOLDERS — substitua pelos obtidos do login.
"""

import asyncio
import logging
from pumabroker import PumaBroker, TradeUpdate, BarUpdateEvent
from pumabroker.auth import PumaBrokerAuth

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(message)s")

# ⚠️ NUNCA hardcoded tokens — obter via login
# Exemplo de como obter tokens dinamicamente:
#   auth = PumaBrokerAuth("seu@email.com", "sua_senha")
#   session = auth.login()
#   SESSION_TOKEN = session.token  # ou extraia do DevTools
#   ACCOUNT_ID = session.user_id
#   JWT_TOKEN = session.token

SESSION_TOKEN = "OBTENHA_VIA_LOGIN_OU_DEVTOOLS"
ACCOUNT_ID    = "OBTENHA_VIA_LOGIN"
JWT_TOKEN     = "OBTENHA_VIA_LOGIN"
VERIFY_TOKEN  = "OBTENHA_VIA_DEVTOOLS"

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
