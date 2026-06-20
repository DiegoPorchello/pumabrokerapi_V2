"""
client.py — Interface unificada para a Puma Broker.

Fluxo completo confirmado via DevTools (15-16/06/2026):

  1. POST /login → JWT token + user_id automático
  2. WSS wsm5.pumabroker.com → candles OHLCV em tempo real
  3. WSS socket.io/trades → resultado de ordens (tradeUpdate)
  4. POST /trades → abertura de ordem com JWT no header

Uso mínimo:
    from pumabroker import PumaBroker

    async def main():
        async with PumaBroker("email@gmail.com", "senha") as pb:
            pb.on_bar("AUDUSD", "1", lambda b: print(b.bar.close))
            pb.on_event("tradeUpdate", lambda e, t: print(t.status))

            result = pb.buy_call("AUDUSD", amount=2.0, timeframe="M1")
            await pb.listen()

    asyncio.run(main())
"""

import asyncio
import logging
from typing import Callable, Optional

from .auth import PumaBrokerAuth, UserSession, AuthError
from .config import config
from .models import BarUpdateEvent, TradeUpdate
from .ws_market import MarketWebSocket
from .ws_trades import TradesWebSocket
from .api import TradesAPI, OrderError

logger = logging.getLogger(__name__)

logging.basicConfig(
    level=getattr(logging, config.LOG_LEVEL, logging.INFO),
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)


class PumaBroker:
    """
    Interface principal para a Puma Broker.

    Modo 1 — Login automático (recomendado):
        pb = PumaBroker("email@gmail.com", "senha")

    Modo 2 — Tokens manuais (do navegador):
        pb = PumaBroker(
            session_token="cd0dc3ba...",
            account_id="28318",
            jwt_token="eyJhbGci...",
        )
    """

    def __init__(
        self,
        email:         Optional[str] = None,
        password:      Optional[str] = None,
        session_token: Optional[str] = None,
        account_id:    Optional[str] = None,
        jwt_token:     Optional[str] = None,
        verify_token:  str = "",
        wallet:        str = "REAL",
    ):
        self._wallet  = wallet
        self._auth:   Optional[PumaBrokerAuth] = None
        self._session: Optional[UserSession]   = None

        # Modo 1: login automático
        if email and password:
            self._auth = PumaBrokerAuth(email, password)

        # Modo 2: tokens manuais
        self._manual_token      = session_token
        self._manual_account_id = account_id
        self._manual_jwt        = jwt_token
        self._verify_token      = verify_token

        # Inicializado após connect()
        self._ws_market:   Optional[MarketWebSocket] = None
        self._ws_trades:   Optional[TradesWebSocket] = None
        self._trades_api:  Optional[TradesAPI]       = None

    # ── Conexão ───────────────────────────────────────────────────────────────

    async def connect(self) -> "PumaBroker":
        """
        Realiza login (se email/senha fornecidos) e conecta os WebSockets.
        """
        # Login automático
        if self._auth:
            logger.info("Autenticando...")
            self._session = self._auth.login()
            jwt       = self._session.token
            user_id   = self._session.user_id
            # Cookie de sessão para WebSocket (do Set-Cookie do login)
            session_token = self._auth.http.cookies.get(
                config.SESSION_COOKIE, ""
            )
        else:
            # Tokens manuais
            jwt           = self._manual_jwt or ""
            user_id       = self._manual_account_id or ""
            session_token = self._manual_token or ""

        # WebSocket de candles (wsm5)
        self._ws_market = MarketWebSocket(session_token or jwt)

        # WebSocket de trades (socket.io)
        self._ws_trades = TradesWebSocket(session_token or jwt, user_id)

        # REST API para ordens
        self._trades_api = TradesAPI(
            jwt_token=jwt,
            user_id=user_id,
            verify_token=self._verify_token,
            wallet=self._wallet,
        )

        # Conecta WebSockets
        logger.info("Conectando WebSockets...")
        await asyncio.gather(
            self._ws_market.connect(),
            self._ws_trades.connect(),
        )

        if self._session:
            logger.info(
                "Conectado como %s | Saldo real: R$%.2f | Demo: R$%.2f",
                self._session.name,
                self._session.balance,
                self._session.demo_balance,
            )
        return self

    async def disconnect(self) -> None:
        """Fecha todas as conexões."""
        tasks = []
        if self._ws_market:
            tasks.append(self._ws_market.disconnect())
        if self._ws_trades:
            tasks.append(self._ws_trades.disconnect())
        if tasks:
            await asyncio.gather(*tasks)
        logger.info("Desconectado.")

    # ── Sessão ────────────────────────────────────────────────────────────────

    @property
    def session(self) -> Optional[UserSession]:
        """Dados do usuário logado (disponível após connect())."""
        return self._session

    @property
    def balance(self) -> float:
        return self._session.balance if self._session else 0.0

    @property
    def demo_balance(self) -> float:
        return self._session.demo_balance if self._session else 0.0

    # ── REST ──────────────────────────────────────────────────────────────────

    def get_profile(self) -> dict:
        """GET /me — perfil do usuário."""
        if self._auth:
            r = self._auth.http.get(config.ME_URL, timeout=config.HTTP_TIMEOUT)
        else:
            import requests
            s = requests.Session()
            s.headers.update({**config.DEFAULT_HEADERS,
                               "Authorization": f"Bearer {self._manual_jwt}"})
            r = s.get(config.ME_URL, timeout=config.HTTP_TIMEOUT)
        r.raise_for_status()
        return r.json()

    def get_balance(self) -> dict:
        """GET /balance — saldo atualizado."""
        http = self._auth.http if self._auth else None
        if not http:
            raise RuntimeError("Sem sessão HTTP. Use email/password no construtor.")
        r = http.get(config.BALANCE_URL, timeout=config.HTTP_TIMEOUT)
        r.raise_for_status()
        return r.json()

    def get_active_assets(self) -> dict:
        """GET /active — ativos disponíveis para negociação."""
        http = self._auth.http if self._auth else None
        if not http:
            raise RuntimeError("Sem sessão HTTP.")
        r = http.get(config.ACTIVE_URL, timeout=config.HTTP_TIMEOUT)
        r.raise_for_status()
        return r.json()

    # ── Candles em tempo real ─────────────────────────────────────────────────

    def on_bar(self, symbol: str, interval: str, handler: Callable) -> None:
        """
        Callback para candles em tempo real (wsm5).

        Args:
            symbol:   "AUDUSD", "BETHUSDT", "EURUSD", etc.
            interval: "1", "5", "15", "30", "60"
            handler:  fn(BarUpdateEvent) → None
        """
        if self._ws_market:
            self._ws_market.on_bar(symbol, interval, handler)

    def on_any_bar(self, handler: Callable) -> None:
        """Callback para todos os candles."""
        if self._ws_market:
            self._ws_market.on_any_bar(handler)

    # ── Eventos de conta ─────────────────────────────────────────────────────

    def on_event(self, event_name: str, handler: Callable) -> None:
        """
        Callback para eventos Socket.IO.

        Eventos confirmados:
          "tradeUpdate" → resultado de ordem (ACTIVE → WIN/LOSS/DRAW)

        Args:
            event_name: nome do evento Socket.IO
            handler:    fn(event_name: str, data: TradeUpdate | dict) → None
        """
        if self._ws_trades:
            self._ws_trades.on(event_name, handler)

    def on_trade_result(self, handler: Callable) -> None:
        """Atalho para o evento tradeUpdate (resultado de ordens)."""
        self.on_event("tradeUpdate", handler)

    # ── Ordens ────────────────────────────────────────────────────────────────

    def buy_call(
        self,
        symbol:      str,
        amount:      float = 2.0,
        timeframe:   str   = "M1",
        entry_price: float = 0.0,
        payout:      float = 0.85,
    ) -> dict:
        """
        Abre posição CALL (alta) via POST REST.

        CONFIRMADO: POST https://trade.pumabroker.com/trades
        JWT token obtido automaticamente via login().

        Args:
            symbol:      "AUDUSD", "BETHUSDT", "EURUSD", etc.
            amount:      Valor da operação
            timeframe:   "M1"|"M5"|"M15"|"M30"|"H1"
            entry_price: Preço atual (use bar.bar.close do on_bar)
            payout:      Retorno esperado (0.85 = 85%)
        """
        self._ensure_jwt()
        return self._trades_api.buy_call(
            symbol=symbol, amount=amount, timeframe=timeframe,
            entry_price=entry_price, payout=payout,
        )

    def buy_put(
        self,
        symbol:      str,
        amount:      float = 2.0,
        timeframe:   str   = "M1",
        entry_price: float = 0.0,
        payout:      float = 0.85,
    ) -> dict:
        """
        Abre posição PUT (queda) via POST REST.
        """
        self._ensure_jwt()
        return self._trades_api.buy_put(
            symbol=symbol, amount=amount, timeframe=timeframe,
            entry_price=entry_price, payout=payout,
        )

    def _ensure_jwt(self):
        """Renova JWT automaticamente se expirou."""
        if not self._trades_api:
            raise RuntimeError("Não conectado. Chame await connect() primeiro.")
        if self._auth:
            # Verifica se precisa renovar
            fresh_token = self._auth.ensure_token()
            if fresh_token != self._trades_api._jwt:
                self._trades_api.update_jwt(fresh_token)

    def update_jwt(self, jwt_token: str, verify_token: str = "") -> None:
        """Atualiza JWT manualmente após erro 401."""
        if self._trades_api:
            self._trades_api.update_jwt(jwt_token)
            if verify_token:
                self._trades_api.update_verify(verify_token)

    # ── Loop ──────────────────────────────────────────────────────────────────

    async def listen(self) -> None:
        """Loop de escuta (blocking) — recebe candles e tradeUpdates."""
        tasks = []
        if self._ws_market:
            tasks.append(self._ws_market.listen())
        if self._ws_trades:
            tasks.append(self._ws_trades.listen())
        if tasks:
            await asyncio.gather(*tasks)

    # ── Context manager ───────────────────────────────────────────────────────

    async def __aenter__(self):
        await self.connect()
        return self

    async def __aexit__(self, *args):
        await self.disconnect()
