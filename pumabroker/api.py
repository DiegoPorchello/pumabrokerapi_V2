"""
api.py — Cliente REST para ordens na Puma Broker (API v1).

Endpoint confirmado (26/06/2026):
  POST https://trade.pumabroker.com/api/v1/trades

  Headers:
    Authorization: Bearer <jwt_token>
    Content-Type: application/json

RESULTADO da ordem chega via WebSocket (WS3):
  42/trades,["tradeUpdate", {"id":"...","status":"ACTIVE",...}]
"""

import logging
import time
from typing import Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from .config import config
from .models import OrderRequest, TradeUpdate

logger = logging.getLogger(__name__)


class OrderError(Exception):
    def __init__(self, message: str, status_code: int = 0):
        super().__init__(message)
        self.status_code = status_code


# Mapa de duração por timeframe (segundos até fim da vela)
# Baseado no payload real: M15 → duration=530 (segundos restantes da vela)
# Na prática, a plataforma calcula os segundos até o fechamento da vela atual
TIMEFRAME_SECONDS = {
    "M1":  60,
    "M5":  300,
    "M15": 900,
    "M30": 1800,
    "H1":  3600,
    "H4":  14400,
    "D1":  86400,
}


class TradesAPI:
    """
    Cliente HTTP para abertura de ordens via REST.

    Uso:
        api = TradesAPI(
            jwt_token="eyJhbGci...",
            user_id="28318",
            verify_token="gAAAAABq...",
        )
        result = api.place_order(
            symbol="AUDUSD",
            direction="CALL",
            amount=2.0,
            timeframe="M15",
            entry_price=0.70563,
            payout=0.85,
            wallet="REAL",
        )
    """

    def __init__(
        self,
        jwt_token:    str,
        user_id:      str,
        verify_token: str = "",
        wallet:       str = "REAL",
    ):
        self._jwt          = jwt_token
        self._user_id      = user_id
        self._verify_token = verify_token
        self._wallet       = wallet
        self._session      = self._build_session()

    def _build_session(self) -> requests.Session:
        session = requests.Session()
        session.headers.update({
            **config.DEFAULT_HEADERS,
            "Content-Type":  "application/json",
            "Accept":        "application/json, text/plain, */*",
            "Authorization": f"Bearer {self._jwt}",
        })
        retry = Retry(total=2, backoff_factor=1, status_forcelist=[500, 502, 503])
        session.mount("https://", HTTPAdapter(max_retries=retry))
        return session

    def update_jwt(self, new_jwt: str) -> None:
        """Atualiza o token JWT (chamar após erro 401)."""
        self._jwt = new_jwt
        self._session.headers["Authorization"] = f"Bearer {new_jwt}"
        logger.info("JWT atualizado.")

    def update_verify(self, new_verify: str) -> None:
        """Atualiza o token verify (renovado a cada sessão)."""
        self._verify_token = new_verify
        logger.info("Verify token atualizado.")

    def _calc_duration(self, timeframe: str) -> int:
        """
        Calcula os segundos restantes até o fechamento da vela atual.
        Reproduz o comportamento do frontend (duration=530 para M15 com ~8min restantes).
        """
        total_seconds = TIMEFRAME_SECONDS.get(timeframe, 60)
        now           = int(time.time())
        elapsed       = now % total_seconds
        remaining     = total_seconds - elapsed
        # Mínimo de 5s para evitar ordens na última fração de vela
        return max(remaining, 5)

    def place_order(
        self,
        symbol:      str,
        direction:   str,
        amount:      float,
        timeframe:   str   = "M1",
        entry_price: float = 0.0,
        payout:      float = 0.85,
        wallet:      Optional[str] = None,
        duration:    Optional[int] = None,
        trace_id:    Optional[str] = None,
    ) -> dict:
        """
        Envia uma ordem via POST REST.

        Endpoint: POST https://trade.pumabroker.com/trades
        Auth: Bearer JWT

        Args:
            symbol:      "AUDUSD", "BETHUSDT", "EURUSD", etc.
            direction:   "CALL" | "PUT"  (maiúsculas — confirmado)
            amount:      Valor da operação (ex: 2.0)
            timeframe:   "M1"|"M5"|"M15"|"M30"|"H1"
            entry_price: Preço de entrada (atual do ativo)
            payout:      Percentual de retorno (0.85 = 85%)
            wallet:      "REAL" | "DEMO" (padrão = configurado no construtor)
            duration:    Segundos até expiração (calculado automaticamente se None)
            trace_id:    ID de rastreamento para correlação

        Returns:
            dict com a resposta do servidor (estrutura do tradeUpdate)

        Raises:
            OrderError: se o servidor retornar erro
        """
        duration_received = duration
        dur = duration or self._calc_duration(timeframe)
        w   = wallet or self._wallet

        logger.info(
            "[TIMING] PUMA_REQUEST_START traceId=%s asset=%s dir=%s duration=%s durationReceived=%s durationEffective=%s ts=%.3f",
            trace_id or "none", symbol, direction.upper(), dur,
            duration_received, dur, time.time()
        )

        order = OrderRequest(
            userId=self._user_id,
            symbol=symbol,
            direction=direction.upper(),
            amount=amount,
            duration=dur,
            entryPrice=entry_price,
            mode="CANDLE_TIME",
            payout=payout,
            timeframe=timeframe,
            verify=self._verify_token,
            wallet=w,
        )

        payload = order.model_dump()

        logger.info(
            "→ POST /trades: %s %s $%.2f %s dur=%ds wallet=%s traceId=%s",
            symbol, direction.upper(), amount, timeframe, dur, w, trace_id or "none",
        )

        t0 = time.perf_counter()
        logger.info("[AUDIT] [PUMA_API_REQUEST] asset=%s direction=%s perf=%s", symbol, direction.upper(), t0)
        try:
            resp = self._session.post(
                config.TRADES_URL,
                json=payload,
                timeout=config.HTTP_TIMEOUT,
            )
            t1 = time.perf_counter()
            api_ms = round((t1 - t0) * 1000, 2)
            logger.info("[AUDIT] [PUMA_API_RESPONSE] asset=%s perf=%s duration_ms=%s", symbol, t1, api_ms)
            logger.info(
                "[TIMING] PUMA_REQUEST_END traceId=%s asset=%s status=%d durationMs=%s durationUsed=%d ts=%.3f",
                trace_id or "none", symbol, resp.status_code, api_ms, dur, time.time()
            )
        except requests.exceptions.RequestException as exc:
            raise OrderError(f"Falha de rede ao enviar ordem: {exc}") from exc

        if resp.status_code == 401:
            raise OrderError(
                "JWT expirado (401). Atualize o token com update_jwt().",
                status_code=401,
            )

        if not resp.ok:
            error_body = resp.text[:500]
            status_msg = {
                400: "Requisição inválida - verifique os parâmetros enviados",
                403: "Acesso negado (verifique permissões)",
                404: "Endpoint não encontrado",
                409: "Conflito - ordem pode já ter sido processada",
                422: "Dados inválidos - verifique formato",
                429: "Muitas solicitações - tente novamente mais tarde",
                500: "Erro interno do servidor",
                502: "Erro de gateway - tente novamente",
                503: "Serviço indisponível momentaneamente"
            }.get(resp.status_code, f"Erro HTTP {resp.status_code}")
            
            raise OrderError(
                f"{status_msg}. Detalhes: {error_body}",
                status_code=resp.status_code,
            )

        result = resp.json()
        logger.info("← Ordem criada: %s", result)
        return result

    def buy_call(
        self,
        symbol:      str,
        amount:      float = 2.0,
        timeframe:   str   = "M1",
        entry_price: float = 0.0,
        payout:      float = 0.85,
        wallet:      Optional[str] = None,
    ) -> dict:
        """Atalho para CALL (alta)."""
        return self.place_order(
            symbol=symbol,
            direction="CALL",
            amount=amount,
            timeframe=timeframe,
            entry_price=entry_price,
            payout=payout,
            wallet=wallet,
        )

    def buy_put(
        self,
        symbol:      str,
        amount:      float = 2.0,
        timeframe:   str   = "M1",
        entry_price: float = 0.0,
        payout:      float = 0.85,
        wallet:      Optional[str] = None,
    ) -> dict:
        """Atalho para PUT (queda)."""
        return self.place_order(
            symbol=symbol,
            direction="PUT",
            amount=amount,
            timeframe=timeframe,
            entry_price=entry_price,
            payout=payout,
            wallet=wallet,
        )
