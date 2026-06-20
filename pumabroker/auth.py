"""
auth.py — Autenticação automática para a Puma Broker.

Descoberta via DevTools (16/06/2026 00:07):

  ENDPOINT CONFIRMADO:
    POST https://trade.pumabroker.com/login

  PAYLOAD ENVIADO (real):
    {"email": "diegoporchello@gmail.com", "password": "Semsenh@123"}

  RESPONSE CONFIRMADA (real):
    {
      "user": {
        "realTrades": 325,
        "id": "28318",
        "email": "diegoporchello@gmail.com",
        "name": "DIEGO MARTINS",
        "firstName": "DIEGO",
        "lastName": "MARTINS",
        "balance": 0,
        "demoBalance": 9998,
        "bonus": 18.97,
        "isDemo": true,
        "isVip": true,
        "vipLevel": 1,
        "verified": true,
        "country": "BR",
        "cpf": "39358547812",
        "rollover": 5182.6,
        "rolloverTotal": 275000,
        "depositos": 600,
        "winrate": 0,
        ...
      },
      "token": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9..."
    }

  O campo "token" É o JWT Bearer usado em todas as requisições seguintes.
  NÃO é necessário o cookie server_name_session — o JWT basta para REST.
  O WebSocket ainda usa o cookie (set automaticamente pela sessão).

SEGURANÇA:
  - Nunca comite email/senha em código — use variáveis de ambiente
  - O JWT expira — implemente refresh automático (chamar login() novamente)
  - Use .env com python-dotenv
"""

import logging
import os
import time
from typing import Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from .config import config

logger = logging.getLogger(__name__)


class AuthError(Exception):
    def __init__(self, message: str, status_code: int = 0):
        super().__init__(message)
        self.status_code = status_code


class UserSession:
    """Dados da sessão do usuário após login."""

    def __init__(self, data: dict):
        user               = data.get("user", data)
        self.token:        str   = data.get("token", "")
        self.user_id:      str   = str(user.get("id", ""))
        self.email:        str   = user.get("email", "")
        self.name:         str   = user.get("name", "")
        self.balance:      float = float(user.get("balance", 0))
        self.demo_balance: float = float(user.get("demoBalance", 0))
        self.is_demo:      bool  = user.get("isDemo", True)
        self.is_vip:       bool  = user.get("isVip", False)
        self.country:      str   = user.get("country", "")
        self.real_trades:  int   = user.get("realTrades", 0)
        self.verified:     bool  = user.get("verified", False)
        self.raw:          dict  = data

    def __repr__(self):
        return (
            f"UserSession(id={self.user_id}, email={self.email}, "
            f"balance={self.balance}, demo={self.demo_balance}, "
            f"is_demo={self.is_demo})"
        )


class PumaBrokerAuth:
    """
    Gerencia autenticação automática na Puma Broker.

    Fluxo:
      1. POST /login → obtém JWT token + dados do usuário
      2. JWT é usado no header Authorization: Bearer <token>
      3. Refresh automático quando token expira (erro 401)

    Uso:
        auth = PumaBrokerAuth("email@gmail.com", "senha")
        session = auth.login()
        print(session.token)    # JWT pronto para usar
        print(session.user_id)  # "28318"

        # Refresh automático
        token = auth.ensure_token()
    """

    # JWT dura aproximadamente 24h (estimado — sem documentação oficial)
    TOKEN_TTL = 23 * 3600  # 23 horas para renovar com margem

    def __init__(self, email: str, password: str):
        self._email    = email
        self._password = password
        self._session: Optional[UserSession] = None
        self._login_ts: float = 0.0
        self._http     = self._build_http()

    def _build_http(self) -> requests.Session:
        s = requests.Session()
        s.headers.update({
            **config.DEFAULT_HEADERS,
            "Content-Type": "application/json",
            "Accept":       "application/json, text/plain, */*",
        })
        retry = Retry(total=3, backoff_factor=1, status_forcelist=[500, 502, 503, 504])
        s.mount("https://", HTTPAdapter(max_retries=retry))
        return s

    def login(self) -> UserSession:
        """
        Realiza login e retorna a sessão com JWT token.

        Endpoint: POST https://trade.pumabroker.com/login
        Payload:  {"email": "...", "password": "..."}
        Response: {"user": {...}, "token": "eyJhbGci..."}

        Raises:
            AuthError: credenciais inválidas ou servidor fora
        """
        logger.info("Fazendo login: %s", self._email)

        payload = {"email": self._email, "password": self._password}

        try:
            resp = self._http.post(
                config.LOGIN_URL,
                json=payload,
                timeout=config.HTTP_TIMEOUT,
            )
        except requests.exceptions.ConnectionError as e:
            raise AuthError(f"Sem conexão com o servidor: {e}") from e
        except requests.exceptions.Timeout:
            raise AuthError("Timeout no login")

        if resp.status_code == 401:
            raise AuthError("Email ou senha inválidos (401)", status_code=401)
        if resp.status_code == 403:
            raise AuthError("Conta bloqueada (403)", status_code=403)
        if not resp.ok:
            raise AuthError(
                f"Erro no login: HTTP {resp.status_code} — {resp.text[:200]}",
                status_code=resp.status_code,
            )

        data = resp.json()

        if "token" not in data:
            raise AuthError(f"Resposta inesperada do login: {data}")

        self._session  = UserSession(data)
        self._login_ts = time.time()

        # Propaga JWT para requisições futuras
        self._http.headers["Authorization"] = f"Bearer {self._session.token}"

        logger.info(
            "Login OK: %s (id=%s) balance=%.2f demo=%.2f",
            self._session.name,
            self._session.user_id,
            self._session.balance,
            self._session.demo_balance,
        )
        return self._session

    def ensure_token(self) -> str:
        """
        Garante que o token está válido, fazendo refresh se necessário.
        Usar antes de cada chamada REST crítica.

        Returns:
            JWT token válido
        """
        if self._session is None:
            self.login()
        elif time.time() - self._login_ts >= self.TOKEN_TTL:
            logger.info("JWT expirado — renovando automaticamente...")
            self.login()
        return self._session.token

    def refresh(self) -> UserSession:
        """Força renovação do token (chamar após erro 401)."""
        logger.info("Refresh forçado do JWT...")
        return self.login()

    @property
    def session(self) -> Optional[UserSession]:
        return self._session

    @property
    def token(self) -> str:
        if not self._session:
            raise AuthError("Não autenticado. Chame login() primeiro.")
        return self._session.token

    @property
    def user_id(self) -> str:
        if not self._session:
            raise AuthError("Não autenticado.")
        return self._session.user_id

    @property
    def http(self) -> requests.Session:
        """Sessão HTTP com Authorization header já configurado."""
        return self._http
