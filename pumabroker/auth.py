"""
auth.py — Autenticação automática para a Puma Broker (API v1).

  ENDPOINT CONFIRMADO (26/06/2026):
    POST https://trade.pumabroker.com/api/v1/auth/login

  Payload:  {"email": "...", "password": "..."}
  Response: {accessToken, refreshToken, user: {id, email, name, balance, ...}}

  O accessToken é o JWT Bearer usado nas requisições seguintes.
  O refreshToken é usado para renovar o accessToken quando expirar.
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
    """Dados da sessão do usuário após login (API v1)."""

    def __init__(self, data: dict):
        user               = data.get("user", data)
        self.token:        str   = data.get("accessToken", data.get("token", ""))
        self.refresh_token: str  = data.get("refreshToken", "")
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

    # JWT dura aproximadamente 24h (estimado)
    TOKEN_TTL = 23 * 3600

    def __init__(self, email: str, password: str):
        self._email    = email
        self._password = password
        self._session: Optional[UserSession] = None
        self._login_ts: float = 0.0
        self._http     = self._build_http()
        self._session_cookie_value: Optional[str] = None

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

    def _extract_session_cookie(self, resp: requests.Response) -> None:
        """Extrai server_name_session dos headers Set-Cookie da resposta e armazena."""
        cookies_raw = resp.headers.get("Set-Cookie", "")
        if not cookies_raw:
            return
        # Pode haver múltiplos Set-Cookie headers; tratamos como string única
        for part in cookies_raw.split(","):
            part = part.strip()
            if "server_name_session=" in part:
                start = part.index("server_name_session=") + len("server_name_session=")
                end = part.index(";", start) if ";" in part[start:] else len(part)
                raw_val = part[start:end].strip()
                if raw_val:
                    self._session_cookie_value = raw_val
                    logger.info("server_name_session capturado do Set-Cookie header")
                    break

    def login(self) -> UserSession:
        """
        Realiza login e retorna a sessão com JWT token (API v1).

        Endpoint: POST https://trade.pumabroker.com/api/v1/auth/login
        Payload:  {"email": "...", "password": "..."}
        Response: {"accessToken": "eyJ...", "refreshToken": "...", "user": {...}}

        Raises:
            AuthError: credenciais inválidas ou servidor fora
        """
        logger.info("Fazendo login: %s", self._email)

        payload = {"email": self._email, "password": self._password}

        try:
            resp = self._http.post(
                config.AUTH_LOGIN_URL,
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

        if "accessToken" not in data and "token" not in data:
            raise AuthError(f"Resposta inesperada do login: {data}")

        self._session  = UserSession(data)
        self._login_ts = time.time()

        # Propaga JWT para requisições futuras
        self._http.headers["Authorization"] = f"Bearer {self._session.token}"

        # Tenta extrair server_name_session dos headers da resposta de login
        self._extract_session_cookie(resp)

        logger.info(
            "Login OK: %s (id=%s) balance=%.2f demo=%.2f cookie=%s",
            self._session.name,
            self._session.user_id,
            self._session.balance,
            self._session.demo_balance,
            "SIM" if self._session_cookie_value else "NÃO",
        )
        return self._session

    def ensure_token(self) -> str:
        """
        Garante que o token está válido, fazendo refresh se necessário.

        Usa refreshToken se disponível (API v1) ou faz login novamente.

        Returns:
            JWT token válido
        """
        if self._session is None:
            self.login()
        elif time.time() - self._login_ts >= self.TOKEN_TTL:
            if self._session.refresh_token:
                self._refresh_token()
            else:
                logger.info("JWT expirado — renovando...")
                self.login()
        return self._session.token

    def _refresh_token(self) -> None:
        """Renova o accessToken usando o refreshToken."""
        logger.info("Renovando JWT via refreshToken...")
        try:
            resp = self._http.post(
                config.AUTH_REFRESH_URL,
                json={"refreshToken": self._session.refresh_token},
                timeout=config.HTTP_TIMEOUT,
            )
            if resp.status_code == 200:
                data = resp.json()
                new_token = data.get("accessToken", "")
                new_refresh = data.get("refreshToken", "")
                if new_token:
                    self._session.token = new_token
                    self._http.headers["Authorization"] = f"Bearer {new_token}"
                if new_refresh:
                    self._session.refresh_token = new_refresh
                self._login_ts = time.time()
                logger.info("JWT renovado com sucesso")
            else:
                logger.warning("Refresh token inválido — refazendo login")
                self.login()
        except Exception as e:
            logger.warning("Erro no refresh token: %s — refazendo login", e)
            self.login()

    def refresh(self) -> UserSession:
        """Força renovação do token (chamar após erro 401)."""
        logger.info("Refresh forçado do JWT...")
        if self._session and self._session.refresh_token:
            self._refresh_token()
            return self._session
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

    @property
    def session_cookie(self) -> Optional[str]:
        """
        Retorna o valor do cookie server_name_session capturado durante login.
        Tenta na ordem:
          1. Valor extraído do Set-Cookie header da resposta de login
          2. Cookies da sessão requests (domínio específico e genérico)
          3. Variável de ambiente PUMA_SESSION
        """
        # 1. Valor extraído diretamente dos headers de resposta
        if self._session_cookie_value:
            return self._session_cookie_value

        # 2. Tenta extrair dos cookies da sessão requests
        if self._http and self._http.cookies:
            for cookie_name in ("server_name_session", "server_name_token"):
                val = self._http.cookies.get(cookie_name, domain="pumabroker.com")
                if val:
                    self._session_cookie_value = val
                    return val
            # Tenta sem domínio específico
            for cookie_name in ("server_name_session", "server_name_token"):
                val = self._http.cookies.get(cookie_name)
                if val:
                    self._session_cookie_value = val
                    return val

        # 3. Fallback: variável de ambiente
        env_val = os.getenv("PUMA_SESSION")
        if env_val:
            self._session_cookie_value = env_val
        return env_val
