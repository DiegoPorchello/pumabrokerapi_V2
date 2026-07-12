"""
auth.py — Autenticação automática para a Puma Broker (API v1).

  ENDPOINT CONFIRMADO (26/06/2026):
    POST https://trade.pumabroker.com/api/v1/auth/login

  Payload:  {"email": "...", "password": "..."}
  Response: {accessToken, refreshToken, user: {id, email, name, balance, ...}}

  O accessToken é o JWT Bearer usado nas requisições seguintes.
  O refreshToken é usado para renovar o accessToken quando expirar.

  O cookie server_name_session (usado pelo WS2) NÃO vem na resposta de login.
  É obtido via uma requisição GET ao perfil do usuário logo após o login.
"""

import logging
import os
import re
import time
from typing import Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from .config import config

logger = logging.getLogger(__name__)


class AuthError(Exception):
    def __init__(self, message: str, status_code: int = 0, response_body: str = ""):
        super().__init__(message)
        self.status_code = status_code
        self.response_body = response_body


class UserSession:
    """Dados da sessao do usuario apos login (API v1)."""

    def __init__(self, data: dict):
        user = data.get("user", data)
        self.token: str = data.get("accessToken", data.get("token", ""))
        self.refresh_token: str = data.get("refreshToken", "")
        self.user_id: str = str(user.get("id", ""))
        self.email: str = user.get("email", "")
        self.name: str = user.get("name", "")
        self.balance: float = float(user.get("balance", 0))
        self.demo_balance: float = float(user.get("demoBalance", 0))
        self.is_demo: bool = user.get("isDemo", True)
        self.is_vip: bool = user.get("isVip", False)
        self.country: str = user.get("country", "")
        self.real_trades: int = user.get("realTrades", 0)
        self.verified: bool = user.get("verified", False)
        self.raw: dict = data

    def __repr__(self):
        return (
            f"UserSession(id={self.user_id}, email={self.email}, "
            f"balance={self.balance}, demo={self.demo_balance}, "
            f"is_demo={self.is_demo})"
        )


class PumaBrokerAuth:
    """
    Gerencia autenticacao automatica na Puma Broker.

    Fluxo:
      1. POST /login obtem JWT token + dados do usuario
      2. JWT e usado no header Authorization: Bearer <token>
      3. GET /api/v1/users/{id} para capturar server_name_session (WS2)
      4. Refresh automatico quando token expira (erro 401 ou JWT exp)

    Uso:
        auth = PumaBrokerAuth("email@gmail.com", "senha")
        session = auth.login()
        print(session.token)    # JWT pronto para usar
        print(session.user_id)  # "28318"
        ws2 = auth.ws2_token    # server_name_session
    """

    TOKEN_TTL = 23 * 3600

    def __init__(self, email: str, password: str):
        self._email = email
        self._password = password
        self._session: Optional[UserSession] = None
        self._login_ts: float = 0.0
        self._http = self._build_http()
        self._ws2_token_value: Optional[str] = None

    def _log_set_cookie(self, resp, **kwargs):
        """Response hook: loga Set-Cookie de TODAS as respostas HTTP."""
        sc = resp.headers.get("Set-Cookie", "")
        if sc:
            logger.info("Set-Cookie de [%s %s]: %s", resp.request.method, resp.url, sc[:500])

    def _build_http(self) -> requests.Session:
        s = requests.Session()
        s.headers.update({
            **config.DEFAULT_HEADERS,
            "Content-Type": "application/json",
            "Accept": "application/json, text/plain, */*",
        })
        retry = Retry(total=3, backoff_factor=1, status_forcelist=[500, 502, 503, 504])
        s.mount("https://", HTTPAdapter(max_retries=retry))
        s.hooks["response"].append(self._log_set_cookie)
        return s

    def _extract_server_name_session(self, resp: requests.Response) -> Optional[str]:
        """Extrai server_name_session de TODOS os raw Set-Cookie headers."""
        import re
        raw_headers = resp.raw.headers
        if hasattr(raw_headers, "getlist"):
            all_cookies = raw_headers.getlist("Set-Cookie")
        else:
            raw = resp.headers.get("Set-Cookie", "")
            all_cookies = [raw] if raw else []

        for cookie_header in all_cookies:
            m = re.search(r"server_name_session\s*=\s*([^;]+)", cookie_header)
            if m:
                val = m.group(1).strip()
                logger.info(
                    "server_name_session capturado: %s...",
                    val[:20],
                )
                return val

        # Fallback: resp.cookies (confiavel se o jar estava limpo)
        val = resp.cookies.get("server_name_session")
        if val:
            logger.warning(
                "server_name_session via resp.cookies: %s...",
                val[:20],
            )
            return val

        return None

    def _fetch_ws2_session(self) -> None:
        """Faz requisicao ao perfil do usuario para capturar server_name_session.
        O endpoint /api/v1/users/{id} e chamado logo apos login, e a Puma
        aproveita para setar o cookie server_name_session na resposta."""
        if not self._session:
            self._ws2_token_value = None
            return

        url = f"{config.BASE_URL}/api/v1/users/{self._session.user_id}"
        try:
            resp = self._http.get(url, timeout=config.HTTP_TIMEOUT)
            val = self._extract_server_name_session(resp)
            if val:
                self._ws2_token_value = val
                return
        except Exception as e:
            logger.warning("Falha ao buscar server_name_session: %s", e)

        self._ws2_token_value = None

    def _clear_ws2_token(self) -> None:
        """Limpa o token WS2 em cache."""
        if self._ws2_token_value:
            logger.info("WS2 token cache limpo: %s...", self._ws2_token_value[:20])
        self._ws2_token_value = None

    def login(self) -> UserSession:
        """Realiza login e retorna a sessao com JWT token (API v1).

        Endpoint: POST https://trade.pumabroker.com/api/v1/auth/login
        Payload:  {"email": "...", "password": "..."}
        Response: {"accessToken": "eyJ...", "refreshToken": "...", "user": {...}}

        Raises:
            AuthError: credenciais invalidas ou servidor fora
        """
        logger.info("Fazendo login: %s", self._email)

        self._clear_ws2_token()

        payload = {"email": self._email, "password": self._password}

        try:
            resp = self._http.post(
                config.AUTH_LOGIN_URL,
                json=payload,
                timeout=config.HTTP_TIMEOUT,
            )
        except requests.exceptions.ConnectionError as e:
            raise AuthError(f"Sem conexao com o servidor: {e}") from e
        except requests.exceptions.Timeout:
            raise AuthError("Timeout no login")

        if resp.status_code == 401:
            raise AuthError("Email ou senha invalidos (401)", status_code=401, response_body=resp.text)
        if resp.status_code == 403:
            raise AuthError("Conta bloqueada (403)", status_code=403, response_body=resp.text)
        if not resp.ok:
            raise AuthError(
                f"Erro no login: HTTP {resp.status_code} - {resp.text}",
                status_code=resp.status_code,
                response_body=resp.text,
            )

        data = resp.json()

        if "accessToken" not in data and "token" not in data:
            raise AuthError(f"Resposta inesperada do login: {data}")

        self._session = UserSession(data)
        self._login_ts = time.time()

        self._http.headers["Authorization"] = f"Bearer {self._session.token}"

        # Tenta capturar server_name_session da propria resposta de login
        val = self._extract_server_name_session(resp)
        if val:
            self._ws2_token_value = val
        else:
            # Login nao retornou o cookie — faz requisicao extra ao perfil
            logger.info("server_name_session nao veio no login — buscando via GET /users/{id}...")
            self._fetch_ws2_session()

        logger.info(
            "Login OK: %s (id=%s) balance=%.2f demo=%.2f ws2_token=%s",
            self._session.name,
            self._session.user_id,
            self._session.balance,
            self._session.demo_balance,
            "SIM" if self._ws2_token_value else "NAO",
        )
        return self._session

    def ensure_token(self) -> str:
        """Garante que o token esta valido, fazendo refresh se necessario.

        Usa refreshToken se disponivel (API v1) ou faz login novamente.

        Returns:
            JWT token valido
        """
        if self._session is None:
            self.login()
        elif time.time() - self._login_ts >= self.TOKEN_TTL:
            if self._session.refresh_token:
                self._refresh_token()
            else:
                logger.info("JWT expirado renovando...")
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

                # Tenta capturar server_name_session da resposta do refresh
                val = self._extract_server_name_session(resp)
                if val:
                    self._ws2_token_value = val
                else:
                    logger.info("server_name_session nao veio no refresh — buscando via GET /users/{id}...")
                    self._fetch_ws2_session()

                logger.info("JWT renovado com sucesso")
            else:
                logger.warning("Refresh token invalido refazendo login")
                self.login()
        except Exception as e:
            logger.warning("Erro no refresh token: %s refazendo login", e)
            self.login()

    def refresh(self) -> UserSession:
        """Forca renovacao do token (chamar apos erro 401)."""
        logger.info("Refresh forcado do JWT...")
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
            raise AuthError("Nao autenticado. Chame login() primeiro.")
        return self._session.token

    @property
    def user_id(self) -> str:
        if not self._session:
            raise AuthError("Nao autenticado.")
        return self._session.user_id

    @property
    def http(self) -> requests.Session:
        """Sessao HTTP com Authorization header ja configurado."""
        return self._http

    @property
    def ws2_token(self) -> Optional[str]:
        """Retorna o server_name_session para o WS2.
        Capturado de Set-Cookie da resposta do login ou de GET /users/{id}."""
        return self._ws2_token_value
