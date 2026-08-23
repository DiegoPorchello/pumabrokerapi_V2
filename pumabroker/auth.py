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

  ATENÇÃO: Esta classe agora delega gerenciamento de tokens ao TokenManager.
  Use TokenManager para obter tokens persistentes e renovação automática.
"""

import logging
import os
import random
import re
import time
from typing import TYPE_CHECKING, Optional

if TYPE_CHECKING:
    from .token_manager import TokenData

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from .config import config

logger = logging.getLogger(__name__)

# ── HTTP 429 Backoff Constants ──
BACKOFF_BASE_SECONDS = 2
BACKOFF_MAX_SECONDS = 60
BACKOFF_MAX_RETRIES = 5
_login_backoff_until: float = 0.0  # Timestamp until which login is blocked


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

    Fluxo (delegado ao TokenManager):
      1. TokenManager carrega tokens do arquivo JSON
      2. TokenManager verifica expiração e renova via refreshToken se necessário
      3. Se refreshToken falhar, faz login completo
      4. Tokens são persistidos automaticamente em tokens.json
      5. JWT é usado no header Authorization: Bearer <token>
      6. GET /api/v1/users/{id} para capturar server_name_session (WS2)

    Uso:
        auth = PumaBrokerAuth("email@gmail.com", "senha")
        token = auth.get_token()        # Obtém token válido (com renovação automática)
        session = auth.ensure_session() # Garante sessão completa
        ws2 = auth.ws2_token            # server_name_session
    """

    TOKEN_TTL = 23 * 3600

    def __init__(self, email: str, password: str):
        self._email = email
        self._password = password
        self._token_manager: "TokenManager"
        self._session: Optional[UserSession] = None
        # Lazy import to avoid circular dependency
        from .token_manager import TokenManager
        self._token_manager = TokenManager(email, password)
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

    # Regex que casa tanto o nome legado (server_name_session) quanto o novo
    # (server_session_<hash>, ex: server_session_e2f6fc13).
    WS2_COOKIE_RE = re.compile(
        r"(?:server_name_session|server_session_[0-9a-f]+)\s*=\s*([^;]+)",
        re.IGNORECASE,
    )

    def _extract_server_name_session(self, resp: requests.Response) -> Optional[str]:
        """Extrai o cookie de sessao WS2 de TODOS os raw Set-Cookie headers.

        O servidor Puma passou a usar o nome dinamico server_session_<hash>
        (ex.: server_session_e2f6fc13) em vez do legado server_name_session.
        Aceitamos ambos.
        """
        import re
        raw_headers = resp.raw.headers
        if hasattr(raw_headers, "getlist"):
            all_cookies = raw_headers.getlist("Set-Cookie")
        else:
            raw = resp.headers.get("Set-Cookie", "")
            all_cookies = [raw] if raw else []

        for cookie_header in all_cookies:
            m = self.WS2_COOKIE_RE.search(cookie_header)
            if m:
                val = m.group(1).strip()
                name = cookie_header.split("=", 1)[0].strip()
                logger.info(
                    "WS2 cookie capturado (%s): %s...",
                    name,
                    val[:20],
                )
                return val

        # Fallback: resp.cookies (confiavel se o jar estava limpo)
        for name in ("server_name_session",):
            val = resp.cookies.get(name)
            if val:
                logger.warning(
                    "%s via resp.cookies: %s...",
                    name,
                    val[:20],
                )
                return val
        for cookie in list(resp.cookies):
            if re.match(r"server_session_[0-9a-f]+", cookie.name, re.IGNORECASE):
                logger.warning(
                    "%s via resp.cookies: %s...",
                    cookie.name,
                    cookie.value[:20],
                )
                return cookie.value

        return None

    def _propagate_ws2_token(self, val: str) -> None:
        """Propaga o server_name_session para o TokenManager compartilhado
        (singleton), de onde o daemon e demais instâncias PumaBrokerAuth leem."""
        try:
            if getattr(self._token_manager, "ws2_token", "") != val:
                self._token_manager.ws2_token = val
        except Exception:
            pass

    def _fetch_ws2_session(self) -> None:
        """Faz requisicao ao perfil do usuario para capturar server_name_session.
        O endpoint /api/v1/users/{id} e chamado logo apos login, e a Puma
        aproveita para setar o cookie server_name_session na resposta."""
        token_data = self._token_manager.get_token_data()
        if not token_data or not token_data.user_id:
            self._ws2_token_value = None
            return

        url = f"{config.BASE_URL}/api/v1/users/{token_data.user_id}"
        try:
            # Usa token válido do TokenManager
            access_token = self._token_manager.get_access_token()
            headers = {"Authorization": f"Bearer {access_token}"}
            resp = self._http.get(url, headers=headers, timeout=config.HTTP_TIMEOUT)
            val = self._extract_server_name_session(resp)
            if val:
                self._ws2_token_value = val
                self._propagate_ws2_token(val)
                return
        except Exception as e:
            logger.warning("Falha ao buscar server_name_session: %s", e)

        # Fallback: o cookie pode já estar no jar da sessão HTTP (setado por respostas anteriores)
        jar_val = None
        for cookie in list(self._http.cookies):
            if cookie.name == "server_name_session" or re.match(
                r"server_session_[0-9a-f]+", cookie.name, re.IGNORECASE
            ):
                jar_val = cookie.value
                break
        if jar_val:
            self._ws2_token_value = jar_val
            self._propagate_ws2_token(jar_val)
            return

        self._ws2_token_value = None

    def _clear_ws2_token(self) -> None:
        """Limpa o token WS2 em cache."""
        if self._ws2_token_value:
            logger.info("WS2 token cache limpo: %s...", self._ws2_token_value[:20])
        self._ws2_token_value = None

    def login(self) -> UserSession:
        """Realiza login completo e retorna a sessao com JWT token (API v1).

        Endpoint: POST https://trade.pumabroker.com/api/v1/auth/login
        Payload:  {"email": "...", "password": "..."}
        Response: {"accessToken": "eyJ...", "refreshToken": "...", "user": {...}}

        Raises:
            AuthError: credenciais invalidas ou servidor fora
        """
        global _login_backoff_until

        # ── Backoff check: se estamos em backoff, bloqueia login ──
        now = time.time()
        if now < _login_backoff_until:
            remaining = int(_login_backoff_until - now)
            logger.warning(
                "[AUTH] LOGIN SKIPPED — backoff ativo, aguardar %ds (até %s)",
                remaining,
                time.strftime("%H:%M:%S", time.localtime(_login_backoff_until)),
            )
            raise AuthError(
                f"Login bloqueado por backoff — aguarde {remaining}s",
                status_code=429,
            )

        logger.info("[AUTH] LOGIN START para: %s", self._email)

        self._clear_ws2_token()

        payload = {"email": self._email, "password": self._password}

        # ── Retry com backoff exponencial + jitter para HTTP 429 ──
        last_error = None
        for attempt in range(BACKOFF_MAX_RETRIES):
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

            if resp.status_code == 429:
                # ── HTTP 429: Backoff Exponencial + Jitter ──
                delay = min(BACKOFF_BASE_SECONDS * (2 ** attempt), BACKOFF_MAX_SECONDS)
                jitter = random.uniform(0, delay * 0.3)
                total_delay = delay + jitter
                _login_backoff_until = time.time() + total_delay

                logger.warning(
                    "[AUTH] HTTP 429 — tentativa %d/%d — BACKOFF %.1fs (base=%ds + jitter=%.1fs)",
                    attempt + 1,
                    BACKOFF_MAX_RETRIES,
                    total_delay,
                    delay,
                    jitter,
                )

                if attempt < BACKOFF_MAX_RETRIES - 1:
                    time.sleep(total_delay)
                    continue
                else:
                    raise AuthError(
                        f"HTTP 429 Too Many Requests após {BACKOFF_MAX_RETRIES} tentativas",
                        status_code=429,
                    )

            # ── Resposta não-429: processar normalmente ──
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

            session = UserSession(data)

            # Atualiza TokenManager com novos tokens
            self._token_manager.update_from_login(session)

            # Configura header Authorization na sessão HTTP
            self._http.headers["Authorization"] = f"Bearer {session.token}"

            # Tenta capturar server_name_session da propria resposta de login
            val = self._extract_server_name_session(resp)
            if val:
                self._ws2_token_value = val
                self._propagate_ws2_token(val)
            else:
                # Login nao retornou o cookie — faz requisicao extra ao perfil
                logger.info("server_name_session nao veio no login — buscando via GET /users/{id}...")
                self._fetch_ws2_session()

            logger.info(
                "[AUTH] LOGIN SUCCESS: %s (id=%s) balance=%.2f demo=%.2f ws2_token=%s",
                session.name,
                session.user_id,
                session.balance,
                session.demo_balance,
                "SIM" if self._ws2_token_value else "NAO",
            )
            return session

        # Should not reach here, but safety
        raise AuthError("Login falhou após todas as tentativas")

    def ensure_session(self) -> UserSession:
        """
        Garante sessão válida - faz login ou refresh conforme necessário.
        Delega ao TokenManager que já gerencia persistência e renovação.
        """
        token_data = self._token_manager.get_token_data()

        if token_data and token_data.is_valid():
            logger.info("[TOKEN] AccessToken válido encontrado — user=%s", token_data.email)
            # Cria sessão a partir dos tokens salvos
            session = UserSession({
                "accessToken": token_data.access_token,
                "refreshToken": token_data.refresh_token,
                "user": {
                    "id": token_data.user_id,
                    "email": token_data.email,
                }
            })
            self._http.headers["Authorization"] = f"Bearer {token_data.access_token}"
            return session

        # Token expirado ou não existe - TokenManager fará refresh ou login
        return self.get_session()

    def get_session(self) -> UserSession:
        """
        Obtém sessão válida (faz login se necessário).
        O TokenManager gerencia automaticamente refresh/login.
        """
        access_token = self._token_manager.get_access_token()
        token_data = self._token_manager.get_token_data()

        if token_data and token_data.refresh_token:
            # Tenta refresh se temos refresh token
            try:
                return self._try_refresh_session(token_data)
            except AuthError:
                logger.warning("[TOKEN] Refresh falhou, fazendo login completo")
                return self.login()

        # Sem refresh token ou refresh falhou - login completo
        return self.login()

    def _try_refresh_session(self, token_data: "TokenData") -> UserSession:
        """Tenta renovar sessão usando refresh token."""
        global _login_backoff_until

        # ── Backoff check ──
        now = time.time()
        if now < _login_backoff_until:
            remaining = int(_login_backoff_until - now)
            logger.warning("[TOKEN] REFRESH SKIPPED — backoff ativo, aguardar %ds", remaining)
            raise AuthError(f"Refresh bloqueado por backoff — aguarde {remaining}s", status_code=429)

        logger.info("[TOKEN] Renovando JWT via refreshToken...")

        try:
            resp = self._http.post(
                config.AUTH_REFRESH_URL,
                json={"refreshToken": token_data.refresh_token},
                timeout=config.HTTP_TIMEOUT,
            )
            # O cookie server_name_session (válido por 24h) pode vir na resposta
            # de refresh MESMO quando o refresh token em si é inválido (HTTP 201).
            # Capturamos e propagamos independentemente do status da resposta.
            refresh_ws2 = self._extract_server_name_session(resp)
            if refresh_ws2:
                self._ws2_token_value = refresh_ws2
                self._propagate_ws2_token(refresh_ws2)

            # ── HTTP 429 no refresh: backoff ──
            if resp.status_code == 429:
                delay = min(BACKOFF_BASE_SECONDS, BACKOFF_MAX_SECONDS)
                jitter = random.uniform(0, delay * 0.3)
                _login_backoff_until = time.time() + delay + jitter
                logger.warning(
                    "[AUTH] HTTP 429 no refresh — BACKOFF %.1fs", delay + jitter,
                )
                raise AuthError("HTTP 429 no refresh", status_code=429)

            if resp.status_code == 200:
                data = resp.json()
                new_token = data.get("accessToken", "")
                new_refresh = data.get("refreshToken", "")
                if not new_token:
                    raise AuthError("Refresh response sem accessToken")

                # Atualiza TokenManager
                self._token_manager.update_from_refresh(data)

                # Atualiza header HTTP
                self._http.headers["Authorization"] = f"Bearer {new_token}"

                # Tenta capturar server_name_session (caso ainda não tenha vindo)
                if not refresh_ws2:
                    val = self._extract_server_name_session(resp)
                    if val:
                        self._ws2_token_value = val
                        self._propagate_ws2_token(val)
                    else:
                        self._fetch_ws2_session()

                logger.info("[TOKEN] Novo AccessToken recebido e salvo")

                return UserSession({
                    "accessToken": new_token,
                    "refreshToken": new_refresh,
                    "user": {
                        "id": token_data.user_id,
                        "email": token_data.email,
                    }
                })
            else:
                logger.warning("[TOKEN] RefreshToken inválido (HTTP %d)", resp.status_code)
                raise AuthError("Refresh token inválido")
        except AuthError:
            raise
        except Exception as e:
            logger.warning("[TOKEN] Erro no refresh token: %s", e)
            raise AuthError(f"Erro no refresh: {e}")

    def refresh(self) -> UserSession:
        """Forca renovacao do token (chamar apos erro 401)."""
        logger.info("[TOKEN] Refresh forcado do JWT...")
        token_data = self._token_manager.get_token_data()
        if token_data and token_data.refresh_token:
            return self._try_refresh_session(token_data)
        return self.login()

    def get_access_token(self) -> str:
        """Obtém access token válido (renova automaticamente se expirado)."""
        return self._token_manager.get_access_token()

    def ensure_token(self) -> str:
        """Compatibilidade com chamadas legadas. Delega para get_access_token()."""
        return self.get_access_token()

    @property
    def token(self) -> str:
        """Retorna o token atual (compatibilidade)."""
        return self._token_manager.get_access_token()

    @property
    def user_id(self) -> str:
        token_data = self._token_manager.get_token_data()
        if not token_data:
            raise AuthError("Nao autenticado.")
        return token_data.user_id

    @property
    def http(self) -> requests.Session:
        """Sessao HTTP com Authorization header ja configurado."""
        return self._http

    @property
    def ws2_token(self) -> Optional[str]:
        """Retorna o server_name_session para o WS2."""
        return self._ws2_token_value

    # Propriedades de compatibilidade
    @property
    def session(self) -> Optional[UserSession]:
        token_data = self._token_manager.get_token_data()
        if token_data:
            return UserSession({
                "accessToken": token_data.access_token,
                "refreshToken": token_data.refresh_token,
                "user": {"id": token_data.user_id, "email": token_data.email}
            })
        return None
