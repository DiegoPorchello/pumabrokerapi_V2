"""
token_manager.py — Gerenciamento centralizado de tokens com persistência.

Responsabilidades:
  1. Persistir accessToken, refreshToken e metadados em arquivo JSON
  2. Carregar tokens na inicialização
  3. Verificar expiração do accessToken (JWT)
  4. Renovar via refreshToken automaticamente
  5. Fazer login completo apenas quando refreshToken falhar
  6. Logging detalhado de todo fluxo de autenticação
"""

import base64
import json
import logging
import os
import time
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from typing import Optional, Dict, Any

import requests

from .config import config
from .auth import PumaBrokerAuth, UserSession, AuthError

logger = logging.getLogger(__name__)

TOKEN_FILE = "puma_tokens.json"
JWT_ALGO = "HS256"
TOKEN_REFRESH_BUFFER = 300


@dataclass
class TokenData:
    access_token: str = ""
    refresh_token: str = ""
    user_id: str = ""
    email: str = ""
    expires_at: float = 0.0
    issued_at: float = 0.0
    updated_at: str = ""

    def is_expired(self, buffer_seconds: int = TOKEN_REFRESH_BUFFER) -> bool:
        if not self.access_token or self.expires_at == 0:
            return True
        return time.time() >= (self.expires_at - buffer_seconds)

    def is_valid(self) -> bool:
        return bool(self.access_token) and not self.is_expired()

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "TokenData":
        return cls(**data)


class TokenManager:
    """
    Gerenciador centralizado de tokens com persistência automática.

    Fluxo:
      1. Na inicialização, carrega tokens do arquivo JSON
      2. get_access_token() retorna token válido ou renova automaticamente
      3. Se accessToken expirado -> usa refreshToken
      4. Se refreshToken falhar -> faz login completo
      5. Qualquer mudança persiste no arquivo JSON
    """

    def __init__(self, email: str, password: str, token_file: Optional[str] = None):
        self.email = email
        self.password = password
        self.token_file = token_file or TOKEN_FILE
        self._tokens = TokenData()
        self._auth = None  # Lazy init to avoid circular import
        self.ws2_token: str = ""  # server_name_session (compartilhado entre instâncias)
        self._load_tokens()

    def _get_auth(self) -> PumaBrokerAuth:
        """Lazy initialization of PumaBrokerAuth to avoid circular import."""
        if self._auth is None:
            from .auth import PumaBrokerAuth
            self._auth = PumaBrokerAuth(self.email, self.password)
        return self._auth

    def _load_tokens(self) -> None:
        """Carrega tokens do arquivo JSON se existir."""
        if not os.path.exists(self.token_file):
            logger.info("TokenManager: Arquivo de tokens não encontrado (%s), iniciando limpo", self.token_file)
            return

        try:
            with open(self.token_file, "r", encoding="utf-8") as f:
                data = json.load(f)
            self._tokens = TokenData.from_dict(data)
            logger.info(
                "TokenManager: Tokens carregados — user=%s, access_token=%s..., expires_at=%s (%.0fs)",
                self._tokens.email,
                self._tokens.access_token[:20] if self._tokens.access_token else "VAZIO",
                datetime.fromtimestamp(self._tokens.expires_at, tz=timezone.utc).isoformat() if self._tokens.expires_at else "N/A",
                self._tokens.expires_at - time.time() if self._tokens.expires_at else 0,
            )
        except (json.JSONDecodeError, KeyError, TypeError) as e:
            logger.warning("TokenManager: Erro ao ler arquivo de tokens (%s): %s", self.token_file, e)
            self._tokens = TokenData()

    def _save_tokens(self) -> None:
        """Salva tokens no arquivo JSON."""
        self._tokens.updated_at = datetime.now(timezone.utc).isoformat()
        try:
            with open(self.token_file, "w", encoding="utf-8") as f:
                json.dump(self._tokens.to_dict(), f, indent=2)
            logger.debug("TokenManager: Tokens salvos em %s", self.token_file)
        except Exception as e:
            logger.error("TokenManager: Falha ao salvar tokens: %s", e)

    def _decode_jwt_payload(self, token: str) -> Optional[Dict[str, Any]]:
        """Decodifica JWT sem verificação de assinatura para extrair exp/iat (base64)."""
        try:
            # JWT tem 3 partes separadas por ponto: header.payload.signature
            parts = token.split(".")
            if len(parts) != 3:
                logger.warning("TokenManager: JWT inválido (partes=%d)", len(parts))
                return None
            # Payload é a segunda parte, base64url encoded
            payload_b64 = parts[1]
            # Adiciona padding se necessário
            padding = 4 - (len(payload_b64) % 4)
            if padding != 4:
                payload_b64 += "=" * padding
            # Substitui caracteres base64url para base64 padrão
            payload_b64 = payload_b64.replace("-", "+").replace("_", "/")
            payload_json = base64.urlsafe_b64decode(payload_b64).decode("utf-8")
            return json.loads(payload_json)
        except Exception as e:
            logger.warning("TokenManager: Falha ao decodificar JWT: %s", e)
            return None

    def _update_tokens_from_login(self, session: UserSession) -> None:
        """Atualiza tokens internos a partir de uma sessão de login."""
        payload = self._decode_jwt_payload(session.token)
        exp = payload.get("exp") if payload else (time.time() + 23 * 3600)
        iat = payload.get("iat") if payload else time.time()

        self._tokens.access_token = session.token
        self._tokens.refresh_token = session.refresh_token
        self._tokens.user_id = session.user_id
        self._tokens.email = session.email
        self._tokens.expires_at = exp
        self._tokens.issued_at = iat
        self._save_tokens()

        logger.info(
            "TokenManager: Login realizado — user=%s, access_token=%s..., exp=%s (%.0fs)",
            self._tokens.email,
            self._tokens.access_token[:20],
            datetime.fromtimestamp(self._tokens.expires_at, tz=timezone.utc).isoformat(),
            self._tokens.expires_at - time.time(),
        )

    def _update_tokens_from_refresh(self, data: Dict[str, Any]) -> None:
        """Atualiza tokens a partir de resposta de refresh."""
        new_access = data.get("accessToken", "")
        new_refresh = data.get("refreshToken", "")

        if not new_access:
            logger.error("TokenManager: Refresh não retornou accessToken")
            return

        payload = self._decode_jwt_payload(new_access)
        exp = payload.get("exp") if payload else (time.time() + 23 * 3600)
        iat = payload.get("iat") if payload else time.time()

        self._tokens.access_token = new_access
        if new_refresh:
            self._tokens.refresh_token = new_refresh
        self._tokens.expires_at = exp
        self._tokens.issued_at = iat
        self._save_tokens()

        logger.info(
            "TokenManager: Tokens renovados via refresh — access_token=%s..., exp=%s (%.0fs)",
            self._tokens.access_token[:20],
            datetime.fromtimestamp(self._tokens.expires_at, tz=timezone.utc).isoformat(),
            self._tokens.expires_at - time.time(),
        )

    def _do_full_login(self) -> UserSession:
        """Executa login completo (email/senha)."""
        logger.info("TokenManager: Iniciando login completo para %s", self.email)
        auth = self._get_auth()
        session = auth.login()
        self._update_tokens_from_login(session)
        return session

    def _do_refresh(self) -> bool:
        """Tenta renovar accessToken usando refreshToken."""
        if not self._tokens.refresh_token:
            logger.warning("TokenManager: Sem refresh_token disponível")
            return False

        logger.info("TokenManager: Tentando renovar token via refresh_token...")
        try:
            auth = self._get_auth()
            resp = auth._http.post(
                config.AUTH_REFRESH_URL,
                json={"refreshToken": self._tokens.refresh_token},
                timeout=config.HTTP_TIMEOUT,
            )
            if resp.status_code == 200:
                data = resp.json()
                self._update_tokens_from_refresh(data)
                auth._session = self._create_session_from_tokens()
                auth._http.headers["Authorization"] = f"Bearer {self._tokens.access_token}"
                auth._login_ts = time.time()
                logger.info("TokenManager: Refresh bem-sucedido")
                return True
            else:
                logger.warning(
                    "TokenManager: Refresh falhou — HTTP %d: %s",
                    resp.status_code,
                    resp.text[:200],
                )
                return False
        except Exception as e:
            logger.error("TokenManager: Erro no refresh: %s", e)
            return False

    def _create_session_from_tokens(self) -> UserSession:
        """Cria objeto UserSession a partir dos tokens atuais."""
        data = {
            "accessToken": self._tokens.access_token,
            "refreshToken": self._tokens.refresh_token,
            "user": {
                "id": self._tokens.user_id,
                "email": self._tokens.email,
            },
        }
        return UserSession(data)

    def get_access_token(self) -> str:
        """
        Retorna accessToken válido, renovando se necessário.

        Fluxo:
          1. Se accessToken válido -> retorna
          2. Se expirado e tem refreshToken -> tenta refresh
          3. Se refresh falhar ou não tiver refreshToken -> login completo
        """
        if self._tokens.is_valid():
            remaining = self._tokens.expires_at - time.time()
            logger.debug(
                "TokenManager: Token válido — user=%s, expira em %.0fs",
                self._tokens.email,
                remaining,
            )
            return self._tokens.access_token

        logger.info("TokenManager: Access token expirado ou ausente, tentando renovar...")

        if self._tokens.refresh_token and self._do_refresh():
            return self._tokens.access_token

        logger.info("TokenManager: Refresh falhou ou indisponível, fazendo login completo...")
        session = self._do_full_login()
        return session.token

    def get_refresh_token(self) -> str:
        """Retorna refreshToken atual."""
        return self._tokens.refresh_token

    def get_user_id(self) -> str:
        """Retorna user_id, faz login se necessário."""
        if not self._tokens.user_id:
            self.get_access_token()
        return self._tokens.user_id

    def get_token_data(self) -> TokenData:
        """Retorna dados completos dos tokens."""
        return self._tokens

    def update_from_login(self, session: UserSession) -> None:
        """Atualiza tokens a partir de sessão de login (método público)."""
        self._update_tokens_from_login(session)

    def update_from_refresh(self, data: Dict[str, Any]) -> None:
        """Atualiza tokens a partir de resposta de refresh (método público)."""
        self._update_tokens_from_refresh(data)

    def get_auth_session(self) -> UserSession:
        """Retorna sessão completa, garantindo token válido."""
        self.get_access_token()
        return self._create_session_from_tokens()

    def force_refresh(self) -> str:
        """Força renovação do token (útil após erro 401)."""
        logger.info("TokenManager: Refresh forçado solicitado")
        self._tokens.expires_at = 0
        return self.get_access_token()

    def invalidate(self) -> None:
        """Invalida tokens (logout)."""
        logger.info("TokenManager: Invalidando tokens")
        self._tokens = TokenData()
        self._save_tokens()
        self._auth = PumaBrokerAuth(self.email, self.password)

    @property
    def tokens(self) -> TokenData:
        return self._tokens

    @property
    def auth(self) -> PumaBrokerAuth:
        """Retorna instância PumaBrokerAuth com sessão sincronizada."""
        if self._auth is None:
            self._auth = self._get_auth()
        if self._auth._session is None or self._auth._session.token != self._tokens.access_token:
            self._auth._session = self._create_session_from_tokens()
            self._auth._http.headers["Authorization"] = f"Bearer {self._tokens.access_token}"
            self._auth._login_ts = self._tokens.issued_at or time.time()
        return self._auth


_global_token_manager: Optional[TokenManager] = None


def get_token_manager(email: str, password: str, token_file: Optional[str] = None) -> TokenManager:
    """Retorna instância global do TokenManager (singleton por credenciais)."""
    global _global_token_manager
    if _global_token_manager is None or _global_token_manager.email != email:
        _global_token_manager = TokenManager(email, password, token_file)
    return _global_token_manager


def reset_token_manager() -> None:
    """Reseta instância global (útil para testes)."""
    global _global_token_manager
    _global_token_manager = None