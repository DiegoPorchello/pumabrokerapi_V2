"""
proxy_daemon.py
Daemon HTTP que faz proxy das chamadas REST para a Puma Broker API.

Elimina problemas de CORS/Origin/405 que ocorrem quando o frontend
chama a API diretamente via Vite proxy.

Uso:
  python proxy_daemon.py --port 3001

Endpoints:
  POST /login      → Autentica e retorna {user, token, ws2Session}
  GET  /balance    → Saldo da conta
  GET  /active     → Ativos disponíveis
  POST /trades     → Abrir ordem
  GET  /trades/<id> → Status de uma ordem
  GET  /ws2-session → Cookie server_name_session para WS2
  GET  /health     → Health check
"""

import asyncio
import json
import logging
import os
import sys
import threading
import time
from datetime import datetime
from http.server import ThreadingHTTPServer, BaseHTTPRequestHandler
from urllib.parse import urlparse, parse_qs
from dotenv import load_dotenv

load_dotenv()

sys.path.insert(0, os.path.dirname(__file__))
from pumabroker.auth import PumaBrokerAuth, AuthError
from pumabroker.api import TradesAPI, OrderError
from pumabroker.config import config

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("proxy_daemon")


class PumaDaemon:
    COPY_SESSIONS_FILE = os.path.join(os.path.dirname(__file__), ".copy_sessions.json")

    # Buffer circular de logs recebidos do frontend
    _log_buffer: list[dict] = []
    _MAX_LOG_BUFFER = 20000

    # Circuit breaker para re-login automático do WS2
    MAX_RELOGIN_ATTEMPTS = 3
    _relogin_failures: int = 0

    @classmethod
    def push_log(cls, entry: dict):
        cls._log_buffer.append(entry)
        if len(cls._log_buffer) > cls._MAX_LOG_BUFFER:
            cls._log_buffer = cls._log_buffer[-cls._MAX_LOG_BUFFER:]

    @classmethod
    def get_logs(cls, limit: int = 200, level: str = "") -> list[dict]:
        logs = cls._log_buffer
        if level:
            logs = [e for e in logs if e.get("level", "").upper() == level.upper()]
        return logs[-limit:]

    def __init__(self):
        self._auth: PumaBrokerAuth | None = None
        self._trades_api: TradesAPI | None = None
        self._user_id: str | None = None
        self._copy_enabled = True
        self._copy_sessions: list[dict] = []
        self._load_sessions()

    def _save_sessions(self):
        data = {
            "enabled": self._copy_enabled,
            "accounts": [
                {
                    "id": acc["id"],
                    "label": acc["label"],
                    "email": acc["email"],
                    "password": acc["_password"],
                    "is_demo": acc["is_demo"],
                    "active": acc["active"],
                    "last_error": acc["last_error"],
                    "last_sync_at": acc["last_sync_at"].isoformat() if acc["last_sync_at"] else None,
                    "initial_balance": acc["initial_balance"],
                    "total_trades": acc["total_trades"],
                    "total_wins": acc["total_wins"],
                    "total_losses": acc["total_losses"],
                    "total_profit": acc["total_profit"],
                    "started_at": acc["started_at"].isoformat() if acc["started_at"] else None,
                }
                for acc in self._copy_sessions
            ],
        }
        try:
            with open(self.COPY_SESSIONS_FILE, "w") as f:
                json.dump(data, f)
        except Exception as e:
            logger.warning("Falha ao salvar sessões copy: %s", e)

    def _load_sessions(self):
        try:
            with open(self.COPY_SESSIONS_FILE) as f:
                data = json.load(f)
            self._copy_enabled = data.get("enabled", True)
            for acc_data in data.get("accounts", []):
                try:
                    acc = self._copy_account_login(
                        acc_data["label"],
                        acc_data["email"],
                        acc_data["password"],
                        acc_data.get("is_demo", True),
                    )
                    acc["id"] = acc_data["id"]
                    acc["active"] = acc_data.get("active", True)
                    acc["last_error"] = acc_data.get("last_error")
                    acc["initial_balance"] = acc_data.get("initial_balance", acc["initial_balance"])
                    acc["total_trades"] = acc_data.get("total_trades", 0)
                    acc["total_wins"] = acc_data.get("total_wins", 0)
                    acc["total_losses"] = acc_data.get("total_losses", 0)
                    acc["total_profit"] = acc_data.get("total_profit", 0.0)
                    if acc_data.get("started_at"):
                        try:
                            acc["started_at"] = datetime.fromisoformat(acc_data["started_at"])
                        except ValueError:
                            pass
                    if acc_data.get("last_sync_at"):
                        try:
                            acc["last_sync_at"] = datetime.fromisoformat(acc_data["last_sync_at"])
                        except ValueError:
                            acc["last_sync_at"] = None
                    self._copy_sessions.append(acc)
                except Exception as e:
                    logger.warning("Falha ao restaurar conta copy %s: %s", acc_data.get("label", "?"), e)
        except FileNotFoundError:
            pass
        except Exception as e:
            logger.warning("Falha ao carregar sessões copy: %s", e)

    def _copy_fetch_balance(self, auth: PumaBrokerAuth, is_demo: bool) -> float:
        try:
            auth.ensure_token()
            url = f"{config.BASE_URL}/api/v1/users/{auth.user_id}"
            r = auth.http.get(url, timeout=config.HTTP_TIMEOUT)
            if r.status_code == 401:
                auth.login()
                r = auth.http.get(url, timeout=config.HTTP_TIMEOUT)
            r.raise_for_status()
            data = r.json()
            return float(data.get("demoBalance" if is_demo else "balance", 0))
        except Exception:
            return 0.0

    def _copy_account_login(self, label: str, email: str, password: str, is_demo: bool) -> dict:
        auth = PumaBrokerAuth(email, password)
        session = auth.login()
        api = TradesAPI(
            jwt_token=session.token,
            user_id=session.user_id,
            wallet="DEMO" if is_demo else "REAL",
        )
        initial_balance = self._copy_fetch_balance(auth, is_demo)
        return {
            "id": f"copy_{int(time.time() * 1000)}_{len(self._copy_sessions)}",
            "label": label,
            "email": email,
            "is_demo": is_demo,
            "_password": password,
            "active": True,
            "api": api,
            "auth": auth,
            "last_error": None,
            "last_sync_at": None,
            "initial_balance": initial_balance,
            "total_trades": 0,
            "total_wins": 0,
            "total_losses": 0,
            "total_profit": 0.0,
            "started_at": datetime.now(),
        }

    def copy_add_account(self, label: str, email: str, password: str, is_demo: bool) -> dict:
        acc = self._copy_account_login(label, email, password, is_demo)
        self._copy_sessions.append(acc)
        self._save_sessions()
        logger.info("Copy account added: %s (%s)", label, email)
        return self._copy_account_to_dict(acc)

    def copy_remove_account(self, account_id: str) -> bool:
        for i, acc in enumerate(self._copy_sessions):
            if acc["id"] == account_id:
                self._copy_sessions.pop(i)
                self._save_sessions()
                logger.info("Copy account removed: %s (%s)", acc["label"], acc["email"])
                return True
        return False

    def copy_edit_account(self, account_id: str, body: dict) -> bool:
        for acc in self._copy_sessions:
            if acc["id"] == account_id:
                if "label" in body:
                    acc["label"] = body["label"]
                if "active" in body:
                    acc["active"] = body["active"]
                if "is_demo" in body:
                    new_is_demo = bool(body["is_demo"])
                    if new_is_demo != acc["is_demo"]:
                        try:
                            new_acc = self._copy_account_login(
                                acc["label"],
                                acc["email"],
                                acc["_password"],
                                new_is_demo,
                            )
                            new_acc["id"] = acc["id"]
                            new_acc["active"] = acc["active"]
                            idx = self._copy_sessions.index(acc)
                            self._copy_sessions[idx] = new_acc
                            logger.info(
                                "Copy account %s tipo alterado: %s -> %s",
                                acc["label"],
                                "DEMO" if acc["is_demo"] else "REAL",
                                "DEMO" if new_is_demo else "REAL",
                            )
                        except Exception as e:
                            logger.warning(
                                "Falha ao alterar tipo da conta %s: %s",
                                acc["label"],
                                e,
                            )
                            return False
                self._save_sessions()
                logger.info("Copy account %s updated", acc["label"])
                return True
        return False

    def copy_toggle_account(self, account_id: str) -> bool:
        for acc in self._copy_sessions:
            if acc["id"] == account_id:
                acc["active"] = not acc["active"]
                self._save_sessions()
                logger.info("Copy account %s toggled to %s", acc["label"], acc["active"])
                return True
        return False

    def _copy_account_to_dict(self, acc: dict) -> dict:
        current_balance = self._copy_fetch_balance(acc["auth"], acc["is_demo"])
        return {
            "id": acc["id"],
            "label": acc["label"],
            "email": acc["email"],
            "is_demo": acc["is_demo"],
            "active": acc["active"],
            "last_error": acc["last_error"],
            "last_sync_at": acc["last_sync_at"].isoformat() if acc["last_sync_at"] else None,
            "initial_balance": acc["initial_balance"],
            "current_balance": current_balance,
            "total_trades": acc["total_trades"],
            "total_wins": acc["total_wins"],
            "total_losses": acc["total_losses"],
            "total_profit": acc["total_profit"],
            "started_at": acc["started_at"].isoformat() if acc["started_at"] else None,
        }

    def copy_list_accounts(self) -> list[dict]:
        return [self._copy_account_to_dict(acc) for acc in self._copy_sessions]

    def copy_get_status(self) -> dict:
        return {
            "enabled": self._copy_enabled,
            "accounts": self.copy_list_accounts(),
        }

    def copy_toggle_enabled(self) -> bool:
        self._copy_enabled = not self._copy_enabled
        self._save_sessions()
        logger.info("Copy trading toggled to %s", self._copy_enabled)
        return self._copy_enabled

    def login(self, email: str, password: str) -> dict:
        self._auth = PumaBrokerAuth(email, password)
        try:
            session = self._auth.login()
        except AuthError as e:
            logger.error("Falha no login para %s: status=%d, erro=%s", email, e.status_code, e)
            raise

        self._user_id = session.user_id
        self._trades_api = TradesAPI(
            jwt_token=session.token,
            user_id=session.user_id,
            wallet="DEMO" if session.is_demo else "REAL",
        )

        # Extrai o cookie de sessão WS2 (server_name_session)
        ws2_session = self._auth.session_cookie

        if ws2_session:
            ws2_preview = ws2_session[:20] + "..." if len(ws2_session) > 20 else ws2_session
        else:
            ws2_preview = "N/D"

        logger.info(
            "Login OK: %s (id=%s) ws2_session=%s preview=%s",
            session.name, session.user_id,
            "SIM" if ws2_session else "NÃO",
            ws2_preview,
        )

        return {
            "user": {
                "id": session.user_id,
                "email": session.email,
                "name": session.name,
                "firstName": session.name.split()[0] if session.name else "",
                "lastName": " ".join(session.name.split()[1:]) if session.name and len(session.name.split()) > 1 else "",
                "balance": session.balance,
                "demoBalance": session.demo_balance,
                "bonus": 0,
                "isDemo": session.is_demo,
                "isVip": session.is_vip,
                "verified": True,
                "country": session.country,
            },
            "token": session.token,
            "ws2Session": ws2_session,
        }

    def _ensure_auth(self):
        if not self._auth:
            raise AuthError("Não autenticado. Faça login primeiro.")

    def _ensure_token(self):
        self._ensure_auth()
        fresh = self._auth.ensure_token()
        if self._trades_api and fresh != self._trades_api._jwt:
            self._trades_api.update_jwt(fresh)
        return fresh

    def get_balance(self) -> dict:
        self._ensure_auth()
        self._ensure_token()
        if not self._user_id:
            raise AuthError("user_id não disponível. Faça login novamente.")
        # /api/v1/users/me retorna 403; o endpoint correto é /api/v1/users/{user_id}
        url = f"{config.BASE_URL}/api/v1/users/{self._user_id}"
        r = self._auth.http.get(url, timeout=config.HTTP_TIMEOUT)
        if r.status_code == 401:
            self._auth.login()
            r = self._auth.http.get(url, timeout=config.HTTP_TIMEOUT)
        r.raise_for_status()
        data = r.json()
        return {
            "balance": data.get("balance", 0),
            "demoBalance": data.get("demoBalance", 0),
        }

    def get_active(self) -> list:
        self._ensure_auth()
        self._ensure_token()
        r = self._auth.http.get(config.ACTIVE_URL, timeout=config.HTTP_TIMEOUT)
        if r.status_code == 401:
            self._auth.login()
            r = self._auth.http.get(config.ACTIVE_URL, timeout=config.HTTP_TIMEOUT)
        r.raise_for_status()
        return r.json()

    def place_trade(self, body: dict) -> dict:
        self._ensure_auth()
        if not self._trades_api:
            raise RuntimeError("TradesAPI não inicializada.")
        token = self._ensure_token()
        payout = body.get("payout", 0.85)
        timeframe = body.get("timeframe") or self._expiration_to_timeframe(body.get("expiration", 60))

        _t0 = time.perf_counter()
        _t_start_send = None

        try:
            _t_start_send = time.perf_counter()
            result = self._trades_api.place_order(
                symbol=body["asset"],
                direction=body["direction"],
                amount=body["amount"],
                timeframe=timeframe,
                entry_price=body.get("entryPrice", 0),
                payout=payout,
                wallet="DEMO" if body.get("isDemo", True) else "REAL",
            )
            _t1 = time.perf_counter()
            puma_ms = round((_t1 - (_t_start_send or _t0)) * 1000)
            total_ms = round((_t1 - _t0) * 1000)
            logger.info(f"[LATENCY] place_trade: proxy_routing={round(((_t_start_send or _t0) - _t0) * 1000)}ms puma_api={puma_ms}ms total={total_ms}ms")
        except OrderError as e:
            if e.status_code != 401:
                raise
            logger.warning("JWT expirado — renovando e retentando ordem...")
            self._auth.login()
            fresh = self._auth.ensure_token()
            self._trades_api.update_jwt(fresh)
            _t_retry = time.perf_counter()
            result = self._trades_api.place_order(
                symbol=body["asset"],
                direction=body["direction"],
                amount=body["amount"],
                timeframe=timeframe,
                entry_price=body.get("entryPrice", 0),
                payout=payout,
                wallet="DEMO" if body.get("isDemo", True) else "REAL",
            )
            _t2 = time.perf_counter()
            logger.info(f"[LATENCY] place_trade RETRY: puma_api={round((_t2 - _t_retry) * 1000)}ms total={round((_t2 - _t0) * 1000)}ms")

        # ── COPY TRADER: dispara em threads separadas (não bloqueia a trade principal) ──
        if self._copy_enabled:
            n_copies = 0
            for acc in self._copy_sessions:
                if not acc["active"] or not acc.get("api"):
                    continue
                n_copies += 1
                t = threading.Thread(
                    target=self._executar_copy_em_thread,
                    args=(acc, body, payout),
                    daemon=True,
                )
                t.start()
            if n_copies > 0:
                logger.info(f"[LATENCY] copy_trades: {n_copies} conta(s) dispatchadas em threads paralelas")

        return result

    def _executar_copy_em_thread(self, acc: dict, body: dict, payout: float) -> None:
        """Executa copy trade em thread separada (fire-and-forget).
        Falhas são logadas e registradas na conta, NUNCA propagam exceção."""
        try:
            self._copy_place_trade(acc, body, payout)
        except OrderError as e:
            if e.status_code == 401:
                try:
                    acc["auth"].login()
                    fresh = acc["auth"].ensure_token()
                    acc["api"].update_jwt(fresh)
                    self._copy_place_trade(acc, body, payout)
                except Exception as e2:
                    acc["last_error"] = str(e2)[:200]
                    logger.warning("Copy trade retry FAIL: %s — %s", acc["label"], str(e2)[:200])
            else:
                acc["last_error"] = str(e)[:200]
                logger.warning("Copy trade FAIL: %s — %s", acc["label"], str(e)[:200])
        except Exception as e:
            acc["last_error"] = str(e)[:200]
            logger.warning("Copy trade FAIL: %s — %s", acc["label"], str(e)[:200])

    def get_trade(self, order_id: str) -> dict:
        self._ensure_auth()
        self._ensure_token()
        r = self._auth.http.get(
            f"{config.TRADES_URL}/{order_id}",
            timeout=config.HTTP_TIMEOUT,
        )
        if r.status_code == 401:
            logger.warning("JWT expirado em get_trade — renovando e retentando...")
            self._auth.login()
            r = self._auth.http.get(
                f"{config.TRADES_URL}/{order_id}",
                timeout=config.HTTP_TIMEOUT,
            )
        r.raise_for_status()
        return r.json()

    @staticmethod
    def _expiration_to_timeframe(seconds: int) -> str:
        if seconds <= 60:
            return "M1"
        if seconds <= 300:
            return "M5"
        if seconds <= 900:
            return "M15"
        if seconds <= 1800:
            return "M30"
        return "H1"

    def _copy_place_trade(self, acc: dict, body: dict, payout: float) -> dict:
        """Executa uma ordem em uma conta copy, com renovação de token se necessário."""
        fresh = acc["auth"].ensure_token()
        if fresh != acc["api"]._jwt:
            acc["api"].update_jwt(fresh)
        order_result = acc["api"].place_order(
            symbol=body["asset"],
            direction=body["direction"],
            amount=body["amount"],
            timeframe=body.get("timeframe", self._expiration_to_timeframe(body.get("expiration", 60))),
            entry_price=body.get("entryPrice", 0),
            payout=payout,
            wallet="DEMO" if acc["is_demo"] else "REAL",
        )
        acc["last_error"] = None
        acc["last_sync_at"] = datetime.now()
        acc["total_trades"] += 1
        order_status = order_result.get("status", "")
        if order_status == "WIN":
            acc["total_wins"] += 1
            amount = float(body.get("amount", 0))
            acc["total_profit"] += amount * payout
        elif order_status == "LOSS":
            acc["total_losses"] += 1
            amount = float(body.get("amount", 0))
            acc["total_profit"] -= amount
        logger.info("Copy trade OK: %s (total trades: %d)", acc["label"], acc["total_trades"])
        return order_result

    def get_ws2_session(self, force_refresh: bool = False) -> str:
        """Retorna o server_name_session cookie para WS2.
        
        Se force_refresh=True ou o cookie não estiver disponível, faz re-login
        para obter um cookie fresco. Inclui circuit breaker para evitar loops
        infinitos de re-login — após MAX_RELOGIN_ATTEMPTS falhas consecutivas,
        exige login manual pelo frontend.
        """
        self._ensure_auth()

        precisa_relogin = force_refresh or not self._auth.session_cookie

        # Circuit breaker: se excedeu tentativas, bloqueia auto-recuperação
        if precisa_relogin and PumaDaemon._relogin_failures >= PumaDaemon.MAX_RELOGIN_ATTEMPTS:
            logger.warning(
                "WS2 re-login bloqueado: %d falhas consecutivas. Exige login manual.",
                PumaDaemon._relogin_failures,
            )
            raise AuthError(
                f"Re-login automático falhou {PumaDaemon._relogin_failures}x consecutivas. "
                "Faça login manualmente pelo frontend (POST /login)."
            )

        if precisa_relogin:
            logger.info("WS2 session cookie ausente/expirado — refazendo login para obter cookie fresco")
            try:
                self._auth.login()
                self._ensure_token()
                PumaDaemon._relogin_failures = 0  # reset no sucesso
                logger.info("WS2 re-login automático OK — cookie renovado")
            except AuthError:
                PumaDaemon._relogin_failures += 1
                logger.warning(
                    "WS2 re-login falhou (%d/%d).",
                    PumaDaemon._relogin_failures,
                    PumaDaemon.MAX_RELOGIN_ATTEMPTS,
                )
                raise

        token = self._auth.session_cookie
        if not token:
            PumaDaemon._relogin_failures += 1
            raise AuthError(
                "Cookie server_name_session não disponível mesmo após re-login"
            )
        return token

    def get_history(self, symbol: str, resolution: str, from_ts: str, to_ts: str) -> dict:
        """Proxy para GET /api/v1/tradingview/history da Puma (candles históricos)."""
        self._ensure_auth()
        token = self._ensure_token()
        params = {"symbol": symbol, "resolution": resolution, "from": from_ts, "to": to_ts}
        url = f"{config.BASE_URL}/api/v1/tradingview/history"
        r = self._auth.http.get(url, params=params, timeout=config.HTTP_TIMEOUT)
        if r.status_code == 401:
            self._auth.login()
            r = self._auth.http.get(url, params=params, timeout=config.HTTP_TIMEOUT)
        r.raise_for_status()
        return r.json()

    # Símbolos que existem na Binance Futures (USDⓈ-M)
    _FUTURES_SYMBOLS = {
        "BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT", "XRPUSDT", "ADAUSDT",
        "DOGEUSDT", "DOTUSDT", "MATICUSDT", "AVAXUSDT", "LINKUSDT",
        "UNIUSDT", "ATOMUSDT", "LTCUSDT", "BCHUSDT", "ALGOUSDT",
        "MANAUSDT", "SANDUSDT", "AXSUSDT", "APEUSDT", "FILUSDT",
        "NEARUSDT", "FTMUSDT", "EGLDUSDT", "THETAUSDT",
        "EURUSDT", "GBPUSDT", "AUDUSDT", "NZDUSDT", "XAUUSDT",
    }

    # Símbolos forex que NÃO existem na Binance
    _FOREX_ONLY_SYMBOLS = {"USDJPY", "USDCAD", "NZDUSD", "XAUUSD"}

    def get_binance_klines(self, symbol: str, interval: str, limit: int = 100) -> list:
        """Proxy para GET klines da Binance (CORS bypass).
        Usa Futures API (fapi) em vez de Spot (api/v3) para suportar pares como EURUSDT, XAUUSDT, etc.
        """
        import requests as req

        upper = symbol.upper()

        # Símbolos que não existem na Binance — retorna lista vazia
        if upper in self._FOREX_ONLY_SYMBOLS:
            logger.warning("Binance: símbolo forex ignorado (não existe na Binance): %s", upper)
            return []

        params = {"symbol": upper, "interval": interval, "limit": limit}

        # Tenta Futures API primeiro
        if upper in self._FUTURES_SYMBOLS or upper.endswith("USDT"):
            url = "https://fapi.binance.com/fapi/v1/klines"
        else:
            url = "https://api.binance.com/api/v3/klines"

        try:
            r = req.get(url, params=params, timeout=config.HTTP_TIMEOUT)
            r.raise_for_status()
            return r.json()
        except req.exceptions.HTTPError as e:
            # Se falhar na Futures, tenta Spot como fallback
            if "fapi" in url:
                logger.warning("Binance Futures falhou para %s, tentando Spot...", upper)
                try:
                    url2 = "https://api.binance.com/api/v3/klines"
                    r2 = req.get(url2, params=params, timeout=config.HTTP_TIMEOUT)
                    r2.raise_for_status()
                    return r2.json()
                except req.exceptions.HTTPError:
                    logger.error("Binance Spot também falhou para %s: %s", upper, str(e))
                    return []
            logger.error("Binance API erro para %s: %s", upper, str(e))
            return []
        except Exception as e:
            logger.error("Erro inesperado ao buscar klines %s: %s", upper, str(e))
            return []


daemon = PumaDaemon()


class RequestHandler(BaseHTTPRequestHandler):
    def _send(self, status: int, data):
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, PATCH, DELETE, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type, Authorization")
        self.end_headers()
        self.wfile.write(json.dumps(data).encode("utf-8"))

    def _read_body(self) -> dict:
        length = int(self.headers.get("Content-Length", 0))
        if length == 0:
            return {}
        raw = self.rfile.read(length)
        return json.loads(raw.decode("utf-8"))

    def _get_path(self) -> str:
        return urlparse(self.path).path.rstrip("/")

    def do_OPTIONS(self):
        self._send(204, {})

    def do_DELETE(self):
        path = self._get_path()
        try:
            if path.startswith("/copy/accounts/"):
                account_id = path.split("/")[-1]
                ok = daemon.copy_remove_account(account_id)
                if ok:
                    self._send(200, {"success": True})
                else:
                    self._send(404, {"error": "Conta não encontrada"})
            else:
                self._send(404, {"error": f"Rota não encontrada: DELETE {path}"})
        except Exception as e:
            logger.error("Erro em DELETE %s: %s", path, e)
            self._send(500, {"error": str(e)})

    def do_PATCH(self):
        path = self._get_path()
        try:
            if path.startswith("/copy/accounts/"):
                account_id = path.split("/")[-1]
                if account_id == "toggle":
                    result = daemon.copy_toggle_enabled()
                    self._send(200, {"enabled": result})
                else:
                    body = self._read_body()
                    if body:
                        ok = daemon.copy_edit_account(account_id, body)
                        if ok:
                            self._send(200, {"success": True})
                        else:
                            self._send(404, {"error": "Conta não encontrada"})
                    else:
                        ok = daemon.copy_toggle_account(account_id)
                        if ok:
                            self._send(200, {"success": True})
                        else:
                            self._send(404, {"error": "Conta não encontrada"})
            else:
                self._send(404, {"error": f"Rota não encontrada: PATCH {path}"})
        except Exception as e:
            logger.error("Erro em PATCH %s: %s", path, e)
            self._send(500, {"error": str(e)})

    def do_POST(self):
        path = self._get_path()

        try:
            if path == "/login":
                body = self._read_body()
                email = body.get("email", "")
                password = body.get("password", "")
                if not email or not password:
                    self._send(400, {"error": "Email e senha obrigatórios"})
                    return
                result = daemon.login(email, password)
                self._send(200, result)

            elif path == "/trades":
                body = self._read_body()
                result = daemon.place_trade(body)
                self._send(200, {
                    "id": result.get("id", ""),
                    "status": result.get("status", "ACTIVE"),
                })

            elif path == "/copy/accounts":
                body = self._read_body()
                label = body.get("label", "")
                email = body.get("email", "")
                password = body.get("password", "")
                is_demo = body.get("is_demo", True)
                if not label or not email or not password:
                    self._send(400, {"error": "label, email e password são obrigatórios"})
                    return
                try:
                    result = daemon.copy_add_account(label, email, password, is_demo)
                    self._send(200, result)
                except AuthError as e:
                    self._send(400, {"error": f"Falha ao autenticar conta copy: {e}"})
                except Exception as e:
                    self._send(400, {"error": f"Erro ao adicionar conta: {e}"})

            elif path == "/logs":
                body = self._read_body()
                if isinstance(body, list):
                    for entry in body:
                        PumaDaemon.push_log(entry)
                elif isinstance(body, dict):
                    PumaDaemon.push_log(body)
                self._send(200, {"ok": True})

            else:
                self._send(404, {"error": f"Rota não encontrada: POST {path}"})

        except AuthError as e:
            self._send(401, {"error": str(e)})
        except OrderError as e:
            logger.warning("Erro de ordem em POST %s: %s", path, e)
            status = e.status_code if e.status_code in (400, 403, 422, 429) else 400
            self._send(status, {"error": str(e)})
        except Exception as e:
            logger.error("Erro em POST %s: %s", path, e)
            self._send(500, {"error": str(e)})

    def do_GET(self):
        path = self._get_path()

        try:
            if path == "/balance":
                result = daemon.get_balance()
                self._send(200, result)

            elif path == "/active":
                result = daemon.get_active()
                self._send(200, result)

            elif path.startswith("/trades/"):
                order_id = path.split("/")[-1]
                result = daemon.get_trade(order_id)
                self._send(200, result)

            elif path == "/health":
                self._send(200, {"status": "ok"})

            elif path == "/ws2-session":
                query = parse_qs(urlparse(self.path).query)
                force = query.get("force", [""])[0] == "1"
                token = daemon.get_ws2_session(force_refresh=force)
                self._send(200, {"session": token})

            elif path == "/history":
                query = parse_qs(urlparse(self.path).query)
                symbol = query.get("symbol", [""])[0]
                resolution = query.get("resolution", ["60"])[0]
                from_ts = query.get("from", [""])[0]
                to_ts = query.get("to", [""])[0]
                if not symbol or not from_ts or not to_ts:
                    self._send(400, {"error": "Parâmetros obrigatórios: symbol, from, to"})
                    return
                result = daemon.get_history(symbol, resolution, from_ts, to_ts)
                self._send(200, result)

            elif path == "/binance-klines":
                query = parse_qs(urlparse(self.path).query)
                symbol = query.get("symbol", [""])[0]
                interval = query.get("interval", ["1m"])[0]
                limit = int(query.get("limit", ["100"])[0])
                if not symbol:
                    self._send(400, {"error": "Parâmetro obrigatório: symbol"})
                    return
                result = daemon.get_binance_klines(symbol, interval, limit)
                self._send(200, result)

            elif path == "/copy/accounts":
                result = daemon.copy_list_accounts()
                self._send(200, {"accounts": result})

            elif path == "/copy/status":
                result = daemon.copy_get_status()
                self._send(200, result)

            elif path == "/logs":
                query = parse_qs(urlparse(self.path).query)
                limit = int(query.get("limit", ["200"])[0])
                level = query.get("level", [""])[0]
                result = PumaDaemon.get_logs(limit=limit, level=level)
                self._send(200, result)

            else:
                self._send(404, {"error": f"Rota não encontrada: GET {path}"})

        except AuthError as e:
            self._send(401, {"error": str(e)})
        except OrderError as e:
            logger.warning("Erro de ordem em GET %s: %s", path, e)
            self._send(400, {"error": str(e)})
        except Exception as e:
            logger.error("Erro em GET %s: %s", path, e)
            self._send(500, {"error": str(e)})

    def log_message(self, format, *args):
        logger.info("%s %s", self.command, args[0] if args else "")


def main():
    import argparse

    parser = argparse.ArgumentParser(description="Puma Broker Proxy Daemon")
    parser.add_argument("--port", type=int, default=3001, help="Porta REST (3001)")
    parser.add_argument("--ws-port", type=int, default=3002, help="Porta WS2 proxy (3002)")
    parser.add_argument("--host", default="127.0.0.1", help="Host (127.0.0.1)")
    parser.add_argument("--no-ws", action="store_true", help="Não iniciar WS2 proxy")
    args = parser.parse_args()

    # Inicia WS2 proxy em thread separada (se não desativado)
    if not args.no_ws:
        try:
            from ws_proxy import main as ws_main
            ws_thread = threading.Thread(
                target=lambda: asyncio.run(ws_main(host=args.host, port=args.ws_port)),
                daemon=True,
            )
            ws_thread.start()
            print(f"  WS2 Proxy: ws://{args.host}:{args.ws_port}")
        except ImportError:
            print("  AVISO: ws_proxy.py não encontrado — WS2 proxy não iniciado")
        except Exception as e:
            print(f"  AVISO: Erro ao iniciar WS2 proxy: {e}")

    server = ThreadingHTTPServer((args.host, args.port), RequestHandler)
    print("=" * 50)
    print(f"  Puma Broker Proxy Daemon")
    print(f"  REST: http://{args.host}:{args.port}")
    print("=" * 50)
    print("Endpoints:")
    print("  POST /login     -> Autenticar")
    print("  GET  /balance   -> Saldo")
    print("  GET  /active    -> Ativos")
    print("  POST /trades    -> Ordem")
    print("  GET  /trades/id -> Status ordem")
    print("  GET  /health    -> Health check")
    print()
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nDaemon encerrado.")
        server.server_close()


if __name__ == "__main__":
    main()
