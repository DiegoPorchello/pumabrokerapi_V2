import json
import os
import sys
import logging
import threading
import time
import sqlite3
from datetime import datetime, timezone
from dataclasses import dataclass, asdict, fields
from typing import Optional, Dict, Any, List, Callable
from collections import deque
from urllib.parse import urlparse, parse_qs
from http.server import ThreadingHTTPServer, BaseHTTPRequestHandler

# Add parent directory for imports
sys.path.insert(0, os.path.dirname(__file__))

import asyncio
import socketio
from dotenv import load_dotenv

load_dotenv()
from pumabroker.auth import PumaBrokerAuth, AuthError, _login_backoff_until
from pumabroker.token_manager import TokenManager, get_token_manager
from pumabroker.api import TradesAPI, OrderError
from pumabroker.config import config
# ============================================================
# RECOVERY MANAGER CLASSES (embedded for self-contained daemon)
# ============================================================

class TradeStatusEnum:
    PENDING = "PENDING"
    ACTIVE = "ACTIVE"
    WIN = "WIN"
    LOSS = "LOSS"
    DRAW = "DRAW"
    CANCELLED = "CANCELLED"
    EXPIRED = "EXPIRED"

@dataclass
class ActiveTrade:
    """
    Represents an active trade being monitored.
    All timestamps in UTC (ISO 8601 with 'Z' suffix).
    
    State machine:
      ACTIVE → (expiresAt + 30s) → WAITING_RESULT → (tradeResult) → WIN/LOSS/DRAW
      WAITING_RESULT → (5min without tradeResult) → ORPHAN
    """
    id: str
    uid: str = ""  # Puma internal UID
    user_id: str = ""  # Puma userId
    symbol: str = ""
    direction: str = ""  # "CALL" or "PUT"
    amount: float = 0.0
    entry_price: float = 0.0
    payout: float = 0.0
    status: str = "ACTIVE"  # ACTIVE, WAITING_RESULT, WIN, LOSS, DRAW, ORPHAN
    profit: float = 0.0
    gross_profit: float = 0.0  # profit before fees
    net_profit: float = 0.0  # profit after fees (same as profit usually)
    opened_at: str = ""  # UTC ISO format
    expires_at: str = ""  # UTC ISO format
    closed_at: str = ""  # UTC ISO format
    exit_price: float = 0.0
    wallet: str = "REAL"  # "REAL" or "DEMO"
    trade_mode: str = ""  # "real", "demo", etc.
    duration: int = 0  # trade duration in seconds
    timeframe: str = "M1"
    verify_token: str = ""
    created_at: str = ""  # When we first learned about this trade
    updated_at: str = ""  # Last update timestamp
    result: str = ""  # "WON"/"LOST"/"DRAW" raw from tradeResult payload
    new_balance: float = 0.0  # Balance after trade from tradeResult payload
    trade_status: str = ""  # DB column for trade state tracking

    def __post_init__(self):
        now = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
        if not self.created_at:
            self.created_at = now
        if not self.updated_at:
            self.updated_at = now

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict) -> "ActiveTrade":
        # Filter out unknown keys for forward compatibility
        valid = {f.name for f in fields(cls)}
        return cls(**{k: v for k, v in data.items() if k in valid})

    def is_expired(self, grace_seconds: int = 30) -> bool:
        """Check if trade has expired (with grace period)"""
        if not self.expires_at:
            return False
        try:
            exp = datetime.fromisoformat(self.expires_at.replace("Z", "+00:00"))
            now = datetime.now(timezone.utc)
            return (now - exp).total_seconds() > grace_seconds
        except Exception:
            return False

    def is_active(self) -> bool:
        return self.status.upper() in ("PENDING", "ACTIVE")

    def is_waiting_result(self) -> bool:
        return self.status.upper() == "WAITING_RESULT"

    def is_final(self) -> bool:
        return self.status.upper() in ("WIN", "LOSS", "DRAW")

    def is_orphan(self) -> bool:
        return self.status.upper() == "ORPHAN"


class PersistenceManager:
    """
    Handles local persistence of active trades using SQLite.
    Thread-safe with connection pooling.
    """

    DB_FILE = "active_trades.db"
    SCHEMA_VERSION = 2

    def __init__(self, db_path: Optional[str] = None):
        self.db_path = db_path or "active_trades.db"
        self._lock = threading.RLock()
        self._init_db()

    def _init_db(self):
        with self._lock:
            conn = sqlite3.connect(self.db_path, check_same_thread=False)
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA synchronous=NORMAL")
            conn.execute("PRAGMA foreign_keys=ON")
            try:
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS active_trades (
                        id TEXT PRIMARY KEY,
                        symbol TEXT NOT NULL,
                        direction TEXT NOT NULL,
                        amount REAL NOT NULL,
                        entry_price REAL NOT NULL,
                        payout REAL NOT NULL,
                        status TEXT NOT NULL,
                        profit REAL DEFAULT 0.0,
                        opened_at TEXT NOT NULL,
                        expires_at TEXT NOT NULL,
                        closed_at TEXT DEFAULT '',
                        exit_price REAL DEFAULT 0.0,
                        wallet TEXT DEFAULT 'REAL',
                        timeframe TEXT DEFAULT 'M1',
                        verify_token TEXT DEFAULT '',
                        created_at TEXT NOT NULL,
                        updated_at TEXT NOT NULL,
                        result TEXT DEFAULT '',
                        new_balance REAL DEFAULT 0.0,
                        trade_status TEXT DEFAULT '',
                        gross_profit REAL DEFAULT 0.0,
                        net_profit REAL DEFAULT 0.0,
                        trade_mode TEXT DEFAULT '',
                        duration INTEGER DEFAULT 0
                    )
                """)
                conn.execute("""
                    CREATE INDEX IF NOT EXISTS idx_active_trades_status 
                    ON active_trades(status)
                """)
                conn.execute("""
                    CREATE INDEX IF NOT EXISTS idx_active_trades_expires 
                    ON active_trades(expires_at)
                """)
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS schema_version (
                        version INTEGER PRIMARY KEY
                    )
                """)
                conn.execute(
                    "INSERT OR IGNORE INTO schema_version (version) VALUES (?)",
                    (2,)
                )
                for col, dtype, default in [
                    ("result", "TEXT", "''"),
                    ("new_balance", "REAL", "0.0"),
                    ("trade_status", "TEXT", "''"),
                    ("gross_profit", "REAL", "0.0"),
                    ("net_profit", "REAL", "0.0"),
                    ("trade_mode", "TEXT", "''"),
                    ("duration", "INTEGER", "0")
                ]:
                    try:
                        conn.execute(f"ALTER TABLE active_trades ADD COLUMN {col} {dtype} DEFAULT {default}")
                    except sqlite3.OperationalError:
                        pass
                conn.commit()
                logger.info("PersistenceManager: Database initialized at %s", self.db_path)
            finally:
                conn.close()

    def _get_conn(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path, check_same_thread=False)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.row_factory = sqlite3.Row
        return conn

    def save(self, trade: ActiveTrade) -> bool:
        """Save or update a trade. Returns True if inserted, False if updated."""
        with self._lock:
            conn = sqlite3.connect(self.db_path, check_same_thread=False)
            conn.execute("PRAGMA journal_mode=WAL")
            try:
                existing = conn.execute("SELECT id FROM active_trades WHERE id = ?", (trade.id,)).fetchone()
                trade.updated_at = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
                data = trade.to_dict()
                data["trade_status"] = data.get("status", "")
                if existing:
                    conn.execute("""
                        UPDATE active_trades SET
                            symbol=?, direction=?, amount=?, entry_price=?, payout=?,
                            status=?, profit=?, opened_at=?, expires_at=?, closed_at=?,
                            exit_price=?, wallet=?, timeframe=?, verify_token=?,
                            updated_at=?, result=?, new_balance=?, trade_status=?,
                            gross_profit=?, net_profit=?, trade_mode=?, duration=?
                        WHERE id=?
                    """, (
                        data["symbol"], data["direction"], data["amount"], data["entry_price"],
                        data["payout"], data["status"], data["profit"], data["opened_at"],
                        data["expires_at"], data["closed_at"], data["exit_price"],
                        data["wallet"], data["timeframe"], data["verify_token"],
                        data["updated_at"], data["result"], data["new_balance"],
                        data.get("trade_status", ""),
                        data.get("gross_profit", 0.0), data.get("net_profit", 0.0),
                        data.get("trade_mode", ""), data.get("duration", 0),
                        data["id"]
                    ))
                    conn.commit()
                    return False
                else:
                    conn.execute("""
                        INSERT INTO active_trades (
                            id, symbol, direction, amount, entry_price, payout,
                            status, profit, opened_at, expires_at, closed_at,
                            exit_price, wallet, timeframe, verify_token,
                            created_at, updated_at, result, new_balance,
                            trade_status, gross_profit, net_profit, trade_mode, duration
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, (
                        data["id"], data["symbol"], data["direction"], data["amount"],
                        data["entry_price"], data["payout"], data["status"], data["profit"],
                        data["opened_at"], data["expires_at"], data["closed_at"],
                        data["exit_price"], data["wallet"], data["timeframe"],
                        data["verify_token"], data["created_at"], data["updated_at"],
                        data["result"], data["new_balance"],
                        data.get("trade_status", ""),
                        data.get("gross_profit", 0.0), data.get("net_profit", 0.0),
                        data.get("trade_mode", ""), data.get("duration", 0)
                    ))
                    conn.commit()
                    return True
            finally:
                conn.close()

    def delete(self, trade_id: str) -> bool:
        with self._lock:
            conn = sqlite3.connect(self.db_path, check_same_thread=False)
            try:
                cursor = conn.execute("DELETE FROM active_trades WHERE id = ?", (trade_id,))
                conn.commit()
                return cursor.rowcount > 0
            finally:
                conn.close()

    def get(self, trade_id: str) -> Optional[ActiveTrade]:
        with self._lock:
            conn = sqlite3.connect(self.db_path, check_same_thread=False)
            conn.row_factory = sqlite3.Row
            try:
                row = conn.execute("SELECT * FROM active_trades WHERE id = ?", (trade_id,)).fetchone()
                if row:
                    return ActiveTrade(**dict(row))
                return None
            finally:
                conn.close()

    def get_all(self) -> List[ActiveTrade]:
        with self._lock:
            conn = sqlite3.connect(self.db_path, check_same_thread=False)
            conn.row_factory = sqlite3.Row
            try:
                rows = conn.execute("SELECT * FROM active_trades ORDER BY created_at DESC").fetchall()
                return [ActiveTrade(**dict(row)) for row in rows]
            finally:
                conn.close()

    def get_active(self) -> List[ActiveTrade]:
        with self._lock:
            conn = sqlite3.connect(self.db_path, check_same_thread=False)
            conn.row_factory = sqlite3.Row
            try:
                rows = conn.execute(
                    "SELECT * FROM active_trades WHERE status IN (?, ?) ORDER BY created_at DESC",
                    ("PENDING", "ACTIVE")
                ).fetchall()
                return [ActiveTrade(**dict(row)) for row in rows]
            finally:
                conn.close()

    def get_expired_active(self, grace_seconds: int = 15) -> List[ActiveTrade]:
        """Get trades that are ACTIVE/PENDING but past their expiry + grace period"""
        with self._lock:
            conn = sqlite3.connect(self.db_path, check_same_thread=False)
            conn.row_factory = sqlite3.Row
            try:
                rows = conn.execute(
                    "SELECT * FROM active_trades WHERE status IN (?, ?)",
                    ("PENDING", "ACTIVE")
                ).fetchall()
                expired = []
                for row in rows:
                    trade = ActiveTrade(**dict(row))
                    if trade.is_expired(grace_seconds):
                        expired.append(trade)
                return expired
            finally:
                conn.close()

    def count_active(self) -> int:
        with self._lock:
            conn = sqlite3.connect(self.db_path, check_same_thread=False)
            try:
                row = conn.execute(
                    "SELECT COUNT(*) FROM active_trades WHERE status IN (?, ?)",
                    ("PENDING", "ACTIVE")
                ).fetchone()
                return row[0] if row else 0
            finally:
                conn.close()

    def clear_all(self) -> int:
        with self._lock:
            conn = sqlite3.connect(self.db_path, check_same_thread=False)
            try:
                cursor = conn.execute("DELETE FROM active_trades")
                conn.commit()
                return cursor.rowcount
            finally:
                conn.close()


class TradeManager:
    """
    Manages trade lifecycle - open, update, close, query.
    Uses PersistenceManager for durability.
    """

    def __init__(self, persistence: PersistenceManager, api_client: Any):
        self.persistence = persistence
        self.api_client = api_client
        self._lock = threading.RLock()
        self._active_trades: Dict[str, ActiveTrade] = {}
        self._callbacks: List[Callable[[ActiveTrade], None]] = []

    def add_callback(self, callback: Callable[[ActiveTrade], None]):
        self._callbacks.append(callback)

    def _notify(self, trade: ActiveTrade):
        for cb in self._callbacks:
            try:
                cb(trade)
            except Exception as e:
                logger.error("TradeManager callback error: %s", e)

    def load_from_persistence(self) -> int:
        """Load all trades from persistence into memory"""
        with self._lock:
            trades = self.persistence.get_all()
            self._active_trades = {t.id: t for t in trades}
            logger.info("TradeManager: Loaded %d trades from persistence", len(trades))
            return len(trades)

    def load_active_from_persistence(self) -> int:
        """Load only ACTIVE/PENDING trades from persistence"""
        with self._lock:
            trades = self.persistence.get_active()
            self._active_trades = {t.id: t for t in trades}
            logger.info("TradeManager: Loaded %d active trades from persistence", len(trades))
            return len(trades)

    TIMEFRAME_SECONDS = {"M1": 60, "M5": 300, "M15": 900, "M30": 1800, "H1": 3600}

    def create_from_order(self, order_data: dict) -> ActiveTrade:
        """Create ActiveTrade from order placement response"""
        now = datetime.now(timezone.utc)
        expires_at = now.timestamp() + 60  # Default M1 = 60 seconds
        if "expiresAt" in order_data and order_data["expiresAt"]:
            expires_at = datetime.fromisoformat(order_data["expiresAt"].replace("Z", "+00:00")).timestamp()
        elif "duration" in order_data:
            dur = order_data["duration"]
            if isinstance(dur, (int, float)):
                expires_at = now.timestamp() + dur
            elif isinstance(dur, str):
                expires_at = now.timestamp() + self.TIMEFRAME_SECONDS.get(dur.upper(), 60)

        trade = ActiveTrade(
            id=str(order_data.get("id", order_data.get("tradeId", ""))),
            symbol=order_data.get("symbol", order_data.get("asset", "")),
            direction=order_data.get("direction", ""),
            amount=float(order_data.get("amount", 0)),
            entry_price=float(order_data.get("entryPrice", order_data.get("entry_price", 0))),
            payout=float(order_data.get("payout", 0)),
            status="ACTIVE",
            profit=0.0,
            opened_at=now.isoformat().replace("+00:00", "Z"),
            expires_at=datetime.fromtimestamp(expires_at, timezone.utc).isoformat().replace("+00:00", "Z"),
            closed_at="",
            exit_price=0.0,
            wallet=order_data.get("wallet", "REAL"),
            timeframe=order_data.get("timeframe", "M1"),
            verify_token=order_data.get("verify", ""),
            created_at=now.isoformat().replace("+00:00", "Z"),
            updated_at=now.isoformat().replace("+00:00", "Z"),
        )
        with self._lock:
            self._active_trades[trade.id] = trade
            self.persistence.save(trade)
            logger.info("TradeManager: Created trade %s %s %s", trade.id, trade.symbol, trade.direction)
            self._notify(trade)
        return trade

    def update_from_result(self, trade_result: dict) -> Optional[ActiveTrade]:
        """Update trade from tradeResult event.
        
        Handles both structures:
          A) {"trade": {...}, "result": "WON"}   (Socket.IO unwraps tradeResult)
          B) {"tradeResult": {"trade": {...}}}    (raw nesting preserved)
        
        Redundant validation: checks both trade.status AND result field.
        Saves ALL fields from the Puma payload.
        """
        trade_data = trade_result.get("trade") or trade_result.get("tradeResult", {}).get("trade", {})
        trade_id = str(trade_data.get("id", trade_result.get("id", "")))
        if not trade_id:
            return None

        # ── Redundant validation: trade.status + result ──
        trade_status = str(trade_data.get("status", "")).upper()
        result = str(trade_result.get("result", "")).upper()
        
        if trade_status == "WON" or result == "WON":
            final_status = "WIN"
        elif trade_status == "LOST" or result == "LOST":
            final_status = "LOSS"
        elif trade_status == "DRAW" or result == "DRAW":
            final_status = "DRAW"
        else:
            final_status = trade_status or "ACTIVE"

        profit = float(trade_data.get("profit", trade_result.get("profit", 0)))
        new_balance = float(trade_result.get("newBalance", 0))

        with self._lock:
            trade = self._active_trades.get(trade_id)
            if not trade:
                trade = ActiveTrade(
                    id=trade_id,
                    uid=str(trade_data.get("uid", "")),
                    user_id=str(trade_data.get("userId", trade_data.get("user_id", ""))),
                    symbol=trade_data.get("symbol", trade_data.get("currency", "")),
                    direction=trade_data.get("direction", ""),
                    amount=float(trade_data.get("amount", 0)),
                    entry_price=float(trade_data.get("entryPrice", trade_data.get("entry_price", 0))),
                    payout=float(trade_data.get("payout", 0)),
                    status=final_status,
                    profit=profit,
                    opened_at=trade_data.get("openedAt", ""),
                    expires_at=trade_data.get("expiresAt", ""),
                    closed_at=trade_data.get("closedAt", datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")),
                    exit_price=float(trade_data.get("exitPrice", trade_data.get("exit_price", 0))),
                    wallet=trade_data.get("wallet", "REAL"),
                    timeframe=trade_data.get("timeframe", "M1"),
                    result=final_status,
                    new_balance=new_balance,
                    trade_mode=str(trade_data.get("tradeMode", "")),
                    duration=int(trade_data.get("duration", 0)),
                )
                self._active_trades[trade.id] = trade
            else:
                trade.status = final_status
                trade.profit = profit
                trade.result = final_status
                trade.new_balance = new_balance
                trade.closed_at = trade_data.get("closedAt", datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"))
                trade.exit_price = float(trade_data.get("exitPrice", trade_data.get("exit_price", trade.exit_price)))
                trade.updated_at = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

            self.persistence.save(trade)
            logger.info("TradeManager: Trade %s finalized — status=%s result=%s profit=%.2f newBalance=%.2f",
                        trade.id, final_status, result, profit, new_balance)
            self._notify(trade)

            if trade.is_final():
                logger.info("TradeManager: Trade %s finalized as %s", trade.id, final_status)

            return trade

    def update_from_poll(self, api_trade: dict) -> Optional[ActiveTrade]:
        """Update trade from REST API poll (GET /trades or /trades/{id})"""
        trade_id = str(api_trade.get("id", ""))
        if not trade_id:
            return None

        status = api_trade.get("status", "ACTIVE").upper()
        result = api_trade.get("result", "").upper()
        profit = float(api_trade.get("profit", 0))

        if result in ("WON", "WIN"):
            status = "WIN"
        elif result in ("LOST", "LOSS"):
            status = "LOSS"
        elif result == "DRAW":
            status = "DRAW"

        with self._lock:
            trade = self._active_trades.get(trade_id)
            if not trade:
                trade = ActiveTrade(
                    id=trade_id,
                    symbol=api_trade.get("symbol", api_trade.get("asset", "")),
                    direction=api_trade.get("direction", ""),
                    amount=float(api_trade.get("amount", 0)),
                    entry_price=float(api_trade.get("entryPrice", api_trade.get("entry_price", 0))),
                    payout=float(api_trade.get("payout", 0)),
                    status=status,
                    profit=profit,
                    opened_at=api_trade.get("openedAt", ""),
                    expires_at=api_trade.get("expiresAt", ""),
                    wallet=api_trade.get("wallet", "REAL"),
                    timeframe=api_trade.get("timeframe", "M1"),
                )
                self._active_trades[trade.id] = trade
            else:
                trade.status = status
                trade.profit = profit
                if status in ("WIN", "LOSS", "DRAW"):
                    trade.closed_at = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
                    trade.exit_price = float(api_trade.get("exitPrice", api_trade.get("exit_price", trade.exit_price)))
                trade.updated_at = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

            self.persistence.save(trade)
            self._notify(trade)
            return trade

    def get(self, trade_id: str) -> Optional[ActiveTrade]:
        with self._lock:
            return self._active_trades.get(trade_id)

    def get_all(self) -> List[ActiveTrade]:
        with self._lock:
            return list(self._active_trades.values())

    def get_active(self) -> List[ActiveTrade]:
        with self._lock:
            return [t for t in self._active_trades.values() if t.is_active()]

    def get_expired_active(self, grace_seconds: int = 15) -> List[ActiveTrade]:
        with self._lock:
            return [t for t in self._active_trades.values() if t.is_expired(grace_seconds)]

    def remove(self, trade_id: str) -> bool:
        with self._lock:
            if trade_id in self._active_trades:
                del self._active_trades[trade_id]
                self.persistence.delete(trade_id)
                logger.info("TradeManager: Removed trade %s", trade_id)
                return True
            return False


class RecoveryManager:
    """
    Trade recovery via persistence + WebSocket tradeResult events only.
    Does NOT call REST API endpoints (they return 404 on Puma).
    """

    EXPIRY_GRACE_SECONDS = 15
    CHECK_INTERVAL = 10

    def __init__(
        self,
        trade_manager: TradeManager,
        api_client: Any,
        socket_manager: Any = None,
        persistence: PersistenceManager = None
    ):
        self.trade_manager = trade_manager
        self.api_client = api_client
        self.socket_manager = socket_manager
        self.persistence = persistence or trade_manager.persistence
        self._lock = threading.RLock()
        self._running = False
        self._thread: Optional[threading.Thread] = None
        self._stop_event = threading.Event()
        self._last_check = 0
        self._socket_connected = False

    def start(self):
        with self._lock:
            if self._running:
                return
            self._running = True
            self._stop_event.clear()
            self._thread = threading.Thread(target=self._run_loop, daemon=True, name="RecoveryManager")
            self._thread.start()
            logger.info("RecoveryManager: Started (persistence-only mode)")

    def stop(self):
        with self._lock:
            if not self._running:
                return
            self._running = False
            self._stop_event.set()
            if self._thread:
                self._thread.join(timeout=5)
            logger.info("RecoveryManager: Stopped")

    def on_socket_connect(self):
        with self._lock:
            logger.info("RecoveryManager: Socket connected")
            self._socket_connected = True

    def on_socket_disconnect(self):
        with self._lock:
            self._socket_connected = False

    def _run_loop(self):
        while not self._stop_event.is_set():
            try:
                now = time.time()
                if now - self._last_check >= self.CHECK_INTERVAL:
                    self._check_expired_trades()
                    self._check_socket_health()
                    self._last_check = now
            except Exception as e:
                logger.error("RecoveryManager loop error: %s", e)
            for _ in range(self.CHECK_INTERVAL):
                if self._stop_event.is_set():
                    break
                time.sleep(1)

    def _check_expired_trades(self):
        """State machine for expired trades:
        1. ACTIVE -> WAITING_RESULT (when expires_at is reached + small grace)
        2. WAITING_RESULT -> ORPHAN (when 5 minutes pass without result)
        3. ORPHAN -> REMOVED (when 1 hour passes — cleanup)
        """
        all_trades = self.trade_manager.get_all()
        now_utc = datetime.now(timezone.utc)
        removed_count = 0
        
        for trade in all_trades:
            if trade.status in ("WIN", "LOSS", "DRAW", "CANCELLED", "MISSING_RESULT", "DELAYED_RESULT"):
                continue
                
            if not trade.expires_at:
                continue
                
            try:
                exp_time = datetime.fromisoformat(trade.expires_at.replace("Z", "+00:00"))
                seconds_since_expiry = (now_utc - exp_time).total_seconds()
            except Exception:
                continue
                
            if trade.status in ("ACTIVE", "PENDING"):
                if seconds_since_expiry > self.EXPIRY_GRACE_SECONDS:
                    logger.info("RecoveryManager: Trade %s expired (%s). Transitioning to WAITING_RESULT", trade.id, trade.expires_at)
                    trade.status = "WAITING_RESULT"
                    trade.updated_at = now_utc.isoformat().replace("+00:00", "Z")
                    self.trade_manager.persistence.save(trade)
                    
            elif trade.status == "WAITING_RESULT":
                if seconds_since_expiry > 300:
                    logger.warning("RecoveryManager: Trade %s WAITING_RESULT for > 5m. Transitioning to ORPHAN", trade.id)
                    trade.status = "ORPHAN"
                    trade.closed_at = now_utc.isoformat().replace("+00:00", "Z")
                    trade.updated_at = trade.closed_at
                    self.trade_manager.persistence.save(trade)

            elif trade.status == "ORPHAN":
                # Limpa trades ORPHAN antigos (> 1 hora) para não acumular no SQLite
                if seconds_since_expiry > 3600:
                    logger.info(
                        "RecoveryManager: Removendo trade ORPHAN antigo %s "
                        "(%.0fs desde expiração)", trade.id, seconds_since_expiry
                    )
                    self.trade_manager.remove(trade.id)
                    removed_count += 1

        if removed_count > 0:
            logger.info("RecoveryManager: %d trades ORPHAN removidos", removed_count)

    def _check_socket_health(self):
        if self.socket_manager:
            try:
                is_connected = getattr(self.socket_manager, 'trades_connected', False)
                if is_connected and not self._socket_connected:
                    self.on_socket_connect()
                elif not is_connected and self._socket_connected:
                    self.on_socket_disconnect()
            except Exception:
                pass

    def force_reconcile(self):
        logger.info("RecoveryManager: Force check triggered")
        self._check_expired_trades()

    def get_status(self) -> dict:
        with self._lock:
            active = self.trade_manager.get_active()
            expired = self.trade_manager.get_expired_active(self.EXPIRY_GRACE_SECONDS)
            return {
                "running": self._running,
                "socket_connected": self._socket_connected,
                "active_trades": len(active),
                "expired_trades": len(expired),
                "total_persisted": self.persistence.count_active(),
            }


class APIClient:
    """Wrapper for API calls with consistent error handling"""

    def __init__(self, base_url: str, auth_token: str = ""):
        self.base_url = base_url.rstrip("/")
        self.auth_token = auth_token
        self._session = None

    def _get_headers(self) -> dict:
        return {
            "Authorization": f"Bearer {self.auth_token}",
            "Content-Type": "application/json",
            "Accept": "application/json",
        }

    def _request(self, method: str, path: str, **kwargs) -> Optional[dict]:
        import requests
        from pumabroker.auth import AuthError
        url = f"{self.base_url}{path}"
        headers = self._get_headers()
        auth_preview = headers.get("Authorization", "MISSING")[:30] + "..."
        logger.info("%s %s | Authorization: %s", method, path, auth_preview)
        try:
            resp = requests.request(method, url, headers=headers, timeout=10, **kwargs)
            logger.info("%s %s | Status HTTP: %d", method, path, resp.status_code)
            if resp.status_code != 200:
                logger.warning("%s %s | Non-200 response: %s", method, path, resp.text[:500])
            if resp.status_code == 200:
                return resp.json()
            elif resp.status_code == 401:
                raise AuthError("Token expired or invalid (401)", status_code=401, response_body=resp.text)
            elif resp.status_code == 403:
                raise AuthError("Access forbidden (403)", status_code=403, response_body=resp.text)
            elif resp.status_code == 404:
                return None
            else:
                logger.error("API %s %s failed: %d %s", method, path, resp.status_code, resp.text)
                return None
        except AuthError:
            raise
        except Exception as e:
            logger.error("API %s %s error: %s", method, path, e)
            return None

    def get_trades(self, limit: int = 50) -> List[dict]:
        data = self._request("GET", f"/api/v1/trades?limit={limit}")
        if isinstance(data, list):
            return data
        if isinstance(data, dict) and "trades" in data:
            return data["trades"]
        return []

    def get_trade(self, trade_id: str) -> Optional[dict]:
        return self._request("GET", f"/api/v1/trades/{trade_id}")

    def place_trade(self, body: dict) -> dict:
        result = self._request("POST", "/api/v1/trades", json=body)
        return result or {}

    def set_auth(self, token: str):
        self.auth_token = token


# ============================================================
# END RECOVERY MANAGER CLASSES
# ============================================================

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

    # Buffer circular de logs de copy trade (visível na UI)
    _copy_log_buffer: list[dict] = []
    _MAX_COPY_LOG_BUFFER = 200

    # Circuit breaker para re-login automático do WS2
    MAX_RELOGIN_ATTEMPTS = 3
    _relogin_failures: int = 0
    _relogin_lock = threading.Lock()
    _relogging_in: bool = False

    # ─────────────────────────────────────────────────────────────────
    # WebSocket listener para tradeUpdate em tempo real (WS3 /trades)
    # ─────────────────────────────────────────────────────────────────
    class _TradeWSListener:
        """Escuta eventos tradeUpdate via Socket.IO e atualiza _trade_history instantaneamente."""

        def __init__(self, daemon_ref: "PumaDaemon"):
            self._daemon = daemon_ref
            self._sio: socketio.AsyncClient | None = None
            self._thread: threading.Thread | None = None
            self._loop: asyncio.AbstractEventLoop | None = None
            self._running = False
            self._connected = False

        @property
        def trades_connected(self) -> bool:
            return self._connected

        def start(self, jwt_token: str, user_id: str) -> None:
            if self._running:
                return
            self._running = True
            self._thread = threading.Thread(target=self._run_loop, args=(jwt_token, user_id), daemon=True)
            self._thread.start()
            logger.info("Trade WS listener iniciado (thread background)")

        def stop(self) -> None:
            self._running = False
            if self._sio and self._sio.connected and self._loop:
                asyncio.run_coroutine_threadsafe(self._sio.disconnect(), self._loop)
            if self._thread:
                self._thread.join(timeout=3)

        def _run_loop(self, jwt_token: str, user_id: str) -> None:
            self._loop = asyncio.new_event_loop()
            asyncio.set_event_loop(self._loop)
            self._loop.run_until_complete(self._connect_and_listen(jwt_token, user_id))

        async def _connect_and_listen(self, jwt_token: str, user_id: str) -> None:
            self._sio = socketio.AsyncClient(
                logger=False,
                engineio_logger=False,
                reconnection=True,
                reconnection_attempts=10,
                reconnection_delay=2,
                reconnection_delay_max=30,
            )

            @self._sio.on("connect", namespace="/trades")
            async def connect():
                self._connected = True
                logger.info("Trade WS conectado — subscrevendo account_id=%s", user_id)
                await self._sio.emit("subscribe", user_id, namespace="/trades")
                if self._daemon.recovery_manager:
                    self._daemon.recovery_manager.on_socket_connect()

            @self._sio.on("tradeUpdate", namespace="/trades")
            async def on_trade_update(*args):
                self._handle_trade_update(args[0] if args else {})

            @self._sio.on("tradeResult", namespace="/trades")
            async def on_trade_result(*args):
                self._handle_trade_result(args[0] if args else {})

            @self._sio.on("disconnect", namespace="/trades")
            async def disconnect():
                self._connected = False
                logger.warning("Trade WS desconectado")
                if self._daemon.recovery_manager:
                    self._daemon.recovery_manager.on_socket_disconnect()

            try:
                await self._sio.connect(
                    config.BASE_URL,
                    socketio_path="/socket.io/",
                    transports=["websocket"],
                    headers={"Authorization": f"Bearer {jwt_token}"},
                    namespaces=["/trades"],
                )
                await self._sio.wait()
            except Exception as e:
                logger.error("Trade WS erro: %s", e)
            finally:
                self._connected = False

        def _handle_trade_update(self, data: dict) -> None:
            try:
                trade_id = str(data.get("id", ""))
                if not trade_id:
                    return
                status = data.get("status", "ACTIVE")
                profit = float(data.get("profit", 0))
                normalized = status.upper()
                if normalized in ("WON", "WIN"):
                    result = "win"
                elif normalized in ("LOST", "LOSS"):
                    result = "loss"
                elif normalized == "DRAW":
                    result = "draw"
                else:
                    result = "pending"

                # ── RECOVERY: Atualiza TradeManager para persistir status em tempo real ──
                trade = self._daemon.trade_manager.update_from_poll(data)
                if trade:
                    logger.info("Recovery: tradeUpdate processado via TradeManager: id=%s status=%s profit=%.2f", trade.id, trade.status, trade.profit)

                # Atualiza entrada existente no histórico
                with self._daemon._trade_history_lock:
                    for entry in self._daemon._trade_history:
                        if entry.get("id") == trade_id:
                            entry["status"] = status
                            entry["result"] = result
                            entry["profit"] = profit
                            entry["exitPrice"] = data.get("exitPrice")
                            logger.info("WS tradeUpdate: id=%s status=%s profit=%.2f", trade_id, status, profit)
                            return

                    # Trade novo (ex: copy trade) — adiciona ao histórico
                    self._daemon._trade_history.insert(0, {
                        "id": trade_id,
                        "asset": data.get("symbol"),
                        "direction": data.get("direction"),
                        "amount": data.get("amount"),
                        "entryPrice": data.get("entryPrice"),
                        "payout": data.get("payout"),
                        "status": status,
                        "profit": profit,
                        "result": result,
                        "openedAt": data.get("createdAt", time.time()),
                    })
                    if len(self._daemon._trade_history) > 200:
                        self._daemon._trade_history = self._daemon._trade_history[:200]

            except Exception as e:
                logger.error("Erro processando tradeUpdate: %s", e)

        def _handle_trade_result(self, data: dict) -> None:
            try:
                trade_data = data.get("trade") or data.get("tradeResult", {}).get("trade", {})
                trade_id = str(trade_data.get("id", "") or data.get("id", "") or data.get("tradeId", ""))
                if not trade_id:
                    return

                # ── Redundant validation: trade.status + result ──
                trade_status = str(trade_data.get("status", "")).upper()
                result = str(data.get("result", "")).upper()
                
                if trade_status == "WON" or result == "WON":
                    final_status = "WIN"
                elif trade_status == "LOST" or result == "LOST":
                    final_status = "LOSS"
                elif trade_status == "DRAW" or result == "DRAW":
                    final_status = "DRAW"
                else:
                    final_status = trade_status or "ACTIVE"

                profit = float(trade_data.get("profit", 0) or data.get("profit", 0) or data.get("pnl", 0))
                new_balance = float(data.get("newBalance", 0))

                logger.info("WS tradeResult: id=%s trade.status=%s result=%s → final=%s profit=%.2f newBalance=%.2f",
                            trade_id, trade_status, result, final_status, profit, new_balance)
                
                # ── RECOVERY: Usa TradeManager para persistir e sincronizar ──
                trade = self._daemon.trade_manager.update_from_result(data)
                if trade:
                    logger.info("Recovery: tradeResult processado via TradeManager: id=%s status=%s profit=%.2f", trade.id, trade.status, trade.profit)
                
                # Mantém _trade_history para compatibilidade com GET /trades
                with self._daemon._trade_history_lock:
                    for entry in self._daemon._trade_history:
                        if entry.get("id") == trade_id:
                            entry["status"] = final_status
                            entry["result"] = final_status.lower()
                            entry["profit"] = profit
                            entry["newBalance"] = new_balance
                            entry["exitPrice"] = trade_data.get("exitPrice", entry.get("exitPrice"))
                            entry["closedAt"] = trade_data.get("closedAt", entry.get("closedAt"))
                            logger.info("WS tradeResult (legacy): id=%s status=%s profit=%.2f", trade_id, final_status, profit)
                            return

                    # Trade novo (ex: copy trade) — adiciona ao histórico
                    self._daemon._trade_history.insert(0, {
                        "id": trade_id,
                        "asset": trade_data.get("symbol", trade_data.get("currency", "")),
                        "direction": trade_data.get("direction", ""),
                        "amount": trade_data.get("amount", 0),
                        "entryPrice": trade_data.get("entryPrice", 0),
                        "payout": trade_data.get("payout", 0),
                        "status": final_status,
                        "profit": profit,
                        "result": result.lower(),
                        "newBalance": new_balance,
                        "openedAt": trade_data.get("openedAt", time.time()),
                        "closedAt": trade_data.get("closedAt", time.time()),
                    })
                    if len(self._daemon._trade_history) > 200:
                        self._daemon._trade_history = self._daemon._trade_history[:200]

            except Exception as e:
                logger.error("Erro processando tradeResult: %s", e)

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

    @classmethod
    def push_copy_log(cls, entry: dict):
        entry["timestamp"] = time.time()
        cls._copy_log_buffer.append(entry)
        if len(cls._copy_log_buffer) > cls._MAX_COPY_LOG_BUFFER:
            cls._copy_log_buffer = cls._copy_log_buffer[-cls._MAX_COPY_LOG_BUFFER:]

    @classmethod
    def get_copy_logs(cls, limit: int = 50) -> list[dict]:
        return cls._copy_log_buffer[-limit:]

    def __init__(self):
        self._auth: PumaBrokerAuth | None = None
        self._trades_api: TradesAPI | None = None
        self._user_id: str | None = None
        self._copy_enabled = False
        self._copy_user_confirmed = False
        self._copy_sessions: list[dict] = []
        self._recent_orders: deque[dict] = deque(maxlen=50)
        self._trade_history: list[dict] = []
        self._trade_history_lock = threading.Lock()
        self._trade_ws = self._TradeWSListener(self)
        self._load_sessions()
        self._token_manager = None

        # ── RECOVERY MANAGER ──────────────────────────────────────
        self.persistence = PersistenceManager()
        self.trade_manager = TradeManager(self.persistence, self)
        self.recovery_manager = RecoveryManager(self.trade_manager, self, self._trade_ws, self.persistence)
        self.api_client = self

    def _save_sessions(self):
        data = {
            "enabled": False,  # NUNCA salva True — só toggle via UI reativa em runtime
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
        """Carrega sessoes de copy trading do arquivo JSON.
        NAO faz login automatico -- login e feito sob demanda (lazy) quando copy esta ativo.
        """
        try:
            with open(self.COPY_SESSIONS_FILE, "r") as f:
                data = json.load(f)
            # NUNCA carrega "enabled" do arquivo — só toggle explícito via UI reinicia copy
            self._copy_enabled = False
            for acc_data in data.get("accounts", []):
                # Reconstrói a conta SEM fazer login -- só guarda configuração
                acc = {
                    "id": acc_data["id"],
                    "label": acc_data["label"],
                    "email": acc_data["email"],
                    "_password": acc_data.get("password", ""),
                    "is_demo": acc_data["is_demo"],
                    "active": acc_data.get("active", True),
                    "api": None,
                    "auth": None,
                    "last_error": None,  # Limpa erros anteriores — será reavaliado no próximo login
                    "last_sync_at": datetime.fromisoformat(acc_data["last_sync_at"]) if acc_data.get("last_sync_at") else None,
                    "initial_balance": acc_data.get("initial_balance", 0),
                    "total_trades": acc_data.get("total_trades", 0),
                    "total_wins": acc_data.get("total_wins", 0),
                    "total_losses": acc_data.get("total_losses", 0),
                    "total_profit": acc_data.get("total_profit", 0.0),
                    "started_at": datetime.fromisoformat(acc_data["started_at"]) if acc_data.get("started_at") else datetime.now(),
                }
                self._copy_sessions.append(acc)
        except FileNotFoundError:
            pass
        except Exception as e:
            logger.warning("Falha ao carregar sessoes copy: %s", e)

    def _copy_account_lazy_login(self, acc: dict) -> bool:
        """Garante que a conta copy esta logada (lazy login).
        Respeita _copy_enabled — NUNCA loga se copy trading estiver desligado.
        Retorna True se logado com sucesso, False caso contrario.
        """
        if not self._copy_enabled:
            return False
        if acc.get("api") and acc.get("auth"):
            return True
        try:
            auth = PumaBrokerAuth(acc["email"], acc["_password"])
            session = auth.login()
            api = TradesAPI(
                jwt_token=session.token,
                user_id=session.user_id,
                wallet="DEMO" if acc["is_demo"] else "REAL",
            )
            initial_balance = self._copy_fetch_balance(auth, acc["is_demo"])
            acc["auth"] = auth
            acc["api"] = api
            acc["initial_balance"] = initial_balance
            acc["started_at"] = datetime.now()
            logger.info("Copy account login (lazy): %s (%s)", acc["label"], acc["email"])
            return True
        except Exception as e:
            acc["last_error"] = str(e)[:200]
            logger.warning("Copy account login failed (lazy): %s -- %s", acc["label"], e)
            return False

    def _copy_fetch_balance(self, auth: PumaBrokerAuth | None, is_demo: bool) -> float:
        if auth is None:
            return 0.0
        try:
            auth.get_access_token()
            logger.info("[AUTH] Access Token obtido para conta copy %s", auth.email if hasattr(auth, 'email') else "?")
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

    def copy_add_account(self, label: str, email: str, password: str, is_demo: bool) -> dict:
        # Cria a conta SEM fazer login imediato -- login sera lazy quando copy ativado
        acc = {
            "id": f"copy_{int(time.time() * 1000)}_{len(self._copy_sessions)}",
            "label": label,
            "email": email,
            "is_demo": is_demo,
            "_password": password,
            "active": True,
            "api": None,
            "auth": None,
            "last_error": None,
            "last_sync_at": None,
            "initial_balance": 0,
            "total_trades": 0,
            "total_wins": 0,
            "total_losses": 0,
            "total_profit": 0.0,
            "started_at": datetime.now(),
        }
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
                        # Atualiza is_demo — reset do auth/api para forçar novo login lazy na próxima sync
                        acc["is_demo"] = new_is_demo
                        acc["auth"] = None
                        acc["api"] = None
                        logger.info(
                            "Copy account %s tipo alterado: %s -> %s (login lazy no próximo sync)",
                            acc["label"],
                            "DEMO" if not new_is_demo else "REAL",
                            "DEMO" if new_is_demo else "REAL",
                        )
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
        current_balance = self._copy_fetch_balance(acc.get("auth"), acc["is_demo"])
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
        self._copy_user_confirmed = self._copy_enabled  # usuário confirmou explicitamente via toggle
        self._save_sessions()
        logger.info("Copy trading toggled to %s (user_confirmed=%s)", self._copy_enabled, self._copy_user_confirmed)
        return self._copy_enabled

    def login(self, email: str, password: str) -> dict:
        # Use TokenManager for persistent token management
        self._token_manager = get_token_manager(email, password)
        self._auth = self._token_manager.auth  # Get synchronized PumaBrokerAuth
        
        try:
            session = self._auth.get_session()
        except AuthError as e:
            logger.error("Falha no login para %s: status=%d, erro=%s", email, e.status_code, e)
            raise

        self._user_id = session.user_id
        self._trades_api = TradesAPI(
            jwt_token=session.token,
            user_id=session.user_id,
            wallet="DEMO" if session.is_demo else "REAL",
        )

        # Extrai o server_name_session (WS2) — lê do TokenManager compartilhado
        # (singleton), pois o cookie pode ter sido capturado em outra instância
        # PumaBrokerAuth durante refresh/login.
        ws2_token = self._token_manager.ws2_token or self._auth.ws2_token

        if ws2_token:
            ws2_preview = ws2_token[:20] + "..." if len(ws2_token) > 20 else ws2_token
        else:
            ws2_preview = "N/D"

        logger.info(
            "Login OK: %s (id=%s) ws2_token=%s preview=%s",
            session.name, session.user_id,
            "SIM" if ws2_token else "NÃO",
            ws2_preview,
        )

        # Inicializa Recovery Manager (carrega trades persistidos + inicia monitoramento)
        self.trade_manager.api_client = self  # self atua como API client
        self.trade_manager.load_active_from_persistence()
        self.recovery_manager.api_client = self
        self.recovery_manager.start()

        # Inicia listener WebSocket de trades em background (atualiza _trade_history em tempo real)
        self._trade_ws.start(session.token, session.user_id)

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
            "ws2Session": ws2_token,
        }

    def _ensure_auth(self):
        if not self._token_manager:
            raise AuthError("Não autenticado. Faça login primeiro.")

    def _ensure_token(self):
        self._ensure_auth()
        # Backoff check: não força refresh se estamos em backoff
        now = time.time()
        if now < _login_backoff_until:
            logger.debug("[AUTH] _ensure_token: backoff ativo, usando token atual")
            return self._token_manager.tokens.access_token if self._token_manager.tokens.access_token else self._token_manager.get_access_token()
        fresh = self._token_manager.get_access_token()
        if self._trades_api and fresh != self._trades_api._jwt:
            self._trades_api.update_jwt(fresh)
        return fresh

    def _get_auth(self) -> PumaBrokerAuth:
        """Retorna a instância sincronizada do PumaBrokerAuth."""
        self._ensure_auth()
        return self._token_manager.auth

    def get_balance(self) -> dict:
        self._ensure_auth()
        self._ensure_token()
        if not self._user_id:
            raise AuthError("user_id não disponível. Faça login novamente.")
        # /api/v1/users/me retorna 403; o endpoint correto é /api/v1/users/{user_id}
        url = f"{config.BASE_URL}/api/v1/users/{self._user_id}"
        auth = self._token_manager.auth
        r = auth.http.get(url, timeout=config.HTTP_TIMEOUT)
        if r.status_code == 401:
            self._token_manager.force_refresh()
            r = auth.http.get(url, timeout=config.HTTP_TIMEOUT)
        r.raise_for_status()
        data = r.json()
        return {
            "balance": data.get("balance", 0),
            "demoBalance": data.get("demoBalance", 0),
        }

    def get_active(self) -> list:
        self._ensure_auth()
        self._ensure_token()
        auth = self._token_manager.auth
        r = auth.http.get(config.ACTIVE_URL, timeout=config.HTTP_TIMEOUT)
        if r.status_code == 401:
            self._token_manager.force_refresh()
            r = auth.http.get(config.ACTIVE_URL, timeout=config.HTTP_TIMEOUT)
        r.raise_for_status()
        return r.json()

    def place_trade(self, body: dict) -> dict:
        self._ensure_auth()
        if not self._trades_api:
            raise RuntimeError("TradesAPI não inicializada.")
        token = self._ensure_token()
        payout = body.get("payout", 0.85)
        timeframe = body.get("timeframe") or self._expiration_to_timeframe(body.get("expiration", 60))
        trace_id = body.get("traceId", "")
        signal_candle_time = body.get("signalCandleTime", 0)
        signal_candle_open = body.get("signalCandleOpen", 0)
        signal_candle_close = body.get("signalCandleClose", 0)
        candle_id = body.get("candleId", "")
        duration_received = body.get("durationReceived", body.get("expiration", 60))

        logger.info(
            "[TIMING] DAEMON_START traceId=%s asset=%s dir=%s duration=%s durationReceived=%s signalCandle=%s candleId=%s ts=%.3f",
            trace_id, body["asset"], body["direction"], body.get("expiration", 60),
            duration_received, signal_candle_time, candle_id, time.time()
        )

        # ── IDEMPOTÊNCIA: BLOQUEIA reenvio da MESMA ordem ──
        # Vetores de duplicata cobertos:
        #   1. Retry do cliente (mesmo traceId em <120s) — ex.: resposta perdida.
        #   2. Re-disparo do engine na mesma vela (<30s): mesmo ativo + direção +
        #      valor + entryPrice.
        # A duplicata NÃO é recolocada: aguarda brevemente o resultado da 1ª
        # tentativa e o retorna; sem resultado, devolve DUPLICATE_BLOCKED.
        now = time.time()
        dupe_key = (body["asset"], body["direction"], body["amount"])

        def _find_prev():
            for prev in self._recent_orders:
                same_trace = bool(trace_id) and prev.get("traceId") == trace_id and now - prev["time"] < 120
                same_sig = (
                    prev.get("key") == dupe_key
                    and now - prev["time"] < 30
                    and prev.get("entryPrice") == body.get("entryPrice")
                )
                if same_trace or same_sig:
                    return prev
            return None

        prev_dup = _find_prev()
        if prev_dup is not None:
            # Aguarda a 1ª tentativa "em voo" resolver (race entre requests)
            waited = 0.0
            while prev_dup.get("result") is None and waited < 8.0:
                time.sleep(0.1)
                waited += 0.1
            first_result = prev_dup.get("result")
            if first_result:
                logger.warning(
                    "═══ DUPLICATA BLOQUEADA ═══ asset=%s dir=%s amount=%s traceId=%s intervalo=%.1fs "
                    "— retornando ordem original %s (NÃO recolocada)",
                    body["asset"], body["direction"], body["amount"], trace_id,
                    now - prev_dup["time"], first_result.get("id", "")
                )
                return first_result
            logger.warning(
                "═══ DUPLICATA BLOQUEADA (1ª tentativa sem resultado) ═══ asset=%s traceId=%s "
                "— ordem NÃO reenviada",
                body["asset"], trace_id
            )
            return {
                "id": "",
                "status": "DUPLICATE_BLOCKED",
                "expiresAt": None,
                "duplicateOfTraceId": prev_dup.get("traceId", ""),
            }

        reserved = {
            "key": dupe_key,
            "traceId": trace_id,
            "entryPrice": body.get("entryPrice"),
            "time": now,
            "result": None,
        }
        self._recent_orders.append(reserved)

        _t0 = time.perf_counter()
        _t_start_send = None

        try:
            _t_start_send = time.perf_counter()
            logger.info(
                "[TIMING] DAEMON_BEFORE_PUMA traceId=%s asset=%s dir=%s ts=%.3f",
                trace_id, body["asset"], body["direction"], time.time()
            )
            result = self._trades_api.place_order(
                symbol=body["asset"],
                direction=body["direction"],
                amount=body["amount"],
                timeframe=timeframe,
                entry_price=body.get("entryPrice", 0),
                payout=payout,
                wallet="DEMO" if body.get("isDemo", True) else "REAL",
                trace_id=trace_id,
            )
            _t1 = time.perf_counter()
            puma_ms = round((_t1 - (_t_start_send or _t0)) * 1000)
            total_ms = round((_t1 - _t0) * 1000)
            logger.info(
                "[TIMING] DAEMON_AFTER_PUMA traceId=%s orderId=%s status=%s pumaMs=%d totalMs=%d expiresAt=%s ts=%.3f",
                trace_id, result.get("id", ""), result.get("status", ""), puma_ms, total_ms,
                result.get("expiresAt", "none"), time.time()
            )
            logger.info(f"[LATENCY] place_trade: proxy_routing={round(((_t_start_send or _t0) - _t0) * 1000)}ms puma_api={puma_ms}ms total={total_ms}ms")
        except OrderError as e:
            if e.status_code != 401:
                raise
            logger.warning("JWT expirado — renovando e retentando ordem...")
            self._token_manager.force_refresh()
            fresh = self._token_manager.get_access_token()
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
                trace_id=trace_id,
            )
            _t2 = time.perf_counter()
            logger.info(f"[LATENCY] place_trade RETRY: puma_api={round((_t2 - _t_retry) * 1000)}ms total={round((_t2 - _t0) * 1000)}ms")

        # ── ARMAZENA NO HISTÓRICO (para GET /trades list) ──
        with self._trade_history_lock:
            self._trade_history.append({
                "id": result.get("id"),
                "asset": body.get("asset"),
                "direction": body.get("direction"),
                "amount": body.get("amount"),
                "entryPrice": body.get("entryPrice", 0),
                "payout": payout,
                "status": result.get("status", "ACTIVE"),
                "profit": result.get("profit", 0),
                "result": result.get("result", "pending"),
                "openedAt": result.get("openedAt", result.get("createdAt", time.time())),
            })
            self._trade_history = self._trade_history[-200:]  # mantém só os 200 mais recentes

        # Libera requests duplicados aguardando em _find_prev() (idempotência)
        reserved["result"] = result

        # ── RECOVERY: Persiste trade ativa no TradeManager ──
        trade = self.trade_manager.create_from_order(result)
        logger.info("TradeManager: Nova trade %s criada e persistida", trade.id)

        # ── COPY TRADER: dispara em threads separadas (não bloqueia a trade principal) ──
        # PROTECAO: só executa se usuario confirmou explicitamente via toggle + flag ativa
        # A flag _copy_user_confirmed NUNCA persiste no arquivo, entao mesmo que o arquivo
        # .copy_sessions.json tenha "enabled": true, a copia nao roda apos restart.
        n_copies = 0
        asset = body.get("asset", "")
        direction = body.get("direction", "")
        timeframe = body.get("timeframe", "")
        if not self._copy_enabled:
            logger.debug("Copy trade desativado (_copy_enabled=False) — cópia ignorada para %s", asset)
        elif self._copy_enabled and not self._copy_user_confirmed:
            logger.warning(
                "COPY TRADE BLOQUEADO: _copy_enabled=True mas _copy_user_confirmed=False. "
                "Usuario precisa ativar copy via toggle explicito na interface."
            )
            PumaDaemon.push_copy_log({
                "type": "blocked", "asset": asset, "direction": direction,
                "timeframe": timeframe, "reason": "Copy desativado — toggle não confirmado",
            })
        elif self._copy_enabled and self._copy_user_confirmed:
            if not self._copy_sessions:
                logger.debug("Copy trade ativo mas sem contas cadastradas — nenhuma cópia para %s", asset)
            for acc in self._copy_sessions:
                if not acc["active"]:
                    logger.info("Copy trade SKIP: conta %s desativada — cópia ignorada", acc["label"])
                    PumaDaemon.push_copy_log({
                        "type": "skipped", "asset": asset, "direction": direction,
                        "timeframe": timeframe, "account": acc["label"],
                        "reason": "Conta desativada",
                    })
                    continue
                # Lazy login: se a conta não tem auth/api, cria agora
                if acc.get("auth") is None or acc.get("api") is None:
                    try:
                        login_ok = self._copy_account_lazy_login(acc)
                        if not login_ok:
                            logger.warning("Copy trade SKIP: lazy login retornou False para %s", acc["label"])
                            PumaDaemon.push_copy_log({
                                "type": "skipped", "asset": asset, "direction": direction,
                                "timeframe": timeframe, "account": acc["label"],
                                "reason": "Lazy login falhou",
                            })
                            continue
                    except Exception as e:
                        acc["last_error"] = str(e)[:200]
                        logger.warning("Copy trade lazy login FAIL: %s — %s", acc["label"], str(e)[:200])
                        PumaDaemon.push_copy_log({
                            "type": "error", "asset": asset, "direction": direction,
                            "timeframe": timeframe, "account": acc["label"],
                            "reason": str(e)[:200],
                        })
                        continue
                n_copies += 1
                logger.info(
                    "[TIMING] COPY_DISPATCH traceId=%s account=%s ts=%.3f",
                    trace_id, acc["label"], time.time()
                )
                t = threading.Thread(
                    target=self._executar_copy_em_thread,
                    args=(acc, body, payout),
                    daemon=True,
                )
                t.start()

        if n_copies > 0:
            logger.info(f"[LATENCY] copy_trades: {n_copies} conta(s) dispatchadas em threads paralelas")

        # Armazena a trade no histórico do servidor (mantém últimos 200)
        trade_entry = {
            "id": result.get("id", ""),
            "asset": body.get("asset", ""),
            "direction": body.get("direction", ""),
            "amount": body.get("amount", 0),
            "entryPrice": body.get("entryPrice", 0),
            "payout": payout,
            "status": result.get("status", "ACTIVE"),
            "profit": 0,  # será atualizado quando houver resultado final via WebSocket
            "result": "PENDING",
            "openedAt": int(time.time() * 1000),
        }
        with self._trade_history_lock:
            self._trade_history.insert(0, trade_entry)
            if len(self._trade_history) > 200:
                self._trade_history = self._trade_history[:200]

        return result

    def _executar_copy_em_thread(self, acc: dict, body: dict, payout: float) -> None:
        """Executa copy trade em thread separada (fire-and-forget).
        Falhas são logadas e registradas na conta, NUNCA propagam exceção."""
        trace_id = body.get("traceId", "")
        asset = body.get("asset", "")
        direction = body.get("direction", "")
        timeframe = body.get("timeframe", "")
        logger.info(
            "[TIMING] COPY_THREAD_START traceId=%s account=%s ts=%.3f",
            trace_id, acc["label"], time.time()
        )
        try:
            self._copy_place_trade(acc, body, payout)
            PumaDaemon.push_copy_log({
                "type": "success", "asset": asset, "direction": direction,
                "timeframe": timeframe, "account": acc["label"],
            })
        except OrderError as e:
            if e.status_code == 401:
                try:
                    acc["auth"].login()
                    fresh = acc["auth"].get_access_token()
                    acc["api"].update_jwt(fresh)
                    self._copy_place_trade(acc, body, payout)
                    PumaDaemon.push_copy_log({
                        "type": "success", "asset": asset, "direction": direction,
                        "timeframe": timeframe, "account": acc["label"],
                        "note": "retry_ok",
                    })
                except Exception as e2:
                    acc["last_error"] = str(e2)[:200]
                    logger.warning("Copy trade retry FAIL: %s — %s", acc["label"], str(e2)[:200])
                    PumaDaemon.push_copy_log({
                        "type": "error", "asset": asset, "direction": direction,
                        "timeframe": timeframe, "account": acc["label"],
                        "reason": str(e2)[:200],
                    })
            else:
                acc["last_error"] = str(e)[:200]
                logger.warning("Copy trade FAIL: %s — %s", acc["label"], str(e)[:200])
                PumaDaemon.push_copy_log({
                    "type": "error", "asset": asset, "direction": direction,
                    "timeframe": timeframe, "account": acc["label"],
                    "reason": str(e)[:200],
                })
        except Exception as e:
            acc["last_error"] = str(e)[:200]
            logger.warning("Copy trade FAIL: %s — %s", acc["label"], str(e)[:200])
            PumaDaemon.push_copy_log({
                "type": "error", "asset": asset, "direction": direction,
                "timeframe": timeframe, "account": acc["label"],
                "reason": str(e)[:200],
            })

    def list_trades(self, limit: int = 50) -> list[dict]:
        """Retorna histórico de trades usando TradeManager + _trade_history.
        
        Trades são resolvidos exclusivamente via eventos WebSocket tradeResult.
        Faz merge de ambas as fontes para garantir que nenhuma trade seja perdida.
        """
        # Trades do TradeManager (persistidos em SQLite)
        tm_trades = self.trade_manager.get_all()[:limit]
        tm_dict = {t.id: self._trade_to_dict(t) for t in tm_trades}
        
        # Merge com _trade_history (lista em memória populada por place_trade)
        with self._trade_history_lock:
            for entry in self._trade_history:
                tid = entry.get("id", "")
                if tid and tid not in tm_dict:
                    tm_dict[tid] = entry
        
        # Ordena por openedAt (mais recente primeiro) e limita
        def _sort_key(x):
            v = x.get("openedAt", 0)
            if isinstance(v, str):
                return v
            return str(v) if v else ""
        result = sorted(tm_dict.values(), key=_sort_key, reverse=True)[:limit]
        return result

    def _trade_to_dict(self, trade: ActiveTrade) -> dict:
        """Converte ActiveTrade para dict compatível com GET /trades"""
        return {
            "id": trade.id,
            "asset": trade.symbol,
            "direction": trade.direction,
            "amount": trade.amount,
            "entryPrice": trade.entry_price,
            "payout": trade.payout,
            "status": trade.status,
            "profit": trade.profit,
            "result": trade.result or trade.status.lower() if trade.status in ("WIN", "LOSS", "DRAW") else "pending",
            "newBalance": trade.new_balance,
            "openedAt": trade.opened_at,
            "expiresAt": trade.expires_at,
            "closedAt": trade.closed_at,
            "exitPrice": trade.exit_price,
        }

    def get_trades(self, limit: int = 50) -> list[dict]:
        """Wrapper para APIClient - usado pelo RecoveryManager"""
        return self.list_trades(limit)

    def get_trade(self, order_id: str) -> dict:
        """Retorna trade do TradeManager (persistido em SQLite).
        Trades são resolvidos via WebSocket tradeResult, não via REST API.
        Se não encontrar no TradeManager, busca em _trade_history como fallback."""
        trade = self.trade_manager.get(order_id)
        if trade:
            logger.info("ENDPOINT: /trades/%s found in TradeManager status=%s", order_id, trade.status)
            return self._trade_to_dict(trade)
        
        # Fallback: buscar em _trade_history (lista em memória populada por place_trade)
        with self._trade_history_lock:
            for entry in self._trade_history:
                if entry.get("id") == order_id:
                    logger.info("ENDPOINT: /trades/%s found in _trade_history status=%s (fallback)", order_id, entry.get("status", "ACTIVE"))
                    return entry
        
        logger.warning("ENDPOINT: /trades/%s NOT FOUND in TradeManager or _trade_history", order_id)
        return None

    def place_trade_for_recovery(self, body: dict) -> dict:
        """Wrapper para APIClient - usado pelo RecoveryManager"""
        return self.place_trade(body)

    def set_auth(self, token: str):
        """Define token de autenticação para APIClient"""
        pass  # O daemon gerencia auth internamente

    def subscribe_trades(self):
        """Re-subscreve no namespace /trades após reconexão"""
        if self._trade_ws and self._trade_ws._sio and self._trade_ws._sio.connected:
            try:
                self._trade_ws._sio.emit("subscribe", self._user_id, namespace="/trades")
                logger.info("Recovery: Re-subscrito no namespace /trades")
            except Exception as e:
                logger.error("Erro ao re-subscrever trades: %s", e)

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
        trace_id = body.get("traceId", "")
        duration_received = body.get("durationReceived", body.get("expiration", 60))
        logger.info("[COPY] Validando token para conta %s...", acc["label"])
        fresh = acc["auth"].get_access_token()
        if fresh != acc["api"]._jwt:
            acc["api"].update_jwt(fresh)
        logger.info("[COPY] Token OK — Account=%s", acc["label"])
        t0 = time.perf_counter()
        logger.info(
            "[TIMING] COPY_BEFORE_PUMA traceId=%s account=%s ts=%.3f",
            trace_id, acc["label"], time.time()
        )
        order_result = acc["api"].place_order(
            symbol=body["asset"],
            direction=body["direction"],
            amount=body["amount"],
            timeframe=body.get("timeframe", self._expiration_to_timeframe(body.get("expiration", 60))),
            entry_price=body.get("entryPrice", 0),
            payout=payout,
            wallet="DEMO" if acc["is_demo"] else "REAL",
        )
        t1 = time.perf_counter()
        copy_ms = round((t1 - t0) * 1000)
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
        logger.info(
            "[TIMING] COPY_AFTER_PUMA traceId=%s account=%s orderId=%s status=%s copyMs=%d ts=%.3f",
            trace_id, acc["label"], order_result.get("id", ""), order_status, copy_ms, time.time()
        )
        logger.info("Copy trade OK: %s (total trades: %d)", acc["label"], acc["total_trades"])
        return order_result

    def copy_test_account(self, account_id: str) -> dict:
        """Testa conexão de uma conta copy (login + fetch saldo)."""
        for acc in self._copy_sessions:
            if acc["id"] == account_id:
                try:
                    auth = PumaBrokerAuth(acc["email"], acc["_password"])
                    session = auth.login()
                    balance = self._copy_fetch_balance(auth, acc["is_demo"])
                    acc["auth"] = auth
                    acc["api"] = TradesAPI(
                        jwt_token=session.token,
                        user_id=session.user_id,
                        wallet="DEMO" if acc["is_demo"] else "REAL",
                    )
                    acc["last_error"] = None
                    acc["initial_balance"] = balance
                    acc["started_at"] = datetime.now()
                    logger.info("Copy test OK: %s — balance=%.2f", acc["label"], balance)
                    return {"success": True, "balance": balance, "user_id": session.user_id}
                except Exception as e:
                    acc["last_error"] = str(e)[:200]
                    logger.warning("Copy test FAIL: %s — %s", acc["label"], e)
                    return {"success": False, "error": str(e)[:200]}
        return {"success": False, "error": "Conta não encontrada"}

    def get_ws2_session(self, force_refresh: bool = False) -> str:
        """Retorna o JWT accessToken como token WS2.

        Se force_refresh=True ou o token não estiver disponível, faz re-login
        para obter um token fresco. Inclui circuit breaker para evitar loops
        infinitos de re-login — após MAX_RELOGIN_ATTEMPTS falhas consecutivas,
        exige login manual pelo frontend.

        Respeita backoff de HTTP 429.
        """
        import time
        from datetime import datetime
        self._ensure_auth()

        # ── Backoff check: se estamos em backoff, não re-login ──
        now = time.time()
        if now < _login_backoff_until:
            remaining = int(_login_backoff_until - now)
            logger.warning(
                "[AUTH] WS2 RE-LOGIN SKIPPED — backoff ativo, aguardar %ds",
                remaining,
            )
            # Retorna token atual se disponível
            auth = self._get_auth()
            if auth.ws2_token:
                return auth.ws2_token
            raise AuthError(
                f"WS2 re-login bloqueado por backoff — aguarde {remaining}s",
                status_code=429,
            )

        # Diagnóstico detalhado do que disparou a necessidade de re-login
        auth = self._get_auth()
        token_presente = bool(auth.ws2_token)
        token_preview = (auth.ws2_token[:20] + "...") if token_presente else "VAZIO"
        trigger = "force_refresh=True (frontend)" if force_refresh else "ws2_token ausente/expirado"
        timestamp_iso = datetime.now().isoformat(timespec='milliseconds')

        logger.info(
            "═══ GET_WS2_SESSION ═══ ts=%s | force_refresh=%s | token_presente=%s | token_preview=%s | trigger=%s",
            timestamp_iso, force_refresh, token_presente, token_preview, trigger
        )

        precisa_relogin = force_refresh or not auth.ws2_token

        # Circuit breaker: se excedeu tentativas, bloqueia auto-recuperação
        if precisa_relogin and PumaDaemon._relogin_failures >= PumaDaemon.MAX_RELOGIN_ATTEMPTS:
            logger.warning(
                "WS2 re-login BLOQUEADO por circuit breaker: %d falhas consecutivas (MAX=%d). Exige login manual.",
                PumaDaemon._relogin_failures,
                PumaDaemon.MAX_RELOGIN_ATTEMPTS,
            )
            raise AuthError(
                f"Re-login automático falhou {PumaDaemon._relogin_failures}x consecutivas. "
                "Faça login manualmente pelo frontend (POST /login)."
            )

        if precisa_relogin:
            # Reentrancy guard: apenas 1 thread faz re-login por vez
            if PumaDaemon._relogging_in:
                logger.info(
                    "═══ WS2 RE-LOGIN JÁ EM ANDAMENTO (outra thread) — aguardando ═══ ts=%s",
                    timestamp_iso
                )
                with PumaDaemon._relogin_lock:
                    pass  # espera a thread que está logando terminar
            else:
                with PumaDaemon._relogin_lock:
                    if PumaDaemon._relogging_in:
                        # Outra thread já logou enquanto esperávamos o lock
                        logger.info("═══ WS2 RE-LOGIN já feito por outra thread ═══ ts=%s", timestamp_iso)
                    else:
                        PumaDaemon._relogging_in = True
                        try:
                            logger.info("═══ WS2 RE-LOGIN INICIADO ═══ ts=%s | trigger=%s | tentativas_anteriores=%d/%d",
                                       timestamp_iso, trigger, PumaDaemon._relogin_failures, PumaDaemon.MAX_RELOGIN_ATTEMPTS)
                            login_start = time.perf_counter()
                            # Use TokenManager to force refresh
                            self._token_manager.force_refresh()
                            self._ensure_token()
                            login_elapsed = round((time.perf_counter() - login_start) * 1000)
                            PumaDaemon._relogin_failures = 0  # reset no sucesso
                            # Refresh auth reference
                            auth = self._get_auth()
                            token_novo = auth.ws2_token
                            token_novo_preview = (token_novo[:20] + "...") if token_novo else "VAZIO"
                            logger.info("═══ WS2 RE-LOGIN SUCESSO ═══ duracao_ms=%d | novo_token=%s",
                                       login_elapsed, token_novo_preview)
                        except AuthError as e:
                            login_elapsed = round((time.perf_counter() - login_start) * 1000)
                            PumaDaemon._relogin_failures += 1
                            error_detail = str(e)
                            if hasattr(e, 'response_body') and e.response_body:
                                error_detail += f" | response_body={e.response_body}"
                            logger.warning(
                                "═══ WS2 RE-LOGIN FALHOU ═══ ts=%s | duracao_ms=%d | tentativa=%d/%d | erro=%s",
                                datetime.now().isoformat(timespec='milliseconds'),
                                login_elapsed,
                                PumaDaemon._relogin_failures,
                                PumaDaemon.MAX_RELOGIN_ATTEMPTS,
                                error_detail,
                            )
                            raise
                        finally:
                            PumaDaemon._relogging_in = False

        token = auth.ws2_token
        if not token:
            PumaDaemon._relogin_failures += 1
            logger.error(
                "WS2: accessToken AUSENTE mesmo após re-login bem-sucedido | tentativas=%d",
                PumaDaemon._relogin_failures,
            )
            raise AuthError(
                "WS2 token (accessToken) não disponível mesmo após re-login"
            )

        logger.info("WS2 token retornado (len=%d, preview=%s...)", len(token), token[:20])
        return token

    def get_history(self, symbol: str, resolution: str, from_ts: str, to_ts: str) -> dict:
        """Proxy para GET /api/v1/tradingview/history da Puma (candles históricos)."""
        self._ensure_auth()
        token = self._ensure_token()
        params = {"symbol": symbol, "resolution": resolution, "from": from_ts, "to": to_ts}
        url = f"{config.BASE_URL}/api/v1/tradingview/history"
        auth = self._token_manager.auth
        r = auth.http.get(url, params=params, timeout=config.HTTP_TIMEOUT)
        if r.status_code == 401:
            self._token_manager.force_refresh()
            r = auth.http.get(url, params=params, timeout=config.HTTP_TIMEOUT)
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
            if path == "/trades":
                with daemon._trade_history_lock:
                    daemon._trade_history.clear()
                logger.info("Trade history cleared via DELETE /trades")
                self._send(200, {"success": True, "cleared": True})
            elif path.startswith("/copy/accounts/"):
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
                t0 = time.perf_counter()
                logger.info("[AUDIT] [DAEMON_POST_TRADES_START] asset=%s direction=%s perf=%s", body.get("asset"), body.get("direction"), t0)
                result = daemon.place_trade(body)
                t1 = time.perf_counter()
                trade_id = result.get("id", "")
                logger.info("[AUDIT] [DAEMON_POST_TRADES_END] asset=%s orderId=%s perf=%s duration_ms=%s", body.get("asset"), trade_id, t1, round((t1 - t0) * 1000, 2))
                logger.info(
                    "🔍 TRADE CRIADA | asset=%s dir=%s | id=%s | result_full=%s",
                    body.get("asset"), body.get("direction"),
                    trade_id,
                    json.dumps(result, ensure_ascii=False, default=str),
                )
                self._send(200, {
                    "id": trade_id,
                    "status": result.get("status", "ACTIVE"),
                    "expiresAt": result.get("expiresAt"),
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

            elif path == "/copy/accounts/test":
                body = self._read_body()
                account_id = body.get("account_id", "")
                if not account_id:
                    self._send(400, {"error": "account_id é obrigatório"})
                    return
                result = daemon.copy_test_account(account_id)
                if result["success"]:
                    self._send(200, result)
                else:
                    self._send(400, result)

            elif path == "/logs":
                body = self._read_body()
                if isinstance(body, list):
                    for entry in body:
                        PumaDaemon.push_log(entry)
                elif isinstance(body, dict):
                    PumaDaemon.push_log(body)
                self._send(200, {"ok": True})

            elif path == "/recovery/reconcile":
                # Força reconciliação imediata
                self._daemon.recovery_manager.force_reconcile()
                self._send(200, {"status": "reconciliation_started"})

            elif path == "/recovery/status":
                # Status do recovery manager
                status = self._daemon.recovery_manager.get_status()
                self._send(200, status)

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
            import traceback
            traceback.print_exc()
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

            elif path == "/trades":
                result = daemon.list_trades(limit=50)
                self._send(200, {"trades": result})

            elif path.startswith("/trades/"):
                order_id = path.split("/")[-1]
                result = daemon.get_trade(order_id)
                if result is None:
                    logger.warning(
                        "ENDPOINT_ERROR: /trades/%s retornou 404 - tradeId=%s endpoint=%s time_opened=",
                        order_id, order_id, f"/trades/{order_id}", daemon.trade_manager._active_trades.get(order_id).opened_at if daemon.trade_manager._active_trades.get(order_id) else "unknown"
                    );
                    self._send(404, {"error": "Trade não encontrada"})
                else:
                    logger.info("ENDPOINT: /trades/%s status=200 tradeId=%s result.status=%s", order_id, order_id, result.get("status", "ACTIVE"))
                    self._send(200, result)

            elif path == "/health":
                token_valid = daemon._token_manager.has_valid_access_token() if daemon._token_manager else False
                broker_connected = (
                    daemon._auth is not None
                    and daemon._token_manager is not None
                    and token_valid
                )
                ws_connected = daemon._trade_ws.trades_connected if hasattr(daemon, '_trade_ws') else False
                self._send(200, {
                    "status": "ok",
                    "broker_connected": broker_connected,
                    "ws_connected": ws_connected,
                    "copy_enabled": daemon._copy_enabled,
                    "copy_user_confirmed": daemon._copy_user_confirmed,
                    "copy_accounts": len(daemon._copy_sessions),
                    "copy_active_accounts": sum(1 for a in daemon._copy_sessions if a["active"]),
                })

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

            elif path == "/copy/logs":
                query = parse_qs(urlparse(self.path).query)
                limit = int(query.get("limit", ["50"])[0])
                result = PumaDaemon.get_copy_logs(limit=limit)
                self._send(200, {"logs": result})

            elif path == "/logs":
                query = parse_qs(urlparse(self.path).query)
                limit = int(query.get("limit", ["200"])[0])
                level = query.get("level", [""])[0]
                result = PumaDaemon.get_logs(limit=limit, level=level)
                self._send(200, result)

            # ── RECOVERY ENDPOINTS ─────────────────────────────────────
            elif path == "/recovery/status":
                result = daemon.recovery_manager.get_status()
                self._send(200, result)

            elif path == "/recovery/reconcile":
                daemon.recovery_manager.force_reconcile()
                self._send(200, {"status": "reconciliation_started"})

            elif path == "/recovery/trades":
                active = daemon.trade_manager.get_active()
                self._send(200, {"trades": [t.to_dict() for t in active]})

            elif path == "/recovery/persisted":
                all_trades = daemon.persistence.get_all()
                self._send(200, {"trades": [t.to_dict() for t in all_trades]})

            elif path == "/recovery/clear":
                count = daemon.persistence.clear_all()
                self._send(200, {"cleared": count})

            else:
                self._send(404, {"error": f"Rota não encontrada: GET {path}"})

        except AuthError as e:
            self._send(401, {"error": str(e)})
        except OrderError as e:
            logger.warning("Erro de ordem em GET %s: %s", path, e)
            status = e.status_code if e.status_code in (400, 404, 422, 429) else 400
            self._send(status, {"error": str(e)})
        except Exception as e:
            logger.error("Erro em GET %s: %s", path, e)
            import traceback
            traceback.print_exc()
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
