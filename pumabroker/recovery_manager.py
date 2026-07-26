"""
Recovery Manager for Puma Broker Daemon
=======================================

Ensures zero trade loss across daemon restarts, crashes, network failures.
Persists active trades locally and reconciles with server on startup/reconnect.

All timestamps stored in UTC. Local conversion only at UI layer.
"""

import json
import os
import sqlite3
import threading
import time
import logging
from dataclasses import dataclass, asdict, field
from datetime import datetime, timezone
from typing import Optional, List, Dict, Any, Callable
from pathlib import Path
from enum import Enum

from pumabroker.auth import AuthError

logger = logging.getLogger("recovery_manager")


class TradeStatus(Enum):
    """Trade status enum - always stored in UTC"""
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
    """
    id: str
    symbol: str
    direction: str  # "CALL" or "PUT"
    amount: float
    entry_price: float
    payout: float
    status: str  # TradeStatus value
    profit: float = 0.0
    opened_at: str = ""  # UTC ISO format
    expires_at: str = ""  # UTC ISO format
    closed_at: str = ""  # UTC ISO format
    exit_price: float = 0.0
    wallet: str = "REAL"  # "REAL" or "DEMO"
    timeframe: str = "M1"
    verify_token: str = ""
    created_at: str = ""  # When we first learned about this trade
    updated_at: str = ""  # Last update timestamp
    result: str = ""
    new_balance: float = 0.0
    trade_status: str = ""
    gross_profit: float = 0.0
    net_profit: float = 0.0
    trade_mode: str = ""
    duration: int = 0

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
        return cls(**data)

    def is_expired(self, grace_seconds: int = 15) -> bool:
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
        return self.status.upper() in (TradeStatus.PENDING.value, TradeStatus.ACTIVE.value)

    def is_final(self) -> bool:
        return self.status.upper() in (TradeStatus.WIN.value, TradeStatus.LOSS.value, TradeStatus.DRAW.value)


class PersistenceManager:
    """
    Handles local persistence of active trades using SQLite.
    Thread-safe with connection pooling.
    """

    DB_FILE = "active_trades.db"
    SCHEMA_VERSION = 1

    def __init__(self, db_path: Optional[str] = None):
        self.db_path = db_path or self.DB_FILE
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
                # Migration: add new columns if they don't exist
                existing_cols = {
                    row[1].lower()
                    for row in conn.execute("PRAGMA table_info(active_trades)").fetchall()
                }
                logger.info("[DB MIGRATION] active_trades columns: %s", ", ".join(sorted(existing_cols)))
                migrations = [
                    ("result", "TEXT", "''"),
                    ("new_balance", "REAL", "0.0"),
                    ("trade_status", "TEXT", "''"),
                    ("gross_profit", "REAL", "0.0"),
                    ("net_profit", "REAL", "0.0"),
                    ("trade_mode", "TEXT", "''"),
                    ("duration", "INTEGER", "0"),
                ]
                added = []
                for col, coltype, default in migrations:
                    if col not in existing_cols:
                        try:
                            conn.execute(f"ALTER TABLE active_trades ADD COLUMN {col} {coltype} DEFAULT {default}")
                            added.append(col)
                            logger.info("[DB MIGRATION] active_trades + %s", col)
                        except sqlite3.OperationalError as e:
                            logger.warning("[DB MIGRATION] Failed to add column %s: %s", col, e)
                conn.commit()
                if added:
                    logger.info("[DB MIGRATION] active_trades columns added: %s. Migration OK.", ", ".join(added))
                else:
                    logger.info("[DB MIGRATION] active_trades schema already up to date. No migration needed.")
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
            conn = self._get_conn()
            try:
                existing = conn.execute("SELECT id FROM active_trades WHERE id = ?", (trade.id,)).fetchone()
                trade.updated_at = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
                data = trade.to_dict()
                
                # Default missing values if using older ActiveTrade format
                data.setdefault("result", "")
                data.setdefault("new_balance", 0.0)
                data.setdefault("trade_status", data.get("status", ""))
                data.setdefault("gross_profit", 0.0)
                data.setdefault("net_profit", 0.0)
                data.setdefault("trade_mode", "")
                data.setdefault("duration", 0)
                
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
                        data["trade_status"], data["gross_profit"], data["net_profit"],
                        data["trade_mode"], data["duration"],
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
                        data["result"], data["new_balance"], data["trade_status"],
                        data["gross_profit"], data["net_profit"], data["trade_mode"], data["duration"]
                    ))
                    conn.commit()
                    return True
            finally:
                conn.close()

    def delete(self, trade_id: str) -> bool:
        with self._lock:
            conn = self._get_conn()
            try:
                cursor = conn.execute("DELETE FROM active_trades WHERE id = ?", (trade_id,))
                conn.commit()
                return cursor.rowcount > 0
            finally:
                conn.close()

    def get(self, trade_id: str) -> Optional[ActiveTrade]:
        with self._lock:
            conn = self._get_conn()
            try:
                row = conn.execute("SELECT * FROM active_trades WHERE id = ?", (trade_id,)).fetchone()
                if row:
                    return ActiveTrade(**dict(row))
                return None
            finally:
                conn.close()

    def get_all(self) -> List[ActiveTrade]:
        with self._lock:
            conn = self._get_conn()
            try:
                rows = conn.execute("SELECT * FROM active_trades ORDER BY created_at DESC").fetchall()
                return [ActiveTrade(**dict(row)) for row in rows]
            finally:
                conn.close()

    def get_active(self) -> List[ActiveTrade]:
        with self._lock:
            conn = self._get_conn()
            try:
                rows = conn.execute(
                    "SELECT * FROM active_trades WHERE status IN (?, ?) ORDER BY created_at DESC",
                    (TradeStatus.PENDING.value, TradeStatus.ACTIVE.value)
                ).fetchall()
                return [ActiveTrade(**dict(row)) for row in rows]
            finally:
                conn.close()

    def get_expired_active(self, grace_seconds: int = 15) -> List[ActiveTrade]:
        """Get trades that are ACTIVE/PENDING but past their expiry + grace period"""
        with self._lock:
            conn = self._get_conn()
            try:
                rows = conn.execute(
                    "SELECT * FROM active_trades WHERE status IN (?, ?)",
                    (TradeStatus.PENDING.value, TradeStatus.ACTIVE.value)
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
            conn = self._get_conn()
            try:
                row = conn.execute(
                    "SELECT COUNT(*) FROM active_trades WHERE status IN (?, ?)",
                    (TradeStatus.PENDING.value, TradeStatus.ACTIVE.value)
                ).fetchone()
                return row[0] if row else 0
            finally:
                conn.close()

    def clear_all(self) -> int:
        with self._lock:
            conn = self._get_conn()
            try:
                cursor = conn.execute("DELETE FROM active_trades")
                conn.commit()
                return cursor.rowcount
            finally:
                conn.close()

    def close(self):
        pass  # SQLite connections are per-operation


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

    def create_from_order(self, order_data: dict) -> ActiveTrade:
        """Create ActiveTrade from order placement response"""
        now = datetime.now(timezone.utc)
        expires_at = now.timestamp() + 60  # Default M1 = 60 seconds
        if "duration" in order_data:
            expires_at = now.timestamp() + order_data["duration"]
        elif "expiresAt" in order_data:
            expires_at = datetime.fromisoformat(order_data["expiresAt"].replace("Z", "+00:00")).timestamp()

        trade = ActiveTrade(
            id=str(order_data.get("id", order_data.get("tradeId", ""))),
            symbol=order_data.get("symbol", order_data.get("asset", "")),
            direction=order_data.get("direction", ""),
            amount=float(order_data.get("amount", 0)),
            entry_price=float(order_data.get("entryPrice", order_data.get("entry_price", 0))),
            payout=float(order_data.get("payout", 0)),
            status=TradeStatus.ACTIVE.value,
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
            logger.info(
                "[TIMING] RECOVERY_CREATE_TRADE tradeId=%s asset=%s dir=%s status=%s ts=%.3f",
                trade.id, trade.symbol, trade.direction, trade.status, time.time()
            )
            self._notify(trade)
        return trade

    def update_from_result(self, trade_result: dict) -> Optional[ActiveTrade]:
        """Update trade from tradeResult event"""
        trade_data = trade_result.get("trade", {})
        trade_id = str(trade_data.get("id", trade_result.get("id", "")))
        result = trade_result.get("result", "").upper()
        profit = float(trade_data.get("profit", trade_result.get("profit", 0)))

        status = TradeStatus.ACTIVE.value
        if result in ("WON", "WIN"):
            status = TradeStatus.WIN.value
        elif result in ("LOST", "LOSS"):
            status = TradeStatus.LOSS.value
        elif result == "DRAW":
            status = TradeStatus.DRAW.value

        with self._lock:
            trade = self._active_trades.get(trade_id)
            if not trade:
                # Trade not in memory - create from result
                trade = ActiveTrade(
                    id=trade_id,
                    symbol=trade_data.get("symbol", trade_data.get("currency", "")),
                    direction=trade_data.get("direction", ""),
                    amount=float(trade_data.get("amount", 0)),
                    entry_price=float(trade_data.get("entryPrice", trade_data.get("entry_price", 0))),
                    payout=float(trade_data.get("payout", 0)),
                    status=status,
                    profit=profit,
                    opened_at=trade_data.get("openedAt", ""),
                    expires_at=trade_data.get("expiresAt", ""),
                    closed_at=trade_data.get("closedAt", datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")),
                    exit_price=float(trade_data.get("exitPrice", trade_data.get("exit_price", 0))),
                    wallet=trade_data.get("wallet", "REAL"),
                    timeframe=trade_data.get("timeframe", "M1"),
                )
                self._active_trades[trade.id] = trade
            else:
                trade.status = status
                trade.profit = profit
                trade.result = result
                trade.closed_at = trade_data.get("closedAt", datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"))
                trade.exit_price = float(trade_data.get("exitPrice", trade_data.get("exit_price", trade.exit_price)))
                trade.updated_at = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

            self.persistence.save(trade)
            logger.info(
                "[TIMING] RECOVERY_UPDATE_TRADE tradeId=%s status=%s profit=%.2f ts=%.3f",
                trade.id, status, profit, time.time()
            )
            self._notify(trade)

            if trade.is_final():
                logger.info("TradeManager: Trade %s finalized as %s", trade.id, status)

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
            status = TradeStatus.WIN.value
        elif result in ("LOST", "LOSS"):
            status = TradeStatus.LOSS.value
        elif result == "DRAW":
            status = TradeStatus.DRAW.value

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
                trade.result = result
                if status in (TradeStatus.WIN.value, TradeStatus.LOSS.value, TradeStatus.DRAW.value):
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
    Orchestrates trade recovery on startup, reconnect, and periodic reconciliation.
    """

    RECONCILE_INTERVAL = 15  # seconds
    EXPIRY_GRACE_SECONDS = 15
    API_BACKOFF_INITIAL = 30  # seconds after first 5xx
    API_BACKOFF_MAX = 300     # max 5 minutes

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
        self._last_reconcile = 0
        self._last_socket_reconnect = 0
        self._socket_connected = False
        self._initial_reconcile_done = False
        self._api_backoff_until = 0  # timestamp until which API calls are skipped
        self._api_backoff_seconds = 0  # current backoff duration
        self._api_error_count = 0

    def start(self):
        """Start recovery manager background tasks"""
        with self._lock:
            if self._running:
                return
            self._running = True
            self._stop_event.clear()
            self._thread = threading.Thread(target=self._run_loop, daemon=True, name="RecoveryManager")
            self._thread.start()
            logger.info("RecoveryManager: Started")

    def stop(self):
        """Stop recovery manager"""
        with self._lock:
            if not self._running:
                return
            self._running = False
            self._stop_event.set()
            if self._thread:
                self._thread.join(timeout=5)
            logger.info("RecoveryManager: Stopped")

    def on_socket_connect(self):
        """Called when Socket.IO connects/reconnects"""
        with self._lock:
            logger.info("RecoveryManager: Socket connected - triggering recovery")
            self._socket_connected = True
            self._socket_reconnect()

    def on_socket_disconnect(self):
        with self._lock:
            self._socket_connected = False

    def _socket_reconnect(self):
        """Handle socket reconnection - re-subscribe and recover"""
        logger.info("RecoveryManager: Socket reconnected - recovering active trades")
        try:
            # Re-subscribe to trades namespace
            if self.socket_manager and hasattr(self.socket_manager, 'subscribe_trades'):
                self.socket_manager.subscribe_trades()
        except Exception as e:
            logger.error("RecoveryManager: Error re-subscribing after reconnect: %s", e)

        # Immediate reconciliation on reconnect (bypass initial delay)
        self._last_reconcile = 0
        self._initial_reconcile_done = False
        self._reconcile_with_server()

    def _run_loop(self):
        """Main recovery loop"""
        while not self._stop_event.is_set():
            try:
                now = time.time()

                # Initial reconciliation - wait for first interval to allow auth to settle
                if not self._initial_reconcile_done:
                    if now - self._last_reconcile >= self.RECONCILE_INTERVAL:
                        self._reconcile_with_server()
                        self._last_reconcile = now
                        self._initial_reconcile_done = True
                else:
                    # Periodic reconciliation
                    if now - self._last_reconcile >= self.RECONCILE_INTERVAL:
                        self._reconcile_with_server()
                        self._last_reconcile = now

                # Check for expired trades
                self._check_expired_trades()

                # Check socket health
                self._check_socket_health()

            except Exception as e:
                logger.error("RecoveryManager loop error: %s", e)

            # Sleep in small chunks to allow clean shutdown
            for _ in range(10):
                if self._stop_event.is_set():
                    break
                time.sleep(1)

    def _reconcile_with_server(self):
        """Fetch server trades and reconcile with local state"""
        now = time.time()
        if now < self._api_backoff_until:
            return  # API instability — skip reconciliation

        logger.info("RecoveryManager: Starting reconciliation with server")
        try:
            # Get all trades from server
            server_trades = self.api_client.get_trades(limit=100)
            if not server_trades:
                logger.warning("RecoveryManager: No trades returned from server (empty list)")
                return

            # Build server index
            server_by_id = {str(t.get("id", "")): t for t in server_trades if t.get("id")}

            # Get local active trades
            local_active = self.trade_manager.get_active()

            # Check each local active trade against server
            for local_trade in local_active:
                server_trade = server_by_id.get(local_trade.id)
                if server_trade:
                    self._reconcile_trade(local_trade, server_trade)
                else:
                    # Trade exists locally but not on server - may be expired or cancelled
                    logger.warning("RecoveryManager: Trade %s not found on server - marking for check", local_trade.id)
                    # Could be between poll and result - wait for next cycle or tradeResult

            # Check for trades on server not in local
            for server_id, server_trade in server_by_id.items():
                local = self.trade_manager.get(server_id)
                if not local:
                    logger.info("RecoveryManager: Found server trade %s not in local - syncing", server_id)
                    self.trade_manager.update_from_poll(server_trade)

            # Check for expired active trades
            self._check_expired_trades()

            # Success — reset error count
            self._api_error_count = 0

        except AuthError as e:
            logger.error("RecoveryManager: Auth failed during reconciliation - status=%s body=%s", e.status_code, e.response_body)
            # Don't treat auth failure as empty trades - let it propagate
            raise
        except Exception as e:
            logger.error("RecoveryManager: Reconciliation error: %s", e)
            self._api_error_count += 1
            if self._api_error_count >= 2:
                self._apply_api_backoff()

    def _reconcile_trade(self, local: ActiveTrade, server: dict):
        """Reconcile a single local trade with server data"""
        server_status = str(server.get("status", "")).upper()
        server_result = str(server.get("result", "")).upper()
        server_profit = float(server.get("profit", 0))

        # Determine final status
        if server_result in ("WON", "WIN"):
            server_final_status = TradeStatus.WIN.value
        elif server_result in ("LOST", "LOSS"):
            server_final_status = TradeStatus.LOSS.value
        elif server_result == "DRAW":
            server_final_status = TradeStatus.DRAW.value
        else:
            server_final_status = server_status

        # If status changed, update local
        if local.status != server_final_status:
            logger.info("RecoveryManager: Reconciling trade %s: local=%s server=%s", 
                       local.id, local.status, server_final_status)
            local.status = server_final_status
            local.profit = server_profit
            local.exit_price = float(server.get("exitPrice", server.get("exit_price", local.exit_price)))
            local.closed_at = server.get("closedAt", server.get("closed_at", datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")))
            local.updated_at = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
            self.trade_manager.persistence.save(local)
            self.trade_manager._notify(local)
            logger.info("RecoveryManager: Trade %s reconciled to %s profit=%.2f", 
                       local.id, local.status, local.profit)

    def _check_expired_trades(self):
        """Check for trades that expired but never got result.
        Uses batch list_trades() instead of individual get_trade() calls
        to reduce API pressure. Respects backoff on 5xx errors."""
        now = time.time()
        if now < self._api_backoff_until:
            return  # API instability — skip individual checks

        expired = self.trade_manager.get_expired_active(self.EXPIRY_GRACE_SECONDS)
        if not expired:
            return

        logger.info("RecoveryManager: %d trades expired — attempting batch sync", len(expired))
        try:
            # Use batch list_trades instead of individual get_trade calls
            server_trades = self.api_client.get_trades(limit=100)
            if server_trades:
                server_by_id = {str(t.get("id", "")): t for t in server_trades if t.get("id")}
                for trade in expired:
                    server_trade = server_by_id.get(trade.id)
                    if server_trade:
                        self.trade_manager.update_from_poll(server_trade)
                    else:
                        logger.warning("RecoveryManager: Expired trade %s not found on server list", trade.id)
                self._api_error_count = 0
            else:
                self._api_error_count = getattr(self, "_api_error_count", 0) + 1
                if self._api_error_count >= 2:
                    self._apply_api_backoff()
        except Exception as e:
            logger.error("RecoveryManager: Error batch-syncing expired trades: %s", e)
            self._api_error_count = getattr(self, "_api_error_count", 0) + 1
            if self._api_error_count >= 2:
                self._apply_api_backoff()

    def _apply_api_backoff(self):
        """Apply exponential backoff after consecutive API failures"""
        self._api_backoff_seconds = min(
            self.API_BACKOFF_INITIAL * (2 ** (self._api_error_count - 2)),
            self.API_BACKOFF_MAX,
        )
        self._api_backoff_until = time.time() + self._api_backoff_seconds
        logger.warning(
            "RecoveryManager: API instability detected — backing off for %ds (errors=%d)",
            self._api_backoff_seconds, self._api_error_count,
        )

    def _check_socket_health(self):
        """Monitor socket connection health"""
        if self.socket_manager:
            try:
                is_connected = getattr(self.socket_manager, 'trades_connected', False)
                if is_connected != self._socket_connected:
                    if is_connected:
                        self.on_socket_connect()
                    else:
                        self.on_socket_disconnect()
            except Exception:
                pass

    def force_reconcile(self):
        """Force immediate reconciliation"""
        logger.info("RecoveryManager: Force reconcile requested")
        self._reconcile_with_server()

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
                "last_reconcile": self._last_reconcile,
                "last_socket_reconnect": self._last_socket_reconnect,
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


def create_recovery_system(
    api_base_url: str,
    auth_token: str,
    db_path: str = "active_trades.db"
) -> tuple[PersistenceManager, TradeManager, RecoveryManager, APIClient]:
    """
    Factory function to create the complete recovery system.
    Returns (persistence, trade_manager, recovery_manager, api_client)
    """
    persistence = PersistenceManager(db_path)
    api_client = APIClient(api_base_url, auth_token)
    trade_manager = TradeManager(persistence, api_client)
    recovery_manager = RecoveryManager(trade_manager, api_client, persistence=persistence)

    # Load existing trades on startup
    trade_manager.load_active_from_persistence()

    return persistence, trade_manager, recovery_manager, api_client


# ============================================================
# INTEGRATION EXAMPLE FOR PROXY_DAEMON.PY
# ============================================================
"""
# In proxy_daemon.py, add these imports:
from pumabroker.recovery_manager import (
    PersistenceManager, TradeManager, RecoveryManager, APIClient,
    ActiveTrade, TradeStatus, create_recovery_system
)

# In PumaDaemon.__init__, add:
self.persistence = PersistenceManager()
self.api_client = APIClient(config.BASE_URL)
self.trade_manager = TradeManager(self.persistence, self.api_client)
self.recovery_manager = RecoveryManager(
    self.trade_manager, 
    self.api_client,
    socket_manager=self._trade_ws,  # your existing _TradeWSListener
    persistence=self.persistence
)

# In PumaDaemon.start() or after login:
self.recovery_manager.start()

# In _TradeWSListener.on_trade_result (or _handle_trade_result):
def on_trade_result(self, data):
    trade = self._daemon.trade_manager.update_from_result(data)
    if trade:
        self._daemon._trade_history.insert(0, trade.to_dict())

# In place_trade method:
def place_trade(self, body):
    result = self._trades_api.place_order(...)
    trade = self.trade_manager.create_from_order(result)
    # Also add to _trade_history for UI
    self._trade_history.insert(0, trade.to_dict())

# In list_trades method:
def list_trades(self, limit=50):
    # Merge local active trades with history
    active = self.trade_manager.get_active()
    # Convert to API format and merge
    ...

# Add endpoint for manual recovery trigger:
elif path == "/recovery/reconcile":
    self.recovery_manager.force_reconcile()
    self._send(200, {"status": "reconciliation_started"})

elif path == "/recovery/status":
    self._send(200, self.recovery_manager.get_status())
"""