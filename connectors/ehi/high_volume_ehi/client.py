"""
MSSQLConnection and ConnectionPool classes for managing pyodbc connections to SQL Server.
This module abstracts connection string construction, connection lifecycle, and retry logic.
"""

# For queue operations
import queue

# For exponential backoff with jitter
import random

# For connection timeout tracking
import time

# Context manager for connection acquisition
from contextlib import contextmanager

# For timestamping connection open time and calculating elapsed time
from datetime import datetime

# ODBC driver for SQL Server
import pyodbc

# For enabling Logs in your connector code
from fivetran_connector_sdk import Logging as log

# constants for connection management and retry logic
from constants import (
    CONNECTION_TIMEOUT_HOURS,
    MAX_RETRIES,
    BASE_RETRY_DELAY,
    MAX_RETRY_DELAY,
    RETRYABLE_SQLSTATES,
    LOCK_TIMEOUT_NATIVE_ERROR,
)


def _is_retryable_error(exc: Exception) -> bool:
    """
    Return True when the exception represents a transient SQL Server error
    that is safe to retry — deadlocks, connection drops, and timeout conditions.
    Uses SQLSTATE codes (standardised, locale-independent) rather than
    substring-matching the human-readable error message.
    Args:
        exc: The exception to evaluate, expected to be a pyodbc.Error.
    """
    if not isinstance(exc, pyodbc.Error) or not exc.args:
        return False
    sqlstate = str(exc.args[0])
    if sqlstate in RETRYABLE_SQLSTATES:
        return True
    # SQL Server lock timeout returns SQLSTATE HY000 with native error 1222
    # embedded in the message string.
    if sqlstate == "HY000":
        message = str(exc.args[1]) if len(exc.args) > 1 else ""
        return LOCK_TIMEOUT_NATIVE_ERROR in message
    return False


class MSSQLConnection:
    """
    Wraps a single pyodbc connection to SQL Server.
    Builds an ODBC Driver 18 connection string with SNI support and
    sets READ UNCOMMITTED isolation level on connect.
    """

    def __init__(self, configuration: dict) -> None:
        self._configuration = configuration
        self._conn = None
        self._connected_at = None

    def _build_conn_str(self) -> str:
        """Construct an ODBC connection string from the configuration dict."""
        server = self._configuration.get("mssql_server", "").strip()
        port = self._configuration.get("mssql_port", "1433").strip()
        cert_server = self._configuration.get("mssql_cert_server", "").strip()
        database = self._configuration.get("mssql_database", "").strip()
        user = self._configuration.get("mssql_user", "").strip()
        password = self._configuration.get("mssql_password", "")

        # TrustServerCertificate=yes when no explicit cert hostname is provided
        # (e.g., AWS RDS with self-signed certs).
        trust_cert = "yes" if not cert_server else "no"

        conn_str = (
            f"DRIVER={{ODBC Driver 18 for SQL Server}};"
            f"SERVER={server},{port};"
            f"DATABASE={database};"
            f"UID={user};"
            f"PWD={password};"
            f"Encrypt=yes;"
            f"TrustServerCertificate={trust_cert};"
        )
        if cert_server:
            conn_str += f"HostNameInCertificate={cert_server};"
        return conn_str

    def _open(self) -> None:
        """Open a new connection using the built connection string and set isolation level."""
        conn_str = self._build_conn_str()
        self._conn = pyodbc.connect(conn_str, autocommit=True)
        cursor = self._conn.cursor()
        cursor.execute("SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED")
        cursor.close()
        self._connected_at = datetime.now()
        log.fine(f"connection opened at {self._connected_at.isoformat()}")

    def _close(self) -> None:
        """Close the connection if it exists, and reset tracking variables."""
        if self._conn is not None:
            try:
                self._conn.close()
            except Exception as exc:
                log.warning(f"error on connection close: {exc}")
            finally:
                self._conn = None
                self._connected_at = None

    def _is_expired(self) -> bool:
        """Return True if the connection has been open longer than the timeout threshold."""
        if self._connected_at is None:
            return True
        elapsed = (datetime.now() - self._connected_at).total_seconds()
        return elapsed > CONNECTION_TIMEOUT_HOURS * 3600

    def ensure_open(self) -> None:
        """Guarantee the connection is open and not expired."""
        if self._conn is None or self._is_expired():
            self._close()
            self._open()

    def execute_with_retry(self, sql: str, params=()) -> pyodbc.Cursor:
        """
        Execute SQL with exponential backoff on transient errors.
        On a retryable SQLSTATE the connection is closed, the thread sleeps
        with jitter, the connection is reopened, and the query retried.
        Non-retryable errors are re-raised immediately.
        """
        last_exc = None
        for attempt in range(MAX_RETRIES):
            try:
                self.ensure_open()
                cur = self._conn.cursor()
                cur.execute(sql, params)
                return cur
            except Exception as exc:
                if not _is_retryable_error(exc):
                    log.severe(f"Non-retryable SQL error: {exc}")
                    raise

                last_exc = exc
                delay = min(BASE_RETRY_DELAY * (2**attempt), MAX_RETRY_DELAY)
                sleep_time = delay + random.uniform(0, delay * 0.2)
                log.warning(
                    f"Retryable error (attempt {attempt + 1}/{MAX_RETRIES}): {exc}. "
                    f"Sleeping {sleep_time:.1f}s before retry."
                )
                self._close()
                time.sleep(sleep_time)

        log.severe(f"All {MAX_RETRIES} retry attempts exhausted. Last error: {last_exc}")
        raise last_exc

    def close(self) -> None:
        self._close()


class ConnectionPool:
    """
    Fixed-size pool of MSSQLConnection instances backed by a thread-safe Queue.
    The pool calls ensure_open on every acquire so callers do not need to
    manage connection lifecycle.
    """

    def __init__(self, configuration: dict, size: int) -> None:
        log.info(f"Initialising connection pool with {size} connection(s)")
        self._queue = queue.Queue(maxsize=size)
        self._all: list = []
        try:
            for _ in range(size):
                conn = MSSQLConnection(configuration)
                conn.ensure_open()
                self._all.append(conn)
                self._queue.put(conn)
        except Exception:
            self.close_all()
            raise
        log.info("Connection pool ready")

    @contextmanager
    def acquire(self, timeout: float = 30.0):
        """
        Acquire a connection from the pool as a context manager.
        Always returns the connection to the pool in the finally block.
        """
        try:
            conn = self._queue.get(timeout=timeout)
        except queue.Empty:
            raise RuntimeError(f"No connection available from pool within {timeout}s timeout")
        try:
            conn.ensure_open()
            yield conn
        finally:
            self._queue.put(conn)

    def close_all(self) -> None:
        for conn in self._all:
            conn.close()
        log.info("All pool connections closed")
