import logging
import threading
from typing import Optional

import peewee
from playhouse.pool import PooledPostgresqlExtDatabase
import jesse.helpers as jh
from jesse.services.env import ENV_VALUES

# Logger for database connection events (useful for debugging connection issues)
logger = logging.getLogger(__name__)

# Error message fragments that indicate a broken connection which can be retried.
_RECONNECT_ERROR_FRAGMENTS = (
    'server closed the connection unexpectedly',
    'connection already closed',
    'connection not open',
    'could not receive data from server',
    'terminating connection',
    'ssl connection has been closed unexpectedly',
    'connection is closed',
)


class ReconnectingPooledPostgresqlExtDatabase(PooledPostgresqlExtDatabase):
    """Pooled Postgres database that can reconnect on transient disconnects."""

    def _should_reconnect(self, exc: Exception) -> bool:
        if not isinstance(exc, (peewee.OperationalError, peewee.InterfaceError)):
            return False

        exc_message = str(exc).lower()
        if not exc_message:
            return False

        return any(fragment in exc_message for fragment in _RECONNECT_ERROR_FRAGMENTS)

    def _reconnect(self) -> None:
        try:
            if not self.is_closed():
                self.close()
        except Exception:
            pass

        self.connect(reuse_if_open=True)

    def execute_sql(self, sql, params=None, commit=peewee.SENTINEL):
        try:
            return super().execute_sql(sql, params, commit)
        except Exception as exc:
            if not self._should_reconnect(exc):
                raise

            logger.warning("DB connection error detected. Reconnecting once: %s", exc)
            self._reconnect()
            return super().execute_sql(sql, params, commit)


class Database:
    """
    Database connection manager with connection pooling and automatic reconnection.

    This class addresses several reliability issues:
    1. Stale connection detection - validates connections before reuse
    2. Automatic reconnection - handles server-side disconnections gracefully
    3. Thread safety - uses locking for multi-threaded environments (live trading)
    4. Connection pooling - efficiently manages database connections

    Compatible with both direct PostgreSQL connections and connection poolers
    like PgBouncer.
    """

    # Connection pool settings
    # These defaults work well for both direct PostgreSQL and PgBouncer setups
    MAX_CONNECTIONS = 8          # Max pooled connections
    STALE_TIMEOUT = 300          # Seconds before a connection is considered stale (5 min)
    CONNECTION_TIMEOUT = 10      # Seconds to wait for a connection from the pool

    def __init__(self):
        self.db: Optional[ReconnectingPooledPostgresqlExtDatabase] = None
        self._lock = threading.RLock()  # Reentrant lock for thread safety

    def is_closed(self) -> bool:
        """Check if the database connection is closed (thread-safe)."""
        with self._lock:
            if self.db is None:
                return True
            return self.db.is_closed()

    def is_open(self) -> bool:
        """Check if the database connection is open (thread-safe)."""
        with self._lock:
            if self.db is None:
                return False
            return not self.db.is_closed()

    def close_connection(self) -> None:
        """Close the database connection (thread-safe)."""
        with self._lock:
            if self.db:
                self.db.close()

    def reconnect(self) -> None:
        """Force a reconnect without replacing the database object."""
        if not jh.is_jesse_project() or jh.is_unit_testing():
            return

        with self._lock:
            if self.db is None:
                self._create_db_locked()
                return

            try:
                self.db.close()
            except Exception:
                pass

            self.db.connect(reuse_if_open=True)

    def open_connection(self) -> None:
        """
        Open a database connection with automatic validation and reconnection.

        This method is thread-safe and handles:
        - Stale connection detection via SELECT 1 validation
        - Automatic reconnection when server closes the connection
        - Connection pooling for efficient resource usage
        """
        if not jh.is_jesse_project() or jh.is_unit_testing():
            return

        with self._lock:
            if self.db is None:
                self._create_db_locked()

            try:
                if self.db.is_closed():
                    self.db.connect(reuse_if_open=True)

                # Validate current connection; reconnect if needed.
                self.db.execute_sql('SELECT 1')
            except Exception as e:
                logger.warning("Connection validation failed, reconnecting: %s", e)
                self.reconnect()

    def _create_db_locked(self) -> None:
        # TCP keepalive settings for detecting dead connections.
        # These settings apply to the underlying TCP socket and help detect
        # network-level issues. Note: When using PgBouncer, these settings
        # apply to the connection between this client and PgBouncer, not
        # between PgBouncer and PostgreSQL. For full effectiveness with
        # PgBouncer, also configure server_check_delay in pgbouncer.ini.
        options = {
            "keepalives": 1,              # Enable TCP keepalives
            "keepalives_idle": 60,        # Start probing after 60s idle
            "keepalives_interval": 10,    # Probe every 10s
            "keepalives_count": 5,        # Give up after 5 failed probes
            "connect_timeout": 10,        # Connection timeout in seconds
        }

        # Use ReconnectingPooledPostgresqlExtDatabase for better connection management:
        # - Automatic reconnection on transient disconnects
        # - Thread-safe connection pool
        # - Works with both direct PostgreSQL and PgBouncer
        self.db = ReconnectingPooledPostgresqlExtDatabase(
            ENV_VALUES['POSTGRES_NAME'],
            user=ENV_VALUES['POSTGRES_USERNAME'],
            password=ENV_VALUES['POSTGRES_PASSWORD'],
            host=ENV_VALUES['POSTGRES_HOST'],
            port=int(ENV_VALUES['POSTGRES_PORT']),
            sslmode=ENV_VALUES.get('POSTGRES_SSLMODE', 'disable'),
            max_connections=self.MAX_CONNECTIONS,
            stale_timeout=self.STALE_TIMEOUT,
            timeout=self.CONNECTION_TIMEOUT,
            **options,
        )


database = Database()
