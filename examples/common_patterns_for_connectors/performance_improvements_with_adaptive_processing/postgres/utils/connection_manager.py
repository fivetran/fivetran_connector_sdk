"""
Connection management with timeout and resource monitoring support.

This module provides a thread-safe connection manager that handles
database connections with automatic reconnection after timeout periods.
"""

import threading
from datetime import datetime, timezone
from contextlib import contextmanager
import pyodbc
from fivetran_connector_sdk import Logging as log

from .config import CONNECTION_TIMEOUT_HOURS


class ConnectionManager:
    """
    Manages database connections with timeout and resource monitoring.
    
    This class provides thread-safe connection management with automatic
    reconnection when connections expire. It's designed to handle long-running
    sync operations that might exceed database connection timeouts.
    
    Attributes:
        configuration: Database connection configuration dictionary
        table_size: Size of table being processed (for context in logging)
        connection_start_time: Timestamp when current connection was created
        current_connection: Active database connection object
        current_cursor: Active database cursor object
        lock: Threading lock for thread-safe operations
        timeout_hours: Hours before connection is considered expired
    
    Example:
        >>> config = {"host": "localhost", "port": "5432", ...}
        >>> conn_mgr = ConnectionManager(config, table_size=1000000)
        >>> with conn_mgr.get_cursor() as cursor:
        ...     cursor.execute("SELECT * FROM my_table")
    """
    
    def __init__(self, configuration: dict, table_size: int = 0):
        """
        Initialize the connection manager.
        
        Args:
            configuration: Database connection configuration with keys:
                - host: Database hostname
                - port: Database port
                - database: Database name
                - username: Database username
                - password: Database password
            table_size: Size of table being processed (optional, for logging context)
        """
        self.configuration = configuration
        self.table_size = table_size
        self.connection_start_time = None
        self.current_connection = None
        self.current_cursor = None
        self.lock = threading.Lock()
        self.timeout_hours = CONNECTION_TIMEOUT_HOURS
        
    def _is_connection_expired(self) -> bool:
        """
        Check if current connection has exceeded timeout limit.
        
        Long-running connections can become stale or be terminated by the database.
        This method checks if the connection has been open longer than the timeout period.
        
        Returns:
            True if connection is expired or doesn't exist, False otherwise
        """
        if not self.connection_start_time:
            return True
        elapsed = datetime.now(timezone.utc) - self.connection_start_time
        return elapsed.total_seconds() > (self.timeout_hours * 3600)
    
    def _create_connection(self):
        """
        Create a new database connection.
        
        This method establishes a new ODBC connection to PostgreSQL using
        the provided configuration. It updates the connection start time
        and creates a new cursor for query execution.
        
        Raises:
            Exception: If connection cannot be established
        
        Note:
            The DRIVER path (/opt/homebrew/lib/psqlodbcw.so) is Mac-specific.
            For Linux, use /usr/lib/x86_64-linux-gnu/odbc/psqlodbcw.so
            For Windows, use {PostgreSQL Unicode}
        """
        try:
            # Build ODBC connection string
            # Note: Update DRIVER path based on your system
            conn_str = (
                f"DRIVER=/opt/homebrew/lib/psqlodbcw.so;"
                f"SERVER={self.configuration['host']};"
                f"PORT={self.configuration['port']};"
                f"DATABASE={self.configuration['database']};"
                f"UID={self.configuration['username']};"
                f"PWD={self.configuration['password']};"
            )
            
            # Create connection and cursor
            conn = pyodbc.connect(conn_str)
            self.current_connection = conn
            self.current_cursor = conn.cursor()
            self.connection_start_time = datetime.now(timezone.utc)
            
            log.info(
                f"New database connection established at {self.connection_start_time}"
            )
            
            return conn, self.current_cursor
            
        except Exception as e:
            log.severe(f"Failed to create database connection: {e}")
            raise
    
    def _close_connection(self):
        """
        Close current database connection.
        
        This method safely closes the cursor and connection, cleaning up
        resources. It handles exceptions gracefully to avoid cascading failures.
        """
        try:
            if self.current_cursor:
                self.current_cursor.close()
                self.current_cursor = None
                
            if self.current_connection:
                self.current_connection.close()
                self.current_connection = None
                
            self.connection_start_time = None
            log.info("Database connection closed")
            
        except Exception as e:
            log.warning(f"Error closing connection: {e}")
    
    @contextmanager
    def get_cursor(self):
        """
        Context manager for database cursor with automatic reconnection.
        
        This method provides a thread-safe context manager for database operations.
        It automatically reconnects if the connection has expired and handles
        errors by closing the connection for a fresh start on retry.
        
        Yields:
            Database cursor for executing queries
        
        Raises:
            Exception: If database operations fail
        
        Example:
            >>> with conn_mgr.get_cursor() as cursor:
            ...     cursor.execute("SELECT * FROM my_table")
            ...     results = cursor.fetchall()
        """
        with self.lock:
            try:
                # Check if connection is expired or doesn't exist
                if self._is_connection_expired() or not self.current_connection:
                    self._close_connection()
                    self._create_connection()
                
                yield self.current_cursor
                
            except Exception as e:
                log.severe(f"Database error: {e}")
                self._close_connection()
                raise
