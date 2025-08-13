"""
This module contains the DbEngine class.

Copyright (c) 2025, Jim Schilling

Please keep this header when you use this code.

This module is licensed under the MIT License.
"""

from __future__ import annotations

import logging
import os
import threading
from collections.abc import Generator
from contextlib import contextmanager
from typing import Any, NamedTuple

from sqlalchemy import Connection, create_engine, text
from sqlalchemy.pool import StaticPool
from sqlalchemy.sql import text as sql_text

from jpy_sync_db_lite.sql_helper import detect_statement_type, parse_sql_statements

# Private module-level constants for SQL commands and operations
_FETCH_STATEMENT: str = "fetch"
_EXECUTE_STATEMENT: str = "execute"
_TRANSACTION_STATEMENT: str = "transaction"
_ERROR_STATEMENT: str = "error"

# SQLite maintenance commands
_SQL_VACUUM: str = "VACUUM"
_SQL_ANALYZE: str = "ANALYZE"
_SQL_INTEGRITY_CHECK: str = "PRAGMA integrity_check"
_SQL_SQLITE_VERSION: str = "SELECT sqlite_version()"

# SQLite PRAGMA commands
_SQL_PRAGMA_JOURNAL_MODE: str = "PRAGMA journal_mode=WAL"
_SQL_PRAGMA_SYNCHRONOUS: str = "PRAGMA synchronous=NORMAL"
_SQL_PRAGMA_CACHE_SIZE: str = "PRAGMA cache_size=-128000"
_SQL_PRAGMA_TEMP_STORE: str = "PRAGMA temp_store=MEMORY"
_SQL_PRAGMA_MMAP_SIZE: str = "PRAGMA mmap_size=268435456"
_SQL_PRAGMA_OPTIMIZE: str = "PRAGMA optimize"
_SQL_PRAGMA_FOREIGN_KEYS: str = "PRAGMA foreign_keys=ON"
_SQL_PRAGMA_BUSY_TIMEOUT: str = "PRAGMA busy_timeout=30000"
_SQL_PRAGMA_AUTO_VACUUM: str = "PRAGMA auto_vacuum=INCREMENTAL"
_SQL_PRAGMA_PAGE_SIZE: str = "PRAGMA page_size=4096"
_SQL_PRAGMA_LOCKING_MODE: str = "PRAGMA locking_mode=NORMAL"
_SQL_PRAGMA_WAL_AUTOCHECKPOINT: str = "PRAGMA wal_autocheckpoint=1000"
_SQL_PRAGMA_CHECKPOINT_TIMEOUT: str = "PRAGMA checkpoint_timeout=30000"
_SQL_PRAGMA_WAL_SYNCHRONOUS: str = "PRAGMA wal_synchronous=NORMAL"
_SQL_PRAGMA_READ_UNCOMMITTED: str = "PRAGMA read_uncommitted=0"

# SQLite info PRAGMA commands
_SQL_PRAGMA_PAGE_COUNT: str = "PRAGMA page_count"
_SQL_PRAGMA_PAGE_SIZE: str = "PRAGMA page_size"
_SQL_PRAGMA_JOURNAL_MODE_INFO: str = "PRAGMA journal_mode"
_SQL_PRAGMA_SYNCHRONOUS_INFO: str = "PRAGMA synchronous"
_SQL_PRAGMA_CACHE_SIZE_INFO: str = "PRAGMA cache_size"
_SQL_PRAGMA_TEMP_STORE_INFO: str = "PRAGMA temp_store"

# Error messages
_ERROR_VACUUM_FAILED: str = "VACUUM operation failed: {}"
_ERROR_ANALYZE_FAILED: str = "ANALYZE operation failed: {}"
_ERROR_INTEGRITY_CHECK_FAILED: str = "Integrity check failed: {}"
_ERROR_OPTIMIZATION_FAILED: str = "Optimization operation failed: {}"
_ERROR_BATCH_COMMIT_FAILED: str = "Batch commit failed: {}"
_ERROR_TRANSACTION_FAILED: str = "Transaction failed: {}"
_ERROR_EXECUTE_FAILED: str = "Execute failed: {}"
_ERROR_FETCH_FAILED: str = "Fetch failed: {}"
_ERROR_BATCH_FAILED: str = "Batch failed: {}"


class DbOperationError(Exception):
    """
    Exception raised when a database operation fails.
    """
    pass


class SQLiteError(Exception):
    """
    SQLite-specific exception with error code and message.
    """
    def __init__(self, error_code: int, message: str) -> None:
        self._error_code: int = error_code
        self._message: str = message
        super().__init__(f"SQLite error {error_code}: {message}")

    @property
    def error_code(self) -> int:
        """Return the SQLite error code."""
        return self._error_code

    @property
    def message(self) -> str:
        """Return the SQLite error message."""
        return self._message


class DbResult(NamedTuple):
    result: bool
    rowcount: int | None = None
    data: list[dict] | None = None


class DbEngine:
    """
    Database engine for managing SQLite operations with thread safety and performance optimizations.
    Optimized for single-connection scenarios.
    """
    def __init__(
        self,
        database_url: str,
        *,
        debug: bool = False,
        timeout: int = 30,
        check_same_thread: bool = False,
        enable_prepared_statements: bool = True
    ) -> None:
        """
        Initialize the DbEngine with a single database connection.

        Args:
            database_url: SQLAlchemy database URL (e.g., 'sqlite:///database.db')
            debug: Enable SQLAlchemy echo mode (default: False)
            timeout: SQLite connection timeout in seconds (default: 30)
            check_same_thread: SQLite thread safety check (default: False)
            enable_prepared_statements: Enable prepared statement caching (default: True)
        """
        self._engine = create_engine(
            database_url,
            poolclass=StaticPool,
            connect_args={
                "check_same_thread": check_same_thread,
                "timeout": timeout,
                "isolation_level": "DEFERRED",
            },
            echo=debug,
        )

        # Initialize locks and stats first
        self._stats_lock = threading.Lock()
        self._shutdown_event = threading.Event()
        self._stats: dict[str, int] = {
            "requests": 0, 
            "errors": 0, 
            "fetch_operations": 0,
            "execute_operations": 0,
            "transaction_operations": 0,
            "batch_operations": 0,
            "prepared_statements_created": 0,
            "connection_recreations": 0
        }

        # Create and maintain a single persistent connection
        self._connection: Connection | None = None
        self._connection_lock = threading.RLock()
        
        # Initialize the single connection (now _stats_lock exists)
        self._initialize_connection()

        # Prepared statement cache
        self._enable_prepared_statements: bool = enable_prepared_statements
        self._prepared_statements: dict[str, Any] = {}
        self._prepared_statements_lock = threading.RLock()

    @property
    def engine(self) -> Any:
        """Return the SQLAlchemy engine instance."""
        return self._engine

    @property
    def shutdown_event(self) -> threading.Event:
        """Return the shutdown event."""
        return self._shutdown_event

    @property
    def stats(self) -> dict[str, int]:
        """Return a copy of the stats dictionary."""
        return self._stats.copy()

    def _initialize_connection(self) -> None:
        """
        Initialize the single persistent database connection.
        """
        with self._connection_lock:
            if self._connection is None or self._connection.closed:
                self._connection = self._engine.connect()
                self._configure_db_performance()
                with self._stats_lock:
                    self._stats["connection_recreations"] += 1

    def _get_connection(self) -> Connection:
        """
        Get the single database connection, recreating if necessary.
        
        Returns:
            Active database connection
        """
        with self._connection_lock:
            if self._connection is None or self._connection.closed:
                self._initialize_connection()
            return self._connection

    def _check_connection_health(self) -> bool:
        """
        Check if the current connection is healthy.
        
        Returns:
            True if connection is healthy, False otherwise
        """
        try:
            with self._connection_lock:
                if self._connection is None or self._connection.closed:
                    return False
                # Simple health check query
                self._connection.execute(text("SELECT 1"))
                return True
        except Exception:
            return False

    def _recreate_connection_if_needed(self) -> None:
        """
        Recreate the connection if it's not healthy.
        """
        if not self._check_connection_health():
            with self._connection_lock:
                if self._connection is not None:
                    try:
                        self._connection.close()
                    except Exception:
                        pass
                self._initialize_connection()

    def _configure_db_performance(self) -> None:
        """
        Configure SQLite database for performance optimizations.
        """
        with self._connection_lock:
            conn = self._get_connection()
            # Core performance PRAGMA settings (connection-scoped)
            conn.execute(text(_SQL_PRAGMA_JOURNAL_MODE))
            conn.execute(text(_SQL_PRAGMA_SYNCHRONOUS))
            conn.execute(text(_SQL_PRAGMA_CACHE_SIZE))
            conn.execute(text(_SQL_PRAGMA_TEMP_STORE))
            conn.execute(text(_SQL_PRAGMA_MMAP_SIZE))
            conn.execute(text(_SQL_PRAGMA_PAGE_SIZE))
            conn.execute(text(_SQL_PRAGMA_LOCKING_MODE))

            # WAL-specific optimizations
            conn.execute(text(_SQL_PRAGMA_WAL_AUTOCHECKPOINT))
            conn.execute(text(_SQL_PRAGMA_CHECKPOINT_TIMEOUT))
            conn.execute(text(_SQL_PRAGMA_WAL_SYNCHRONOUS))

            # Transaction and concurrency settings
            conn.execute(text(_SQL_PRAGMA_READ_UNCOMMITTED))
            conn.execute(text(_SQL_PRAGMA_FOREIGN_KEYS))
            conn.execute(text(_SQL_PRAGMA_BUSY_TIMEOUT))
            conn.execute(text(_SQL_PRAGMA_AUTO_VACUUM))

            # Run optimization
            conn.execute(text(_SQL_PRAGMA_OPTIMIZE))
            conn.commit()

    def configure_pragma(self, pragma_name: str, value: str) -> None:
        """
        Configure a specific SQLite PRAGMA setting.

        Args:
            pragma_name: Name of the PRAGMA (e.g., 'cache_size', 'synchronous')
            value: Value to set for the PRAGMA
        """
        try:
            with self._connection_lock:
                conn = self._get_connection()
                conn.execute(text(f"PRAGMA {pragma_name}={value}"))
                conn.commit()
        except Exception as e:
            raise DbOperationError(f"PRAGMA configuration failed: {e}") from e

    # Synchronous execution path (no background worker)
    def _execute_single_request(self, operation: str, query: str, params: Any | None) -> dict | list[dict]:
        """Execute a single request directly and return raw payload."""
        self._recreate_connection_if_needed()

        with self._connection_lock:
            conn = self._get_connection()

            if operation == _FETCH_STATEMENT:
                stmt = self._get_prepared_statement(query)
                result = conn.execute(stmt, params or {})
                rows = result.fetchall()
                return [dict(row._mapping) for row in rows]

            if operation == _EXECUTE_STATEMENT:
                stmt_type = detect_statement_type(query)
                stmt = self._get_prepared_statement(query)
                result = conn.execute(stmt, params or {})
                conn.commit()

                rowcount: int | None = None
                if stmt_type == _FETCH_STATEMENT:
                    rowcount = 0
                elif hasattr(result, 'rowcount') and result.rowcount is not None and result.rowcount >= 0:
                    rowcount = result.rowcount
                elif isinstance(params, list):
                    rowcount = len(params)

                return {"rowcount": rowcount}

            raise DbOperationError(f"Invalid operation type: {operation}")

    # Removed legacy worker-based transaction path

    def get_stats(self) -> dict[str, int]:
        """
        Get current statistics about database operations.
        Returns:
            Dictionary containing request and error counts
        """
        with self._stats_lock:
            return self._stats.copy()

    def get_sqlite_info(self) -> dict[str, Any]:
        """
        Get SQLite-specific information and statistics.
        Returns:
            Dictionary containing SQLite information
        """
        # Helper to extract scalar from result row
        def extract_scalar(row):
            if isinstance(row, dict):
                # Return the first value in the dict
                return next(iter(row.values()), None)
            if isinstance(row, (list, tuple)):
                return row[0] if row else None
            return row

        version_result = self.fetch(_SQL_SQLITE_VERSION)
        page_count_result = self.fetch(_SQL_PRAGMA_PAGE_COUNT)
        page_size_result = self.fetch(_SQL_PRAGMA_PAGE_SIZE)
        journal_mode_result = self.fetch(_SQL_PRAGMA_JOURNAL_MODE_INFO)
        synchronous_result = self.fetch(_SQL_PRAGMA_SYNCHRONOUS_INFO)
        cache_size_result = self.fetch(_SQL_PRAGMA_CACHE_SIZE_INFO)
        temp_store_result = self.fetch(_SQL_PRAGMA_TEMP_STORE_INFO)
        mmap_size_result = self.fetch(_SQL_PRAGMA_MMAP_SIZE)
        busy_timeout_result = self.fetch(_SQL_PRAGMA_BUSY_TIMEOUT)
        
        # Extract scalar values from results
        sqlite_version = extract_scalar(version_result.data[0]) if version_result.data else None
        page_count = extract_scalar(page_count_result.data[0]) if page_count_result.data else None
        page_size = extract_scalar(page_size_result.data[0]) if page_size_result.data else None
        journal_mode = extract_scalar(journal_mode_result.data[0]) if journal_mode_result.data else None
        synchronous = extract_scalar(synchronous_result.data[0]) if synchronous_result.data else None
        cache_size = extract_scalar(cache_size_result.data[0]) if cache_size_result.data else None
        temp_store = extract_scalar(temp_store_result.data[0]) if temp_store_result.data else None
        mmap_size = extract_scalar(mmap_size_result.data[0]) if mmap_size_result.data else None
        busy_timeout = extract_scalar(busy_timeout_result.data[0]) if busy_timeout_result.data else None
        
        # Get database size using the single connection
        database_size = None
        try:
            conn = self._get_connection()
            if hasattr(conn, "engine") and hasattr(conn.engine, "url"):
                db_path = str(conn.engine.url.database)
                if db_path and db_path != ":memory:":
                    try:
                        database_size = os.path.getsize(db_path)
                    except Exception:
                        database_size = None
        except Exception:
            database_size = None
        
        return {
            "version": sqlite_version,
            "database_size": database_size,
            "page_count": page_count,
            "page_size": page_size,
            "cache_size": cache_size,
            "journal_mode": journal_mode,
            "synchronous": synchronous,
            "temp_store": temp_store,
            "mmap_size": mmap_size,
            "busy_timeout": busy_timeout,
        }

    def get_performance_info(self) -> dict[str, Any]:
        """
        Get comprehensive performance information including SQLite settings and engine statistics.
        
        Returns:
            Dictionary containing performance metrics and configuration
        """
        sqlite_info = self.get_sqlite_info()
        stats = self.get_stats()
        
        # Calculate performance ratios
        total_operations = stats.get("requests", 0)
        error_rate = (stats.get("errors", 0) / total_operations * 100) if total_operations > 0 else 0
        
        # Get connection pool info (for single connection, this is simplified)
        pool_info = {
            "pool_size": 1,
            "checked_in": 1 if self._connection is not None and not self._connection.closed else 0,
            "checked_out": 0,
            "overflow": 0,
            "connection_healthy": self._check_connection_health(),
        }
        
        return {
            "engine_stats": stats,
            "sqlite_info": sqlite_info,
            "connection_pool": pool_info,
            "performance_metrics": {
                "total_operations": total_operations,
                "error_rate_percent": round(error_rate, 2),
                "prepared_statements_cached": self.get_prepared_statement_count(),
                "active_workers": 1,
            },
            "configuration": {
                "pool_size": 1,
                "max_overflow": 0,
                "pool_timeout": 30,
                "pool_recycle": 3600,
                "enable_prepared_statements": self._enable_prepared_statements,
                "connection_type": "single_persistent",
            }
        }

    def shutdown(self) -> None:
        """
        Gracefully shutdown the database engine and worker threads.
        """
        self._shutdown_event.set()
        
        # Close the single connection
        with self._connection_lock:
            if self._connection is not None:
                try:
                    self._connection.close()
                except Exception:
                    pass
                self._connection = None
        
        self._engine.dispose()

    # Context manager support for simpler lifecycle management
    def __enter__(self) -> "DbEngine":
        """Enter context by returning self."""
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: Any | None,
    ) -> None:
        """Ensure resources are cleaned up on exit."""
        self.shutdown()

    def execute(
        self,
        query: str,
        *,
        params: dict | list[dict] | None = None
    ) -> DbResult:
        """
        Execute a SQL statement (INSERT, UPDATE, DELETE, etc.) with thread safety.
        Returns:
            DbResult(result: bool, rowcount: Optional[int], data: None)
        """
        try:
            # Execute synchronously
            with self._stats_lock:
                self._stats["requests"] += 1
                self._stats["execute_operations"] += 1
            payload = self._execute_single_request(_EXECUTE_STATEMENT, query, params)
            rowcount = payload.get('rowcount') if isinstance(payload, dict) else None
            return DbResult(result=True, rowcount=rowcount, data=None)
        except Exception as e:
            if not isinstance(e, DbOperationError):
                raise DbOperationError(_ERROR_EXECUTE_FAILED.format(e)) from e
            raise

    def fetch(
        self,
        query: str,
        *,
        params: dict | None = None
    ) -> DbResult:
        """
        Execute a SELECT query and return results as a DbResult namedtuple.
        Returns:
            DbResult(result: bool, rowcount: Optional[int], data: Optional[list[dict]])
        """
        try:
            with self._stats_lock:
                self._stats["requests"] += 1
                self._stats["fetch_operations"] += 1
            payload = self._execute_single_request(_FETCH_STATEMENT, query, params)
            if isinstance(payload, list):
                return DbResult(result=bool(payload), rowcount=len(payload), data=payload)
            raise DbOperationError("Unexpected result type from fetch operation")
        except Exception as e:
            if not isinstance(e, DbOperationError):
                raise DbOperationError(_ERROR_FETCH_FAILED.format(e)) from e
            raise

    def batch(
        self,
        batch_sql: str,
    ) -> list[dict[str, any]]:
        """
        Execute multiple SQL statements in a batch with thread safety.
        Optimized to use a single connection for all statements.
        Returns:
            List of dicts, each containing 'statement', 'operation', and 'result' (DbResult)
        """
        with self._stats_lock:
            self._stats["batch_operations"] += 1
            
        statements = parse_sql_statements(batch_sql)
        results: list[dict[str, any]] = []
        
        # Create operations with proper statement type detection
        operations = []
        for stmt in statements:
            stmt_type = detect_statement_type(stmt)
            if stmt_type == _FETCH_STATEMENT:
                operations.append({"operation": "fetch", "query": stmt})
            else:
                operations.append({"operation": "execute", "query": stmt})
        
        # Execute batch synchronously within one transaction
        with self._connection_lock:
            conn = self._get_connection()
            try:
                for op in operations:
                    stmt = self._get_prepared_statement(op["query"])
                    if op["operation"] == _FETCH_STATEMENT:
                        result = conn.execute(stmt)
                        rows = [dict(row._mapping) for row in result.fetchall()]
                        results.append({
                            "statement": op["query"],
                            "operation": _FETCH_STATEMENT,
                            "result": DbResult(result=True, rowcount=len(rows), data=rows),
                        })
                    else:
                        result = conn.execute(stmt)
                        rc = result.rowcount if hasattr(result, "rowcount") and result.rowcount is not None else 1
                        results.append({
                            "statement": op["query"],
                            "operation": _EXECUTE_STATEMENT,
                            "result": DbResult(result=True, rowcount=rc, data=None),
                        })
                conn.commit()
            except Exception as e:
                try:
                    conn.rollback()
                except Exception:
                    pass
                raise DbOperationError(_ERROR_BATCH_FAILED.format(e)) from e
        return results

    def execute_transaction(
        self,
        operations: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        """
        Execute a list of operations as a single transaction.
        Args:
            operations: List of operation dictionaries, each containing:
                - 'operation': 'execute' or 'fetch'
                - 'query': SQL statement
                - 'params': Parameters (optional)
        Returns:
            List of result dicts for each operation (with 'type', 'result', etc.)
        Raises:
            DbOperationError: If the transaction fails or an invalid operation type is provided
        """
        # Execute provided operations inside a single transaction synchronously
        with self._connection_lock:
            conn = self._get_connection()
            try:
                results: list[dict[str, Any]] = []
                for operation in operations:
                    op_type = operation.get("operation")
                    query = operation.get("query")
                    params = operation.get("params")
                    if op_type not in [_FETCH_STATEMENT, _EXECUTE_STATEMENT] or not query:
                        raise DbOperationError("Invalid operation in transaction")

                    if op_type == _FETCH_STATEMENT:
                        stmt = self._get_prepared_statement(query)
                        res = conn.execute(stmt, params or {})
                        rows = [dict(row._mapping) for row in res.fetchall()]
                        results.append({"operation": _FETCH_STATEMENT, "result": rows})
                    else:
                        stmt = self._get_prepared_statement(query)
                        conn.execute(stmt, params or {})
                        results.append({"operation": _EXECUTE_STATEMENT, "result": True})
                conn.commit()
                return results
            except Exception as e:
                try:
                    conn.rollback()
                except Exception:
                    pass
                raise DbOperationError(_ERROR_TRANSACTION_FAILED.format(e)) from e

    @contextmanager
    def get_raw_connection(self) -> Generator[Connection, None, None]:
        """
        Get the raw SQLAlchemy connection for advanced operations.
        Note: This uses the single persistent connection and should be used with caution.
        Yields:
            SQLAlchemy Connection object
        """
        conn = self._get_connection()
        try:
            yield conn
        except Exception:
            # Ensure connection is healthy after any errors
            self._recreate_connection_if_needed()
            raise

    def vacuum(self) -> None:
        """
        Perform a VACUUM operation to reclaim space and optimize the database.
        Raises:
            DbOperationError: If the VACUUM operation fails
        """
        try:
            with self._connection_lock:
                conn = self._get_connection()
                conn.execute(text(_SQL_VACUUM))
                conn.commit()
        except Exception as e:
            raise DbOperationError(_ERROR_VACUUM_FAILED.format(e)) from e

    def analyze(
        self,
        *,
        table_name: str | None = None
    ) -> None:
        """
        Perform an ANALYZE operation to update query planner statistics.
        Args:
            table_name: Specific table to analyze, or None for all tables
        Raises:
            DbOperationError: If the ANALYZE operation fails
        """
        try:
            with self._connection_lock:
                conn = self._get_connection()
                analyze_sql = f"{_SQL_ANALYZE} {table_name}" if table_name else _SQL_ANALYZE
                conn.execute(text(analyze_sql))
                conn.commit()
        except Exception as e:
            raise DbOperationError(_ERROR_ANALYZE_FAILED.format(e)) from e

    def integrity_check(self) -> list[str]:
        """
        Perform an integrity check on the database.
        Returns:
            List of integrity issues found (empty list if no issues)
        Raises:
            DbOperationError: If the integrity check fails
        """
        def extract_scalar(row):
            if isinstance(row, dict):
                return next(iter(row.values()), None)
            if isinstance(row, (list, tuple)):
                return row[0] if row else None
            return row

        try:
            with self._connection_lock:
                conn = self._get_connection()
                res = conn.execute(text(_SQL_INTEGRITY_CHECK))
                rows = [dict(row._mapping) for row in res.fetchall()]
                issues = [extract_scalar(row) for row in rows if extract_scalar(row) != "ok"]
                return issues
        except Exception as e:
            raise DbOperationError(_ERROR_INTEGRITY_CHECK_FAILED.format(e)) from e

    def optimize(self) -> None:
        """
        Perform database optimization operations.
        This method combines VACUUM and ANALYZE operations to optimize the database for better performance.
        Raises:
            DbOperationError: If the optimization operation fails
        """
        # Execute VACUUM and ANALYZE sequentially using the queue system
        self.vacuum()
        self.analyze()

    def _get_prepared_statement(self, query: str) -> Any:
        """
        Get or create a prepared statement for the given query.
        
        Args:
            query: SQL query string
            
        Returns:
            Prepared statement object
        """
        if not self._enable_prepared_statements:
            return sql_text(query)
        
        with self._prepared_statements_lock:
            if query not in self._prepared_statements:
                self._prepared_statements[query] = sql_text(query)
                with self._stats_lock:
                    self._stats["prepared_statements_created"] += 1
            return self._prepared_statements[query]

    def clear_prepared_statements(self) -> None:
        """
        Clear the prepared statement cache.
        Useful when schema changes occur.
        """
        with self._prepared_statements_lock:
            self._prepared_statements.clear()

    def get_prepared_statement_count(self) -> int:
        """
        Get the number of cached prepared statements.
        
        Returns:
            Number of cached prepared statements
        """
        with self._prepared_statements_lock:
            return len(self._prepared_statements)

    def check_connection_health(self) -> bool:
        """
        Check if the current connection is healthy.
        
        Returns:
            True if connection is healthy, False otherwise
        """
        return self._check_connection_health()

    def recreate_connection(self) -> None:
        """
        Manually recreate the database connection.
        Useful when the connection becomes stale or after schema changes.
        """
        with self._connection_lock:
            if self._connection is not None:
                try:
                    self._connection.close()
                except Exception:
                    pass
            self._initialize_connection()

    def get_connection_info(self) -> dict[str, Any]:
        """
        Get information about the current connection state.
        
        Returns:
            Dictionary containing connection information
        """
        with self._connection_lock:
            return {
                "connection_exists": self._connection is not None,
                "connection_closed": self._connection.closed if self._connection is not None else True,
                "connection_healthy": self._check_connection_health(),
                "connection_recreations": self._stats.get("connection_recreations", 0),
            }
