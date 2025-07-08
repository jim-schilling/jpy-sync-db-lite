"""
This module contains the DbEngine class.

Copyright (c) 2025, Jim Schilling

Please keep this header when you use this code.

This module is licensed under the MIT License.
"""

import logging
import queue
import threading
import time
from collections.abc import Generator
from contextlib import contextmanager
from typing import Any, NamedTuple, Optional

from sqlalchemy import Connection, create_engine, text
from sqlalchemy.pool import StaticPool

from jpy_sync_db_lite.db_request import DbRequest
from jpy_sync_db_lite.sql_helper import detect_statement_type, parse_sql_statements

# Private module-level constants for SQL commands and operations
_FETCH_STATEMENT: str = "fetch"
_EXECUTE_STATEMENT: str = "execute"
_BATCH_STATEMENT: str = "batch"
_ERROR_STATEMENT: str = "error"
_SUCCESS: str = "success"
_ERROR: str = "error"

# SQLite maintenance commands
_SQL_VACUUM: str = "VACUUM"
_SQL_ANALYZE: str = "ANALYZE"
_SQL_INTEGRITY_CHECK: str = "PRAGMA integrity_check"
_SQL_SQLITE_VERSION: str = "SELECT sqlite_version()"

# SQLite PRAGMA commands
_SQL_PRAGMA_JOURNAL_MODE: str = "PRAGMA journal_mode=WAL"
_SQL_PRAGMA_SYNCHRONOUS: str = "PRAGMA synchronous=NORMAL"
_SQL_PRAGMA_CACHE_SIZE: str = "PRAGMA cache_size=-64000"
_SQL_PRAGMA_TEMP_STORE: str = "PRAGMA temp_store=MEMORY"
_SQL_PRAGMA_MMAP_SIZE: str = "PRAGMA mmap_size=268435456"
_SQL_PRAGMA_OPTIMIZE: str = "PRAGMA optimize"
_SQL_PRAGMA_FOREIGN_KEYS: str = "PRAGMA foreign_keys=ON"
_SQL_PRAGMA_BUSY_TIMEOUT: str = "PRAGMA busy_timeout=30000"
_SQL_PRAGMA_AUTO_VACUUM: str = "PRAGMA auto_vacuum=INCREMENTAL"

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
_ERROR_COMMIT_FAILED: str = "Commit failed: {}"
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
    rowcount: Optional[int] = None
    data: Optional[list[dict]] = None


class DbEngine:
    """
    Database engine for managing SQLite operations with thread safety and performance optimizations.
    """
    def __init__(
        self,
        database_url: str,
        *,
        num_workers: int = 1,
        debug: bool = False,
        timeout: int = 30,
        check_same_thread: bool = False
    ) -> None:
        """
        Initialize the DbEngine with database connection and threading configuration.

        Args:
            database_url: SQLAlchemy database URL (e.g., 'sqlite:///database.db')
            num_workers: Number of worker threads (default: 1)
            debug: Enable SQLAlchemy echo mode (default: False)
            timeout: SQLite connection timeout in seconds (default: 30)
            check_same_thread: SQLite thread safety check (default: False)
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

        self._configure_db_performance()

        self._request_queue: queue.Queue[DbRequest] = queue.Queue()
        self._stats_lock = threading.Lock()
        self._db_engine_lock = threading.RLock()
        self._shutdown_event = threading.Event()
        self._stats: dict[str, int] = {"requests": 0, "errors": 0}

        self._num_workers: int = num_workers
        self._workers: list[threading.Thread] = []
        for i in range(self._num_workers):
            worker = threading.Thread(
                target=self._worker, daemon=True, name=f"DB-Worker-{i}"
            )
            worker.start()
            self._workers.append(worker)

    @property
    def engine(self) -> Any:
        """Return the SQLAlchemy engine instance."""
        return self._engine

    @property
    def request_queue(self) -> queue.Queue:
        """Return the request queue."""
        return self._request_queue

    @property
    def stats_lock(self) -> threading.Lock:
        """Return the stats lock."""
        return self._stats_lock

    @property
    def db_engine_lock(self) -> threading.RLock:
        """Return the database engine lock."""
        return self._db_engine_lock

    @property
    def shutdown_event(self) -> threading.Event:
        """Return the shutdown event."""
        return self._shutdown_event

    @property
    def stats(self) -> dict[str, int]:
        """Return a copy of the stats dictionary."""
        return self._stats.copy()

    @property
    def num_workers(self) -> int:
        """Return the number of worker threads."""
        return self._num_workers

    @property
    def workers(self) -> list[threading.Thread]:
        """Return the list of worker threads."""
        return self._workers

    def _configure_db_performance(self) -> None:
        """
        Configure SQLite database for performance optimizations.
        """
        with self._engine.connect() as conn:
            conn.execute(text(_SQL_PRAGMA_JOURNAL_MODE))
            conn.execute(text(_SQL_PRAGMA_SYNCHRONOUS))
            conn.execute(text(_SQL_PRAGMA_CACHE_SIZE))
            conn.execute(text(_SQL_PRAGMA_TEMP_STORE))
            conn.execute(text(_SQL_PRAGMA_MMAP_SIZE))
            conn.execute(text(_SQL_PRAGMA_OPTIMIZE))
            conn.execute(text(_SQL_PRAGMA_FOREIGN_KEYS))
            conn.execute(text(_SQL_PRAGMA_BUSY_TIMEOUT))
            conn.execute(text(_SQL_PRAGMA_AUTO_VACUUM))
            conn.commit()

    def configure_pragma(self, pragma_name: str, value: str) -> None:
        """
        Configure a specific SQLite PRAGMA setting.

        Args:
            pragma_name: Name of the PRAGMA (e.g., 'cache_size', 'synchronous')
            value: Value to set for the PRAGMA
        """
        with self._db_engine_lock:
            with self._engine.connect() as conn:
                conn.execute(text(f"PRAGMA {pragma_name}={value}"))
                conn.commit()

    def _worker(self) -> None:
        """
        Main worker thread for processing database requests.
        Continuously processes requests from the queue and executes them immediately.
        """
        while not self._shutdown_event.is_set():
            try:
                request = self._request_queue.get(timeout=1)
                with self._stats_lock:
                    self._stats["requests"] += 1
                self._execute_single_request_with_connection(request)
            except queue.Empty:
                continue
            except Exception:
                logging.exception("Worker error on request.")
                with self._stats_lock:
                    self._stats["errors"] += 1

    def _execute_single_request_with_connection(self, request: DbRequest) -> None:
        """
        Execute a single database request using a new connection.
        """
        with self._db_engine_lock:
            with self._engine.connect() as conn:
                self._execute_single_request(conn, request)

    def _execute_single_request(self, conn: Connection, request: DbRequest) -> None:
        """
        Execute a single database request using the provided connection.
        """
        try:
            stmt_type = detect_statement_type(request.query)
            if stmt_type == _FETCH_STATEMENT:
                result = conn.execute(text(request.query), request.params or {})
                rows = result.fetchall()
                if request.response_queue:
                    request.response_queue.put([dict(row._mapping) for row in rows])
            else:
                conn.execute(text(request.query), request.params or {})
                if request.response_queue:
                    request.response_queue.put(True)
            conn.commit()
        except Exception as e:
            conn.rollback()
            if request.response_queue:
                request.response_queue.put(DbOperationError(str(e)))
            raise DbOperationError(_ERROR_EXECUTE_FAILED.format(e)) from e

    def _execute_batch_statements(
        self,
        conn: Connection,
        batch_sql: str,
    ) -> list[dict[str, Any]]:
        """
        Execute a batch of SQL statements and return results for each.
        This method raises an exception if any statement fails.

        Args:
            conn: Database connection
            batch_sql: SQL string containing multiple statements
        Returns:
            List of results for each statement
        """
        statements = parse_sql_statements(batch_sql)
        results: list[dict[str, Any]] = []
        for stmt in statements:
            try:
                stmt_type = detect_statement_type(stmt)
                if stmt_type == _FETCH_STATEMENT:
                    result = conn.execute(text(stmt))
                    rows = result.fetchall()
                    results.append(
                        {
                            "statement": stmt,
                            "operation": _FETCH_STATEMENT,
                            "result": [dict(row._mapping) for row in rows],
                        }
                    )
                else:
                    conn.execute(text(stmt))
                    results.append(
                        {
                            "statement": stmt,
                            "operation": _EXECUTE_STATEMENT,
                            "result": True,
                        }
                    )
            except Exception as e:
                conn.rollback()
                results.append(
                    {
                        "statement": stmt,
                        "operation": _ERROR_STATEMENT,
                        "error": str(e),
                    }
                )
                raise
        try:
            conn.commit()
        except Exception as commit_error:
            conn.rollback()
            raise DbOperationError(_ERROR_BATCH_COMMIT_FAILED.format(commit_error)) from commit_error
        return results

    @contextmanager
    def _acquire_db_lock(self) -> Generator[None, None, None]:
        """
        Context manager for acquiring the database engine lock.
        This ensures thread safety by preventing concurrent database operations.
        """
        with self._db_engine_lock:
            yield

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
        with self._db_engine_lock:
            with self._engine.connect() as conn:
                sqlite_version = conn.execute(text(_SQL_SQLITE_VERSION)).scalar()
                result = conn.execute(text(_SQL_PRAGMA_PAGE_COUNT))
                page_count = result.scalar()
                result = conn.execute(text(_SQL_PRAGMA_PAGE_SIZE))
                page_size = result.scalar()
                result = conn.execute(text(_SQL_PRAGMA_JOURNAL_MODE_INFO))
                journal_mode = result.scalar()
                result = conn.execute(text(_SQL_PRAGMA_SYNCHRONOUS_INFO))
                synchronous = result.scalar()
                result = conn.execute(text(_SQL_PRAGMA_CACHE_SIZE_INFO))
                cache_size = result.scalar()
                result = conn.execute(text(_SQL_PRAGMA_TEMP_STORE_INFO))
                temp_store = result.scalar()
                result = conn.execute(text(_SQL_PRAGMA_MMAP_SIZE))
                mmap_size = result.scalar()
                result = conn.execute(text(_SQL_PRAGMA_BUSY_TIMEOUT))
                busy_timeout = result.scalar()
                database_size = None
                if hasattr(conn, "engine") and hasattr(conn.engine, "url"):
                    db_path = str(conn.engine.url.database)
                    if db_path and db_path != ":memory:":
                        try:
                            database_size = os.path.getsize(db_path)
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

    def shutdown(self) -> None:
        """
        Gracefully shutdown the database engine and worker threads.
        """
        self._shutdown_event.set()
        for worker in self._workers:
            worker.join(timeout=7)
        self._engine.dispose()

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
        with self._db_engine_lock:
            with self._engine.connect() as conn:
                try:
                    stmt_type = detect_statement_type(query)
                    result = conn.execute(text(query), params or {})
                    conn.commit()
                    rowcount = None
                    
                    # For SELECT statements, rowcount should be 0
                    if stmt_type == _FETCH_STATEMENT:
                        rowcount = 0
                    elif hasattr(result, 'rowcount') and result.rowcount is not None and result.rowcount >= 0:
                        rowcount = result.rowcount
                    elif isinstance(params, list):
                        rowcount = len(params)
                    
                    return DbResult(result=(rowcount is not None and rowcount > 0), rowcount=rowcount, data=None)
                except Exception as e:
                    conn.rollback()
                    raise DbOperationError(_ERROR_EXECUTE_FAILED.format(e)) from e

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
        with self._db_engine_lock:
            with self._engine.connect() as conn:
                try:
                    result = conn.execute(text(query), params or {})
                    rows = result.fetchall()
                    data = [dict(row._mapping) for row in rows]
                    return DbResult(result=bool(data), rowcount=len(data), data=data)
                except Exception as e:
                    raise DbOperationError(_ERROR_FETCH_FAILED.format(e)) from e

    def batch(
        self,
        batch_sql: str,
    ) -> list[dict[str, any]]:
        """
        Execute multiple SQL statements in a batch with thread safety.
        Returns:
            List of dicts, each containing 'statement', 'operation', and 'result' (DbResult)
        """
        with self._db_engine_lock:
            with self._engine.connect() as conn:
                try:
                    statements = parse_sql_statements(batch_sql)
                    results: list[dict[str, any]] = []
                    for stmt in statements:
                        try:
                            stmt_type = detect_statement_type(stmt)
                            if stmt_type == _FETCH_STATEMENT:
                                result = conn.execute(text(stmt))
                                rows = result.fetchall()
                                data = [dict(row._mapping) for row in rows]
                                results.append({
                                    "statement": stmt,
                                    "operation": _FETCH_STATEMENT,
                                    "result": DbResult(result=bool(data), rowcount=len(data), data=data),
                                })
                            else:
                                result = conn.execute(text(stmt))
                                conn.commit()
                                rowcount = None
                                if hasattr(result, 'rowcount') and result.rowcount is not None and result.rowcount >= 0:
                                    rowcount = result.rowcount
                                results.append({
                                    "statement": stmt,
                                    "operation": _EXECUTE_STATEMENT,
                                    "result": DbResult(result=(rowcount is not None and rowcount > 0), rowcount=rowcount, data=None),
                                })
                        except Exception as e:
                            conn.rollback()
                            results.append({
                                "statement": stmt,
                                "operation": _ERROR_STATEMENT,
                                "error": str(e),
                            })
                            raise
                    conn.commit()
                    return results
                except Exception as e:
                    conn.rollback()
                    raise DbOperationError(_ERROR_BATCH_FAILED.format(e)) from e

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
        with self._db_engine_lock:
            with self._engine.connect() as conn:
                results: list[dict[str, Any]] = []
                try:
                    for operation in operations:
                        if "query" not in operation:
                            raise DbOperationError("Missing required key 'query' in operation")
                        if "operation" not in operation:
                            raise DbOperationError("Missing required key 'operation' in operation")
                        op_type = operation["operation"]
                        query = operation["query"]
                        params = operation.get("params")
                        if op_type not in [_FETCH_STATEMENT, _EXECUTE_STATEMENT]:
                            raise DbOperationError(f"Invalid operation type: {op_type}. Must be 'fetch' or 'execute'")
                        try:
                            if op_type == _FETCH_STATEMENT:
                                result = conn.execute(text(query), params or {})
                                rows = result.fetchall()
                                results.append({
                                    "operation": _FETCH_STATEMENT,
                                    "result": [dict(row._mapping) for row in rows],
                                })
                            else:
                                conn.execute(text(query), params or {})
                                results.append({
                                    "operation": _EXECUTE_STATEMENT,
                                    "result": True,
                                })
                        except Exception as e:
                            conn.rollback()
                            results.append({
                                "operation": _ERROR_STATEMENT,
                                "error": str(e),
                            })
                            raise DbOperationError(_ERROR_TRANSACTION_FAILED.format(e)) from e
                    conn.commit()
                    return results
                except Exception as e:
                    conn.rollback()
                    raise DbOperationError(_ERROR_TRANSACTION_FAILED.format(e)) from e

    @contextmanager
    def get_raw_connection(self) -> Generator[Connection, None, None]:
        """
        Get a raw SQLAlchemy connection for advanced operations.
        Yields:
            SQLAlchemy Connection object
        """
        with self._db_engine_lock:
            with self._engine.connect() as conn:
                yield conn

    def vacuum(self) -> None:
        """
        Perform a VACUUM operation to reclaim space and optimize the database.
        Raises:
            DbOperationError: If the VACUUM operation fails
        """
        with self._db_engine_lock:
            with self._engine.connect() as conn:
                try:
                    conn.execute(text(_SQL_VACUUM))
                    conn.commit()
                except Exception as e:
                    conn.rollback()
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
        with self._db_engine_lock:
            with self._engine.connect() as conn:
                try:
                    if table_name:
                        conn.execute(text(f"{_SQL_ANALYZE} {table_name}"))
                    else:
                        conn.execute(text(_SQL_ANALYZE))
                    conn.commit()
                except Exception as e:
                    conn.rollback()
                    raise DbOperationError(_ERROR_ANALYZE_FAILED.format(e)) from e

    def integrity_check(self) -> list[str]:
        """
        Perform an integrity check on the database.
        Returns:
            List of integrity issues found (empty list if no issues)
        Raises:
            DbOperationError: If the integrity check fails
        """
        with self._db_engine_lock:
            with self._engine.connect() as conn:
                try:
                    result = conn.execute(text(_SQL_INTEGRITY_CHECK))
                    rows = result.fetchall()
                    issues = [row[0] for row in rows if row[0] != "ok"]
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
        with self._db_engine_lock:
            with self._engine.connect() as conn:
                try:
                    conn.execute(text(_SQL_VACUUM))
                    conn.execute(text(_SQL_ANALYZE))
                    conn.commit()
                except Exception as e:
                    conn.rollback()
                    raise DbOperationError(_ERROR_OPTIMIZATION_FAILED.format(e)) from e
