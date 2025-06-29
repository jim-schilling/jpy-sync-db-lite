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
from typing import Any

from sqlalchemy import Connection, create_engine, text
from sqlalchemy.pool import StaticPool

from jpy_sync_db_lite.db_request import DbRequest
from jpy_sync_db_lite.sql_helper import detect_statement_type, parse_sql_statements

# Private module-level constants for SQL commands and operations
_FETCH_STATEMENT = "fetch"
_EXECUTE_STATEMENT = "execute"
_BATCH_STATEMENT = "batch"
_ERROR_STATEMENT = "error"
_SUCCESS = "success"
_ERROR = "error"

# SQLite maintenance commands
_SQL_VACUUM = "VACUUM"
_SQL_ANALYZE = "ANALYZE"
_SQL_INTEGRITY_CHECK = "PRAGMA integrity_check"
_SQL_SQLITE_VERSION = "SELECT sqlite_version()"

# SQLite PRAGMA commands
_SQL_PRAGMA_JOURNAL_MODE = "PRAGMA journal_mode=WAL"
_SQL_PRAGMA_SYNCHRONOUS = "PRAGMA synchronous=NORMAL"
_SQL_PRAGMA_CACHE_SIZE = "PRAGMA cache_size=-64000"
_SQL_PRAGMA_TEMP_STORE = "PRAGMA temp_store=MEMORY"
_SQL_PRAGMA_MMAP_SIZE = "PRAGMA mmap_size=268435456"
_SQL_PRAGMA_OPTIMIZE = "PRAGMA optimize"
_SQL_PRAGMA_FOREIGN_KEYS = "PRAGMA foreign_keys=ON"
_SQL_PRAGMA_BUSY_TIMEOUT = "PRAGMA busy_timeout=30000"
_SQL_PRAGMA_AUTO_VACUUM = "PRAGMA auto_vacuum=INCREMENTAL"

# SQLite info PRAGMA commands
_SQL_PRAGMA_PAGE_COUNT = "PRAGMA page_count"
_SQL_PRAGMA_PAGE_SIZE = "PRAGMA page_size"
_SQL_PRAGMA_JOURNAL_MODE_INFO = "PRAGMA journal_mode"
_SQL_PRAGMA_SYNCHRONOUS_INFO = "PRAGMA synchronous"
_SQL_PRAGMA_CACHE_SIZE_INFO = "PRAGMA cache_size"
_SQL_PRAGMA_TEMP_STORE_INFO = "PRAGMA temp_store"

# Error messages
_ERROR_VACUUM_FAILED = "VACUUM operation failed: {}"
_ERROR_ANALYZE_FAILED = "ANALYZE operation failed: {}"
_ERROR_INTEGRITY_CHECK_FAILED = "Integrity check failed: {}"
_ERROR_OPTIMIZATION_FAILED = "Optimization operation failed: {}"
_ERROR_COMMIT_FAILED = "Commit failed: {}"
_ERROR_BATCH_COMMIT_FAILED = "Batch commit failed: {}"
_ERROR_TRANSACTION_FAILED = "Transaction failed: {}"
_ERROR_EXECUTE_FAILED = "Execute failed: {}"
_ERROR_FETCH_FAILED = "Fetch failed: {}"
_ERROR_BATCH_FAILED = "Batch failed: {}"


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
        self.error_code = error_code
        self.message = message
        super().__init__(f"SQLite error {error_code}: {message}")


class DbEngine:
    def __init__(self, database_url: str, **kwargs: Any) -> None:
        """
        Initialize the DbEngine with database connection and threading configuration.

        Args:
            database_url: SQLAlchemy database URL (e.g., 'sqlite:///database.db')
            **kwargs: Additional configuration options:
                - num_workers: Number of worker threads (default: 1)
                - debug: Enable SQLAlchemy echo mode (default: False)
                - timeout: SQLite connection timeout in seconds (default: 30)
                - check_same_thread: SQLite thread safety check (default: False)
        """
        # SQLite-specific engine configuration
        self.engine = create_engine(
            database_url,
            poolclass=StaticPool,
            connect_args={
                "check_same_thread": kwargs.get("check_same_thread", False),
                "timeout": kwargs.get("timeout", 30),
                "isolation_level": "DEFERRED",  # Enable proper transaction support
            },
            echo=kwargs.get("debug", False),
        )

        # Configure database for performance
        self._configure_db_performance()

        # Queue and threading with locks for thread safety
        self.request_queue: queue.Queue[DbRequest] = queue.Queue()
        self.stats_lock = threading.Lock()  # Lock for stats updates
        self.db_engine_lock = threading.RLock()  # Lock to prevent concurrent operations
        self.shutdown_event = threading.Event()
        self.stats = {"requests": 0, "errors": 0}

        # Start worker threads
        self.num_workers = kwargs.get("num_workers", 1)
        self.workers = []
        for i in range(self.num_workers):
            worker = threading.Thread(
                target=self._worker, daemon=True, name=f"DB-Worker-{i}"
            )
            worker.start()
            self.workers.append(worker)

    def _configure_db_performance(self) -> None:
        """
        Apply performance optimizations to the database.

        Configures SQLite-specific pragmas for optimal performance:
        - WAL mode for better concurrency
        - Optimized cache and memory settings
        - Query planner optimization
        - Additional performance tuning
        """
        with self.engine.connect() as conn:
            # Enable WAL mode for better concurrency
            conn.execute(text(_SQL_PRAGMA_JOURNAL_MODE))

            # Performance pragmas
            conn.execute(text(_SQL_PRAGMA_SYNCHRONOUS))  # Faster than FULL
            conn.execute(text(_SQL_PRAGMA_CACHE_SIZE))  # 64MB cache
            conn.execute(text(_SQL_PRAGMA_TEMP_STORE))  # Keep temp tables in RAM
            conn.execute(text(_SQL_PRAGMA_MMAP_SIZE))  # 256MB memory map
            conn.execute(text(_SQL_PRAGMA_OPTIMIZE))  # Query planner optimization

            # Additional SQLite optimizations
            conn.execute(
                text(_SQL_PRAGMA_FOREIGN_KEYS)
            )  # Enable foreign key constraints
            conn.execute(text(_SQL_PRAGMA_BUSY_TIMEOUT))  # 30 second busy timeout
            conn.execute(
                text(_SQL_PRAGMA_AUTO_VACUUM)
            )  # Incremental vacuum for space efficiency

            conn.commit()

    def configure_pragma(self, pragma_name: str, value: str) -> None:
        """
        Configure a specific SQLite PRAGMA setting.

        Args:
            pragma_name: Name of the PRAGMA (e.g., 'cache_size', 'synchronous')
            value: Value to set for the PRAGMA

        Example:
            db.configure_pragma('cache_size', '-128000')  # 128MB cache
            db.configure_pragma('synchronous', 'OFF')     # Fastest sync mode
        """
        with self._db_engine_lock():
            with self.engine.connect() as conn:
                conn.execute(text(f"PRAGMA {pragma_name}={value}"))
                conn.commit()

    def _worker(self) -> None:
        """
        Main worker thread for processing database requests.

        Continuously processes requests from the queue and executes them immediately.
        """
        while not self.shutdown_event.is_set():
            try:
                # Get request with timeout
                request = self.request_queue.get(timeout=1)

                # Thread-safe stats update
                with self.stats_lock:
                    self.stats["requests"] += 1

                # Execute immediately
                self._execute_single_request_with_connection(request)

            except queue.Empty:
                continue
            except Exception:
                logging.exception("Worker error on request.")
                with self.stats_lock:
                    self.stats["errors"] += 1

    def _execute_single_request_with_connection(self, request: DbRequest) -> None:
        """
        Outer handler for a single database request.
        - Manages thread safety by acquiring the engine lock.
        - Creates a new database connection for the request.
        - Delegates actual SQL execution to _execute_single_request.
        - Handles response queue for success/error.
        """
        try:
            with self._db_engine_lock():
                with self.engine.connect() as conn:
                    self._execute_single_request(conn, request)
                    if request.response_queue:
                        request.response_queue.put((_SUCCESS, True))
        except Exception as e:
            if request.response_queue:
                request.response_queue.put((_ERROR, str(e)))

    def _execute_single_request(self, conn: Connection, request: DbRequest) -> None:
        """
        Inner handler for executing a single database request using a provided connection.
        - Does NOT manage connection or locking; expects caller to handle that.
        - Executes the SQL operation (fetch, execute, or batch) and puts results/errors in the response queue.
        """
        if request.operation == _FETCH_STATEMENT:
            fetch_result = conn.execute(text(request.query), request.params or {})
            rows = fetch_result.fetchall()
            if request.response_queue:
                request.response_queue.put(
                    (_SUCCESS, [dict(row._mapping) for row in rows])
                )
        elif request.operation == _EXECUTE_STATEMENT:
            if isinstance(request.params, list):
                conn.execute(text(request.query), request.params)
            else:
                conn.execute(text(request.query), request.params or {})
            try:
                conn.commit()
            except Exception as commit_error:
                # Rollback on commit failure
                conn.rollback()
                raise DbOperationError(_ERROR_COMMIT_FAILED.format(commit_error)) from commit_error
            if request.response_queue:
                execute_result: bool | int = (
                    len(request.params) if isinstance(request.params, list) else True
                )
                request.response_queue.put((_SUCCESS, execute_result))
        elif request.operation == _BATCH_STATEMENT:
            batch_results = self._execute_batch_statements(conn, request.query)
            if request.response_queue:
                request.response_queue.put((_SUCCESS, batch_results))
        else:
            raise ValueError(f"Unsupported operation type: {request.operation}")

    def _execute_batch_statements(
        self, conn: Connection, batch_sql: str
    ) -> list[dict[str, Any]]:
        """
        Execute a batch of SQL statements and return results for each.

        Args:
            conn: Database connection
            batch_sql: SQL string containing multiple statements

        Returns:
            List of results for each statement
        """
        statements = parse_sql_statements(batch_sql)
        results: list[dict[str, Any]] = []

        for i, stmt in enumerate(statements):
            try:
                # Use detect_statement_type to determine operation type
                stmt_type = detect_statement_type(stmt)

                if stmt_type == _FETCH_STATEMENT:
                    # Execute fetch statement
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
                    # Execute non-fetch statement
                    conn.execute(text(stmt))
                    results.append(
                        {
                            "statement": stmt,
                            "operation": _EXECUTE_STATEMENT,
                            "result": True
                        }
                    )

            except Exception as e:
                # Rollback on any error
                conn.rollback()
                results.append(
                    {
                        "statement": stmt,
                        "operation": _ERROR_STATEMENT,
                        "error": str(e),
                    }
                )
                # Re-raise to stop processing
                raise

        # Commit all statements if no errors
        try:
            conn.commit()
        except Exception as commit_error:
            # Rollback on commit failure
            conn.rollback()
            raise DbOperationError(_ERROR_BATCH_COMMIT_FAILED.format(commit_error)) from commit_error
        return results

    @contextmanager
    def _db_engine_lock(self) -> Generator[None, None, None]:
        """
        Context manager for acquiring the database engine lock.

        This ensures thread safety by preventing concurrent database operations.
        """
        with self.db_engine_lock:
            yield

    def get_stats(self) -> dict[str, Any]:
        """
        Get current statistics about database operations.

        Returns:
            Dictionary containing request and error counts
        """
        with self.stats_lock:
            return self.stats.copy()

    def get_sqlite_info(self) -> dict[str, Any]:
        """
        Get SQLite-specific information about the database.

        Returns:
            Dictionary containing SQLite version, database size, and other info
        """
        with self._db_engine_lock():
            with self.engine.connect() as conn:
                # Get SQLite version
                version_result = conn.execute(text(_SQL_SQLITE_VERSION))
                sqlite_version = version_result.scalar()

                # Get database file size
                pragma_result = conn.execute(text(_SQL_PRAGMA_PAGE_COUNT))
                page_count = pragma_result.scalar()
                pragma_result = conn.execute(text(_SQL_PRAGMA_PAGE_SIZE))
                page_size = pragma_result.scalar()
                database_size = page_count * page_size if page_count and page_size else None

                # Get additional info
                pragma_result = conn.execute(text(_SQL_PRAGMA_JOURNAL_MODE_INFO))
                journal_mode = pragma_result.scalar()
                pragma_result = conn.execute(text(_SQL_PRAGMA_SYNCHRONOUS_INFO))
                synchronous = pragma_result.scalar()
                pragma_result = conn.execute(text(_SQL_PRAGMA_CACHE_SIZE_INFO))
                cache_size = pragma_result.scalar()
                pragma_result = conn.execute(text(_SQL_PRAGMA_TEMP_STORE_INFO))
                temp_store = pragma_result.scalar()

                return {
                    "version": sqlite_version,  # Alias for backward compatibility
                    "sqlite_version": sqlite_version,
                    "database_size": database_size,
                    "page_count": page_count,
                    "page_size": page_size,
                    "cache_size": cache_size,
                    "journal_mode": journal_mode,
                    "synchronous": synchronous,
                    "temp_store": temp_store,
                }

    def shutdown(self) -> None:
        """
        Shutdown the database engine and cleanup resources.

        This method:
        1. Sets the shutdown event to stop worker threads
        2. Waits for all worker threads to complete
        3. Closes the database engine
        """
        # Signal shutdown to worker threads
        self.shutdown_event.set()

        # Wait for all worker threads to complete
        for worker in self.workers:
            worker.join(timeout=5)  # 5 second timeout per worker
            if worker.is_alive():
                logging.warning(f"Worker thread {worker.name} did not complete within 5 seconds. Forcing shutdown.")
                worker.terminate()

        # Close the engine
        self.engine.dispose()

    def execute(
        self,
        query: str,
        params: dict[str, Any] | list[dict[str, Any]] | None = None,
    ) -> bool | int:
        """
        Execute a SQL statement that doesn't return rows.

        Args:
            query: SQL statement to execute
            params: Parameters for the SQL statement (optional)

        Returns:
            True for single operations, number of affected rows for bulk operations

        Raises:
            DbOperationError: If the database operation fails
        """
        response_queue: queue.Queue[tuple[str, Any]] = queue.Queue()
        request = DbRequest(
            operation=_EXECUTE_STATEMENT,
            query=query,
            params=params,
            response_queue=response_queue,
        )

        self.request_queue.put(request)
        status, result = response_queue.get()

        if status == _ERROR:
            raise DbOperationError(_ERROR_EXECUTE_FAILED.format(result))
        if isinstance(result, bool):
            return result
        elif isinstance(result, int):
            return result
        else:
            return True

    def fetch(
        self, query: str, params: dict[str, Any] | None = None
    ) -> list[dict[str, Any]]:
        """
        Execute a SQL query that returns rows.

        Args:
            query: SQL query to execute
            params: Parameters for the SQL query (optional)

        Returns:
            List of dictionaries representing the result rows

        Raises:
            DbOperationError: If the database operation fails
        """
        response_queue: queue.Queue[tuple[str, Any]] = queue.Queue()
        request = DbRequest(
            operation=_FETCH_STATEMENT,
            query=query,
            params=params,
            response_queue=response_queue,
        )

        self.request_queue.put(request)
        status, result = response_queue.get()

        if status == _ERROR:
            raise DbOperationError(_ERROR_FETCH_FAILED.format(result))
        return result if isinstance(result, list) else []

    def batch(self, batch_sql: str) -> list[dict[str, Any]]:
        """
        Execute a batch of SQL statements.

        Args:
            batch_sql: SQL string containing multiple statements

        Returns:
            List of results for each statement

        Raises:
            DbOperationError: If the database operation fails
        """
        response_queue: queue.Queue[tuple[str, Any]] = queue.Queue()
        request = DbRequest(
            operation=_BATCH_STATEMENT,
            query=batch_sql,
            params={},
            response_queue=response_queue,
        )

        self.request_queue.put(request)
        status, result = response_queue.get()

        if status == _ERROR:
            raise DbOperationError(_ERROR_BATCH_FAILED.format(result))
        return result if isinstance(result, list) else []

    def execute_transaction(self, operations: list[dict[str, Any]]) -> list[dict[str, Any]]:
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
            DbOperationError: If the transaction fails
        """
        with self._db_engine_lock():
            with self.engine.connect() as conn:
                results: list[dict[str, Any]] = []
                try:
                    for operation in operations:
                        op_type = operation.get("operation", _EXECUTE_STATEMENT)
                        query = operation["query"]
                        params = operation.get("params")
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

        This context manager provides direct access to the SQLAlchemy connection
        for operations that require more control than the standard methods.

        Yields:
            SQLAlchemy Connection object

        Example:
            with db.get_raw_connection() as conn:
                result = conn.execute(text("SELECT * FROM users"))
                rows = result.fetchall()
        """
        with self._db_engine_lock():
            with self.engine.connect() as conn:
                yield conn

    def vacuum(self) -> None:
        """
        Perform a VACUUM operation to reclaim space and optimize the database.

        VACUUM rebuilds the database file, reclaiming unused space and
        defragmenting the data. This can significantly improve performance
        and reduce file size.

        Raises:
            DbOperationError: If the VACUUM operation fails
        """
        with self._db_engine_lock():
            with self.engine.connect() as conn:
                try:
                    conn.execute(text(_SQL_VACUUM))
                    conn.commit()
                except Exception as e:
                    conn.rollback()
                    raise DbOperationError(_ERROR_VACUUM_FAILED.format(e)) from e

    def analyze(self, table_name: str | None = None) -> None:
        """
        Perform an ANALYZE operation to update query planner statistics.

        Args:
            table_name: Specific table to analyze, or None for all tables

        Raises:
            DbOperationError: If the ANALYZE operation fails
        """
        with self._db_engine_lock():
            with self.engine.connect() as conn:
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
        with self._db_engine_lock():
            with self.engine.connect() as conn:
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

        This method combines VACUUM and ANALYZE operations to optimize
        the database for better performance.

        Raises:
            DbOperationError: If the optimization operation fails
        """
        with self._db_engine_lock():
            with self.engine.connect() as conn:
                try:
                    conn.execute(text(_SQL_VACUUM))
                    conn.execute(text(_SQL_ANALYZE))
                    conn.commit()
                except Exception as e:
                    conn.rollback()
                    raise DbOperationError(_ERROR_OPTIMIZATION_FAILED.format(e)) from e


# Usage examples
if __name__ == "__main__":
    # Initialize with SQLite-specific settings
    db = DbEngine("sqlite:///myapp.db", timeout=60, check_same_thread=False)

    # Get SQLite information
    sqlite_info = db.get_sqlite_info()
    print(f"SQLite version: {sqlite_info['sqlite_version']}")
    print(f"Database size: {sqlite_info['database_size']} bytes")
    print(f"Journal mode: {sqlite_info['journal_mode']}")

    # Configure custom pragma settings
    db.configure_pragma("cache_size", "-128000")  # 128MB cache
    db.configure_pragma("synchronous", "NORMAL")  # Balance between speed and safety

    # Read operation
    users = db.fetch("SELECT * FROM users WHERE active = :active", {"active": True})

    # Single operations using execute
    db.execute(
        "INSERT INTO users (name, email) VALUES (:name, :email)",
        {"name": "John Doe", "email": "john@example.com"},
    )

    # Bulk operations using execute
    user_data = [
        {"name": f"User{i}", "email": f"user{i}@example.com"} for i in range(1000)
    ]
    count = db.execute(
        "INSERT INTO users (name, email) VALUES (:name, :email)", user_data
    )
    print(f"Inserted {count} users")

    # Bulk update using execute
    update_data = [{"id": i, "last_login": time.time()} for i in range(1, 101)]
    count = db.execute(
        "UPDATE users SET last_login = :last_login WHERE id = :id", update_data
    )
    print(f"Updated {count} users")

    # Batch operations using batch
    batch_sql = """
        -- Create a new table
        CREATE TABLE IF NOT EXISTS logs (
            id INTEGER PRIMARY KEY,
            message TEXT,
            timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
        );

        -- Insert some log entries
        INSERT INTO logs (message) VALUES ('Application started');
        INSERT INTO logs (message) VALUES ('User login successful');

        -- Query the logs
        SELECT * FROM logs ORDER BY timestamp DESC LIMIT 5;

        -- Update a log entry
        UPDATE logs SET message = 'Application started successfully' WHERE message = 'Application started';
    """
    batch_results = db.batch(batch_sql)
    print(f"Batch executed {len(batch_results)} statements")
    for i, result in enumerate(batch_results):
        print(
            f"Statement {i}: {result['operation']} - {result.get('row_count', 'N/A')} rows"
        )

    # Batch operations without SELECT statements (DDL/DML only)
    ddl_dml_batch = """
        -- Create a new table
        CREATE TABLE IF NOT EXISTS events (
            id INTEGER PRIMARY KEY,
            event_type TEXT,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP
        );

        -- Insert events
        INSERT INTO events (event_type) VALUES ('user_login');
        INSERT INTO events (event_type) VALUES ('data_export');
        INSERT INTO events (event_type) VALUES ('system_backup');
    """
    ddl_results = db.batch(ddl_dml_batch)
    print(f"DDL/DML batch executed {len(ddl_results)} statements")

    # Query separately for verification
    events = db.fetch("SELECT * FROM events ORDER BY created_at DESC LIMIT 3")
    print(f"Found {len(events)} events")

    # Transaction for complex operations
    operations: list[dict[str, Any]] = [
        {
            "operation": "execute",
            "query": "UPDATE users SET last_login = :now WHERE id = :id",
            "params": {"now": time.time(), "id": 1},
        },
        {"operation": "fetch", "query": "SELECT COUNT(*) as count FROM users"},
    ]
    results = db.execute_transaction(operations)

    # SQLite-specific maintenance operations
    print("Running SQLite maintenance...")

    # Check database integrity
    issues = db.integrity_check()
    if issues:
        print(f"Integrity issues found: {issues}")
    else:
        print("Database integrity check passed")

    # Update query planner statistics
    db.analyze()
    print("Query planner statistics updated")

    # Run optimization
    db.optimize()
    print("Database optimization completed")

    # Performance stats
    print("Performance stats:", db.get_stats())

    # Optional: Run VACUUM for space reclamation (use sparingly)
    # db.vacuum()  # Uncomment if needed

    db.shutdown()
