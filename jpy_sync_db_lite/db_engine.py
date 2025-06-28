"""
This module contains the DbEngine class.

Copyright (c) 2025, Jim Schilling

Please keep this header when you use this code.

This module is licensed under the MIT License.
"""
import queue
import threading
from typing import Dict, List, Optional, Union, Any, Generator
from contextlib import contextmanager
import time
import logging
import queue
from sqlalchemy import create_engine, text, Connection
from sqlalchemy.pool import StaticPool

from jpy_sync_db_lite.db_request import DbRequest
from jpy_sync_db_lite.sql_helper import parse_sql_statements, detect_statement_type

_FETCH_STATEMENT = 'fetch'
_EXECUTE_STATEMENT = 'execute'
_BATCH_STATEMENT = 'batch'
_ERROR_STATEMENT = 'error'
_SUCCESS = 'success'
_ERROR = 'error'


class DbOperationError(Exception):
    """
    Exception raised when a database operation fails.
    """
    pass


class SQLiteError(Exception):
    """
    SQLite-specific exception with error code and message.
    """
    def __init__(self, error_code: int, message: str):
        self.error_code = error_code
        self.message = message
        super().__init__(f"SQLite error {error_code}: {message}")


class DbEngine:
    def __init__(self, database_url: str, **kwargs) -> None:
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
                'check_same_thread': kwargs.get('check_same_thread', False),
                'timeout': kwargs.get('timeout', 30),
                'isolation_level': 'DEFERRED',  # Enable proper transaction support
            },
            echo=kwargs.get('debug', False)
        )
        
        # Configure database for performance
        self._configure_db_performance()
        
        # Queue and threading with locks for thread safety
        self.request_queue = queue.Queue()  # Regular queue for request ordering (thread-safe)
        self.stats_lock = threading.Lock()  # Lock for stats updates
        self.db_engine_lock = threading.RLock()  # Lock to prevent concurrent operations
        self.shutdown_event = threading.Event()
        self.stats = {'requests': 0, 'errors': 0}
        
        # Start worker threads
        self.num_workers = kwargs.get('num_workers', 1)
        self.workers = []
        for i in range(self.num_workers):
            worker = threading.Thread(target=self._worker, daemon=True, name=f"DB-Worker-{i}")
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
            conn.execute(text("PRAGMA journal_mode=WAL"))
            
            # Performance pragmas
            conn.execute(text("PRAGMA synchronous=NORMAL"))   # Faster than FULL
            conn.execute(text("PRAGMA cache_size=-64000"))    # 64MB cache
            conn.execute(text("PRAGMA temp_store=MEMORY"))    # Keep temp tables in RAM
            conn.execute(text("PRAGMA mmap_size=268435456"))  # 256MB memory map
            conn.execute(text("PRAGMA optimize"))             # Query planner optimization
            
            # Additional SQLite optimizations
            conn.execute(text("PRAGMA foreign_keys=ON"))      # Enable foreign key constraints
            conn.execute(text("PRAGMA busy_timeout=30000"))   # 30 second busy timeout
            conn.execute(text("PRAGMA auto_vacuum=INCREMENTAL"))  # Incremental vacuum for space efficiency
            
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
                    self.stats['requests'] += 1
                
                # Execute immediately
                self._execute_single_request_with_connection(request)
                
            except queue.Empty:
                continue
            except Exception as e:
                logging.exception(f"Worker error on request.")
                with self.stats_lock:
                    self.stats['errors'] += 1
    
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
            result = conn.execute(text(request.query), request.params or {})
            rows = result.fetchall()
            if request.response_queue:
                request.response_queue.put((_SUCCESS, [dict(row._mapping) for row in rows]))
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
                raise DbOperationError(f"Commit failed: {commit_error}")
            if request.response_queue:
                result = len(request.params) if isinstance(request.params, list) else True
                request.response_queue.put((_SUCCESS, result))
        elif request.operation == _BATCH_STATEMENT:
            results = self._execute_batch_statements(conn, request.query, request.params['allow_select'])
            if request.response_queue:
                request.response_queue.put((_SUCCESS, results))
        else:
            raise ValueError(f"Unsupported operation type: {request.operation}")
    
    def _execute_batch_statements(self, conn: Connection, batch_sql: str, allow_select: bool = True) -> List[Dict[str, Any]]:
        """
        Execute a batch of SQL statements and return results for each.
        
        Args:
            conn: Database connection
            batch_sql: SQL string containing multiple statements
            allow_select: If False, raises an error when SELECT statements are found
            
        Returns:
            List of results for each statement
        Raises:
            ValueError: If allow_select is False and a SELECT statement is found
        """
        statements = parse_sql_statements(batch_sql)
        results = []
        
        for i, stmt in enumerate(statements):
            try:
                # Use detect_statement_type to determine operation type
                operation_type = detect_statement_type(stmt)
                
                if operation_type == _FETCH_STATEMENT:
                    if not allow_select:
                        raise ValueError(f"SELECT statements are not allowed in batch mode. Found: {stmt}")
                    # Execute as fetch operation
                    result = conn.execute(text(stmt))
                    rows = result.fetchall()
                    results.append({
                        'statement_index': i,
                        'statement': stmt,
                        'type': _FETCH_STATEMENT,
                        'result': [dict(row._mapping) for row in rows],
                        'row_count': len(rows)
                    })
                else:
                    # Execute as non-SELECT operation
                    result = conn.execute(text(stmt))
                    results.append({
                        'statement_index': i,
                        'statement': stmt,
                        'type': _EXECUTE_STATEMENT,
                        'result': True,
                        'row_count': result.rowcount if hasattr(result, 'rowcount') else None
                    })
            except Exception as e:
                # If this is a SELECT error due to allow_select, raise immediately
                if not allow_select and 'SELECT statements are not allowed' in str(e):
                    raise
                results.append({
                    'statement_index': i,
                    'statement': stmt,
                    'type': _ERROR_STATEMENT,
                    'error': str(e)
                })
                # Continue with next statement instead of failing entire batch
                continue
        
        # Commit all changes
        try:
            conn.commit()
        except Exception as commit_error:
            # Rollback on commit failure
            conn.rollback()
            raise DbOperationError(f"Batch commit failed: {commit_error}")
        return results
    
    @contextmanager
    def _db_engine_lock(self):
        """
        Context manager for database engine lock to prevent concurrent operations.
        """
        self.db_engine_lock.acquire()
        try:
            yield
        finally:
            self.db_engine_lock.release()

    def get_stats(self) -> Dict[str, Any]:
        """
        Get performance statistics with thread safety.
        
        Returns:
            Dictionary containing performance statistics:
                - 'requests': Total number of requests processed
                - 'errors': Total number of errors encountered
                - 'queue_size': Current size of request queue
        """
        with self.stats_lock:
            stats_copy = self.stats.copy()
        
        return {
            **stats_copy,
            'queue_size': self.request_queue.qsize(),
        }
    
    def get_sqlite_info(self) -> Dict[str, Any]:
        """
        Get SQLite-specific information and statistics.
        
        Returns:
            Dictionary containing SQLite information:
                - 'version': SQLite version
                - 'database_size': Database file size in bytes
                - 'page_count': Number of pages in database
                - 'page_size': Page size in bytes
                - 'cache_size': Current cache size
                - 'journal_mode': Current journal mode
                - 'synchronous': Current synchronous mode
                - 'temp_store': Current temp store mode
        """
        with self._db_engine_lock():
            with self.engine.connect() as conn:
                # Get SQLite version
                version_result = conn.execute(text("SELECT sqlite_version() as version"))
                version = version_result.fetchone()[0]
                
                # Get database statistics
                pragmas = [
                    'page_count', 'page_size', 'cache_size', 'journal_mode',
                    'synchronous', 'temp_store', 'mmap_size', 'busy_timeout'
                ]
                
                pragma_values = {}
                for pragma in pragmas:
                    try:
                        result = conn.execute(text(f"PRAGMA {pragma}"))
                        value = result.fetchone()[0]
                        pragma_values[pragma] = value
                    except Exception:
                        pragma_values[pragma] = None
                
                # Get database file size if possible
                try:
                    import os
                    db_path = self.engine.url.database
                    if db_path and os.path.exists(db_path):
                        database_size = os.path.getsize(db_path)
                    else:
                        database_size = None
                except Exception:
                    database_size = None
                
                return {
                    'version': version,
                    'database_size': database_size,
                    **pragma_values
                }
    
    def shutdown(self) -> None:
        """
        Clean shutdown of all threads and database connections.
        
        Stops all worker threads and disposes of database engine connections.
        """
        self.shutdown_event.set()
        
        for worker in self.workers:
            worker.join(timeout=5)
        
        self.engine.dispose()

    def execute(self, query: str, params: Optional[Union[Dict[str, Any], List[Dict[str, Any]]]] = None) -> Union[bool, int]:
        """
        Execute a query (INSERT, UPDATE, DELETE, or any non-SELECT statement).
        Args:
            query: SQL query string to execute
            params: Query parameters as dict (single operation) or list of dicts (bulk operations)
        Returns:
            True for single operations, number of records for bulk operations
        Raises:
            Exception: If query execution fails
        """
        response_queue = queue.Queue()
        request = DbRequest(_EXECUTE_STATEMENT, query, params, response_queue)
        self.request_queue.put(request)
        status, result = response_queue.get()
        if status == _ERROR:
            raise DbOperationError(result)
        return result

    def fetch(self, query: str, params: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        """
        Fetch data with thread safety.
        Args:
            query: SQL SELECT query string
            params: Query parameters as dictionary
        Returns:
            List of dictionaries representing query results
        Raises:
            Exception: If query execution fails
        """
        response_queue = queue.Queue()
        request = DbRequest(_FETCH_STATEMENT, query, params, response_queue)
        self.request_queue.put(request)
        status, result = response_queue.get()
        if status == _ERROR:
            raise DbOperationError(result)
        return result

    def batch(self, batch_sql: str, allow_select: bool = True) -> List[Dict[str, Any]]:
        """
        Execute multiple SQL statements in a batch with thread safety.
        
        Args:
            batch_sql: SQL string containing multiple statements separated by semicolons.
                      Supports both DDL (CREATE, ALTER, DROP) and DML (INSERT, UPDATE, DELETE) statements.
                      Comments (-- and /* */) are automatically removed.
            allow_select: If True, allows SELECT statements in the batch (default: True).
                         If False, raises an error if SELECT statements are found.
                         Note: SELECT statements in batches may have different performance characteristics
                         than dedicated fetch() operations.
                      
        Returns:
            List of dictionaries containing results for each statement:
                - 'statement_index': Index of the statement in the batch
                - 'statement': The actual SQL statement executed
                - 'type': 'fetch', 'execute', or 'error'
                - 'result': Query results (for SELECT) or True (for other operations)
                - 'row_count': Number of rows affected/returned
                - 'error': Error message (only for failed statements)
                
        Raises:
            Exception: If the batch operation fails entirely
            ValueError: If SELECT statements are found and allow_select=False
            
        Example:
            batch_sql = '''
                -- Create a table
                CREATE TABLE IF NOT EXISTS users (
                    id INTEGER PRIMARY KEY,
                    name TEXT NOT NULL
                );
                
                -- Insert some data
                INSERT INTO users (name) VALUES ('John');
                INSERT INTO users (name) VALUES ('Jane');
                
                -- Query the data (only if allow_select=True)
                SELECT * FROM users;
            '''
            results = db.batch(batch_sql)
        """
        response_queue = queue.Queue()
        request = DbRequest(_BATCH_STATEMENT, batch_sql, {'allow_select': allow_select}, response_queue)
        self.request_queue.put(request)
        status, result = response_queue.get()
        if status == _ERROR:
            raise DbOperationError(result)
        return result

    def execute_transaction(self, operations: List[Dict[str, Any]]) -> List[Any]:
        """
        Execute multiple operations in a single transaction for data consistency.
        Args:
            operations: List of operation dictionaries with keys:
                - 'operation': Operation type ('fetch' or 'execute')
                - 'query': SQL query string
                - 'params': Query parameters (optional)
        Returns:
            List of results for each operation
        Raises:
            Exception: If any operation in the transaction fails
        """
        with self._db_engine_lock():
            with self.engine.connect() as conn:
                trans = conn.begin()
                try:
                    results = []
                    for op in operations:
                        if 'operation' not in op:
                            raise ValueError("Operation type is required")
                        if op['operation'] == _FETCH_STATEMENT:
                            result = conn.execute(text(op['query']), op.get('params', {}))
                            results.append([dict(row._mapping) for row in result.fetchall()])
                        elif op['operation'] == _EXECUTE_STATEMENT:
                            params = op.get('params', {})
                            if isinstance(params, list):
                                conn.execute(text(op['query']), params)
                                results.append(len(params))
                            else:
                                conn.execute(text(op['query']), params)
                                results.append(True)
                        else:
                            raise ValueError(f"Unsupported operation type: {op['operation']}")
                    trans.commit()
                    return results
                except Exception as e:
                    trans.rollback()
                    raise e

    @contextmanager
    def get_raw_connection(self) -> Generator[Connection, None, None]:
        """
        Get a raw database connection for complex operations.
        This is a low-level method that allows direct access to the database connection.
        It locks the database engine for the duration of the context manager.
        It is only intended for use in special cases where the DbEngine API is not sufficient.
        Yields:
            SQLAlchemy database connection
        Example:
            with db.get_raw_connection() as conn:
                result = conn.execute(text("SELECT * FROM table"))
        """
        with self._db_engine_lock():
            conn = self.engine.connect()
            try:
                yield conn
            finally:
                conn.close()

    def vacuum(self) -> None:
        """
        Perform SQLite VACUUM operation to reclaim space and optimize database.
        Note: VACUUM requires exclusive access to the database.
        Note: VACUUM does not support mode parameters.
        """
        with self._db_engine_lock():
            with self.engine.connect() as conn:
                try:
                    conn.execute(text("VACUUM"))
                    conn.commit()
                except Exception as e:
                    conn.rollback()
                    raise DbOperationError(f"VACUUM operation failed: {e}")
    
    def analyze(self, table_name: Optional[str] = None) -> None:
        """
        Update SQLite query planner statistics for better query performance.
        
        Args:
            table_name: Specific table to analyze (None for all tables)
        """
        with self._db_engine_lock():
            with self.engine.connect() as conn:
                try:
                    if table_name:
                        conn.execute(text(f"ANALYZE {table_name}"))
                    else:
                        conn.execute(text("ANALYZE"))
                    conn.commit()
                except Exception as e:
                    conn.rollback()
                    raise DbOperationError(f"ANALYZE operation failed: {e}")
    
    def integrity_check(self) -> List[str]:
        """
        Perform SQLite integrity check and return any issues found.
        
        Returns:
            List of integrity issues (empty list if database is healthy)
        """
        with self._db_engine_lock():
            with self.engine.connect() as conn:
                try:
                    result = conn.execute(text("PRAGMA integrity_check"))
                    rows = result.fetchall()
                    
                    issues = []
                    for row in rows:
                        if row[0] != 'ok':
                            issues.append(row[0])
                    
                    return issues
                except Exception as e:
                    raise DbOperationError(f"Integrity check failed: {e}")
    
    def optimize(self) -> None:
        """
        Run SQLite optimization commands for better performance.
        """
        with self._db_engine_lock():
            with self.engine.connect() as conn:
                try:
                    # Run optimize pragma
                    conn.execute(text("PRAGMA optimize"))
                    
                    # Update statistics
                    conn.execute(text("ANALYZE"))
                    
                    conn.commit()
                except Exception as e:
                    conn.rollback()
                    raise DbOperationError(f"Optimization operation failed: {e}")


# Usage examples
if __name__ == "__main__":
    # Initialize with SQLite-specific settings
    db = DbEngine("sqlite:///myapp.db", timeout=60, check_same_thread=False)
    
    # Get SQLite information
    sqlite_info = db.get_sqlite_info()
    print(f"SQLite version: {sqlite_info['version']}")
    print(f"Database size: {sqlite_info['database_size']} bytes")
    print(f"Journal mode: {sqlite_info['journal_mode']}")
    
    # Configure custom pragma settings
    db.configure_pragma('cache_size', '-128000')  # 128MB cache
    db.configure_pragma('synchronous', 'NORMAL')   # Balance between speed and safety
    
    # Read operation
    users = db.fetch("SELECT * FROM users WHERE active = :active", 
                     {"active": True})
    
    # Single operations using execute
    db.execute("INSERT INTO users (name, email) VALUES (:name, :email)", 
               {"name": "John Doe", "email": "john@example.com"})
    
    # Bulk operations using execute
    user_data = [{"name": f"User{i}", "email": f"user{i}@example.com"} for i in range(1000)]
    count = db.execute("INSERT INTO users (name, email) VALUES (:name, :email)", user_data)
    print(f"Inserted {count} users")
    
    # Bulk update using execute
    update_data = [{"id": i, "last_login": time.time()} for i in range(1, 101)]
    count = db.execute("UPDATE users SET last_login = :last_login WHERE id = :id", update_data)
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
    for result in batch_results:
        print(f"Statement {result['statement_index']}: {result['type']} - {result.get('row_count', 'N/A')} rows")
    
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
    ddl_results = db.batch(ddl_dml_batch, allow_select=False)
    print(f"DDL/DML batch executed {len(ddl_results)} statements")
    
    # Query separately for verification
    events = db.fetch("SELECT * FROM events ORDER BY created_at DESC LIMIT 3")
    print(f"Found {len(events)} events")
    
    # Transaction for complex operations
    operations = [
        {"operation": _EXECUTE_STATEMENT, "query": "UPDATE users SET last_login = :now WHERE id = :id", 
         "params": {"now": time.time(), "id": 1}},
        {"operation": _FETCH_STATEMENT, "query": "SELECT COUNT(*) as count FROM users"}
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
    # db.vacuum('FULL')  # Uncomment if needed
    
    db.shutdown()