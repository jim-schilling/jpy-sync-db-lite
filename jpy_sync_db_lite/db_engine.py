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
from sqlalchemy import create_engine, text, Connection
from sqlalchemy.pool import StaticPool

from jpy_sync_db_lite.db_request import DbRequest


class DbEngine:
    def __init__(self, database_url: str, **kwargs) -> None:
        """
        Initialize the DbEngine with database connection and threading configuration.
        
        Args:
            database_url: SQLAlchemy database URL (e.g., 'sqlite:///database.db')
            **kwargs: Additional configuration options:
                - num_workers: Number of worker threads (default: 1)
                - debug: Enable SQLAlchemy echo mode (default: False)
        """
        # Performance-tuned engine configuration
        self.engine = create_engine(
            database_url,
            poolclass=StaticPool,
            connect_args={
                'check_same_thread': False,
                'timeout': 30,
            },
            echo=kwargs.get('debug', False)
        )
        
        # Configure database for performance
        self._configure_db_performance()
        
        # Queue and threading with locks for thread safety
        self.request_queue = queue.Queue()  # Regular queue for request ordering (thread-safe)
        self.stats_lock = threading.Lock()  # Lock for stats updates
        self.shutdown_event = threading.Event()
        self.stats = {'requests': 0, 'errors': 0}
        
        # Start worker threads
        self.num_workers = kwargs.get('num_workers', 1)  # SQLite: stick to 1 for writes
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
                logging.error(f"Worker error: {e}")
                with self.stats_lock:
                    self.stats['errors'] += 1
    
    def _execute_single_request_with_connection(self, request: DbRequest) -> None:
        """
        Execute a single request with its own database connection.
        
        Args:
            request: DbRequest object to execute
        """
        try:
            with self.engine.connect() as conn:
                self._execute_single_request(conn, request)
                
                if request.response_queue:
                    request.response_queue.put(('success', True))
                    
        except Exception as e:
            if request.response_queue:
                request.response_queue.put(('error', str(e)))
    
    def _execute_single_request(self, conn: Connection, request: DbRequest) -> None:
        """
        Execute a single database request based on operation type.
        
        Args:
            conn: SQLAlchemy database connection
            request: DbRequest object to execute
        """
        if request.operation == 'fetch':
            result = conn.execute(text(request.query), request.params or {})
            rows = result.fetchall()
            
            if request.response_queue:
                request.response_queue.put(('success', [dict(row._mapping) for row in rows]))
        
        elif request.operation == 'execute':
            if isinstance(request.params, list):
                conn.execute(text(request.query), request.params)
            else:
                conn.execute(text(request.query), request.params or {})
            # Commit changes for non-fetch operations
            conn.commit()
            
            if request.response_queue:
                # Return count for bulk operations, True for single operations
                result = len(request.params) if isinstance(request.params, list) else True
                request.response_queue.put(('success', result))
        else:
            raise ValueError(f"Unsupported operation type: {request.operation}")
    
    # Public API methods
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
        request = DbRequest('execute', query, params, response_queue)
        
        self.request_queue.put(request)
        
        status, result = response_queue.get()
        if status == 'error':
            raise Exception(result)
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
        request = DbRequest('fetch', query, params, response_queue)
        
        self.request_queue.put(request)
        
        status, result = response_queue.get()
        if status == 'error':
            raise Exception(result)
        return result
    
    def execute_transaction(self, operations: List[Dict[str, Any]]) -> List[Any]:
        """
        Execute multiple operations in a single transaction for data consistency.
        
        Args:
            operations: List of operation dictionaries with keys:
                - 'type': Operation type ('fetch' or 'execute')
                - 'query': SQL query string
                - 'params': Query parameters (optional)
                
        Returns:
            List of results for each operation
            
        Raises:
            Exception: If any operation in the transaction fails
        """
        with self.engine.connect() as conn:
            trans = conn.begin()
            try:
                results = []
                for op in operations:
                    if 'type' not in op:
                        raise ValueError("Operation type is required")
                    if op['type'] == 'fetch':
                        result = conn.execute(text(op['query']), op.get('params', {}))
                        results.append([dict(row._mapping) for row in result.fetchall()])
                    elif op['type'] == 'execute':
                        params = op.get('params', {})
                        if isinstance(params, list):
                            conn.execute(text(op['query']), params)
                            results.append(len(params))  # Return count for bulk operations
                        else:
                            conn.execute(text(op['query']), params)
                            results.append(True)  # Return True for single operations
                    else:
                        raise ValueError(f"Unsupported operation type: {op['type']}")
                
                trans.commit()
                return results
                
            except Exception as e:
                trans.rollback()
                raise e
    
    @contextmanager
    def get_raw_connection(self) -> Generator[Connection, None, None]:
        """
        Get a raw database connection for complex operations.
        
        Yields:
            SQLAlchemy database connection
            
        Example:
            with db.get_raw_connection() as conn:
                # Perform complex operations
                result = conn.execute(text("SELECT * FROM table"))
        """
        conn = self.engine.connect()
        try:
            yield conn
        finally:
            conn.close()
    
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
            'queue_size': self.request_queue.qsize()
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


# Usage examples
if __name__ == "__main__":
    # Initialize with performance settings
    db = DbEngine("sqlite:///myapp.db")
    
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
    
    # Transaction for complex operations
    operations = [
        {"type": "execute", "query": "UPDATE users SET last_login = :now WHERE id = :id", 
         "params": {"now": time.time(), "id": 1}},
        {"type": "fetch", "query": "SELECT COUNT(*) as count FROM users"}
    ]
    results = db.execute_transaction(operations)
    
    # Performance stats
    print("Performance stats:", db.get_stats())
    
    db.shutdown()