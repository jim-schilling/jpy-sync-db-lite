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
import os
import shutil
from sqlalchemy import create_engine, text, Connection
from sqlalchemy.pool import StaticPool

from jpy_sync_db_lite.db_request import DbRequest


class DbOperationError(Exception):
    """
    Exception raised when a database operation fails.
    """
    pass


class DbEngine:
    def __init__(self, database_url: str, **kwargs) -> None:
        """
        Initialize the DbEngine with database connection and threading configuration.
        
        Args:
            database_url: SQLAlchemy database URL (e.g., 'sqlite:///database.db')
            **kwargs: Additional configuration options:
                - num_workers: Number of worker threads (default: 1)
                - debug: Enable SQLAlchemy echo mode (default: False)
                - backup_enabled: Enable periodic backups (default: False)
                - backup_interval: Backup interval in seconds (default: 3600)
                - backup_dir: Directory for backups (default: './backups')
                - backup_cleanup_enabled: Enable backup cleanup (default: True)
                - backup_cleanup_keep_count: Number of recent backups to keep (default: 10)
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
        self.backup_lock = threading.Lock()  # Lock to prevent concurrent backups
        self.db_engine_lock = threading.RLock()  # Lock to prevent operations during backup
        self.shutdown_event = threading.Event()
        self.stats = {'requests': 0, 'errors': 0, 'backups': 0}
        
        # Backup configuration
        self.backup_enabled = kwargs.get('backup_enabled', False)
        self.backup_interval = kwargs.get('backup_interval', 3600)  # 1 hour default
        self.backup_dir = kwargs.get('backup_dir', './backups')
        self.backup_cleanup_enabled = kwargs.get('backup_cleanup_enabled', True)
        self.backup_cleanup_keep_count = kwargs.get('backup_cleanup_keep_count', 10)
        self.last_backup_time = 0
        
        # Start worker threads
        self.num_workers = kwargs.get('num_workers', 1)  # SQLite: stick to 1 for writes
        self.workers = []
        for i in range(self.num_workers):
            worker = threading.Thread(target=self._worker, daemon=True, name=f"DB-Worker-{i}")
            worker.start()
            self.workers.append(worker)
        
        # Start backup thread if enabled
        if self.backup_enabled:
            self.backup_thread = threading.Thread(target=self._backup_worker, daemon=True, name="DB-Backup-Worker")
            self.backup_thread.start()
    
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
                logging.exception(f"Worker error on request.")
                with self.stats_lock:
                    self.stats['errors'] += 1
    
    def _execute_single_request_with_connection(self, request: DbRequest) -> None:
        """
        Execute a single request with its own database connection.
        
        Args:
            request: DbRequest object to execute
        """
        try:
            # Use context manager for database engine lock
            with self._db_engine_lock():
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
    
    def _backup_worker(self) -> None:
        """
        Backup worker thread for periodic checkpoint backups.
        
        Runs independently and creates backups at specified intervals.
        """
        while not self.shutdown_event.is_set():
            try:
                current_time = time.time()
                
                # Check if it's time for a backup
                if current_time - self.last_backup_time >= self.backup_interval:
                    # Use backup lock to prevent concurrent backups
                    try:
                        with self._backup_lock():
                            self._perform_checkpoint_backup()
                            self.last_backup_time = current_time
                    except RuntimeError:
                        logging.debug("Skipping periodic backup - manual backup in progress")
                
                # Sleep for a short interval to avoid busy waiting
                time.sleep(60)  # Check every minute
                
            except Exception as e:
                logging.exception("Backup worker error")
                time.sleep(300)  # Wait 5 minutes on error before retrying
    
    def _perform_checkpoint_backup(self) -> None:
        """
        Perform a proper backup by checkpointing WAL and copying the database file.
        
        This method acquires the database engine lock to prevent any operations
        during the backup process for data consistency.
        """
        # Use context manager for database engine lock
        with self._db_engine_lock():
            # Create backup directory if it doesn't exist
            os.makedirs(self.backup_dir, exist_ok=True)
            
            # Get database file path
            db_path = self.engine.url.database
            if not os.path.isabs(db_path):
                db_path = os.path.abspath(db_path)
            
            # Perform WAL checkpoint to ensure all data is in the main database file
            with self.engine.connect() as conn:
                # Checkpoint WAL to main database file
                conn.execute(text("PRAGMA wal_checkpoint(TRUNCATE)"))
                conn.commit()
            
            # Create timestamped backup filename
            timestamp = time.strftime("%Y%m%d_%H%M%S")
            backup_filename = f"backup_{timestamp}.sqlite"
            backup_path = os.path.join(self.backup_dir, backup_filename)
            
            # Copy the database file after checkpoint
            try:
                shutil.copy2(db_path, backup_path)
            except Exception as e:
                # If direct copy fails, try with a small delay
                time.sleep(0.1)
                shutil.copy2(db_path, backup_path)
            
            # Update stats
            with self.stats_lock:
                self.stats['backups'] += 1
            
            logging.info(f"Checkpoint backup created: {backup_path}")
                        
            if self.backup_cleanup_enabled:
                self._cleanup_old_backups(self.backup_cleanup_keep_count)
    
    def _cleanup_old_backups(self, keep_count: int = 10) -> None:
        """
        Clean up old backup files, keeping only the most recent ones.
        
        Args:
            keep_count: Number of recent backups to keep
        """
        try:
            if not os.path.exists(self.backup_dir):
                return
            
            backup_files = []
            for filename in os.listdir(self.backup_dir):
                if filename.startswith("backup_") and filename.endswith(".sqlite"):
                    filepath = os.path.join(self.backup_dir, filename)
                    try:
                        # Get file modification time with timeout
                        mtime = os.path.getmtime(filepath)
                        backup_files.append((filepath, mtime))
                    except (OSError, IOError) as e:
                        # Skip files that can't be accessed
                        logging.warning(f"Could not access backup file {filepath}: {e}")
                        continue
            
            # Sort by modification time (newest first)
            backup_files.sort(key=lambda x: x[1], reverse=True)
            
            # Remove old backups with timeout and retry logic
            files_to_remove = backup_files[keep_count:]
            for filepath, _ in files_to_remove:
                try:
                    # Use concurrent.futures for better timeout handling
                    import concurrent.futures
                    
                    def remove_file():
                        try:
                            os.remove(filepath)
                            return True
                        except Exception as e:
                            return str(e)
                    
                    # Use ThreadPoolExecutor with timeout
                    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
                        future = executor.submit(remove_file)
                        try:
                            result = future.result(timeout=1.0)  # 1 second timeout
                            if result is True:
                                logging.info(f"Removed old backup: {filepath}")
                            else:
                                logging.warning(f"Failed to remove old backup {filepath}: {result}")
                        except concurrent.futures.TimeoutError:
                            logging.warning(f"Timeout removing backup file: {filepath}")
                            continue
                        except Exception as e:
                            logging.warning(f"Error removing old backup {filepath}: {e}")
                            continue
                        
                except Exception as e:
                    logging.warning(f"Error removing old backup {filepath}: {e}")
                    continue
                    
        except Exception as e:
            logging.warning(f"Backup cleanup failed: {e}")
            # Don't raise the exception - cleanup failure shouldn't break the backup process
    
    @contextmanager
    def _backup_lock(self):
        """
        Context manager for backup lock to prevent concurrent backup operations.
        """
        if not self.backup_lock.acquire(blocking=False):
            raise RuntimeError("Backup already in progress")
        try:
            yield
        finally:
            self.backup_lock.release()

    def request_backup(self) -> bool:
        """
        Request an immediate backup.
        
        Returns:
            True if backup was completed successfully
        """
        try:
            # Use context manager for backup lock
            with self._backup_lock():
                # Perform backup directly instead of going through worker thread
                self._perform_checkpoint_backup()
                self.last_backup_time = time.time()
                return True
                
        except RuntimeError:
            logging.warning("Backup already in progress, request ignored")
            return False
        except Exception as e:
            logging.exception(f"Backup request failed: {e}")
            return False
    
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
        request = DbRequest('fetch', query, params, response_queue)
        
        self.request_queue.put(request)
        
        status, result = response_queue.get()
        if status == 'error':
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
        # Use context manager for database engine lock
        with self._db_engine_lock():
            with self.engine.connect() as conn:
                trans = conn.begin()
                try:
                    results = []
                    for op in operations:
                        if 'operation' not in op:
                            raise ValueError("Operation type is required")
                        if op['operation'] == 'fetch':
                            result = conn.execute(text(op['query']), op.get('params', {}))
                            results.append([dict(row._mapping) for row in result.fetchall()])
                        elif op['operation'] == 'execute':
                            params = op.get('params', {})
                            if isinstance(params, list):
                                conn.execute(text(op['query']), params)
                                results.append(len(params))  # Return count for bulk operations
                            else:
                                conn.execute(text(op['query']), params)
                                results.append(True)  # Return True for single operations
                        else:
                            raise ValueError(f"Unsupported operation type: {op['operation']}")
                    
                    trans.commit()
                    return results
                    
                except Exception as e:
                    logging.exception(f"Transaction failed. Rolling back.", exc_info=True)
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
        # Use context manager for database engine lock
        with self._db_engine_lock():
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
                - 'backups': Total number of backups created
                - 'backup_enabled': Whether backups are enabled
                - 'last_backup_time': Timestamp of last backup
        """
        with self.stats_lock:
            stats_copy = self.stats.copy()
        
        return {
            **stats_copy,
            'queue_size': self.request_queue.qsize(),
            'backup_enabled': self.backup_enabled,
            'backup_interval': self.backup_interval,
            'last_backup_time': self.last_backup_time,
            'backup_dir': self.backup_dir
        }
    
    def get_backup_info(self) -> Dict[str, Any]:
        """
        Get detailed backup information.
        
        Returns:
            Dictionary containing backup information
        """
        backup_info = {
            'enabled': self.backup_enabled,
            'interval_seconds': self.backup_interval,
            'directory': self.backup_dir,
            'last_backup': self.last_backup_time,
            'total_backups': self.stats.get('backups', 0)
        }
        
        if self.backup_enabled and os.path.exists(self.backup_dir):
            try:
                backup_files = []
                for filename in os.listdir(self.backup_dir):
                    if filename.startswith("backup_") and filename.endswith(".sqlite"):
                        filepath = os.path.join(self.backup_dir, filename)
                        backup_files.append({
                            'filename': filename,
                            'path': filepath,
                            'size_bytes': os.path.getsize(filepath),
                            'modified_time': os.path.getmtime(filepath)
                        })
                
                # Sort by modification time (newest first)
                backup_files.sort(key=lambda x: x['modified_time'], reverse=True)
                backup_info['backup_files'] = backup_files
                
            except Exception as e:
                backup_info['error'] = str(e)
        
        return backup_info
    
    def shutdown(self) -> None:
        """
        Clean shutdown of all threads and database connections.
        
        Stops all worker threads and disposes of database engine connections.
        """
        self.shutdown_event.set()
        
        for worker in self.workers:
            worker.join(timeout=5)
        
        # Wait for backup thread if it exists
        if hasattr(self, 'backup_thread'):
            self.backup_thread.join(timeout=5)
        
        self.engine.dispose()
    
    def disable_backup_thread(self) -> None:
        """
        Disable the background backup thread for testing purposes.
        
        This method should only be used in test scenarios to prevent
        background backup interference with test assertions.
        """
        if hasattr(self, 'backup_thread'):
            self.shutdown_event.set()
            self.backup_thread.join(timeout=1)
            delattr(self, 'backup_thread')
    
    def enable_backup_thread(self) -> None:
        """
        Re-enable the background backup thread.
        
        This method should only be used in test scenarios after
        disable_backup_thread() has been called.
        """
        if not hasattr(self, 'backup_thread'):
            self.backup_thread = threading.Thread(target=self._backup_worker, daemon=True, name="DB-Backup-Worker")
            self.backup_thread.start()

    @contextmanager
    def _db_engine_lock(self):
        """
        Context manager for database engine lock to prevent operations during backup.
        """
        self.db_engine_lock.acquire()
        try:
            yield
        finally:
            self.db_engine_lock.release()


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
        {"operation": "execute", "query": "UPDATE users SET last_login = :now WHERE id = :id", 
         "params": {"now": time.time(), "id": 1}},
        {"operation": "fetch", "query": "SELECT COUNT(*) as count FROM users"}
    ]
    results = db.execute_transaction(operations)
    
    # Performance stats
    print("Performance stats:", db.get_stats())
    
    db.shutdown()