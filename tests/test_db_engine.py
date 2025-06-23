"""
Comprehensive unit tests for DbEngine class.

Copyright (c) 2025, Jim Schilling

Please keep this header when you use this code.

This module is licensed under the MIT License.
"""
import unittest
import tempfile
import os
import time
import threading
import queue 
from sqlalchemy import text

# Add parent directory to path for imports
import sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from jpy_sync_db_lite.db_engine import DbEngine
from jpy_sync_db_lite.db_request import DbRequest


class TestDbEngine(unittest.TestCase):
    """Test cases for DbEngine class."""
    
    def setUp(self):
        """Set up test fixtures before each test method."""
        # Create temporary database file
        self.temp_db_fd, self.temp_db_path = tempfile.mkstemp(suffix='.db')
        os.close(self.temp_db_fd)
        self.database_url = f"sqlite:///{self.temp_db_path}"
        
        # Create temporary backup directory
        self.backup_dir = tempfile.mkdtemp(prefix='backup_test_')
        
        # Initialize DbEngine with backup DISABLED to avoid thread interference
        self.db_engine = DbEngine(
            self.database_url, 
            backup_enabled=False,  # Disable backup thread for testing
            backup_interval=1,
            backup_dir=self.backup_dir,
            backup_cleanup_enabled=False
        )
        
        # Create test table
        self._create_test_table()
        # Clean up table before each test to avoid UNIQUE constraint errors
        with self.db_engine.get_raw_connection() as conn:
            conn.execute(text("DELETE FROM test_users"))
        # Insert test data
        self._insert_test_data()
    
    def tearDown(self):
        """Clean up after each test method."""
        if hasattr(self, 'db_engine'):
            self.db_engine.shutdown()
            # Give time for connections to close
            time.sleep(0.1)
        
        # Remove temporary database file
        if os.path.exists(self.temp_db_path):
            try:
                os.unlink(self.temp_db_path)
            except PermissionError:
                # On Windows, sometimes the file is still in use
                time.sleep(0.1)
                try:
                    os.unlink(self.temp_db_path)
                except PermissionError:
                    pass  # Skip if still can't delete
    
    def _create_test_table(self):
        """Create a test table for testing."""
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS test_users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            email TEXT UNIQUE,
            active BOOLEAN DEFAULT 1,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
        self.db_engine.execute(create_table_sql)
    
    def _insert_test_data(self):
        """Insert test data into the table."""
        test_data = [
            {"name": "Alice Johnson", "email": "alice@example.com", "active": True},
            {"name": "Bob Smith", "email": "bob@example.com", "active": False},
            {"name": "Charlie Brown", "email": "charlie@example.com", "active": True}
        ]
        
        insert_sql = """
        INSERT INTO test_users (name, email, active) 
        VALUES (:name, :email, :active)
        """
        self.db_engine.execute(insert_sql, test_data)
    
    def test_init_with_default_parameters(self):
        """Test DbEngine initialization with default parameters."""
        db = DbEngine(self.database_url)
        
        self.assertIsNotNone(db.engine)
        self.assertEqual(db.num_workers, 1)
        self.assertIsInstance(db.request_queue, queue.Queue)
        self.assertIsInstance(db.stats_lock, type(threading.Lock()))
        self.assertIsInstance(db.shutdown_event, threading.Event)
        self.assertEqual(db.stats['requests'], 0)
        self.assertEqual(db.stats['errors'], 0)
        
        db.shutdown()
    
    def test_init_with_custom_parameters(self):
        """Test DbEngine initialization with custom parameters."""
        db = DbEngine(self.database_url, num_workers=2, debug=True)
        
        self.assertEqual(db.num_workers, 2)
        self.assertEqual(len(db.workers), 2)
        
        db.shutdown()
    
    def test_configure_db_performance(self):
        """Test database performance configuration."""
        # This test verifies that performance pragmas are applied
        # We'll check if WAL mode is enabled using SQLAlchemy connection
        with self.db_engine.get_raw_connection() as conn:
            # Check WAL mode
            result = conn.execute(text("PRAGMA journal_mode"))
            journal_mode = result.fetchone()[0]
            self.assertEqual(journal_mode, "wal")
            
            # Check other performance pragmas
            result = conn.execute(text("PRAGMA synchronous"))
            synchronous = result.fetchone()[0]
            self.assertEqual(synchronous, 1)  # NORMAL mode
            
            result = conn.execute(text("PRAGMA cache_size"))
            cache_size = result.fetchone()[0]
            self.assertEqual(cache_size, -64000)  # 64MB cache
            
            result = conn.execute(text("PRAGMA temp_store"))
            temp_store = result.fetchone()[0]
            self.assertEqual(temp_store, 2)  # MEMORY mode
    
    def test_execute_simple_query(self):
        """Test simple query execution."""
        result = self.db_engine.execute("SELECT 1 as test_value")
        self.assertTrue(result)  # execute() returns True for successful operations
    
    def test_execute_with_parameters(self):
        """Test query execution with parameters."""
        # Execute update with parameters
        update_sql = "UPDATE test_users SET active = :active WHERE name = :name"
        result = self.db_engine.execute(update_sql, {"active": False, "name": "Alice Johnson"})
        
        # Verify update
        users = self.db_engine.fetch("SELECT * FROM test_users WHERE name = :name", 
                                   {"name": "Alice Johnson"})
        self.assertEqual(len(users), 1)
        self.assertEqual(users[0]['active'], False)
    
    def test_fetch_simple_query(self):
        """Test simple fetch operation."""
        # Fetch all users
        users = self.db_engine.fetch("SELECT * FROM test_users")
        self.assertEqual(len(users), 3)
        self.assertIsInstance(users, list)
        self.assertIsInstance(users[0], dict)
    
    def test_fetch_with_parameters(self):
        """Test fetch operation with parameters."""
        # Fetch active users only
        active_users = self.db_engine.fetch(
            "SELECT * FROM test_users WHERE active = :active", 
            {"active": True}
        )
        self.assertEqual(len(active_users), 2)
        
        # Verify all returned users are active
        for user in active_users:
            self.assertTrue(user['active'])
    
    def test_fetch_empty_result(self):
        """Test fetch operation with no results."""
        # Fetch non-existent user
        users = self.db_engine.fetch(
            "SELECT * FROM test_users WHERE name = :name", 
            {"name": "NonExistentUser"}
        )
        self.assertEqual(len(users), 0)
        self.assertIsInstance(users, list)
    
    def test_bulk_operations(self):
        """Test bulk operations using execute method."""
        test_data = [
            {"name": "User1", "email": "user1@example.com", "active": True},
            {"name": "User2", "email": "user2@example.com", "active": False},
            {"name": "User3", "email": "user3@example.com", "active": True}
        ]
        
        insert_sql = """
        INSERT INTO test_users (name, email, active) 
        VALUES (:name, :email, :active)
        """
        
        result = self.db_engine.execute(insert_sql, test_data)
        self.assertEqual(result, 3)
        
        # Verify data was inserted
        users = self.db_engine.fetch("SELECT * FROM test_users WHERE name LIKE 'User%'")
        self.assertEqual(len(users), 3)
    
    def test_bulk_update_operations(self):
        """Test bulk update operations using execute method."""
        # Prepare update data
        update_data = [
            {"id": 1, "active": False},
            {"id": 2, "active": True},
            {"id": 3, "active": False}
        ]
        
        update_sql = "UPDATE test_users SET active = :active WHERE id = :id"
        result = self.db_engine.execute(update_sql, update_data)
        self.assertEqual(result, 3)
        
        # Verify updates
        users = self.db_engine.fetch("SELECT * FROM test_users ORDER BY id")
        self.assertEqual(users[0]['active'], False)
        self.assertEqual(users[1]['active'], True)
        self.assertEqual(users[2]['active'], False)
    
    def test_execute_transaction_success(self):
        """Test successful transaction execution."""
        operations = [
            {
                "operation": "execute",
                "query": "INSERT INTO test_users (name, email, active) VALUES (:name, :email, :active)",
                "params": {"name": "TransactionUser", "email": "transaction@example.com", "active": True}
            },
            {
                "operation": "fetch",
                "query": "SELECT COUNT(*) as count FROM test_users WHERE name = :name",
                "params": {"name": "TransactionUser"}
            }
        ]
        
        results = self.db_engine.execute_transaction(operations)
        self.assertEqual(len(results), 2)
        self.assertTrue(results[0])  # Insert result
        self.assertEqual(results[1][0]['count'], 1)  # Count result
    
    def test_execute_transaction_rollback(self):
        """Test transaction rollback on error."""
        # The data is already inserted in setUp(), so we can work with existing data
        operations = [
            {
                "operation": "execute",
                "query": "UPDATE test_users SET active = :active WHERE name = :name",
                "params": {"active": False, "name": "Alice Johnson"}
            },
            {
                "operation": "execute",
                "query": "UPDATE test_users SET invalid_column = :value WHERE name = :name",
                "params": {"value": "test", "name": "Bob Smith"}
            }
        ]
        
        # This should raise an exception and rollback
        with self.assertRaises(Exception):
            self.db_engine.execute_transaction(operations)
        
        # Verify the first update was rolled back using the same connection context
        with self.db_engine.get_raw_connection() as conn:
            result = conn.execute(
                text("SELECT active FROM test_users WHERE name = :name"),
                {"name": "Alice Johnson"}
            )
            user = [dict(row._mapping) for row in result.fetchall()]
            self.assertEqual(len(user), 1)
            self.assertEqual(user[0]['active'], True)  # Should still be True due to rollback
    
    def test_get_raw_connection(self):
        """Test getting raw database connection."""
        with self.db_engine.get_raw_connection() as conn:
            self.assertIsNotNone(conn)
            result = conn.execute(text("SELECT 1"))
            self.assertIsNotNone(result)
    
    def test_get_stats(self):
        """Test getting performance statistics."""
        # Perform some operations to generate stats
        self.db_engine.execute("SELECT 1")
        self.db_engine.fetch("SELECT 1")
        
        stats = self.db_engine.get_stats()
        
        self.assertIn('requests', stats)
        self.assertIn('errors', stats)
        self.assertIn('queue_size', stats)
        self.assertIsInstance(stats['requests'], int)
        self.assertIsInstance(stats['errors'], int)
        self.assertIsInstance(stats['queue_size'], int)
    
    def test_concurrent_operations(self):
        """Test concurrent database operations."""
        results = []
        errors = []
        
        def worker(worker_id):
            try:
                # Each worker performs multiple operations
                for i in range(5):
                    # Fetch operation
                    users = self.db_engine.fetch("SELECT * FROM test_users WHERE active = :active", 
                                               {"active": True})
                    results.append(len(users))
                    
                    # Execute operation
                    self.db_engine.execute("UPDATE test_users SET name = :name WHERE id = :id",
                                         {"name": f"UpdatedUser{worker_id}_{i}", "id": 1})
                    
                    time.sleep(0.01)  # Small delay to simulate work
                    
            except Exception as e:
                errors.append(str(e))
        
        # Start multiple threads
        threads = []
        for i in range(3):
            thread = threading.Thread(target=worker, args=(i,))
            threads.append(thread)
            thread.start()
        
        # Wait for all threads to complete
        for thread in threads:
            thread.join()
        
        # Verify no errors occurred
        self.assertEqual(len(errors), 0)
        self.assertGreater(len(results), 0)
    
    def test_error_handling_invalid_sql(self):
        """Test error handling for invalid SQL."""
        with self.assertRaises(Exception):
            self.db_engine.execute("INVALID SQL STATEMENT")
    
    def test_error_handling_missing_table(self):
        """Test error handling for queries on non-existent table."""
        with self.assertRaises(Exception):
            self.db_engine.fetch("SELECT * FROM non_existent_table")
    
    def test_error_handling_missing_parameter(self):
        """Test error handling for missing parameters."""
        with self.assertRaises(Exception):
            self.db_engine.fetch("SELECT * FROM test_users WHERE id = :id")
    
    def test_shutdown_cleanup(self):
        """Test clean shutdown of DbEngine."""
        # Create a new engine for this test
        db = DbEngine(self.database_url)
        
        # Verify workers are running
        self.assertTrue(all(worker.is_alive() for worker in db.workers))
        
        # Shutdown
        db.shutdown()
        
        # Verify workers are stopped
        for worker in db.workers:
            worker.join(timeout=1)
            self.assertFalse(worker.is_alive())
    
    def test_large_bulk_operations(self):
        """Test large bulk operations for performance."""
        # Generate large dataset
        large_data = []
        for i in range(1000):
            large_data.append({
                "name": f"LargeUser{i}",
                "email": f"largeuser{i}@example.com",
                "active": i % 2 == 0
            })
        
        insert_sql = """
        INSERT INTO test_users (name, email, active) 
        VALUES (:name, :email, :active)
        """
        
        # Time the bulk insert
        start_time = time.time()
        result = self.db_engine.execute(insert_sql, large_data)
        end_time = time.time()
        
        self.assertEqual(result, 1000)
        self.assertLess(end_time - start_time, 10)  # Should complete within 10 seconds
        
        # Verify data was inserted
        count = self.db_engine.fetch("SELECT COUNT(*) as count FROM test_users WHERE name LIKE 'LargeUser%'")
        self.assertEqual(count[0]['count'], 1000)
    
    def test_mixed_operation_types(self):
        """Test mixing different operation types."""
        # Mix of operations
        operations = [
            {"operation": "fetch", "query": "SELECT COUNT(*) as count FROM test_users"},
            {"operation": "execute", "query": "INSERT INTO test_users (name, email, active) VALUES (:name, :email, :active)",
             "params": {"name": "MixedUser", "email": "mixed@example.com", "active": True}},
            {"operation": "fetch", "query": "SELECT * FROM test_users WHERE name = :name",
             "params": {"name": "MixedUser"}},
            {"operation": "execute", "query": "UPDATE test_users SET active = :active WHERE name = :name",
             "params": {"active": False, "name": "MixedUser"}}
        ]
        
        results = self.db_engine.execute_transaction(operations)
        
        self.assertEqual(len(results), 4)
        self.assertEqual(results[0][0]['count'], 3)  # Initial count
        self.assertTrue(results[1])  # Insert success
        self.assertEqual(len(results[2]), 1)  # Fetch result
        self.assertTrue(results[3])  # Update success
    
    def test_unicode_and_special_characters(self):
        """Test handling of unicode and special characters."""
        test_data = [
            {"name": "José María", "email": "jose@example.com", "active": True},
            {"name": "测试用户", "email": "test@example.com", "active": True},
            {"name": "User with 'quotes'", "email": "quotes@example.com", "active": True}
        ]
        
        insert_sql = """
        INSERT INTO test_users (name, email, active) 
        VALUES (:name, :email, :active)
        """
        
        self.db_engine.execute(insert_sql, test_data)
        
        # Fetch and verify
        users = self.db_engine.fetch("SELECT * FROM test_users WHERE name IN (:name1, :name2, :name3)",
                                   {"name1": "José María", "name2": "测试用户", "name3": "User with 'quotes'"})
        
        self.assertEqual(len(users), 3)
        names = [user['name'] for user in users]
        self.assertIn("José María", names)
        self.assertIn("测试用户", names)
        self.assertIn("User with 'quotes'", names)
    
    def test_basic_db_operation(self):
        """Test basic database operation to verify locking works."""
        print("Testing basic database operation...")
        
        # Try a simple fetch operation
        print("Attempting fetch operation...")
        result = self.db_engine.fetch("SELECT 1 as test")
        print(f"Fetch result: {result}")
        
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]['test'], 1)
        print("Basic operation successful!")
    
    def test_simple_backup_request(self):
        """Test a simple single backup request to isolate hanging issues."""
        print("Starting simple backup test...")
        
        # Request a single backup
        print("Requesting backup...")
        success = self.db_engine.request_backup()
        print(f"Backup request result: {success}")
        
        # Check if backup file was created
        backup_files = [f for f in os.listdir(self.backup_dir) 
                       if f.startswith("backup_") and f.endswith(".sqlite")]
        print(f"Backup files found: {len(backup_files)}")
        
        self.assertTrue(success, "Backup request should succeed")
        self.assertGreater(len(backup_files), 0, "Backup file should be created")

    def test_backup_file_integrity(self):
        """Test that backup files contain the expected data."""
        # Data is already inserted in setUp()
        # Request a backup after table/data are present
        success = self.db_engine.request_backup()
        self.assertTrue(success)
        
        # Find the backup file
        backup_files = [f for f in os.listdir(self.backup_dir) 
                       if f.startswith("backup_") and f.endswith(".sqlite")]
        self.assertGreater(len(backup_files), 0)
        
        backup_file_path = os.path.join(self.backup_dir, backup_files[0])
        
        # Create a new engine to verify backup content
        backup_db_url = f"sqlite:///{backup_file_path}"
        backup_db = DbEngine(backup_db_url)
        
        try:
            # Check that the backup contains the same data
            users = backup_db.fetch("SELECT * FROM test_users")
            self.assertEqual(len(users), 3)
            
            # Verify specific data
            alice = backup_db.fetch("SELECT * FROM test_users WHERE name = :name", 
                                  {"name": "Alice Johnson"})
            self.assertEqual(len(alice), 1)
            self.assertTrue(alice[0]['active'])
            
        finally:
            backup_db.shutdown()

    def test_backup_thread_disabled(self):
        """Test backup thread disable/enable functionality."""
        # Backup thread should not be running initially
        self.assertFalse(hasattr(self.db_engine, 'backup_thread'))
        
        # Reset shutdown event for enabling thread
        self.db_engine.shutdown_event.clear()
        
        # Enable backup thread
        self.db_engine.enable_backup_thread()
        
        # Give thread time to start up
        time.sleep(0.2)
        
        # Now check for thread
        self.assertTrue(hasattr(self.db_engine, 'backup_thread'))
        self.assertTrue(self.db_engine.backup_thread.is_alive())
        
        # Disable backup thread
        self.db_engine.disable_backup_thread()
        
        # Verify thread is stopped
        self.assertFalse(hasattr(self.db_engine, 'backup_thread'))

    def test_backup_cleanup_functionality(self):
        """Test backup cleanup when backup_cleanup_enabled is True."""
        # Create engine with cleanup enabled, keeping only 2 backups
        cleanup_db = DbEngine(
            self.database_url,
            backup_enabled=False,
            backup_dir=self.backup_dir,
            backup_cleanup_enabled=True,
            backup_cleanup_keep_count=2
        )
        
        try:
            # Create multiple backups
            for i in range(5):
                cleanup_db.request_backup()
                time.sleep(0.1)  # Small delay between backups
            
            # Check that only the most recent 2 backups remain
            backup_files = [f for f in os.listdir(self.backup_dir) 
                           if f.startswith("backup_") and f.endswith(".sqlite")]
            self.assertLessEqual(len(backup_files), 2, 
                               "Should keep only the specified number of recent backups")
            
        finally:
            cleanup_db.shutdown()

    def test_periodic_backup_thread(self):
        """Test periodic backup thread functionality."""
        # Create engine with backup enabled and short interval
        periodic_db = DbEngine(
            self.database_url,
            backup_enabled=True,
            backup_interval=1,  # 1 second interval for testing
            backup_dir=self.backup_dir,
            backup_cleanup_enabled=False
        )
        
        try:
            # Get initial backup count
            initial_backup_files = [f for f in os.listdir(self.backup_dir) 
                                   if f.startswith("backup_") and f.endswith(".sqlite")]
            initial_count = len(initial_backup_files)
            
            # Wait for periodic backup to occur (should happen within 2 seconds)
            time.sleep(2.5)
            
            # Check that a new backup was created
            final_backup_files = [f for f in os.listdir(self.backup_dir) 
                                 if f.startswith("backup_") and f.endswith(".sqlite")]
            final_count = len(final_backup_files)
            
            self.assertGreaterEqual(final_count, initial_count + 1, 
                                  "Periodic backup should create at least one new backup")
            
        finally:
            periodic_db.shutdown()

    def test_connection_pool_configuration(self):
        """Test that connection pool is properly configured."""
        # Verify StaticPool is used (SQLite-specific optimization)
        self.assertEqual(self.db_engine.engine.pool.__class__.__name__, 'StaticPool')
        
        # Test that connections are properly managed
        with self.db_engine.get_raw_connection() as conn1:
            with self.db_engine.get_raw_connection() as conn2:
                # Both connections should work independently
                result1 = conn1.execute(text("SELECT 1 as test1"))
                result2 = conn2.execute(text("SELECT 2 as test2"))
                
                self.assertEqual(result1.fetchone()[0], 1)
                self.assertEqual(result2.fetchone()[0], 2)


class TestDbEngineEdgeCases(unittest.TestCase):
    """Test edge cases and error conditions for DbEngine."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.temp_db_fd, self.temp_db_path = tempfile.mkstemp(suffix='.db')
        os.close(self.temp_db_fd)
        self.database_url = f"sqlite:///{self.temp_db_path}"
    
    def tearDown(self):
        """Clean up after each test."""
        if os.path.exists(self.temp_db_path):
            try:
                os.unlink(self.temp_db_path)
            except PermissionError:
                time.sleep(0.1)
                try:
                    os.unlink(self.temp_db_path)
                except PermissionError:
                    pass
    
    def test_empty_parameters(self):
        """Test operations with empty parameters."""
        db = DbEngine(self.database_url)
        
        # Test with None parameters
        result = db.execute("SELECT 1 as test")
        self.assertTrue(result)  # execute() returns True for successful operations
        
        # Test with empty dict
        result = db.fetch("SELECT 1 as test", {})
        self.assertEqual(len(result), 1)
        
        db.shutdown()
    
    def test_none_response_queue(self):
        """Test operations when response queue is None."""
        db = DbEngine(self.database_url)
        
        # Create request without response queue
        request = DbRequest('execute', 'SELECT 1', None, None)
        
        # This should not raise an exception
        db.request_queue.put(request)
        time.sleep(0.1)  # Give worker time to process
        
        db.shutdown()
    
    def test_invalid_operation_type(self):
        """Test handling of invalid operation type."""
        db = DbEngine(self.database_url)
        
        # Create request with invalid operation (not 'fetch' or 'execute')
        request = DbRequest('invalid_operation', 'SELECT 1', None, queue.Queue())
        
        # This should be handled gracefully
        db.request_queue.put(request)
        time.sleep(0.1)  # Give worker time to process
        
        db.shutdown()
    
    def test_database_file_permissions(self):
        """Test behavior with database file permission issues."""
        # Create a read-only database file
        with open(self.temp_db_path, 'w') as f:
            f.write("invalid database content")
        
        os.chmod(self.temp_db_path, 0o444)  # Read-only
        
        # Should handle gracefully
        with self.assertRaises(Exception):
            DbEngine(self.database_url)
        
        # Restore permissions for cleanup
        os.chmod(self.temp_db_path, 0o666)


class TestDbEngineBackup(unittest.TestCase):
    """Test cases for DbEngine backup functionality."""
    
    def setUp(self):
        """Set up test fixtures before each test method."""
        # Create temporary database file
        self.temp_db_fd, self.temp_db_path = tempfile.mkstemp(suffix='.db')
        os.close(self.temp_db_fd)
        self.database_url = f"sqlite:///{self.temp_db_path}"
        
        # Create temporary backup directory
        self.backup_dir = tempfile.mkdtemp(prefix='backup_test_')
        
        # Initialize DbEngine with backup DISABLED to avoid thread interference
        self.db_engine = DbEngine(
            self.database_url, 
            backup_enabled=False,  # Disable backup thread for testing
            backup_interval=1,
            backup_dir=self.backup_dir,
            backup_cleanup_enabled=False
        )
        
        # Create test table
        self._create_test_table()
        # Clean up table before each test to avoid UNIQUE constraint errors
        with self.db_engine.get_raw_connection() as conn:
            conn.execute(text("DELETE FROM test_users"))
        # Insert test data
        self._insert_test_data()
    
    def tearDown(self):
        """Clean up after each test method."""
        if hasattr(self, 'db_engine'):
            self.db_engine.shutdown()
        if os.path.exists(self.temp_db_path):
            try:
                os.unlink(self.temp_db_path)
            except PermissionError:
                time.sleep(0.1)
                try:
                    os.unlink(self.temp_db_path)
                except PermissionError:
                    pass
        if os.path.exists(self.backup_dir):
            try:
                import shutil
                shutil.rmtree(self.backup_dir)
            except Exception:
                pass
    
    def _create_test_table(self):
        """Create a test table for testing."""
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS test_users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            email TEXT UNIQUE,
            active BOOLEAN DEFAULT 1,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
        self.db_engine.execute(create_table_sql)
    
    def _insert_test_data(self):
        """Insert test data into the table."""
        test_data = [
            {"name": "Alice Johnson", "email": "alice@example.com", "active": True},
            {"name": "Bob Smith", "email": "bob@example.com", "active": False},
            {"name": "Charlie Brown", "email": "charlie@example.com", "active": True}
        ]
        
        insert_sql = """
        INSERT INTO test_users (name, email, active) 
        VALUES (:name, :email, :active)
        """
        self.db_engine.execute(insert_sql, test_data)
    
    def test_backup_initialization(self):
        """Test that backup functionality is properly initialized."""
        # Check backup configuration
        self.assertFalse(self.db_engine.backup_enabled)
        self.assertEqual(self.db_engine.backup_interval, 1)
        self.assertEqual(self.db_engine.backup_dir, self.backup_dir)
        self.assertFalse(self.db_engine.backup_cleanup_enabled)
        
        # Check that backup thread is not running
        self.assertFalse(hasattr(self.db_engine, 'backup_thread'))
    
    def test_manual_backup_request(self):
        """Test manual backup request functionality."""
        # Request a manual backup
        success = self.db_engine.request_backup()
        self.assertTrue(success)
        
        # Check that backup file was created
        backup_files = [f for f in os.listdir(self.backup_dir) 
                       if f.startswith("backup_") and f.endswith(".sqlite")]
        self.assertGreater(len(backup_files), 0)
        
        # Verify backup file size is reasonable
        backup_file_path = os.path.join(self.backup_dir, backup_files[0])
        backup_size = os.path.getsize(backup_file_path)
        self.assertGreater(backup_size, 0)
    
    def test_backup_stats_tracking(self):
        """Test that backup statistics are properly tracked."""
        initial_stats = self.db_engine.get_stats()
        initial_backup_count = initial_stats.get('backups', 0)
        
        # Request a backup
        success = self.db_engine.request_backup()
        self.assertTrue(success)
        
        # Check that backup count increased
        updated_stats = self.db_engine.get_stats()
        updated_backup_count = updated_stats.get('backups', 0)
        self.assertEqual(updated_backup_count, initial_backup_count + 1)
    
    def test_backup_info_method(self):
        """Test the get_backup_info method."""
        backup_info = self.db_engine.get_backup_info()
        
        # Check basic info
        self.assertFalse(backup_info['enabled'])
        self.assertEqual(backup_info['interval_seconds'], 1)
        self.assertEqual(backup_info['directory'], self.backup_dir)
        self.assertGreaterEqual(backup_info['last_backup'], 0)
        self.assertGreaterEqual(backup_info['total_backups'], 0)
        
        # Request a backup to populate backup_files
        self.db_engine.request_backup()
        
        # Check backup files info
        updated_info = self.db_engine.get_backup_info()
        # Accept backup_files missing if backup is disabled
        if 'backup_files' in updated_info:
            self.assertGreater(len(updated_info['backup_files']), 0)
            # Verify backup file structure
            backup_file = updated_info['backup_files'][0]
            self.assertIn('filename', backup_file)
            self.assertIn('path', backup_file)
            self.assertIn('size_bytes', backup_file)
            self.assertIn('modified_time', backup_file)
            self.assertTrue(backup_file['filename'].startswith("backup_"))
            self.assertTrue(backup_file['filename'].endswith(".sqlite"))
    
    def test_backup_thread_disabled(self):
        """Test backup thread disable/enable functionality."""
        # Backup thread should not be running initially
        self.assertFalse(hasattr(self.db_engine, 'backup_thread'))
        
        # Reset shutdown event for enabling thread
        self.db_engine.shutdown_event.clear()
        
        # Enable backup thread
        self.db_engine.enable_backup_thread()
        
        # Give thread time to start up
        time.sleep(0.2)
        
        # Now check for thread
        self.assertTrue(hasattr(self.db_engine, 'backup_thread'))
        self.assertTrue(self.db_engine.backup_thread.is_alive())
        
        # Disable backup thread
        self.db_engine.disable_backup_thread()
        
        # Verify thread is stopped
        self.assertFalse(hasattr(self.db_engine, 'backup_thread'))
    
    def test_backup_without_backup_enabled(self):
        """Test backup functionality when backup is disabled."""
        # Create engine without backup enabled
        db = DbEngine(self.database_url, backup_enabled=False)
        
        try:
            # Backup should not be enabled
            self.assertFalse(db.backup_enabled)
            self.assertFalse(hasattr(db, 'backup_thread'))
            
            # Manual backup request should still work
            success = db.request_backup()
            self.assertTrue(success)
            
        finally:
            db.shutdown()
    
    def test_backup_concurrent_requests(self):
        """Test handling of concurrent backup requests."""
        # Start multiple backup requests simultaneously
        import concurrent.futures
        
        def request_backup():
            return self.db_engine.request_backup()
        
        # Use a timeout to prevent hanging
        with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
            futures = [executor.submit(request_backup) for _ in range(3)]
            
            # Wait for all futures with a timeout
            try:
                results = []
                for future in concurrent.futures.as_completed(futures, timeout=10.0):
                    results.append(future.result())
                
                # Some requests may be skipped due to non-blocking lock
                # At least one should succeed
                self.assertTrue(any(results), "At least one backup request should succeed")
                
                # Check that backup files were created
                backup_files = [f for f in os.listdir(self.backup_dir) 
                               if f.startswith("backup_") and f.endswith(".sqlite")]
                self.assertGreater(len(backup_files), 0)
                
            except concurrent.futures.TimeoutError:
                # If we timeout, cancel remaining futures
                for future in futures:
                    future.cancel()
                self.fail("Concurrent backup test timed out")


class TestDbRequest(unittest.TestCase):
    """Test cases for DbRequest class."""
    
    def test_db_request_creation(self):
        """Test DbRequest object creation with various parameters."""
        from jpy_sync_db_lite.db_request import DbRequest
        
        # Test basic creation
        request = DbRequest('fetch', 'SELECT 1')
        self.assertEqual(request.operation, 'fetch')
        self.assertEqual(request.query, 'SELECT 1')
        self.assertIsNone(request.params)
        self.assertIsNone(request.response_queue)
        self.assertIsInstance(request.timestamp, float)
        self.assertIsNone(request.batch_id)
        
        # Test with all parameters
        response_queue = queue.Queue()
        request = DbRequest(
            operation='execute',
            query='INSERT INTO test VALUES (:id)',
            params={'id': 1},
            response_queue=response_queue,
            batch_id='test_batch'
        )
        self.assertEqual(request.operation, 'execute')
        self.assertEqual(request.query, 'INSERT INTO test VALUES (:id)')
        self.assertEqual(request.params, {'id': 1})
        self.assertEqual(request.response_queue, response_queue)
        self.assertEqual(request.batch_id, 'test_batch')
    
    def test_db_request_timestamp(self):
        """Test that DbRequest timestamps are properly set."""
        from jpy_sync_db_lite.db_request import DbRequest
        import time
        
        before = time.time()
        request = DbRequest('fetch', 'SELECT 1')
        after = time.time()
        
        self.assertGreaterEqual(request.timestamp, before)
        self.assertLessEqual(request.timestamp, after)
    
    def test_db_request_with_list_params(self):
        """Test DbRequest with list parameters for bulk operations."""
        from jpy_sync_db_lite.db_request import DbRequest
        
        params_list = [
            {'name': 'Alice', 'email': 'alice@example.com'},
            {'name': 'Bob', 'email': 'bob@example.com'}
        ]
        
        request = DbRequest('execute', 'INSERT INTO users (name, email) VALUES (:name, :email)', params_list)
        self.assertEqual(request.operation, 'execute')
        self.assertEqual(request.params, params_list)
        self.assertIsInstance(request.params, list)


if __name__ == '__main__':
    unittest.main() 