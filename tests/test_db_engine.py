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
        
        # Initialize DbEngine with test configuration
        self.db_engine = DbEngine(self.database_url, num_workers=1, debug=False)
        
        # Create test table
        self._create_test_table()
    
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
            result = conn.execute(text("PRAGMA journal_mode"))
            journal_mode = result.fetchone()[0]
            self.assertEqual(journal_mode, "wal")
    
    def test_execute_simple_query(self):
        """Test simple query execution."""
        result = self.db_engine.execute("SELECT 1 as test_value")
        self.assertTrue(result)  # execute() returns True for successful operations
    
    def test_execute_with_parameters(self):
        """Test query execution with parameters."""
        # Insert test data
        self._insert_test_data()
        
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
        # Insert test data
        self._insert_test_data()
        
        # Fetch all users
        users = self.db_engine.fetch("SELECT * FROM test_users")
        self.assertEqual(len(users), 3)
        self.assertIsInstance(users, list)
        self.assertIsInstance(users[0], dict)
    
    def test_fetch_with_parameters(self):
        """Test fetch operation with parameters."""
        # Insert test data
        self._insert_test_data()
        
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
        # Insert test data first
        self._insert_test_data()
        
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
        # First insert some data using the same connection context as transactions
        with self.db_engine.get_raw_connection() as conn:
            # Create test data directly in the connection
            test_data = [
                {"name": "Alice Johnson", "email": "alice@example.com", "active": True},
                {"name": "Bob Smith", "email": "bob@example.com", "active": False},
                {"name": "Charlie Brown", "email": "charlie@example.com", "active": True}
            ]
            
            insert_sql = """
            INSERT INTO test_users (name, email, active) 
            VALUES (:name, :email, :active)
            """
            
            for data in test_data:
                conn.execute(text(insert_sql), data)
            conn.commit()
        
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
        # Insert test data
        self._insert_test_data()
        
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
        # Insert initial data
        self._insert_test_data()
        
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


if __name__ == '__main__':
    unittest.main() 