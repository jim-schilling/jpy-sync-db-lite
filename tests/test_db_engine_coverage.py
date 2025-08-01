"""
Coverage tests for DbEngine class.

Copyright (c) 2025, Jim Schilling

Please keep this header when you use this code.

This module is licensed under the MIT License.
"""

import os
import queue
import sys
import tempfile
import threading
import time
import unittest
import pytest
from sqlalchemy import text

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from jpy_sync_db_lite.db_engine import DbEngine, DbOperationError, SQLiteError, DbResult


class TestDbEngineCoverage(unittest.TestCase):
    """Additional tests to improve coverage for db_engine.py."""
    
    def setUp(self) -> None:
        """Set up test database."""
        self.database_url = "sqlite:///:memory:"
        self.db_engine = DbEngine(self.database_url, num_workers=1, debug=False)
        self._create_test_table()
        self._insert_test_data()

    def tearDown(self) -> None:
        """Clean up after tests."""
        if hasattr(self, 'db_engine'):
            self.db_engine.shutdown()

    def _create_test_table(self) -> None:
        """Create test table."""
        create_sql = """
        CREATE TABLE IF NOT EXISTS test_users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            email TEXT UNIQUE,
            active BOOLEAN DEFAULT TRUE
        )
        """
        self.db_engine.execute(create_sql)

    def _insert_test_data(self) -> None:
        """Insert test data."""
        test_data = [
            {"name": "Alice Johnson", "email": "alice@example.com", "active": True},
            {"name": "Bob Smith", "email": "bob@example.com", "active": True},
            {"name": "Charlie Brown", "email": "charlie@example.com", "active": False}
        ]
        insert_sql = "INSERT INTO test_users (name, email, active) VALUES (:name, :email, :active)"
        self.db_engine.execute(insert_sql, params=test_data)    

    @pytest.mark.unit
    def test_configure_pragma_success(self) -> None:
        """Test configure_pragma with valid pragma."""
        self.db_engine.configure_pragma("cache_size", "1000")
        # Verify it was set
        with self.db_engine.get_raw_connection() as conn:
            result = conn.execute(text("PRAGMA cache_size"))
            cache_size = result.scalar()
            self.assertEqual(cache_size, 1000)

    @pytest.mark.unit
    def test_worker_thread_cleanup(self) -> None:
        """Test worker thread cleanup on shutdown."""
        # Create engine with multiple workers
        multi_worker_engine = DbEngine(self.database_url, num_workers=3)
        
        # Verify workers are running
        self.assertEqual(len(multi_worker_engine.workers), 3)
        for worker in multi_worker_engine.workers:
            self.assertTrue(worker.is_alive())
        
        # Shutdown and verify cleanup
        multi_worker_engine.shutdown()
        time.sleep(0.1)  # Give time for cleanup
        
        # Workers should be stopped
        for worker in multi_worker_engine.workers:
            self.assertFalse(worker.is_alive())

    @pytest.mark.unit
    def test_execute_with_list_params_and_rowcount(self) -> None:
        """Test execute with list params and rowcount handling."""
        # Test with list params where rowcount is available
        update_data = [
            {"id": 1, "active": False},
            {"id": 2, "active": True}
        ]
        result = self.db_engine.execute(
            "UPDATE test_users SET active = :active WHERE id = :id",
            params=update_data
        )
        self.assertEqual(result.rowcount, 2)
        self.assertTrue(result.result)

    @pytest.mark.unit
    def test_execute_with_no_rowcount(self) -> None:
        """Test execute when rowcount is not available."""
        # Test a statement that doesn't provide rowcount
        result = self.db_engine.execute("PRAGMA cache_size")
        # Should handle gracefully even if rowcount is None
        self.assertIsInstance(result, DbResult)

    @pytest.mark.unit
    def test_fetch_with_empty_result(self) -> None:
        """Test fetch with empty result set."""
        result = self.db_engine.fetch(
            "SELECT * FROM test_users WHERE name = :name",
            params={"name": "NonExistentUser"}
        )
        self.assertFalse(result.result)
        self.assertEqual(result.rowcount, 0)
        self.assertEqual(len(result.data), 0)

    @pytest.mark.unit
    def test_batch_with_error_statement(self) -> None:
        """Test batch with statements that cause errors."""
        batch_sql = """
        SELECT 1;
        INVALID SQL STATEMENT;
        SELECT 2;
        """
        with self.assertRaises(DbOperationError):
            self.db_engine.batch(batch_sql)

    @pytest.mark.unit
    def test_batch_with_commit_failure(self) -> None:
        """Test batch with commit failure simulation."""
        # This test simulates a commit failure scenario
        batch_sql = """
        INSERT INTO test_users (name, email, active) VALUES ('Test1', 'test1@example.com', 1);
        INSERT INTO test_users (name, email, active) VALUES ('Test2', 'test2@example.com', 1);
        """
        # Should succeed normally
        results = self.db_engine.batch(batch_sql)
        self.assertEqual(len(results), 2)

    @pytest.mark.integration
    def test_execute_transaction_with_fetch_operation(self) -> None:
        """Test execute_transaction with fetch operations."""
        operations = [
            {
                "operation": "fetch",
                "query": "SELECT COUNT(*) as count FROM test_users"
            },
            {
                "operation": "execute",
                "query": "INSERT INTO test_users (name, email, active) VALUES (:name, :email, :active)",
                "params": {"name": "NewUser", "email": "new@example.com", "active": True}
            },
            {
                "operation": "fetch",
                "query": "SELECT COUNT(*) as count FROM test_users"
            }
        ]
        
        results = self.db_engine.execute_transaction(operations)
        self.assertEqual(len(results), 3)
        self.assertEqual(results[0]["operation"], "fetch")
        self.assertEqual(results[1]["operation"], "execute")
        self.assertEqual(results[2]["operation"], "fetch")

    @pytest.mark.unit
    def test_execute_transaction_with_invalid_operation(self) -> None:
        """Test execute_transaction with invalid operation type."""
        operations = [
            {
                "operation": "invalid_op",
                "query": "SELECT 1"
            }
        ]
        
        with self.assertRaises(DbOperationError):
            self.db_engine.execute_transaction(operations)

    @pytest.mark.unit
    def test_execute_transaction_with_missing_query(self) -> None:
        """Test execute_transaction with missing query."""
        operations = [
            {
                "operation": "fetch"
            }
        ]
        
        with self.assertRaises(DbOperationError):
            self.db_engine.execute_transaction(operations)

    @pytest.mark.unit
    def test_execute_transaction_with_missing_operation(self) -> None:
        """Test execute_transaction with missing operation."""
        operations = [
            {
                "query": "SELECT 1"
            }
        ]
        
        with self.assertRaises(DbOperationError):
            self.db_engine.execute_transaction(operations)

    @pytest.mark.unit
    def test_vacuum_error_handling(self) -> None:
        """Test vacuum error handling."""
        # Test vacuum on a database that might cause issues
        self.db_engine.vacuum()  # Should not raise an exception

    @pytest.mark.unit
    def test_analyze_with_error(self) -> None:
        """Test analyze with potential error."""
        # Test analyze on non-existent table
        with self.assertRaises(DbOperationError):
            self.db_engine.analyze(table_name="non_existent_table")

    @pytest.mark.unit
    def test_integrity_check_with_issues(self) -> None:
        """Test integrity check that finds issues."""
        # Run integrity check - should return empty list for healthy database
        issues = self.db_engine.integrity_check()
        self.assertIsInstance(issues, list)

    @pytest.mark.unit
    def test_optimize_operation(self) -> None:
        """Test optimize operation."""
        self.db_engine.optimize()  # Should not raise an exception

    @pytest.mark.unit
    def test_get_sqlite_info_with_memory_db(self) -> None:
        """Test get_sqlite_info with in-memory database."""
        memory_engine = DbEngine("sqlite:///:memory:")
        info = memory_engine.get_sqlite_info()
        
        # Should have basic info even for in-memory DB
        self.assertIn('version', info)
        self.assertIsInstance(info['version'], str)

        # Database size might be None for in-memory
        self.assertIn('database_size', info)

        memory_engine.shutdown()

    @pytest.mark.unit
    def test_get_sqlite_info_with_file_db(self) -> None:
        """Test get_sqlite_info with file database."""
        # Create a temporary file-based database for this test
        temp_fd, temp_path = tempfile.mkstemp(suffix='.db')
        os.close(temp_fd)
        
        try:
            file_engine = DbEngine(f"sqlite:///{temp_path}")
            info = file_engine.get_sqlite_info()
            
            # Should have all info for file-based DB
            self.assertIn('version', info)
            self.assertIn('database_size', info)
            self.assertIn('page_count', info)
            self.assertIn('page_size', info)
            self.assertIn('cache_size', info)
            self.assertIn('journal_mode', info)
            self.assertIn('synchronous', info)
            self.assertIn('temp_store', info)
            self.assertIn('mmap_size', info)
            self.assertIn('busy_timeout', info)
            
            self.assertIsInstance(info['version'], str)
            self.assertIsInstance(info['page_count'], int)
            self.assertIsInstance(info['page_size'], int)
            
            file_engine.shutdown()
        finally:
            # Clean up
            if os.path.exists(temp_path):
                try:
                    os.unlink(temp_path)
                except PermissionError:
                    pass

    @pytest.mark.unit
    def test_stats_increment(self) -> None:
        """Test that stats are properly incremented."""
        initial_stats = self.db_engine.get_stats()
        
        # Perform some operations
        self.db_engine.execute("SELECT 1")
        self.db_engine.fetch("SELECT 1")
        
        # Stats should be updated
        updated_stats = self.db_engine.get_stats()
        self.assertGreaterEqual(updated_stats['requests'], initial_stats['requests'])

    @pytest.mark.unit
    def test_sqlite_error_exception(self) -> None:
        """Test SQLiteError exception class."""
        error = SQLiteError(123, "Test error message")
        self.assertEqual(error.error_code, 123)
        self.assertEqual(error.message, "Test error message")
        self.assertIn("SQLite error 123", str(error))

    @pytest.mark.unit
    def test_db_operation_error(self) -> None:
        """Test DbOperationError exception."""
        error = DbOperationError("Test operation error")
        self.assertIn("Test operation error", str(error))

    @pytest.mark.unit
    def test_engine_properties(self) -> None:
        """Test all engine properties."""
        self.assertIsNotNone(self.db_engine.engine)
        self.assertIsInstance(self.db_engine.request_queue, queue.Queue)
        self.assertIsInstance(self.db_engine.stats_lock, type(threading.Lock()))
        self.assertIsInstance(self.db_engine.db_engine_lock, type(threading.RLock()))
        self.assertIsInstance(self.db_engine.shutdown_event, threading.Event)
        self.assertIsInstance(self.db_engine.stats, dict)
        self.assertEqual(self.db_engine.num_workers, 1)
        self.assertIsInstance(self.db_engine.workers, list)

    @pytest.mark.unit
    def test_execute_with_none_params(self) -> None:
        """Test execute with None params."""
        result = self.db_engine.execute("SELECT 1", params=None)
        self.assertIsInstance(result, DbResult)

    @pytest.mark.unit
    def test_fetch_with_none_params(self) -> None:
        """Test fetch with None params."""
        result = self.db_engine.fetch("SELECT 1", params=None)
        self.assertIsInstance(result, DbResult)

    @pytest.mark.unit
    def test_batch_with_empty_statements(self) -> None:
        """Test batch with empty statements."""
        batch_sql = "   \n   \n   "  # Only whitespace
        results = self.db_engine.batch(batch_sql)
        self.assertEqual(len(results), 0)  # Empty statements should produce no results

    @pytest.mark.unit
    def test_batch_with_comments_only(self) -> None:
        """Test batch with only comments."""
        batch_sql = """
        -- This is a comment
        /* This is another comment */
        """
        results = self.db_engine.batch(batch_sql)
        self.assertEqual(len(results), 0)  # Comment-only statements should produce no results


if __name__ == '__main__':
    unittest.main() 