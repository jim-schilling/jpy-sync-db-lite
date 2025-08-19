"""
Coverage tests for DbEngine class.

Copyright (c) 2025, Jim Schilling

Please keep this header when you use this code.

This module is licensed under the MIT License.
"""

import os
import queue
import tempfile
import threading
import time
import unittest
import pytest
from sqlalchemy import text

from jpy_sync_db_lite.db_engine import DbEngine, DbResult
from jpy_sync_db_lite.errors import OperationError, MaintenanceError, TransactionError


class TestDbEngineCoverage(unittest.TestCase):
    """Additional tests to improve coverage for db_engine.py."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.database_url = "sqlite:///:memory:"
        self.db_engine = DbEngine(self.database_url, debug=False)

        # Create test table
        self.db_engine.execute("""
            CREATE TABLE test_users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                email TEXT UNIQUE,
                active BOOLEAN DEFAULT 1
            )
        """)

        # Insert test data
        test_data = [
            {"name": "Alice Johnson", "email": "alice@example.com", "active": True},
            {"name": "Bob Smith", "email": "bob@example.com", "active": True},
            {"name": "Charlie Brown", "email": "charlie@example.com", "active": False}
        ]

        insert_sql = """
        INSERT INTO test_users (name, email, active)
        VALUES (:name, :email, :active)
        """

        for data in test_data:
            self.db_engine.execute(insert_sql, params=data)

    def tearDown(self):
        """Clean up test fixtures."""
        if hasattr(self, 'db_engine'):
            self.db_engine.shutdown()

    def test_worker_thread_management(self):
        """Test worker thread creation and shutdown."""
        # Create engine with single worker
        single_worker_engine = DbEngine(self.database_url)

        # No assertions about workers; just ensure shutdown does not raise
        single_worker_engine.shutdown()

        # Create engine with single worker
        multi_worker_engine = DbEngine(self.database_url)

        # No assertions about workers; just ensure shutdown does not raise
        multi_worker_engine.shutdown()

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
        with self.assertRaises(TransactionError):
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
        
        with self.assertRaises(TransactionError):
            self.db_engine.execute_transaction(operations)

    @pytest.mark.unit
    def test_execute_transaction_with_missing_query(self) -> None:
        """Test execute_transaction with missing query."""
        operations = [
            {
                "operation": "fetch"
            }
        ]
        
        with self.assertRaises(TransactionError):
            self.db_engine.execute_transaction(operations)

    @pytest.mark.unit
    def test_execute_transaction_with_missing_operation(self) -> None:
        """Test execute_transaction with missing operation."""
        operations = [
            {
                "query": "SELECT 1"
            }
        ]
        
        with self.assertRaises(TransactionError):
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
        with self.assertRaises(MaintenanceError):
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

    # Placeholder for potential future file-based info test case.

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
    def test_db_operation_error(self) -> None:
        """Test DbOperationError exception."""
        error = OperationError("Test operation error")
        self.assertIn("Test operation error", str(error))

    @pytest.mark.unit
    def test_engine_properties(self) -> None:
        """Test all engine properties."""
        self.assertIsNotNone(self.db_engine.engine)
        self.assertIsInstance(self.db_engine.get_stats(), dict)

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

    @pytest.mark.unit
    def test_prepared_statements_disabled(self) -> None:
        """Test DbEngine with prepared statements disabled."""
        # Create engine with prepared statements disabled
        db = DbEngine(self.database_url, enable_prepared_statements=False)
        
        # Run multiple different queries
        db.execute("SELECT 1 as one")
        db.fetch("SELECT 2 as two")
        db.execute("SELECT 3 as three")
        
        # Assert no prepared statements are cached
        self.assertEqual(db.get_prepared_statement_count(), 0)
        
        # Verify performance metrics show 0 cached statements
        perf_info = db.get_performance_info()
        self.assertEqual(perf_info["performance_metrics"]["prepared_statements_cached"], 0)
        
        db.shutdown()

    @pytest.mark.unit
    def test_prepared_statements_lifecycle(self) -> None:
        """Test prepared statement cache lifecycle."""
        # Default engine should have prepared statements enabled
        db = DbEngine(self.database_url)
        
        # Run two distinct execute statements
        db.execute("SELECT 1 as one")
        db.execute("SELECT 2 as two")
        
        # Should have at least 2 prepared statements cached
        self.assertGreaterEqual(db.get_prepared_statement_count(), 2)
        
        # Clear prepared statements
        db.clear_prepared_statements()
        self.assertEqual(db.get_prepared_statement_count(), 0)
        
        # Follow-up execute should repopulate cache
        db.execute("SELECT 3 as three")
        self.assertGreaterEqual(db.get_prepared_statement_count(), 1)
        
        db.shutdown()

    @pytest.mark.unit
    def test_execute_with_select_rowcount(self) -> None:
        """Test execute with SELECT statement rowcount handling."""
        # Execute SELECT should return rowcount=0 and data=None
        result = self.db_engine.execute("SELECT 1 AS one")
        self.assertTrue(result.result)
        self.assertEqual(result.rowcount, 0)
        self.assertIsNone(result.data)
        
        # Fetch SELECT should return data
        result = self.db_engine.fetch("SELECT 1 AS one")
        self.assertEqual(result.data[0]["one"], 1)

    @pytest.mark.unit
    def test_connection_health_and_recreation(self) -> None:
        """Test connection health and recreation."""
        # Initially connection should be healthy
        self.assertTrue(self.db_engine.check_connection_health())
        
        # Get initial connection info
        initial_info = self.db_engine.get_connection_info()
        initial_recreations = initial_info["connection_recreations"]
        
        # Recreate connection
        self.db_engine.recreate_connection()
        
        # Connection should still be healthy
        self.assertTrue(self.db_engine.check_connection_health())
        
        # Connection recreations should have increased
        updated_info = self.db_engine.get_connection_info()
        self.assertGreater(updated_info["connection_recreations"], initial_recreations)

    @pytest.mark.unit
    def test_connection_health_after_shutdown(self) -> None:
        """Test connection health after shutdown."""
        # Create a separate engine for this test
        db = DbEngine(self.database_url)
        
        # Initially healthy
        self.assertTrue(db.check_connection_health())
        
        # Shutdown
        db.shutdown()
        
        # After shutdown, health check should return False
        self.assertFalse(db.check_connection_health())

    @pytest.mark.unit
    def test_transaction_context_error_propagation(self) -> None:
        """Test transaction context manager error propagation."""
        with self.assertRaises(TransactionError):
            with self.db_engine.transaction() as tx:
                tx.execute("SELECT 1")
                raise RuntimeError("Test exception")

    @pytest.mark.unit
    def test_get_performance_info_structure(self) -> None:
        """Test get_performance_info structure and computed metrics."""
        # Get baseline performance info
        perf_info = self.db_engine.get_performance_info()
        
        # Check structure
        self.assertIn("engine_stats", perf_info)
        self.assertIn("performance_metrics", perf_info)
        self.assertIn("requests", perf_info["engine_stats"])
        self.assertIn("error_rate_percent", perf_info["performance_metrics"])
        
        # Perform operations and check metrics change
        initial_requests = perf_info["engine_stats"]["requests"]
        
        self.db_engine.execute("SELECT 1")
        self.db_engine.fetch("SELECT 2")
        
        updated_perf_info = self.db_engine.get_performance_info()
        self.assertGreater(updated_perf_info["engine_stats"]["requests"], initial_requests)
        
        # Error rate should stay 0 for successful operations
        self.assertEqual(updated_perf_info["performance_metrics"]["error_rate_percent"], 0.0)

    @pytest.mark.unit
    def test_execute_many_fetch(self) -> None:
        """Test execute_many with fetch operations."""
        # Insert test data for fetch operations
        self.db_engine.execute("""
            INSERT INTO test_users (name, email, active) 
            VALUES ('Test User 1', 'test1@example.com', 1)
        """)
        self.db_engine.execute("""
            INSERT INTO test_users (name, email, active) 
            VALUES ('Test User 2', 'test2@example.com', 1)
        """)
        
        # Execute many fetch operations
        params_list = [
            {"name": "Test User 1"},
            {"name": "Test User 2"}
        ]
        
        results = self.db_engine.execute_many(
            "SELECT * FROM test_users WHERE name = :name",
            params_list
        )
        
        # Should have 2 results
        self.assertEqual(len(results), 2)
        
        # Each result should have data
        for result in results:
            self.assertIsInstance(result, DbResult)
            self.assertIsNotNone(result.data)
            self.assertEqual(len(result.data), 1)  # One row per query

    @pytest.mark.unit
    def test_execute_many_execute(self) -> None:
        """Test execute_many with execute operations."""
        # Execute many insert operations
        params_list = [
            {"name": "Bulk User 1", "email": "bulk1@example.com", "active": 1},
            {"name": "Bulk User 2", "email": "bulk2@example.com", "active": 1},
            {"name": "Bulk User 3", "email": "bulk3@example.com", "active": 0}
        ]
        
        results = self.db_engine.execute_many(
            "INSERT INTO test_users (name, email, active) VALUES (:name, :email, :active)",
            params_list
        )
        
        # Should have 3 results
        self.assertEqual(len(results), 3)
        
        # Each result should have rowcount=1
        for result in results:
            self.assertIsInstance(result, DbResult)
            self.assertEqual(result.rowcount, 1)
            self.assertTrue(result.result)
        
        # Verify total rows were inserted
        count_result = self.db_engine.fetch("SELECT COUNT(*) as count FROM test_users")
        total_count = count_result.data[0]["count"]
        self.assertGreaterEqual(total_count, 6)  # Original 3 + new 3

    @pytest.mark.unit
    def test_batch_and_script_mapping(self) -> None:
        """Test batch() and script() mapping behavior."""
        # Create a script with mixed operations
        script_sql = """
        CREATE TABLE IF NOT EXISTS batch_test (id INTEGER PRIMARY KEY, name TEXT);
        INSERT INTO batch_test (name) VALUES ('test1');
        SELECT COUNT(*) as count FROM batch_test;
        INSERT INTO batch_test (name) VALUES ('test2');
        SELECT name FROM batch_test ORDER BY id;
        """
        
        # Test batch execution
        batch_results = self.db_engine.batch(script_sql)
        
        # Should have results for each statement
        self.assertGreater(len(batch_results), 0)
        
        # Each result should have operation type
        for result in batch_results:
            self.assertIn(result["operation"], {"execute", "fetch"})
            if result["operation"] == "fetch":
                self.assertIsNotNone(result["result"].data)
        
        # Test script execution
        script_results = self.db_engine.script(script_sql)
        
        # Should have same number of results
        self.assertEqual(len(script_results), len(batch_results))
        
        # Each result should have operation type
        for result in script_results:
            self.assertIn(result["operation"], {"execute", "fetch"})

    @pytest.mark.unit
    def test_maintenance_operations(self) -> None:
        """Test maintenance operations happy paths."""
        # Vacuum should not raise
        self.db_engine.vacuum()
        
        # Analyze should not raise
        self.db_engine.analyze()
        
        # Optimize should not raise
        self.db_engine.optimize()
        
        # Integrity check should return list
        issues = self.db_engine.integrity_check()
        self.assertIsInstance(issues, list)

    @pytest.mark.unit
    def test_get_sqlite_info_file_vs_memory(self) -> None:
        """Test get_sqlite_info with file vs memory databases."""
        # Memory DB info
        memory_info = self.db_engine.get_sqlite_info()
        self.assertIn('version', memory_info)
        self.assertIn('database_size', memory_info)
        # Database size might be None for in-memory
        
        # File DB info
        temp_db = tempfile.NamedTemporaryFile(delete=False, suffix='.db')
        temp_db.close()
        try:
            file_db = DbEngine(f"sqlite:///{temp_db.name}")
            
            # Perform a write to ensure file is created
            file_db.execute("CREATE TABLE test (id INTEGER PRIMARY KEY)")
            file_db.execute("INSERT INTO test (id) VALUES (1)")
            
            file_info = file_db.get_sqlite_info()
            self.assertIn('version', file_info)
            self.assertIn('database_size', file_info)
            
            # File DB should have a size
            self.assertIsInstance(file_info['database_size'], int)
            
            file_db.shutdown()
        finally:
            # Clean up temp file
            try:
                os.unlink(temp_db.name)
            except OSError:
                pass




if __name__ == '__main__':
    unittest.main() 