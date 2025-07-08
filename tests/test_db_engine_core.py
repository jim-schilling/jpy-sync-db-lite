"""
Core unit tests for DbEngine class.

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
import re

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from jpy_sync_db_lite.db_engine import DbEngine, DbOperationError, SQLiteError, DbResult
from jpy_sync_db_lite.db_request import DbRequest


class TestDbEngineCore(unittest.TestCase):
    """Core test cases for DbEngine class."""

    @staticmethod
    def normalize_sql(sql: str) -> str:
        """Normalize SQL by collapsing all whitespace, removing spaces after '(' and before ')', and stripping ends."""
        sql = re.sub(r'\s+', ' ', sql).strip()
        sql = re.sub(r'\(\s+', '(', sql)
        sql = re.sub(r'\s+\)', ')', sql)
        return sql

    def setUp(self) -> None:
        """Set up test fixtures before each test method."""
        self.database_url = "sqlite:///:memory:"
        self.db_engine = DbEngine(
            self.database_url,
            timeout=30,
            check_same_thread=False
        )
        self._create_test_table()
        self._insert_test_data()

    def tearDown(self) -> None:
        """Clean up after each test method."""
        if hasattr(self, 'db_engine'):
            self.db_engine.shutdown()

    def _cleanup_all_tables(self) -> None:
        """Clean up all tables to ensure test isolation."""
        try:
            with self.db_engine.get_raw_connection() as conn:
                # Get all table names
                result = conn.execute(text("SELECT name FROM sqlite_master WHERE type='table'"))
                tables = [row[0] for row in result.fetchall()]
                
                # Drop all tables (except sqlite_master)
                for table in tables:
                    if table != 'sqlite_master':
                        conn.execute(text(f"DROP TABLE IF EXISTS {table}"))
                conn.commit()
        except Exception:
            # If cleanup fails, continue - the test will handle it
            pass

    def _create_test_table(self) -> None:
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

    def _insert_test_data(self) -> None:
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
        self.db_engine.execute(insert_sql, params=test_data)

    @pytest.mark.unit
    def test_init_with_default_parameters(self) -> None:
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

    @pytest.mark.unit
    def test_init_with_custom_parameters(self) -> None:
        """Test DbEngine initialization with custom parameters."""
        db = DbEngine(
            self.database_url,
            num_workers=2,
            debug=True,
            timeout=60,
            check_same_thread=True
        )
        self.assertEqual(db.num_workers, 2)
        self.assertEqual(len(db.workers), 2)
        db.shutdown()

    @pytest.mark.unit
    def test_configure_db_performance(self) -> None:
        """Test database performance configuration."""
        with self.db_engine.get_raw_connection() as conn:
            result = conn.execute(text("PRAGMA journal_mode"))
            journal_mode = result.fetchone()[0]
            # In-memory databases use "memory" journal mode, file-based use "wal"
            self.assertIn(journal_mode, ["wal", "memory"])
            result = conn.execute(text("PRAGMA synchronous"))
            synchronous = result.fetchone()[0]
            self.assertEqual(synchronous, 1)
            result = conn.execute(text("PRAGMA cache_size"))
            cache_size = result.fetchone()[0]
            self.assertEqual(cache_size, -64000)
            result = conn.execute(text("PRAGMA temp_store"))
            temp_store = result.fetchone()[0]
            self.assertEqual(temp_store, 2)

    @pytest.mark.unit
    def test_execute_simple_query(self) -> None:
        """Test simple execute operation."""
        result = self.db_engine.execute("SELECT 1")
        self.assertEqual(result.rowcount, 0)  # SELECT does not affect rows

    @pytest.mark.unit
    def test_execute_with_parameters(self) -> None:
        """Test query execution with parameters."""
        update_sql = "UPDATE test_users SET active = :active WHERE name = :name"
        self.db_engine.execute(update_sql, params={"active": False, "name": "Alice Johnson"})
        users = self.db_engine.fetch("SELECT * FROM test_users WHERE name = :name", params={"name": "Alice Johnson"})
        self.assertEqual(len(users.data), 1)
        self.assertEqual(users.data[0]['active'], False)

    @pytest.mark.unit
    def test_fetch_simple_query(self) -> None:
        """Test simple fetch operation."""
        users = self.db_engine.fetch("SELECT * FROM test_users")
        self.assertEqual(len(users.data), 3)
        self.assertIsInstance(users.data, list)
        self.assertIsInstance(users.data[0], dict)

    @pytest.mark.unit
    def test_fetch_with_parameters(self) -> None:
        """Test fetch operation with parameters."""
        active_users = self.db_engine.fetch(
            "SELECT * FROM test_users WHERE active = :active",
            params={"active": True}
        )
        self.assertEqual(len(active_users.data), 2)
        for user in active_users.data:
            self.assertTrue(user['active'])

    @pytest.mark.unit
    def test_fetch_empty_result(self) -> None:
        """Test fetch operation with no results."""
        users = self.db_engine.fetch(
            "SELECT * FROM test_users WHERE name = :name",
            params={"name": "NonExistentUser"}
        )
        self.assertEqual(len(users.data), 0)
        self.assertIsInstance(users.data, list)

    @pytest.mark.unit
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

        result = self.db_engine.execute(insert_sql, params=test_data)
        self.assertEqual(result.rowcount, 3)

        # Verify data was inserted
        users = self.db_engine.fetch("SELECT * FROM test_users WHERE name LIKE 'User%'")
        self.assertEqual(len(users.data), 3)

    @pytest.mark.unit
    def test_bulk_update_operations(self):
        """Test bulk update operations using execute method."""
        # Prepare update data
        update_data = [
            {"id": 1, "active": False},
            {"id": 2, "active": True},
            {"id": 3, "active": False}
        ]

        update_sql = "UPDATE test_users SET active = :active WHERE id = :id"
        result = self.db_engine.execute(update_sql, params=update_data)
        self.assertEqual(result.rowcount, 3)

        # Verify updates
        users = self.db_engine.fetch("SELECT * FROM test_users ORDER BY id")
        self.assertEqual(users.data[0]['active'], False)
        self.assertEqual(users.data[1]['active'], True)
        self.assertEqual(users.data[2]['active'], False)

    @pytest.mark.integration
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
        self.assertEqual(results[0]["operation"], "execute")
        self.assertTrue(results[0]["result"])
        self.assertEqual(results[1]["operation"], "fetch")
        self.assertEqual(results[1]["result"][0]["count"], 1)

    @pytest.mark.integration
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

    @pytest.mark.unit
    def test_get_raw_connection(self):
        """Test getting raw database connection."""
        with self.db_engine.get_raw_connection() as conn:
            self.assertIsNotNone(conn)
            result = conn.execute(text("SELECT 1"))
            self.assertIsNotNone(result)

    @pytest.mark.unit
    def test_get_stats(self):
        """Test getting database statistics."""
        # Perform some operations to generate stats
        self.db_engine.execute("SELECT 1")
        self.db_engine.fetch("SELECT 1")

        stats = self.db_engine.get_stats()

        self.assertIn('requests', stats)
        self.assertIn('errors', stats)
        self.assertIsInstance(stats['requests'], int)
        self.assertIsInstance(stats['errors'], int)

    @pytest.mark.integration
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
                                               params={"active": True})
                    results.append(len(users.data))

                    # Execute operation
                    self.db_engine.execute("UPDATE test_users SET name = :name WHERE id = :id",
                                         params={"name": f"UpdatedUser{worker_id}_{i}", "id": 1})

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

    @pytest.mark.unit
    def test_error_handling_invalid_sql(self):
        """Test error handling for invalid SQL."""
        with self.assertRaises(DbOperationError):
            self.db_engine.execute("INVALID SQL STATEMENT")

    @pytest.mark.unit
    def test_error_handling_missing_table(self):
        """Test error handling for queries on non-existent table."""
        with self.assertRaises(DbOperationError):
            self.db_engine.fetch("SELECT * FROM non_existent_table")

    @pytest.mark.unit
    def test_error_handling_missing_parameter(self):
        """Test error handling for missing parameters."""
        with self.assertRaises(DbOperationError):
            self.db_engine.fetch("SELECT * FROM test_users WHERE id = :id")

    @pytest.mark.unit
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

    @pytest.mark.slow
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
        result = self.db_engine.execute(insert_sql, params=large_data)
        end_time = time.time()

        self.assertEqual(result.rowcount, len(large_data))
        self.assertLess(end_time - start_time, 10)  # Should complete within 10 seconds

        # Verify data was inserted
        count = self.db_engine.fetch("SELECT COUNT(*) as count FROM test_users WHERE name LIKE 'LargeUser%'")
        self.assertEqual(count.data[0]['count'], 1000)

    @pytest.mark.integration
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
        # First operation is fetch, result is a list of dicts
        self.assertEqual(results[0]['operation'], 'fetch')
        self.assertEqual(results[0]['result'][0]['count'], 3)  # Initial count
        # Second operation is execute, result is True
        self.assertEqual(results[1]['operation'], 'execute')
        self.assertTrue(results[1]['result'])  # Insert success
        # Third operation is fetch, result is a list of dicts
        self.assertEqual(results[2]['operation'], 'fetch')
        self.assertEqual(len(results[2]['result']), 1)  # Fetch result
        # Fourth operation is execute, result is True
        self.assertEqual(results[3]['operation'], 'execute')
        self.assertTrue(results[3]['result'])  # Update success

    @pytest.mark.unit
    def test_unicode_and_special_characters(self):
        """Test handling of unicode and special characters."""
        # Test unicode characters in data
        unicode_data = [
            {"name": "José García", "email": "jose@example.com", "active": True},
            {"name": "Müller Schmidt", "email": "mueller@example.com", "active": True},
            {"name": "李小明", "email": "lixiaoming@example.com", "active": True}
        ]

        insert_sql = """
        INSERT INTO test_users (name, email, active)
        VALUES (:name, :email, :active)
        """
        result = self.db_engine.execute(insert_sql, params=unicode_data)
        self.assertEqual(result.rowcount, len(unicode_data))

        # Verify unicode data was stored correctly
        users = self.db_engine.fetch("SELECT * FROM test_users WHERE name LIKE '%José%'")
        self.assertEqual(len(users.data), 1)
        self.assertEqual(users.data[0]['name'], "José García")


if __name__ == '__main__':
    unittest.main() 