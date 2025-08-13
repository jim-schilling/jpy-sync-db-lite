"""
Core unit tests for DbEngine class.

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
import re

from jpy_sync_db_lite.db_engine import DbEngine, DbOperationError, SQLiteError, DbResult
from jpy_sync_db_lite.db_request import DbRequest


class TestDbEngine(unittest.TestCase):
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
    def test_init_with_default_parameters(self):
        """Test DbEngine initialization with default parameters."""
        db = DbEngine(self.database_url)
        # No assertions about workers; just ensure initialization does not raise
        db.shutdown()

    @pytest.mark.unit
    def test_execute_simple_query(self) -> None:
        """Test execute for DML and fetch for SELECT (behavior)."""
        result = self.db_engine.execute("UPDATE test_users SET active = 1 WHERE 1=1")
        self.assertTrue(result.result)
        self.assertIsInstance(result.rowcount, int)
        self.assertGreaterEqual(result.rowcount, 0)

        data = self.db_engine.fetch("SELECT 1 as one")
        self.assertEqual(data.data[0]["one"], 1)

    @pytest.mark.unit
    def test_fetch_simple_query(self) -> None:
        """Test simple fetch operation."""
        users = self.db_engine.fetch("SELECT * FROM test_users")
        self.assertEqual(len(users.data), 3)
        self.assertIsInstance(users.data, list)
        self.assertIsInstance(users.data[0], dict)

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
    def test_shutdown_cleanup(self):
        """Test clean shutdown of DbEngine."""
        db = DbEngine(self.database_url)
        # No assertions about workers; just ensure shutdown does not raise
        db.shutdown()

    @pytest.mark.integration
    def test_batch_simple_ddl_dml(self) -> None:
        """Test simple batch execution with DDL and DML statements."""
        batch_sql = """
        CREATE TABLE IF NOT EXISTS batch_test (
            id INTEGER PRIMARY KEY,
            name TEXT,
            value INTEGER
        );

        INSERT INTO batch_test (name, value) VALUES ('test1', 100);
        UPDATE batch_test SET value = 150 WHERE name = 'test1';
        """

        results = self.db_engine.batch(batch_sql)

        # Verify results
        self.assertEqual(len(results), 3)

        # Check CREATE TABLE
        self.assertEqual(results[0]['operation'], 'execute')
        self.assertEqual(results[1]['operation'], 'execute')
        self.assertEqual(results[2]['operation'], 'execute')

        # Verify data was actually inserted and updated
        data = self.db_engine.fetch("SELECT * FROM batch_test ORDER BY id")
        self.assertEqual(len(data.data), 1)
        self.assertEqual(data.data[0]['name'], 'test1')
        self.assertEqual(data.data[0]['value'], 150)

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

    @pytest.mark.unit
    def test_get_raw_connection(self):
        """Test getting raw database connection."""
        with self.db_engine.get_raw_connection() as conn:
            self.assertIsNotNone(conn)
            result = conn.execute(text("SELECT 1"))
            self.assertIsNotNone(result)




if __name__ == '__main__':
    unittest.main() 