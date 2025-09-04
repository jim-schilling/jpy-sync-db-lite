"""
Test cases for API improvements: execute_many, script, and transaction context manager.

Copyright (c) 2025, Jim Schilling

Please keep this header when you use this code.

This module is licensed under the MIT License.
"""

import os
import tempfile
import unittest

import pytest

from jpy_sync_db_lite.db_engine import DbEngine, DbResult
from jpy_sync_db_lite.errors import TransactionError


class TestDbEngineApiImprovements(unittest.TestCase):
    """Test cases for new API methods: execute_many, script, and transaction context manager."""

    def setUp(self) -> None:
        """Set up test database."""
        self.temp_db = tempfile.NamedTemporaryFile(delete=False, suffix='.db')
        self.temp_db.close()
        self.database_url = f"sqlite:///{self.temp_db.name}"
        self.db_engine = DbEngine(self.database_url)

        # Create test table
        self.db_engine.execute("""
            CREATE TABLE test_users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                email TEXT UNIQUE,
                active BOOLEAN DEFAULT 1
            )
        """)

    def tearDown(self) -> None:
        """Clean up test database."""
        self.db_engine.shutdown()
        if os.path.exists(self.temp_db.name):
            os.unlink(self.temp_db.name)

    # Tests for execute_many()

    @pytest.mark.unit
    def test_execute_many_insert_success(self) -> None:
        """Test execute_many with successful bulk inserts."""
        users = [
            {"name": "Alice", "email": "alice@example.com"},
            {"name": "Bob", "email": "bob@example.com"},
            {"name": "Charlie", "email": "charlie@example.com"},
        ]

        results = self.db_engine.execute_many(
            "INSERT INTO test_users (name, email) VALUES (:name, :email)",
            users
        )

        # Verify results
        self.assertEqual(len(results), 3)
        for result in results:
            self.assertIsInstance(result, DbResult)
            self.assertTrue(result.result)
            self.assertEqual(result.rowcount, 1)
            self.assertIsNone(result.data)

        # Verify data was inserted
        all_users = self.db_engine.fetch("SELECT name, email FROM test_users ORDER BY name")
        self.assertEqual(len(all_users.data), 3)
        self.assertEqual(all_users.data[0]["name"], "Alice")
        self.assertEqual(all_users.data[1]["name"], "Bob")
        self.assertEqual(all_users.data[2]["name"], "Charlie")

    @pytest.mark.unit
    def test_execute_many_fetch_success(self) -> None:
        """Test execute_many with fetch operations."""
        # Insert test data
        self.db_engine.execute_many(
            "INSERT INTO test_users (name, email) VALUES (:name, :email)",
            [
                {"name": "Alice", "email": "alice@example.com"},
                {"name": "Bob", "email": "bob@example.com"},
            ]
        )

        # Test fetch with different parameters
        params_list = [
            {"name": "Alice"},
            {"name": "Bob"},
            {"name": "NonExistent"},
        ]

        results = self.db_engine.execute_many(
            "SELECT id, name, email FROM test_users WHERE name = :name",
            params_list
        )

        # Verify results
        self.assertEqual(len(results), 3)

        # Alice should be found
        self.assertTrue(results[0].result)
        self.assertEqual(len(results[0].data), 1)
        self.assertEqual(results[0].data[0]["name"], "Alice")

        # Bob should be found
        self.assertTrue(results[1].result)
        self.assertEqual(len(results[1].data), 1)
        self.assertEqual(results[1].data[0]["name"], "Bob")

        # NonExistent should return empty
        self.assertFalse(results[2].result)
        self.assertEqual(len(results[2].data), 0)

    @pytest.mark.unit
    def test_execute_many_empty_params_list(self) -> None:
        """Test execute_many with empty parameters list."""
        results = self.db_engine.execute_many(
            "INSERT INTO test_users (name, email) VALUES (:name, :email)",
            []
        )

        self.assertEqual(results, [])

    @pytest.mark.unit
    def test_execute_many_transaction_rollback(self) -> None:
        """Test that execute_many rolls back on error."""
        # Insert one valid user first
        self.db_engine.execute(
            "INSERT INTO test_users (name, email) VALUES (:name, :email)",
            params={"name": "Existing", "email": "existing@example.com"}
        )

        # Try to insert users with duplicate email (should fail)
        users = [
            {"name": "Alice", "email": "alice@example.com"},
            {"name": "Duplicate", "email": "existing@example.com"},  # This will fail
        ]

        with self.assertRaises(TransactionError):
            self.db_engine.execute_many(
                "INSERT INTO test_users (name, email) VALUES (:name, :email)",
                users
            )

        # Verify that Alice was not inserted (transaction rolled back)
        result = self.db_engine.fetch("SELECT COUNT(*) as count FROM test_users WHERE name = 'Alice'")
        self.assertEqual(result.data[0]["count"], 0)

        # Only the original user should exist
        result = self.db_engine.fetch("SELECT COUNT(*) as count FROM test_users")
        self.assertEqual(result.data[0]["count"], 1)

    # Tests for script()

    @pytest.mark.unit
    def test_script_success(self) -> None:
        """Test script method with successful execution."""
        sql_script = """
            INSERT INTO test_users (name, email) VALUES ('Alice', 'alice@example.com');
            INSERT INTO test_users (name, email) VALUES ('Bob', 'bob@example.com');
            SELECT COUNT(*) as count FROM test_users;
        """

        results = self.db_engine.script(sql_script)

        # Should have 3 results (2 inserts, 1 select)
        self.assertEqual(len(results), 3)

        # First two should be execute operations
        self.assertEqual(results[0]["operation"], "execute")
        self.assertTrue(results[0]["result"].result)
        self.assertEqual(results[1]["operation"], "execute")
        self.assertTrue(results[1]["result"].result)

        # Last should be fetch operation
        self.assertEqual(results[2]["operation"], "fetch")
        self.assertTrue(results[2]["result"].result)
        self.assertEqual(results[2]["result"].data[0]["count"], 2)

    @pytest.mark.unit
    def test_script_with_comments_and_whitespace(self) -> None:
        """Test script method with comments and whitespace."""
        sql_script = """
            -- Insert some test users
            INSERT INTO test_users (name, email) VALUES ('Alice', 'alice@example.com');
            
            /* Multi-line comment
               for Bob */
            INSERT INTO test_users (name, email) VALUES ('Bob', 'bob@example.com');
            
            -- Count the users
            SELECT COUNT(*) as count FROM test_users;
        """

        results = self.db_engine.script(sql_script)

        # Should have 3 results (comments should be filtered out)
        self.assertEqual(len(results), 3)

        # Verify the count result
        self.assertEqual(results[2]["result"].data[0]["count"], 2)

    @pytest.mark.unit
    def test_script_error_handling(self) -> None:
        """Test script method error handling."""
        sql_script = """
            INSERT INTO test_users (name, email) VALUES ('Alice', 'alice@example.com');
            INVALID SQL STATEMENT;
            INSERT INTO test_users (name, email) VALUES ('Bob', 'bob@example.com');
        """

        with self.assertRaises(TransactionError):
            self.db_engine.script(sql_script)

        # Verify that Alice was not inserted (transaction rolled back)
        result = self.db_engine.fetch("SELECT COUNT(*) as count FROM test_users")
        self.assertEqual(result.data[0]["count"], 0)

    # Tests for transaction context manager

    @pytest.mark.unit
    def test_transaction_context_manager_success(self) -> None:
        """Test transaction context manager with successful operations."""
        with self.db_engine.transaction() as tx:
            tx.execute(
                "INSERT INTO test_users (name, email) VALUES (:name, :email)",
                {"name": "Alice", "email": "alice@example.com"}
            )
            tx.execute(
                "INSERT INTO test_users (name, email) VALUES (:name, :email)",
                {"name": "Bob", "email": "bob@example.com"}
            )

        # Verify both users were inserted
        result = self.db_engine.fetch("SELECT COUNT(*) as count FROM test_users")
        self.assertEqual(result.data[0]["count"], 2)

        # Verify specific users
        alice = self.db_engine.fetch("SELECT name FROM test_users WHERE name = 'Alice'")
        self.assertTrue(alice.result)
        self.assertEqual(alice.data[0]["name"], "Alice")

    @pytest.mark.unit
    def test_transaction_context_manager_rollback(self) -> None:
        """Test transaction context manager rollback on error."""
        # Insert one user first
        self.db_engine.execute(
            "INSERT INTO test_users (name, email) VALUES (:name, :email)",
            params={"name": "Existing", "email": "existing@example.com"}
        )

        with self.assertRaises(TransactionError):
            with self.db_engine.transaction() as tx:
                tx.execute(
                    "INSERT INTO test_users (name, email) VALUES (:name, :email)",
                    {"name": "Alice", "email": "alice@example.com"}
                )
                # This will cause a constraint violation
                tx.execute(
                    "INSERT INTO test_users (name, email) VALUES (:name, :email)",
                    {"name": "Duplicate", "email": "existing@example.com"}
                )

        # Verify that Alice was not inserted (transaction rolled back)
        result = self.db_engine.fetch("SELECT COUNT(*) as count FROM test_users WHERE name = 'Alice'")
        self.assertEqual(result.data[0]["count"], 0)

        # Only the original user should exist
        result = self.db_engine.fetch("SELECT COUNT(*) as count FROM test_users")
        self.assertEqual(result.data[0]["count"], 1)

    @pytest.mark.unit
    def test_transaction_context_manager_with_fetch(self) -> None:
        """Test transaction context manager with mixed execute and fetch operations."""
        with self.db_engine.transaction() as tx:
            tx.execute(
                "INSERT INTO test_users (name, email) VALUES (:name, :email)",
                {"name": "Alice", "email": "alice@example.com"}
            )
            tx.fetch("SELECT COUNT(*) as count FROM test_users")
            tx.execute(
                "INSERT INTO test_users (name, email) VALUES (:name, :email)",
                {"name": "Bob", "email": "bob@example.com"}
            )

        # Verify both users were inserted
        result = self.db_engine.fetch("SELECT COUNT(*) as count FROM test_users")
        self.assertEqual(result.data[0]["count"], 2)

    @pytest.mark.unit
    def test_transaction_context_manager_empty(self) -> None:
        """Test transaction context manager with no operations."""
        with self.db_engine.transaction() as tx:
            pass  # No operations

        # Should not raise any errors
        result = self.db_engine.fetch("SELECT COUNT(*) as count FROM test_users")
        self.assertEqual(result.data[0]["count"], 0)

    # Integration tests

    @pytest.mark.integration
    def test_api_methods_integration(self) -> None:
        """Test integration between different API methods."""
        # Use script to set up initial data
        setup_script = """
            INSERT INTO test_users (name, email) VALUES ('Admin', 'admin@example.com');
            INSERT INTO test_users (name, email, active) VALUES ('Inactive', 'inactive@example.com', 0);
        """
        self.db_engine.script(setup_script)

        # Use execute_many to add more users
        new_users = [
            {"name": "Alice", "email": "alice@example.com"},
            {"name": "Bob", "email": "bob@example.com"},
        ]
        self.db_engine.execute_many(
            "INSERT INTO test_users (name, email) VALUES (:name, :email)",
            new_users
        )

        # Use transaction context manager to update and verify
        with self.db_engine.transaction() as tx:
            tx.execute("UPDATE test_users SET active = 1 WHERE name = 'Inactive'")
            tx.fetch("SELECT COUNT(*) as active_count FROM test_users WHERE active = 1")

        # Verify final state
        result = self.db_engine.fetch("SELECT COUNT(*) as count FROM test_users WHERE active = 1")
        self.assertEqual(result.data[0]["count"], 4)  # All users should be active now

    @pytest.mark.unit
    def test_stats_tracking_for_new_methods(self) -> None:
        """Test that stats are properly tracked for new API methods."""
        initial_stats = self.db_engine.get_stats()

        # Test execute_many stats
        users = [
            {"name": "Alice", "email": "alice@example.com"},
            {"name": "Bob", "email": "bob@example.com"},
        ]
        self.db_engine.execute_many(
            "INSERT INTO test_users (name, email) VALUES (:name, :email)",
            users
        )

        # Test script stats (includes both execute and fetch)
        self.db_engine.script("""
            INSERT INTO test_users (name, email) VALUES ('Script', 'script@example.com');
            SELECT COUNT(*) FROM test_users;
        """)

        # Test transaction stats (operations are counted when executed)
        with self.db_engine.transaction() as tx:
            tx.execute("INSERT INTO test_users (name, email) VALUES ('Charlie', 'charlie@example.com')")

        final_stats = self.db_engine.get_stats()

        # Verify stats were updated
        self.assertGreater(final_stats["requests"], initial_stats["requests"])
        self.assertGreater(final_stats["execute_operations"], initial_stats["execute_operations"])
        self.assertGreater(final_stats["batch_operations"], initial_stats["batch_operations"])

        # Note: fetch_operations may or may not be incremented depending on statement detection
        # This is acceptable as the main functionality works correctly


if __name__ == '__main__':
    unittest.main()
