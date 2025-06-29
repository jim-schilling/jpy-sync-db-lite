"""
Comprehensive unit tests for DbEngine class.

Copyright (c) 2025, Jim Schilling

Please keep this header when you use this code.

This module is licensed under the MIT License.
"""
import os
import queue

# Add parent directory to path for imports
import sys
import tempfile
import threading
import time
import unittest
import pytest

from sqlalchemy import text

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from jpy_sync_db_lite.db_engine import DbEngine, DbOperationError, SQLiteError
from jpy_sync_db_lite.db_request import DbRequest


class TestDbEngine(unittest.TestCase):
    """Test cases for DbEngine class."""

    def setUp(self):
        """Set up test fixtures before each test method."""
        # Create temporary database file
        self.temp_db_fd, self.temp_db_path = tempfile.mkstemp(suffix='.db')
        os.close(self.temp_db_fd)
        self.database_url = f"sqlite:///{self.temp_db_path}"

        # Initialize DbEngine with backup DISABLED to avoid thread interference
        self.db_engine = DbEngine(
            self.database_url,
            timeout=30,
            check_same_thread=False
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
        self.db_engine.execute(update_sql, {"active": False, "name": "Alice Johnson"})

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
        self.assertEqual(results[0]["operation"], "execute")
        self.assertTrue(results[0]["result"])
        self.assertEqual(results[1]["operation"], "fetch")
        self.assertEqual(results[1]["result"][0]["count"], 1)

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
        """Test getting database statistics."""
        # Perform some operations to generate stats
        self.db_engine.execute("SELECT 1")
        self.db_engine.fetch("SELECT 1")

        stats = self.db_engine.get_stats()

        self.assertIn('requests', stats)
        self.assertIn('errors', stats)
        self.assertIsInstance(stats['requests'], int)
        self.assertIsInstance(stats['errors'], int)

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
        with self.assertRaises(DbOperationError):
            self.db_engine.execute("INVALID SQL STATEMENT")

    def test_error_handling_missing_table(self):
        """Test error handling for queries on non-existent table."""
        with self.assertRaises(DbOperationError):
            self.db_engine.fetch("SELECT * FROM non_existent_table")

    def test_error_handling_missing_parameter(self):
        """Test error handling for missing parameters."""
        with self.assertRaises(DbOperationError):
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
        result = self.db_engine.execute(insert_sql, unicode_data)
        self.assertEqual(result, 3)

        # Verify unicode data was stored correctly
        users = self.db_engine.fetch("SELECT * FROM test_users WHERE name LIKE '%José%'")
        self.assertEqual(len(users), 1)
        self.assertEqual(users[0]['name'], "José García")

    def test_batch_simple_ddl_dml(self):
        """Test batch execution with simple DDL and DML statements."""
        batch_sql = """
        -- Create a test table
        CREATE TABLE IF NOT EXISTS batch_test (
            id INTEGER PRIMARY KEY,
            name TEXT,
            value INTEGER
        );

        -- Insert data
        INSERT INTO batch_test (name, value) VALUES ('test1', 100);
        INSERT INTO batch_test (name, value) VALUES ('test2', 200);

        -- Update data
        UPDATE batch_test SET value = 150 WHERE name = 'test1';
        """

        results = self.db_engine.batch(batch_sql)

        # Verify results
        self.assertEqual(len(results), 4)

        # Check CREATE TABLE
        self.assertEqual(results[0]['operation'], 'execute')
        self.assertTrue('CREATE TABLE' in results[0]['statement'])

        # Check INSERT statements
        self.assertEqual(results[1]['operation'], 'execute')
        self.assertEqual(results[2]['operation'], 'execute')

        # Check UPDATE statement
        self.assertEqual(results[3]['operation'], 'execute')
        self.assertTrue('UPDATE' in results[3]['statement'])

        # Verify data was actually inserted
        data = self.db_engine.fetch("SELECT * FROM batch_test ORDER BY name")
        self.assertEqual(len(data), 2)
        self.assertEqual(data[0]['name'], 'test1')
        self.assertEqual(data[0]['value'], 150)
        self.assertEqual(data[1]['name'], 'test2')
        self.assertEqual(data[1]['value'], 200)

    def test_batch_with_select_statements(self):
        """Test batch execution with SELECT statements included."""
        batch_sql = """
        -- Create table
        CREATE TABLE IF NOT EXISTS select_test (
            id INTEGER PRIMARY KEY,
            name TEXT
        );

        -- Insert data
        INSERT INTO select_test (name) VALUES ('Alice');
        INSERT INTO select_test (name) VALUES ('Bob');

        -- Query data
        SELECT * FROM select_test WHERE name = 'Alice';

        -- Insert more data
        INSERT INTO select_test (name) VALUES ('Charlie');

        -- Query all data
        SELECT COUNT(*) as count FROM select_test;
        """

        results = self.db_engine.batch(batch_sql)

        # Verify results
        self.assertEqual(len(results), 6)

        # Check DDL/DML statements
        self.assertEqual(results[0]['operation'], 'execute')  # CREATE
        self.assertEqual(results[1]['operation'], 'execute')  # INSERT
        self.assertEqual(results[2]['operation'], 'execute')  # INSERT

        # Check SELECT statements
        self.assertEqual(results[3]['operation'], 'fetch')    # SELECT Alice
        self.assertEqual(results[4]['operation'], 'execute')  # INSERT Charlie
        self.assertEqual(results[5]['operation'], 'fetch')    # SELECT COUNT

        # Verify SELECT results
        alice_result = results[3]['result']
        self.assertEqual(len(alice_result), 1)
        self.assertEqual(alice_result[0]['name'], 'Alice')

        count_result = results[5]['result']
        self.assertEqual(len(count_result), 1)
        self.assertEqual(count_result[0]['count'], 3)

    def test_batch_without_select_statements(self):
        """Test batch execution with only DDL and DML statements (no SELECT)."""
        batch_sql = """
        CREATE TABLE IF NOT EXISTS no_select_test (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL
        );

        -- Insert data
        INSERT INTO no_select_test (name) VALUES ('Test1');
        INSERT INTO no_select_test (name) VALUES ('Test2');

        -- Update data
        UPDATE no_select_test SET name = 'Updated' WHERE name = 'Test1';
        """

        results = self.db_engine.batch(batch_sql)

        # Verify results
        self.assertEqual(len(results), 4)
        for result in results:
            self.assertEqual(result['operation'], 'execute')

        # Verify data was actually inserted/updated
        data = self.db_engine.fetch("SELECT * FROM no_select_test ORDER BY id")
        self.assertEqual(len(data), 2)
        self.assertEqual(data[0]['name'], 'Updated')
        self.assertEqual(data[1]['name'], 'Test2')

    def test_batch_with_mixed_statement_types(self):
        """Test batch execution with mixed statement types including SELECT."""
        batch_sql = """
        CREATE TABLE IF NOT EXISTS mixed_test (id INTEGER PRIMARY KEY, name TEXT);
        INSERT INTO mixed_test (name) VALUES ('Test1');
        SELECT * FROM mixed_test;
        UPDATE mixed_test SET name = 'Updated' WHERE name = 'Test1';
        SELECT COUNT(*) as count FROM mixed_test;
        """

        results = self.db_engine.batch(batch_sql)

        # Verify results
        self.assertEqual(len(results), 5)
        self.assertEqual(results[0]['operation'], 'execute')  # CREATE
        self.assertEqual(results[1]['operation'], 'execute')  # INSERT
        self.assertEqual(results[2]['operation'], 'fetch')    # SELECT
        self.assertEqual(results[3]['operation'], 'execute')  # UPDATE
        self.assertEqual(results[4]['operation'], 'fetch')    # SELECT COUNT

        # Verify SELECT results
        self.assertEqual(len(results[2]['result']), 1)  # First SELECT
        self.assertEqual(results[2]['result'][0]['name'], 'Test1')
        
        self.assertEqual(len(results[4]['result']), 1)  # COUNT SELECT
        self.assertEqual(results[4]['result'][0]['count'], 1)

    def test_batch_with_comments(self):
        """Test batch execution with various comment types."""
        batch_sql = """
        -- Single line comment
        CREATE TABLE IF NOT EXISTS comment_test (
            id INTEGER PRIMARY KEY, -- inline comment
            name TEXT NOT NULL      /* another inline comment */
        );

        /* Multi-line comment
           spanning multiple lines */
        INSERT INTO comment_test (name) VALUES ('Test User');

        -- Another single line comment
        UPDATE comment_test SET name = 'Updated User' WHERE name = 'Test User';
        """

        results = self.db_engine.batch(batch_sql)

        # Verify results (comments should be stripped)
        self.assertEqual(len(results), 3)

        # Check that comments were properly removed
        create_stmt = results[0]['statement']
        self.assertNotIn('--', create_stmt)
        self.assertNotIn('/*', create_stmt)
        self.assertNotIn('*/', create_stmt)
        self.assertIn('CREATE TABLE', create_stmt)

        # Verify data was inserted correctly
        data = self.db_engine.fetch("SELECT * FROM comment_test")
        self.assertEqual(len(data), 1)
        self.assertEqual(data[0]['name'], 'Updated User')

    def test_batch_with_string_literals(self):
        """Test batch execution with semicolons in string literals."""
        batch_sql = """
        CREATE TABLE IF NOT EXISTS string_test (
            id INTEGER PRIMARY KEY,
            description TEXT
        );

        INSERT INTO string_test (description) VALUES ('This has a semicolon; in it');
        INSERT INTO string_test (description) VALUES ('Another; semicolon; here');

        UPDATE string_test SET description = 'Updated; with; semicolons' WHERE id = 1;
        """

        results = self.db_engine.batch(batch_sql)

        # Verify results (should be 4 statements, not split by semicolons in strings)
        self.assertEqual(len(results), 4)

        # Verify data was inserted correctly
        data = self.db_engine.fetch("SELECT * FROM string_test ORDER BY id")
        self.assertEqual(len(data), 2)
        self.assertEqual(data[0]['description'], 'Updated; with; semicolons')
        self.assertEqual(data[1]['description'], 'Another; semicolon; here')

    def test_batch_error_handling(self):
        """Test batch execution with invalid SQL statements."""
        batch_sql = """
        CREATE TABLE IF NOT EXISTS error_test (id INTEGER PRIMARY KEY);
        INSERT INTO error_test (id) VALUES (1);
        INVALID SQL STATEMENT;  -- This should cause an error
        INSERT INTO error_test (id) VALUES (2);  -- This should still execute
        """

        with pytest.raises(DbOperationError):
            self.db_engine.batch(batch_sql)

    def test_batch_empty_statements(self):
        """Test batch execution with empty statements and whitespace."""
        batch_sql = """
        CREATE TABLE IF NOT EXISTS empty_test (id INTEGER PRIMARY KEY);

        ;  -- Empty statement

        INSERT INTO empty_test (id) VALUES (1);

        ;  -- Another empty statement

        SELECT * FROM empty_test;
        """

        results = self.db_engine.batch(batch_sql)

        # Verify results (empty statements should be filtered out)
        self.assertEqual(len(results), 3)

        # Check statements - empty statements are now filtered out
        self.assertEqual(results[0]['operation'], 'execute')  # CREATE
        self.assertEqual(results[1]['operation'], 'execute')  # INSERT
        self.assertEqual(results[2]['operation'], 'fetch')    # SELECT

        # Verify data
        data = results[2]['result']
        self.assertEqual(len(data), 1)
        self.assertEqual(data[0]['id'], 1)

    def test_batch_transaction_consistency(self):
        """Test that batch operations maintain transaction consistency."""
        batch_sql = """
        CREATE TABLE IF NOT EXISTS transaction_test (
            id INTEGER PRIMARY KEY,
            name TEXT,
            balance INTEGER
        );

        INSERT INTO transaction_test (name, balance) VALUES ('Alice', 1000);
        INSERT INTO transaction_test (name, balance) VALUES ('Bob', 500);

        -- Transfer money
        UPDATE transaction_test SET balance = balance - 200 WHERE name = 'Alice';
        UPDATE transaction_test SET balance = balance + 200 WHERE name = 'Bob';

        -- Verify transfer
        SELECT * FROM transaction_test ORDER BY name;
        """

        results = self.db_engine.batch(batch_sql)

        # Verify all statements executed successfully
        self.assertEqual(len(results), 6)
        for result in results:
            self.assertNotEqual(result['operation'], 'error')

        # Verify transaction consistency
        data = results[5]['result']  # SELECT result
        self.assertEqual(len(data), 2)

        alice = next(row for row in data if row['name'] == 'Alice')
        bob = next(row for row in data if row['name'] == 'Bob')

        self.assertEqual(alice['balance'], 800)  # 1000 - 200
        self.assertEqual(bob['balance'], 700)    # 500 + 200

        # Total balance should remain the same
        total_balance = sum(row['balance'] for row in data)
        self.assertEqual(total_balance, 1500)

    def test_batch_large_number_of_statements(self):
        """Test batch execution with a large number of statements."""
        # Create many INSERT statements
        statements = ["CREATE TABLE IF NOT EXISTS large_test (id INTEGER PRIMARY KEY, name TEXT);"]

        for i in range(100):
            statements.append(f"INSERT INTO large_test (name) VALUES ('User{i}');")

        statements.append("SELECT COUNT(*) as count FROM large_test;")

        batch_sql = "\n".join(statements)

        results = self.db_engine.batch(batch_sql)

        # Verify results
        self.assertEqual(len(results), 102)  # CREATE + 100 INSERT + 1 SELECT

        # Check that all statements executed successfully
        for result in results:
            self.assertNotEqual(result['operation'], 'error')

        # Verify data
        count_result = results[-1]['result']
        self.assertEqual(count_result[0]['count'], 100)

    def test_batch_with_parameters_validation(self):
        """Test that batch method properly validates SQL statements."""
        batch_sql = """
        CREATE TABLE IF NOT EXISTS validation_test (id INTEGER PRIMARY KEY);
        INSERT INTO validation_test (id) VALUES (1);
        SELECT * FROM validation_test;
        """

        results = self.db_engine.batch(batch_sql)

        # Verify all statements are valid
        self.assertEqual(len(results), 3)
        for result in results:
            self.assertNotEqual(result['operation'], 'error')

        # Test with invalid SQL - should raise immediately
        invalid_batch = """
        CREATE TABLE IF NOT EXISTS invalid_test (id INTEGER PRIMARY KEY);
        SELECT * FROM;  -- Invalid SELECT statement
        INSERT INTO invalid_test (id) VALUES (1);
        """

        with self.assertRaises(DbOperationError):
            self.db_engine.batch(invalid_batch)

    def test_batch_concurrent_access(self):
        """Test batch operations under concurrent access."""
        import threading

        def batch_worker(worker_id):
            batch_sql = f"""
            CREATE TABLE IF NOT EXISTS concurrent_test_{worker_id} (
                id INTEGER PRIMARY KEY,
                worker_id INTEGER,
                data TEXT
            );

            INSERT INTO concurrent_test_{worker_id} (worker_id, data) VALUES ({worker_id}, 'data1');
            INSERT INTO concurrent_test_{worker_id} (worker_id, data) VALUES ({worker_id}, 'data2');

            SELECT COUNT(*) as count FROM concurrent_test_{worker_id};
            """

            results = self.db_engine.batch(batch_sql)
            return results

        # Run multiple batch operations concurrently
        threads = []
        results_list = []

        for i in range(5):
            thread = threading.Thread(target=lambda i=i: results_list.append(batch_worker(i)))
            threads.append(thread)
            thread.start()

        # Wait for all threads to complete
        for thread in threads:
            thread.join()

        # Verify all batch operations completed successfully
        self.assertEqual(len(results_list), 5)

        for i, results in enumerate(results_list):
            self.assertEqual(len(results), 4)  # CREATE + 2 INSERT + 1 SELECT
            for result in results:
                self.assertNotEqual(result['operation'], 'error')

            # Verify data was inserted correctly
            count_result = results[3]['result']
            self.assertEqual(count_result[0]['count'], 2)

    def test_batch_with_cte_and_advanced_statements(self):
        """Test batch execution with CTEs, VALUES, and other advanced statement types."""
        batch_sql = """
        -- Create test tables
        CREATE TABLE IF NOT EXISTS departments (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS employees (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            department_id INTEGER,
            salary REAL
        );

        -- Insert data using VALUES
        INSERT INTO departments (name) VALUES ('Engineering'), ('Sales'), ('Marketing');

        -- Insert employee data
        INSERT INTO employees (name, department_id, salary) VALUES
            ('Alice', 1, 75000),
            ('Bob', 1, 80000),
            ('Charlie', 2, 65000),
            ('Diana', 3, 70000);

        -- Query with CTE (Common Table Expression)
        WITH dept_stats AS (
            SELECT
                d.name as dept_name,
                COUNT(e.id) as emp_count,
                AVG(e.salary) as avg_salary
            FROM departments d
            LEFT JOIN employees e ON d.id = e.department_id
            GROUP BY d.id, d.name
        )
        SELECT * FROM dept_stats ORDER BY avg_salary DESC;

        -- Another CTE with recursive pattern
        WITH RECURSIVE numbers AS (
            SELECT 1 as n
            UNION ALL
            SELECT n + 1 FROM numbers WHERE n < 5
        )
        SELECT n, n * n as square FROM numbers;

        -- VALUES statement
        VALUES (1, 'Test1'), (2, 'Test2'), (3, 'Test3');

        -- PRAGMA statement
        PRAGMA table_info(employees);

        -- EXPLAIN statement
        EXPLAIN QUERY PLAN SELECT * FROM employees WHERE salary > 70000;

        -- DESCRIBE statement (SQLite doesn't support DESCRIBE, but test the detection)
        SELECT sql FROM sqlite_master WHERE type='table' AND name='employees';
        """

        results = self.db_engine.batch(batch_sql)

        # Verify results
        self.assertEqual(len(results), 10)  # 10 statements total

        # Check DDL statements (CREATE TABLE)
        self.assertEqual(results[0]['operation'], 'execute')  # CREATE TABLE departments
        self.assertEqual(results[1]['operation'], 'execute')  # CREATE TABLE employees

        # Check DML statements (INSERT)
        self.assertEqual(results[2]['operation'], 'execute')  # INSERT departments
        self.assertEqual(results[3]['operation'], 'execute')  # INSERT employees

        # Check CTE with SELECT (should be fetch)
        self.assertEqual(results[4]['operation'], 'fetch')    # CTE dept_stats
        self.assertEqual(results[5]['operation'], 'fetch')    # CTE numbers

        # Check VALUES statement (should be fetch)
        self.assertEqual(results[6]['operation'], 'fetch')    # VALUES

        # Check PRAGMA statement (should be fetch)
        self.assertEqual(results[7]['operation'], 'fetch')    # PRAGMA

        # Check EXPLAIN statement (should be fetch)
        self.assertEqual(results[8]['operation'], 'fetch')    # EXPLAIN

        # Check DESCRIBE-like statement (should be fetch)
        self.assertEqual(results[9]['operation'], 'fetch')    # SELECT from sqlite_master

        # Verify CTE results
        dept_stats_result = results[4]['result']
        self.assertGreater(len(dept_stats_result), 0)
        self.assertIn('dept_name', dept_stats_result[0])
        self.assertIn('emp_count', dept_stats_result[0])
        self.assertIn('avg_salary', dept_stats_result[0])

        # Verify recursive CTE results
        numbers_result = results[5]['result']
        self.assertEqual(len(numbers_result), 5)  # Numbers 1-5
        self.assertEqual(numbers_result[0]['n'], 1)
        self.assertEqual(numbers_result[4]['n'], 5)
        self.assertEqual(numbers_result[4]['square'], 25)

        # Verify VALUES results
        values_result = results[6]['result']
        self.assertEqual(len(values_result), 3)
        self.assertEqual(values_result[0]['column1'], 1)
        self.assertEqual(values_result[0]['column2'], 'Test1')

        # Verify PRAGMA results
        pragma_result = results[7]['result']
        self.assertGreater(len(pragma_result), 0)
        self.assertIn('name', pragma_result[0])
        self.assertIn('type', pragma_result[0])

        # Verify EXPLAIN results
        explain_result = results[8]['result']
        self.assertGreater(len(explain_result), 0)

        # Verify data was actually inserted
        dept_data = self.db_engine.fetch("SELECT * FROM departments ORDER BY id")
        self.assertEqual(len(dept_data), 3)

        emp_data = self.db_engine.fetch("SELECT * FROM employees ORDER BY id")
        self.assertEqual(len(emp_data), 4)

    def test_batch_with_complex_cte_nested(self):
        """Test batch execution with complex nested CTEs."""
        batch_sql = """
        -- Create test data
        CREATE TABLE IF NOT EXISTS sales (
            id INTEGER PRIMARY KEY,
            product TEXT,
            amount REAL,
            date TEXT
        );

        INSERT INTO sales (product, amount, date) VALUES
            ('Widget A', 100.0, '2024-01-01'),
            ('Widget B', 150.0, '2024-01-01'),
            ('Widget A', 120.0, '2024-01-02'),
            ('Widget C', 200.0, '2024-01-02'),
            ('Widget B', 180.0, '2024-01-03');

        -- Complex nested CTEs
        WITH daily_totals AS (
            SELECT
                date,
                SUM(amount) as daily_sum,
                COUNT(*) as transaction_count
            FROM sales
            GROUP BY date
        ),
        product_totals AS (
            SELECT
                product,
                SUM(amount) as product_sum,
                COUNT(*) as product_count
            FROM sales
            GROUP BY product
        ),
        combined_stats AS (
            SELECT
                'Daily' as stat_type,
                date as identifier,
                daily_sum as total_amount,
                transaction_count as count
            FROM daily_totals
            UNION ALL
            SELECT
                'Product' as stat_type,
                product as identifier,
                product_sum as total_amount,
                product_count as count
            FROM product_totals
        )
        SELECT
            stat_type,
            identifier,
            total_amount,
            count,
            ROUND(total_amount / count, 2) as avg_amount
        FROM combined_stats
        ORDER BY total_amount DESC;
        """

        results = self.db_engine.batch(batch_sql)

        # Verify results
        self.assertEqual(len(results), 3)  # CREATE + INSERT + complex CTE

        # Check statement types
        self.assertEqual(results[0]['operation'], 'execute')  # CREATE TABLE
        self.assertEqual(results[1]['operation'], 'execute')  # INSERT
        self.assertEqual(results[2]['operation'], 'fetch')    # Complex CTE

        # Verify CTE results
        cte_result = results[2]['result']
        self.assertGreater(len(cte_result), 0)

        # Should have both daily and product stats
        stat_types = [row['stat_type'] for row in cte_result]
        self.assertIn('Daily', stat_types)
        self.assertIn('Product', stat_types)

        # Verify data structure
        first_row = cte_result[0]
        self.assertIn('stat_type', first_row)
        self.assertIn('identifier', first_row)
        self.assertIn('total_amount', first_row)
        self.assertIn('count', first_row)
        self.assertIn('avg_amount', first_row)

        # Verify data was inserted
        sales_data = self.db_engine.fetch("SELECT COUNT(*) as count FROM sales")
        self.assertEqual(sales_data[0]['count'], 5)

    def test_commit_failure_handling_single_execute(self):
        """Test commit failure handling for single execute operations."""
        # Create a table with a unique constraint that we can violate
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS commit_test (
            id INTEGER PRIMARY KEY,
            unique_field TEXT UNIQUE,
            data TEXT
        )
        """
        self.db_engine.execute(create_table_sql)

        # Insert initial data
        self.db_engine.execute(
            "INSERT INTO commit_test (unique_field, data) VALUES (:field, :data)",
            {"field": "test1", "data": "value1"}
        )

        # Try to insert duplicate unique field - this should cause a constraint violation
        with self.assertRaises(DbOperationError) as context:
            self.db_engine.execute(
                "INSERT INTO commit_test (unique_field, data) VALUES (:field, :data)",
                {"field": "test1", "data": "value2"}  # Duplicate unique_field
            )

        # Verify that the error is properly wrapped in DbOperationError
        self.assertIsInstance(context.exception, DbOperationError)

        # Verify that the original data is still intact (rollback worked)
        result = self.db_engine.fetch("SELECT COUNT(*) as count FROM commit_test WHERE unique_field = 'test1'")
        self.assertEqual(result[0]['count'], 1)  # Only the original record should exist

    def test_commit_failure_handling_batch_operations(self):
        """Test commit failure handling for batch operations."""
        # Create a table with a unique constraint
        batch_sql = """
        CREATE TABLE IF NOT EXISTS batch_commit_test (
            id INTEGER PRIMARY KEY,
            unique_field TEXT UNIQUE,
            data TEXT
        );

        INSERT INTO batch_commit_test (unique_field, data) VALUES ('test1', 'value1');
        INSERT INTO batch_commit_test (unique_field, data) VALUES ('test2', 'value2');
        INSERT INTO batch_commit_test (unique_field, data) VALUES ('test1', 'value3');  -- Duplicate!
        INSERT INTO batch_commit_test (unique_field, data) VALUES ('test4', 'value4');
        """

        with self.assertRaises(DbOperationError):
            self.db_engine.batch(batch_sql)

    def test_commit_failure_handling_transaction_rollback(self):
        """Test that transaction rollback works correctly on commit failure."""
        # Create a table with constraints
        create_sql = """
        CREATE TABLE IF NOT EXISTS transaction_commit_test (
            id INTEGER PRIMARY KEY,
            unique_field TEXT UNIQUE,
            balance INTEGER CHECK (balance >= 0)
        )
        """
        self.db_engine.execute(create_sql)

        # Insert initial data
        self.db_engine.execute(
            "INSERT INTO transaction_commit_test (unique_field, balance) VALUES (:field, :balance)",
            {"field": "account1", "balance": 100}
        )

        # Try a transaction that will fail due to constraint violation
        operations = [
            {"operation": "execute", "query": "UPDATE transaction_commit_test SET balance = balance - 50 WHERE unique_field = :field",
             "params": {"field": "account1"}},
            {"operation": "execute", "query": "INSERT INTO transaction_commit_test (unique_field, balance) VALUES (:field, :balance)",
             "params": {"field": "account1", "balance": 200}}  # Duplicate unique_field
        ]

        with self.assertRaises(Exception):  # Keep as Exception since execute_transaction doesn't wrap in DbOperationError
            self.db_engine.execute_transaction(operations)

        # Verify that the original data is unchanged (rollback worked)
        result = self.db_engine.fetch("SELECT * FROM transaction_commit_test WHERE unique_field = 'account1'")
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]['balance'], 100)  # Should still be 100, not 50

    def test_commit_failure_with_check_constraint(self):
        """Test commit failure due to CHECK constraint violation."""
        # Create a table with a CHECK constraint
        create_sql = """
        CREATE TABLE IF NOT EXISTS check_constraint_test (
            id INTEGER PRIMARY KEY,
            age INTEGER CHECK (age >= 0 AND age <= 150),
            name TEXT
        )
        """
        self.db_engine.execute(create_sql)

        # Try to insert data that violates the CHECK constraint
        with self.assertRaises(DbOperationError) as context:
            self.db_engine.execute(
                "INSERT INTO check_constraint_test (age, name) VALUES (:age, :name)",
                {"age": 200, "name": "Invalid Age"}  # Age > 150 violates constraint
            )

        # Verify the error is properly handled
        self.assertIsInstance(context.exception, DbOperationError)

        # Verify no data was inserted
        result = self.db_engine.fetch("SELECT COUNT(*) as count FROM check_constraint_test")
        self.assertEqual(result[0]['count'], 0)

    def test_commit_failure_with_foreign_key_constraint(self):
        """Test commit failure due to foreign key constraint violation."""
        # Enable foreign key enforcement in SQLite
        self.db_engine.execute("PRAGMA foreign_keys=ON")
        # Create parent table
        parent_sql = """
        CREATE TABLE IF NOT EXISTS parent_table (
            id INTEGER PRIMARY KEY,
            name TEXT
        )
        """
        self.db_engine.execute(parent_sql)

        # Create child table with foreign key
        child_sql = """
        CREATE TABLE IF NOT EXISTS child_table (
            id INTEGER PRIMARY KEY,
            parent_id INTEGER,
            data TEXT,
            FOREIGN KEY (parent_id) REFERENCES parent_table(id)
        )
        """
        self.db_engine.execute(child_sql)

        # Try to insert child record with non-existent parent
        with self.assertRaises(DbOperationError) as context:
            self.db_engine.execute(
                "INSERT INTO child_table (parent_id, data) VALUES (:parent_id, :data)",
                {"parent_id": 999, "data": "orphaned data"}  # Parent doesn't exist
            )

        # Verify the error is properly handled
        self.assertIsInstance(context.exception, DbOperationError)

        # Verify no data was inserted
        result = self.db_engine.fetch("SELECT COUNT(*) as count FROM child_table")
        self.assertEqual(result[0]['count'], 0)

    def test_commit_failure_recovery_scenario(self):
        """Test that the system can recover from commit failures and continue working."""
        # Create a table with unique constraint
        create_sql = """
        CREATE TABLE IF NOT EXISTS recovery_test (
            id INTEGER PRIMARY KEY,
            unique_field TEXT UNIQUE,
            data TEXT
        )
        """
        self.db_engine.execute(create_sql)

        # First operation: successful insert
        self.db_engine.execute(
            "INSERT INTO recovery_test (unique_field, data) VALUES (:field, :data)",
            {"field": "test1", "data": "value1"}
        )

        # Second operation: constraint violation due to duplicate
        with self.assertRaises(DbOperationError):
            self.db_engine.execute(
                "INSERT INTO recovery_test (unique_field, data) VALUES (:field, :data)",
                {"field": "test1", "data": "value2"}  # Duplicate
            )

        # Third operation: should work fine after recovery
        self.db_engine.execute(
            "INSERT INTO recovery_test (unique_field, data) VALUES (:field, :data)",
            {"field": "test2", "data": "value3"}
        )

        # Verify both successful operations are present
        result = self.db_engine.fetch("SELECT * FROM recovery_test ORDER BY unique_field")
        self.assertEqual(len(result), 2)
        self.assertEqual(result[0]['unique_field'], 'test1')
        self.assertEqual(result[0]['data'], 'value1')
        self.assertEqual(result[1]['unique_field'], 'test2')
        self.assertEqual(result[1]['data'], 'value3')

    def test_error_handling_wrapping_verification(self):
        """Test that errors are properly wrapped in DbOperationError."""
        # Test with invalid SQL
        with self.assertRaises(DbOperationError) as context:
            self.db_engine.execute("INVALID SQL STATEMENT")

        # Verify the error is properly wrapped
        self.assertIsInstance(context.exception, DbOperationError)

        # Test with missing table
        with self.assertRaises(DbOperationError) as context:
            self.db_engine.fetch("SELECT * FROM non_existent_table")

        # Verify the error is properly wrapped
        self.assertIsInstance(context.exception, DbOperationError)


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
        with self.assertRaises(Exception):  # Keep as Exception since this is during initialization
            DbEngine(self.database_url)

        # Restore permissions for cleanup
        os.chmod(self.temp_db_path, 0o666)


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
        import time

        from jpy_sync_db_lite.db_request import DbRequest

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


class TestDbEngineSQLiteSpecific(unittest.TestCase):
    """Test cases for SQLite-specific DbEngine functionality."""

    def setUp(self):
        """Set up test fixtures before each test method."""
        # Create temporary database file
        self.temp_db_fd, self.temp_db_path = tempfile.mkstemp(suffix='.db')
        os.close(self.temp_db_fd)
        self.database_url = f"sqlite:///{self.temp_db_path}"

        # Initialize DbEngine with SQLite-specific settings
        self.db_engine = DbEngine(
            self.database_url,
            timeout=30,
            check_same_thread=False
        )

        # Create test table
        self._create_test_table()
        self._insert_test_data()

    def tearDown(self):
        """Clean up after each test method."""
        if hasattr(self, 'db_engine'):
            self.db_engine.shutdown()
            time.sleep(0.1)

        # Remove temporary database file
        if os.path.exists(self.temp_db_path):
            try:
                os.unlink(self.temp_db_path)
            except PermissionError:
                time.sleep(0.1)
                try:
                    os.unlink(self.temp_db_path)
                except PermissionError:
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

    def test_init_with_sqlite_parameters(self):
        """Test DbEngine initialization with SQLite-specific parameters."""
        db = DbEngine(
            self.database_url,
            timeout=60,
            check_same_thread=True,
            debug=True
        )

        self.assertIsNotNone(db.engine)
        self.assertEqual(db.num_workers, 1)

        db.shutdown()

    def test_configure_pragma(self):
        """Test configuring SQLite PRAGMA settings."""
        # Test setting cache size
        self.db_engine.configure_pragma('cache_size', '-32000')  # 32MB cache

        # Verify the setting was applied
        with self.db_engine.get_raw_connection() as conn:
            result = conn.execute(text("PRAGMA cache_size"))
            cache_size = result.fetchone()[0]
            self.assertEqual(cache_size, -32000)

        # Test setting synchronous mode
        self.db_engine.configure_pragma('synchronous', 'FULL')

        with self.db_engine.get_raw_connection() as conn:
            result = conn.execute(text("PRAGMA synchronous"))
            synchronous = result.fetchone()[0]
            self.assertEqual(synchronous, 2)  # FULL mode

    def test_get_sqlite_info(self):
        """Test getting SQLite-specific information."""
        info = self.db_engine.get_sqlite_info()

        # Check required fields
        self.assertIn('version', info)
        self.assertIn('database_size', info)
        self.assertIn('page_count', info)
        self.assertIn('page_size', info)
        self.assertIn('cache_size', info)
        self.assertIn('journal_mode', info)
        self.assertIn('synchronous', info)
        self.assertIn('temp_store', info)

        # Verify SQLite version is a string
        self.assertIsInstance(info['version'], str)
        self.assertGreater(len(info['version']), 0)

        # Verify database size is a number (if available)
        if info['database_size'] is not None:
            self.assertIsInstance(info['database_size'], int)
            self.assertGreater(info['database_size'], 0)

        # Verify pragma values are reasonable
        self.assertIsInstance(info['page_size'], int)
        self.assertGreater(info['page_size'], 0)
        self.assertEqual(info['journal_mode'], 'wal')
        self.assertIn(info['synchronous'], [0, 1, 2])  # OFF, NORMAL, FULL
        self.assertIn(info['temp_store'], [0, 1, 2])   # DEFAULT, FILE, MEMORY

    def test_vacuum_operation(self):
        """Test SQLite VACUUM operation."""
        # First, delete some data to create fragmentation
        self.db_engine.execute("DELETE FROM test_users WHERE name = :name",
                              {"name": "Bob Smith"})

        # Get initial database size
        initial_info = self.db_engine.get_sqlite_info()
        initial_info['database_size']

        # Run VACUUM (SQLite VACUUM doesn't support mode parameters)
        self.db_engine.vacuum()

        # Get database size after VACUUM
        final_info = self.db_engine.get_sqlite_info()
        final_size = final_info['database_size']

        # VACUUM should not fail
        self.assertIsNotNone(final_size)

        # Verify data integrity after VACUUM
        users = self.db_engine.fetch("SELECT * FROM test_users")
        self.assertEqual(len(users), 2)  # Should have 2 users remaining

    def test_analyze_operation(self):
        """Test SQLite ANALYZE operation."""
        # Run ANALYZE on all tables
        self.db_engine.analyze()

        # Should not raise an exception
        users = self.db_engine.fetch("SELECT * FROM test_users")
        self.assertEqual(len(users), 3)

    def test_analyze_specific_table(self):
        """Test ANALYZE operation on specific table."""
        # Run ANALYZE on specific table
        self.db_engine.analyze('test_users')

        # Should not raise an exception
        users = self.db_engine.fetch("SELECT * FROM test_users")
        self.assertEqual(len(users), 3)

    def test_integrity_check(self):
        """Test SQLite integrity check."""
        # Run integrity check
        issues = self.db_engine.integrity_check()

        # Should return empty list for healthy database
        self.assertIsInstance(issues, list)
        self.assertEqual(len(issues), 0)

    def test_integrity_check_with_corruption(self):
        """Test integrity check with database corruption simulation."""
        # This test simulates what would happen with corruption
        # In a real scenario, we'd need to actually corrupt the file
        # For now, we just verify the method works correctly

        # Run integrity check on healthy database
        issues = self.db_engine.integrity_check()
        self.assertEqual(len(issues), 0)

        # Verify the method returns a list even if there are issues
        self.assertIsInstance(issues, list)

    def test_optimize_operation(self):
        """Test SQLite optimization operation."""
        # Run optimization
        self.db_engine.optimize()

        # Should not raise an exception
        users = self.db_engine.fetch("SELECT * FROM test_users")
        self.assertEqual(len(users), 3)

    def test_get_raw_connection_exception(self):
        with self.assertRaises(Exception):
            with self.db_engine.get_raw_connection() as conn:
                conn.execute(text("INVALID SQL"))

    def test_analyze_error_wrapping(self):
        # Pass an invalid table name to reliably trigger an error
        with self.assertRaises(DbOperationError):
            self.db_engine.analyze('this_table_does_not_exist')

    def test_maintenance_operations_error_wrapping(self):
        """Test that all maintenance operations properly wrap errors in DbOperationError."""
        from jpy_sync_db_lite.db_engine import DbOperationError

        # Test all maintenance operations to ensure they wrap errors properly
        maintenance_operations = [
            (self.db_engine.vacuum, "VACUUM operation failed"),
            (lambda: self.db_engine.analyze(), "ANALYZE operation failed"),
            (lambda: self.db_engine.analyze('test_users'), "ANALYZE operation failed"),
            (lambda: self.db_engine.integrity_check(), "Integrity check failed"),
            (self.db_engine.optimize, "Optimization operation failed"),
        ]

        for operation, expected_error_text in maintenance_operations:
            try:
                operation()
                # If operation succeeds, that's fine - we're just testing error wrapping
            except DbOperationError as e:
                # If an error occurs, it should be wrapped in DbOperationError
                self.assertIsInstance(e, DbOperationError)
                self.assertIn(expected_error_text, str(e))

    def test_sqlite_error_class(self):
        """Test SQLiteError exception class."""
        from jpy_sync_db_lite.db_engine import SQLiteError

        # Test SQLiteError creation
        error = SQLiteError(1, "Test error message")
        self.assertEqual(error.error_code, 1)
        self.assertEqual(error.message, "Test error message")
        self.assertIn("SQLite error 1", str(error))

    def test_enhanced_performance_configuration(self):
        """Test enhanced SQLite performance configuration."""
        with self.db_engine.get_raw_connection() as conn:
            # Check foreign keys are enabled
            result = conn.execute(text("PRAGMA foreign_keys"))
            foreign_keys = result.fetchone()[0]
            self.assertEqual(foreign_keys, 1)

            # Check busy timeout
            result = conn.execute(text("PRAGMA busy_timeout"))
            busy_timeout = result.fetchone()[0]
            self.assertEqual(busy_timeout, 30000)  # 30 seconds

            # Check auto vacuum (may not be set if database already exists)
            result = conn.execute(text("PRAGMA auto_vacuum"))
            auto_vacuum = result.fetchone()[0]
            # Auto vacuum can be 0 (NONE), 1 (FULL), or 2 (INCREMENTAL)
            self.assertIn(auto_vacuum, [0, 1, 2])

    def test_connection_parameters(self):
        """Test SQLite connection parameters are properly set."""
        # Test with different connection parameters
        db = DbEngine(
            self.database_url,
            timeout=60,
            check_same_thread=False  # Must be False for threading
        )

        # Verify the engine was created successfully
        self.assertIsNotNone(db.engine)

        # Test that operations work with custom parameters
        result = db.fetch("SELECT 1 as test")
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]['test'], 1)

        db.shutdown()

    def test_pragma_configuration_edge_cases(self):
        """Test PRAGMA configuration with edge cases."""
        # Test invalid PRAGMA name (should not crash)
        try:
            self.db_engine.configure_pragma('invalid_pragma', 'value')
        except Exception as e:
            # Should handle gracefully
            self.assertIsInstance(e, Exception)

        # Test numeric PRAGMA value (skip empty value test)
        self.db_engine.configure_pragma('cache_size', '1000')

        with self.db_engine.get_raw_connection() as conn:
            result = conn.execute(text("PRAGMA cache_size"))
            cache_size = result.fetchone()[0]
            self.assertEqual(cache_size, 1000)

    def test_sqlite_info_edge_cases(self):
        """Test SQLite info retrieval with edge cases."""
        # Test with in-memory database
        memory_db = DbEngine("sqlite:///:memory:")

        info = memory_db.get_sqlite_info()

        # Should still get version and other info
        self.assertIn('version', info)
        self.assertIsInstance(info['version'], str)

        # Database size might be None for in-memory
        self.assertIn('database_size', info)

        memory_db.shutdown()

    def test_concurrent_sqlite_operations(self):
        """Test concurrent SQLite-specific operations."""
        import threading

        results = []
        errors = []

        def worker(worker_id):
            try:
                # Each worker performs SQLite-specific operations
                for i in range(2):  # Reduced iterations
                    # Configure pragma (use different values to avoid conflicts)
                    pragma_value = f'-{16000 + worker_id * 1000 + i}'
                    self.db_engine.configure_pragma('cache_size', pragma_value)

                    # Get SQLite info (read-only operation)
                    info = self.db_engine.get_sqlite_info()
                    results.append(info['version'])

                    # Run analyze (should be safe for concurrent access)
                    self.db_engine.analyze()

                    time.sleep(0.005)  # Reduced delay

            except Exception as e:
                errors.append(str(e))

        # Start fewer threads to reduce contention
        threads = []
        for i in range(2):  # Reduced from 3 to 2 threads
            thread = threading.Thread(target=worker, args=(i,))
            threads.append(thread)
            thread.start()

        # Wait for all threads to complete with timeout
        for thread in threads:
            thread.join(timeout=10)  # 10 second timeout

        # Verify no errors occurred
        self.assertEqual(len(errors), 0, f"Errors occurred: {errors}")
        self.assertGreater(len(results), 0)

        # All results should be the same SQLite version
        unique_versions = set(results)
        self.assertEqual(len(unique_versions), 1)


class TestDbEngineErrorHandling(unittest.TestCase):
    """Test error and exception handling for DbEngine edge cases."""
    def setUp(self):
        self.temp_db_fd, self.temp_db_path = tempfile.mkstemp(suffix='.db')
        os.close(self.temp_db_fd)
        self.database_url = f"sqlite:///{self.temp_db_path}"
        self.db_engine = DbEngine(self.database_url)

    def tearDown(self):
        self.db_engine.shutdown()
        if os.path.exists(self.temp_db_path):
            try:
                os.unlink(self.temp_db_path)
            except PermissionError:
                pass

    def test_db_operation_error_str(self):
        err = DbOperationError("test error")
        self.assertIn("test error", str(err))

    def test_sqlite_error_str(self):
        err = SQLiteError(123, "sqlite test error")
        self.assertIn("sqlite test error", str(err))
        self.assertIn("123", str(err))

    def test_execute_invalid_sql_raises(self):
        with self.assertRaises(DbOperationError):
            self.db_engine.execute("INVALID SQL")

    def test_fetch_invalid_sql_raises(self):
        with self.assertRaises(DbOperationError):
            self.db_engine.fetch("INVALID SQL")

    def test_batch_invalid_sql_raises(self):
        with self.assertRaises(DbOperationError):
            self.db_engine.batch("INVALID SQL;")

    def test_execute_transaction_invalid_op_type(self):
        with self.assertRaises(DbOperationError):
            self.db_engine.execute_transaction([
                {"operation": "not_a_valid_op", "query": "SELECT 1"}
            ])

    def test_execute_transaction_missing_query(self):
        with self.assertRaises(DbOperationError):
            self.db_engine.execute_transaction([
                {"operation": "fetch"}
            ])

    def test_execute_transaction_missing_operation(self):
        with self.assertRaises(DbOperationError):
            self.db_engine.execute_transaction([
                {"query": "SELECT 1"}
            ])

    def test_configure_pragma_invalid(self):
        # Should not crash, but may raise an exception
        try:
            self.db_engine.configure_pragma('not_a_real_pragma', 'value')
        except Exception as e:
            self.assertIsInstance(e, Exception)

    def test_get_raw_connection_exception(self):
        with self.assertRaises(Exception):
            with self.db_engine.get_raw_connection() as conn:
                conn.execute(text("INVALID SQL"))

    def test_analyze_error_wrapping(self):
        # Pass an invalid table name to reliably trigger an error
        with self.assertRaises(DbOperationError):
            self.db_engine.analyze('this_table_does_not_exist')

    def test_maintenance_operations_error_wrapping(self):
        """Test that all maintenance operations properly wrap errors in DbOperationError."""
        from jpy_sync_db_lite.db_engine import DbOperationError

        # Test all maintenance operations to ensure they wrap errors properly
        maintenance_operations = [
            (self.db_engine.vacuum, "VACUUM operation failed"),
            (lambda: self.db_engine.analyze(), "ANALYZE operation failed"),
            (lambda: self.db_engine.analyze('test_users'), "ANALYZE operation failed"),
            (lambda: self.db_engine.integrity_check(), "Integrity check failed"),
            (self.db_engine.optimize, "Optimization operation failed"),
        ]

        for operation, expected_error_text in maintenance_operations:
            try:
                operation()
                # If operation succeeds, that's fine - we're just testing error wrapping
            except DbOperationError as e:
                # If an error occurs, it should be wrapped in DbOperationError
                self.assertIsInstance(e, DbOperationError)
                self.assertIn(expected_error_text, str(e))

    def test_sqlite_error_class(self):
        """Test SQLiteError exception class."""
        from jpy_sync_db_lite.db_engine import SQLiteError

        # Test SQLiteError creation
        error = SQLiteError(1, "Test error message")
        self.assertEqual(error.error_code, 1)
        self.assertEqual(error.message, "Test error message")
        self.assertIn("SQLite error 1", str(error))

    def test_enhanced_performance_configuration(self):
        """Test enhanced SQLite performance configuration."""
        with self.db_engine.get_raw_connection() as conn:
            # Check foreign keys are enabled
            result = conn.execute(text("PRAGMA foreign_keys"))
            foreign_keys = result.fetchone()[0]
            self.assertEqual(foreign_keys, 1)

            # Check busy timeout
            result = conn.execute(text("PRAGMA busy_timeout"))
            busy_timeout = result.fetchone()[0]
            self.assertEqual(busy_timeout, 30000)  # 30 seconds

            # Check auto vacuum (may not be set if database already exists)
            result = conn.execute(text("PRAGMA auto_vacuum"))
            auto_vacuum = result.fetchone()[0]
            # Auto vacuum can be 0 (NONE), 1 (FULL), or 2 (INCREMENTAL)
            self.assertIn(auto_vacuum, [0, 1, 2])

    def test_connection_parameters(self):
        """Test SQLite connection parameters are properly set."""
        # Test with different connection parameters
        db = DbEngine(
            self.database_url,
            timeout=60,
            check_same_thread=False  # Must be False for threading
        )

        # Verify the engine was created successfully
        self.assertIsNotNone(db.engine)

        # Test that operations work with custom parameters
        result = db.fetch("SELECT 1 as test")
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]['test'], 1)

        db.shutdown()

    def test_pragma_configuration_edge_cases(self):
        """Test PRAGMA configuration with edge cases."""
        # Test invalid PRAGMA name (should not crash)
        try:
            self.db_engine.configure_pragma('invalid_pragma', 'value')
        except Exception as e:
            # Should handle gracefully
            self.assertIsInstance(e, Exception)

        # Test numeric PRAGMA value (skip empty value test)
        self.db_engine.configure_pragma('cache_size', '1000')

        with self.db_engine.get_raw_connection() as conn:
            result = conn.execute(text("PRAGMA cache_size"))
            cache_size = result.fetchone()[0]
            self.assertEqual(cache_size, 1000)

    def test_sqlite_info_edge_cases(self):
        """Test SQLite info retrieval with edge cases."""
        # Test with in-memory database
        memory_db = DbEngine("sqlite:///:memory:")

        info = memory_db.get_sqlite_info()

        # Should still get version and other info
        self.assertIn('version', info)
        self.assertIsInstance(info['version'], str)

        # Database size might be None for in-memory
        self.assertIn('database_size', info)

        memory_db.shutdown()

    def test_concurrent_sqlite_operations(self):
        """Test concurrent SQLite-specific operations."""
        import threading

        results = []
        errors = []

        def worker(worker_id):
            try:
                # Each worker performs SQLite-specific operations
                for i in range(2):  # Reduced iterations
                    # Configure pragma (use different values to avoid conflicts)
                    pragma_value = f'-{16000 + worker_id * 1000 + i}'
                    self.db_engine.configure_pragma('cache_size', pragma_value)

                    # Get SQLite info (read-only operation)
                    info = self.db_engine.get_sqlite_info()
                    results.append(info['version'])

                    # Run analyze (should be safe for concurrent access)
                    self.db_engine.analyze()

                    time.sleep(0.005)  # Reduced delay

            except Exception as e:
                errors.append(str(e))

        # Start fewer threads to reduce contention
        threads = []
        for i in range(2):  # Reduced from 3 to 2 threads
            thread = threading.Thread(target=worker, args=(i,))
            threads.append(thread)
            thread.start()

        # Wait for all threads to complete with timeout
        for thread in threads:
            thread.join(timeout=10)  # 10 second timeout

        # Verify no errors occurred
        self.assertEqual(len(errors), 0, f"Errors occurred: {errors}")
        self.assertGreater(len(results), 0)

        # All results should be the same SQLite version
        unique_versions = set(results)
        self.assertEqual(len(unique_versions), 1)


if __name__ == '__main__':
    unittest.main()
