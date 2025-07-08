"""
Batch operation tests for DbEngine class.

Copyright (c) 2025, Jim Schilling

Please keep this header when you use this code.

This module is licensed under the MIT License.
"""

import os
import sys
import unittest
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from jpy_sync_db_lite.db_engine import DbEngine, DbOperationError


class TestDbEngineBatch(unittest.TestCase):
    """Test cases for DbEngine batch operations."""

    def setUp(self) -> None:
        """Set up test fixtures before each test method."""
        self.database_url = "sqlite:///:memory:"
        self.db_engine = DbEngine(
            self.database_url,
            timeout=30,
            check_same_thread=False
        )

    def tearDown(self) -> None:
        """Clean up after each test method."""
        if hasattr(self, 'db_engine'):
            self.db_engine.shutdown()

    def _cleanup_all_tables(self) -> None:
        """Clean up all tables to ensure test isolation."""
        try:
            with self.db_engine.get_raw_connection() as conn:
                # Get all table names
                result = conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
                tables = [row[0] for row in result.fetchall()]
                
                # Drop all tables (except sqlite_master)
                for table in tables:
                    if table != 'sqlite_master':
                        conn.execute(f"DROP TABLE IF EXISTS {table}")
                conn.commit()
        except Exception:
            # If cleanup fails, continue - the test will handle it
            pass

    @staticmethod
    def normalize_sql(sql: str) -> str:
        """Normalize SQL by collapsing all whitespace, removing spaces after '(' and before ')', and stripping ends."""
        import re
        sql = re.sub(r'\s+', ' ', sql).strip()
        sql = re.sub(r'\(\s+', '(', sql)
        sql = re.sub(r'\s+\)', ')', sql)
        return sql

    @pytest.mark.integration
    def test_batch_simple_ddl_dml(self) -> None:
        """Test simple batch execution with DDL and DML statements."""
        # Ensure clean slate for this test
        self._cleanup_all_tables()
        
        batch_sql = """
        CREATE TABLE IF NOT EXISTS batch_test (
            id INTEGER PRIMARY KEY,
            name TEXT,
            value INTEGER
        );

        INSERT INTO batch_test (name, value) VALUES ('test1', 100);
        INSERT INTO batch_test (name, value) VALUES ('test2', 200);
        UPDATE batch_test SET value = 150 WHERE name = 'test1';
        DELETE FROM batch_test WHERE name = 'test2';
        """

        results = self.db_engine.batch(batch_sql)

        # Verify results
        self.assertEqual(len(results), 5)

        # Check CREATE TABLE
        self.assertEqual(results[0]['operation'], 'execute')
        self.assertEqual(results[1]['operation'], 'execute')

        # Check INSERT statements
        self.assertEqual(results[2]['operation'], 'execute')
        self.assertEqual(results[3]['operation'], 'execute')

        # Check UPDATE and DELETE statements by content, not index
        self.assertTrue(any(
            r['statement'].strip() == "UPDATE batch_test SET value = 150 WHERE name = 'test1';" and r['operation'] == 'execute'
            for r in results
        ))
        self.assertTrue(any(
            r['statement'].strip() == "DELETE FROM batch_test WHERE name = 'test2';" and r['operation'] == 'execute'
            for r in results
        ))

        # Verify data was actually inserted and updated
        data = self.db_engine.fetch("SELECT * FROM batch_test ORDER BY id")
        self.assertEqual(len(data.data), 1)
        self.assertEqual(data.data[0]['name'], 'test1')
        self.assertEqual(data.data[0]['value'], 150)

        self.assertEqual(
            self.normalize_sql(results[0]['statement']),
            self.normalize_sql('CREATE TABLE IF NOT EXISTS batch_test (id INTEGER PRIMARY KEY, name TEXT, value INTEGER);')
        )

    @pytest.mark.integration
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
        alice_result = results[3]['result'].data
        self.assertEqual(len(alice_result), 1)
        self.assertEqual(alice_result[0]['name'], 'Alice')

        count_result = results[5]['result'].data
        self.assertEqual(len(count_result), 1)
        self.assertEqual(count_result[0]['count'], 3)

    @pytest.mark.unit
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
        self.assertEqual(len(data.data), 2)
        self.assertEqual(data.data[0]['name'], 'Updated')
        self.assertEqual(data.data[1]['name'], 'Test2')

    @pytest.mark.integration
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
        self.assertEqual(len(results[2]['result'].data), 1)  # First SELECT
        self.assertEqual(results[2]['result'].data[0]['name'], 'Test1')
        
        self.assertEqual(len(results[4]['result'].data), 1)  # COUNT SELECT
        self.assertEqual(results[4]['result'].data[0]['count'], 1)

    @pytest.mark.unit
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
        self.assertEqual(len(data.data), 1)
        self.assertEqual(data.data[0]['name'], 'Updated User')

    @pytest.mark.unit
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
        self.assertEqual(len(data.data), 2)
        self.assertEqual(data.data[0]['description'], 'Updated; with; semicolons')
        self.assertEqual(data.data[1]['description'], 'Another; semicolon; here')

    @pytest.mark.unit
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

    @pytest.mark.unit
    def test_batch_empty_statements(self):
        """Test batch execution with empty statements and comments only."""
        batch_sql = """
        -- This is a comment
        ;
        ;
        SELECT 1 as test_value;
        ;
        -- Another comment
        ;
        """
        results = self.db_engine.batch(batch_sql)

        # There should be only one actual result (the SELECT statement)
        self.assertEqual(len(results), 1)
        data = results[0]['result'].data
        self.assertEqual(len(data), 1)
        self.assertEqual(data[0]['test_value'], 1)

    @pytest.mark.integration
    def test_batch_transaction_consistency(self):
        """Test that batch operations maintain transaction consistency."""
        # Ensure clean slate for this test
        self._cleanup_all_tables()
        
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
        data = results[5]['result'].data  # SELECT result
        self.assertEqual(len(data), 2)

        alice = next(row for row in data if row['name'] == 'Alice')
        bob = next(row for row in data if row['name'] == 'Bob')

        self.assertEqual(alice['balance'], 800)  # 1000 - 200
        self.assertEqual(bob['balance'], 700)    # 500 + 200

        # Total balance should remain the same
        total_balance = sum(row['balance'] for row in data)
        self.assertEqual(total_balance, 1500)

    @pytest.mark.slow
    def test_batch_large_number_of_statements(self):
        """Test batch execution with a large number of statements."""
        batch_sql = """
        CREATE TABLE IF NOT EXISTS large_batch_test (id INTEGER PRIMARY KEY);
        """
        # Add 100 INSERT statements
        for i in range(1, 101):
            batch_sql += f"INSERT INTO large_batch_test (id) VALUES ({i});\n"
        batch_sql += "SELECT COUNT(*) as count FROM large_batch_test;"

        results = self.db_engine.batch(batch_sql)

        # There should be 1 CREATE + 100 INSERT + 1 SELECT = 102 results
        self.assertEqual(len(results), 102)
        self.assertEqual(results[0]['operation'], 'execute')  # CREATE
        for i in range(1, 101):
            self.assertEqual(results[i]['operation'], 'execute')  # INSERT
        self.assertEqual(results[101]['operation'], 'fetch')  # SELECT

        # Check the SELECT result
        count_result = results[101]['result'].data
        self.assertEqual(count_result[0]['count'], 100)

    @pytest.mark.unit
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

    @pytest.mark.integration
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
            count_result = results[3]['result'].data
            self.assertEqual(count_result[0]['count'], 2)

    @pytest.mark.integration
    def test_batch_with_cte_and_advanced_statements(self):
        """Test batch execution with CTEs, VALUES, and other advanced statement types."""
        # Ensure clean slate for this test
        self._cleanup_all_tables()
        
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

        -- Insert data using VALUES with explicit IDs
        INSERT INTO departments (id, name) VALUES (1, 'Engineering'), (2, 'Sales'), (3, 'Marketing');

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

        # Verify we have the expected number of statements
        self.assertEqual(len(results), 10)  # 10 statements total

        # Verify all statements executed successfully
        for result in results:
            self.assertNotEqual(result['operation'], 'error')

        # Find and verify CREATE TABLE statements
        create_statements = [r for r in results if 'CREATE TABLE' in r['statement']]
        self.assertEqual(len(create_statements), 2)
        for create_stmt in create_statements:
            self.assertEqual(create_stmt['operation'], 'execute')

        # Find and verify INSERT statements
        insert_statements = [r for r in results if 'INSERT INTO' in r['statement']]
        self.assertEqual(len(insert_statements), 2)
        for insert_stmt in insert_statements:
            self.assertEqual(insert_stmt['operation'], 'execute')

        # Find and verify fetch statements
        fetch_statements = [r for r in results if r['operation'] == 'fetch']
        self.assertEqual(len(fetch_statements), 6)  # CTE dept_stats, CTE numbers, VALUES, PRAGMA, EXPLAIN, DESCRIBE-like

        # Find and verify CTE dept_stats results
        dept_stats_result = next(r for r in fetch_statements if 'dept_stats' in r['statement'])
        dept_stats_data = dept_stats_result['result'].data
        self.assertGreater(len(dept_stats_data), 0)
        self.assertIn('dept_name', dept_stats_data[0])
        self.assertIn('emp_count', dept_stats_data[0])
        self.assertIn('avg_salary', dept_stats_data[0])

        # Find and verify recursive CTE numbers results
        numbers_result = next(r for r in fetch_statements if 'RECURSIVE numbers' in r['statement'])
        numbers_data = numbers_result['result'].data
        self.assertEqual(len(numbers_data), 5)  # Numbers 1-5
        self.assertEqual(numbers_data[0]['n'], 1)
        self.assertEqual(numbers_data[4]['n'], 5)
        self.assertEqual(numbers_data[4]['square'], 25)

        # Find and verify VALUES results
        values_result = next(r for r in fetch_statements if 'VALUES (' in r['statement'])
        values_data = values_result['result'].data
        self.assertEqual(len(values_data), 3)
        self.assertEqual(values_data[0]['column1'], 1)
        self.assertEqual(values_data[0]['column2'], 'Test1')

        # Find and verify PRAGMA results
        pragma_result = next(r for r in fetch_statements if 'PRAGMA table_info' in r['statement'])
        pragma_data = pragma_result['result'].data
        self.assertGreater(len(pragma_data), 0)
        self.assertIn('name', pragma_data[0])
        self.assertIn('type', pragma_data[0])

        # Find and verify EXPLAIN results
        explain_result = next(r for r in fetch_statements if 'EXPLAIN QUERY PLAN' in r['statement'])
        explain_data = explain_result['result'].data
        self.assertGreater(len(explain_data), 0)

        # Verify data was actually inserted
        dept_data = self.db_engine.fetch("SELECT * FROM departments ORDER BY id")
        self.assertEqual(len(dept_data.data), 3)

        emp_data = self.db_engine.fetch("SELECT * FROM employees ORDER BY id")
        self.assertEqual(len(emp_data.data), 4)

    @pytest.mark.integration
    def test_batch_with_complex_cte_nested(self):
        """Test batch execution with complex CTEs and nested queries."""
        # Ensure clean slate for this test
        self._cleanup_all_tables()
        
        batch_sql = """
        -- Create sales table
        CREATE TABLE IF NOT EXISTS sales (
            id INTEGER PRIMARY KEY,
            product TEXT,
            amount INTEGER
        );

        -- Insert data
        INSERT INTO sales (product, amount) VALUES ('A', 100);
        INSERT INTO sales (product, amount) VALUES ('B', 200);
        INSERT INTO sales (product, amount) VALUES ('A', 150);
        INSERT INTO sales (product, amount) VALUES ('C', 50);
        INSERT INTO sales (product, amount) VALUES ('B', 75);

        -- Complex CTE
        WITH product_totals AS (
            SELECT product, SUM(amount) as total
            FROM sales
            GROUP BY product
        )
        SELECT * FROM product_totals ORDER BY product;

        -- Nested query
        SELECT COUNT(*) as count FROM sales;
        """
        results = self.db_engine.batch(batch_sql)

        # Check that the last SELECT returns the correct count
        sales_data = results[7]['result'].data
        self.assertEqual(sales_data[0]['count'], 5)


if __name__ == '__main__':
    unittest.main() 