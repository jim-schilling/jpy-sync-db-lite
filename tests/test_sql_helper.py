"""
Tests for sql_helper module.

Copyright (c) 2025, Jim Schilling

Please keep this header when you use this code.

This module is licensed under the MIT License.
"""
import unittest
import tempfile
import os
from jpy_sync_db_lite.sql_helper import (
    remove_sql_comments, 
    parse_sql_statements, 
    split_sql_file,
    detect_statement_type
)


class TestSqlHelper(unittest.TestCase):
    
    def test_remove_sql_comments_single_line(self):
        """Test removing single-line comments."""
        sql = """
        SELECT * FROM users; -- This is a comment
        INSERT INTO users VALUES (1, 'John'); -- Another comment
        """
        clean_sql = remove_sql_comments(sql)
        self.assertNotIn('--', clean_sql)
        self.assertIn('SELECT * FROM users;', clean_sql)
        self.assertIn('INSERT INTO users VALUES (1, \'John\');', clean_sql)
    
    def test_remove_sql_comments_multi_line(self):
        """Test removing multi-line comments."""
        sql = """
        CREATE TABLE users (
            id INTEGER PRIMARY KEY, /* This is a multi-line
            comment that spans multiple lines */
            name TEXT NOT NULL
        );
        """
        clean_sql = remove_sql_comments(sql)
        self.assertNotIn('/*', clean_sql)
        self.assertNotIn('*/', clean_sql)
        self.assertIn('CREATE TABLE users', clean_sql)
    
    def test_remove_sql_comments_preserve_strings(self):
        """Test that comments within string literals are preserved."""
        sql = """
        INSERT INTO users VALUES (1, 'John -- This is not a comment');
        UPDATE users SET name = 'Jane /* This is not a comment */';
        """
        clean_sql = remove_sql_comments(sql)
        self.assertIn("'John -- This is not a comment'", clean_sql)
        self.assertIn("'Jane /* This is not a comment */'", clean_sql)
    
    def test_parse_sql_statements_simple(self):
        """Test parsing simple SQL statements."""
        sql = """
        CREATE TABLE users (id INTEGER PRIMARY KEY);
        INSERT INTO users VALUES (1, 'John');
        SELECT * FROM users;
        """
        statements = parse_sql_statements(sql)
        self.assertEqual(len(statements), 3)
        self.assertIn('CREATE TABLE users (id INTEGER PRIMARY KEY);', statements)
        self.assertIn('INSERT INTO users VALUES (1, \'John\');', statements)
        self.assertIn('SELECT * FROM users;', statements)
    
    def test_parse_sql_statements_with_comments(self):
        """Test parsing SQL statements with comments."""
        sql = """
        CREATE TABLE users (id INTEGER PRIMARY KEY); -- Create table
        INSERT INTO users VALUES (1, 'John'); /* Insert user */
        SELECT * FROM users; -- Get all users
        """
        statements = parse_sql_statements(sql)
        self.assertEqual(len(statements), 3)
        # Comments should be removed
        for stmt in statements:
            self.assertNotIn('--', stmt)
            self.assertNotIn('/*', stmt)
            self.assertNotIn('*/', stmt)
            self.assertTrue(stmt.endswith(';'))
    
    def test_parse_sql_statements_preserve_semicolons_in_strings(self):
        """Test that semicolons in string literals are preserved."""
        sql = """
        INSERT INTO users VALUES (1, 'John; Doe');
        UPDATE users SET name = 'Jane; Smith' WHERE id = 1;
        """
        statements = parse_sql_statements(sql)
        self.assertEqual(len(statements), 2)
        self.assertIn("'John; Doe'", statements[0])
        self.assertIn("'Jane; Smith'", statements[1])
        self.assertTrue(statements[0].endswith(';'))
        self.assertTrue(statements[1].endswith(';'))
    
    def test_parse_sql_statements_empty(self):
        """Test parsing empty SQL."""
        statements = parse_sql_statements("")
        self.assertEqual(statements, [])
        
        statements = parse_sql_statements("   ")
        self.assertEqual(statements, [])
        
        statements = parse_sql_statements("-- Only comments\n/* More comments */")
        self.assertEqual(statements, [])
    
    def test_split_sql_file(self):
        """Test reading and parsing a SQL file."""
        # Create a temporary SQL file
        sql_content = """
        CREATE TABLE users (id INTEGER PRIMARY KEY);
        INSERT INTO users VALUES (1, 'John');
        SELECT * FROM users;
        """
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.sql', delete=False) as f:
            f.write(sql_content)
            temp_file = f.name
        
        try:
            statements = split_sql_file(temp_file)
            self.assertEqual(len(statements), 3)
            self.assertIn('CREATE TABLE users (id INTEGER PRIMARY KEY);', statements)
        finally:
            os.unlink(temp_file)
    
    def test_split_sql_file_not_found(self):
        """Test handling of non-existent file."""
        with self.assertRaises(FileNotFoundError):
            split_sql_file("non_existent_file.sql")
    
    def test_complex_sql_parsing(self):
        """Test parsing complex SQL with mixed content."""
        complex_sql = """
        -- Create users table
        CREATE TABLE users (
            id INTEGER PRIMARY KEY, -- user id
            name TEXT NOT NULL,     /* user name */
            email TEXT,             -- user email
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        
        /* Insert some test data */
        INSERT INTO users (name, email) VALUES ('John Doe', 'john@example.com');
        INSERT INTO users (name, email) VALUES ('Jane Smith', 'jane@example.com');
        
        -- Create index
        CREATE INDEX idx_users_email ON users(email);
        
        -- Query to verify
        SELECT * FROM users WHERE email LIKE '%@example.com';
        """
        
        statements = parse_sql_statements(complex_sql)
        self.assertEqual(len(statements), 5)
        
        # Verify all comments are removed
        for stmt in statements:
            self.assertNotIn('--', stmt)
            self.assertNotIn('/*', stmt)
            self.assertNotIn('*/', stmt)
        
        # Verify specific statements are present
        self.assertTrue(any('CREATE TABLE users' in stmt for stmt in statements))
        self.assertTrue(any('INSERT INTO users' in stmt for stmt in statements))
        self.assertTrue(any('CREATE INDEX' in stmt for stmt in statements))
        self.assertTrue(any('SELECT * FROM users' in stmt for stmt in statements))

    def test_parse_sql_statements_with_begin_end_blocks(self):
        """Test parsing SQL statements with BEGIN...END blocks (triggers)."""
        sql_with_triggers = """
        CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);
        CREATE TRIGGER update_timestamp 
        AFTER INSERT ON users 
        BEGIN 
            UPDATE users SET name = 'Updated' WHERE id = NEW.id; 
            INSERT INTO audit_log (action) VALUES ('user_inserted'); 
        END;
        INSERT INTO users VALUES (1, 'John');
        """
        
        statements = parse_sql_statements(sql_with_triggers)
        self.assertEqual(len(statements), 3)
        
        # Verify the trigger is parsed as a single statement
        trigger_statement = [stmt for stmt in statements if 'CREATE TRIGGER' in stmt][0]
        self.assertIn('BEGIN', trigger_statement)
        self.assertIn('END', trigger_statement)
        self.assertIn('UPDATE users SET name', trigger_statement)
        self.assertIn('INSERT INTO audit_log', trigger_statement)
        
        # Verify other statements are parsed correctly
        self.assertTrue(any('CREATE TABLE users' in stmt for stmt in statements))
        self.assertTrue(any('INSERT INTO users VALUES' in stmt for stmt in statements))

    def test_parse_sql_statements_strip_semicolons(self):
        """Test that trailing semicolons are stripped from parsed statements by default."""
        sql = """
        CREATE TABLE users (id INTEGER PRIMARY KEY);
        INSERT INTO users VALUES (1, 'John');
        SELECT * FROM users;
        """
        statements = parse_sql_statements(sql, strip_semicolon=True)
        for stmt in statements:
            self.assertFalse(stmt.endswith(';'), f"Statement should not end with semicolon: {stmt}")
        
        # Test strip_semicolon=False behavior
        statements_with_semicolons = parse_sql_statements(sql, strip_semicolon=False)
        self.assertEqual(len(statements_with_semicolons), 3)
        
        # Verify semicolons are preserved
        for stmt in statements_with_semicolons:
            self.assertTrue(stmt.endswith(';'), f"Statement should end with semicolon: {stmt}")
        
        # Verify the statements are correct
        self.assertEqual(statements_with_semicolons[0], "CREATE TABLE users (id INTEGER PRIMARY KEY);")
        self.assertEqual(statements_with_semicolons[1], "INSERT INTO users VALUES (1, 'John');")
        self.assertEqual(statements_with_semicolons[2], "SELECT * FROM users;")

    def test_parse_all_sqlite_statements(self):
        """Test parsing a multi-statement SQL string with all major SQLite statement types."""
        multi_sql = """
        CREATE TABLE users (id INTEGER PRIMARY KEY);
        CREATE INDEX idx_name ON users(name);
        CREATE VIEW v_users AS SELECT * FROM users;
        CREATE TRIGGER trg AFTER INSERT ON users BEGIN UPDATE users SET name = 'X'; END;
        INSERT INTO users VALUES (1, 'John');
        UPDATE users SET name = 'Jane' WHERE id = 1;
        DELETE FROM users WHERE id = 1;
        DROP TABLE users;
        DROP INDEX idx_name;
        DROP VIEW v_users;
        DROP TRIGGER trg;
        ALTER TABLE users ADD COLUMN email TEXT;
        REINDEX idx_name;
        ANALYZE users;
        VACUUM;
        PRAGMA journal_mode=WAL;
        ATTACH DATABASE 'file.db' AS db2;
        DETACH DATABASE db2;
        BEGIN TRANSACTION;
        COMMIT;
        ROLLBACK;
        SAVEPOINT sp1;
        RELEASE sp1;
        EXPLAIN QUERY PLAN SELECT * FROM users;
        """
        statements = parse_sql_statements(multi_sql)
        
        # Debug: Print what we actually got
        print(f"\nParsed {len(statements)} statements:")
        for i, stmt in enumerate(statements, 1):
            print(f"{i}. {stmt[:80]}...")
        
        expected_count = 24  # Now the trigger should be parsed as a single statement
        self.assertEqual(len(statements), expected_count)
        # Spot check a few
        self.assertTrue(any(stmt.startswith('CREATE TABLE') for stmt in statements))
        self.assertTrue(any(stmt.startswith('INSERT INTO') for stmt in statements))
        self.assertTrue(any(stmt.startswith('PRAGMA') for stmt in statements))
        self.assertTrue(any(stmt.startswith('EXPLAIN') for stmt in statements))

    def test_split_sql_file_with_semicolons(self):
        """Test split_sql_file with strip_semicolon parameter."""
        sql_content = """
        CREATE TABLE users (id INTEGER PRIMARY KEY);
        INSERT INTO users VALUES (1, 'John');
        SELECT * FROM users;
        """
        with tempfile.NamedTemporaryFile(mode='w', suffix='.sql', delete=False) as f:
            f.write(sql_content)
            temp_file = f.name
        try:
            statements = split_sql_file(temp_file, strip_semicolon=False)
            for stmt in statements:
                self.assertTrue(stmt.endswith(';'))
            statements_stripped = split_sql_file(temp_file, strip_semicolon=True)
            for stmt in statements_stripped:
                self.assertFalse(stmt.endswith(';'))
        finally:
            os.unlink(temp_file)


class TestDetectStatementType(unittest.TestCase):
    """Test cases for detect_statement_type function."""
    
    def test_detect_statement_type_select(self):
        """Test SELECT statement detection."""
        sql = "SELECT * FROM users"
        self.assertEqual(detect_statement_type(sql), 'fetch')
        
        sql = "SELECT id, name FROM users WHERE id = 1"
        self.assertEqual(detect_statement_type(sql), 'fetch')
        
        sql = "SELECT COUNT(*) FROM users"
        self.assertEqual(detect_statement_type(sql), 'fetch')
    
    def test_detect_statement_type_cte_with_select(self):
        """Test CTE (WITH ... SELECT) statement detection."""
        sql = """
        WITH user_counts AS (
            SELECT department, COUNT(*) as count
            FROM users
            GROUP BY department
        )
        SELECT * FROM user_counts
        """
        self.assertEqual(detect_statement_type(sql), 'fetch')
        
        sql = """
        WITH recursive_cte AS (
            SELECT 1 as n
            UNION ALL
            SELECT n + 1 FROM recursive_cte WHERE n < 10
        )
        SELECT * FROM recursive_cte
        """
        self.assertEqual(detect_statement_type(sql), 'fetch')
        
        sql = """
        WITH cte1 AS (SELECT 1 as a),
             cte2 AS (SELECT 2 as b)
        SELECT a, b FROM cte1, cte2
        """
        self.assertEqual(detect_statement_type(sql), 'fetch')
    
    def test_detect_statement_type_cte_with_insert(self):
        """Test CTE with INSERT statement detection."""
        sql = """
        WITH user_data AS (
            SELECT 'John' as name, 'john@example.com' as email
        )
        INSERT INTO users (name, email) SELECT name, email FROM user_data
        """
        self.assertEqual(detect_statement_type(sql), 'execute')
        
        sql = """
        WITH temp_data AS (
            SELECT id, name FROM source_table WHERE active = 1
        )
        INSERT INTO target_table SELECT * FROM temp_data
        """
        self.assertEqual(detect_statement_type(sql), 'execute')
    
    def test_detect_statement_type_cte_with_update(self):
        """Test CTE with UPDATE statement detection."""
        sql = """
        WITH user_updates AS (
            SELECT id, 'Updated' as new_name FROM users WHERE id = 1
        )
        UPDATE users SET name = new_name FROM user_updates WHERE users.id = user_updates.id
        """
        self.assertEqual(detect_statement_type(sql), 'execute')
    
    def test_detect_statement_type_cte_with_delete(self):
        """Test CTE with DELETE statement detection."""
        sql = """
        WITH users_to_delete AS (
            SELECT id FROM users WHERE last_login < '2020-01-01'
        )
        DELETE FROM users WHERE id IN (SELECT id FROM users_to_delete)
        """
        self.assertEqual(detect_statement_type(sql), 'execute')
    
    def test_detect_statement_type_values(self):
        """Test VALUES statement detection."""
        sql = "VALUES (1, 'John'), (2, 'Jane')"
        self.assertEqual(detect_statement_type(sql), 'fetch')
        
        sql = "VALUES (1, 'John')"
        self.assertEqual(detect_statement_type(sql), 'fetch')
    
    def test_detect_statement_type_show(self):
        """Test SHOW statement detection."""
        sql = "SHOW TABLES"
        self.assertEqual(detect_statement_type(sql), 'fetch')
        
        sql = "SHOW DATABASES"
        self.assertEqual(detect_statement_type(sql), 'fetch')
        
        sql = "SHOW CREATE TABLE users"
        self.assertEqual(detect_statement_type(sql), 'fetch')
    
    def test_detect_statement_type_explain(self):
        """Test EXPLAIN statement detection."""
        sql = "EXPLAIN SELECT * FROM users"
        self.assertEqual(detect_statement_type(sql), 'fetch')
        
        sql = "EXPLAIN QUERY PLAN SELECT * FROM users"
        self.assertEqual(detect_statement_type(sql), 'fetch')
    
    def test_detect_statement_type_pragma(self):
        """Test PRAGMA statement detection."""
        sql = "PRAGMA table_info(users)"
        self.assertEqual(detect_statement_type(sql), 'fetch')
        
        sql = "PRAGMA foreign_key_list(users)"
        self.assertEqual(detect_statement_type(sql), 'fetch')
        
        sql = "PRAGMA journal_mode=WAL"
        self.assertEqual(detect_statement_type(sql), 'fetch')
    
    def test_detect_statement_type_describe(self):
        """Test DESCRIBE/DESC statement detection."""
        sql = "DESCRIBE users"
        self.assertEqual(detect_statement_type(sql), 'fetch')
        
        sql = "DESC users"
        self.assertEqual(detect_statement_type(sql), 'fetch')
    
    def test_detect_statement_type_insert(self):
        """Test INSERT statement detection."""
        sql = "INSERT INTO users (name) VALUES ('John')"
        self.assertEqual(detect_statement_type(sql), 'execute')
        
        sql = "INSERT INTO users SELECT * FROM temp_users"
        self.assertEqual(detect_statement_type(sql), 'execute')
    
    def test_detect_statement_type_update(self):
        """Test UPDATE statement detection."""
        sql = "UPDATE users SET name = 'John' WHERE id = 1"
        self.assertEqual(detect_statement_type(sql), 'execute')
    
    def test_detect_statement_type_delete(self):
        """Test DELETE statement detection."""
        sql = "DELETE FROM users WHERE id = 1"
        self.assertEqual(detect_statement_type(sql), 'execute')
    
    def test_detect_statement_type_create(self):
        """Test CREATE statement detection."""
        sql = "CREATE TABLE users (id INTEGER PRIMARY KEY)"
        self.assertEqual(detect_statement_type(sql), 'execute')
        
        sql = "CREATE INDEX idx_name ON users(name)"
        self.assertEqual(detect_statement_type(sql), 'execute')
        
        sql = "CREATE VIEW v_users AS SELECT * FROM users"
        self.assertEqual(detect_statement_type(sql), 'execute')
    
    def test_detect_statement_type_alter(self):
        """Test ALTER statement detection."""
        sql = "ALTER TABLE users ADD COLUMN email TEXT"
        self.assertEqual(detect_statement_type(sql), 'execute')
        
        sql = "ALTER TABLE users DROP COLUMN email"
        self.assertEqual(detect_statement_type(sql), 'execute')
    
    def test_detect_statement_type_drop(self):
        """Test DROP statement detection."""
        sql = "DROP TABLE users"
        self.assertEqual(detect_statement_type(sql), 'execute')
        
        sql = "DROP INDEX idx_name"
        self.assertEqual(detect_statement_type(sql), 'execute')
    
    def test_detect_statement_type_transaction(self):
        """Test transaction statement detection."""
        sql = "BEGIN TRANSACTION"
        self.assertEqual(detect_statement_type(sql), 'execute')
        
        sql = "COMMIT"
        self.assertEqual(detect_statement_type(sql), 'execute')
        
        sql = "ROLLBACK"
        self.assertEqual(detect_statement_type(sql), 'execute')
    
    def test_detect_statement_type_empty_and_whitespace(self):
        """Test empty and whitespace-only statements."""
        self.assertEqual(detect_statement_type(""), 'execute')
        self.assertEqual(detect_statement_type("   "), 'execute')
        self.assertEqual(detect_statement_type("\n\t"), 'execute')
    
    def test_detect_statement_type_with_comments(self):
        """Test statements with comments."""
        sql = "-- This is a comment\nSELECT * FROM users"
        self.assertEqual(detect_statement_type(sql), 'fetch')
        
        sql = "/* Comment */ SELECT * FROM users"
        self.assertEqual(detect_statement_type(sql), 'fetch')
        
        sql = "WITH cte AS (SELECT 1) -- Comment\nSELECT * FROM cte"
        self.assertEqual(detect_statement_type(sql), 'fetch')
    
    def test_detect_statement_type_case_insensitive(self):
        """Test that statement detection is case insensitive."""
        sql = "select * from users"
        self.assertEqual(detect_statement_type(sql), 'fetch')
        
        sql = "Select * From users"
        self.assertEqual(detect_statement_type(sql), 'fetch')
        
        sql = "with cte as (select 1) select * from cte"
        self.assertEqual(detect_statement_type(sql), 'fetch')
        
        sql = "insert into users values (1, 'John')"
        self.assertEqual(detect_statement_type(sql), 'execute')
    
    def test_detect_statement_type_complex_nested(self):
        """Test complex nested statements."""
        sql = """
        WITH user_stats AS (
            SELECT 
                department,
                COUNT(*) as user_count,
                AVG(salary) as avg_salary
            FROM users 
            WHERE active = 1
            GROUP BY department
        ),
        dept_rankings AS (
            SELECT 
                department,
                user_count,
                avg_salary,
                RANK() OVER (ORDER BY avg_salary DESC) as salary_rank
            FROM user_stats
        )
        SELECT 
            department,
            user_count,
            avg_salary,
            salary_rank
        FROM dept_rankings
        WHERE salary_rank <= 5
        """
        self.assertEqual(detect_statement_type(sql), 'fetch')


class TestSqlHelperEdgeCases(unittest.TestCase):
    """Test edge cases and error conditions in sql_helper module."""
    
    def test_remove_sql_comments_empty_input(self):
        """Test comment removal with empty input."""
        self.assertEqual(remove_sql_comments(""), "")
        self.assertEqual(remove_sql_comments(None), None)
        self.assertEqual(remove_sql_comments("   "), "")
    
    def test_remove_sql_comments_nested_comments(self):
        """Test removing nested comments."""
        sql = """
        /* Outer comment /* Inner comment */ */
        SELECT * FROM users; -- Line comment
        """
        clean_sql = remove_sql_comments(sql)
        # Should handle gracefully without errors
        self.assertIsInstance(clean_sql, str)
        # Should remove at least some comments
        self.assertNotIn('--', clean_sql)
        # May not handle nested comments perfectly, but should not crash
        self.assertIn('SELECT * FROM users;', clean_sql)
    
    def test_remove_sql_comments_incomplete_comments(self):
        """Test handling incomplete comments."""
        sql = """
        SELECT * FROM users; -- Incomplete comment
        SELECT * FROM users; /* Incomplete comment
        """
        clean_sql = remove_sql_comments(sql)
        # Should handle gracefully without errors
        self.assertIsInstance(clean_sql, str)
    
    def test_parse_sql_statements_malformed_sql(self):
        """Test parsing malformed SQL statements."""
        malformed_sql = """
        SELECT * FROM users; -- Missing semicolon
        INSERT INTO users VALUES (1, 'John' -- Missing closing quote
        CREATE TABLE users (id INTEGER -- Missing closing parenthesis
        """
        statements = parse_sql_statements(malformed_sql)
        # Should handle gracefully and return what can be parsed
        self.assertIsInstance(statements, list)
    
    def test_parse_sql_statements_only_semicolons(self):
        """Test parsing SQL with only semicolons."""
        sql = ";;;;"
        statements = parse_sql_statements(sql)
        self.assertEqual(statements, [])
    
    def test_parse_sql_statements_whitespace_only(self):
        """Test parsing SQL with only whitespace."""
        sql = "   \n\t   \n"
        statements = parse_sql_statements(sql)
        self.assertEqual(statements, [])
    
    def test_detect_statement_type_malformed_sql(self):
        """Test detecting statement type in malformed SQL."""
        malformed_sql = "SELECT * FROM users -- Missing semicolon"
        result = detect_statement_type(malformed_sql)
        self.assertEqual(result, 'fetch')
        
        malformed_sql = "INSERT INTO users VALUES (1, 'John' -- Missing closing quote"
        result = detect_statement_type(malformed_sql)
        self.assertEqual(result, 'execute')
    
    def test_detect_statement_type_very_long_sql(self):
        """Test detecting statement type in very long SQL."""
        long_sql = "SELECT " + "a, " * 1000 + "b FROM very_long_table_name"
        result = detect_statement_type(long_sql)
        self.assertEqual(result, 'fetch')
    
    def test_split_sql_file_empty_file(self):
        """Test splitting empty SQL file."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.sql', delete=False) as f:
            f.write("")
            temp_file = f.name
        
        try:
            statements = split_sql_file(temp_file)
            self.assertEqual(statements, [])
        finally:
            os.unlink(temp_file)
    
    def test_split_sql_file_large_file(self):
        """Test splitting large SQL file."""
        large_sql = ""
        for i in range(1000):
            large_sql += f"INSERT INTO users VALUES ({i}, 'User{i}');\n"
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.sql', delete=False) as f:
            f.write(large_sql)
            temp_file = f.name
        
        try:
            statements = split_sql_file(temp_file)
            self.assertEqual(len(statements), 1000)
            for i, stmt in enumerate(statements):
                self.assertIn(f"INSERT INTO users VALUES ({i}, 'User{i}')", stmt)
        finally:
            os.unlink(temp_file)
    
    def test_split_sql_file_invalid_path(self):
        """Test splitting SQL file with invalid path."""
        with self.assertRaises(ValueError):
            split_sql_file("")
        
        with self.assertRaises(ValueError):
            split_sql_file(None)
        
        with self.assertRaises(ValueError):
            split_sql_file(123)  # Not a string


class TestSqlHelperPerformance(unittest.TestCase):
    """Test performance characteristics of sql_helper functions."""
    
    def test_remove_sql_comments_performance(self):
        """Test performance of comment removal with large SQL."""
        import time
        
        # Create large SQL with many comments
        large_sql = ""
        for i in range(1000):
            large_sql += f"SELECT * FROM table{i}; -- Comment {i}\n"
            large_sql += f"/* Multi-line comment {i} */\n"
        
        start_time = time.time()
        clean_sql = remove_sql_comments(large_sql)
        end_time = time.time()
        
        # Should complete in reasonable time (less than 5 second)
        self.assertLess(end_time - start_time, 5.0)
        self.assertNotIn('--', clean_sql)
        self.assertNotIn('/*', clean_sql)
        self.assertNotIn('*/', clean_sql)
    
    def test_parse_sql_statements_performance(self):
        """Test performance of SQL parsing with many statements."""
        import time
        
        # Create SQL with many statements
        many_statements = ""
        for i in range(1000):
            many_statements += f"INSERT INTO users VALUES ({i}, 'User{i}');\n"
        
        start_time = time.time()
        statements = parse_sql_statements(many_statements)
        end_time = time.time()
        
        # Should complete in reasonable time (less than 5 second)
        self.assertLess(end_time - start_time, 5.0)
        self.assertEqual(len(statements), 1000)
    
    def test_detect_statement_type_performance(self):
        """Test performance of statement type detection with complex SQL."""
        import time
        
        # Create complex SQL with CTEs
        complex_sql = """
        WITH cte1 AS (
            SELECT id, name FROM users WHERE active = 1
        ), cte2 AS (
            SELECT id, email FROM profiles WHERE verified = 1
        ), cte3 AS (
            SELECT id, phone FROM contacts WHERE primary = 1
        )
        SELECT c1.name, c2.email, c3.phone 
        FROM cte1 c1 
        JOIN cte2 c2 ON c1.id = c2.id
        JOIN cte3 c3 ON c1.id = c3.id
        WHERE c1.name LIKE '%John%'
        """
        
        start_time = time.time()
        for _ in range(1000):
            result = detect_statement_type(complex_sql)
        end_time = time.time()
        
        # Should complete 1000 iterations in reasonable time (less than 5 second)
        self.assertLess(end_time - start_time, 5.0)
        self.assertEqual(result, 'fetch')


class TestSqlHelperIntegration(unittest.TestCase):
    """Test integration scenarios combining multiple sql_helper functions."""
    
    def test_full_sql_processing_pipeline(self):
        """Test complete SQL processing pipeline."""
        complex_sql = """
        -- Create users table
        CREATE TABLE users (
            id INTEGER PRIMARY KEY, -- user id
            name TEXT NOT NULL,     /* user name */
            email TEXT,             -- user email
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        
        /* Insert some test data */
        INSERT INTO users (name, email) VALUES ('John Doe', 'john@example.com');
        INSERT INTO users (name, email) VALUES ('Jane Smith', 'jane@example.com');
        
        -- Create index
        CREATE INDEX idx_users_email ON users(email);
        
        -- Query to verify
        SELECT * FROM users WHERE email LIKE '%@example.com';
        """
        
        # Step 1: Remove comments
        clean_sql = remove_sql_comments(complex_sql)
        self.assertNotIn('--', clean_sql)
        self.assertNotIn('/*', clean_sql)
        self.assertNotIn('*/', clean_sql)
        
        # Step 2: Parse statements
        statements = parse_sql_statements(clean_sql)
        self.assertEqual(len(statements), 5)
        
        # Step 3: Detect types for each statement
        expected_types = ['execute', 'execute', 'execute', 'execute', 'fetch']
        for i, stmt in enumerate(statements):
            stmt_type = detect_statement_type(stmt)
            self.assertEqual(stmt_type, expected_types[i])
    
    def test_cte_processing_pipeline(self):
        """Test processing pipeline with complex CTEs."""
        cte_sql = """
        WITH user_stats AS (
            SELECT 
                user_id,
                COUNT(*) as post_count, -- comment in CTE
                AVG(rating) as avg_rating /* another comment */
            FROM posts 
            WHERE active = 1
        ), user_profiles AS (
            SELECT 
                id,
                name,
                email
            FROM users 
            WHERE verified = 1
        )
        SELECT 
            up.name,
            us.post_count,
            us.avg_rating
        FROM user_profiles up
        JOIN user_stats us ON up.id = us.user_id
        WHERE us.post_count > 5;
        """
        
        # Process through pipeline
        clean_sql = remove_sql_comments(cte_sql)
        statements = parse_sql_statements(clean_sql)
        self.assertEqual(len(statements), 1)
        
        stmt_type = detect_statement_type(statements[0])
        self.assertEqual(stmt_type, 'fetch')
    
    def test_batch_processing_with_file(self):
        """Test batch processing using file operations."""
        batch_sql = """
        -- Batch of operations
        CREATE TABLE temp_users (id INTEGER, name TEXT);
        INSERT INTO temp_users VALUES (1, 'User1');
        INSERT INTO temp_users VALUES (2, 'User2');
        SELECT * FROM temp_users;
        DROP TABLE temp_users;
        """
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.sql', delete=False) as f:
            f.write(batch_sql)
            temp_file = f.name
        
        try:
            # Read and process file
            statements = split_sql_file(temp_file)
            self.assertEqual(len(statements), 5)
            
            # Process each statement
            for stmt in statements:
                clean_stmt = remove_sql_comments(stmt)
                stmt_type = detect_statement_type(clean_stmt)
                self.assertIn(stmt_type, ['execute', 'fetch'])
        finally:
            os.unlink(temp_file)


if __name__ == '__main__':
    unittest.main() 