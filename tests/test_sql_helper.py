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
    validate_sql_statement
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
        self.assertIn('CREATE TABLE users (id INTEGER PRIMARY KEY)', statements)
        self.assertIn('INSERT INTO users VALUES (1, \'John\')', statements)
        self.assertIn('SELECT * FROM users', statements)
    
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
            self.assertIn('CREATE TABLE users (id INTEGER PRIMARY KEY)', statements)
        finally:
            os.unlink(temp_file)
    
    def test_split_sql_file_not_found(self):
        """Test handling of non-existent file."""
        with self.assertRaises(FileNotFoundError):
            split_sql_file("non_existent_file.sql")
    
    def test_validate_sql_statement_valid(self):
        """Test validation of valid SQL statements, including all major SQLite types."""
        valid_statements = [
            # DML
            "SELECT * FROM users",
            "INSERT INTO users VALUES (1, 'John')",
            "UPDATE users SET name = 'John' WHERE id = 1",
            "DELETE FROM users WHERE id = 1",
            # DDL
            "CREATE TABLE users (id INTEGER PRIMARY KEY)",
            "CREATE INDEX idx_name ON users(name)",
            "CREATE VIEW v_users AS SELECT * FROM users",
            "CREATE TRIGGER trg AFTER INSERT ON users BEGIN UPDATE users SET name = 'X'; END;",
            "DROP TABLE users",
            "DROP INDEX idx_name",
            "DROP VIEW v_users",
            "DROP TRIGGER trg",
            "ALTER TABLE users ADD COLUMN email TEXT",
            # Other SQLite statements
            "REINDEX idx_name",
            "ANALYZE users",
            "VACUUM",
            "PRAGMA journal_mode=WAL",
            "ATTACH DATABASE 'file.db' AS db2",
            "DETACH DATABASE db2",
            "BEGIN TRANSACTION",
            "COMMIT",
            "ROLLBACK",
            "SAVEPOINT sp1",
            "RELEASE sp1",
            "EXPLAIN QUERY PLAN SELECT * FROM users"
        ]
        
        for stmt in valid_statements:
            is_valid, error = validate_sql_statement(stmt)
            self.assertTrue(is_valid, f"Statement should be valid: {stmt}")
            self.assertEqual(error, "")
    
    def test_validate_sql_statement_invalid(self):
        """Test validation of invalid SQL statements, including all major SQLite types."""
        invalid_statements = [
            ("", "Empty statement"),
            ("   ", "Empty statement"),
            ("SELECT * FROM", "SELECT statement missing table name after FROM"),
            ("INSERT users VALUES (1)", "INSERT statement missing INTO clause"),
            ("UPDATE users name = 'John'", "UPDATE statement missing SET clause"),
            ("DELETE users WHERE id = 1", "DELETE statement missing FROM clause"),
            ("CREATE users (id INTEGER)", "CREATE statement missing TABLE, INDEX, VIEW, or TRIGGER clause"),
            ("DROP users", "DROP statement missing TABLE, INDEX, VIEW, or TRIGGER clause"),
            ("ALTER users ADD COLUMN email", "ALTER statement missing TABLE clause"),
            # Some malformed SQLite statements
            ("PRAGMA", "PRAGMA statement missing argument"),
            ("ATTACH DATABASE", "ATTACH statement missing AS keyword"),
            ("DETACH", "DETACH statement missing DATABASE keyword"),
            ("REINDEX", "REINDEX statement missing target"),
            ("ANALYZE", "ANALYZE statement missing target"),
            ("SAVEPOINT", "SAVEPOINT statement missing name"),
            ("RELEASE", "RELEASE statement missing savepoint name")
        ]
        
        for stmt, expected_error in invalid_statements:
            is_valid, error = validate_sql_statement(stmt)
            self.assertFalse(is_valid, f"Statement should be invalid: {stmt}")
            self.assertIn(expected_error, error)
    
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
        
        # Test default behavior (strip semicolons)
        statements = parse_sql_statements(sql)
        self.assertEqual(len(statements), 3)
        
        # Verify semicolons are stripped
        for stmt in statements:
            self.assertFalse(stmt.endswith(';'), f"Statement should not end with semicolon: {stmt}")
        
        # Verify the statements are correct
        self.assertEqual(statements[0], "CREATE TABLE users (id INTEGER PRIMARY KEY)")
        self.assertEqual(statements[1], "INSERT INTO users VALUES (1, 'John')")
        self.assertEqual(statements[2], "SELECT * FROM users")
        
        # Test preserve_trailing_semicolon=True behavior
        statements_with_semicolons = parse_sql_statements(sql, preserve_trailing_semicolon=True)
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
        """Test split_sql_file with preserve_trailing_semicolon parameter."""
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
            # Test default behavior (strip semicolons)
            statements = split_sql_file(temp_file)
            self.assertEqual(len(statements), 3)
            for stmt in statements:
                self.assertFalse(stmt.endswith(';'), f"Statement should not end with semicolon: {stmt}")
            
            # Test preserve_trailing_semicolon=True
            statements_with_semicolons = split_sql_file(temp_file, preserve_trailing_semicolon=True)
            self.assertEqual(len(statements_with_semicolons), 3)
            for stmt in statements_with_semicolons:
                self.assertTrue(stmt.endswith(';'), f"Statement should end with semicolon: {stmt}")
                
        finally:
            os.unlink(temp_file)


if __name__ == '__main__':
    unittest.main() 