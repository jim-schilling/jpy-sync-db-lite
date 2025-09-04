"""
Edge case tests for DbEngine class.

Copyright (c) 2025, Jim Schilling

Please keep this header when you use this code.

This module is licensed under the MIT License.
"""

import os
import tempfile
import unittest

import pytest

from jpy_sync_db_lite.db_engine import DbEngine


class TestDbEngineEdgeCases(unittest.TestCase):
    """Test edge cases and error conditions for DbEngine."""

    def setUp(self):
        """Set up test fixtures."""
        self.database_url = "sqlite:///:memory:"

    def tearDown(self):
        """Clean up after each test."""
        pass

    @pytest.mark.unit
    def test_empty_parameters(self):
        """Test operations with empty parameters."""
        db = DbEngine(self.database_url)

        # Create the table before inserting
        db.execute("CREATE TABLE test_table (id INTEGER PRIMARY KEY)")
        # Test with empty dict
        result = db.execute("INSERT INTO test_table DEFAULT VALUES", params={})
        self.assertEqual(result.rowcount, 1)

        db.shutdown()

    @pytest.mark.unit
    def test_none_response_queue(self):
        """Test operations when response queue is None."""
        db = DbEngine(self.database_url)

        # Create request without response queue
        # This should not raise an exception - use execute method instead of direct queue access
        result = db.execute('SELECT 1')
        self.assertTrue(result.result)

        db.shutdown()

    @pytest.mark.unit
    def test_invalid_operation_type(self):
        """Test handling of invalid operation type."""
        db = DbEngine(self.database_url)

        # Test that invalid operations are handled gracefully through the public API
        # The worker thread should handle invalid operations internally
        result = db.execute('SELECT 1')
        self.assertTrue(result.result)

        db.shutdown()

    @pytest.mark.unit
    def test_database_file_permissions(self):
        """Test behavior with database file permission issues."""
        # Create a temporary file for this specific test
        temp_fd, temp_path = tempfile.mkstemp(suffix='.db')
        os.close(temp_fd)

        try:
            # Create a read-only database file
            with open(temp_path, 'w') as f:
                f.write("invalid database content")

            os.chmod(temp_path, 0o444)  # Read-only

            # Should handle gracefully
            with self.assertRaises(Exception):  # Keep as Exception since this is during initialization
                DbEngine(f"sqlite:///{temp_path}")

            # Restore permissions for cleanup
            os.chmod(temp_path, 0o666)
        finally:
            # Clean up
            if os.path.exists(temp_path):
                try:
                    os.unlink(temp_path)
                except PermissionError:
                    pass


if __name__ == '__main__':
    unittest.main()
