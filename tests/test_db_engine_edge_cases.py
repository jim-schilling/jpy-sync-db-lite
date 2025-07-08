"""
Edge case tests for DbEngine class.

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

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from jpy_sync_db_lite.db_engine import DbEngine, DbOperationError, SQLiteError, DbResult
from jpy_sync_db_lite.db_request import DbRequest


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
        request = DbRequest('execute', 'SELECT 1')

        # This should not raise an exception
        db.request_queue.put(request)
        time.sleep(0.1)  # Give worker time to process

        db.shutdown()

    @pytest.mark.unit
    def test_invalid_operation_type(self):
        """Test handling of invalid operation type."""
        db = DbEngine(self.database_url)

        # Create request with invalid operation (not 'fetch' or 'execute')
        request = DbRequest('invalid_operation', 'SELECT 1', response_queue=queue.Queue())

        # This should be handled gracefully
        db.request_queue.put(request)
        time.sleep(0.1)  # Give worker time to process

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


class TestDbRequest(unittest.TestCase):
    """Test cases for DbRequest class."""

    @pytest.mark.unit
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

    @pytest.mark.unit
    def test_db_request_timestamp(self):
        """Test that DbRequest timestamps are properly set."""
        import time

        from jpy_sync_db_lite.db_request import DbRequest

        before = time.time()
        request = DbRequest('fetch', 'SELECT 1')
        after = time.time()

        self.assertGreaterEqual(request.timestamp, before)
        self.assertLessEqual(request.timestamp, after)

    @pytest.mark.unit
    def test_db_request_with_list_params(self):
        """Test DbRequest with list parameters for bulk operations."""
        from jpy_sync_db_lite.db_request import DbRequest

        params_list = [
            {'name': 'Alice', 'email': 'alice@example.com'},
            {'name': 'Bob', 'email': 'bob@example.com'}
        ]

        request = DbRequest('execute', 'INSERT INTO users (name, email) VALUES (:name, :email)', params=params_list)
        self.assertEqual(request.operation, 'execute')
        self.assertEqual(request.params, params_list)
        self.assertIsInstance(request.params, list)


if __name__ == '__main__':
    unittest.main() 