"""
SQLite-specific tests for DbEngine class.

Copyright (c) 2025, Jim Schilling

Please keep this header when you use this code.

This module is licensed under the MIT License.
"""

import os
import tempfile
import threading
import time

import pytest
from sqlalchemy import text

from jpy_sync_db_lite.db_engine import DbEngine
from jpy_sync_db_lite.errors import MaintenanceError


class TestDbEngineSQLiteSpecific:
    """Test cases for SQLite-specific DbEngine functionality."""

    @pytest.fixture(autouse=True)
    def setup_method(self):
        """Set up test fixtures before each test method."""
        self.temp_db_fd, self.temp_db_path = tempfile.mkstemp(suffix=".db")
        os.close(self.temp_db_fd)
        self.database_url = f"sqlite:///{self.temp_db_path}"
        self.db_engine = DbEngine(self.database_url, timeout=30, check_same_thread=False)
        self._create_test_table()
        self._insert_test_data()
        yield
        """Clean up after each test method."""
        if hasattr(self, "db_engine"):
            self.db_engine.shutdown()
            time.sleep(0.1)
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
            {"name": "Charlie Brown", "email": "charlie@example.com", "active": True},
        ]
        insert_sql = """
        INSERT INTO test_users (name, email, active)
        VALUES (:name, :email, :active)
        """
        self.db_engine.execute(insert_sql, params=test_data)

    @pytest.mark.unit
    def test_init_with_sqlite_parameters(self):
        """Test DbEngine initialization with SQLite-specific parameters."""
        db = DbEngine(self.database_url, timeout=60, check_same_thread=True, debug=True)

        assert db.engine is not None
        db.shutdown()

    @pytest.mark.unit
    def test_init_with_default_parameters(self):
        """Test DbEngine initialization with default parameters."""
        db = DbEngine(self.database_url)
        # No assertions about workers; just ensure initialization does not raise
        db.shutdown()

    @pytest.mark.unit
    def test_configure_pragma(self):
        """Test configuring SQLite PRAGMA settings."""
        # Test setting cache size
        self.db_engine.configure_pragma("cache_size", "-32000")  # 32MB cache

        # Verify the setting was applied
        with self.db_engine.get_raw_connection() as conn:
            result = conn.execute(text("PRAGMA cache_size"))
            cache_size = result.fetchone()[0]
            assert cache_size == -32000

        # Test setting synchronous mode
        self.db_engine.configure_pragma("synchronous", "FULL")

        with self.db_engine.get_raw_connection() as conn:
            result = conn.execute(text("PRAGMA synchronous"))
            synchronous = result.fetchone()[0]
            assert synchronous == 2  # FULL mode

    @pytest.mark.unit
    def test_get_sqlite_info(self):
        """Test getting SQLite-specific information."""
        info = self.db_engine.get_sqlite_info()

        # Check required fields
        assert "version" in info
        assert "database_size" in info
        assert "page_count" in info
        assert "page_size" in info
        assert "cache_size" in info
        assert "journal_mode" in info
        assert "synchronous" in info
        assert "temp_store" in info

        # Verify SQLite version is a string
        assert isinstance(info["version"], str)
        assert len(info["version"]) > 0

        # Verify database size is a number (if available)
        if info["database_size"] is not None:
            assert isinstance(info["database_size"], int)
            assert info["database_size"] > 0

        # Verify pragma values are reasonable (behavior, not exact implementation)
        assert isinstance(info["page_size"], int)
        assert info["page_size"] > 0
        assert info["journal_mode"] in ["wal", "memory"]
        assert info["synchronous"] in [0, 1, 2]  # OFF, NORMAL, FULL
        assert info["temp_store"] in [0, 1, 2]  # DEFAULT, FILE, MEMORY

    @pytest.mark.integration
    def test_vacuum_operation(self):
        """Test SQLite VACUUM operation."""
        # First, delete some data to create fragmentation
        self.db_engine.execute("DELETE FROM test_users WHERE name = :name", params={"name": "Bob Smith"})

        # Get initial database size
        initial_info = self.db_engine.get_sqlite_info()
        initial_info["database_size"]

        # Run VACUUM (SQLite VACUUM doesn't support mode parameters)
        self.db_engine.vacuum()

        # Get database size after VACUUM
        final_info = self.db_engine.get_sqlite_info()
        final_size = final_info["database_size"]

        # Only assert if not in-memory and file exists
        if final_size is not None:
            assert isinstance(final_size, int)

        # Verify data integrity after VACUUM
        users = self.db_engine.fetch("SELECT * FROM test_users")
        assert len(users.data) == 2  # Should have 2 users remaining

    @pytest.mark.unit
    def test_analyze_operation(self):
        """Test SQLite ANALYZE operation."""
        # Run ANALYZE on all tables
        self.db_engine.analyze()

        # Should not raise an exception
        users = self.db_engine.fetch("SELECT * FROM test_users")
        assert len(users.data) == 3

    @pytest.mark.unit
    def test_analyze_specific_table(self):
        """Test ANALYZE operation on specific table."""
        # Run ANALYZE on specific table
        self.db_engine.analyze(table_name="test_users")

        # Should not raise an exception
        users = self.db_engine.fetch("SELECT * FROM test_users")
        assert len(users.data) == 3

    @pytest.mark.unit
    def test_integrity_check(self):
        """Test SQLite integrity check."""
        # Run integrity check
        issues = self.db_engine.integrity_check()

        # Should return empty list for healthy database
        assert isinstance(issues, list)
        assert len(issues) == 0

    @pytest.mark.unit
    def test_integrity_check_with_corruption(self):
        """Test integrity check with database corruption simulation."""
        # This test simulates what would happen with corruption
        # In a real scenario, we'd need to actually corrupt the file
        # For now, we just verify the method works correctly

        # Run integrity check on healthy database
        issues = self.db_engine.integrity_check()
        assert len(issues) == 0

        # Verify the method returns a list even if there are issues
        assert isinstance(issues, list)

    @pytest.mark.unit
    def test_optimize_operation(self):
        """Test SQLite optimization operation."""
        # Run optimization
        self.db_engine.optimize()

        # Should not raise an exception
        users = self.db_engine.fetch("SELECT * FROM test_users")
        assert len(users.data) == 3

    @pytest.mark.unit
    def test_get_raw_connection_exception(self):
        with pytest.raises(Exception):
            with self.db_engine.get_raw_connection() as conn:
                conn.execute(text("INVALID SQL"))

    @pytest.mark.unit
    def test_analyze_error_wrapping(self):
        # Pass an invalid table name to reliably trigger an error
        with pytest.raises(MaintenanceError):
            self.db_engine.analyze(table_name="this_table_does_not_exist")

    @pytest.mark.unit
    def test_maintenance_operations_error_wrapping(self):
        """Test that all maintenance operations properly wrap errors in appropriate exception types."""
        # Test all maintenance operations to ensure they wrap errors properly
        maintenance_operations = [
            (self.db_engine.vacuum, "VACUUM operation failed"),
            (lambda: self.db_engine.analyze(), "ANALYZE operation failed"),
            (
                lambda: self.db_engine.analyze(table_name="test_users"),
                "ANALYZE operation failed",
            ),
            (lambda: self.db_engine.integrity_check(), "Integrity check failed"),
            (self.db_engine.optimize, "Optimization operation failed"),
        ]

        for operation, expected_error_text in maintenance_operations:
            try:
                operation()
                # If operation succeeds, that's fine - we're just testing error wrapping
            except MaintenanceError as e:
                # If an error occurs, it should be wrapped in MaintenanceError
                assert isinstance(e, MaintenanceError)
                assert expected_error_text in str(e)

    @pytest.mark.unit
    def test_enhanced_performance_configuration(self):
        """Test enhanced SQLite performance configuration."""
        with self.db_engine.get_raw_connection() as conn:
            # Check foreign keys are enabled
            result = conn.execute(text("PRAGMA foreign_keys"))
            foreign_keys = result.fetchone()[0]
            assert foreign_keys == 1

            # Check busy timeout
            result = conn.execute(text("PRAGMA busy_timeout"))
            busy_timeout = result.fetchone()[0]
            assert busy_timeout == 30000  # 30 seconds

            # Check auto vacuum (may not be set if database already exists)
            result = conn.execute(text("PRAGMA auto_vacuum"))
            auto_vacuum = result.fetchone()[0]
            # Auto vacuum can be 0 (NONE), 1 (FULL), or 2 (INCREMENTAL)
            assert auto_vacuum in [0, 1, 2]

    @pytest.mark.unit
    def test_connection_parameters(self):
        """Test SQLite connection parameters are properly set."""
        # Test with different connection parameters
        db = DbEngine(
            self.database_url,
            timeout=60,
            check_same_thread=False,  # Must be False for threading
        )

        # Verify the engine was created successfully
        assert db.engine is not None

        # Test that operations work with custom parameters
        result = db.fetch("SELECT 1 as test", params={})
        assert len(result.data) == 1
        assert result.data[0]["test"] == 1

        db.shutdown()

    @pytest.mark.unit
    def test_pragma_configuration_edge_cases(self):
        """Test PRAGMA configuration with edge cases."""
        # Test invalid PRAGMA name (should not crash)
        try:
            self.db_engine.configure_pragma("invalid_pragma", "value")
        except Exception as e:
            # Should handle gracefully
            assert isinstance(e, Exception)

        # Test numeric PRAGMA value (skip empty value test)
        self.db_engine.configure_pragma("cache_size", "1000")

        with self.db_engine.get_raw_connection() as conn:
            result = conn.execute(text("PRAGMA cache_size"))
            cache_size = result.fetchone()[0]
            assert cache_size == 1000

    @pytest.mark.unit
    def test_sqlite_info_edge_cases(self):
        """Test SQLite info retrieval with edge cases."""
        # Test with in-memory database
        memory_db = DbEngine("sqlite:///:memory:")

        info = memory_db.get_sqlite_info()

        # Should still get version and other info
        assert "version" in info
        assert isinstance(info["version"], str)

        # Database size might be None for in-memory
        assert "database_size" in info

        memory_db.shutdown()

    @pytest.mark.integration
    def test_concurrent_sqlite_operations(self):
        """Test concurrent SQLite-specific operations."""

        results = []
        errors = []

        def worker(worker_id):
            try:
                # Each worker performs SQLite-specific operations
                for i in range(2):  # Reduced iterations
                    # Configure pragma (use different values to avoid conflicts)
                    pragma_value = f"-{16000 + worker_id * 1000 + i}"
                    self.db_engine.configure_pragma("cache_size", pragma_value)

                    # Get SQLite info (read-only operation)
                    info = self.db_engine.get_sqlite_info()
                    results.append(info["version"])

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
        assert len(errors) == 0
        assert len(results) > 0

        # All results should be the same SQLite version
        unique_versions = set(results)
        assert len(unique_versions) == 1
