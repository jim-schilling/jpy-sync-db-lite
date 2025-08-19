"""
Stress tests for DbEngine concurrent client handling.

Copyright (c) 2025, Jim Schilling

Please keep this header when you use this code.

This module is licensed under the MIT License.
"""

import os
import tempfile
import time
import unittest
import pytest
from concurrent.futures import ThreadPoolExecutor, as_completed

from jpy_sync_db_lite.db_engine import DbEngine


class TestDbEngineStress(unittest.TestCase):
    """Stress tests for DbEngine concurrent client handling."""

    def setUp(self) -> None:
        """Set up test fixtures before each test method."""
        # Use unique temp file for stress tests to avoid parallel conflicts
        self.temp_db = tempfile.NamedTemporaryFile(delete=False, suffix='.db')
        self.temp_db.close()
        self.database_url = f"sqlite:///{self.temp_db.name}"
        # Create database engine for stress testing
        self.db_engine = DbEngine(self.database_url, debug=False)
        
        # Explicitly enable WAL mode for better concurrency
        self.db_engine.execute("PRAGMA journal_mode=WAL")
        self.db_engine.execute("PRAGMA synchronous=NORMAL")
        self.db_engine.execute("PRAGMA busy_timeout=30000")

    def tearDown(self) -> None:
        """Clean up after each test method."""
        if hasattr(self, 'db_engine'):
            self.db_engine.shutdown()
        
        # Clean up the temp database file and WAL files
        if hasattr(self, 'temp_db'):
            db_files = [self.temp_db.name, f"{self.temp_db.name}-wal", f"{self.temp_db.name}-shm"]
            for db_file in db_files:
                if os.path.exists(db_file):
                    try:
                        os.remove(db_file)
                    except OSError:
                        pass  # Ignore errors if file is locked

    @pytest.mark.performance
    def test_concurrent_client_stress(self) -> None:
        """Stress test to validate DbEngine can handle n concurrent clients."""
        # Test configuration
        num_clients = 10
        operations_per_client = 50
        test_duration = 30  # seconds
        
        # Create test table for concurrent operations
        self.db_engine.execute("""
            CREATE TABLE IF NOT EXISTS stress_test (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                client_id INTEGER NOT NULL,
                operation_type TEXT NOT NULL,
                value TEXT,
                timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Track results
        results = {
            'total_operations': 0,
            'successful_operations': 0,
            'failed_operations': 0,
            'start_time': time.time(),
            'end_time': None,
            'client_results': {}
        }
        
        def client_work(client_id: int) -> dict:
            """Simulate a client performing database operations."""
            client_results = {
                'client_id': client_id,
                'operations': 0,
                'successful': 0,
                'failed': 0,
                'errors': []
            }
            
            try:
                for i in range(operations_per_client):
                    operation_type = 'read' if i % 3 == 0 else 'write'
                    
                    if operation_type == 'read':
                        # Read operation
                        try:
                            data = self.db_engine.fetch(
                                "SELECT COUNT(*) as count FROM stress_test WHERE client_id = :client_id",
                                params={"client_id": client_id}
                            )
                            client_results['successful'] += 1
                        except Exception as e:
                            client_results['failed'] += 1
                            client_results['errors'].append(f"Read error: {e}")
                    
                    else:
                        # Write operation
                        try:
                            self.db_engine.execute(
                                "INSERT INTO stress_test (client_id, operation_type, value) VALUES (:client_id, :op_type, :value)",
                                params={
                                    "client_id": client_id,
                                    "op_type": operation_type,
                                    "value": f"client_{client_id}_op_{i}"
                                }
                            )
                            client_results['successful'] += 1
                        except Exception as e:
                            client_results['failed'] += 1
                            client_results['errors'].append(f"Write error: {e}")
                    
                    client_results['operations'] += 1
                    
                    # Small delay to simulate real-world usage
                    time.sleep(0.01)
                    
            except Exception as e:
                client_results['errors'].append(f"Client error: {e}")
            
            return client_results
        
        # Run concurrent clients
        
        start_time = time.time()
        
        with ThreadPoolExecutor(max_workers=num_clients) as executor:
            # Submit all client work
            future_to_client = {
                executor.submit(client_work, client_id): client_id 
                for client_id in range(num_clients)
            }
            
            # Collect results as they complete
            for future in as_completed(future_to_client):
                client_id = future_to_client[future]
                try:
                    client_result = future.result()
                    results['client_results'][client_id] = client_result
                    results['total_operations'] += client_result['operations']
                    results['successful_operations'] += client_result['successful']
                    results['failed_operations'] += client_result['failed']
                except Exception as e:
                    pass  # Client failed, but continue with other clients
        
        results['end_time'] = time.time()
        test_duration_actual = results['end_time'] - results['start_time']
        
        # Calculate performance metrics
        total_ops = results['total_operations']
        successful_ops = results['successful_operations']
        failed_ops = results['failed_operations']
        ops_per_sec = total_ops / test_duration_actual if test_duration_actual > 0 else 0
        success_rate = (successful_ops / total_ops * 100) if total_ops > 0 else 0
        
        # Verify database integrity
        final_count = self.db_engine.fetch("SELECT COUNT(*) as count FROM stress_test")
        
        # Performance assertions
        self.assertGreater(success_rate, 95.0, f"Success rate {success_rate:.2f}% is below 95% threshold")
        self.assertGreater(ops_per_sec, 10.0, f"Operations per second {ops_per_sec:.2f} is below 10 ops/sec threshold")
        self.assertLess(test_duration_actual, test_duration * 1.5, f"Test took too long: {test_duration_actual:.2f}s")
        
        # Verify all clients completed
        self.assertEqual(len(results['client_results']), num_clients, "Not all clients completed")
        
        # Verify database state
        self.assertGreater(final_count.data[0]['count'], 0, "No records were inserted during stress test")

    @pytest.mark.performance
    def test_concurrent_read_write_stress(self) -> None:
        """Stress test focusing on concurrent read/write operations."""
        # Test configuration
        num_readers = 8
        num_writers = 4
        operations_per_thread = 100
        
        # Create test table
        self.db_engine.execute("""
            CREATE TABLE IF NOT EXISTS rw_stress_test (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                thread_id INTEGER NOT NULL,
                thread_type TEXT NOT NULL,
                counter INTEGER DEFAULT 0,
                data TEXT,
                timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Initialize with some data
        for i in range(10):
            self.db_engine.execute(
                "INSERT INTO rw_stress_test (thread_id, thread_type, counter, data) VALUES (:tid, :type, :counter, :data)",
                params={"tid": i, "type": "init", "counter": i, "data": f"initial_data_{i}"}
            )
        
        results = {
            'readers': {'total': 0, 'successful': 0, 'failed': 0, 'errors': []},
            'writers': {'total': 0, 'successful': 0, 'failed': 0, 'errors': []},
            'start_time': time.time(),
            'end_time': None
        }
        
        def reader_work(thread_id: int) -> dict:
            """Reader thread that performs SELECT operations."""
            thread_results = {'operations': 0, 'successful': 0, 'failed': 0, 'errors': []}
            
            for i in range(operations_per_thread):
                try:
                    # Read operations
                    data = self.db_engine.fetch(
                        "SELECT * FROM rw_stress_test WHERE thread_id = :tid ORDER BY timestamp DESC LIMIT 5",
                        params={"tid": thread_id % 10}
                    )
                    thread_results['successful'] += 1
                except Exception as e:
                    thread_results['failed'] += 1
                    thread_results['errors'].append(f"Read error: {e}")
                thread_results['operations'] += 1
                time.sleep(0.001)  # Small delay
            
            return thread_results
        
        def writer_work(thread_id: int) -> dict:
            """Writer thread that performs INSERT/UPDATE operations."""
            thread_results = {'operations': 0, 'successful': 0, 'failed': 0, 'errors': []}
            
            for i in range(operations_per_thread):
                try:
                    # Alternate between INSERT and UPDATE
                    if i % 2 == 0:
                        self.db_engine.execute(
                            "INSERT INTO rw_stress_test (thread_id, thread_type, counter, data) VALUES (:tid, :type, :counter, :data)",
                            params={
                                "tid": thread_id,
                                "type": "writer",
                                "counter": i,
                                "data": f"writer_{thread_id}_data_{i}"
                            }
                        )
                    else:
                        self.db_engine.execute(
                            "UPDATE rw_stress_test SET counter = :counter, data = :data WHERE id = (SELECT id FROM rw_stress_test WHERE thread_id = :tid ORDER BY timestamp DESC LIMIT 1)",
                            params={
                                "tid": thread_id,
                                "counter": i,
                                "data": f"updated_data_{i}"
                            }
                        )
                    thread_results['successful'] += 1
                except Exception as e:
                    thread_results['failed'] += 1
                    thread_results['errors'].append(f"Write error: {e}")
                thread_results['operations'] += 1
                time.sleep(0.002)  # Slightly longer delay for writers
            
            return thread_results
        

        
        start_time = time.time()
        
        with ThreadPoolExecutor(max_workers=num_readers + num_writers) as executor:
            # Submit reader tasks
            reader_futures = [
                executor.submit(reader_work, i) for i in range(num_readers)
            ]
            
            # Submit writer tasks
            writer_futures = [
                executor.submit(writer_work, i) for i in range(num_writers)
            ]
            
            # Collect reader results
            for future in as_completed(reader_futures):
                result = future.result()
                results['readers']['total'] += result['operations']
                results['readers']['successful'] += result['successful']
                results['readers']['failed'] += result['failed']
                results['readers']['errors'].extend(result['errors'])
            
            # Collect writer results
            for future in as_completed(writer_futures):
                result = future.result()
                results['writers']['total'] += result['operations']
                results['writers']['successful'] += result['successful']
                results['writers']['failed'] += result['failed']
                results['writers']['errors'].extend(result['errors'])
        
        results['end_time'] = time.time()
        test_duration = results['end_time'] - results['start_time']
        
        # Calculate metrics
        total_ops = results['readers']['total'] + results['writers']['total']
        total_successful = results['readers']['successful'] + results['writers']['successful']
        ops_per_sec = total_ops / test_duration if test_duration > 0 else 0
        success_rate = (total_successful / total_ops * 100) if total_ops > 0 else 0
        
        # Verify final database state
        final_count = self.db_engine.fetch("SELECT COUNT(*) as count FROM rw_stress_test")
        
        # Assertions
        self.assertGreater(success_rate, 90.0, f"Success rate {success_rate:.2f}% is below 90% threshold")
        self.assertGreater(ops_per_sec, 50.0, f"Operations per second {ops_per_sec:.2f} is below 50 ops/sec threshold")
        self.assertGreater(final_count.data[0]['count'], 10, "Database should have more records after stress test")

    @pytest.mark.performance
    def test_high_concurrency_stress(self) -> None:
        """Stress test with high concurrency to test system limits."""
        # Test configuration - higher concurrency
        num_clients = 20
        operations_per_client = 25
        test_duration = 60  # seconds
        
        # Create test table
        self.db_engine.execute("""
            CREATE TABLE IF NOT EXISTS high_concurrency_test (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                client_id INTEGER NOT NULL,
                operation_id INTEGER NOT NULL,
                timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
                data TEXT
            )
        """)
        
        results = {
            'total_operations': 0,
            'successful_operations': 0,
            'failed_operations': 0,
            'start_time': time.time(),
            'end_time': None,
            'client_results': {}
        }
        
        def high_concurrency_work(client_id: int) -> dict:
            """High concurrency client work."""
            client_results = {
                'client_id': client_id,
                'operations': 0,
                'successful': 0,
                'failed': 0,
                'errors': []
            }
            
            try:
                for i in range(operations_per_client):
                    # Mix of operations
                    operation_type = i % 4  # 0=read, 1=insert, 2=update, 3=delete
                    
                    try:
                        if operation_type == 0:
                            # Read
                            self.db_engine.fetch(
                                "SELECT COUNT(*) as count FROM high_concurrency_test WHERE client_id = :client_id",
                                params={"client_id": client_id}
                            )
                        elif operation_type == 1:
                            # Insert
                            self.db_engine.execute(
                                "INSERT INTO high_concurrency_test (client_id, operation_id, data) VALUES (:client_id, :op_id, :data)",
                                params={
                                    "client_id": client_id,
                                    "op_id": i,
                                    "data": f"data_{client_id}_{i}"
                                }
                            )
                        elif operation_type == 2:
                            # Update
                            self.db_engine.execute(
                                "UPDATE high_concurrency_test SET data = :data WHERE id = (SELECT id FROM high_concurrency_test WHERE client_id = :client_id ORDER BY timestamp DESC LIMIT 1)",
                                params={
                                    "client_id": client_id,
                                    "data": f"updated_{client_id}_{i}"
                                }
                            )
                        else:
                            # Delete (only if records exist)
                            self.db_engine.execute(
                                "DELETE FROM high_concurrency_test WHERE client_id = :client_id AND operation_id = :op_id",
                                params={
                                    "client_id": client_id,
                                    "op_id": i
                                }
                            )
                        
                        client_results['successful'] += 1
                    except Exception as e:
                        client_results['failed'] += 1
                        client_results['errors'].append(f"Operation {operation_type} error: {e}")
                    
                    client_results['operations'] += 1
                    time.sleep(0.005)  # Very small delay
                    
            except Exception as e:
                client_results['errors'].append(f"Client error: {e}")
            
            return client_results
        

        
        start_time = time.time()
        
        with ThreadPoolExecutor(max_workers=num_clients) as executor:
            future_to_client = {
                executor.submit(high_concurrency_work, client_id): client_id 
                for client_id in range(num_clients)
            }
            
            for future in as_completed(future_to_client):
                client_id = future_to_client[future]
                try:
                    client_result = future.result()
                    results['client_results'][client_id] = client_result
                    results['total_operations'] += client_result['operations']
                    results['successful_operations'] += client_result['successful']
                    results['failed_operations'] += client_result['failed']
                except Exception as e:
                    pass  # Client failed, but continue with other clients
        
        results['end_time'] = time.time()
        test_duration_actual = results['end_time'] - results['start_time']
        
        # Calculate metrics
        total_ops = results['total_operations']
        successful_ops = results['successful_operations']
        failed_ops = results['failed_operations']
        ops_per_sec = total_ops / test_duration_actual if test_duration_actual > 0 else 0
        success_rate = (successful_ops / total_ops * 100) if total_ops > 0 else 0
        
        # Verify database state
        final_count = self.db_engine.fetch("SELECT COUNT(*) as count FROM high_concurrency_test")
        
        # Assertions for high concurrency
        self.assertGreater(success_rate, 85.0, f"Success rate {success_rate:.2f}% is below 85% threshold")
        self.assertGreater(ops_per_sec, 20.0, f"Operations per second {ops_per_sec:.2f} is below 20 ops/sec threshold")
        self.assertEqual(len(results['client_results']), num_clients, "Not all clients completed")


if __name__ == '__main__':
    unittest.main() 