"""
Stress tests for DbEngine concurrent client handling.

Copyright (c) 2025, Jim Schilling

Please keep this header when you use this code.

This module is licensed under the MIT License.
"""

import os
import sys
import time
import unittest
import pytest
from concurrent.futures import ThreadPoolExecutor, as_completed

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from jpy_sync_db_lite.db_engine import DbEngine


class TestDbEngineStress(unittest.TestCase):
    """Stress tests for DbEngine concurrent client handling."""

    def setUp(self) -> None:
        """Set up test fixtures before each test method."""
        # Use file-based SQLite for stress tests to improve concurrency
        self.database_url = "sqlite:///stress_test.db"
        self.db_engine = DbEngine(
            self.database_url,
            timeout=30,
            check_same_thread=False,
            num_workers=4  # Use multiple workers for stress testing
        )
        
        # Explicitly enable WAL mode for better concurrency
        self.db_engine.execute("PRAGMA journal_mode=WAL")
        self.db_engine.execute("PRAGMA synchronous=NORMAL")
        self.db_engine.execute("PRAGMA busy_timeout=30000")

    def tearDown(self) -> None:
        """Clean up after each test method."""
        if hasattr(self, 'db_engine'):
            self.db_engine.shutdown()
        
        # Clean up the stress test database file and WAL files
        db_files = ['stress_test.db', 'stress_test.db-wal', 'stress_test.db-shm']
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
        print(f"\nStarting stress test with {num_clients} concurrent clients...")
        print(f"Each client will perform {operations_per_client} operations")
        print(f"Test duration target: {test_duration} seconds")
        
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
                    print(f"Client {client_id} failed with exception: {e}")
        
        results['end_time'] = time.time()
        test_duration_actual = results['end_time'] - results['start_time']
        
        # Calculate performance metrics
        total_ops = results['total_operations']
        successful_ops = results['successful_operations']
        failed_ops = results['failed_operations']
        ops_per_sec = total_ops / test_duration_actual if test_duration_actual > 0 else 0
        success_rate = (successful_ops / total_ops * 100) if total_ops > 0 else 0
        
        # Print results
        print(f"\n=== Stress Test Results ===")
        print(f"Test Duration: {test_duration_actual:.2f} seconds")
        print(f"Total Operations: {total_ops}")
        print(f"Successful Operations: {successful_ops}")
        print(f"Failed Operations: {failed_ops}")
        print(f"Success Rate: {success_rate:.2f}%")
        print(f"Operations per Second: {ops_per_sec:.2f}")
        print(f"Concurrent Clients: {num_clients}")
        
        # Print per-client results
        print(f"\n=== Per-Client Results ===")
        for client_id, client_result in results['client_results'].items():
            client_success_rate = (client_result['successful'] / client_result['operations'] * 100) if client_result['operations'] > 0 else 0
            print(f"Client {client_id}: {client_result['operations']} ops, {client_result['successful']} success, {client_success_rate:.1f}% success rate")
            if client_result['errors']:
                print(f"  Errors: {len(client_result['errors'])}")
                # Print the first error for diagnosis
                print(f"    First error: {client_result['errors'][0]}")
        
        # Verify database integrity
        final_count = self.db_engine.fetch("SELECT COUNT(*) as count FROM stress_test")
        print(f"\nFinal database record count: {final_count.data[0]['count']}")
        
        # Performance assertions
        self.assertGreater(success_rate, 95.0, f"Success rate {success_rate:.2f}% is below 95% threshold")
        self.assertGreater(ops_per_sec, 10.0, f"Operations per second {ops_per_sec:.2f} is below 10 ops/sec threshold")
        self.assertLess(test_duration_actual, test_duration * 1.5, f"Test took too long: {test_duration_actual:.2f}s")
        
        # Verify all clients completed
        self.assertEqual(len(results['client_results']), num_clients, "Not all clients completed")
        
        # Verify database state
        self.assertGreater(final_count.data[0]['count'], 0, "No records were inserted during stress test")
        
        print(f"\n✅ Stress test completed successfully!")
        print(f"   Success Rate: {success_rate:.2f}%")
        print(f"   Throughput: {ops_per_sec:.2f} ops/sec")
        print(f"   Duration: {test_duration_actual:.2f}s")

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
        
        print(f"\nStarting read/write stress test...")
        print(f"Readers: {num_readers}, Writers: {num_writers}")
        print(f"Operations per thread: {operations_per_thread}")
        
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
        
        print(f"\n=== Read/Write Stress Test Results ===")
        print(f"Test Duration: {test_duration:.2f} seconds")
        print(f"Total Operations: {total_ops}")
        print(f"Read Operations: {results['readers']['total']} ({results['readers']['successful']} successful)")
        print(f"Write Operations: {results['writers']['total']} ({results['writers']['successful']} successful)")
        print(f"Overall Success Rate: {success_rate:.2f}%")
        print(f"Operations per Second: {ops_per_sec:.2f}")
        
        # Show first few errors for diagnosis
        if results['readers']['errors']:
            print(f"\nFirst few read errors:")
            for i, error in enumerate(results['readers']['errors'][:3]):
                print(f"  {i+1}. {error}")
        
        if results['writers']['errors']:
            print(f"\nFirst few write errors:")
            for i, error in enumerate(results['writers']['errors'][:3]):
                print(f"  {i+1}. {error}")
        
        # Verify final database state
        final_count = self.db_engine.fetch("SELECT COUNT(*) as count FROM rw_stress_test")
        print(f"Final database record count: {final_count.data[0]['count']}")
        
        # Assertions
        self.assertGreater(success_rate, 90.0, f"Success rate {success_rate:.2f}% is below 90% threshold")
        self.assertGreater(ops_per_sec, 50.0, f"Operations per second {ops_per_sec:.2f} is below 50 ops/sec threshold")
        self.assertGreater(final_count.data[0]['count'], 10, "Database should have more records after stress test")
        
        print(f"\n✅ Read/Write stress test completed successfully!")
        print(f"   Success Rate: {success_rate:.2f}%")
        print(f"   Throughput: {ops_per_sec:.2f} ops/sec")
        print(f"   Duration: {test_duration:.2f}s")

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
        
        print(f"\nStarting high concurrency stress test...")
        print(f"Concurrent clients: {num_clients}")
        print(f"Operations per client: {operations_per_client}")
        print(f"Total operations: {num_clients * operations_per_client}")
        
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
                    print(f"Client {client_id} failed with exception: {e}")
        
        results['end_time'] = time.time()
        test_duration_actual = results['end_time'] - results['start_time']
        
        # Calculate metrics
        total_ops = results['total_operations']
        successful_ops = results['successful_operations']
        failed_ops = results['failed_operations']
        ops_per_sec = total_ops / test_duration_actual if test_duration_actual > 0 else 0
        success_rate = (successful_ops / total_ops * 100) if total_ops > 0 else 0
        
        print(f"\n=== High Concurrency Stress Test Results ===")
        print(f"Test Duration: {test_duration_actual:.2f} seconds")
        print(f"Total Operations: {total_ops}")
        print(f"Successful Operations: {successful_ops}")
        print(f"Failed Operations: {failed_ops}")
        print(f"Success Rate: {success_rate:.2f}%")
        print(f"Operations per Second: {ops_per_sec:.2f}")
        print(f"Concurrent Clients: {num_clients}")
        
        # Verify database state
        final_count = self.db_engine.fetch("SELECT COUNT(*) as count FROM high_concurrency_test")
        print(f"Final database record count: {final_count.data[0]['count']}")
        
        # Assertions for high concurrency
        self.assertGreater(success_rate, 85.0, f"Success rate {success_rate:.2f}% is below 85% threshold")
        self.assertGreater(ops_per_sec, 20.0, f"Operations per second {ops_per_sec:.2f} is below 20 ops/sec threshold")
        self.assertEqual(len(results['client_results']), num_clients, "Not all clients completed")
        
        print(f"\n✅ High concurrency stress test completed successfully!")
        print(f"   Success Rate: {success_rate:.2f}%")
        print(f"   Throughput: {ops_per_sec:.2f} ops/sec")
        print(f"   Duration: {test_duration_actual:.2f}s")


if __name__ == '__main__':
    unittest.main() 