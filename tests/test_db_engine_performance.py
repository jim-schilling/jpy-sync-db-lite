"""
Performance tests for DbEngine.

Copyright (c) 2025, Jim Schilling

Please keep this header when you use this code.

This module is licensed under the MIT License.
"""

import os
import statistics
import sys
import tempfile
import time
import unittest
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any

# Try to import psutil for memory monitoring, but make it optional
try:
    import psutil
    PSUTIL_AVAILABLE = True
except ImportError:
    PSUTIL_AVAILABLE = False
    psutil = None

# Add parent directory to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from jpy_sync_db_lite.db_engine import DbEngine


class TestDbEnginePerformance(unittest.TestCase):
    """Performance tests for DbEngine."""

    def setUp(self) -> None:
        """Set up test database for performance tests."""
        self.database_url = "sqlite:///:memory:"
        self.db_engine = DbEngine(self.database_url, num_workers=2, debug=False)
        self.performance_results = {}
        self._create_test_table()

    def tearDown(self) -> None:
        """Clean up after performance tests."""
        if hasattr(self, 'db_engine'):
            self.db_engine.shutdown()

    def _create_test_table(self) -> None:
        """Create a test table for performance testing."""
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS perf_test (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            value INTEGER,
            data TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
        self.db_engine.execute(create_table_sql)

    def _generate_test_data(self, count: int) -> list[dict[str, Any]]:
        """Generate test data for performance tests."""
        return [
            {
                "name": f"PerfUser{i}",
                "value": i,
                "data": f"Performance data {i}"
            }
            for i in range(count)
        ]

    def _measure_memory_usage(self) -> dict[str, float]:
        """Measure current memory usage."""
        if PSUTIL_AVAILABLE:
            process = psutil.Process()
            memory_info = process.memory_info()
            return {
                'rss_mb': memory_info.rss / 1024 / 1024,  # Resident Set Size in MB
                'vms_mb': memory_info.vms / 1024 / 1024,  # Virtual Memory Size in MB
            }
        else:
            return {'rss_mb': 0.0, 'vms_mb': 0.0}
    
    def test_single_insert_performance(self) -> None:
        """Test single insert performance."""
        num_operations = 100
        latencies = []
        for i in range(num_operations):
            op_start = time.time()
            result = self.db_engine.execute(
                "INSERT INTO perf_test (name, value, data) VALUES (:name, :value, :data)",
                params={"name": f"User{i}", "value": i, "data": f"Data {i}"}
            )
            self.assertEqual(result.rowcount, 1)
            self.assertTrue(result.result)
            op_end = time.time()
            latencies.append((op_end - op_start) * 1000)
        avg_latency = statistics.mean(latencies)
        self.assertLess(avg_latency, 100)

    def test_bulk_insert_performance(self):
        """Test performance of bulk insert operations."""

        memory_before = self._measure_memory_usage()

        # Test parameters
        batch_sizes = [10, 50, 100, 250]
        results_by_batch = {}

        for batch_size in batch_sizes:

            # Generate test data
            self._generate_test_data(batch_size)

            # Warm up
            warmup_data = self._generate_test_data(10)
            self.db_engine.execute(
                "INSERT INTO perf_test (name, value, data) VALUES (:name, :value, :data)",
                params=warmup_data
            )

            # Performance test
            latencies = []
            num_batches = max(1, 250 // batch_size)

            start_time = time.time()
            for _ in range(num_batches):
                batch_data = self._generate_test_data(batch_size)

                op_start = time.time()
                self.db_engine.execute(
                    "INSERT INTO perf_test (name, value, data) VALUES (:name, :value, :data)",
                    params=batch_data
                )
                op_end = time.time()

                latencies.append((op_end - op_start) * 1000)

            end_time = time.time()
            total_time = end_time - start_time
            total_operations = num_batches * batch_size
            if total_time == 0:
                throughput = float('inf')
            else:
                throughput = total_operations / total_time

            results_by_batch[batch_size] = {
                'latency_ms': latencies,
                'throughput': throughput,
                'total_time': total_time,
                'avg_latency_per_record': statistics.mean(latencies) / batch_size
            }

        memory_after = self._measure_memory_usage()

        self.performance_results['bulk_insert'] = {
            'by_batch_size': results_by_batch,
            'memory_before': memory_before,
            'memory_after': memory_after
        }



        # Assertions
        best_throughput = max(r['throughput'] for r in results_by_batch.values())
        self.assertGreater(best_throughput, 100)  # At least 100 ops/sec for bulk operations

    def test_batch_performance(self):
        """Test performance of the batch method for bulk inserts and mixed statements."""


        memory_before = self._measure_memory_usage()
        batch_sizes = [10, 50, 100, 250]
        results_by_batch = {}

        for batch_size in batch_sizes:
            # Generate batch SQL for inserts
            statements = [
                "CREATE TABLE IF NOT EXISTS batch_perf_test (id INTEGER PRIMARY KEY, name TEXT, value INTEGER);"
            ]
            for i in range(batch_size):
                statements.append(f"INSERT INTO batch_perf_test (name, value) VALUES ('User{i}', {i});")
            statements.append("SELECT COUNT(*) as count FROM batch_perf_test;")
            batch_sql = "\n".join(statements)

            # Warm up
            self.db_engine.batch(batch_sql)

            # Performance test
            latencies = []
            num_batches = max(1, 250 // batch_size)
            start_time = time.time()
            for _ in range(num_batches):
                # Regenerate batch SQL to avoid PK conflicts
                statements = [
                    "CREATE TABLE IF NOT EXISTS batch_perf_test (id INTEGER PRIMARY KEY, name TEXT, value INTEGER);"
                ]
                offset = _ * batch_size
                for i in range(batch_size):
                    statements.append(f"INSERT INTO batch_perf_test (name, value) VALUES ('User{offset}_{i}', {offset + i});")
                statements.append("SELECT COUNT(*) as count FROM batch_perf_test;")
                batch_sql = "\n".join(statements)

                op_start = time.time()
                self.db_engine.batch(batch_sql)
                op_end = time.time()
                latencies.append((op_end - op_start) * 1000)
            end_time = time.time()
            total_time = end_time - start_time
            total_operations = num_batches * batch_size
            throughput = total_operations / total_time if total_time > 0 else float('inf')
            results_by_batch[batch_size] = {
                'latency_ms': latencies,
                'throughput': throughput,
                'total_time': total_time,
                'avg_latency_per_record': statistics.mean(latencies) / batch_size
            }

        memory_after = self._measure_memory_usage()
        self.performance_results['batch'] = {
            'by_batch_size': results_by_batch,
            'memory_before': memory_before,
            'memory_after': memory_after
        }

        # Assertions
        best_throughput = max(r['throughput'] for r in results_by_batch.values())
        self.assertGreater(best_throughput, 100)  # At least 100 ops/sec for batch operations

    def test_select_performance(self):
        """Test performance of select operations."""


        # Insert test data first
        test_data = self._generate_test_data(250)
        self.db_engine.execute(
            "INSERT INTO perf_test (name, value, data) VALUES (:name, :value, :data)",
            params=test_data
        )

        memory_before = self._measure_memory_usage()

        # Test different query types
        query_tests = [
            ("Simple SELECT", "SELECT * FROM perf_test LIMIT 100"),
            ("Filtered SELECT", "SELECT * FROM perf_test WHERE value > 125"),
            ("Indexed SELECT", "SELECT * FROM perf_test WHERE name = 'PerfUser125'"),
            ("Aggregate SELECT", "SELECT COUNT(*), AVG(value) FROM perf_test"),
            ("Complex SELECT", """
                SELECT name, value, data
                FROM perf_test
                WHERE value BETWEEN 50 AND 200
                AND name LIKE 'PerfUser%'
                ORDER BY value DESC
                LIMIT 50
            """)
        ]

        results_by_query = {}

        for query_name, query in query_tests:

            # Warm up
            for _ in range(5):
                self.db_engine.fetch(query)

            # Performance test
            latencies = []
            num_operations = 100

            start_time = time.time()
            for _ in range(num_operations):
                op_start = time.time()
                self.db_engine.fetch(query)
                op_end = time.time()
                latencies.append((op_end - op_start) * 1000)

            end_time = time.time()
            total_time = end_time - start_time
            if total_time == 0:
                throughput = float('inf')
            else:
                throughput = num_operations / total_time

            results_by_query[query_name] = {
                'latency_ms': latencies,
                'throughput': throughput,
                'total_time': total_time,
                'result_count': len(self.db_engine.fetch(query).data)
            }

        memory_after = self._measure_memory_usage()

        self.performance_results['select'] = {
            'by_query': results_by_query,
            'memory_before': memory_before,
            'memory_after': memory_after
        }



        # Assertions
        simple_select_throughput = results_by_query["Simple SELECT"]['throughput']
        self.assertGreater(simple_select_throughput, 200)  # At least 200 ops/sec for simple selects

    def test_concurrent_operations_performance(self):
        """Test performance under concurrent load."""


        # Insert base data
        test_data = self._generate_test_data(250)
        self.db_engine.execute(
            "INSERT INTO perf_test (name, value, data) VALUES (:name, :value, :data)",
            params=test_data
        )

        memory_before = self._measure_memory_usage()

        # Test different concurrency levels
        concurrency_levels = [1, 2, 4, 8]
        results_by_concurrency = {}

        def worker_operation(worker_id: int, num_ops: int) -> list[float]:
            """Worker function for concurrent operations."""
            latencies = []
            for i in range(num_ops):
                op_start = time.time()

                # Mix of read and write operations
                if i % 3 == 0:  # Write operation
                    self.db_engine.execute(
                        "INSERT INTO perf_test (name, value, data) VALUES (:name, :value, :data)",
                        params={"name": f"ConcurrentUser{worker_id}_{i}", "value": worker_id * 1000 + i,
                                "data": f"Concurrent test data {worker_id}_{i}"}
                    )
                else:  # Read operation
                    self.db_engine.fetch(
                        "SELECT * FROM perf_test WHERE value > :min_value LIMIT 10",
                        params={"min_value": worker_id * 100}
                    )

                op_end = time.time()
                latencies.append((op_end - op_start) * 1000)

            return latencies

        for concurrency in concurrency_levels:

            # Warm up
            with ThreadPoolExecutor(max_workers=concurrency) as executor:
                futures = [executor.submit(worker_operation, i, 5) for i in range(concurrency)]
                for future in as_completed(futures):
                    future.result()

            # Performance test
            num_ops_per_worker = 50
            all_latencies = []

            start_time = time.time()
            with ThreadPoolExecutor(max_workers=concurrency) as executor:
                futures = [executor.submit(worker_operation, i, num_ops_per_worker)
                          for i in range(concurrency)]

                for future in as_completed(futures):
                    all_latencies.extend(future.result())

            end_time = time.time()
            total_time = end_time - start_time
            total_operations = concurrency * num_ops_per_worker
            if total_time == 0:
                throughput = float('inf')
            else:
                throughput = total_operations / total_time

            results_by_concurrency[concurrency] = {
                'latency_ms': all_latencies,
                'throughput': throughput,
                'total_time': total_time,
                'concurrency_level': concurrency
            }

        memory_after = self._measure_memory_usage()

        self.performance_results['concurrent'] = {
            'by_concurrency': results_by_concurrency,
            'memory_before': memory_before,
            'memory_after': memory_after
        }



        # Assertions
        single_thread_throughput = results_by_concurrency[1]['throughput']
        self.assertGreater(single_thread_throughput, 50)  # At least 50 ops/sec under load

    def test_memory_efficiency(self):
        """Test memory efficiency over time."""


        memory_before = self._measure_memory_usage()
        memory_samples = [memory_before]

        # Perform many operations to test memory growth
        num_operations = 250

        for i in range(0, num_operations, 250):
            # Insert batch
            batch_data = self._generate_test_data(250)
            self.db_engine.execute(
                "INSERT INTO perf_test (name, value, data) VALUES (:name, :value, :data)",
                params=batch_data
            )

            # Query batch
            self.db_engine.fetch("SELECT * FROM perf_test LIMIT 100")

            # Measure memory every 250 operations
            if i > 0:
                memory_samples.append(self._measure_memory_usage())

        memory_after = self._measure_memory_usage()

        # Calculate memory growth
        initial_memory = memory_samples[0]['rss_mb']
        final_memory = memory_after['rss_mb']
        memory_growth = final_memory - initial_memory

        # Calculate memory growth rate
        memory_growth_rate = memory_growth / (num_operations / 250)

        results = {
            'memory_samples': memory_samples,
            'initial_memory_mb': initial_memory,
            'final_memory_mb': final_memory,
            'memory_growth_mb': memory_growth,
            'memory_growth_rate_mb_per_250_ops': memory_growth_rate,
            'total_operations': num_operations
        }

        self.performance_results['memory_efficiency'] = results



        # Assertions (only if psutil is available)
        if PSUTIL_AVAILABLE:
            self.assertLess(memory_growth_rate, 10)
            self.assertLess(memory_growth, 100)

    def test_transaction_performance(self):
        """Test performance of transaction operations."""


        # Test different transaction sizes
        transaction_sizes = [10, 50, 100, 250]
        results_by_size = {}

        for size in transaction_sizes:

            # Generate operations for transaction
            operations = []
            for i in range(size):
                operations.append({
                    'operation': 'execute',
                    'query': "INSERT INTO perf_test (name, value, data) VALUES (:name, :value, :data)",
                    'params': {"name": f"TxnUser{i}", "value": i, "data": f"Transaction test data {i}"}
                })

            # Warm up
            warmup_ops = operations[:5]
            self.db_engine.execute_transaction(warmup_ops)

            # Performance test
            latencies = []
            num_transactions = max(1, 250 // size)

            start_time = time.time()
            for _ in range(num_transactions):
                op_start = time.time()
                self.db_engine.execute_transaction(operations)
                op_end = time.time()
                latencies.append((op_end - op_start) * 1000)

            end_time = time.time()
            total_time = end_time - start_time
            total_operations = num_transactions * size
            if total_time == 0:
                throughput = float('inf')
            else:
                throughput = total_operations / total_time

            results_by_size[size] = {
                'latency_ms': latencies,
                'throughput': throughput,
                'total_time': total_time,
                'avg_latency_per_operation': statistics.mean(latencies) / size
            }

        self.performance_results['transaction'] = {
            'by_size': results_by_size
        }



        # Assertions
        best_throughput = max(r['throughput'] for r in results_by_size.values())
        self.assertGreater(best_throughput, 50)  # At least 50 ops/sec for transactions

    def test_worker_thread_scaling(self):
        """Test performance with different numbers of worker threads."""


        # Test different worker configurations
        worker_configs = [1, 2, 4]
        results_by_workers = {}

        for num_workers in worker_configs:

            # Create new engine with specific worker count
            temp_fd, temp_db_path = tempfile.mkstemp(suffix=f'_workers_{num_workers}.db')
            os.close(temp_fd)
            test_engine = DbEngine(f"sqlite:///{temp_db_path}", num_workers=num_workers, debug=False)

            # Create table
            test_engine.execute("""
                CREATE TABLE IF NOT EXISTS worker_test (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT NOT NULL,
                    value INTEGER
                )
            """)

            # Generate test data
            self._generate_test_data(250)

            # Performance test
            latencies = []
            num_operations = 100

            start_time = time.time()
            for i in range(num_operations):
                op_start = time.time()

                # Mix of operations
                if i % 2 == 0:
                    test_engine.execute(
                        "INSERT INTO worker_test (name, value) VALUES (:name, :value)",
                        params={"name": f"WorkerTest{i}", "value": i}
                    )
                else:
                    test_engine.fetch("SELECT * FROM worker_test LIMIT 10")

                op_end = time.time()
                latencies.append((op_end - op_start) * 1000)

            end_time = time.time()
            total_time = end_time - start_time
            if total_time == 0:
                throughput = float('inf')
            else:
                throughput = num_operations / total_time

            results_by_workers[num_workers] = {
                'latency_ms': latencies,
                'throughput': throughput,
                'total_time': total_time,
                'num_workers': num_workers
            }

            # Cleanup
            test_engine.shutdown()
            time.sleep(0.1)
            try:
                os.unlink(temp_db_path)
            except PermissionError:
                pass

        self.performance_results['worker_scaling'] = results_by_workers



        # Assertions
        single_worker_throughput = results_by_workers[1]['throughput']
        self.assertGreater(single_worker_throughput, 50)  # At least 50 ops/sec with single worker

    def test_overall_performance_summary(self):
        """Generate overall performance summary."""

        if not self.performance_results:
            return

        # Calculate overall metrics
        all_throughputs = []
        all_latencies = []

        for _test_name, results in self.performance_results.items():
            if isinstance(results, dict) and 'throughput' in results:
                all_throughputs.append(results['throughput'])
                if 'latency_ms' in results:
                    all_latencies.extend(results['latency_ms'])
            elif isinstance(results, dict):
                # Handle nested results
                for sub_results in results.values():
                    if isinstance(sub_results, dict) and 'throughput' in sub_results:
                        all_throughputs.append(sub_results['throughput'])
                        if 'latency_ms' in sub_results:
                            all_latencies.extend(sub_results['latency_ms'])




if __name__ == '__main__':
    # Run performance tests
    unittest.main(verbosity=2)
