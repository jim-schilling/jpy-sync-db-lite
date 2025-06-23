"""
Performance tests for DbEngine class.

Copyright (c) 2025, Jim Schilling

Please keep this header when you use this code.

This module is licensed under the MIT License.
"""
import unittest
import tempfile
import os
import time
import statistics
from typing import List, Dict, Any
from concurrent.futures import ThreadPoolExecutor, as_completed

# Try to import psutil for memory monitoring, but make it optional
try:
    import psutil
    PSUTIL_AVAILABLE = True
except ImportError:
    PSUTIL_AVAILABLE = False
    psutil = None

# Add parent directory to path for imports
import sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from jpy_sync_db_lite.db_engine import DbEngine


class TestDbEnginePerformance(unittest.TestCase):
    """Performance test cases for DbEngine class."""
    
    def setUp(self):
        """Set up test fixtures before each test method."""
        # Create temporary database file
        self.temp_db_fd, self.temp_db_path = tempfile.mkstemp(suffix='.db')
        os.close(self.temp_db_fd)
        self.database_url = f"sqlite:///{self.temp_db_path}"
        
        # Initialize DbEngine with test configuration
        self.db_engine = DbEngine(self.database_url, num_workers=1, debug=False)
        
        # Create test table
        self._create_test_table()
        
        # Performance tracking
        self.performance_results = {}
    
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
        """Create a test table for performance testing."""
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS performance_test (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            value INTEGER,
            data TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
        self.db_engine.execute(create_table_sql)
        
        # Create index for better query performance
        self.db_engine.execute("CREATE INDEX IF NOT EXISTS idx_name ON performance_test(name)")
        self.db_engine.execute("CREATE INDEX IF NOT EXISTS idx_value ON performance_test(value)")
    
    def _generate_test_data(self, count: int) -> List[Dict[str, Any]]:
        """Generate test data for performance testing."""
        return [
            {
                "name": f"TestUser{i}",
                "value": i,
                "data": f"Test data for user {i} with some additional content to simulate real data"
            }
            for i in range(count)
        ]
    
    def _measure_memory_usage(self) -> Dict[str, float]:
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
    
    def _print_performance_summary(self, test_name: str, results: Dict[str, Any]):
        """Print a formatted performance summary."""
        print(f"\n{'='*60}")
        print(f"Performance Test: {test_name}")
        print(f"{'='*60}")
        
        if 'latency_ms' in results:
            latencies = results['latency_ms']
            print(f"Latency Statistics (ms):")
            print(f"  Min: {min(latencies):.2f}")
            print(f"  Max: {max(latencies):.2f}")
            print(f"  Mean: {statistics.mean(latencies):.2f}")
            print(f"  Median: {statistics.median(latencies):.2f}")
            if len(latencies) > 1:
                print(f"  Std Dev: {statistics.stdev(latencies):.2f}")
        
        if 'throughput' in results:
            print(f"Throughput: {results['throughput']:.2f} operations/second")
        
        if 'memory_before' in results and 'memory_after' in results:
            mem_before = results['memory_before']
            mem_after = results['memory_after']
            if mem_before['rss_mb'] > 0 or mem_after['rss_mb'] > 0:  # Only show if psutil is available
                print(f"Memory Usage:")
                print(f"  Before: {mem_before['rss_mb']:.2f} MB RSS, {mem_before['vms_mb']:.2f} MB VMS")
                print(f"  After:  {mem_after['rss_mb']:.2f} MB RSS, {mem_after['vms_mb']:.2f} MB VMS")
                print(f"  Delta:  {mem_after['rss_mb'] - mem_before['rss_mb']:.2f} MB RSS")
    
    def test_single_insert_performance(self):
        """Test performance of single insert operations."""
        print("\nTesting single insert performance...")
        
        # Measure memory before
        memory_before = self._measure_memory_usage()
        
        # Test parameters
        num_operations = 1000
        latencies = []
        
        # Warm up
        for _ in range(10):
            self.db_engine.execute("INSERT INTO performance_test (name, value, data) VALUES (:name, :value, :data)",
                                 {"name": "warmup", "value": 0, "data": "warmup"})
        
        # Performance test
        start_time = time.time()
        for i in range(num_operations):
            op_start = time.time()
            
            self.db_engine.execute(
                "INSERT INTO performance_test (name, value, data) VALUES (:name, :value, :data)",
                {"name": f"PerfTest{i}", "value": i, "data": f"Performance test data {i}"}
            )
            
            op_end = time.time()
            latencies.append((op_end - op_start) * 1000)  # Convert to milliseconds
        
        end_time = time.time()
        total_time = end_time - start_time
        
        # Measure memory after
        memory_after = self._measure_memory_usage()
        
        # Calculate results
        if total_time == 0:
            throughput = float('inf')
        else:
            throughput = num_operations / total_time
        
        results = {
            'latency_ms': latencies,
            'throughput': throughput,
            'total_time': total_time,
            'memory_before': memory_before,
            'memory_after': memory_after
        }
        
        self.performance_results['single_insert'] = results
        self._print_performance_summary("Single Insert Operations", results)
        
        # Assertions for performance expectations
        self.assertGreater(throughput, 50)  # At least 50 ops/sec
        self.assertLess(statistics.mean(latencies), 100)  # Average latency < 100ms
    
    def test_bulk_insert_performance(self):
        """Test performance of bulk insert operations."""
        print("\nTesting bulk insert performance...")
        
        memory_before = self._measure_memory_usage()
        
        # Test parameters
        batch_sizes = [10, 50, 100, 500, 1000]
        results_by_batch = {}
        
        for batch_size in batch_sizes:
            print(f"  Testing batch size: {batch_size}")
            
            # Generate test data
            test_data = self._generate_test_data(batch_size)
            
            # Warm up
            warmup_data = self._generate_test_data(10)
            self.db_engine.execute(
                "INSERT INTO performance_test (name, value, data) VALUES (:name, :value, :data)",
                warmup_data
            )
            
            # Performance test
            latencies = []
            num_batches = max(1, 1000 // batch_size)  # Aim for ~1000 total operations
            
            start_time = time.time()
            for _ in range(num_batches):
                batch_data = self._generate_test_data(batch_size)
                
                op_start = time.time()
                self.db_engine.execute(
                    "INSERT INTO performance_test (name, value, data) VALUES (:name, :value, :data)",
                    batch_data
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
        
        # Print results
        print(f"\nBulk Insert Performance Summary:")
        for batch_size, results in results_by_batch.items():
            print(f"  Batch size {batch_size}: {results['throughput']:.2f} ops/sec, "
                  f"{results['avg_latency_per_record']:.2f} ms/record")
        
        # Assertions
        best_throughput = max(r['throughput'] for r in results_by_batch.values())
        self.assertGreater(best_throughput, 100)  # At least 100 ops/sec for bulk operations
    
    def test_select_performance(self):
        """Test performance of select operations."""
        print("\nTesting select performance...")
        
        # Insert test data first
        test_data = self._generate_test_data(10000)
        self.db_engine.execute(
            "INSERT INTO performance_test (name, value, data) VALUES (:name, :value, :data)",
            test_data
        )
        
        memory_before = self._measure_memory_usage()
        
        # Test different query types
        query_tests = [
            ("Simple SELECT", "SELECT * FROM performance_test LIMIT 100"),
            ("Filtered SELECT", "SELECT * FROM performance_test WHERE value > 5000"),
            ("Indexed SELECT", "SELECT * FROM performance_test WHERE name = 'TestUser5000'"),
            ("Aggregate SELECT", "SELECT COUNT(*), AVG(value) FROM performance_test"),
            ("Complex SELECT", """
                SELECT name, value, data 
                FROM performance_test 
                WHERE value BETWEEN 1000 AND 2000 
                AND name LIKE 'TestUser%'
                ORDER BY value DESC
                LIMIT 50
            """)
        ]
        
        results_by_query = {}
        
        for query_name, query in query_tests:
            print(f"  Testing: {query_name}")
            
            # Warm up
            for _ in range(5):
                self.db_engine.fetch(query)
            
            # Performance test
            latencies = []
            num_operations = 100
            
            start_time = time.time()
            for _ in range(num_operations):
                op_start = time.time()
                result = self.db_engine.fetch(query)
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
                'result_count': len(self.db_engine.fetch(query))
            }
        
        memory_after = self._measure_memory_usage()
        
        self.performance_results['select'] = {
            'by_query': results_by_query,
            'memory_before': memory_before,
            'memory_after': memory_after
        }
        
        # Print results
        print(f"\nSelect Performance Summary:")
        for query_name, results in results_by_query.items():
            print(f"  {query_name}: {results['throughput']:.2f} ops/sec, "
                  f"{statistics.mean(results['latency_ms']):.2f} ms avg")
        
        # Assertions
        simple_select_throughput = results_by_query["Simple SELECT"]['throughput']
        self.assertGreater(simple_select_throughput, 200)  # At least 200 ops/sec for simple selects
    
    def test_concurrent_operations_performance(self):
        """Test performance under concurrent load."""
        print("\nTesting concurrent operations performance...")
        
        # Insert base data
        test_data = self._generate_test_data(1000)
        self.db_engine.execute(
            "INSERT INTO performance_test (name, value, data) VALUES (:name, :value, :data)",
            test_data
        )
        
        memory_before = self._measure_memory_usage()
        
        # Test different concurrency levels
        concurrency_levels = [1, 2, 4, 8]
        results_by_concurrency = {}
        
        def worker_operation(worker_id: int, num_ops: int) -> List[float]:
            """Worker function for concurrent operations."""
            latencies = []
            for i in range(num_ops):
                op_start = time.time()
                
                # Mix of read and write operations
                if i % 3 == 0:  # Write operation
                    self.db_engine.execute(
                        "INSERT INTO performance_test (name, value, data) VALUES (:name, :value, :data)",
                        {"name": f"ConcurrentUser{worker_id}_{i}", "value": worker_id * 1000 + i, 
                         "data": f"Concurrent test data {worker_id}_{i}"}
                    )
                else:  # Read operation
                    self.db_engine.fetch(
                        "SELECT * FROM performance_test WHERE value > :min_value LIMIT 10",
                        {"min_value": worker_id * 100}
                    )
                
                op_end = time.time()
                latencies.append((op_end - op_start) * 1000)
            
            return latencies
        
        for concurrency in concurrency_levels:
            print(f"  Testing concurrency level: {concurrency}")
            
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
        
        # Print results
        print(f"\nConcurrent Operations Performance Summary:")
        for concurrency, results in results_by_concurrency.items():
            print(f"  Concurrency {concurrency}: {results['throughput']:.2f} ops/sec, "
                  f"{statistics.mean(results['latency_ms']):.2f} ms avg")
        
        # Assertions
        single_thread_throughput = results_by_concurrency[1]['throughput']
        self.assertGreater(single_thread_throughput, 50)  # At least 50 ops/sec under load
    
    def test_memory_efficiency(self):
        """Test memory efficiency over time."""
        print("\nTesting memory efficiency...")
        
        memory_before = self._measure_memory_usage()
        memory_samples = [memory_before]
        
        # Perform many operations to test memory growth
        num_operations = 5000
        
        for i in range(0, num_operations, 1000):
            # Insert batch
            batch_data = self._generate_test_data(1000)
            self.db_engine.execute(
                "INSERT INTO performance_test (name, value, data) VALUES (:name, :value, :data)",
                batch_data
            )
            
            # Query batch
            self.db_engine.fetch("SELECT * FROM performance_test LIMIT 100")
            
            # Measure memory every 1000 operations
            if i > 0:
                memory_samples.append(self._measure_memory_usage())
        
        memory_after = self._measure_memory_usage()
        
        # Calculate memory growth
        initial_memory = memory_samples[0]['rss_mb']
        final_memory = memory_after['rss_mb']
        memory_growth = final_memory - initial_memory
        
        # Calculate memory growth rate
        memory_growth_rate = memory_growth / (num_operations / 1000)  # MB per 1000 operations
        
        results = {
            'memory_samples': memory_samples,
            'initial_memory_mb': initial_memory,
            'final_memory_mb': final_memory,
            'memory_growth_mb': memory_growth,
            'memory_growth_rate_mb_per_1000_ops': memory_growth_rate,
            'total_operations': num_operations
        }
        
        self.performance_results['memory_efficiency'] = results
        
        print(f"\nMemory Efficiency Results:")
        if PSUTIL_AVAILABLE:
            print(f"  Initial memory: {initial_memory:.2f} MB")
            print(f"  Final memory: {final_memory:.2f} MB")
            print(f"  Memory growth: {memory_growth:.2f} MB")
            print(f"  Growth rate: {memory_growth_rate:.4f} MB per 1000 operations")
        else:
            print(f"  Memory monitoring not available (psutil not installed)")
        
        # Assertions (only if psutil is available)
        if PSUTIL_AVAILABLE:
            self.assertLess(memory_growth_rate, 10)  # Less than 10MB growth per 1000 operations
            self.assertLess(memory_growth, 100)  # Total growth less than 100MB
    
    def test_transaction_performance(self):
        """Test performance of transaction operations."""
        print("\nTesting transaction performance...")
        
        # Test different transaction sizes
        transaction_sizes = [10, 50, 100, 500]
        results_by_size = {}
        
        for size in transaction_sizes:
            print(f"  Testing transaction size: {size}")
            
            # Generate operations for transaction
            operations = []
            for i in range(size):
                operations.append({
                    'operation': 'execute',
                    'query': "INSERT INTO performance_test (name, value, data) VALUES (:name, :value, :data)",
                    'params': {"name": f"TxnUser{i}", "value": i, "data": f"Transaction test data {i}"}
                })
            
            # Warm up
            warmup_ops = operations[:5]
            self.db_engine.execute_transaction(warmup_ops)
            
            # Performance test
            latencies = []
            num_transactions = max(1, 100 // size)  # Aim for ~100 total operations
            
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
        
        # Print results
        print(f"\nTransaction Performance Summary:")
        for size, results in results_by_size.items():
            print(f"  Transaction size {size}: {results['throughput']:.2f} ops/sec, "
                  f"{results['avg_latency_per_operation']:.2f} ms/op")
        
        # Assertions
        best_throughput = max(r['throughput'] for r in results_by_size.values())
        self.assertGreater(best_throughput, 50)  # At least 50 ops/sec for transactions
    
    def test_worker_thread_scaling(self):
        """Test performance with different numbers of worker threads."""
        print("\nTesting worker thread scaling...")
        
        # Test different worker configurations
        worker_configs = [1, 2, 4]
        results_by_workers = {}
        
        for num_workers in worker_configs:
            print(f"  Testing with {num_workers} worker(s)")
            
            # Create new engine with specific worker count
            test_db_path = f"{self.temp_db_path}_workers_{num_workers}"
            test_engine = DbEngine(f"sqlite:///{test_db_path}", num_workers=num_workers, debug=False)
            
            # Create table
            test_engine.execute("""
                CREATE TABLE IF NOT EXISTS worker_test (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT NOT NULL,
                    value INTEGER
                )
            """)
            
            # Generate test data
            test_data = self._generate_test_data(1000)
            
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
                        {"name": f"WorkerTest{i}", "value": i}
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
                os.unlink(test_db_path)
            except PermissionError:
                pass
        
        self.performance_results['worker_scaling'] = results_by_workers
        
        # Print results
        print(f"\nWorker Thread Scaling Summary:")
        for num_workers, results in results_by_workers.items():
            print(f"  {num_workers} worker(s): {results['throughput']:.2f} ops/sec, "
                  f"{statistics.mean(results['latency_ms']):.2f} ms avg")
        
        # Assertions
        single_worker_throughput = results_by_workers[1]['throughput']
        self.assertGreater(single_worker_throughput, 30)  # At least 30 ops/sec with single worker
    
    def test_overall_performance_summary(self):
        """Generate overall performance summary."""
        print(f"\n{'='*80}")
        print(f"OVERALL PERFORMANCE SUMMARY")
        print(f"{'='*80}")
        
        if not self.performance_results:
            print("No performance tests have been run yet.")
            return
        
        # Calculate overall metrics
        all_throughputs = []
        all_latencies = []
        
        for test_name, results in self.performance_results.items():
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
        
        if all_throughputs:
            print(f"Overall Throughput Statistics:")
            print(f"  Min: {min(all_throughputs):.2f} ops/sec")
            print(f"  Max: {max(all_throughputs):.2f} ops/sec")
            print(f"  Mean: {statistics.mean(all_throughputs):.2f} ops/sec")
            print(f"  Median: {statistics.median(all_throughputs):.2f} ops/sec")
        
        if all_latencies:
            print(f"Overall Latency Statistics:")
            print(f"  Min: {min(all_latencies):.2f} ms")
            print(f"  Max: {max(all_latencies):.2f} ms")
            print(f"  Mean: {statistics.mean(all_latencies):.2f} ms")
            print(f"  Median: {statistics.median(all_latencies):.2f} ms")
        
        # Performance recommendations
        print(f"\nPerformance Recommendations:")
        if 'bulk_insert' in self.performance_results:
            bulk_results = self.performance_results['bulk_insert']['by_batch_size']
            best_batch_size = max(bulk_results.keys(), key=lambda k: bulk_results[k]['throughput'])
            print(f"  Optimal batch size for inserts: {best_batch_size}")
        
        if 'worker_scaling' in self.performance_results:
            worker_results = self.performance_results['worker_scaling']
            best_workers = max(worker_results.keys(), key=lambda k: worker_results[k]['throughput'])
            print(f"  Optimal worker threads: {best_workers}")
        
        if 'memory_efficiency' in self.performance_results and PSUTIL_AVAILABLE:
            mem_results = self.performance_results['memory_efficiency']
            growth_rate = mem_results['memory_growth_rate_mb_per_1000_ops']
            if growth_rate > 5:
                print(f"  Warning: High memory growth rate ({growth_rate:.2f} MB/1000 ops)")
            else:
                print(f"  Good memory efficiency ({growth_rate:.2f} MB/1000 ops)")
        elif 'memory_efficiency' in self.performance_results:
            print(f"  Memory efficiency: Not measured (psutil not available)")


if __name__ == '__main__':
    # Run performance tests
    unittest.main(verbosity=2) 