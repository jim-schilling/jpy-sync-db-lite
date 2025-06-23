#!/usr/bin/env python3
"""
Standalone benchmark script for DbEngine performance testing.

Copyright (c) 2025, Jim Schilling

Please keep this header when you use this code.

This module is licensed under the MIT License.
"""
import tempfile
import os
import time
import statistics
import argparse
from typing import List, Dict, Any

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


class DbEngineBenchmark:
    """Benchmark class for DbEngine performance testing."""
    
    def __init__(self, database_url: str = None, num_workers: int = 1):
        """Initialize benchmark with database configuration."""
        if database_url is None:
            # Create temporary database
            temp_db_fd, temp_db_path = tempfile.mkstemp(suffix='.db')
            os.close(temp_db_fd)
            self.database_url = f"sqlite:///{temp_db_path}"
            self.temp_db_path = temp_db_path
        else:
            self.database_url = database_url
            self.temp_db_path = None
        
        self.num_workers = num_workers
        
        self.db_engine = DbEngine(self.database_url, num_workers=num_workers, debug=False)
        
        self._setup_database()
    
    def _setup_database(self):
        """Set up test database with tables and indexes."""
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS benchmark_test (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            value INTEGER,
            data TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
        self.db_engine.execute(create_table_sql)
        
        # Create indexes for better performance
        self.db_engine.execute("CREATE INDEX IF NOT EXISTS idx_name ON benchmark_test(name)")
        self.db_engine.execute("CREATE INDEX IF NOT EXISTS idx_value ON benchmark_test(value)")
    
    def _generate_test_data(self, count: int) -> List[Dict[str, Any]]:
        """Generate test data for benchmarking."""
        return [
            {
                "name": f"BenchmarkUser{i}",
                "value": i,
                "data": f"Benchmark data for user {i} with additional content"
            }
            for i in range(count)
        ]
    
    def benchmark_single_inserts(self, num_operations: int = 1000) -> Dict[str, Any]:
        """Benchmark single insert operations."""
        print(f"Benchmarking {num_operations} single insert operations...")
        
        # Warm up
        for _ in range(10):
            self.db_engine.execute(
                "INSERT INTO benchmark_test (name, value, data) VALUES (:name, :value, :data)",
                {"name": "warmup", "value": 0, "data": "warmup"}
            )
        
        # Benchmark
        latencies = []
        start_time = time.time()
        
        for i in range(num_operations):
            op_start = time.time()
            self.db_engine.execute(
                "INSERT INTO benchmark_test (name, value, data) VALUES (:name, :value, :data)",
                {"name": f"Benchmark{i}", "value": i, "data": f"Benchmark data {i}"}
            )
            op_end = time.time()
            latencies.append((op_end - op_start) * 1000)
        
        end_time = time.time()
        total_time = end_time - start_time
        throughput = num_operations / total_time
        
        return {
            'operation': 'single_insert',
            'num_operations': num_operations,
            'total_time': total_time,
            'throughput': throughput,
            'latency_ms': latencies,
            'avg_latency': statistics.mean(latencies),
            'min_latency': min(latencies),
            'max_latency': max(latencies),
            'median_latency': statistics.median(latencies)
        }
    
    def benchmark_bulk_inserts(self, batch_sizes: List[int] = None) -> Dict[str, Any]:
        """Benchmark bulk insert operations with different batch sizes."""
        if batch_sizes is None:
            batch_sizes = [10, 50, 100, 500, 1000]
        
        print(f"Benchmarking bulk insert operations with batch sizes: {batch_sizes}")
        
        results = {}
        
        for batch_size in batch_sizes:
            print(f"  Testing batch size: {batch_size}")
            
            # Generate test data
            test_data = self._generate_test_data(batch_size)
            
            # Warm up
            warmup_data = self._generate_test_data(10)
            self.db_engine.execute(
                "INSERT INTO benchmark_test (name, value, data) VALUES (:name, :value, :data)",
                warmup_data
            )
            
            # Benchmark
            latencies = []
            num_batches = max(1, 1000 // batch_size)
            
            start_time = time.time()
            for _ in range(num_batches):
                batch_data = self._generate_test_data(batch_size)
                
                op_start = time.time()
                self.db_engine.execute(
                    "INSERT INTO benchmark_test (name, value, data) VALUES (:name, :value, :data)",
                    batch_data
                )
                op_end = time.time()
                latencies.append((op_end - op_start) * 1000)
            
            end_time = time.time()
            total_time = end_time - start_time
            total_operations = num_batches * batch_size
            throughput = total_operations / total_time
            
            results[batch_size] = {
                'batch_size': batch_size,
                'num_batches': num_batches,
                'total_operations': total_operations,
                'total_time': total_time,
                'throughput': throughput,
                'latency_ms': latencies,
                'avg_latency_per_batch': statistics.mean(latencies),
                'avg_latency_per_record': statistics.mean(latencies) / batch_size
            }
        
        return {
            'operation': 'bulk_insert',
            'batch_sizes': batch_sizes,
            'results': results
        }
    
    def benchmark_selects(self, num_operations: int = 100) -> Dict[str, Any]:
        """Benchmark select operations."""
        print(f"Benchmarking {num_operations} select operations...")
        
        # Insert test data first
        test_data = self._generate_test_data(10000)
        self.db_engine.execute(
            "INSERT INTO benchmark_test (name, value, data) VALUES (:name, :value, :data)",
            test_data
        )
        
        # Test different query types
        query_tests = [
            ("simple_select", "SELECT * FROM benchmark_test LIMIT 100"),
            ("filtered_select", "SELECT * FROM benchmark_test WHERE value > 5000"),
            ("indexed_select", "SELECT * FROM benchmark_test WHERE name = 'BenchmarkUser5000'"),
            ("aggregate_select", "SELECT COUNT(*), AVG(value) FROM benchmark_test"),
            ("complex_select", """
                SELECT name, value, data 
                FROM benchmark_test 
                WHERE value BETWEEN 1000 AND 2000 
                AND name LIKE 'BenchmarkUser%'
                ORDER BY value DESC
                LIMIT 50
            """)
        ]
        
        results = {}
        
        for query_name, query in query_tests:
            print(f"  Testing: {query_name}")
            
            # Warm up
            for _ in range(5):
                self.db_engine.fetch(query)
            
            # Benchmark
            latencies = []
            start_time = time.time()
            
            for _ in range(num_operations):
                op_start = time.time()
                result = self.db_engine.fetch(query)
                op_end = time.time()
                latencies.append((op_end - op_start) * 1000)
            
            end_time = time.time()
            total_time = end_time - start_time
            throughput = num_operations / total_time
            
            results[query_name] = {
                'query': query,
                'num_operations': num_operations,
                'total_time': total_time,
                'throughput': throughput,
                'latency_ms': latencies,
                'avg_latency': statistics.mean(latencies),
                'min_latency': min(latencies),
                'max_latency': max(latencies),
                'median_latency': statistics.median(latencies),
                'result_count': len(self.db_engine.fetch(query))
            }
        
        return {
            'operation': 'select',
            'num_operations': num_operations,
            'results': results
        }
    
    def benchmark_worker_scaling(self, worker_configs: List[int] = None) -> Dict[str, Any]:
        """Benchmark performance with different worker thread configurations."""
        if worker_configs is None:
            worker_configs = [1, 2, 4]
        
        print(f"Benchmarking worker thread scaling with configurations: {worker_configs}")
        
        results = {}
        
        for num_workers in worker_configs:
            print(f"  Testing with {num_workers} worker(s)")
            
            # Create new engine with specific worker count
            test_db_path = f"benchmark_workers_{num_workers}.db"
            test_engine = DbEngine(f"sqlite:///{test_db_path}", num_workers=num_workers, debug=False)
            
            # Create table
            test_engine.execute("""
                CREATE TABLE IF NOT EXISTS worker_benchmark (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT NOT NULL,
                    value INTEGER
                )
            """)
            
            # Generate test data
            test_data = self._generate_test_data(1000)
            
            # Benchmark
            latencies = []
            num_operations = 100
            
            start_time = time.time()
            for i in range(num_operations):
                op_start = time.time()
                
                # Mix of operations
                if i % 2 == 0:
                    test_engine.execute(
                        "INSERT INTO worker_benchmark (name, value) VALUES (:name, :value)",
                        {"name": f"WorkerBenchmark{i}", "value": i}
                    )
                else:
                    test_engine.fetch("SELECT * FROM worker_benchmark LIMIT 10")
                
                op_end = time.time()
                latencies.append((op_end - op_start) * 1000)
            
            end_time = time.time()
            total_time = end_time - start_time
            throughput = num_operations / total_time
            
            results[num_workers] = {
                'num_workers': num_workers,
                'num_operations': num_operations,
                'total_time': total_time,
                'throughput': throughput,
                'latency_ms': latencies,
                'avg_latency': statistics.mean(latencies),
                'min_latency': min(latencies),
                'max_latency': max(latencies),
                'median_latency': statistics.median(latencies)
            }
            
            # Cleanup
            test_engine.shutdown()
            time.sleep(0.1)
            try:
                os.unlink(test_db_path)
            except PermissionError:
                pass
        
        return {
            'operation': 'worker_scaling',
            'worker_configs': worker_configs,
            'results': results
        }
    
    def print_results(self, results: Dict[str, Any]):
        """Print benchmark results in a formatted way."""
        print(f"\n{'='*80}")
        print(f"BENCHMARK RESULTS")
        print(f"{'='*80}")
        
        if results['operation'] == 'single_insert':
            self._print_single_insert_results(results)
        elif results['operation'] == 'bulk_insert':
            self._print_bulk_insert_results(results)
        elif results['operation'] == 'select':
            self._print_select_results(results)
        elif results['operation'] == 'worker_scaling':
            self._print_worker_scaling_results(results)
    
    def _print_single_insert_results(self, results: Dict[str, Any]):
        """Print single insert benchmark results."""
        print(f"Single Insert Performance:")
        print(f"  Operations: {results['num_operations']:,}")
        print(f"  Total Time: {results['total_time']:.2f} seconds")
        print(f"  Throughput: {results['throughput']:.2f} ops/sec")
        print(f"  Latency Statistics (ms):")
        print(f"    Average: {results['avg_latency']:.2f}")
        print(f"    Median:  {results['median_latency']:.2f}")
        print(f"    Min:     {results['min_latency']:.2f}")
        print(f"    Max:     {results['max_latency']:.2f}")
    
    def _print_bulk_insert_results(self, results: Dict[str, Any]):
        """Print bulk insert benchmark results."""
        print(f"Bulk Insert Performance:")
        print(f"  Batch Size | Throughput (ops/sec) | Avg Latency per Record (ms)")
        print(f"{'-'*12} {'-'*15} {'-'*20}")
        
        for batch_size, batch_results in results['results'].items():
            print(f"{batch_size:<12} {batch_results['throughput']:<15.2f} "
                  f"{batch_results['avg_latency_per_record']:<20.2f}")
    
    def _print_select_results(self, results: Dict[str, Any]):
        """Print select benchmark results."""
        print(f"Select Performance:")
        print(f"{'Query Type':<20} {'Throughput':<15} {'Avg Latency':<15} {'Results':<10}")
        print(f"{'-'*20} {'-'*15} {'-'*15} {'-'*10}")
        
        for query_name, query_results in results['results'].items():
            print(f"{query_name:<20} {query_results['throughput']:<15.2f} "
                  f"{query_results['avg_latency']:<15.2f} {query_results['result_count']:<10}")
    
    def _print_worker_scaling_results(self, results: Dict[str, Any]):
        """Print worker scaling benchmark results."""
        print(f"Worker Thread Scaling Performance:")
        print(f"{'Workers':<10} {'Throughput':<15} {'Avg Latency':<15}")
        print(f"{'-'*10} {'-'*15} {'-'*15}")
        
        for num_workers, worker_results in results['results'].items():
            print(f"{num_workers:<10} {worker_results['throughput']:<15.2f} "
                  f"{worker_results['avg_latency']:<15.2f}")
    
    def cleanup(self):
        """Clean up resources."""
        if hasattr(self, 'db_engine'):
            self.db_engine.shutdown()
        
        # Clean up temporary database file
        if hasattr(self, 'temp_db_path') and self.temp_db_path and os.path.exists(self.temp_db_path):
            try:
                os.unlink(self.temp_db_path)
            except Exception as e:
                print(f"Warning: Could not clean up temporary database {self.temp_db_path}: {e}")


def main():
    """Main function for running benchmarks."""
    parser = argparse.ArgumentParser(description='DbEngine Performance Benchmark')
    parser.add_argument('--database', '-d', help='Database URL (default: temporary SQLite)')
    parser.add_argument('--workers', '-w', type=int, default=1, help='Number of worker threads')
    parser.add_argument('--operations', '-o', type=int, default=1000, help='Number of operations for single insert test')
    parser.add_argument('--select-ops', '-s', type=int, default=100, help='Number of operations for select test')
    parser.add_argument('--batch-sizes', nargs='+', type=int, default=[10, 50, 100, 500, 1000], 
                       help='Batch sizes for bulk insert test')
    parser.add_argument('--worker-configs', nargs='+', type=int, default=[1, 2, 4], 
                       help='Worker thread configurations for scaling test')
    parser.add_argument('--tests', nargs='+', 
                       choices=['single', 'bulk', 'select', 'scaling', 'all'], 
                       default=['all'], help='Tests to run')
    
    args = parser.parse_args()
    
    # Initialize benchmark
    benchmark = DbEngineBenchmark(args.database, args.workers)
    
    try:
        if 'all' in args.tests or 'single' in args.tests:
            results = benchmark.benchmark_single_inserts(args.operations)
            benchmark.print_results(results)
        
        if 'all' in args.tests or 'bulk' in args.tests:
            results = benchmark.benchmark_bulk_inserts(args.batch_sizes)
            benchmark.print_results(results)
        
        if 'all' in args.tests or 'select' in args.tests:
            results = benchmark.benchmark_selects(args.select_ops)
            benchmark.print_results(results)
        
        if 'all' in args.tests or 'scaling' in args.tests:
            results = benchmark.benchmark_worker_scaling(args.worker_configs)
            benchmark.print_results(results)
    
    finally:
        benchmark.cleanup()


if __name__ == '__main__':
    main() 