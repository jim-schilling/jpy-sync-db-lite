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
import random

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
    
    def __init__(self, database_url: str = None, num_workers: int = 1, backup_enabled: bool = False):
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
        self.backup_enabled = backup_enabled
        
        # Create backup directory if backup is enabled
        if backup_enabled:
            self.backup_dir = tempfile.mkdtemp(prefix='benchmark_backup_')
            self.db_engine = DbEngine(
                self.database_url, 
                num_workers=num_workers, 
                debug=False,
                backup_enabled=True,
                backup_interval=3600,  # 1 hour for benchmarks
                backup_dir=self.backup_dir,
                backup_cleanup_enabled=False  # Disable cleanup during benchmarks
            )
        else:
            self.backup_dir = None
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
    
    def benchmark_backup_performance(self, dataset_sizes: List[int] = None) -> Dict[str, Any]:
        """Benchmark backup performance with different dataset sizes."""
        if dataset_sizes is None:
            dataset_sizes = [100, 1000, 5000, 10000]
        
        if not self.backup_enabled:
            print("Backup benchmarking requires backup_enabled=True")
            return {'error': 'Backup not enabled'}
        
        print(f"Benchmarking backup performance with dataset sizes: {dataset_sizes}")
        
        results = {}
        
        for dataset_size in dataset_sizes:
            print(f"  Testing dataset size: {dataset_size}")
            
            # Clear existing data
            self.db_engine.execute("DELETE FROM benchmark_test")
            
            # Populate with test data
            test_data = self._generate_test_data(dataset_size)
            self.db_engine.execute(
                "INSERT INTO benchmark_test (name, value, data) VALUES (:name, :value, :data)",
                test_data
            )
            
            # Get database size before backup
            db_path = self.db_engine.engine.url.database
            if not os.path.isabs(db_path):
                db_path = os.path.abspath(db_path)
            
            db_size_before = os.path.getsize(db_path)
            
            # Benchmark backup
            latencies = []
            num_backups = 3  # Reduced from 5 for faster testing
            
            for i in range(num_backups):
                op_start = time.time()
                success = self.db_engine.request_backup()
                op_end = time.time()
                
                if success:
                    latencies.append((op_end - op_start) * 1000)
                else:
                    print(f"    Backup {i+1} failed")
            
            if latencies:
                # Get backup file size
                backup_info = self.db_engine.get_backup_info()
                backup_files = backup_info.get('backup_files', [])
                backup_size = backup_files[-1]['size_bytes'] if backup_files else 0
                
                results[dataset_size] = {
                    'dataset_size': dataset_size,
                    'db_size_bytes': db_size_before,
                    'backup_size_bytes': backup_size,
                    'compression_ratio': backup_size / db_size_before if db_size_before > 0 else 0,
                    'num_backups': len(latencies),
                    'latency_ms': latencies,
                    'avg_latency': statistics.mean(latencies),
                    'min_latency': min(latencies),
                    'max_latency': max(latencies),
                    'median_latency': statistics.median(latencies),
                    'throughput_mb_per_sec': (db_size_before / 1024 / 1024) / (statistics.mean(latencies) / 1000)
                }
            else:
                results[dataset_size] = {'error': 'All backups failed'}
        
        return {
            'operation': 'backup_performance',
            'dataset_sizes': dataset_sizes,
            'results': results
        }
    
    def benchmark_backup_with_concurrent_operations(self, num_operations: int = 500) -> Dict[str, Any]:
        """Benchmark backup performance while database is actively being used."""
        if not self.backup_enabled:
            print("Backup benchmarking requires backup_enabled=True")
            return {'error': 'Backup not enabled'}
        
        print(f"Benchmarking backup with {num_operations} concurrent operations...")
        
        # Populate with substantial data
        test_data = self._generate_test_data(10000)  # Reduced from 50000
        self.db_engine.execute(
            "INSERT INTO benchmark_test (name, value, data) VALUES (:name, :value, :data)",
            test_data
        )
        
        # Track operation results
        operation_results = {
            'inserts': {'success': 0, 'failed': 0, 'latencies': []},
            'updates': {'success': 0, 'failed': 0, 'latencies': []},
            'selects': {'success': 0, 'failed': 0, 'latencies': []},
            'backups': {'success': 0, 'failed': 0, 'latencies': []}
        }
        
        import threading
        import queue
        
        # Operation queue
        op_queue = queue.Queue()
        results_queue = queue.Queue()
        
        def background_operations():
            """Background thread for database operations."""
            while True:
                try:
                    op = op_queue.get(timeout=1)
                    if op == 'STOP':
                        break
                    
                    op_type, op_data = op
                    op_start = time.time()
                    
                    try:
                        if op_type == 'insert':
                            self.db_engine.execute(
                                "INSERT INTO benchmark_test (name, value, data) VALUES (:name, :value, :data)",
                                op_data
                            )
                        elif op_type == 'update':
                            self.db_engine.execute(
                                "UPDATE benchmark_test SET data = :data WHERE id = :id",
                                op_data
                            )
                        elif op_type == 'select':
                            self.db_engine.fetch(
                                "SELECT * FROM benchmark_test WHERE value > :value LIMIT 10",
                                op_data
                            )
                        elif op_type == 'backup':
                            self.db_engine.request_backup()
                        
                        op_end = time.time()
                        results_queue.put(('success', op_type, (op_end - op_start) * 1000))
                        
                    except Exception as e:
                        op_end = time.time()
                        results_queue.put(('failed', op_type, (op_end - op_start) * 1000))
                    
                    op_queue.task_done()
                    
                except queue.Empty:
                    continue
        
        # Start background thread
        bg_thread = threading.Thread(target=background_operations)
        bg_thread.start()
        
        # Queue operations
        for i in range(num_operations):
            op_type = random.choice(['insert', 'update', 'select', 'backup'])
            
            if op_type == 'insert':
                op_data = {
                    "name": f"ConcurrentUser{i}",
                    "value": i,
                    "data": f"Concurrent data {i}"
                }
            elif op_type == 'update':
                op_data = {
                    "data": f"Updated data {i}",
                    "id": random.randint(1, 10000)
                }
            elif op_type == 'select':
                op_data = {"value": random.randint(0, 10000)}
            else:  # backup
                op_data = None
            
            op_queue.put((op_type, op_data))
        
        # Wait for all operations to complete
        op_queue.join()
        op_queue.put('STOP')
        bg_thread.join()
        
        # Collect results
        while not results_queue.empty():
            status, op_type, latency = results_queue.get()
            if status == 'success':
                operation_results[op_type + 's']['success'] += 1
            else:
                operation_results[op_type + 's']['failed'] += 1
            operation_results[op_type + 's']['latencies'].append(latency)
        
        # Calculate statistics
        stats = {}
        for op_type, results in operation_results.items():
            if results['latencies']:
                stats[op_type] = {
                    'total_operations': results['success'] + results['failed'],
                    'success_rate': results['success'] / (results['success'] + results['failed']),
                    'avg_latency': statistics.mean(results['latencies']),
                    'median_latency': statistics.median(results['latencies']),
                    'min_latency': min(results['latencies']),
                    'max_latency': max(results['latencies'])
                }
            else:
                stats[op_type] = {'error': 'No operations completed'}
        
        return {
            'operation': 'backup_with_concurrent_operations',
            'num_operations': num_operations,
            'results': stats
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
        elif results['operation'] == 'backup_performance':
            self._print_backup_performance_results(results)
        elif results['operation'] == 'backup_with_concurrent_operations':
            self._print_backup_with_concurrent_operations_results(results)
    
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
    
    def _print_backup_performance_results(self, results: Dict[str, Any]):
        """Print backup performance benchmark results."""
        print(f"\nBackup Performance Results:")
        print(f"{'Dataset Size':<15} {'DB Size (MB)':<15} {'Backup Size (MB)':<15} {'Compression':<15} {'Throughput (MB/s)':<15}")
        print(f"{'-'*15} {'-'*15} {'-'*15} {'-'*15} {'-'*15}")
        
        for dataset_size, backup_results in results['results'].items():
            if 'error' not in backup_results:
                db_size_mb = backup_results['db_size_bytes'] / 1024 / 1024
                backup_size_mb = backup_results['backup_size_bytes'] / 1024 / 1024
                compression = backup_results['compression_ratio']
                throughput = backup_results['throughput_mb_per_sec']
                
                print(f"{dataset_size:<15} {db_size_mb:<15.2f} {backup_size_mb:<15.2f} "
                      f"{compression:<15.4f} {throughput:<15.2f}")
            else:
                print(f"{dataset_size:<15} {backup_results['error']}")
    
    def _print_backup_with_concurrent_operations_results(self, results: Dict[str, Any]):
        """Print backup with concurrent operations benchmark results."""
        print(f"\nBackup with Concurrent Operations Results:")
        print(f"{'Operation':<20} {'Total Ops':<15} {'Success Rate':<15} {'Avg Latency (ms)':<15}")
        print(f"{'-'*20} {'-'*15} {'-'*15} {'-'*15}")
        
        for op_type, op_results in results['results'].items():
            if 'error' not in op_results:
                print(f"{op_type:<20} {op_results['total_operations']:<15} "
                      f"{op_results['success_rate']:<15.2%} {op_results['avg_latency']:<15.2f}")
            else:
                print(f"{op_type:<20} {op_results['error']}")
    
    def cleanup(self):
        """Clean up resources."""
        if hasattr(self, 'db_engine'):
            self.db_engine.shutdown()
        
        # Clean up backup directory if it exists
        if hasattr(self, 'backup_dir') and self.backup_dir and os.path.exists(self.backup_dir):
            try:
                import shutil
                shutil.rmtree(self.backup_dir)
            except Exception as e:
                print(f"Warning: Could not clean up backup directory {self.backup_dir}: {e}")
        
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
    parser.add_argument('--backup-enabled', action='store_true', help='Enable backup functionality for testing')
    parser.add_argument('--backup-dataset-sizes', nargs='+', type=int, default=[100, 1000, 5000], 
                       help='Dataset sizes for backup performance test')
    parser.add_argument('--backup-concurrent-ops', type=int, default=500, 
                       help='Number of concurrent operations for backup test')
    parser.add_argument('--tests', nargs='+', 
                       choices=['single', 'bulk', 'select', 'scaling', 'backup', 'backup_concurrent', 'all'], 
                       default=['all'], help='Tests to run')
    
    args = parser.parse_args()
    
    # Initialize benchmark
    benchmark = DbEngineBenchmark(args.database, args.workers, args.backup_enabled)
    
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
        
        if 'all' in args.tests or 'backup' in args.tests:
            if args.backup_enabled:
                results = benchmark.benchmark_backup_performance(args.backup_dataset_sizes)
                benchmark.print_results(results)
            else:
                print("Skipping backup tests (use --backup-enabled to enable)")
        
        if 'all' in args.tests or 'backup_concurrent' in args.tests:
            if args.backup_enabled:
                results = benchmark.benchmark_backup_with_concurrent_operations(args.backup_concurrent_ops)
                benchmark.print_results(results)
            else:
                print("Skipping backup concurrent tests (use --backup-enabled to enable)")
    
    finally:
        benchmark.cleanup()


if __name__ == '__main__':
    main() 