"""
Database backup performance tests for DbEngine class.

Copyright (c) 2025, Jim Schilling

Please keep this header when you use this code.

This module is licensed under the MIT License.
"""
import unittest
import tempfile
import os
import time
import random
import string
from sqlalchemy import text

# Add parent directory to path for imports
import sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from jpy_sync_db_lite.db_engine import DbEngine


class TestBackupPerformance(unittest.TestCase):
    """Performance tests for database backup functionality."""
    
    def setUp(self):
        """Set up test fixtures before each test method."""
        # Create temporary database file
        self.temp_db_fd, self.temp_db_path = tempfile.mkstemp(suffix='.db')
        os.close(self.temp_db_fd)
        self.database_url = f"sqlite:///{self.temp_db_path}"
        
        # Create temporary backup directory
        self.backup_dir = tempfile.mkdtemp(prefix='backup_perf_test_')
        
        # Initialize DbEngine with backup DISABLED to avoid thread interference
        self.db_engine = DbEngine(
            self.database_url, 
            backup_enabled=False,  # Disable backup thread for testing
            backup_interval=1,
            backup_dir=self.backup_dir,
            backup_cleanup_enabled=False
        )
        
        # Create test table
        self._create_test_table()
    
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
                time.sleep(0.1)
                try:
                    os.unlink(self.temp_db_path)
                except PermissionError:
                    pass
        
        # Remove temporary backup directory
        if os.path.exists(self.backup_dir):
            try:
                import shutil
                shutil.rmtree(self.backup_dir)
            except Exception:
                pass
    
    def _create_test_table(self):
        """Create a test table for performance testing."""
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS performance_users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            email TEXT UNIQUE,
            age INTEGER,
            salary REAL,
            department TEXT,
            hire_date TEXT,
            active BOOLEAN DEFAULT 1,
            notes TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
        self.db_engine.execute(create_table_sql)
    
    def _clear_test_data(self):
        """Clear all test data from the table."""
        self.db_engine.execute("DELETE FROM performance_users")
    
    def _generate_test_data(self, num_rows):
        """Generate test data with specified number of rows."""
        departments = ['Engineering', 'Sales', 'Marketing', 'HR', 'Finance', 'Operations']
        
        test_data = []
        for i in range(num_rows):
            # Generate unique email
            email = f"user{i}_{random.randint(1000, 9999)}@example.com"
            
            # Generate random name
            first_name = ''.join(random.choices(string.ascii_lowercase, k=8)).capitalize()
            last_name = ''.join(random.choices(string.ascii_lowercase, k=10)).capitalize()
            name = f"{first_name} {last_name}"
            
            # Generate other fields
            age = random.randint(22, 65)
            salary = round(random.uniform(30000, 150000), 2)
            department = random.choice(departments)
            hire_date = f"202{random.randint(0, 4)}-{random.randint(1, 12):02d}-{random.randint(1, 28):02d}"
            active = random.choice([True, False])
            notes = ''.join(random.choices(string.ascii_letters + string.digits + ' ', k=random.randint(50, 200)))
            
            test_data.append({
                "name": name,
                "email": email,
                "age": age,
                "salary": salary,
                "department": department,
                "hire_date": hire_date,
                "active": active,
                "notes": notes
            })
        
        return test_data
    
    def _insert_test_data(self, num_rows):
        """Insert test data into the table."""
        print(f"Generating {num_rows} rows of test data...")
        test_data = self._generate_test_data(num_rows)
        
        insert_sql = """
        INSERT INTO performance_users (name, email, age, salary, department, hire_date, active, notes) 
        VALUES (:name, :email, :age, :salary, :department, :hire_date, :active, :notes)
        """
        
        print(f"Inserting {num_rows} rows...")
        start_time = time.time()
        result = self.db_engine.execute(insert_sql, test_data)
        end_time = time.time()
        
        insert_time = end_time - start_time
        print(f"Inserted {result} rows in {insert_time:.2f} seconds ({result/insert_time:.0f} rows/sec)")
        
        return insert_time
    
    def _measure_backup_performance(self, dataset_size):
        """Measure backup performance for a given dataset size."""
        print(f"\n=== Testing backup performance with {dataset_size:,} rows ===")
        
        # Clear any existing data first
        self._clear_test_data()
        
        # Insert test data
        insert_time = self._insert_test_data(dataset_size)
        
        # Verify data was inserted
        count_result = self.db_engine.fetch("SELECT COUNT(*) as count FROM performance_users")
        actual_count = count_result[0]['count']
        print(f"Verified {actual_count:,} rows in database")
        
        # Measure database file size
        db_size = os.path.getsize(self.temp_db_path)
        print(f"Database file size: {db_size:,} bytes ({db_size/1024/1024:.2f} MB)")
        
        # Perform backup and measure time
        print("Starting backup...")
        start_time = time.time()
        success = self.db_engine.request_backup()
        end_time = time.time()
        
        if not success:
            raise Exception("Backup failed")
        
        backup_time = end_time - start_time
        
        # Find backup file and measure its size
        backup_files = [f for f in os.listdir(self.backup_dir) 
                       if f.startswith("backup_") and f.endswith(".sqlite")]
        if not backup_files:
            raise Exception("No backup file found")
        
        backup_files.sort(key=lambda f: os.path.getmtime(os.path.join(self.backup_dir, f)), reverse=True)
        backup_file_path = os.path.join(self.backup_dir, backup_files[0])
        backup_size = os.path.getsize(backup_file_path)
        
        # Verify backup integrity
        backup_db_url = f"sqlite:///{backup_file_path}"
        backup_db = DbEngine(backup_db_url)
        try:
            backup_count_result = backup_db.fetch("SELECT COUNT(*) as count FROM performance_users")
            backup_count = backup_count_result[0]['count']
            print(f"Backup contains {backup_count:,} rows")
            
            if backup_count != actual_count:
                raise Exception(f"Backup integrity check failed: expected {actual_count}, got {backup_count}")
            
        finally:
            backup_db.shutdown()
        
        # Calculate performance metrics
        backup_rate = actual_count / backup_time if backup_time > 0 else 0
        data_rate = (db_size / 1024 / 1024) / backup_time if backup_time > 0 else 0  # MB/s
        
        print(f"Backup completed in {backup_time:.2f} seconds")
        print(f"Backup rate: {backup_rate:.0f} rows/sec")
        print(f"Data rate: {data_rate:.2f} MB/s")
        print(f"Backup file size: {backup_size:,} bytes ({backup_size/1024/1024:.2f} MB)")
        
        # Clean up backup file for next test
        os.remove(backup_file_path)
        
        return {
            'dataset_size': dataset_size,
            'insert_time': insert_time,
            'backup_time': backup_time,
            'backup_rate': backup_rate,
            'data_rate': data_rate,
            'db_size_mb': db_size / 1024 / 1024,
            'backup_size_mb': backup_size / 1024 / 1024
        }
    
    def test_backup_performance_1000_rows(self):
        """Test backup performance with 1,000 rows of data."""
        results = self._measure_backup_performance(1000)
        
        # Performance assertions
        self.assertLess(results['backup_time'], 5.0, "Backup should complete within 5 seconds for 1K rows")
        self.assertGreater(results['backup_rate'], 100, "Should backup at least 100 rows/sec")
        self.assertGreater(results['data_rate'], 0.1, "Should backup at least 0.1 MB/s")
        
        print(f"✓ 1,000 row backup test passed: {results['backup_time']:.2f}s")
    
    def test_backup_performance_5000_rows(self):
        """Test backup performance with 5,000 rows of data."""
        results = self._measure_backup_performance(5000)
        
        # Performance assertions
        self.assertLess(results['backup_time'], 15.0, "Backup should complete within 15 seconds for 5K rows")
        self.assertGreater(results['backup_rate'], 200, "Should backup at least 200 rows/sec")
        self.assertGreater(results['data_rate'], 0.1, "Should backup at least 0.1 MB/s")  # Lowered threshold
        
        print(f"✓ 5,000 row backup test passed: {results['backup_time']:.2f}s")
    
    def test_backup_performance_25000_rows(self):
        """Test backup performance with 25,000 rows of data."""
        results = self._measure_backup_performance(25000)
        
        # Performance assertions
        self.assertLess(results['backup_time'], 60.0, "Backup should complete within 60 seconds for 25K rows")
        self.assertGreater(results['backup_rate'], 300, "Should backup at least 300 rows/sec")
        self.assertGreater(results['data_rate'], 0.5, "Should backup at least 0.5 MB/s")
        
        print(f"✓ 25,000 row backup test passed: {results['backup_time']:.2f}s")
    
    def test_backup_performance_comparison(self):
        """Compare backup performance across different dataset sizes."""
        print("\n" + "="*60)
        print("BACKUP PERFORMANCE COMPARISON")
        print("="*60)
        
        dataset_sizes = [1000, 5000, 25000]
        all_results = []
        
        for size in dataset_sizes:
            try:
                results = self._measure_backup_performance(size)
                all_results.append(results)
            except Exception as e:
                print(f"Failed to test {size} rows: {e}")
                continue
        
        # Print comparison table
        print("\n" + "-"*80)
        print(f"{'Dataset Size':<12} {'Backup Time':<12} {'Rate (rows/s)':<15} {'Data Rate (MB/s)':<15} {'DB Size (MB)':<12}")
        print("-"*80)
        
        for results in all_results:
            print(f"{results['dataset_size']:<12} {results['backup_time']:<12.2f} "
                  f"{results['backup_rate']:<15.0f} {results['data_rate']:<15.2f} "
                  f"{results['db_size_mb']:<12.2f}")
        
        print("-"*80)
        
        # Performance analysis
        if len(all_results) >= 2:
            print("\nPERFORMANCE ANALYSIS:")
            
            # Check if backup time scales linearly
            first_result = all_results[0]
            last_result = all_results[-1]
            
            size_ratio = last_result['dataset_size'] / first_result['dataset_size']
            time_ratio = last_result['backup_time'] / first_result['backup_time']
            
            print(f"Dataset size increased by {size_ratio:.1f}x")
            print(f"Backup time increased by {time_ratio:.1f}x")
            
            if time_ratio <= size_ratio * 1.5:  # Allow some overhead
                print("✓ Backup performance scales reasonably with dataset size")
            else:
                print("⚠ Backup performance may not scale well with larger datasets")
        
        # Ensure we have at least one successful test
        self.assertGreater(len(all_results), 0, "At least one backup performance test should pass")

    def test_backup_performance_with_concurrent_operations(self):
        """Test backup performance with concurrent fetch and execute operations queued."""
        import threading
        dataset_size = 5000
        self._clear_test_data()
        self._insert_test_data(dataset_size)
        
        # Shared flag to stop threads
        stop_event = threading.Event()
        results = {'fetches': 0, 'executes': 0, 'errors': 0}
        
        def fetch_worker():
            while not stop_event.is_set():
                try:
                    # Randomly fetch a user
                    user_id = random.randint(1, dataset_size)
                    self.db_engine.fetch("SELECT * FROM performance_users WHERE id = :id", {"id": user_id})
                    results['fetches'] += 1
                except Exception:
                    results['errors'] += 1
        
        def execute_worker():
            while not stop_event.is_set():
                try:
                    # Randomly update a user's active status
                    user_id = random.randint(1, dataset_size)
                    active = random.choice([True, False])
                    self.db_engine.execute("UPDATE performance_users SET active = :active WHERE id = :id", {"active": active, "id": user_id})
                    results['executes'] += 1
                except Exception:
                    results['errors'] += 1
        
        # Start worker threads
        fetch_threads = [threading.Thread(target=fetch_worker) for _ in range(2)]
        execute_threads = [threading.Thread(target=execute_worker) for _ in range(2)]
        for t in fetch_threads + execute_threads:
            t.start()
        
        # Wait a moment to let threads queue some work
        time.sleep(0.5)
        
        # Start backup while threads are running
        print("\n=== Testing backup performance with concurrent operations (5,000 rows) ===")
        start_time = time.time()
        success = self.db_engine.request_backup()
        end_time = time.time()
        backup_time = end_time - start_time
        
        # Stop worker threads
        stop_event.set()
        for t in fetch_threads + execute_threads:
            t.join()
        
        # Find backup file and measure its size
        backup_files = [f for f in os.listdir(self.backup_dir) if f.startswith("backup_") and f.endswith(".sqlite")]
        self.assertTrue(backup_files, "No backup file found")
        backup_file_path = os.path.join(self.backup_dir, backup_files[0])
        backup_size = os.path.getsize(backup_file_path)
        
        # Verify backup integrity
        backup_db_url = f"sqlite:///{backup_file_path}"
        backup_db = DbEngine(backup_db_url)
        try:
            backup_count_result = backup_db.fetch("SELECT COUNT(*) as count FROM performance_users")
            backup_count = backup_count_result[0]['count']
            self.assertEqual(backup_count, dataset_size, f"Backup integrity check failed: expected {dataset_size}, got {backup_count}")
        finally:
            backup_db.shutdown()
        
        print(f"Backup completed in {backup_time:.2f} seconds with concurrent operations.")
        print(f"Backup file size: {backup_size:,} bytes ({backup_size/1024/1024:.2f} MB)")
        print(f"Fetches during backup: {results['fetches']}")
        print(f"Executes during backup: {results['executes']}")
        print(f"Errors during backup: {results['errors']}")
        
        # Clean up backup file
        os.remove(backup_file_path)
        
        # Assert backup time is reasonable and no integrity errors
        self.assertLess(backup_time, 15.0, "Backup should complete within 15 seconds with concurrent operations")
        self.assertEqual(results['errors'], 0, "There should be no errors during concurrent operations and backup")
        print(f"✓ Concurrent backup test passed: {backup_time:.2f}s")


if __name__ == "__main__":
    # Run the tests with verbose output
    unittest.main(verbosity=2) 