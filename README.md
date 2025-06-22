# jpy-sync-db-lite

Jim's Python - Synchronous Database Wrapper for SQLite

A lightweight, thread-safe SQLite database wrapper built on SQLAlchemy with optimized performance for concurrent operations.

## Features

- **Thread-safe operations** with worker thread pool
- **SQLAlchemy 2.0+ compatibility** with modern async patterns
- **Performance optimized** with SQLite-specific pragmas
- **Simple API** for common database operations
- **Consolidated operations** for both single and bulk operations
- **Transaction support** for complex operations
- **Statistics tracking** for monitoring performance
- **Automatic backup system** with periodic and manual backup capabilities

## Installation

### From PyPI (when published)
```bash
pip install jpy-sync-db-lite
```

### From source
```bash
git clone https://github.com/jim-schilling/jpy-sync-db-lite.git
cd jpy-sync-db-lite
pip install -e .
```

### Development setup
```bash
git clone https://github.com/jim-schilling/jpy-sync-db-lite.git
cd jpy-sync-db-lite
pip install -e ".[dev]"
```

## Quick Start

```python
from jpy_sync_db_lite import DbEngine

# Initialize database engine with backup enabled
db = DbEngine('sqlite:///my_database.db', 
              num_workers=2, 
              debug=False,
              backup_enabled=True,
              backup_interval=3600,  # 1 hour
              backup_dir='./backups')

# Create a table
db.execute("""
    CREATE TABLE IF NOT EXISTS users (
        id INTEGER PRIMARY KEY,
        name TEXT NOT NULL,
        email TEXT UNIQUE
    )
""")

# Insert single record
db.execute(
    "INSERT INTO users (name, email) VALUES (:name, :email)",
    {"name": "John Doe", "email": "john@example.com"}
)

# Fetch data
users = db.fetch("SELECT * FROM users WHERE name = :name", {"name": "John Doe"})
print(users)  # [{'id': 1, 'name': 'John Doe', 'email': 'john@example.com'}]

# Request immediate backup
success = db.request_backup()
print(f"Backup completed: {success}")

# Get backup information
backup_info = db.get_backup_info()
print(f"Total backups: {backup_info['total_backups']}")

# Cleanup
db.shutdown()
```

## API Changes

The library has been simplified to use a consolidated API:

- **`execute()`** now handles both single operations and bulk operations
- **`bulk_insert()` and `bulk_update()`** methods have been removed
- **`fetch()`** remains unchanged for SELECT operations

### Migration Guide

If you're upgrading from an older version, here are the changes needed:

```python
# Old API
db.bulk_insert("INSERT INTO users (name) VALUES (:name)", user_data)
db.bulk_update("UPDATE users SET status = :status WHERE id = :id", update_data)

# New API
count = db.execute("INSERT INTO users (name) VALUES (:name)", user_data)
count = db.execute("UPDATE users SET status = :status WHERE id = :id", update_data)
```

The `execute()` method now returns:
- `True` for single operations
- `int` (count) for bulk operations

## API Reference

### DbEngine

The main database engine class that manages connections and operations.

#### Constructor

```python
DbEngine(database_url: str, **kwargs)
```

**Parameters:**
- `database_url`: SQLAlchemy database URL (e.g., 'sqlite:///database.db')
- `num_workers`: Number of worker threads (default: 1)
- `debug`: Enable SQLAlchemy echo mode (default: False)
- `backup_enabled`: Enable backup system (default: False)
- `backup_interval`: Backup interval in seconds (default: 3600)
- `backup_dir`: Backup directory path (default: './backups')
- `backup_cleanup_enabled`: Enable automatic backup cleanup (default: True)
- `backup_cleanup_keep_count`: Number of backups to keep (default: 10)

#### Methods

##### execute(query, params=None)
Execute a non-query SQL statement (INSERT, UPDATE, DELETE, etc.). Handles both single operations and bulk operations.

```python
# Single operation
db.execute("UPDATE users SET name = :name WHERE id = :id", 
          {"name": "New Name", "id": 1})

# Bulk operation
updates = [{"id": 1, "status": "active"}, {"id": 2, "status": "inactive"}]
count = db.execute("UPDATE users SET status = :status WHERE id = :id", updates)
print(f"Updated {count} users")
```

##### fetch(query, params=None)
Execute a SELECT query and return results as a list of dictionaries.

```python
results = db.fetch("SELECT * FROM users WHERE age > :min_age", 
                  {"min_age": 18})
```

##### execute_transaction(operations)
Execute multiple operations in a single transaction.

```python
operations = [
    {"operation": "execute", "query": "INSERT INTO users (name) VALUES (:name)", "params": {"name": "User1"}},
    {"operation": "fetch", "query": "SELECT COUNT(*) as count FROM users"}
]
results = db.execute_transaction(operations)
```

##### request_backup()
Request an immediate backup operation.

```python
success = db.request_backup()
if success:
    print("Backup completed successfully")
```

##### get_backup_info()
Get detailed backup information and file list.

```python
backup_info = db.get_backup_info()
print(f"Total backups: {backup_info['total_backups']}")
print(f"Backup files: {len(backup_info.get('backup_files', []))}")
```

##### get_raw_connection()
Get a raw SQLAlchemy connection for advanced operations.

```python
with db.get_raw_connection() as conn:
    # Use conn for complex operations
    result = conn.execute(text("SELECT * FROM users"))
```

##### get_stats()
Get database operation statistics.

```python
stats = db.get_stats()
print(f"Requests: {stats['requests']}, Errors: {stats['errors']}")
print(f"Backups: {stats['backups']}")
```

##### shutdown()
Gracefully shutdown the database engine and worker threads.

```python
db.shutdown()
```

## Performance Optimizations

The library includes several SQLite-specific optimizations:

- **WAL mode** for better concurrency
- **Optimized cache settings** (64MB cache)
- **Memory-mapped files** (256MB)
- **Query planner optimization**
- **Static connection pooling**

## Thread Safety

All operations are thread-safe through:
- Worker thread pool for request processing
- Thread-safe queues for request/response handling
- Proper connection management per operation
- Lock-protected statistics updates

## Performance Testing

The library includes comprehensive performance testing tools to help you optimize your database operations.

### Quick Performance Check

Run the standalone benchmark script for a quick performance overview:

```bash
# Run all benchmarks
python tests/benchmark_db_engine.py

# Run specific tests
python tests/benchmark_db_engine.py --tests single bulk select scaling

# Run backup performance tests
python tests/benchmark_db_engine.py --tests backup --backup-enabled

# Customize parameters
python tests/benchmark_db_engine.py --operations 2000 --workers 2 --batch-sizes 100 500 1000
```

### Comprehensive Performance Tests

Run the full unittest suite for detailed performance analysis:

```bash
# Run all performance tests
python -m unittest tests.test_db_engine_performance -v

# Run backup performance tests
python -m unittest tests.test_backup_performance -v

# Run specific test categories
python -m unittest tests.test_db_engine_performance.TestDbEnginePerformance.test_single_insert_performance -v
python -m unittest tests.test_db_engine_performance.TestDbEnginePerformance.test_bulk_insert_performance -v
python -m unittest tests.test_db_engine_performance.TestDbEnginePerformance.test_select_performance -v
```

### Performance Test Categories

#### 1. Single Insert Performance
- Measures individual insert operation latency and throughput
- **Expected**: >50 ops/sec, <100ms average latency

#### 2. Bulk Insert Performance
- Tests different batch sizes (10, 50, 100, 500, 1000 records)
- Measures throughput and per-record latency
- **Expected**: >100 ops/sec for optimal batch sizes

#### 3. Select Performance
- Tests various query types:
  - Simple SELECT with LIMIT
  - Filtered SELECT with WHERE clauses
  - Indexed SELECT using indexed columns
  - Aggregate SELECT with COUNT/AVG
  - Complex SELECT with multiple conditions and ORDER BY
- **Expected**: >200 ops/sec for simple selects

#### 4. Concurrent Operations Performance
- Tests performance under concurrent load (1, 2, 4, 8 threads)
- Mix of read and write operations
- **Expected**: >50 ops/sec under load

#### 5. Transaction Performance
- Tests transaction operations with different sizes
- **Expected**: >50 ops/sec for transactions

#### 6. Worker Thread Scaling
- Tests performance with different worker thread configurations
- Helps determine optimal worker count
- **Expected**: >30 ops/sec with single worker

#### 7. Backup Performance
- Tests backup performance with different dataset sizes
- Measures backup time, throughput, and data transfer rates
- Tests backup integrity and verification
- **Expected**: >100 rows/sec backup rate, <15 seconds for 5K rows

### Performance Metrics

The tests measure:

- **Throughput**: Operations per second (ops/sec)
- **Latency**: Time per operation in milliseconds
- **Memory Usage**: Memory consumption and growth rate (requires `psutil`)
- **Concurrency Scaling**: Performance with multiple threads
- **Backup Performance**: Backup time, rate, and data transfer speed

### Performance Expectations

Based on SQLite with WAL mode and optimized pragmas:

| Operation Type | Expected Throughput | Expected Latency |
|----------------|-------------------|------------------|
| Single Insert  | >50 ops/sec       | <100ms avg       |
| Bulk Insert    | >100 ops/sec      | <50ms per record |
| Simple Select  | >200 ops/sec      | <10ms avg        |
| Complex Select | >50 ops/sec       | <50ms avg        |
| Transactions   | >50 ops/sec       | <100ms avg       |
| Backup         | >100 rows/sec     | <15s for 5K rows |

### Optimization Recommendations

The performance tests provide recommendations for:
- **Optimal batch sizes** for bulk operations
- **Optimal worker threads** for your workload
- **Memory efficiency** analysis
- **Scaling considerations** for concurrent operations

### Memory Monitoring

Memory usage monitoring requires the `psutil` package:

```bash
pip install psutil
```

Without `psutil`, the tests will run but skip memory measurements.

### Performance Troubleshooting

Common performance issues and solutions:

1. **Low throughput**: Use batch operations, optimize worker count
2. **High latency**: Check for blocking operations, monitor system resources
3. **Memory growth**: Look for unclosed connections or large result sets
4. **Concurrency issues**: SQLite has limitations with concurrent writes

For detailed performance analysis, see [tests/PERFORMANCE_TESTS.md](tests/PERFORMANCE_TESTS.md).

## Development

### Running Tests
```bash
pytest
```

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests for new functionality
5. Ensure all tests pass
6. Submit a pull request

## Changelog

### 0.1.0
- Initial release
- Thread-safe SQLite operations
- SQLAlchemy 2.0+ compatibility
- Performance optimizations
- Bulk operations support
- Automatic backup system with periodic and manual backup capabilities
- Backup performance testing and monitoring tools

## Backup System

The library includes a comprehensive backup system that provides both automatic periodic backups and manual backup requests.

### Backup Configuration

When initializing the database engine, you can configure backup behavior:

```python
db = DbEngine('sqlite:///my_database.db',
              backup_enabled=True,           # Enable backup system
              backup_interval=3600,          # Backup every hour (seconds)
              backup_dir='./backups',        # Backup directory
              backup_cleanup_enabled=True,   # Enable automatic cleanup
              backup_cleanup_keep_count=10)  # Keep last 10 backups
```

**Configuration Parameters:**
- `backup_enabled`: Enable/disable the backup system (default: False)
- `backup_interval`: Time between automatic backups in seconds (default: 3600)
- `backup_dir`: Directory to store backup files (default: './backups')
- `backup_cleanup_enabled`: Enable automatic cleanup of old backups (default: True)
- `backup_cleanup_keep_count`: Number of recent backups to keep (default: 10)

### Backup Features

#### Automatic Periodic Backups
- **Background thread** runs independently of database operations
- **Configurable intervals** from seconds to hours
- **Thread-safe** with proper locking to prevent concurrent backups
- **WAL checkpointing** ensures data consistency

#### Manual Backup Requests
- **Immediate backup** on demand
- **Non-blocking** - returns immediately if backup is already in progress
- **Thread-safe** with proper error handling

#### Backup File Management
- **Timestamped filenames** (e.g., `backup_20250122_143052.sqlite`)
- **Automatic cleanup** of old backups
- **File integrity verification** during backup process
- **Size and modification time tracking**

### Backup API Methods

#### request_backup()
Request an immediate backup operation.

```python
success = db.request_backup()
if success:
    print("Backup completed successfully")
```

#### get_backup_info()
Get detailed backup information and file list.

```python
backup_info = db.get_backup_info()
print(f"Total backups: {backup_info['total_backups']}")
print(f"Backup files: {len(backup_info.get('backup_files', []))}")
```

#### get_stats()
Get backup statistics along with other performance metrics.

```python
stats = db.get_stats()
print(f"Total backups: {stats['backups']}")
print(f"Backup enabled: {stats['backup_enabled']}")
print(f"Last backup time: {stats['last_backup_time']}")
```

### Backup Examples

#### Basic Backup Setup
```python
from jpy_sync_db_lite import DbEngine

# Initialize with backup enabled
db = DbEngine('sqlite:///app.db',
              backup_enabled=True,
              backup_interval=1800,  # 30 minutes
              backup_dir='./database_backups')

# Database operations continue normally
db.execute("INSERT INTO users (name) VALUES (:name)", {"name": "Alice"})

# Manual backup when needed
if important_operation_completed:
    db.request_backup()
```

#### Backup Monitoring
```python
# Check backup status
backup_info = db.get_backup_info()
if backup_info['enabled']:
    print(f"Backup system is active")
    print(f"Next automatic backup in: {backup_info['interval_seconds']} seconds")
    
    if backup_info['backup_files']:
        latest_backup = backup_info['backup_files'][0]
        print(f"Latest backup: {latest_backup['filename']}")
        print(f"Size: {latest_backup['size_bytes'] / 1024 / 1024:.2f} MB")
    else:
        print("No backups created yet")
```

#### Backup Verification
```python
# Verify backup integrity by opening backup file
backup_info = db.get_backup_info()
if backup_info['backup_files']:
    latest_backup = backup_info['backup_files'][0]
    
    # Create temporary engine to verify backup
    backup_db = DbEngine(f"sqlite:///{latest_backup['path']}")
    try:
        # Verify data exists in backup
        user_count = backup_db.fetch("SELECT COUNT(*) as count FROM users")
        print(f"Backup contains {user_count[0]['count']} users")
    finally:
        backup_db.shutdown()
```

### Backup Performance

The backup system is designed for minimal impact on database performance:

- **WAL checkpointing** ensures fast, consistent backups
- **Non-blocking operations** during backup process
- **Background thread** doesn't interfere with database operations
- **Optimized file copying** with retry logic

### Backup File Format

Backup files are standard SQLite database files with:
- **Timestamped naming**: `backup_YYYYMMDD_HHMMSS.sqlite`
- **Full database copy** including all tables, indexes, and data
- **WAL checkpointed** for data consistency
- **Compressed storage** (SQLite's built-in compression)

### Backup Best Practices

1. **Configure appropriate intervals** based on data change frequency
2. **Monitor backup directory size** and adjust cleanup settings
3. **Test backup restoration** periodically
4. **Use manual backups** before major operations
5. **Monitor backup statistics** for system health
