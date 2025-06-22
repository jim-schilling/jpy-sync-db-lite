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

# Initialize database engine
db = DbEngine('sqlite:///my_database.db', num_workers=2, debug=False)

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

# Bulk insert using execute method
users_data = [
    {"name": "Jane Smith", "email": "jane@example.com"},
    {"name": "Bob Johnson", "email": "bob@example.com"}
]
count = db.execute(
    "INSERT INTO users (name, email) VALUES (:name, :email)",
    users_data
)
print(f"Inserted {count} users")

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
    {"type": "execute", "query": "INSERT INTO users (name) VALUES (:name)", "params": {"name": "User1"}},
    {"type": "fetch", "query": "SELECT COUNT(*) as count FROM users"}
]
results = db.execute_transaction(operations)
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

# Customize parameters
python tests/benchmark_db_engine.py --operations 2000 --workers 2 --batch-sizes 100 500 1000
```

### Comprehensive Performance Tests

Run the full unittest suite for detailed performance analysis:

```bash
# Run all performance tests
python -m unittest tests.test_db_engine_performance -v

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

### Performance Metrics

The tests measure:

- **Throughput**: Operations per second (ops/sec)
- **Latency**: Time per operation in milliseconds
- **Memory Usage**: Memory consumption and growth rate (requires `psutil`)
- **Concurrency Scaling**: Performance with multiple threads

### Performance Expectations

Based on SQLite with WAL mode and optimized pragmas:

| Operation Type | Expected Throughput | Expected Latency |
|----------------|-------------------|------------------|
| Single Insert  | >50 ops/sec       | <100ms avg       |
| Bulk Insert    | >100 ops/sec      | <50ms per record |
| Simple Select  | >200 ops/sec      | <10ms avg        |
| Complex Select | >50 ops/sec       | <50ms avg        |
| Transactions   | >50 ops/sec       | <100ms avg       |

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
