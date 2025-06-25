# jpy-sync-db-lite

Jim's Python - Synchronous Database Wrapper for SQLite

A lightweight, thread-safe SQLite database wrapper built on SQLAlchemy with optimized performance for concurrent operations.

## Features

- **Thread-safe operations** with worker thread pool
- **SQLAlchemy 2.0+ compatibility** with modern async patterns
- **Performance optimized** with SQLite-specific pragmas
- **Simple API** for common database operations
- **Consolidated operations** for both single and bulk operations
- **Batch SQL execution** for multiple statements in a single operation
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

db = DbEngine('sqlite:///my_database.db', 
              num_workers=1, 
              debug=False)

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

# Batch operations - execute multiple SQL statements
batch_sql = """
    -- Create a new table
    CREATE TABLE IF NOT EXISTS logs (
        id INTEGER PRIMARY KEY,
        message TEXT,
        timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
    );
    
    -- Insert multiple log entries
    INSERT INTO logs (message) VALUES ('Application started');
    INSERT INTO logs (message) VALUES ('User login successful');
    
    -- Query the logs
    SELECT * FROM logs ORDER BY timestamp DESC LIMIT 5;
    
    -- Update a log entry
    UPDATE logs SET message = 'Application started successfully' WHERE message = 'Application started';
"""
batch_results = db.batch(batch_sql)
print(f"Batch executed {len(batch_results)} statements")

# Cleanup
db.shutdown()
```

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
    {"operation": "execute", "query": "INSERT INTO users (name) VALUES (:name)", "params": {"name": "User1"}},
    {"operation": "fetch", "query": "SELECT COUNT(*) as count FROM users"}
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

##### batch(batch_sql, allow_select=True)
Execute multiple SQL statements in a batch with thread safety.

```python
batch_sql = """
    -- Create a table
    CREATE TABLE IF NOT EXISTS users (
        id INTEGER PRIMARY KEY,
        name TEXT NOT NULL
    );
    
    -- Insert some data
    INSERT INTO users (name) VALUES ('John');
    INSERT INTO users (name) VALUES ('Jane');
    
    -- Query the data (only if allow_select=True)
    SELECT * FROM users;
"""
results = db.batch(batch_sql)

# Process results
for result in results:
    print(f"Statement {result['statement_index']}: {result['type']} - {result.get('row_count', 'N/A')} rows")
    if result['type'] == 'fetch':
        print(f"  Data: {result['result']}")
```

**Parameters:**
- `batch_sql`: SQL string containing multiple statements separated by semicolons
- `allow_select`: If True, allows SELECT statements (default: True). If False, raises an error if SELECT statements are found

**Returns:**
List of dictionaries containing results for each statement:
- `statement_index`: Index of the statement in the batch
- `statement`: The actual SQL statement executed
- `type`: 'fetch', 'execute', or 'error'
- `result`: Query results (for SELECT) or True (for other operations)
- `row_count`: Number of rows affected/returned
- `error`: Error message (only for failed statements)

**Features:**
- Automatically removes SQL comments (-- and /* */)
- Handles semicolons within string literals
- Supports DDL (CREATE, ALTER, DROP) and DML (INSERT, UPDATE, DELETE) statements
- Continues execution even if individual statements fail
- Maintains transaction consistency across all statements

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
python tests/benchmark_db_engine.py --tests single bulk select scaling batch

# Customize parameters
python tests/benchmark_db_engine.py --operations 2000 --workers 2 --batch-sizes 100 500 1000
```

### Comprehensive Performance Tests

Run the full unittest suite for detailed performance analysis:

```bash
# Run all performance tests
python -m unittest tests.test_db_engine_performance -v
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

#### 4. Batch Performance
- Tests batch SQL execution with multiple statements
- Measures performance of mixed DDL/DML operations
- Tests different batch sizes and statement types
- **Expected**: >100 ops/sec for batch operations

#### 5. Concurrent Operations Performance
- Tests performance under concurrent load (1, 2, 4, 8 threads)
- Mix of read and write operations
- **Expected**: >50 ops/sec under load

#### 6. Transaction Performance
- Tests transaction operations with different sizes
- **Expected**: >50 ops/sec for transactions

#### 7. Worker Thread Scaling
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
| Batch Operations| >100 ops/sec      | <100ms avg       |
| Transactions   | >50 ops/sec       | <100ms avg       |
| Concurrent Ops | >50 ops/sec       | <100ms avg       |
| Single Worker  | >30 ops/sec       | <100ms avg       |

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

### [Unreleased]
- Performance improvements and optimizations
- Enhanced error handling and logging
- Additional performance testing scenarios

### 0.1.3 (2025-06-23)
- Thread-safe SQLite operations with worker thread pool
- SQLAlchemy 2.0+ compatibility with modern async patterns
- Performance optimizations with SQLite-specific pragmas
- Consolidated API with `execute()` method handling both single and bulk operations
- Transaction support for complex operations
- Statistics tracking for monitoring performance
- Extensive performance testing suite with benchmarks
- Memory usage monitoring (requires `psutil`)
- Thread safety through proper connection management
- WAL mode and optimized cache settings for better concurrency

### 0.2.0 (2025-06-25)
- **New batch SQL execution feature** for executing multiple SQL statements in a single operation
- **SQL statement parsing and validation** with automatic comment removal
- **Enhanced error handling** for batch operations with individual statement error reporting
- **Thread-safe batch processing** with proper connection management
- **Support for mixed DDL/DML operations** in batch mode
- **Automatic semicolon handling** within string literals and BEGIN...END blocks
- **Batch performance testing** and benchmarking tools
- **Improved SQL validation** with comprehensive statement type checking
- **Enhanced documentation** with batch operation examples and API reference
