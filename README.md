# jpy-sync-db-lite

Jim's Python - Synchronous Database Wrapper for SQLite

A lightweight, thread-safe SQLite database wrapper built on SQLAlchemy with optimized performance for concurrent operations.

## Features

- **Thread-safe operations** via a single persistent connection protected by locks
- **SQLAlchemy 2.0+ compatibility** with modern async patterns
- **Performance optimized** with SQLite-specific pragmas
- **Simple API** for common database operations
- **Consolidated operations** for both single and bulk operations
- **Batch SQL execution** for multiple statements in a single operation
- **Transaction support** for complex operations
- **Statistics tracking** for monitoring performance
- **Robust SQL parsing** using sqlparse library for reliable statement parsing
- **SQLite-specific management** with VACUUM, ANALYZE, integrity checks, and PRAGMA configuration
- **Database optimization tools** for performance tuning and maintenance
- **Enhanced error handling** with SQLite-specific exception types

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
from jpy_sync_db_lite.db_engine import DbEngine

with DbEngine('sqlite:///my_database.db', debug=False) as db:

    # Get SQLite information
    sqlite_info = db.get_sqlite_info()
    print(f"SQLite version: {sqlite_info['version']}")
    print(f"Database size: {sqlite_info['database_size']} bytes")

    # Configure SQLite settings for better performance
    db.configure_pragma('cache_size', '-128000')
    db.configure_pragma('synchronous', 'NORMAL')

    # Create a table
    db.execute(
        """
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            email TEXT UNIQUE
        )
        """
    )

    # Insert single record
    db.execute(
        "INSERT INTO users (name, email) VALUES (:name, :email)",
        params={"name": "John Doe", "email": "john@example.com"}
    )

    # Fetch data
    users = db.fetch("SELECT * FROM users WHERE name = :name", params={"name": "John Doe"})
    print(users)

    # Run SQLite maintenance operations
    db.analyze()
    db.optimize()

    # Check database integrity
    issues = db.integrity_check()
    if issues:
        print(f"Integrity issues: {issues}")
    else:
        print("Database integrity check passed")

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

    # Optional: Run VACUUM for space reclamation (use sparingly)
    # db.vacuum()
```

### More examples

- Basic usage: `python examples/basic_usage.py`
- Transactions and batch: `python examples/transactions_and_batch.py`

## API Reference

### DbEngine

The main database engine class that manages connections and operations.

#### Constructor

```python
DbEngine(
    database_url: str,
    *,
    debug: bool = False,
    timeout: int = 30,
    check_same_thread: bool = False,
    enable_prepared_statements: bool = True,
)
```

**Parameters:**
- `database_url`: SQLAlchemy database URL (e.g., 'sqlite:///database.db')
- `debug`: Enable SQLAlchemy echo mode (default: False)
- `timeout`: SQLite connection timeout in seconds (default: 30)
- `check_same_thread`: SQLite thread safety check (default: False)
- `enable_prepared_statements`: Enable prepared statement caching (default: True)

#### Methods

##### execute(query, params=None)
Execute a non-query SQL statement (INSERT, UPDATE, DELETE, etc.). Handles both single operations and bulk operations.

```python
# Single operation
db.execute("UPDATE users SET name = :name WHERE id = :id", 
          {"name": "New Name", "id": 1})

# Bulk operation
updates = [{"id": 1, "status": "active"}, {"id": 2, "status": "inactive"}]
res = db.execute("UPDATE users SET status = :status WHERE id = :id", updates)
print(f"Updated {res.rowcount} users")
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
Also supported via context manager protocol.

```python
db.shutdown()

# or
with DbEngine('sqlite:///db.sqlite') as db:
    ...
```

##### get_sqlite_info()
Get SQLite-specific information and statistics.

```python
info = db.get_sqlite_info()
print(f"SQLite version: {info['version']}")
print(f"Database size: {info['database_size']} bytes")
print(f"Journal mode: {info['journal_mode']}")
print(f"Cache size: {info['cache_size']}")
```

**Returns:**
Dictionary containing SQLite information:
- `version`: SQLite version string
- `database_size`: Database file size in bytes (None for in-memory)
- `page_count`: Number of pages in database
- `page_size`: Page size in bytes
- `cache_size`: Current cache size
- `journal_mode`: Current journal mode (wal, delete, truncate, persist, memory, off)
- `synchronous`: Current synchronous mode (0=OFF, 1=NORMAL, 2=FULL)
- `temp_store`: Current temp store mode (0=DEFAULT, 1=FILE, 2=MEMORY)
- `mmap_size`: Memory map size in bytes
- `busy_timeout`: Busy timeout in milliseconds

##### configure_pragma(pragma_name, value)
Configure a specific SQLite PRAGMA setting.

```python
# Set cache size to 128MB
db.configure_pragma('cache_size', '-128000')

# Set synchronous mode to FULL for maximum durability
db.configure_pragma('synchronous', 'FULL')

# Set busy timeout to 60 seconds
db.configure_pragma('busy_timeout', '60000')
```

**Parameters:**
- `pragma_name`: Name of the PRAGMA (e.g., 'cache_size', 'synchronous', 'busy_timeout')
- `value`: Value to set for the PRAGMA

##### vacuum()
Perform SQLite VACUUM operation to reclaim space and optimize database.

```python
# Reclaim space and optimize database
db.vacuum()
```

**Note:** VACUUM requires exclusive access to the database and may take time for large databases.

##### analyze(table_name=None)
Update SQLite query planner statistics for better query performance.

```python
# Analyze all tables
db.analyze()

# Analyze specific table
db.analyze('users')
```

**Parameters:**
- `table_name`: Specific table to analyze (None for all tables)

##### integrity_check()
Perform SQLite integrity check and return any issues found.

```python
issues = db.integrity_check()
if issues:
    print(f"Database integrity issues found: {issues}")
else:
    print("Database integrity check passed")
```

**Returns:**
List of integrity issues (empty list if database is healthy)

##### optimize()
Run SQLite optimization commands for better performance.

```python
# Run optimization commands
db.optimize()
```

This method runs `PRAGMA optimize` and `ANALYZE` to improve query performance.

##### batch(batch_sql)
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
for i, result in enumerate(results):
    print(f"Statement {i}: {result['operation']} - {result.get('row_count', 'N/A')} rows")
    if result['operation'] == 'fetch':
        print(f"  Data: {result['result']}")
```

**Parameters:**
- `batch_sql`: SQL string containing multiple statements separated by semicolons

**Returns:**
List of dictionaries containing results for each statement:
- `statement`: The actual SQL statement executed
- `operation`: 'fetch' or 'execute'
- `result`: DbResult for each statement (fetch has data populated, execute has rowcount)

**Features:**
- **Robust SQL parsing** using sqlparse library for reliable statement parsing
- Automatically removes SQL comments (-- and /* */) while preserving comments within string literals
- Handles semicolons within string literals and complex SQL constructs
- Supports DDL (CREATE, ALTER, DROP) and DML (INSERT, UPDATE, DELETE) statements
- Continues execution even if individual statements fail
- Maintains transaction consistency across all statements
- Enhanced support for complex SQL constructs including triggers and BEGIN...END blocks

## Performance Optimizations

The library includes several SQLite-specific optimizations:

- **WAL mode** for better concurrency
- **Configurable cache settings** (via PRAGMA)
- **Memory-mapped files** (via PRAGMA)
- **Query planner optimization**
- **Static connection pooling**

## Thread Safety

All operations are thread-safe via a single persistent connection protected by locks.
Requests are executed serially to ensure SQLite correctness, while remaining safe to call from multiple threads.

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
- Tests different batch sizes (10, 50, 100, 250 records)
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

#### 7. Benchmark-like Behavioral Checks
- Lightweight concurrency and throughput checks derived from the standalone benchmark
- Runs quickly in CI while validating behavior under threaded access

### Performance Metrics

The tests measure:

- **Throughput**: Operations per second (ops/sec)
- **Latency**: Time per operation in milliseconds
- **Memory Usage**: Memory consumption and growth rate (optional, requires `psutil`)
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


### Optimization Recommendations

The performance tests provide recommendations for:
- **Optimal batch sizes** for bulk operations
- **Optimal worker threads** for your workload
- **Memory efficiency** analysis
- **Scaling considerations** for concurrent operations

### Memory Monitoring (Optional)

Memory usage monitoring is optional and requires the `psutil` package:

```bash
pip install psutil
```

**Note**: `psutil` is not a dependency of this package. Without `psutil`, the tests will run normally but skip memory measurements.

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

### 0.4.0 (2025-08-13)
- Simplified connection configuration: PRAGMAs applied on the persistent connection for correctness
- Transaction error signaling simplified to a single error response for failures
- Prepared statement stats updates are thread-safe
- Added context manager support to `DbEngine` for automatic shutdown
- Introduced local `jpy_sync_db_lite.errors` with `SqlFileError` and `SqlValidationError`
- Tests focus on behavior over implementation and use real SQLite (no mocks)
- README Quick Start now uses context manager and corrected API examples
- Removed background worker/queue in favor of synchronous execution with a single persistent connection
- Added examples under `examples/` and a new benchmark-like test suite for concurrent behavior

### 0.3.1 (2025-07-11)
- **Dead code elimination** with removal of unused constants, methods, and imports from the database engine
- **Code cleanup** with removal of `_BATCH_STATEMENT`, `_SUCCESS`, `_ERROR`, and `_ERROR_COMMIT_FAILED` unused constants
- **Method cleanup** with removal of unused `_acquire_db_lock` context manager method (~45 lines of dead code)
- **Import optimization** with removal of unused `time` import from db_engine.py
- **Code maintainability improvements** with elimination of ~50 lines of unused code
- **Enhanced code quality** with cleaner, more focused database engine implementation
- **Better code organization** with removal of redundant and unused code elements

### 0.3.0 (2025-07-07)
- **Comprehensive test suite cleanup and optimization** with removal of all debug and extraneous print statements from test files
- **Enhanced SQL helper test coverage** with 95 comprehensive tests covering edge cases, error handling, and boundary conditions
- **Improved SQL statement type detection** with robust CTE (Common Table Expression) parsing and handling
- **Enhanced SQL parsing robustness** with better handling of invalid SQL statements and edge cases
- **Comprehensive edge case testing** for SQL helper functions including malformed SQL, nested comments, and complex CTE scenarios
- **Performance testing improvements** with optimized test execution and better coverage of SQL parsing performance
- **Enhanced error handling** for SQL parsing edge cases including incomplete comments, malformed statements, and invalid file paths
- **Improved test maintainability** with cleaner test structure and removal of debug output
- **Better SQL statement type detection** for complex scenarios including:
  - CTEs with no main statement (invalid SQL handling)
  - Multiple CTEs with complex nesting
  - CTEs with unknown statement types after them
  - Complex parentheses and nested structures in CTEs
  - Window functions, JSON operations, and recursive CTEs
- **Enhanced SQL parsing edge cases** including:
  - Empty statements and whitespace-only input
  - Statements with only comments
  - Malformed SQL with unclosed strings or comments
  - Very long SQL statements and complex nesting
  - String literals containing SQL keywords or semicolons
- **Improved file handling** for SQL file operations with comprehensive error handling for invalid paths and file operations
- **Enhanced integration testing** with full SQL processing pipeline tests and batch processing scenarios
- **Better test categorization** with unit, integration, performance, and coverage test classifications
- **Comprehensive performance benchmarking** for SQL parsing operations with realistic workload testing
- **Code quality improvements** with 90% test coverage for sql_helper.py and robust error handling patterns
- **Documentation updates** reflecting current test coverage and API behavior expectations

### 0.2.7 (2025-06-29)
- **Enhanced project configuration** with updated setuptools and setuptools-scm for better version management
- **Improved dependency management** with specific version constraints for all development and testing dependencies
- **Enhanced development tooling** with comprehensive linting, formatting, and type checking configurations (ruff, black, isort, mypy, bandit)
- **Better test infrastructure** with enhanced pytest configuration, coverage reporting, and test categorization
- **Documentation improvements** with updated API examples and corrected return type documentation for batch operations
- **Code quality enhancements** with improved logging and error handling in SQLite operations
- **Enhanced test coverage** for performance and integration scenarios with robust validation of new features
- **Project metadata improvements** with additional classifiers, keywords, and better package discovery

### 0.2.6 (2025-06-29)
- **Enhanced input validation for `split_sql_file()` function** with proper handling of invalid path types
- **Improved error handling** for `None`, empty strings, and non-string/non-Path objects in file path parameters
- **Better type safety** with explicit validation of file path parameters before processing
- **Consistent error messaging** with descriptive ValueError messages for invalid inputs
- **Enhanced robustness** of SQL file processing with comprehensive input validation
- **Test coverage improvements** with edge case testing for invalid file path scenarios

### 0.2.5 (2025-06-28)
- **Enhanced error handling for database maintenance operations** with proper exception wrapping and rollback support
- **Improved robustness of maintenance methods** (`vacuum`, `analyze`, `integrity_check`, `optimize`) with try-catch blocks
- **Better error messages** for maintenance operations with descriptive failure descriptions
- **Comprehensive test coverage** for error handling scenarios in maintenance operations
- **Consistent error handling patterns** across all database maintenance methods
- **Enhanced SQLite-specific functionality** with comprehensive database management features
- **New `get_sqlite_info()` method** to retrieve SQLite version, database statistics, and PRAGMA values
- **New `configure_pragma()` method** for dynamic SQLite PRAGMA configuration (cache_size, synchronous, etc.)
- **New `vacuum()` method** for database space reclamation and optimization
- **New `analyze()` method** for updating query planner statistics (all tables or specific table)
- **New `integrity_check()` method** for database integrity verification
- **New `optimize()` method** for running SQLite optimization commands
- **Enhanced engine configuration** with SQLite-specific connection parameters (timeout, check_same_thread)
- **Improved transaction support** with proper isolation level configuration (DEFERRED mode)
- **Enhanced performance configuration** with additional SQLite pragmas (foreign_keys, busy_timeout, auto_vacuum)
- **Comprehensive SQLite-specific test suite** with 16 new test methods covering all new functionality
- **Better error handling** with SQLiteError exception class for SQLite-specific errors
- **Documentation updates** with complete API reference for all new SQLite-specific methods
- **Performance optimizations** with enhanced SQLite pragma settings for better concurrency and reliability

### 0.2.4 (2025-06-27)
- **Test suite refactoring** with removal of private function tests to focus on public API testing
- **Improved test maintainability** by eliminating tests for internal implementation details
- **Enhanced nested comment handling** in SQL parsing with more realistic expectations
- **Better test coverage** focusing on public interface behavior rather than implementation details
- **Code quality improvements** with cleaner test structure and more maintainable test suite
- **Documentation updates** reflecting current test coverage and API expectations

### 0.2.3 (2025-06-27)
- **Enhanced thread safety and concurrency** with improved locking mechanisms and connection management
- **Optimized database engine performance** with refined worker thread handling and request processing
- **Improved SQL statement parsing** with better support for complex SQL constructs and edge cases
- **Enhanced error handling and recovery** with more robust exception management and detailed error reporting
- **Code quality improvements** with comprehensive test coverage and performance benchmarking
- **Memory usage optimizations** with better resource management and cleanup procedures
- **Documentation enhancements** with improved API documentation and usage examples

### 0.2.2 (2025-06-26)
- **Code refactoring and architectural improvements** for better maintainability and performance
- **Enhanced error handling and logging** with more detailed exception information
- **Optimized database performance** with refined SQLite pragma configurations
- **Enhanced SQL parsing robustness** with better handling of edge cases and malformed SQL
- **Code documentation improvements** with more detailed docstrings and usage examples

### 0.2.1 (2025-06-26)
- **Refactored SQL parsing to use sqlparse library** for improved reliability and standards compliance
- **Enhanced SQL comment removal** with proper handling of comments within string literals
- **Improved SQL statement parsing** with better handling of complex SQL constructs including BEGIN...END blocks
- **Added sqlparse dependency** for robust SQL parsing and formatting
- **Improved error handling** for malformed SQL statements
- **Better support for complex SQL constructs** including triggers, stored procedures, and multi-line statements

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

### 0.1.3 (2025-06-23)
- Thread-safe SQLite operations with worker thread pool
- SQLAlchemy 2.0+ compatibility with modern async patterns
- Performance optimizations with SQLite-specific pragmas
- Consolidated API with `execute()` method handling both single and bulk operations
- Transaction support for complex operations
- Statistics tracking for monitoring performance
- Extensive performance testing suite with benchmarks
- Memory usage monitoring (optional, requires `psutil`)
- Thread safety through proper connection management
- WAL mode and optimized cache settings for better concurrency

