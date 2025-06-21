# Tests for jpy-sync-db-lite

This directory contains comprehensive unit tests for the `jpy-sync-db-lite` package.

## Test Coverage

The test suite covers:

### Core Functionality
- Database engine initialization with various parameters
- SQL query execution (execute, fetch, bulk_insert, bulk_update)
- Transaction handling with rollback on errors
- Raw connection management
- Performance statistics collection

### Threading and Concurrency
- Multi-threaded database operations
- Thread safety verification
- Concurrent read/write operations
- Worker thread management

### Error Handling
- Invalid SQL statements
- Missing database tables
- Missing query parameters
- Database connection errors
- File permission issues

### Edge Cases
- Empty parameter handling
- Unicode and special character support
- Large bulk operations
- Mixed operation types in transactions
- Database performance configuration

### Performance Testing
- Large dataset bulk operations
- Concurrent operation stress testing
- Database performance pragma verification

## Running Tests

### Using the Test Runner
```bash
# From the project root directory
python tests/run_tests.py
```

### Using unittest directly
```bash
# From the project root directory
python -m unittest discover tests -v
```

### Running specific test classes
```bash
# Run only the main DbEngine tests
python -m unittest tests.test_db_engine.TestDbEngine -v

# Run only edge case tests
python -m unittest tests.test_db_engine.TestDbEngineEdgeCases -v
```

### Running specific test methods
```bash
# Run a specific test method
python -m unittest tests.test_db_engine.TestDbEngine.test_fetch_simple_query -v
```

## Test Structure

### TestDbEngine
Main test class covering all core functionality:
- `test_init_with_default_parameters`: Engine initialization
- `test_execute_simple_query`: Basic query execution
- `test_fetch_simple_query`: Data retrieval
- `test_bulk_insert`: Bulk insert operations
- `test_bulk_update`: Bulk update operations
- `test_execute_transaction_success`: Successful transactions
- `test_execute_transaction_rollback`: Transaction rollback
- `test_concurrent_operations`: Threading tests
- `test_large_bulk_operations`: Performance tests
- `test_unicode_and_special_characters`: Character encoding tests

### TestDbEngineEdgeCases
Edge case and error condition tests:
- `test_empty_parameters`: Empty parameter handling
- `test_none_response_queue`: Null response queue handling
- `test_invalid_operation_type`: Invalid operation handling
- `test_database_file_permissions`: File permission issues

## Test Dependencies

The tests require:
- Python 3.7+
- SQLAlchemy
- sqlite3 (built-in)
- tempfile (built-in)
- threading (built-in)
- queue (built-in)
- unittest (built-in)

## Test Environment

Tests use temporary SQLite database files that are automatically cleaned up after each test. Each test method runs in isolation with its own database instance.

## Continuous Integration

The test suite is designed to run in CI/CD environments and provides:
- Detailed output for debugging
- Proper exit codes for CI systems
- Comprehensive error reporting
- Performance benchmarks

## Adding New Tests

When adding new tests:

1. Follow the existing naming convention: `test_<method_name>_<scenario>`
2. Use descriptive test method names
3. Include proper setup and teardown
4. Test both success and failure scenarios
5. Add edge cases where appropriate
6. Update this README if adding new test categories 