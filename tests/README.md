# Tests for jpy-sync-db-lite

This directory contains comprehensive unit tests for the `jpy-sync-db-lite` package.

## Test Coverage

The test suite provides extensive coverage across all major components:

### Core Database Functionality
- Database engine initialization with various parameters
- SQL query execution (`execute`, `fetch`, bulk operations)
- Transaction handling with rollback on errors
- Raw connection management
- Performance statistics collection
- Connection pool configuration and management

### Threading and Concurrency
- Multi-threaded database operations
- Thread safety verification with locks
- Concurrent read/write operations
- Worker thread management and shutdown
- Thread interference prevention

### Backup System
- Manual backup requests
- Periodic backup thread functionality
- Backup file integrity verification
- Backup cleanup and retention policies
- Backup thread enable/disable functionality
- Concurrent backup request handling

### Error Handling and Edge Cases
- Invalid SQL statements
- Missing database tables
- Missing query parameters
- Database connection errors
- File permission issues
- Empty parameter handling
- Unicode and special character support
- Large bulk operations (1000+ records)
- Mixed operation types in transactions

### Performance and Configuration
- Database performance pragma verification (WAL, cache, temp_store)
- Large dataset bulk operations
- Concurrent operation stress testing
- Database performance optimization testing
- Connection pool behavior verification

### DbRequest Class Testing
- Request object creation and validation
- Parameter handling (dict and list formats)
- Timestamp generation
- Batch ID support
- Response queue integration

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

# Run only backup functionality tests
python -m unittest tests.test_db_engine.TestDbEngineBackup -v

# Run only DbRequest tests
python -m unittest tests.test_db_engine.TestDbRequest -v
```

### Running specific test methods
```bash
# Run a specific test method
python -m unittest tests.test_db_engine.TestDbEngine.test_fetch_simple_query -v

# Run backup-related tests
python -m unittest tests.test_db_engine.TestDbEngine.test_simple_backup_request -v
python -m unittest tests.test_db_engine.TestDbEngineBackup.test_backup_cleanup_functionality -v
```

## Test Structure

### TestDbEngine (Main Test Class)
Core functionality tests:
- **Initialization**: `test_init_with_default_parameters`, `test_init_with_custom_parameters`
- **Database Operations**: `test_execute_simple_query`, `test_fetch_simple_query`, `test_bulk_operations`
- **Transactions**: `test_execute_transaction_success`, `test_execute_transaction_rollback`
- **Concurrency**: `test_concurrent_operations`, `test_mixed_operation_types`
- **Performance**: `test_large_bulk_operations`, `test_configure_db_performance`
- **Backup**: `test_simple_backup_request`, `test_backup_file_integrity`, `test_periodic_backup_thread`
- **Connection Management**: `test_get_raw_connection`, `test_connection_pool_configuration`
- **Statistics**: `test_get_stats`, `test_backup_stats_tracking`

### TestDbEngineEdgeCases
Edge case and error condition tests:
- `test_empty_parameters`: Empty parameter handling
- `test_none_response_queue`: Null response queue handling
- `test_invalid_operation_type`: Invalid operation handling
- `test_database_file_permissions`: File permission issues

### TestDbEngineBackup
Comprehensive backup system testing:
- `test_backup_initialization`: Backup configuration verification
- `test_manual_backup_request`: Manual backup functionality
- `test_backup_stats_tracking`: Backup statistics
- `test_backup_info_method`: Backup information retrieval
- `test_backup_thread_disabled`: Thread enable/disable
- `test_backup_cleanup_functionality`: Backup cleanup and retention
- `test_backup_concurrent_requests`: Concurrent backup handling

### TestDbRequest
DbRequest class testing:
- `test_db_request_creation`: Object creation and validation
- `test_db_request_timestamp`: Timestamp generation
- `test_db_request_with_list_params`: List parameter handling

## Test Features

### Isolation and Cleanup
- Each test uses temporary SQLite database files
- Automatic cleanup after each test method
- Thread-safe test execution
- Backup thread disabled by default to prevent interference

### Performance Testing
- Large bulk operations (1000+ records)
- Concurrent operation stress testing
- Database performance pragma verification
- Connection pool behavior testing

### Backup System Testing
- Manual and automatic backup creation
- Backup file integrity verification
- Cleanup and retention policy testing
- Thread enable/disable functionality
- Concurrent backup request handling

### Error Simulation
- Invalid SQL statements
- Missing database tables
- File permission issues
- Network connection problems
- Thread interruption scenarios

## Test Dependencies

The tests require:
- Python 3.10-3.12
- SQLAlchemy
- sqlite3 (built-in)
- tempfile (built-in)
- threading (built-in)
- queue (built-in)
- unittest (built-in)
- time (built-in)
- os (built-in)
- shutil (built-in)

## Test Environment

- **Database**: Temporary SQLite files with automatic cleanup
- **Backup Directory**: Temporary directories for backup testing
- **Threading**: Controlled thread execution with proper shutdown
- **Performance**: Optimized for fast execution while maintaining coverage

## Continuous Integration

The test suite is designed for CI/CD environments:
- Detailed output for debugging
- Proper exit codes for CI systems
- Comprehensive error reporting
- Performance benchmarks
- Thread-safe execution
- Resource cleanup verification

## Adding New Tests

When adding new tests:

1. **Follow naming convention**: `test_<method_name>_<scenario>`
2. **Use descriptive names**: Clear indication of what is being tested
3. **Include proper setup/teardown**: Ensure test isolation
4. **Test both success and failure**: Cover edge cases
5. **Add to appropriate test class**: Group related functionality
6. **Update this README**: Document new test categories
7. **Consider performance**: Tests should run quickly
8. **Thread safety**: Ensure tests don't interfere with each other

## Test Categories Summary

| Category | Test Count | Coverage |
|----------|------------|----------|
| Core Database Operations | 15+ | ✅ Complete |
| Threading & Concurrency | 5+ | ✅ Complete |
| Backup System | 8+ | ✅ Complete |
| Error Handling | 6+ | ✅ Complete |
| Performance Testing | 4+ | ✅ Complete |
| Edge Cases | 4+ | ✅ Complete |
| DbRequest Class | 3+ | ✅ Complete |

**Total Test Coverage**: ~45+ comprehensive test methods covering all major functionality and edge cases. 

## Changelog

### [0.2.8] 2025-07-07