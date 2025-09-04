# Changelog

All notable changes to this project will be documented in this file.

## 2025.4.1 (2025-08-19)
- **Comprehensive test suite cleanup and refactoring** with removal of implementation-dependent tests and focus on behavior-only testing
- **Enhanced test isolation** with parallel-safe test execution using unique database files and in-memory databases
- **Improved code coverage** with db_engine.py coverage increased to 90% and sql_helper.py coverage increased to 85%
- **Behavior-focused testing** with removal of internal attribute assertions and SQL string equality checks
- **Parallel test execution support** with all 187 tests passing in parallel using pytest-xdist
- **Enhanced test maintainability** with removal of duplicate test files and consolidation of test suites
- **Better test organization** with clear separation of unit, integration, and performance tests
- **Improved error handling coverage** with comprehensive testing of edge cases and error conditions
- **Enhanced SQL helper testing** with 15 new tests covering complex CTEs, statement variants, and parsing edge cases
- **Database engine coverage improvements** with 15 new tests covering prepared statements, connection health, and performance metrics
- **Test infrastructure improvements** with removal of sys.path modifications and print statements
- **Documentation updates** reflecting current test coverage and behavior expectations

## 2025.4.0 (2025-08-13)
- Simplified connection configuration: PRAGMAs applied on the persistent connection for correctness
- Transaction error signaling simplified to a single error response for failures
- Prepared statement stats updates are thread-safe
- Added context manager support to `DbEngine` for automatic shutdown
- Introduced local `jpy_sync_db_lite.errors` with `SqlFileError` and `SqlValidationError`
- Tests focus on behavior over implementation and use real SQLite (no mocks)
- README Quick Start now uses context manager and corrected API examples
- Removed background worker/queue in favor of synchronous execution with a single persistent connection
- Added examples under `examples/` and a new benchmark-like test suite for concurrent behavior

## 0.3.1 (2025-07-11)
- **Dead code elimination** with removal of unused constants, methods, and imports from the database engine
- **Code cleanup** with removal of `_BATCH_STATEMENT`, `_SUCCESS`, `_ERROR`, and `_ERROR_COMMIT_FAILED` unused constants
- **Method cleanup** with removal of unused `_acquire_db_lock` context manager method (~45 lines of dead code)
- **Import optimization** with removal of unused `time` import from db_engine.py
- **Code maintainability improvements** with elimination of ~50 lines of unused code
- **Enhanced code quality** with cleaner, more focused database engine implementation
- **Better code organization** with removal of redundant and unused code elements

## 0.3.0 (2025-07-07)
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

## 0.2.7 (2025-06-29)
- **Enhanced project configuration** with updated setuptools and setuptools-scm for better version management
- **Improved dependency management** with specific version constraints for all development and testing dependencies
- **Enhanced development tooling** with comprehensive linting, formatting, and type checking configurations (ruff, black, isort, mypy, bandit)
- **Better test infrastructure** with enhanced pytest configuration, coverage reporting, and test categorization
- **Documentation improvements** with updated API examples and corrected return type documentation for batch operations
- **Code quality enhancements** with improved logging and error handling in SQLite operations
- **Enhanced test coverage** for performance and integration scenarios with robust validation of new features
- **Project metadata improvements** with additional classifiers, keywords, and better package discovery

## 0.2.6 (2025-06-29)
- **Enhanced input validation for `split_sql_file()` function** with proper handling of invalid path types
- **Improved error handling** for `None`, empty strings, and non-string/non-Path objects in file path parameters
- **Better type safety** with explicit validation of file path parameters before processing
- **Consistent error messaging** with descriptive ValueError messages for invalid inputs
- **Enhanced robustness** of SQL file processing with comprehensive input validation
- **Test coverage improvements** with edge case testing for invalid file path scenarios

## 0.2.5 (2025-06-28)
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

## 0.2.4 (2025-06-27)
- **Test suite refactoring** with removal of private function tests to focus on public API testing
- **Improved test maintainability** by eliminating tests for internal implementation details
- **Enhanced nested comment handling** in SQL parsing with more realistic expectations
- **Better test coverage** focusing on public interface behavior rather than implementation details
- **Code quality improvements** with cleaner test structure and more maintainable test suite
- **Documentation updates** reflecting current test coverage and API expectations

## 0.2.3 (2025-06-27)
- **Enhanced thread safety and concurrency** with improved locking mechanisms and connection management
- **Optimized database engine performance** with refined worker thread handling and request processing
- **Improved SQL statement parsing** with better support for complex SQL constructs and edge cases
- **Enhanced error handling and recovery** with more robust exception management and detailed error reporting
- **Code quality improvements** with comprehensive test coverage and performance benchmarking
- **Memory usage optimizations** with better resource management and cleanup procedures
- **Documentation enhancements** with improved API documentation and usage examples

## 0.2.2 (2025-06-26)
- **Code refactoring and architectural improvements** for better maintainability and performance
- **Enhanced error handling and logging** with more detailed exception information
- **Optimized database performance** with refined SQLite pragma configurations
- **Enhanced SQL parsing robustness** with better handling of edge cases and malformed SQL
- **Code documentation improvements** with more detailed docstrings and usage examples

## 0.2.1 (2025-06-26)
- **Refactored SQL parsing to use sqlparse library** for improved reliability and standards compliance
- **Enhanced SQL comment removal** with proper handling of comments within string literals
- **Improved SQL statement parsing** with better handling of complex SQL constructs including BEGIN...END blocks
- **Added sqlparse dependency** for robust SQL parsing and formatting
- **Improved error handling** for malformed SQL statements
- **Better support for complex SQL constructs** including triggers, stored procedures, and multi-line statements

## 0.2.0 (2025-06-25)
- **New batch SQL execution feature** for executing multiple SQL statements in a single operation
- **SQL statement parsing and validation** with automatic comment removal
- **Enhanced error handling** for batch operations with individual statement error reporting
- **Thread-safe batch processing** with proper connection management
- **Support for mixed DDL/DML operations** in batch mode
- **Automatic semicolon handling** within string literals and BEGIN...END blocks
- **Batch performance testing** and benchmarking tools
- **Improved SQL validation** with comprehensive statement type checking
- **Enhanced documentation** with batch operation examples and API reference

## 0.1.3 (2025-06-23)
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
