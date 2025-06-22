# DbEngine Performance Tests

This directory contains comprehensive performance tests for the DbEngine class.

## Files

- `test_db_engine_performance.py` - Comprehensive unittest-based performance tests
- `benchmark_db_engine.py` - Standalone benchmark script for quick performance testing

## Running Performance Tests

### Using unittest (Comprehensive)

```bash
# Run all performance tests
python -m unittest tests.test_db_engine_performance -v

# Run specific test
python -m unittest tests.test_db_engine_performance.TestDbEnginePerformance.test_single_insert_performance -v
```

### Using Standalone Benchmark Script

```bash
# Run all benchmarks
python tests/benchmark_db_engine.py

# Run specific tests
python tests/benchmark_db_engine.py --tests single bulk select scaling

# Customize parameters
python tests/benchmark_db_engine.py --operations 2000 --workers 2 --batch-sizes 100 500 1000
```

## Test Categories

### 1. Single Insert Performance
- Measures performance of individual insert operations
- Tests latency and throughput for single record inserts
- **Expected**: >50 ops/sec, <100ms average latency

### 2. Bulk Insert Performance
- Tests performance with different batch sizes (10, 50, 100, 500, 1000)
- Measures throughput and per-record latency
- **Expected**: >100 ops/sec for optimal batch sizes

### 3. Select Performance
- Tests different query types:
  - Simple SELECT with LIMIT
  - Filtered SELECT with WHERE clause
  - Indexed SELECT using indexed columns
  - Aggregate SELECT with COUNT/AVG
  - Complex SELECT with multiple conditions and ORDER BY
- **Expected**: >200 ops/sec for simple selects

### 4. Concurrent Operations Performance
- Tests performance under concurrent load (1, 2, 4, 8 threads)
- Mix of read and write operations
- **Expected**: >50 ops/sec under load

### 5. Transaction Performance
- Tests performance of transaction operations
- Different transaction sizes (10, 50, 100, 500 operations)
- **Expected**: >50 ops/sec for transactions

### 6. Worker Thread Scaling
- Tests performance with different worker thread configurations
- Helps determine optimal worker count
- **Expected**: >30 ops/sec with single worker

## Performance Metrics

### Throughput
- Operations per second (ops/sec)
- Higher is better
- Measured for each operation type

### Latency
- Time per operation in milliseconds
- Lower is better
- Statistics: min, max, mean, median, standard deviation

### Memory Usage
- Memory consumption during operations
- Growth rate per operation
- Helps identify memory leaks

## Performance Expectations

Based on SQLite with WAL mode and optimized pragmas:

| Operation Type | Expected Throughput | Expected Latency |
|----------------|-------------------|------------------|
| Single Insert  | >50 ops/sec       | <100ms avg       |
| Bulk Insert    | >100 ops/sec      | <50ms per record |
| Simple Select  | >200 ops/sec      | <10ms avg        |
| Complex Select | >50 ops/sec       | <50ms avg        |
| Transactions   | >50 ops/sec       | <100ms avg       |

## Optimization Recommendations

The tests provide recommendations for:
- Optimal batch sizes for bulk operations
- Optimal number of worker threads
- Memory efficiency analysis

## Interpreting Results

### Good Performance
- Throughput meets or exceeds expectations
- Latency is consistent and low
- Memory usage is stable

### Performance Issues
- Low throughput: Consider batch operations or worker scaling
- High latency: Check for blocking operations or resource contention
- Memory growth: Potential memory leaks or inefficient queries

### Scaling Considerations
- SQLite has limitations with concurrent writes
- Multiple workers may not improve performance for write-heavy workloads
- Read operations scale better with multiple workers

## Environment Considerations

Performance may vary based on:
- Hardware specifications (CPU, RAM, storage)
- Operating system and file system
- SQLite version and configuration
- System load and available resources

## Troubleshooting

### Common Issues
1. **Low throughput**: Check if database file is on fast storage
2. **High latency**: Monitor system resources (CPU, memory, disk I/O)
3. **Memory growth**: Look for unclosed connections or large result sets
4. **Test failures**: Ensure sufficient disk space and permissions

### Debug Mode
Enable debug mode for detailed SQL logging:
```python
db_engine = DbEngine(database_url, debug=True)
```

## Continuous Performance Monitoring

Consider integrating these tests into CI/CD pipelines to:
- Track performance regressions
- Monitor performance trends over time
- Ensure performance requirements are met
- Alert on performance degradation 