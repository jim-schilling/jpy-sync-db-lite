# jpy-sync-db-lite — Detailed Documentation

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
    db.execute(
        """
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            email TEXT UNIQUE
        )
        """
    )

    db.execute(
        "INSERT INTO users (name, email) VALUES (:name, :email)",
        params={"name": "John Doe", "email": "john@example.com"}
    )

    users = db.fetch("SELECT * FROM users WHERE name = :name", params={"name": "John Doe"})
    print(users)
```

More examples:
- `python examples/basic_usage.py`
- `python examples/transactions_and_batch.py`

## API Reference (Selected)

### DbEngine constructor
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

Key methods: `execute`, `fetch`, `execute_transaction`, `batch`, `get_sqlite_info`, `configure_pragma`, `vacuum`, `analyze`, `integrity_check`, `optimize`, `get_performance_info`, `get_connection_info`, `check_connection_health`, `get_prepared_statement_count`, `clear_prepared_statements`.

See inline docstrings and examples in `examples/` for more.

## Performance & Thread Safety
- Single persistent connection with locking for correctness and concurrency.
- WAL mode and PRAGMA tuning for performance.
- Batch execution and transactions supported.

## Testing

Run tests:
```bash
pytest -n auto --cov=jpy_sync_db_lite --cov-report=term-missing
```

Performance tests:
```bash
python -m unittest tests.test_db_engine_performance -v
```

## Development
- Type checking: `mypy`
- Linting: `ruff`, `isort`
- Security: `bandit`

## License
MIT. See `LICENSE`.
