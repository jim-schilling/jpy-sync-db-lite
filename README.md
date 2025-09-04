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

## Quick Start

```python
from jpy_sync_db_lite.db_engine import DbEngine

with DbEngine('sqlite:///my_database.db') as db:
    # Create table
    db.execute("""
        CREATE TABLE users (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            email TEXT UNIQUE
        )
    """)

    # Insert data
    db.execute(
        "INSERT INTO users (name, email) VALUES (:name, :email)",
        {"name": "John Doe", "email": "john@example.com"}
    )

    # Query data
    users = db.fetch("SELECT * FROM users")
    print(users)
```

## Documentation

For detailed documentation including API reference, installation instructions, and examples, see [docs/README-details.md](docs/README-details.md).
