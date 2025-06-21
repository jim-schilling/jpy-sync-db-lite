# jpy-sync-db-lite

Jim's Python - Synchronous Database Wrapper for SQLite

A lightweight, thread-safe SQLite database wrapper built on SQLAlchemy with optimized performance for concurrent operations.

## Features

- **Thread-safe operations** with worker thread pool
- **SQLAlchemy 2.0+ compatibility** with modern async patterns
- **Performance optimized** with SQLite-specific pragmas
- **Simple API** for common database operations
- **Bulk operations** for efficient data handling
- **Transaction support** for complex operations
- **Statistics tracking** for monitoring performance

## Installation

### From PyPI (when published)
```bash
pip install jpy-sync-db-lite
```

### From source
```bash
git clone https://github.com/jimschilling/jpy-sync-db-lite.git
cd jpy-sync-db-lite
pip install -e .
```

### Development setup
```bash
git clone https://github.com/jimschilling/jpy-sync-db-lite.git
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

# Insert data
db.execute(
    "INSERT INTO users (name, email) VALUES (:name, :email)",
    {"name": "John Doe", "email": "john@example.com"}
)

# Fetch data
users = db.fetch("SELECT * FROM users WHERE name = :name", {"name": "John Doe"})
print(users)  # [{'id': 1, 'name': 'John Doe', 'email': 'john@example.com'}]

# Bulk insert
users_data = [
    {"name": "Jane Smith", "email": "jane@example.com"},
    {"name": "Bob Johnson", "email": "bob@example.com"}
]
db.bulk_insert(
    "INSERT INTO users (name, email) VALUES (:name, :email)",
    users_data
)

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
Execute a non-query SQL statement (INSERT, UPDATE, DELETE, etc.).

```python
db.execute("UPDATE users SET name = :name WHERE id = :id", 
          {"name": "New Name", "id": 1})
```

##### fetch(query, params=None)
Execute a SELECT query and return results as a list of dictionaries.

```python
results = db.fetch("SELECT * FROM users WHERE age > :min_age", 
                  {"min_age": 18})
```

##### bulk_insert(query, params_list)
Perform efficient bulk insert operations.

```python
data = [{"name": "User1"}, {"name": "User2"}]
db.bulk_insert("INSERT INTO users (name) VALUES (:name)", data)
```

##### bulk_update(query, params_list)
Perform efficient bulk update operations.

```python
updates = [{"id": 1, "status": "active"}, {"id": 2, "status": "inactive"}]
db.bulk_update("UPDATE users SET status = :status WHERE id = :id", updates)
```

##### execute_transaction(operations)
Execute multiple operations in a single transaction.

```python
operations = [
    {"query": "INSERT INTO users (name) VALUES (:name)", "params": {"name": "User1"}},
    {"query": "UPDATE counters SET user_count = user_count + 1", "params": None}
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

## Development

### Running Tests
```bash
pytest
```

### Code Formatting
```bash
black .
isort .
```

### Type Checking
```bash
mypy jpy_sync_db_lite/
```

### Linting
```bash
flake8 jpy_sync_db_lite/
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
