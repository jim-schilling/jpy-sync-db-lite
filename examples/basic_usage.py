"""
Basic usage example for jpy-sync-db-lite.

Run:
  python examples/basic_usage.py
"""

from __future__ import annotations

from jpy_sync_db_lite.db_engine import DbEngine


def main() -> None:
	with DbEngine("sqlite:///example.db") as db:
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

		# Insert a record
		db.execute(
			"INSERT INTO users (name, email) VALUES (:name, :email)",
			params={"name": "Ada", "email": "ada@example.com"},
		)

		# Query it back
		res = db.fetch("SELECT * FROM users WHERE name = :n", params={"n": "Ada"})
		print(res.data)


if __name__ == "__main__":
	main()


