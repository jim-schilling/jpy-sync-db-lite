"""
Basic usage example for jpy-sync-db-lite.

Run:
  python examples/basic_usage.py
"""

from __future__ import annotations
import tempfile

from jpy_sync_db_lite.db_engine import DbEngine


def main() -> None:
	# Use a temporary database to avoid conflicts between runs
	temp_db = tempfile.NamedTemporaryFile(delete=False, suffix='.db')
	temp_db.close()

	try:
		with DbEngine(f"sqlite:///{temp_db.name}") as db:
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

	finally:
		# Clean up temporary database
		import os
		if os.path.exists(temp_db.name):
			os.unlink(temp_db.name)


if __name__ == "__main__":
	main()


