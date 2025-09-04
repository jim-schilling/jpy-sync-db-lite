"""
Transactions and batch example.

Run:
  python examples/transactions_and_batch.py
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
			# Schema
			db.execute(
				"""
				CREATE TABLE IF NOT EXISTS items (
					id INTEGER PRIMARY KEY,
					name TEXT NOT NULL,
					qty INTEGER NOT NULL
				)
				"""
			)

			# Transaction
			ops = [
				{"operation": "execute", "query": "INSERT INTO items (name, qty) VALUES ('a', 1)"},
				{"operation": "execute", "query": "INSERT INTO items (name, qty) VALUES ('b', 2)"},
				{"operation": "fetch", "query": "SELECT COUNT(*) as c FROM items"},
			]
			trx_res = db.execute_transaction(ops)
			print(trx_res)

			# Batch
			batch_sql = """
			INSERT INTO items (name, qty) VALUES ('c', 3);
			INSERT INTO items (name, qty) VALUES ('d', 4);
			SELECT * FROM items ORDER BY id;
			"""
			batch_res = db.batch(batch_sql)
			for r in batch_res:
				print(r["operation"], r["result"].rowcount)

	finally:
		# Clean up temporary database
		import os
		if os.path.exists(temp_db.name):
			os.unlink(temp_db.name)


if __name__ == "__main__":
	main()


