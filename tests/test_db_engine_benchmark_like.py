"""
Benchmark-inspired behavioral tests for DbEngine using real SQLite.

These tests validate concurrent/threaded behavior and basic throughput at
modest scales to keep CI fast and reliable. They are marked as performance.
"""

import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any

import pytest

from jpy_sync_db_lite.db_engine import DbEngine


def _create_schema(db: DbEngine) -> None:
    db.execute(
        """
        CREATE TABLE IF NOT EXISTS benchmark_test (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            value INTEGER,
            data TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    db.execute("CREATE INDEX IF NOT EXISTS idx_name ON benchmark_test(name)")
    db.execute("CREATE INDEX IF NOT EXISTS idx_value ON benchmark_test(value)")


def _gen_rows(n: int) -> list[dict[str, Any]]:
    return [
        {"name": f"User{i}", "value": i, "data": f"Data {i}"}
        for i in range(n)
    ]


@pytest.mark.performance
def test_connection_health_and_basic_ops_loop() -> None:
    with DbEngine("sqlite:///:memory:") as db:
        _create_schema(db)

        iterations = 200
        start = time.time()
        for i in range(iterations):
            assert db.check_connection_health() is True
            res = db.fetch("SELECT 1 as one")
            assert res.data and res.data[0]["one"] == 1
        total = time.time() - start
        # Very lenient bound to avoid flakiness in CI
        assert total < 5.0


@pytest.mark.performance
def test_small_single_insert_throughput() -> None:
    with DbEngine("sqlite:///:memory:") as db:
        _create_schema(db)

        # Warmup
        db.execute(
            "INSERT INTO benchmark_test (name, value, data) VALUES (:name, :value, :data)",
            params={"name": "warmup", "value": 0, "data": "warmup"},
        )

        ops = 200
        start = time.time()
        for i in range(ops):
            db.execute(
                "INSERT INTO benchmark_test (name, value, data) VALUES (:name, :value, :data)",
                params={"name": f"Benchmark{i}", "value": i, "data": f"Benchmark data {i}"},
            )
        total = time.time() - start
        throughput = ops / total if total > 0 else float("inf")
        assert throughput > 50  # lenient


@pytest.mark.performance
def test_small_bulk_insert_throughput() -> None:
    with DbEngine("sqlite:///:memory:") as db:
        _create_schema(db)

        batch_sizes = [10, 50]
        for batch in batch_sizes:
            rows = _gen_rows(batch)
            start = time.time()
            db.execute(
                "INSERT INTO benchmark_test (name, value, data) VALUES (:name, :value, :data)",
                params=rows,
            )
            total = time.time() - start
            throughput = batch / total if total > 0 else float("inf")
            assert throughput > 50  # lenient


@pytest.mark.performance
def test_concurrent_mixed_ops_threaded() -> None:
    with DbEngine("sqlite:///:memory:") as db:
        _create_schema(db)
        # Seed a little data
        db.execute(
            "INSERT INTO benchmark_test (name, value, data) VALUES (:name, :value, :data)",
            params=_gen_rows(100),
        )

        def worker(worker_id: int, num_ops: int) -> int:
            completed = 0
            for i in range(num_ops):
                if i % 3 == 0:
                    db.execute(
                        "INSERT INTO benchmark_test (name, value, data) VALUES (:name, :value, :data)",
                        params={"name": f"W{worker_id}-{i}", "value": i, "data": "x"},
                    )
                else:
                    res = db.fetch(
                        "SELECT * FROM benchmark_test WHERE value >= :v LIMIT 5",
                        params={"v": i % 50},
                    )
                    assert isinstance(res.data, list)
                completed += 1
            return completed

        errors: list[str] = []
        concurrency = 4
        ops_per_worker = 50
        with ThreadPoolExecutor(max_workers=concurrency) as ex:
            futures = [ex.submit(worker, w, ops_per_worker) for w in range(concurrency)]
            for f in as_completed(futures):
                try:
                    assert f.result() == ops_per_worker
                except Exception as e:  # pragma: no cover - defensive
                    errors.append(str(e))

        assert not errors

