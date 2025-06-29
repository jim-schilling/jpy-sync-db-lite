"""
This module is used to send requests to the database.

Copyright (c) 2025, Jim Schilling

Please keep this header when you use this code.

This module is licensed under the MIT License.
"""
import queue
import time
from dataclasses import dataclass, field
from typing import Any


@dataclass
class DbRequest:
    operation: str
    query: str | Any
    params: dict | list[dict] | None = None
    response_queue: queue.Queue | None = None
    timestamp: float = field(default_factory=time.time)
    batch_id: str | None = None
