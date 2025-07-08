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
    """
    Data structure for a database request.
    """
    _operation: str
    _query: str | Any
    _params: dict | list[dict] | None = None
    _response_queue: queue.Queue | None = None
    _timestamp: float = field(default_factory=time.time)
    _batch_id: str | None = None

    @property
    def operation(self) -> str:
        """Return the operation type."""
        return self._operation

    @property
    def query(self) -> str | Any:
        """Return the SQL query or command."""
        return self._query

    @property
    def params(self) -> dict | list[dict] | None:
        """Return the parameters for the query."""
        return self._params

    @property
    def response_queue(self) -> queue.Queue | None:
        """Return the response queue."""
        return self._response_queue

    @property
    def timestamp(self) -> float:
        """Return the timestamp of the request."""
        return self._timestamp

    @property
    def batch_id(self) -> str | None:
        """Return the batch ID if present."""
        return self._batch_id

    def __init__(
        self,
        operation: str,
        query: str | Any,
        *,
        params: dict | list[dict] | None = None,
        response_queue: queue.Queue | None = None,
        timestamp: float = None,
        batch_id: str | None = None,
    ) -> None:
        self._operation = operation
        self._query = query
        self._params = params
        self._response_queue = response_queue
        self._timestamp = timestamp if timestamp is not None else time.time()
        self._batch_id = batch_id
