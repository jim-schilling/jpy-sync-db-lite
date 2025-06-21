"""
This module is used to send requests to the database.

Copyright (c) 2025, Jim Schilling

Please keep this header when you use this code.

This module is licensed under the MIT License.
"""
import queue
from dataclasses import dataclass, field
from typing import Any, Optional, Dict, List, Union
import time

@dataclass
class DbRequest:
    operation: str
    query: Union[str, Any]
    params: Optional[Union[Dict, List[Dict]]] = None
    response_queue: Optional[queue.Queue] = None
    timestamp: float = field(default_factory=time.time)
    batch_id: Optional[str] = None

