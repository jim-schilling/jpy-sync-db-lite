"""
Module defining package-specific exceptions.

Copyright (c) 2025, Jim Schilling

Please keep this header when you use this code.

This module is licensed under the MIT License.
"""

from __future__ import annotations


class SqlFileError(Exception):
    """Raised for SQL file I/O or OS-related problems.

    Examples include file not found, permission denied, or decode errors.
    """

    def __init__(self, message: str) -> None:
        super().__init__(message)


class SqlValidationError(Exception):

 """Raised for validation issues with provided SQL-related inputs."""

 def __init__(self, message: str) -> None:
  super().__init__(message)


__all__ = [
 "SqlFileError",
 "SqlValidationError",
]


