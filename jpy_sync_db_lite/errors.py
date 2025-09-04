"""
Module defining package-specific exceptions.

Copyright (c) 2025, Jim Schilling

Please keep this header when you use this code.

This module is licensed under the MIT License.
"""

from __future__ import annotations


class JpySyncDbLiteError(Exception):
    """Base exception for all jpy-sync-db-lite errors."""

    def __init__(self, message: str) -> None:
        super().__init__(message)


class SqlFileError(JpySyncDbLiteError):
    """Raised for SQL file I/O or OS-related problems.

    Examples include file not found, permission denied, or decode errors.
    """

    def __init__(self, message: str) -> None:
        super().__init__(message)


class SqlValidationError(JpySyncDbLiteError):
    """Raised for validation issues with provided SQL-related inputs."""

    def __init__(self, message: str) -> None:
        super().__init__(message)


class DatabaseError(JpySyncDbLiteError):
    """Base exception for database-related errors."""

    def __init__(self, message: str) -> None:
        super().__init__(message)


class ConnectionError(DatabaseError):
    """Raised for connection-related issues."""

    def __init__(self, message: str) -> None:
        super().__init__(message)


class TransactionError(DatabaseError):
    """Raised for transaction-related issues."""

    def __init__(self, message: str) -> None:
        super().__init__(message)


class OperationError(DatabaseError):
    """Raised for database operation failures."""

    def __init__(self, message: str) -> None:
        super().__init__(message)


class MaintenanceError(DatabaseError):
    """Raised for database maintenance operation failures."""

    def __init__(self, message: str) -> None:
        super().__init__(message)


__all__ = [
    "JpySyncDbLiteError",
    "SqlFileError",
    "SqlValidationError",
    "DatabaseError",
    "ConnectionError",
    "TransactionError",
    "OperationError",
    "MaintenanceError",
]
