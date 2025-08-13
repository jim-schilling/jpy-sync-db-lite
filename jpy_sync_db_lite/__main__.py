"""
Main entry point for jpy_sync_db_lite package.

Copyright (c) 2025, Jim Schilling

Please keep this header when you use this code.

This module is licensed under the MIT License.
"""

from __future__ import annotations

from typing import NoReturn


def main() -> NoReturn:
	"""Minimal CLI that prints the package version if available."""
	try:
		from jpy_sync_db_lite._version import version  # type: ignore
		print(version)
	except Exception:
		print("jpy-sync-db-lite")


if __name__ == "__main__":
	main()
