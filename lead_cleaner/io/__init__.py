"""
IO module __init__.py

Exposes file loading utilities.
"""

from lead_cleaner.io.db_reader import load_from_sqlite

__all__ = ["load_from_sqlite"]
