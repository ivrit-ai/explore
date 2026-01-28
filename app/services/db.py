from __future__ import annotations

import sqlite3
import threading
from typing import Any


def _get_sqlite_max_variable_number() -> int:
    """
    Query SQLite for SQLITE_MAX_VARIABLE_NUMBER at runtime.
    Returns the actual limit or 999 (standard default) if unable to determine.
    """
    try:
        conn = sqlite3.connect(":memory:")
        try:
            cursor = conn.execute("PRAGMA compile_options")
            for (opt,) in cursor:
                if opt.startswith("MAX_VARIABLE_NUMBER="):
                    return int(opt.split("=", 1)[1])
        finally:
            conn.close()
    except Exception:
        pass

    # Standard SQLite default
    return 999


# Query once at module load and cache the result
SQLITE_MAX_PARAMS = _get_sqlite_max_variable_number()


class DatabaseService:
    """Database service that abstracts operations across different database providers."""

    def __init__(self, for_index_generation: bool = False, **kwargs):
        """
        Initialize database service.

        Args:
            for_index_generation: If True, avoid memory-only settings for SQLite
            **kwargs: Database-specific connection parameters
        """
        self.for_index_generation = for_index_generation
        self._kwargs = kwargs
        self._local = threading.local()
        self._setup_connection()

    def _get_connection(self):
        """Get thread-local connection."""
        if not hasattr(self._local, "conn"):
            self._setup_connection()
        return self._local.conn

    def _setup_connection(self):
        """Setup SQLite database connection."""
        path = self._kwargs.get("path", "explore.sqlite")
        self._local.conn = sqlite3.connect(path)

        # Configure SQLite parameters for better performance
        cursor = self._local.conn.cursor()
        cursor.execute("PRAGMA cache_size = -524288")  # 512MB cache (negative value means KB)
        cursor.execute("PRAGMA journal_mode = WAL")

        # Only use memory temp store if not generating an index (to allow saving)
        if not self.for_index_generation:
            cursor.execute("PRAGMA temp_store = MEMORY")

        self._local.conn.commit()

    def execute(self, sql: str, params: list[Any] | None = None):
        """Execute SQL query and return cursor/result."""
        conn = self._get_connection()
        cursor = conn.cursor()
        if params:
            cursor.execute(sql, params)
        else:
            cursor.execute(sql)
        return cursor

    def batch_execute(self, sql: str, params_list: list[list[Any]]):
        """
        Execute batch insert using true multi-row SQL VALUES for SQLite.
        Dramatically faster than executemany for large batches.
        """
        conn = self._get_connection()
        cursor = conn.cursor()

        if not params_list:
            return cursor

        # Calculate batch size based on SQLite's variable limit
        # If each row has N parameters, we can insert floor(SQLITE_MAX_PARAMS/N) rows at most
        params_per_row = len(params_list[0])
        BATCH_SIZE = max(1, SQLITE_MAX_PARAMS // params_per_row)  # At least 1 row

        for i in range(0, len(params_list), BATCH_SIZE):
            batch = params_list[i : i + BATCH_SIZE]

            # Build "(?, ?, ...), (?, ?, ...)" placeholders
            row_placeholder = "(" + ",".join(["?"] * len(batch[0])) + ")"
            all_placeholders = ",".join([row_placeholder] * len(batch))

            # Replace VALUES clause dynamically (match any number of placeholders)
            import re

            # Find "VALUES (...)" pattern and replace it
            final_sql = re.sub(r"VALUES\s*\([?,\s]+\)", f"VALUES {all_placeholders}", sql, count=1)

            # Flatten parameters
            flat_params = [val for row in batch for val in row]

            cursor.execute(final_sql, flat_params)

        return cursor

    def commit(self) -> None:
        """Commit transaction."""
        conn = self._get_connection()
        conn.commit()

    def close(self) -> None:
        """Close database connection."""
        if hasattr(self._local, "conn") and self._local.conn:
            self._local.conn.close()
            delattr(self._local, "conn")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
