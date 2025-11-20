from __future__ import annotations
from pathlib import Path
from typing import Any, List, Optional, Union
import logging
import threading
import regex

import sqlite3


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
        if not hasattr(self._local, 'conn'):
            self._setup_connection()
        return self._local.conn
    
    def _setup_connection(self):
        """Setup SQLite database connection."""
        path = self._kwargs.get("path", "explore.sqlite")
        self._local.conn = sqlite3.connect(path)
        
        # Configure SQLite parameters for better performance
        cursor = self._local.conn.cursor()
        cursor.execute("PRAGMA cache_size = -4194304")  # 4GB cache (negative value means KB)
        cursor.execute("PRAGMA journal_mode = WAL")
        
        # Only use memory temp store if not generating an index (to allow saving)
        if not self.for_index_generation:
            cursor.execute("PRAGMA temp_store = MEMORY")
        
        self._local.conn.commit()
        
        # Register UDF for SQLite
        self._register_udf()
    
    def _register_udf(self):
        """Register user-defined functions for SQLite."""
        def match_offsets_partial(text, pattern):
            """Find all partial/substring matches (current behavior)."""
            if text is None or pattern is None:
                return ""

            # Escape the pattern for literal matching
            compiled_pattern = regex.compile(regex.escape(pattern))
            return ','.join([str(m.start()) for m in compiled_pattern.finditer(text)])

        def match_offsets_exact(text, pattern):
            """Find all exact/whole word matches using word boundaries."""
            if text is None or pattern is None:
                return ""

            # Use word boundaries for exact word matching
            # \b matches word boundaries (start/end of words)
            pattern_with_boundaries = r'\b' + regex.escape(pattern) + r'\b'
            try:
                compiled_pattern = regex.compile(pattern_with_boundaries)
                return ','.join([str(m.start()) for m in compiled_pattern.finditer(text)])
            except regex.error:
                return ""

        def match_offsets_regex(text, pattern):
            """Find all regex matches (pattern used as-is)."""
            if text is None or pattern is None:
                return ""

            try:
                compiled_pattern = regex.compile(pattern)
                return ','.join([str(m.start()) for m in compiled_pattern.finditer(text)])
            except regex.error:
                # If regex is invalid, return empty string
                return ""

        conn = self._get_connection()
        # Register all three match functions
        conn.create_function("match_offsets_partial", 2, match_offsets_partial)
        conn.create_function("match_offsets_exact", 2, match_offsets_exact)
        conn.create_function("match_offsets_regex", 2, match_offsets_regex)
        # Keep old function name for backwards compatibility
        conn.create_function("match_offsets", 2, match_offsets_partial)
    
    def execute(self, sql: str, params: Optional[List[Any]] = None):
        """Execute SQL query and return cursor/result."""
        conn = self._get_connection()
        cursor = conn.cursor()
        if params:
            cursor.execute(sql, params)
        else:
            cursor.execute(sql)
        return cursor
    
    def batch_execute(self, sql: str, params_list: List[List[Any]]):
        """Execute SQL query with multiple parameter sets (batch insert)."""
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.executemany(sql, params_list)
        return cursor
    
    def commit(self) -> None:
        """Commit transaction."""
        conn = self._get_connection()
        conn.commit()
    
    def close(self) -> None:
        """Close database connection."""
        if hasattr(self._local, 'conn') and self._local.conn:
            self._local.conn.close()
            delattr(self._local, 'conn')
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close() 