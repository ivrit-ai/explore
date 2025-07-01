from __future__ import annotations
from pathlib import Path
from typing import Any, List, Optional, Union
import logging
import threading
import regex

import sqlite3

# Import database drivers conditionally
try:
    import psycopg2
    POSTGRESQL_AVAILABLE = True
except ImportError:
    POSTGRESQL_AVAILABLE = False


class DatabaseService:
    """Database service that abstracts operations across different database providers."""
    
    def __init__(self, db_type: str = "sqlite", for_index_generation: bool = False, **kwargs):
        """
        Initialize database service.
        
        Args:
            db_type: Database type ("postgresql", "sqlite")
            for_index_generation: If True, avoid memory-only settings for SQLite
            **kwargs: Database-specific connection parameters
        """
        self.db_type = db_type.lower()
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
        """Setup database connection based on type."""
        if self.db_type == "postgresql":
            if not POSTGRESQL_AVAILABLE:
                raise ImportError("PostgreSQL is not available. Install with: pip install psycopg2-binary")
            
            # Extract PostgreSQL connection parameters
            host = self._kwargs.get("host", "localhost")
            port = self._kwargs.get("port", 5432)
            database = self._kwargs.get("database", "postgres")
            user = self._kwargs.get("user", "postgres")
            password = self._kwargs.get("password", "")
            
            self._local.conn = psycopg2.connect(
                host=host,
                port=port,
                database=database,
                user=user,
                password=password
            )
            # Setup PostgreSQL functions
            self._setup_postgresql_functions()
            
        elif self.db_type == "sqlite":
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
            
        else:
            raise ValueError(f"Unsupported database type: {self.db_type}")
    
    def _register_udf(self):
        """Register user-defined functions for SQLite."""
        def match_offsets(text, pattern):
            if text is None or pattern is None:
                return ""
            
            # Compile the pattern before using finditer
            compiled_pattern = regex.compile(regex.escape(pattern))
            return ','.join([str(m.start()) for m in compiled_pattern.finditer(text)])
        
        conn = self._get_connection()
        conn.create_function("match_offsets", 2, match_offsets)
    
    def _setup_postgresql_functions(self):
        """Setup PostgreSQL functions for text matching."""
        conn = self._get_connection()
        cursor = conn.cursor()
        
        # Create the match_offsets function for PostgreSQL
        cursor.execute("""
            CREATE OR REPLACE FUNCTION match_offsets(text_content text, pattern text)
            RETURNS text AS $$
            DECLARE
                result text := '';
                match_pos integer;
                search_text text;
            BEGIN
                IF text_content IS NULL OR pattern IS NULL THEN
                    RETURN '';
                END IF;
                
                search_text := text_content;
                LOOP
                    match_pos := position(pattern in search_text);
                    IF match_pos = 0 THEN
                        EXIT;
                    END IF;
                    
                    IF result != '' THEN
                        result := result || ',';
                    END IF;
                    result := result || (match_pos - 1);
                    
                    search_text := substring(search_text from match_pos + length(pattern));
                END LOOP;
                
                RETURN result;
            END;
            $$ LANGUAGE plpgsql;
        """)
        
        conn.commit()
    
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