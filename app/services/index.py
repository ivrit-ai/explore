from __future__ import annotations
from pathlib import Path
import time
import logging
from dataclasses import dataclass, field, asdict
from typing import List, Tuple, Optional
from bisect import bisect_right
from concurrent.futures import ThreadPoolExecutor
import os
import gzip
import orjson
from tqdm.auto import tqdm
import itertools

from ..utils import FileRecord
from .db import DatabaseService


@dataclass(slots=True)
class TranscriptIndex:
    """Database-agnostic transcript index with useful query methods."""
    _db: DatabaseService
    
    def get_document_count(self) -> int:
        """Get the total number of documents in the index."""
        cursor = self._db.execute("SELECT COUNT(*) FROM documents")
        result = cursor.fetchone()
        return result[0]
    
    def get_document_stats(self) -> tuple[int, int]:
        """Get document count and total character count in a single query."""
        cursor = self._db.execute("""
            SELECT COUNT(*) as doc_count, SUM(LENGTH(full_text)) as total_chars 
            FROM documents
        """)
        result = cursor.fetchone()
        return (result[0], result[1])
    
    def get_document_text(self, doc_id: int) -> str:
        """Get the full text of a document by its ID."""
        cursor = self._db.execute(
            "SELECT full_text FROM documents WHERE doc_id = ?", 
            [doc_id]
        )
        result = cursor.fetchone()
        if not result:
            raise IndexError(f"Document {doc_id} not found")
        return result[0]
    
    def get_document_info(self, doc_id: int) -> dict:
        """Get document information including source, episode, and text."""
        cursor = self._db.execute(
            "SELECT doc_id, source, episode, full_text FROM documents WHERE doc_id = ?", 
            [doc_id]
        )
        result = cursor.fetchone()
        if not result:
            raise IndexError(f"Document {doc_id} not found")
        return {
            "doc_id": result[0],
            "source": result[1],
            "episode": result[2],
            "full_text": result[3]
        }
    
    def get_segments_for_document(self, doc_id: int) -> list[dict]:
        """Get all segments for a document."""
        cursor = self._db.execute("""
            SELECT segment_id, segment_text, avg_logprob, char_offset, start_time, end_time
            FROM segments 
            WHERE doc_id = ? 
            ORDER BY segment_id
        """, [doc_id])
        
        result = cursor.fetchall()
        return [
            {
                "segment_id": row[0],
                "text": row[1],
                "avg_logprob": row[2],
                "char_offset": row[3],
                "start_time": row[4],
                "end_time": row[5]
            }
            for row in result
        ]
    
    def get_segments_by_ids(self, lookups: list[tuple[int, int]]) -> list[dict]:
        """Get multiple segments by (doc_id, segment_id) pairs."""
        if not lookups:
            return []
        
        logger = logging.getLogger(__name__)
        logger.info(f"Fetching segments by IDs: {len(lookups)} lookups")
        
        # Build the WHERE clause for multiple (doc_id, segment_id) pairs
        placeholders = []
        params = []
        for doc_id, segment_id in lookups:
            placeholders.append("(doc_id = ? AND segment_id = ?)")
            params.extend([doc_id, segment_id])
        
        query = f"""
            SELECT doc_id, segment_id, segment_text, avg_logprob, char_offset, start_time, end_time
            FROM segments 
            WHERE {' OR '.join(placeholders)}
            ORDER BY doc_id, segment_id
        """
        
        cursor = self._db.execute(query, params)
        result = cursor.fetchall()
        
        logger.info(f"Fetched segments by IDs: {len(lookups)} lookups, {len(result)} results")
        
        return [
            {
                "doc_id": row[0],
                "segment_id": row[1],
                "text": row[2],
                "avg_logprob": row[3],
                "char_offset": row[4],
                "start_time": row[5],
                "end_time": row[6]
            }
            for row in result
        ]
    
    def get_segment_at_offset(self, doc_id: int, char_offset: int) -> dict:
        """Get the segment that contains the given character offset."""
        logger = logging.getLogger(__name__)
        logger.info(f"Fetching segment at offset: doc_id={doc_id}, char_offset={char_offset}")
        
        cursor = self._db.execute("""
            SELECT segment_id, segment_text, avg_logprob, char_offset, start_time, end_time
            FROM segments 
            WHERE doc_id = ? AND char_offset <= ? 
            ORDER BY char_offset DESC 
            LIMIT 1
        """, [doc_id, char_offset])
        
        result = cursor.fetchone()
        if not result:
            raise IndexError(f"No segment found at offset {char_offset} for document {doc_id}")
        
        logger.info(f"Fetched segment at offset: doc_id={doc_id}, char_offset={char_offset}, segment_id={result[0]}")
        
        return {
            "segment_id": result[0],
            "text": result[1],
            "avg_logprob": result[2],
            "char_offset": result[3],
            "start_time": result[4],
            "end_time": result[5]
        }
            
    def get_document_by_episode_idx(self, episode_idx: int) -> dict:
        """Get document by episode index (0-based)."""
        doc_id = episode_idx
        return self.get_document_info(doc_id)
    
    def get_text_by_episode_idx(self, episode_idx: int) -> str:
        """Get document text by episode index (0-based)."""
        doc_id = episode_idx
        return self.get_document_text(doc_id)
    
    def get_source_by_episode_idx(self, episode_idx: int) -> str:
        """Get document source by episode index (0-based)."""
        doc_id = episode_idx
        cursor = self._db.execute(
            "SELECT source FROM documents WHERE doc_id = ?", 
            [doc_id]
        )
        result = cursor.fetchone()
        if not result:
            raise IndexError(f"Document {doc_id} not found")
        return result[0]

    def search_hits(self, query: str) -> list[tuple[int, int]]:
        """Search for query and return (episode_idx, char_offset) pairs for hits."""
        if self._db.db_type == "sqlite":
            return self._search_sqlite_simple(query)
        elif self._db.db_type == "postgresql":
            return self._search_postgresql_simple(query)
        else:
            raise ValueError(f"Unsupported database type: {self._db.db_type}")
    
    def _search_sqlite_simple(self, query: str) -> list[tuple[int, int]]:
        """Search using SQLite UDF for pattern matching."""

        log = logging.getLogger("index")
        log.info(f"Searching for query: {query}")

        cursor = self._db.execute("""
            SELECT doc_id, match_offsets(full_text, ?) as offsets
            FROM documents 
            WHERE full_text LIKE ?
        """, [query, f'%{query}%'])
        
        result = cursor.fetchall()
        hits = []
        
        for row in result:
            doc_id = row[0]
            offsets_str = row[1]
            if offsets_str:
                # Split the comma-separated offsets and convert to integers
                offsets = [int(offset) for offset in offsets_str.split(',')]
                # Add (doc_id, offset) pairs to hits
                hits.extend([(doc_id, offset) for offset in offsets])
        
        return hits
    
    def _search_postgresql_simple(self, query: str) -> list[tuple[int, int]]:
        """Search using PostgreSQL function for pattern matching."""

        log = logging.getLogger("index")
        log.info(f"Searching for query: {query}")

        cursor = self._db.execute("""
            SELECT doc_id, match_offsets(full_text, %s) as offsets
            FROM documents 
            WHERE full_text ILIKE %s
        """, [query, f'%{query}%'])
        
        result = cursor.fetchall()
        hits = []
        
        for row in result:
            doc_id = row[0]
            offsets_str = row[1]
            if offsets_str:
                # Split the comma-separated offsets and convert to integers
                offsets = [int(offset) for offset in offsets_str.split(',')]
                # Add (doc_id, offset) pairs to hits
                hits.extend([(doc_id, offset) for offset in offsets])
        
        return hits


def _setup_schema(db: DatabaseService):
    """Create the transcript database schema."""
    # Apply SQLite performance optimizations
    if db.db_type == "sqlite":
        db.execute("PRAGMA journal_mode = WAL")
        db.execute("PRAGMA synchronous = NORMAL")
        db.execute("PRAGMA cache_size = 1000000")
    
    # Create documents table
    db.execute("""
        CREATE TABLE documents (
            doc_id INTEGER PRIMARY KEY,
            source VARCHAR,
            episode VARCHAR,
            full_text TEXT
        )
    """)
    
    # Create segments table
    db.execute("""
        CREATE TABLE segments (
            doc_id INTEGER,
            segment_id INTEGER,
            segment_text TEXT,
            avg_logprob DOUBLE,
            char_offset INTEGER,
            start_time DOUBLE,
            end_time DOUBLE,
            FOREIGN KEY (doc_id) REFERENCES documents(doc_id)
        )
    """)
    
    # Create indexes for better performance
    db.execute("""
        CREATE INDEX idx_segments_doc_id 
        ON segments(doc_id)
    """)
    
    db.execute("""
        CREATE INDEX idx_segments_segment_id 
        ON segments(segment_id)
    """)
    
    db.execute("""
        CREATE INDEX idx_segments_char_offset 
        ON segments(char_offset)
    """)
    
    db.execute("""
        CREATE INDEX idx_segments_doc_id_segment_id 
        ON segments(doc_id, segment_id)
    """)


# ­­­­­­­­­­­­­­­­­­­­­­­­­­­­-------------------------------------------------- #
class IndexManager:
    """Global, read-only index using database-agnostic service."""
    def __init__(self, file_records: Optional[List[FileRecord]] = None, index_path: Optional[Path] = None, db_type: str = "sqlite", **db_kwargs) -> None:
        self._file_records = file_records
        self._index_path = Path(index_path) if index_path else None
        self._db_type = db_type
        self._db_kwargs = db_kwargs
        self._index = None
        
        if index_path and Path(index_path).exists():
            self._index = self._load_index()
        elif file_records:
            self._index = self._build()
        else:
            raise ValueError("Either file_records or index_path must be provided")

    def get(self) -> TranscriptIndex:
        return self._index

    def save_index(self, path: str | Path) -> None:
        """Save the index to a database file."""
        path = Path(path)
        if self._db_type == "sqlite" and path.suffix != '.db':
            path = path.with_suffix('.db')
        
        # For SQLite, we can copy the file directly
        if self._db_type == "sqlite":
            import shutil
            if "path" in self._db_kwargs and self._db_kwargs["path"] != ":memory:":
                shutil.copy2(self._db_kwargs["path"], path)
            else:
                raise NotImplementedError("Cannot save in-memory SQLite database")
        else:
            raise NotImplementedError(f"Save not implemented for {self._db_type}")

    def _load_index(self) -> TranscriptIndex:
        """Load index from a database file."""
        log = logging.getLogger("index")
        log.info(f'Loading index: {self._index_path}')
        if not self._index_path or not self._index_path.exists():
            raise ValueError(f"Index path not found: {self._index_path}")
        
        # Load from single file
        db_path = self._index_path
        if self._db_type == "sqlite" and db_path.suffix != '.db':
            db_path = db_path.with_suffix('.db')
        
        db = DatabaseService(
            db_type=self._db_type,
            **self._db_kwargs
        )

        # Apply SQLite performance optimizations for existing databases
        if self._db_type == "sqlite":
            db.execute("PRAGMA journal_mode = WAL")
            db.execute("PRAGMA synchronous = NORMAL")
            db.execute("PRAGMA cache_size = 1000000")

        return TranscriptIndex(db)

    def _load_and_convert(self, rec_idx: int, rec: FileRecord) -> Tuple[int, str, dict, float, float]:
        """Load and convert a single record, with timing."""
        t0 = time.perf_counter()
        
        # Time JSON read
        t_read = time.perf_counter()
        data = rec.read_json()
        read_ms = (time.perf_counter() - t_read) * 1000
        
        # Time string conversion
        t_conv = time.perf_counter()
        full, segments_data = _episode_to_string_and_segments(data)
        conv_ms = (time.perf_counter() - t_conv) * 1000
        
        return rec_idx, rec.id, {"full": full, "segments": segments_data}, read_ms, conv_ms

    def _build(self) -> TranscriptIndex:
        log = logging.getLogger("index")
        print(self._file_records)
        for f in self._file_records:
            print(f.id)
        records = list(enumerate(self._file_records))
        total_files = len(records)
        
        # Create database service
        db = DatabaseService(db_type=self._db_type, for_index_generation=True, **self._db_kwargs)
        
        # Setup schema
        log.info("Setting up schema...")
        _setup_schema(db)
        
        # Use CPU count for thread pool size, but cap at 16 to avoid too many threads
        n_threads = min(1, os.cpu_count() or 4)
        log.info(f"Building index with {n_threads} threads for {total_files} files")
        
        with ThreadPoolExecutor(max_workers=n_threads) as executor:
            # Submit all jobs
            futures = [
                executor.submit(self._load_and_convert, rec_idx, rec)
                for rec_idx, rec in records
            ]
            
            # Process results and insert directly into database
            with tqdm(total=total_files, desc="Building index", unit="file") as pbar:
                for future in futures:
                    t_append = time.perf_counter()
                    rec_idx, rec_id, data, read_ms, conv_ms = future.result()
                    
                    # Insert document directly
                    doc_id = rec_idx
                    
                    # Start transaction for this document
                    db.execute("BEGIN TRANSACTION")
                    
                    db.execute(
                        "INSERT INTO documents (doc_id, source, episode, full_text) VALUES (?, ?, ?, ?)",
                        [doc_id, rec_id, rec_id, data["full"]]
                    )
                    
                    # Insert all segments for this document in one batch
                    # Prepare batch insert for segments
                    segment_values = []
                    for seg_idx, seg in enumerate(data["segments"]):
                        segment_values.append((
                            doc_id,
                            seg_idx,
                            seg["text"],
                            seg.get("avg_logprob", 0.0),
                            seg["char_offset"],
                            seg["start"],
                            seg["end"]
                        ))
                        
                    # Batch insert segments
                    db.batch_execute(
                        """INSERT INTO segments 
                            (doc_id, segment_id, segment_text, avg_logprob, char_offset, start_time, end_time) 
                            VALUES (?, ?, ?, ?, ?, ?, ?)""",
                        segment_values
                    )
                    
                    # Commit transaction for this document
                    db.commit()
                    
                    append_ms = (time.perf_counter() - t_append) * 1000
                    total_ms = read_ms + conv_ms + append_ms
                    
                    pbar.update(1)
                
        log.info(f"Index built successfully: {total_files} documents")
        
        return TranscriptIndex(db)

# helper converts Kaldi-style or plain list JSON to a single string and segments
def _episode_to_string_and_segments(data: dict | list) -> tuple[str, list[dict]]:
    """
    Returns:
        full_text, segments_data[]
    segments_data contains text, start, end, offset, and avg_logprob for each segment
    """
    if isinstance(data, dict) and "segments" in data:
        segs = data["segments"]
    elif isinstance(data, list):
        segs = data
    else:
        raise ValueError("Unrecognised transcript JSON structure")

    parts = []
    segments_data = []
    cursor = 0
    
    for seg in segs:
        part = seg["text"]
        parts.append(part)
        
        segment_info = {
            "text": part,
            "start": float(seg["start"]),
            "end": float(seg["end"]),
            "char_offset": cursor,
            "avg_logprob": seg.get("avg_logprob", 0.0)
        }
        segments_data.append(segment_info)
        cursor += len(part) + 1  # +1 for the space we'll add below
    
    full_text = " ".join(parts)
    return full_text, segments_data


# ------------------------------------------------------------------ #
@dataclass(slots=True, frozen=True)
class Segment:
    episode_idx: int
    seg_idx: int
    text: str
    start_sec: float
    end_sec: float


def segment_for_hit(index: TranscriptIndex, episode_idx: int,
                    char_offset: int) -> Segment:
    """Lookup segment containing `char_offset` using database service."""
    # Get the document ID
    doc_id = episode_idx
    
    # Use the new targeted method
    segment_data = index.get_segment_at_offset(doc_id, char_offset)
    
    return Segment(
        episode_idx=episode_idx,
        seg_idx=segment_data["segment_id"],
        text=segment_data["text"],
        start_sec=segment_data["start_time"],
        end_sec=segment_data["end_time"]
    )