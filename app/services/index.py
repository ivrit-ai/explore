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
import duckdb
import pandas as pd
import itertools
import regex
from duckdb.typing import *

from ..utils import FileRecord


@dataclass(slots=True)
class TranscriptIndex:
    """DuckDB-based transcript index with useful query methods."""
    _db: duckdb.DuckDBPyConnection
    
    def get_document_count(self) -> int:
        """Get the total number of documents in the index."""
        result = self._db.execute("SELECT COUNT(*) FROM documents").fetchone()
        return result[0]
    
    def get_document_stats(self) -> tuple[int, int]:
        """Get document count and total character count in a single query."""
        result = self._db.execute("""
            SELECT COUNT(*) as doc_count, SUM(LENGTH(full_text)) as total_chars 
            FROM documents
        """).fetchone()
        return (result[0], result[1])
    
    def get_document_text(self, doc_id: int) -> str:
        """Get the full text of a document by its ID."""
        result = self._db.execute(
            "SELECT full_text FROM documents WHERE doc_id = ?", 
            [doc_id]
        ).fetchone()
        if not result:
            raise IndexError(f"Document {doc_id} not found")
        return result[0]
    
    def get_document_info(self, doc_id: int) -> dict:
        """Get document information including source, episode, and text."""
        result = self._db.execute(
            "SELECT doc_id, source, episode, full_text FROM documents WHERE doc_id = ?", 
            [doc_id]
        ).fetchone()
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
        result = self._db.execute("""
            SELECT segment_id, segment_text, avg_logprob, char_offset, start_time, end_time
            FROM segments 
            WHERE doc_id = ? 
            ORDER BY segment_id
        """, [doc_id]).fetchall()
        
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
        
        result = self._db.execute(query, params).fetchall()
        
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
        
        result = self._db.execute("""
            SELECT segment_id, segment_text, avg_logprob, char_offset, start_time, end_time
            FROM segments 
            WHERE doc_id = ? AND char_offset <= ? 
            ORDER BY char_offset DESC 
            LIMIT 1
        """, [doc_id, char_offset]).fetchone()
        
        logger.info(f"Fetched segment at offset: doc_id={doc_id}, char_offset={char_offset}, segment_id={result[0] if result else None}")
        
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
        result = self._db.execute(
            "SELECT source FROM documents WHERE doc_id = ?", 
            [doc_id]
        ).fetchone()
        if not result:
            raise IndexError(f"Document {doc_id} not found")
        return result[0]

    def search_hits(self, query: str) -> list[tuple[int, int]]:
        """Search for query and return (episode_idx, char_offset) pairs for hits."""
        # Use regex UDF to find character offsets directly in DuckDB
        result = self._db.execute("""
            SELECT doc_id, regexp_match_offsets(full_text, ?) AS matches
            FROM documents 
            WHERE full_text LIKE ?
        """, [regex.escape(query), f"%{query}%"]).fetchall()
        
        hits = []
        for doc_id, matches in result:
            if matches:
                episode_idx = doc_id
                for match in matches:
                    hits.append((episode_idx, match))
        
        return hits

# ­­­­­­­­­­­­­­­­­­­­­­­­­­­­-------------------------------------------------- #
class IndexManager:
    """Global, read-only index using DuckDB."""
    def __init__(self, file_records: Optional[List[FileRecord]] = None, index_path: Optional[Path] = None) -> None:
        self._file_records = file_records
        self._index_path = Path(index_path) if index_path else None
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
        """Save the index to a DuckDB database file."""
        path = Path(path)
        if path.suffix != '.db':
            path = path.with_suffix('.db')
        
        # Copy the in-memory database to the file
        self._index._db.execute(f"EXPORT DATABASE '{path}'")

    def _load_index(self) -> TranscriptIndex:
        """Load index from a DuckDB database export directory."""
        log = logging.getLogger("index")
        log.info(f'Loading index: {self._index_path}')
        if not self._index_path or not self._index_path.exists():
            raise ValueError(f"Index path not found: {self._index_path}")
        
        # Check if it's a directory (exported format) or a single file
        if self._index_path.is_dir():
            # Load from exported directory with parquet files
            db = duckdb.connect(':memory:')
            
            # Create regex UDF for pattern matching (same as in _build)
            def regexp_match_offsets(content, pattern):
                if content is None or pattern is None:
                    return None
                matches = []
                for match in regex.finditer(pattern, content):
                    matches.append(match.start())
                return matches
            
            db.create_function(
                'regexp_match_offsets',
                regexp_match_offsets,
                return_type=DuckDBPyType(list[int])
            )
            
            log.info(f'Importing index: {self._index_path}')
            # Import the exported database
            db.execute(f"IMPORT DATABASE '{self._index_path}'")
            
            log.info(f'Imported index: {self._index_path}')

            return TranscriptIndex(db)
        else:
            # Load from single .db file (legacy format)
            db_path = self._index_path
            if db_path.suffix != '.db':
                db_path = db_path.with_suffix('.db')
            
            db = duckdb.connect(str(db_path), read_only=True)
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
        
        # Create in-memory DuckDB database
        db = duckdb.connect(':memory:')
                
        # Create regex UDF for pattern matching
        def regexp_match_offsets(content, pattern):
            if content is None or pattern is None:
                return None
            matches = []
            for match in regex.finditer(pattern, content):
                matches.append(match.start())
            return matches
        
        db.create_function(
            'regexp_match_offsets',
            regexp_match_offsets,
            return_type=DuckDBPyType(list[int])
        )
        
        # Create tables
        db.execute("""
            CREATE TABLE documents (
                doc_id INTEGER PRIMARY KEY,
                source VARCHAR,
                episode VARCHAR,
                full_text TEXT
            )
        """)
        
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
        
        # Create index on segments for faster lookups
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
        
        # Use CPU count for thread pool size, but cap at 16 to avoid too many threads
        n_threads = min(16, os.cpu_count() or 4)
        log.info(f"Building index with {n_threads} threads for {total_files} files")
        
        # Collect data in lists for DataFrame creation
        documents_data = []
        
        # Pre-allocate segment data arrays for better performance
        segment_doc_ids_lists = []
        segment_ids_lists = []
        segment_texts_lists = []
        segment_avg_logprobs_lists = []
        segment_char_offsets_lists = []
        segment_start_times_lists = []
        segment_end_times_lists = []
        
        with ThreadPoolExecutor(max_workers=n_threads) as executor:
            # Submit all jobs
            futures = [
                executor.submit(self._load_and_convert, rec_idx, rec)
                for rec_idx, rec in records
            ]
            
            # Process results in order as they complete
            with tqdm(total=total_files, desc="Building index", unit="file") as pbar:
                for future in futures:
                    t_append = time.perf_counter()
                    rec_idx, rec_id, data, read_ms, conv_ms = future.result()
                    
                    # Collect document data
                    doc_id = rec_idx
                    documents_data.append({
                        "doc_id": doc_id,
                        "source": rec_id,
                        "episode": rec_id,
                        "full_text": data["full"]
                    })
                    
                    # Collect segment data using list comprehension
                    segments = data["segments"]
                    segment_doc_ids_lists.append([doc_id] * len(segments))
                    segment_ids_lists.append(list(range(len(segments))))
                    segment_texts_lists.append([seg["text"] for seg in segments])
                    segment_avg_logprobs_lists.append([seg.get("avg_logprob", 0.0) for seg in segments])
                    segment_char_offsets_lists.append([seg["char_offset"] for seg in segments])
                    segment_start_times_lists.append([seg["start"] for seg in segments])
                    segment_end_times_lists.append([seg["end"] for seg in segments])
                    
                    append_ms = (time.perf_counter() - t_append) * 1000
                    total_ms = read_ms + conv_ms + append_ms
                    
                    pbar.update(1)
        
        # Create DataFrames and bulk insert
        log.info("Creating DataFrames and bulk inserting data...")
        
        documents_df = pd.DataFrame(documents_data)
        
        # Flatten the lists of lists using itertools
        segment_doc_ids = list(itertools.chain.from_iterable(segment_doc_ids_lists))
        segment_ids = list(itertools.chain.from_iterable(segment_ids_lists))
        segment_texts = list(itertools.chain.from_iterable(segment_texts_lists))
        segment_avg_logprobs = list(itertools.chain.from_iterable(segment_avg_logprobs_lists))
        segment_char_offsets = list(itertools.chain.from_iterable(segment_char_offsets_lists))
        segment_start_times = list(itertools.chain.from_iterable(segment_start_times_lists))
        segment_end_times = list(itertools.chain.from_iterable(segment_end_times_lists))
        
        # Create segments DataFrame from separate arrays
        segments_df = pd.DataFrame({
            "doc_id": segment_doc_ids,
            "segment_id": segment_ids,
            "segment_text": segment_texts,
            "avg_logprob": segment_avg_logprobs,
            "char_offset": segment_char_offsets,
            "start_time": segment_start_times,
            "end_time": segment_end_times
        })
        
        # Bulk insert documents
        db.execute("INSERT INTO documents SELECT * FROM documents_df")
        
        # Bulk insert segments
        db.execute("INSERT INTO segments SELECT * FROM segments_df")
                
        log.info(f"Index built successfully: {len(documents_data)} documents, {len(segment_doc_ids)} segments")
        
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
    """Lookup segment containing `char_offset` using DuckDB."""
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

def segment_by_idx(index: TranscriptIndex, episode_idx: int,
                   seg_idx: int) -> Segment:
    """Get segment by index using DuckDB."""
    doc_id = episode_idx
    
    # Use the new batch method for consistency
    segments = index.get_segments_by_ids([(doc_id, seg_idx)])
    if not segments:
        raise IndexError(f"Segment {seg_idx} not found for document {doc_id}")
    
    segment_data = segments[0]
    
    return Segment(
        episode_idx=episode_idx,
        seg_idx=segment_data["segment_id"],
        text=segment_data["text"],
        start_sec=segment_data["start_time"],
        end_sec=segment_data["end_time"]
    )
