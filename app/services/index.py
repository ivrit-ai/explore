from pathlib import Path
import time
import logging
from dataclasses import dataclass, field
from typing import List, Tuple, Optional
from concurrent.futures import ThreadPoolExecutor
import os
from tqdm.auto import tqdm
import itertools
from datetime import datetime
import re
import uuid

from ..utils import FileRecord
from .db import DatabaseService


@dataclass(slots=True)
class TranscriptIndex:
    """Database-agnostic transcript index with useful query methods."""
    _db: DatabaseService
        
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
                "SELECT doc_id, uuid, source, episode, episode_date, episode_title, full_text FROM documents WHERE doc_id = ?",
            [doc_id]
        )
        result = cursor.fetchone()
        if not result:
            raise IndexError(f"Document {doc_id} not found")
        return {
            "doc_id": result[0],
            "uuid": result[1],
            "source": result[2],
            "episode": result[3],
            "episode_date": result[4],
            "episode_title": result[5],
            "full_text": result[6]
        }

    def get_episode_by_uuid(self, doc_uuid: str) -> str:
        """Get episode path by UUID."""
        cursor = self._db.execute(
            "SELECT episode FROM documents WHERE uuid = ?",
            [doc_uuid]
        )
        result = cursor.fetchone()
        if not result:
            raise IndexError(f"Document with UUID {doc_uuid} not found")
        return result[0]
    
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
        """
        Get multiple segments by (doc_id, segment_id) pairs.

        For large batches (>100), uses a temporary table for optimal performance.
        For small batches, uses VALUES clause for simplicity.
        """
        if not lookups:
            return []

        logger = logging.getLogger(__name__)
        logger.debug(f"Fetching segments by IDs: {len(lookups)} lookups")

    
        self._db.execute("""
            CREATE TEMPORARY TABLE IF NOT EXISTS temp_segment_lookups (
                doc_id INTEGER,
                segment_id INTEGER,
                PRIMARY KEY (doc_id, segment_id)
            ) WITHOUT ROWID
        """)

        # Clear any existing data
        self._db.execute("DELETE FROM temp_segment_lookups")

        # Batch insert lookups (SQLite supports ~999 vars per insert)
        BATCH_SIZE = 499  # 499 pairs = 998 parameters
        for i in range(0, len(lookups), BATCH_SIZE):
            batch = lookups[i:i + BATCH_SIZE]
            placeholders = ','.join(['(?, ?)'] * len(batch))
            params = [val for pair in batch for val in pair]

            self._db.execute(
                f"INSERT OR IGNORE INTO temp_segment_lookups VALUES {placeholders}",
                params
            )

        # Single join query
        cursor = self._db.execute("""
            SELECT s.doc_id, s.segment_id, s.segment_text, s.avg_logprob,
                    s.char_offset, s.start_time, s.end_time
            FROM segments s
            INNER JOIN temp_segment_lookups t
            ON s.doc_id = t.doc_id AND s.segment_id = t.segment_id
            ORDER BY s.doc_id, s.segment_id
        """)

        result = cursor.fetchall()

        # Cleanup
        self._db.execute("DELETE FROM temp_segment_lookups")

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

    def search_hits(self, query: str, search_mode: str = 'partial', date_from: Optional[str] = None,
                   date_to: Optional[str] = None, sources: Optional[list[str]] = None) -> list[tuple[int, int]]:
        """Search for query and return (episode_idx, char_offset) pairs for hits.

        Args:
            query: Search query string
            search_mode: Search mode - 'exact' (whole words), 'partial' (substring), or 'regex'
            date_from: Optional start date filter (YYYY-MM-DD format)
            date_to: Optional end date filter (YYYY-MM-DD format)
            sources: Optional list of sources to filter by
        """
        return self._search_sqlite_simple(query, search_mode, date_from, date_to, sources)

    def _search_sqlite_simple(self, query: str, search_mode: str = 'partial', date_from: Optional[str] = None,
                             date_to: Optional[str] = None, sources: Optional[list[str]] = None) -> list[tuple[int, int]]:
        """Search using SQLite UDF for pattern matching with optional filters."""

        log = logging.getLogger("index")
        log.info(f"Searching for query: {query}, mode: {search_mode}, date_from: {date_from}, date_to: {date_to}, sources: {sources}")

        # Choose the appropriate match function based on search mode
        if search_mode == 'exact':
            match_func = 'match_offsets_exact'
            # For exact match, also use word boundaries in LIKE for efficiency
            like_pattern = f'%{query}%'  # Still use substring for initial filtering
        elif search_mode == 'regex':
            match_func = 'match_offsets_regex'
            # For regex, we can't reliably filter with LIKE, so use a broad match
            like_pattern = '%'  # Match everything, let regex UDF do the work
        else:  # partial (default)
            match_func = 'match_offsets_partial'
            like_pattern = f'%{query}%'

        # Build WHERE clause with filters
        where_clauses = ["full_text LIKE ?"]
        params = [like_pattern]

        # Add date filters
        if date_from:
            where_clauses.append("episode_date >= ?")
            params.append(date_from)
        if date_to:
            where_clauses.append("episode_date <= ?")
            params.append(date_to)

        # Add source filter
        if sources and len(sources) > 0:
            # Create placeholders for IN clause
            placeholders = ','.join('?' * len(sources))
            where_clauses.append(f"source IN ({placeholders})")
            params.extend(sources)

        # Combine WHERE clauses
        where_clause = ' AND '.join(where_clauses)

        sql = f"""
            SELECT doc_id, {match_func}(full_text, ?) as offsets
            FROM documents
            WHERE {where_clause}
        """

        # Add query parameter at the beginning for match function
        all_params = [query] + params

        cursor = self._db.execute(sql, all_params)

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

        log.info(f"Search completed: found {len(hits)} hits with mode '{search_mode}'")

        return hits


def _setup_schema(db: DatabaseService):
    """Create the transcript database schema."""
    # Apply SQLite performance optimizations
    db.execute("PRAGMA journal_mode = WAL")
    db.execute("PRAGMA synchronous = NORMAL")
    db.execute("PRAGMA cache_size = 1000000")

    # Create documents table
    db.execute("""
        CREATE TABLE documents (
            doc_id INTEGER PRIMARY KEY,
            uuid VARCHAR UNIQUE NOT NULL,
            source VARCHAR,
            episode VARCHAR,
            episode_date DATE,
            episode_title TEXT,
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

    db.execute("""
        CREATE INDEX idx_documents_uuid
        ON documents(uuid)
    """)


# ­­­­­­­­­­­­­­­­­­­­­­­­­­­­-------------------------------------------------- #
class IndexManager:
    """Global, read-only index using database-agnostic service."""
    def __init__(self, file_records: Optional[List[FileRecord]] = None, index_path: Optional[Path] = None, **db_kwargs) -> None:
        self._file_records = file_records
        self._index_path = Path(index_path) if index_path else None
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

        # For SQLite, we can copy the file directly
        import shutil
        if "path" in self._db_kwargs and self._db_kwargs["path"] != ":memory:":
            shutil.copy2(self._db_kwargs["path"], path)
        else:
            raise NotImplementedError("Cannot save in-memory SQLite database")

    def _load_index(self) -> TranscriptIndex:
        """Load index from a database file."""
        log = logging.getLogger("index")
        log.info(f'Loading index: {self._index_path}')
        if not self._index_path or not self._index_path.exists():
            raise ValueError(f"Index path not found: {self._index_path}")

        # Load from single file - use the exact path provided
        # Don't change the suffix if the file exists
        db_path = self._index_path
        
        # Create database kwargs with the correct path
        db_kwargs = self._db_kwargs.copy()
        db_kwargs['path'] = str(db_path)
        
        db = DatabaseService(**db_kwargs)

        # Apply SQLite performance optimizations for existing databases
        db.execute("PRAGMA journal_mode = WAL")
        db.execute("PRAGMA synchronous = NORMAL")
        db.execute("PRAGMA cache_size = 1000000")

        return TranscriptIndex(db)

    @staticmethod
    def split_episode(episode_str: str) -> tuple[Optional[str], str]:
        """
        Input:  '{SOURCE}/YYYY.MM.DD {TITLE}'
        Output: (ISO date 'YYYY-MM-DD' or None, title)
        """
        # take last component after the last '/'
        leaf = episode_str.rsplit('/', 1)[-1].strip()

        # date at the beginning, optional title after whitespace
        m = re.match(r'^(?P<date>\d{4}[.\-]\d{2}[.\-]\d{2})\s*(?P<title>.*)$', leaf)
        if not m:
            return None, leaf  # couldn't parse date, keep whole leaf as title

        raw_date = m.group('date').replace('.', '-')
        try:
            iso_date = datetime.strptime(raw_date, "%Y-%m-%d").date().isoformat()
        except ValueError:
            iso_date = None

        title = m.group('title').strip()
        return iso_date, title
    
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
        """
        Optimized index builder with CHUNKED TRANSACTIONS to prevent WAL explosion,
        NVMe throttling, and dramatic slowdowns.
        """
        import queue
        import threading

        log = logging.getLogger("index")
        records = list(enumerate(self._file_records))
        total_files = len(records)

        # --- Create DB service ---
        db = DatabaseService(for_index_generation=True, **self._db_kwargs)
        log.info("Setting up empty schema...")

        _setup_schema(db)

        # ----- DROP INDEXES BEFORE BULK INSERT -----
        log.info("Dropping indexes before bulk insert")
        db.execute("DROP INDEX IF EXISTS idx_segments_doc_id")
        db.execute("DROP INDEX IF EXISTS idx_segments_segment_id")
        db.execute("DROP INDEX IF EXISTS idx_segments_char_offset")
        db.execute("DROP INDEX IF EXISTS idx_segments_doc_id_segment_id")
        db.execute("DROP INDEX IF EXISTS idx_documents_uuid")

        # ----- THREADPOOL FOR JSON PARSING -----
        from concurrent.futures import ThreadPoolExecutor
        cpu_threads = min(16, os.cpu_count() or 4)
        log.info(f"Using {cpu_threads} threads for JSON parsing")

        def load_worker(rec_idx, rec):
            data = rec.read_json()
            full, segments = _episode_to_string_and_segments(data)
            episode_date, episode_title = self.split_episode(rec.id)
            source = rec.id.rsplit('/', 1)[0]
            doc_uuid = str(uuid.uuid4())

            segment_rows = [
                (
                    rec_idx,
                    s_idx,
                    seg["text"],
                    seg.get("avg_logprob", 0.0),
                    seg["char_offset"],
                    seg["start"],
                    seg["end"],
                )
                for s_idx, seg in enumerate(segments)
            ]

            document_row = (
                rec_idx,
                doc_uuid,
                source,
                rec.id,
                episode_date,
                episode_title,
                full
            )
            return (rec_idx, document_row, segment_rows)

        # ----- QUEUE + WRITER THREAD -----
        write_queue = queue.Queue(maxsize=2000)
        finished_producers = False

        DOCS_PER_TX = 1000   # ⭐⭐ CHUNK SIZE (tune 500–2000)

        def writer_thread():
            """Single DB writer with CHUNKED TRANSACTIONS."""
            nonlocal finished_producers

            db.execute("BEGIN")   # start first transaction

            docs_in_tx = 0
            batch_docs = []
            batch_segments = []

            DOC_BATCH_SIZE = 1000
            SEGMENT_BATCH_SIZE = 30000

            while True:
                try:
                    item = write_queue.get(timeout=0.2)
                except queue.Empty:
                    if finished_producers:
                        break
                    continue

                if item is None:
                    break

                doc_row, seg_rows = item

                batch_docs.append(doc_row)
                batch_segments.extend(seg_rows)
                docs_in_tx += 1

                # Flush docs if needed
                if len(batch_docs) >= DOC_BATCH_SIZE:
                    db.batch_execute(
                        """INSERT INTO documents 
                        (doc_id, uuid, source, episode, episode_date,
                            episode_title, full_text)
                        VALUES (?, ?, ?, ?, ?, ?, ?)""",
                        batch_docs
                    )
                    batch_docs = []

                # Flush segments if needed
                if len(batch_segments) >= SEGMENT_BATCH_SIZE:
                    db.batch_execute(
                        """INSERT INTO segments 
                        (doc_id, segment_id, segment_text, avg_logprob,
                            char_offset, start_time, end_time)
                        VALUES (?, ?, ?, ?, ?, ?, ?)""",
                        batch_segments
                    )
                    batch_segments = []

                # ⭐⭐ CHUNKED TRANSACTION: commit every N docs
                if docs_in_tx >= DOCS_PER_TX:
                    # Flush pending rows
                    if batch_docs:
                        db.batch_execute(
                            """INSERT INTO documents 
                            (doc_id, uuid, source, episode, episode_date,
                                episode_title, full_text)
                            VALUES (?, ?, ?, ?, ?, ?, ?)""",
                            batch_docs
                        )
                        batch_docs = []

                    if batch_segments:
                        db.batch_execute(
                            """INSERT INTO segments 
                            (doc_id, segment_id, segment_text, avg_logprob,
                                char_offset, start_time, end_time)
                            VALUES (?, ?, ?, ?, ?, ?, ?)""",
                            batch_segments
                        )
                        batch_segments = []

                    # Commit and start fresh TX
                    db.commit()
                    db.execute("BEGIN")
                    docs_in_tx = 0

            # ----- FINAL FLUSH & COMMIT -----
            if batch_docs:
                db.batch_execute(
                    """INSERT INTO documents 
                    (doc_id, uuid, source, episode, episode_date,
                        episode_title, full_text)
                    VALUES (?, ?, ?, ?, ?, ?, ?)""",
                    batch_docs
                )

            if batch_segments:
                db.batch_execute(
                    """INSERT INTO segments 
                    (doc_id, segment_id, segment_text, avg_logprob,
                        char_offset, start_time, end_time)
                    VALUES (?, ?, ?, ?, ?, ?, ?)""",
                    batch_segments
                )

            db.commit()  # final commit

        # Start writer thread
        writer = threading.Thread(target=writer_thread)
        writer.start()

        # ----- PRODUCE JSON PARSED ROWS -----
        log.info(f"Parsing & queueing {total_files} files...")
        with ThreadPoolExecutor(max_workers=cpu_threads) as pool:
            futures = [pool.submit(load_worker, i, rec) for i, rec in records]

            from tqdm.auto import tqdm
            with tqdm(total=total_files, desc="Building index", unit="file") as pbar:
                for fut in futures:
                    rec_idx, document_row, segment_rows = fut.result()
                    write_queue.put((document_row, segment_rows))
                    pbar.update(1)

        finished_producers = True
        writer.join()

        # ----- RECREATE INDEXES -----
        log.info("Recreating indexes (fast bulk build)...")

        db.execute("""CREATE INDEX idx_segments_doc_id
                    ON segments(doc_id)""")

        db.execute("""CREATE INDEX idx_segments_segment_id
                    ON segments(segment_id)""")

        db.execute("""CREATE INDEX idx_segments_char_offset
                    ON segments(char_offset)""")

        db.execute("""CREATE INDEX idx_segments_doc_id_segment_id
                    ON segments(doc_id, segment_id)""")

        db.execute("""CREATE INDEX idx_documents_uuid
                    ON documents(uuid)""")

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
