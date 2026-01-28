from pathlib import Path
import time
import logging
from dataclasses import dataclass, field
from typing import List, Tuple, Optional, Set
from concurrent.futures import ThreadPoolExecutor
import os
from tqdm.auto import tqdm
import itertools
from datetime import datetime
import re
import uuid
import bisect

from ..utils import FileRecord
from .db import DatabaseService, SQLITE_MAX_PARAMS


def _extract_fts5_tokens(query: str) -> list[str]:
    """
    Extract valid tokens from a query for use in FTS5 queries.

    FTS5 interprets certain characters as operators:
    - "-" as NOT operator
    - "+" as required term
    - "*" as prefix wildcard
    - Punctuation can cause syntax errors

    This function splits on punctuation and extracts word tokens.
    E.g., "בית־ספר" becomes ["בית", "ספר"]
          "צה״ל" becomes ["צה", "ל"]
          "hello-world!" becomes ["hello", "world"]
    """
    import regex
    # Split on any non-word character and filter empty strings
    tokens = regex.split(r'[^\w]+', query)
    return [t for t in tokens if t]


def _build_ignore_punct_pattern(query: str, word_boundary: bool = False) -> str:
    """
    Build a regex pattern that allows optional punctuation between characters.

    This handles cases like:
    - "צהל" matching "צה״ל" (gershayim inside word)
    - "גון" matching "ג׳ון" (geresh inside word)
    - "1000" matching "1,000" (comma in number)
    - "dont" matching "don't" (apostrophe)

    Args:
        query: The search query
        word_boundary: If True, add word boundaries around the pattern

    Returns:
        Regex pattern string
    """
    import regex

    # Split query into tokens (words separated by spaces)
    tokens = query.split()

    token_patterns = []
    for token in tokens:
        # For each character in the token, allow optional punctuation after it
        # But not after the last character (that's handled by word boundary or next token)
        chars = list(token)
        if len(chars) == 0:
            continue

        # Build pattern: char1 + optional_punct + char2 + optional_punct + ... + charN
        char_patterns = [regex.escape(c) for c in chars]
        # Join with optional punctuation between each character
        token_pattern = r'[\p{P}]*'.join(char_patterns)
        token_patterns.append(token_pattern)

    if not token_patterns:
        return ''

    # Join tokens with required punctuation/whitespace between them
    pattern = r'[\p{P}\s]+'.join(token_patterns)

    if word_boundary:
        pattern = r'\b' + pattern + r'\b'

    return pattern


def _classify_hit_position(hit_start: int, hit_end: int, full_text: str,
                           seg_boundaries: list[tuple[int, int]],
                           seg_offsets: list[int]) -> set[str]:
    """Return set of position labels for a hit: {'start', 'end', 'cross'}.

    Args:
        hit_start: Start character offset of the match in full_text
        hit_end: End character offset of the match in full_text
        full_text: The full document text
        seg_boundaries: List of (char_offset, segment_length) tuples
        seg_offsets: List of segment start offsets for binary search

    Returns:
        Set of position labels: 'start' if match starts in first word,
        'end' if match ends in last word, 'cross' if match spans segments.
    """
    # Find which segment contains the hit start
    idx = bisect.bisect_right(seg_offsets, hit_start) - 1
    if idx < 0:
        idx = 0

    seg_offset, seg_len = seg_boundaries[idx]
    offset_in_seg = hit_start - seg_offset
    match_len = hit_end - hit_start
    seg_text = full_text[seg_offset:seg_offset + seg_len]

    positions = set()

    # Beginning: match starts within first word
    first_space = seg_text.find(' ')
    if first_space == -1:
        # Single-word segment - any hit starting at 0 is at beginning
        if offset_in_seg == 0:
            positions.add('start')
    elif offset_in_seg < first_space:
        positions.add('start')

    # End: match ends within last word
    last_space = seg_text.rfind(' ')
    if last_space == -1:
        # Single-word segment - match ending at or after segment length is at end
        if offset_in_seg + match_len >= seg_len:
            positions.add('end')
    elif offset_in_seg + match_len > last_space:
        positions.add('end')

    # Cross-segment: match extends beyond this segment
    if offset_in_seg + match_len > seg_len:
        positions.add('cross')

    return positions


@dataclass(slots=True)
class TranscriptIndex:
    """Database-agnostic transcript index with useful query methods."""
    _db: DatabaseService
        
    def get_document_stats(self) -> tuple[int, int]:
        """Get document count and total character count from FTS5."""
        # Query documents count from documents table
        cursor = self._db.execute("SELECT COUNT(*) FROM documents")
        doc_count = cursor.fetchone()[0]

        # Query total character count from FTS5
        cursor = self._db.execute("""
            SELECT SUM(LENGTH(full_text)) as total_chars
            FROM documents_fts
        """)
        total_chars = cursor.fetchone()[0] or 0

        return (doc_count, total_chars)

    def get_document_text(self, doc_id: int) -> str:
        """Get full text of a document from FTS5."""
        cursor = self._db.execute("""
            SELECT fts.full_text
            FROM documents_fts fts
            JOIN fts_doc_mapping m ON fts.rowid = m.fts_rowid
            WHERE m.doc_id = ?
        """, [doc_id])
        result = cursor.fetchone()
        if not result:
            raise IndexError(f"Document {doc_id} not found in FTS5")
        return result[0]
    
    def get_document_info(self, doc_id: int) -> dict:
        """Get document information including source, episode (no full_text - use get_document_text for that)."""
        cursor = self._db.execute(
                "SELECT doc_id, uuid, source, episode, episode_date, episode_title FROM documents WHERE doc_id = ?",
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
            "episode_title": result[5]
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

        Uses a temporary table for optimal performance.
        """
        if not lookups:
            return []

        logger = logging.getLogger(__name__)
        logger.debug(f"Fetching segments by IDs: {len(lookups)} lookups")

        # For large batches, use temporary table (more efficient)
        # Create temp table
        self._db.execute("""
            CREATE TEMPORARY TABLE IF NOT EXISTS temp_segment_lookups (
                doc_id INTEGER,
                segment_id INTEGER,
                PRIMARY KEY (doc_id, segment_id)
            ) WITHOUT ROWID
        """)

        # Clear any existing data
        self._db.execute("DELETE FROM temp_segment_lookups")

        # Batch insert lookups (SQLite variable limit)
        BATCH_SIZE = SQLITE_MAX_PARAMS // 2  # Each lookup pair = 2 parameters
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

    def _get_segment_boundaries(self, doc_id: int) -> list[tuple[int, int]]:
        """Return sorted list of (char_offset, segment_length) for a document."""
        cursor = self._db.execute(
            "SELECT char_offset, LENGTH(segment_text) FROM segments WHERE doc_id = ? ORDER BY char_offset",
            [doc_id]
        )
        return cursor.fetchall()

    def search_hits(self, query: str, search_mode: str = 'partial', date_from: Optional[str] = None,
                   date_to: Optional[str] = None, sources: Optional[list[str]] = None,
                   ignore_punct: bool = False,
                   position_filters: Optional[Set[str]] = None) -> list[tuple[int, int]]:
        """Search for query and return (episode_idx, char_offset) pairs for hits.

        Args:
            query: Search query string
            search_mode: Search mode - 'exact' (whole words), 'partial' (substring), or 'regex'
            date_from: Optional start date filter (YYYY-MM-DD format)
            date_to: Optional end date filter (YYYY-MM-DD format)
            sources: Optional list of sources to filter by
            ignore_punct: If True, ignore punctuation between words when matching
            position_filters: Optional set of position labels to filter by ('start', 'end', 'cross')
        """
        return self._search_fts5(query, search_mode, date_from, date_to, sources, ignore_punct, position_filters)

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

    def _search_fts5(self, query: str, search_mode: str = 'partial',
                     date_from: Optional[str] = None, date_to: Optional[str] = None,
                     sources: Optional[list[str]] = None,
                     ignore_punct: bool = False,
                     position_filters: Optional[Set[str]] = None) -> list[tuple[int, int]]:
        """Search using FTS5 with mode-specific strategies."""
        log = logging.getLogger("index")
        log.info(f"FTS5 search: query={query}, mode={search_mode}, ignore_punct={ignore_punct}, position_filters={position_filters}")

        if search_mode == 'exact':
            return self._search_fts5_exact(query, date_from, date_to, sources, ignore_punct, position_filters)
        elif search_mode == 'partial':
            return self._search_fts5_partial(query, date_from, date_to, sources, ignore_punct, position_filters)
        elif search_mode == 'regex':
            return self._search_fts5_regex(query, date_from, date_to, sources, position_filters)
        else:
            raise ValueError(f"Unknown search mode: {search_mode}")

    def _search_fts5_exact(self, query: str, date_from, date_to, sources, ignore_punct: bool = False,
                           position_filters: Optional[Set[str]] = None):
        """Exact word match using FTS5 phrase query."""
        import regex

        log = logging.getLogger("index")

        # Extract valid tokens for FTS5 query (split on punctuation)
        fts_tokens = _extract_fts5_tokens(query)

        if not fts_tokens:
            log.warning(f"Query '{query}' has no valid tokens after extraction")
            return []

        # FTS5 phrase query: "word1 word2" matches tokens in sequence
        fts_query = '"' + ' '.join(fts_tokens) + '"'

        # Build query with filters
        sql = """
            SELECT m.doc_id, fts.full_text
            FROM documents_fts fts
            JOIN fts_doc_mapping m ON fts.rowid = m.fts_rowid
            JOIN documents d ON m.doc_id = d.doc_id
            WHERE documents_fts MATCH ?
        """
        params = [fts_query]

        # Add date filters
        if date_from:
            sql += " AND d.episode_date >= ?"
            params.append(date_from)
        if date_to:
            sql += " AND d.episode_date <= ?"
            params.append(date_to)

        # Add source filter
        if sources and len(sources) > 0:
            placeholders = ','.join('?' * len(sources))
            sql += f" AND d.source IN ({placeholders})"
            params.extend(sources)

        cursor = self._db.execute(sql, params)
        results = cursor.fetchall()

        # Extract character offsets using word boundary regex
        hits = []
        if ignore_punct:
            # Use pattern that allows punctuation between/within characters
            pattern = _build_ignore_punct_pattern(query, word_boundary=True)
        else:
            pattern = r'\b' + regex.escape(query) + r'\b'

        if not pattern:
            return []

        compiled_pattern = regex.compile(pattern)

        for doc_id, full_text in results:
            # Get segment boundaries if position filtering is enabled
            seg_boundaries = None
            seg_offsets = None
            if position_filters:
                seg_boundaries = self._get_segment_boundaries(doc_id)
                seg_offsets = [s[0] for s in seg_boundaries]

            for match in compiled_pattern.finditer(full_text):
                # Apply position filter if specified
                if position_filters and seg_boundaries:
                    positions = _classify_hit_position(
                        match.start(), match.end(), full_text, seg_boundaries, seg_offsets
                    )
                    if not positions & position_filters:
                        continue
                hits.append((doc_id, match.start()))

        log.info(f"Exact search completed: {len(hits)} hits from {len(results)} candidates")
        return hits

    def _search_fts5_partial(self, query: str, date_from, date_to, sources, ignore_punct: bool = False,
                             position_filters: Optional[Set[str]] = None):
        """Partial/substring match using FTS5 prefix matching + Python regex."""
        import regex

        log = logging.getLogger("index")

        # Extract valid tokens for FTS5 query (split on punctuation)
        fts_tokens = _extract_fts5_tokens(query)

        # Build FTS5 query for candidate narrowing
        # Use OR to match any token prefix (broader candidates)
        if fts_tokens:
            fts_query = ' OR '.join([f'{token}*' for token in fts_tokens])
            use_fts_filter = True
        else:
            # No valid tokens - fall back to full scan
            log.warning(f"Query '{query}' has no valid tokens - falling back to full scan")
            use_fts_filter = False

        if use_fts_filter:
            sql = """
                SELECT m.doc_id, fts.full_text
                FROM documents_fts fts
                JOIN fts_doc_mapping m ON fts.rowid = m.fts_rowid
                JOIN documents d ON m.doc_id = d.doc_id
                WHERE documents_fts MATCH ?
            """
            params = [fts_query]
        else:
            sql = """
                SELECT d.doc_id, fts.full_text
                FROM documents_fts fts
                JOIN fts_doc_mapping m ON fts.rowid = m.fts_rowid
                JOIN documents d ON m.doc_id = d.doc_id
                WHERE 1=1
            """
            params = []

        # Add filters
        if date_from:
            sql += " AND d.episode_date >= ?"
            params.append(date_from)
        if date_to:
            sql += " AND d.episode_date <= ?"
            params.append(date_to)
        if sources and len(sources) > 0:
            placeholders = ','.join('?' * len(sources))
            sql += f" AND d.source IN ({placeholders})"
            params.extend(sources)

        cursor = self._db.execute(sql, params)
        results = cursor.fetchall()

        # Python substring search on candidates
        hits = []
        if ignore_punct:
            # Use pattern that allows punctuation between/within characters (no word boundary)
            escaped_pattern = _build_ignore_punct_pattern(query, word_boundary=False)
        else:
            escaped_pattern = regex.escape(query)

        if not escaped_pattern:
            return []

        compiled_pattern = regex.compile(escaped_pattern)

        for doc_id, full_text in results:
            # Get segment boundaries if position filtering is enabled
            seg_boundaries = None
            seg_offsets = None
            if position_filters:
                seg_boundaries = self._get_segment_boundaries(doc_id)
                seg_offsets = [s[0] for s in seg_boundaries]

            for match in compiled_pattern.finditer(full_text):
                # Apply position filter if specified
                if position_filters and seg_boundaries:
                    positions = _classify_hit_position(
                        match.start(), match.end(), full_text, seg_boundaries, seg_offsets
                    )
                    if not positions & position_filters:
                        continue
                hits.append((doc_id, match.start()))

        log.info(f"Partial search completed: {len(hits)} hits from {len(results)} candidates")
        return hits

    def _search_fts5_regex(self, query: str, date_from, date_to, sources,
                           position_filters: Optional[Set[str]] = None):
        """Regex search with FTS5 candidate narrowing."""
        import regex

        log = logging.getLogger("index")

        # Extract potential literal tokens from regex pattern
        # Look for sequences of 2+ word characters
        potential_tokens = regex.findall(r'\w{2,}', query)

        if potential_tokens:
            # Use first token as FTS5 filter (prefix match)
            fts_query = f'{potential_tokens[0]}*'
            use_fts_filter = True
        else:
            # No good token - fetch all documents (slow path)
            fts_query = None
            use_fts_filter = False
            log.warning(f"Regex pattern '{query}' has no extractable tokens - full scan")

        # Build query
        if use_fts_filter:
            sql = """
                SELECT m.doc_id, fts.full_text
                FROM documents_fts fts
                JOIN fts_doc_mapping m ON fts.rowid = m.fts_rowid
                JOIN documents d ON m.doc_id = d.doc_id
                WHERE documents_fts MATCH ?
            """
            params = [fts_query]
        else:
            sql = """
                SELECT d.doc_id, fts.full_text
                FROM documents_fts fts
                JOIN fts_doc_mapping m ON fts.rowid = m.fts_rowid
                JOIN documents d ON m.doc_id = d.doc_id
                WHERE 1=1
            """
            params = []

        # Add filters
        if date_from:
            sql += " AND d.episode_date >= ?"
            params.append(date_from)
        if date_to:
            sql += " AND d.episode_date <= ?"
            params.append(date_to)
        if sources and len(sources) > 0:
            placeholders = ','.join('?' * len(sources))
            sql += f" AND d.source IN ({placeholders})"
            params.extend(sources)

        cursor = self._db.execute(sql, params)
        results = cursor.fetchall()

        # Apply full regex on candidates
        hits = []
        try:
            compiled_regex = regex.compile(query)
            for doc_id, full_text in results:
                # Get segment boundaries if position filtering is enabled
                seg_boundaries = None
                seg_offsets = None
                if position_filters:
                    seg_boundaries = self._get_segment_boundaries(doc_id)
                    seg_offsets = [s[0] for s in seg_boundaries]

                for match in compiled_regex.finditer(full_text):
                    # Apply position filter if specified
                    if position_filters and seg_boundaries:
                        positions = _classify_hit_position(
                            match.start(), match.end(), full_text, seg_boundaries, seg_offsets
                        )
                        if not positions & position_filters:
                            continue
                    hits.append((doc_id, match.start()))
        except regex.error as e:
            log.error(f"Invalid regex pattern: {query}, error: {e}")
            return []

        log.info(f"Regex search completed: {len(hits)} hits from {len(results)} candidates")
        return hits


def _setup_schema(db: DatabaseService):
    """Create the transcript database schema."""
    # Apply SQLite performance optimizations
    db.execute("PRAGMA journal_mode = WAL")
    db.execute("PRAGMA synchronous = NORMAL")
    # Note: cache_size is set globally in DatabaseService connection setup

    # Create documents table (WITHOUT full_text - now in FTS5)
    db.execute("""
        CREATE TABLE documents (
            doc_id INTEGER PRIMARY KEY,
            uuid VARCHAR UNIQUE NOT NULL,
            source VARCHAR,
            episode VARCHAR,
            episode_date DATE,
            episode_title TEXT
        )
    """)

    # Create FTS5 virtual table for full-text search
    # Note: FTS5 stores both content and index (no content='' option)
    # so we can retrieve full_text in queries
    db.execute("""
        CREATE VIRTUAL TABLE documents_fts USING fts5(
            full_text,
            tokenize='unicode61 remove_diacritics 0'
        )
    """)

    # Create FTS5 mapping table (bridge FTS5 rowid <-> doc_id)
    db.execute("""
        CREATE TABLE IF NOT EXISTS fts_doc_mapping (
            fts_rowid INTEGER PRIMARY KEY,
            doc_id INTEGER NOT NULL,
            FOREIGN KEY (doc_id) REFERENCES documents(doc_id)
        )
    """)

    # Create segments table
    db.execute("""
        CREATE TABLE IF NOT EXISTS segments (
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
        CREATE INDEX IF NOT EXISTS idx_segments_doc_id
        ON segments(doc_id)
    """)

    db.execute("""
        CREATE INDEX IF NOT EXISTS idx_segments_segment_id
        ON segments(segment_id)
    """)

    db.execute("""
        CREATE INDEX IF NOT EXISTS idx_segments_char_offset
        ON segments(char_offset)
    """)

    db.execute("""
        CREATE INDEX IF NOT EXISTS idx_segments_doc_id_segment_id
        ON segments(doc_id, segment_id)
    """)

    db.execute("""
        CREATE INDEX IF NOT EXISTS idx_documents_uuid
        ON documents(uuid)
    """)

    db.execute("""
        CREATE INDEX IF NOT EXISTS idx_documents_date
        ON documents(episode_date)
    """)

    db.execute("""
        CREATE INDEX IF NOT EXISTS idx_documents_source
        ON documents(source)
    """)

    db.execute("""
        CREATE INDEX IF NOT EXISTS idx_fts_doc_mapping_doc_id
        ON fts_doc_mapping(doc_id)
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
        
        # Load from single file
        db_path = self._index_path

        # Create database kwargs with the correct path
        db_kwargs = self._db_kwargs.copy()
        db_kwargs['path'] = str(db_path)
        
        db = DatabaseService(**db_kwargs)

        # Apply SQLite performance optimizations for existing databases
        db.execute("PRAGMA journal_mode = WAL")
        db.execute("PRAGMA synchronous = NORMAL")
        # Note: cache_size is set globally in DatabaseService connection setup

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

        # --- Clean up existing data to avoid duplicates/PK errors ---
        log.info("Dropping existing tables if present...")
        db.execute("DROP TABLE IF EXISTS segments")
        db.execute("DROP TABLE IF EXISTS fts_doc_mapping")
        db.execute("DROP TABLE IF EXISTS documents_fts")  # FTS5 virtual table
        db.execute("DROP TABLE IF EXISTS documents")
        db.commit()

        log.info("Setting up empty schema...")
        _setup_schema(db)

        # ----- DROP INDEXES BEFORE BULK INSERT -----
        log.info("Dropping indexes before bulk insert")
        db.execute("DROP INDEX IF EXISTS idx_segments_doc_id")
        db.execute("DROP INDEX IF EXISTS idx_segments_segment_id")
        db.execute("DROP INDEX IF EXISTS idx_segments_char_offset")
        db.execute("DROP INDEX IF EXISTS idx_segments_doc_id_segment_id")
        db.execute("DROP INDEX IF EXISTS idx_documents_uuid")
        db.execute("DROP INDEX IF EXISTS idx_documents_date")
        db.execute("DROP INDEX IF EXISTS idx_documents_source")
        db.execute("DROP INDEX IF EXISTS idx_fts_doc_mapping_doc_id")
        # Note: FTS5 manages its own indexes internally

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

            # Document metadata (WITHOUT full_text - will be stored in FTS5)
            document_row = (
                rec_idx,
                doc_uuid,
                source,
                rec.id,
                episode_date,
                episode_title
            )
            # Return full_text as separate 4th element for FTS5 insertion
            return (rec_idx, document_row, segment_rows, full)

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
            batch_fts = []          # FTS5 full_text entries
            batch_mapping = []       # (fts_rowid, doc_id) pairs

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

                doc_row, seg_rows, full_text = item

                batch_docs.append(doc_row)
                batch_segments.extend(seg_rows)
                batch_fts.append(full_text)  # Store full_text for FTS5
                docs_in_tx += 1

                # Flush docs if needed
                if len(batch_docs) >= DOC_BATCH_SIZE:
                    # Insert documents (WITHOUT full_text)
                    db.batch_execute(
                        """INSERT INTO documents
                        (doc_id, uuid, source, episode, episode_date, episode_title)
                        VALUES (?, ?, ?, ?, ?, ?)""",
                        batch_docs
                    )

                    # Insert into FTS5 and build mapping
                    for idx, full_text in enumerate(batch_fts):
                        cursor = db.execute(
                            "INSERT INTO documents_fts(full_text) VALUES (?)",
                            [full_text]
                        )
                        fts_rowid = cursor.lastrowid
                        doc_id = batch_docs[idx][0]  # First element is doc_id
                        batch_mapping.append((fts_rowid, doc_id))

                    # Insert mappings
                    db.batch_execute(
                        "INSERT INTO fts_doc_mapping(fts_rowid, doc_id) VALUES (?, ?)",
                        batch_mapping
                    )

                    batch_docs = []
                    batch_fts = []
                    batch_mapping = []

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
                        # Insert documents (WITHOUT full_text)
                        db.batch_execute(
                            """INSERT INTO documents
                            (doc_id, uuid, source, episode, episode_date, episode_title)
                            VALUES (?, ?, ?, ?, ?, ?)""",
                            batch_docs
                        )

                        # Insert into FTS5 and build mapping
                        for idx, full_text in enumerate(batch_fts):
                            cursor = db.execute(
                                "INSERT INTO documents_fts(full_text) VALUES (?)",
                                [full_text]
                            )
                            fts_rowid = cursor.lastrowid
                            doc_id = batch_docs[idx][0]  # First element is doc_id
                            batch_mapping.append((fts_rowid, doc_id))

                        # Insert mappings
                        db.batch_execute(
                            "INSERT INTO fts_doc_mapping(fts_rowid, doc_id) VALUES (?, ?)",
                            batch_mapping
                        )

                        batch_docs = []
                        batch_fts = []
                        batch_mapping = []

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
                # Insert documents (WITHOUT full_text)
                db.batch_execute(
                    """INSERT INTO documents
                    (doc_id, uuid, source, episode, episode_date, episode_title)
                    VALUES (?, ?, ?, ?, ?, ?)""",
                    batch_docs
                )

                # Insert into FTS5 and build mapping
                for idx, full_text in enumerate(batch_fts):
                    cursor = db.execute(
                        "INSERT INTO documents_fts(full_text) VALUES (?)",
                        [full_text]
                    )
                    fts_rowid = cursor.lastrowid
                    doc_id = batch_docs[idx][0]  # First element is doc_id
                    batch_mapping.append((fts_rowid, doc_id))

                # Insert mappings
                db.batch_execute(
                    "INSERT INTO fts_doc_mapping(fts_rowid, doc_id) VALUES (?, ?)",
                    batch_mapping
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
                    rec_idx, document_row, segment_rows, full_text = fut.result()
                    write_queue.put((document_row, segment_rows, full_text))
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

        db.execute("""CREATE INDEX idx_documents_date
                    ON documents(episode_date)""")

        db.execute("""CREATE INDEX idx_documents_source
                    ON documents(source)""")

        db.execute("""CREATE INDEX idx_fts_doc_mapping_doc_id
                    ON fts_doc_mapping(doc_id)""")

        # Optimize FTS5 index after bulk insert
        log.info("Optimizing FTS5 index...")
        db.execute("INSERT INTO documents_fts(documents_fts) VALUES('optimize')")

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
