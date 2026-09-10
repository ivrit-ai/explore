"""PostgreSQL-backed transcript index.

Mirrors the read surface of :class:`app.services.index.TranscriptIndex` so
SearchService and the routes work against either backend unchanged. The
SQLite implementation stays the local-development path; this one is what runs
where the index lives in a managed Postgres.

Two things differ from the SQLite version on purpose:

* **Full-text matching** uses a ``tsvector`` column with a GIN index instead
  of an FTS5 virtual table. Both are only a *candidate* filter — the exact hit
  offsets still come from the regex post-filter, so the two backends return the
  same hits for the same query.
* **Batch lookups issue one round trip** instead of one query per key. Over a
  network connection the per-query latency dominates, so the segment lookups
  fan out through ``unnest`` + ``LATERAL`` rather than a Python loop.
"""

from __future__ import annotations

import logging
import os
import time
from typing import Iterable, Optional, Sequence

logger = logging.getLogger(__name__)

# Documents whose full_text is fetched per round trip during the post-filter.
# Bounds peak memory to roughly this many documents' text rather than the whole
# result set, which matters when a caller asks for a large page.
_TEXT_CHUNK = 200

_FTS_CONFIG = "simple"   # no stemming, no stopwords — closest to FTS5 unicode61


def _quote_lexeme(token: str) -> str:
    """Quote a token for use inside a tsquery literal."""
    return "'" + token.replace("'", "''") + "'"


# Postgres stores tsvector positions in 14 bits and clamps everything past
# this onto the last slot, which breaks adjacency (``<->``) beyond that point.
TSV_MAX_POSITION = 16383


def build_tsquery(query: str, search_mode: str) -> tuple[Optional[str], Optional[str]]:
    """Translate a user query into a tsquery.

    Returns ``(sql_function, argument)`` where sql_function is the Postgres
    function to call and argument is passed as a bound parameter, or
    ``(None, None)`` when the query yields no usable tokens and the caller
    must fall back to scanning every document.

    Exact mode returns the phrase form. It is only usable on documents short
    enough to keep real positions; see ``_match_clause`` for how the long ones
    are handled.
    """
    import regex

    if search_mode == "exact":
        if not query.split():
            return None, None
        return "phraseto_tsquery", query

    if search_mode == "partial":
        tokens = query.split()
        if not tokens:
            return None, None
        return "to_tsquery", " | ".join(f"{_quote_lexeme(t)}:*" for t in tokens)

    if search_mode == "regex":
        tokens = regex.findall(r"\w{2,}", query)
        if not tokens:
            return None, None
        return "to_tsquery", " & ".join(f"{_quote_lexeme(t)}:*" for t in tokens[:3])

    raise ValueError(f"Unknown search mode: {search_mode}")


def build_and_tsquery(query: str) -> Optional[str]:
    """The position-free fallback for exact mode: every token, ANDed.

    A document containing the phrase necessarily contains all of its tokens,
    so this is a strict superset of the phrase match and never misses one.
    """
    tokens = query.split()
    if not tokens:
        return None
    return " & ".join(_quote_lexeme(t) for t in tokens)


class PostgresTranscriptIndex:
    """Read-only transcript index over PostgreSQL."""

    def __init__(self, pool):
        self._pool = pool

    # ── plumbing ────────────────────────────────────────────────────────
    def _query(self, sql: str, params: Sequence = ()) -> list[tuple]:
        with self._pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, params)
                return cur.fetchall()

    def _one(self, sql: str, params: Sequence = ()) -> Optional[tuple]:
        with self._pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, params)
                return cur.fetchone()

    def _match_clause(self, query: str, search_mode: str) -> tuple[Optional[str], list]:
        """Build the tsvector WHERE fragment and its parameters.

        Exact mode is a union of two branches. Documents whose tsvector
        positions are intact get a true phrase match. The 1% of transcripts
        long enough to have their positions clamped at TSV_MAX_POSITION cannot
        answer a phrase query at all, so they fall back to ANDing the tokens —
        a superset, which the regex post-filter then narrows to real hits.

        Splitting it this way keeps phrase precision for almost every document
        instead of degrading the whole corpus to the AND filter, and the
        overflow branch reads a partial index covering only those documents.
        """
        func, arg = build_tsquery(query, search_mode)
        if func is None:
            return None, []

        if search_mode != "exact":
            return f"d.full_text_tsv @@ {func}(%s, %s)", [_FTS_CONFIG, arg]

        and_arg = build_and_tsquery(query)
        clause = (
            "((NOT d.tsv_overflow AND d.full_text_tsv @@ phraseto_tsquery(%s, %s))"
            " OR (d.tsv_overflow AND d.full_text_tsv @@ to_tsquery(%s, %s)))"
        )
        return clause, [_FTS_CONFIG, arg, _FTS_CONFIG, and_arg]

    @staticmethod
    def _append_filters(sql: str, params: list, date_from, date_to, sources) -> tuple[str, list]:
        if date_from:
            sql += " AND d.episode_date >= %s"
            params.append(date_from)
        if date_to:
            sql += " AND d.episode_date <= %s"
            params.append(date_to)
        if sources:
            sql += " AND d.source = ANY(%s)"
            params.append(list(sources))
        return sql, params

    # ── document reads ──────────────────────────────────────────────────
    def get_document_stats(self) -> tuple[int, int]:
        # full_text_len is stored so this does not have to detoast ~1.6 GB of
        # transcript text on every startup.
        row = self._one("SELECT COUNT(*), COALESCE(SUM(full_text_len), 0) FROM documents")
        return (row[0], row[1]) if row else (0, 0)

    def get_document_text(self, doc_id: int) -> str:
        row = self._one("SELECT full_text FROM documents WHERE doc_id = %s", [doc_id])
        if not row:
            raise IndexError(f"Document {doc_id} not found")
        return row[0]

    def get_document_info(self, doc_id: int) -> dict:
        row = self._one(
            "SELECT doc_id, uuid, source, episode, episode_date, episode_title "
            "FROM documents WHERE doc_id = %s",
            [doc_id],
        )
        if not row:
            raise IndexError(f"Document {doc_id} not found")
        return _doc_dict(row)

    def get_episode_by_uuid(self, doc_uuid: str) -> str:
        row = self._one("SELECT episode FROM documents WHERE uuid = %s", [doc_uuid])
        if not row:
            raise IndexError(f"Document with UUID {doc_uuid} not found")
        return row[0]

    def get_source_by_episode_idx(self, episode_idx: int) -> str:
        row = self._one("SELECT source FROM documents WHERE doc_id = %s", [episode_idx])
        if not row:
            raise IndexError(f"Document {episode_idx} not found")
        return row[0]

    def get_documents_batch(self, doc_ids: list[int]) -> dict[int, dict]:
        if not doc_ids:
            return {}
        rows = self._query(
            "SELECT doc_id, uuid, source, episode, episode_date, episode_title "
            "FROM documents WHERE doc_id = ANY(%s)",
            [list(set(doc_ids))],
        )
        return {row[0]: _doc_dict(row) for row in rows}

    # ── segment reads ───────────────────────────────────────────────────
    def get_segments_for_document(self, doc_id: int) -> list[dict]:
        rows = self._query(
            "SELECT segment_id, segment_text, avg_logprob, char_offset, start_time, end_time "
            "FROM segments WHERE doc_id = %s ORDER BY segment_id",
            [doc_id],
        )
        return [_seg_dict(r) for r in rows]

    def get_segments_by_ids(self, lookups: list[tuple[int, int]]) -> list[dict]:
        """One round trip for every (doc_id, segment_id) pair."""
        if not lookups:
            return []
        t0 = time.perf_counter()
        doc_ids = [d for d, _ in lookups]
        seg_ids = [s for _, s in lookups]
        rows = self._query(
            """
            SELECT s.doc_id, s.segment_id, s.segment_text, s.avg_logprob,
                   s.char_offset, s.start_time, s.end_time
            FROM segments s
            JOIN unnest(%s::int[], %s::int[]) AS p(doc_id, segment_id)
              ON s.doc_id = p.doc_id AND s.segment_id = p.segment_id
            """,
            [doc_ids, seg_ids],
        )
        result = [
            {
                "doc_id": r[0], "segment_id": r[1], "text": r[2], "avg_logprob": r[3],
                "char_offset": r[4], "start_time": r[5], "end_time": r[6],
            }
            for r in rows
        ]
        logger.info(
            "Fetched segments by IDs: %d lookups, %d results in %.1fms",
            len(lookups), len(result), (time.perf_counter() - t0) * 1000,
        )
        return result

    def get_segment_at_offset(self, doc_id: int, char_offset: int) -> dict:
        row = self._one(
            "SELECT segment_id, segment_text, avg_logprob, char_offset, start_time, end_time "
            "FROM segments WHERE doc_id = %s AND char_offset <= %s "
            "ORDER BY char_offset DESC LIMIT 1",
            [doc_id, char_offset],
        )
        if not row:
            raise IndexError(f"No segment found at offset {char_offset} for document {doc_id}")
        return _seg_dict(row)

    def get_segments_at_offsets(self, pairs: list[tuple[int, int]]) -> dict[tuple[int, int], dict]:
        """Floor-lookup every (doc_id, char_offset) pair in a single query."""
        if not pairs:
            return {}
        doc_ids = [d for d, _ in pairs]
        offsets = [o for _, o in pairs]
        rows = self._query(
            """
            SELECT p.doc_id, p.char_offset,
                   s.segment_id, s.segment_text, s.avg_logprob,
                   s.char_offset, s.start_time, s.end_time
            FROM unnest(%s::int[], %s::int[]) AS p(doc_id, char_offset)
            CROSS JOIN LATERAL (
                SELECT segment_id, segment_text, avg_logprob, char_offset,
                       start_time, end_time
                FROM segments
                WHERE doc_id = p.doc_id AND char_offset <= p.char_offset
                ORDER BY char_offset DESC
                LIMIT 1
            ) s
            """,
            [doc_ids, offsets],
        )
        return {(r[0], r[1]): _seg_dict(r[2:]) for r in rows}

    # ── search ──────────────────────────────────────────────────────────
    def get_search_metadata(self, query: str, search_mode: str = "partial",
                            date_from: Optional[str] = None, date_to: Optional[str] = None,
                            sources: Optional[list[str]] = None) -> dict:
        """Aggregated source counts and date range for a query."""
        match, params = self._match_clause(query, search_mode)
        if match is None:
            return {"sources": {}, "date_range": {"min": None, "max": None}, "total_docs": 0}

        sql = f"""
            SELECT d.source, COUNT(*), MIN(d.episode_date), MAX(d.episode_date)
            FROM documents d
            WHERE {match}
        """
        sql, params = self._append_filters(sql, params, date_from, date_to, sources)
        sql += " GROUP BY d.source"

        sources_dict: dict[str, int] = {}
        global_min = global_max = None
        total_docs = 0
        for source, cnt, min_date, max_date in self._query(sql, params):
            if source:
                sources_dict[source] = cnt
            total_docs += cnt
            if min_date and (global_min is None or min_date < global_min):
                global_min = min_date
            if max_date and (global_max is None or max_date > global_max):
                global_max = max_date

        return {
            "sources": sources_dict,
            "date_range": {"min": _iso(global_min), "max": _iso(global_max)},
            "total_docs": total_docs,
        }

    def search_hits(self, query: str, search_mode: str = "partial",
                    date_from: Optional[str] = None, date_to: Optional[str] = None,
                    sources: Optional[list[str]] = None,
                    doc_limit: int = 0, doc_offset: int = 0,
                    seed: Optional[int] = None) -> tuple[list[tuple[int, int]], bool]:
        """Candidate docs from the GIN index, then a regex pass for exact offsets."""
        import random as random_mod
        import regex

        if search_mode not in ("exact", "partial", "regex"):
            raise ValueError(f"Unknown search mode: {search_mode}")

        t_start = time.perf_counter()

        # ── 1. candidate doc_ids ────────────────────────────────────────
        match, params = self._match_clause(query, search_mode)
        if match is None:
            logger.warning("Query %r has no extractable tokens - full scan", query)
            sql = "SELECT d.doc_id FROM documents d WHERE TRUE"
            params = []
        else:
            sql = f"SELECT d.doc_id FROM documents d WHERE {match}"
        sql, params = self._append_filters(sql, params, date_from, date_to, sources)
        sql += " ORDER BY d.doc_id"

        if seed is None:
            if doc_limit:
                sql += " LIMIT %s OFFSET %s"
                params.extend([doc_limit + 1, doc_offset])   # +1 to detect has_more
            elif doc_offset:
                sql += " OFFSET %s"
                params.append(doc_offset)

        t_ids = time.perf_counter()
        doc_ids = [r[0] for r in self._query(sql, params)]
        t_ids_done = time.perf_counter()
        logger.info("[BENCH] doc_ids: %.1fms, %d ids", (t_ids_done - t_ids) * 1000, len(doc_ids))

        if not doc_ids:
            return [], False

        # ── 2. paginate ─────────────────────────────────────────────────
        if seed is not None:
            rng = random_mod.Random(seed)
            rng.shuffle(doc_ids)
            fetch_limit = doc_limit if doc_limit else len(doc_ids)
            page_ids = doc_ids[doc_offset:doc_offset + fetch_limit + 1]
            has_more = len(page_ids) > fetch_limit
            if has_more:
                page_ids = page_ids[:fetch_limit]
        elif doc_limit and len(doc_ids) > doc_limit:
            has_more, page_ids = True, doc_ids[:doc_limit]
        else:
            has_more, page_ids = False, doc_ids

        if not page_ids:
            return [], False

        # ── 3. regex post-filter, chunked so peak memory stays bounded ──
        if search_mode == "exact":
            pattern = r"\b" + regex.escape(query) + r"\b"
        elif search_mode == "partial":
            pattern = regex.escape(query)
        else:
            pattern = query

        try:
            compiled = regex.compile(pattern)
        except regex.error as exc:
            logger.error("Invalid regex pattern %r: %s", query, exc)
            return [], False

        hits: list[tuple[int, int]] = []
        t_scan = time.perf_counter()
        for chunk in _chunks(page_ids, _TEXT_CHUNK):
            rows = self._query(
                "SELECT doc_id, full_text FROM documents WHERE doc_id = ANY(%s)", [list(chunk)]
            )
            texts = dict(rows)
            for doc_id in chunk:
                full_text = texts.get(doc_id)
                if full_text is None:
                    continue
                hits.extend((doc_id, m.start()) for m in compiled.finditer(full_text))
            texts.clear()
        t_scan_done = time.perf_counter()

        logger.info(
            "[BENCH] %s%s: ids=%.1fms, scan=%.1fms (%d docs), %d hits, has_more=%s, total=%.1fms",
            search_mode, " shuffle" if seed is not None else "",
            (t_ids_done - t_ids) * 1000, (t_scan_done - t_scan) * 1000,
            len(page_ids), len(hits), has_more, (time.perf_counter() - t_start) * 1000,
        )
        return hits, has_more


def _iso(value):
    """Dates as ISO strings, matching what the sqlite backend returns."""
    return value.isoformat() if hasattr(value, "isoformat") else value


def _doc_dict(row: Sequence) -> dict:
    return {
        "doc_id": row[0], "uuid": row[1], "source": row[2],
        "episode": row[3], "episode_date": _iso(row[4]), "episode_title": row[5],
    }


def _seg_dict(row: Sequence) -> dict:
    return {
        "segment_id": row[0], "text": row[1], "avg_logprob": row[2],
        "char_offset": row[3], "start_time": row[4], "end_time": row[5],
    }


def _chunks(seq: Sequence, size: int) -> Iterable[Sequence]:
    for i in range(0, len(seq), size):
        yield seq[i:i + size]


def normalise_dsn(url: str) -> str:
    """Accept the SQLAlchemy-style scheme some hosts inject."""
    return url.replace("postgresql+psycopg://", "postgresql://", 1)


class PostgresIndexManager:
    """Drop-in replacement for IndexManager backed by a connection pool."""

    def __init__(self, dsn: Optional[str] = None, min_size: int = 1, max_size: int = 4):
        from psycopg_pool import ConnectionPool

        dsn = dsn or os.environ.get("DATABASE_URL")
        if not dsn:
            raise ValueError("Postgres index requires a DSN (DATABASE_URL)")
        self._pool = ConnectionPool(
            normalise_dsn(dsn), min_size=min_size, max_size=max_size, open=True,
            kwargs={"options": "-c statement_timeout=120000"},
        )
        self._index = PostgresTranscriptIndex(self._pool)
        logger.info("Postgres index ready (pool %d-%d)", min_size, max_size)

    def get(self) -> PostgresTranscriptIndex:
        return self._index

    def close(self) -> None:
        self._pool.close()
