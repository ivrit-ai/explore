#!/usr/bin/env python
"""Populate the PostgreSQL transcript index from an explore.sqlite file.

This runs once, out of band — the app never builds the index at startup. The
transfer is a few gigabytes, so every stage is restartable: progress is
recorded in a ``migration_progress`` table and a re-run picks up where it
stopped rather than starting over.

    python scripts/migrate_sqlite_to_postgres.py --sqlite explore.sqlite \
        --dsn "postgresql://user:token@db.xhostd.com:5432/explore?sslmode=require"

Stages run in order and can be selected individually with --stage:

    schema     create tables (no indexes yet — they are built last)
    documents  copy the 34.5k documents, including full_text
    segments   copy the 33M segments, resumable in doc_id batches
    indexes    build the GIN full-text index and the segment indexes
    verify     compare row counts and spot-check a document

Indexes are deliberately created after the bulk load; building them up front
makes the COPY several times slower.
"""

from __future__ import annotations

import argparse
import logging
import os
import sqlite3
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

log = logging.getLogger("migrate")

DOC_BATCH = 500        # documents per COPY flush (their full_text is large)
SEG_DOC_BATCH = 200    # documents' worth of segments per resumable batch

SCHEMA = """
CREATE TABLE IF NOT EXISTS documents (
    doc_id        INTEGER PRIMARY KEY,
    uuid          TEXT UNIQUE NOT NULL,
    source        TEXT,
    episode       TEXT,
    episode_date  DATE,
    episode_title TEXT,
    full_text     TEXT,
    full_text_tsv tsvector
        GENERATED ALWAYS AS (to_tsvector('simple', coalesce(full_text, ''))) STORED
);

CREATE TABLE IF NOT EXISTS segments (
    doc_id       INTEGER NOT NULL,
    segment_id   INTEGER NOT NULL,
    segment_text TEXT,
    avg_logprob  DOUBLE PRECISION,
    char_offset  INTEGER,
    start_time   DOUBLE PRECISION,
    end_time     DOUBLE PRECISION
);

CREATE TABLE IF NOT EXISTS migration_progress (
    stage        TEXT PRIMARY KEY,
    last_doc_id  INTEGER NOT NULL DEFAULT -1,
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);
"""

INDEXES = [
    ("documents_tsv_gin",
     "CREATE INDEX IF NOT EXISTS documents_tsv_gin ON documents USING GIN (full_text_tsv)"),
    ("documents_source_idx",
     "CREATE INDEX IF NOT EXISTS documents_source_idx ON documents (source)"),
    ("documents_date_idx",
     "CREATE INDEX IF NOT EXISTS documents_date_idx ON documents (episode_date)"),
    ("segments_doc_seg_idx",
     "CREATE INDEX IF NOT EXISTS segments_doc_seg_idx ON segments (doc_id, segment_id)"),
    ("segments_doc_off_idx",
     "CREATE INDEX IF NOT EXISTS segments_doc_off_idx ON segments (doc_id, char_offset)"),
]


def connect_pg(dsn: str):
    import psycopg
    from app.services.pg_index import normalise_dsn
    return psycopg.connect(normalise_dsn(dsn), autocommit=True)


def _progress(conn, stage: str) -> int:
    with conn.cursor() as cur:
        cur.execute("SELECT last_doc_id FROM migration_progress WHERE stage = %s", [stage])
        row = cur.fetchone()
        return row[0] if row else -1


def _set_progress(conn, stage: str, last_doc_id: int) -> None:
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO migration_progress (stage, last_doc_id, updated_at) "
            "VALUES (%s, %s, now()) "
            "ON CONFLICT (stage) DO UPDATE SET last_doc_id = EXCLUDED.last_doc_id, "
            "updated_at = now()",
            [stage, last_doc_id],
        )


# ── stages ──────────────────────────────────────────────────────────────
def stage_schema(sq, conn) -> None:
    log.info("Creating schema")
    with conn.cursor() as cur:
        cur.execute(SCHEMA)
    log.info("Schema ready")


def stage_documents(sq, conn) -> None:
    resume_after = _progress(conn, "documents")
    total = sq.execute("SELECT COUNT(*) FROM documents WHERE doc_id > ?", (resume_after,)).fetchone()[0]
    if not total:
        log.info("documents: nothing to do (resume point %d)", resume_after)
        return
    log.info("documents: copying %d rows (resuming after doc_id %d)", total, resume_after)

    rows = sq.execute(
        """
        SELECT d.doc_id, d.uuid, d.source, d.episode, d.episode_date, d.episode_title,
               fts.full_text
        FROM documents d
        LEFT JOIN fts_doc_mapping m ON m.doc_id = d.doc_id
        LEFT JOIN documents_fts fts ON fts.rowid = m.fts_rowid
        WHERE d.doc_id > ?
        ORDER BY d.doc_id
        """,
        (resume_after,),
    )

    copy_sql = ("COPY documents (doc_id, uuid, source, episode, episode_date, "
                "episode_title, full_text) FROM STDIN")
    done = 0
    t0 = time.perf_counter()
    batch: list[tuple] = []

    def flush(batch):
        if not batch:
            return
        with conn.cursor() as cur:
            with cur.copy(copy_sql) as copy:
                for r in batch:
                    copy.write_row(r)
        _set_progress(conn, "documents", batch[-1][0])

    for row in rows:
        batch.append(row)
        if len(batch) >= DOC_BATCH:
            flush(batch)
            done += len(batch)
            batch = []
            log.info("documents: %d/%d (%.0f rows/s)", done, total,
                     done / max(time.perf_counter() - t0, 1e-6))
    flush(batch)
    done += len(batch)
    log.info("documents: done, %d rows in %.1fs", done, time.perf_counter() - t0)


def stage_segments(sq, conn) -> None:
    resume_after = _progress(conn, "segments")
    total = sq.execute("SELECT COUNT(*) FROM segments WHERE doc_id > ?", (resume_after,)).fetchone()[0]
    if not total:
        log.info("segments: nothing to do (resume point %d)", resume_after)
        return
    doc_ids = [r[0] for r in sq.execute(
        "SELECT DISTINCT doc_id FROM segments WHERE doc_id > ? ORDER BY doc_id",
        (resume_after,))]
    log.info("segments: copying %d rows across %d documents (resuming after doc_id %d)",
             total, len(doc_ids), resume_after)

    copy_sql = ("COPY segments (doc_id, segment_id, segment_text, avg_logprob, "
                "char_offset, start_time, end_time) FROM STDIN")
    done = 0
    t0 = time.perf_counter()

    for i in range(0, len(doc_ids), SEG_DOC_BATCH):
        chunk = doc_ids[i:i + SEG_DOC_BATCH]
        lo, hi = chunk[0], chunk[-1]
        rows = sq.execute(
            "SELECT doc_id, segment_id, segment_text, avg_logprob, char_offset, "
            "start_time, end_time FROM segments WHERE doc_id BETWEEN ? AND ? "
            "ORDER BY doc_id, segment_id",
            (lo, hi),
        )
        n = 0
        with conn.cursor() as cur:
            with cur.copy(copy_sql) as copy:
                for row in rows:
                    copy.write_row(row)
                    n += 1
        _set_progress(conn, "segments", hi)
        done += n
        rate = done / max(time.perf_counter() - t0, 1e-6)
        eta = (total - done) / rate if rate else 0
        log.info("segments: %d/%d rows (%.0f rows/s, eta %.0f min), through doc_id %d",
                 done, total, rate, eta / 60, hi)

    log.info("segments: done, %d rows in %.1fs", done, time.perf_counter() - t0)


def stage_indexes(sq, conn) -> None:
    for name, sql in INDEXES:
        log.info("Building index %s", name)
        t0 = time.perf_counter()
        with conn.cursor() as cur:
            cur.execute(sql)
        log.info("  %s built in %.1fs", name, time.perf_counter() - t0)
    log.info("Running ANALYZE")
    with conn.cursor() as cur:
        cur.execute("ANALYZE documents")
        cur.execute("ANALYZE segments")
    log.info("Indexes complete")


def stage_verify(sq, conn) -> None:
    ok = True
    for table, sqlite_sql in (("documents", "SELECT COUNT(*) FROM documents"),
                              ("segments", "SELECT COUNT(*) FROM segments")):
        want = sq.execute(sqlite_sql).fetchone()[0]
        with conn.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) FROM {table}")
            got = cur.fetchone()[0]
        status = "ok" if want == got else "MISMATCH"
        if want != got:
            ok = False
        log.info("%-10s sqlite=%d postgres=%d  %s", table, want, got, status)

    want_chars = sq.execute("SELECT SUM(LENGTH(full_text)) FROM documents_fts").fetchone()[0]
    with conn.cursor() as cur:
        cur.execute("SELECT COALESCE(SUM(char_length(full_text)), 0) FROM documents")
        got_chars = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM documents WHERE full_text_tsv IS NULL "
                    "OR full_text_tsv = ''::tsvector")
        empty_tsv = cur.fetchone()[0]
        cur.execute("SELECT pg_size_pretty(pg_total_relation_size('documents')), "
                    "pg_size_pretty(pg_total_relation_size('segments'))")
        sizes = cur.fetchone()
    log.info("full_text chars sqlite=%s postgres=%s  %s", f"{want_chars:,}", f"{got_chars:,}",
             "ok" if want_chars == got_chars else "MISMATCH")
    if want_chars != got_chars:
        ok = False
    log.info("documents with empty tsvector: %d", empty_tsv)
    log.info("relation sizes: documents=%s segments=%s", sizes[0], sizes[1])
    log.info("VERIFY %s", "PASSED" if ok else "FAILED")
    return ok


STAGES = {
    "schema": stage_schema,
    "documents": stage_documents,
    "segments": stage_segments,
    "indexes": stage_indexes,
    "verify": stage_verify,
}
ORDER = ["schema", "documents", "segments", "indexes", "verify"]


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--sqlite", default="explore.sqlite", help="Source explore.sqlite")
    ap.add_argument("--dsn", default=os.environ.get("DATABASE_URL"),
                    help="Target Postgres DSN (default $DATABASE_URL)")
    ap.add_argument("--stage", choices=ORDER + ["all"], default="all")
    ap.add_argument("--restart", action="store_true",
                    help="Clear progress and re-copy from scratch (drops both tables)")
    args = ap.parse_args()

    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s | %(levelname)s | %(message)s")

    if not args.dsn:
        log.error("No DSN given; pass --dsn or set DATABASE_URL")
        return 2
    if not os.path.exists(args.sqlite):
        log.error("SQLite file not found: %s", args.sqlite)
        return 2

    sq = sqlite3.connect(f"file:{args.sqlite}?mode=ro", uri=True)
    conn = connect_pg(args.dsn)

    if args.restart:
        log.warning("--restart: dropping documents and segments")
        with conn.cursor() as cur:
            cur.execute("DROP TABLE IF EXISTS documents, segments, migration_progress")

    stages = ORDER if args.stage == "all" else [args.stage]
    for name in stages:
        log.info("── stage: %s ──", name)
        result = STAGES[name](sq, conn)
        if name == "verify" and result is False:
            return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
