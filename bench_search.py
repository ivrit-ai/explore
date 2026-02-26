#!/usr/bin/env python3
"""Benchmark: document-based pagination search performance."""

import time
import logging
import sys
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(name)s %(message)s", stream=sys.stdout)

from app.services.index import IndexManager
from app.services.search import SearchService

DB_PATH = Path("explore.sqlite")
print(f"Loading index from {DB_PATH}...")
index_mgr = IndexManager(index_path=DB_PATH)
idx = index_mgr.get()
search_svc = SearchService(index_mgr)

DOCS_PER_PAGE = 100

test_cases = [
    ("common word (exact)",  "של",         "exact"),
    ("common word (partial)","של",         "partial"),
    ("phrase (exact)",       "בוקר טוב",   "exact"),
    ("longer word (partial)","טכנולוגיה",  "partial"),
    ("regex",               r"טכנולוגי\w+","regex"),
]

for label, query, mode in test_cases:
    print(f"\n{'='*60}")
    print(f"TEST: {label}  |  query={query!r}  mode={mode}")
    print(f"{'='*60}")

    for page in [1, 2, 5]:
        doc_offset = (page - 1) * DOCS_PER_PAGE

        t0 = time.perf_counter()
        hits, has_more = search_svc.search(query, search_mode=mode,
                                           doc_limit=DOCS_PER_PAGE, doc_offset=doc_offset)
        t_search = time.perf_counter()

        # Batch enrichment
        offset_pairs = [(h.episode_idx, h.char_offset) for h in hits]
        segments_map = idx.get_segments_at_offsets(offset_pairs)
        unique_doc_ids = list(set(h.episode_idx for h in hits))
        docs_map = idx.get_documents_batch(unique_doc_ids)
        t_enrich = time.perf_counter()

        search_ms = (t_search - t0) * 1000
        enrich_ms = (t_enrich - t_search) * 1000
        total_ms  = (t_enrich - t0) * 1000

        print(f"  Page {page}: {len(hits)} hits from {len(unique_doc_ids)} docs | "
              f"search={search_ms:.0f}ms enrich={enrich_ms:.0f}ms total={total_ms:.0f}ms | "
              f"has_more={has_more}")
