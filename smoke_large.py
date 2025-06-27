#!/usr/bin/env python3
"""
Smoke-test the Explore search stack on a real corpus.

* Builds the TranscriptIndex
* Runs three hand-picked literal queries   → times + hit counts
* Runs three regex queries                 → times + hit counts
* For every query, fetches one random hit's Segment and checks:
      • query text is inside segment.text
      • start_sec is non-negative
* Samples 100 random words that appear in the corpus and
  checks that each full-text search finds ≥ 1 hit
  + segment correctness for 20 random hits
* Prints average per-search latency

Run:  python scripts/smoke_large.py
"""

from __future__ import annotations
import random, re, time
from collections import Counter
from pathlib import Path
from typing import Iterable

from app.utils import get_transcripts
from app.services.index import IndexManager
from app.services.search import SearchService, SearchHit

# ---------- CONFIG --------------------------------------------------------- #
JSON_DIR       = Path.home() / "data" / "json"      # ~/data/json
HAND_QUERIES   = ["שלום", "מתושלח", "אם"]           # literal
REGEX_QUERIES  = [r"\b[Aa]udio\b", r"(בית|בית־ספר)", r"^שלום"]   # regex
BATCH_SIZE     = 100          # random words to test (search hits ≥ 1)
BATCH_SEGCHK   = 20           # random hits to verify segment lookup
WORD_RE        = re.compile(r"\w{4,}", re.UNICODE)  # ≥4-char "words"

# ---------- helpers -------------------------------------------------------- #
def time_call(fn, *args, **kwargs):
    t0 = time.perf_counter()
    out = fn(*args, **kwargs)
    return out, time.perf_counter() - t0


def harvest_random_words(index_texts: Iterable[str], k: int) -> list[str]:
    freq = Counter()
    for txt in index_texts:
        freq.update(WORD_RE.findall(txt))
    common = [w for w, c in freq.items() if c >= 2] or list(freq)
    return random.sample(common, k)


def preview_hit(idx_mgr, hit: SearchHit, span=50) -> str:
    txt = idx_mgr.get().get_text_by_episode_idx(hit.episode_idx)
    start = max(0, hit.char_offset - 10)
    return txt[start : hit.char_offset + span]


def assert_segment_contains(svc: SearchService, hit: SearchHit, needle: str):
    seg = svc.segment(hit)
    assert needle in seg.text, "segment text mismatch"
    assert seg.start_sec >= 0, "negative start_sec"
    return seg


# ---------- main ----------------------------------------------------------- #
def main() -> None:
    print("🔧  Building index …")
    file_records = get_transcripts(JSON_DIR)
    _, build_sec = time_call(IndexManager, file_records)
    idxm = _
    epi_cnt = idxm.get().get_document_count()
    print(f"✅  {epi_cnt:,} episodes indexed in {build_sec:.1f} s")

    svc = SearchService(idxm)

    # --------------------------------------------------------------------- #
    print("\n=== Hand-picked literal queries ===")
    for q in HAND_QUERIES:
        hits, sec = time_call(svc.search, q, regex=False)
        print(f"q={q!r:<8}  hits={len(hits):4}  {sec*1000:6.1f} ms")
        if hits:
            hit = random.choice(hits)
            seg = assert_segment_contains(svc, hit, q)
            print("  ↳", preview_hit(idxm, hit), f"(seg start={seg.start_sec:.2f}s)")

    # --------------------------------------------------------------------- #
    print("\n=== Hand-picked REGEX queries ===")
    for q in REGEX_QUERIES:
        hits, sec = time_call(svc.search, q, regex=True)
        print(f"rx={q!r:<18} hits={len(hits):4}  {sec*1000:6.1f} ms")
        if hits:
            hit = random.choice(hits)
            # for regex, use the *matched* text as needle
            m = re.search(q, idxm.get().get_text_by_episode_idx(hit.episode_idx)[hit.char_offset:])
            needle = m.group(0) if m else q
            seg = assert_segment_contains(svc, hit, needle)
            print("  ↳", preview_hit(idxm, hit), f"(seg start={seg.start_sec:.2f}s)")

    # --------------------------------------------------------------------- #
    print(f"\n=== Batch test on {BATCH_SIZE} random words ===")
    # Get all document texts for word harvesting
    idx = idxm.get()
    all_texts = [idx.get_text_by_episode_idx(i) for i in range(epi_cnt)]
    rnd_words = harvest_random_words(all_texts, BATCH_SIZE)
    failures = seg_fail = 0
    total_sec = 0.0
    segchk_hits: list[SearchHit] = []

    for w in rnd_words:
        hits, sec = time_call(svc.search, w, regex=False)
        total_sec += sec
        if not hits:
            failures += 1
        elif len(segchk_hits) < BATCH_SEGCHK:
            segchk_hits.append(random.choice(hits))

    # segment-level assertions for 20 random hits
    for hit in segchk_hits:
        needle = idx.get_text_by_episode_idx(hit.episode_idx)[hit.char_offset:
                                                  hit.char_offset + len(rnd_words[0])]
        try:
            assert_segment_contains(svc, hit, needle.strip())
        except AssertionError:
            seg_fail += 1

    avg_ms = (total_sec / BATCH_SIZE) * 1000
    print(f"avg per-search time: {avg_ms:.1f} ms "
          f"| zero-hit words: {failures} "
          f"| seg-mismatch: {seg_fail}")

    print("\n🎉  Smoke-test finished.")


if __name__ == "__main__":
    main()
