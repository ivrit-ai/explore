# app/routes/search.py
from __future__ import annotations

from fastapi import APIRouter, Request, Query, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from typing import List

from app.services.search import SearchHit

router = APIRouter()


class SegmentLookup(BaseModel):
    episode_idx: int
    char_offset: int


class SegmentLookupRequest(BaseModel):
    lookups: List[SegmentLookup]


class SegmentByIdxLookup(BaseModel):
    episode_idx: int
    segment_idx: int


class SegmentByIdxRequest(BaseModel):
    lookups: List[SegmentByIdxLookup]


@router.get("/", name="search.search")
def search(
    request: Request,
    q: str = Query(""),
    regex: bool = Query(False),
):
    search_svc = request.app.state.search_service

    if not q:
        raise HTTPException(status_code=400, detail="missing ?q=")

    hits = search_svc.search(q, regex=regex)

    # Enrich hits with segment and episode info
    index = search_svc._index_mgr.get()
    results = []
    for h in hits:
        seg = search_svc.segment(h)
        doc_info = index.get_document_info(h.episode_idx)
        results.append({
            "episode_idx": h.episode_idx,
            "char_offset": h.char_offset,
            "source": doc_info.get("source", ""),
            "episode": doc_info.get("episode", ""),
            "episode_title": doc_info.get("episode_title", ""),
            "episode_date": doc_info.get("episode_date", ""),
            "segment_idx": seg.seg_idx,
            "start_sec": seg.start_sec,
            "end_sec": seg.end_sec,
        })

    return JSONResponse(results)


@router.post("/segment", name="search.get_segment")
def get_segment(request: Request, body: SegmentLookupRequest):
    """Batch char-offset → segment lookup.  Returns a 1:1 aligned array (null for misses)."""
    search_svc = request.app.state.search_service
    index = search_svc._index_mgr.get()

    # Single batch query instead of N individual queries
    pairs = [(l.episode_idx, l.char_offset) for l in body.lookups]
    segments_map = index.get_segments_at_offsets(pairs)

    results = []
    for lookup in body.lookups:
        key = (lookup.episode_idx, lookup.char_offset)
        seg = segments_map.get(key)
        if seg:
            results.append({
                "episode_idx": lookup.episode_idx,
                "char_offset": lookup.char_offset,
                "segment_index": seg["segment_id"],
                "start_sec": seg["start_time"],
                "end_sec": seg["end_time"],
                "text": seg["text"],
            })
        else:
            results.append(None)

    return JSONResponse(results)


@router.post("/segment/by_idx", name="search.get_segments_by_idx")
def get_segments_by_idx(request: Request, body: SegmentByIdxRequest):
    """Batch (episode_idx, segment_idx) lookup.  Returns a 1:1 aligned array (null for misses)."""
    search_svc = request.app.state.search_service
    index_mgr = search_svc._index_mgr.get()

    batch_lookups = [(l.episode_idx, l.segment_idx) for l in body.lookups]

    # get_segments_by_ids returns a FLAT list (INNER JOIN: fewer results,
    # re-ordered by doc_id,segment_id, duplicates collapsed).
    # Build a dict so we can map back to input order.
    raw = index_mgr.get_segments_by_ids(batch_lookups)
    seg_dict = {(s["doc_id"], s["segment_id"]): s for s in raw}

    # Return 1:1 with input, null for missing segments
    results = []
    for epi, idx in batch_lookups:
        seg = seg_dict.get((epi, idx))
        if seg:
            results.append({
                "episode_idx": epi,
                "segment_index": seg["segment_id"],
                "start_sec": seg["start_time"],
                "end_sec": seg["end_time"],
                "text": seg["text"],
            })
        else:
            results.append(None)

    return JSONResponse(results)
