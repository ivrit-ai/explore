"""JSON API consumed by the React frontend. Every endpoint requires login."""
from __future__ import annotations

import logging
import random
import time
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from pydantic import BaseModel

from ..services.index import InvalidQuery, SearchTimeout
from .auth import require_api_login
from .params import SearchFilters, SearchMode, search_filters

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api", dependencies=[Depends(require_api_login)])

MAX_PER_PAGE = 500
MAX_CONTEXT_RADIUS = 10


class Span(BaseModel):
    """[start, end) in code points, relative to the hit's segment text."""
    start: int
    end: int


class Hit(BaseModel):
    char_offset: int
    segment_idx: int
    start_sec: float
    end_sec: float
    text: str
    match: Span
    groups: list[Optional[Span]]


class Episode(BaseModel):
    episode_idx: int
    uuid: str
    source: str
    title: str
    date: Optional[str]
    duration_sec: float
    hits: list[Hit]


class SearchResponse(BaseModel):
    query: str
    mode: SearchMode
    page: int
    per_page: int
    seed: Optional[int]
    has_more: bool
    total_hits: int
    episodes: list[Episode]


class SourceCount(BaseModel):
    name: str
    count: int


class DateRange(BaseModel):
    min: Optional[str]
    max: Optional[str]


class MetadataResponse(BaseModel):
    sources: list[SourceCount]
    date_range: DateRange
    total_docs: int


class Segment(BaseModel):
    segment_idx: int
    start_sec: float
    end_sec: float
    text: str


class Me(BaseModel):
    email: str


def _relative_span(start: int, end: int, seg_offset: int, seg_len: int) -> Optional[Span]:
    """Map a full-text span onto the segment, clipping matches that run past it."""
    if start < 0:
        return None
    s = min(max(0, start - seg_offset), seg_len)
    e = min(max(s, end - seg_offset), seg_len)
    return Span(start=s, end=e)


def _query_errors(fn):
    """Translate search-layer failures into client errors."""
    try:
        return fn()
    except InvalidQuery:
        raise HTTPException(status_code=400, detail="לא ניתן לחפש את הביטוי הזה")
    except SearchTimeout:
        raise HTTPException(status_code=422, detail="הביטוי מורכב מדי והחיפוש נעצר. נסו ביטוי פשוט יותר.")


@router.get("/me", response_model=Me)
def me(user_email: str = Depends(require_api_login)):
    return Me(email=user_email)


@router.get("/search", response_model=SearchResponse)
def search(
    request: Request,
    filters: SearchFilters = Depends(search_filters),
    page: int = Query(1, ge=1, le=10_000),
    per_page: int = Query(50, ge=1, le=MAX_PER_PAGE),
    shuffle: bool = Query(True),
    seed: Optional[int] = Query(None, ge=0, le=2**31 - 1),
    user_email: str = Depends(require_api_login),
):
    started = time.time()
    if shuffle and seed is None:
        # Returned to the client so later pages keep the same order.
        seed = random.randint(0, 2**31 - 1)
    if not shuffle:
        seed = None

    search_service = request.app.state.search_service
    hits, has_more = _query_errors(lambda: search_service.search(
        filters.q, search_mode=filters.mode, date_from=filters.date_from,
        date_to=filters.date_to, sources=filters.sources,
        doc_limit=per_page, doc_offset=(page - 1) * per_page, seed=seed,
    ))

    index = search_service._index_mgr.get()
    segments = index.get_segments_at_offsets([(h.episode_idx, h.char_offset) for h in hits])
    doc_ids = list({h.episode_idx for h in hits})
    docs = index.get_documents_batch(doc_ids)
    durations = index.get_durations(doc_ids)

    episodes: dict[int, Episode] = {}
    for h in hits:
        seg = segments.get((h.episode_idx, h.char_offset))
        doc = docs.get(h.episode_idx)
        if not seg or not doc:
            continue
        text = seg["text"] or ""
        offset = seg["char_offset"]
        episode = episodes.get(h.episode_idx)
        if episode is None:
            episode = episodes[h.episode_idx] = Episode(
                episode_idx=h.episode_idx,
                uuid=doc["uuid"],
                source=doc["source"] or "",
                title=doc["episode_title"] or doc["episode"] or "",
                date=str(doc["episode_date"]) if doc["episode_date"] else None,
                duration_sec=durations.get(h.episode_idx, 0.0),
                hits=[],
            )
        episode.hits.append(Hit(
            char_offset=h.char_offset,
            segment_idx=seg["segment_id"],
            start_sec=seg["start_time"],
            end_sec=seg["end_time"],
            text=text,
            match=_relative_span(h.char_offset, h.end_offset, offset, len(text)),
            groups=[_relative_span(s, e, offset, len(text)) for s, e in h.groups],
        ))

    total_hits = sum(len(e.hits) for e in episodes.values())
    analytics = request.app.state.analytics
    if analytics:
        analytics.capture_search(
            query=filters.q,
            max_results_per_page=per_page,
            page=page,
            execution_time_ms=(time.time() - started) * 1000,
            results_count=total_hits,
            total_results=total_hits,
            user_email=user_email,
        )

    return SearchResponse(
        query=filters.q, mode=filters.mode, page=page, per_page=per_page, seed=seed,
        has_more=has_more, total_hits=total_hits, episodes=list(episodes.values()),
    )


@router.get("/search/metadata", response_model=MetadataResponse)
def search_metadata(request: Request, filters: SearchFilters = Depends(search_filters)):
    meta = _query_errors(lambda: request.app.state.search_service.metadata(
        filters.q, filters.mode, filters.date_from, filters.date_to, filters.sources))
    sources = sorted(meta["sources"].items(), key=lambda kv: -kv[1])
    return MetadataResponse(
        sources=[SourceCount(name=n, count=c) for n, c in sources],
        date_range=DateRange(
            min=str(meta["date_range"]["min"]) if meta["date_range"]["min"] else None,
            max=str(meta["date_range"]["max"]) if meta["date_range"]["max"] else None,
        ),
        total_docs=meta["total_docs"],
    )


@router.get("/episodes/{episode_idx}/segments", response_model=list[Segment])
def episode_segments(
    request: Request,
    episode_idx: int,
    center: int = Query(..., ge=0),
    radius: int = Query(5, ge=0, le=MAX_CONTEXT_RADIUS),
):
    """Transcript segments around `center`, for the context panel and player."""
    index = request.app.state.search_service._index_mgr.get()
    lookups = [(episode_idx, i) for i in range(max(0, center - radius), center + radius + 1)]
    rows = index.get_segments_by_ids(lookups)
    return [
        Segment(segment_idx=r["segment_id"], start_sec=r["start_time"], end_sec=r["end_time"], text=r["text"] or "")
        for r in sorted(rows, key=lambda r: r["segment_id"])
        if r["doc_id"] == episode_idx
    ]
