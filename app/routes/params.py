"""Query parameters shared by the search, metadata and export endpoints."""
from dataclasses import dataclass
from typing import Literal, Optional

import regex
from fastapi import HTTPException, Query

SearchMode = Literal['exact', 'partial', 'regex']

MAX_QUERY_LEN = 200
MAX_SOURCES = 200
DATE_PATTERN = r'^\d{4}-\d{2}-\d{2}$'


@dataclass(frozen=True)
class SearchFilters:
    q: str
    mode: SearchMode
    date_from: Optional[str]
    date_to: Optional[str]
    sources: Optional[list[str]]


def search_filters(
    q: str = Query(..., max_length=MAX_QUERY_LEN),
    mode: SearchMode = Query('exact'),
    date_from: Optional[str] = Query(None, pattern=DATE_PATTERN),
    date_to: Optional[str] = Query(None, pattern=DATE_PATTERN),
    source: list[str] = Query([], max_length=MAX_SOURCES),
) -> SearchFilters:
    query = q.strip()
    if not query:
        raise HTTPException(status_code=400, detail="נא להזין מונח לחיפוש")
    if mode == 'regex':
        try:
            regex.compile(query)
        except regex.error as e:
            raise HTTPException(status_code=400, detail=f"ביטוי רגולרי לא תקין: {e}")
    sources = [s.strip() for s in source if s.strip()]
    return SearchFilters(query, mode, date_from, date_to, sources or None)
