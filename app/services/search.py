from __future__ import annotations

import logging
import time
from dataclasses import dataclass

from .index import IndexManager, Segment, segment_for_hit

logger = logging.getLogger(__name__)


@dataclass(slots=True, frozen=True)
class SearchHit:
    episode_idx: int
    char_offset: int


class SearchService:
    """Stateless, one-pass search over the current TranscriptIndex."""

    def __init__(self, index_mgr: IndexManager) -> None:
        self._index_mgr = index_mgr
        # Log index statistics on initialization
        idx = self._index_mgr.get()
        doc_count, total_chars = idx.get_document_stats()
        logger.info(f"SearchService initialized with {doc_count} texts, total size: {total_chars:,} characters")

    # ­­­­­­­­­­­­­­­­­­­­­­­­­­­­­­­­­­­­­­­­­­­­­­­­­­­ #
    def search(
        self,
        query: str,
        search_mode: str = "partial",
        date_from: str = None,
        date_to: str = None,
        sources: list[str] = None,
        ignore_punct: bool = False,
        position_filters: set[str] | None = None,
    ) -> list[SearchHit]:
        """Search with optional filters and search mode.

        Args:
            query: Search query string
            search_mode: Search mode - 'exact' (whole words), 'partial' (substring), or 'regex'
            date_from: Optional start date filter (YYYY-MM-DD format)
            date_to: Optional end date filter (YYYY-MM-DD format)
            sources: Optional list of sources to filter by
            ignore_punct: If True, ignore punctuation between words when matching
            position_filters: Optional set of position labels to filter by ('start', 'end', 'cross')
        """
        start_time = time.perf_counter()
        idx = self._index_mgr.get()

        # Log search parameters
        logger.info(
            f"Starting search for query: '{query}', mode: {search_mode}, date_from: {date_from}, "
            f"date_to: {date_to}, sources: {sources}, ignore_punct: {ignore_punct}, "
            f"position_filters: {position_filters}"
        )

        hits_data = idx.search_hits(
            query,
            search_mode=search_mode,
            date_from=date_from,
            date_to=date_to,
            sources=sources,
            ignore_punct=ignore_punct,
            position_filters=position_filters,
        )
        hits = [SearchHit(episode_idx, char_offset) for episode_idx, char_offset in hits_data]

        total_time = time.perf_counter() - start_time
        logger.info(
            f"Search completed in {total_time*1000:.2f}ms. " f"Found {len(hits)} hits with mode '{search_mode}'"
        )
        return hits

    def segment(self, hit: SearchHit) -> Segment:
        """Return the segment that contains this hit."""
        idx = self._index_mgr.get()
        return segment_for_hit(idx, hit.episode_idx, hit.char_offset)
