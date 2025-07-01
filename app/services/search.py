from __future__ import annotations
import time
import logging
from dataclasses import dataclass
from typing import List

from .index import IndexManager, TranscriptIndex, segment_for_hit, Segment

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
    def search(self, query: str) -> List[SearchHit]:
        start_time = time.perf_counter()
        idx = self._index_mgr.get()
        
        # Log search parameters
        logger.info(f"Starting search for query: '{query}'")

        hits_data = idx.search_hits(query)
        hits = [SearchHit(episode_idx, char_offset) for episode_idx, char_offset in hits_data]
                    
        total_time = time.perf_counter() - start_time
        logger.info(f"Search completed in {total_time*1000:.2f}ms. "
                   f"Found {len(hits)} hits")
        return hits

    def segment(self, hit: SearchHit) -> Segment:
        """Return the segment that contains this hit."""
        idx = self._index_mgr.get()
        return segment_for_hit(idx, hit.episode_idx, hit.char_offset)
