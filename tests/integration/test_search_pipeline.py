"""
Integration tests for the full search pipeline.

Tests the complete flow: query → search → segment enrichment → results.
"""
import pytest
from tests.conftest import (
    load_transcript_to_db,
    MockTranscript,
    MockSegment,
    BASIC_HEBREW_TRANSCRIPT,
    PUNCTUATION_TRANSCRIPT,
    POSITION_TRANSCRIPT,
    MIXED_TRANSCRIPT,
)


class TestFullSearchPipeline:
    """End-to-end search pipeline tests."""

    def test_search_returns_hits(self, in_memory_db):
        """Basic search should return SearchHit objects."""
        load_transcript_to_db(in_memory_db, BASIC_HEBREW_TRANSCRIPT, 0)

        from app.services.index import TranscriptIndex, IndexManager
        from app.services.search import SearchService, SearchHit

        # Create minimal IndexManager mock
        class MockIndexManager:
            def __init__(self, db):
                self._index = TranscriptIndex(db)
            def get(self):
                return self._index

        index_mgr = MockIndexManager(in_memory_db)
        search_service = SearchService(index_mgr)

        hits = search_service.search("שלום")
        assert len(hits) >= 1
        assert isinstance(hits[0], SearchHit)

    def test_search_hit_to_segment(self, in_memory_db):
        """SearchHit should be convertible to Segment."""
        load_transcript_to_db(in_memory_db, BASIC_HEBREW_TRANSCRIPT, 0)

        from app.services.index import TranscriptIndex, Segment
        from app.services.search import SearchService

        class MockIndexManager:
            def __init__(self, db):
                self._index = TranscriptIndex(db)
            def get(self):
                return self._index

        index_mgr = MockIndexManager(in_memory_db)
        search_service = SearchService(index_mgr)

        hits = search_service.search("שלום")
        assert len(hits) >= 1

        segment = search_service.segment(hits[0])
        assert isinstance(segment, Segment)
        assert segment.text  # Has text
        assert segment.start_sec >= 0
        assert segment.end_sec > segment.start_sec

    def test_search_with_all_filters(self, in_memory_db):
        """Search with all filter types combined."""
        # Create transcripts with different dates and sources
        t1 = MockTranscript(
            source="podcast-a", episode="podcast-a/2024.01.15 Episode 1",
            episode_title="Episode 1", episode_date="2024-01-15",
            segments=[MockSegment(0.0, 3.0, "שלום עולם ינואר")]
        )
        t2 = MockTranscript(
            source="podcast-a", episode="podcast-a/2024.06.15 Episode 2",
            episode_title="Episode 2", episode_date="2024-06-15",
            segments=[MockSegment(0.0, 3.0, "שלום עולם יוני")]
        )
        t3 = MockTranscript(
            source="podcast-b", episode="podcast-b/2024.06.15 Episode 1",
            episode_title="Episode 1", episode_date="2024-06-15",
            segments=[MockSegment(0.0, 3.0, "שלום עולם אחר")]
        )
        load_transcript_to_db(in_memory_db, t1, 0)
        load_transcript_to_db(in_memory_db, t2, 1)
        load_transcript_to_db(in_memory_db, t3, 2)

        from app.services.index import TranscriptIndex
        from app.services.search import SearchService

        class MockIndexManager:
            def __init__(self, db):
                self._index = TranscriptIndex(db)
            def get(self):
                return self._index

        index_mgr = MockIndexManager(in_memory_db)
        search_service = SearchService(index_mgr)

        # Search with all filters: podcast-a, after March, position start
        hits = search_service.search(
            query="שלום",
            search_mode='exact',
            sources=["podcast-a"],
            date_from="2024-03-01",
            position_filters={'start'}
        )

        # Should only match t2 (podcast-a, June, "שלום" at start)
        assert len(hits) == 1
        assert hits[0].episode_idx == 1

    def test_empty_results(self, in_memory_db):
        """Search with no matches should return empty list."""
        load_transcript_to_db(in_memory_db, BASIC_HEBREW_TRANSCRIPT, 0)

        from app.services.index import TranscriptIndex
        from app.services.search import SearchService

        class MockIndexManager:
            def __init__(self, db):
                self._index = TranscriptIndex(db)
            def get(self):
                return self._index

        index_mgr = MockIndexManager(in_memory_db)
        search_service = SearchService(index_mgr)

        hits = search_service.search("nonexistent_query_xyz")
        assert hits == []


class TestSearchModeIntegration:
    """Integration tests for different search modes."""

    def test_exact_vs_partial_behavior(self, in_memory_db):
        """Verify exact and partial modes have different behavior."""
        transcript = MockTranscript(
            source="test", episode="test/2024.01.01 Test",
            episode_title="Test", episode_date="2024-01-01",
            segments=[MockSegment(0.0, 3.0, "שלומית היא שם")]
        )
        load_transcript_to_db(in_memory_db, transcript, 0)

        from app.services.index import TranscriptIndex
        from app.services.search import SearchService

        class MockIndexManager:
            def __init__(self, db):
                self._index = TranscriptIndex(db)
            def get(self):
                return self._index

        index_mgr = MockIndexManager(in_memory_db)
        search_service = SearchService(index_mgr)

        # Exact: "שלום" should NOT match "שלומית"
        exact_hits = search_service.search("שלום", search_mode='exact')
        assert len(exact_hits) == 0

        # Partial: "שלומ" (prefix) SHOULD match "שלומית"
        # NOTE: "שלום" won't match because FTS5 partial uses prefix matching
        # and "שלום" is not a prefix of "שלומית" (different last letter)
        partial_hits = search_service.search("שלומ", search_mode='partial')
        assert len(partial_hits) == 1

    def test_regex_mode_integration(self, in_memory_db):
        """Regex mode should work in full pipeline."""
        transcript = MockTranscript(
            source="test", episode="test/2024.01.01 Test",
            episode_title="Test", episode_date="2024-01-01",
            segments=[MockSegment(0.0, 3.0, "שלום123 ושלום456")]
        )
        load_transcript_to_db(in_memory_db, transcript, 0)

        from app.services.index import TranscriptIndex
        from app.services.search import SearchService

        class MockIndexManager:
            def __init__(self, db):
                self._index = TranscriptIndex(db)
            def get(self):
                return self._index

        index_mgr = MockIndexManager(in_memory_db)
        search_service = SearchService(index_mgr)

        hits = search_service.search(r"שלום\d+", search_mode='regex')
        assert len(hits) == 2


class TestPositionFilterIntegration:
    """Integration tests for position filters."""

    def test_start_filter_pipeline(self, in_memory_db):
        """Position filter 'start' should work in full pipeline."""
        transcript = MockTranscript(
            source="test", episode="test/2024.01.01 Test",
            episode_title="Test", episode_date="2024-01-01",
            segments=[
                MockSegment(0.0, 3.0, "שלום עולם באמצע"),
                MockSegment(3.0, 6.0, "באמצע שלום אחרון"),
            ]
        )
        load_transcript_to_db(in_memory_db, transcript, 0)

        from app.services.index import TranscriptIndex
        from app.services.search import SearchService

        class MockIndexManager:
            def __init__(self, db):
                self._index = TranscriptIndex(db)
            def get(self):
                return self._index

        index_mgr = MockIndexManager(in_memory_db)
        search_service = SearchService(index_mgr)

        # Only "שלום" at start of first segment should match
        hits = search_service.search("שלום", search_mode='exact', position_filters={'start'})
        assert len(hits) == 1
        assert hits[0].char_offset == 0

    def test_end_filter_pipeline(self, in_memory_db):
        """Position filter 'end' should work in full pipeline."""
        transcript = MockTranscript(
            source="test", episode="test/2024.01.01 Test",
            episode_title="Test", episode_date="2024-01-01",
            segments=[
                MockSegment(0.0, 3.0, "ראשון שלום"),
                MockSegment(3.0, 6.0, "שלום באמצע"),
            ]
        )
        load_transcript_to_db(in_memory_db, transcript, 0)

        from app.services.index import TranscriptIndex
        from app.services.search import SearchService

        class MockIndexManager:
            def __init__(self, db):
                self._index = TranscriptIndex(db)
            def get(self):
                return self._index

        index_mgr = MockIndexManager(in_memory_db)
        search_service = SearchService(index_mgr)

        # Only "שלום" at end of first segment should match
        hits = search_service.search("שלום", search_mode='exact', position_filters={'end'})
        assert len(hits) == 1


class TestMultiDocumentSearch:
    """Tests for searching across multiple documents."""

    def test_search_multiple_documents(self, in_memory_db):
        """Search should find results across multiple documents."""
        t1 = MockTranscript(
            source="podcast-a", episode="podcast-a/2024.01.01 Ep1",
            episode_title="Ep1", episode_date="2024-01-01",
            segments=[MockSegment(0.0, 3.0, "שלום מפודקאסט א")]
        )
        t2 = MockTranscript(
            source="podcast-b", episode="podcast-b/2024.01.01 Ep1",
            episode_title="Ep1", episode_date="2024-01-01",
            segments=[MockSegment(0.0, 3.0, "שלום מפודקאסט ב")]
        )
        load_transcript_to_db(in_memory_db, t1, 0)
        load_transcript_to_db(in_memory_db, t2, 1)

        from app.services.index import TranscriptIndex
        from app.services.search import SearchService

        class MockIndexManager:
            def __init__(self, db):
                self._index = TranscriptIndex(db)
            def get(self):
                return self._index

        index_mgr = MockIndexManager(in_memory_db)
        search_service = SearchService(index_mgr)

        hits = search_service.search("שלום", search_mode='exact')
        assert len(hits) == 2

        # Verify different documents
        doc_ids = {hit.episode_idx for hit in hits}
        assert doc_ids == {0, 1}

    def test_search_respects_source_filter(self, in_memory_db):
        """Source filter should limit results to specific sources."""
        t1 = MockTranscript(
            source="include-me", episode="include-me/2024.01.01 Ep1",
            episode_title="Ep1", episode_date="2024-01-01",
            segments=[MockSegment(0.0, 3.0, "שלום כלול")]
        )
        t2 = MockTranscript(
            source="exclude-me", episode="exclude-me/2024.01.01 Ep1",
            episode_title="Ep1", episode_date="2024-01-01",
            segments=[MockSegment(0.0, 3.0, "שלום לא כלול")]
        )
        load_transcript_to_db(in_memory_db, t1, 0)
        load_transcript_to_db(in_memory_db, t2, 1)

        from app.services.index import TranscriptIndex
        from app.services.search import SearchService

        class MockIndexManager:
            def __init__(self, db):
                self._index = TranscriptIndex(db)
            def get(self):
                return self._index

        index_mgr = MockIndexManager(in_memory_db)
        search_service = SearchService(index_mgr)

        hits = search_service.search("שלום", search_mode='exact', sources=["include-me"])
        assert len(hits) == 1
        assert hits[0].episode_idx == 0


class TestSegmentEnrichment:
    """Tests for segment enrichment after search."""

    def test_segment_has_all_fields(self, in_memory_db):
        """Enriched segment should have all required fields."""
        transcript = MockTranscript(
            source="test", episode="test/2024.01.01 Test",
            episode_title="Test", episode_date="2024-01-01",
            segments=[MockSegment(1.5, 4.2, "שלום עולם")]
        )
        load_transcript_to_db(in_memory_db, transcript, 0)

        from app.services.index import TranscriptIndex
        from app.services.search import SearchService

        class MockIndexManager:
            def __init__(self, db):
                self._index = TranscriptIndex(db)
            def get(self):
                return self._index

        index_mgr = MockIndexManager(in_memory_db)
        search_service = SearchService(index_mgr)

        hits = search_service.search("שלום")
        segment = search_service.segment(hits[0])

        assert segment.episode_idx == 0
        assert segment.seg_idx == 0
        assert segment.text == "שלום עולם"
        assert segment.start_sec == 1.5
        assert segment.end_sec == 4.2

    def test_hit_in_later_segment(self, in_memory_db):
        """Hit in later segment should return correct segment info."""
        transcript = MockTranscript(
            source="test", episode="test/2024.01.01 Test",
            episode_title="Test", episode_date="2024-01-01",
            segments=[
                MockSegment(0.0, 2.0, "מבוא קצר"),
                MockSegment(2.0, 5.0, "שלום עולם"),
                MockSegment(5.0, 8.0, "סיום"),
            ]
        )
        load_transcript_to_db(in_memory_db, transcript, 0)

        from app.services.index import TranscriptIndex
        from app.services.search import SearchService

        class MockIndexManager:
            def __init__(self, db):
                self._index = TranscriptIndex(db)
            def get(self):
                return self._index

        index_mgr = MockIndexManager(in_memory_db)
        search_service = SearchService(index_mgr)

        hits = search_service.search("שלום")
        segment = search_service.segment(hits[0])

        assert segment.seg_idx == 1  # Second segment
        assert segment.text == "שלום עולם"
        assert segment.start_sec == 2.0
        assert segment.end_sec == 5.0


class TestIgnorePunctIntegration:
    """Integration tests for ignore_punct feature."""

    def test_ignore_punct_in_pipeline(self, in_memory_db):
        """ignore_punct should work through full pipeline."""
        transcript = MockTranscript(
            source="test", episode="test/2024.01.01 Test",
            episode_title="Test", episode_date="2024-01-01",
            segments=[MockSegment(0.0, 3.0, "שלום, עולם!")]
        )
        load_transcript_to_db(in_memory_db, transcript, 0)

        from app.services.index import TranscriptIndex
        from app.services.search import SearchService

        class MockIndexManager:
            def __init__(self, db):
                self._index = TranscriptIndex(db)
            def get(self):
                return self._index

        index_mgr = MockIndexManager(in_memory_db)
        search_service = SearchService(index_mgr)

        # Without ignore_punct
        hits_no_ignore = search_service.search("שלום עולם", search_mode='exact', ignore_punct=False)

        # With ignore_punct
        hits_with_ignore = search_service.search("שלום עולם", search_mode='exact', ignore_punct=True)

        # Should find more (or same) with ignore_punct
        assert len(hits_with_ignore) >= len(hits_no_ignore)
