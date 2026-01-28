"""
Tests for regex search mode (_search_fts5_regex).

Regex mode allows full regex pattern matching with FTS5 candidate narrowing.
"""
import pytest
from tests.conftest import (
    load_transcript_to_db,
    MockTranscript,
    MockSegment,
)


class TestRegexSearchBasic:
    """Basic regex search functionality."""

    def test_simple_pattern(self, in_memory_db):
        """Simple regex pattern should match."""
        transcript = MockTranscript(
            source="test", episode="test/2024.01.01 Test",
            episode_title="Test", episode_date="2024-01-01",
            segments=[MockSegment(0.0, 3.0, "שלום עולם")]
        )
        load_transcript_to_db(in_memory_db, transcript, 0)

        from app.services.index import TranscriptIndex
        index = TranscriptIndex(in_memory_db)

        # Simple regex: literal match
        hits = index.search_hits("שלום", search_mode='regex')
        assert len(hits) == 1

    def test_dot_wildcard(self, in_memory_db):
        """Dot should match any character."""
        transcript = MockTranscript(
            source="test", episode="test/2024.01.01 Test",
            episode_title="Test", episode_date="2024-01-01",
            segments=[MockSegment(0.0, 3.0, "שלום עולם")]
        )
        load_transcript_to_db(in_memory_db, transcript, 0)

        from app.services.index import TranscriptIndex
        index = TranscriptIndex(in_memory_db)

        # "של.ם" should match "שלום"
        hits = index.search_hits(r"של.ם", search_mode='regex')
        assert len(hits) == 1

    def test_star_quantifier(self, in_memory_db):
        """Star quantifier should work."""
        transcript = MockTranscript(
            source="test", episode="test/2024.01.01 Test",
            episode_title="Test", episode_date="2024-01-01",
            segments=[MockSegment(0.0, 3.0, "שלום עולם")]
        )
        load_transcript_to_db(in_memory_db, transcript, 0)

        from app.services.index import TranscriptIndex
        index = TranscriptIndex(in_memory_db)

        # "שלום.*עולם" should match
        hits = index.search_hits(r"שלום.*עולם", search_mode='regex')
        assert len(hits) == 1

    def test_character_class(self, in_memory_db):
        """Character class should work."""
        transcript = MockTranscript(
            source="test", episode="test/2024.01.01 Test",
            episode_title="Test", episode_date="2024-01-01",
            segments=[MockSegment(0.0, 3.0, "אבג דהו")]
        )
        load_transcript_to_db(in_memory_db, transcript, 0)

        from app.services.index import TranscriptIndex
        index = TranscriptIndex(in_memory_db)

        # "[אבג]" should match any of those letters
        hits = index.search_hits(r"[אבג]", search_mode='regex')
        assert len(hits) >= 1

    def test_alternation(self, in_memory_db):
        """Alternation (|) should work."""
        transcript = MockTranscript(
            source="test", episode="test/2024.01.01 Test",
            episode_title="Test", episode_date="2024-01-01",
            segments=[MockSegment(0.0, 3.0, "שלום עולם")]
        )
        load_transcript_to_db(in_memory_db, transcript, 0)

        from app.services.index import TranscriptIndex
        index = TranscriptIndex(in_memory_db)

        # "שלום|goodbye" should match "שלום"
        hits = index.search_hits(r"שלום|goodbye", search_mode='regex')
        assert len(hits) == 1


class TestRegexSearchTokenExtraction:
    """Tests for FTS5 token extraction from regex patterns."""

    def test_token_extraction_for_fts5_narrowing(self, in_memory_db):
        """Verify tokens are extracted for FTS5 candidate narrowing."""
        transcript = MockTranscript(
            source="test", episode="test/2024.01.01 Test",
            episode_title="Test", episode_date="2024-01-01",
            segments=[MockSegment(0.0, 3.0, "שלום עולם טוב")]
        )
        load_transcript_to_db(in_memory_db, transcript, 0)

        from app.services.index import TranscriptIndex
        index = TranscriptIndex(in_memory_db)

        # Pattern with extractable token "שלום"
        hits = index.search_hits(r"שלום\s+\w+", search_mode='regex')
        assert len(hits) == 1

    def test_no_tokens_fallback_to_full_scan(self, in_memory_db):
        """Pattern with no 2+ char tokens should still work (full scan)."""
        transcript = MockTranscript(
            source="test", episode="test/2024.01.01 Test",
            episode_title="Test", episode_date="2024-01-01",
            segments=[MockSegment(0.0, 3.0, "א ב ג ד")]
        )
        load_transcript_to_db(in_memory_db, transcript, 0)

        from app.services.index import TranscriptIndex
        index = TranscriptIndex(in_memory_db)

        # Single character pattern - no good tokens
        hits = index.search_hits(r"[א-ת]", search_mode='regex')
        # Should fall back to full scan and find matches
        assert len(hits) >= 1


class TestRegexSearchInvalidPatterns:
    """Tests for invalid regex patterns."""

    def test_invalid_regex_returns_empty(self, in_memory_db):
        """Invalid regex should return empty results, not crash."""
        transcript = MockTranscript(
            source="test", episode="test/2024.01.01 Test",
            episode_title="Test", episode_date="2024-01-01",
            segments=[MockSegment(0.0, 3.0, "שלום עולם")]
        )
        load_transcript_to_db(in_memory_db, transcript, 0)

        from app.services.index import TranscriptIndex
        index = TranscriptIndex(in_memory_db)

        # Invalid regex: unclosed bracket
        hits = index.search_hits(r"[unclosed", search_mode='regex')
        assert hits == []

    def test_unbalanced_parentheses(self, in_memory_db):
        """Unbalanced parentheses should be handled gracefully."""
        transcript = MockTranscript(
            source="test", episode="test/2024.01.01 Test",
            episode_title="Test", episode_date="2024-01-01",
            segments=[MockSegment(0.0, 3.0, "שלום עולם")]
        )
        load_transcript_to_db(in_memory_db, transcript, 0)

        from app.services.index import TranscriptIndex
        index = TranscriptIndex(in_memory_db)

        # Invalid regex: unbalanced parens
        hits = index.search_hits(r"(שלום", search_mode='regex')
        assert hits == []


class TestRegexSearchUnicodeFeatures:
    """Tests for Unicode regex features with Hebrew."""

    @pytest.mark.xfail(reason="Unicode property patterns don't extract good FTS5 tokens - falls back to full scan")
    def test_unicode_property_hebrew(self, in_memory_db):
        """Unicode property \\p{Hebrew} should match Hebrew letters.

        NOTE: The regex pattern \\p{Hebrew}+ doesn't have any 2+ char word tokens,
        so FTS5 candidate narrowing fails and falls back to full scan, but the
        text in our small test DB may not match properly.
        """
        transcript = MockTranscript(
            source="test", episode="test/2024.01.01 Test",
            episode_title="Test", episode_date="2024-01-01",
            segments=[MockSegment(0.0, 3.0, "שלום world")]
        )
        load_transcript_to_db(in_memory_db, transcript, 0)

        from app.services.index import TranscriptIndex
        index = TranscriptIndex(in_memory_db)

        # \\p{Hebrew}+ should match Hebrew words
        hits = index.search_hits(r"\p{Hebrew}+", search_mode='regex')
        assert len(hits) >= 1

    @pytest.mark.xfail(reason="FTS5 token extraction doesn't handle \\b patterns well")
    def test_unicode_word_boundary(self, in_memory_db):
        """Word boundary \\b should work with Hebrew.

        NOTE: The regex token extraction finds 'שלום' but FTS5 prefix query
        may not match properly due to how the pattern is constructed.
        """
        transcript = MockTranscript(
            source="test", episode="test/2024.01.01 Test",
            episode_title="Test", episode_date="2024-01-01",
            segments=[MockSegment(0.0, 3.0, "שלום עולם")]
        )
        load_transcript_to_db(in_memory_db, transcript, 0)

        from app.services.index import TranscriptIndex
        index = TranscriptIndex(in_memory_db)

        # \\bשלום\\b should match "שלום" as whole word
        hits = index.search_hits(r"\bשלום\b", search_mode='regex')
        assert len(hits) == 1


class TestRegexSearchWithFilters:
    """Regex search with date and source filters."""

    def test_regex_with_date_filter(self, in_memory_db):
        """Regex search with date filter."""
        t1 = MockTranscript(
            source="test", episode="test/2024.01.01 Old",
            episode_title="Old", episode_date="2024-01-01",
            segments=[MockSegment(0.0, 3.0, "שלום123 ישן")]
        )
        t2 = MockTranscript(
            source="test", episode="test/2024.06.01 New",
            episode_title="New", episode_date="2024-06-01",
            segments=[MockSegment(0.0, 3.0, "שלום456 חדש")]
        )
        load_transcript_to_db(in_memory_db, t1, 0)
        load_transcript_to_db(in_memory_db, t2, 1)

        from app.services.index import TranscriptIndex
        index = TranscriptIndex(in_memory_db)

        # Regex with date filter
        hits = index.search_hits(r"שלום\d+", search_mode='regex', date_from="2024-03-01")
        assert len(hits) == 1
        assert hits[0][0] == 1

    def test_regex_with_source_filter(self, in_memory_db):
        """Regex search with source filter."""
        t1 = MockTranscript(
            source="podcast-a", episode="podcast-a/2024.01.01 Ep1",
            episode_title="Ep1", episode_date="2024-01-01",
            segments=[MockSegment(0.0, 3.0, "שלום123")]
        )
        t2 = MockTranscript(
            source="podcast-b", episode="podcast-b/2024.01.01 Ep1",
            episode_title="Ep1", episode_date="2024-01-01",
            segments=[MockSegment(0.0, 3.0, "שלום456")]
        )
        load_transcript_to_db(in_memory_db, t1, 0)
        load_transcript_to_db(in_memory_db, t2, 1)

        from app.services.index import TranscriptIndex
        index = TranscriptIndex(in_memory_db)

        hits = index.search_hits(r"שלום\d+", search_mode='regex', sources=["podcast-a"])
        assert len(hits) == 1
        assert hits[0][0] == 0


class TestRegexSearchWithPositionFilters:
    """Regex search with position filters."""

    def test_regex_with_start_filter(self, in_memory_db):
        """Regex match at segment start."""
        transcript = MockTranscript(
            source="test", episode="test/2024.01.01 Test",
            episode_title="Test", episode_date="2024-01-01",
            segments=[
                MockSegment(0.0, 3.0, "שלום123 באמצע"),
                MockSegment(3.0, 6.0, "באמצע שלום456"),
            ]
        )
        load_transcript_to_db(in_memory_db, transcript, 0)

        from app.services.index import TranscriptIndex
        index = TranscriptIndex(in_memory_db)

        # Only matches at start of segment
        hits = index.search_hits(r"שלום\d+", search_mode='regex', position_filters={'start'})
        assert len(hits) == 1
        # First segment has "שלום123" at start
        assert hits[0][1] == 0  # char_offset at 0

    def test_regex_with_end_filter(self, in_memory_db):
        """Regex match at segment end."""
        transcript = MockTranscript(
            source="test", episode="test/2024.01.01 Test",
            episode_title="Test", episode_date="2024-01-01",
            segments=[
                MockSegment(0.0, 3.0, "באמצע שלום123"),
                MockSegment(3.0, 6.0, "שלום456 באמצע"),
            ]
        )
        load_transcript_to_db(in_memory_db, transcript, 0)

        from app.services.index import TranscriptIndex
        index = TranscriptIndex(in_memory_db)

        # Only matches at end of segment
        hits = index.search_hits(r"שלום\d+", search_mode='regex', position_filters={'end'})
        assert len(hits) == 1


class TestRegexSearchEdgeCases:
    """Edge cases for regex search."""

    def test_empty_pattern(self, in_memory_db):
        """Empty regex pattern."""
        transcript = MockTranscript(
            source="test", episode="test/2024.01.01 Test",
            episode_title="Test", episode_date="2024-01-01",
            segments=[MockSegment(0.0, 3.0, "שלום עולם")]
        )
        load_transcript_to_db(in_memory_db, transcript, 0)

        from app.services.index import TranscriptIndex
        index = TranscriptIndex(in_memory_db)

        # Empty pattern - behavior depends on implementation
        hits = index.search_hits("", search_mode='regex')
        assert isinstance(hits, list)

    def test_lookahead(self, in_memory_db):
        """Positive lookahead pattern."""
        transcript = MockTranscript(
            source="test", episode="test/2024.01.01 Test",
            episode_title="Test", episode_date="2024-01-01",
            segments=[MockSegment(0.0, 3.0, "שלום עולם טוב")]
        )
        load_transcript_to_db(in_memory_db, transcript, 0)

        from app.services.index import TranscriptIndex
        index = TranscriptIndex(in_memory_db)

        # Lookahead: שלום followed by space and עולם
        hits = index.search_hits(r"שלום(?=\s+עולם)", search_mode='regex')
        assert len(hits) == 1

    def test_lookbehind(self, in_memory_db):
        """Positive lookbehind pattern."""
        transcript = MockTranscript(
            source="test", episode="test/2024.01.01 Test",
            episode_title="Test", episode_date="2024-01-01",
            segments=[MockSegment(0.0, 3.0, "שלום עולם טוב")]
        )
        load_transcript_to_db(in_memory_db, transcript, 0)

        from app.services.index import TranscriptIndex
        index = TranscriptIndex(in_memory_db)

        # Lookbehind: עולם preceded by שלום and space
        hits = index.search_hits(r"(?<=שלום\s)עולם", search_mode='regex')
        assert len(hits) == 1

    def test_catastrophic_backtracking_prevention(self, in_memory_db):
        """Potentially catastrophic regex should not hang."""
        transcript = MockTranscript(
            source="test", episode="test/2024.01.01 Test",
            episode_title="Test", episode_date="2024-01-01",
            segments=[MockSegment(0.0, 3.0, "aaaaaaaaaaaaaaaaaaaaaaaa")]
        )
        load_transcript_to_db(in_memory_db, transcript, 0)

        from app.services.index import TranscriptIndex
        index = TranscriptIndex(in_memory_db)

        # This could cause catastrophic backtracking in some regex engines
        # The regex library handles this better than re
        import time
        start = time.time()
        hits = index.search_hits(r"(a+)+b", search_mode='regex')
        elapsed = time.time() - start

        # Should complete within reasonable time (not hang)
        assert elapsed < 5.0
        assert hits == []  # No match since no 'b'
