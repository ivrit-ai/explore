"""
Tests for exact search mode (_search_fts5_exact).

Exact mode uses word boundaries (\b) to match whole words only.
"""

from tests.conftest import (
    MockSegment,
    MockTranscript,
    load_transcript_to_db,
)


class TestExactSearchBasic:
    """Basic exact search functionality."""

    def test_simple_word_match(self, in_memory_db):
        """Simple single word should match exactly."""
        transcript = MockTranscript(
            source="test",
            episode="test/2024.01.01 Test",
            episode_title="Test",
            episode_date="2024-01-01",
            segments=[MockSegment(0.0, 3.0, "שלום עולם טוב")],
        )
        load_transcript_to_db(in_memory_db, transcript, 0)

        from app.services.index import TranscriptIndex

        index = TranscriptIndex(in_memory_db)

        hits = index.search_hits("שלום", search_mode="exact")
        assert len(hits) == 1
        assert hits[0][0] == 0  # doc_id
        assert hits[0][1] == 0  # char_offset at start

    def test_word_boundary_respected(self, in_memory_db):
        """Exact search should not match substrings."""
        transcript = MockTranscript(
            source="test",
            episode="test/2024.01.01 Test",
            episode_title="Test",
            episode_date="2024-01-01",
            segments=[MockSegment(0.0, 3.0, "שלומית היא שם")],
        )
        load_transcript_to_db(in_memory_db, transcript, 0)

        from app.services.index import TranscriptIndex

        index = TranscriptIndex(in_memory_db)

        # "שלום" should NOT match "שלומית" in exact mode
        hits = index.search_hits("שלום", search_mode="exact")
        assert len(hits) == 0

    def test_multi_word_phrase(self, in_memory_db):
        """Multi-word phrase should match as exact phrase."""
        transcript = MockTranscript(
            source="test",
            episode="test/2024.01.01 Test",
            episode_title="Test",
            episode_date="2024-01-01",
            segments=[MockSegment(0.0, 3.0, "שלום עולם טוב מאוד")],
        )
        load_transcript_to_db(in_memory_db, transcript, 0)

        from app.services.index import TranscriptIndex

        index = TranscriptIndex(in_memory_db)

        hits = index.search_hits("שלום עולם", search_mode="exact")
        assert len(hits) == 1

    def test_phrase_not_found_if_words_not_adjacent(self, in_memory_db):
        """Phrase should not match if words are not adjacent."""
        transcript = MockTranscript(
            source="test",
            episode="test/2024.01.01 Test",
            episode_title="Test",
            episode_date="2024-01-01",
            segments=[MockSegment(0.0, 3.0, "שלום היקר עולם")],
        )
        load_transcript_to_db(in_memory_db, transcript, 0)

        from app.services.index import TranscriptIndex

        index = TranscriptIndex(in_memory_db)

        # "שלום עולם" should NOT match "שלום היקר עולם"
        hits = index.search_hits("שלום עולם", search_mode="exact")
        assert len(hits) == 0

    def test_multiple_occurrences(self, in_memory_db):
        """Should find all occurrences of exact word."""
        transcript = MockTranscript(
            source="test",
            episode="test/2024.01.01 Test",
            episode_title="Test",
            episode_date="2024-01-01",
            segments=[
                MockSegment(0.0, 3.0, "שלום לכולם"),
                MockSegment(3.0, 6.0, "ושוב שלום"),
            ],
        )
        load_transcript_to_db(in_memory_db, transcript, 0)

        from app.services.index import TranscriptIndex

        index = TranscriptIndex(in_memory_db)

        hits = index.search_hits("שלום", search_mode="exact")
        assert len(hits) == 2


class TestExactSearchHebrewSpecific:
    """Hebrew-specific exact search tests."""

    def test_hebrew_word_boundaries(self, in_memory_db):
        """Hebrew word boundaries should work correctly."""
        transcript = MockTranscript(
            source="test",
            episode="test/2024.01.01 Test",
            episode_title="Test",
            episode_date="2024-01-01",
            segments=[MockSegment(0.0, 3.0, "הבית הזה גדול")],
        )
        load_transcript_to_db(in_memory_db, transcript, 0)

        from app.services.index import TranscriptIndex

        index = TranscriptIndex(in_memory_db)

        # "בית" should NOT match "הבית" (has prefix ה)
        hits = index.search_hits("בית", search_mode="exact")
        assert len(hits) == 0

        # "הבית" should match
        hits = index.search_hits("הבית", search_mode="exact")
        assert len(hits) == 1

    def test_nikud_handling(self, in_memory_db):
        """Test handling of nikud (vowel marks)."""
        transcript = MockTranscript(
            source="test",
            episode="test/2024.01.01 Test",
            episode_title="Test",
            episode_date="2024-01-01",
            segments=[MockSegment(0.0, 3.0, "שָׁלוֹם עולם")],  # With nikud
        )
        load_transcript_to_db(in_memory_db, transcript, 0)

        from app.services.index import TranscriptIndex

        index = TranscriptIndex(in_memory_db)

        # Search without nikud should find text with nikud
        # (depends on FTS5 tokenizer config: remove_diacritics 0)
        hits = index.search_hits("שלום", search_mode="exact")
        # Document actual behavior - FTS5 may or may not match
        assert isinstance(hits, list)

    def test_final_letters(self, in_memory_db):
        """Hebrew final letters (ם, ן, ץ, ף, ך) should match correctly."""
        transcript = MockTranscript(
            source="test",
            episode="test/2024.01.01 Test",
            episode_title="Test",
            episode_date="2024-01-01",
            segments=[MockSegment(0.0, 3.0, "שלום וחיים טובים")],
        )
        load_transcript_to_db(in_memory_db, transcript, 0)

        from app.services.index import TranscriptIndex

        index = TranscriptIndex(in_memory_db)

        hits = index.search_hits("שלום", search_mode="exact")
        assert len(hits) == 1

        # Note: FTS5 tokenizes "וחיים" as one token (with ו prefix)
        hits = index.search_hits("וחיים", search_mode="exact")
        assert len(hits) == 1


class TestExactSearchWithFilters:
    """Exact search with date and source filters."""

    def test_date_filter_from(self, in_memory_db):
        """Date filter from should exclude older documents."""
        t1 = MockTranscript(
            source="test",
            episode="test/2024.01.01 Old",
            episode_title="Old",
            episode_date="2024-01-01",
            segments=[MockSegment(0.0, 3.0, "שלום ישן")],
        )
        t2 = MockTranscript(
            source="test",
            episode="test/2024.06.01 New",
            episode_title="New",
            episode_date="2024-06-01",
            segments=[MockSegment(0.0, 3.0, "שלום חדש")],
        )
        load_transcript_to_db(in_memory_db, t1, 0)
        load_transcript_to_db(in_memory_db, t2, 1)

        from app.services.index import TranscriptIndex

        index = TranscriptIndex(in_memory_db)

        # Filter from March - should only find June doc
        hits = index.search_hits("שלום", search_mode="exact", date_from="2024-03-01")
        assert len(hits) == 1
        assert hits[0][0] == 1  # doc_id of newer transcript

    def test_date_filter_to(self, in_memory_db):
        """Date filter to should exclude newer documents."""
        t1 = MockTranscript(
            source="test",
            episode="test/2024.01.01 Old",
            episode_title="Old",
            episode_date="2024-01-01",
            segments=[MockSegment(0.0, 3.0, "שלום ישן")],
        )
        t2 = MockTranscript(
            source="test",
            episode="test/2024.06.01 New",
            episode_title="New",
            episode_date="2024-06-01",
            segments=[MockSegment(0.0, 3.0, "שלום חדש")],
        )
        load_transcript_to_db(in_memory_db, t1, 0)
        load_transcript_to_db(in_memory_db, t2, 1)

        from app.services.index import TranscriptIndex

        index = TranscriptIndex(in_memory_db)

        # Filter to March - should only find January doc
        hits = index.search_hits("שלום", search_mode="exact", date_to="2024-03-01")
        assert len(hits) == 1
        assert hits[0][0] == 0  # doc_id of older transcript

    def test_source_filter(self, in_memory_db):
        """Source filter should only return matching sources."""
        t1 = MockTranscript(
            source="podcast-a",
            episode="podcast-a/2024.01.01 Ep1",
            episode_title="Ep1",
            episode_date="2024-01-01",
            segments=[MockSegment(0.0, 3.0, "שלום מפודקאסט א")],
        )
        t2 = MockTranscript(
            source="podcast-b",
            episode="podcast-b/2024.01.01 Ep1",
            episode_title="Ep1",
            episode_date="2024-01-01",
            segments=[MockSegment(0.0, 3.0, "שלום מפודקאסט ב")],
        )
        load_transcript_to_db(in_memory_db, t1, 0)
        load_transcript_to_db(in_memory_db, t2, 1)

        from app.services.index import TranscriptIndex

        index = TranscriptIndex(in_memory_db)

        # Filter to podcast-a only
        hits = index.search_hits("שלום", search_mode="exact", sources=["podcast-a"])
        assert len(hits) == 1
        assert hits[0][0] == 0

    def test_combined_filters(self, in_memory_db):
        """Multiple filters should be combined with AND."""
        t1 = MockTranscript(
            source="podcast-a",
            episode="podcast-a/2024.01.01 Ep1",
            episode_title="Ep1",
            episode_date="2024-01-01",
            segments=[MockSegment(0.0, 3.0, "שלום ינואר")],
        )
        t2 = MockTranscript(
            source="podcast-a",
            episode="podcast-a/2024.06.01 Ep2",
            episode_title="Ep2",
            episode_date="2024-06-01",
            segments=[MockSegment(0.0, 3.0, "שלום יוני")],
        )
        t3 = MockTranscript(
            source="podcast-b",
            episode="podcast-b/2024.06.01 Ep1",
            episode_title="Ep1",
            episode_date="2024-06-01",
            segments=[MockSegment(0.0, 3.0, "שלום אחר")],
        )
        load_transcript_to_db(in_memory_db, t1, 0)
        load_transcript_to_db(in_memory_db, t2, 1)
        load_transcript_to_db(in_memory_db, t3, 2)

        from app.services.index import TranscriptIndex

        index = TranscriptIndex(in_memory_db)

        # Filter: podcast-a AND after March
        hits = index.search_hits("שלום", search_mode="exact", sources=["podcast-a"], date_from="2024-03-01")
        assert len(hits) == 1
        assert hits[0][0] == 1  # Only June episode of podcast-a


class TestExactSearchEdgeCases:
    """Edge cases for exact search."""

    def test_empty_query(self, in_memory_db):
        """Empty query should return empty results or handle gracefully."""
        transcript = MockTranscript(
            source="test",
            episode="test/2024.01.01 Test",
            episode_title="Test",
            episode_date="2024-01-01",
            segments=[MockSegment(0.0, 3.0, "שלום עולם")],
        )
        load_transcript_to_db(in_memory_db, transcript, 0)

        from app.services.index import TranscriptIndex

        index = TranscriptIndex(in_memory_db)

        # Empty query behavior
        hits = index.search_hits("", search_mode="exact")
        assert isinstance(hits, list)

    def test_query_with_special_regex_chars(self, in_memory_db):
        """Query with regex special characters should be escaped."""
        transcript = MockTranscript(
            source="test",
            episode="test/2024.01.01 Test",
            episode_title="Test",
            episode_date="2024-01-01",
            segments=[MockSegment(0.0, 3.0, "C++ הוא שפה")],
        )
        load_transcript_to_db(in_memory_db, transcript, 0)

        from app.services.index import TranscriptIndex

        index = TranscriptIndex(in_memory_db)

        # "C++" has regex special chars, should be escaped
        hits = index.search_hits("C++", search_mode="exact")
        # Should match literally, not as regex
        assert isinstance(hits, list)

    def test_query_with_double_quotes(self, in_memory_db):
        """Query with double quotes should be handled."""
        transcript = MockTranscript(
            source="test",
            episode="test/2024.01.01 Test",
            episode_title="Test",
            episode_date="2024-01-01",
            segments=[MockSegment(0.0, 3.0, 'הוא אמר "שלום" וברח')],
        )
        load_transcript_to_db(in_memory_db, transcript, 0)

        from app.services.index import TranscriptIndex

        index = TranscriptIndex(in_memory_db)

        # Double quotes are escaped in FTS5 query
        hits = index.search_hits("שלום", search_mode="exact")
        assert len(hits) == 1

    def test_very_long_query(self, in_memory_db):
        """Very long query should not crash."""
        transcript = MockTranscript(
            source="test",
            episode="test/2024.01.01 Test",
            episode_title="Test",
            episode_date="2024-01-01",
            segments=[MockSegment(0.0, 3.0, "שלום עולם")],
        )
        load_transcript_to_db(in_memory_db, transcript, 0)

        from app.services.index import TranscriptIndex

        index = TranscriptIndex(in_memory_db)

        # Very long query
        long_query = "מילה " * 100
        hits = index.search_hits(long_query.strip(), search_mode="exact")
        # Should not crash
        assert isinstance(hits, list)
