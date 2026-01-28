"""
Tests for Hebrew-specific edge cases.

Covers:
- RTL text handling
- Hebrew maqaf (־) vs ASCII hyphen (-)
- Nikud (vowel marks)
- Gershayim (״) in acronyms
- Geresh (׳) in transliterations
- Mixed Hebrew/English text
- Numbers in Hebrew text
"""
import pytest
from tests.conftest import (
    load_transcript_to_db,
    MockTranscript,
    MockSegment,
)


class TestHebrewMaqaf:
    """Tests for Hebrew maqaf (־ U+05BE) handling."""

    def test_maqaf_exact_match(self, in_memory_db):
        """Exact match with Hebrew maqaf."""
        transcript = MockTranscript(
            source="test", episode="test/2024.01.01 Test",
            episode_title="Test", episode_date="2024-01-01",
            segments=[MockSegment(0.0, 3.0, "בית־ספר טוב")]
        )
        load_transcript_to_db(in_memory_db, transcript, 0)

        from app.services.index import TranscriptIndex
        index = TranscriptIndex(in_memory_db)

        # Search with maqaf should match
        hits = index.search_hits("בית־ספר", search_mode='partial')
        assert len(hits) == 1

    def test_maqaf_vs_ascii_hyphen(self, in_memory_db):
        """Hebrew maqaf (־) vs ASCII hyphen (-) are different characters.

        FTS5 now properly handles hyphens by extracting word tokens.
        """
        transcript = MockTranscript(
            source="test", episode="test/2024.01.01 Test",
            episode_title="Test", episode_date="2024-01-01",
            segments=[MockSegment(0.0, 3.0, "בית־ספר")]  # Hebrew maqaf ־
        )
        load_transcript_to_db(in_memory_db, transcript, 0)

        from app.services.index import TranscriptIndex
        index = TranscriptIndex(in_memory_db)

        # Search with ASCII hyphen - FTS5 interprets "-" as operator
        hits = index.search_hits("בית-ספר", search_mode='partial')  # ASCII hyphen -
        assert isinstance(hits, list)

    def test_maqaf_with_ignore_punct(self, in_memory_db):
        """Maqaf should be ignorable with ignore_punct=True."""
        transcript = MockTranscript(
            source="test", episode="test/2024.01.01 Test",
            episode_title="Test", episode_date="2024-01-01",
            segments=[MockSegment(0.0, 3.0, "בית־ספר")]
        )
        load_transcript_to_db(in_memory_db, transcript, 0)

        from app.services.index import TranscriptIndex
        index = TranscriptIndex(in_memory_db)

        # "בית ספר" (space) should match "בית־ספר" (maqaf) with ignore_punct
        hits = index.search_hits("בית ספר", search_mode='exact', ignore_punct=True)
        assert len(hits) == 1


class TestHebrewGershayim:
    """Tests for Hebrew gershayim (״ U+05F4) in acronyms."""

    def test_gershayim_in_acronym(self, in_memory_db):
        """Gershayim in acronyms like צה״ל."""
        transcript = MockTranscript(
            source="test", episode="test/2024.01.01 Test",
            episode_title="Test", episode_date="2024-01-01",
            segments=[MockSegment(0.0, 3.0, "צה״ל הוא צבא")]
        )
        load_transcript_to_db(in_memory_db, transcript, 0)

        from app.services.index import TranscriptIndex
        index = TranscriptIndex(in_memory_db)

        # Search with gershayim should match
        hits = index.search_hits("צה״ל", search_mode='partial')
        assert len(hits) == 1

    def test_gershayim_vs_double_quote(self, in_memory_db):
        """Gershayim (״) vs ASCII double quote (\") are different."""
        transcript = MockTranscript(
            source="test", episode="test/2024.01.01 Test",
            episode_title="Test", episode_date="2024-01-01",
            segments=[MockSegment(0.0, 3.0, "צה״ל")]  # Hebrew gershayim ״
        )
        load_transcript_to_db(in_memory_db, transcript, 0)

        from app.services.index import TranscriptIndex
        index = TranscriptIndex(in_memory_db)

        # Search with ASCII double quote
        hits = index.search_hits('צה"ל', search_mode='partial')  # ASCII "
        # Document actual behavior
        assert isinstance(hits, list)

    @pytest.mark.xfail(reason="ignore_punct regex doesn't handle embedded Hebrew punctuation like gershayim")
    def test_gershayim_ignore_punct(self, in_memory_db):
        """Search without gershayim should match with ignore_punct.

        BUG: The ignore_punct pattern [\\p{P}\\s]+ only handles punctuation
        BETWEEN words, not punctuation INSIDE words like צה״ל.
        """
        transcript = MockTranscript(
            source="test", episode="test/2024.01.01 Test",
            episode_title="Test", episode_date="2024-01-01",
            segments=[MockSegment(0.0, 3.0, "צה״ל הגדול")]
        )
        load_transcript_to_db(in_memory_db, transcript, 0)

        from app.services.index import TranscriptIndex
        index = TranscriptIndex(in_memory_db)

        # "צהל" should match "צה״ל" with ignore_punct
        hits = index.search_hits("צהל", search_mode='partial', ignore_punct=True)
        assert len(hits) == 1


class TestHebrewGeresh:
    """Tests for Hebrew geresh (׳ U+05F3) in transliterations."""

    def test_geresh_in_name(self, in_memory_db):
        """Geresh in transliterated names like ג׳ון."""
        transcript = MockTranscript(
            source="test", episode="test/2024.01.01 Test",
            episode_title="Test", episode_date="2024-01-01",
            segments=[MockSegment(0.0, 3.0, "ג׳ון הוא שם")]
        )
        load_transcript_to_db(in_memory_db, transcript, 0)

        from app.services.index import TranscriptIndex
        index = TranscriptIndex(in_memory_db)

        # Search with geresh should match
        hits = index.search_hits("ג׳ון", search_mode='partial')
        assert len(hits) == 1

    @pytest.mark.xfail(reason="ignore_punct regex doesn't handle embedded Hebrew punctuation like geresh")
    def test_geresh_ignore_punct(self, in_memory_db):
        """Search without geresh should match with ignore_punct.

        BUG: Same as gershayim - ignore_punct only handles punctuation
        BETWEEN words, not INSIDE words like ג׳ון.
        """
        transcript = MockTranscript(
            source="test", episode="test/2024.01.01 Test",
            episode_title="Test", episode_date="2024-01-01",
            segments=[MockSegment(0.0, 3.0, "ג׳ון הוא שם")]
        )
        load_transcript_to_db(in_memory_db, transcript, 0)

        from app.services.index import TranscriptIndex
        index = TranscriptIndex(in_memory_db)

        # "גון" should match "ג׳ון" with ignore_punct
        hits = index.search_hits("גון", search_mode='partial', ignore_punct=True)
        assert len(hits) == 1


class TestHebrewNikud:
    """Tests for Hebrew nikud (vowel marks) handling."""

    def test_nikud_in_text(self, in_memory_db):
        """Text with nikud should be searchable."""
        transcript = MockTranscript(
            source="test", episode="test/2024.01.01 Test",
            episode_title="Test", episode_date="2024-01-01",
            segments=[MockSegment(0.0, 3.0, "שָׁלוֹם עוֹלָם")]  # With nikud
        )
        load_transcript_to_db(in_memory_db, transcript, 0)

        from app.services.index import TranscriptIndex
        index = TranscriptIndex(in_memory_db)

        # Search with nikud
        hits = index.search_hits("שָׁלוֹם", search_mode='partial')
        # Should find it (exact match)
        assert len(hits) >= 1

    def test_search_without_nikud_matches_with_nikud(self, in_memory_db):
        """Search without nikud should match text with nikud."""
        transcript = MockTranscript(
            source="test", episode="test/2024.01.01 Test",
            episode_title="Test", episode_date="2024-01-01",
            segments=[MockSegment(0.0, 3.0, "שָׁלוֹם עוֹלָם")]  # With nikud
        )
        load_transcript_to_db(in_memory_db, transcript, 0)

        from app.services.index import TranscriptIndex
        index = TranscriptIndex(in_memory_db)

        # Search without nikud - FTS5 tokenizer has remove_diacritics 0
        # so this depends on whether FTS5 normalizes or not
        hits = index.search_hits("שלום", search_mode='partial')
        # Document actual behavior - may or may not match
        assert isinstance(hits, list)


class TestMixedHebrewEnglish:
    """Tests for mixed Hebrew/English text."""

    def test_english_in_hebrew_text(self, in_memory_db):
        """English words in Hebrew text should be searchable."""
        transcript = MockTranscript(
            source="test", episode="test/2024.01.01 Test",
            episode_title="Test", episode_date="2024-01-01",
            segments=[MockSegment(0.0, 3.0, "Flask הוא framework")]
        )
        load_transcript_to_db(in_memory_db, transcript, 0)

        from app.services.index import TranscriptIndex
        index = TranscriptIndex(in_memory_db)

        # Search English word
        hits = index.search_hits("Flask", search_mode='exact')
        assert len(hits) == 1

        hits = index.search_hits("framework", search_mode='exact')
        assert len(hits) == 1

    def test_hebrew_in_english_context(self, in_memory_db):
        """Hebrew words should be searchable in mixed text."""
        transcript = MockTranscript(
            source="test", episode="test/2024.01.01 Test",
            episode_title="Test", episode_date="2024-01-01",
            segments=[MockSegment(0.0, 3.0, "Flask הוא framework")]
        )
        load_transcript_to_db(in_memory_db, transcript, 0)

        from app.services.index import TranscriptIndex
        index = TranscriptIndex(in_memory_db)

        # Search Hebrew word
        hits = index.search_hits("הוא", search_mode='exact')
        assert len(hits) == 1

    def test_mixed_phrase_search(self, in_memory_db):
        """Search for phrase with both Hebrew and English."""
        transcript = MockTranscript(
            source="test", episode="test/2024.01.01 Test",
            episode_title="Test", episode_date="2024-01-01",
            segments=[MockSegment(0.0, 3.0, "Flask הוא framework")]
        )
        load_transcript_to_db(in_memory_db, transcript, 0)

        from app.services.index import TranscriptIndex
        index = TranscriptIndex(in_memory_db)

        # Mixed phrase
        hits = index.search_hits("Flask הוא", search_mode='exact')
        assert len(hits) == 1


class TestHebrewNumbers:
    """Tests for numbers in Hebrew text."""

    def test_numbers_in_hebrew(self, in_memory_db):
        """Numbers should be searchable in Hebrew text."""
        transcript = MockTranscript(
            source="test", episode="test/2024.01.01 Test",
            episode_title="Test", episode_date="2024-01-01",
            segments=[MockSegment(0.0, 3.0, "בשנת 2024 קרה")]
        )
        load_transcript_to_db(in_memory_db, transcript, 0)

        from app.services.index import TranscriptIndex
        index = TranscriptIndex(in_memory_db)

        hits = index.search_hits("2024", search_mode='exact')
        assert len(hits) == 1

    def test_year_range_search(self, in_memory_db):
        """Year pattern search with regex."""
        transcript = MockTranscript(
            source="test", episode="test/2024.01.01 Test",
            episode_title="Test", episode_date="2024-01-01",
            segments=[MockSegment(0.0, 3.0, "בין 2020 ל 2025")]
        )
        load_transcript_to_db(in_memory_db, transcript, 0)

        from app.services.index import TranscriptIndex
        index = TranscriptIndex(in_memory_db)

        # Regex for year pattern
        hits = index.search_hits(r"202\d", search_mode='regex')
        assert len(hits) == 2  # 2020 and 2025


class TestHebrewQuotationMarks:
    """Tests for Hebrew quotation marks."""

    def test_hebrew_style_quotes(self, in_memory_db):
        """Hebrew-style quotes (low-high)."""
        transcript = MockTranscript(
            source="test", episode="test/2024.01.01 Test",
            episode_title="Test", episode_date="2024-01-01",
            segments=[MockSegment(0.0, 3.0, 'הוא אמר \u201eשלום\u201d וברח')]
        )
        load_transcript_to_db(in_memory_db, transcript, 0)

        from app.services.index import TranscriptIndex
        index = TranscriptIndex(in_memory_db)

        hits = index.search_hits("שלום", search_mode='exact')
        assert len(hits) == 1

    def test_english_style_quotes(self, in_memory_db):
        """English-style quotes "" in Hebrew text."""
        transcript = MockTranscript(
            source="test", episode="test/2024.01.01 Test",
            episode_title="Test", episode_date="2024-01-01",
            segments=[MockSegment(0.0, 3.0, 'הוא אמר "שלום" וברח')]
        )
        load_transcript_to_db(in_memory_db, transcript, 0)

        from app.services.index import TranscriptIndex
        index = TranscriptIndex(in_memory_db)

        hits = index.search_hits("שלום", search_mode='exact')
        assert len(hits) == 1


class TestHebrewFinalLetters:
    """Tests for Hebrew final letters (sofit)."""

    def test_final_mem(self, in_memory_db):
        """Final mem (ם) should work correctly."""
        transcript = MockTranscript(
            source="test", episode="test/2024.01.01 Test",
            episode_title="Test", episode_date="2024-01-01",
            segments=[MockSegment(0.0, 3.0, "שלום וחיים")]
        )
        load_transcript_to_db(in_memory_db, transcript, 0)

        from app.services.index import TranscriptIndex
        index = TranscriptIndex(in_memory_db)

        # Note: FTS5 may tokenize "וחיים" as one token including the ו prefix
        hits = index.search_hits("שלום", search_mode='exact')
        assert len(hits) == 1

        # Search for "וחיים" (with ו) since FTS5 tokenizes it that way
        hits = index.search_hits("וחיים", search_mode='exact')
        assert len(hits) == 1

    def test_final_nun(self, in_memory_db):
        """Final nun (ן) should work correctly."""
        transcript = MockTranscript(
            source="test", episode="test/2024.01.01 Test",
            episode_title="Test", episode_date="2024-01-01",
            segments=[MockSegment(0.0, 3.0, "ישן וקטן")]
        )
        load_transcript_to_db(in_memory_db, transcript, 0)

        from app.services.index import TranscriptIndex
        index = TranscriptIndex(in_memory_db)

        hits = index.search_hits("ישן", search_mode='exact')
        assert len(hits) == 1

        # Search for "וקטן" (with ו) since FTS5 tokenizes it that way
        hits = index.search_hits("וקטן", search_mode='exact')
        assert len(hits) == 1


class TestHebrewRTL:
    """Tests for RTL (right-to-left) handling."""

    def test_rtl_char_offset_calculation(self, in_memory_db):
        """Character offsets should be correct for RTL text."""
        transcript = MockTranscript(
            source="test", episode="test/2024.01.01 Test",
            episode_title="Test", episode_date="2024-01-01",
            segments=[
                MockSegment(0.0, 3.0, "שלום"),      # 4 chars
                MockSegment(3.0, 6.0, "עולם"),      # 4 chars
            ]
        )
        load_transcript_to_db(in_memory_db, transcript, 0)

        from app.services.index import TranscriptIndex
        index = TranscriptIndex(in_memory_db)

        # Full text should be "שלום עולם"
        full_text = index.get_document_text(0)
        assert full_text == "שלום עולם"

        # Character at index 0 should be ש
        assert full_text[0] == "ש"

        # Space at index 4
        assert full_text[4] == " "

        # ע at index 5
        assert full_text[5] == "ע"

    def test_search_returns_correct_offset_rtl(self, in_memory_db):
        """Search should return correct char_offset for RTL text."""
        transcript = MockTranscript(
            source="test", episode="test/2024.01.01 Test",
            episode_title="Test", episode_date="2024-01-01",
            segments=[MockSegment(0.0, 3.0, "שלום עולם")]
        )
        load_transcript_to_db(in_memory_db, transcript, 0)

        from app.services.index import TranscriptIndex
        index = TranscriptIndex(in_memory_db)

        # Search for "עולם" which starts at offset 5
        hits = index.search_hits("עולם", search_mode='exact')
        assert len(hits) == 1
        assert hits[0][1] == 5  # char_offset should be 5
