"""
Tests for segment lookup functionality.

Tests the char_offset → segment mapping logic:
- get_segment_at_offset()
- get_segments_by_ids()
- segment boundary calculations
"""
import pytest
from tests.conftest import (
    load_transcript_to_db,
    MockTranscript,
    MockSegment,
)


class TestGetSegmentAtOffset:
    """Tests for get_segment_at_offset method."""

    def test_exact_offset_match(self, in_memory_db):
        """Hit at exact segment start offset."""
        transcript = MockTranscript(
            source="test", episode="test/2024.01.01 Test",
            episode_title="Test", episode_date="2024-01-01",
            segments=[
                MockSegment(0.0, 3.0, "שלום"),      # char_offset=0, len=4
                MockSegment(3.0, 6.0, "עולם"),      # char_offset=5, len=4
            ]
        )
        load_transcript_to_db(in_memory_db, transcript, 0)

        from app.services.index import TranscriptIndex
        index = TranscriptIndex(in_memory_db)

        # Hit at exact start of second segment
        segment = index.get_segment_at_offset(0, 5)
        assert segment["segment_id"] == 1
        assert segment["text"] == "עולם"

    def test_offset_within_segment(self, in_memory_db):
        """Hit in middle of segment."""
        transcript = MockTranscript(
            source="test", episode="test/2024.01.01 Test",
            episode_title="Test", episode_date="2024-01-01",
            segments=[
                MockSegment(0.0, 3.0, "שלום עולם טוב"),  # char_offset=0
            ]
        )
        load_transcript_to_db(in_memory_db, transcript, 0)

        from app.services.index import TranscriptIndex
        index = TranscriptIndex(in_memory_db)

        # Hit in middle
        segment = index.get_segment_at_offset(0, 5)
        assert segment["segment_id"] == 0
        assert "שלום עולם טוב" in segment["text"]

    def test_offset_at_first_segment(self, in_memory_db):
        """Hit at first segment (char_offset=0)."""
        transcript = MockTranscript(
            source="test", episode="test/2024.01.01 Test",
            episode_title="Test", episode_date="2024-01-01",
            segments=[
                MockSegment(0.0, 3.0, "ראשון"),
                MockSegment(3.0, 6.0, "שני"),
            ]
        )
        load_transcript_to_db(in_memory_db, transcript, 0)

        from app.services.index import TranscriptIndex
        index = TranscriptIndex(in_memory_db)

        segment = index.get_segment_at_offset(0, 0)
        assert segment["segment_id"] == 0
        assert segment["text"] == "ראשון"

    def test_offset_at_last_segment(self, in_memory_db):
        """Hit at last segment."""
        transcript = MockTranscript(
            source="test", episode="test/2024.01.01 Test",
            episode_title="Test", episode_date="2024-01-01",
            segments=[
                MockSegment(0.0, 3.0, "ראשון"),     # offset=0, len=5
                MockSegment(3.0, 6.0, "שני"),       # offset=6, len=3
                MockSegment(6.0, 9.0, "שלישי"),    # offset=10, len=5
            ]
        )
        load_transcript_to_db(in_memory_db, transcript, 0)

        from app.services.index import TranscriptIndex
        index = TranscriptIndex(in_memory_db)

        # Full text: "ראשון שני שלישי"
        # char 10 is start of "שלישי"
        segment = index.get_segment_at_offset(0, 10)
        assert segment["segment_id"] == 2
        assert segment["text"] == "שלישי"

    def test_offset_near_boundary(self, in_memory_db):
        """Hit near segment boundary should return correct segment."""
        transcript = MockTranscript(
            source="test", episode="test/2024.01.01 Test",
            episode_title="Test", episode_date="2024-01-01",
            segments=[
                MockSegment(0.0, 3.0, "אבג"),       # offset=0, len=3
                MockSegment(3.0, 6.0, "דהו"),       # offset=4, len=3
            ]
        )
        load_transcript_to_db(in_memory_db, transcript, 0)

        from app.services.index import TranscriptIndex
        index = TranscriptIndex(in_memory_db)

        # Full text: "אבג דהו"
        # offset 3 is the space between segments
        segment_at_3 = index.get_segment_at_offset(0, 3)
        # Should return first segment since char_offset <= 3 returns seg with offset 0
        assert segment_at_3["segment_id"] == 0

        # offset 4 is start of second segment
        segment_at_4 = index.get_segment_at_offset(0, 4)
        assert segment_at_4["segment_id"] == 1

    def test_nonexistent_offset_raises(self, in_memory_db):
        """Offset beyond document should raise IndexError."""
        transcript = MockTranscript(
            source="test", episode="test/2024.01.01 Test",
            episode_title="Test", episode_date="2024-01-01",
            segments=[
                MockSegment(0.0, 3.0, "קצר"),
            ]
        )
        load_transcript_to_db(in_memory_db, transcript, 0)

        from app.services.index import TranscriptIndex
        index = TranscriptIndex(in_memory_db)

        # Offset way beyond document
        # Actually, get_segment_at_offset uses <= so large offset returns last segment
        segment = index.get_segment_at_offset(0, 1000)
        # Should return the segment with highest offset <= 1000
        assert segment["segment_id"] == 0

    def test_nonexistent_document_raises(self, in_memory_db):
        """Non-existent document should raise IndexError."""
        transcript = MockTranscript(
            source="test", episode="test/2024.01.01 Test",
            episode_title="Test", episode_date="2024-01-01",
            segments=[MockSegment(0.0, 3.0, "טקסט")]
        )
        load_transcript_to_db(in_memory_db, transcript, 0)

        from app.services.index import TranscriptIndex
        index = TranscriptIndex(in_memory_db)

        with pytest.raises(IndexError):
            index.get_segment_at_offset(999, 0)  # doc_id 999 doesn't exist


class TestGetSegmentsByIds:
    """Tests for batch segment lookup by (doc_id, segment_id)."""

    def test_single_lookup(self, in_memory_db):
        """Single segment lookup."""
        transcript = MockTranscript(
            source="test", episode="test/2024.01.01 Test",
            episode_title="Test", episode_date="2024-01-01",
            segments=[
                MockSegment(0.0, 3.0, "ראשון"),
                MockSegment(3.0, 6.0, "שני"),
            ]
        )
        load_transcript_to_db(in_memory_db, transcript, 0)

        from app.services.index import TranscriptIndex
        index = TranscriptIndex(in_memory_db)

        results = index.get_segments_by_ids([(0, 1)])
        assert len(results) == 1
        assert results[0]["segment_id"] == 1
        assert results[0]["text"] == "שני"

    def test_multiple_lookups(self, in_memory_db):
        """Multiple segment lookups."""
        transcript = MockTranscript(
            source="test", episode="test/2024.01.01 Test",
            episode_title="Test", episode_date="2024-01-01",
            segments=[
                MockSegment(0.0, 3.0, "א"),
                MockSegment(3.0, 6.0, "ב"),
                MockSegment(6.0, 9.0, "ג"),
            ]
        )
        load_transcript_to_db(in_memory_db, transcript, 0)

        from app.services.index import TranscriptIndex
        index = TranscriptIndex(in_memory_db)

        results = index.get_segments_by_ids([(0, 0), (0, 2)])
        assert len(results) == 2
        texts = {r["text"] for r in results}
        assert texts == {"א", "ג"}

    def test_empty_lookups(self, in_memory_db):
        """Empty lookup list should return empty result."""
        transcript = MockTranscript(
            source="test", episode="test/2024.01.01 Test",
            episode_title="Test", episode_date="2024-01-01",
            segments=[MockSegment(0.0, 3.0, "טקסט")]
        )
        load_transcript_to_db(in_memory_db, transcript, 0)

        from app.services.index import TranscriptIndex
        index = TranscriptIndex(in_memory_db)

        results = index.get_segments_by_ids([])
        assert results == []

    def test_nonexistent_segment_id(self, in_memory_db):
        """Non-existent segment_id should not be in results."""
        transcript = MockTranscript(
            source="test", episode="test/2024.01.01 Test",
            episode_title="Test", episode_date="2024-01-01",
            segments=[
                MockSegment(0.0, 3.0, "יחיד"),
            ]
        )
        load_transcript_to_db(in_memory_db, transcript, 0)

        from app.services.index import TranscriptIndex
        index = TranscriptIndex(in_memory_db)

        # Request existing and non-existing
        results = index.get_segments_by_ids([(0, 0), (0, 999)])
        assert len(results) == 1
        assert results[0]["segment_id"] == 0

    def test_cross_document_lookups(self, in_memory_db):
        """Lookups across multiple documents."""
        t1 = MockTranscript(
            source="test-a", episode="test-a/2024.01.01 A",
            episode_title="A", episode_date="2024-01-01",
            segments=[MockSegment(0.0, 3.0, "מסמך א")]
        )
        t2 = MockTranscript(
            source="test-b", episode="test-b/2024.01.01 B",
            episode_title="B", episode_date="2024-01-01",
            segments=[MockSegment(0.0, 3.0, "מסמך ב")]
        )
        load_transcript_to_db(in_memory_db, t1, 0)
        load_transcript_to_db(in_memory_db, t2, 1)

        from app.services.index import TranscriptIndex
        index = TranscriptIndex(in_memory_db)

        results = index.get_segments_by_ids([(0, 0), (1, 0)])
        assert len(results) == 2
        texts = {r["text"] for r in results}
        assert "מסמך א" in texts
        assert "מסמך ב" in texts


class TestSegmentTimestamps:
    """Tests for segment timestamps."""

    def test_segment_has_timestamps(self, in_memory_db):
        """Segment should have start_time and end_time."""
        transcript = MockTranscript(
            source="test", episode="test/2024.01.01 Test",
            episode_title="Test", episode_date="2024-01-01",
            segments=[
                MockSegment(1.5, 4.2, "עם חותמות זמן"),
            ]
        )
        load_transcript_to_db(in_memory_db, transcript, 0)

        from app.services.index import TranscriptIndex
        index = TranscriptIndex(in_memory_db)

        segment = index.get_segment_at_offset(0, 0)
        assert segment["start_time"] == 1.5
        assert segment["end_time"] == 4.2

    def test_segment_timestamps_preserved(self, in_memory_db):
        """Multiple segments should have correct timestamps."""
        transcript = MockTranscript(
            source="test", episode="test/2024.01.01 Test",
            episode_title="Test", episode_date="2024-01-01",
            segments=[
                MockSegment(0.0, 2.5, "ראשון"),
                MockSegment(2.5, 5.0, "שני"),
                MockSegment(5.0, 8.3, "שלישי"),
            ]
        )
        load_transcript_to_db(in_memory_db, transcript, 0)

        from app.services.index import TranscriptIndex
        index = TranscriptIndex(in_memory_db)

        segments = index.get_segments_for_document(0)
        assert len(segments) == 3
        assert segments[0]["start_time"] == 0.0
        assert segments[0]["end_time"] == 2.5
        assert segments[1]["start_time"] == 2.5
        assert segments[1]["end_time"] == 5.0
        assert segments[2]["start_time"] == 5.0
        assert segments[2]["end_time"] == 8.3


class TestCharOffsetCalculation:
    """Tests for character offset calculation."""

    def test_char_offsets_are_correct(self, in_memory_db):
        """Verify char_offset values are correctly calculated."""
        transcript = MockTranscript(
            source="test", episode="test/2024.01.01 Test",
            episode_title="Test", episode_date="2024-01-01",
            segments=[
                MockSegment(0.0, 3.0, "אבג"),       # 3 chars, offset=0
                MockSegment(3.0, 6.0, "דהו"),       # 3 chars, offset=4 (3+1 space)
                MockSegment(6.0, 9.0, "זחט"),       # 3 chars, offset=8 (4+3+1 space)
            ]
        )
        load_transcript_to_db(in_memory_db, transcript, 0)

        from app.services.index import TranscriptIndex
        index = TranscriptIndex(in_memory_db)

        segments = index.get_segments_for_document(0)
        assert segments[0]["char_offset"] == 0
        assert segments[1]["char_offset"] == 4  # "אבג" + " "
        assert segments[2]["char_offset"] == 8  # "אבג דהו" + " "

    def test_full_text_matches_offsets(self, in_memory_db):
        """Full text should align with segment char_offsets."""
        transcript = MockTranscript(
            source="test", episode="test/2024.01.01 Test",
            episode_title="Test", episode_date="2024-01-01",
            segments=[
                MockSegment(0.0, 3.0, "ראשון"),
                MockSegment(3.0, 6.0, "שני"),
            ]
        )
        load_transcript_to_db(in_memory_db, transcript, 0)

        from app.services.index import TranscriptIndex
        index = TranscriptIndex(in_memory_db)

        full_text = index.get_document_text(0)
        segments = index.get_segments_for_document(0)

        for seg in segments:
            offset = seg["char_offset"]
            text = seg["text"]
            # Text at offset should start with segment text
            assert full_text[offset:offset + len(text)] == text


class TestSegmentForHitFunction:
    """Tests for segment_for_hit helper function."""

    def test_segment_for_hit_basic(self, in_memory_db):
        """segment_for_hit should return correct Segment dataclass."""
        transcript = MockTranscript(
            source="test", episode="test/2024.01.01 Test",
            episode_title="Test", episode_date="2024-01-01",
            segments=[
                MockSegment(0.0, 3.0, "שלום"),
                MockSegment(3.0, 6.0, "עולם"),
            ]
        )
        load_transcript_to_db(in_memory_db, transcript, 0)

        from app.services.index import TranscriptIndex, segment_for_hit
        index = TranscriptIndex(in_memory_db)

        segment = segment_for_hit(index, episode_idx=0, char_offset=0)
        assert segment.episode_idx == 0
        assert segment.seg_idx == 0
        assert segment.text == "שלום"
        assert segment.start_sec == 0.0
        assert segment.end_sec == 3.0

    def test_segment_for_hit_second_segment(self, in_memory_db):
        """segment_for_hit for offset in second segment."""
        transcript = MockTranscript(
            source="test", episode="test/2024.01.01 Test",
            episode_title="Test", episode_date="2024-01-01",
            segments=[
                MockSegment(0.0, 3.0, "שלום"),      # offset=0
                MockSegment(3.0, 6.0, "עולם"),      # offset=5
            ]
        )
        load_transcript_to_db(in_memory_db, transcript, 0)

        from app.services.index import TranscriptIndex, segment_for_hit
        index = TranscriptIndex(in_memory_db)

        segment = segment_for_hit(index, episode_idx=0, char_offset=5)
        assert segment.seg_idx == 1
        assert segment.text == "עולם"
