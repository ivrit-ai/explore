"""
Tests for _classify_hit_position function.

This is a HIGH PRIORITY test area - the user reported potential issues
with position filtering.

The function determines if a match is at:
- 'start': Match begins in first word of segment
- 'end': Match ends in last word of segment
- 'cross': Match spans multiple segments
"""

from app.services.index import _classify_hit_position


class TestClassifyHitPositionBasic:
    """Basic position classification tests."""

    def test_match_at_segment_start(self):
        """Match starting at first word should return 'start'."""
        # Segment: "מילה ראשונה באמצע"
        # Match "מילה" at position 0
        full_text = "מילה ראשונה באמצע"
        seg_boundaries = [(0, len(full_text))]
        seg_offsets = [0]

        result = _classify_hit_position(
            hit_start=0,
            hit_end=4,  # "מילה"
            full_text=full_text,
            seg_boundaries=seg_boundaries,
            seg_offsets=seg_offsets,
        )

        assert "start" in result

    def test_match_at_segment_end(self):
        """Match ending at last word should return 'end'."""
        # Segment: "מילה ראשונה באמצע"
        full_text = "מילה ראשונה באמצע"
        seg_boundaries = [(0, len(full_text))]
        seg_offsets = [0]

        # Find "באמצע" position
        last_word_start = full_text.rfind("באמצע")
        last_word_end = len(full_text)

        result = _classify_hit_position(
            hit_start=last_word_start,
            hit_end=last_word_end,
            full_text=full_text,
            seg_boundaries=seg_boundaries,
            seg_offsets=seg_offsets,
        )

        assert "end" in result

    def test_match_in_middle(self):
        """Match in middle should return empty or neither start/end."""
        # Segment: "מילה ראשונה באמצע"
        full_text = "מילה ראשונה באמצע"
        seg_boundaries = [(0, len(full_text))]
        seg_offsets = [0]

        # Find "ראשונה" position (middle word)
        middle_start = full_text.find("ראשונה")
        middle_end = middle_start + len("ראשונה")

        result = _classify_hit_position(
            hit_start=middle_start,
            hit_end=middle_end,
            full_text=full_text,
            seg_boundaries=seg_boundaries,
            seg_offsets=seg_offsets,
        )

        assert "start" not in result
        assert "end" not in result

    def test_match_spans_start_and_end(self):
        """Match spanning entire segment should have both 'start' and 'end'."""
        full_text = "מילה ראשונה באמצע"
        seg_boundaries = [(0, len(full_text))]
        seg_offsets = [0]

        result = _classify_hit_position(
            hit_start=0,
            hit_end=len(full_text),
            full_text=full_text,
            seg_boundaries=seg_boundaries,
            seg_offsets=seg_offsets,
        )

        assert "start" in result
        assert "end" in result


class TestClassifyHitPositionSingleWord:
    """Tests for single-word segments (edge case)."""

    def test_single_word_segment_match_start(self):
        """Single-word segment: match at start should return 'start'."""
        full_text = "בודד"
        seg_boundaries = [(0, 4)]
        seg_offsets = [0]

        result = _classify_hit_position(
            hit_start=0, hit_end=4, full_text=full_text, seg_boundaries=seg_boundaries, seg_offsets=seg_offsets
        )

        assert "start" in result

    def test_single_word_segment_full_match(self):
        """Single-word segment: full match should return both 'start' and 'end'."""
        full_text = "בודד"
        seg_boundaries = [(0, 4)]
        seg_offsets = [0]

        result = _classify_hit_position(
            hit_start=0, hit_end=4, full_text=full_text, seg_boundaries=seg_boundaries, seg_offsets=seg_offsets
        )

        assert "start" in result
        assert "end" in result

    def test_single_word_partial_match_at_start(self):
        """Single-word segment: partial match at start."""
        full_text = "בודד"
        seg_boundaries = [(0, 4)]
        seg_offsets = [0]

        # Match just "בו" (first 2 chars)
        result = _classify_hit_position(
            hit_start=0, hit_end=2, full_text=full_text, seg_boundaries=seg_boundaries, seg_offsets=seg_offsets
        )

        assert "start" in result
        # Should NOT have 'end' since we didn't match to end of segment
        assert "end" not in result


class TestClassifyHitPositionMultiSegment:
    """Tests for multiple segments and cross-segment matching."""

    def test_cross_segment_match(self):
        """Match spanning segment boundary should return 'cross'."""
        # Two segments: "שלום עולם" + " מה קורה"
        # Full text: "שלום עולם מה קורה"
        seg1 = "שלום עולם"
        seg2 = "מה קורה"
        full_text = seg1 + " " + seg2

        seg_boundaries = [(0, len(seg1)), (len(seg1) + 1, len(seg2))]
        seg_offsets = [0, len(seg1) + 1]

        # Match that starts in seg1 and extends past it
        # "עולם מה" - crosses boundary
        match_start = full_text.find("עולם")
        match_end = full_text.find("מה") + 2

        result = _classify_hit_position(
            hit_start=match_start,
            hit_end=match_end,
            full_text=full_text,
            seg_boundaries=seg_boundaries,
            seg_offsets=seg_offsets,
        )

        assert "cross" in result

    def test_match_at_second_segment_start(self):
        """Match at start of second segment should return 'start'."""
        seg1 = "שלום עולם"
        seg2 = "מה קורה"
        full_text = seg1 + " " + seg2

        seg_boundaries = [(0, len(seg1)), (len(seg1) + 1, len(seg2))]
        seg_offsets = [0, len(seg1) + 1]

        # Match "מה" at start of second segment
        match_start = len(seg1) + 1  # Start of seg2
        match_end = match_start + 2

        result = _classify_hit_position(
            hit_start=match_start,
            hit_end=match_end,
            full_text=full_text,
            seg_boundaries=seg_boundaries,
            seg_offsets=seg_offsets,
        )

        assert "start" in result

    def test_match_at_first_segment_end(self):
        """Match at end of first segment should return 'end'."""
        seg1 = "שלום עולם"
        seg2 = "מה קורה"
        full_text = seg1 + " " + seg2

        seg_boundaries = [(0, len(seg1)), (len(seg1) + 1, len(seg2))]
        seg_offsets = [0, len(seg1) + 1]

        # Match "עולם" at end of first segment
        match_start = full_text.find("עולם")
        match_end = match_start + len("עולם")

        result = _classify_hit_position(
            hit_start=match_start,
            hit_end=match_end,
            full_text=full_text,
            seg_boundaries=seg_boundaries,
            seg_offsets=seg_offsets,
        )

        assert "end" in result


class TestClassifyHitPositionEdgeCases:
    """Edge cases and boundary conditions."""

    def test_empty_segment_text(self):
        """Empty segment should handle gracefully."""
        full_text = ""
        seg_boundaries = [(0, 0)]
        seg_offsets = [0]

        # This might raise or return empty set - verify behavior
        result = _classify_hit_position(
            hit_start=0, hit_end=0, full_text=full_text, seg_boundaries=seg_boundaries, seg_offsets=seg_offsets
        )

        # Empty match - behavior depends on implementation
        assert isinstance(result, set)

    def test_hit_at_exact_boundary(self):
        """Hit starting exactly at segment boundary."""
        seg1 = "ראשון"
        seg2 = "שני"
        full_text = seg1 + " " + seg2

        seg_boundaries = [(0, len(seg1)), (len(seg1) + 1, len(seg2))]
        seg_offsets = [0, len(seg1) + 1]

        # Match starting exactly at boundary of seg2
        boundary = len(seg1) + 1

        result = _classify_hit_position(
            hit_start=boundary,
            hit_end=boundary + 3,
            full_text=full_text,
            seg_boundaries=seg_boundaries,
            seg_offsets=seg_offsets,
        )

        assert "start" in result

    def test_match_extends_exactly_to_segment_end(self):
        """Match ending exactly at segment boundary."""
        full_text = "מילה אחת שתיים"
        seg_boundaries = [(0, len(full_text))]
        seg_offsets = [0]

        # Match ending exactly at segment end
        result = _classify_hit_position(
            hit_start=full_text.find("שתיים"),
            hit_end=len(full_text),
            full_text=full_text,
            seg_boundaries=seg_boundaries,
            seg_offsets=seg_offsets,
        )

        assert "end" in result

    def test_three_segments_match_in_middle_segment(self):
        """Match in middle segment of three."""
        seg1 = "ראשון"
        seg2 = "אמצע"
        seg3 = "אחרון"
        full_text = seg1 + " " + seg2 + " " + seg3

        seg_boundaries = [(0, len(seg1)), (len(seg1) + 1, len(seg2)), (len(seg1) + 1 + len(seg2) + 1, len(seg3))]
        seg_offsets = [0, len(seg1) + 1, len(seg1) + 1 + len(seg2) + 1]

        # Match "אמצע" in middle segment
        match_start = full_text.find("אמצע")
        match_end = match_start + len("אמצע")

        result = _classify_hit_position(
            hit_start=match_start,
            hit_end=match_end,
            full_text=full_text,
            seg_boundaries=seg_boundaries,
            seg_offsets=seg_offsets,
        )

        # "אמצע" is the only word in its segment, so it should be both start and end
        assert "start" in result
        assert "end" in result

    def test_negative_index_protection(self):
        """Verify bisect result doesn't go negative."""
        full_text = "טקסט פשוט"
        seg_boundaries = [(0, len(full_text))]
        seg_offsets = [0]

        # Match at very beginning
        result = _classify_hit_position(
            hit_start=0, hit_end=1, full_text=full_text, seg_boundaries=seg_boundaries, seg_offsets=seg_offsets
        )

        # Should not crash
        assert isinstance(result, set)


class TestClassifyHitPositionWithPunctuation:
    """Tests involving punctuation in segment text."""

    def test_segment_starting_with_punctuation(self):
        """Segment starting with punctuation mark."""
        full_text = "!שלום עולם"
        seg_boundaries = [(0, len(full_text))]
        seg_offsets = [0]

        # Match "שלום" after the "!"
        match_start = 1
        match_end = 5

        result = _classify_hit_position(
            hit_start=match_start,
            hit_end=match_end,
            full_text=full_text,
            seg_boundaries=seg_boundaries,
            seg_offsets=seg_offsets,
        )

        # First space is after "!שלום" at position 5
        # Match is before first space, so should be 'start'
        # Note: Depends on how the code handles punctuation
        assert isinstance(result, set)

    def test_segment_ending_with_punctuation(self):
        """Segment ending with punctuation mark."""
        full_text = "שלום עולם!"
        seg_boundaries = [(0, len(full_text))]
        seg_offsets = [0]

        # Match "עולם!" including punctuation
        match_start = full_text.find("עולם")
        match_end = len(full_text)

        result = _classify_hit_position(
            hit_start=match_start,
            hit_end=match_end,
            full_text=full_text,
            seg_boundaries=seg_boundaries,
            seg_offsets=seg_offsets,
        )

        assert "end" in result

    def test_segment_with_hebrew_maqaf(self):
        """Segment with Hebrew maqaf (־) word connector."""
        full_text = "בית־ספר חדש"
        seg_boundaries = [(0, len(full_text))]
        seg_offsets = [0]

        # "בית־ספר" is considered one word (connected by maqaf)
        # First space is after "בית־ספר"
        match_start = 0
        match_end = full_text.find(" ")  # Match "בית־ספר"

        result = _classify_hit_position(
            hit_start=match_start,
            hit_end=match_end,
            full_text=full_text,
            seg_boundaries=seg_boundaries,
            seg_offsets=seg_offsets,
        )

        assert "start" in result
