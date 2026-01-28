"""
Shared pytest fixtures for search logic tests.

Provides mock transcripts, in-memory database setup, and common test utilities.
"""
import pytest
import tempfile
import os
from pathlib import Path
from typing import List, Tuple
from dataclasses import dataclass

# Import the actual modules we're testing
from app.services.db import DatabaseService
from app.services.index import (
    TranscriptIndex,
    _classify_hit_position,
    _episode_to_string_and_segments,
    _setup_schema,
)


# ============================================================================
# MOCK DATA STRUCTURES
# ============================================================================

@dataclass
class MockSegment:
    """Mock segment for test data."""
    start: float
    end: float
    text: str
    avg_logprob: float = -0.2


@dataclass
class MockTranscript:
    """Mock transcript with metadata and segments."""
    source: str
    episode: str
    episode_title: str
    episode_date: str
    segments: List[MockSegment]

    def to_dict(self) -> dict:
        """Convert to JSON-compatible dict."""
        return {
            "metadata": {
                "source": self.source,
                "episode": self.episode,
                "episode_title": self.episode_title,
                "episode_date": self.episode_date,
            },
            "segments": [
                {
                    "start": s.start,
                    "end": s.end,
                    "text": s.text,
                    "avg_logprob": s.avg_logprob,
                }
                for s in self.segments
            ],
        }


# ============================================================================
# HEBREW TEST DATA
# ============================================================================

# Basic Hebrew transcript for simple tests
BASIC_HEBREW_TRANSCRIPT = MockTranscript(
    source="test-podcast",
    episode="test-podcast/2024.01.15 Test Episode",
    episode_title="Test Episode",
    episode_date="2024-01-15",
    segments=[
        MockSegment(0.0, 3.0, "שלום וברוכים הבאים"),
        MockSegment(3.0, 6.0, "לפודקאסט שלנו היום"),
        MockSegment(6.0, 9.0, "נדבר על נושאים מעניינים"),
    ],
)

# Transcript with punctuation variations
PUNCTUATION_TRANSCRIPT = MockTranscript(
    source="punct-test",
    episode="punct-test/2024.02.01 Punctuation Test",
    episode_title="Punctuation Test",
    episode_date="2024-02-01",
    segments=[
        MockSegment(0.0, 3.0, "שלום! מה קורה?"),
        MockSegment(3.0, 6.0, "הכל בסדר, תודה."),
        MockSegment(6.0, 9.0, "בית־ספר חדש"),  # Hebrew maqaf
        MockSegment(9.0, 12.0, "צה״ל ומשטרה"),  # Gershayim
        MockSegment(12.0, 15.0, "ג׳ון וג׳ני"),  # Geresh
    ],
)

# Transcript for position filter tests (designed with known boundaries)
POSITION_TRANSCRIPT = MockTranscript(
    source="position-test",
    episode="position-test/2024.03.01 Position Test",
    episode_title="Position Test",
    episode_date="2024-03-01",
    segments=[
        MockSegment(0.0, 3.0, "מילה ראשונה באמצע אחרונה"),
        MockSegment(3.0, 6.0, "התחלה חדשה כאן סיום"),
        MockSegment(6.0, 9.0, "בודד"),  # Single word segment
        MockSegment(9.0, 12.0, "המשך הטקסט הזה"),
    ],
)

# Transcript for cross-segment matching
CROSS_SEGMENT_TRANSCRIPT = MockTranscript(
    source="cross-test",
    episode="cross-test/2024.04.01 Cross Test",
    episode_title="Cross Test",
    episode_date="2024-04-01",
    segments=[
        MockSegment(0.0, 3.0, "זה טקסט שנמשך"),
        MockSegment(3.0, 6.0, "אל הסגמנט הבא"),
        MockSegment(6.0, 9.0, "וממשיך עוד"),
    ],
)

# Mixed Hebrew/English transcript
MIXED_TRANSCRIPT = MockTranscript(
    source="mixed-test",
    episode="mixed-test/2024.05.01 Mixed Test",
    episode_title="Mixed Test",
    episode_date="2024-05-01",
    segments=[
        MockSegment(0.0, 3.0, "Flask הוא framework מעולה"),
        MockSegment(3.0, 6.0, "Python זה שפת תכנות"),
        MockSegment(6.0, 9.0, "API endpoint חדש"),
    ],
)


# ============================================================================
# DATABASE FIXTURES
# ============================================================================

@pytest.fixture
def temp_db_path(tmp_path):
    """Provide a temporary database path."""
    return str(tmp_path / "test_explore.sqlite")


@pytest.fixture
def in_memory_db():
    """Create an in-memory DatabaseService."""
    db = DatabaseService(path=":memory:")
    _setup_schema(db)
    yield db
    db.close()


@pytest.fixture
def transcript_index(in_memory_db) -> TranscriptIndex:
    """Create a TranscriptIndex with in-memory database."""
    return TranscriptIndex(in_memory_db)


# ============================================================================
# DATA LOADING HELPERS
# ============================================================================

def load_transcript_to_db(db: DatabaseService, transcript: MockTranscript, doc_id: int) -> Tuple[str, List[dict]]:
    """
    Load a mock transcript into the database.

    Returns:
        Tuple of (full_text, segments_data)
    """
    import uuid as uuid_module

    data = transcript.to_dict()
    full_text, segments_data = _episode_to_string_and_segments(data)

    # Insert document
    doc_uuid = str(uuid_module.uuid4())
    db.execute(
        """INSERT INTO documents
           (doc_id, uuid, source, episode, episode_date, episode_title)
           VALUES (?, ?, ?, ?, ?, ?)""",
        [doc_id, doc_uuid, transcript.source, transcript.episode,
         transcript.episode_date, transcript.episode_title]
    )

    # Insert into FTS5
    cursor = db.execute(
        "INSERT INTO documents_fts(full_text) VALUES (?)",
        [full_text]
    )
    fts_rowid = cursor.lastrowid

    # Insert FTS mapping
    db.execute(
        "INSERT INTO fts_doc_mapping(fts_rowid, doc_id) VALUES (?, ?)",
        [fts_rowid, doc_id]
    )

    # Insert segments
    for seg_idx, seg in enumerate(segments_data):
        db.execute(
            """INSERT INTO segments
               (doc_id, segment_id, segment_text, avg_logprob, char_offset, start_time, end_time)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            [doc_id, seg_idx, seg["text"], seg["avg_logprob"],
             seg["char_offset"], seg["start"], seg["end"]]
        )

    db.commit()
    return full_text, segments_data


@pytest.fixture
def basic_index(in_memory_db) -> Tuple[TranscriptIndex, str, List[dict]]:
    """
    Create index with basic Hebrew transcript.

    Returns:
        Tuple of (index, full_text, segments_data)
    """
    full_text, segments_data = load_transcript_to_db(in_memory_db, BASIC_HEBREW_TRANSCRIPT, 0)
    index = TranscriptIndex(in_memory_db)
    return index, full_text, segments_data


@pytest.fixture
def punctuation_index(in_memory_db) -> Tuple[TranscriptIndex, str, List[dict]]:
    """Create index with punctuation test transcript."""
    full_text, segments_data = load_transcript_to_db(in_memory_db, PUNCTUATION_TRANSCRIPT, 0)
    index = TranscriptIndex(in_memory_db)
    return index, full_text, segments_data


@pytest.fixture
def position_index(in_memory_db) -> Tuple[TranscriptIndex, str, List[dict]]:
    """Create index with position test transcript."""
    full_text, segments_data = load_transcript_to_db(in_memory_db, POSITION_TRANSCRIPT, 0)
    index = TranscriptIndex(in_memory_db)
    return index, full_text, segments_data


@pytest.fixture
def multi_transcript_index(in_memory_db) -> TranscriptIndex:
    """Create index with multiple transcripts for filter testing."""
    transcripts = [
        BASIC_HEBREW_TRANSCRIPT,
        PUNCTUATION_TRANSCRIPT,
        POSITION_TRANSCRIPT,
        MIXED_TRANSCRIPT,
    ]

    for doc_id, transcript in enumerate(transcripts):
        load_transcript_to_db(in_memory_db, transcript, doc_id)

    return TranscriptIndex(in_memory_db)


# ============================================================================
# HELPER FUNCTIONS FOR TESTS
# ============================================================================

def get_segment_boundaries(segments_data: List[dict]) -> Tuple[List[Tuple[int, int]], List[int]]:
    """
    Extract segment boundaries from segments_data.

    Returns:
        Tuple of (boundaries list, offsets list)
        - boundaries: List of (char_offset, segment_length)
        - offsets: List of just char_offsets for bisect
    """
    boundaries = [(s["char_offset"], len(s["text"])) for s in segments_data]
    offsets = [s["char_offset"] for s in segments_data]
    return boundaries, offsets


def find_substring_offset(full_text: str, substring: str) -> int:
    """Find the character offset of a substring in full_text."""
    offset = full_text.find(substring)
    if offset == -1:
        raise ValueError(f"Substring '{substring}' not found in text")
    return offset
