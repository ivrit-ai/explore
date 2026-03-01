"""Split raw radio recordings into per-program segments."""
from __future__ import annotations

import logging
import re
import subprocess
from datetime import date, datetime, time, timedelta
from pathlib import Path
from typing import Optional

from .config import RadioConfig, StationConfig
from .schedule import get_scraper, ProgramSlot

log = logging.getLogger(__name__)


def detect_silence(path: Path, thresh: int = -35, min_dur: float = 1.5) -> list[tuple[float, float]]:
    """Run ffmpeg silencedetect and return list of (start, end) silence intervals."""
    cmd = [
        "ffmpeg", "-i", str(path),
        "-af", f"silencedetect=noise={thresh}dB:d={min_dur}",
        "-f", "null", "-",
    ]
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=600)
    stderr = result.stderr

    silences: list[tuple[float, float]] = []
    starts: list[float] = []

    for line in stderr.splitlines():
        m_start = re.search(r"silence_start:\s*([\d.]+)", line)
        m_end = re.search(r"silence_end:\s*([\d.]+)", line)
        if m_start:
            starts.append(float(m_start.group(1)))
        elif m_end and starts:
            silences.append((starts.pop(), float(m_end.group(1))))

    return silences


def _get_duration(path: Path) -> float:
    """Get audio file duration in seconds using ffprobe."""
    cmd = [
        "ffprobe", "-v", "quiet", "-show_entries", "format=duration",
        "-of", "default=noprint_wrappers=1:nokey=1", str(path),
    ]
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
    return float(result.stdout.strip())


def _nearest_silence(target: float, silences: list[tuple[float, float]], tolerance: float = 120) -> Optional[float]:
    """Find the silence midpoint nearest to target time within tolerance."""
    best: Optional[float] = None
    best_dist = tolerance + 1
    for s_start, s_end in silences:
        mid = (s_start + s_end) / 2
        dist = abs(mid - target)
        if dist < best_dist:
            best = mid
            best_dist = dist
    return best


def _recording_start_time(raw_path: Path) -> datetime:
    """Extract recording start datetime from filename (YYYY-MM-DD_HHMMSS.mp3)."""
    stem = raw_path.stem  # e.g. "2026-02-27_140000"
    return datetime.strptime(stem, "%Y-%m-%d_%H%M%S")


def _schedule_to_boundaries(
    schedule: list[ProgramSlot],
    rec_start: datetime,
    duration: float,
) -> list[tuple[float, float, str]]:
    """Convert schedule slots to (offset_start, offset_end, title) relative to recording start."""
    rec_start_secs = rec_start.hour * 3600 + rec_start.minute * 60 + rec_start.second
    rec_end_secs = rec_start_secs + duration
    segments: list[tuple[float, float, str]] = []

    for slot in schedule:
        slot_start = slot.start_seconds()
        slot_end = slot.end_seconds()
        # Handle overnight wrap
        if slot_end <= slot_start:
            slot_end += 86400

        # Convert to offsets relative to recording start
        offset_start = slot_start - rec_start_secs
        offset_end = slot_end - rec_start_secs

        # Skip slots entirely outside the recording window
        if offset_end <= 0 or offset_start >= duration:
            continue

        # Clamp to recording bounds
        offset_start = max(0, offset_start)
        offset_end = min(duration, offset_end)

        if offset_end - offset_start >= 60:  # at least 1 min
            segments.append((offset_start, offset_end, slot.title))

    return segments


def _fixed_chunks(duration: float, chunk_dur: float, rec_start: datetime) -> list[tuple[float, float, str]]:
    """Fall back to fixed-duration chunks with timestamps as titles."""
    segments: list[tuple[float, float, str]] = []
    offset = 0.0
    while offset < duration:
        end = min(offset + chunk_dur, duration)
        chunk_time = rec_start + timedelta(seconds=offset)
        title = chunk_time.strftime("%H%M")
        segments.append((offset, end, title))
        offset = end
    return segments


def _sanitize_filename(s: str) -> str:
    """Remove characters unsafe for filenames."""
    s = re.sub(r'[/\\:*?"<>|]', '', s)
    return s.strip()


def split_recording(
    raw_path: Path,
    station: StationConfig,
    config: RadioConfig,
) -> list[Path]:
    """Split a raw recording into per-program opus files.

    Returns list of output paths.
    """
    duration = _get_duration(raw_path)
    rec_start = _recording_start_time(raw_path)
    rec_date = rec_start.date()

    # Try to get schedule
    scraper = get_scraper(station.schedule_scraper, schedule_id=station.schedule_id or "")
    schedule: list[ProgramSlot] = []
    if scraper:
        schedule = scraper.get_schedule(rec_date)

    # Get silence points for snapping boundaries
    silences = detect_silence(raw_path, config.silence_thresh, config.silence_min_dur)

    # Determine segment boundaries
    if schedule:
        raw_segments = _schedule_to_boundaries(schedule, rec_start, duration)
    else:
        raw_segments = _fixed_chunks(duration, config.fallback_chunk_dur, rec_start)

    if not raw_segments:
        raw_segments = _fixed_chunks(duration, config.fallback_chunk_dur, rec_start)

    # Snap boundaries to silence points
    segments: list[tuple[float, float, str]] = []
    for start, end, title in raw_segments:
        if start > 0:
            snapped = _nearest_silence(start, silences)
            if snapped is not None:
                start = snapped
        if end < duration:
            snapped = _nearest_silence(end, silences)
            if snapped is not None:
                end = snapped
        if end - start >= config.min_segment_dur:
            segments.append((start, end, title))

    # If snapping collapsed everything, use raw boundaries
    if not segments:
        segments = raw_segments

    # Output directory
    out_dir = config.split_dir / station.key
    out_dir.mkdir(parents=True, exist_ok=True)

    outputs: list[Path] = []
    for start, end, title in segments:
        seg_time = rec_start + timedelta(seconds=start)
        safe_title = _sanitize_filename(title)
        # Format: "YYYY.MM.DD HHMM title.opus" — compatible with split_episode regex
        fname = f"{rec_date.strftime('%Y.%m.%d')} {seg_time.strftime('%H%M')} {safe_title}.opus"
        out_path = out_dir / fname

        cmd = [
            "ffmpeg", "-y",
            "-i", str(raw_path),
            "-ss", f"{start:.2f}",
            "-to", f"{end:.2f}",
            "-acodec", "libopus",
            "-b:a", config.opus_bitrate,
            "-ac", str(config.opus_channels),
            "-ar", str(config.opus_sample_rate),
            "-v", "warning",
            str(out_path),
        ]

        try:
            subprocess.run(cmd, check=True, timeout=300)
            outputs.append(out_path)
            log.info("Created segment: %s (%.0fs)", out_path.name, end - start)
        except (subprocess.CalledProcessError, subprocess.TimeoutExpired):
            log.error("Failed to create segment: %s", out_path.name)

    return outputs
