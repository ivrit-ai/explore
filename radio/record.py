"""Record live radio streams continuously using ffmpeg segment muxer.

Produces 1-hour MP3 chunks aligned to clock hours (8:00, 9:00, …).
When ffmpeg closes a completed chunk and moves to the next hour, the
completed chunk is split into per-program segments in a background thread.
"""
from __future__ import annotations

import logging
import signal
import subprocess
import threading
from pathlib import Path

from .config import RadioConfig, StationConfig

log = logging.getLogger(__name__)

MIN_FILE_SIZE = 100 * 1024  # 100 KB — discard tiny/empty files
CHUNK_DURATION = 3600       # 1 hour per chunk


def _split_chunk(mp3_path: Path, station: StationConfig, config: RadioConfig):
    """Split a completed MP3 chunk into program segments (runs in background thread)."""
    from .split import split_recording

    try:
        outputs = split_recording(mp3_path, station, config)
        if outputs:
            log.info("Split into %d segments, removing raw chunk %s", len(outputs), mp3_path.name)
            mp3_path.unlink()
        else:
            log.warning("Split produced no output for %s — keeping raw chunk", mp3_path.name)
    except Exception:
        log.exception("Split failed for %s — keeping raw chunk", mp3_path.name)


def _process_completed_chunk(mp3_path: Path, station: StationConfig, config: RadioConfig, split_threads: list[threading.Thread]):
    """Dispatch split of a completed chunk to a background thread."""
    size = mp3_path.stat().st_size
    if size < MIN_FILE_SIZE:
        log.warning("Discarding tiny chunk %s (%d bytes)", mp3_path.name, size)
        mp3_path.unlink()
        return
    log.info("Completed chunk: %s (%d KB) — dispatching split", mp3_path.name, size // 1024)
    t = threading.Thread(target=_split_chunk, args=(mp3_path, station, config), daemon=True)
    t.start()
    split_threads.append(t)


def _segment_list_reader(proc: subprocess.Popen, station: StationConfig, config: RadioConfig, out_dir: Path, split_threads: list[threading.Thread]):
    """Read completed segment filenames from ffmpeg stdout and dispatch splitting."""
    for line in proc.stdout:
        name = line.strip()
        if not name:
            continue
        mp3_path = out_dir / name
        if mp3_path.exists():
            _process_completed_chunk(mp3_path, station, config, split_threads)


def record_station(
    station: StationConfig,
    config: RadioConfig,
) -> list[Path]:
    """Record a station continuously, producing hour-aligned MP3 chunks.

    Each completed hourly chunk is automatically split into per-program
    segments in a background thread while recording continues.
    Runs until interrupted (Ctrl-C).

    Returns list of remaining raw chunk paths (if any).
    """
    out_dir = config.raw_dir / station.key
    out_dir.mkdir(parents=True, exist_ok=True)

    pattern = str(out_dir / "%Y-%m-%d_%H%M%S.mp3")

    cmd = [
        "ffmpeg", "-y",
        "-reconnect", "1",
        "-reconnect_streamed", "1",
        "-reconnect_delay_max", "30",
        "-i", station.url,
        "-acodec", "copy",
        "-f", "segment",
        "-segment_time", str(CHUNK_DURATION),
        "-segment_atclocktime", "1",
        "-strftime", "1",
        "-segment_list", "pipe:1",
        "-reset_timestamps", "1",
        "-v", "warning",
        pattern,
    ]

    log.info(
        "Recording %s continuously (1h chunks, hour-aligned, until stopped) → %s",
        station.key, out_dir,
    )

    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, text=True)
    split_threads: list[threading.Thread] = []

    reader = threading.Thread(
        target=_segment_list_reader, args=(proc, station, config, out_dir, split_threads), daemon=True,
    )
    reader.start()

    try:
        proc.wait()
    except KeyboardInterrupt:
        log.info("Stopping recording for %s (interrupted)", station.key)
        proc.send_signal(signal.SIGINT)
        try:
            proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait()

    reader.join(timeout=5)

    # Wait for all in-flight split threads to finish before returning
    for t in split_threads:
        t.join()

    chunks = sorted(out_dir.glob("*.mp3"))
    log.info("Recording session finished: %d remaining chunks for %s", len(chunks), station.key)
    return list(chunks)
