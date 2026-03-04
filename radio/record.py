"""Record live radio streams continuously using ffmpeg segment muxer.

Produces 1-hour opus chunks aligned to clock hours (8:00, 9:00, …).
The stream is transcoded to opus once during recording so that splitting
is a simple copy (no re-encoding).  When ffmpeg closes a completed chunk
and moves to the next hour, the completed chunk is split into per-program
segments in a background thread.
"""
from __future__ import annotations

import logging
import os
import signal
import subprocess
import threading
from pathlib import Path

from .config import RadioConfig, StationConfig

# All timestamps (filenames, schedules) use Israel time
_IL_ENV = {**os.environ, "TZ": "Asia/Jerusalem"}

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
    segments_seen = 0
    for line in proc.stdout:
        name = line.strip()
        if not name:
            continue
        segments_seen += 1
        mp3_path = out_dir / name
        if mp3_path.exists():
            _process_completed_chunk(mp3_path, station, config, split_threads)

    # ffmpeg stdout closed — check for early failure (e.g. geo-blocked stream)
    rc = proc.wait()
    if rc != 0 and segments_seen == 0:
        log.warning(
            "ffmpeg exited with code %d for %s before producing any segments — "
            "stream may be geo-restricted or unavailable",
            rc, station.key,
        )


def record_station(
    station: StationConfig,
    config: RadioConfig,
) -> subprocess.Popen:
    """Start recording a station continuously, producing hour-aligned MP3 chunks.

    Each completed hourly chunk is automatically split into per-program
    segments in a background thread while recording continues.

    Returns the ffmpeg Popen handle (caller manages lifecycle).
    """
    out_dir = config.raw_dir / station.key
    out_dir.mkdir(parents=True, exist_ok=True)

    pattern = str(out_dir / "%Y-%m-%d_%H%M%S.opus")

    common_output_args = [
        "-acodec", "libopus",
        "-b:a", config.opus_bitrate,
        "-ac", str(config.opus_channels),
        "-ar", str(config.opus_sample_rate),
        "-f", "segment",
        "-segment_format", "ogg",
        "-segment_time", str(CHUNK_DURATION),
        "-segment_atclocktime", "1",
        "-strftime", "1",
        "-segment_list", "pipe:1",
        "-reset_timestamps", "1",
        "-v", "warning",
        pattern,
    ]

    if station.is_tv:
        # HLS TV stream: strip video, encode audio to opus segments
        cmd = ["ffmpeg", "-y", "-i", station.url, "-vn"] + common_output_args
    else:
        cmd = [
            "ffmpeg", "-y",
            "-reconnect", "1",
            "-reconnect_streamed", "1",
            "-reconnect_delay_max", "30",
            "-i", station.url,
        ] + common_output_args

    log.info(
        "Recording %s continuously (1h chunks, hour-aligned, until stopped) → %s",
        station.key, out_dir,
    )

    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, text=True, env=_IL_ENV)
    split_threads: list[threading.Thread] = []

    reader = threading.Thread(
        target=_segment_list_reader, args=(proc, station, config, out_dir, split_threads), daemon=True,
    )
    reader.start()

    # Store references on the proc so the caller can join them on shutdown
    proc._reader_thread = reader          # type: ignore[attr-defined]
    proc._split_threads = split_threads   # type: ignore[attr-defined]
    proc._station_key = station.key       # type: ignore[attr-defined]

    return proc


def record_stations(
    stations: list[StationConfig],
    config: RadioConfig,
) -> None:
    """Record multiple stations in parallel, each in its own thread.

    All ffmpeg processes and their split threads release the GIL during
    I/O (subprocess wait / pipe reads), so they run truly in parallel.

    Blocks until interrupted (Ctrl-C), then shuts down all processes.
    """
    procs: list[subprocess.Popen] = []

    for station in stations:
        proc = record_station(station, config)
        procs.append(proc)

    log.info("Recording %d station(s) — press Ctrl-C to stop", len(procs))

    # Block until interrupted
    try:
        for proc in procs:
            proc.wait()
    except KeyboardInterrupt:
        log.info("Stopping all recordings...")

    # Signal all ffmpeg processes to stop
    for proc in procs:
        if proc.poll() is None:
            proc.send_signal(signal.SIGINT)
    for proc in procs:
        try:
            proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait()

    # Wait for reader and split threads to finish
    for proc in procs:
        proc._reader_thread.join(timeout=5)   # type: ignore[attr-defined]
        for t in proc._split_threads:          # type: ignore[attr-defined]
            t.join()

    for proc in procs:
        key = proc._station_key               # type: ignore[attr-defined]
        out_dir = config.raw_dir / key
        chunks = sorted(out_dir.glob("*.opus"))
        log.info("Recording finished for %s: %d remaining chunks", key, len(chunks))
