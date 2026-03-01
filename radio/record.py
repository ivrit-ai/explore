"""Record live radio streams using ffmpeg."""
from __future__ import annotations

import json
import logging
import subprocess
from datetime import datetime
from pathlib import Path
from typing import Optional

from .config import RadioConfig, StationConfig
from .schedule import get_scraper

log = logging.getLogger(__name__)

MIN_FILE_SIZE = 100 * 1024  # 100 KB — discard tiny/empty files


def _collect_metadata(
    station: StationConfig,
    config: RadioConfig,
    out_path: Path,
    duration: int,
    start_time: datetime,
) -> dict:
    """Collect recording metadata: station info, timestamps, schedule, stream info."""
    meta: dict = {
        "station_key": station.key,
        "station_name": station.name,
        "stream_url": station.url,
        "start_time": start_time.isoformat(),
        "requested_duration": duration,
    }

    # Probe actual duration and stream info from the recorded file
    if out_path.exists():
        meta["file_size"] = out_path.stat().st_size
        try:
            probe = subprocess.run(
                ["ffprobe", "-v", "quiet", "-print_format", "json",
                 "-show_format", "-show_streams", str(out_path)],
                capture_output=True, text=True, timeout=15,
            )
            info = json.loads(probe.stdout)
            fmt = info.get("format", {})
            meta["actual_duration"] = float(fmt.get("duration", 0))
            meta["format_name"] = fmt.get("format_name")
            meta["bitrate"] = int(fmt.get("bit_rate", 0))
            # Icecast metadata (title, genre, etc.) ends up in format tags
            tags = fmt.get("tags", {})
            if tags:
                meta["stream_tags"] = tags
        except Exception:
            pass

    # Fetch schedule for the recording window
    scraper = get_scraper(station.schedule_scraper, schedule_id=station.schedule_id or "")
    if scraper:
        try:
            schedule = scraper.get_schedule(start_time.date())
            if schedule:
                meta["schedule"] = [
                    {"start": s.start.isoformat(), "end": s.end.isoformat(),
                     "title": s.title}
                    for s in schedule
                ]
        except Exception:
            log.debug("Failed to fetch schedule for metadata", exc_info=True)

    return meta


def record_station(
    station: StationConfig,
    config: RadioConfig,
    duration: Optional[int] = None,
) -> Optional[Path]:
    """Record a radio station stream for the given duration.

    Saves a metadata JSON sidecar alongside the MP3.
    Returns the path to the recorded MP3 file, or None on failure.
    """
    duration = duration or config.duration
    out_dir = config.raw_dir / station.key
    out_dir.mkdir(parents=True, exist_ok=True)

    start_time = datetime.now()
    timestamp = start_time.strftime("%Y-%m-%d_%H%M%S")
    out_path = out_dir / f"{timestamp}.mp3"

    cmd = [
        "ffmpeg", "-y",
        "-reconnect", "1",
        "-reconnect_streamed", "1",
        "-reconnect_delay_max", "30",
        "-i", station.url,
        "-t", str(duration),
        "-acodec", "copy",
        "-v", "warning",
        str(out_path),
    ]

    log.info("Recording %s for %ds → %s", station.key, duration, out_path)
    try:
        subprocess.run(cmd, check=True, timeout=duration + 120)
    except subprocess.TimeoutExpired:
        log.warning("ffmpeg timed out for %s — keeping partial file", station.key)
    except subprocess.CalledProcessError as exc:
        log.error("ffmpeg failed for %s (rc=%d)", station.key, exc.returncode)

    # Keep partial recordings if they're large enough
    if out_path.exists():
        size = out_path.stat().st_size
        if size >= MIN_FILE_SIZE:
            log.info("Recorded %s (%d KB)", out_path.name, size // 1024)
            # Save metadata sidecar
            meta = _collect_metadata(station, config, out_path, duration, start_time)
            meta_path = out_path.with_suffix(".json")
            meta_path.write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")
            log.info("Saved metadata: %s", meta_path.name)
            return out_path
        else:
            log.warning("Discarding tiny file %s (%d bytes)", out_path, size)
            out_path.unlink()

    return None
