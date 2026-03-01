"""Move transcribed radio segments into the explore data directory."""
from __future__ import annotations

import logging
import os
import shutil
from pathlib import Path

from .config import RadioConfig

log = logging.getLogger(__name__)


def ingest(config: RadioConfig, data_dir: str | Path) -> int:
    """Move opus + transcript pairs from split_dir into the explore data directory.

    Expected input layout:
        {split_dir}/{station_key}/{YYYY.MM.DD HHMM title}.opus
        {split_dir}/{station_key}/{YYYY.MM.DD HHMM title}.json.gz

    Output layout (compatible with get_transcripts):
        {data_dir}/json/radio_{station_key}/{YYYY.MM.DD HHMM title}/full_transcript.json.gz
        {data_dir}/audio/radio_{station_key}/{YYYY.MM.DD HHMM title}.opus

    Returns number of episodes ingested.
    """
    data_dir = Path(data_dir)
    json_dir = data_dir / "json"
    audio_dir = data_dir / "audio"
    count = 0

    for station_dir in config.split_dir.iterdir():
        if not station_dir.is_dir():
            continue
        station_key = station_dir.name
        source = f"radio_{station_key}"

        for opus_path in station_dir.glob("*.opus"):
            transcript_path = opus_path.with_suffix(".json.gz")
            if not transcript_path.exists():
                continue  # no transcript yet, skip

            episode_id = opus_path.stem  # "YYYY.MM.DD HHMM title"

            # Transcript destination
            json_dest_dir = json_dir / source / episode_id
            json_dest_dir.mkdir(parents=True, exist_ok=True)
            json_dest = json_dest_dir / "full_transcript.json.gz"

            # Audio destination
            audio_dest_dir = audio_dir / source
            audio_dest_dir.mkdir(parents=True, exist_ok=True)
            audio_dest = audio_dest_dir / f"{episode_id}.opus"

            # Move files (overwrite if exists)
            shutil.move(str(transcript_path), str(json_dest))
            shutil.move(str(opus_path), str(audio_dest))
            count += 1
            log.info("Ingested: %s/%s", source, episode_id)

    log.info("Ingested %d episodes total", count)
    return count
