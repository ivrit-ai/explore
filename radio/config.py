from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

import yaml


@dataclass
class StationConfig:
    key: str
    name: str
    url: str
    schedule_scraper: Optional[str] = None
    schedule_id: Optional[str] = None


@dataclass
class RadioConfig:
    stations: dict[str, StationConfig] = field(default_factory=dict)
    duration: int = 3600
    raw_dir: Path = Path("data/radio/raw")
    split_dir: Path = Path("data/radio/split")
    opus_bitrate: str = "48k"
    opus_channels: int = 1
    opus_sample_rate: int = 16000
    silence_thresh: int = -35
    silence_min_dur: float = 1.5
    min_segment_dur: int = 300
    fallback_chunk_dur: int = 1800


def load_config(path: Optional[str | Path] = None) -> RadioConfig:
    """Load radio config from YAML file.

    Looks for the config file in this order:
    1. Explicit path argument
    2. RADIO_CONFIG env var
    3. radio/stations.yaml next to this file
    """
    if path is None:
        path = os.environ.get("RADIO_CONFIG")
    if path is None:
        path = Path(__file__).parent / "stations.yaml"

    path = Path(path)
    with open(path) as f:
        raw = yaml.safe_load(f)

    defaults = raw.get("defaults", {})
    cfg = RadioConfig(
        duration=defaults.get("duration", 3600),
        raw_dir=Path(defaults.get("raw_dir", "/data/radio/raw")),
        split_dir=Path(defaults.get("split_dir", "/data/radio/split")),
        opus_bitrate=str(defaults.get("opus_bitrate", "48k")),
        opus_channels=defaults.get("opus_channels", 1),
        opus_sample_rate=defaults.get("opus_sample_rate", 16000),
        silence_thresh=defaults.get("silence_thresh", -35),
        silence_min_dur=defaults.get("silence_min_dur", 1.5),
        min_segment_dur=defaults.get("min_segment_dur", 300),
        fallback_chunk_dur=defaults.get("fallback_chunk_dur", 1800),
    )

    for key, station_raw in raw.get("stations", {}).items():
        cfg.stations[key] = StationConfig(
            key=key,
            name=station_raw["name"],
            url=station_raw["url"],
            schedule_scraper=station_raw.get("schedule_scraper"),
            schedule_id=station_raw.get("schedule_id"),
        )

    return cfg
