"""CLI orchestrator for the radio scraping pipeline.

Usage:
    python -m radio.pipeline record [--station X] [--duration N]
    python -m radio.pipeline split [--station X]
    python -m radio.pipeline ingest --data-dir DIR
    python -m radio.pipeline run [--station X] --data-dir DIR
"""
from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

from .config import load_config
from .record import record_station
from .split import split_recording
from .ingest import ingest

log = logging.getLogger(__name__)


def cmd_record(args, config):
    stations = _resolve_stations(args.station, config)
    for station in stations:
        result = record_station(station, config, duration=args.duration)
        if result:
            print(f"Recorded: {result}")
        else:
            print(f"Recording failed: {station.key}", file=sys.stderr)


def cmd_split(args, config):
    stations = _resolve_stations(args.station, config)
    for station in stations:
        raw_dir = config.raw_dir / station.key
        if not raw_dir.exists():
            log.warning("No raw directory for %s", station.key)
            continue
        for raw_file in sorted(raw_dir.glob("*.mp3")):
            outputs = split_recording(raw_file, station, config)
            for out in outputs:
                print(f"Split: {out}")
            # Remove raw file after successful split
            if outputs:
                raw_file.unlink()
                log.info("Removed raw file: %s", raw_file)


def cmd_ingest(args, config):
    if not args.data_dir:
        print("--data-dir is required for ingest", file=sys.stderr)
        sys.exit(1)
    count = ingest(config, args.data_dir)
    print(f"Ingested {count} episodes")


def cmd_run(args, config):
    """Record + split (full pipeline minus transcription and ingest)."""
    cmd_record(args, config)
    cmd_split(args, config)


def _resolve_stations(station_key, config):
    if station_key:
        if station_key not in config.stations:
            print(f"Unknown station: {station_key}", file=sys.stderr)
            print(f"Available: {', '.join(config.stations.keys())}", file=sys.stderr)
            sys.exit(1)
        return [config.stations[station_key]]
    return list(config.stations.values())


def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    parser = argparse.ArgumentParser(description="Radio scraping pipeline")
    parser.add_argument("--config", help="Path to stations.yaml config file")
    sub = parser.add_subparsers(dest="command", required=True)

    # record
    p_rec = sub.add_parser("record", help="Record radio streams")
    p_rec.add_argument("--station", help="Station key (default: all stations)")
    p_rec.add_argument("--duration", type=int, help="Recording duration in seconds")

    # split
    p_split = sub.add_parser("split", help="Split raw recordings into segments")
    p_split.add_argument("--station", help="Station key (default: all stations)")

    # ingest
    p_ingest = sub.add_parser("ingest", help="Move transcribed segments into explore data dir")
    p_ingest.add_argument("--data-dir", required=True, help="Explore data directory")

    # run (record + split)
    p_run = sub.add_parser("run", help="Record + split (full pipeline)")
    p_run.add_argument("--station", help="Station key (default: all stations)")
    p_run.add_argument("--duration", type=int, help="Recording duration in seconds")
    p_run.add_argument("--data-dir", help="Explore data directory (for ingest)")

    args = parser.parse_args()
    config = load_config(args.config)

    commands = {
        "record": cmd_record,
        "split": cmd_split,
        "ingest": cmd_ingest,
        "run": cmd_run,
    }
    commands[args.command](args, config)


if __name__ == "__main__":
    main()
