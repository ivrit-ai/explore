"""CLI orchestrator for the radio scraping pipeline.

Usage:
    python -m radio.pipeline record [--station X [--station Y ...]]
    python -m radio.pipeline split [--station X [--station Y ...]]
    python -m radio.pipeline ingest --data-dir DIR
"""
from __future__ import annotations

import argparse
import logging
import sys

# Ensure stdout handles Unicode (needed on Windows with Hebrew filenames)
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

from .config import load_config
from .record import record_stations
from .split import split_recording
from .ingest import ingest

log = logging.getLogger(__name__)


def cmd_record(args, config):
    stations = _resolve_stations(args.station, config)
    record_stations(stations, config)


def cmd_split(args, config):
    stations = _resolve_stations(args.station, config)
    for station in stations:
        raw_dir = config.raw_dir / station.key
        if not raw_dir.exists():
            log.warning("No raw directory for %s", station.key)
            continue
        for raw_file in sorted(raw_dir.glob("*.opus")):
            outputs = split_recording(raw_file, station, config)
            for out in outputs:
                print(f"Split: {out}")
            if outputs:
                raw_file.unlink()
                log.info("Removed raw file: %s", raw_file.name)


def cmd_ingest(args, config):
    if not args.data_dir:
        print("--data-dir is required for ingest", file=sys.stderr)
        sys.exit(1)
    count = ingest(config, args.data_dir)
    print(f"Ingested {count} episodes")


def _resolve_stations(station_keys, config):
    if station_keys:
        stations = []
        for key in station_keys:
            if key not in config.stations:
                print(f"Unknown station: {key}", file=sys.stderr)
                print(f"Available: {', '.join(config.stations.keys())}", file=sys.stderr)
                sys.exit(1)
            stations.append(config.stations[key])
        return stations
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
    p_rec = sub.add_parser("record", help="Record radio streams (continuous, 1h chunks)")
    p_rec.add_argument("--station", action="append", help="Station key (repeatable; default: all stations)")

    # split
    p_split = sub.add_parser("split", help="Split raw recordings into program segments")
    p_split.add_argument("--station", action="append", help="Station key (repeatable; default: all stations)")

    # ingest
    p_ingest = sub.add_parser("ingest", help="Move transcribed segments into explore data dir")
    p_ingest.add_argument("--data-dir", required=True, help="Explore data directory")

    args = parser.parse_args()
    config = load_config(args.config)

    commands = {
        "record": cmd_record,
        "split": cmd_split,
        "ingest": cmd_ingest,
    }
    commands[args.command](args, config)


if __name__ == "__main__":
    main()
