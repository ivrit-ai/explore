"""
CLI for managing the search index.

Usage:
    python -m app.cli build --data-dir ../data
    python -m app.cli status
"""

import argparse
import logging
import os
import sys
import time
from pathlib import Path

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger(__name__)


def build_index(data_dir: str):
    """Build the search index from transcript files."""
    from .services.index import IndexManager
    from .utils import get_transcripts

    data_root = Path(data_dir).expanduser().resolve()
    json_dir = data_root / "json"

    if not json_dir.is_dir():
        log.error(f"Transcript directory not found: {json_dir}")
        sys.exit(1)

    # Get database path
    db_path = Path(os.environ.get("SQLITE_PATH", "explore.sqlite"))

    # Check if database already exists
    if db_path.exists():
        log.error(f"Database already exists: {db_path}")
        log.error("Please delete it manually if you want to rebuild the index")
        sys.exit(1)

    # Scan transcript files
    log.info(f"Scanning for transcripts in {json_dir}")
    start_time = time.perf_counter()
    file_records = get_transcripts(json_dir)
    scan_duration = time.perf_counter() - start_time
    log.info(f"Found {len(file_records)} transcript files in {scan_duration:.2f}s")

    if not file_records:
        log.error("No transcript files found")
        sys.exit(1)

    # Build index
    log.info(f"Building index at {db_path}")
    start_time = time.perf_counter()

    index_mgr = IndexManager(file_records=file_records, path=str(db_path))

    build_duration = time.perf_counter() - start_time
    log.info(f"Index built successfully in {build_duration:.2f}s")

    # Get index statistics
    index = index_mgr.get()
    doc_count, total_chars = index.get_document_stats()
    log.info(f"Index stats: {doc_count} documents, {total_chars:,} total characters")

    return index_mgr


def show_status():
    """Show status and statistics of the current index."""
    from .services.index import IndexManager

    db_path = Path(os.environ.get("SQLITE_PATH", "explore.sqlite"))

    if not db_path.exists():
        log.error(f"Database not found: {db_path}")
        log.info("Run 'python -m app.cli build --data-dir <path>' to create the index")
        sys.exit(1)

    log.info(f"Reading index from {db_path}")

    # Get file size
    size_mb = db_path.stat().st_size / (1024 * 1024)
    log.info(f"Database size: {size_mb:.2f} MB")

    # Load index and get stats
    index_mgr = IndexManager(index_path=db_path)
    index = index_mgr.get()
    doc_count, total_chars = index.get_document_stats()

    log.info(f"Documents: {doc_count}")
    log.info(f"Total characters: {total_chars:,}")

    # Check for WAL files
    for suffix in ["-wal", "-shm"]:
        wal_file = Path(str(db_path) + suffix)
        if wal_file.exists():
            wal_size = wal_file.stat().st_size / (1024 * 1024)
            log.info(f"WAL file {wal_file.name}: {wal_size:.2f} MB")


def main():
    parser = argparse.ArgumentParser(
        description="Manage the ivrit.ai Explore search index",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python -m app.cli build --data-dir ../data
  python -m app.cli status
        """,
    )

    subparsers = parser.add_subparsers(dest="command", help="Command to run")

    # Build command
    build_parser = subparsers.add_parser("build", help="Build the search index")
    build_parser.add_argument("--data-dir", required=True, help="Path to data directory containing json/ subdirectory")

    # Status command
    subparsers.add_parser("status", help="Show index status and statistics")

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        sys.exit(1)

    try:
        if args.command == "build":
            build_index(args.data_dir)
        elif args.command == "status":
            show_status()
        else:
            parser.print_help()
            sys.exit(1)
    except KeyboardInterrupt:
        log.info("\nInterrupted by user")
        sys.exit(1)
    except Exception as e:
        log.error(f"Error: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
