# run.py – bootstrap ivrit.ai Explore
# ----------------------------------------------------------------------------
# Usage examples:
#   # First, build the index (one-time or when data changes):
#   python -m app.cli build --data-dir ../data
#
#   # Then run the server:
#   python run.py --data-dir ../data --dev          # http://localhost:5000
#   python run.py --data-dir /home/data/explore     # http://0.0.0.0:8200
#
#   # Serve audio from S3 instead of a local audio/ directory:
#   python run.py --audio-source s3 --s3-bucket explore-datastore
#
#   # Read the search index from Postgres instead of explore.sqlite:
#   DATABASE_URL=postgresql://... python run.py --index-backend postgres
#
#   # Or use auto-build for convenience (builds if DB doesn't exist):
#   python run.py --data-dir ../data --dev --auto-build
# ----------------------------------------------------------------------------

from __future__ import annotations

import argparse
import logging
import os
import sys
import time
from pathlib import Path

# ---------------------------------------------------------------------------
# 1. CLI parsing
# ---------------------------------------------------------------------------
parser = argparse.ArgumentParser(description="Run ivrit.ai Explore server")
parser.add_argument("--data-dir", default="../data",
                    help="Path holding 'json/' and 'audio/' sub-dirs (default ../data)")
parser.add_argument("--auto-build", action="store_true",
                    help="Automatically build index if database doesn't exist")
parser.add_argument("--port", type=int, default=8200,
                    help="Port to bind (default 8200)")
parser.add_argument("--dev", action="store_true", help="Run in dev mode (localhost:5000)")
parser.add_argument("--audio-source", choices=("auto", "local", "s3"),
                    default=os.environ.get("EXPLORE_AUDIO_SOURCE", "auto"),
                    help="Where episode audio is read from; 'auto' picks s3 when a "
                         "bucket is configured, else the local audio/ dir (default auto)")
parser.add_argument("--s3-bucket", default=os.environ.get("EXPLORE_S3_BUCKET"),
                    help="S3 bucket holding the audio/ tree (env EXPLORE_S3_BUCKET)")
parser.add_argument("--s3-prefix", default=os.environ.get("EXPLORE_S3_PREFIX", "audio"),
                    help="Key prefix within the bucket (default 'audio')")
parser.add_argument("--index-backend", choices=("auto", "sqlite", "postgres"),
                    default=os.environ.get("EXPLORE_INDEX_BACKEND", "auto"),
                    help="Where the search index lives; 'auto' picks postgres when "
                         "DATABASE_URL is set, else the local sqlite file (default auto)")
args, _unknown = parser.parse_known_args()

# Set environment variables for dev mode
if args.dev:
    os.environ['APP_ENV'] = 'development'
    os.environ['TS_USER_EMAIL'] = 'dev@ivrit.ai'

# ---------------------------------------------------------------------------
# 2. Logging to file + stdout
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.FileHandler("app.log", encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
)
log = logging.getLogger("run")

# ---------------------------------------------------------------------------
# 3. Decorator for timing
# ---------------------------------------------------------------------------

def timeit(name: str):
    def _decor(fn):
        def wrapper(*a, **kw):
            t0 = time.perf_counter()
            log.info(f"▶ {name} …")
            out = fn(*a, **kw)
            log.info(f"✓ {name} done in {(time.perf_counter()-t0):.2f}s")
            return out
        return wrapper
    return _decor

# ---------------------------------------------------------------------------
# 4. Initialise FastAPI + services
# ---------------------------------------------------------------------------

from app import create_app, init_index_manager, resolve_index_backend
from app.utils import get_transcripts

@timeit("FastAPI app init")
def init_app(data_dir: str, audio_store=None):
    app = create_app(data_dir=data_dir, audio_store=audio_store)
    return app

@timeit("Index auto-build")
def auto_build_index(data_dir: Path):
    """Automatically build index if it doesn't exist."""
    from app.cli import build_index

    log.info("--auto-build: Building index from transcript files")
    build_index(str(data_dir))

# ---------------------------------------------------------------------------
# 5. Wire everything up
# ---------------------------------------------------------------------------

from app.services.audio_store import build_audio_store

data_root = Path(args.data_dir).expanduser().resolve()
json_dir  = data_root / "json"

audio_store = build_audio_store(
    source=args.audio_source,
    data_dir=data_root,
    bucket=args.s3_bucket,
    prefix=args.s3_prefix,
)
log.info(f"Audio source: {audio_store.describe()}")

# Transcripts are only read here to populate app.state.file_records; searching
# uses the prebuilt index. A missing json/ dir is therefore not fatal.
if not json_dir.is_dir():
    log.warning(f"Transcript directory not found: {json_dir} (serving from the index only)")

index_backend = resolve_index_backend(args.index_backend)
log.info(f"Index backend: {index_backend}")

# The sqlite index is a local file that must already exist; the postgres index
# is populated out of band (scripts/migrate_sqlite_to_postgres.py), never here.
if index_backend == 'sqlite':
    db_path = Path(os.environ.get('SQLITE_PATH', 'explore.sqlite'))

    if not db_path.exists():
        if args.auto_build:
            auto_build_index(data_root)
        else:
            log.error(f"Database not found: {db_path}")
            log.error("Please build the index first using: python -m app.cli build --data-dir " + args.data_dir)
            log.error("Or use --auto-build flag to build automatically")
            sys.exit(1)

app = init_app(str(data_root), audio_store=audio_store)

# Initialize search service and file records eagerly (before server starts).
init_index_manager(app, backend=index_backend)

file_records = get_transcripts(json_dir) if json_dir.is_dir() else []
app.state.file_records = file_records

# Memory diagnostics (optional)
try:
    import psutil
    rss = psutil.Process().memory_info().rss / (1024 ** 2)
    log.info(f"Resident memory: {rss:.1f} MB")
except ImportError:
    pass

# ---------------------------------------------------------------------------
# 6. Run with uvicorn
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import uvicorn

    host = "0.0.0.0"
    port = 5000 if args.dev else args.port

    log.info(f"{'DEV' if args.dev else 'PROD'} mode – http://{host}:{port}")
    uvicorn.run(app, host=host, port=port, log_level="info")
