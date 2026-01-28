# run.py – bootstrap ivrit.ai Explore (new search pipeline 2025‑05)
# ----------------------------------------------------------------------------
# Usage examples:
#   # First, build the index (one-time or when data changes):
#   python -m app.cli build --data-dir ../data
#
#   # Then run the server:
#   python run.py --data-dir ../data --dev          # http://localhost:5000
#   python run.py --data-dir /srv/explore/data      # https + letsencrypt
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

from app import create_app
from app.utils import get_transcripts

# ---------------------------------------------------------------------------
# 1. CLI parsing
# ---------------------------------------------------------------------------
parser = argparse.ArgumentParser(description="Run ivrit.ai Explore server")
parser.add_argument(
    "--data-dir", default="../data", help="Path holding 'json/' and 'audio/' sub‑dirs (default ../data)"
)
parser.add_argument("--auto-build", action="store_true", help="Automatically build index if database doesn't exist")
parser.add_argument("--port", type=int, default=443, help="Port to bind (443 for prod, 5000 dev)")
parser.add_argument("--dev", action="store_true", help="Run in HTTP dev mode (no SSL)")
parser.add_argument("--ssl-cert", default="/etc/letsencrypt/live/explore.ivrit.ai/fullchain.pem")
parser.add_argument("--ssl-key", default="/etc/letsencrypt/live/explore.ivrit.ai/privkey.pem")
args = parser.parse_args()

# Set environment variables for dev mode
if args.dev:
    os.environ["FLASK_ENV"] = "development"
    os.environ["TS_USER_EMAIL"] = "dev@ivrit.ai"

# ---------------------------------------------------------------------------
# 2. Logging to file + stdout
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.FileHandler("app.log", encoding="utf‑8"),
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
# 4. Initialise Flask + services
# ---------------------------------------------------------------------------


@timeit("Flask app init")
def init_app(data_dir: str):
    app = create_app(data_dir=data_dir)
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

data_root = Path(args.data_dir).expanduser().resolve()
json_dir = data_root / "json"
audio_dir = data_root / "audio"

if not json_dir.is_dir():
    log.error(f"Transcript directory not found: {json_dir}")
    sys.exit(1)

# Check if database exists
db_path = Path(os.environ.get("SQLITE_PATH", "explore.sqlite"))

if not db_path.exists():
    if args.auto_build:
        # Auto-build the index
        auto_build_index(data_root)
    else:
        log.error(f"Database not found: {db_path}")
        log.error("Please build the index first using: python -m app.cli build --data-dir " + args.data_dir)
        log.error("Or use --auto-build flag to build automatically")
        sys.exit(1)

app = init_app(str(data_root))
with app.app_context():
    from app import init_index_manager

    # Load index from existing database
    index_manager = init_index_manager(app)

    # Load file records for the app
    file_records = get_transcripts(json_dir)
    app.config["FILE_RECORDS"] = file_records

    # make main blueprint globals match
    from app.routes import main as main_bp

    main_bp.file_records = file_records
    main_bp.search_service = app.config["SEARCH_SERVICE"]

    # memory diagnostics (optional)
    try:
        import psutil

        rss = psutil.Process().memory_info().rss / (1024**2)
        log.info(f"Resident memory: {rss:.1f} MB")
    except ImportError:
        pass

# ---------------------------------------------------------------------------
# 6. Run Flask (dev HTTP or prod HTTPS)
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    host = "0.0.0.0"
    if args.dev:
        log.info("DEV mode – http://localhost:5000")
        app.run(host=host, port=5000, debug=False, threaded=True)
    else:
        if not (Path(args.ssl_cert).exists() and Path(args.ssl_key).exists()):
            log.error("SSL cert/key not found. Use --dev for HTTP mode or supply valid paths.")
            sys.exit(1)
        log.info(f"PROD mode – https://0.0.0.0:{args.port}")
        app.run(host=host, port=args.port, ssl_context=(args.ssl_cert, args.ssl_key), threaded=True)
