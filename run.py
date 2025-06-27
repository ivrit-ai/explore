# run.py – bootstrap ivrit.ai Explore (new search pipeline 2025‑05)
# ----------------------------------------------------------------------------
# Usage examples:
#   python run.py --data-dir ../data --dev          # http://localhost:5000
#   python run.py --data-dir /srv/explore/data      # https + letsencrypt
#   python run.py --force-reindex                   # drop cache & rebuild
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
from app.services.index import IndexManager
from app.services.search import SearchService

# ---------------------------------------------------------------------------
# 1. CLI parsing
# ---------------------------------------------------------------------------
parser = argparse.ArgumentParser(description="Run ivrit.ai Explore server")
parser.add_argument("--data-dir", default="../data",
                    help="Path holding 'json/' and 'audio/' sub‑dirs (default ../data)")
parser.add_argument("--index-file", default=None,
                    help="Path to custom index file (default: auto-generated in data directory)")
parser.add_argument("--force-reindex", action="store_true",
                    help="Rebuild in‑memory index even if it seems fresh")
parser.add_argument("--port", type=int, default=443,
                    help="Port to bind (443 for prod, 5000 dev)")
parser.add_argument("--dev", action="store_true", help="Run in HTTP dev mode (no SSL)")
parser.add_argument("--ssl-cert", default="/etc/letsencrypt/live/explore.ivrit.ai/fullchain.pem")
parser.add_argument("--ssl-key",  default="/etc/letsencrypt/live/explore.ivrit.ai/privkey.pem")
args = parser.parse_args()

# Set environment variables for dev mode
if args.dev:
    os.environ['FLASK_ENV'] = 'development'
    os.environ['TS_USER_EMAIL'] = 'dev@ivrit.ai'

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

@timeit("Transcript scan")
def init_file_service(json_dir: Path, audio_dir: Path):
    file_records = get_transcripts(json_dir)
    
    log.info(f"Found {len(file_records)} transcript files")
    return file_records

@timeit("Index build")
def build_index(file_records, force: bool, index_file: str | None = None):
    if force:
        log.info("--force-reindex supplied; building fresh index …")
    return IndexManager(file_records, index_file=index_file)

# ---------------------------------------------------------------------------
# 5. Wire everything up
# ---------------------------------------------------------------------------

data_root = Path(args.data_dir).expanduser().resolve()
json_dir  = data_root / "json"
audio_dir = data_root / "audio"

if not json_dir.is_dir():
    log.error(f"Transcript directory not found: {json_dir}")
    sys.exit(1)

app = init_app(str(data_root))
with app.app_context():
    file_records = init_file_service(json_dir, audio_dir)
    print(file_records)
    from app import init_index_manager
    index_manager = init_index_manager(
        app,
        file_records=file_records,
        index_file=args.index_file,
        force_reindex=args.force_reindex
    )

    # expose to blueprints
    app.config["FILE_RECORDS"] = file_records

    # make main blueprint globals match
    from app.routes import main as main_bp
    main_bp.file_records = file_records
    main_bp.search_service = app.config["SEARCH_SERVICE"]
    

    # memory diagnostics (optional)
    try:
        import psutil
        rss = psutil.Process().memory_info().rss / (1024 ** 2)
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
