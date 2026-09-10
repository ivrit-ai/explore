#!/usr/bin/env python
"""ASGI entrypoint for uvicorn workers."""
import os
import argparse
from pathlib import Path

from app import create_app, init_index_manager
from app.services.audio_store import build_audio_store
from app.utils import get_transcripts

# Parse arguments (supports --data-dir for compatibility)
parser = argparse.ArgumentParser(description='Run the ivrit.ai Explore application')
parser.add_argument('--data-dir', type=str, help='Path to the data directory', default='/home/data/explore')
parser.add_argument('--audio-source', choices=('auto', 'local', 's3'),
                    default=os.environ.get('EXPLORE_AUDIO_SOURCE', 'auto'),
                    help="Where episode audio is read from (default auto)")
parser.add_argument('--s3-bucket', default=os.environ.get('EXPLORE_S3_BUCKET'),
                    help='S3 bucket holding the audio/ tree (env EXPLORE_S3_BUCKET)')
parser.add_argument('--s3-prefix', default=os.environ.get('EXPLORE_S3_PREFIX', 'audio'),
                    help="Key prefix within the bucket (default 'audio')")
parser.add_argument('--index-backend', choices=('auto', 'sqlite', 'postgres'),
                    default=os.environ.get('EXPLORE_INDEX_BACKEND', 'auto'),
                    help="Where the search index lives (default auto)")
args, unknown = parser.parse_known_args()

data_dir = os.path.abspath(args.data_dir)
json_dir = Path(data_dir) / "json"

audio_store = build_audio_store(
    source=args.audio_source,
    data_dir=data_dir,
    bucket=args.s3_bucket,
    prefix=args.s3_prefix,
)

app = create_app(data_dir=data_dir, audio_store=audio_store)

# Initialize search service and file records eagerly
init_index_manager(app, backend=args.index_backend)
file_records = get_transcripts(json_dir) if json_dir.is_dir() else []
app.state.file_records = file_records
