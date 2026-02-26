#!/usr/bin/env python
"""ASGI entrypoint for uvicorn workers."""
import os
import argparse
from pathlib import Path

from app import create_app, init_index_manager
from app.utils import get_transcripts

# Parse arguments (supports --data-dir for compatibility)
parser = argparse.ArgumentParser(description='Run the ivrit.ai Explore application')
parser.add_argument('--data-dir', type=str, help='Path to the data directory', default='/root/data')
args, unknown = parser.parse_known_args()

data_dir = os.path.abspath(args.data_dir)
json_dir = Path(data_dir) / "json"

app = create_app(data_dir=data_dir)

# Initialize search service and file records eagerly
init_index_manager(app)
file_records = get_transcripts(json_dir)
app.state.file_records = file_records
