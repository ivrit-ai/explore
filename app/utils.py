import os
from urllib.parse import unquote
from typing import Optional, List
from flask import current_app
from pathlib import Path
from dataclasses import dataclass
from typing import NamedTuple
import gzip
import orjson
import logging

_JSON_FILENAME = "full_transcript.json.gz"          # gzipped transcripts


class FileRecord(NamedTuple):
    id: str
    json_path: Path

    def read_json(self) -> dict | list:
        """Read and parse the gzipped JSON file."""
        with gzip.open(self.json_path, 'rb') as fh:
            return orjson.loads(fh.read())


def get_transcripts(root: Path) -> List[FileRecord]:
    """Find all full_transcript.json.gz files and return a records list.
    
    Args:
        root: Root directory to search for transcript files
        
    Returns:
        List of FileRecord objects, one per transcript JSON file
        
    Supports both legacy flat files:   <id>.json.gz
    and new nested files:            <source>/<id>/full_transcript.json.gz
    """
    recs: list[FileRecord] = []
    for p in root.rglob(f"*{_JSON_FILENAME}"):
        rec_id = f"{p.parent.parent.name}/{p.parent.name}"
        recs.append(FileRecord(rec_id, p))

    # complain loudly if we picked up duplicates
    seen: set[str] = set()
    dups: set[str] = set()
    for r in recs:
        if r.id in seen:
            dups.add(r.id)
        seen.add(r.id)
    if dups:
        logging.warning("get_transcripts: duplicate IDs detected: %s", ", ".join(sorted(dups)))

    recs.sort(key=lambda r: r.id)
    return recs


def resolve_audio_path(source: str) -> Optional[str]:
    """
    Resolve the path to an audio file based on source.
    
    Args:
        source: The source identifier
        
    Returns:
        The full path to the audio file if it exists, None otherwise.
        Audio files are expected to be stored as: audio_dir/source/source.opus
        Handles URL decoding if the file doesn't exist initially.
    """
    # Get audio directory from Flask app config
    audio_dir = current_app.config.get('AUDIO_DIR')
    if not audio_dir:
        return None

    # Construct the direct path to the audio file based on source
    # Assuming the audio files are stored as: audio_dir/source/source.opus

    source = source.split('/')
    audio_path = os.path.join(audio_dir, *source)
    
    # Return the path if file exists, None otherwise
    return audio_path if os.path.exists(audio_path) else None 