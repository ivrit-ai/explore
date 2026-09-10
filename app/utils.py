import os
from urllib.parse import unquote
from typing import List
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
