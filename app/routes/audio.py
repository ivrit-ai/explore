from flask import Blueprint, send_file, current_app, request, Response
from ..routes.auth import login_required
from ..utils import resolve_audio_path
import os
import mimetypes
import re
import time
import logging
import uuid
import glob
from functools import wraps

bp = Blueprint('audio', __name__)
logger = logging.getLogger(__name__)

def track_audio_request(func):
    """Decorator to track timing and logging for audio requests"""
    @wraps(func)
    def wrapper(*args, **kwargs):
        request_id = str(uuid.uuid4())[:8]
        start_time = time.time()

        # Log the request start
        request_path = kwargs.get('doc_uuid') or kwargs.get('filename', 'unknown')
        logger.info(f"[TIMING] [REQ:{request_id}] Audio request received for: {request_path}")

        try:
            # Pass request_id to the function
            result = func(*args, request_id=request_id, **kwargs)

            # Log successful completion
            duration_ms = (time.time() - start_time) * 1000
            logger.info(f"[TIMING] [REQ:{request_id}] Request completed successfully in {duration_ms:.2f}ms")

            return result

        except Exception:
            # Log timing for failed requests (error details logged by specific handlers)
            duration_ms = (time.time() - start_time) * 1000
            logger.info(f"[TIMING] [REQ:{request_id}] Request completed with error in {duration_ms:.2f}ms")
            raise

    return wrapper

def parse_range_header(range_header, file_size):
    """Parse HTTP Range header and return (start, end) tuple or None."""
    if not range_header:
        return None

    m = re.search(r'bytes=(\d+)-(\d*)', range_header)
    if not m:
        return None

    start = int(m.group(1))
    end = int(m.group(2)) if m.group(2) else file_size - 1
    return (start, end)


def stream_file(path, start_byte=0, end_byte=None, chunk_size=8192):
    """Generate file chunks. Pure I/O, no logging or protocol logic."""
    with open(path, 'rb') as f:
        if end_byte is not None:
            # Range request
            f.seek(start_byte)
            remaining = end_byte - start_byte + 1
            while remaining > 0:
                chunk = f.read(min(chunk_size, remaining))
                if not chunk:
                    break
                remaining -= len(chunk)
                yield chunk
        else:
            # Full file
            while True:
                chunk = f.read(chunk_size)
                if not chunk:
                    break
                yield chunk


def build_range_response(path, file_size, content_type, byte_range):
    """Build HTTP 206 Partial Content response."""
    start, end = byte_range
    length = end - start + 1

    resp = Response(stream_file(path, start, end), 206, mimetype=content_type)
    resp.headers.add('Content-Range', f'bytes {start}-{end}/{file_size}')
    resp.headers.add('Accept-Ranges', 'bytes')
    resp.headers.add('Content-Length', str(length))
    return resp


def build_full_response(path, file_size, content_type):
    """Build HTTP 200 OK response for full file."""
    resp = Response(stream_file(path), 200, mimetype=content_type)
    resp.headers.add('Accept-Ranges', 'bytes')
    resp.headers.add('Content-Length', str(file_size))
    return resp


def send_range_file(path, request_id=None):
    """Serve a file with optional HTTP range support. Orchestrates parsing, streaming, and logging."""
    start_time = time.time()

    # Validate file
    if not os.path.exists(path):
        if request_id:
            logger.warning(f"[TIMING] [REQ:{request_id}] File not found: {path}")
        return "File not found", 404

    # Get file metadata
    file_size = os.path.getsize(path)
    content_type = mimetypes.guess_type(path)[0] or 'application/octet-stream'

    # Parse range request
    byte_range = parse_range_header(request.headers.get('Range'), file_size)

    # Build response
    if byte_range:
        start, end = byte_range
        if request_id:
            logger.debug(f"[TIMING] [REQ:{request_id}] Serving range: bytes {start}-{end}/{file_size}")
        resp = build_range_response(path, file_size, content_type, byte_range)
    else:
        if request_id:
            logger.debug(f"[TIMING] [REQ:{request_id}] Serving full file: {file_size} bytes")
        resp = build_full_response(path, file_size, content_type)

    # Log completion
    if request_id:
        duration_ms = (time.time() - start_time) * 1000
        logger.debug(f"[TIMING] [REQ:{request_id}] Response prepared in {duration_ms:.2f}ms")

    return resp

@bp.route('/audio/<path:doc_uuid>')
@login_required
@track_audio_request
def serve_audio_by_uuid(doc_uuid, request_id=None):
    # Strip file extension from UUID (e.g., "uuid.opus" -> "uuid")
    # The frontend sends UUIDs with file extensions, but the database stores them without
    original_uuid = doc_uuid
    doc_uuid_clean, ext = os.path.splitext(doc_uuid)
    if ext:
        logger.debug(f"[TIMING] [REQ:{request_id}] Stripped extension '{ext}' from UUID: '{original_uuid}' -> '{doc_uuid_clean}'")
    else:
        logger.debug(f"[TIMING] [REQ:{request_id}] No extension found in UUID: '{doc_uuid}'")

    try:
        # Get the episode path from the UUID
        index = current_app.config.get('SEARCH_SERVICE')._index_mgr.get()
        episode_path = index.get_episode_by_uuid(doc_uuid_clean)

        logger.debug(f"[TIMING] [REQ:{request_id}] Resolved UUID {doc_uuid_clean} to episode: {episode_path}")

        # Resolve the audio file path
        audio_path = resolve_audio_path(episode_path)
        if not audio_path:
            logger.warning(f"[TIMING] [REQ:{request_id}] Audio file not found for episode: {episode_path}")
            return f"Audio file not found for {episode_path}", 404

        logger.debug(f"[TIMING] [REQ:{request_id}] Found audio file: {audio_path}")
        return send_range_file(audio_path, request_id)

    except IndexError:
        logger.warning(f"[TIMING] [REQ:{request_id}] UUID not found: '{original_uuid}' (cleaned: '{doc_uuid_clean}')")
        return f"UUID not found: {original_uuid}", 404
    except Exception:
        logger.exception(f"[TIMING] [REQ:{request_id}] Unexpected error serving audio for UUID '{original_uuid}' (cleaned: '{doc_uuid_clean}')")
        return "Internal server error", 500
