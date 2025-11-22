from flask import Blueprint, send_file, current_app, request
from ..routes.auth import login_required
from ..utils import resolve_audio_path
import os
import mimetypes
import re
import time
import logging
import uuid as uuid_module
from functools import wraps

bp = Blueprint('audio', __name__)
logger = logging.getLogger(__name__)

def track_audio_request(func):
    """Decorator to track timing and logging for audio requests"""
    @wraps(func)
    def wrapper(*args, **kwargs):
        request_id = str(uuid_module.uuid4())[:8]
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

def send_range_file(path, request_id=None):
    """
    Serve a file with HTTP range support using Werkzeug's optimized file serving.

    Uses send_file which:
    - Handles range requests automatically per RFC 7233
    - Uses efficient C-level file I/O
    - Doesn't block worker threads for the entire duration
    - Supports conditional requests (If-Modified-Since, etc.)
    """
    start_time = time.time()

    # Validate file existence
    if not os.path.exists(path):
        if request_id:
            logger.warning(f"[TIMING] [REQ:{request_id}] File not found: {path}")
        return "File not found", 404

    # Log request details
    if request_id:
        file_size = os.path.getsize(path)
        range_header = request.headers.get('Range')

        if range_header:
            logger.debug(f"[TIMING] [REQ:{request_id}] Range request: {range_header} for file size {file_size}")
        else:
            logger.debug(f"[TIMING] [REQ:{request_id}] Serving full file: {file_size} bytes")

    # Use Werkzeug's optimized send_file with automatic range support
    # conditional=True enables RFC 7233 range request handling
    # as_attachment=False serves inline (for audio playback)
    resp = send_file(
        path,
        conditional=True,
        as_attachment=False,
        mimetype=mimetypes.guess_type(path)[0] or 'application/octet-stream'
    )

    # Log completion
    if request_id:
        duration_ms = (time.time() - start_time) * 1000
        status = resp.status_code
        logger.debug(f"[TIMING] [REQ:{request_id}] Response prepared ({status}) in {duration_ms:.2f}ms")

    return resp

@bp.route('/audio/<path:doc_uuid>')
@login_required
@track_audio_request
def serve_audio_by_uuid(doc_uuid, request_id=None):
    # Strip file extension from UUID (e.g., "uuid.opus" -> "uuid")
    # The frontend sends UUIDs with file extensions, but the database stores them without
    original_uuid = doc_uuid
    doc_uuid_clean, ext = os.path.splitext(doc_uuid)

    # Validate UUID format to prevent path traversal attacks
    try:
        uuid_module.UUID(doc_uuid_clean)
    except ValueError:
        logger.warning(f"[TIMING] [REQ:{request_id}] Invalid UUID format: '{original_uuid}'")
        return "Invalid UUID format", 400

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
