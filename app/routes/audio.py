import logging
import mimetypes
import os
import time
import uuid as uuid_module

from flask import Blueprint, current_app, request, send_file

from ..routes.auth import login_required
from ..utils import resolve_audio_path

bp = Blueprint("audio", __name__)
logger = logging.getLogger(__name__)


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
        range_header = request.headers.get("Range")

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
        mimetype=mimetypes.guess_type(path)[0] or "application/octet-stream",
    )

    # Log completion
    if request_id:
        duration_ms = (time.time() - start_time) * 1000
        status = resp.status_code
        logger.debug(f"[TIMING] [REQ:{request_id}] Response prepared ({status}) in {duration_ms:.2f}ms")

    return resp


@bp.route("/audio/<path:doc_uuid>")
@login_required
def serve_audio_by_uuid(doc_uuid):
    # --- Request metadata ---
    request_id = uuid_module.uuid4().hex[:8]
    tag = f"[TIMING] [REQ:{request_id}]"
    start = time.perf_counter()
    original = doc_uuid

    logger.info(f"{tag} Start audio request for '{original}'")

    # --- UUID cleanup & validation ---
    uuid_clean, ext = os.path.splitext(doc_uuid)
    try:
        uuid_module.UUID(uuid_clean)
    except ValueError:
        logger.warning(f"{tag} Invalid UUID format: '{original}'")
        return "Invalid UUID format", 400

    if ext:
        logger.debug(f"{tag} Stripped extension '{ext}' → '{uuid_clean}'")
    else:
        logger.debug(f"{tag} No extension found in '{original}'")

    try:
        # --- Resolve episode path ---
        index = current_app.config["SEARCH_SERVICE"]._index_mgr.get()
        episode_path = index.get_episode_by_uuid(uuid_clean)

        logger.debug(f"{tag} UUID resolved to episode: {episode_path}")

        # --- Resolve audio path ---
        audio_path = resolve_audio_path(episode_path)
        if not audio_path:
            logger.warning(f"{tag} Audio not found for episode: {episode_path}")
            return f"Audio file not found for {episode_path}", 404

        logger.debug(f"{tag} Serving audio file: {audio_path}")

        # --- Serve ---
        response = send_range_file(audio_path, request_id=request_id)

        duration_ms = (time.perf_counter() - start) * 1000
        logger.info(f"{tag} Completed ({response.status_code}) in {duration_ms:.2f}ms")

        return response

    except IndexError:
        logger.warning(f"{tag} UUID not found: '{original}' → '{uuid_clean}'")
        return f"UUID not found: {original}", 404

    except Exception:
        logger.exception(f"{tag} Unexpected error for '{original}'")
        return "Internal server error", 500
