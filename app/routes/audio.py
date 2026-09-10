from fastapi import APIRouter, Request, Depends, HTTPException
from ..routes.auth import require_login
import os
import time
import logging
import uuid as uuid_module

router = APIRouter()
logger = logging.getLogger(__name__)


@router.get('/audio/{doc_uuid:path}', name='audio.serve_audio_by_uuid')
def serve_audio_by_uuid(
    request: Request,
    doc_uuid: str,
    user_email: str = Depends(require_login),
):
    request_id = uuid_module.uuid4().hex[:8]
    tag = f"[TIMING] [REQ:{request_id}]"
    start = time.perf_counter()
    original = doc_uuid

    logger.info(f"{tag} Start audio request for '{original}'")

    # UUID cleanup & validation
    uuid_clean, ext = os.path.splitext(doc_uuid)
    try:
        uuid_module.UUID(uuid_clean)
    except ValueError:
        logger.warning(f"{tag} Invalid UUID format: '{original}'")
        raise HTTPException(status_code=400, detail="Invalid UUID format")

    if ext:
        logger.debug(f"{tag} Stripped extension '{ext}' → '{uuid_clean}'")
    else:
        logger.debug(f"{tag} No extension found in '{original}'")

    try:
        # Resolve episode path
        index = request.app.state.search_service._index_mgr.get()
        episode_path = index.get_episode_by_uuid(uuid_clean)

        logger.debug(f"{tag} UUID resolved to episode: {episode_path}")

        # Hand off to the configured audio backend (local directory or S3).
        # Both honour Range requests, so player seeking works either way.
        store = request.app.state.audio_store
        response = store.response(request, episode_path)

        duration_ms = (time.perf_counter() - start) * 1000
        logger.info(f"{tag} Serving {episode_path} from {store.describe()} in {duration_ms:.2f}ms")

        return response

    except IndexError:
        logger.warning(f"{tag} UUID not found: '{original}' → '{uuid_clean}'")
        raise HTTPException(status_code=404, detail=f"UUID not found: {original}")

    except HTTPException:
        raise

    except Exception:
        logger.exception(f"{tag} Unexpected error for '{original}'")
        raise HTTPException(status_code=500, detail="Internal server error")
