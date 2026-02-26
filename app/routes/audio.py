from fastapi import APIRouter, Request, Depends, HTTPException
from fastapi.responses import FileResponse
from ..routes.auth import require_login
from ..utils import resolve_audio_path
import os
import mimetypes
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

        # Resolve audio path
        audio_dir = request.app.state.audio_dir
        audio_path = resolve_audio_path(episode_path, audio_dir)
        if not audio_path:
            logger.warning(f"{tag} Audio not found for episode: {episode_path}")
            raise HTTPException(status_code=404, detail=f"Audio file not found for {episode_path}")

        logger.debug(f"{tag} Serving audio file: {audio_path}")

        # Validate file existence
        if not os.path.exists(audio_path):
            logger.warning(f"{tag} File not found: {audio_path}")
            raise HTTPException(status_code=404, detail="File not found")

        media_type = mimetypes.guess_type(audio_path)[0] or 'application/octet-stream'

        duration_ms = (time.perf_counter() - start) * 1000
        logger.info(f"{tag} Serving file in {duration_ms:.2f}ms")

        # FileResponse handles Range requests natively via Starlette
        return FileResponse(
            audio_path,
            media_type=media_type,
        )

    except IndexError:
        logger.warning(f"{tag} UUID not found: '{original}' → '{uuid_clean}'")
        raise HTTPException(status_code=404, detail=f"UUID not found: {original}")

    except HTTPException:
        raise

    except Exception:
        logger.exception(f"{tag} Unexpected error for '{original}'")
        raise HTTPException(status_code=500, detail="Internal server error")
