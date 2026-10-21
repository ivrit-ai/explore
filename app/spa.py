from pathlib import Path

from fastapi import HTTPException
from starlette.responses import FileResponse

# Vite build output (see frontend/). Built on the server by deploy.sh.
DIST_DIR = Path(__file__).resolve().parent.parent / "frontend" / "dist"


def spa_index() -> FileResponse:
    """Serve the React app shell; client-side routing takes it from there."""
    index = DIST_DIR / "index.html"
    if not index.is_file():
        raise HTTPException(status_code=503, detail="Frontend not built: run `npm run build` in frontend/")
    # The shell must never be cached, so a deploy is picked up on the next load.
    return FileResponse(index, headers={"Cache-Control": "no-cache"})
