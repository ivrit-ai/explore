from contextlib import asynccontextmanager
from pathlib import Path
from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from starlette.middleware.sessions import SessionMiddleware
from starlette.responses import RedirectResponse
from starlette.templating import Jinja2Templates
from .services.analytics_service import AnalyticsService
import os
import logging

from dotenv import load_dotenv

load_dotenv()


def create_app(data_dir: str, index_file: str = None):
    @asynccontextmanager
    async def lifespan(app: FastAPI):
        app.state.data_dir = Path(data_dir)
        app.state.audio_dir = Path(data_dir) / "audio"
        app.state.index_file = index_file

        # Configure PostHog
        posthog_api_key = os.environ.get('POSTHOG_API_KEY', '')
        posthog_host = os.environ.get('POSTHOG_HOST', 'https://app.posthog.com')
        disable_analytics = os.environ.get('DISABLE_ANALYTICS', '').lower() in ('true', '1', 'yes')

        app.state.analytics = AnalyticsService(
            api_key=posthog_api_key,
            host=posthog_host,
            disabled=disable_analytics,
        )

        app.state.mime_types = {'opus': 'audio/opus'}

        templates_dir = Path(__file__).parent / "templates"
        app.state.templates = Jinja2Templates(directory=str(templates_dir))

        yield

    app = FastAPI(lifespan=lifespan)

    secret_key = os.environ.get('SECRET_KEY', 'dev-secret-key')
    app.add_middleware(SessionMiddleware, secret_key=secret_key)

    static_dir = Path(__file__).parent / "static"
    app.mount("/static", StaticFiles(directory=str(static_dir)), name="static")

    # Import and register routers
    from .routes import auth, main, search, export, audio

    app.include_router(auth.router)
    app.include_router(main.router)
    app.include_router(search.router, prefix="/search")
    app.include_router(export.router)
    app.include_router(audio.router)

    # Exception handler for login required
    @app.exception_handler(auth.LoginRequired)
    async def handle_login_required(request, exc):
        request.session['next_url'] = str(request.url)
        return RedirectResponse(url="/login", status_code=302)

    return app


def init_index_manager(app, **db_kwargs):
    """Initialize the index manager from an existing database.

    Args:
        app: FastAPI application instance
        **db_kwargs: Database-specific connection parameters (e.g., path)
    """
    from .services.index import IndexManager
    from .services.search import SearchService

    log = logging.getLogger(__name__)

    if not db_kwargs:
        db_kwargs = {
            "path": os.environ.get('SQLITE_PATH', 'explore.sqlite')
        }

    db_path = Path(db_kwargs.get('path', 'explore.sqlite'))

    if not db_path.exists():
        log.error(f"Database not found: {db_path}")
        log.error("Please build the index first using: python -m app.cli build --data-dir <path>")
        raise FileNotFoundError(f"Database not found: {db_path}")

    index_mgr = IndexManager(index_path=db_path)

    app.state.search_service = SearchService(index_mgr)
    return index_mgr
