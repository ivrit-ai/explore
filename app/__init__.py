from contextlib import asynccontextmanager
from pathlib import Path
from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from starlette.middleware.sessions import SessionMiddleware
from starlette.responses import RedirectResponse
from starlette.templating import Jinja2Templates
from .services.analytics_service import AnalyticsService
from .services.audio_store import AudioStore, build_audio_store
import os
import logging

from dotenv import load_dotenv

load_dotenv()


def create_app(data_dir: str = None, index_file: str = None,
               audio_store: AudioStore = None):
    """Build the FastAPI app.

    Args:
        data_dir: Directory holding 'json/' and 'audio/'. Optional when audio
            is served from S3 and the index is prebuilt.
        index_file: Optional explicit index path.
        audio_store: Audio backend. Defaults to one built from the environment
            (see build_audio_store), falling back to data_dir/audio.
    """
    @asynccontextmanager
    async def lifespan(app: FastAPI):
        app.state.data_dir = Path(data_dir) if data_dir else None
        app.state.index_file = index_file

        store = audio_store or build_audio_store(data_dir=data_dir)
        app.state.audio_store = store
        # Kept for backwards compatibility; only meaningful for local storage.
        app.state.audio_dir = getattr(store, "audio_dir", None)
        logging.getLogger(__name__).info(f"Audio backend: {store.describe()}")

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

    # Initialize Google OAuth in production
    if os.environ.get('APP_ENV') != 'development':
        auth.init_oauth(app)

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


def resolve_index_backend(backend: str = None) -> str:
    """Pick the index backend: 'sqlite' or 'postgres'.

    'auto' (the default) chooses postgres when a DATABASE_URL is present, which
    is how managed hosts hand over their database, and sqlite otherwise.
    """
    backend = (backend or os.environ.get('EXPLORE_INDEX_BACKEND') or 'auto').lower()
    if backend == 'auto':
        backend = 'postgres' if os.environ.get('DATABASE_URL') else 'sqlite'
    if backend not in ('sqlite', 'postgres'):
        raise ValueError(f"Unknown index backend: {backend!r} (expected 'sqlite', "
                         "'postgres' or 'auto')")
    return backend


def init_index_manager(app, backend: str = None, dsn: str = None, **db_kwargs):
    """Initialize the index manager from an existing index.

    Args:
        app: FastAPI application instance
        backend: 'sqlite', 'postgres', or 'auto' (see resolve_index_backend)
        dsn: Postgres connection string; defaults to $DATABASE_URL
        **db_kwargs: SQLite connection parameters (e.g., path)
    """
    from .services.search import SearchService

    log = logging.getLogger(__name__)
    backend = resolve_index_backend(backend)

    if backend == 'postgres':
        from .services.pg_index import PostgresIndexManager

        index_mgr = PostgresIndexManager(dsn=dsn)
        log.info("Index backend: postgres")
    else:
        from .services.index import IndexManager

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
        log.info(f"Index backend: sqlite ({db_path})")

    app.state.index_backend = backend
    app.state.search_service = SearchService(index_mgr)
    return index_mgr
