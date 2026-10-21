from contextlib import asynccontextmanager
from pathlib import Path
from fastapi import FastAPI, Request
from fastapi.staticfiles import StaticFiles
from starlette.middleware.sessions import SessionMiddleware
from starlette.responses import RedirectResponse
from .services.analytics_service import AnalyticsService
from .spa import DIST_DIR
import os
import logging

from dotenv import load_dotenv

load_dotenv()

SESSION_MAX_AGE_S = 14 * 24 * 3600

# The app only loads its own bundle, fonts and audio.
CONTENT_SECURITY_POLICY = (
    "default-src 'self'; script-src 'self'; style-src 'self'; img-src 'self' data:; "
    "font-src 'self'; media-src 'self'; connect-src 'self'; object-src 'none'; "
    "base-uri 'self'; frame-ancestors 'none'; form-action 'self'"
)


def is_dev() -> bool:
    return os.environ.get('APP_ENV') == 'development'


def _secret_key() -> str:
    secret_key = os.environ.get('SECRET_KEY')
    if secret_key:
        return secret_key
    if is_dev():
        return 'dev-secret-key'
    # A guessable key would let anyone forge a logged-in session cookie.
    raise RuntimeError("SECRET_KEY must be set outside development (APP_ENV=development)")


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

        yield

    app = FastAPI(lifespan=lifespan, docs_url=None, redoc_url=None, openapi_url=None)

    app.add_middleware(
        SessionMiddleware,
        secret_key=_secret_key(),
        max_age=SESSION_MAX_AGE_S,
        same_site='lax',
        https_only=not is_dev(),
    )

    @app.middleware("http")
    async def security_headers(request: Request, call_next):
        response = await call_next(request)
        response.headers.setdefault('X-Content-Type-Options', 'nosniff')
        response.headers.setdefault('Referrer-Policy', 'same-origin')
        response.headers.setdefault('X-Frame-Options', 'DENY')
        response.headers.setdefault('Content-Security-Policy', CONTENT_SECURITY_POLICY)
        if request.url.path.startswith('/assets/'):
            # Vite fingerprints these file names, so they never change in place.
            response.headers['Cache-Control'] = 'public, max-age=31536000, immutable'
        return response

    # Built frontend: hashed bundles under /assets, logo/favicon under /static.
    app.mount("/assets", StaticFiles(directory=str(DIST_DIR / "assets"), check_dir=False), name="assets")
    app.mount("/static", StaticFiles(directory=str(DIST_DIR / "static"), check_dir=False), name="static")

    # Import and register routers
    from .routes import api, auth, audio, export, pages

    # Initialize Google OAuth in production
    if not is_dev():
        auth.init_oauth(app)

    app.include_router(auth.router)
    app.include_router(pages.router)
    app.include_router(api.router)
    app.include_router(export.router)
    app.include_router(audio.router)

    # Exception handler for login required
    @app.exception_handler(auth.LoginRequired)
    async def handle_login_required(request, exc):
        # Path only, so the post-login redirect can't point at another host.
        next_url = request.url.path + (f"?{request.url.query}" if request.url.query else "")
        request.session['next_url'] = next_url
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
