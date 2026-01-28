import os
from pathlib import Path

from dotenv import dotenv_values, load_dotenv
from flask import Flask
from flask_oauthlib.client import OAuth

from .services.analytics_service import AnalyticsService
from .services.index import IndexManager
from .services.search import SearchService

load_dotenv()


def create_app(data_dir: str, index_file: str = None):
    app = Flask(__name__)

    # Configure paths
    app.config["DATA_DIR"] = data_dir
    app.config["AUDIO_DIR"] = Path(data_dir) / "audio"
    app.config["INDEX_FILE"] = index_file

    # Configure PostHog
    app.config["POSTHOG_API_KEY"] = os.environ.get("POSTHOG_API_KEY", "")
    app.config["POSTHOG_HOST"] = os.environ.get("POSTHOG_HOST", "https://app.posthog.com")
    app.config["DISABLE_ANALYTICS"] = os.environ.get("DISABLE_ANALYTICS", "").lower() in ("true", "1", "yes")

    # Initialize analytics service
    analytics_service = AnalyticsService(
        api_key=app.config["POSTHOG_API_KEY"], host=app.config["POSTHOG_HOST"], disabled=app.config["DISABLE_ANALYTICS"]
    )
    app.config["ANALYTICS_SERVICE"] = analytics_service

    app.config["MIME_TYPES"] = {"opus": "audio/opus"}

    # Set secret key for session
    app.secret_key = os.environ.get("SECRET_KEY", "dev-secret-key")

    # Initialize Google OAuth
    if os.environ.get("FLASK_ENV") != "development":
        from .routes.auth import bp as auth_bp
        from .routes.auth import init_oauth

        google = init_oauth(app)
        app.extensions["google_oauth"] = google

    # Register blueprints
    from .routes import audio, auth, export, main, search

    app.register_blueprint(main.bp)
    app.register_blueprint(search.bp)
    app.register_blueprint(auth.bp)
    app.register_blueprint(export.bp)
    app.register_blueprint(audio.bp)

    return app


def init_index_manager(app, **db_kwargs):
    """Initialize the index manager from an existing database.

    Args:
        app: Flask application instance
        **db_kwargs: Database-specific connection parameters (e.g., path)
    """
    import logging

    log = logging.getLogger(__name__)

    # Set default database parameters if not provided
    if not db_kwargs:
        db_kwargs = {"path": os.environ.get("SQLITE_PATH", "explore.sqlite")}

    db_path = Path(db_kwargs.get("path", "explore.sqlite"))

    # Check if database exists
    if not db_path.exists():
        log.error(f"Database not found: {db_path}")
        log.error("Please build the index first using: python -m app.cli build --data-dir <path>")
        raise FileNotFoundError(f"Database not found: {db_path}")

    # Load from existing database
    index_mgr = IndexManager(index_path=db_path)

    app.config["SEARCH_SERVICE"] = SearchService(index_mgr)
    return index_mgr


def register_error_handlers(app):
    @app.errorhandler(404)
    def handle_not_found(e):
        analytics = app.config.get("ANALYTICS_SERVICE")
        if analytics:
            analytics.capture_error("not_found", str(e))
        return "Page not found", 404

    @app.errorhandler(500)
    def handle_server_error(e):
        analytics = app.config.get("ANALYTICS_SERVICE")
        if analytics:
            analytics.capture_error("server_error", str(e))
        return "Internal server error", 500
