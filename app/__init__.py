from flask import Flask
from pathlib import Path
from .services.analytics_service import AnalyticsService
import os
from dotenv import load_dotenv, dotenv_values 
from flask_oauthlib.client import OAuth
from .services.index import IndexManager
from .services.search import SearchService

load_dotenv() 

def create_app(data_dir: str, index_file: str = None):
    app = Flask(__name__)
    
    # Configure paths
    app.config['DATA_DIR'] = data_dir
    app.config['AUDIO_DIR'] = Path(data_dir) / "audio"
    app.config['INDEX_FILE'] = index_file
        
    # Configure PostHog
    app.config['POSTHOG_API_KEY'] = os.environ.get('POSTHOG_API_KEY', '')
    app.config['POSTHOG_HOST'] = os.environ.get('POSTHOG_HOST', 'https://app.posthog.com')
    app.config['DISABLE_ANALYTICS'] = os.environ.get('DISABLE_ANALYTICS', '').lower() in ('true', '1', 'yes')
    
    # Initialize analytics service
    analytics_service = AnalyticsService(
        api_key=app.config['POSTHOG_API_KEY'],
        host=app.config['POSTHOG_HOST'],
        disabled=app.config['DISABLE_ANALYTICS']
    )
    app.config['ANALYTICS_SERVICE'] = analytics_service
    
    app.config['MIME_TYPES'] = {'opus': 'audio/opus'}
    
    # Set secret key for session
    app.secret_key = os.environ.get('SECRET_KEY', 'dev-secret-key')
   
    # Initialize Google OAuth
    if os.environ.get('FLASK_ENV') != 'development':
        from .routes.auth import init_oauth, bp as auth_bp
        google = init_oauth(app)
        app.extensions['google_oauth'] = google
 
    # Register blueprints
    from .routes import main, search, auth, export, audio
    app.register_blueprint(main.bp)
    app.register_blueprint(search.bp)
    app.register_blueprint(auth.bp)
    app.register_blueprint(export.bp)
    app.register_blueprint(audio.bp)
        
    return app

def init_index_manager(app, file_records=None, index_file=None, force_reindex=False, db_type=None, **db_kwargs):
    """Initialize the index manager with the given parameters.
    
    Args:
        app: Flask application instance
        file_records: Optional list of FileRecord objects
        index_file: Optional path to index file
        force_reindex: Whether to force rebuilding the index
        db_type: Database type ("postgresql", "sqlite")
        **db_kwargs: Database-specific connection parameters
    """
    # Get database configuration from environment or use defaults
    if db_type is None:
        db_type = os.environ.get('DEFAULT_DB_TYPE', 'sqlite')
    
    # Set default database parameters if not provided
    if not db_kwargs:
        if db_type == "postgresql":
            db_kwargs = {
                "host": os.environ.get('POSTGRES_HOST', 'localhost'),
                "port": int(os.environ.get('POSTGRES_PORT', 5432)),
                "database": os.environ.get('POSTGRES_DB', 'transcripts'),
                "user": os.environ.get('POSTGRES_USER', 'postgres'),
                "password": os.environ.get('POSTGRES_PASSWORD', '')
            }
        elif db_type == "sqlite":
            db_kwargs = {
                "path": os.environ.get('SQLITE_PATH', 'explore.sqlite')
            }
    
    if index_file:
        # Load from flat index file
        index_mgr = IndexManager(index_path=index_file, db_type=db_type, **db_kwargs)
    elif file_records:
        # Build index from files
        index_mgr = IndexManager(file_records=file_records, db_type=db_type, **db_kwargs)
    else:
        raise ValueError("Either file_records or index_file must be provided")
    
    app.config['SEARCH_SERVICE'] = SearchService(index_mgr)
    return index_mgr

def register_error_handlers(app):
    @app.errorhandler(404)
    def handle_not_found(e):
        analytics = app.config.get('ANALYTICS_SERVICE')
        if analytics:
            analytics.capture_error('not_found', str(e))
        return 'Page not found', 404
        
    @app.errorhandler(500)
    def handle_server_error(e):
        analytics = app.config.get('ANALYTICS_SERVICE')
        if analytics:
            analytics.capture_error('server_error', str(e))
        return 'Internal server error', 500 
