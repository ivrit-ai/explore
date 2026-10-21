# Transcript Index Manager

This application provides functionality to manage and search through transcript data. It supports both building indexes on-the-fly and working with pre-generated flat index files.

## Installation

1. Clone the repository
2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

## Usage

### Command Line Interface

The application provides a CLI for managing index files:

1. Generate an index file from transcript data:
   ```bash
   python -m app.cli generate-index /path/to/data/dir explore-index.json.gz
   ```

2. Validate an existing index file:
   ```bash
   python -m app.cli validate-index explore-index.json.gz
   ```

### Running the Web Application

The backend is FastAPI; the UI is a React app in `frontend/` (Node 20+).

1. Backend, in dev mode (no Google login, listens on :5000):
   ```bash
   python run.py --data-dir data --dev
   ```

2. Frontend with hot reload on http://localhost:5173 (proxies the API to :5000):
   ```bash
   cd frontend && npm install && npm run dev
   ```

   Or build once (`npm run build`) and open http://localhost:5000 — FastAPI serves `frontend/dist`.

3. Tests:
   ```bash
   pip install -r requirements-dev.txt && pytest
   cd frontend && npm test
   ```

Production settings go in `.env` (see `.env.example`); deploy with `./deploy.sh`.

## Environment Variables

See `.env.example` for the full list. The important ones:

- `APP_ENV`: `development` skips Google login; anything else is production
- `SECRET_KEY`: Session signing key (required in production)
- `GOOGLE_CLIENT_ID` / `GOOGLE_CLIENT_SECRET`: Google OAuth credentials
- `SQLITE_PATH`: Path to the SQLite index (default `explore.sqlite`)
- `POSTHOG_API_KEY`: PostHog API key for analytics (optional)
- `POSTHOG_HOST`: PostHog host URL (optional)
- `DISABLE_ANALYTICS`: Set to "true" to disable analytics

## Directory Structure

- `app/`: FastAPI application
  - `services/`: Core services including index management
  - `routes/`: Page, JSON API, audio, export and auth routes
  - `cli.py`: Command line interface
- `frontend/`: React + TypeScript UI (Vite)
- `tests/`: Backend tests
- `data/`: Data directory
  - `json/`: Transcript JSON files
  - `audio/`: Audio files

See `ARCHITECTURE.md` for details.

## Data Format

The application expects JSON transcript files with the following structure:
```json
[
  {
    "start": 0.0,
    "text": "Transcript text segment"
  },
  ...
]
```

## License

This project is licensed under the MIT License. The data accessible through this application is licensed under the [ivrit.ai license](https://www.ivrit.ai/en/the-license/).

## Contact

For support, help, ideas, and contributions, please contact us at [info@ivrit.ai](mailto:info@ivrit.ai).
