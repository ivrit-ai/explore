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

### Production (Docker)

The `Dockerfile` is multi-stage: a Node stage builds `frontend/dist`, and the
Python stage serves it together with the API. The image holds no data — the
index lives in PostgreSQL and the audio in S3, both configured through the
environment below.

## Search Index

Two backends expose the same read surface; `EXPLORE_INDEX_BACKEND`
(`auto`, `sqlite` or `postgres`) picks one. `auto` chooses Postgres when
`DATABASE_URL` is set.

- **SQLite** (local development): build with `python -m app.cli build --data-dir <dir>`.
- **PostgreSQL** (production): populated out of band by
  `scripts/migrate_sqlite_to_postgres.py`. The app connects over
  `DATABASE_URL_DIRECT` when set (it bypasses a transaction pooler), otherwise
  `DATABASE_URL`.

## Audio Storage

Episode audio is read either from a local `audio/` directory or from an S3
bucket. The default (`auto`) picks S3 when a bucket is configured and the local
directory otherwise, so existing local setups keep working untouched.

```bash
# Local audio directory (the historical behaviour)
python run.py --data-dir /home/data/explore

# S3, no local audio needed
python run.py --audio-source s3 --s3-bucket explore-datastore
```

AWS credentials come from the standard boto3 chain: environment variables,
`~/.aws/credentials`, or an instance role. They are never read from app config.

The bucket mirrors the local layout, so `audio/<source>/<episode>.opus` on disk
becomes `s3://<bucket>/<prefix>/<source>/<episode>.opus`.

## Authentication

Sign-in is provided by the hosting platform, which serves `/xhost-auth/login`
and `/xhost-auth/logout` and returns a signed identity cookie that the app
verifies. There is no OAuth client ID, client secret or session key to
configure. Pages redirect to the platform sign-in when logged out; `/api/*`
answers 401 and the React app sends the user to its `/login` page, whose
button starts the platform sign-in.

For local development, set `APP_ENV=development` and `TS_USER_EMAIL` to bypass
sign-in.

## Environment Variables

See `.env.example` for a template. The important ones:

- `APP_ENV`: `development` bypasses sign-in (with `TS_USER_EMAIL`); anything else is production
- `EXPLORE_AUTH_AUDIENCES`: Comma-separated hostnames whose identity tokens this
  deployment accepts (needed for a custom domain; defaults to the request host)
- `EXPLORE_INDEX_BACKEND`: `auto` (default), `sqlite` or `postgres`
- `DATABASE_URL` / `DATABASE_URL_DIRECT`: Postgres index connection (injected by the host)
- `SQLITE_PATH`: Path to the SQLite index (default `explore.sqlite`)
- `EXPLORE_AUDIO_SOURCE`: `auto` (default), `local` or `s3`
- `EXPLORE_S3_BUCKET`: Bucket holding the audio tree
- `EXPLORE_S3_PREFIX`: Key prefix within the bucket (default `audio`)
- `EXPLORE_S3_REGION`: Bucket region (falls back to `AWS_REGION`/`AWS_DEFAULT_REGION`)
- `EXPLORE_S3_ENDPOINT_URL`: Custom endpoint for S3-compatible storage (optional)
- `EXPLORE_S3_PRESIGN_TTL`: Lifetime in seconds of presigned URLs handed to FFmpeg (default 3600)
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
  - `audio/`: Audio files (optional when using the S3 backend)

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
