# Architecture — ivrit.ai Explore

> Full-text search engine for Hebrew podcast transcripts, powered by SQLite FTS5.

## High-Level Overview

```
┌─────────────────────────────────────────────────────────┐
│                     Web Browser                         │
│  (HTML/CSS/JS — RTL Hebrew UI, audio player, filters)   │
└──────────────────────┬──────────────────────────────────┘
                       │ HTTPS
┌──────────────────────▼──────────────────────────────────┐
│                   Flask Application                      │
│                                                          │
│  ┌────────────────────────────────────────────────────┐  │
│  │              Routes (Blueprints)                    │  │
│  │  main · search · audio · export · auth             │  │
│  └────────────────────┬───────────────────────────────┘  │
│                       │                                  │
│  ┌────────────────────▼───────────────────────────────┐  │
│  │              Services Layer                         │  │
│  │  SearchService · IndexManager · DatabaseService     │  │
│  │  AnalyticsService                                   │  │
│  └────────────────────┬───────────────────────────────┘  │
│                       │                                  │
│  ┌────────────────────▼───────────────────────────────┐  │
│  │         SQLite + FTS5  (explore.sqlite)             │  │
│  │  documents · documents_fts · segments               │  │
│  └────────────────────────────────────────────────────┘  │
└──────────────────────────────────────────────────────────┘
         │                              │
   ┌─────▼──────────┐          ┌───────▼────────┐
   │  Audio files   │          │ Transcript JSON │
   │   (*.opus)     │          │  (*.json.gz)    │
   │ local dir │ S3 │          │                 │
   └────────────────┘          └────────────────┘
```

## Dependency Pinning

`requirements.txt` pins exact versions. Templates call a Flask-compatible
`url_for` shim in `app/templating.py`, which finds a route by the `name=`
declared on its decorator. Starlette 1.0 stopped flattening included routers
into `app.routes`, so the shim walks the router tree; a flat scan silently
finds nothing and turns every page into a 500. When bumping versions, run the
backend differential test and the route smoke test, and confirm `url_for` still
resolves every name used in `app/templates/`.

## Tech Stack

| Layer | Technology |
|-------|-----------|
| Language | Python 3 |
| Web framework | FastAPI + Jinja2 templates |
| Database | SQLite with FTS5, or PostgreSQL with tsvector + GIN |
| Search | FTS5 full-text indexing + Python `regex` post-filtering |
| Auth | Google OAuth2 (Authlib) |
| Analytics | PostHog (optional) |
| Frontend | Vanilla JS + CSS (no framework), RTL Hebrew |
| Audio | HTML5 `<audio>` with HTTP range requests; FFmpeg for export |
| Audio storage | Local directory or S3 bucket (`app/services/audio_store.py`, boto3) |
| Production server | uvicorn behind a TLS-terminating reverse proxy |
| Data parsing | orjson, pandas, duckdb |

## Directory Structure

```
├── app/
│   ├── __init__.py              # App factory (create_app, init_index_manager)
│   ├── cli.py                   # CLI for building/inspecting the index
│   ├── utils.py                 # FileRecord, transcript discovery
│   ├── templating.py            # Flask-compatible url_for over Starlette routing
│   ├── routes/
│   │   ├── main.py              # GET / (home), GET /search (results), GET /search/metadata
│   │   ├── search.py            # JSON API: search hits, segment lookup
│   │   ├── audio.py             # Serve audio with HTTP range request support
│   │   ├── export.py            # CSV + audio segment export
│   │   └── auth.py              # Google OAuth login/logout
│   ├── services/
│   │   ├── index.py             # IndexManager, TranscriptIndex (FTS5 queries)
│   │   ├── search.py            # SearchService (orchestrates search flow)
│   │   ├── db.py                # DatabaseService (SQLite connection, batching)
│   │   └── analytics_service.py # PostHog event tracking
│   ├── templates/               # Jinja2: base.html, home.html, results.html, login.html
│   └── static/
│       ├── css/                 # style.css, results.css, login.css
│       ├── js/                  # results.js (audio player), filters.js (date/source filters)
│       └── img/                 # favicon, Google logo
├── Dockerfile                   # Web-tier image (app + ffmpeg; no data baked in)
├── run.py                       # Local/standalone entry point (uvicorn, logging)
├── wsgi.py                      # ASGI entry point used by the container
├── app.py                       # Development entry point
├── start.sh                     # uWSGI production startup script
├── explore.sqlite               # FTS5 database (~6.4 GB, ~35K docs, ~33M segments)
└── requirements.txt
```

## Database Schema

```
┌──────────────────────┐       ┌──────────────────────────┐
│     documents        │       │      documents_fts       │
│──────────────────────│       │   (FTS5 virtual table)   │
│ doc_id    PK INTEGER │◄──┐   │──────────────────────────│
│ uuid      UNIQUE     │   │   │ full_text   TEXT         │
│ source    VARCHAR     │   │   │ tokenize: unicode61      │
│ episode   VARCHAR     │   │   └──────────┬───────────────┘
│ episode_date DATE     │   │              │
│ episode_title TEXT    │   │   ┌──────────▼───────────────┐
└──────────────────────┘   │   │    fts_doc_mapping       │
                           │   │──────────────────────────│
                           │   │ fts_rowid  PK INTEGER    │
                           └───│ doc_id     FK → documents│
                               └──────────────────────────┘
┌──────────────────────────────┐
│          segments            │
│──────────────────────────────│
│ doc_id       FK → documents  │
│ segment_id   INTEGER (0-idx) │
│ segment_text TEXT             │
│ avg_logprob  DOUBLE          │
│ char_offset  INTEGER         │
│ start_time   DOUBLE (secs)   │
│ end_time     DOUBLE (secs)   │
└──────────────────────────────┘
```

**Key indexes:** `idx_segments_doc_id`, `idx_segments_char_offset`, `idx_segments_doc_id_segment_id` (composite), `idx_documents_uuid`, `idx_documents_date`, `idx_documents_source`.

## Core Data Flow

### Search

```
User query  ──►  main.search() route
                      │
                      ▼
              SearchService.search()
                      │
                      ▼
           TranscriptIndex.search_hits()
             ┌────────┼────────┐
             │        │        │
           exact   partial   regex
          (FTS5    (FTS5     (FTS5 candidate
          match)   LIKE)     + Python regex)
             │        │        │
             └────────┼────────┘
                      ▼
              List of (doc_id, char_offset)
                      │
                      ▼
            get_segment_at_offset()
            (resolve to segment with timing)
                      │
                      ▼
            Group by (source, episode)
            Apply date/source filters
            Paginate → render results.html
```

**Search modes:**
- **Exact** — FTS5 word-boundary matching.
- **Partial** — FTS5 candidate narrowing, then substring filter.
- **Regex** — FTS5 candidate narrowing, then full Python `regex` matching.

### Index Building

```
Transcript files (source/episode/full_transcript.json.gz)
                      │
       python -m app.cli build --data-dir <path>
                      │
                      ▼
        ThreadPoolExecutor (16 workers)
        Parse JSON (orjson) per episode
                      │
                      ▼
        Writer thread — chunked transactions
        (1000 docs / 30K segments per flush)
                      │
         ┌────────────┼────────────┐
         ▼            ▼            ▼
     documents    documents_fts  segments
         │            │            │
         └────────────┼────────────┘
                      ▼
           Recreate indexes + FTS5 optimize
                      │
                      ▼
              explore.sqlite (~6.4 GB)
```

### Audio Playback

```
Browser click  ──►  GET /audio/<doc_uuid>#t=<start>
                          │
                          ▼
                   Resolve UUID → episode path
                          │
                          ▼
                   AudioStore.response()
                    ├─ local: FileResponse (Starlette range support)
                    └─ s3:    GetObject with the Range header forwarded
                          │
                          ▼
                   HTML5 <audio> seeks to start_time
```

Audio requests support HTTP 206 Partial Content for efficient seeking. With the
S3 backend the browser's `Range` header is passed straight through to S3, so a
seek transfers only the bytes the player asked for.

### Search Index Backends

`app/services/index.py` (SQLite + FTS5) and `app/services/pg_index.py`
(PostgreSQL + tsvector) expose the same read surface, chosen by
`--index-backend {auto,sqlite,postgres}`. `auto` picks postgres when
`DATABASE_URL` is set. The Postgres index is populated out of band by
`scripts/migrate_sqlite_to_postgres.py`; the app never builds it at startup.

Both backends use the same two-stage search: the full-text index narrows to
candidate documents, then a second pass over those documents' text finds the
exact hit offsets. Only the second stage decides results, so the candidate
filter must never exclude a real match — it may over-include, at the cost of a
wasted scan.

Where that second stage runs differs. SQLite pulls each candidate document's
text into the app and scans it with Python's `regex`. Postgres does it in SQL
for exact and partial mode, splitting the text on the search term and turning
the gap lengths into offsets in one pass per document, so only `(doc_id,
offset)` pairs cross the wire and no transcript text is transferred at all. At
the default page of 20 documents that is about 1 MB saved per search, and at
the 5000-document maximum about 266 MB.

User-supplied regexes stay in Python. Its dialect is not Postgres' Advanced
Regular Expressions — `\b` is a word boundary in one and a backspace in the
other — so running them server-side would quietly change what a pattern means.

That constraint shapes exact (phrase) search on Postgres. tsvector positions
are held in 14 bits, so a document long enough to pass position 16383 has
every later word collapsed onto that position and cannot answer a phrase
query at all — matches late in a long transcript are silently missed. Exact
search therefore runs two branches:

| Documents | Branch | Index |
|-----------|--------|-------|
| positions intact (~99%) | `phraseto_tsquery`, a true phrase match | `documents_tsv_gin` |
| positions clamped (~1%) | tokens ANDed, position-free superset | `documents_tsv_overflow_gin` (partial) |

The planner reads this as a `BitmapOr` of the two GIN scans. The
`tsv_overflow` flag is measured after loading, from the highest position
actually stored, rather than guessed from text length. Degrading the whole
corpus to the AND filter instead would scan roughly 1.7x more documents on
average and up to 6x for some phrases, which also thins out result pages,
since pagination walks candidate documents.

### Audio Storage Backends

`app/services/audio_store.py` puts one interface in front of both layouts:

| Method | Local | S3 |
|--------|-------|-----|
| `response()` | `FileResponse` | streamed `GetObject`, Range forwarded |
| `ffmpeg_input()` | filesystem path | presigned URL (ffmpeg range-reads it) |
| `local_copy()` | filesystem path | temp download, deleted on exit |

The backend is chosen by `--audio-source {auto,local,s3}` (default `auto`: S3
when a bucket is configured, otherwise the local `audio/` directory). Episode
keys mirror the local layout, so `audio/<source>/<episode>.opus` on disk is
`s3://<bucket>/<prefix>/<source>/<episode>.opus` in the bucket. Credentials use
the standard boto3 chain (environment, `~/.aws`, instance role) and are never
read from application config.

Segment export runs FFmpeg against a presigned URL, which range-reads only the
part it needs. If that FFmpeg build lacks HTTPS support the route retries once
against a temporary local copy.

## API Endpoints

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/` | Home page with search form |
| `GET` | `/search` | Search results page (HTML, paginated) |
| `GET` | `/search/metadata` | JSON: available sources + date range for current query |
| `GET` | `/search/` | JSON search API |
| `POST` | `/search/segment` | Batch segment lookup by char_offset |
| `POST` | `/search/segment/by_idx` | Batch segment lookup by segment_idx |
| `GET` | `/audio/<doc_uuid>` | Serve audio file (range requests) |
| `GET` | `/export/results` | CSV export of search results |
| `GET` | `/export/segment/<source>/<path>` | Audio segment MP3 via FFmpeg |
| `GET` | `/login` | Google OAuth login |
| `GET` | `/authorize` | OAuth redirect |
| `GET` | `/logout` | Clear session |

## Key Services

### `IndexManager` (`app/services/index.py`)
Owns the SQLite database. Handles index building (bulk inserts with chunked transactions, dropping/recreating indexes for speed) and wraps a `TranscriptIndex` for querying.

### `TranscriptIndex` (`app/services/index.py`)
Database-agnostic query interface. Implements the three search modes against FTS5, segment retrieval, and document metadata lookups.

### `SearchService` (`app/services/search.py`)
Stateless orchestrator. Accepts query parameters, delegates to `TranscriptIndex`, and returns enriched `SearchHit` results.

### `DatabaseService` (`app/services/db.py`)
Low-level SQLite abstraction. Thread-local connections, WAL mode, 512 MB cache, batch inserts respecting `SQLITE_MAX_VARIABLE_NUMBER`.

### `AnalyticsService` (`app/services/analytics_service.py`)
PostHog wrapper. Tracks searches, page views, exports, and errors. Can be disabled via env var.

## Frontend

- **No JS framework** — vanilla JavaScript with two main modules:
  - `results.js` — audio player management (lazy load queue, single-instance playback), batch segment fetching with caching.
  - `filters.js` — date range pickers, multi-select source filter, URL parameter sync.
- **RTL layout** — `lang="he" dir="rtl"`, Rubik font for Hebrew text.
- **Templates** — Jinja2 with `base.html` layout, partials for pagination.

## Authentication

- **Production:** Google OAuth2 via flask-oauthlib. `@login_required` decorator on all content routes.
- **Development:** Bypassed when `FLASK_ENV=development`; uses `TS_USER_EMAIL` env var as mock identity.

## Configuration

| Variable | Purpose |
|----------|---------|
| `SQLITE_PATH` | Path to SQLite database (default: `explore.sqlite`) |
| `FLASK_ENV` | `development` / `production` |
| `SECRET_KEY` | Flask session secret |
| `GOOGLE_CLIENT_ID` / `GOOGLE_CLIENT_SECRET` | OAuth credentials |
| `TS_USER_EMAIL` | Dev-mode email bypass |
| `POSTHOG_API_KEY` / `POSTHOG_HOST` | Analytics (optional) |
| `DISABLE_ANALYTICS` | `true` to disable PostHog |

## Deployment

**Production** runs via uWSGI (`start.sh`) with:
- 2 processes, 4 threads per process
- HTTPS with Let's Encrypt certificates
- 30-second request timeout (harakiri)

**Index build** is a separate CLI step (`python -m app.cli build --data-dir <path>`) that must run before the app starts. The `--auto-build` flag on `run.py` can trigger it at startup.

## Data Scale

- ~34,580 indexed episodes from 78 sources
- ~33 million transcript segments
- Date range: 2007–2025
- Database size: ~6.4 GB
