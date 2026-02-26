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
   ┌─────▼──────┐              ┌───────▼────────┐
   │ Audio files │              │ Transcript JSON │
   │  (*.opus)   │              │  (*.json.gz)    │
   └─────────────┘              └────────────────┘
```

## Tech Stack

| Layer | Technology |
|-------|-----------|
| Language | Python 3 |
| Web framework | Flask 2.x + Jinja2 templates |
| Database | SQLite with FTS5 extension |
| Search | FTS5 full-text indexing + Python `regex` post-filtering |
| Auth | Google OAuth2 (flask-oauthlib) |
| Analytics | PostHog (optional) |
| Frontend | Vanilla JS + CSS (no framework), RTL Hebrew |
| Audio | HTML5 `<audio>` with HTTP range requests; FFmpeg for export |
| Production server | uWSGI with SSL (Let's Encrypt) |
| Data parsing | orjson, pandas, duckdb |

## Directory Structure

```
├── app/
│   ├── __init__.py              # Flask app factory (create_app, init_index_manager)
│   ├── cli.py                   # CLI for building/inspecting the index
│   ├── utils.py                 # FileRecord, transcript discovery, audio path resolution
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
├── run.py                       # Production entry point (SSL, logging)
├── wsgi.py                      # uWSGI entry point
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
                   Resolve audio file (.opus)
                          │
                          ▼
                   send_file() with range support
                          │
                          ▼
                   HTML5 <audio> seeks to start_time
```

Audio requests support HTTP 206 Partial Content for efficient seeking.

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
