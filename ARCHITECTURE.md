# Architecture — ivrit.ai Explore

> Full-text search engine for Hebrew podcast transcripts, powered by SQLite FTS5.

## High-Level Overview

```
┌─────────────────────────────────────────────────────────┐
│                     Web Browser                         │
│  React SPA (Vite + TS) — RTL Hebrew UI, player, filters │
└──────────────────────┬──────────────────────────────────┘
                       │ HTTPS
┌──────────────────────▼──────────────────────────────────┐
│                  FastAPI Application                     │
│                                                          │
│  ┌────────────────────────────────────────────────────┐  │
│  │              Routes (Blueprints)                    │  │
│  │  pages · api · audio · export · auth               │  │
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
| Language | Python 3 / TypeScript |
| Web framework | FastAPI (JSON API + serves the built frontend) |
| Frontend | React 18 + TypeScript, built with Vite; CSS modules; self-hosted fonts (Fontsource) |
| Database | SQLite with FTS5 extension |
| Search | FTS5 full-text indexing + Python `regex` post-filtering (time-bounded) |
| Auth | Google OAuth2 (Authlib), signed session cookie |
| Analytics | PostHog (optional, server-side only) |
| Audio | HTTP range requests; FFmpeg for clip export |
| Production server | uvicorn behind a TLS-terminating reverse proxy |
| Data parsing | orjson, pandas, duckdb |

## Directory Structure

```
├── app/
│   ├── __init__.py              # App factory: session, security headers, static mounts, routers
│   ├── spa.py                   # Serves frontend/dist/index.html
│   ├── cli.py                   # CLI for building/inspecting the index
│   ├── utils.py                 # FileRecord, transcript discovery, audio path resolution
│   ├── routes/
│   │   ├── pages.py             # GET /, /search, /privacy → React shell
│   │   ├── api.py               # /api/* JSON: search, metadata, segments, me
│   │   ├── params.py            # Shared, validated search query parameters
│   │   ├── audio.py             # Serve audio with HTTP range request support
│   │   ├── export.py            # /api/export/csv and /api/export/audio/<uuid>
│   │   └── auth.py              # Google OAuth login/logout, login dependencies
│   └── services/
│       ├── index.py             # IndexManager, TranscriptIndex (FTS5 queries)
│       ├── search.py            # SearchService (orchestrates search flow)
│       ├── db.py                # DatabaseService (SQLite connection, batching)
│       └── analytics_service.py # PostHog event tracking
├── frontend/
│   ├── src/
│   │   ├── api/                 # Typed fetch client + response types
│   │   ├── components/          # ui/ primitives, results/, player/, filters/, search/, layout/
│   │   ├── lib/                 # highlight, regex pattern, insights, URL state (unit-tested)
│   │   ├── player/              # PlayerProvider: one audio element shared by every result
│   │   ├── pages/               # Home, Results, Login, Privacy
│   │   └── styles/              # Design tokens + base styles
│   ├── public/static/           # Logo, favicon, Google logo
│   └── dist/                    # Build output (not committed; built by deploy.sh)
├── tests/                       # pytest: API, auth, validation, export
├── run.py                       # Server entry point (index check, uvicorn)
├── wsgi.py                      # ASGI entry point for external uvicorn workers
├── start.sh                     # Production startup script
├── deploy.sh                    # Pull, install, build frontend
├── .env.example                 # Required settings (names only)
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

All `/api/*` endpoints require login and answer `401` JSON when the session is missing.

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/`, `/search`, `/privacy`, `/login` | React app shell (pages require login except privacy and login) |
| `GET` | `/api/me` | Current user's email |
| `GET` | `/api/search` | Results grouped by episode; each hit carries segment text, timing, the match span and regex group spans |
| `GET` | `/api/search/metadata` | Source counts and date range for the query |
| `GET` | `/api/episodes/<idx>/segments?center=&radius=` | Transcript context around a segment (radius ≤ 10) |
| `GET` | `/api/export/csv` | CSV export of all results |
| `GET` | `/api/export/audio/<uuid>?start=&end=` | MP3 clip via FFmpeg (≤ 600 s) |
| `GET` | `/audio/<uuid>` | Episode audio (range requests) |
| `GET` | `/authorize`, `/login/authorized`, `/logout` | Google OAuth flow |

Search parameters: `q` (≤ 200 chars), `mode` (`exact`/`partial`/`regex`), `date_from`/`date_to` (`YYYY-MM-DD`),
repeated `source`, `page`, `per_page` (≤ 500), `shuffle`, `seed`.
An invalid regex returns `400`; a regex scan over its 5 s budget returns `422`.

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

- **React + TypeScript SPA** built by Vite into `frontend/dist`, served by FastAPI (same origin, no CORS).
- **State in the URL**: every results page is `/search?q=…&mode=…&source=…&seed=…`, so it can be shared and reloaded.
- **One player**: `PlayerProvider` owns a single audio element; result rows, the sticky bottom bar and the
  full-screen mobile player all drive it. Time updates live in a separate context so long lists don't re-render.
- **Highlighting** uses spans computed by the server (code points, relative to the segment), so exact, partial
  and regex (with numbered capture groups) all highlight what actually matched.
- **Responsive**: under 820px the filter sidebar becomes a bottom sheet and the player becomes a mini bar.
- **Dev**: `npm run dev` on :5173 proxies `/api`, `/audio` and the OAuth routes to the backend on :5000.

## Authentication

- **Production:** Google OAuth2 via Authlib. Page routes redirect to `/login`; `/api/*` returns `401`.
- **Session:** signed cookie, `SameSite=Lax`, HTTPS-only outside development, 14-day lifetime.
  The app refuses to start without `SECRET_KEY` unless `APP_ENV=development`.
- **Development:** `APP_ENV=development` with `TS_USER_EMAIL` logs that identity in automatically.

## Configuration

Settings are read from the environment or a `.env` file next to the app (see `.env.example`; `.env` is never committed).

| Variable | Purpose |
|----------|---------|
| `APP_ENV` | `development`, or production (anything else) |
| `SECRET_KEY` | Session signing key (required in production) |
| `GOOGLE_CLIENT_ID` / `GOOGLE_CLIENT_SECRET` | OAuth credentials |
| `SQLITE_PATH` | Path to SQLite database (default: `explore.sqlite`) |
| `TS_USER_EMAIL` | Dev-mode identity |
| `POSTHOG_API_KEY` / `POSTHOG_HOST` | Analytics (optional) |
| `DISABLE_ANALYTICS` | `true` to disable PostHog |

## Deployment

**Production** runs uvicorn via `start.sh` (port 8200) behind a reverse proxy that terminates TLS.

To deploy a new version on the server:

```bash
./deploy.sh            # or: ./deploy.sh <branch>
```

It checks that the required secrets exist (without printing them), requires Node 20+, pulls the branch,
installs Python dependencies and builds `frontend/dist`. Then restart the app (re-run `start.sh`).

Responses carry a strict Content-Security-Policy (same-origin scripts, styles, fonts and media only),
`X-Frame-Options: DENY` and `nosniff`. Hashed assets under `/assets` are cached for a year; the HTML shell is never cached.

**Index build** is a separate CLI step (`python -m app.cli build --data-dir <path>`) that must run before the app starts. The `--auto-build` flag on `run.py` can trigger it at startup.

## Data Scale

- ~34,580 indexed episodes from 78 sources
- ~33 million transcript segments
- Date range: 2007–2025
- Database size: ~6.4 GB
