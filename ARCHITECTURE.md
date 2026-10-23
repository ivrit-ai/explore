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
   ┌─────▼──────────┐          ┌───────▼────────┐
   │  Audio files   │          │ Transcript JSON │
   │   (*.opus)     │          │  (*.json.gz)    │
   │ local dir │ S3 │          │                 │
   └────────────────┘          └────────────────┘
```

## Dependency Pinning

`requirements.txt` pins exact versions. When bumping versions, run the
backend differential test, the route smoke test and the pytest suite.

## Tech Stack

| Layer | Technology |
|-------|-----------|
| Language | Python 3 / TypeScript |
| Web framework | FastAPI (JSON API + serves the built frontend) |
| Frontend | React 18 + TypeScript, built with Vite; CSS modules; self-hosted fonts (Fontsource) |
| Database | SQLite with FTS5, or PostgreSQL with tsvector + GIN |
| Search | Full-text candidate filter + exact offsets (SQL for exact/partial on Postgres, Python `regex` otherwise, time-bounded) |
| Auth | Platform-provided Google sign-in; RS256 cookie verified in-app (PyJWT) |
| Analytics | PostHog (optional, server-side only) |
| Audio | HTTP range requests; FFmpeg for clip export |
| Audio storage | Local directory or S3 bucket (`app/services/audio_store.py`, boto3) |
| Production server | uvicorn behind a TLS-terminating reverse proxy |
| Data parsing | orjson |

## Directory Structure

```
├── app/
│   ├── __init__.py              # App factory: security headers, static mounts, routers, init_index_manager
│   ├── spa.py                   # Serves frontend/dist/index.html
│   ├── cli.py                   # CLI for building/inspecting the index
│   ├── utils.py                 # FileRecord, transcript discovery
│   ├── routes/
│   │   ├── pages.py             # GET /, /search, /privacy → React shell
│   │   ├── api.py               # /api/* JSON: search, metadata, segments, me
│   │   ├── params.py            # Shared, validated search query parameters
│   │   ├── audio.py             # Serve audio with HTTP range request support
│   │   ├── export.py            # /api/export/csv and /api/export/audio/<uuid>
│   │   └── auth.py              # Platform sign-in: identity cookie verification, login dependencies
│   └── services/
│       ├── index.py             # IndexManager, TranscriptIndex (FTS5 queries)
│       ├── pg_index.py          # PostgresIndexManager, PostgresTranscriptIndex (tsvector + GIN)
│       ├── audio_store.py       # Local / S3 audio backends
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
│   └── dist/                    # Build output (not committed; built in the Docker image or by deploy.sh)
├── tests/                       # pytest: API, auth, validation, export
├── Dockerfile                   # Multi-stage: Node builds the frontend, Python image serves it (no data baked in)
├── run.py                       # Local/standalone entry point (uvicorn, logging)
├── wsgi.py                      # ASGI entry point used by the container
├── app.py                       # Development entry point
├── start.sh                     # Startup script for running from a checkout
├── deploy.sh                    # Checkout deploy: pull, install, build frontend
├── .env.example                 # Settings template (names only)
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

### Authentication

Sign-in is the hosting platform's. It serves the reserved
`/xhost-auth/login`, `/xhost-auth/logout` and `/xhost-auth/whoami` paths, runs
the Google flow itself, and returns an RS256-signed JWT in the reserved
`__Host-xhost_id` cookie. The platform does not tell the app who is calling, so
`app/routes/auth.py` verifies that cookie and decides access itself.

Verification pins `RS256` and selects the key by the token's `kid` rather than
trusting the token's own `alg`, which is what defeats an algorithm-confusion
forgery, and checks issuer, audience and expiry. Keys come from the platform's
JWKS endpoint, cached by the client. `sub` is the stable identifier; emails can
change.

The audience is one exact hostname. `EXPLORE_AUTH_AUDIENCES` lists the
hostnames a deployment accepts, which a custom domain needs; unset, it falls
back to the host the request arrived on.

An unauthenticated request to a gated route redirects to
`/xhost-auth/login?return_to=<original path>`, so `/` answers 3xx and still
satisfies the deploy health probe. There is no app-side session or
client secret to hold.

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
| `GET` | `/authorize`, `/logout` | Hand off to the platform sign-in / sign-out (`/xhost-auth/*`) |

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
- **Dev**: `npm run dev` on :5173 proxies `/api`, `/audio` and the auth routes to the backend on :5000.

## Authentication Summary

- **Production:** the hosting platform's Google sign-in; the app verifies the
  signed identity cookie itself. Page routes redirect to `/xhost-auth/login`;
  `/api/*` returns `401` and the React app shows `/login`, whose button starts
  the platform sign-in. See **Authentication** above for the verification rules.
- **No app session:** there is no session cookie and no `SECRET_KEY`.
- **Development:** bypassed when `APP_ENV=development`, using `TS_USER_EMAIL`
  as the mock identity.

## Configuration

Settings are read from the environment or a `.env` file next to the app (see `.env.example`; `.env` is never committed).

| Variable | Purpose |
|----------|---------|
| `DATABASE_URL` / `DATABASE_URL_DIRECT` | Postgres index connection (injected by the host; DIRECT bypasses the pooler) |
| `EXPLORE_INDEX_BACKEND` | `auto` (default) / `sqlite` / `postgres` |
| `SQLITE_PATH` | SQLite index path when that backend is used (default: `explore.sqlite`) |
| `EXPLORE_AUDIO_SOURCE` | `auto` (default) / `local` / `s3` |
| `EXPLORE_S3_BUCKET` / `EXPLORE_S3_PREFIX` | Bucket holding the audio tree |
| `EXPLORE_S3_REGION` / `EXPLORE_S3_ENDPOINT_URL` | S3 region and custom endpoint (optional) |
| `EXPLORE_S3_PRESIGN_TTL` | Lifetime of presigned URLs given to FFmpeg (default 3600) |
| `EXPLORE_AUTH_AUDIENCES` | Hostnames whose identity tokens are accepted (defaults to the request host) |
| `APP_ENV` | `development` / `production` |
| `TS_USER_EMAIL` | Dev-mode email bypass |
| `POSTHOG_API_KEY` / `POSTHOG_HOST` | Analytics (optional) |
| `DISABLE_ANALYTICS` | `true` to disable PostHog |

## Deployment

**Production** runs the `Dockerfile` image: a Node stage builds `frontend/dist`,
then uvicorn serves `wsgi:app` on the port the host injects, with ffmpeg for
clip export. Nothing large is baked in — audio comes from S3 and the index from
Postgres, both configured by environment variable. TLS is terminated by the
platform.

`start.sh` / `deploy.sh` still run the app from a checkout for local or
single-machine use (`deploy.sh` requires Node 20+ and builds `frontend/dist`).

Responses carry a strict Content-Security-Policy (same-origin scripts, styles, fonts and media only),
`X-Frame-Options: DENY` and `nosniff`. Hashed assets under `/assets` are cached for a year; the HTML shell is never cached.

**Index build** is a separate CLI step (`python -m app.cli build --data-dir <path>`) that must run before the app starts. The `--auto-build` flag on `run.py` can trigger it at startup.

## Data Scale

- ~34,580 indexed episodes from 78 sources
- ~33 million transcript segments
- Date range: 2007–2025
- Database size: ~6.4 GB
