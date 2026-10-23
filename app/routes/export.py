from fastapi import APIRouter, Request, Query, Depends, HTTPException
from starlette.responses import StreamingResponse
from ..routes.auth import require_api_login
from ..services.index import InvalidQuery, SearchTimeout
from .api import query_errors
from .params import SearchFilters, search_filters
import io
import csv
import subprocess
import logging
import random
import time
import re
import unicodedata
import uuid as uuid_module
from datetime import datetime
from urllib.parse import quote

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/export")

def _content_disposition(filename: str) -> str:
    """Build a Content-Disposition value that survives non-ASCII filenames.

    Header values are latin-1 encoded, so a Hebrew episode name raises on the
    way out. Send an ASCII-folded name for old clients plus the RFC 5987
    ``filename*`` form that carries the real UTF-8 name.
    """
    folded = unicodedata.normalize('NFKD', filename).encode('ascii', 'ignore').decode()
    folded = re.sub(r'[^\w.\-]+', '_', folded).strip('_') or 'download'
    return f"attachment; filename=\"{folded}\"; filename*=UTF-8''{quote(filename, safe='')}"



# Context segment configuration for CSV exports
DEFAULT_CONTEXT_SEGMENTS_LENGTH = 5

# Longest audio clip a user can export in one request.
MAX_CLIP_SECONDS = 600
FFMPEG_TIMEOUT_S = 60


# CSV export limits. A broad term (e.g. a one-letter partial search) matches
# millions of times; building all of that in memory killed the worker and no
# spreadsheet could open the result. Exports are a balanced sample instead.
CSV_MAX_ROWS = 100_000
CSV_TIME_BUDGET_S = 60.0
CSV_DOC_BATCH = 100


def _spread(items: list, k: int) -> list:
    """k items evenly spaced across the list (all of them when k >= len)."""
    n = len(items)
    if k >= n:
        return items
    return [items[(i * n) // k] for i in range(k)]


def _csv_line(row) -> str:
    buf = io.StringIO()
    csv.writer(buf, dialect='excel').writerow(row)
    return buf.getvalue()


def _sample_batches(search_service, filters: SearchFilters, total_docs: int, seed: int,
                    max_rows: int, time_budget_s: float, doc_batch: int, stats: dict):
    """Yield lists of (hit, doc_info, target_segment, context) for a balanced sample.

    Episodes are visited in shuffled order, a batch at a time. Each episode's
    share is (rows left) / (episodes left), so capacity a small episode leaves
    unused rolls over to later ones. Sampling stops at max_rows or when the
    time budget runs out; since the order is shuffled, the episodes covered by
    then are still a random sample.
    """
    index = search_service._index_mgr.get()
    deadline = time.monotonic() + time_budget_s
    offset = 0
    while stats['rows'] < max_rows:
        hits, has_more = search_service.search(
            filters.q, search_mode=filters.mode, date_from=filters.date_from,
            date_to=filters.date_to, sources=filters.sources,
            doc_limit=doc_batch, doc_offset=offset, seed=seed)
        offset += doc_batch

        by_doc: dict[int, list] = {}
        for h in hits:
            by_doc.setdefault(h.episode_idx, []).append(h)

        picked = []
        batch_docs = list(by_doc.values())
        for i, doc_hits in enumerate(batch_docs):
            stats['episodes_seen'] += 1
            stats['hits_seen'] += len(doc_hits)
            room = max_rows - stats['rows'] - len(picked)
            if room <= 0:
                break
            # The metadata count can be missing (it reports 0 for some regexes),
            # so never assume fewer episodes remain than this batch and, when
            # there is more, at least one further batch.
            seen_here = len(batch_docs) - i + (doc_batch if has_more else 0)
            docs_left = max(1, total_docs - stats['episodes_seen'] + 1, seen_here)
            share = max(1, -(-room // docs_left))  # ceil division
            take = _spread(doc_hits, min(share, room))
            stats['episodes_sampled'] += 1
            picked.extend(take)

        rows = []
        if picked:
            targets = index.get_segments_at_offsets([(h.episode_idx, h.char_offset) for h in picked])
            wanted = set()
            for h in picked:
                t = targets.get((h.episode_idx, h.char_offset))
                if t:
                    sid = t["segment_id"]
                    wanted.update(
                        (h.episode_idx, sid + d)
                        for d in range(-DEFAULT_CONTEXT_SEGMENTS_LENGTH, DEFAULT_CONTEXT_SEGMENTS_LENGTH + 1)
                        if sid + d >= 0)
            # get_segments_by_ids returns an unordered list; key it explicitly.
            seg_map = {(x["doc_id"], x["segment_id"]): x for x in index.get_segments_by_ids(list(wanted))}
            docs = index.get_documents_batch(list({h.episode_idx for h in picked}))
            for h in picked:
                t = targets.get((h.episode_idx, h.char_offset))
                if not t:
                    continue
                sid = t["segment_id"]
                context = ' '.join(
                    seg_map[(h.episode_idx, i)]['text']
                    for i in range(sid - DEFAULT_CONTEXT_SEGMENTS_LENGTH, sid + DEFAULT_CONTEXT_SEGMENTS_LENGTH + 1)
                    if seg_map.get((h.episode_idx, i), {}).get('text'))
                rows.append((h, docs.get(h.episode_idx, {}), t, context))
            stats['rows'] += len(rows)
        yield rows

        if not has_more:
            stats['complete'] = stats['rows'] >= stats['hits_seen']
            stats['capped'] = not stats['complete'] and stats['rows'] >= max_rows
            return
        if time.monotonic() > deadline:
            stats['timed_out'] = True
            return
    stats['capped'] = True


@router.get('/csv', name='export.export_results_csv')
def export_results_csv(
    request: Request,
    filters: SearchFilters = Depends(search_filters),
    user_email: str = Depends(require_api_login),
):
    start_time = time.time()
    search_service = request.app.state.search_service
    query = filters.q
    logger.info(f"CSV export: {query} (mode: {filters.mode}, date_from={filters.date_from}, "
                f"date_to={filters.date_to}, sources={filters.sources})")

    seed = random.randint(0, 2**31 - 1)
    stats = {'rows': 0, 'episodes_seen': 0, 'episodes_sampled': 0, 'hits_seen': 0,
             'complete': False, 'capped': False, 'timed_out': False}
    # Run the metadata query and the first batch before the response starts,
    # so a bad query still answers 400/422 instead of a truncated file.
    def start():
        meta = search_service.metadata(query, filters.mode, filters.date_from, filters.date_to, filters.sources)
        total_docs = int(meta.get('total_docs') or 0)
        batches = _sample_batches(search_service, filters, total_docs, seed,
                                  CSV_MAX_ROWS, CSV_TIME_BUDGET_S, CSV_DOC_BATCH, stats)
        return total_docs, batches, next(batches, [])

    total_docs, batches, first = query_errors(start)

    header = [
        ['# ivrit.ai Explore - Search Results Export'],
        ['# Query:', query],
        ['# Search Mode:', filters.mode],
    ]
    if filters.date_from:
        header.append(['# Date From:', filters.date_from])
    if filters.date_to:
        header.append(['# Date To:', filters.date_to])
    if filters.sources:
        header.append(['# Sources Filter:', ', '.join(filters.sources)])
    header += [
        ['# Matching Episodes:', total_docs if total_docs else 'unknown'],
        ['# Row Limit:', f'{CSV_MAX_ROWS} (balanced sample across episodes; see the summary at the end)'],
        ['# Sample Seed:', seed],
        ['# Exported:', datetime.now().strftime('%Y-%m-%d %H:%M:%S')],
        [],
        ['Episode Index', 'Date', 'Source', 'Episode', 'Text', 'Context', 'Start Time', 'End Time'],
    ]

    def row_lines(rows):
        for h, doc, t, context in rows:
            yield _csv_line([h.episode_idx, doc.get('episode_date', ''), doc.get('source', ''),
                             doc.get('episode_title', ''), t.get('text', ''), context,
                             t.get('start_time', ''), t.get('end_time', '')])

    def body():
        yield '﻿'  # UTF-8 BOM so Excel detects the encoding
        for line in header:
            yield _csv_line(line)
        yield from row_lines(first)
        try:
            for rows in batches:
                yield from row_lines(rows)
        except (InvalidQuery, SearchTimeout) as exc:
            logger.warning("CSV export stopped early: %s", exc)
            stats['timed_out'] = True

        if stats['complete']:
            note = 'all matches exported'
        elif stats['capped']:
            note = f'row limit reached; sampled evenly across {stats["episodes_sampled"]} episodes'
        elif stats['timed_out']:
            of_total = f' of {total_docs}' if total_docs else ''
            note = (f'time limit reached; sampled {stats["episodes_sampled"]}{of_total} '
                    f'matching episodes in random order')
        else:
            note = f'sampled across {stats["episodes_sampled"]} episodes'
        yield _csv_line([])
        yield _csv_line(['# Summary:', f'{stats["rows"]} rows exported of {stats["hits_seen"]} matches seen - {note}'])

        analytics = request.app.state.analytics
        if analytics:
            analytics.capture_export(export_type='csv', query=query,
                                     execution_time_ms=(time.time() - start_time) * 1000,
                                     url=str(request.url), user_email=user_email)

    def encoded():
        for chunk in body():
            yield chunk.encode('utf-8')

    # ASCII-only filename; the query itself stays inside the file.
    filename = f'ivrit_explore_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv'
    return StreamingResponse(
        encoded(),
        media_type='text/csv; charset=utf-8',
        headers={'Content-Disposition': _content_disposition(filename)},
    )


def _transcode(audio_input: str, start: float, end: float) -> subprocess.CompletedProcess:
    """Cut [start, end) out of a path or URL as 64kbps MP3 on stdout.

    Raises subprocess.TimeoutExpired after FFMPEG_TIMEOUT_S.
    """
    cmd = [
        'ffmpeg', '-nostdin', '-y',
        '-ss', f'{start:.3f}',
        '-to', f'{end:.3f}',
        '-i', audio_input,
        '-acodec', 'libmp3lame',
        '-ab', '64k',
        '-f', 'mp3',
        '-'
    ]
    return subprocess.run(cmd, capture_output=True, timeout=FFMPEG_TIMEOUT_S)


@router.get('/audio/{doc_uuid}', name='export.export_audio')
def export_audio(
    request: Request,
    doc_uuid: str,
    start: float = Query(..., ge=0),
    end: float = Query(..., gt=0),
    user_email: str = Depends(require_api_login),
):
    """Cut [start, end) out of an episode's audio and return it as MP3."""
    try:
        uuid_module.UUID(doc_uuid)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid UUID")
    if end <= start:
        raise HTTPException(status_code=400, detail="End time must be greater than start time")
    if end - start > MAX_CLIP_SECONDS:
        raise HTTPException(status_code=400, detail=f"Clips are limited to {MAX_CLIP_SECONDS} seconds")

    index = request.app.state.search_service._index_mgr.get()
    try:
        episode_path = index.get_episode_by_uuid(doc_uuid)
    except IndexError:
        raise HTTPException(status_code=404, detail="Episode not found")

    # Audio comes from the configured backend (local directory or S3); the
    # store rejects traversal in the episode key itself.
    store = request.app.state.audio_store
    try:
        try:
            with store.ffmpeg_input(episode_path) as audio_input:
                proc = _transcode(audio_input, start, end)
        except FileNotFoundError:
            raise HTTPException(status_code=404, detail="Audio not found")

        if proc.returncode != 0 and store.is_remote:
            # A remote backend hands ffmpeg a URL, which needs an ffmpeg built
            # with HTTPS support. Fall back to a local copy before giving up.
            logger.warning(f"FFmpeg failed on remote input for {doc_uuid}: "
                           f"{proc.stderr.decode(errors='replace')[-2000:]}")
            try:
                with store.local_copy(episode_path) as audio_path:
                    proc = _transcode(audio_path, start, end)
            except FileNotFoundError:
                raise HTTPException(status_code=404, detail="Audio not found")
    except subprocess.TimeoutExpired:
        logger.error(f"FFmpeg timed out exporting {doc_uuid} [{start}-{end}]")
        raise HTTPException(status_code=504, detail="Audio export timed out")
    if proc.returncode != 0:
        logger.error(f"FFmpeg error for {doc_uuid}: {proc.stderr.decode(errors='replace')[-2000:]}")
        raise HTTPException(status_code=500, detail="Error processing audio")

    analytics = request.app.state.analytics
    if analytics:
        analytics.capture_export(
            export_type='audio',
            query=doc_uuid,
            execution_time_ms=0,
            url=str(request.url),
            user_email=user_email,
        )

    download_name = f'ivrit_{doc_uuid[:8]}_{start:.1f}-{end:.1f}.mp3'
    return StreamingResponse(
        io.BytesIO(proc.stdout),
        media_type='audio/mpeg',
        headers={'Content-Disposition': _content_disposition(download_name)},
    )
