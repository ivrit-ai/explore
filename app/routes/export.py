from fastapi import APIRouter, Request, Query, Depends, HTTPException
from starlette.responses import StreamingResponse
from ..routes.auth import require_api_login
from ..services.index import InvalidQuery, SearchTimeout
from ..utils import resolve_audio_path
from .params import SearchFilters, search_filters
from pathlib import Path
import io
import csv
import subprocess
import logging
import time
import uuid as uuid_module
from datetime import datetime

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/export")

# Context segment configuration for CSV exports
DEFAULT_CONTEXT_SEGMENTS_LENGTH = 5

# Longest audio clip a user can export in one request.
MAX_CLIP_SECONDS = 600
FFMPEG_TIMEOUT_S = 60


@router.get('/csv', name='export.export_results_csv')
def export_results_csv(
    request: Request,
    filters: SearchFilters = Depends(search_filters),
    user_email: str = Depends(require_api_login),
):
    start_time = time.time()

    search_service = request.app.state.search_service
    query = filters.q

    logger.info(f"Performing search for CSV export: {query} (mode: {filters.mode}, filters: date_from={filters.date_from}, date_to={filters.date_to}, sources={filters.sources})")
    try:
        hits, _ = search_service.search(query, search_mode=filters.mode,
                                        date_from=filters.date_from, date_to=filters.date_to,
                                        sources=filters.sources)
    except InvalidQuery:
        raise HTTPException(status_code=400, detail="לא ניתן לחפש את הביטוי הזה")
    except SearchTimeout:
        raise HTTPException(status_code=422, detail="הביטוי מורכב מדי והחיפוש נעצר. נסו ביטוי פשוט יותר.")

    index = search_service._index_mgr.get()

    # Target segment per hit, then one batch fetch for target + context segments
    targets = index.get_segments_at_offsets([(h.episode_idx, h.char_offset) for h in hits])
    segments_to_fetch = set()
    for h in hits:
        target = targets.get((h.episode_idx, h.char_offset))
        if not target:
            continue
        seg_idx = target["segment_id"]
        for offset in range(-DEFAULT_CONTEXT_SEGMENTS_LENGTH, DEFAULT_CONTEXT_SEGMENTS_LENGTH + 1):
            if seg_idx + offset >= 0:
                segments_to_fetch.add((h.episode_idx, seg_idx + offset))

    # get_segments_by_ids returns an unordered list; key it explicitly.
    segment_map = {(s["doc_id"], s["segment_id"]): s for s in index.get_segments_by_ids(list(segments_to_fetch))}
    docs = index.get_documents_batch(list({h.episode_idx for h in hits}))

    all_results = []
    for h in hits:
        target = targets.get((h.episode_idx, h.char_offset))
        if not target:
            continue
        seg_idx = target["segment_id"]
        context_parts = []
        for ctx_idx in range(seg_idx - DEFAULT_CONTEXT_SEGMENTS_LENGTH, seg_idx + DEFAULT_CONTEXT_SEGMENTS_LENGTH + 1):
            ctx_seg = segment_map.get((h.episode_idx, ctx_idx))
            if ctx_seg and ctx_seg.get('text'):
                context_parts.append(ctx_seg['text'])

        doc_info = docs.get(h.episode_idx, {})
        all_results.append({
            "episode_idx": h.episode_idx,
            "podcast_title": doc_info.get("source", ""),
            "date": doc_info.get("episode_date", ""),
            "episode_title": doc_info.get("episode_title", ""),
            "start": target.get('start_time', ''),
            "end": target.get('end_time', ''),
            "text": target.get('text', ''),
            "context": ' '.join(context_parts),
        })

    # Create CSV in memory with UTF-8 BOM for Excel compatibility
    output = io.StringIO()
    output.write('﻿')  # UTF-8 BOM
    writer = csv.writer(output, dialect='excel')

    # Write metadata as comments (info rows)
    writer.writerow(['# ivrit.ai Explore - Search Results Export'])
    writer.writerow(['# Query:', query])
    writer.writerow(['# Search Mode:', filters.mode])
    if filters.date_from:
        writer.writerow(['# Date From:', filters.date_from])
    if filters.date_to:
        writer.writerow(['# Date To:', filters.date_to])
    if filters.sources:
        writer.writerow(['# Sources Filter:', ', '.join(filters.sources)])
    writer.writerow(['# Total Results:', len(all_results)])
    writer.writerow(['# Exported:', datetime.now().strftime('%Y-%m-%d %H:%M:%S')])
    writer.writerow([])

    writer.writerow([
        'Episode Index', 'Date', 'Source', 'Episode',
        'Text', 'Context', 'Start Time', 'End Time'
    ])

    for r in all_results:
        writer.writerow([
            r['episode_idx'],
            r['date'],
            r['podcast_title'],
            r['episode_title'],
            r['text'],
            r['context'],
            r['start'],
            r['end'],
        ])

    execution_time = (time.time() - start_time) * 1000

    analytics = request.app.state.analytics
    if analytics:
        analytics.capture_export(
            export_type='csv',
            query=query,
            execution_time_ms=execution_time,
            url=str(request.url),
            user_email=user_email,
        )

    # ASCII-only filename; the query itself stays inside the file.
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    filename = f'ivrit_explore_{timestamp}.csv'

    return StreamingResponse(
        io.BytesIO(output.getvalue().encode('utf-8')),
        media_type='text/csv; charset=utf-8',
        headers={'Content-Disposition': f'attachment; filename="{filename}"'},
    )


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

    audio_dir = Path(request.app.state.audio_dir).resolve()
    audio_path = resolve_audio_path(episode_path, audio_dir)
    if not audio_path or not Path(audio_path).resolve().is_relative_to(audio_dir):
        raise HTTPException(status_code=404, detail="Audio not found")

    cmd = [
        'ffmpeg', '-nostdin', '-y',
        '-ss', f'{start:.3f}',
        '-to', f'{end:.3f}',
        '-i', audio_path,
        '-acodec', 'libmp3lame',
        '-ab', '64k',
        '-f', 'mp3',
        '-'
    ]
    try:
        proc = subprocess.run(cmd, capture_output=True, timeout=FFMPEG_TIMEOUT_S)
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
        headers={'Content-Disposition': f'attachment; filename="{download_name}"'},
    )
