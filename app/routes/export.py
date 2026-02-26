from fastapi import APIRouter, Request, Query, Depends, HTTPException
from starlette.responses import StreamingResponse
from ..routes.auth import require_login
from ..utils import resolve_audio_path
import io
import csv
import subprocess
import logging
import time
import re
from datetime import datetime

logger = logging.getLogger(__name__)

router = APIRouter()

# Context segment configuration for CSV exports
DEFAULT_CONTEXT_SEGMENTS_LENGTH = 5


@router.get('/export/results', name='export.export_results_csv')
def export_results_csv(
    request: Request,
    q: str = Query(''),
    search_mode: str = Query('exact'),
    date_from: str = Query(''),
    date_to: str = Query(''),
    sources: str = Query(''),
    user_email: str = Depends(require_login),
):
    start_time = time.time()

    search_service = request.app.state.search_service

    query = q.strip()
    if not query:
        raise HTTPException(status_code=400, detail="Missing query parameter")

    # Validate search mode
    if search_mode not in ['exact', 'partial', 'regex']:
        search_mode = 'exact'

    # Get filter parameters
    date_from_val = date_from.strip() or None
    date_to_val = date_to.strip() or None
    sources_param = sources.strip()
    sources_list = [s.strip() for s in sources_param.split(',') if s.strip()] if sources_param else None

    # Perform search with filters
    logger.info(f"Performing search for CSV export: {query} (mode: {search_mode}, filters: date_from={date_from_val}, date_to={date_to_val}, sources={sources_list})")
    hits, _ = search_service.search(query, search_mode=search_mode,
                                 date_from=date_from_val, date_to=date_to_val, sources=sources_list)

    # Enrich hits with segment info
    all_results = []
    index = search_service._index_mgr.get()

    # Build a set of all segments we need (target + context)
    segments_to_fetch = []
    hit_to_segments_map = {}

    for i, hit in enumerate(hits):
        seg = search_service.segment(hit)
        context_indices = []
        for offset in range(-DEFAULT_CONTEXT_SEGMENTS_LENGTH, DEFAULT_CONTEXT_SEGMENTS_LENGTH + 1):
            context_idx = seg.seg_idx + offset
            if context_idx >= 0:
                segments_to_fetch.append((hit.episode_idx, context_idx))
                context_indices.append(context_idx)

        hit_to_segments_map[i] = {
            'episode_idx': hit.episode_idx,
            'target_seg_idx': seg.seg_idx,
            'context_indices': context_indices
        }

    # Batch fetch all segments
    fetched_segments = index.get_segments_by_ids(segments_to_fetch)

    # Create a lookup map for quick access
    segment_map = {}
    for (ep_idx, seg_idx), seg_data in zip(segments_to_fetch, fetched_segments):
        key = f"{ep_idx}|{seg_idx}"
        segment_map[key] = seg_data

    # Build results with context
    for i, hit in enumerate(hits):
        hit_info = hit_to_segments_map[i]
        episode_idx = hit_info['episode_idx']
        target_seg_idx = hit_info['target_seg_idx']

        target_key = f"{episode_idx}|{target_seg_idx}"
        target_seg = segment_map.get(target_key, {})

        context_parts = []
        for ctx_idx in hit_info['context_indices']:
            ctx_key = f"{episode_idx}|{ctx_idx}"
            ctx_seg = segment_map.get(ctx_key)
            if ctx_seg and ctx_seg.get('text'):
                context_parts.append(ctx_seg['text'])

        context_text = ' '.join(context_parts)

        doc_info = index.get_document_info(episode_idx)
        source_str = doc_info.get("source", "")
        episode_title = doc_info.get("episode_title", "")
        date = doc_info.get("episode_date", "")
        podcast_title = source_str

        all_results.append({
            "source": source_str,
            "episode_idx": episode_idx,
            "podcast_title": podcast_title,
            "date": date,
            "episode_title": episode_title,
            "segment_idx": target_seg_idx,
            "start": target_seg.get('start_time', ''),
            "end": target_seg.get('end_time', ''),
            "text": target_seg.get('text', ''),
            "context": context_text
        })

    # Create CSV in memory with UTF-8 BOM for Excel compatibility
    output = io.StringIO()
    output.write('\ufeff')  # UTF-8 BOM
    writer = csv.writer(output, dialect='excel')

    # Write metadata as comments (info rows)
    writer.writerow(['# ivrit.ai Explore - Search Results Export'])
    writer.writerow(['# Query:', query])
    writer.writerow(['# Search Mode:', search_mode])
    if date_from_val:
        writer.writerow(['# Date From:', date_from_val])
    if date_to_val:
        writer.writerow(['# Date To:', date_to_val])
    if sources_list:
        writer.writerow(['# Sources Filter:', ', '.join(sources_list)])
    writer.writerow(['# Total Results:', len(all_results)])
    writer.writerow(['# Exported:', datetime.now().strftime('%Y-%m-%d %H:%M:%S')])
    writer.writerow([])

    writer.writerow([
        'Episode Index', 'Date', 'Source', 'Episode',
        'Text', 'Context', 'Start Time', 'End Time'
    ])

    for r in all_results:
        text = r.get('text', '').encode('utf-8', errors='replace').decode('utf-8')
        context = r.get('context', '').encode('utf-8', errors='replace').decode('utf-8')
        writer.writerow([
            r.get('episode_idx', ''),
            r.get('date', ''),
            r.get('podcast_title', ''),
            r.get('episode_title', ''),
            text,
            context,
            r.get('start', ''),
            r.get('end', '')
        ])

    execution_time = (time.time() - start_time) * 1000

    # Track export analytics
    analytics = request.app.state.analytics
    if analytics:
        analytics.capture_export(
            export_type='csv',
            query=query,
            execution_time_ms=execution_time,
            url=str(request.url),
            user_email=user_email,
        )

    # Create safe filename
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    safe_query = re.sub(r'[^\w\s-]', '', query)[:50]
    safe_query = re.sub(r'[-\s]+', '_', safe_query)
    filename = f'ivrit_explore_{safe_query}_{timestamp}.csv' if safe_query else f'ivrit_explore_{timestamp}.csv'

    csv_bytes = output.getvalue().encode('utf-8')

    return StreamingResponse(
        io.BytesIO(csv_bytes),
        media_type='text/csv; charset=utf-8',
        headers={'Content-Disposition': f'attachment; filename="{filename}"'},
    )


@router.get('/export/segment/{source}/{filename:path}', name='export.export_segment')
def export_segment(
    request: Request,
    source: str,
    filename: str,
    start: float = Query(0),
    end: float = Query(0),
):
    if end <= start:
        raise HTTPException(status_code=400, detail="End time must be greater than start time")

    try:
        logger.info(f"Exporting segment: {source}/{filename}")
        audio_dir = request.app.state.audio_dir
        audio_path = resolve_audio_path(f'{source}/{filename}.opus', audio_dir)
        if not audio_path:
            from fastapi import HTTPException
            raise HTTPException(status_code=404, detail="Source not found")

        logger.info(f"Found audio file: {audio_path}")

        cmd = [
            'ffmpeg', '-y',
            '-i', audio_path,
            '-ss', str(start),
            '-to', str(end),
            '-acodec', 'libmp3lame',
            '-ab', '64k',
            '-f', 'mp3',
            '-'
        ]

        process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )

        output_data, error = process.communicate()

        if process.returncode != 0:
            logger.error(f"FFmpeg error: {error.decode()}")
            from fastapi import HTTPException
            raise HTTPException(status_code=500, detail="Error processing audio")

        download_name = f'{source}_{filename}_{start:.2f}-{end:.2f}.mp3'

        return StreamingResponse(
            io.BytesIO(output_data),
            media_type='audio/mpeg',
            headers={'Content-Disposition': f'attachment; filename="{download_name}"'},
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error exporting segment: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error: {str(e)}")
