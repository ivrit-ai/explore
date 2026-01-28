from flask import Blueprint, request, send_file, current_app, jsonify
import io
import csv
import subprocess
from ..services.search import SearchService
from ..services.analytics_service import track_performance
from ..utils import resolve_audio_path
import logging
import time
import os
import glob
import re
from datetime import datetime

logger = logging.getLogger(__name__)

bp = Blueprint('export', __name__)

# Context segment configuration for CSV exports
DEFAULT_CONTEXT_SEGMENTS_LENGTH = 5

@bp.route('/export/results')
@track_performance('export_csv', include_args=['query'])
def export_results_csv():
    start_time = time.time()

    # Get search service and cache from main module
    from ..routes import main
    search_service = main.search_service

    # Get all search parameters
    query = request.args.get('q', '').strip()
    if not query:
        return "Missing query parameter", 400

    # Get search mode parameter
    search_mode = request.args.get('search_mode', 'exact').strip()
    # Validate search mode
    if search_mode not in ['exact', 'partial', 'regex']:
        search_mode = 'exact'

    # Get ignore punctuation option
    ignore_punct = request.args.get('ignore_punct', '0').strip() == '1'

    # Get filter parameters
    date_from = request.args.get('date_from', '').strip() or None
    date_to = request.args.get('date_to', '').strip() or None
    sources_param = request.args.get('sources', '').strip()

    # Parse sources list
    sources = None
    if sources_param:
        sources = [s.strip() for s in sources_param.split(',') if s.strip()]

    # Perform search with filters
    logger.info(f"Performing search for CSV export: {query} (mode: {search_mode}, filters: date_from={date_from}, date_to={date_to}, sources={sources}, ignore_punct={ignore_punct})")
    hits = search_service.search(query, search_mode=search_mode,
                                 date_from=date_from, date_to=date_to, sources=sources,
                                 ignore_punct=ignore_punct)
    
    # Enrich hits with segment info
    all_results = []
    index = search_service._index_mgr.get()

    # Build a set of all segments we need (target + context)
    segments_to_fetch = []
    hit_to_segments_map = {}

    for i, hit in enumerate(hits):
        seg = search_service.segment(hit)
        # For each hit, we need 5 segments before and 5 after
        context_indices = []
        for offset in range(-DEFAULT_CONTEXT_SEGMENTS_LENGTH, DEFAULT_CONTEXT_SEGMENTS_LENGTH + 1):  # -5 to +5 inclusive
            context_idx = seg.seg_idx + offset
            if context_idx >= 0:  # Only non-negative indices
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

    # Now build results with context
    for i, hit in enumerate(hits):
        hit_info = hit_to_segments_map[i]
        episode_idx = hit_info['episode_idx']
        target_seg_idx = hit_info['target_seg_idx']

        # Get the target segment
        target_key = f"{episode_idx}|{target_seg_idx}"
        target_seg = segment_map.get(target_key, {})

        # Build context text from surrounding segments
        context_parts = []
        for ctx_idx in hit_info['context_indices']:
            ctx_key = f"{episode_idx}|{ctx_idx}"
            ctx_seg = segment_map.get(ctx_key)
            if ctx_seg and ctx_seg.get('text'):
                context_parts.append(ctx_seg['text'])

        context_text = ' '.join(context_parts)

        # Get document info from index
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
    if date_from:
        writer.writerow(['# Date From:', date_from])
    if date_to:
        writer.writerow(['# Date To:', date_to])
    if sources:
        writer.writerow(['# Sources Filter:', ', '.join(sources)])
    if ignore_punct:
        writer.writerow(['# Ignore Punctuation:', 'Yes'])
    writer.writerow(['# Total Results:', len(all_results)])
    writer.writerow(['# Exported:', datetime.now().strftime('%Y-%m-%d %H:%M:%S')])
    writer.writerow([])  # Empty row for separation

    # Write column headers
    writer.writerow([
        'Episode Index', 'Date','Source',  'Episode',
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
    analytics = current_app.config.get('ANALYTICS_SERVICE')
    if analytics:
        analytics.capture_export(
            export_type='csv',
            query=query,
            execution_time_ms=execution_time
        )

    # Create safe filename
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    # Sanitize query for filename (remove special chars, limit length)
    safe_query = re.sub(r'[^\w\s-]', '', query)[:50]
    safe_query = re.sub(r'[-\s]+', '_', safe_query)
    filename = f'ivrit_explore_{safe_query}_{timestamp}.csv' if safe_query else f'ivrit_explore_{timestamp}.csv'

    output.seek(0)
    return send_file(
        io.BytesIO(output.getvalue().encode('utf-8')),
        mimetype='text/csv; charset=utf-8',
        as_attachment=True,
        download_name=filename
    )

@bp.route('/export/segment/<source>/<path:filename>')
def export_segment(source, filename):
    start_time = float(request.args.get('start', 0))
    end_time = float(request.args.get('end', 0))
    
    if end_time <= start_time:
        return "End time must be greater than start time", 400
    
    try:
        # Resolve the audio file path
        logger.info(f"Exporting segment: {source}/{filename}")
        audio_path = resolve_audio_path(f'{source}/{filename}.opus')
        if not audio_path:
            logger.error(f"Audio file not found. Tried paths: {possible_paths}")
            return "Source not found", 404
        
        logger.info(f"Found audio file: {audio_path}")
        
        # Create a temporary buffer for the output
        buffer = io.BytesIO()
        
        # Build ffmpeg command for segment extraction
        # -y: overwrite output file without asking
        # -i: input file
        # -ss: start time
        # -to: end time
        # -acodec: audio codec (libmp3lame)
        # -ab: audio bitrate (192k)
        # -f: output format (mp3)
        # -: output to stdout
        cmd = [
            'ffmpeg', '-y',
            '-i', audio_path,
            '-ss', str(start_time),
            '-to', str(end_time),
            '-acodec', 'libmp3lame',
            '-ab', '64k',
            '-f', 'mp3',
            '-'
        ]
        
        # Run ffmpeg and capture output
        process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )
        
        # Read the output
        output, error = process.communicate()
        
        if process.returncode != 0:
            logger.error(f"FFmpeg error: {error.decode()}")
            return "Error processing audio", 500
            
        # Write the output to the buffer
        buffer.write(output)
        buffer.seek(0)
        
        return send_file(
            buffer,
            mimetype='audio/mpeg',
            as_attachment=True,
            download_name=f'{source}_{filename}_{start_time:.2f}-{end_time:.2f}.mp3'
        )
        
    except Exception as e:
        logger.error(f"Error exporting segment: {str(e)}")
        return f"Error: {str(e)}", 500 