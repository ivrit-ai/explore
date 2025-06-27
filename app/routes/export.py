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

logger = logging.getLogger(__name__)

bp = Blueprint('export', __name__)

@bp.route('/export/results/<query>')
@track_performance('export_csv', include_args=['query'])
def export_results_csv(query):
    start_time = time.time()
    
    # Get search service from main module
    from ..routes import main
    search_service = main.search_service
    
    # Always perform a new search to get all results
    logger.info(f"Performing new search for CSV export: {query}")
    
    # Get search hits
    hits = search_service.search(query)
    
    # Enrich hits with segment info
    all_results = []
    for hit in hits:
        seg = search_service.segment(hit)
        all_results.append({
            "episode_idx": hit.episode_idx,
            "char_offset": hit.char_offset,
            "source": search_service._index_mgr.get().get_source_by_episode_idx(hit.episode_idx),
            "segment_idx": seg.seg_idx,
            "start": seg.start_sec,
            "end": seg.end_sec,
            "text": seg.text
        })
    
    # Create CSV in memory with UTF-8 BOM for Excel compatibility
    output = io.StringIO()
    output.write('\ufeff')  # UTF-8 BOM
    writer = csv.writer(output, dialect='excel')
    writer.writerow(['Source', 'Text', 'Start Time', 'End Time'])
    
    for r in all_results:
        text = r['text'].encode('utf-8', errors='replace').decode('utf-8')
        writer.writerow([r['source'], text, r['start'], r['end']])
    
    execution_time = (time.time() - start_time) * 1000
    
    # Track export analytics
    analytics = current_app.config.get('ANALYTICS_SERVICE')
    if analytics:
        analytics.capture_export(
            export_type='csv',
            query=query,
            execution_time_ms=execution_time
        )
    
    output.seek(0)
    return send_file(
        io.BytesIO(output.getvalue().encode('utf-8')),
        mimetype='text/csv; charset=utf-8',
        as_attachment=True,
        download_name=f'search_results_{query}.csv'
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
            return "Source not found", 404
        
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