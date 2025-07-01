from flask import Blueprint, render_template, request, jsonify, current_app
from ..services.search import SearchService
from ..services.analytics_service import track_performance
from ..routes.auth import login_required
import time
import os
import logging
import uuid
from ..services.index import IndexManager

logger = logging.getLogger(__name__)

bp = Blueprint('main', __name__)

# Global search service instance for persistence
search_service = None
file_records = None

@bp.route('/')
@login_required
def home():
    # Track page view
    analytics = current_app.config.get('ANALYTICS_SERVICE')
    if analytics:
        analytics.capture_event('page_viewed', {'page': 'home'})
    return render_template('home.html')

@bp.route('/search')
@login_required
@track_performance('search_executed', include_args=['query', 'page'])
def search():
    query      = request.args.get('q', '').strip()
    per_page   = int(request.args.get('max_results_per_page', 100))
    page       = max(1, int(request.args.get('page', 1)))
    start_time = time.time()

    global search_service, file_records
    if file_records is None:
        from ..utils import get_transcripts
        json_dir = current_app.config.get('DATA_DIR') / "json"
        file_records = get_transcripts(json_dir)
    if search_service is None:
        # Get database type from environment
        db_type = os.environ.get('DEFAULT_DB_TYPE', 'sqlite')
        
        search_service = SearchService(IndexManager(file_records, db_type=db_type))

    hits = search_service.search(query)
    total = len(hits)

    # simple slicing
    start_i = (page - 1) * per_page
    end_i   = start_i + per_page
    page_hits = hits[start_i:end_i]

    # enrich hits with segment info (start time + index)
    records = []
    for h in page_hits:
        seg = search_service.segment(h)
        records.append({
            "episode_idx":  h.episode_idx,
            "char_offset":  h.char_offset,
            "recording_id": search_service._index_mgr.get().get_source_by_episode_idx(h.episode_idx),
            "source":       search_service._index_mgr.get().get_source_by_episode_idx(h.episode_idx),
            "segment_idx":  seg.seg_idx,
            "start_sec":    seg.start_sec,
            "end_sec":      seg.end_sec,
        })

    pagination = {
        "page": page,
        "per_page": per_page,
        "total_pages": max(1, (total + per_page - 1) // per_page),
        "total_results": total,
        "has_prev": page > 1,
        "has_next": end_i < total,
    }

    # Track search analytics
    execution_time_ms = (time.time() - start_time) * 1000
    analytics = current_app.config.get('ANALYTICS_SERVICE')
    if analytics:
        analytics.capture_search(
            query=query,
            max_results_per_page=per_page,
            page=page,
            execution_time_ms=execution_time_ms,
            results_count=len(records),
            total_results=total
        )

    if request.headers.get('Accept') == 'application/json':
        return jsonify({"results": records, "pagination": pagination})

    return render_template('results.html',
                           query=query,
                           results=records,
                           pagination=pagination,
                           max_results_per_page=per_page)

@bp.route('/privacy')
def privacy_policy():
    # Track page view
    analytics = current_app.config.get('ANALYTICS_SERVICE')
    if analytics:
        analytics.capture_event('page_viewed', {'page': 'privacy_policy'})
    return render_template('privacy.html') 