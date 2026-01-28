import logging
import os
import time
from collections import defaultdict

from flask import Blueprint, current_app, jsonify, render_template, request

from ..routes.auth import login_required
from ..services.analytics_service import track_performance
from ..services.index import IndexManager
from ..services.search import SearchService

logger = logging.getLogger(__name__)

bp = Blueprint("main", __name__)

# Global search service instance for persistence
search_service = None
file_records = None


@bp.route("/")
@login_required
def home():
    # Track page view
    analytics = current_app.config.get("ANALYTICS_SERVICE")
    if analytics:
        analytics.capture_event("page_viewed", {"page": "home"})
    return render_template("home.html")


@bp.route("/search")
@login_required
@track_performance("search_executed", include_args=["query", "page"])
def search():
    query = request.args.get("q", "").strip()

    # Block empty queries
    if not query:
        logger.warning("Search attempted with empty query")
        return render_template("home.html", error="נא להזין מונח לחיפוש"), 400

    per_page = int(request.args.get("max_results_per_page", 1000))
    per_page = min(per_page, 5000)  # Cap at 5000
    page = max(1, int(request.args.get("page", 1)))

    # Get search mode parameter
    search_mode = request.args.get("search_mode", "exact").strip()
    # Validate search mode
    if search_mode not in ["exact", "partial", "regex"]:
        search_mode = "exact"

    # Get ignore punctuation option
    ignore_punct = request.args.get("ignore_punct", "0").strip() == "1"

    # Get position filter parameters
    position_filters = set()
    if request.args.get("pos_start", "0").strip() == "1":
        position_filters.add("start")
    if request.args.get("pos_end", "0").strip() == "1":
        position_filters.add("end")
    if request.args.get("pos_cross", "0").strip() == "1":
        position_filters.add("cross")
    position_filters = position_filters or None

    # Get filter parameters from request
    date_from = request.args.get("date_from", "").strip() or None
    date_to = request.args.get("date_to", "").strip() or None
    sources_param = request.args.get("sources", "").strip()
    sources = [s.strip() for s in sources_param.split(",") if s.strip()] if sources_param else None

    # Validate that query is not empty (after reading all parameters)
    if not query:
        return render_template(
            "results.html",
            query="",
            results=[],
            pagination={
                "page": page,
                "per_page": per_page,
                "total_pages": 1,
                "total_results": 0,
                "has_prev": False,
                "has_next": False,
            },
            max_results_per_page=per_page,
            search_mode=search_mode,
            date_from=date_from,
            date_to=date_to,
            sources=sources,
            sources_param=sources_param,
            error_message="אנא הזן מונח לחיפוש",
        )

    start_time = time.time()

    global search_service, file_records
    if file_records is None:
        from ..utils import get_transcripts

        json_dir = current_app.config.get("DATA_DIR") / "json"
        file_records = get_transcripts(json_dir)
    if search_service is None:
        # Get database type from environment
        db_type = os.environ.get("DEFAULT_DB_TYPE", "sqlite")

        search_service = SearchService(IndexManager(file_records, db_type=db_type))

    # Apply filters to search with search mode
    hits = search_service.search(
        query,
        search_mode=search_mode,
        date_from=date_from,
        date_to=date_to,
        sources=sources,
        ignore_punct=ignore_punct,
        position_filters=position_filters,
    )
    total = len(hits)

    # simple slicing
    start_i = (page - 1) * per_page
    end_i = start_i + per_page
    page_hits = hits[start_i:end_i]

    # enrich hits with segment info (start time + index)
    records = []
    for h in page_hits:
        seg = search_service.segment(h)
        doc_info = search_service._index_mgr.get().get_document_info(h.episode_idx)
        records.append(
            {
                "episode_idx": h.episode_idx,
                "char_offset": h.char_offset,
                "uuid": doc_info.get("uuid", ""),
                "episode": doc_info.get("episode", ""),
                "source": doc_info.get("source", ""),
                "segment_idx": seg.seg_idx,
                "start_sec": seg.start_sec,
                "end_sec": seg.end_sec,
                "episode_title": doc_info.get("episode_title", ""),
                "episode_date": doc_info.get("episode_date", ""),
            }
        )

    # Group results by (source, episode_idx)
    grouped = defaultdict(list)
    for r in records:
        grouped[(r["source"], r["episode_idx"])].append(r)

    display_groups = []
    for (source, episode_idx), group in grouped.items():
        meta = group[0]
        display_groups.append(
            {
                "source": source,
                "episode_idx": episode_idx,
                "uuid": meta.get("uuid", ""),
                "episode_title": meta.get("episode_title", ""),
                "episode_date": meta.get("episode_date", ""),
                "episode": meta.get("episode", ""),
                "results": group,
            }
        )

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
    analytics = current_app.config.get("ANALYTICS_SERVICE")
    if analytics:
        analytics.capture_search(
            query=query,
            max_results_per_page=per_page,
            page=page,
            execution_time_ms=execution_time_ms,
            results_count=len(records),
            total_results=total,
        )

    if request.headers.get("Accept") == "application/json":
        return jsonify({"results": records, "pagination": pagination})

    return render_template(
        "results.html",
        query=query,
        results=display_groups,
        pagination=pagination,
        max_results_per_page=per_page,
        search_mode=search_mode,
        date_from=date_from,
        date_to=date_to,
        sources=sources,
        sources_param=sources_param,
        ignore_punct=ignore_punct,
        position_filters=position_filters,
    )


@bp.route("/search/metadata")
@login_required
def search_metadata():
    """Return metadata (sources and date range) for all search results."""
    query = request.args.get("q", "").strip()

    if not query:
        return jsonify({"error": "Missing query parameter 'q'"}), 400

    # Get search mode parameter
    search_mode = request.args.get("search_mode", "exact").strip()
    # Validate search mode
    if search_mode not in ["exact", "partial", "regex"]:
        search_mode = "exact"

    # Get ignore punctuation option
    ignore_punct = request.args.get("ignore_punct", "0").strip() == "1"

    # Get position filter parameters
    position_filters = set()
    if request.args.get("pos_start", "0").strip() == "1":
        position_filters.add("start")
    if request.args.get("pos_end", "0").strip() == "1":
        position_filters.add("end")
    if request.args.get("pos_cross", "0").strip() == "1":
        position_filters.add("cross")
    position_filters = position_filters or None

    global search_service, file_records
    if file_records is None:
        from ..utils import get_transcripts

        json_dir = current_app.config.get("DATA_DIR") / "json"
        file_records = get_transcripts(json_dir)
    if search_service is None:
        # Get database type from environment
        db_type = os.environ.get("DEFAULT_DB_TYPE", "sqlite")

        search_service = SearchService(IndexManager(file_records, db_type=db_type))

    # Get ALL hits (not paginated) using the specified search mode
    hits = search_service.search(
        query, search_mode=search_mode, ignore_punct=ignore_punct, position_filters=position_filters
    )

    # Extract metadata from all hits
    sources = defaultdict(int)  # source -> count
    dates = []  # list of all dates

    index = search_service._index_mgr.get()
    for h in hits:
        doc_info = index.get_document_info(h.episode_idx)
        source = doc_info.get("source", "")
        episode_date = doc_info.get("episode_date", "")

        if source:
            sources[source] += 1

        if episode_date:
            try:
                # Parse date to validate and normalize
                from datetime import datetime

                datetime.strptime(episode_date, "%Y-%m-%d")  # Validate format
                dates.append(episode_date)  # Keep as string for JSON
            except (ValueError, TypeError):
                # Skip invalid dates
                pass

    # Convert sources dict to regular dict for JSON
    sources_dict = dict(sources)

    # Find min and max dates
    date_range = {"min": None, "max": None}
    if dates:
        dates_sorted = sorted(dates)  # Sort as strings (YYYY-MM-DD format)
        date_range["min"] = dates_sorted[0]
        date_range["max"] = dates_sorted[-1]

    return jsonify({"sources": sources_dict, "date_range": date_range, "total_results": len(hits)})


@bp.route("/privacy")
def privacy_policy():
    # Track page view
    analytics = current_app.config.get("ANALYTICS_SERVICE")
    if analytics:
        analytics.capture_event("page_viewed", {"page": "privacy_policy"})
    return render_template("privacy.html")
