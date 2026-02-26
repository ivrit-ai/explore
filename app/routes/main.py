from fastapi import APIRouter, Request, Query, Depends
from fastapi.responses import JSONResponse, RedirectResponse
from ..routes.auth import require_login
from ..templating import render
import time
import os
import random
import logging
from urllib.parse import urlencode
from collections import defaultdict

logger = logging.getLogger(__name__)

router = APIRouter()


@router.get('/', name='main.home')
def home(request: Request, user_email: str = Depends(require_login)):
    analytics = request.app.state.analytics
    if analytics:
        analytics.capture_event('page_viewed', {'page': 'home'}, user_email=user_email)
    return render(request, 'home.html')


@router.get('/search', name='main.search')
def search(
    request: Request,
    q: str = Query(''),
    max_results_per_page: int = Query(20),
    page: int = Query(1),
    search_mode: str = Query('exact'),
    date_from: str = Query(''),
    date_to: str = Query(''),
    sources: str = Query(''),
    shuffle: str = Query(''),
    seed: str = Query(''),
    user_email: str = Depends(require_login),
):
    query = q.strip()

    # Block empty queries
    if not query:
        logger.warning("Search attempted with empty query")
        return render(request, 'home.html', error="נא להזין מונח לחיפוש")

    per_page = min(int(max_results_per_page), 5000)
    page = max(1, int(page))

    # Validate search mode
    if search_mode not in ['exact', 'partial', 'regex']:
        search_mode = 'exact'

    # Handle shuffle: if shuffle requested but no seed, redirect with a random seed
    shuffle_on = shuffle.strip() == '1'
    if shuffle_on and not seed.strip():
        new_seed = random.randint(0, 2**31 - 1)
        params = dict(request.query_params)
        params['seed'] = str(new_seed)
        url = str(request.url_for('main.search')) + '?' + urlencode(params)
        return RedirectResponse(url=url, status_code=302)

    seed_val = int(seed.strip()) if shuffle_on and seed.strip() else None

    # Process filter parameters
    date_from_val = date_from.strip() or None
    date_to_val = date_to.strip() or None
    sources_param = sources.strip()
    sources_list = [s.strip() for s in sources_param.split(',') if s.strip()] if sources_param else None

    start_time = time.time()

    search_service = request.app.state.search_service

    # Document-based pagination
    doc_offset = (page - 1) * per_page
    hits, has_more = search_service.search(
        query, search_mode=search_mode, date_from=date_from_val,
        date_to=date_to_val, sources=sources_list,
        doc_limit=per_page, doc_offset=doc_offset,
        seed=seed_val,
    )
    total = len(hits)

    # Batch enrich hits with segment info + document info
    t_enrich = time.time()
    index = search_service._index_mgr.get()

    # Batch segment lookup
    offset_pairs = [(h.episode_idx, h.char_offset) for h in hits]
    segments_map = index.get_segments_at_offsets(offset_pairs)

    # Batch document lookup
    unique_doc_ids = list(set(h.episode_idx for h in hits))
    docs_map = index.get_documents_batch(unique_doc_ids)

    records = []
    for h in hits:
        seg = segments_map.get((h.episode_idx, h.char_offset), {})
        doc_info = docs_map.get(h.episode_idx, {})
        records.append({
            "episode_idx":  h.episode_idx,
            "char_offset":  h.char_offset,
            "uuid":         doc_info.get("uuid", ""),
            "episode":      doc_info.get("episode", ""),
            "source":       doc_info.get("source", ""),
            "segment_idx":  seg.get("segment_id", 0),
            "start_sec":    seg.get("start_time", 0),
            "end_sec":      seg.get("end_time", 0),
            "episode_title": doc_info.get("episode_title", ""),
            "episode_date": doc_info.get("episode_date", ""),
        })
    t_enrich_done = time.time()
    logger.info(f"[BENCH] enrichment: {((t_enrich_done-t_enrich)*1000):.1f}ms for {len(hits)} hits "
                f"({len(unique_doc_ids)} docs, batch)")

    # Group results by (source, episode_idx)
    grouped = defaultdict(list)
    for r in records:
        grouped[(r['source'], r['episode_idx'])].append(r)

    display_groups = []
    for (source, episode_idx), group in grouped.items():
        meta = group[0]
        display_groups.append({
            'source': source,
            'episode_idx': episode_idx,
            'uuid': meta.get('uuid', ''),
            'episode_title': meta.get('episode_title', ''),
            'episode_date': meta.get('episode_date', ''),
            'episode': meta.get('episode', ''),
            'results': group,
        })

    pagination = {
        "page": page,
        "per_page": per_page,
        "total_results": total,
        "total_results_display": f"{total:,}",
        "docs_on_page": len(unique_doc_ids),
        "has_prev": page > 1,
        "has_next": has_more,
    }

    # Track search analytics
    execution_time_ms = (time.time() - start_time) * 1000
    logger.info(f"[BENCH] total request: {execution_time_ms:.1f}ms (query={query}, mode={search_mode}, "
                f"hits={total}, docs={len(unique_doc_ids)}, has_more={has_more})")
    analytics = request.app.state.analytics
    if analytics:
        analytics.capture_search(
            query=query,
            max_results_per_page=per_page,
            page=page,
            execution_time_ms=execution_time_ms,
            results_count=len(records),
            total_results=total,
            user_email=user_email,
        )

    accept = request.headers.get('Accept', '')
    if accept == 'application/json':
        return JSONResponse({"results": records, "pagination": pagination})

    return render(request, 'results.html',
                  query=query,
                  results=display_groups,
                  pagination=pagination,
                  max_results_per_page=per_page,
                  search_mode=search_mode,
                  date_from=date_from_val,
                  date_to=date_to_val,
                  sources=sources_list,
                  sources_param=sources_param,
                  shuffle='1' if shuffle_on else '',
                  seed=seed.strip() if shuffle_on else '')


@router.get('/search/metadata', name='main.search_metadata')
def search_metadata(
    request: Request,
    q: str = Query(''),
    search_mode: str = Query('exact'),
    date_from: str = Query(''),
    date_to: str = Query(''),
    sources: str = Query(''),
    user_email: str = Depends(require_login),
):
    """Return metadata (sources and date range) for all search results."""
    query = q.strip()

    if not query:
        return JSONResponse({"error": "Missing query parameter 'q'"}, status_code=400)

    if search_mode not in ['exact', 'partial', 'regex']:
        search_mode = 'exact'

    search_service = request.app.state.search_service

    date_from_val = date_from.strip() or None
    date_to_val = date_to.strip() or None
    sources_param = sources.strip()
    sources_list = [s.strip() for s in sources_param.split(',') if s.strip()] if sources_param else None

    import regex as re_mod
    index = search_service._index_mgr.get()

    if search_mode == 'exact':
        escaped = query.replace('"', '""')
        fts_query = f'"{escaped}"'
    elif search_mode == 'partial':
        escaped = query.replace('"', '""')
        tokens = escaped.split()
        fts_query = ' OR '.join([f'{t}*' for t in tokens])
    else:  # regex
        potential_tokens = re_mod.findall(r'\w{2,}', query)
        if potential_tokens:
            fts_query = ' AND '.join([f'{t}*' for t in potential_tokens[:3]])
        else:
            fts_query = None

    if fts_query:
        metadata = index.get_search_metadata(fts_query, date_from_val, date_to_val, sources_list)
    else:
        metadata = {"sources": {}, "date_range": {"min": None, "max": None}, "total_docs": 0}

    return JSONResponse({
        "sources": metadata["sources"],
        "date_range": metadata["date_range"],
        "total_results": metadata["total_docs"],
    })


@router.get('/privacy', name='main.privacy')
def privacy_policy(request: Request):
    analytics = request.app.state.analytics
    if analytics:
        analytics.capture_event('page_viewed', {'page': 'privacy_policy'})
    return render(request, 'privacy.html')
