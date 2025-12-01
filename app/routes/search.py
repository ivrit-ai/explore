# app/routes/search.py
from __future__ import annotations
from flask import Blueprint, request, jsonify, current_app, abort

from app.services.search import SearchService, SearchHit
from app.services.index import IndexManager

bp = Blueprint("search", __name__, url_prefix="/search")


@bp.route("/", methods=["GET"])
def search():
    search_svc = current_app.config["SEARCH_SERVICE"]

    q = request.args.get("q", "")
    if not q:
        abort(400, "missing ?q=")
    regex = bool(request.args.get("regex"))
    hits = search_svc.search(q, regex=regex)

    # Enrich hits with segment and episode info
    index = search_svc._index_mgr.get()
    results = []
    for h in hits:
        seg = search_svc.segment(h)
        doc_info = index.get_document_info(h.episode_idx)
        results.append({
            "episode_idx": h.episode_idx,
            "char_offset": h.char_offset,
            "source": doc_info.get("source", ""),
            "episode": doc_info.get("episode", ""),
            "episode_title": doc_info.get("episode_title", ""),
            "episode_date": doc_info.get("episode_date", ""),
            "segment_idx": seg.seg_idx,
            "start_sec": seg.start_sec,
            "end_sec": seg.end_sec,
        })

    return jsonify(results)

@bp.route("/segment", methods=["POST"])
def get_segment():
    search_svc = current_app.config["SEARCH_SERVICE"]
    
    try:
        lookups = request.json["lookups"]
        if not isinstance(lookups, list):
            abort(400, "lookups must be an array")
        
        results = []
        for lookup in lookups:
            try:
                epi = int(lookup["episode_idx"])
                char = int(lookup["char_offset"])
                hit = SearchHit(epi, char)
                seg = search_svc.segment(hit)
                results.append({
                    "episode_idx": epi,
                    "char_offset": char,
                    "segment_index": seg.seg_idx,
                    "start_sec": seg.start_sec,
                    "end_sec": seg.end_sec,
                    "text": seg.text,
                })
            except (KeyError, ValueError) as e:
                # Skip invalid lookups but continue processing others
                continue
        
        return jsonify(results)
            
    except (KeyError, ValueError) as e:
        abort(400, str(e))

@bp.route("/segment/by_idx", methods=["POST"])
def get_segments_by_idx():
    search_svc = current_app.config["SEARCH_SERVICE"]
    index_mgr = search_svc._index_mgr.get()
    
    try:
        lookups = request.json["lookups"]
        if not isinstance(lookups, list):
            abort(400, "lookups must be an array")
        
        # Prepare batch lookup
        batch_lookups = []
        valid_lookups = []
        
        for lookup in lookups:
            try:
                epi = int(lookup["episode_idx"])
                idx = int(lookup["segment_idx"])
                doc_id = epi
                batch_lookups.append((doc_id, idx))
                valid_lookups.append((epi, idx))
            except (KeyError, ValueError) as e:
                # Skip invalid lookups but continue processing others
                continue
        
        # Perform batch lookup
        segments = index_mgr.get_segments_by_ids(batch_lookups)
        
        # Map results back to original format
        results = []
        for (epi, idx), segment_data in zip(valid_lookups, segments):
            results.append({
                "episode_idx": epi,
                "segment_index": segment_data["segment_id"],
                "start_sec": segment_data["start_time"],
                "end_sec": segment_data["end_time"],
                "text": segment_data["text"]
            })
        
        return jsonify(results)
            
    except (KeyError, ValueError) as e:
        abort(400, str(e))