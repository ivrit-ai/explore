from urllib.parse import urlencode

import pytest

from app import create_app
from app.services import index as index_module

from .conftest import DOC_UUIDS


@pytest.mark.parametrize("path", [
    "/api/me",
    "/api/search?q=בינה",
    "/api/search/metadata?q=בינה",
    "/api/episodes/0/segments?center=1",
    "/api/export/csv?q=בינה",
    f"/api/export/audio/{DOC_UUIDS[0]}?start=0&end=1",
])
def test_api_requires_login(anon, path):
    resp = anon.get(path)
    assert resp.status_code == 401


def test_pages_redirect_to_platform_login(anon):
    resp = anon.get("/search?q=x", follow_redirects=False)
    assert resp.status_code == 302
    # The platform sign-in brings the user back to the page they asked for.
    assert resp.headers["location"] == "/xhost-auth/login?" + urlencode({"return_to": "/search?q=x"})


def test_home_redirects_when_logged_out(anon):
    # The platform health check expects a 2xx/3xx at GET /.
    resp = anon.get("/", follow_redirects=False)
    assert resp.status_code == 302
    assert resp.headers["location"].startswith("/xhost-auth/login?")


def test_authorize_hands_off_to_platform_and_stays_on_site(anon):
    resp = anon.get("/authorize", params={"next": "/search?q=x"}, follow_redirects=False)
    assert resp.headers["location"] == "/xhost-auth/login?" + urlencode({"return_to": "/search?q=x"})
    for evil in ("https://evil.example", "//evil.example", r"/\evil.example"):
        resp = anon.get("/authorize", params={"next": evil}, follow_redirects=False)
        assert resp.headers["location"] == "/xhost-auth/login?" + urlencode({"return_to": "/"})


def test_logout_goes_through_platform(anon):
    resp = anon.get("/logout", follow_redirects=False)
    assert resp.headers["location"].startswith("/xhost-auth/logout?")


def test_forged_identity_cookie_is_rejected(anon):
    anon.cookies.set("__Host-xhost_id", "not-a-jwt")
    assert anon.get("/api/me").status_code == 401


def test_audio_requires_login(anon):
    resp = anon.get(f"/audio/{DOC_UUIDS[0]}.opus", follow_redirects=False)
    assert resp.status_code == 302


def test_old_unauthenticated_endpoints_are_gone(anon):
    assert anon.post("/search/segment", json={"lookups": []}).status_code in (302, 404, 405)
    assert anon.get("/export/segment/929/ep1?start=0&end=1").status_code == 404


def test_production_starts_without_secret_key(monkeypatch, tmp_path):
    # Identity is the platform's signed cookie; there is no app session to sign.
    monkeypatch.setenv("APP_ENV", "production")
    monkeypatch.delenv("SECRET_KEY", raising=False)
    (tmp_path / "audio").mkdir()
    create_app(data_dir=str(tmp_path))


def test_me(client):
    assert client.get("/api/me").json() == {"email": "tester@ivrit.ai"}


def test_exact_search_returns_relative_match(client):
    data = client.get("/api/search", params={"q": "בינה", "shuffle": "false"}).json()
    assert data["seed"] is None
    assert data["total_hits"] == 1
    hit = data["episodes"][0]["hits"][0]
    assert hit["text"][hit["match"]["start"]:hit["match"]["end"]] == "בינה"
    assert hit["start_sec"] == 2.0
    assert data["episodes"][0]["duration_sec"] == 8.0


def test_exact_mode_skips_partial_words(client):
    data = client.get("/api/search", params={"q": "ברית", "mode": "exact", "shuffle": "false"}).json()
    assert data["total_hits"] == 0


def test_partial_search_highlights_substring(client):
    data = client.get("/api/search", params={"q": "חבר", "mode": "partial", "shuffle": "false"}).json()
    found = {h["text"][h["match"]["start"]:h["match"]["end"]] for e in data["episodes"] for h in e["hits"]}
    assert found == {"חבר"}
    assert data["total_hits"] == 2  # "חברים" in both episodes


def test_partial_search_tolerates_punctuation(client):
    resp = client.get("/api/search", params={"q": "ו-929", "mode": "partial", "shuffle": "false"})
    assert resp.status_code == 200
    assert resp.json()["total_hits"] == 1


def test_regex_word_boundary_escape(client):
    data = client.get("/api/search", params={"q": r"\bבינה\b", "mode": "regex", "shuffle": "false"}).json()
    assert data["total_hits"] == 1


def test_regex_groups_are_relative_to_segment(client):
    data = client.get("/api/search", params={
        "q": r"(שלום|היי)\s+(חברים)", "mode": "regex", "shuffle": "false"}).json()
    texts = set()
    for e in data["episodes"]:
        for h in e["hits"]:
            g1, g2 = h["groups"]
            texts.add((h["text"][g1["start"]:g1["end"]], h["text"][g2["start"]:g2["end"]]))
    assert texts == {("שלום", "חברים"), ("היי", "חברים")}


def test_shuffle_returns_seed_and_is_stable(client):
    first = client.get("/api/search", params={"q": "חברים"}).json()
    assert isinstance(first["seed"], int)
    again = client.get("/api/search", params={"q": "חברים", "seed": first["seed"]}).json()
    assert [e["episode_idx"] for e in again["episodes"]] == [e["episode_idx"] for e in first["episodes"]]


def test_invalid_regex_is_400(client):
    resp = client.get("/api/search", params={"q": "(שלום", "mode": "regex"})
    assert resp.status_code == 400


def test_query_length_is_capped(client):
    assert client.get("/api/search", params={"q": "א" * 201}).status_code == 422


def test_bad_date_is_rejected(client):
    assert client.get("/api/search", params={"q": "בינה", "date_from": "2020/01/01"}).status_code == 422


def test_regex_timeout_is_422(client, monkeypatch):
    monkeypatch.setattr(index_module, "REGEX_TIME_BUDGET_S", 0)
    resp = client.get("/api/search", params={"q": "חברים", "mode": "regex", "shuffle": "false"})
    assert resp.status_code == 422


def test_source_filter(client):
    data = client.get("/api/search", params=[("q", "חברים"), ("source", "929"), ("shuffle", "false")]).json()
    assert {e["source"] for e in data["episodes"]} == {"929"}


def test_metadata(client):
    data = client.get("/api/search/metadata", params={"q": "חברים"}).json()
    assert {s["name"] for s in data["sources"]} == {"929", "אחד ביום"}
    assert data["date_range"] == {"min": "2020-11-11", "max": "2022-02-07"}


def test_episode_segments(client):
    segs = client.get("/api/episodes/0/segments", params={"center": 1, "radius": 1}).json()
    assert [s["segment_idx"] for s in segs] == [0, 1, 2]
    assert client.get("/api/episodes/0/segments", params={"center": 1, "radius": 50}).status_code == 422


def test_csv_export_context_matches_hit(client):
    resp = client.get("/api/export/csv", params={"q": "בינה"})
    assert resp.status_code == 200
    body = resp.content.decode("utf-8-sig")
    row = [line for line in body.splitlines() if "בינה" in line and not line.startswith("#")][0]
    assert "שלום חברים" in row  # context comes from the same episode


@pytest.mark.parametrize("params", [
    {"start": 5, "end": 1},
    {"start": 0, "end": 601},
])
def test_audio_export_validates_range(client, params):
    assert client.get(f"/api/export/audio/{DOC_UUIDS[0]}", params=params).status_code == 400


def test_audio_export_rejects_bad_uuid(client):
    assert client.get("/api/export/audio/..%2f..%2fetc", params={"start": 0, "end": 1}).status_code in (400, 404)


def test_audio_export_missing_file_is_404(client):
    assert client.get(f"/api/export/audio/{DOC_UUIDS[0]}", params={"start": 0, "end": 1}).status_code == 404


def test_security_headers(client):
    resp = client.get("/api/me")
    assert resp.headers["X-Content-Type-Options"] == "nosniff"
    assert "frame-ancestors 'none'" in resp.headers["Content-Security-Policy"]


def test_api_docs_disabled(anon):
    assert anon.get("/docs").status_code == 404
    assert anon.get("/openapi.json").status_code == 404


def _all_hits(client, q, mode):
    return client.get("/api/search", params={
        "q": q, "mode": mode, "shuffle": "false", "max_hits_per_episode": 500}).json()


def test_hits_per_episode_are_capped_with_true_counts(client):
    # A broad partial term used to return every hit of the page at once, and
    # rendering them all froze the browser.
    full = _all_hits(client, "ב", "partial")
    capped = client.get("/api/search", params={
        "q": "ב", "mode": "partial", "shuffle": "false", "max_hits_per_episode": 1}).json()

    assert capped["total_hits"] == full["total_hits"] > len(capped["episodes"])
    for ep in capped["episodes"]:
        assert len(ep["hits"]) == 1
        assert ep["hit_offset"] == 0
    full_counts = {e["episode_idx"]: len(e["hits"]) for e in full["episodes"]}
    assert {e["episode_idx"]: e["total_hits"] for e in capped["episodes"]} == full_counts
    assert sum(full_counts.values()) == full["total_hits"]


def test_episode_hits_page_in_order(client):
    full = _all_hits(client, "ב", "partial")
    ep = max(full["episodes"], key=lambda e: len(e["hits"]))
    assert len(ep["hits"]) >= 3
    offsets = [h["char_offset"] for h in ep["hits"]]

    page = client.get("/api/search", params={
        "q": "ב", "mode": "partial", "shuffle": "false",
        "episode": ep["episode_idx"], "hit_offset": 1, "max_hits_per_episode": 2}).json()

    assert [e["episode_idx"] for e in page["episodes"]] == [ep["episode_idx"]]
    got = page["episodes"][0]
    assert got["hit_offset"] == 1
    assert got["total_hits"] == len(offsets)
    assert [h["char_offset"] for h in got["hits"]] == offsets[1:3]


def test_paging_an_episode_not_on_the_page_is_empty(client):
    data = client.get("/api/search", params={"q": "ב", "mode": "partial", "episode": 9999}).json()
    assert data["episodes"] == []


def test_hits_per_episode_limit_is_bounded(client):
    assert client.get("/api/search", params={"q": "ב", "max_hits_per_episode": 0}).status_code == 422
    assert client.get("/api/search", params={"q": "ב", "max_hits_per_episode": 501}).status_code == 422
