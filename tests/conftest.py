import uuid

import pytest
from fastapi.testclient import TestClient

from app import create_app, init_index_manager
from app.services.db import DatabaseService
from app.services.index import _episode_to_string_and_segments, _setup_schema

DOC_UUIDS = [str(uuid.UUID(int=i + 1)) for i in range(2)]

DOCS = [
    (DOC_UUIDS[0], "929", "929/ep1", "2020-11-11", "פרשה. הפטרה. שיר",
     [("שלום חברים, אנחנו בשיעור", 0.0, 2.0),
      ("על פרשה ושיתוף של בינה ו-929", 2.0, 5.0),
      ("מלמדים עברית בערבים", 5.0, 8.0)]),
    (DOC_UUIDS[1], "אחד ביום", "one/ep2", "2022-02-07", "המלחמה בתימן",
     [("היי חברים וברוכים הבאים", 0.0, 3.0),
      ("ארצות הברית נכנסה", 3.0, 6.0)]),
]


def _build_db(path):
    db = DatabaseService(path=str(path), for_index_generation=True)
    _setup_schema(db)
    for doc_id, (doc_uuid, source, episode, date, title, segs) in enumerate(DOCS):
        full_text, segments = _episode_to_string_and_segments(
            [{"text": t, "start": s, "end": e} for t, s, e in segs])
        db.execute("INSERT INTO documents VALUES (?,?,?,?,?,?)",
                   [doc_id, doc_uuid, source, episode, date, title])
        db.execute("INSERT INTO documents_fts(rowid, full_text) VALUES (?,?)", [doc_id + 1, full_text])
        db.execute("INSERT INTO fts_doc_mapping VALUES (?,?)", [doc_id + 1, doc_id])
        for i, s in enumerate(segments):
            db.execute("INSERT INTO segments VALUES (?,?,?,?,?,?,?)",
                       [doc_id, i, s["text"], s["avg_logprob"], s["char_offset"], s["start"], s["end"]])
    db.commit()
    db.close()


@pytest.fixture
def app(tmp_path, monkeypatch):
    monkeypatch.setenv("APP_ENV", "development")
    monkeypatch.setenv("DISABLE_ANALYTICS", "true")
    monkeypatch.delenv("TS_USER_EMAIL", raising=False)
    db_path = tmp_path / "explore.sqlite"
    _build_db(db_path)
    (tmp_path / "audio").mkdir()
    application = create_app(data_dir=str(tmp_path))
    init_index_manager(application, path=str(db_path))
    return application


@pytest.fixture
def anon(app):
    with TestClient(app) as client:
        yield client


@pytest.fixture
def client(app, monkeypatch):
    # Dev mode logs this identity in on the first authenticated request.
    monkeypatch.setenv("TS_USER_EMAIL", "tester@ivrit.ai")
    with TestClient(app) as c:
        yield c
