"""PostgresTranscriptIndex behaviour that doesn't need a live server.

SQL is answered by a stub keyed on the statement, so these cover the Python
side: tsquery construction, hit shape, and error translation.
"""
import psycopg
import pytest

from app.services import index as index_module
from app.services.index import InvalidQuery, SearchTimeout
from app.services.pg_index import PostgresTranscriptIndex, _quote_lexeme, build_tsquery

TEXTS = {
    0: "שלום חברים, אנחנו בשיעור על פרשה ושיתוף של בינה ו-929",
    1: "היי חברים וברוכים הבאים",
}


class StubIndex(PostgresTranscriptIndex):
    def __init__(self, offsets=None, error=None):
        super().__init__(pool=None)
        self.offsets = offsets or []
        self.error = error
        self.sql = []

    def _query(self, sql, params=()):
        self.sql.append(sql)
        if self.error is not None:
            raise self.error
        if "SELECT d.doc_id FROM documents d" in sql:
            return [(doc_id,) for doc_id in TEXTS]
        if "SELECT doc_id, full_text FROM documents" in sql:
            return [(d, TEXTS[d]) for d in params[0] if d in TEXTS]
        if "regexp_split_to_table" in sql:
            return self.offsets
        if "MAX(end_time)" in sql:
            return [(0, 8.0), (1, None)]
        raise AssertionError(f"unexpected SQL: {sql}")


def test_regex_tsquery_ignores_escapes_and_classes():
    assert build_tsquery(r"\bבינה\b", "regex") == ("to_tsquery", "'בינה':*")
    assert build_tsquery(r"[a-z]+\dשלום", "regex") == ("to_tsquery", "'שלום':*")


def test_regex_tsquery_alternation_ors_every_token():
    func, arg = build_tsquery(r"(שלום|היי)\s+(חברים)", "regex")
    assert arg == "'שלום':* | 'היי':* | 'חברים':*"
    # Not truncated to three: any branch alone may be the match.
    assert build_tsquery("אא|בב|גג|דד", "regex")[1].count("|") == 3


def test_regex_tsquery_branch_without_token_scans_everything():
    assert build_tsquery("שלום|.", "regex") == (None, None)


def test_quote_lexeme_escapes_backslash_and_quote():
    assert _quote_lexeme("a\\") == "'a\\\\'"
    assert _quote_lexeme("it's") == "'it''s'"


def test_regex_hits_carry_end_and_groups():
    idx = StubIndex()
    hits, has_more = idx.search_hits(r"(שלום|היי)\s+(חברים)", search_mode="regex")
    assert not has_more
    by_doc = {h[0]: h for h in hits}
    doc_id, start, end, groups = by_doc[0]
    assert TEXTS[0][start:end] == "שלום חברים"
    assert [TEXTS[0][s:e] for s, e in groups] == ["שלום", "חברים"]
    assert [TEXTS[1][s:e] for s, e in by_doc[1][3]] == ["היי", "חברים"]


def test_unmatched_group_is_minus_one():
    hits, _ = StubIndex().search_hits(r"חברים(XYZ)?", search_mode="regex")
    assert all(h[3] == ((-1, -1),) for h in hits)


def test_server_side_hits_span_the_literal():
    idx = StubIndex(offsets=[(0, 5), (1, 4)])
    hits, _ = idx.search_hits("חברים", search_mode="exact")
    assert hits == [(0, 5, 10, ()), (1, 4, 9, ())]


def test_invalid_regex_raises_invalid_query():
    with pytest.raises(InvalidQuery):
        StubIndex().search_hits("(שלום", search_mode="regex")


def test_regex_budget_raises_search_timeout(monkeypatch):
    monkeypatch.setattr(index_module, "REGEX_TIME_BUDGET_S", 0)
    with pytest.raises(SearchTimeout):
        StubIndex().search_hits("חברים", search_mode="regex")


@pytest.mark.parametrize("error, expected", [
    (psycopg.errors.SyntaxError("bad tsquery"), InvalidQuery),
    (psycopg.errors.InvalidRegularExpression("bad re"), InvalidQuery),
    (psycopg.errors.QueryCanceled("statement timeout"), SearchTimeout),
])
def test_database_errors_are_translated(error, expected):
    idx = StubIndex(error=error)
    with pytest.raises(expected):
        idx.search_hits("חברים", search_mode="partial")
    with pytest.raises(expected):
        idx.get_search_metadata("חברים", "partial")


def test_durations():
    assert StubIndex().get_durations([0, 1, 0]) == {0: 8.0, 1: 0.0}
    assert StubIndex().get_durations([]) == {}
