"""The regex pre-filter may only require words that every match contains."""
import pytest
import regex

from app.services.index import regex_prefilter_tokens


@pytest.mark.parametrize("pattern, expected", [
    # Optional parts are not required (PR #30 review, finding 4).
    ("שלום?", [["שלו"]]),
    ("שלום*", [["שלו"]]),
    ("שלום(ים)?", [["שלום"]]),
    ("(?:ab|cd)?\\sef", [["ef"]]),
    ("colou?r", [["colo"]]),
    # Alternation: one OR clause, only when every branch guarantees a word.
    (r"(שלום|היי)\s+(חברים)", [["היי", "שלום"], ["חברים"]]),
    ("אא|בב|גג|דד", [["אא", "בב", "גג", "דד"]]),
    ("שלום|.", None),
    ("שלו(ם|ן)", [["שלו"]]),
    # Escapes, classes, lookarounds and comments contribute no letters.
    (r"\bבינה\b", [["בינה"]]),
    (r"מספר\s+\d+", [["מספר"]]),
    ("(?=foo)bar", [["bar"]]),
    (r"(?P<x>שלום)\s(?#c)עולם", [["שלום"], ["עולם"]]),
    ("(?i)hello", [["hello"]]),
    (r"\.שלום", [["שלום"]]),
    # A word right after something that can be a letter isn't a reliable prefix.
    (r"\dשלום", None),
    ("x.שלום", None),
    (r"\x41bc", None),
    (r"\Bבינה", None),
    ("(?:ab|cd)?ef", None),
    # Nothing to narrow on: scan every document.
    (r"\d+", None),
    ("[רב]", None),
    ("ב", None),
    ("(?x) ab", None),
])
def test_prefilter(pattern, expected):
    assert regex_prefilter_tokens(pattern) == expected


@pytest.mark.parametrize("pattern, text", [
    ("שלום?", "שלו"),
    ("שלום(ים)?", "שלום"),
    (r"(שלום|היי)\s+(חברים)", "היי חברים"),
    ("colou?r", "color"),
    ("שלו(ם|ן)", "שלון"),
])
def test_every_match_starts_a_required_word(pattern, text):
    """For each clause, some token is a prefix of a word in the matched text."""
    match = regex.search(pattern, text)
    assert match
    words = regex.findall(r"\w+", text)
    for clause in regex_prefilter_tokens(pattern):
        assert any(w.startswith(t) for t in clause for w in words), clause
