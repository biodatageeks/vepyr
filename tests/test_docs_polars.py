"""Polars calls in the documentation that must not depend on the Polars version."""

import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
# The pages a user copies code from: the README, the documentation site and the
# notebooks. docs/superpowers holds dated plans, which are not kept current.
PAGES = sorted(
    [
        ROOT / "README.md",
        *(ROOT / "docs").glob("*.md"),
        *(ROOT / "notebooks").glob("*.ipynb"),
    ]
)


def explode_calls(text: str) -> list[tuple[int, str]]:
    """Every ``.explode(...)`` call in ``text`` as (line number, call)."""
    calls = []
    for match in re.finditer(r"\.explode\(", text):
        depth, end = 1, match.end()
        while depth and end < len(text):
            depth += {"(": 1, ")": -1}.get(text[end], 0)
            end += 1
        calls.append(
            (text.count("\n", 0, match.start()) + 1, text[match.start() : end])
        )
    return calls


def test_explode_calls_are_found_with_their_arguments():
    text = "a\nlf.explode(cols)\ndf.explode(\n    [f(x), 'b'],\n    empty_as_null=True,\n)\n"
    assert explode_calls(text) == [
        (2, ".explode(cols)"),
        (3, ".explode(\n    [f(x), 'b'],\n    empty_as_null=True,\n)"),
    ]


def test_documented_explode_calls_set_empty_as_null():
    # Polars 2.0 changed the default from True (an empty list becomes one null
    # row) to False (the row is dropped), and vepyr supports both sides of that
    # change. A documented query must return the same rows on either.
    assert PAGES
    bare = [
        f"{page.relative_to(ROOT)}:{line}: {call}"
        for page in PAGES
        for line, call in explode_calls(page.read_text(encoding="utf-8"))
        if "empty_as_null=" not in call
    ]
    assert not bare, bare
