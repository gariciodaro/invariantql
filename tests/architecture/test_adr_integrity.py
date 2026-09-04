"""FF-16: ADR filenames, statuses, headings, links, and register membership."""

from __future__ import annotations

import re
from pathlib import Path

import pytest

from tests.conftest import DOCS_ROOT

pytestmark = pytest.mark.architecture

DECISIONS = DOCS_ROOT / "decisions"
REQUIRED_HEADINGS = [
    "## Context",
    "## Decision",
    "## Alternatives considered",
    "## Consequences and trade-offs",
    "## Fitness functions",
    "## Revisit when",
]
FILENAME = re.compile(r"^(\d{4})-[a-z0-9-]+\.md$")


def _adrs() -> list[Path]:
    return sorted(p for p in DECISIONS.glob("*.md") if not p.name.startswith("0000-"))


@pytest.mark.parametrize("path", _adrs(), ids=lambda p: p.name)
def test_adr_has_required_structure(path: Path) -> None:
    assert FILENAME.match(path.name), path.name
    text = path.read_text(encoding="utf-8")
    number = path.name[:4]
    assert text.startswith(f"# ADR-{number}: "), path.name
    status = re.search(r"^- \*\*Status:\*\* (Accepted|Proposed|Superseded|Deprecated)", text, re.M)
    assert status, f"{path.name} has no valid status line"
    for heading in REQUIRED_HEADINGS:
        assert heading in text, f"{path.name} lacks {heading}"
    for link in re.findall(r"\]\(([^)#]+)(?:#[^)]*)?\)", text):
        if "://" in link:
            continue
        assert (path.parent / link).resolve().exists(), f"{path.name} links to missing {link}"


def test_register_lists_every_adr() -> None:
    readme = (DOCS_ROOT / "README.md").read_text(encoding="utf-8")
    for path in _adrs():
        number = path.name[:4]
        status = re.search(
            r"^- \*\*Status:\*\* (\w+)", path.read_text(encoding="utf-8"), re.M
        ).group(1)
        row = re.search(
            rf"^\| \[{number}\]\(decisions/{re.escape(path.name)}\) \| .+ \| (\w+) \|$",
            readme,
            re.M,
        )
        assert row, f"ADR {number} missing from the decision register"
        assert row.group(1) == status, f"register status for {number} differs from the ADR"


def test_adr_numbers_are_unique_and_contiguous() -> None:
    numbers = [int(p.name[:4]) for p in _adrs()]
    assert numbers == list(range(1, len(numbers) + 1)), numbers
