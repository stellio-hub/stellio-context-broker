#!/usr/bin/env python3
"""NGSI-LD spec MCP server — searches ETSI TS 104 175 (Core API) and TS 104 176 (HTTP Binding)."""

import re
from pathlib import Path
from typing import Literal
from mcp.server.fastmcp import FastMCP

# Resolved from this file so the server works whatever directory it is launched from
SPEC_DIR = Path(__file__).resolve().parents[3] / "docs/specification"
DOCUMENTS = {
    "core": ("ETSI TS 104 175 (NGSI-LD Core API)", SPEC_DIR / "ts_104175v010101p.pdf"),
    "http": ("ETSI TS 104 176 (NGSI-LD HTTP Binding)", SPEC_DIR / "ts_104176v010101p.pdf"),
}
Document = Literal["core", "http"]

mcp = FastMCP("ngsi-ld-spec")

_cached_texts: dict[str, str] = {}


def _get_text(document: str) -> str:
    if document not in _cached_texts:
        from pypdf import PdfReader
        reader = PdfReader(DOCUMENTS[document][1])
        _cached_texts[document] = "\n".join(page.extract_text() or "" for page in reader.pages)
    return _cached_texts[document]


def _find_section(text: str, section: str) -> str | None:
    # Match section number at the start of a line, possibly after whitespace
    pattern = re.compile(
        rf"(?m)^[ \t]*{re.escape(section)}[ \t]+\S",
    )
    # Skip table-of-contents entries (lines with "....." leaders) to land on the actual section
    match = next(
        (m for m in pattern.finditer(text) if "....." not in text[m.start() : text.find("\n", m.start())]),
        None,
    )
    return text[match.start() : match.start() + 4000].strip() if match else None


@mcp.tool()
def get_spec_section(section: str, document: Document | None = None) -> str:
    """
    Return the text of a numbered section (e.g. '7.2.3', '10.2.2').
    `document` is 'core' (TS 104 175: data model, behaviours, abstract operations) or 'http'
    (TS 104 176: HTTP resources, headers, status codes). Section numbers overlap between the two documents,
    so when omitted, the section is returned from both.
    Returns up to 4 000 characters starting at the section heading, per document.
    """
    results = []
    for key in [document] if document else DOCUMENTS:
        excerpt = _find_section(_get_text(key), section)
        if excerpt:
            results.append(f"## {DOCUMENTS[key][0]} — §{section}\n\n{excerpt}")
    if not results:
        return f"Section {section!r} not found in the spec. Try a broader number (e.g. '7.2' instead of '7.2.3')."
    return "\n\n---\n\n".join(results)


@mcp.tool()
def search_spec(keyword: str, document: Document | None = None) -> str:
    """
    Full-text search for a keyword or phrase in TS 104 175 ('core') and/or TS 104 176 ('http'); both when omitted.
    Returns up to 5 excerpts per document with 200 chars of context before and 500 after each match.
    """
    pattern = re.compile(re.escape(keyword), re.IGNORECASE)
    results = []
    for key in [document] if document else DOCUMENTS:
        text = _get_text(key)
        matches = list(pattern.finditer(text))
        if not matches:
            continue
        snippets: list[str] = []
        seen_offsets: list[int] = []
        for m in matches:
            # Skip matches too close to one another (dedup overlapping excerpts)
            if any(abs(m.start() - s) < 600 for s in seen_offsets):
                continue
            seen_offsets.append(m.start())
            start = max(0, m.start() - 200)
            end = min(len(text), m.end() + 500)
            snippets.append(text[start:end].strip())
            if len(snippets) == 5:
                break
        header = f"## {DOCUMENTS[key][0]}: {len(matches)} occurrences of {keyword!r}, showing {len(snippets)} excerpt(s)\n"
        results.append(header + "\n" + "\n\n---\n\n".join(snippets))

    if not results:
        return f"No matches found for {keyword!r} in the spec."
    return "\n\n===\n\n".join(results)


if __name__ == "__main__":
    mcp.run()
