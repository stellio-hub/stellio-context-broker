#!/usr/bin/env python3
"""NGSI-LD spec MCP server — searches ETSI GS CIM 009 by section number or keyword."""

import re
from mcp.server.fastmcp import FastMCP

PDF_PATH = "/Users/bobeal/Documents/NGSI-LD/gs_CIM009v010901p.pdf"

mcp = FastMCP("ngsi-ld-spec")

_cached_text: str | None = None


def _get_text() -> str:
    global _cached_text
    if _cached_text is None:
        from pypdf import PdfReader
        reader = PdfReader(PDF_PATH)
        _cached_text = "\n".join(page.extract_text() or "" for page in reader.pages)
    return _cached_text


@mcp.tool()
def get_spec_section(section: str) -> str:
    """
    Return the text of a numbered section from ETSI GS CIM 009 (e.g. '5.6.3', '4.3').
    Returns up to 4 000 characters starting at the section heading.
    """
    text = _get_text()
    # Match section number at the start of a line, possibly after whitespace
    pattern = re.compile(
        rf"(?m)^[ \t]*{re.escape(section)}[ \t]+\S",
    )
    match = pattern.search(text)
    if not match:
        return f"Section {section!r} not found in the spec. Try a broader number (e.g. '5.6' instead of '5.6.3')."

    start = match.start()
    excerpt = text[start : start + 4000].strip()
    return f"## ETSI GS CIM 009 — §{section}\n\n{excerpt}"


@mcp.tool()
def search_spec(keyword: str) -> str:
    """
    Full-text search of ETSI GS CIM 009 for a keyword or phrase.
    Returns up to 5 excerpts with 200 chars of context before and 500 after each match.
    """
    text = _get_text()
    pattern = re.compile(re.escape(keyword), re.IGNORECASE)
    matches = list(pattern.finditer(text))

    if not matches:
        return f"No matches found for {keyword!r} in the spec."

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

    header = f"Found {len(matches)} occurrences of {keyword!r}. Showing {len(snippets)} excerpt(s):\n"
    body = "\n\n---\n\n".join(snippets)
    return header + "\n" + body


if __name__ == "__main__":
    mcp.run()
