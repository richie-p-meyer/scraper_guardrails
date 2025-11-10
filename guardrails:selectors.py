"""
Resilient selector utilities for HTML extraction.

Provide multiple fallback strategies so pipelines keep working even when
a page template changes (anti-fragile parsing).
"""

from typing import List, Optional, Tuple
from bs4 import BeautifulSoup


class SelectorResult:
    """
    Holds the extraction result AND the provenance:
      - value: the extracted string
      - strategy: which method succeeded ("css", "attr", "none")
      - idx: index of the fallback candidate
    """
    def __init__(self, value: Optional[str], strategy: str, idx: int):
        self.value = value
        self.strategy = strategy
        self.idx = idx

    def __repr__(self):
        return f"SelectorResult(value={self.value!r}, strategy={self.strategy!r}, idx={self.idx})"


def try_select(
    soup: BeautifulSoup,
    candidates: List[Tuple[str, str]]
) -> SelectorResult:
    """
    Try multiple selectors until one succeeds.

    candidates: List of (strategy, expr)
        strategy ∈ {"css", "attr"}
        - "css": standard CSS selector (soup.select_one(expr))
        - "attr": "selector::attribute" form; extracts HTML attributes

    Returns:
        SelectorResult(value, strategy_name, candidate_index)

    If no selector matches, returns SelectorResult(None, "none", -1).
    """

    for i, (strategy, expr) in enumerate(candidates):

        # CSS selector extraction
        if strategy == "css":
            el = soup.select_one(expr)
            if el:
                text = el.get_text(strip=True)
                if text:
                    return SelectorResult(text, "css", i)

        # Attribute extraction: e.g.  "meta[property='og:title']::content"
        elif strategy == "attr":
            selector, _, attr = expr.partition("::")
            el = soup.select_one(selector)
            if el and el.get(attr):
                return SelectorResult(el.get(attr), "attr", i)

    # No candidate worked → return empty result
    return SelectorResult(None, "none", -1)
