"""
Structural diffing utilities.

Detect when an HTML template has changed by comparing structural
fingerprints between runs.

This allows the scraper to detect upstream changes BEFORE failures cascade.
"""

import hashlib
from bs4 import BeautifulSoup


def structural_fingerprint(html: str) -> str:
    """
    Converts the rough structure of an HTML document into a stable fingerprint.

    We hash the sequence:
        <tagname>:<classnames>

    This creates a robust structural signature that changes when:
      - the DOM layout changes
      - class names shift
      - elements are added/removed

    It's intentionally coarse but extremely effective for drift detection.
    """
    soup = BeautifulSoup(html, "lxml")
    tokens = []

    for el in soup.find_all(True):  # True => all tags
        cls = " ".join(el.get("class", []))
        tokens.append(f"{el.name}:{cls}")

    combined = "|".join(tokens)
    return hashlib.sha1(combined.encode()).hexdigest()


def diff_summary(prev_html: str, curr_html: str) -> dict:
    """
    Compare previous HTML snapshot with current snapshot.
    Returns:
        {
            "changed": bool,
            "prev_fp": <hash>,
            "curr_fp": <hash>
        }
    """
    prev_fp = structural_fingerprint(prev_html) if prev_html else ""
    curr_fp = structural_fingerprint(curr_html)

    changed = bool(prev_fp and prev_fp != curr_fp)

    return {
        "changed": changed,
        "prev_fp": prev_fp,
        "curr_fp": curr_fp,
    }
