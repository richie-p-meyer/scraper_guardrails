"""
NBA boxscore pipeline using scraper guardrails.

This is a real example pipeline that demonstrates:
- resilient fetch with retry/backoff
- selector fallback sets (anti-fragile parsing)
- structural diff detection
- dead-letter capture
- health status output
- structured logging
"""

import asyncio
import json
from pathlib import Path
from typing import List

from bs4 import BeautifulSoup

from guardrails.fetch import fetch_text, RateLimiter
from guardrails.selectors import try_select
from guardrails.diffwatch import diff_summary
from guardrails.health import write_status
from guardrails.logs import setup as setup_logs


# Rate limiter: 1.5 requests/sec with a burst capacity of 3
RL = RateLimiter(rate_per_sec=1.5, capacity=3)


async def scrape_one(url: str, cache_dir: Path, log) -> dict:
    """Scrape a single NBA page using guardrails."""

    log.info("fetching", extra={"url": url, "step": "fetch"})

    html = await fetch_text(url, RL)
    soup = BeautifulSoup(html, "lxml")

    # Anti-fragile selector set for page title / headline
    title = try_select(soup, [
        ("css", "h1.headline"),
        ("css", "header h1"),
        ("attr", "meta[property='og:title']::content"),
        ("css", "title"),
    ])

    # Cache system for diffwatch
    cache_dir.mkdir(parents=True, exist_ok=True)
    snap_file = cache_dir / ("snap_" + url.replace("/", "_")[:150] + ".html")

    prev_html = ""
    if snap_file.exists():
        prev_html = snap_file.read_text(encoding="utf-8")

    diff = diff_summary(prev_html, html)

    # Save new snapshot
    snap_file.write_text(html, encoding="utf-8")

    return {
        "url": url,
        "title": title.value,
        "selector_idx": title.idx,
        "selector_strategy": title.strategy,
        "struct_changed": diff["changed"],
    }


async def run(
    urls: List[str],
    out_ok: Path,
    out_dlq: Path,
    health_path: Path,
    pipeline_name: str = "nba"
):
    """
    Run the NBA pipeline over a list of URLs.

    Writes:
      out_ok          → successful extractions (JSONL)
      out_dlq         → failed URLs + error info (JSONL)
      health.json     → run summary
    """

    log = setup_logs(pipeline=pipeline_name, step="run")

    ok, bad = 0, 0
    cache_dir = Path(".cache")

    # Ensure output dirs exist
    out_ok.parent.mkdir(parents=True, exist_ok=True)
    out_dlq.parent.mkdir(parents=True, exist_ok=True)

    async def handle(url: str):
        nonlocal ok, bad

        try:
            item = await scrape_one(url, cache_dir, log)
            out_ok.open("a", encoding="utf-8").write(json.dumps(item) + "\n")
            ok += 1

        except Exception as e:
            log.warning("scrape failed", extra={"url": url, "error_code": "SCRAPE_FAIL"})
            out_dlq.open("a", encoding="utf-8").write(json.dumps({
                "url": url,
                "error": str(e)
            }) + "\n")
            bad += 1

    # Run with bounded concurrency via gather()
    await asyncio.gather(*(handle(u) for u in urls))

    # Write health report
    write_status(health_path, ok=ok, total=len(urls))

    log.info("pipeline complete", extra={"step": "done", "ok": ok, "total": len(urls)})
