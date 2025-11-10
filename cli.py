#!/usr/bin/env python3
"""
Scraper Guardrails CLI
Run pipelines, triage dead-letter files, and inspect structural diffs.
"""

import argparse
import asyncio
from pathlib import Path
from guardrails.logs import setup as setup_logs
from pipelines.nba_boxscores import run as run_nba


def read_urls(path: Path):
    """Read URL list from a text file."""
    if not path.exists():
        raise FileNotFoundError(f"URL file not found: {path}")
    return [
        line.strip()
        for line in path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]


def main():
    parser = argparse.ArgumentParser(
        prog="scraper-guardrails",
        description="Fault-tolerant scraping with anti-fragile guardrails."
    )
    sub = parser.add_subparsers(dest="cmd", required=True)

    # ───────────────────────── RUN PIPELINE ─────────────────────────
    pr = sub.add_parser("run", help="Run a pipeline with guardrails")
    pr.add_argument("--pipeline", choices=["nba"], required=True)
    pr.add_argument("--urls-file", type=Path, required=True)
    pr.add_argument("--out", type=Path, default=Path("out/data.jsonl"))
    pr.add_argument("--dead-letter", type=Path, default=Path("out/dead_letter.jsonl"))
    pr.add_argument("--health", type=Path, default=Path("out/health.json"))

    # ───────────────────────── TRIAGE DEAD LETTER ─────────────────────────
    pt = sub.add_parser("triage", help="Summarize dead-letter failures")
    pt.add_argument("--dead-letter", type=Path, default=Path("out/dead_letter.jsonl"))

    # ───────────────────────── VIEW DIFF SNAPSHOTS ─────────────────────────
    pd = sub.add_parser("diffs", help="List cached structural snapshots")
    pd.add_argument("--cache", type=Path, default=Path(".cache"))

    args = parser.parse_args()
    log = setup_logs(step=args.cmd)

    # ───────────────────────── DISPATCH COMMANDS ─────────────────────────
    if args.cmd == "run":
        urls = read_urls(args.urls_file)

        if args.pipeline == "nba":
            asyncio.run(
                run_nba(
                    urls,
                    args.out,
                    args.dead_letter,
                    args.health,
                    pipeline_name="nba"
                )
            )

    elif args.cmd == "triage":
        dlq = args.dead_letter
        if not dlq.exists():
            print(f"No dead-letter file found at {dlq}")
            return 2

        import json
        counts = {}
        for line in dlq.read_text(encoding="utf-8").splitlines():
            try:
                err = json.loads(line).get("error", "UNKNOWN")
            except Exception:
                err = "PARSE_ERROR"
            counts[err] = counts.get(err, 0) + 1

        print("Dead-letter summary:")
        for k, v in sorted(counts.items(), key=lambda kv: (-kv[1], kv[0])):
            print(f"  - ({v}) {k[:160]}")

    elif args.cmd == "diffs":
        cache = args.cache
        if not cache.exists():
            print("No .cache directory found. Run a pipeline first.")
            return 0

        snaps = list(cache.glob("snap_*.html"))
        print(f"Cached snapshots: {len(snaps)}")
        for s in snaps[:25]:
            print("  -", s.name)

    else:
        parser.print_help()


if __name__ == "__main__":
    main()
