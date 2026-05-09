#!/usr/bin/env python3
"""
collect_nbm.py
--------------
Fetches NBM 5.0 text bulletins for KLAS from NOMADS, extracts the station
block for each product (NBH, NBS, NBE, NBX, NBP), and writes the result to
data/nbm_klas.txt in the repo.

Run by GitHub Actions at 02Z, 08Z, 14Z, 20Z — one hour after each bulletin
cycle (01Z, 07Z, 13Z, 19Z) to ensure the files are fully written to NOMADS.

NOMADS path pattern:
  https://nomads.ncep.noaa.gov/pub/data/nccf/com/blend/prod/
    blend.YYYYMMDD/HH/text/blend_nb{s,h,e,x,p}tx.tHHz

Station block format (fixed-width ASCII, unchanged from v4.3 per SCN 26-24):
  Starts with a line matching:  KLAS   NBM V5.0 NB* GUIDANCE ...
  Ends just before the next station header or end of file.
"""

import os
import re
import sys
import time
import subprocess
import logging
from datetime import datetime, timezone, timedelta
from pathlib import Path

import requests

# ── Config ────────────────────────────────────────────────────────────────────
STATION       = "KLAS"
OUTPUT_FILE   = Path("data/nbm_klas.txt")
NOMADS_BASE   = "https://nomads.ncep.noaa.gov/pub/data/nccf/com/blend/prod"
REQUEST_DELAY = 4          # seconds between NOMADS requests — be polite
TIMEOUT       = 60         # seconds per request
MAX_RETRIES   = 3

# Product codes → NOMADS filename suffix
PRODUCTS = {
    "NBH": "nbhtx",
    "NBS": "nbstx",
    "NBE": "nbetx",
    "NBX": "nbxtx",
    "NBP": "nbptx",
}

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S UTC",
)
log = logging.getLogger(__name__)


# ── Cycle resolution ──────────────────────────────────────────────────────────
def resolve_cycle() -> tuple[str, str]:
    """
    Return (YYYYMMDD, HH) for the most recently completed NBM cycle.
    Valid bulletin cycles: 01, 07, 13, 19 UTC.
    We run ~1 hour after each cycle, so 'most recent completed' is reliable.
    """
    now = datetime.now(timezone.utc)
    valid_cycles = [1, 7, 13, 19]

    # Walk back up to 24 hours to find the last completed cycle
    for hours_back in range(0, 25):
        candidate = now - timedelta(hours=hours_back)
        if candidate.hour in valid_cycles:
            date_str  = candidate.strftime("%Y%m%d")
            cycle_str = f"{candidate.hour:02d}"
            return date_str, cycle_str

    # Fallback — should never reach here
    return now.strftime("%Y%m%d"), "19"


# ── NOMADS fetch ──────────────────────────────────────────────────────────────
def fetch_product(date_str: str, cycle: str, prod_key: str) -> str | None:
    """
    Download the full bulk text file for one NBM product from NOMADS and
    return the raw text, or None on failure.
    """
    suffix   = PRODUCTS[prod_key]
    filename = f"blend_{suffix}.t{cycle}z"
    url      = f"{NOMADS_BASE}/blend.{date_str}/{cycle}/text/{filename}"

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            log.info(f"Fetching {prod_key} ({filename}) attempt {attempt}/{MAX_RETRIES}")
            resp = requests.get(url, timeout=TIMEOUT, stream=True,
                                headers={"User-Agent": "KLAS-NBM-Collector/1.0 (Jeff KE7KLV)"})

            if resp.status_code == 404:
                log.warning(f"  404 — {prod_key} not yet available for {date_str}/{cycle}Z")
                return None

            resp.raise_for_status()

            # Read streaming to avoid loading entire ~25MB file at once
            chunks = []
            for chunk in resp.iter_content(chunk_size=65536, decode_unicode=False):
                chunks.append(chunk)
            raw_bytes = b"".join(chunks)

            # Try UTF-8, fall back to latin-1 (NBM bulletins are ASCII-safe)
            try:
                text = raw_bytes.decode("utf-8")
            except UnicodeDecodeError:
                text = raw_bytes.decode("latin-1")

            log.info(f"  OK — {len(text):,} chars")
            return text

        except requests.RequestException as e:
            log.warning(f"  Request error: {e}")
            if attempt < MAX_RETRIES:
                time.sleep(REQUEST_DELAY * attempt)

    log.error(f"  Failed after {MAX_RETRIES} attempts — skipping {prod_key}")
    return None


# ── Station block extractor ───────────────────────────────────────────────────
# Station header pattern — matches lines like:
#   "KLAS   NBM V5.0 NBH GUIDANCE  5/08/2026  1300 UTC"
# The regex is intentionally broad to handle version number changes.
STATION_HDR = re.compile(
    r"^" + re.escape(STATION) + r"\s+NBM\s+V[\d.]+\s+\w+\s+GUIDANCE",
    re.MULTILINE,
)

# Any station header (to detect where next station starts)
ANY_STATION_HDR = re.compile(
    r"^[A-Z]{4}\s+NBM\s+V[\d.]+\s+\w+\s+GUIDANCE",
    re.MULTILINE,
)


def extract_station_block(text: str, station: str) -> str | None:
    """
    Find and return the bulletin block for `station` within the full bulk text.
    The block starts at the station's header line and ends just before the
    next station's header (or end of file).
    """
    # Find our station's header
    our_match = STATION_HDR.search(text)
    if not our_match:
        log.warning(f"  Station {station} not found in bulletin")
        return None

    block_start = our_match.start()

    # Find the next station header after ours
    next_match = ANY_STATION_HDR.search(text, our_match.end())
    block_end = next_match.start() if next_match else len(text)

    block = text[block_start:block_end].rstrip()
    log.info(f"  Extracted {station} block: {len(block):,} chars, "
             f"{block.count(chr(10))} lines")
    return block


# ── Output writer ─────────────────────────────────────────────────────────────
def write_output(blocks: dict[str, str], date_str: str, cycle: str) -> None:
    """
    Write all extracted blocks to OUTPUT_FILE, separated by a delimiter line.
    The dashboard reads this file and splits on the delimiter to get per-product text.
    """
    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)

    now_utc = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    lines = [
        f"# NBM 5.0 KLAS bulletin extract",
        f"# Cycle: {date_str} {cycle}Z",
        f"# Retrieved: {now_utc}",
        f"# Products: {', '.join(blocks.keys())}",
        f"# Generated by collect_nbm.py",
        "",
    ]

    for prod_key, block in blocks.items():
        lines.append(f"### PRODUCT:{prod_key} ###")
        lines.append(block)
        lines.append("")   # blank line between products

    OUTPUT_FILE.write_text("\n".join(lines), encoding="utf-8")
    log.info(f"Wrote {OUTPUT_FILE} — {OUTPUT_FILE.stat().st_size:,} bytes")


# ── Git commit & push ─────────────────────────────────────────────────────────
def git_commit_push(date_str: str, cycle: str) -> None:
    """
    Pull latest, stage the output file, commit, and push.
    Mirrors the approach used in collect_forecast.py.
    """
    def run(cmd: list[str]) -> subprocess.CompletedProcess:
        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode != 0:
            log.warning(f"  git {cmd[1]}: {result.stderr.strip()}")
        else:
            log.info(f"  git {cmd[1]}: OK")
        return result

    log.info("Committing and pushing…")
    run(["git", "config", "user.name",  "github-actions[bot]"])
    run(["git", "config", "user.email", "github-actions[bot]@users.noreply.github.com"])
    run(["git", "pull", "--rebase", "origin", "main"])
    run(["git", "add", str(OUTPUT_FILE)])

    msg = f"NBM {cycle}Z {date_str}: update KLAS bulletins"
    result = subprocess.run(
        ["git", "commit", "-m", msg],
        capture_output=True, text=True
    )
    if "nothing to commit" in result.stdout + result.stderr:
        log.info("  Nothing changed — skip push")
        return

    run(["git", "push", "--force-with-lease"])


# ── Main ──────────────────────────────────────────────────────────────────────
def main() -> int:
    date_str, cycle = resolve_cycle()
    log.info(f"NBM collector starting — target cycle: {date_str} {cycle}Z")
    log.info(f"Station: {STATION}  Output: {OUTPUT_FILE}")

    blocks: dict[str, str] = {}

    for prod_key in PRODUCTS:
        raw_text = fetch_product(date_str, cycle, prod_key)
        if raw_text is None:
            log.warning(f"Skipping {prod_key} — no data")
            time.sleep(REQUEST_DELAY)
            continue

        block = extract_station_block(raw_text, STATION)
        if block:
            blocks[prod_key] = block
        else:
            log.warning(f"  No {STATION} block found in {prod_key}")

        time.sleep(REQUEST_DELAY)

    if not blocks:
        log.error("No data extracted for any product — aborting without commit")
        return 1

    log.info(f"Extracted {len(blocks)}/{len(PRODUCTS)} products: {list(blocks.keys())}")
    write_output(blocks, date_str, cycle)
    git_commit_push(date_str, cycle)

    log.info("Done.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
