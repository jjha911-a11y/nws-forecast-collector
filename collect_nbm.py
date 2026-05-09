#!/usr/bin/env python3
"""
collect_nbm.py — NBM 5.0 KLAS bulletin collector
Discovers the latest available date/cycle from NOMADS dynamically
rather than assuming today's date (NOMADS only retains ~2 days).
"""

import os
import re
import sys
import time
import subprocess
import logging
from datetime import datetime, timezone
from pathlib import Path

import requests

STATION       = "KLAS"
OUTPUT_FILE   = Path("data/nbm_klas.txt")
NOMADS_BASE   = "https://nomads.ncep.noaa.gov/pub/data/nccf/com/blend/prod"
REQUEST_DELAY = 4
TIMEOUT       = 90
MAX_RETRIES   = 3
MAJOR_CYCLES  = {f"{c:02d}" for c in [1, 7, 13, 19]}

PRODUCTS = {
    "NBH": "nbhtx",
    "NBS": "nbstx",
    "NBE": "nbetx",
    "NBX": "nbxtx",
    "NBP": "nbptx",
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S UTC",
)
log = logging.getLogger(__name__)

SESSION = requests.Session()
SESSION.headers.update({"User-Agent": "KLAS-NBM-Collector/1.0 (Jeff KE7KLV)"})


def get_available_dates() -> list[str]:
    resp = SESSION.get(NOMADS_BASE + "/", timeout=30)
    resp.raise_for_status()
    dates = sorted(set(re.findall(r'blend\.(\d{8})/', resp.text)), reverse=True)
    log.info(f"NOMADS available dates: {dates}")
    return dates


def get_available_cycles(date_str: str) -> list[str]:
    url = f"{NOMADS_BASE}/blend.{date_str}/"
    resp = SESSION.get(url, timeout=30)
    if resp.status_code == 403:
        log.warning(f"  403 on {date_str} — skipping")
        return []
    resp.raise_for_status()
    # Directory listing: both HTML anchor and plain formats
    cycles = set(re.findall(r'href="(\d{2})/"', resp.text))
    cycles |= set(re.findall(r'\b(\d{2})/\s+\d{2}-\w{3}-\d{4}', resp.text))
    result = sorted(cycles, reverse=True)
    log.info(f"  Cycles for {date_str}: {result}")
    return result


def resolve_best_cycle() -> tuple[str, str] | None:
    """Walk NOMADS newest-first to find the latest major cycle with text/ files."""
    try:
        dates = get_available_dates()
    except Exception as e:
        log.error(f"Failed to list NOMADS dates: {e}")
        return None

    for date_str in dates:
        try:
            cycles = get_available_cycles(date_str)
        except Exception as e:
            log.warning(f"Could not list cycles for {date_str}: {e}")
            continue

        for cycle in cycles:
            if cycle not in MAJOR_CYCLES:
                continue
            text_url = f"{NOMADS_BASE}/blend.{date_str}/{cycle}/text/"
            try:
                r = SESSION.get(text_url, timeout=20)
                if r.status_code == 200 and "blend_nbstx" in r.text:
                    log.info(f"Best cycle found: {date_str} {cycle}Z")
                    return date_str, cycle
            except Exception:
                pass
            time.sleep(1)

    return None


def fetch_product(date_str: str, cycle: str, prod_key: str) -> str | None:
    suffix   = PRODUCTS[prod_key]
    filename = f"blend_{suffix}.t{cycle}z"
    url      = f"{NOMADS_BASE}/blend.{date_str}/{cycle}/text/{filename}"

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            log.info(f"Fetching {prod_key} ({filename}) attempt {attempt}/{MAX_RETRIES}")
            resp = SESSION.get(url, timeout=TIMEOUT, stream=True)

            if resp.status_code in (404, 403):
                log.warning(f"  HTTP {resp.status_code} — skipping {prod_key}")
                return None
            resp.raise_for_status()

            chunks = []
            for chunk in resp.iter_content(chunk_size=131072, decode_unicode=False):
                chunks.append(chunk)
            raw_bytes = b"".join(chunks)
            log.info(f"  Downloaded {len(raw_bytes):,} bytes")

            try:
                return raw_bytes.decode("utf-8")
            except UnicodeDecodeError:
                return raw_bytes.decode("latin-1")

        except requests.RequestException as e:
            log.warning(f"  Attempt {attempt} failed: {e}")
            if attempt < MAX_RETRIES:
                time.sleep(REQUEST_DELAY * attempt)

    log.error(f"  All {MAX_RETRIES} attempts failed for {prod_key}")
    return None


STATION_HDR     = re.compile(rf"^{re.escape(STATION)}\s+NBM\s+V[\d.]+\s+\w+\s+GUIDANCE", re.MULTILINE)
ANY_STATION_HDR = re.compile(r"^[A-Z]{3,4}\s+NBM\s+V[\d.]+\s+\w+\s+GUIDANCE", re.MULTILINE)


def extract_station_block(text: str) -> str | None:
    m = STATION_HDR.search(text)
    if not m:
        log.warning(f"  {STATION} not found in bulletin")
        return None
    nxt = ANY_STATION_HDR.search(text, m.end())
    block = text[m.start() : nxt.start() if nxt else len(text)].rstrip()
    log.info(f"  Extracted {STATION} block: {len(block):,} chars")
    return block


def write_output(blocks: dict, date_str: str, cycle: str) -> None:
    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    now_utc = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    lines = [
        "# NBM 5.0 KLAS bulletin extract",
        f"# Cycle: {date_str} {cycle}Z",
        f"# Retrieved: {now_utc}",
        f"# Products: {', '.join(blocks.keys())}",
        "# Generated by collect_nbm.py",
        "",
    ]
    for prod_key, block in blocks.items():
        lines.append(f"### PRODUCT:{prod_key} ###")
        lines.append(block)
        lines.append("")
    OUTPUT_FILE.write_text("\n".join(lines), encoding="utf-8")
    log.info(f"Wrote {OUTPUT_FILE} ({OUTPUT_FILE.stat().st_size:,} bytes)")


def git_commit_push(date_str: str, cycle: str) -> None:
    def run(cmd):
        r = subprocess.run(cmd, capture_output=True, text=True)
        log.info(f"  $ {' '.join(cmd)}: {(r.stdout+r.stderr).strip()[:120]}")
        return r

    run(["git", "config", "user.name",  "github-actions[bot]"])
    run(["git", "config", "user.email", "github-actions[bot]@users.noreply.github.com"])
    run(["git", "pull", "--rebase", "origin", "main"])
    run(["git", "add", str(OUTPUT_FILE)])

    r = subprocess.run(["git", "commit", "-m", f"NBM {cycle}Z {date_str}: update KLAS bulletins"],
                       capture_output=True, text=True)
    if "nothing to commit" in r.stdout + r.stderr:
        log.info("  Nothing changed — skip push")
        return
    run(["git", "push", "--force-with-lease"])


def main() -> int:
    log.info("NBM collector starting")

    result = resolve_best_cycle()
    if result is None:
        log.error("No available NBM cycle found on NOMADS — aborting")
        return 1

    date_str, cycle = result
    log.info(f"Target: {date_str} {cycle}Z  Station: {STATION}  Output: {OUTPUT_FILE}")

    blocks: dict[str, str] = {}
    for prod_key in PRODUCTS:
        raw = fetch_product(date_str, cycle, prod_key)
        if raw:
            block = extract_station_block(raw)
            if block:
                blocks[prod_key] = block
        time.sleep(REQUEST_DELAY)

    if not blocks:
        log.error("No data extracted — aborting without commit")
        return 1

    log.info(f"Extracted {len(blocks)}/{len(PRODUCTS)} products: {list(blocks.keys())}")
    write_output(blocks, date_str, cycle)
    git_commit_push(date_str, cycle)
    log.info("Done.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
