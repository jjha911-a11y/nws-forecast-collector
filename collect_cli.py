#!/usr/bin/env python3
"""
CLI (Daily Climate Report) Collector for KLAS (Las Vegas)
==========================================================
Fetches the NWS CLI product for LAS from the NWS products API and parses
it into a structured CSV row, one row per calendar day.

The CLI is issued once per day (typically 8-10 AM local time) and contains
the official observed record for the previous calendar day: high/low temp
with time of occurrence, precipitation, wind (average, highest, gust),
sky cover, sunshine, relative humidity, and weather conditions.

This CSV joins to forecasts.csv on forecast_date = obs_date to enable
verification of the NDFD forecast against the official observed record.

Run schedule: once daily, shortly after 10 AM PDT/PST to ensure the CLI
has been issued. A separate GitHub Actions workflow handles this.
"""

import requests
import csv
import os
import sys
import re
import time
from datetime import datetime, timezone, date

# ── Configuration ──────────────────────────────────────────────────────────────
STATION      = "LAS"        # NWS CLI location code for Las Vegas
OUTPUT_FILE  = "data/observations_cli.csv"

HEADERS = {
    "User-Agent": "CLI-Collector/1.0 (github-actions; klas-verification)",
    "Accept":     "application/geo+json",
}

CSV_COLUMNS = [
    # ── Identity ───────────────────────────────────────────────────────────────
    "obs_date",              # calendar date this CLI covers (YYYY-MM-DD)
    "product_id",            # NWS product ID (e.g. CLILAS)
    "issued_at",             # when NWS issued this CLI (UTC ISO8601)
    "retrieved_at",          # when this script ran (UTC ISO8601)
    # ── Temperature ───────────────────────────────────────────────────────────
    "max_temp_f",            # observed high temperature °F
    "max_temp_time",         # time of max temp (e.g. "215 PM")
    "min_temp_f",            # observed low temperature °F
    "min_temp_time",         # time of min temp (e.g. "558 AM")
    "avg_temp_f",            # observed average temperature °F
    "normal_high_f",         # climatological normal high °F
    "normal_low_f",          # climatological normal low °F
    "normal_avg_f",          # climatological normal average °F
    "dep_max_f",             # departure from normal — high temp
    "dep_min_f",             # departure from normal — low temp
    "dep_avg_f",             # departure from normal — average temp
    "record_high_f",         # record high for this date
    "record_high_year",      # year record high was set
    "record_low_f",          # record low for this date
    "record_low_year",       # year record low was set
    # ── Precipitation ─────────────────────────────────────────────────────────
    "precip_today_in",       # observed precipitation inches (T = 0.001)
    "precip_month_in",       # month-to-date precipitation inches
    "precip_year_in",        # year-to-date precipitation inches
    "precip_normal_month_in",# normal month-to-date precipitation
    "precip_normal_year_in", # normal year-to-date precipitation
    # ── Wind ──────────────────────────────────────────────────────────────────
    "avg_wind_speed_mph",    # average wind speed mph
    "max_wind_speed_mph",    # highest sustained wind speed mph
    "max_wind_dir_deg",      # direction of highest wind (degrees)
    "max_wind_dir_compass",  # direction of highest wind (cardinal)
    "max_wind_time",         # time of highest wind
    "peak_gust_mph",         # peak wind gust mph
    "peak_gust_dir_deg",     # direction of peak gust (degrees)
    "peak_gust_dir_compass", # direction of peak gust (cardinal)
    "peak_gust_time",        # time of peak gust
    # ── Sky / Sunshine ────────────────────────────────────────────────────────
    "sky_cover_tenths",      # average sky cover (tenths, 0-10)
    "sunshine_pct",          # percent of possible sunshine
    "sunshine_hours",        # hours of sunshine
    # ── Humidity ──────────────────────────────────────────────────────────────
    "rh_max_pct",            # maximum relative humidity %
    "rh_max_time",           # time of max RH
    "rh_min_pct",            # minimum relative humidity %
    "rh_min_time",           # time of min RH
    # ── Weather Conditions ────────────────────────────────────────────────────
    "weather_conditions",    # observed weather (fog, dust, thunderstorm, etc.)
]


# ── NWS API ────────────────────────────────────────────────────────────────────
def nws_get(url, retries=3, timeout=20):
    for attempt in range(1, retries + 1):
        try:
            print(f"    GET {url}  (attempt {attempt}/{retries})")
            resp = requests.get(url, headers=HEADERS, timeout=timeout)
            resp.raise_for_status()
            return resp
        except (requests.exceptions.Timeout, requests.exceptions.ConnectionError):
            if attempt == retries:
                raise
            print(f"    Timeout — waiting 10s before retry…")
            time.sleep(10)
        except requests.exceptions.HTTPError:
            if attempt == retries or resp.status_code not in (500, 503):
                raise
            print(f"    HTTP {resp.status_code} — waiting 10s before retry…")
            time.sleep(10)


def fetch_latest_cli():
    """
    Fetch the most recent CLI product for the station.
    Returns (product_id, issuance_time_str, product_text) or raises.
    """
    list_url = f"https://api.weather.gov/products/types/CLI/locations/{STATION}"
    products = nws_get(list_url).json().get("@graph", [])
    if not products:
        raise RuntimeError(f"No CLI products found for {STATION}")

    latest = products[0]
    product_id   = latest["id"]
    issued_at    = latest.get("issuanceTime", "")

    detail_url = f"https://api.weather.gov/products/{product_id}"
    text = nws_get(detail_url).json().get("productText", "")

    return product_id, issued_at, text


# ── Text parsing helpers ───────────────────────────────────────────────────────
def pn(s):
    """Parse a number, returning None for missing (M/MM) and 0.001 for trace (T)."""
    if not s:
        return None
    t = s.strip()
    if re.match(r'^M+$', t) or t == '':
        return None
    if t.upper() == 'T':
        return 0.001
    try:
        return float(t)
    except ValueError:
        return None

def pi(s):
    """Parse an integer."""
    n = pn(s)
    return int(round(n)) if n is not None else None

def degrees_to_compass(deg):
    if deg is None:
        return None
    dirs = ['N','NNE','NE','ENE','E','ESE','SE','SSE','S','SSW','SW','WSW','W','WNW','NW','NNW']
    return dirs[round(deg / 22.5) % 16]

def extract_block(lines, start_re, end_re):
    """Return lines between start_re match and end_re match."""
    capturing = False
    block = []
    for line in lines:
        if not capturing and re.search(start_re, line, re.I):
            capturing = True
            block.append(line)
            continue
        if capturing:
            if end_re and re.search(end_re, line, re.I) and len(block) > 1:
                break
            block.append(line)
    return block


# ── CLI section parsers ────────────────────────────────────────────────────────
def parse_obs_date(text, issued_at):
    """
    Extract the calendar date this CLI covers.
    The CLI headline contains "CLIMATE SUMMARY FOR <MONTH> <D>, <YYYY>"
    or similar. Falls back to issued_at date minus one day.
    """
    # Try: "CLIMATE SUMMARY FOR APRIL 21, 2026" or "FOR APRIL 21 2026"
    m = re.search(
        r'CLIMATE\s+(?:REPORT|SUMMARY)\s+FOR\s+(\w+)\s+(\d{1,2})[,\s]+(\d{4})',
        text, re.I
    )
    if m:
        months = {
            'JANUARY':1,'FEBRUARY':2,'MARCH':3,'APRIL':4,'MAY':5,'JUNE':6,
            'JULY':7,'AUGUST':8,'SEPTEMBER':9,'OCTOBER':10,'NOVEMBER':11,'DECEMBER':12
        }
        mon = months.get(m.group(1).upper())
        if mon:
            return f"{m.group(3)}-{mon:02d}-{int(m.group(2)):02d}"

    # Fallback: issued_at date minus one day (CLI covers yesterday)
    if issued_at:
        try:
            issued_date = date.fromisoformat(issued_at[:10])
            from datetime import timedelta
            return str(issued_date - timedelta(days=1))
        except Exception:
            pass

    return None


def parse_temperature(lines, text):
    temp = {}
    block = extract_block(
        lines,
        r'TEMPERATURE',
        r'PRECIPITATION|SNOWFALL|DEGREE|WIND|SKY|SUNSHINE|RELATIVE|WEATHER'
    )
    for line in block:
        # MAXIMUM  87  215 PM  116  1942  105  -18  92
        m = re.match(
            r'^\s*MAXIMUM\s+([\-\d]+)\s+(\d{1,4}\s*(?:AM|PM))\s+([\-\d]+)\s+(\d{4})\s+([\-\d]+)\s+([\-+\d]+)(?:\s+([\-\d]+))?',
            line, re.I
        )
        if m:
            temp['max'] = pi(m.group(1))
            temp['max_time'] = m.group(2).strip()
            temp['record_high'] = pn(m.group(3))
            temp['record_high_year'] = pi(m.group(4))
            temp['normal_high'] = pi(m.group(5))
            temp['dep_max'] = pi(m.group(6))

        m = re.match(
            r'^\s*MINIMUM\s+([\-\d]+)\s+(\d{1,4}\s*(?:AM|PM))\s+([\-\d]+)\s+(\d{4})\s+([\-\d]+)\s+([\-+\d]+)(?:\s+([\-\d]+))?',
            line, re.I
        )
        if m:
            temp['min'] = pi(m.group(1))
            temp['min_time'] = m.group(2).strip()
            temp['record_low'] = pn(m.group(3))
            temp['record_low_year'] = pi(m.group(4))
            temp['normal_low'] = pi(m.group(5))
            temp['dep_min'] = pi(m.group(6))

        m = re.match(
            r'^\s*AVERAGE\s+([\-\d]+)\s+([\-\d]+)\s+([\-+\d]+)(?:\s+([\-\d]+))?',
            line, re.I
        )
        if m:
            temp['avg'] = pi(m.group(1))
            temp['normal_avg'] = pi(m.group(2))
            temp['dep_avg'] = pi(m.group(3))

    # Fallbacks
    for key, pattern in [
        ('max', r'MAXIMUM\s+([\-\d]+)'),
        ('min', r'MINIMUM\s+([\-\d]+)'),
        ('avg', r'AVERAGE\s+([\-\d]+)'),
    ]:
        if temp.get(key) is None:
            m = re.search(pattern, text, re.I)
            if m:
                temp[key] = pi(m.group(1))

    return temp


def parse_precipitation(text):
    """Extract today, month-to-date, and year-to-date precip."""
    precip = {}

    # TODAY row
    m = re.search(r'TODAY\s+([\d.T]+)', text, re.I)
    if m:
        precip['today'] = pn(m.group(1))

    # MONTH TO DATE
    m = re.search(r'MONTH\s+TO\s+DATE\s+([\d.T]+)\s+([\d.T]+)', text, re.I)
    if m:
        precip['month'] = pn(m.group(1))
        precip['normal_month'] = pn(m.group(2))

    # SINCE JAN 1 / YEAR TO DATE
    m = re.search(r'(?:SINCE\s+JAN(?:UARY)?\s+1|YEAR\s+TO\s+DATE)\s+([\d.T]+)\s+([\d.T]+)', text, re.I)
    if m:
        precip['year'] = pn(m.group(1))
        precip['normal_year'] = pn(m.group(2))

    return precip


def parse_wind(lines, text):
    """
    Parse wind section. VEF/LAS format:
      AVERAGE WIND SPEED        7.0
      HIGHEST WIND SPEED       23  DIRECTION  230  TIME  0234P
      HIGHEST GUST SPEED       35  DIRECTION  230  TIME  0312P
    """
    wind = {}
    wind_block = extract_block(lines, r'^\s*WIND', r'SKY|SUNSHINE|RELATIVE|WEATHER|HEATING|DEGREE')
    wind_text = '\n'.join(wind_block)

    # Average wind
    m = re.search(r'AVERAGE WIND SPEED\s+([\d.]+)', wind_text, re.I)
    if m:
        wind['avg_speed'] = pn(m.group(1))

    def parse_wind_entry(label_re):
        """Parse speed, direction, time for a given label."""
        result = {}
        for line in wind_block:
            m = re.search(label_re + r'\s+([\d.]+)', line, re.I)
            if not m:
                continue
            result['speed'] = pn(m.group(1))
            after = line[m.end():]

            # Direction
            dm = re.search(r'DIRECTION\s+(\d{1,3})', after, re.I)
            if dm:
                result['dir'] = pi(dm.group(1))
                result['compass'] = degrees_to_compass(result['dir'])
            # Direction as compass + degrees: "NW (300)"
            dm2 = re.search(r'\b([NSEW]{1,3})\s*\((\d{1,3})\)', after, re.I)
            if dm2:
                result['dir'] = pi(dm2.group(2))
                result['compass'] = dm2.group(1).upper()

            # Time
            tm = re.search(r'TIME\s+(\d{1,4}\s*(?:AM|PM|A|P))', after, re.I)
            if tm:
                result['time'] = tm.group(1).strip()

            break
        return result

    max_w  = parse_wind_entry(r'HIGHEST WIND SPEED')
    gust_w = parse_wind_entry(r'HIGHEST GUST SPEED')

    if max_w.get('speed') is not None:
        wind['max_speed']   = max_w['speed']
        wind['max_dir']     = max_w.get('dir')
        wind['max_compass'] = max_w.get('compass')
        wind['max_time']    = max_w.get('time')

    if gust_w.get('speed') is not None:
        wind['gust_speed']   = gust_w['speed']
        wind['gust_dir']     = gust_w.get('dir')
        wind['gust_compass'] = gust_w.get('compass')
        wind['gust_time']    = gust_w.get('time')

    return wind


def parse_sky(text):
    sky = {}
    m = re.search(r'AVERAGE SKY COVER\s+([\d.]+)', text, re.I)
    if m:
        sky['avg_cover'] = pn(m.group(1))
    return sky


def parse_sunshine(text):
    sun = {}
    m = re.search(r'PERCENT POSSIBLE SUNSHINE\s+(\d+)', text, re.I)
    if m:
        sun['pct'] = pi(m.group(1))
    m = re.search(r'HOURS OF SUNSHINE\s+([\d.]+)', text, re.I)
    if m:
        sun['hours'] = pn(m.group(1))
    return sun


def parse_humidity(lines):
    rh = {}
    rh_block = extract_block(
        lines, r'^\s*RELATIVE HUMIDITY',
        r'WEATHER|SUNSHINE|WIND|SKY|DEGREE|HEATING|COOLING'
    )
    for line in rh_block:
        m = re.search(r'MAXIMUM\s+(\d+)\s+(\d{1,4}\s*(?:AM|PM))', line, re.I)
        if m:
            rh['max'] = pi(m.group(1))
            rh['max_time'] = m.group(2).strip()
        else:
            m = re.search(r'MAXIMUM\s+(\d+)', line, re.I)
            if m:
                rh['max'] = pi(m.group(1))

        m = re.search(r'MINIMUM\s+(\d+)\s+(\d{1,4}\s*(?:AM|PM))', line, re.I)
        if m:
            rh['min'] = pi(m.group(1))
            rh['min_time'] = m.group(2).strip()
        else:
            m = re.search(r'MINIMUM\s+(\d+)', line, re.I)
            if m:
                rh['min'] = pi(m.group(1))
    return rh


def parse_weather_conditions(text):
    """Extract free-text weather conditions line."""
    m = re.search(
        r'WEATHER CONDITIONS[\s\S]*?\n([\s\S]*?)(?=\n\s*\n\s*\$\$|\$\$|SUNSHINE|SKY|$)',
        text, re.I
    )
    if m:
        lines = [l.strip() for l in m.group(1).split('\n') if l.strip() and '$$' not in l]
        return ' '.join(lines) if lines else None
    return None


# ── Main parsing orchestrator ─────────────────────────────────────────────────
def parse_cli(product_id, issued_at, text, retrieved_at):
    lines = text.replace('\r\n', '\n').split('\n')

    obs_date = parse_obs_date(text, issued_at)
    temp     = parse_temperature(lines, text)
    precip   = parse_precipitation(text)
    wind     = parse_wind(lines, text)
    sky      = parse_sky(text)
    sun      = parse_sunshine(text)
    rh       = parse_humidity(lines)
    wx       = parse_weather_conditions(text)

    def v(val):
        return val if val is not None else ""

    return {
        "obs_date":               v(obs_date),
        "product_id":             product_id,
        "issued_at":              issued_at,
        "retrieved_at":           retrieved_at,
        "max_temp_f":             v(temp.get('max')),
        "max_temp_time":          v(temp.get('max_time')),
        "min_temp_f":             v(temp.get('min')),
        "min_temp_time":          v(temp.get('min_time')),
        "avg_temp_f":             v(temp.get('avg')),
        "normal_high_f":          v(temp.get('normal_high')),
        "normal_low_f":           v(temp.get('normal_low')),
        "normal_avg_f":           v(temp.get('normal_avg')),
        "dep_max_f":              v(temp.get('dep_max')),
        "dep_min_f":              v(temp.get('dep_min')),
        "dep_avg_f":              v(temp.get('dep_avg')),
        "record_high_f":          v(temp.get('record_high')),
        "record_high_year":       v(temp.get('record_high_year')),
        "record_low_f":           v(temp.get('record_low')),
        "record_low_year":        v(temp.get('record_low_year')),
        "precip_today_in":        v(precip.get('today')),
        "precip_month_in":        v(precip.get('month')),
        "precip_year_in":         v(precip.get('year')),
        "precip_normal_month_in": v(precip.get('normal_month')),
        "precip_normal_year_in":  v(precip.get('normal_year')),
        "avg_wind_speed_mph":     v(wind.get('avg_speed')),
        "max_wind_speed_mph":     v(wind.get('max_speed')),
        "max_wind_dir_deg":       v(wind.get('max_dir')),
        "max_wind_dir_compass":   v(wind.get('max_compass')),
        "max_wind_time":          v(wind.get('max_time')),
        "peak_gust_mph":          v(wind.get('gust_speed')),
        "peak_gust_dir_deg":      v(wind.get('gust_dir')),
        "peak_gust_dir_compass":  v(wind.get('gust_compass')),
        "peak_gust_time":         v(wind.get('gust_time')),
        "sky_cover_tenths":       v(sky.get('avg_cover')),
        "sunshine_pct":           v(sun.get('pct')),
        "sunshine_hours":         v(sun.get('hours')),
        "rh_max_pct":             v(rh.get('max')),
        "rh_max_time":            v(rh.get('max_time')),
        "rh_min_pct":             v(rh.get('min')),
        "rh_min_time":            v(rh.get('min_time')),
        "weather_conditions":     v(wx),
    }


# ── CSV helpers ────────────────────────────────────────────────────────────────
def ensure_output_file():
    os.makedirs("data", exist_ok=True)
    if not os.path.exists(OUTPUT_FILE) or os.path.getsize(OUTPUT_FILE) == 0:
        with open(OUTPUT_FILE, "w", newline="") as f:
            csv.DictWriter(f, fieldnames=CSV_COLUMNS).writeheader()
        print(f"  Created file with header: {OUTPUT_FILE}")
        return
    with open(OUTPUT_FILE, "r", newline="") as f:
        first_line = f.readline().strip()
    if first_line != ",".join(CSV_COLUMNS):
        print(f"  Header outdated — rewriting…")
        with open(OUTPUT_FILE, "r") as f:
            existing = f.read()
        with open(OUTPUT_FILE, "w", newline="") as f:
            csv.DictWriter(f, fieldnames=CSV_COLUMNS).writeheader()
            for line in existing.splitlines():
                if line and not line.startswith("obs_date"):
                    f.write(line + "\n")
        print(f"  Header updated.")


def load_existing_dates():
    """Return set of obs_date values already in the CSV."""
    seen = set()
    if not os.path.exists(OUTPUT_FILE):
        return seen
    with open(OUTPUT_FILE, newline="") as f:
        for row in csv.DictReader(f):
            d = row.get("obs_date", "").strip()
            if d:
                seen.add(d)
    return seen


def append_row(row):
    with open(OUTPUT_FILE, "a", newline="") as f:
        csv.DictWriter(f, fieldnames=CSV_COLUMNS).writerow(row)


# ── Main ───────────────────────────────────────────────────────────────────────
def main():
    retrieved_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    print(f"CLI Collector — KLAS / {STATION}")
    print(f"Output: {OUTPUT_FILE}")
    print(f"Retrieved at: {retrieved_at}")

    ensure_output_file()
    existing_dates = load_existing_dates()
    print(f"  Existing observation dates in CSV: {len(existing_dates)}")

    print("  Fetching latest CLI product…")
    product_id, issued_at, text = fetch_latest_cli()
    print(f"  Product ID  : {product_id}")
    print(f"  Issued at   : {issued_at}")

    print("  Parsing CLI text…")
    row = parse_cli(product_id, issued_at, text, retrieved_at)
    obs_date = row.get("obs_date", "")

    if not obs_date:
        print("\n  ERROR — could not determine observation date from CLI text.")
        sys.exit(1)

    print(f"  Observation date: {obs_date}")

    if obs_date in existing_dates:
        print(f"\n  SKIP — {obs_date} already in CSV. No rows written.")
        return

    append_row(row)
    print(f"\n  NEW — wrote observation for {obs_date}")

    # Summary
    print(f"\n  Temperature : High {row['max_temp_f']}°F at {row['max_temp_time']}"
          f"  Low {row['min_temp_f']}°F at {row['min_temp_time']}"
          f"  Avg {row['avg_temp_f']}°F")
    print(f"  Normals     : High {row['normal_high_f']}°F  Low {row['normal_low_f']}°F"
          f"  Departures: Hi {row['dep_max_f']}  Lo {row['dep_min_f']}")
    print(f"  Wind        : Avg {row['avg_wind_speed_mph']} mph"
          f"  Max {row['max_wind_speed_mph']} mph ({row['max_wind_dir_compass']})"
          f"  Gust {row['peak_gust_mph']} mph ({row['peak_gust_dir_compass']})")
    print(f"  Precip      : {row['precip_today_in'] or '0.00'} in"
          f"  Month: {row['precip_month_in']}  YTD: {row['precip_year_in']}")
    print(f"  Sky/Sun     : Cover {row['sky_cover_tenths']}/10"
          f"  Sunshine {row['sunshine_pct']}%")
    print(f"  Humidity    : Max {row['rh_max_pct']}%  Min {row['rh_min_pct']}%")
    if row['weather_conditions']:
        print(f"  Weather     : {row['weather_conditions']}")
    print(f"\n  Done.")


if __name__ == "__main__":
    try:
        main()
    except requests.exceptions.RequestException as e:
        print(f"\nERROR: Network request failed: {e}", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        import traceback
        traceback.print_exc()
        print(f"\nERROR: {e}", file=sys.stderr)
        sys.exit(1)
