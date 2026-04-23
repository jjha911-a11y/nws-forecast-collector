#!/usr/bin/env python3
"""
CLI (Daily Climate Report) Collector for KLAS (Las Vegas)
==========================================================
Fetches the NWS CLI product for LAS and parses it into a structured
CSV row — one row per calendar day of observation.

Verified against actual VEF/LAS CLI format (April 2026).
"""

import requests
import csv
import os
import sys
import re
import time
from datetime import datetime, timezone, date, timedelta

# ── Configuration ──────────────────────────────────────────────────────────────
STATION     = "LAS"
OUTPUT_FILE = "data/observations_cli.csv"

HEADERS = {
    "User-Agent": "CLI-Collector/2.0 (github-actions; klas-verification)",
    "Accept":     "application/geo+json",
}

CSV_COLUMNS = [
    # ── Identity ───────────────────────────────────────────────────────────────
    "obs_date",               # calendar date this CLI covers (YYYY-MM-DD)
    "product_id",             # NWS product ID
    "issued_at",              # when NWS issued this CLI (UTC ISO8601)
    "retrieved_at",           # when this script ran (UTC ISO8601)
    # ── Temperature ───────────────────────────────────────────────────────────
    "max_temp_f",             # observed high °F
    "max_temp_time",          # time of max temp (e.g. "3:26 PM")
    "min_temp_f",             # observed low °F
    "min_temp_time",          # time of min temp
    "avg_temp_f",             # observed average °F
    "normal_high_f",          # climatological normal high °F
    "normal_low_f",           # climatological normal low °F
    "normal_avg_f",           # climatological normal average °F
    "dep_max_f",              # departure from normal — high
    "dep_min_f",              # departure from normal — low
    "dep_avg_f",              # departure from normal — average
    "record_high_f",          # record high for this date
    "record_high_year",       # year record high was set
    "record_low_f",           # record low for this date
    "record_low_year",        # year record low was set
    # ── Precipitation ─────────────────────────────────────────────────────────
    "precip_today_in",        # observed precipitation (T = 0.001)
    "precip_month_in",        # month-to-date precipitation
    "precip_normal_month_in", # normal month-to-date
    "precip_year_in",         # year-to-date (since Jan 1)
    "precip_normal_year_in",  # normal year-to-date
    "precip_water_year_in",   # water year-to-date (since Oct 1)
    "precip_normal_water_year_in", # normal water year-to-date
    # ── Wind ──────────────────────────────────────────────────────────────────
    "avg_wind_speed_mph",     # average wind speed mph
    "resultant_wind_speed_mph",   # resultant wind speed mph
    "resultant_wind_dir_deg",     # resultant wind direction degrees
    "resultant_wind_dir_compass", # resultant wind direction cardinal
    "max_wind_speed_mph",     # highest sustained wind speed mph
    "max_wind_dir_deg",       # direction of highest wind degrees
    "max_wind_dir_compass",   # direction of highest wind cardinal
    "max_wind_time",          # time of highest wind
    "peak_gust_mph",          # peak wind gust mph
    "peak_gust_dir_deg",      # direction of peak gust degrees
    "peak_gust_dir_compass",  # direction of peak gust cardinal
    "peak_gust_time",         # time of peak gust
    # ── Sky / Sunshine ────────────────────────────────────────────────────────
    "sky_cover_tenths",       # average sky cover (tenths, 0-10)
    "sunshine_pct",           # percent of possible sunshine
    "sunshine_hours",         # hours of sunshine
    # ── Humidity ──────────────────────────────────────────────────────────────
    "rh_max_pct",             # maximum relative humidity %
    "rh_max_time",            # time of max RH
    "rh_min_pct",             # minimum relative humidity %
    "rh_min_time",            # time of min RH
    "rh_avg_pct",             # average relative humidity %
    # ── Weather Conditions ────────────────────────────────────────────────────
    "weather_conditions",     # observed weather text
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
    list_url = f"https://api.weather.gov/products/types/CLI/locations/{STATION}"
    products = nws_get(list_url).json().get("@graph", [])
    if not products:
        raise RuntimeError(f"No CLI products found for {STATION}")
    latest     = products[0]
    product_id = latest["id"]
    issued_at  = latest.get("issuanceTime", "")
    text       = nws_get(f"https://api.weather.gov/products/{product_id}").json().get("productText", "")
    return product_id, issued_at, text


# ── Parsing helpers ────────────────────────────────────────────────────────────
def pn(s):
    if not s: return None
    t = s.strip()
    if re.match(r'^M+$', t) or t == '': return None
    if t.upper() == 'T': return 0.001
    try: return float(t)
    except ValueError: return None

def pi(s):
    n = pn(s)
    return int(round(n)) if n is not None else None

def degrees_to_compass(deg):
    if deg is None: return None
    dirs = ['N','NNE','NE','ENE','E','ESE','SE','SSE','S','SSW','SW','WSW','W','WNW','NW','NNW']
    return dirs[round(deg / 22.5) % 16]


# ── Section parsers (tuned to VEF/LAS format) ─────────────────────────────────
def parse_obs_date(text, issued_at):
    """Extract the calendar date this CLI covers from the headline."""
    m = re.search(
        r'CLIMATE\s+(?:REPORT|SUMMARY)\s+FOR\s+(\w+)\s+(\d{1,2})\s+(\d{4})',
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
    # Fallback: issued_at minus one day
    if issued_at:
        try:
            return str(date.fromisoformat(issued_at[:10]) - timedelta(days=1))
        except Exception:
            pass
    return None


def parse_temperature(text):
    """
    VEF format:
      MAXIMUM  77   3:26 PM  99    2012  80     -3       89
      MINIMUM  56   5:26 AM  33    1963  58     -2       63
      AVERAGE  67                        69     -2       76
    Time is H:MM AM/PM or HH:MM AM/PM.
    """
    temp = {}

    m = re.search(
        r'MAXIMUM\s+([\-\d]+)\s+(\d{1,2}:\d{2}\s*(?:AM|PM))\s+([\-\d]+)\s+(\d{4})\s+([\-\d]+)\s+([\-+\d]+)',
        text, re.I
    )
    if m:
        temp['max']              = pi(m.group(1))
        temp['max_time']         = m.group(2).strip()
        temp['record_high']      = pi(m.group(3))
        temp['record_high_year'] = pi(m.group(4))
        temp['normal_high']      = pi(m.group(5))
        temp['dep_max']          = pi(m.group(6))

    m = re.search(
        r'MINIMUM\s+([\-\d]+)\s+(\d{1,2}:\d{2}\s*(?:AM|PM))\s+([\-\d]+)\s+(\d{4})\s+([\-\d]+)\s+([\-+\d]+)',
        text, re.I
    )
    if m:
        temp['min']             = pi(m.group(1))
        temp['min_time']        = m.group(2).strip()
        temp['record_low']      = pi(m.group(3))
        temp['record_low_year'] = pi(m.group(4))
        temp['normal_low']      = pi(m.group(5))
        temp['dep_min']         = pi(m.group(6))

    # AVERAGE line has no time or record — just obs, normal, departure
    m = re.search(r'^\s*AVERAGE\s+([\-\d]+)\s+([\-\d]+)\s+([\-+\d]+)', text, re.I | re.M)
    if m:
        temp['avg']        = pi(m.group(1))
        temp['normal_avg'] = pi(m.group(2))
        temp['dep_avg']    = pi(m.group(3))

    return temp


def parse_precipitation(text):
    """
    VEF format:
      TODAY            0.00          0.11 1955   0.01  -0.01     0.00
      MONTH TO DATE    T                         0.16  -0.16      T
      SINCE OCT 1      3.57                      3.01   0.56     0.59
      SINCE JAN 1      0.49                      1.94  -1.45     0.59
    """
    precip = {}

    m = re.search(r'^\s*TODAY\s+([\d.T]+)', text, re.I | re.M)
    if m: precip['today'] = pn(m.group(1))

    # Month to date: first value is observed, then skip optional last-year,
    # then normal. Pattern: observed + optional-stuff + normal + departure
    m = re.search(r'MONTH\s+TO\s+DATE\s+([\d.T]+)\s+([\d.T]+)\s+([\-\d.T]+)', text, re.I)
    if m:
        precip['month']        = pn(m.group(1))
        precip['normal_month'] = pn(m.group(2))

    m = re.search(r'SINCE\s+JAN(?:UARY)?\s+1\s+([\d.T]+)\s+([\d.T]+)', text, re.I)
    if m:
        precip['year']        = pn(m.group(1))
        precip['normal_year'] = pn(m.group(2))

    m = re.search(r'SINCE\s+OCT(?:OBER)?\s+1\s+([\d.T]+)\s+([\d.T]+)', text, re.I)
    if m:
        precip['water_year']        = pn(m.group(1))
        precip['normal_water_year'] = pn(m.group(2))

    return precip


def parse_wind(text):
    """
    VEF format (all on single lines):
      RESULTANT WIND SPEED  10   RESULTANT WIND DIRECTION  SW (210)
      HIGHEST WIND SPEED    23   HIGHEST WIND DIRECTION    SW (220)
      HIGHEST GUST SPEED    33   HIGHEST GUST DIRECTION    SW (220)
      AVERAGE WIND SPEED    11.6
    Direction format: COMPASS (DEGREES) e.g. SW (220)
    Time is optional and appears as H:MM AM/PM after the speed.
    """
    wind = {}

    def parse_entry(speed_re, dir_re):
        result = {}
        m = re.search(speed_re + r'\s+([\d.]+)', text, re.I)
        if m:
            result['speed'] = pn(m.group(1))
            # Look for time on same line
            line_start = text.rfind('\n', 0, m.start()) + 1
            line_end   = text.find('\n', m.end())
            line = text[line_start:line_end]
            tm = re.search(r'(\d{1,2}:\d{2}\s*(?:AM|PM))', line, re.I)
            if tm: result['time'] = tm.group(1).strip()

        m = re.search(dir_re + r'\s+([NSEW]{1,3})\s*\((\d{1,3})\)', text, re.I)
        if m:
            result['compass'] = m.group(1).upper()
            result['deg']     = pi(m.group(2))
        elif re.search(dir_re, text, re.I):
            # Fallback: degrees only
            m2 = re.search(dir_re + r'\s+(\d{1,3})', text, re.I)
            if m2:
                result['deg']     = pi(m2.group(1))
                result['compass'] = degrees_to_compass(result['deg'])
        return result

    m = re.search(r'AVERAGE WIND SPEED\s+([\d.]+)', text, re.I)
    if m: wind['avg_speed'] = pn(m.group(1))

    res = parse_entry(r'RESULTANT WIND SPEED', r'RESULTANT WIND DIRECTION')
    if res.get('speed') is not None:
        wind['resultant_speed']   = res['speed']
        wind['resultant_deg']     = res.get('deg')
        wind['resultant_compass'] = res.get('compass')

    max_w = parse_entry(r'HIGHEST WIND SPEED', r'HIGHEST WIND DIRECTION')
    if max_w.get('speed') is not None:
        wind['max_speed']   = max_w['speed']
        wind['max_deg']     = max_w.get('deg')
        wind['max_compass'] = max_w.get('compass')
        wind['max_time']    = max_w.get('time')

    gust = parse_entry(r'HIGHEST GUST SPEED', r'HIGHEST GUST DIRECTION')
    if gust.get('speed') is not None:
        wind['gust_speed']   = gust['speed']
        wind['gust_deg']     = gust.get('deg')
        wind['gust_compass'] = gust.get('compass')
        wind['gust_time']    = gust.get('time')

    return wind


def parse_sky(text):
    m = re.search(r'AVERAGE SKY COVER\s+([\d.]+)', text, re.I)
    return {'avg_cover': pn(m.group(1))} if m else {}


def parse_sunshine(text):
    sun = {}
    m = re.search(r'PERCENT POSSIBLE SUNSHINE\s+(\d+)', text, re.I)
    if m: sun['pct'] = pi(m.group(1))
    m = re.search(r'HOURS OF SUNSHINE\s+([\d.]+)', text, re.I)
    if m: sun['hours'] = pn(m.group(1))
    return sun


def parse_humidity(text):
    """
    VEF format:
      HIGHEST  44   4:00 AM
      LOWEST   11   1:00 PM
      AVERAGE  28
    Note: VEF uses HIGHEST/LOWEST not MAXIMUM/MINIMUM.
    """
    rh = {}
    m = re.search(r'HIGHEST\s+(\d+)\s+(\d{1,2}:\d{2}\s*(?:AM|PM))', text, re.I)
    if m:
        rh['max']      = pi(m.group(1))
        rh['max_time'] = m.group(2).strip()
    else:
        m = re.search(r'HIGHEST\s+(\d+)', text, re.I)
        if m: rh['max'] = pi(m.group(1))

    m = re.search(r'LOWEST\s+(\d+)\s+(\d{1,2}:\d{2}\s*(?:AM|PM))', text, re.I)
    if m:
        rh['min']      = pi(m.group(1))
        rh['min_time'] = m.group(2).strip()
    else:
        m = re.search(r'LOWEST\s+(\d+)', text, re.I)
        if m: rh['min'] = pi(m.group(1))

    # AVERAGE appears after the RELATIVE HUMIDITY header
    m = re.search(r'RELATIVE HUMIDITY.*?AVERAGE\s+(\d+)', text, re.I | re.S)
    if m: rh['avg'] = pi(m.group(1))

    return rh


def parse_weather_conditions(text):
    """
    VEF format:
      WEATHER CONDITIONS
      THE FOLLOWING WEATHER WAS RECORDED TODAY.
        NO SIGNIFICANT WEATHER WAS OBSERVED.
      or
        FOG. DUST.
    Skip the preamble line and grab the actual condition text.
    """
    m = re.search(
        r'WEATHER CONDITIONS\s*\n.*?RECORDED TODAY\.\s*\n([\s\S]*?)(?:\n\s*\n|\$\$)',
        text, re.I
    )
    if m:
        conds = ' '.join(
            l.strip() for l in m.group(1).splitlines()
            if l.strip() and '$$' not in l
        )
        return conds or None

    # Fallback: no preamble
    m = re.search(r'WEATHER CONDITIONS\s*\n([\s\S]*?)(?:\n\s*\n|\$\$)', text, re.I)
    if m:
        conds = ' '.join(
            l.strip() for l in m.group(1).splitlines()
            if l.strip() and 'RECORDED' not in l.upper() and '$$' not in l
        )
        return conds or None

    return None


# ── Build CSV row ──────────────────────────────────────────────────────────────
def build_row(product_id, issued_at, text, retrieved_at):
    obs_date = parse_obs_date(text, issued_at)
    temp     = parse_temperature(text)
    precip   = parse_precipitation(text)
    wind     = parse_wind(text)
    sky      = parse_sky(text)
    sun      = parse_sunshine(text)
    rh       = parse_humidity(text)
    wx       = parse_weather_conditions(text)

    def v(val):
        return val if val is not None else ""

    return {
        "obs_date":                    v(obs_date),
        "product_id":                  product_id,
        "issued_at":                   issued_at,
        "retrieved_at":                retrieved_at,
        "max_temp_f":                  v(temp.get('max')),
        "max_temp_time":               v(temp.get('max_time')),
        "min_temp_f":                  v(temp.get('min')),
        "min_temp_time":               v(temp.get('min_time')),
        "avg_temp_f":                  v(temp.get('avg')),
        "normal_high_f":               v(temp.get('normal_high')),
        "normal_low_f":                v(temp.get('normal_low')),
        "normal_avg_f":                v(temp.get('normal_avg')),
        "dep_max_f":                   v(temp.get('dep_max')),
        "dep_min_f":                   v(temp.get('dep_min')),
        "dep_avg_f":                   v(temp.get('dep_avg')),
        "record_high_f":               v(temp.get('record_high')),
        "record_high_year":            v(temp.get('record_high_year')),
        "record_low_f":                v(temp.get('record_low')),
        "record_low_year":             v(temp.get('record_low_year')),
        "precip_today_in":             v(precip.get('today')),
        "precip_month_in":             v(precip.get('month')),
        "precip_normal_month_in":      v(precip.get('normal_month')),
        "precip_year_in":              v(precip.get('year')),
        "precip_normal_year_in":       v(precip.get('normal_year')),
        "precip_water_year_in":        v(precip.get('water_year')),
        "precip_normal_water_year_in": v(precip.get('normal_water_year')),
        "avg_wind_speed_mph":          v(wind.get('avg_speed')),
        "resultant_wind_speed_mph":    v(wind.get('resultant_speed')),
        "resultant_wind_dir_deg":      v(wind.get('resultant_deg')),
        "resultant_wind_dir_compass":  v(wind.get('resultant_compass')),
        "max_wind_speed_mph":          v(wind.get('max_speed')),
        "max_wind_dir_deg":            v(wind.get('max_deg')),
        "max_wind_dir_compass":        v(wind.get('max_compass')),
        "max_wind_time":               v(wind.get('max_time')),
        "peak_gust_mph":               v(wind.get('gust_speed')),
        "peak_gust_dir_deg":           v(wind.get('gust_deg')),
        "peak_gust_dir_compass":       v(wind.get('gust_compass')),
        "peak_gust_time":              v(wind.get('gust_time')),
        "sky_cover_tenths":            v(sky.get('avg_cover')),
        "sunshine_pct":                v(sun.get('pct')),
        "sunshine_hours":              v(sun.get('hours')),
        "rh_max_pct":                  v(rh.get('max')),
        "rh_max_time":                 v(rh.get('max_time')),
        "rh_min_pct":                  v(rh.get('min')),
        "rh_min_time":                 v(rh.get('min_time')),
        "rh_avg_pct":                  v(rh.get('avg')),
        "weather_conditions":          v(wx),
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
    seen = set()
    if not os.path.exists(OUTPUT_FILE):
        return seen
    with open(OUTPUT_FILE, newline="") as f:
        for row in csv.DictReader(f):
            d = row.get("obs_date", "").strip()
            if d:
                seen.add(d)
    return seen


# ── Main ───────────────────────────────────────────────────────────────────────
def main():
    retrieved_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    print(f"CLI Collector — KLAS / {STATION}")
    print(f"Output: {OUTPUT_FILE}")
    print(f"Retrieved at: {retrieved_at}")

    ensure_output_file()
    existing_dates = load_existing_dates()
    print(f"  Existing observation dates: {len(existing_dates)}")

    print("  Fetching latest CLI product…")
    product_id, issued_at, text = fetch_latest_cli()
    print(f"  Product ID  : {product_id}")
    print(f"  Issued at   : {issued_at}")

    print("  Parsing…")
    row = build_row(product_id, issued_at, text, retrieved_at)
    obs_date = row.get("obs_date", "")

    if not obs_date:
        print("\n  ERROR — could not determine observation date.")
        sys.exit(1)

    print(f"  Observation date: {obs_date}")

    if obs_date in existing_dates:
        print(f"\n  SKIP — {obs_date} already in CSV. No rows written.")
        return

    with open(OUTPUT_FILE, "a", newline="") as f:
        csv.DictWriter(f, fieldnames=CSV_COLUMNS).writerow(row)

    print(f"\n  NEW — wrote observation for {obs_date}")
    print(f"\n  Temp  : High {row['max_temp_f']}°F at {row['max_temp_time']}"
          f"  Low {row['min_temp_f']}°F at {row['min_temp_time']}"
          f"  Avg {row['avg_temp_f']}°F")
    print(f"  Normal: High {row['normal_high_f']}°F  Low {row['normal_low_f']}°F"
          f"  Dep Hi {row['dep_max_f']}  Lo {row['dep_min_f']}")
    print(f"  Wind  : Avg {row['avg_wind_speed_mph']} mph"
          f"  Max {row['max_wind_speed_mph']} mph ({row['max_wind_dir_compass']})"
          f"  Gust {row['peak_gust_mph']} mph ({row['peak_gust_dir_compass']})")
    print(f"  Precip: {row['precip_today_in'] or '0.00'} in"
          f"  Month {row['precip_month_in']}"
          f"  YTD {row['precip_year_in']}"
          f"  Water Yr {row['precip_water_year_in']}")
    print(f"  Sky   : {row['sky_cover_tenths']}/10"
          f"  Sun {row['sunshine_pct'] or '—'}%")
    print(f"  RH    : Max {row['rh_max_pct']}%  Min {row['rh_min_pct']}%"
          f"  Avg {row['rh_avg_pct']}%")
    if row['weather_conditions']:
        print(f"  WX    : {row['weather_conditions']}")
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
