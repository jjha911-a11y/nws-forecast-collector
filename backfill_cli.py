#!/usr/bin/env python3
"""
CLI Backfill Script
===================
One-time script to seed observations_cli.csv with historical CLI products
back to March 1, 2026. Run manually once from GitHub Actions, then delete.

The NWS products API typically keeps ~60-90 days of CLI products, so
March 1 should be within reach. Products are fetched oldest-first so the
CSV is written in chronological order.
"""

import requests
import csv
import os
import sys
import re
import time
from datetime import datetime, timezone, date, timedelta

# ── Reuse all parsers from collect_cli.py ──────────────────────────────────────
# (copy of all parsing functions — keeps backfill self-contained)

STATION     = "LAS"
OUTPUT_FILE = "data/observations_cli.csv"
BACKFILL_FROM = date(2026, 3, 1)   # collect everything from this date forward

HEADERS = {
    "User-Agent": "CLI-Backfill/1.0 (github-actions; klas-verification)",
    "Accept":     "application/geo+json",
}

CSV_COLUMNS = [
    "obs_date","product_id","issued_at","retrieved_at",
    "max_temp_f","max_temp_time","min_temp_f","min_temp_time","avg_temp_f",
    "normal_high_f","normal_low_f","normal_avg_f",
    "dep_max_f","dep_min_f","dep_avg_f",
    "record_high_f","record_high_year","record_low_f","record_low_year",
    "precip_today_in","precip_month_in","precip_normal_month_in",
    "precip_year_in","precip_normal_year_in",
    "precip_water_year_in","precip_normal_water_year_in",
    "avg_wind_speed_mph","resultant_wind_speed_mph",
    "resultant_wind_dir_deg","resultant_wind_dir_compass",
    "max_wind_speed_mph","max_wind_dir_deg","max_wind_dir_compass","max_wind_time",
    "peak_gust_mph","peak_gust_dir_deg","peak_gust_dir_compass","peak_gust_time",
    "sky_cover_tenths","sunshine_pct","sunshine_hours",
    "rh_max_pct","rh_max_time","rh_min_pct","rh_min_time","rh_avg_pct",
    "weather_conditions",
]


def nws_get(url, retries=3, timeout=20):
    for attempt in range(1, retries + 1):
        try:
            resp = requests.get(url, headers=HEADERS, timeout=timeout)
            resp.raise_for_status()
            return resp
        except (requests.exceptions.Timeout, requests.exceptions.ConnectionError):
            if attempt == retries: raise
            print(f"    Timeout — waiting 15s…"); time.sleep(15)
        except requests.exceptions.HTTPError:
            if attempt == retries or resp.status_code not in (500, 503): raise
            print(f"    HTTP {resp.status_code} — waiting 15s…"); time.sleep(15)


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

def parse_obs_date(text, issued_at):
    m = re.search(r'CLIMATE\s+(?:REPORT|SUMMARY)\s+FOR\s+(\w+)\s+(\d{1,2})\s+(\d{4})', text, re.I)
    if m:
        months = {'JANUARY':1,'FEBRUARY':2,'MARCH':3,'APRIL':4,'MAY':5,'JUNE':6,
                  'JULY':7,'AUGUST':8,'SEPTEMBER':9,'OCTOBER':10,'NOVEMBER':11,'DECEMBER':12}
        mon = months.get(m.group(1).upper())
        if mon:
            return f"{m.group(3)}-{mon:02d}-{int(m.group(2)):02d}"
    if issued_at:
        try: return str(date.fromisoformat(issued_at[:10]) - timedelta(days=1))
        except: pass
    return None

def parse_temperature(text):
    temp = {}
    m = re.search(r'MAXIMUM\s+([\-\d]+)\s+(\d{1,2}:\d{2}\s*(?:AM|PM))\s+([\-\d]+)\s+(\d{4})\s+([\-\d]+)\s+([\-+\d]+)', text, re.I)
    if m:
        temp['max'] = pi(m.group(1)); temp['max_time'] = m.group(2).strip()
        temp['record_high'] = pi(m.group(3)); temp['record_high_year'] = pi(m.group(4))
        temp['normal_high'] = pi(m.group(5)); temp['dep_max'] = pi(m.group(6))
    m = re.search(r'MINIMUM\s+([\-\d]+)\s+(\d{1,2}:\d{2}\s*(?:AM|PM))\s+([\-\d]+)\s+(\d{4})\s+([\-\d]+)\s+([\-+\d]+)', text, re.I)
    if m:
        temp['min'] = pi(m.group(1)); temp['min_time'] = m.group(2).strip()
        temp['record_low'] = pi(m.group(3)); temp['record_low_year'] = pi(m.group(4))
        temp['normal_low'] = pi(m.group(5)); temp['dep_min'] = pi(m.group(6))
    m = re.search(r'^\s*AVERAGE\s+([\-\d]+)\s+([\-\d]+)\s+([\-+\d]+)', text, re.I | re.M)
    if m:
        temp['avg'] = pi(m.group(1)); temp['normal_avg'] = pi(m.group(2)); temp['dep_avg'] = pi(m.group(3))
    return temp

def parse_precipitation(text):
    precip = {}
    m = re.search(r'^\s*TODAY\s+([\d.T]+)', text, re.I | re.M)
    if m: precip['today'] = pn(m.group(1))
    m = re.search(r'MONTH\s+TO\s+DATE\s+([\d.T]+)\s+([\d.T]+)\s+([\-\d.T]+)', text, re.I)
    if m: precip['month'] = pn(m.group(1)); precip['normal_month'] = pn(m.group(2))
    m = re.search(r'SINCE\s+JAN(?:UARY)?\s+1\s+([\d.T]+)\s+([\d.T]+)', text, re.I)
    if m: precip['year'] = pn(m.group(1)); precip['normal_year'] = pn(m.group(2))
    m = re.search(r'SINCE\s+OCT(?:OBER)?\s+1\s+([\d.T]+)\s+([\d.T]+)', text, re.I)
    if m: precip['water_year'] = pn(m.group(1)); precip['normal_water_year'] = pn(m.group(2))
    return precip

def parse_wind(text):
    wind = {}
    def parse_entry(speed_re, dir_re):
        result = {}
        m = re.search(speed_re + r'\s+([\d.]+)', text, re.I)
        if m:
            result['speed'] = pn(m.group(1))
            line_start = text.rfind('\n', 0, m.start()) + 1
            line = text[line_start:text.find('\n', m.end())]
            tm = re.search(r'(\d{1,2}:\d{2}\s*(?:AM|PM))', line, re.I)
            if tm: result['time'] = tm.group(1).strip()
        m = re.search(dir_re + r'\s+([NSEW]{1,3})\s*\((\d{1,3})\)', text, re.I)
        if m:
            result['compass'] = m.group(1).upper(); result['deg'] = pi(m.group(2))
        elif re.search(dir_re, text, re.I):
            m2 = re.search(dir_re + r'\s+(\d{1,3})', text, re.I)
            if m2: result['deg'] = pi(m2.group(1)); result['compass'] = degrees_to_compass(result['deg'])
        return result
    m = re.search(r'AVERAGE WIND SPEED\s+([\d.]+)', text, re.I)
    if m: wind['avg_speed'] = pn(m.group(1))
    res = parse_entry(r'RESULTANT WIND SPEED', r'RESULTANT WIND DIRECTION')
    if res.get('speed') is not None:
        wind['resultant_speed'] = res['speed']; wind['resultant_deg'] = res.get('deg'); wind['resultant_compass'] = res.get('compass')
    max_w = parse_entry(r'HIGHEST WIND SPEED', r'HIGHEST WIND DIRECTION')
    if max_w.get('speed') is not None:
        wind['max_speed'] = max_w['speed']; wind['max_deg'] = max_w.get('deg')
        wind['max_compass'] = max_w.get('compass'); wind['max_time'] = max_w.get('time')
    gust = parse_entry(r'HIGHEST GUST SPEED', r'HIGHEST GUST DIRECTION')
    if gust.get('speed') is not None:
        wind['gust_speed'] = gust['speed']; wind['gust_deg'] = gust.get('deg')
        wind['gust_compass'] = gust.get('compass'); wind['gust_time'] = gust.get('time')
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
    rh = {}
    m = re.search(r'HIGHEST\s+(\d+)\s+(\d{1,2}:\d{2}\s*(?:AM|PM))', text, re.I)
    if m: rh['max'] = pi(m.group(1)); rh['max_time'] = m.group(2).strip()
    else:
        m = re.search(r'HIGHEST\s+(\d+)', text, re.I)
        if m: rh['max'] = pi(m.group(1))
    m = re.search(r'LOWEST\s+(\d+)\s+(\d{1,2}:\d{2}\s*(?:AM|PM))', text, re.I)
    if m: rh['min'] = pi(m.group(1)); rh['min_time'] = m.group(2).strip()
    else:
        m = re.search(r'LOWEST\s+(\d+)', text, re.I)
        if m: rh['min'] = pi(m.group(1))
    m = re.search(r'RELATIVE HUMIDITY.*?AVERAGE\s+(\d+)', text, re.I | re.S)
    if m: rh['avg'] = pi(m.group(1))
    return rh

def parse_weather_conditions(text):
    m = re.search(r'WEATHER CONDITIONS\s*\n.*?RECORDED TODAY\.\s*\n([\s\S]*?)(?:\n\s*\n|\$\$)', text, re.I)
    if m:
        conds = ' '.join(l.strip() for l in m.group(1).splitlines() if l.strip() and '$$' not in l)
        return conds or None
    m = re.search(r'WEATHER CONDITIONS\s*\n([\s\S]*?)(?:\n\s*\n|\$\$)', text, re.I)
    if m:
        conds = ' '.join(l.strip() for l in m.group(1).splitlines()
                         if l.strip() and 'RECORDED' not in l.upper() and '$$' not in l)
        return conds or None
    return None

def build_row(product_id, issued_at, text, retrieved_at):
    obs_date = parse_obs_date(text, issued_at)
    temp = parse_temperature(text); precip = parse_precipitation(text)
    wind = parse_wind(text); sky = parse_sky(text)
    sun = parse_sunshine(text); rh = parse_humidity(text)
    wx = parse_weather_conditions(text)
    v = lambda val: val if val is not None else ""
    return {
        "obs_date": v(obs_date), "product_id": product_id,
        "issued_at": issued_at, "retrieved_at": retrieved_at,
        "max_temp_f": v(temp.get('max')), "max_temp_time": v(temp.get('max_time')),
        "min_temp_f": v(temp.get('min')), "min_temp_time": v(temp.get('min_time')),
        "avg_temp_f": v(temp.get('avg')), "normal_high_f": v(temp.get('normal_high')),
        "normal_low_f": v(temp.get('normal_low')), "normal_avg_f": v(temp.get('normal_avg')),
        "dep_max_f": v(temp.get('dep_max')), "dep_min_f": v(temp.get('dep_min')),
        "dep_avg_f": v(temp.get('dep_avg')), "record_high_f": v(temp.get('record_high')),
        "record_high_year": v(temp.get('record_high_year')), "record_low_f": v(temp.get('record_low')),
        "record_low_year": v(temp.get('record_low_year')),
        "precip_today_in": v(precip.get('today')), "precip_month_in": v(precip.get('month')),
        "precip_normal_month_in": v(precip.get('normal_month')),
        "precip_year_in": v(precip.get('year')), "precip_normal_year_in": v(precip.get('normal_year')),
        "precip_water_year_in": v(precip.get('water_year')),
        "precip_normal_water_year_in": v(precip.get('normal_water_year')),
        "avg_wind_speed_mph": v(wind.get('avg_speed')),
        "resultant_wind_speed_mph": v(wind.get('resultant_speed')),
        "resultant_wind_dir_deg": v(wind.get('resultant_deg')),
        "resultant_wind_dir_compass": v(wind.get('resultant_compass')),
        "max_wind_speed_mph": v(wind.get('max_speed')), "max_wind_dir_deg": v(wind.get('max_deg')),
        "max_wind_dir_compass": v(wind.get('max_compass')), "max_wind_time": v(wind.get('max_time')),
        "peak_gust_mph": v(wind.get('gust_speed')), "peak_gust_dir_deg": v(wind.get('gust_deg')),
        "peak_gust_dir_compass": v(wind.get('gust_compass')), "peak_gust_time": v(wind.get('gust_time')),
        "sky_cover_tenths": v(sky.get('avg_cover')), "sunshine_pct": v(sun.get('pct')),
        "sunshine_hours": v(sun.get('hours')), "rh_max_pct": v(rh.get('max')),
        "rh_max_time": v(rh.get('max_time')), "rh_min_pct": v(rh.get('min')),
        "rh_min_time": v(rh.get('min_time')), "rh_avg_pct": v(rh.get('avg')),
        "weather_conditions": v(wx),
    }


# ── Main backfill logic ────────────────────────────────────────────────────────
def main():
    retrieved_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    print(f"CLI Backfill — {STATION}  (from {BACKFILL_FROM} onward)")
    print(f"Output: {OUTPUT_FILE}")

    # Ensure file exists with header
    os.makedirs("data", exist_ok=True)
    write_header = not os.path.exists(OUTPUT_FILE) or os.path.getsize(OUTPUT_FILE) == 0
    if not write_header:
        with open(OUTPUT_FILE, "r") as f:
            first = f.readline().strip()
        write_header = first != ",".join(CSV_COLUMNS)
    if write_header:
        with open(OUTPUT_FILE, "w", newline="") as f:
            csv.DictWriter(f, fieldnames=CSV_COLUMNS).writeheader()
        print("  Created/reset file with header.")

    # Load already-saved dates
    existing_dates = set()
    with open(OUTPUT_FILE, newline="") as f:
        for row in csv.DictReader(f):
            d = row.get("obs_date", "").strip()
            if d: existing_dates.add(d)
    print(f"  Already have {len(existing_dates)} dates in CSV.")

    # Fetch full product list
    print(f"\n  Fetching CLI product list for {STATION}…")
    list_url = f"https://api.weather.gov/products/types/CLI/locations/{STATION}"
    products = nws_get(list_url).json().get("@graph", [])
    print(f"  Found {len(products)} products in NWS archive.")

    # Filter to our date range, oldest first
    candidates = []
    for p in products:
        issued = p.get("issuanceTime", "")
        if not issued:
            continue
        try:
            issued_date = date.fromisoformat(issued[:10])
            # CLI issued on day D covers observations from day D-1
            obs_date_est = str(issued_date - timedelta(days=1))
            if date.fromisoformat(obs_date_est) >= BACKFILL_FROM:
                candidates.append(p)
        except Exception:
            continue

    candidates.reverse()  # oldest first
    print(f"  {len(candidates)} products cover {BACKFILL_FROM} onward.")

    # Process each
    written = 0
    skipped = 0
    failed  = 0

    for p in candidates:
        product_id = p["id"]
        issued_at  = p.get("issuanceTime", "")

        try:
            text = nws_get(f"https://api.weather.gov/products/{product_id}").json().get("productText", "")
            row  = build_row(product_id, issued_at, text, retrieved_at)
            obs_date = row.get("obs_date", "")

            if not obs_date:
                print(f"  ?? {product_id} — could not determine obs_date, skipping")
                failed += 1
                continue

            if obs_date in existing_dates:
                print(f"  -- {obs_date}  already exists, skipping")
                skipped += 1
                continue

            with open(OUTPUT_FILE, "a", newline="") as f:
                csv.DictWriter(f, fieldnames=CSV_COLUMNS).writerow(row)
            existing_dates.add(obs_date)

            hi = row['max_temp_f']; lo = row['min_temp_f']
            wind = row['max_wind_speed_mph']; gust = row['peak_gust_mph']
            print(f"  OK {obs_date}  Hi={hi} Lo={lo} Wind={wind} Gust={gust} mph")
            written += 1

            # Be polite to the NWS API
            time.sleep(1)

        except Exception as e:
            print(f"  !! {product_id} — ERROR: {e}")
            failed += 1

    print(f"\n  Backfill complete.")
    print(f"  Written : {written}")
    print(f"  Skipped : {skipped} (already existed)")
    print(f"  Failed  : {failed}")
    print(f"  Total in CSV: {len(existing_dates)}")


if __name__ == "__main__":
    try:
        main()
    except requests.exceptions.RequestException as e:
        print(f"\nERROR: Network request failed: {e}", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        import traceback
        traceback.print_exc()
        sys.exit(1)
