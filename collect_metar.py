#!/usr/bin/env python3
"""
METAR Collector for KLAS (Las Vegas)
=====================================
Fetches hourly METAR observations from the Aviation Weather Center API
(aviationweather.gov) and appends them to a growing CSV file.

Each run fetches the last 3 hours of METARs and writes any that are not
already in the CSV. Running 4x per day (every 6 hours) with a 3-hour
lookback ensures complete coverage with overlap for reliability.

Source: https://aviationweather.gov/api/data/metar
  - Free, no API key required
  - Keeps 15 days of history
  - Returns clean pre-decoded JSON

Data model:
  One row per METAR observation. Each row joins to forecasts.csv on
  obs_time_utc date matching forecast_date (with lead_days = 0 for
  same-day verification, or join all leads for lead-time analysis).

Scheduled: runs alongside collect_forecast.py (4x daily).
"""

import requests
import csv
import os
import sys
import time
from datetime import datetime, timezone, date

# ── Configuration ──────────────────────────────────────────────────────────────
STATION     = "KLAS"
OUTPUT_FILE = "data/observations_metar.csv"
LOOKBACK_HOURS = 7   # fetch 7 hours per run — covers full 6-hour interval
                     # plus 1-hour buffer, so a missed run leaves no gaps

AWC_BASE = "https://aviationweather.gov/api/data/metar"
HEADERS  = {
    "User-Agent": "METAR-Collector/1.0 (github-actions; klas-verification)",
    "Accept":     "application/json",
}

# Cloud cover codes → sky cover percentage (oktas * 12.5%)
CLOUD_COVER = {"SKC": 0, "CLR": 0, "FEW": 19, "SCT": 44, "BKN": 75, "OVC": 100, "OVX": 100, "VV": 100}

CSV_COLUMNS = [
    # ── Identity ───────────────────────────────────────────────────────────────
    "obs_time_utc",          # observation timestamp (UTC ISO8601) — primary key
    "obs_date_utc",          # calendar date of observation (UTC YYYY-MM-DD)
    "obs_date_local",        # calendar date of observation (PDT/PST YYYY-MM-DD)
    "metar_type",            # METAR or SPECI
    "retrieved_at",          # when this script ran (UTC ISO8601)
    # ── Temperature & Dewpoint ─────────────────────────────────────────────────
    "temp_c",                # temperature °C
    "temp_f",                # temperature °F
    "dewpoint_c",            # dewpoint °C
    "dewpoint_f",            # dewpoint °F
    "relative_humidity",     # computed relative humidity %
    # ── Wind ──────────────────────────────────────────────────────────────────
    "wind_dir_deg",          # wind direction degrees (0 = calm/variable)
    "wind_dir_compass",      # wind direction cardinal
    "wind_speed_kt",         # wind speed knots
    "wind_speed_mph",        # wind speed mph
    "wind_gust_kt",          # wind gust knots (empty if no gust)
    "wind_gust_mph",         # wind gust mph (empty if no gust)
    # ── Visibility ────────────────────────────────────────────────────────────
    "visibility_sm",         # visibility statute miles ("10+" = unlimited)
    "visibility_sm_numeric", # visibility as number (10.0 for "10+")
    # ── Sky Cover ─────────────────────────────────────────────────────────────
    "sky_cover_pct",         # estimated sky cover % from cloud layers
    "ceiling_ft",            # ceiling height AGL feet (lowest BKN/OVC/VV layer)
    "cloud_layers",          # raw cloud layer string e.g. "FEW025 SCT080 BKN200"
    # ── Pressure ──────────────────────────────────────────────────────────────
    "altimeter_inhg",        # altimeter setting inHg
    "sea_level_pressure_mb", # sea level pressure mb
    # ── Precipitation ─────────────────────────────────────────────────────────
    "precip_1hr_in",         # 1-hour precipitation inches
    "precip_3hr_in",         # 3-hour precipitation inches
    "precip_6hr_in",         # 6-hour precipitation inches
    "precip_24hr_in",        # 24-hour precipitation inches
    # ── Weather Phenomena ─────────────────────────────────────────────────────
    "wx_string",             # present weather string e.g. "-RA BR" "DU BLDU"
    "flight_category",       # VFR / MVFR / IFR / LIFR
    # ── Raw ───────────────────────────────────────────────────────────────────
    "raw_metar",             # complete raw METAR string
]


# ── Helpers ────────────────────────────────────────────────────────────────────
def c_to_f(c):
    if c is None: return None
    return round(c * 9/5 + 32, 1)

def kt_to_mph(kt):
    if kt is None: return None
    return round(kt * 1.15078, 1)

def compute_rh(temp_c, dewp_c):
    """August-Roche-Magnus approximation."""
    if temp_c is None or dewp_c is None: return None
    try:
        num   = 17.625 * float(dewp_c)  / (243.04 + float(dewp_c))
        denom = 17.625 * float(temp_c)  / (243.04 + float(temp_c))
        rh    = 100 * (num - denom)
        # Very dry desert air (dewpoint far below temp) legitimately gives low RH;
        # use the simpler approximation rh ≈ 100 - 5*(T-Td) as a sanity floor
        rh2   = max(0, 100 - 5 * (float(temp_c) - float(dewp_c)))
        return max(1, min(100, round(max(rh, rh2) if rh < 0 else rh)))
    except (TypeError, ValueError):
        return None

def degrees_to_compass(deg):
    if deg is None: return None
    try:
        deg = float(deg)
    except (TypeError, ValueError):
        return None   # handles "VRB" and other non-numeric values
    return ['N','NNE','NE','ENE','E','ESE','SE','SSE','S','SSW','SW','WSW','W','WNW','NW','NNW'][round(deg / 22.5) % 16]

def parse_visibility(vis_str):
    """Return (display_str, numeric_value) from AWC visibility field."""
    if vis_str is None: return None, None
    s = str(vis_str).strip()
    if s == "10+" or s == "10+SM":
        return "10+", 10.0
    try:
        return s, float(s)
    except ValueError:
        return s, None

def parse_clouds(cloud_list):
    """
    Parse AWC clouds array: [{"cover": "FEW", "base": 2500}, ...]
    Returns (sky_cover_pct, ceiling_ft, cloud_layer_string).
    Ceiling = lowest BKN, OVC, or VV layer.
    """
    if not cloud_list:
        return None, None, ""

    sky_pct = 0
    ceiling = None
    layer_parts = []

    for layer in cloud_list:
        cover = (layer.get("cover") or "").upper()
        base  = layer.get("base")  # feet AGL, may be None

        if cover in CLOUD_COVER:
            pct = CLOUD_COVER[cover]
            sky_pct = max(sky_pct, pct)

        if cover in ("BKN", "OVC", "OVX", "VV") and base is not None:
            if ceiling is None or base < ceiling:
                ceiling = base

        if cover and cover not in ("SKC", "CLR", "NSC", "CAVOK"):
            if base is not None:
                layer_parts.append(f"{cover}{base:03d}" if base < 100000 else cover)
            else:
                layer_parts.append(cover)

    return sky_pct if sky_pct > 0 else 0, ceiling, " ".join(layer_parts)

def flight_category(vis_numeric, ceiling_ft):
    """Compute standard IFR flight category from visibility and ceiling."""
    if vis_numeric is None and ceiling_ft is None:
        return ""
    v = vis_numeric if vis_numeric is not None else 10.0
    c = ceiling_ft  if ceiling_ft  is not None else 99999

    if v >= 5 and c >= 3000:  return "VFR"
    if v >= 3 and c >= 1000:  return "MVFR"
    if v >= 1 and c >= 500:   return "IFR"
    return "LIFR"

def utc_to_local_date(utc_str):
    """Convert UTC ISO string to Las Vegas local date (PDT=-7, PST=-8)."""
    try:
        # Parse the UTC time
        dt = datetime.fromisoformat(utc_str.replace("Z", "+00:00"))
        # Determine offset: PDT (Mar 2nd Sun – Nov 1st Sun) = -7, else -8
        # Simple approximation: months 3-10 = PDT=-7, else PST=-8
        offset_hours = -7 if 3 <= dt.month <= 10 else -8
        local_hour = dt.hour + offset_hours
        local_date = dt.date()
        if local_hour < 0:
            from datetime import timedelta
            local_date = local_date - timedelta(days=1)
        return str(local_date)
    except Exception:
        return ""


# ── API fetch ──────────────────────────────────────────────────────────────────
def fetch_metars(hours=3, retries=3, timeout=20):
    """
    Fetch recent METARs from AWC API.
    Returns list of observation dicts.
    """
    url = f"{AWC_BASE}?ids={STATION}&format=json&hours={hours}"
    for attempt in range(1, retries + 1):
        try:
            print(f"    GET {url}  (attempt {attempt}/{retries})")
            resp = requests.get(url, headers=HEADERS, timeout=timeout)
            resp.raise_for_status()
            data = resp.json()
            # AWC returns a list directly
            if isinstance(data, list):
                return data
            # Or wrapped in a data key
            return data.get("data", data)
        except (requests.exceptions.Timeout, requests.exceptions.ConnectionError):
            if attempt == retries: raise
            print(f"    Timeout — waiting 10s…"); time.sleep(10)
        except requests.exceptions.HTTPError:
            if attempt == retries or resp.status_code not in (500, 503): raise
            print(f"    HTTP {resp.status_code} — waiting 10s…"); time.sleep(10)


# ── Parse one METAR observation ────────────────────────────────────────────────
def parse_observation(ob, retrieved_at):
    """Convert one AWC METAR JSON object into a CSV row dict."""

    # Timestamp — AWC gives obsTime (epoch) and reportTime (string)
    obs_time_utc = ""
    obs_date_utc = ""
    obs_date_local = ""

    obs_epoch = ob.get("obsTime")
    if obs_epoch:
        dt = datetime.fromtimestamp(obs_epoch, tz=timezone.utc)
        obs_time_utc   = dt.strftime("%Y-%m-%dT%H:%M:%SZ")
        obs_date_utc   = dt.strftime("%Y-%m-%d")
        obs_date_local = utc_to_local_date(obs_time_utc)

    # Temperature
    temp_c  = ob.get("temp")
    dewp_c  = ob.get("dewp")
    temp_f  = c_to_f(temp_c)
    dewp_f  = c_to_f(dewp_c)
    rh      = compute_rh(temp_c, dewp_c)

    # Wind — wdir may be an int, float, or "VRB" (variable)
    wdir_raw = ob.get("wdir")
    wspd_kt  = ob.get("wspd")
    wgst_kt  = ob.get("wgst")

    # Normalise wdir: "VRB" → store as "VRB"; numeric → int
    if wdir_raw is None:
        wdir    = None
        compass = None
    elif str(wdir_raw).upper() == "VRB":
        wdir    = "VRB"
        compass = "VRB"
    else:
        try:
            wdir    = int(float(wdir_raw))
            compass = degrees_to_compass(wdir) if wdir != 0 else "CALM"
        except (TypeError, ValueError):
            wdir    = wdir_raw
            compass = None

    # Visibility
    vis_str, vis_num = parse_visibility(ob.get("visib"))

    # Clouds
    sky_pct, ceil_ft, cloud_str = parse_clouds(ob.get("clouds", []))

    # Flight category (use AWC's if available, else compute)
    fc = ob.get("flightCategory") or flight_category(vis_num, ceil_ft)

    # Pressure
    altim = ob.get("altim")   # inHg in AWC JSON
    slp   = ob.get("slp")     # mb

    # Precip accumulations (may be None)
    def pn(val):
        if val is None or val == "": return ""
        try: return round(float(val), 2)
        except: return ""

    def v(val):
        return val if val is not None else ""

    return {
        "obs_time_utc":          obs_time_utc,
        "obs_date_utc":          obs_date_utc,
        "obs_date_local":        obs_date_local,
        "metar_type":            v(ob.get("metarType")),
        "retrieved_at":          retrieved_at,
        "temp_c":                v(temp_c),
        "temp_f":                v(temp_f),
        "dewpoint_c":            v(dewp_c),
        "dewpoint_f":            v(dewp_f),
        "relative_humidity":     v(rh),
        "wind_dir_deg":          v(wdir),      # int degrees, "VRB", or None
        "wind_dir_compass":      v(compass),   # cardinal, "VRB", "CALM", or None
        "wind_speed_kt":         v(wspd_kt),
        "wind_speed_mph":        v(kt_to_mph(wspd_kt)),
        "wind_gust_kt":          v(wgst_kt),
        "wind_gust_mph":         v(kt_to_mph(wgst_kt)),
        "visibility_sm":         v(vis_str),
        "visibility_sm_numeric": v(vis_num),
        "sky_cover_pct":         v(sky_pct),
        "ceiling_ft":            v(ceil_ft),
        "cloud_layers":          cloud_str,
        "altimeter_inhg":        v(round(altim, 2) if altim else None),
        "sea_level_pressure_mb": v(slp),
        "precip_1hr_in":         pn(ob.get("precip")),
        "precip_3hr_in":         pn(ob.get("pcp3hr")),
        "precip_6hr_in":         pn(ob.get("pcp6hr")),
        "precip_24hr_in":        pn(ob.get("pcp24hr")),
        "wx_string":             v(ob.get("wxString")),
        "flight_category":       v(fc),
        "raw_metar":             v(ob.get("rawOb")),
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
                if line and not line.startswith("obs_time_utc"):
                    f.write(line + "\n")
        print(f"  Header updated.")


def load_existing_timestamps():
    """Return set of obs_time_utc values already in the CSV."""
    seen = set()
    if not os.path.exists(OUTPUT_FILE):
        return seen
    with open(OUTPUT_FILE, newline="") as f:
        for row in csv.DictReader(f):
            t = row.get("obs_time_utc", "").strip()
            if t: seen.add(t)
    return seen


# ── Main ───────────────────────────────────────────────────────────────────────
def main():
    retrieved_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    print(f"METAR Collector — {STATION}")
    print(f"Output: {OUTPUT_FILE}")
    print(f"Retrieved at: {retrieved_at}")

    ensure_output_file()
    existing = load_existing_timestamps()
    print(f"  Existing observations in CSV: {len(existing)}")

    print(f"  Fetching last {LOOKBACK_HOURS} hours of METARs…")
    observations = fetch_metars(hours=LOOKBACK_HOURS)
    print(f"  Got {len(observations)} observations from API")

    written = 0
    skipped = 0

    with open(OUTPUT_FILE, "a", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_COLUMNS)
        for ob in observations:
            row = parse_observation(ob, retrieved_at)
            ts  = row.get("obs_time_utc", "")

            if not ts:
                print(f"  ?? Could not parse timestamp, skipping")
                continue

            if ts in existing:
                skipped += 1
                continue

            writer.writerow(row)
            existing.add(ts)
            written += 1

            # Print each new observation on one line
            wx = f"  {row['wx_string']}" if row.get('wx_string') else ""
            print(
                f"  OK {ts}  "
                f"T={row['temp_f']}°F  "
                f"Td={row['dewpoint_f']}°F  "
                f"RH={row['relative_humidity']}%  "
                f"Wnd={row['wind_speed_mph']}mph {row['wind_dir_compass']}  "
                f"Gst={row['wind_gust_mph'] or '—'}  "
                f"Vis={row['visibility_sm']}SM  "
                f"Sky={row['sky_cover_pct']}%  "
                f"{row['flight_category']}"
                f"{wx}"
            )

    print(f"\n  Written : {written}")
    print(f"  Skipped : {skipped} (already existed)")
    print(f"  Done.")


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
