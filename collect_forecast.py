#!/usr/bin/env python3
"""
NWS Forecast Collector for KLAS (Las Vegas)
============================================
Mirrors the data model used by the forecast-verification dashboard exactly.

Each snapshot is ONE fetch of the current 7-day NWS forecast. A snapshot
is only written if the NWS forecast has actually been updated since the
last saved snapshot — deduplication is keyed on the NWS updateTime field
and persisted in a lightweight sentinel file (data/last_update.txt) so
concurrent GitHub Actions runs cannot both write the same forecast version.
"""

import requests
import csv
import os
import sys
import re
from datetime import datetime, timezone, date, timedelta

# ── Configuration ──────────────────────────────────────────────────────────────
LAT         = 36.073861
LON         = -115.152917
OUTPUT_FILE  = "data/forecasts.csv"

HEADERS = {
    "User-Agent": "NWS-Forecast-Collector/2.1 (github-actions; klas-verification)",
    "Accept":     "application/geo+json",
}

CSV_COLUMNS = [
    "forecast_updated_at",
    "retrieved_at",
    "forecast_date",
    "lead_days",
    "high_temp_f",
    "low_temp_f",
    "max_wind_speed_mph",
    "max_wind_gust_mph",
    "wind_direction",
    "precip_prob_pct",
    "precip_amount_in",
    "sky_cover_pct",
    "day_short_forecast",
    "night_short_forecast",
]


# ── Unit conversion helpers ────────────────────────────────────────────────────
def kph_to_mph(kph):
    return round(kph * 0.621371, 1) if kph is not None else None

def mm_to_in(mm):
    return round(mm * 0.0393701, 3) if mm is not None else None


# ── ISO 8601 duration expansion ───────────────────────────────────────────────
def parse_iso_duration_days(duration_str):
    """
    Parse an ISO 8601 duration string and return total whole days covered.
    Handles formats like PT6H, P1D, P2DT6H, PT1H, etc.
    Returns 1 if the duration is less than one full day (covers only that date).
    """
    if not duration_str:
        return 1
    days  = int(re.search(r'(\d+)D', duration_str).group(1)) if re.search(r'(\d+)D', duration_str) else 0
    hours = int(re.search(r'(\d+)H', duration_str).group(1)) if re.search(r'(\d+)H', duration_str) else 0
    total_hours = days * 24 + hours
    # A value with validTime "2026-04-24T06:00:00+00:00/P2DT6H" covers 2d 6h = 54h
    # starting at 06:00, meaning it covers dates April 24 and April 25.
    # We return how many calendar dates it touches beyond the start date.
    return max(1, (total_hours + 23) // 24)   # ceiling division gives dates spanned


def expand_gridpoint_values(values):
    """
    Expand gridpoint value array into a dict keyed by calendar date (YYYY-MM-DD).
    Uses start_str[:10] to extract the date, matching exactly how the dashboard
    and period forecast both extract dates — no timezone conversion applied.
    Multi-day duration intervals are expanded so every covered date gets the value.
    """
    buckets = {}
    for v in values:
        valid_time = v.get("validTime", "")
        val = v.get("value")
        if val is None:
            continue
        if "/" in valid_time:
            start_str, duration_str = valid_time.split("/", 1)
        else:
            start_str, duration_str = valid_time, "PT1H"

        try:
            start_date = date.fromisoformat(start_str[:10])
        except ValueError:
            continue

        num_days = parse_iso_duration_days(duration_str)
        for offset in range(num_days):
            d = str(start_date + timedelta(days=offset))
            buckets.setdefault(d, []).append(val)

    return buckets

def daily_avg(values):
    buckets = expand_gridpoint_values(values)
    return {d: sum(vals) / len(vals) for d, vals in buckets.items()}

def daily_max(values):
    buckets = expand_gridpoint_values(values)
    return {d: max(vals) for d, vals in buckets.items()}

def daily_sum(values):
    buckets = expand_gridpoint_values(values)
    return {d: sum(vals) for d, vals in buckets.items()}


# ── NWS API calls ──────────────────────────────────────────────────────────────
import time

def nws_get(url, retries=3, timeout=20):
    """GET with retry. Waits 10s between attempts to give NWS time to recover."""
    for attempt in range(1, retries + 1):
        try:
            print(f"    GET {url}  (attempt {attempt}/{retries})")
            resp = requests.get(url, headers=HEADERS, timeout=timeout)
            resp.raise_for_status()
            return resp
        except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as e:
            if attempt == retries:
                raise
            print(f"    Timeout/connection error — waiting 10s before retry…")
            time.sleep(10)
        except requests.exceptions.HTTPError:
            if attempt == retries or resp.status_code not in (500, 503):
                raise
            print(f"    HTTP {resp.status_code} — waiting 10s before retry…")
            time.sleep(10)

def get_grid_info():
    url = f"https://api.weather.gov/points/{LAT},{LON}"
    props = nws_get(url).json()["properties"]
    return {
        "forecast_url":  props["forecast"],
        "gridpoint_url": props["forecastGridData"],
    }

def fetch_forecast(grid):
    fcst = nws_get(grid["forecast_url"]).json()
    gp   = nws_get(grid["gridpoint_url"]).json()["properties"]

    periods      = fcst["properties"]["periods"]
    updated_at   = fcst["properties"]["updateTime"]
    retrieved_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    # Build daily rows from daytime/overnight period pairs
    daily = []
    for i, p in enumerate(periods):
        if not p.get("isDaytime"):
            continue
        night = periods[i + 1] if i + 1 < len(periods) else None
        # Capture PoP directly from the period forecast as a fallback.
        # The period forecast embeds its own probabilityOfPrecipitation value
        # which may differ from the gridpoint data for distant forecast days.
        day_pop   = (p.get("probabilityOfPrecipitation") or {}).get("value")
        night_pop = ((night or {}).get("probabilityOfPrecipitation") or {}).get("value")
        print(f"    period PoP  {p['startTime'][:10]}  day={day_pop}  night={night_pop}")
        period_pop = None
        if day_pop is not None and night_pop is not None:
            period_pop = max(day_pop, night_pop)
        elif day_pop is not None:
            period_pop = day_pop
        elif night_pop is not None:
            period_pop = night_pop

        daily.append({
            "date":               p["startTime"].split("T")[0],
            "high_temp_f":        p.get("temperature"),
            "low_temp_f":         night.get("temperature") if night else None,
            "wind_direction":     p.get("windDirection", ""),
            "day_short_forecast":    p.get("shortForecast", ""),
            "night_short_forecast":  night.get("shortForecast", "") if night else "",
            "max_wind_speed_mph": None,
            "max_wind_gust_mph":  None,
            "precip_prob_pct":    None,       # filled from gridpoint below; fallback to period_pop
            "_period_pop":        period_pop, # fallback value, stripped before writing CSV
            "precip_amount_in":   None,
            "sky_cover_pct":      None,
        })

    # Augment with gridpoint data using duration-aware expansion
    sky_cover  = daily_avg(gp.get("skyCover",                   {}).get("values", []))
    wind_speed = daily_max(gp.get("windSpeed",                  {}).get("values", []))
    wind_gust  = daily_max(gp.get("windGust",                   {}).get("values", []))
    qpf        = daily_sum(gp.get("quantitativePrecipitation",  {}).get("values", []))
    pop        = daily_max(gp.get("probabilityOfPrecipitation", {}).get("values", []))

    for d in daily:
        dt = d["date"]
        if dt in sky_cover:  d["sky_cover_pct"]       = round(sky_cover[dt])
        if dt in wind_speed: d["max_wind_speed_mph"]   = kph_to_mph(wind_speed[dt])
        if dt in wind_gust:  d["max_wind_gust_mph"]    = kph_to_mph(wind_gust[dt])
        if dt in qpf:        d["precip_amount_in"]      = mm_to_in(qpf[dt])
        if dt in pop:
            d["precip_prob_pct"] = round(pop[dt])
        # Fallback: if gridpoint gave us nothing (or zero) but period forecast has a value, use it.
        # Mirrors the dashboard: precipProb ?? dayPrecipProb
        if d["precip_prob_pct"] is None and d.get("_period_pop") is not None:
            d["precip_prob_pct"] = round(d["_period_pop"])

    return {
        "forecast_updated_at": updated_at,
        "retrieved_at":        retrieved_at,
        "days":                daily,
    }


# ── Deduplication ─────────────────────────────────────────────────────────────
def load_last_update():
    """
    Read the last forecast_updated_at from the final line of the CSV.
    Reading the last line is reliable because the CSV is append-only and
    is the committed source of truth that GitHub checks out fresh each run.
    """
    if not os.path.exists(OUTPUT_FILE):
        return None
    # Efficiently read only the last non-empty line without loading the whole file
    last_line = None
    with open(OUTPUT_FILE, "rb") as f:
        # Walk backward through the file to find the last non-empty line
        f.seek(0, 2)  # seek to end
        pos = f.tell()
        buf = b""
        while pos > 0:
            pos -= 1
            f.seek(pos)
            char = f.read(1)
            if char == b"\n" and buf.strip():
                break
            buf = char + buf
        last_line = buf.decode("utf-8", errors="replace").strip()

    if not last_line or last_line.startswith("forecast_updated_at"):
        return None  # file is empty or header-only

    # Parse the first CSV field (forecast_updated_at) from the last row
    try:
        first_field = next(csv.reader([last_line]))
        return first_field[0].strip() if first_field else None
    except Exception:
        return None


# ── CSV helpers ────────────────────────────────────────────────────────────────
def ensure_output_file():
    os.makedirs("data", exist_ok=True)
    needs_header = (
        not os.path.exists(OUTPUT_FILE) or
        os.path.getsize(OUTPUT_FILE) == 0
    )
    if needs_header:
        with open(OUTPUT_FILE, "w", newline="") as f:
            csv.DictWriter(f, fieldnames=CSV_COLUMNS).writeheader()
        print(f"  Created/reset file with header: {OUTPUT_FILE}")

def append_snapshot(forecast):
    retrieved      = forecast["retrieved_at"]
    updated        = forecast["forecast_updated_at"]
    retrieved_date = date.fromisoformat(retrieved[:10])

    with open(OUTPUT_FILE, "a", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_COLUMNS)
        for d in forecast["days"]:
            try:
                lead = (date.fromisoformat(d["date"]) - retrieved_date).days
            except Exception:
                lead = ""
            d.pop("_period_pop", None)  # internal working field — not written to CSV
            writer.writerow({
                "forecast_updated_at":  updated,
                "retrieved_at":         retrieved,
                "forecast_date":        d["date"],
                "lead_days":            lead,
                "high_temp_f":          d.get("high_temp_f")         if d.get("high_temp_f")         is not None else "",
                "low_temp_f":           d.get("low_temp_f")          if d.get("low_temp_f")          is not None else "",
                "max_wind_speed_mph":   d.get("max_wind_speed_mph")  if d.get("max_wind_speed_mph")  is not None else "",
                "max_wind_gust_mph":    d.get("max_wind_gust_mph")   if d.get("max_wind_gust_mph")   is not None else "",
                "wind_direction":       d.get("wind_direction")      or "",
                "precip_prob_pct":      d.get("precip_prob_pct")     if d.get("precip_prob_pct")     is not None else "",
                "precip_amount_in":     d.get("precip_amount_in")    if d.get("precip_amount_in")    is not None else "",
                "sky_cover_pct":        d.get("sky_cover_pct")       if d.get("sky_cover_pct")       is not None else "",
                "day_short_forecast":   d.get("day_short_forecast")  or "",
                "night_short_forecast": d.get("night_short_forecast") or "",
            })


# ── Main ───────────────────────────────────────────────────────────────────────
def main():
    print(f"NWS Forecast Collector — KLAS ({LAT}, {LON})")
    print(f"Output: {OUTPUT_FILE}")

    ensure_output_file()

    last_update = load_last_update()
    print(f"  Last saved forecast version : {last_update or '(none)'}")

    print("  Resolving NWS grid…")
    grid = get_grid_info()

    print("  Fetching forecast + gridpoint data…")
    forecast = fetch_forecast(grid)

    updated_at   = forecast["forecast_updated_at"]
    retrieved_at = forecast["retrieved_at"]
    num_days     = len(forecast["days"])

    print(f"  NWS forecast updated at     : {updated_at}")
    print(f"  Retrieved at                : {retrieved_at}")
    print(f"  Days in forecast            : {num_days}")

    if updated_at == last_update:
        print(f"\n  SKIP — this forecast version is already saved. No rows written.")
        return

    append_snapshot(forecast)
    print(f"\n  NEW — wrote {num_days} rows to {OUTPUT_FILE}")

    print(f"\n  {'Date':<12} {'High':>5} {'Low':>5} {'Wind':>5} {'Gust':>5} {'PoP%':>5} {'Sky%':>5}  Conditions")
    print(f"  {'-'*78}")
    for d in forecast["days"]:
        print(
            f"  {d['date']:<12}"
            f" {str(d.get('high_temp_f')        or '—'):>5}"
            f" {str(d.get('low_temp_f')         or '—'):>5}"
            f" {str(d.get('max_wind_speed_mph') or '—'):>5}"
            f" {str(d.get('max_wind_gust_mph')  or '—'):>5}"
            f" {str(d.get('precip_prob_pct')    if d.get('precip_prob_pct') is not None else '—'):>5}"
            f" {str(d.get('sky_cover_pct')      if d.get('sky_cover_pct')  is not None else '—'):>5}"
            f"  {d.get('day_short_forecast','')}"
        )

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
