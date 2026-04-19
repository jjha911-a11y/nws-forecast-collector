#!/usr/bin/env python3
"""
NWS Forecast Collector for KLAS (Las Vegas)
============================================
Mirrors the data model used by the forecast-verification dashboard exactly.

Each snapshot is ONE fetch of the current 7-day NWS forecast. A snapshot
is only written if the NWS forecast has actually been updated since the
last saved snapshot — deduplication is keyed on the NWS `updateTime` field.

Each snapshot produces exactly 7 rows (one per forecast day), plus two
timestamp columns:
  - forecast_updated_at  : when NWS last issued this forecast (their updateTime)
  - retrieved_at         : when THIS script ran and pulled the data (UTC)

CSV grows by 7 rows per new forecast version. Typically NWS updates the
forecast 2-4 times per day, so you'll accumulate 14-28 rows per day.
Running the script more often than the NWS update cycle costs nothing —
duplicates are simply skipped and logged.
"""

import requests
import csv
import os
import sys
from datetime import datetime, timezone, date

# ── Configuration ──────────────────────────────────────────────────────────────
LAT         = 36.073861      # matches dashboard exactly
LON         = -115.152917    # matches dashboard exactly
OUTPUT_FILE = "data/forecasts.csv"

HEADERS = {
    "User-Agent": "NWS-Forecast-Collector/2.0 (github-actions; klas-verification)",
    "Accept":     "application/geo+json",
}

# Column order must never change once collection has started.
# Adding new columns at the end is safe; reordering breaks existing data.
CSV_COLUMNS = [
    "forecast_updated_at",   # NWS updateTime — the deduplication key
    "retrieved_at",          # when this script ran (UTC ISO8601)
    "forecast_date",         # the calendar date this row covers (YYYY-MM-DD)
    "lead_days",             # days from retrieval date to forecast_date
    "high_temp_f",           # forecast daytime high °F
    "low_temp_f",            # forecast overnight low °F
    "max_wind_speed_mph",    # max sustained wind speed mph (from gridpoint data)
    "max_wind_gust_mph",     # max wind gust mph (from gridpoint data)
    "wind_direction",        # daytime wind direction (cardinal)
    "precip_prob_pct",       # max probability of precipitation % (from gridpoint)
    "precip_amount_in",      # quantitative precipitation forecast inches
    "sky_cover_pct",         # average sky cover % for the day
    "day_short_forecast",    # NWS short text for daytime period
    "night_short_forecast",  # NWS short text for overnight period
]


# ── Unit conversion helpers ────────────────────────────────────────────────────
def kph_to_mph(kph):
    return round(kph * 0.621371, 1) if kph is not None else None

def mm_to_in(mm):
    return round(mm * 0.0393701, 3) if mm is not None else None


# ── Gridpoint data aggregation (matches dashboard JS exactly) ─────────────────
def extract_daily_avg(values):
    buckets = {}
    for v in values:
        day = v["validTime"].split("/")[0].split("T")[0]
        buckets.setdefault(day, []).append(v["value"])
    return {d: sum(vals) / len(vals) for d, vals in buckets.items()}

def extract_daily_max(values):
    result = {}
    for v in values:
        day = v["validTime"].split("/")[0].split("T")[0]
        if v["value"] is not None:
            if day not in result or v["value"] > result[day]:
                result[day] = v["value"]
    return result

def extract_daily_sum(values):
    result = {}
    for v in values:
        day = v["validTime"].split("/")[0].split("T")[0]
        result[day] = result.get(day, 0) + (v["value"] or 0)
    return result


# ── NWS API calls ──────────────────────────────────────────────────────────────
def get_grid_info():
    url = f"https://api.weather.gov/points/{LAT},{LON}"
    resp = requests.get(url, headers=HEADERS, timeout=15)
    resp.raise_for_status()
    props = resp.json()["properties"]
    return {
        "forecast_url":  props["forecast"],
        "gridpoint_url": props["forecastGridData"],
    }

def fetch_forecast(grid):
    fcst_resp = requests.get(grid["forecast_url"],  headers=HEADERS, timeout=15)
    fcst_resp.raise_for_status()
    fcst = fcst_resp.json()

    grid_resp = requests.get(grid["gridpoint_url"], headers=HEADERS, timeout=15)
    grid_resp.raise_for_status()
    gp = grid_resp.json()["properties"]

    periods    = fcst["properties"]["periods"]
    updated_at = fcst["properties"]["updateTime"]
    retrieved_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    daily = []
    for i, p in enumerate(periods):
        if not p.get("isDaytime"):
            continue
        night = periods[i + 1] if i + 1 < len(periods) else None
        daily.append({
            "date":               p["startTime"].split("T")[0],
            "high_temp_f":        p.get("temperature"),
            "low_temp_f":         night.get("temperature") if night else None,
            "wind_direction":     p.get("windDirection", ""),
            "day_short_forecast":    p.get("shortForecast", ""),
            "night_short_forecast":  night.get("shortForecast", "") if night else "",
            "max_wind_speed_mph": None,
            "max_wind_gust_mph":  None,
            "precip_prob_pct":    None,
            "precip_amount_in":   None,
            "sky_cover_pct":      None,
        })

    sky_cover  = extract_daily_avg(gp.get("skyCover",                   {}).get("values", []))
    wind_speed = extract_daily_max(gp.get("windSpeed",                  {}).get("values", []))
    wind_gust  = extract_daily_max(gp.get("windGust",                   {}).get("values", []))
    qpf        = extract_daily_sum(gp.get("quantitativePrecipitation",  {}).get("values", []))
    pop        = extract_daily_max(gp.get("probabilityOfPrecipitation", {}).get("values", []))

    for d in daily:
        dt = d["date"]
        if dt in sky_cover:  d["sky_cover_pct"]      = round(sky_cover[dt])
        if dt in wind_speed: d["max_wind_speed_mph"]  = kph_to_mph(wind_speed[dt])
        if dt in wind_gust:  d["max_wind_gust_mph"]   = kph_to_mph(wind_gust[dt])
        if dt in qpf:        d["precip_amount_in"]     = mm_to_in(qpf[dt])
        if dt in pop:        d["precip_prob_pct"]      = round(pop[dt])

    return {
        "forecast_updated_at": updated_at,
        "retrieved_at":        retrieved_at,
        "days":                daily,
    }


# ── CSV helpers ────────────────────────────────────────────────────────────────
def ensure_output_file():
    os.makedirs("data", exist_ok=True)
    if not os.path.exists(OUTPUT_FILE):
        with open(OUTPUT_FILE, "w", newline="") as f:
            csv.DictWriter(f, fieldnames=CSV_COLUMNS).writeheader()
        print(f"  Created new file: {OUTPUT_FILE}")

def load_existing_update_times():
    seen = set()
    if not os.path.exists(OUTPUT_FILE):
        return seen
    with open(OUTPUT_FILE, newline="") as f:
        for row in csv.DictReader(f):
            val = row.get("forecast_updated_at", "").strip()
            if val:
                seen.add(val)
    return seen

def append_snapshot(forecast):
    retrieved    = forecast["retrieved_at"]
    updated      = forecast["forecast_updated_at"]
    retrieved_date = date.fromisoformat(retrieved[:10])

    with open(OUTPUT_FILE, "a", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_COLUMNS)
        for d in forecast["days"]:
            try:
                lead = (date.fromisoformat(d["date"]) - retrieved_date).days
            except Exception:
                lead = ""
            writer.writerow({
                "forecast_updated_at":  updated,
                "retrieved_at":         retrieved,
                "forecast_date":        d["date"],
                "lead_days":            lead,
                "high_temp_f":          d.get("high_temp_f")        or "",
                "low_temp_f":           d.get("low_temp_f")         or "",
                "max_wind_speed_mph":   d.get("max_wind_speed_mph") or "",
                "max_wind_gust_mph":    d.get("max_wind_gust_mph")  or "",
                "wind_direction":       d.get("wind_direction")     or "",
                "precip_prob_pct":      d.get("precip_prob_pct")    if d.get("precip_prob_pct") is not None else "",
                "precip_amount_in":     d.get("precip_amount_in")   if d.get("precip_amount_in") is not None else "",
                "sky_cover_pct":        d.get("sky_cover_pct")      if d.get("sky_cover_pct") is not None else "",
                "day_short_forecast":   d.get("day_short_forecast") or "",
                "night_short_forecast": d.get("night_short_forecast") or "",
            })


# ── Main ───────────────────────────────────────────────────────────────────────
def main():
    print(f"NWS Forecast Collector — KLAS ({LAT}, {LON})")
    print(f"Output: {OUTPUT_FILE}")

    ensure_output_file()

    seen_updates = load_existing_update_times()
    print(f"  Known forecast versions in CSV: {len(seen_updates)}")

    print("  Resolving NWS grid…")
    grid = get_grid_info()

    print("  Fetching forecast + gridpoint data…")
    forecast = fetch_forecast(grid)

    updated_at   = forecast["forecast_updated_at"]
    retrieved_at = forecast["retrieved_at"]
    num_days     = len(forecast["days"])

    print(f"  NWS forecast updated at : {updated_at}")
    print(f"  Retrieved at            : {retrieved_at}")
    print(f"  Days in forecast        : {num_days}")

    if updated_at in seen_updates:
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
            f" {str(d.get('precip_prob_pct')    or '—'):>5}"
            f" {str(d.get('sky_cover_pct')      or '—'):>5}"
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
