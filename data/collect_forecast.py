#!/usr/bin/env python3
"""
NWS Forecast Collector for KLAS (Las Vegas)
Runs on a schedule via GitHub Actions and appends forecast snapshots to CSV.

Each run captures the CURRENT NWS hourly forecast and saves it with a
"captured_at" timestamp so we know exactly when that forecast was issued.
This is the data that would otherwise be lost — NWS only serves the current
forecast, not historical ones.
"""

import requests
import csv
import json
import os
import sys
from datetime import datetime, timezone

# ── Configuration ──────────────────────────────────────────────────────────────
LAT = 36.08
LON = -115.15
STATION = "KLAS"
OUTPUT_FILE = "data/forecasts.csv"   # relative to repo root
HEADERS = {
    "User-Agent": "NWS-Forecast-Collector/1.0 (github-actions; klas-verification)",
    "Accept": "application/geo+json",
}

# These are the CSV column headers — order matters, must stay consistent
CSV_COLUMNS = [
    "captured_at",        # when THIS script ran (UTC ISO8601)
    "forecast_valid_time",# the hour this forecast period covers (UTC ISO8601)
    "temp_f",             # forecast temperature °F
    "dewpoint_c",         # forecast dewpoint °C (NWS native unit)
    "wind_speed_mph",     # forecast wind speed mph
    "wind_direction",     # forecast wind direction (cardinal, e.g. "SW")
    "relative_humidity",  # forecast RH %
    "prob_precip",        # probability of precipitation %
    "short_forecast",     # NWS short text description
]


def get_nws_gridpoint():
    """Get the NWS grid coordinates for our lat/lon."""
    url = f"https://api.weather.gov/points/{LAT},{LON}"
    resp = requests.get(url, headers=HEADERS, timeout=15)
    resp.raise_for_status()
    props = resp.json()["properties"]
    return props["forecastHourly"]


def get_hourly_forecast(hourly_url):
    """Fetch the current NWS hourly forecast."""
    resp = requests.get(hourly_url, headers=HEADERS, timeout=15)
    resp.raise_for_status()
    return resp.json()["properties"]["periods"]


def parse_wind_speed(wind_str):
    """Extract numeric mph from strings like '10 mph' or '5 to 10 mph'."""
    if not wind_str:
        return ""
    parts = wind_str.replace(" mph", "").split(" to ")
    try:
        # If range like "5 to 10", take the average
        nums = [float(p.strip()) for p in parts if p.strip().isdigit() or p.strip().replace(".", "").isdigit()]
        return round(sum(nums) / len(nums), 1) if nums else ""
    except Exception:
        return ""


def ensure_output_file():
    """Create the data directory and CSV with headers if it doesn't exist yet."""
    os.makedirs("data", exist_ok=True)
    if not os.path.exists(OUTPUT_FILE):
        with open(OUTPUT_FILE, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=CSV_COLUMNS)
            writer.writeheader()
        print(f"Created new file: {OUTPUT_FILE}")


def append_forecast_rows(periods, captured_at):
    """Append all forecast periods from this snapshot to the CSV."""
    rows_written = 0
    with open(OUTPUT_FILE, "a", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_COLUMNS)
        for period in periods:
            # Parse dewpoint — NWS returns it as {"value": X, "unitCode": "..."}
            dp = period.get("dewpoint", {})
            dewpoint_c = dp.get("value", "") if isinstance(dp, dict) else ""

            rh = period.get("relativeHumidity", {})
            rel_humidity = rh.get("value", "") if isinstance(rh, dict) else ""

            pp = period.get("probabilityOfPrecipitation", {})
            prob_precip = pp.get("value", "") if isinstance(pp, dict) else ""

            row = {
                "captured_at": captured_at,
                "forecast_valid_time": period.get("startTime", ""),
                "temp_f": period.get("temperature", ""),
                "dewpoint_c": dewpoint_c if dewpoint_c is not None else "",
                "wind_speed_mph": parse_wind_speed(period.get("windSpeed", "")),
                "wind_direction": period.get("windDirection", ""),
                "relative_humidity": rel_humidity if rel_humidity is not None else "",
                "prob_precip": prob_precip if prob_precip is not None else "",
                "short_forecast": period.get("shortForecast", ""),
            }
            writer.writerow(row)
            rows_written += 1
    return rows_written


def main():
    captured_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    print(f"[{captured_at}] Starting NWS forecast collection for {STATION}")

    ensure_output_file()

    print("  Fetching NWS grid point...")
    hourly_url = get_nws_gridpoint()
    print(f"  Hourly forecast URL: {hourly_url}")

    print("  Fetching hourly forecast...")
    periods = get_hourly_forecast(hourly_url)
    print(f"  Got {len(periods)} forecast periods")

    rows = append_forecast_rows(periods, captured_at)
    print(f"  Wrote {rows} rows to {OUTPUT_FILE}")

    # Report the first few hours as a sanity check in the Actions log
    print("\n  Forecast snapshot (next 6 hours):")
    for p in periods[:6]:
        print(f"    {p['startTime'][:16]}  {p['temperature']}°F  {p.get('shortForecast','')}")

    print(f"\n[{captured_at}] Collection complete.")


if __name__ == "__main__":
    try:
        main()
    except requests.exceptions.RequestException as e:
        print(f"ERROR: Network request failed: {e}", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"ERROR: Unexpected error: {e}", file=sys.stderr)
        sys.exit(1)
