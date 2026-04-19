#!/usr/bin/env python3
"""
NWS Forecast Collector for KLAS (Las Vegas)
============================================
Mirrors the data model used by the forecast-verification dashboard exactly.

Each snapshot is ONE fetch of the current 7-day NWS forecast. A snapshot
is only written if the NWS forecast has actually been updated since the
last saved snapshot — deduplication is keyed on the NWS updateTime field.

New fields added (April 2026):
  dewpoint, relative_humidity, apparent_temp (heat index/wind chill),
  visibility, prob_thunder, weather_summary (structured condition codes)
"""

import requests
import csv
import os
import sys
import re
import time
from datetime import datetime, timezone, date, timedelta

# ── Configuration ──────────────────────────────────────────────────────────────
LAT          = 36.073861
LON          = -115.152917
OUTPUT_FILE  = "data/forecasts.csv"

HEADERS = {
    "User-Agent": "NWS-Forecast-Collector/3.0 (github-actions; klas-verification)",
    "Accept":     "application/geo+json",
}

# Column order must never change once collection has started.
# New columns may be appended at the end safely.
CSV_COLUMNS = [
    # ── Identity ───────────────────────────────────────────────────────────────
    "forecast_updated_at",    # NWS updateTime — deduplication key
    "retrieved_at",           # when this script ran (UTC ISO8601)
    "forecast_date",          # calendar date this row covers (YYYY-MM-DD)
    "lead_days",              # days from retrieval to forecast_date
    # ── Temperature ───────────────────────────────────────────────────────────
    "high_temp_f",            # forecast daytime high °F
    "low_temp_f",             # forecast overnight low °F
    "apparent_temp_max_f",    # max apparent temp (heat index) °F
    "apparent_temp_min_f",    # min apparent temp (wind chill) °F
    # ── Wind ──────────────────────────────────────────────────────────────────
    "max_wind_speed_mph",     # max sustained wind speed mph
    "max_wind_gust_mph",      # max wind gust mph
    "wind_direction",         # daytime wind direction (cardinal)
    # ── Moisture ──────────────────────────────────────────────────────────────
    "avg_dewpoint_f",         # average dewpoint °F for the day
    "avg_relative_humidity",  # average relative humidity %
    # ── Precipitation ─────────────────────────────────────────────────────────
    "precip_prob_pct",        # max probability of precipitation %
    "precip_amount_in",       # quantitative precipitation forecast inches
    "prob_thunder_pct",       # max probability of thunder %
    # ── Sky / Visibility ──────────────────────────────────────────────────────
    "sky_cover_pct",          # average sky cover %
    "min_visibility_miles",   # minimum visibility miles (dust storm detection)
    # ── Conditions ────────────────────────────────────────────────────────────
    "day_short_forecast",     # NWS short text — daytime
    "night_short_forecast",   # NWS short text — overnight
    "weather_summary",        # structured condition codes, pipe-separated
                              # format: "coverage:weather:intensity"
                              # e.g. "slight_chance:thunderstorms:|chance:dust_storm:"
]


# ── Unit conversion helpers ────────────────────────────────────────────────────
def kph_to_mph(kph):
    return round(kph * 0.621371, 1) if kph is not None else None

def mm_to_in(mm):
    return round(mm * 0.0393701, 3) if mm is not None else None

def c_to_f(c):
    return round(c * 9/5 + 32, 1) if c is not None else None

def m_to_miles(m):
    return round(m / 1609.344, 2) if m is not None else None


# ── ISO 8601 duration expansion ───────────────────────────────────────────────
def parse_iso_duration_days(duration_str):
    """
    Parse an ISO 8601 duration and return the number of calendar days spanned.
    e.g. PT6H=1, P1D=1, P2DT6H=3 (ceiling: 54h touches 3 dates)
    """
    if not duration_str:
        return 1
    days  = int(re.search(r'(\d+)D', duration_str).group(1)) if re.search(r'(\d+)D', duration_str) else 0
    hours = int(re.search(r'(\d+)H', duration_str).group(1)) if re.search(r'(\d+)H', duration_str) else 0
    return max(1, (days * 24 + hours + 23) // 24)


def expand_gridpoint_values(values):
    """
    Expand gridpoint time-series into {date_str: [values]} dict.
    Uses start_str[:10] for date extraction, matching how the dashboard
    and period forecast both handle dates.
    """
    buckets = {}
    for v in values:
        valid_time = v.get("validTime", "")
        val = v.get("value")
        if val is None:
            continue
        start_str, duration_str = (valid_time.split("/", 1) + ["PT1H"])[:2]
        try:
            start_date = date.fromisoformat(start_str[:10])
        except ValueError:
            continue
        for offset in range(parse_iso_duration_days(duration_str)):
            buckets.setdefault(str(start_date + timedelta(days=offset)), []).append(val)
    return buckets


def daily_avg(values):
    b = expand_gridpoint_values(values)
    return {d: sum(v) / len(v) for d, v in b.items()}

def daily_max(values):
    b = expand_gridpoint_values(values)
    return {d: max(v) for d, v in b.items()}

def daily_min(values):
    b = expand_gridpoint_values(values)
    return {d: min(v) for d, v in b.items()}

def daily_sum(values):
    b = expand_gridpoint_values(values)
    return {d: sum(v) for d, v in b.items()}


def parse_weather_summary(weather_values, target_date):
    """
    Extract structured weather condition codes for a given date from the
    gridpoint 'weather' layer. Each value is a list of condition objects:
      {"coverage": "slight_chance", "weather": "thunderstorms", "intensity": ""}
    Returns a pipe-separated string of "coverage:weather:intensity" tuples,
    or empty string if none. Null/none entries are skipped.
    e.g. "slight_chance:thunderstorms:|chance:dust_storm:"
    """
    seen = []
    for v in weather_values:
        valid_time = v.get("validTime", "")
        start_str = valid_time.split("/")[0]
        if start_str[:10] != target_date:
            continue
        conditions = v.get("value") or []
        for c in conditions:
            if not isinstance(c, dict):
                continue
            wx = c.get("weather") or ""
            if not wx or wx == "nothing":
                continue
            cov = c.get("coverage") or ""
            ins = c.get("intensity") or ""
            entry = f"{cov}:{wx}:{ins}"
            if entry not in seen:
                seen.append(entry)
    return "|".join(seen)


# ── NWS API calls ──────────────────────────────────────────────────────────────
def nws_get(url, retries=3, timeout=20):
    """GET with retry on timeout or 5xx. Waits 10s between attempts."""
    for attempt in range(1, retries + 1):
        try:
            print(f"    GET {url}  (attempt {attempt}/{retries})")
            resp = requests.get(url, headers=HEADERS, timeout=timeout)
            resp.raise_for_status()
            return resp
        except (requests.exceptions.Timeout, requests.exceptions.ConnectionError):
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
    props = nws_get(f"https://api.weather.gov/points/{LAT},{LON}").json()["properties"]
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

    # ── Build daily rows from daytime/overnight period pairs ──────────────────
    daily = []
    for i, p in enumerate(periods):
        if not p.get("isDaytime"):
            continue
        night = periods[i + 1] if i + 1 < len(periods) else None

        # Period-level PoP fallback (max of day + night)
        day_pop   = (p.get("probabilityOfPrecipitation") or {}).get("value")
        night_pop = ((night or {}).get("probabilityOfPrecipitation") or {}).get("value")
        candidates = [v for v in (day_pop, night_pop) if v is not None]
        period_pop = max(candidates) if candidates else None

        daily.append({
            "date":                  p["startTime"].split("T")[0],
            "high_temp_f":           p.get("temperature"),
            "low_temp_f":            night.get("temperature") if night else None,
            "wind_direction":        p.get("windDirection", ""),
            "day_short_forecast":    p.get("shortForecast", ""),
            "night_short_forecast":  night.get("shortForecast", "") if night else "",
            # filled from gridpoint below:
            "max_wind_speed_mph":    None,
            "max_wind_gust_mph":     None,
            "avg_dewpoint_f":        None,
            "avg_relative_humidity": None,
            "apparent_temp_max_f":   None,
            "apparent_temp_min_f":   None,
            "precip_prob_pct":       None,
            "_period_pop":           period_pop,
            "precip_amount_in":      None,
            "prob_thunder_pct":      None,
            "sky_cover_pct":         None,
            "min_visibility_miles":  None,
            "weather_summary":       "",
        })

    # ── Extract all gridpoint layers ──────────────────────────────────────────
    sky_cover       = daily_avg(gp.get("skyCover",                   {}).get("values", []))
    wind_speed      = daily_max(gp.get("windSpeed",                  {}).get("values", []))
    wind_gust       = daily_max(gp.get("windGust",                   {}).get("values", []))
    qpf             = daily_sum(gp.get("quantitativePrecipitation",  {}).get("values", []))
    pop             = daily_max(gp.get("probabilityOfPrecipitation", {}).get("values", []))
    dewpoint        = daily_avg(gp.get("dewpoint",                   {}).get("values", []))
    rel_humidity    = daily_avg(gp.get("relativeHumidity",           {}).get("values", []))
    apparent_max    = daily_max(gp.get("apparentTemperature",        {}).get("values", []))
    apparent_min    = daily_min(gp.get("apparentTemperature",        {}).get("values", []))
    prob_thunder    = daily_max(gp.get("probabilityOfThunder",       {}).get("values", []))
    visibility      = daily_min(gp.get("visibility",                 {}).get("values", []))
    weather_values  = gp.get("weather", {}).get("values", [])

    # ── Augment each day row ──────────────────────────────────────────────────
    for d in daily:
        dt = d["date"]

        if dt in sky_cover:    d["sky_cover_pct"]        = round(sky_cover[dt])
        if dt in wind_speed:   d["max_wind_speed_mph"]   = kph_to_mph(wind_speed[dt])
        if dt in wind_gust:    d["max_wind_gust_mph"]    = kph_to_mph(wind_gust[dt])
        if dt in qpf:          d["precip_amount_in"]     = mm_to_in(qpf[dt])
        if dt in dewpoint:     d["avg_dewpoint_f"]       = c_to_f(dewpoint[dt])
        if dt in rel_humidity: d["avg_relative_humidity"]= round(rel_humidity[dt])
        if dt in apparent_max: d["apparent_temp_max_f"]  = c_to_f(apparent_max[dt])
        if dt in apparent_min: d["apparent_temp_min_f"]  = c_to_f(apparent_min[dt])
        if dt in prob_thunder: d["prob_thunder_pct"]     = round(prob_thunder[dt])
        if dt in visibility:   d["min_visibility_miles"] = m_to_miles(visibility[dt])

        # PoP: max of gridpoint and period-level values
        gp_pop     = round(pop[dt]) if dt in pop else None
        per_pop    = round(d["_period_pop"]) if d.get("_period_pop") is not None else None
        pop_cands  = [v for v in (gp_pop, per_pop) if v is not None]
        d["precip_prob_pct"] = max(pop_cands) if pop_cands else None

        # Weather summary: structured condition codes for this date
        d["weather_summary"] = parse_weather_summary(weather_values, dt)

    return {
        "forecast_updated_at": updated_at,
        "retrieved_at":        retrieved_at,
        "days":                daily,
    }


# ── Deduplication ─────────────────────────────────────────────────────────────
def load_last_update():
    """Read forecast_updated_at from the last line of the CSV."""
    if not os.path.exists(OUTPUT_FILE):
        return None
    with open(OUTPUT_FILE, "rb") as f:
        f.seek(0, 2)
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
        return None
    try:
        first_field = next(csv.reader([last_line]))
        return first_field[0].strip() if first_field else None
    except Exception:
        return None


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
        print(f"  Header missing or outdated — rewriting header…")
        with open(OUTPUT_FILE, "r") as f:
            existing = f.read()
        with open(OUTPUT_FILE, "w", newline="") as f:
            csv.DictWriter(f, fieldnames=CSV_COLUMNS).writeheader()
            # Write existing data rows only (skip any old header line)
            for line in existing.splitlines():
                if line and not line.startswith("forecast_updated_at"):
                    f.write(line + "\n")
        print(f"  Header updated in {OUTPUT_FILE}")


def append_snapshot(forecast):
    retrieved      = forecast["retrieved_at"]
    updated        = forecast["forecast_updated_at"]
    retrieved_date = date.fromisoformat(retrieved[:10])

    def val(d, key):
        v = d.get(key)
        return v if v is not None else ""

    with open(OUTPUT_FILE, "a", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_COLUMNS)
        for d in forecast["days"]:
            d.pop("_period_pop", None)
            try:
                lead = (date.fromisoformat(d["date"]) - retrieved_date).days
            except Exception:
                lead = ""
            writer.writerow({
                "forecast_updated_at":   updated,
                "retrieved_at":          retrieved,
                "forecast_date":         d["date"],
                "lead_days":             lead,
                "high_temp_f":           val(d, "high_temp_f"),
                "low_temp_f":            val(d, "low_temp_f"),
                "apparent_temp_max_f":   val(d, "apparent_temp_max_f"),
                "apparent_temp_min_f":   val(d, "apparent_temp_min_f"),
                "max_wind_speed_mph":    val(d, "max_wind_speed_mph"),
                "max_wind_gust_mph":     val(d, "max_wind_gust_mph"),
                "wind_direction":        d.get("wind_direction") or "",
                "avg_dewpoint_f":        val(d, "avg_dewpoint_f"),
                "avg_relative_humidity": val(d, "avg_relative_humidity"),
                "precip_prob_pct":       val(d, "precip_prob_pct"),
                "precip_amount_in":      val(d, "precip_amount_in"),
                "prob_thunder_pct":      val(d, "prob_thunder_pct"),
                "sky_cover_pct":         val(d, "sky_cover_pct"),
                "min_visibility_miles":  val(d, "min_visibility_miles"),
                "day_short_forecast":    d.get("day_short_forecast") or "",
                "night_short_forecast":  d.get("night_short_forecast") or "",
                "weather_summary":       d.get("weather_summary") or "",
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

    # Summary table
    print(f"\n  {'Date':<12} {'Hi':>4} {'Lo':>4} {'ApHi':>5} {'ApLo':>5} "
          f"{'Wind':>5} {'Gust':>5} {'PoP':>4} {'Thnd':>4} "
          f"{'Dew':>5} {'RH':>4} {'Vis':>6} {'Sky':>4}  Conditions")
    print(f"  {'-'*100}")
    for d in forecast["days"]:
        print(
            f"  {d['date']:<12}"
            f" {str(d.get('high_temp_f')          or '—'):>4}"
            f" {str(d.get('low_temp_f')            or '—'):>4}"
            f" {str(d.get('apparent_temp_max_f')   or '—'):>5}"
            f" {str(d.get('apparent_temp_min_f')   or '—'):>5}"
            f" {str(d.get('max_wind_speed_mph')    or '—'):>5}"
            f" {str(d.get('max_wind_gust_mph')     or '—'):>5}"
            f" {str(d.get('precip_prob_pct')       if d.get('precip_prob_pct')  is not None else '—'):>4}"
            f" {str(d.get('prob_thunder_pct')      if d.get('prob_thunder_pct') is not None else '—'):>4}"
            f" {str(d.get('avg_dewpoint_f')        or '—'):>5}"
            f" {str(d.get('avg_relative_humidity') or '—'):>4}"
            f" {str(d.get('min_visibility_miles')  or '—'):>6}"
            f" {str(d.get('sky_cover_pct')         if d.get('sky_cover_pct')   is not None else '—'):>4}"
            f"  {d.get('day_short_forecast', '')}"
        )
        if d.get("weather_summary"):
            print(f"  {'':12}  wx: {d['weather_summary']}")

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
