#!/usr/bin/env python3
"""
METAR Backfill Script
=====================
One-time script to seed observations_metar.csv with the last 15 days
of METAR history from the AWC API. Run once via GitHub Actions, then delete.

The AWC API keeps exactly 15 days of METAR history.
"""

import requests, csv, os, sys, time
from datetime import datetime, timezone

# Import everything from the collector
exec(open('collect_metar.py').read().split('if __name__')[0])

def main():
    retrieved_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    print(f"METAR Backfill — {STATION}  (15 days)")
    print(f"Output: {OUTPUT_FILE}")

    ensure_output_file()
    existing = load_existing_timestamps()
    print(f"  Existing observations: {len(existing)}")

    print(f"  Fetching 15 days of METARs from AWC…")
    observations = fetch_metars(hours=360)   # 15 days = 360 hours
    print(f"  Got {len(observations)} observations")

    # Sort oldest first for clean CSV ordering
    observations.sort(key=lambda o: o.get("obsTime") or 0)

    written = skipped = 0
    with open(OUTPUT_FILE, "a", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_COLUMNS)
        for ob in observations:
            row = parse_observation(ob, retrieved_at)
            ts  = row.get("obs_time_utc", "")
            if not ts: continue
            if ts in existing:
                skipped += 1
                continue
            writer.writerow(row)
            existing.add(ts)
            written += 1
            wx = f"  {row['wx_string']}" if row.get('wx_string') else ""
            print(f"  OK {ts}  T={row['temp_f']}°F  Wnd={row['wind_speed_mph']}mph  "
                  f"Vis={row['visibility_sm']}SM  {row['flight_category']}{wx}")

    print(f"\n  Written : {written}")
    print(f"  Skipped : {skipped}")
    print(f"  Done.")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        import traceback; traceback.print_exc()
        sys.exit(1)
