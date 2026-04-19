NWS Forecast Collector — KLAS (36.073861, -115.152917)
Output: data/forecasts.csv
  Last saved forecast version : (none)
  Resolving NWS grid…
    GET https://api.weather.gov/points/36.073861,-115.152917  (attempt 1/3)
  Fetching forecast + gridpoint data…
    GET https://api.weather.gov/gridpoints/VEF/122,94/forecast  (attempt 1/3)
    GET https://api.weather.gov/gridpoints/VEF/122,94  (attempt 1/3)
    period PoP  2026-04-19  day=0  night=1
    period PoP  2026-04-20  day=0  night=0
    period PoP  2026-04-21  day=5  night=5
    period PoP  2026-04-22  day=0  night=1
    period PoP  2026-04-23  day=1  night=0
    period PoP  2026-04-24  day=0  night=0
    period PoP  2026-04-25  day=1  night=1
  NWS forecast updated at     : 2026-04-19T17:58:16+00:00
  Retrieved at                : 2026-04-19T19:15:52Z
  Days in forecast            : 7

  NEW — wrote 7 rows to data/forecasts.csv

  Date          High   Low  Wind  Gust  PoP%  Sky%  Conditions
  ------------------------------------------------------------------------------
  2026-04-19      87    62   6.9  12.7     0    61  Partly Sunny
  2026-04-20      91    62  16.1  25.3     1    21  Sunny
  2026-04-21      88    56  25.3  35.7     0    13  Sunny
  2026-04-22      76    54  23.0  32.2     2     8  Sunny
  2026-04-23      80    57  10.4  19.6     0    12  Sunny
  2026-04-24      82    59  10.4  18.4     0    31  Sunny
  2026-04-25      83    60  10.4  17.3     1    25  Mostly Sunny

  Done.
