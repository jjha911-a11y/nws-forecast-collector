# NWS Forecast Collector — Setup Guide
## Complete Step-by-Step for GitHub Actions

This guide assumes you have never used GitHub before. Every step is covered.

---

## What You'll End Up With

A GitHub repository that:
- Runs automatically 4× per day (every 6 hours)
- Fetches the current NWS hourly forecast for KLAS
- Appends it to a growing CSV file (`data/forecasts.csv`)
- Commits that file back to the repo automatically
- Costs nothing. Runs even when your computer is off.

---

## Part 1 — Create a GitHub Account (skip if you already have one)

1. Open your browser and go to **https://github.com**
2. Click **Sign up** in the top-right corner
3. Enter your email address, create a password, and choose a username
   - The username will appear in your repo URL, e.g. `github.com/yourusername`
   - Keep it simple — something like your name or callsign is fine
4. Complete the verification puzzle and click **Create account**
5. Check your email for a verification code and enter it
6. On the "Welcome" survey, you can click **Skip personalization** at the bottom

---

## Part 2 — Create a New Repository

A "repository" (repo) is just a folder on GitHub that stores your files and tracks changes.

1. After signing in, click the **+** icon in the top-right corner
2. Select **New repository**
3. Fill in the form:
   - **Repository name:** `nws-forecast-collector` (no spaces — use hyphens)
   - **Description:** `NWS hourly forecast collector for KLAS verification`
   - **Visibility:** Select **Private** (your data stays yours)
   - Check the box: **Add a README file** ← important, do this
   - Leave everything else at defaults
4. Click the green **Create repository** button

You now have a repo. You'll see a page with a single file called `README.md`.

---

## Part 3 — Create the Folder Structure

GitHub's web interface lets you create files and folders directly. You need to create:
```
your-repo/
├── collect_forecast.py          ← the Python script
├── data/
│   └── .gitkeep                 ← placeholder so the folder exists
└── .github/
    └── workflows/
        └── collect_forecast.yml ← the automation schedule
```

### 3a — Create the `data` folder placeholder

GitHub won't let you create an empty folder, so we put a tiny placeholder file in it.

1. On your repo page, click **Add file → Create new file**
2. In the filename box at the top, type: `data/.gitkeep`
   - When you type the `/`, GitHub automatically creates the folder
3. Leave the file content completely blank
4. Scroll down to **Commit new file**
   - Leave the commit message as-is ("Create data/.gitkeep")
5. Click the green **Commit new file** button

### 3b — Create the Python collector script

1. Click **Add file → Create new file**
2. In the filename box, type: `collect_forecast.py`
3. Open the file **collect_forecast.py** that was provided with this guide
4. Select all of its contents (Ctrl+A / Cmd+A) and copy (Ctrl+C / Cmd+C)
5. Click inside the large text area in GitHub (under "Edit new file")
6. Paste (Ctrl+V / Cmd+V)
7. Scroll down and click **Commit new file**

### 3c — Create the GitHub Actions workflow

This is the file that tells GitHub *when* to run your script automatically.

1. Click **Add file → Create new file**
2. In the filename box, type: `.github/workflows/collect_forecast.yml`
   - Type it exactly — the leading dot matters
   - GitHub will create both folders as you type the slashes
3. Open the file **collect_forecast.yml** provided with this guide
4. Copy all of its contents and paste into the GitHub text area
5. Click **Commit new file**

---

## Part 4 — Enable GitHub Actions

GitHub Actions may need to be enabled for new repos.

1. In your repo, click the **Actions** tab (top menu bar)
2. If you see a yellow banner saying "Workflows aren't being run on this forked repository"
   or a button saying **I understand my workflows, go ahead and enable them** — click it
3. If you see your workflow listed as "Collect NWS Forecast" — you're already enabled
4. If Actions appears empty, that's normal — it hasn't run yet

---

## Part 5 — Test It Manually Right Now

Don't wait 6 hours to find out if it works. Trigger it manually:

1. Click the **Actions** tab
2. In the left sidebar, click **Collect NWS Forecast**
3. On the right side, click the **Run workflow** dropdown button
4. Leave "Branch: main" selected
5. Click the green **Run workflow** button
6. The page will refresh. You'll see a yellow circle appear — that means it's running.
7. Click on the running workflow to watch it in real time
8. You'll see each step expand as it completes:
   - ✅ Checkout repository
   - ✅ Set up Python
   - ✅ Install dependencies
   - ✅ Collect NWS forecast
   - ✅ Commit updated forecast data
9. The whole run takes about 30–45 seconds

### Verifying the data was collected

1. Click the **<> Code** tab to go back to your file list
2. Click the **data** folder
3. You should now see **forecasts.csv**
4. Click it — you'll see rows of forecast data with columns like:
   `captured_at, forecast_valid_time, temp_f, dewpoint_c, ...`

If you see data, **it's working**. 🎉

---

## Part 6 — Verify the Schedule Is Running

After the first scheduled run (within 6 hours of setup):

1. Go to the **Actions** tab
2. You'll see a list of completed workflow runs with timestamps
3. They should appear approximately every 6 hours
4. Green checkmark = success, Red X = something failed (see below)

**Note:** GitHub may delay the first few scheduled runs on new repos by up to 15 minutes. This is normal.

---

## Part 7 — Accessing Your Data

### Viewing it on GitHub
- Go to `data/forecasts.csv` in your repo
- GitHub renders it as a table automatically for small files
- For large files, use the **Raw** button to see the plain text

### Downloading it
- Navigate to `data/forecasts.csv`
- Click the **Raw** button
- In your browser: File → Save As (or Ctrl+S / Cmd+S)
- This gives you the CSV to open in Excel, load into your dashboard, etc.

### The raw URL (for your dashboard to fetch directly)
Your CSV has a permanent URL you can use to fetch it programmatically:
```
https://raw.githubusercontent.com/YOURUSERNAME/nws-forecast-collector/main/data/forecasts.csv
```
Replace `YOURUSERNAME` with your actual GitHub username.

---

## Troubleshooting

### The workflow shows a red X

1. Click on the failed run
2. Click on the failed step (it will show a red X)
3. Read the error message — common causes:

**"HTTP Error 503" or "Connection timeout"**
The NWS API was temporarily down. This happens occasionally.
The next scheduled run will succeed. No data is lost — you just miss that snapshot.

**"Permission denied" on the git push step**
The repo permissions weren't set correctly. Fix:
1. Go to repo **Settings → Actions → General**
2. Scroll to "Workflow permissions"
3. Select **Read and write permissions**
4. Click **Save**
Then re-run the workflow manually.

**"No module named requests"**
The pip install step failed. Check that your workflow YAML was copied exactly —
look for any missing lines around the `pip install requests` step.

### The workflow never runs on schedule

GitHub occasionally pauses scheduled workflows on repos that have had no
activity for 60 days. To re-enable: go to Actions tab, find the workflow,
and click **Enable workflow**. Making any small commit (like editing README.md)
also resets the inactivity timer.

---

## What the Data Looks Like

Each time the script runs, it appends ~156 rows (NWS provides 6.5 days of
hourly forecasts). After a week of 4× daily collection, you'll have ~4,400 rows.
After a month, ~19,000 rows. The CSV stays manageable for well over a year.

**Column reference:**

| Column | Description | Example |
|--------|-------------|---------|
| `captured_at` | When THIS run collected the data (UTC) | `2026-04-18T12:15:00Z` |
| `forecast_valid_time` | The hour this forecast covers (UTC) | `2026-04-19T06:00:00-07:00` |
| `temp_f` | Forecast temperature | `78` |
| `dewpoint_c` | Forecast dewpoint in Celsius | `5.6` |
| `wind_speed_mph` | Forecast wind speed | `12.5` |
| `wind_direction` | Forecast wind direction | `SW` |
| `relative_humidity` | Forecast relative humidity % | `18` |
| `prob_precip` | Probability of precipitation % | `0` |
| `short_forecast` | NWS text description | `Sunny` |

**The key verification field is `captured_at` vs `forecast_valid_time`.**
The difference between those two timestamps tells you the *lead time* of the
forecast — e.g., a forecast captured 18 hours before its valid time is an
18-hour forecast. This lets you analyze how forecast accuracy degrades with
increasing lead time.

---

## Next Steps (future work)

Once data has been accumulating for a few weeks, the next phase is building
the verification comparison — joining these forecast rows against METAR
observations at KLAS for the same valid times. That's a separate script/tool
we can build together once the collection pipeline is established.
