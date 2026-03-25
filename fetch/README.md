# EP Tracker — Data Pipeline

This folder contains the Python script that pulls data from the
**European Parliament Open Data API v2** and writes clean JSON files
into Jekyll's `_data/` directory.

## What gets fetched

| File | Source endpoint | Content |
|---|---|---|
| `_data/meps.json` | `/members` | All sitting MEPs (term 10) |
| `_data/meps/<id>.json` | `/members/<id>` | One file per MEP (detail page) |
| `_data/committees.json` | `/bodies?type=COMMITTEE` | All committees |
| `_data/sessions.json` | `/plenary-sessions` | Past 6 months + upcoming |
| `_data/votes.json` | `/vote-results` | Roll-call votes (past 6 months) |
| `_data/documents.json` | `/adopted-texts` + `/plenary-session-documents` | Tabled & adopted texts |
| `_data/questions.json` | `/parliamentary-questions` | MEP questions (past 3 months) |
| `_data/meta.json` | — | Last-updated timestamp & counts |

## Running locally

```bash
# Install dependencies
pip install -r fetch/requirements.txt

# Run the fetch (takes ~5–10 minutes due to per-MEP API calls)
python fetch/fetch_ep_data.py
```

You'll see structured logs:

```
2025-03-25 06:30:01  INFO     Fetching MEP list (term 10)…
2025-03-25 06:30:04  INFO       → 720 MEPs retrieved
2025-03-25 06:30:04  INFO       … processed 50/720 MEPs
...
2025-03-25 06:42:11  INFO     All datasets fetched successfully.
```

## Automated updates via GitHub Actions

The workflow in `.github/workflows/fetch-data.yml` runs every day at
06:30 UTC. It:

1. Runs `fetch_ep_data.py`
2. Commits any changed JSON files with a descriptive message
3. Rebuilds the Jekyll site
4. Deploys to GitHub Pages

You can also trigger it manually from the **Actions** tab in GitHub.

## API notes

- **Base URL**: `https://data.europarl.europa.eu/api/v2`
- **Auth**: none required (public API)
- **Rate limiting**: the script waits 0.3 s between requests; EP's
  API has not published explicit rate limits but this keeps things polite
- **MEP detail calls**: fetching full contact/social/committee data
  requires one API call per MEP (~720 calls). This is the slow part.
  Consider caching unchanged MEPs to speed up daily runs.
- **Format**: the API returns JSON-LD; the script flattens it to
  plain dicts for easy use in Liquid templates

## Extending the pipeline

To add a new data source, add a `fetch_<thing>()` function following
the same pattern and call it from `main()`. The output file will
automatically be available as `site.data.<thing>` in Jekyll templates.
