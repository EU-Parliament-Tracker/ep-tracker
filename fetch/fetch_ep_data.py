#!/usr/bin/env python3
"""
EP Tracker — European Parliament data pipeline
Fetches data from the EP Open Data API v2 and writes clean JSON
files into the Jekyll _data/ directory.

Run manually:    python fetch/fetch_ep_data.py
Run via cron:    0 6 * * * cd /path/to/repo && python fetch/fetch_ep_data.py
Run via CI:      see .github/workflows/fetch-data.yml

Outputs (written to ../_data/):
  meps.json                  — all current MEPs, enriched
  meps/<id>.json             — one file per MEP (for individual pages)
  committees.json            — all committees with membership lists
  sessions.json              — plenary sessions (past 6 months + upcoming)
  votes.json                 — recent roll-call votes
  documents.json             — recently tabled / adopted texts
  questions.json             — recent parliamentary questions
  meta.json                  — last-updated timestamp + stats
"""

import json
import logging
import os
import sys
import time
from datetime import date, timedelta
from pathlib import Path

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

EP_API_BASE = "https://data.europarl.europa.eu/api/v2"
EP_WEBSITE   = "https://www.europarl.europa.eu"

# Current parliamentary term (10th, started July 2024)
CURRENT_TERM = 10

# How far back to fetch time-series data
LOOKBACK_DAYS = 180   # sessions, votes, documents
QUESTIONS_LOOKBACK_DAYS = 90

# Max items per category (keeps file sizes reasonable for Jekyll)
MAX_VOTES     = 500
MAX_DOCUMENTS = 300
MAX_QUESTIONS = 300

# Output directory — relative to this script's location
OUTPUT_DIR = Path(__file__).parent.parent / "_data"
MEPS_DIR   = OUTPUT_DIR / "meps"

# Request settings
TIMEOUT   = 30          # seconds per request
PAGE_SIZE = 100         # items per paginated request
RATE_LIMIT = 0.3        # seconds between requests (be polite)

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("ep-fetch")

# ---------------------------------------------------------------------------
# HTTP session with retry logic
# ---------------------------------------------------------------------------

def make_session() -> requests.Session:
    session = requests.Session()
    retry = Retry(
        total=4,
        backoff_factor=1.5,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.headers.update({
        "Accept": "application/json",
        "User-Agent": "EP-Tracker/1.0 (https://github.com/your-org/ep-tracker)",
    })
    return session


SESSION = make_session()


def get(endpoint: str, params: dict = None) -> dict | None:
    """GET a single page from the EP API."""
    url = f"{EP_API_BASE}/{endpoint.lstrip('/')}"
    try:
        r = SESSION.get(url, params=params, timeout=TIMEOUT)
        r.raise_for_status()
        time.sleep(RATE_LIMIT)
        return r.json()
    except requests.HTTPError as e:
        log.warning("HTTP %s — %s", e.response.status_code, url)
        return None
    except Exception as e:
        log.warning("Request failed — %s — %s", url, e)
        return None


def get_all(endpoint: str, params: dict = None, max_items: int = 9999) -> list:
    """Paginate through all pages of a collection endpoint."""
    params = dict(params or {})
    params.setdefault("limit", PAGE_SIZE)
    params.setdefault("offset", 0)

    results = []
    while len(results) < max_items:
        data = get(endpoint, params)
        if not data:
            break
        items = data.get("data", [])
        if not items:
            break
        results.extend(items)
        log.debug("  … fetched %d/%d items from %s", len(results), max_items, endpoint)
        # EP API uses offset pagination
        if len(items) < PAGE_SIZE:
            break
        params["offset"] += PAGE_SIZE

    return results[:max_items]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def write_json(path: Path, data, indent: int = 2):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, ensure_ascii=False, indent=indent), encoding="utf-8")
    log.info("  wrote %s (%d bytes)", path.relative_to(OUTPUT_DIR.parent), path.stat().st_size)


def ep_profile_url(mep_id: str) -> str:
    return f"{EP_WEBSITE}/meps/en/{mep_id}"


def safe_str(value) -> str:
    if isinstance(value, list):
        return value[0] if value else ""
    return value or ""


def date_str(days_ago: int = 0) -> str:
    return (date.today() - timedelta(days=days_ago)).isoformat()


# ---------------------------------------------------------------------------
# MEP fetching
# ---------------------------------------------------------------------------

def fetch_meps() -> list[dict]:
    """
    Fetch all MEPs for the current term and enrich each record with:
      - committee memberships
      - contact details (email, website, social media)
      - high-res photo URL
    Returns a list of clean dicts ready for Jekyll.
    """
    log.info("Fetching MEP list (term %d)…", CURRENT_TERM)
    raw = get_all(
        "members",
        params={
            "parliamentary-term": CURRENT_TERM,
            "status": "current",   # only sitting MEPs
        },
    )
    log.info("  → %d MEPs retrieved", len(raw))

    meps = []
    for i, m in enumerate(raw):
        mep_id = m.get("identifier", "")
        if not mep_id:
            continue

        # Fetch full detail for each MEP (contact info, social, committees)
        detail = get(f"members/{mep_id}") or {}
        d = detail.get("data", {}) if "data" in detail else m

        mep = _parse_mep(mep_id, d)
        meps.append(mep)

        # Also write individual file for the MEP's own page
        write_json(MEPS_DIR / f"{mep_id}.json", mep, indent=2)

        if (i + 1) % 50 == 0:
            log.info("  … processed %d/%d MEPs", i + 1, len(raw))

    meps.sort(key=lambda x: x.get("last_name", ""))
    return meps


def _parse_mep(mep_id: str, d: dict) -> dict:
    """Normalise a raw MEP record into a clean flat dict."""

    # Name
    given  = safe_str(d.get("givenName",  d.get("label", "")))
    family = safe_str(d.get("familyName", ""))
    full   = safe_str(d.get("label",      f"{given} {family}").strip())

    # Political group
    group_ref  = (d.get("memberOf") or [{}])
    group_name = ""
    group_abbr = ""
    for g in group_ref:
        if isinstance(g, dict) and "politicalGroup" in str(g.get("@type", "")):
            group_name = safe_str(g.get("label", ""))
            group_abbr = safe_str(g.get("notation", ""))
            break

    # Country & national party
    country      = safe_str(d.get("countryOfRepresentation", ""))
    national_party = safe_str(d.get("nationalPoliticalGroup", ""))

    # Contact details
    contact   = d.get("contactPoint", {}) if isinstance(d.get("contactPoint"), dict) else {}
    email     = safe_str(contact.get("email", d.get("email", "")))
    website   = safe_str(contact.get("url",   d.get("url",   "")))
    phone     = safe_str(contact.get("telephone", ""))

    # Social media
    social  = {}
    for link in d.get("homepage", []):
        url_str = str(link)
        if "twitter.com" in url_str or "x.com" in url_str:
            social["twitter"] = url_str
        elif "linkedin.com" in url_str:
            social["linkedin"] = url_str
        elif "facebook.com" in url_str:
            social["facebook"] = url_str
        elif "instagram.com" in url_str:
            social["instagram"] = url_str

    # Photo
    photo_url = (
        d.get("img")
        or f"{EP_WEBSITE}/meps/en/{mep_id}/home/widget/photo.jpg"
    )

    # Committees (from hasMembership list)
    committees = []
    for membership in d.get("hasMembership", []):
        if not isinstance(membership, dict):
            continue
        role      = safe_str(membership.get("role", ""))
        org       = membership.get("organization", {}) if isinstance(membership.get("organization"), dict) else {}
        org_label = safe_str(org.get("label", ""))
        org_id    = safe_str(org.get("identifier", ""))
        org_type  = safe_str(org.get("@type", ""))
        if "Committee" in org_type or "COMM" in org_id.upper():
            committees.append({
                "id":    org_id,
                "name":  org_label,
                "role":  role,
            })

    return {
        "id":              mep_id,
        "full_name":       full,
        "given_name":      given,
        "last_name":       family,
        "country":         country,
        "national_party":  national_party,
        "group_name":      group_name,
        "group_abbr":      group_abbr,
        "email":           email,
        "phone":           phone,
        "website":         website,
        "social":          social,
        "photo_url":       photo_url,
        "ep_profile_url":  ep_profile_url(mep_id),
        "committees":      committees,
    }


# ---------------------------------------------------------------------------
# Committees
# ---------------------------------------------------------------------------

def fetch_committees() -> list[dict]:
    log.info("Fetching committees…")
    raw = get_all("bodies", params={"type": "COMMITTEE", "parliamentary-term": CURRENT_TERM})
    log.info("  → %d committees retrieved", len(raw))

    committees = []
    for c in raw:
        body_id = safe_str(c.get("identifier", ""))
        committees.append({
            "id":           body_id,
            "abbreviation": safe_str(c.get("notation", body_id)),
            "name":         safe_str(c.get("label", "")),
            "ep_url":       f"{EP_WEBSITE}/committees/en/{body_id.lower()}/home",
        })

    committees.sort(key=lambda x: x.get("abbreviation", ""))
    return committees


# ---------------------------------------------------------------------------
# Plenary sessions
# ---------------------------------------------------------------------------

def fetch_sessions() -> list[dict]:
    log.info("Fetching plenary sessions…")
    start = date_str(LOOKBACK_DAYS)

    raw = get_all(
        "plenary-sessions",
        params={"start-date-gte": start},
    )
    log.info("  → %d sessions retrieved", len(raw))

    sessions = []
    for s in raw:
        sessions.append({
            "id":       safe_str(s.get("identifier", "")),
            "label":    safe_str(s.get("label", "")),
            "start":    safe_str(s.get("hadActivity", {}).get("startDate", "")) if isinstance(s.get("hadActivity"), dict) else "",
            "end":      safe_str(s.get("hadActivity", {}).get("endDate", "")) if isinstance(s.get("hadActivity"), dict) else "",
            "location": safe_str(s.get("place", "")),
            "ep_url":   safe_str(s.get("seeAlso", "")),
        })

    sessions.sort(key=lambda x: x.get("start", ""), reverse=True)
    return sessions


# ---------------------------------------------------------------------------
# Votes
# ---------------------------------------------------------------------------

def fetch_votes() -> list[dict]:
    log.info("Fetching roll-call votes…")
    start = date_str(LOOKBACK_DAYS)

    raw = get_all(
        "vote-results",
        params={"start-date-gte": start},
        max_items=MAX_VOTES,
    )
    log.info("  → %d vote records retrieved", len(raw))

    votes = []
    for v in raw:
        votes.append({
            "id":          safe_str(v.get("identifier", "")),
            "date":        safe_str(v.get("date", "")),
            "title":       safe_str(v.get("label", "")),
            "result":      safe_str(v.get("result", "")),
            "for":         v.get("numberOfVotesFor", 0),
            "against":     v.get("numberOfVotesAgainst", 0),
            "abstention":  v.get("numberOfAbstentions", 0),
            "subject":     safe_str(v.get("subject", "")),
            "ep_ref":      safe_str(v.get("notation", "")),
            "ep_url":      safe_str(v.get("seeAlso", "")),
        })

    votes.sort(key=lambda x: x.get("date", ""), reverse=True)
    return votes


# ---------------------------------------------------------------------------
# Documents (tabled + adopted texts)
# ---------------------------------------------------------------------------

def fetch_documents() -> list[dict]:
    log.info("Fetching legislative documents…")
    start = date_str(LOOKBACK_DAYS)

    # Adopted texts
    adopted = get_all(
        "adopted-texts",
        params={"start-date-gte": start},
        max_items=MAX_DOCUMENTS // 2,
    )

    # Tabled / plenary documents
    tabled = get_all(
        "plenary-session-documents",
        params={"start-date-gte": start},
        max_items=MAX_DOCUMENTS // 2,
    )

    log.info("  → %d adopted + %d tabled documents", len(adopted), len(tabled))

    docs = []
    for d in adopted:
        docs.append(_parse_doc(d, "adopted"))
    for d in tabled:
        docs.append(_parse_doc(d, "tabled"))

    docs.sort(key=lambda x: x.get("date", ""), reverse=True)
    return docs[:MAX_DOCUMENTS]


def _parse_doc(d: dict, doc_type: str) -> dict:
    return {
        "id":       safe_str(d.get("identifier", "")),
        "type":     doc_type,
        "date":     safe_str(d.get("date", d.get("dateDocument", ""))),
        "title":    safe_str(d.get("label", d.get("title", ""))),
        "ref":      safe_str(d.get("notation", d.get("reference", ""))),
        "ep_url":   safe_str(d.get("seeAlso", "")),
        "subjects": d.get("subject", []) if isinstance(d.get("subject"), list) else [],
    }


# ---------------------------------------------------------------------------
# Parliamentary questions
# ---------------------------------------------------------------------------

def fetch_questions() -> list[dict]:
    log.info("Fetching parliamentary questions…")
    start = date_str(QUESTIONS_LOOKBACK_DAYS)

    raw = get_all(
        "parliamentary-questions",
        params={"start-date-gte": start},
        max_items=MAX_QUESTIONS,
    )
    log.info("  → %d questions retrieved", len(raw))

    questions = []
    for q in raw:
        authors = q.get("author", [])
        if isinstance(authors, str):
            authors = [authors]

        questions.append({
            "id":       safe_str(q.get("identifier", "")),
            "date":     safe_str(q.get("date", "")),
            "title":    safe_str(q.get("label", "")),
            "type":     safe_str(q.get("questionType", "")),
            "ref":      safe_str(q.get("notation", "")),
            "authors":  authors,
            "ep_url":   safe_str(q.get("seeAlso", "")),
        })

    questions.sort(key=lambda x: x.get("date", ""), reverse=True)
    return questions


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    log.info("=" * 60)
    log.info("EP Tracker data fetch — %s", date.today().isoformat())
    log.info("=" * 60)

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    MEPS_DIR.mkdir(parents=True, exist_ok=True)

    errors = []

    # 1. MEPs (slowest — one API call per MEP)
    try:
        meps = fetch_meps()
        write_json(OUTPUT_DIR / "meps.json", meps)
    except Exception as e:
        log.error("MEP fetch failed: %s", e)
        errors.append("meps")

    # 2. Committees
    try:
        committees = fetch_committees()
        write_json(OUTPUT_DIR / "committees.json", committees)
    except Exception as e:
        log.error("Committee fetch failed: %s", e)
        errors.append("committees")

    # 3. Plenary sessions
    try:
        sessions = fetch_sessions()
        write_json(OUTPUT_DIR / "sessions.json", sessions)
    except Exception as e:
        log.error("Session fetch failed: %s", e)
        errors.append("sessions")

    # 4. Votes
    try:
        votes = fetch_votes()
        write_json(OUTPUT_DIR / "votes.json", votes)
    except Exception as e:
        log.error("Vote fetch failed: %s", e)
        errors.append("votes")

    # 5. Documents
    try:
        documents = fetch_documents()
        write_json(OUTPUT_DIR / "documents.json", documents)
    except Exception as e:
        log.error("Document fetch failed: %s", e)
        errors.append("documents")

    # 6. Parliamentary questions
    try:
        questions = fetch_questions()
        write_json(OUTPUT_DIR / "questions.json", questions)
    except Exception as e:
        log.error("Question fetch failed: %s", e)
        errors.append("questions")

    # 7. Meta (always written, even on partial failure)
    meta = {
        "last_updated":      date.today().isoformat(),
        "last_updated_ts":   time.time(),
        "term":              CURRENT_TERM,
        "lookback_days":     LOOKBACK_DAYS,
        "failed_datasets":   errors,
        "counts": {
            "meps":       len(meps)       if "meps"       not in errors else None,
            "committees": len(committees) if "committees" not in errors else None,
            "sessions":   len(sessions)   if "sessions"   not in errors else None,
            "votes":      len(votes)      if "votes"       not in errors else None,
            "documents":  len(documents)  if "documents"  not in errors else None,
            "questions":  len(questions)  if "questions"  not in errors else None,
        },
    }
    write_json(OUTPUT_DIR / "meta.json", meta)

    if errors:
        log.warning("Completed with errors in: %s", ", ".join(errors))
        sys.exit(1)
    else:
        log.info("All datasets fetched successfully.")


if __name__ == "__main__":
    main()
