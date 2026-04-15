#!/usr/bin/env python3
"""
EP Tracker — European Parliament data pipeline v3
Uses the EP website XML feed for the current MEP list,
then enriches each MEP with the Open Data API v2 for detail.
"""

import json
import logging
import sys
import time
import xml.etree.ElementTree as ET
from datetime import date, timedelta
from pathlib import Path
import os
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

EP_API_BASE  = "https://data.europarl.europa.eu/api/v2"
EP_WEBSITE   = "https://www.europarl.europa.eu"
EP_MEP_XML   = "https://www.europarl.europa.eu/meps/en/full-list/xml"

LOOKBACK_DAYS = 180
QUESTIONS_LOOKBACK_DAYS = 90
MAX_VOTES     = 500
MAX_DOCUMENTS = 300
MAX_QUESTIONS = 300
OUTPUT_DIR = Path(os.environ.get("EP_DATA_DIR", str(Path(__file__).resolve().parent.parent / "_data")))
MEPS_DIR   = OUTPUT_DIR / "meps"
TIMEOUT    = 30
PAGE_SIZE  = 100
RATE_LIMIT = 0.3

logging.basicConfig(level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S")
log = logging.getLogger("ep-fetch")

def make_session():
    s = requests.Session()
    retry = Retry(total=4, backoff_factor=2,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"])
    s.mount("https://", HTTPAdapter(max_retries=retry))
    s.headers.update({"Accept": "application/json",
        "User-Agent": "EP-Tracker/1.0 (https://github.com/EU-Parliament-Tracker/ep-tracker)"})
    return s

SESSION = make_session()

def get_json(endpoint, params=None):
    url = f"{EP_API_BASE}/{endpoint.lstrip('/')}"
    p = dict(params or {})
    p.setdefault("format", "application/json")
    try:
        r = SESSION.get(url, params=p, timeout=TIMEOUT)
        if r.status_code == 404:
            return None
        r.raise_for_status()
        time.sleep(RATE_LIMIT)
        return r.json()
    except Exception as e:
        log.warning("API request failed — %s — %s", url, e)
        return None

def get_all(endpoint, params=None, max_items=9999):
    params = dict(params or {})
    params.setdefault("limit", PAGE_SIZE)
    params.setdefault("offset", 0)
    results = []
    while len(results) < max_items:
        data = get_json(endpoint, params)
        if not data:
            break
        items = data.get("data", [])
        if not items:
            break
        results.extend(items)
        if len(items) < params["limit"]:
            break
        params["offset"] += params["limit"]
    return results[:max_items]

def write_json(path, data):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    log.info("  wrote %s (%d bytes)", path.name, path.stat().st_size)

def safe_str(v):
    if isinstance(v, list): return v[0] if v else ""
    return str(v) if v else ""

def date_str(days_ago=0):
    return (date.today() - timedelta(days=days_ago)).isoformat()

# ---------------------------------------------------------------------------
# MEPs — use XML feed for current list, API for detail
# ---------------------------------------------------------------------------

def fetch_mep_list_from_xml():
    """Fetch current MEPs from the EP website XML feed."""
    log.info("Fetching current MEP list from EP website XML feed…")
    try:
        r = SESSION.get(EP_MEP_XML, timeout=TIMEOUT,
            headers={"Accept": "application/xml, text/xml, */*"})
        r.raise_for_status()
        root = ET.fromstring(r.content)
        meps = []
        for mep in root.findall("mep"):
            meps.append({
                "id":            mep.findtext("id", ""),
                "full_name":     mep.findtext("fullName", ""),
                "country":       mep.findtext("country", ""),
                "group_name":    mep.findtext("politicalGroup", ""),
                "national_party":mep.findtext("nationalPoliticalGroup", ""),
            })
        log.info("  → %d current MEPs from XML feed", len(meps))
        return meps
    except Exception as e:
        log.error("XML feed failed: %s", e)
        return []

def _parse_group_abbr(group_name):
    """Map full group name to common abbreviation."""
    mapping = {
        "European People's Party": "EPP",
        "Progressive Alliance of Socialists and Democrats": "S&D",
        "Renew Europe": "Renew",
        "European Conservatives and Reformists": "ECR",
        "Greens": "Greens/EFA",
        "Left group": "GUE/NGL",
        "Patriots for Europe": "PfE",
        "Europe of Sovereign Nations": "ESN",
        "Non-attached": "NI",
    }
    for key, abbr in mapping.items():
        if key.lower() in group_name.lower():
            return abbr
    # Fallback: first word(s)
    words = group_name.split()
    return words[0] if words else ""

def fetch_mep_detail(mep_id):
    """Fetch contact info, social media, committees from the API."""
    detail = get_json(f"meps/{mep_id}",
        params={"format": "application/ld+json"})
    if not detail:
        return {}
    data = detail.get("data", [])
    if isinstance(data, list):
        return data[0] if data else {}
    return data if isinstance(data, dict) else {}

def fetch_meps():
    base_list = fetch_mep_list_from_xml()
    if not base_list:
        log.error("Could not get MEP list — aborting MEP fetch")
        return []

    meps = []
    for i, base in enumerate(base_list):
        mep_id = base["id"]
        if not mep_id:
            continue

        # Get enriched detail from API
        d = fetch_mep_detail(mep_id)

        # Email
        email = ""
        has_email = safe_str(d.get("hasEmail", "")).replace("mailto:", "")
        if has_email:
            email = has_email
        else:
            for mem in d.get("hasMembership", []):
                if not isinstance(mem, dict): continue
                for cp in (mem.get("contactPoint") or []):
                    if not isinstance(cp, dict): continue
                    e = safe_str(cp.get("hasEmail", "")).replace("mailto:", "")
                    if e:
                        email = e
                        break
                if email: break

        # Phone
        phone = ""
        for mem in d.get("hasMembership", []):
            if not isinstance(mem, dict): continue
            for cp in (mem.get("contactPoint") or []):
                if not isinstance(cp, dict): continue
                tel = cp.get("hasTelephone", {})
                if isinstance(tel, dict):
                    p = safe_str(tel.get("hasValue", "")).replace("tel:", "")
                    if p: phone = p; break
            if phone: break

        # Social media
        social = {}
        for link in (d.get("homePage") or d.get("homepage") or []):
            u = str(link.get("url", link) if isinstance(link, dict) else link)
            if "twitter.com" in u or "x.com" in u: social["twitter"] = u
            elif "linkedin.com" in u: social["linkedin"] = u
            elif "facebook.com" in u: social["facebook"] = u
            elif "instagram.com" in u: social["instagram"] = u

        # Photo
        photo_url = (safe_str(d.get("img", ""))
            or f"{EP_WEBSITE}/mepphoto/{mep_id}.jpg")

        # Committees
        committees = []
        today = date.today().isoformat()
        for mem in d.get("hasMembership", []):
            if not isinstance(mem, dict): continue
            cls = safe_str(mem.get("membershipClassification", ""))
            if "COMMITTEE" not in cls.upper(): continue
            period = mem.get("memberDuring", {}) if isinstance(mem.get("memberDuring"), dict) else {}
            end = safe_str(period.get("endDate", ""))
            if end and end < today: continue  # past membership
            org_id = safe_str(mem.get("organization", "")).replace("org/", "")
            role_raw = mem.get("role", "")
            role = safe_str(role_raw).split("/")[-1] if role_raw else ""
            if org_id:
                committees.append({"id": org_id, "name": "", "role": role})

        # Name parts
        full = base["full_name"]
        given  = safe_str(d.get("givenName", ""))
        family = safe_str(d.get("familyName", ""))
        if not given and not family and full:
            parts = full.split()
            family = parts[-1] if parts else ""
            given  = " ".join(parts[:-1]) if len(parts) > 1 else ""

        group_name = base["group_name"]
        group_abbr = _parse_group_abbr(group_name)

        mep = {
            "id":             mep_id,
            "full_name":      full,
            "given_name":     given,
            "last_name":      family,
            "country":        base["country"],
            "national_party": base["national_party"],
            "group_name":     group_name,
            "group_abbr":     group_abbr,
            "email":          email,
            "phone":          phone,
            "website":        "",
            "social":         social,
            "photo_url":      photo_url,
            "ep_profile_url": f"{EP_WEBSITE}/meps/en/{mep_id}",
            "committees":     committees,
        }
        meps.append(mep)
        write_json(MEPS_DIR / f"{mep_id}.json", mep)

        if (i + 1) % 50 == 0:
            log.info("  … processed %d/%d MEPs", i + 1, len(base_list))

    meps.sort(key=lambda x: x.get("last_name", "").upper())
    return meps

# ---------------------------------------------------------------------------
# Committees
# ---------------------------------------------------------------------------

def fetch_committees():
    log.info("Fetching committees…")
    raw = get_all("corporate-bodies", params={
        "corporate-body-classification": "COMMITTEE_PARLIAMENTARY_STANDING"})
    log.info("  → %d committees", len(raw))
    committees = []
    for c in raw:
        body_id = safe_str(c.get("notation", c.get("identifier", "")))
        committees.append({
            "id":           body_id,
            "abbreviation": body_id,
            "name":         safe_str(c.get("label", "")),
            "ep_url":       f"{EP_WEBSITE}/committees/en/{body_id.lower()}/home",
        })
    return sorted(committees, key=lambda x: x.get("abbreviation", ""))

# ---------------------------------------------------------------------------
# Sessions / Meetings
# ---------------------------------------------------------------------------

def fetch_sessions():
    log.info("Fetching plenary meetings…")
    raw = get_all("meetings", params={"start-date-gte": date_str(LOOKBACK_DAYS)})
    log.info("  → %d meetings", len(raw))
    sessions = []
    for s in raw:
        activity = s.get("hadActivity", {}) if isinstance(s.get("hadActivity"), dict) else {}
        sessions.append({
            "id":       safe_str(s.get("identifier", "")),
            "label":    safe_str(s.get("label", "")),
            "start":    safe_str(activity.get("startDate", s.get("startDate", ""))),
            "end":      safe_str(activity.get("endDate",   s.get("endDate", ""))),
            "location": safe_str(s.get("place", {}).get("label", "") if isinstance(s.get("place"), dict) else ""),
            "ep_url":   safe_str(s.get("seeAlso", "")),
        })
    return sorted(sessions, key=lambda x: x.get("start", ""), reverse=True)

# ---------------------------------------------------------------------------
# Votes
# ---------------------------------------------------------------------------

def fetch_votes():
    log.info("Fetching roll-call votes…")
    raw = get_all("vote-results",
        params={"start-date-gte": date_str(LOOKBACK_DAYS)},
        max_items=MAX_VOTES)
    log.info("  → %d votes", len(raw))
    votes = []
    for v in raw:
        result_raw = v.get("result", "")
        votes.append({
            "id":         safe_str(v.get("identifier", "")),
            "date":       safe_str(v.get("date", "")),
            "title":      safe_str(v.get("label", "")),
            "result":     safe_str(result_raw.get("label", "") if isinstance(result_raw, dict) else result_raw),
            "for":        v.get("numberOfVotesFor", 0),
            "against":    v.get("numberOfVotesAgainst", 0),
            "abstention": v.get("numberOfAbstentions", 0),
            "ep_ref":     safe_str(v.get("notation", "")),
            "ep_url":     safe_str(v.get("seeAlso", "")),
        })
    return sorted(votes, key=lambda x: x.get("date", ""), reverse=True)

# ---------------------------------------------------------------------------
# Documents
# ---------------------------------------------------------------------------

def _parse_doc(d, doc_type):
    return {
        "id":     safe_str(d.get("identifier", "")),
        "type":   doc_type,
        "date":   safe_str(d.get("date", d.get("dateDocument", ""))),
        "title":  safe_str(d.get("label", d.get("title", ""))),
        "ref":    safe_str(d.get("notation", "")),
        "ep_url": safe_str(d.get("seeAlso", "")),
    }

def fetch_documents():
    log.info("Fetching legislative documents…")
    start = date_str(LOOKBACK_DAYS)
    adopted = get_all("adopted-texts", params={"start-date-gte": start},
        max_items=MAX_DOCUMENTS // 2)
    tabled  = get_all("plenary-documents", params={"start-date-gte": start},
        max_items=MAX_DOCUMENTS // 2)
    log.info("  → %d adopted + %d tabled", len(adopted), len(tabled))
    docs = ([_parse_doc(d, "adopted") for d in adopted]
          + [_parse_doc(d, "tabled")  for d in tabled])
    return sorted(docs, key=lambda x: x.get("date", ""), reverse=True)[:MAX_DOCUMENTS]

# ---------------------------------------------------------------------------
# Questions
# ---------------------------------------------------------------------------

def fetch_questions():
    log.info("Fetching parliamentary questions…")
    raw = get_all("parliamentary-questions",
        params={"start-date-gte": date_str(QUESTIONS_LOOKBACK_DAYS)},
        max_items=MAX_QUESTIONS)
    log.info("  → %d questions", len(raw))
    questions = []
    for q in raw:
        authors = q.get("author", [])
        if isinstance(authors, str): authors = [authors]
        author_ids = [safe_str(a.get("identifier", a.get("label", ""))
            if isinstance(a, dict) else a) for a in authors]
        qtype = q.get("questionType", "")
        questions.append({
            "id":      safe_str(q.get("identifier", "")),
            "date":    safe_str(q.get("date", "")),
            "title":   safe_str(q.get("label", "")),
            "type":    safe_str(qtype.get("label", "") if isinstance(qtype, dict) else qtype),
            "ref":     safe_str(q.get("notation", "")),
            "authors": author_ids,
            "ep_url":  safe_str(q.get("seeAlso", "")),
        })
    return sorted(questions, key=lambda x: x.get("date", ""), reverse=True)

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
    counts = {}
    datasets = [
        ("meps",       fetch_meps),
        ("committees", fetch_committees),
        ("sessions",   fetch_sessions),
        ("votes",      fetch_votes),
        ("documents",  fetch_documents),
        ("questions",  fetch_questions),
    ]

    for name, fn in datasets:
        try:
            data = fn()
            meps_dict = {m["id"]: m for m in meps}
write_json(OUTPUT_DIR / "meps.json", meps_dict)
            counts[name] = len(data)
        except Exception as e:
            log.error("%s fetch failed: %s", name, e)
            errors.append(name)
            counts[name] = None

    write_json(OUTPUT_DIR / "meta.json", {
        "last_updated":    date.today().isoformat(),
        "last_updated_ts": time.time(),
        "failed_datasets": errors,
        "counts":          counts,
    })

    if errors:
        log.warning("Completed with errors in: %s", ", ".join(errors))
        sys.exit(1)
    else:
        log.info("All datasets fetched successfully.")
        for name, count in counts.items():
            log.info("  %-12s  %s items", name, count)

if __name__ == "__main__":
    main()
