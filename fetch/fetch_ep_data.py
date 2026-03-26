#!/usr/bin/env python3
"""
EP Tracker — European Parliament data pipeline
Correct API base: https://data.europarl.europa.eu/api/v2
All endpoints require format=application/json as a query parameter.
"""

import json
import logging
import sys
import time
from datetime import date, timedelta
from pathlib import Path

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

EP_API_BASE  = "https://data.europarl.europa.eu/api/v2"
EP_WEBSITE   = "https://www.europarl.europa.eu"
CURRENT_TERM = 10
LOOKBACK_DAYS = 180
QUESTIONS_LOOKBACK_DAYS = 90
MAX_VOTES     = 500
MAX_DOCUMENTS = 300
MAX_QUESTIONS = 300
OUTPUT_DIR = Path(__file__).parent.parent / "_data"
MEPS_DIR   = OUTPUT_DIR / "meps"
TIMEOUT    = 30
PAGE_SIZE  = 100
RATE_LIMIT = 0.4
API_FORMAT = "application/json"

logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(levelname)-8s  %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
log = logging.getLogger("ep-fetch")

def make_session():
    session = requests.Session()
    retry = Retry(total=4, backoff_factor=2, status_forcelist=[429, 500, 502, 503, 504], allowed_methods=["GET"])
    session.mount("https://", HTTPAdapter(max_retries=retry))
    session.headers.update({"Accept": "application/json", "User-Agent": "EP-Tracker/1.0"})
    return session

SESSION = make_session()

def get(endpoint, params=None):
    url = f"{EP_API_BASE}/{endpoint.lstrip('/')}"
    p = dict(params or {})
    p.setdefault("format", API_FORMAT)
    try:
        r = SESSION.get(url, params=p, timeout=TIMEOUT)
        if r.status_code == 404:
            log.warning("HTTP 404 — %s", url)
            return None
        r.raise_for_status()
        time.sleep(RATE_LIMIT)
        return r.json()
    except Exception as e:
        log.warning("Request failed — %s — %s", url, e)
        return None

def get_all(endpoint, params=None, max_items=9999):
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
        if len(items) < params["limit"]:
            break
        params["offset"] += params["limit"]
    return results[:max_items]

def write_json(path, data):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    log.info("  wrote %s (%d bytes)", path.name, path.stat().st_size)

def safe_str(value):
    if isinstance(value, list): return value[0] if value else ""
    return str(value) if value else ""

def date_str(days_ago=0):
    return (date.today() - timedelta(days=days_ago)).isoformat()

def fetch_meps():
    log.info("Fetching MEP list (term %d)…", CURRENT_TERM)
    raw = get_all("meps", params={"parliamentary-term": CURRENT_TERM})
    log.info("  → %d MEPs retrieved", len(raw))
    meps = []
    for i, m in enumerate(raw):
        mep_id = safe_str(m.get("identifier", ""))
        if not mep_id:
            continue
        detail = get(f"meps/{mep_id}") or {}
        d = detail.get("data", m)
        if isinstance(d, list): d = d[0] if d else m
        mep = _parse_mep(mep_id, d)
        meps.append(mep)
        write_json(MEPS_DIR / f"{mep_id}.json", mep)
        if (i + 1) % 50 == 0:
            log.info("  … processed %d/%d MEPs", i + 1, len(raw))
    meps.sort(key=lambda x: x.get("last_name", ""))
    return meps

def _parse_mep(mep_id, d):
    full   = safe_str(d.get("label", ""))
    given  = safe_str(d.get("givenName", ""))
    family = safe_str(d.get("familyName", ""))
    if not full: full = f"{given} {family}".strip()

    country = ""
    for ref in d.get("represents", []):
        if isinstance(ref, dict):
            country = safe_str(ref.get("label", ref.get("notation", "")))
            break
    if not country: country = safe_str(d.get("countryOfRepresentation", ""))

    group_name = group_abbr = national_party = ""
    for mem in d.get("hasMembership", []):
        if not isinstance(mem, dict): continue
        org = mem.get("memberOf", {}) if isinstance(mem.get("memberOf"), dict) else {}
        org_type = safe_str(org.get("@type", ""))
        if "PoliticalGroup" in org_type:
            group_name = safe_str(org.get("label", ""))
            group_abbr = safe_str(org.get("notation", ""))
        elif "NationalPoliticalGroup" in org_type:
            national_party = safe_str(org.get("label", ""))

    email = phone = website = ""
    for contact in (d.get("contactPoint") or []):
        if not isinstance(contact, dict): continue
        if not email: email = safe_str(contact.get("email", "")).replace("mailto:", "")
        if not phone: phone = safe_str(contact.get("telephone", ""))
        if not website: website = safe_str(contact.get("url", ""))

    social = {}
    for link in (d.get("homePage") or d.get("homepage") or []):
        u = str(link.get("url", link) if isinstance(link, dict) else link)
        if "twitter.com" in u or "x.com" in u: social["twitter"] = u
        elif "linkedin.com" in u: social["linkedin"] = u
        elif "facebook.com" in u: social["facebook"] = u
        elif "instagram.com" in u: social["instagram"] = u

    photo_url = safe_str(d.get("img", "")) or f"{EP_WEBSITE}/meps/en/{mep_id}/home/widget/photo.jpg"

    committees = []
    for mem in d.get("hasMembership", []):
        if not isinstance(mem, dict): continue
        org = mem.get("memberOf", {}) if isinstance(mem.get("memberOf"), dict) else {}
        if "Committee" in safe_str(org.get("@type", "")):
            role_raw = mem.get("role", "")
            committees.append({
                "id":   safe_str(org.get("notation", org.get("identifier", ""))),
                "name": safe_str(org.get("label", "")),
                "role": safe_str(role_raw.get("label", "") if isinstance(role_raw, dict) else role_raw),
            })

    return {"id": mep_id, "full_name": full, "given_name": given, "last_name": family,
            "country": country, "national_party": national_party, "group_name": group_name,
            "group_abbr": group_abbr, "email": email, "phone": phone, "website": website,
            "social": social, "photo_url": photo_url,
            "ep_profile_url": f"{EP_WEBSITE}/meps/en/{mep_id}", "committees": committees}

def fetch_committees():
    log.info("Fetching committees…")
    raw = get_all("corporate-bodies", params={"parliamentary-term": CURRENT_TERM, "corporate-body-classification": "COMMITTEE"})
    log.info("  → %d committees retrieved", len(raw))
    committees = []
    for c in raw:
        body_id = safe_str(c.get("notation", c.get("identifier", "")))
        committees.append({"id": body_id, "abbreviation": body_id, "name": safe_str(c.get("label", "")),
                           "ep_url": f"{EP_WEBSITE}/committees/en/{body_id.lower()}/home"})
    return sorted(committees, key=lambda x: x.get("abbreviation", ""))

def fetch_sessions():
    log.info("Fetching plenary meetings…")
    raw = get_all("meetings", params={"start-date-gte": date_str(LOOKBACK_DAYS)})
    log.info("  → %d meetings retrieved", len(raw))
    sessions = []
    for s in raw:
        activity = s.get("hadActivity", {}) if isinstance(s.get("hadActivity"), dict) else {}
        sessions.append({
            "id": safe_str(s.get("identifier", "")), "label": safe_str(s.get("label", "")),
            "start": safe_str(activity.get("startDate", s.get("startDate", ""))),
            "end":   safe_str(activity.get("endDate",   s.get("endDate", ""))),
            "location": safe_str(s.get("place", {}).get("label", "") if isinstance(s.get("place"), dict) else ""),
            "ep_url": safe_str(s.get("seeAlso", "")),
        })
    return sorted(sessions, key=lambda x: x.get("start", ""), reverse=True)

def fetch_votes():
    log.info("Fetching roll-call votes…")
    raw = get_all("vote-results", params={"start-date-gte": date_str(LOOKBACK_DAYS)}, max_items=MAX_VOTES)
    log.info("  → %d votes retrieved", len(raw))
    votes = []
    for v in raw:
        result_raw = v.get("result", "")
        votes.append({
            "id": safe_str(v.get("identifier", "")), "date": safe_str(v.get("date", "")),
            "title": safe_str(v.get("label", "")),
            "result": safe_str(result_raw.get("label", "") if isinstance(result_raw, dict) else result_raw),
            "for": v.get("numberOfVotesFor", 0), "against": v.get("numberOfVotesAgainst", 0),
            "abstention": v.get("numberOfAbstentions", 0),
            "ep_ref": safe_str(v.get("notation", "")), "ep_url": safe_str(v.get("seeAlso", "")),
        })
    return sorted(votes, key=lambda x: x.get("date", ""), reverse=True)

def _parse_doc(d, doc_type):
    return {"id": safe_str(d.get("identifier", "")), "type": doc_type,
            "date": safe_str(d.get("date", d.get("dateDocument", ""))),
            "title": safe_str(d.get("label", d.get("title", ""))),
            "ref": safe_str(d.get("notation", "")), "ep_url": safe_str(d.get("seeAlso", ""))}

def fetch_documents():
    log.info("Fetching legislative documents…")
    start = date_str(LOOKBACK_DAYS)
    adopted = get_all("adopted-texts",    params={"start-date-gte": start}, max_items=MAX_DOCUMENTS // 2)
    tabled  = get_all("plenary-documents", params={"start-date-gte": start}, max_items=MAX_DOCUMENTS // 2)
    log.info("  → %d adopted + %d tabled", len(adopted), len(tabled))
    docs = [_parse_doc(d, "adopted") for d in adopted] + [_parse_doc(d, "tabled") for d in tabled]
    return sorted(docs, key=lambda x: x.get("date", ""), reverse=True)[:MAX_DOCUMENTS]

def fetch_questions():
    log.info("Fetching parliamentary questions…")
    raw = get_all("parliamentary-questions", params={"start-date-gte": date_str(QUESTIONS_LOOKBACK_DAYS)}, max_items=MAX_QUESTIONS)
    log.info("  → %d questions retrieved", len(raw))
    questions = []
    for q in raw:
        authors = q.get("author", [])
        if isinstance(authors, str): authors = [authors]
        author_ids = [safe_str(a.get("identifier", a.get("label", "")) if isinstance(a, dict) else a) for a in authors]
        qtype = q.get("questionType", "")
        questions.append({
            "id": safe_str(q.get("identifier", "")), "date": safe_str(q.get("date", "")),
            "title": safe_str(q.get("label", "")),
            "type": safe_str(qtype.get("label", "") if isinstance(qtype, dict) else qtype),
            "ref": safe_str(q.get("notation", "")), "authors": author_ids,
            "ep_url": safe_str(q.get("seeAlso", "")),
        })
    return sorted(questions, key=lambda x: x.get("date", ""), reverse=True)

def main():
    log.info("=" * 60)
    log.info("EP Tracker data fetch — %s", date.today().isoformat())
    log.info("=" * 60)
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    MEPS_DIR.mkdir(parents=True, exist_ok=True)

    errors = {}
    datasets = [("meps", fetch_meps), ("committees", fetch_committees), ("sessions", fetch_sessions),
                ("votes", fetch_votes), ("documents", fetch_documents), ("questions", fetch_questions)]

    counts = {}
    for name, fn in datasets:
        try:
            data = fn()
            write_json(OUTPUT_DIR / f"{name}.json", data)
            counts[name] = len(data)
        except Exception as e:
            log.error("%s fetch failed: %s", name, e)
            errors[name] = str(e)
            counts[name] = None

    write_json(OUTPUT_DIR / "meta.json", {
        "last_updated": date.today().isoformat(),
        "last_updated_ts": time.time(),
        "term": CURRENT_TERM,
        "failed_datasets": list(errors.keys()),
        "counts": counts,
    })

    if errors:
        log.warning("Completed with errors in: %s", ", ".join(errors.keys()))
        sys.exit(1)
    else:
        log.info("All datasets fetched successfully.")
        for name, count in counts.items():
            log.info("  %-12s  %s items", name, count)

if __name__ == "__main__":
    main()
