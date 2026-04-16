#!/usr/bin/env python3
"""
EP Tracker — European Parliament data pipeline
Uses the EP website XML feed for the current MEP list,
then enriches each MEP with the Open Data API v2 for detail.

meps.json is written as a dict keyed by MEP ID for Jekyll compatibility.
All other datasets are written as arrays.
"""

import json
import logging
import os
import sys
import time
import xml.etree.ElementTree as ET
from datetime import date, timedelta
from pathlib import Path

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

OUTPUT_DIR = Path(os.environ.get("EP_DATA_DIR",
    str(Path(__file__).resolve().parent.parent / "_data")))
MEPS_DIR   = OUTPUT_DIR / "meps"

TIMEOUT    = 30
PAGE_SIZE  = 100
RATE_LIMIT = 0.3

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
log = logging.getLogger("ep-fetch")


def make_session():
    s = requests.Session()
    retry = Retry(
        total=4,
        backoff_factor=2,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"]
    )
    s.mount("https://", HTTPAdapter(max_retries=retry))
    s.headers.update({
        "Accept": "application/json",
        "User-Agent": "EP-Tracker/1.0"
    })
    return s


SESSION = make_session()


def get_json(endpoint, params=None):
    url = f"{EP_API_BASE}/{endpoint.lstrip('/')}"
    p = dict(params or {})
    p.setdefault("format", "application/ld+json")
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
    if isinstance(v, list):
        return v[0] if v else ""
    return str(v) if v else ""


def safe_label(v):
    """Extract a plain string from a possibly multilingual JSON-LD label.

    JSON-LD labels can arrive as:
      - a plain string: "ECON"
      - a language-tagged object: {"@value": "Economic Affairs", "@language": "en"}
      - a list of the above
    """
    if isinstance(v, list):
        # Prefer English, otherwise take first
        for item in v:
            if isinstance(item, dict) and item.get("@language", "").startswith("en"):
                return str(item.get("@value", "")).strip()
        v = v[0] if v else ""
    if isinstance(v, dict):
        return str(v.get("@value", v.get("value", ""))).strip()
    return str(v).strip() if v else ""


def _int(v):
    """Safely coerce a JSON-LD numeric value to int."""
    try:
        return int(v) if v is not None else 0
    except (TypeError, ValueError):
        return 0


def _lookback_years(lookback_days=None):
    """Return the list of calendar years that cover the lookback window."""
    days = lookback_days if lookback_days is not None else LOOKBACK_DAYS
    start = date.today() - timedelta(days=days)
    return list(range(start.year, date.today().year + 1))


def date_str(days_ago=0):
    return (date.today() - timedelta(days=days_ago)).isoformat()


# ---------------------------------------------------------------------------
# MEPs
# ---------------------------------------------------------------------------

def fetch_mep_list_from_xml():
    log.info("Fetching current MEP list from EP website XML feed...")
    try:
        r = SESSION.get(EP_MEP_XML, timeout=TIMEOUT,
            headers={"Accept": "application/xml, text/xml, */*"})
        r.raise_for_status()
        root = ET.fromstring(r.content)
        meps = []
        for mep in root.findall("mep"):
            meps.append({
                "id":             mep.findtext("id", ""),
                "full_name":      mep.findtext("fullName", ""),
                "country":        mep.findtext("country", ""),
                "group_name":     mep.findtext("politicalGroup", ""),
                "national_party": mep.findtext("nationalPoliticalGroup", ""),
            })
        log.info("  -> %d current MEPs from XML feed", len(meps))
        return meps
    except Exception as e:
        log.error("XML feed failed: %s", e)
        return []


def _parse_group_abbr(group_name):
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
    words = group_name.split()
    return words[0] if words else ""


def fetch_mep_detail(mep_id):
    detail = get_json(f"meps/{mep_id}",
        params={"format": "application/ld+json"})
    if not detail:
        return {}
    data = detail.get("data", [])
    if isinstance(data, list):
        return data[0] if data else {}
    return data if isinstance(data, dict) else {}


def fetch_meps(committee_lookup=None):
    committee_lookup = committee_lookup or {}
    base_list = fetch_mep_list_from_xml()
    if not base_list:
        log.error("Could not get MEP list — aborting MEP fetch")
        return {}

    meps = {}
    for i, base in enumerate(base_list):
        mep_id = base["id"]
        if not mep_id:
            continue

        d = fetch_mep_detail(mep_id)

        # Email
        email = safe_str(d.get("hasEmail", "")).replace("mailto:", "")
        if not email:
            for mem in d.get("hasMembership", []):
                if not isinstance(mem, dict):
                    continue
                for cp in (mem.get("contactPoint") or []):
                    if not isinstance(cp, dict):
                        continue
                    e = safe_str(cp.get("hasEmail", "")).replace("mailto:", "")
                    if e:
                        email = e
                        break
                if email:
                    break

        # Phone
        phone = ""
        for mem in d.get("hasMembership", []):
            if not isinstance(mem, dict):
                continue
            for cp in (mem.get("contactPoint") or []):
                if not isinstance(cp, dict):
                    continue
                tel = cp.get("hasTelephone", {})
                if isinstance(tel, dict):
                    p = safe_str(tel.get("hasValue", "")).replace("tel:", "")
                    if p:
                        phone = p
                        break
            if phone:
                break

        # Social media
        social = {}
        for link in (d.get("homePage") or d.get("homepage") or []):
            u = str(link.get("url", link) if isinstance(link, dict) else link)
            if "twitter.com" in u or "x.com" in u:
                social["twitter"] = u
            elif "linkedin.com" in u:
                social["linkedin"] = u
            elif "facebook.com" in u:
                social["facebook"] = u
            elif "instagram.com" in u:
                social["instagram"] = u

        # Photo
        photo_url = (safe_str(d.get("img", ""))
            or f"{EP_WEBSITE}/mepphoto/{mep_id}.jpg")

        # Committees
        committees = []
        today = date.today().isoformat()
        for mem in d.get("hasMembership", []):
            if not isinstance(mem, dict):
                continue
            cls = safe_str(mem.get("membershipClassification", ""))
            if "COMMITTEE" not in cls.upper():
                continue
            period = mem.get("memberDuring", {}) if isinstance(mem.get("memberDuring"), dict) else {}
            end = safe_str(period.get("endDate", ""))
            if end and end < today:
                continue
            org_id = safe_str(mem.get("organization", "")).replace("org/", "")
            role_raw = mem.get("role", "")
            role = safe_str(role_raw).split("/")[-1] if role_raw else ""
            if org_id:
                info  = committee_lookup.get(org_id, {})
                committees.append({
                    "id":   org_id,
                    "abbr": info.get("abbreviation", ""),
                    "name": info.get("name", ""),
                    "role": role,
                })

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

        # Store in dict keyed by ID (for Jekyll compatibility)
        meps[mep_id] = mep

        # Also write individual file
        write_json(MEPS_DIR / f"{mep_id}.json", mep)

        if (i + 1) % 50 == 0:
            log.info("  ... processed %d/%d MEPs", i + 1, len(base_list))

    return meps


# ---------------------------------------------------------------------------
# Committees
# ---------------------------------------------------------------------------

def fetch_committees():
    """Returns (committees_list, org_id_lookup).

    Uses the /corporate-bodies/show-current endpoint which returns only the
    bodies that are active today, using JSON-LD field names.

    org_id_lookup maps the numeric URI fragment used in MEP membership records
    (e.g. "6579") to {"abbreviation": "ECON", "name": "..."} so that
    fetch_meps() can enrich each MEP's committee entries with human-readable
    data rather than raw numeric IDs.
    """
    log.info("Fetching committees...")
    raw = get_all("corporate-bodies/show-current")
    log.info("  -> %d corporate bodies", len(raw))
    committees = []
    org_id_lookup = {}
    for c in raw:
        # notation / skos:notation holds the short abbreviation (ECON, BUDG…)
        body_id = safe_str(
            c.get("notation", c.get("skos:notation",
            c.get("identifier", c.get("id", "")))))
        # prefLabel / skos:prefLabel / label holds the full name
        name = safe_label(
            c.get("prefLabel", c.get("skos:prefLabel",
            c.get("label", ""))))
        if not body_id:
            continue
        committees.append({
            "id":           body_id,
            "abbreviation": body_id,
            "name":         name,
            "ep_url":       f"{EP_WEBSITE}/committees/en/{body_id.lower()}/home",
        })
        # The EP API uses numeric URI fragments in MEP membership records
        # (e.g. "org/6579") while corporate-bodies uses the notation abbreviation.
        # Extract the numeric tail from @id so we can cross-reference later.
        uri = safe_str(c.get("@id", ""))
        if "/" in uri:
            numeric_id = uri.rstrip("/").split("/")[-1]
            if numeric_id and numeric_id != body_id:
                org_id_lookup[numeric_id] = {"abbreviation": body_id, "name": name}
    log.info("  -> %d committees kept", len(committees))
    return sorted(committees, key=lambda x: x.get("abbreviation", "")), org_id_lookup


# ---------------------------------------------------------------------------
# Sessions
# ---------------------------------------------------------------------------

def fetch_sessions():
    """Fetch plenary meetings using year= parameter (API does not support start-date-gte).

    JSON-LD field names for meetings:
      activity_id / id   → meeting identifier
      activity_date      → date of the sitting
      hasLocality        → location (Strasbourg / Brussels)
      label              → human label
      seeAlso            → URL to EP agenda page
    """
    log.info("Fetching plenary meetings...")
    years = _lookback_years()
    all_raw = []
    for year in years:
        batch = get_all("meetings", params={"year": year})
        all_raw.extend(batch)
    log.info("  -> %d meeting records", len(all_raw))

    sessions = []
    seen_ids = set()
    for s in all_raw:
        sid = safe_str(s.get("activity_id", s.get("identifier", s.get("id", ""))))
        if not sid or sid in seen_ids:
            continue
        seen_ids.add(sid)

        date_val = safe_str(
            s.get("activity_date",
            s.get("eli-dl:activity_date",
            s.get("startDate", ""))))
        end_val = safe_str(
            s.get("activity_date_end",
            s.get("endDate", date_val)))

        loc_raw = s.get("hasLocality", "")
        if isinstance(loc_raw, list):
            loc_raw = loc_raw[0] if loc_raw else ""
        location = safe_label(loc_raw) if isinstance(loc_raw, dict) else safe_str(loc_raw)

        sessions.append({
            "id":       sid,
            "label":    safe_label(s.get("label", "")) or sid,
            "start":    date_val,
            "end":      end_val,
            "location": location,
            "ep_url":   safe_str(s.get("seeAlso", "")),
        })

    log.info("  -> %d sessions", len(sessions))
    return sorted(sessions, key=lambda x: x.get("start", ""), reverse=True)


# ---------------------------------------------------------------------------
# Votes
# ---------------------------------------------------------------------------

def fetch_votes():
    """Fetch roll-call votes via meetings/{id}/vote-results.

    The EP Open Data API v2 does not support a standalone /vote-results
    endpoint with date filtering.  Instead, votes are nested under each
    plenary meeting.  We:
      1. Collect meeting IDs for the lookback window (via year= param).
      2. For each meeting, fetch its vote results.

    JSON-LD field names for vote results:
      activity_id / id                  → vote ID
      activity_label / label            → vote title
      notation                          → reference
      had_decision_outcome              → result (ADOPTED / REJECTED …)
      number_of_votes_favor             → for count
      number_of_votes_against           → against count
      number_of_votes_abstention        → abstention count
      activity_date / date              → date
      seeAlso                           → EP URL
    """
    log.info("Fetching roll-call votes via plenary meetings...")
    years = _lookback_years()

    # Step 1: collect meeting IDs
    meeting_ids = []
    seen_m = set()
    for year in years:
        raw = get_all("meetings", params={"year": year})
        for s in raw:
            mid = safe_str(s.get("activity_id", s.get("identifier", s.get("id", ""))))
            if mid and mid not in seen_m:
                seen_m.add(mid)
                meeting_ids.append(mid)
    log.info("  -> %d meetings to scan", len(meeting_ids))

    # Step 2: per-meeting vote results
    votes = []
    for mid in meeting_ids:
        if len(votes) >= MAX_VOTES:
            break
        raw = get_all(f"meetings/{mid}/vote-results",
                      max_items=MAX_VOTES - len(votes))
        for v in raw:
            outcome = v.get("had_decision_outcome", v.get("decision_method", ""))
            if isinstance(outcome, dict):
                outcome = (safe_label(outcome.get("label",
                           outcome.get("prefLabel", "")))
                           or safe_str(outcome.get("@id", "")).split("/")[-1])
            elif isinstance(outcome, str) and "/" in outcome:
                outcome = outcome.split("/")[-1]
            else:
                outcome = safe_str(outcome)

            votes.append({
                "id":         safe_str(v.get("activity_id",
                              v.get("identifier", v.get("id", "")))),
                "date":       safe_str(v.get("activity_date",
                              v.get("date", ""))),
                "title":      safe_label(v.get("activity_label",
                              v.get("label", ""))),
                "result":     outcome,
                "for":        _int(v.get("number_of_votes_favor",
                              v.get("had_voter_favor", 0))),
                "against":    _int(v.get("number_of_votes_against",
                              v.get("had_voter_against", 0))),
                "abstention": _int(v.get("number_of_votes_abstention",
                              v.get("had_voter_abstention", 0))),
                "ep_ref":     safe_str(v.get("notation", "")),
                "ep_url":     safe_str(v.get("seeAlso", "")),
            })

    log.info("  -> %d votes total", len(votes))
    return sorted(votes, key=lambda x: x.get("date", ""), reverse=True)


# ---------------------------------------------------------------------------
# Documents
# ---------------------------------------------------------------------------

def _parse_doc(d, doc_type):
    # JSON-LD field names vary; try the most common ones in priority order
    work_type = safe_str(d.get("work_type", doc_type))
    if "adopted" in work_type.lower():
        doc_type = "adopted"
    return {
        "id":     safe_str(d.get("work_id", d.get("identifier", d.get("id", "")))),
        "type":   doc_type,
        "date":   safe_str(d.get("document_date",
                  d.get("work_date_document",
                  d.get("date_document",
                  d.get("date", ""))))),
        "title":  safe_label(d.get("title_dcterms",
                  d.get("label",
                  d.get("title", "")))),
        "ref":    safe_str(d.get("notation", "")),
        "ep_url": safe_str(d.get("seeAlso", "")),
    }


def fetch_documents():
    """Fetch plenary documents using year= parameter.

    The EP API does not support start-date-gte for document endpoints.
    We query by each calendar year in the lookback window.

    JSON-LD field names:
      work_id / identifier / id    → document ID
      title_dcterms / label        → title
      work_type                    → document type
      document_date / date         → date
      notation                     → reference number
      seeAlso                      → EP URL
    """
    log.info("Fetching legislative documents...")
    years = _lookback_years()
    docs = []
    seen_ids = set()
    for year in years:
        batch = get_all("plenary-documents",
                        params={"year": year},
                        max_items=MAX_DOCUMENTS)
        for d in batch:
            docs.append(_parse_doc(d, "tabled"))
    log.info("  -> %d documents", len(docs))
    # Deduplicate and sort
    unique = []
    for d in docs:
        if d["id"] and d["id"] not in seen_ids:
            seen_ids.add(d["id"])
            unique.append(d)
    return sorted(unique, key=lambda x: x.get("date", ""), reverse=True)[:MAX_DOCUMENTS]


# ---------------------------------------------------------------------------
# Questions
# ---------------------------------------------------------------------------

def fetch_questions():
    """Fetch parliamentary questions using year= parameter.

    The API does not support start-date-gte; date filtering is by year only.

    JSON-LD field names:
      work_id / identifier          → ID
      work_type                     → QUESTION_WRITTEN / QUESTION_ORAL
      was_created_by / author       → list of author identifiers
      document_date / date          → date
      title_dcterms / label         → title
      notation                      → reference
      seeAlso                       → EP URL
    """
    log.info("Fetching parliamentary questions...")
    years = _lookback_years(QUESTIONS_LOOKBACK_DAYS)
    questions = []
    seen_ids = set()
    for year in years:
        batch = get_all("parliamentary-questions",
                        params={"year": year},
                        max_items=MAX_QUESTIONS)
        for q in batch:
            qid = safe_str(q.get("work_id", q.get("identifier", q.get("id", ""))))
            if qid in seen_ids:
                continue
            seen_ids.add(qid)

            # Normalize type: QUESTION_WRITTEN → Written Question, etc.
            qtype = safe_str(q.get("work_type", q.get("questionType", "")))
            if "written" in qtype.lower():
                qtype = "Written Question"
            elif "oral" in qtype.lower():
                qtype = "Oral Question"

            # Authors: was_created_by is the JSON-LD predicate for author
            authors_raw = q.get("was_created_by",
                          q.get("created_by",
                          q.get("author", [])))
            if isinstance(authors_raw, (str, dict)):
                authors_raw = [authors_raw]
            author_ids = []
            for a in (authors_raw or []):
                if isinstance(a, dict):
                    aid = safe_str(a.get("identifier",
                                   a.get("notation",
                                   a.get("@id", a.get("id", "")))))
                    if "/" in aid:
                        aid = aid.split("/")[-1]
                else:
                    aid = safe_str(a)
                    if "/" in aid:
                        aid = aid.split("/")[-1]
                if aid:
                    author_ids.append(aid)

            questions.append({
                "id":      qid,
                "date":    safe_str(q.get("document_date",
                           q.get("work_date_document",
                           q.get("date_document",
                           q.get("date", ""))))),
                "title":   safe_label(q.get("title_dcterms",
                           q.get("label", q.get("title", "")))),
                "type":    qtype,
                "ref":     safe_str(q.get("notation", "")),
                "authors": author_ids,
                "ep_url":  safe_str(q.get("seeAlso", "")),
            })
    log.info("  -> %d questions", len(questions))
    return sorted(questions, key=lambda x: x.get("date", ""), reverse=True)[:MAX_QUESTIONS]


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    log.info("=" * 60)
    log.info("EP Tracker data fetch — %s", date.today().isoformat())
    log.info("=" * 60)
    log.info("Writing data to: %s", OUTPUT_DIR)

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    MEPS_DIR.mkdir(parents=True, exist_ok=True)

    errors = []
    counts = {}

    # 1. Committees — fetched first so MEP entries can be enriched with
    #    human-readable abbreviations and names via the org_id_lookup.
    committee_lookup = {}
    try:
        committees_data, committee_lookup = fetch_committees()
        write_json(OUTPUT_DIR / "committees.json", committees_data)
        counts["committees"] = len(committees_data)
    except Exception as e:
        log.error("committees fetch failed: %s", e)
        errors.append("committees")
        counts["committees"] = None

    # 2. MEPs — written as dict keyed by ID for Jekyll compatibility.
    #    Passes committee_lookup so each MEP's committee entries get
    #    abbreviation + name populated.
    try:
        meps = fetch_meps(committee_lookup)
        write_json(OUTPUT_DIR / "meps.json", meps)
        counts["meps"] = len(meps)
    except Exception as e:
        log.error("meps fetch failed: %s", e)
        errors.append("meps")
        counts["meps"] = None

    # 3. Remaining datasets — written as arrays
    other_datasets = [
        ("sessions",   fetch_sessions),
        ("votes",      fetch_votes),
        ("documents",  fetch_documents),
        ("questions",  fetch_questions),
    ]

    for name, fn in other_datasets:
        try:
            data = fn()
            write_json(OUTPUT_DIR / f"{name}.json", data)
            counts[name] = len(data)
        except Exception as e:
            log.error("%s fetch failed: %s", name, e)
            errors.append(name)
            counts[name] = None

    # Meta
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
