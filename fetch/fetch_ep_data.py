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

# Full names for EP committees (abbreviation → full name)
COMMITTEE_FULL_NAMES = {
    "AFET":  "Foreign Affairs",
    "DROI":  "Human Rights",
    "SEDE":  "Security and Defence",
    "DEVE":  "Development",
    "INTA":  "International Trade",
    "BUDG":  "Budgets",
    "CONT":  "Budgetary Control",
    "ECON":  "Economic and Monetary Affairs",
    "EMPL":  "Employment and Social Affairs",
    "ENVI":  "Environment, Public Health and Food Safety",
    "ITRE":  "Industry, Research and Energy",
    "IMCO":  "Internal Market and Consumer Protection",
    "TRAN":  "Transport and Tourism",
    "REGI":  "Regional Development",
    "AGRI":  "Agriculture and Rural Development",
    "PECH":  "Fisheries",
    "CULT":  "Culture and Education",
    "JURI":  "Legal Affairs",
    "LIBE":  "Civil Liberties, Justice and Home Affairs",
    "AFCO":  "Constitutional Affairs",
    "FEMM":  "Women's Rights and Gender Equality",
    "PETI":  "Petitions",
    "STOA":  "Science and Technology Options Assessment",
    "NI":    "Non-Attached Members",
    "INGE":  "Foreign Interference in Democratic Processes",
    "DMAS":  "Future of European Defence",
    "FISC":  "Tax Matters",
    "BECA":  "Beating Cancer",
    "SURE":  "Sustainable Urban Mobility",
    "AIDA":  "Artificial Intelligence in a Digital Age",
}

OUTPUT_DIR = Path(os.environ.get("EP_DATA_DIR",
    str(Path(__file__).resolve().parent.parent / "_data")))
MEPS_DIR   = OUTPUT_DIR / "meps"

# Authority code → human-readable place name
PLACE_LOOKUP = {
    "FRA_SXB": "Strasbourg",
    "BEL_BRU": "Brussels",
    "LUX_LUX": "Luxembourg",
}


def _decode_location(uri_or_code: str) -> str:
    """Convert a Publications Office place URI or bare code to a city name."""
    code = uri_or_code.rstrip("/").split("/")[-1]
    return PLACE_LOOKUP.get(code, code)


def _label_from_meeting_id(sid: str, date_val: str) -> str:
    """Generate a human-readable session label from the meeting ID and date."""
    try:
        from datetime import date as _date
        y, m, d = int(sid[7:11]), int(sid[12:14]), int(sid[15:17])
        return _date(y, m, d).strftime("Plenary – %-d %B %Y")
    except Exception:
        if date_val:
            return f"Plenary – {date_val}"
        return sid

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
      - a language-keyed dict: {"en": "Economic Affairs", "fr": "...", ...}
    """
    if isinstance(v, list):
        # Prefer English, otherwise take first
        for item in v:
            if isinstance(item, dict) and item.get("@language", "").startswith("en"):
                return str(item.get("@value", "")).strip()
        v = v[0] if v else ""
    if isinstance(v, dict):
        if "@value" in v or "value" in v:
            return str(v.get("@value", v.get("value", ""))).strip()
        # Language-keyed dict from EP API: {"en": "...", "fr": "...", ...}
        if "en" in v:
            return str(v["en"]).strip()
        for val in v.values():
            if isinstance(val, str) and val:
                return val.strip()
        return ""
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

        # Social media — try several field names used by the EP API
        social = {}
        home_links = (d.get("homePage") or d.get("homepage")
                      or d.get("sameAs") or d.get("owl:sameAs")
                      or d.get("foaf:homepage") or d.get("schema:sameAs") or [])
        if isinstance(home_links, str):
            home_links = [home_links]
        for link in home_links:
            if isinstance(link, dict):
                u = str(link.get("url", link.get("@id", link.get("id", ""))))
            else:
                u = str(link)
            if not u:
                continue
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
        # notation / skos:notation holds the numeric body ID (e.g. "6562")
        body_id = safe_str(
            c.get("notation", c.get("skos:notation",
            c.get("identifier", c.get("id", "")))))
        # prefLabel / skos:prefLabel holds the short abbreviation (AFET, ECON…)
        abbr = safe_label(
            c.get("prefLabel", c.get("skos:prefLabel",
            c.get("label", ""))))
        if not body_id:
            continue
        full_name = COMMITTEE_FULL_NAMES.get(abbr, abbr) if abbr else body_id
        committees.append({
            "id":           body_id,
            "abbreviation": abbr or body_id,
            "name":         full_name,
            "ep_url":       f"{EP_WEBSITE}/committees/en/{body_id.lower()}/home",
        })
        # Build lookup keyed by body_id so fetch_meps() can enrich committee entries.
        # Bug fix: previously the condition "numeric_id != body_id" meant the lookup
        # was never populated (they're always equal). Now we always add the entry.
        lookup_val = {"abbreviation": abbr or body_id, "name": full_name}
        org_id_lookup[body_id] = lookup_val
        # Also index by the @id URI numeric fragment in case it differs.
        uri = safe_str(c.get("@id", ""))
        if "/" in uri:
            numeric_id = uri.rstrip("/").split("/")[-1]
            if numeric_id and numeric_id not in org_id_lookup:
                org_id_lookup[numeric_id] = lookup_val
    log.info("  -> %d committees kept, %d lookup entries", len(committees), len(org_id_lookup))
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
        if isinstance(loc_raw, dict):
            location = safe_label(loc_raw)
        else:
            raw_str = safe_str(loc_raw)
            location = _decode_location(raw_str) if raw_str else ""

        raw_label = safe_label(s.get("label", ""))
        label = raw_label if raw_label and raw_label != sid else _label_from_meeting_id(sid, date_val)

        sessions.append({
            "id":       sid,
            "label":    label,
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
    _debug_vote_saved = False
    for mid in meeting_ids:
        if len(votes) >= MAX_VOTES:
            break
        raw = get_all(f"meetings/{mid}/vote-results",
                      max_items=MAX_VOTES - len(votes))
        if raw and not _debug_vote_saved:
            write_json(OUTPUT_DIR / "debug_vote_sample.json", raw[:3])
            log.info("  Saved debug vote sample (%d fields in first item)", len(raw[0]))
            _debug_vote_saved = True
        for v in raw:
            # outcome: try many field name variants used across API versions
            outcome_raw = (v.get("had_decision_outcome")
                           or v.get("decision_method")
                           or v.get("eli-dl:had_decision_outcome")
                           or v.get("outcome")
                           or v.get("result")
                           or v.get("decision")
                           or "")
            if isinstance(outcome_raw, dict):
                outcome = (safe_label(outcome_raw.get("label",
                           outcome_raw.get("prefLabel", "")))
                           or safe_str(outcome_raw.get("@id", "")).split("/")[-1])
            elif isinstance(outcome_raw, str) and "/" in outcome_raw:
                outcome = outcome_raw.split("/")[-1]
            else:
                outcome = safe_str(outcome_raw)

            vote_id = safe_str(v.get("activity_id",
                           v.get("identifier", v.get("id", ""))))
            # Extract date from the ID if not present as a field
            # ID format: MTG-PL-2025-05-07-VOT-ITM-965514
            raw_date = safe_str(v.get("activity_date",
                        v.get("date", v.get("eli-dl:activity_date",
                        v.get("startDate", "")))))
            if not raw_date and vote_id:
                import re as _re
                m = _re.search(r'(\d{4}-\d{2}-\d{2})', vote_id)
                if m:
                    raw_date = m.group(1)

            # Build EP vote URL from date if not in response
            ep_url = safe_str(v.get("seeAlso", v.get("url", v.get("@id", ""))))
            if not ep_url and raw_date:
                ep_url = (f"https://www.europarl.europa.eu/doceo/document/"
                          f"PV-10-{raw_date}-RCV_EN.html")

            votes.append({
                "id":         vote_id,
                "date":       raw_date,
                "title":      safe_label(v.get("activity_label",
                              v.get("label", v.get("prefLabel",
                              v.get("skos:prefLabel", v.get("rdfs:label",
                              v.get("eli-dl:activity_label",
                              v.get("title", v.get("dcterms:title",
                              v.get("name", "")))))))))),
                "result":     outcome,
                "for":        _int(v.get("number_of_votes_favor",
                              v.get("had_voter_favor",
                              v.get("votesFor", v.get("for", 0))))),
                "against":    _int(v.get("number_of_votes_against",
                              v.get("had_voter_against",
                              v.get("votesAgainst", v.get("against", 0))))),
                "abstention": _int(v.get("number_of_votes_abstention",
                              v.get("had_voter_abstention",
                              v.get("abstentions", v.get("abstention", 0))))),
                "ep_ref":     safe_str(v.get("notation", v.get("reference", ""))),
                "ep_url":     ep_url,
            })

    log.info("  -> %d votes total", len(votes))
    return sorted(votes, key=lambda x: x.get("date", ""), reverse=True)


def fetch_votes_xml():
    """Fetch roll-call votes from EP published XML vote records.

    URL: https://www.europarl.europa.eu/doceo/document/PV-10-{date}-RCV_EN.xml

    These files contain vote titles, ADOPTED/REJECTED results, and per-MEP
    For/Against/Abstain positions keyed by PersId.

    Returns (votes_list, mep_votes_dict) where mep_votes_dict maps
    mep_id -> list of {vote_id, date, title, position, result, ep_url}.
    """
    import re
    log.info("Fetching roll-call votes from EP XML files...")

    years = _lookback_years()
    meeting_dates = set()
    for year in years:
        raw = get_all("meetings", params={"year": year})
        for s in raw:
            d = safe_str(s.get("activity_date", s.get("startDate", "")))[:10]
            if re.match(r"\d{4}-\d{2}-\d{2}", d):
                meeting_dates.add(d)

    log.info("  -> %d candidate dates for XML fetch", len(meeting_dates))

    votes = []
    mep_votes = {}
    xml_hdr = {
        "Accept": "application/xml, text/xml, */*",
        "User-Agent": (
            "Mozilla/5.0 (compatible; EP-Tracker/1.0; "
            "+https://github.com/EU-Parliament-Tracker/ep-tracker)"
        ),
        "Referer": "https://www.europarl.europa.eu/plenary/en/minutes.html",
        "Accept-Language": "en-US,en;q=0.9",
    }
    xml_ok = 0
    xml_fail_status: dict = {}

    for meeting_date in sorted(meeting_dates, reverse=True):
        if len(votes) >= MAX_VOTES:
            break

        xml_url = (
            f"https://www.europarl.europa.eu/doceo/document/"
            f"PV-10-{meeting_date}-RCV_EN.xml"
        )
        try:
            resp = SESSION.get(xml_url, timeout=TIMEOUT, headers=xml_hdr)
            if resp.status_code == 404:
                xml_fail_status[404] = xml_fail_status.get(404, 0) + 1
                continue
            if not resp.ok:
                xml_fail_status[resp.status_code] = xml_fail_status.get(resp.status_code, 0) + 1
                log.warning("  XML %s → HTTP %d for %s", xml_url, resp.status_code, meeting_date)
                continue
            resp.raise_for_status()
            xml_ok += 1
        except Exception as e:
            log.warning("  XML request failed for %s: %s", meeting_date, e)
            continue

        try:
            root = ET.fromstring(resp.content)
        except ET.ParseError as e:
            log.warning("  XML parse error for %s: %s", meeting_date, e)
            continue

        # Save the first successfully parsed XML so we can inspect the schema
        if xml_ok == 1:
            debug_xml_path = OUTPUT_DIR / "debug_vote_xml_raw.xml"
            debug_xml_path.write_bytes(resp.content)
            children = [c.tag for c in root]
            log.info("  Saved debug XML (%s). Root: %r, children: %s",
                     meeting_date, root.tag, children)

        log.info("  -> Parsing XML for %s", meeting_date)
        ep_url = (
            f"https://www.europarl.europa.eu/doceo/document/"
            f"PV-10-{meeting_date}-RCV_EN.html"
        )

        items = (
            root.findall(".//RollCallVote.Result")
            or root.findall(".//RollCallVote.Result.Item")
            or root.findall(".//Vote.Result.Item")
            or root.findall(".//VoteResult.Item")
            or root.findall(".//Vote")
        )

        if not items:
            first_child = list(root)
            log.warning(
                "  XML %s: downloaded OK but found 0 vote items. "
                "Root tag: %r, first child tag: %r",
                meeting_date,
                root.tag,
                first_child[0].tag if first_child else "(no children)",
            )

        for item in items:
            # Title — prefer English language tag
            title = ""
            for tag in ("RollCallVote.Result.Description.Text",
                        "RollCallVote.Description.Text",
                        "Description.Text", "Description", "Title"):
                for el in item.findall(tag):
                    if el.get("language", "EN").upper() == "EN":
                        title = (el.text or "").strip()
                        break
                if title:
                    break

            # Result — old format: Result="+"/"−" attribute; new (10th term) format: derive
            result_attr = item.get("Result", "")
            if result_attr == "+":
                result = "Adopted"
            elif result_attr == "-":
                result = "Rejected"
            elif result_attr:
                result = result_attr
            else:
                result = ""  # derived from counts below after they are computed
                res_el = item.find("Result")
                if res_el is not None:
                    t = (res_el.text or "").strip().lower()
                    result = "Adopted" if "adopt" in t else "Rejected" if "reject" in t else (res_el.text or "").strip()

            # Vote identifier
            ident = item.get("Identifier", item.get("Id", ""))
            if ident:
                vote_id = f"{meeting_date}-RCV-{ident}"
            else:
                import hashlib
                vote_id = f"{meeting_date}-RCV-{hashlib.md5(title.encode()).hexdigest()[:8]}"

            # Per-position MEP IDs and counts
            # Supports two XML formats:
            #   old (9th term): <Votes.Plus Count="N"><Member.Name PersId="...">
            #   new (10th term): <Result.For Number="N"><Result.PoliticalGroup.List>
            def _parse_pos(tag):
                el = item.find(tag)
                if el is None:
                    return 0, set()
                cnt = _int(el.get("Number", el.get("Count", 0)))
                ids = {m.get("PersId", "") for m in el.findall(".//Member.Name")
                       if m.get("PersId", "")}
                return cnt or len(ids), ids

            # Try new format first, fall back to old format
            for_count,     for_ids = _parse_pos("Result.For")
            against_count, ag_ids  = _parse_pos("Result.Against")
            abstain_count, ab_ids  = _parse_pos("Result.Abstention")
            if not for_count:     for_count,     for_ids = _parse_pos("Votes.Plus")
            if not against_count: against_count, ag_ids  = _parse_pos("Votes.Minus")
            if not abstain_count: abstain_count, ab_ids  = _parse_pos("Votes.Abstention")

            # ultimate fallback to summary attributes on the item element
            for_count     = for_count     or _int(item.get("NumberOfVotesFor", 0))
            against_count = against_count or _int(item.get("NumberOfVotesAgainst", 0))
            abstain_count = abstain_count or _int(item.get("NumberOfAbstentions", 0))

            # derive result from counts if not set by attribute
            if not result and (for_count or against_count):
                result = "Adopted" if for_count > against_count else "Rejected"

            votes.append({
                "id":         vote_id,
                "date":       meeting_date,
                "title":      title,
                "result":     result,
                "for":        for_count,
                "against":    against_count,
                "abstention": abstain_count,
                "ep_ref":     "",
                "ep_url":     ep_url,
            })

            summary = {"vote_id": vote_id, "date": meeting_date,
                       "title": title, "result": result, "ep_url": ep_url}
            for mid in for_ids:
                mep_votes.setdefault(mid, []).append(dict(summary, position="for"))
            for mid in ag_ids:
                mep_votes.setdefault(mid, []).append(dict(summary, position="against"))
            for mid in ab_ids:
                mep_votes.setdefault(mid, []).append(dict(summary, position="abstain"))

        time.sleep(RATE_LIMIT)

    log.info("  -> XML results: %d files parsed, %d votes, %d MEPs with positions",
             xml_ok, len(votes), len(mep_votes))
    if xml_fail_status:
        log.warning("  -> XML failures by HTTP status: %s", xml_fail_status)
    return (
        sorted(votes, key=lambda x: x.get("date", ""), reverse=True)[:MAX_VOTES],
        mep_votes,
    )


# ---------------------------------------------------------------------------
# Documents
# ---------------------------------------------------------------------------

def _parse_doc(d, doc_type):
    work_type = safe_str(d.get("work_type", doc_type))
    if "adopted" in work_type.lower():
        doc_type = "adopted"
    doc_id = safe_str(d.get("work_id", d.get("identifier", d.get("id", ""))))
    ref = safe_str(d.get("notation", d.get("reference", d.get("eli:notation", ""))))
    # title_dcterms is the primary title field; label is often the reference notation
    raw_title = safe_label(d.get("title_dcterms",
                d.get("eli:title", d.get("dcterms:title",
                d.get("title", d.get("label", ""))))))
    # If title is same as ref/id (i.e. the API returned a reference notation as title),
    # keep it as the ref and leave title blank so the template can show a proper label.
    if raw_title and (raw_title == ref or raw_title.replace("-", "").replace("/", "") ==
                      doc_id.replace("-", "").replace("/", "")):
        ref = ref or raw_title
        raw_title = ""
    return {
        "id":     doc_id,
        "type":   doc_type,
        "date":   safe_str(d.get("document_date",
                  d.get("work_date_document",
                  d.get("date_document",
                  d.get("eli:date_document", d.get("date", "")))))),
        "title":  raw_title,
        "ref":    ref or doc_id,
        "ep_url": safe_str(d.get("seeAlso", d.get("url", ""))),
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
    _debug_saved = False
    for year in years:
        batch = get_all("plenary-documents",
                        params={"year": year},
                        max_items=MAX_DOCUMENTS)
        if batch and not _debug_saved:
            write_json(OUTPUT_DIR / "debug_document_sample.json", batch[:3])
            log.info("  Saved debug document sample (%d fields)", len(batch[0]))
            _debug_saved = True
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

def _parse_question_record(q: dict) -> dict:
    """Extract all fields from a question record (bulk or detail response)."""
    qid = safe_str(q.get("work_id", q.get("identifier", q.get("id", ""))))
    if "/" in qid:
        qid = qid.split("/")[-1]

    qtype = safe_str(q.get("work_type", q.get("questionType",
            q.get("type", q.get("eli:work_type", "")))))
    if "written" in qtype.lower():
        qtype = "Written Question"
    elif "oral" in qtype.lower():
        qtype = "Oral Question"

    authors_raw = (q.get("was_created_by")
                   or q.get("created_by")
                   or q.get("author")
                   or q.get("creator")
                   or q.get("eli:author")
                   or q.get("dcterms:creator")
                   or q.get("hasMember")
                   or [])
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
        if aid and aid.isdigit():
            author_ids.append(aid)

    return {
        "id":      qid,
        "date":    safe_str(q.get("document_date",
                   q.get("work_date_document",
                   q.get("date_document",
                   q.get("eli:date_document", q.get("date", "")))))),
        "title":   safe_label(q.get("title_dcterms",
                   q.get("eli:title", q.get("dcterms:title",
                   q.get("label", q.get("title", q.get("name", ""))))))),
        "type":    qtype,
        "ref":     safe_str(q.get("notation", q.get("reference", ""))),
        "authors": author_ids,
        "ep_url":  safe_str(q.get("seeAlso", q.get("url", ""))),
    }


def fetch_questions():
    """Fetch parliamentary questions using year= parameter.

    The bulk endpoint returns minimal data (id, type only), so each question
    is enriched with a per-record detail call — the same pattern used for MEPs.
    """
    log.info("Fetching parliamentary questions...")
    years = _lookback_years(QUESTIONS_LOOKBACK_DAYS)
    stub_list = []
    seen_ids: set = set()
    for year in years:
        batch = get_all("parliamentary-questions",
                        params={"year": year},
                        max_items=MAX_QUESTIONS)
        for q in batch:
            qid = safe_str(q.get("work_id", q.get("identifier", q.get("id", ""))))
            if "/" in qid:
                qid = qid.split("/")[-1]
            if qid and qid not in seen_ids:
                seen_ids.add(qid)
                qtype = safe_str(q.get("work_type", q.get("type", "")))
                stub_list.append({"id": qid, "raw_type": qtype})
        if len(stub_list) >= MAX_QUESTIONS:
            break

    log.info("  -> %d question stubs; fetching details…", len(stub_list))
    questions = []
    _debug_saved = False
    for stub in stub_list[:MAX_QUESTIONS]:
        qid = stub["id"]
        detail_resp = get_json(f"parliamentary-questions/{qid}")
        if detail_resp:
            data = detail_resp.get("data", [])
            record = (data[0] if isinstance(data, list) and data
                      else data if isinstance(data, dict) else {})
            if not _debug_saved and record:
                write_json(OUTPUT_DIR / "debug_question_detail.json", record)
                log.info("  Saved debug question detail (%d fields)", len(record))
                _debug_saved = True
        else:
            record = {}

        parsed = _parse_question_record(record)
        # Preserve ID and type from stub if detail was empty
        if not parsed["id"]:
            parsed["id"] = qid
        if not parsed["type"]:
            raw = stub["raw_type"]
            if "written" in raw.lower():
                parsed["type"] = "Written Question"
            elif "oral" in raw.lower():
                parsed["type"] = "Oral Question"
        questions.append(parsed)

    log.info("  -> %d questions", len(questions))
    return sorted(questions, key=lambda x: x.get("date", ""), reverse=True)[:MAX_QUESTIONS]


# ---------------------------------------------------------------------------
# MEP page stub generator
# ---------------------------------------------------------------------------

def generate_mep_stubs(meps: dict) -> int:
    """Write one _pages/meps/<id>.md stub per MEP so Jekyll can build
    individual pages without relying on the Ruby plugin."""
    stubs_dir = OUTPUT_DIR.parent / "_pages" / "meps"
    stubs_dir.mkdir(parents=True, exist_ok=True)

    def yaml_escape(s: str) -> str:
        return str(s).replace("\\", "\\\\").replace('"', '\\"')

    written = 0
    for mep_id, mep_data in meps.items():
        name = yaml_escape(mep_data.get("full_name", mep_id))
        desc = yaml_escape(
            f"{mep_data.get('full_name', '')} \u00b7 "
            f"{mep_data.get('group_name', '')} \u00b7 "
            f"{mep_data.get('country', '')}"
        )
        stub = (
            f'---\n'
            f'layout: mep\n'
            f'title: "{name}"\n'
            f'mep_id: "{mep_id}"\n'
            f'permalink: /meps/{mep_id}/\n'
            f'description: "{desc}"\n'
            f'---\n'
        )
        stub_path = stubs_dir / f"{mep_id}.md"
        stub_path.write_text(stub, encoding="utf-8")
        written += 1

    # Remove stubs for MEPs no longer in the dataset
    for existing in stubs_dir.glob("*.md"):
        if existing.stem not in meps:
            existing.unlink()

    return written


# ---------------------------------------------------------------------------
# Groups and MEP vote history helpers
# ---------------------------------------------------------------------------

def compute_groups(meps: dict) -> list:
    """Derive political-group seat counts from live MEP data."""
    GROUP_STYLE = {
        "EPP":        {"color": "#003399", "css": "epp"},
        "S&D":        {"color": "#C8001A", "css": "sd"},
        "PfE":        {"color": "#7C3E99", "css": "pfe"},
        "ECR":        {"color": "#0066CC", "css": "ecr"},
        "Renew":      {"color": "#F7931E", "css": "renew"},
        "Greens/EFA": {"color": "#50A045", "css": "greens-efa"},
        "GUE/NGL":    {"color": "#800000", "css": "gue-ngl"},
        "NI":         {"color": "#888888", "css": "ni"},
        "ESN":        {"color": "#002B7F", "css": "esn"},
    }
    counts, names = {}, {}
    for m in meps.values():
        abbr = m.get("group_abbr", "")
        if abbr:
            counts[abbr] = counts.get(abbr, 0) + 1
            names.setdefault(abbr, m.get("group_name", ""))
    groups = []
    for abbr, seats in sorted(counts.items(), key=lambda x: -x[1]):
        s = GROUP_STYLE.get(abbr, {
            "color": "#999999",
            "css": abbr.lower().replace("/", "-").replace("&", ""),
        })
        groups.append({"abbr": abbr, "name": names.get(abbr, abbr),
                       "seats": seats, "color": s["color"], "css": s["css"]})
    return groups


def write_mep_votes(mep_votes: dict) -> int:
    """Write _data/mep_votes/<id>.json for each MEP."""
    out_dir = OUTPUT_DIR / "mep_votes"
    out_dir.mkdir(parents=True, exist_ok=True)
    for mep_id, records in mep_votes.items():
        write_json(
            out_dir / f"{mep_id}.json",
            sorted(records, key=lambda x: x.get("date", ""), reverse=True),
        )
    log.info("  wrote vote history for %d MEPs", len(mep_votes))
    return len(mep_votes)


def _parse_voting_position(raw) -> str:
    """Normalise an EP API voting position value to 'for'/'against'/'abstain'."""
    s = safe_str(raw).lower()
    if not s:
        return ""
    # URI form: def/ep-voting-positions/FAVOR  /AGAINST  /ABSTENTION
    tail = s.split("/")[-1]
    if tail in ("favor", "for", "yes", "pour", "+"):
        return "for"
    if tail in ("against", "no", "contre", "-"):
        return "against"
    if tail in ("abstention", "abstain", "abstaining", "0"):
        return "abstain"
    return ""


def fetch_mep_votes_from_api(mep_ids: list) -> dict:
    """Fetch individual MEP vote positions via GET meps/{id}/voting-activities.

    Falls back gracefully if the endpoint doesn't exist or returns nothing.
    Returns mep_votes dict: {mep_id: [vote_record, ...]}.
    """
    log.info("Fetching per-MEP voting activities from API (%d MEPs)...", len(mep_ids))
    mep_votes: dict = {}
    _debug_saved = False

    # Probe the first MEP to check whether the endpoint works at all
    first_id = mep_ids[0] if mep_ids else None
    if first_id:
        probe = get_json(f"meps/{first_id}/voting-activities", params={"limit": 2})
        if probe is None:
            log.warning("  voting-activities endpoint returned None for MEP %s "
                        "(likely 404 — endpoint may not exist in API v2)", first_id)
            write_json(OUTPUT_DIR / "debug_mep_voting_probe.json", {"mep_id": first_id, "response": None})
            return {}
        write_json(OUTPUT_DIR / "debug_mep_voting_probe.json", probe)
        log.info("  Probe OK for MEP %s — top-level keys: %s", first_id, list(probe.keys()))

    for i, mep_id in enumerate(mep_ids):
        records_raw = get_all(
            f"meps/{mep_id}/voting-activities",
            params={"limit": PAGE_SIZE},
            max_items=500,
        )
        if not records_raw:
            continue

        if not _debug_saved:
            write_json(OUTPUT_DIR / "debug_mep_voting_activity.json", records_raw[:2])
            log.info("  Saved debug MEP voting activity sample (%d fields)", len(records_raw[0]))
            _debug_saved = True

        records = []
        for v in records_raw:
            vid = safe_str(v.get("activity_id", v.get("identifier", v.get("id", ""))))
            date_val = safe_str(v.get("activity_date",
                       v.get("eli-dl:activity_date", v.get("date", ""))))[:10]
            title = safe_label(v.get("activity_label",
                    v.get("label", v.get("eli-dl:activity_label", ""))))

            # Position: look for the MEP's specific vote value
            pos_raw = (v.get("voting_position")
                       or v.get("had_voting_position")
                       or v.get("position")
                       or v.get("votingPosition")
                       or "")
            position = _parse_voting_position(pos_raw)
            if not position:
                continue

            # Overall result
            outcome_raw = (v.get("had_decision_outcome")
                           or v.get("decision_outcome")
                           or v.get("result")
                           or "")
            outcome_str = safe_str(outcome_raw).lower()
            if "adopt" in outcome_str:
                result = "Adopted"
            elif "reject" in outcome_str:
                result = "Rejected"
            else:
                result = ""

            ep_url = safe_str(v.get("seeAlso", v.get("url", "")))
            if not ep_url and date_val:
                ep_url = (f"https://www.europarl.europa.eu/doceo/document/"
                          f"PV-10-{date_val}-RCV_EN.html")

            records.append({
                "vote_id":  vid,
                "date":     date_val,
                "title":    title,
                "position": position,
                "result":   result,
                "ep_url":   ep_url,
            })

        if records:
            mep_votes[mep_id] = records

        if (i + 1) % 50 == 0:
            log.info("  ... %d / %d MEPs processed", i + 1, len(mep_ids))

    log.info("  -> voting history fetched for %d MEPs", len(mep_votes))
    return mep_votes


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    import argparse
    parser = argparse.ArgumentParser(description="Fetch EU Parliament data")
    parser.add_argument(
        "--quick", action="store_true",
        help="Only fetch votes and sessions; skip MEPs, committees, documents, questions",
    )
    args = parser.parse_args()
    quick = args.quick

    log.info("=" * 60)
    log.info("EP Tracker data fetch — %s%s",
             date.today().isoformat(), "  [QUICK MODE]" if quick else "")
    log.info("=" * 60)
    log.info("Writing data to: %s", OUTPUT_DIR)

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    MEPS_DIR.mkdir(parents=True, exist_ok=True)

    errors = []
    counts = {}

    # In quick mode, carry over existing counts for skipped datasets so that
    # meta.json stays accurate for the datasets we didn't re-fetch.
    if quick:
        meta_path = OUTPUT_DIR / "meta.json"
        if meta_path.exists():
            try:
                existing = json.loads(meta_path.read_text(encoding="utf-8"))
                counts.update(existing.get("counts", {}))
            except Exception:
                pass

    # 1. Committees
    committee_lookup = {}
    if not quick:
        try:
            committees_data, committee_lookup = fetch_committees()
            write_json(OUTPUT_DIR / "committees.json", committees_data)
            counts["committees"] = len(committees_data)
        except Exception as e:
            log.error("committees fetch failed: %s", e)
            errors.append("committees")
            counts["committees"] = None
    else:
        log.info("Quick mode: skipping committees fetch")

    # 2. MEPs
    meps = {}
    if not quick:
        try:
            meps = fetch_meps(committee_lookup)
            write_json(OUTPUT_DIR / "meps.json", meps)
            counts["meps"] = len(meps)
            stub_count = generate_mep_stubs(meps)
            log.info("Generated %d MEP page stubs", stub_count)
        except Exception as e:
            log.error("meps fetch failed: %s", e)
            errors.append("meps")
            counts["meps"] = None
    else:
        log.info("Quick mode: skipping MEP fetch — loading existing meps.json")
        meps_path = OUTPUT_DIR / "meps.json"
        if meps_path.exists():
            try:
                meps = json.loads(meps_path.read_text(encoding="utf-8"))
                log.info("  loaded %d MEPs from existing data", len(meps))
            except Exception as e:
                log.warning("  could not load existing meps.json: %s", e)

    # 2b. Political group composition (derived from MEP data)
    if meps and not quick:
        try:
            groups = compute_groups(meps)
            write_json(OUTPUT_DIR / "groups.json", groups)
            counts["groups"] = len(groups)
        except Exception as e:
            log.error("groups compute failed: %s", e)

    # 3. Sessions
    try:
        sessions_data = fetch_sessions()
        write_json(OUTPUT_DIR / "sessions.json", sessions_data)
        counts["sessions"] = len(sessions_data)
    except Exception as e:
        log.error("sessions fetch failed: %s", e)
        errors.append("sessions")
        counts["sessions"] = None

    # 4. Votes — try XML first for real titles/results/MEP positions
    mep_votes: dict = {}
    try:
        votes_data, mep_votes = fetch_votes_xml()
        if not votes_data:
            log.warning("XML vote fetch empty — falling back to API")
            votes_data = fetch_votes()
            mep_votes = {}
        elif all(not v.get("title") for v in votes_data):
            log.warning(
                "XML returned %d votes but ALL have empty titles — "
                "possible tag/schema mismatch in fetch_votes_xml().",
                len(votes_data),
            )
        write_json(OUTPUT_DIR / "votes.json", votes_data)
        counts["votes"] = len(votes_data)
        if mep_votes:
            counts["mep_votes"] = write_mep_votes(mep_votes)
            log.info("Per-MEP votes written from XML (%d MEPs)", len(mep_votes))
    except Exception as e:
        log.error("votes fetch failed: %s", e)
        errors.append("votes")
        counts["votes"] = None

    # 4b. Per-MEP voting history — if XML gave no per-MEP data, try the API
    #     endpoint meps/{id}/voting-activities as an alternative source.
    if not mep_votes and meps:
        try:
            mep_votes = fetch_mep_votes_from_api(list(meps.keys()))
            if mep_votes:
                counts["mep_votes"] = write_mep_votes(mep_votes)
            else:
                log.warning("API voting-activities returned no data; MEP vote pages will be empty")
        except Exception as e:
            log.error("per-MEP vote API fetch failed: %s", e)

    # 5. Documents and questions
    if not quick:
        other_datasets = [
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
    else:
        log.info("Quick mode: skipping documents and questions fetch")

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
