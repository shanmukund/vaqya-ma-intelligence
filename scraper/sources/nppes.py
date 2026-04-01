"""
NPPES NPI Registry scraper — 100% free, no API key required.

The National Plan and Provider Enumeration System (NPPES) is maintained by CMS
and publicly accessible. Medical billing and RCM companies that submit claims
on behalf of providers must register here.

API: https://npiregistry.cms.hhs.gov/api/
- No key, no account, no rate limit posted (we self-limit to 1 req/sec)
- NPI Type 2 = Organizations (billing companies, groups, practices)
- 200 results per page, paginate with &skip=N
- Structured JSON: name, address, city, state, zip, phone, taxonomy

Expected yield: 3,000–8,000 active RCM/billing organizations nationwide.
"""

from __future__ import annotations
import time
import uuid
import random
import requests
from datetime import datetime, timezone
from tenacity import retry, wait_exponential, stop_after_attempt
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
from config import TARGET_METROS, RATE_LIMITS

NPPES_API_URL = "https://npiregistry.cms.hhs.gov/api/"
DELAY         = 1.2   # seconds between requests — respectful self-throttle
PAGE_LIMIT    = 200   # max per page (API ceiling)
MAX_PAGES     = 6     # per query — 6 × 200 = 1,200 records per keyword
MAX_PAGES_TOTAL = 80  # hard cap on total API calls across all queries

# NPPES org-name search keywords that identify RCM / billing companies
NPPES_QUERIES = [
    "medical billing",
    "revenue cycle",
    "billing solutions",
    "billing services",
    "physician billing",
    "healthcare billing",
    "billing group",
    "coding services",
    "billing and coding",
    "rcm solutions",
    "claims management",
    "billing specialists",
    "billing associates",
    "medical coding",
    "billing consultants",
]

# Taxonomy descriptions that confirm a billing/RCM organization
BILLING_TAXONOMY_KEYWORDS = [
    "billing", "revenue cycle", "rcm", "coding", "claims", "reimbursement",
]

# Name fragments that indicate a non-target (provider, hospital, insurer)
EXCLUDE_NAME_FRAGMENTS = [
    "hospital", "health system", "medical center", "university", "university of",
    "county", "city of", "department of", "school of", "college of",
    "insurance", "hmo", "blue cross", "aetna", "cigna", "humana", "united health",
    "pharmacy", "drug store", "cvs ", "walgreen",
]

# Build a metro lookup: state → list of metro names (for region assignment)
_STATE_METROS: dict[str, list[str]] = {}
for _m in TARGET_METROS:
    _STATE_METROS.setdefault(_m["state"], []).append(_m["metro"])


def _metro_for(state: str, city: str) -> str:
    """Best-effort metro region for a given state + city."""
    city_lower = city.lower()
    for m in TARGET_METROS:
        if m["state"] == state and m["metro"].lower() in city_lower:
            return m["metro"]
    # Fall back to first known metro in that state
    return _STATE_METROS.get(state, [city or state])[0]


def _is_billing_name(name: str) -> bool:
    """True if the org name looks like a billing/RCM company (not a provider)."""
    name_lower = name.lower()
    if any(ex in name_lower for ex in EXCLUDE_NAME_FRAGMENTS):
        return False
    return any(kw in name_lower for kw in [
        "billing", "revenue cycle", "rcm", "coding", "claims", "reimbursement",
        "ar services", "accounts receivable",
    ])


def _normalize(record: dict) -> dict:
    """Convert one NPPES API result → our standard company dict."""
    basic   = record.get("basic") or {}
    addrs   = record.get("addresses") or []
    taxons  = record.get("taxonomies") or []

    name = basic.get("organization_name") or basic.get("name") or ""
    if not name:
        return {}

    # Primary address — prefer "LOCATION" type, fall back to first
    addr_obj = next((a for a in addrs if a.get("address_purpose") == "LOCATION"), None)
    if not addr_obj and addrs:
        addr_obj = addrs[0]
    addr_obj = addr_obj or {}

    city    = addr_obj.get("city", "").title()
    state   = addr_obj.get("state", "").upper()[:2]
    zip_    = addr_obj.get("postal_code", "")[:5]
    phone   = addr_obj.get("telephone_number", "")
    street  = addr_obj.get("address_1", "")
    if addr_obj.get("address_2"):
        street += " " + addr_obj["address_2"]

    metro   = _metro_for(state, city)

    # Taxonomy
    spec_list = []
    for t in taxons:
        desc = (t.get("desc") or "").lower()
        if any(kw in desc for kw in BILLING_TAXONOMY_KEYWORDS):
            spec_list.append(t.get("desc", ""))
    if not spec_list and taxons:
        spec_list = [taxons[0].get("desc", "")]

    now = datetime.now(timezone.utc).isoformat()
    return {
        "id":                   str(uuid.uuid4()),
        "company_name":         name.title(),
        "website":              "",
        "phone":                phone,
        "address":              street,
        "city":                 city,
        "state":                state,
        "metro_region":         metro,
        "zip":                  zip_,
        "estimated_revenue":    None,
        "revenue_band":         "Unknown",
        "employee_count_range": None,
        "employee_count_est":   None,
        "founded_year":         None,
        "company_age":          None,
        "owner_signals":        ["npi_registered"],
        "specialties":          spec_list,
        "technology_signals":   [],
        "pe_backed":            False,
        "offshore_mentions":    False,
        "multi_state":          False,
        "recent_funding":       False,
        "job_posting_count":    0,
        "job_titles_found":     [],
        "source":               ["nppes"],
        "data_confidence":      "high",
        "pipeline_stage":       "Prospect",
        "assigned_to":          None,
        "priority":             None,
        "notes":                [],
        "next_action":          None,
        "next_action_due":      None,
        "contacts":             [],
        "date_added":           now,
        "date_updated":         now,
        "_npi_number":          record.get("number", ""),
    }


@retry(wait=wait_exponential(min=2, max=30), stop=stop_after_attempt(4))
def _fetch_page(query: str, skip: int) -> dict:
    params = {
        "version":          "2.1",
        "organization_name": query,
        "enumeration_type":  "NPI-2",
        "limit":             PAGE_LIMIT,
        "skip":              skip,
    }
    resp = requests.get(
        NPPES_API_URL, params=params,
        timeout=20,
        headers={"User-Agent": "Vaqya-MA-Research/1.0 (internal; contact: ops@vaqya.com)"},
    )
    resp.raise_for_status()
    return resp.json()


def scrape(queries: list[str] | None = None) -> list[dict]:
    """
    Query the NPPES NPI Registry for billing/RCM organizations nationwide.
    Returns normalized company dicts. No API key needed.
    """
    queries = queries or NPPES_QUERIES
    results: list[dict] = []
    seen_npis: set[str] = set()
    total_calls = 0

    for query in queries:
        page = 0
        while page < MAX_PAGES and total_calls < MAX_PAGES_TOTAL:
            skip = page * PAGE_LIMIT
            print(f"  [nppes] '{query}' page {page+1} (skip={skip}) — calls so far: {total_calls}")

            try:
                data = _fetch_page(query, skip)
            except Exception as e:
                print(f"    [nppes] API error: {e}")
                break

            records = data.get("results") or []
            if not records:
                break

            page_hits = 0
            for rec in records:
                npi = rec.get("number", "")
                if npi in seen_npis:
                    continue
                seen_npis.add(npi)

                name = (rec.get("basic") or {}).get("organization_name", "")
                if not _is_billing_name(name):
                    continue

                normalized = _normalize(rec)
                if normalized and normalized.get("company_name"):
                    results.append(normalized)
                    page_hits += 1

            total_calls += 1
            print(f"    → {page_hits} billing orgs this page (running total: {len(results)})")

            # If fewer records than limit, we've hit the last page
            if len(records) < PAGE_LIMIT:
                break

            page += 1
            time.sleep(DELAY + random.uniform(0, 0.4))

        # Brief pause between queries
        time.sleep(DELAY)

    print(f"[nppes] Complete: {len(results)} billing organizations, {total_calls} API calls")
    return results
