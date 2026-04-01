"""
SearchAPI.io — Google Maps Local Search scraper.

Free tier: 100 lifetime credits (one-time on signup, no card required).
Sign up:   https://www.searchapi.io/
Docs:      https://www.searchapi.io/docs/google-maps

Strategy — why tier 2 metros:
  Major metros (NY, LA, Chicago, etc.) are dominated by PE-backed roll-ups
  and large national RCM firms — expensive and hard to acquire.
  Tier 2 cities have far more owner-operated shops, lower valuations,
  and founders actively looking for exits. This is where Vaqya's
  offshoring + automation value proposition wins.

Credit math:
  100 lifetime free credits
  4 queries × 25 tier-2 metros = 100 calls → one complete tier-2 sweep
  Hard run cap: 95 (keeps 5-credit safety buffer)

Each call returns up to 20 local business listings with:
  name, phone, website, address, rating, review count, business type
"""

from __future__ import annotations
import time
import uuid
import random
import requests
from datetime import datetime, timezone
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
from config import SEARCHAPI_KEY, TIER2_METROS, RATE_LIMITS, FREE_TIER_CAPS

_DELAY   = RATE_LIMITS["google_maps"]["delay_seconds"]
_RUN_CAP = FREE_TIER_CAPS["searchapi"]["run_cap"]

SEARCHAPI_URL = "https://www.searchapi.io/api/v1/search"

# 4 focused queries — highest RCM company density per credit spent
SEARCHAPI_QUERIES = [
    "medical billing company",
    "revenue cycle management company",
    "physician billing services",
    "healthcare billing services",
]

# Filter out non-billing businesses that appear in generic searches
EXCLUDE_NAMES = [
    "hospital", "health system", "medical center", "urgent care", "clinic",
    "pharmacy", "laboratory", "radiology group", "insurance company",
    "staffing", "recruiting", "temp agency",
]


def _is_relevant(name: str) -> bool:
    """Keep only RCM / medical billing businesses."""
    nl = name.lower()
    if any(ex in nl for ex in EXCLUDE_NAMES):
        return False
    return any(kw in nl for kw in [
        "billing", "rcm", "revenue cycle", "coding", "medical billing",
        "healthcare billing", "reimbursement", "claims management",
        "accounts receivable", "denial management",
    ])


def _searchapi_maps(query: str, lat: float, lng: float) -> list[dict]:
    """Single SearchAPI.io Google Maps call — costs 1 credit."""
    params = {
        "engine":  "google_maps",
        "q":       query,
        "ll":      f"@{lat},{lng},14z",
        "type":    "search",
        "api_key": SEARCHAPI_KEY,
    }
    try:
        resp = requests.get(SEARCHAPI_URL, params=params, timeout=20)
        resp.raise_for_status()
        data = resp.json()
        return data.get("local_results") or []
    except Exception as e:
        print(f"  [google_maps] SearchAPI error: {e}")
        return []


def _normalize(item: dict, metro: str, state: str) -> dict:
    """Convert a SearchAPI Google Maps result to the standard company dict."""
    now     = datetime.now(timezone.utc).isoformat()
    address = item.get("address") or ""

    # Parse city/state from "123 Main St, Charlotte, NC 28202, USA"
    city         = metro
    parsed_state = state
    parts = [p.strip() for p in address.split(",")]
    if len(parts) >= 3:
        city = parts[-3].strip()
    if len(parts) >= 2:
        state_zip = parts[-2].strip()          # e.g. "NC 28202"
        sp = state_zip.split()
        if sp:
            parsed_state = sp[0][:2].upper()

    return {
        "id":                   str(uuid.uuid4()),
        "company_name":         item.get("title") or "",
        "website":              item.get("website") or "",
        "phone":                item.get("phone") or "",
        "address":              address,
        "city":                 city,
        "state":                parsed_state,
        "metro_region":         metro,
        "zip":                  "",
        "estimated_revenue":    None,
        "revenue_band":         "Unknown",
        "employee_count_range": None,
        "employee_count_est":   None,
        "founded_year":         None,
        "company_age":          None,
        "owner_signals":        [],
        "specialties":          [],
        "technology_signals":   [],
        "pe_backed":            False,
        "offshore_mentions":    False,
        "multi_state":          False,
        "recent_funding":       False,
        "job_posting_count":    0,
        "job_titles_found":     [],
        "source":               ["google_maps"],
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
        # Internal enrichment fields
        "_google_rating":       item.get("rating"),
        "_google_reviews":      item.get("reviews"),
        "_google_place_id":     item.get("place_id") or "",
    }


def scrape(metros: list[dict] | None = None, queries: list[str] | None = None) -> list[dict]:
    """
    Scrape Google Maps via SearchAPI.io for RCM companies.

    Defaults to TIER2_METROS (skips major metros dominated by PE-backed firms).
    Hard cap: 95 calls/run to protect the 100 lifetime free credits.

    metros  — override metro list (default: TIER2_METROS)
    queries — override queries (default: SEARCHAPI_QUERIES)
    """
    if SEARCHAPI_KEY == "YOUR_SEARCHAPI_KEY":
        print("[google_maps] SEARCHAPI_KEY not set — skipping.")
        print("             Get 100 free credits at: https://www.searchapi.io/")
        return []

    metros  = metros  or TIER2_METROS
    queries = queries or SEARCHAPI_QUERIES

    total_possible = len(metros) * len(queries)
    capped_total   = min(total_possible, _RUN_CAP)

    print(f"[google_maps] SearchAPI.io — {len(metros)} tier-2 metros × "
          f"{len(queries)} queries = {total_possible} calls planned "
          f"(hard cap: {_RUN_CAP})")
    print(f"  Tier 2 focus: owner-operated shops, lower PE competition, better M&A targets")

    all_results: list[dict] = []
    call_count = 0

    for metro_info in metros:
        metro = metro_info["metro"]
        state = metro_info["state"]
        lat   = metro_info["lat"]
        lng   = metro_info["lng"]

        for query in queries:
            if call_count >= _RUN_CAP:
                print(f"  [google_maps] Cap ({_RUN_CAP}) reached — "
                      f"stopping to protect remaining free credits.")
                return all_results

            done = call_count + 1
            print(f"  [google_maps] {done}/{capped_total} — "
                  f"'{query}' in {metro}, {state} [{call_count}/{_RUN_CAP} credits]")

            items = _searchapi_maps(query, lat, lng)
            call_count += 1

            hits = 0
            for item in items:
                name = item.get("title", "")
                if name and _is_relevant(name):
                    all_results.append(_normalize(item, metro, state))
                    hits += 1

            print(f"    → {hits} RCM matches of {len(items)} results")
            time.sleep(_DELAY + random.uniform(0, 0.5))

    print(f"[google_maps] Complete: {len(all_results)} companies, "
          f"{call_count} credits used ({100 - call_count} of 100 remaining approx)")
    return all_results
