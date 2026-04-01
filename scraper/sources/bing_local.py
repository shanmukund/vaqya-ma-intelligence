"""
Bing Maps Local Search API scraper.
Free tier: 125,000 transactions/year — no credit card required.

Free tier math (per weekly run):
  47 metros × 4 queries = 188 calls/run
  188 × 52 weeks        = 9,776 calls/year  (7.8% of 125K free limit)
  Hard run cap set to 300 to prevent accidental overuse.

Sign up: https://www.bingmapsportal.com/ → Create key → Basic (free)
Docs:    https://docs.microsoft.com/en-us/bingmaps/rest-services/locations/local-search
"""

from __future__ import annotations
import time
import uuid
import random
import requests
from datetime import datetime, timezone
from tenacity import retry, wait_exponential, stop_after_attempt
from fake_useragent import UserAgent
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
from config import BING_MAPS_API_KEY, TARGET_METROS, RATE_LIMITS, FREE_TIER_CAPS

_DELAY     = RATE_LIMITS["bing_local"]["delay_seconds"]
_RUN_CAP   = FREE_TIER_CAPS["bing_maps"]["run_cap"]       # hard stop per run
_ANNUAL    = FREE_TIER_CAPS["bing_maps"]["annual_limit"]   # for reference logging
_ua        = UserAgent()

BING_LOCAL_URL = "https://dev.virtualearth.net/REST/v1/LocalSearch/"

# 4 targeted queries — cover the key RCM search intents
BING_QUERIES = [
    "medical billing company",
    "revenue cycle management",
    "physician billing services",
    "healthcare billing",
]

# Name-level filters: drop listings that aren't billing companies
EXCLUDE_NAMES = [
    "hospital", "health system", "medical center", "urgent care", "clinic",
    "pharmacy", "laboratory", "radiology", "insurance", "staffing", "recruiting",
]


def _is_relevant(name: str) -> bool:
    nl = name.lower()
    if any(ex in nl for ex in EXCLUDE_NAMES):
        return False
    return any(kw in nl for kw in [
        "billing", "rcm", "revenue cycle", "coding", "medical", "healthcare",
        "reimbursement", "claims",
    ])


@retry(wait=wait_exponential(min=2, max=20), stop=stop_after_attempt(3))
def _bing_search(query: str, lat: float, lng: float, max_results: int = 25) -> list[dict]:
    """Single Bing Local Search API call — counts as 1 transaction."""
    params = {
        "query":        query,
        "userLocation": f"{lat},{lng}",
        "maxResults":   max_results,
        "key":          BING_MAPS_API_KEY,
        "output":       "json",
    }
    resp = requests.get(
        BING_LOCAL_URL, params=params, timeout=15,
        headers={"User-Agent": _ua.random},
    )
    resp.raise_for_status()
    data = resp.json()
    return (data.get("resourceSets") or [{}])[0].get("resources") or []


def _normalize(item: dict, metro: str, state: str) -> dict:
    now     = datetime.now(timezone.utc).isoformat()
    address = item.get("Address") or {}
    return {
        "id":                   str(uuid.uuid4()),
        "company_name":         item.get("name") or "",
        "website":              item.get("Website") or item.get("url") or "",
        "phone":                item.get("PhoneNumber") or "",
        "address":              address.get("addressLine", ""),
        "city":                 address.get("locality", metro),
        "state":                address.get("adminDistrict", state).upper()[:2],
        "metro_region":         metro,
        "zip":                  address.get("postalCode", ""),
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
        "source":               ["bing_local"],
        "data_confidence":      "medium",
        "pipeline_stage":       "Prospect",
        "assigned_to":          None,
        "priority":             None,
        "notes":                [],
        "next_action":          None,
        "next_action_due":      None,
        "contacts":             [],
        "date_added":           now,
        "date_updated":         now,
    }


def scrape(metros: list[dict] | None = None) -> list[dict]:
    """
    Scrape Bing Local Business API for RCM companies.
    Requires BING_MAPS_API_KEY (free, 125K transactions/year).
    Enforces a hard per-run cap to stay comfortably within the free tier.
    """
    if BING_MAPS_API_KEY == "YOUR_BING_MAPS_API_KEY":
        print("[bing_local] BING_MAPS_API_KEY not set — skipping.")
        print("             Get a free key at: https://www.bingmapsportal.com/")
        return []

    metros        = metros or TARGET_METROS
    all_results:  list[dict] = []
    request_count = 0
    total         = len(metros) * len(BING_QUERIES)
    done          = 0

    # Show free-tier budget at start of run
    projected = len(metros) * len(BING_QUERIES)
    pct_annual = (projected * 52 / _ANNUAL) * 100
    print(f"  [bing_local] Free tier budget: {projected} calls this run "
          f"→ {projected * 52:,}/yr = {pct_annual:.1f}% of {_ANNUAL:,} free limit")

    for metro_info in metros:
        metro = metro_info["metro"]
        state = metro_info["state"]
        lat   = metro_info["lat"]
        lng   = metro_info["lng"]

        for query in BING_QUERIES:
            done += 1

            if request_count >= _RUN_CAP:
                print(f"  [bing_local] Run cap ({_RUN_CAP}) reached — stopping to protect free tier.")
                return all_results

            print(f"  [bing_local] {done}/{total} — '{query}' in {metro}, {state} "
                  f"[{request_count}/{_RUN_CAP} cap]")

            try:
                items = _bing_search(query, lat, lng)
                request_count += 1
                hits = 0
                for item in items:
                    name = item.get("name", "")
                    if name and _is_relevant(name):
                        all_results.append(_normalize(item, metro, state))
                        hits += 1
                print(f"    → {hits} RCM matches of {len(items)} results")
            except Exception as e:
                print(f"    [bing_local] Error: {e}")

            time.sleep(_DELAY + random.uniform(0, 0.5))

    print(f"[bing_local] Complete: {len(all_results)} records, "
          f"{request_count} API calls used ({request_count * 52:,}/yr projected)")
    return all_results
