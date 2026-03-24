"""
Bing Local Business API scraper (supplemental coverage).

Uses Bing Maps Local Search API (free tier: 125,000 transactions/year).
Good coverage for smaller metros and markets underserved by Google Places.

Docs: https://docs.microsoft.com/en-us/bingmaps/rest-services/locations/local-search
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
from config import BING_MAPS_API_KEY, TARGET_METROS, RATE_LIMITS

_DELAY = RATE_LIMITS["bing_local"]["delay_seconds"]
_DAILY_CAP = RATE_LIMITS["bing_local"]["daily_cap"]
_ua = UserAgent()

BING_LOCAL_URL = "https://dev.virtualearth.net/REST/v1/LocalSearch/"

BING_QUERIES = [
    "medical billing company",
    "revenue cycle management",
    "physician billing services",
    "healthcare billing",
]


@retry(wait=wait_exponential(min=2, max=20), stop=stop_after_attempt(3))
def _bing_search(query: str, lat: float, lng: float, max_results: int = 25) -> list[dict]:
    """Call Bing Local Search API."""
    params = {
        "query":              query,
        "userLocation":       f"{lat},{lng}",
        "maxResults":         max_results,
        "key":                BING_MAPS_API_KEY,
        "output":             "json",
    }
    resp = requests.get(BING_LOCAL_URL, params=params, timeout=15,
                        headers={"User-Agent": _ua.random})
    resp.raise_for_status()
    data = resp.json()
    resources = (data.get("resourceSets") or [{}])[0].get("resources") or []
    return resources


def _normalize_result(item: dict, metro: str, state: str) -> dict:
    now = datetime.now(timezone.utc).isoformat()
    address = item.get("Address") or {}
    phone_list = item.get("PhoneNumber") or ""
    entity_url = ""
    for entity in (item.get("EntityType") or []):
        pass  # Bing doesn't return website directly in local search
    url_list = item.get("Website") or item.get("url") or ""

    return {
        "id":                   str(uuid.uuid4()),
        "company_name":         item.get("name") or "",
        "website":              url_list,
        "phone":                phone_list,
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
    """Scrape Bing Local Business API for RCM companies."""
    if BING_MAPS_API_KEY == "YOUR_BING_MAPS_API_KEY":
        print("[bing_local] Bing Maps API key not set — skipping.")
        return []

    metros = metros or TARGET_METROS
    all_results: list[dict] = []
    request_count = 0
    total = len(metros) * len(BING_QUERIES)
    done  = 0

    for metro_info in metros:
        metro = metro_info["metro"]
        state = metro_info["state"]
        lat   = metro_info["lat"]
        lng   = metro_info["lng"]

        for query in BING_QUERIES:
            done += 1
            print(f"  [bing_local] {done}/{total} — '{query}' in {metro}, {state}")

            if request_count >= _DAILY_CAP:
                print(f"  [bing_local] Daily cap reached.")
                return all_results

            try:
                items = _bing_search(query, lat, lng)
                for item in items:
                    name = item.get("name", "")
                    if not name:
                        continue
                    # Basic filter: must look like a billing/RCM company
                    name_lower = name.lower()
                    if any(kw in name_lower for kw in
                           ["billing", "rcm", "revenue cycle", "coding", "medical", "healthcare"]):
                        all_results.append(_normalize_result(item, metro, state))
                request_count += 1
                print(f"    → {len(items)} results")
            except Exception as e:
                print(f"    [bing_local] Error: {e}")

            time.sleep(_DELAY + random.uniform(0, 0.5))

    print(f"[bing_local] Complete: {len(all_results)} raw results")
    return all_results
