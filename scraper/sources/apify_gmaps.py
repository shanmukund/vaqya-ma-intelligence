"""
Apify Google Maps Scraper — RCM company discovery via Apify platform.

Actor: compass/crawler-google-places
Pricing: $0.004 per place scraped = $4 per 1,000 places
Free tier: $5/month credits = ~1,250 places/month

Advantages over SearchAPI.io:
  - Apify handles anti-bot detection automatically
  - Pay-per-result (only pay for what you get)
  - Returns richer data: rating, review count, hours, categories
  - No lifetime credit cap — resets monthly
  - maxTotalChargeUsd cap prevents runaway spend

Strategy:
  - 32 tier-2 metros × 4 queries = 128 searches
  - Max 10 results per search = ~1,280 places max
  - Cost cap: $4.00 per run (keeps $1 buffer from $5 free)
  - Expected yield after RCM filtering: 200-500 companies

Sign up: https://apify.com/ (free, no card required for $5/mo credit)
API token: apify.com → Settings → Integrations → API token
"""

from __future__ import annotations
import time
import uuid
import random
import requests
from datetime import datetime, timezone
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
from config import APIFY_API_TOKEN, TIER2_METROS

ACTOR_ID      = "compass~crawler-google-places"
APIFY_RUN_URL = f"https://api.apify.com/v2/acts/{ACTOR_ID}/runs"
APIFY_DS_URL  = "https://api.apify.com/v2/datasets/{dataset_id}/items"

MAX_CHARGE_USD    = 4.00   # hard cost cap per run — stays within $5 free credit
MAX_RESULTS_EACH  = 10     # results per query-metro combination
RUN_TIMEOUT_SECS  = 300    # 5 minutes max wait for Apify to finish

RCM_QUERIES = [
    "medical billing company",
    "revenue cycle management company",
    "physician billing services",
    "healthcare billing services",
]

EXCLUDE_NAMES = [
    "hospital", "health system", "medical center", "urgent care", "clinic",
    "pharmacy", "laboratory", "staffing", "recruiting", "insurance company",
]


def _is_relevant(name: str, categories: list) -> bool:
    nl = name.lower()
    if any(ex in nl for ex in EXCLUDE_NAMES):
        return False
    cat_text = " ".join(categories).lower()
    return any(kw in nl or kw in cat_text for kw in [
        "billing", "rcm", "revenue cycle", "coding", "medical billing",
        "healthcare billing", "reimbursement", "claims",
    ])


def _run_actor(metros: list[dict], queries: list[str]) -> str | None:
    """
    Submit Apify actor run. Returns dataset ID or None on failure.
    """
    # Build search strings: "medical billing company in Phoenix AZ"
    search_strings = [
        f"{q} in {m['metro']}, {m['state']}"
        for m in metros
        for q in queries
    ]

    payload = {
        "searchStringsArray":         search_strings,
        "maxCrawledPlacesPerSearch":  MAX_RESULTS_EACH,
        "language":                   "en",
        "maxTotalChargeUsd":          MAX_CHARGE_USD,
        "includeWebResults":          False,
        "skipClosedPlaces":           True,
    }

    params = {
        "token":             APIFY_API_TOKEN,
        "maxTotalChargeUsd": str(MAX_CHARGE_USD),
    }

    resp = None
    try:
        resp = requests.post(
            APIFY_RUN_URL,
            json=payload,
            params=params,
            timeout=30,
        )
        if not resp.ok:
            print(f"  [apify_gmaps] HTTP {resp.status_code} error starting run")
            print(f"  [apify_gmaps] Response: {resp.text[:500]}")
            return None, None
        run_data   = resp.json().get("data") or {}
        run_id     = run_data.get("id")
        dataset_id = run_data.get("defaultDatasetId")
        if not run_id:
            print(f"  [apify_gmaps] No run_id in response: {resp.text[:300]}")
            return None, None
        print(f"  [apify_gmaps] Run started: {run_id}")
        print(f"  [apify_gmaps] Dataset: {dataset_id}")
        return run_id, dataset_id
    except Exception as e:
        body = resp.text[:300] if resp is not None else "no response"
        print(f"  [apify_gmaps] Failed to start actor run: {e}")
        print(f"  [apify_gmaps] Response body: {body}")
        return None, None


def _wait_for_run(run_id: str, timeout: int = RUN_TIMEOUT_SECS) -> bool:
    """Poll Apify run status until finished or timeout."""
    status_url = f"https://api.apify.com/v2/actor-runs/{run_id}"
    params     = {"token": APIFY_API_TOKEN}
    elapsed    = 0
    interval   = 10

    while elapsed < timeout:
        try:
            resp = requests.get(status_url, params=params, timeout=15)
            data = resp.json().get("data") or {}
            status = data.get("status", "")
            usage  = data.get("usageTotalUsd", 0)
            print(f"  [apify_gmaps] Status: {status} | Cost so far: ${usage:.3f}")
            if status in ("SUCCEEDED", "FINISHED"):
                return True
            if status in ("FAILED", "ABORTED", "TIMED-OUT"):
                print(f"  [apify_gmaps] Run ended with status: {status}")
                return False
        except Exception as e:
            print(f"  [apify_gmaps] Status check error: {e}")

        time.sleep(interval)
        elapsed += interval

    print(f"  [apify_gmaps] Timeout waiting for run after {timeout}s")
    return False


def _fetch_results(dataset_id: str) -> list[dict]:
    """Download results from Apify dataset."""
    url    = APIFY_DS_URL.format(dataset_id=dataset_id)
    params = {"token": APIFY_API_TOKEN, "format": "json", "limit": 5000}
    try:
        resp = requests.get(url, params=params, timeout=30)
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        print(f"  [apify_gmaps] Failed to fetch dataset: {e}")
        return []


def _normalize(item: dict) -> dict | None:
    """Convert Apify Google Maps result to standard company dict."""
    name = item.get("title") or item.get("name") or ""
    if not name:
        return None

    categories = item.get("categories") or []
    if not _is_relevant(name, categories):
        return None

    address = item.get("address") or item.get("street") or ""
    city    = item.get("city") or ""
    state   = item.get("state") or ""
    if state and len(state) > 2:
        # Apify returns full state names — map to 2-letter codes
        STATE_ABBREV = {
            "Alabama": "AL", "Alaska": "AK", "Arizona": "AZ", "Arkansas": "AR",
            "California": "CA", "Colorado": "CO", "Connecticut": "CT",
            "Delaware": "DE", "Florida": "FL", "Georgia": "GA", "Hawaii": "HI",
            "Idaho": "ID", "Illinois": "IL", "Indiana": "IN", "Iowa": "IA",
            "Kansas": "KS", "Kentucky": "KY", "Louisiana": "LA", "Maine": "ME",
            "Maryland": "MD", "Massachusetts": "MA", "Michigan": "MI",
            "Minnesota": "MN", "Mississippi": "MS", "Missouri": "MO",
            "Montana": "MT", "Nebraska": "NE", "Nevada": "NV",
            "New Hampshire": "NH", "New Jersey": "NJ", "New Mexico": "NM",
            "New York": "NY", "North Carolina": "NC", "North Dakota": "ND",
            "Ohio": "OH", "Oklahoma": "OK", "Oregon": "OR",
            "Pennsylvania": "PA", "Rhode Island": "RI", "South Carolina": "SC",
            "South Dakota": "SD", "Tennessee": "TN", "Texas": "TX",
            "Utah": "UT", "Vermont": "VT", "Virginia": "VA",
            "Washington": "WA", "West Virginia": "WV", "Wisconsin": "WI",
            "Wyoming": "WY", "District of Columbia": "DC",
        }
        state = STATE_ABBREV.get(state, state[:2].upper())

    # Find metro
    from config import TARGET_METROS
    metro = next(
        (m["metro"] for m in TARGET_METROS
         if m["state"] == state.upper()[:2]),
        city or state
    )

    now = datetime.now(timezone.utc).isoformat()
    return {
        "id":                   str(uuid.uuid4()),
        "company_name":         name,
        "website":              item.get("website") or "",
        "phone":                item.get("phone") or item.get("phoneUnformatted") or "",
        "address":              address,
        "city":                 city,
        "state":                state.upper()[:2] if state else "",
        "metro_region":         metro,
        "zip":                  item.get("postalCode") or "",
        "estimated_revenue":    None,
        "revenue_band":         "Unknown",
        "employee_count_range": None,
        "employee_count_est":   None,
        "founded_year":         None,
        "company_age":          None,
        "owner_signals":        [],
        "specialties":          categories[:5],
        "technology_signals":   [],
        "pe_backed":            False,
        "offshore_mentions":    False,
        "multi_state":          False,
        "recent_funding":       False,
        "job_posting_count":    0,
        "job_titles_found":     [],
        "source":               ["apify_gmaps"],
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
        "_google_rating":       item.get("totalScore"),
        "_google_reviews":      item.get("reviewsCount"),
        "_google_place_id":     item.get("placeId") or "",
    }


def scrape(metros: list[dict] | None = None, queries: list[str] | None = None) -> list[dict]:
    """
    Scrape Google Maps via Apify for RCM companies in tier-2 metros.
    Cost capped at $4.00/run to stay within $5 free monthly credit.
    """
    if not APIFY_API_TOKEN or APIFY_API_TOKEN == "YOUR_APIFY_API_TOKEN":
        print("[apify_gmaps] APIFY_API_TOKEN not set — skipping.")
        print("             Sign up free: https://apify.com/ → Settings → Integrations")
        return []

    metros  = metros  or TIER2_METROS
    queries = queries or RCM_QUERIES

    total_searches = len(metros) * len(queries)
    print(f"[apify_gmaps] Starting Google Maps scrape via Apify")
    print(f"  {len(metros)} tier-2 metros × {len(queries)} queries = {total_searches} searches")
    print(f"  Cost cap: ${MAX_CHARGE_USD:.2f} | Max results: {total_searches * MAX_RESULTS_EACH}")

    run_id, dataset_id = _run_actor(metros, queries)
    if not run_id:
        return []

    print(f"  [apify_gmaps] Waiting for run to complete (up to {RUN_TIMEOUT_SECS}s)...")
    success = _wait_for_run(run_id, timeout=RUN_TIMEOUT_SECS)
    if not success:
        print(f"  [apify_gmaps] Run did not complete successfully")
        return []

    raw_items = _fetch_results(dataset_id)
    print(f"  [apify_gmaps] Raw results: {len(raw_items)}")

    results = []
    for item in raw_items:
        normalized = _normalize(item)
        if normalized:
            results.append(normalized)

    print(f"[apify_gmaps] Complete: {len(results)} RCM companies from {len(raw_items)} raw results")
    return results
