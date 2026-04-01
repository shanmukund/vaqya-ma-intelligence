"""
Yellow Pages scraper for medical billing / RCM companies — free, no API key.

yellowpages.com returns static HTML for business searches, making it easy to
scrape with requests + BeautifulSoup. It gives us phone numbers, addresses,
and website URLs for smaller, local billing companies that often don't appear
on Clutch or LinkedIn.

Expected yield: 500–2,000 records across 47 metros.
"""

from __future__ import annotations
import time
import uuid
import random
import re
import requests
from datetime import datetime, timezone
from bs4 import BeautifulSoup
from tenacity import retry, wait_exponential, stop_after_attempt
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
from config import TARGET_METROS

DELAY = 3.0   # seconds between requests
SESSION_CAP = 300  # max requests per scraper run

YP_SEARCH_URL = "https://www.yellowpages.com/search"

YP_QUERIES = [
    "medical billing service",
    "revenue cycle management",
    "physician billing",
    "healthcare billing",
    "medical coding service",
    "billing and coding",
]

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/121.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

# Name fragments that indicate non-target listings
EXCLUDE_NAMES = [
    "hospital", "health system", "medical center", "urgent care", "clinic",
    "pharmacy", "drug store", "laboratory", "radiology", "imaging center",
    "insurance", "staffing", "recruiting", "temp agency",
]

# Build state lookup
_STATE_METROS: dict[str, str] = {m["state"]: m["metro"] for m in TARGET_METROS}


def _is_relevant(name: str) -> bool:
    n = name.lower()
    if any(ex in n for ex in EXCLUDE_NAMES):
        return False
    return True  # YP search already filtered by keyword, keep most results


@retry(wait=wait_exponential(min=3, max=30), stop=stop_after_attempt(3))
def _fetch_yp(query: str, location: str, page: int = 1) -> requests.Response:
    params = {
        "search_terms":      query,
        "geo_location_terms": location,
        "page":              page,
    }
    return requests.get(YP_SEARCH_URL, params=params, headers=HEADERS, timeout=15)


def _parse_yp_results(html: str, state: str, metro: str) -> list[dict]:
    """Parse YellowPages search results HTML → list of raw company dicts."""
    soup = BeautifulSoup(html, "html.parser")
    companies: list[dict] = []

    # Primary listing cards
    listings = (soup.select("div.search-results div.v-card") or
                soup.select("div.result") or
                soup.select("div[class*='listing']"))

    for card in listings:
        # Business name
        name_el = (card.select_one("a.business-name") or
                   card.select_one("h2.n") or
                   card.select_one("span.business-name"))
        if not name_el:
            continue
        name = name_el.get_text(strip=True)
        if not name or not _is_relevant(name):
            continue

        # Phone
        phone_el = (card.select_one("div.phones.phone.primary") or
                    card.select_one("a.phone") or
                    card.select_one("span.phone"))
        phone = phone_el.get_text(strip=True) if phone_el else ""

        # Address
        street_el  = card.select_one("span.street-address")
        locality_el = card.select_one("span.locality")
        street   = street_el.get_text(strip=True) if street_el else ""
        locality = locality_el.get_text(strip=True) if locality_el else ""
        city_match = re.match(r"^([^,]+),?\s*([A-Z]{2})\s*(\d{5})?", locality)
        city = city_match.group(1).strip() if city_match else metro
        zip_ = city_match.group(3) or "" if city_match else ""

        # Website
        web_el = (card.select_one("a.track-visit-website") or
                  card.select_one("a[class*='website']"))
        website = web_el.get("href", "") if web_el else ""

        # Categories (specialty hint)
        cats_el = card.select("div.categories a, span.categories")
        categories = [c.get_text(strip=True) for c in cats_el]

        companies.append({
            "company_name": name,
            "website":      website,
            "phone":        phone,
            "address":      street,
            "city":         city,
            "state":        state,
            "metro_region": metro,
            "zip":          zip_,
            "specialties":  categories,
        })

    return companies


def _build_company_dict(raw: dict) -> dict:
    now = datetime.now(timezone.utc).isoformat()
    return {
        "id":                   str(uuid.uuid4()),
        "company_name":         raw["company_name"],
        "website":              raw.get("website", ""),
        "phone":                raw.get("phone", ""),
        "address":              raw.get("address", ""),
        "city":                 raw.get("city", ""),
        "state":                raw.get("state", ""),
        "metro_region":         raw.get("metro_region", ""),
        "zip":                  raw.get("zip", ""),
        "estimated_revenue":    None,
        "revenue_band":         "Unknown",
        "employee_count_range": None,
        "employee_count_est":   None,
        "founded_year":         None,
        "company_age":          None,
        "owner_signals":        [],
        "specialties":          raw.get("specialties", []),
        "technology_signals":   [],
        "pe_backed":            False,
        "offshore_mentions":    False,
        "multi_state":          False,
        "recent_funding":       False,
        "job_posting_count":    0,
        "job_titles_found":     [],
        "source":               ["yellowpages"],
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
    """Scrape YellowPages for medical billing companies across target metros."""
    metros = metros or TARGET_METROS
    all_raw:    list[dict] = []
    seen_names: set[str]   = set()
    request_count = 0

    session = requests.Session()
    session.headers.update(HEADERS)

    total = len(metros) * len(YP_QUERIES)
    done  = 0

    for metro_info in metros:
        metro    = metro_info["metro"]
        state    = metro_info["state"]
        location = f"{metro}, {state}"

        for query in YP_QUERIES:
            done += 1
            if request_count >= SESSION_CAP:
                print(f"  [yellowpages] Session cap {SESSION_CAP} reached.")
                break

            print(f"  [yellowpages] {done}/{total} — '{query}' in {location}")

            for page in range(1, 4):   # up to 3 pages per (metro, query)
                if request_count >= SESSION_CAP:
                    break
                try:
                    resp = _fetch_yp(query, location, page)
                    request_count += 1

                    if resp.status_code == 404:
                        break   # No more pages
                    resp.raise_for_status()

                    companies = _parse_yp_results(resp.text, state, metro)
                    if not companies:
                        break

                    new_count = 0
                    for raw in companies:
                        key = raw["company_name"].lower().strip()
                        if key not in seen_names:
                            seen_names.add(key)
                            all_raw.append(raw)
                            new_count += 1

                    print(f"    → page {page}: {len(companies)} results, {new_count} new")
                    if new_count == 0 or len(companies) < 15:
                        break  # Last page

                except Exception as e:
                    print(f"    [yellowpages] Error: {e}")
                    break

                time.sleep(DELAY + random.uniform(0, 1.0))

        if request_count >= SESSION_CAP:
            break

    results = [_build_company_dict(r) for r in all_raw]
    print(f"[yellowpages] Complete: {len(results)} companies, {request_count} requests")
    return results
