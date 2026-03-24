"""
Secretary of State registry scraper.

Pulls registered business entities by NAICS code from:
  - Florida Division of Corporations (search.sunbiz.org)
  - OpenCorporates API (GA, and others)

Returns companies with high-confidence founding year data.
"""

from __future__ import annotations
import time
import uuid
import random
import requests
from datetime import datetime, timezone
from bs4 import BeautifulSoup
from tenacity import retry, wait_exponential, stop_after_attempt
from fake_useragent import UserAgent
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
from config import SOS_CONFIGS, OPENCORPORATES_BASE, RATE_LIMITS, TARGET_METROS

_DELAY = RATE_LIMITS["secretary_of_state"]["delay_seconds"]
_DAILY_CAP = RATE_LIMITS["secretary_of_state"]["daily_cap"]
_ua = UserAgent()

_CURRENT_YEAR = datetime.now().year

NAICS_KEYWORDS = {
    "621111": "physician billing",
    "621112": "dental billing",
    "524114": "medical billing services",
    "524291": "healthcare billing",
}


def _base_record(company_name: str, state: str, city: str = "", founded_year: int | None = None) -> dict:
    now = datetime.now(timezone.utc).isoformat()
    age = (_CURRENT_YEAR - founded_year) if founded_year else None
    # Find metro for this state from TARGET_METROS
    metro = next((m["metro"] for m in TARGET_METROS if m["state"] == state), city or state)
    return {
        "id":                   str(uuid.uuid4()),
        "company_name":         company_name,
        "website":              "",
        "phone":                "",
        "address":              "",
        "city":                 city,
        "state":                state.upper(),
        "metro_region":         metro,
        "zip":                  "",
        "estimated_revenue":    None,
        "revenue_band":         "Unknown",
        "employee_count_range": None,
        "employee_count_est":   None,
        "founded_year":         founded_year,
        "company_age":          age,
        "owner_signals":        ["registered business entity"],
        "specialties":          [],
        "technology_signals":   [],
        "pe_backed":            False,
        "offshore_mentions":    False,
        "multi_state":          False,
        "recent_funding":       False,
        "job_posting_count":    0,
        "job_titles_found":     [],
        "source":               ["secretary_of_state"],
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
    }


# ─── OpenCorporates scraper (GA, general fallback) ────────────────────────────
@retry(wait=wait_exponential(min=3, max=30), stop=stop_after_attempt(3))
def _opencorporates_search(
    jurisdiction: str,
    query: str,
    per_page: int = 50,
) -> list[dict]:
    """Search OpenCorporates for companies in a given jurisdiction."""
    params = {
        "q":                  query,
        "jurisdiction_code":  jurisdiction,
        "per_page":           per_page,
        "inactive":           "false",
        "order":              "score",
    }
    resp = requests.get(
        OPENCORPORATES_BASE,
        params=params,
        headers={"User-Agent": _ua.random},
        timeout=15,
    )
    resp.raise_for_status()
    data = resp.json()
    return data.get("results", {}).get("companies", [])


def _scrape_opencorporates(state: str, queries: list[str], request_count: list) -> list[dict]:
    jurisdiction = f"us_{state.lower()}"
    results = []
    for query in queries:
        if request_count[0] >= _DAILY_CAP:
            break
        print(f"  [sos] OpenCorporates: '{query}' in {state}")
        try:
            companies = _opencorporates_search(jurisdiction, query)
            for item in companies:
                c = item.get("company", item)
                name = c.get("name", "")
                if not name:
                    continue
                # Parse founding year from incorporation_date
                inc_date = c.get("incorporation_date") or ""
                founded_year = None
                if inc_date and len(inc_date) >= 4:
                    try:
                        founded_year = int(inc_date[:4])
                    except ValueError:
                        pass
                city = (c.get("registered_address") or {}).get("locality", "")
                results.append(_base_record(name, state, city, founded_year))
            request_count[0] += 1
            time.sleep(_DELAY + random.uniform(0, 0.5))
        except Exception as e:
            print(f"    [sos] OpenCorporates error: {e}")
    return results


# ─── Florida SunBiz scraper ───────────────────────────────────────────────────
def _scrape_florida(request_count: list) -> list[dict]:
    """Search Florida Division of Corporations for RCM-related entities."""
    results = []
    search_terms = [
        "medical billing", "revenue cycle", "physician billing",
        "healthcare billing", "medical coding",
    ]
    base_url = "https://search.sunbiz.org/Inquiry/CorporationSearch/SearchResults"

    for term in search_terms:
        if request_count[0] >= _DAILY_CAP:
            break
        print(f"  [sos] Florida SunBiz: '{term}'")
        params = {
            "inquiryType":          "EntityName",
            "inquiryDirectionType": "ForwardList",
            "searchNameOrder":      term,
            "aggregateId":          "",
            "searchTerm":           term,
            "listNameOrder":        "",
        }
        try:
            resp = requests.get(
                base_url, params=params,
                headers={"User-Agent": _ua.random},
                timeout=15,
            )
            resp.raise_for_status()
            request_count[0] += 1

            soup = BeautifulSoup(resp.text, "html.parser")
            rows = soup.select("table.tablesorter tbody tr, #search-results tbody tr")

            for row in rows:
                cells = row.find_all("td")
                if len(cells) < 3:
                    continue
                name = cells[0].get_text(strip=True)
                status = cells[1].get_text(strip=True) if len(cells) > 1 else ""
                if "active" not in status.lower() and "inactive" not in status.lower():
                    continue  # Skip if status unclear
                if not name:
                    continue
                # Filter for billing-related companies
                name_lower = name.lower()
                if not any(kw in name_lower for kw in
                           ["billing", "revenue", "rcm", "medical", "healthcare", "coding"]):
                    continue
                results.append(_base_record(name, "FL"))
        except Exception as e:
            print(f"    [sos] Florida error: {e}")
        time.sleep(_DELAY)

    return results


def scrape(states: list[str] | None = None) -> list[dict]:
    """
    Scrape Secretary of State registries for target states.
    Default: FL (SunBiz) + GA, TX, IL via OpenCorporates.
    """
    states = states or ["FL", "GA", "TX", "IL", "AZ", "MO", "KY"]
    all_results: list[dict] = []
    request_count = [0]

    oc_queries = list(NAICS_KEYWORDS.values())  # Use descriptive queries for OC

    for state in states:
        print(f"[sos] Scraping {state}...")
        if request_count[0] >= _DAILY_CAP:
            break
        if state == "FL":
            results = _scrape_florida(request_count)
        else:
            results = _scrape_opencorporates(state, oc_queries, request_count)

        all_results.extend(results)
        print(f"  → {len(results)} records from {state}")

    print(f"[sos] Complete: {len(all_results)} records, {request_count[0]} requests")
    return all_results
