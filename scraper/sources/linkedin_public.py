"""
LinkedIn public company page scraper (no authentication required).

Strategy: Use SerpAPI to find LinkedIn company URLs via
  site:linkedin.com/company "medical billing"
then fetch each public LinkedIn company page via Playwright to extract:
  - Employee count range
  - Founded year
  - Specialties
  - Description (for signal detection)

Rate limited to 50 requests/hour (15s delay). LinkedIn blocks aggressively —
the scraper degrades gracefully and skips blocked pages.
"""

from __future__ import annotations
import time
import uuid
import random
import re
from datetime import datetime, timezone
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeout
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
from config import SERPAPI_KEY, RATE_LIMITS, TARGET_METROS

_DELAY = RATE_LIMITS["linkedin"]["delay_seconds"]
_HOURLY_CAP = RATE_LIMITS["linkedin"]["hourly_cap"]

LINKEDIN_SEARCH_QUERIES = [
    'site:linkedin.com/company "medical billing" "United States"',
    'site:linkedin.com/company "revenue cycle management" "United States"',
    'site:linkedin.com/company "physician billing" "United States"',
    'site:linkedin.com/company "healthcare billing" "United States"',
]


def _serpapi_search(query: str, start: int = 0) -> list[dict]:
    """Use SerpAPI to search Google for LinkedIn company pages."""
    if SERPAPI_KEY == "YOUR_SERPAPI_KEY":
        return []
    try:
        import requests
        params = {
            "api_key": SERPAPI_KEY,
            "engine":  "google",
            "q":       query,
            "start":   start,
            "num":     10,
        }
        resp = requests.get("https://serpapi.com/search", params=params, timeout=15)
        resp.raise_for_status()
        data = resp.json()
        return data.get("organic_results", [])
    except Exception as e:
        print(f"  [linkedin] SerpAPI error: {e}")
        return []


def _extract_linkedin_url(result: dict) -> str | None:
    """Extract linkedin.com/company URL from a SERP result."""
    link = result.get("link", "")
    if "linkedin.com/company/" in link:
        # Normalize to profile URL
        match = re.search(r"(https?://[a-z.]*linkedin\.com/company/[^/?&#]+)", link)
        if match:
            return match.group(1)
    return None


def _scrape_linkedin_page(page, url: str) -> dict | None:
    """Scrape a LinkedIn company page. Returns partial company data or None."""
    try:
        page.goto(url, timeout=20_000, wait_until="domcontentloaded")
        time.sleep(3 + random.uniform(0, 1))

        # Check if we hit a login wall
        if "authwall" in page.url or "login" in page.url:
            return None

        html = page.content()
        from bs4 import BeautifulSoup
        soup = BeautifulSoup(html, "html.parser")

        data: dict = {"_linkedin_url": url}

        # Company name
        name_el = (soup.select_one("h1.top-card-layout__title") or
                   soup.select_one("h1") or
                   soup.select_one('[data-test-id="about-us__name"]'))
        if name_el:
            data["company_name"] = name_el.get_text(strip=True)

        # About section (description)
        about_el = (soup.select_one(".org-about-us-organization-description__text") or
                    soup.select_one('[data-test-id="about-us__description"]') or
                    soup.select_one(".about-us__description"))
        if about_el:
            data["_website_text"] = about_el.get_text(strip=True).lower()

        # Details (employee count, founded, etc.)
        details_text = ""
        for dt_el in soup.select("dt, .org-page-details__definition-term"):
            dd_el = dt_el.find_next_sibling(["dd", "p"])
            if not dd_el:
                continue
            label = dt_el.get_text(strip=True).lower()
            value = dd_el.get_text(strip=True)
            if "employee" in label or "company size" in label:
                data["employee_count_range"] = value
            elif "founded" in label:
                match = re.search(r"\d{4}", value)
                if match:
                    data["founded_year"] = int(match.group())
                    data["company_age"] = datetime.now().year - data["founded_year"]
            elif "specialt" in label:
                data["specialties"] = [s.strip() for s in value.split(",")]
            elif "headquarter" in label or "location" in label:
                parts = value.split(",")
                if len(parts) >= 2:
                    data["city"] = parts[0].strip()
                    data["state"] = parts[-1].strip()[:2].upper()

        return data if data.get("company_name") else None

    except PlaywrightTimeout:
        return None
    except Exception as e:
        print(f"  [linkedin] Page error {url}: {e}")
        return None


def _build_company_dict(raw: dict) -> dict:
    now = datetime.now(timezone.utc).isoformat()
    state = raw.get("state", "")
    metro = next((m["metro"] for m in TARGET_METROS if m["state"] == state),
                 raw.get("city", ""))
    return {
        "id":                   str(uuid.uuid4()),
        "company_name":         raw.get("company_name", ""),
        "website":              "",
        "phone":                "",
        "address":              "",
        "city":                 raw.get("city", ""),
        "state":                state,
        "metro_region":         metro,
        "zip":                  "",
        "estimated_revenue":    None,
        "revenue_band":         "Unknown",
        "employee_count_range": raw.get("employee_count_range"),
        "employee_count_est":   None,
        "founded_year":         raw.get("founded_year"),
        "company_age":          raw.get("company_age"),
        "owner_signals":        [],
        "specialties":          raw.get("specialties", []),
        "technology_signals":   [],
        "pe_backed":            False,
        "offshore_mentions":    False,
        "multi_state":          False,
        "recent_funding":       False,
        "job_posting_count":    0,
        "job_titles_found":     [],
        "source":               ["linkedin"],
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
        "_website_text":        raw.get("_website_text", ""),
        "_linkedin_url":        raw.get("_linkedin_url", ""),
    }


def scrape(max_companies: int = 200) -> list[dict]:
    """Scrape LinkedIn public company pages via SERP + direct page fetch."""
    if SERPAPI_KEY == "YOUR_SERPAPI_KEY":
        print("[linkedin] SerpAPI key not set — skipping LinkedIn scraper.")
        return []

    # Step 1: Collect LinkedIn URLs via SerpAPI
    linkedin_urls: set[str] = set()
    for query in LINKEDIN_SEARCH_QUERIES:
        for start in range(0, 50, 10):  # Up to 5 pages per query
            results = _serpapi_search(query, start)
            if not results:
                break
            for r in results:
                url = _extract_linkedin_url(r)
                if url:
                    linkedin_urls.add(url)
            time.sleep(1)
        if len(linkedin_urls) >= max_companies * 2:
            break

    print(f"[linkedin] Found {len(linkedin_urls)} LinkedIn company URLs to scrape")

    # Step 2: Scrape each page with Playwright
    results: list[dict] = []
    request_count = 0

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            ),
            viewport={"width": 1280, "height": 900},
        )

        for i, url in enumerate(list(linkedin_urls)[:max_companies]):
            if request_count >= _HOURLY_CAP:
                print(f"  [linkedin] Hourly cap reached. Pausing 65 seconds...")
                time.sleep(65)
                request_count = 0

            print(f"  [linkedin] {i+1}/{min(len(linkedin_urls), max_companies)} — {url}")
            page = context.new_page()
            raw = _scrape_linkedin_page(page, url)
            page.close()

            if raw and raw.get("company_name"):
                results.append(_build_company_dict(raw))

            request_count += 1
            time.sleep(_DELAY + random.uniform(0, _DELAY * 0.2))

        browser.close()

    print(f"[linkedin] Complete: {len(results)} companies scraped")
    return results
