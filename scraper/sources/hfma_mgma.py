"""
HFMA/MGMA chapter directory scraper.

Scrapes public member listing pages from:
  - HFMA (Healthcare Financial Management Association) state chapter pages
  - MGMA (Medical Group Management Association) state affiliate pages

Members listed here are confirmed healthcare finance/RCM professionals,
making their employer companies high-confidence acquisition targets.

Uses Playwright for JS-rendered content.
"""

from __future__ import annotations
import time
import uuid
import random
import re
from datetime import datetime, timezone
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeout
from bs4 import BeautifulSoup
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
from config import HFMA_CHAPTER_BASE, RATE_LIMITS, TARGET_METROS

_DELAY = RATE_LIMITS["hfma_mgma"]["delay_seconds"]
_DAILY_CAP = RATE_LIMITS["hfma_mgma"]["daily_cap"]

# Known HFMA state chapter page patterns
HFMA_STATE_CHAPTERS = [
    "https://www.hfma.org/chapters/arizona/",
    "https://www.hfma.org/chapters/florida/",
    "https://www.hfma.org/chapters/georgia/",
    "https://www.hfma.org/chapters/illinois/",
    "https://www.hfma.org/chapters/texas/",
    "https://www.hfma.org/chapters/missouri/",
    "https://www.hfma.org/chapters/kentucky/",
    "https://www.hfma.org/chapters/california/",
    "https://www.hfma.org/chapters/new-york/",
    "https://www.hfma.org/chapters/north-carolina/",
]

STATE_ABBR_MAP = {
    "arizona": "AZ", "florida": "FL", "georgia": "GA", "illinois": "IL",
    "texas": "TX", "missouri": "MO", "kentucky": "KY", "california": "CA",
    "new-york": "NY", "north-carolina": "NC", "ohio": "OH", "michigan": "MI",
    "pennsylvania": "PA", "tennessee": "TN", "virginia": "VA",
    "washington": "WA", "massachusetts": "MA", "colorado": "CO",
}


def _extract_state_from_url(url: str) -> str:
    for name, abbr in STATE_ABBR_MAP.items():
        if name in url.lower():
            return abbr
    return ""


def _parse_member_page(html: str, state: str) -> list[dict]:
    """Extract company names from an HFMA/MGMA chapter page."""
    soup = BeautifulSoup(html, "html.parser")
    companies: set[str] = set()

    # Look for company/organization mentions in member listings
    # Pattern 1: Table rows with company columns
    for row in soup.select("table tr, .member-row, .directory-entry"):
        cells = row.find_all(["td", "div"])
        for cell in cells:
            text = cell.get_text(strip=True)
            # Heuristic: cells that look like company names
            if 20 < len(text) < 100 and not "@" in text:
                # Filter for healthcare/billing related companies
                text_lower = text.lower()
                if any(kw in text_lower for kw in
                       ["billing", "rcm", "revenue", "healthcare", "medical",
                        "health system", "hospital", "clinic", "physician"]):
                    companies.add(text)

    # Pattern 2: Lists with member names + companies
    for li in soup.select("li, .member-card, .speaker-card"):
        # Look for spans/divs that might be company names
        company_el = (li.select_one(".company") or li.select_one(".organization") or
                      li.select_one("[class*='company']") or li.select_one("[class*='org']"))
        if company_el:
            name = company_el.get_text(strip=True)
            if name and 5 < len(name) < 100:
                companies.add(name)

    # Pattern 3: Any text that looks like an RCM company (broad fallback)
    all_text = soup.get_text()
    for line in all_text.split("\n"):
        line = line.strip()
        if 15 < len(line) < 80:
            line_lower = line.lower()
            if any(kw in line_lower for kw in
                   ["billing", "revenue cycle", "rcm services", "medical billing",
                    "physician billing", "health billing"]):
                companies.add(line)

    now = datetime.now(timezone.utc).isoformat()
    metro = next((m["metro"] for m in TARGET_METROS if m["state"] == state), state)

    result = []
    for company_name in companies:
        result.append({
            "id":                   str(uuid.uuid4()),
            "company_name":         company_name,
            "website":              "",
            "phone":                "",
            "address":              "",
            "city":                 "",
            "state":                state,
            "metro_region":         metro,
            "zip":                  "",
            "estimated_revenue":    None,
            "revenue_band":         "Unknown",
            "employee_count_range": None,
            "employee_count_est":   None,
            "founded_year":         None,
            "company_age":          None,
            "owner_signals":        ["hfma_mgma_member"],
            "specialties":          [],
            "technology_signals":   [],
            "pe_backed":            False,
            "offshore_mentions":    False,
            "multi_state":          False,
            "recent_funding":       False,
            "job_posting_count":    0,
            "job_titles_found":     [],
            "source":               ["hfma_mgma"],
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
        })
    return result


def scrape() -> list[dict]:
    """Scrape HFMA state chapter pages for RCM company names."""
    all_results: list[dict] = []
    request_count = 0

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            ),
        )

        for url in HFMA_STATE_CHAPTERS:
            if request_count >= _DAILY_CAP:
                break

            state = _extract_state_from_url(url)
            print(f"  [hfma_mgma] Scraping {url} (state: {state})")

            try:
                page = context.new_page()
                page.goto(url, timeout=20_000, wait_until="domcontentloaded")
                time.sleep(2 + random.uniform(0, 1))
                html = page.content()
                page.close()
                request_count += 1

                companies = _parse_member_page(html, state)
                all_results.extend(companies)
                print(f"    → {len(companies)} companies from {state}")

            except PlaywrightTimeout:
                print(f"    [hfma_mgma] Timeout: {url}")
                try:
                    page.close()
                except Exception:
                    pass
            except Exception as e:
                print(f"    [hfma_mgma] Error {url}: {e}")

            time.sleep(_DELAY + random.uniform(0, 1))

        browser.close()

    print(f"[hfma_mgma] Complete: {len(all_results)} companies from {request_count} pages")
    return all_results
