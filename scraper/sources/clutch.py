"""
Clutch.co RCM / Healthcare Billing directory scraper — free, no API key.

Clutch is a verified B2B directory. Their healthcare RCM pages list companies
with employee ranges, revenue bands, hourly rates, and client reviews — giving
us richer enrichment data than most sources.

Pages scraped:
  - /healthcare/revenue-cycle-management  (primary RCM list)
  - /it-services/healthcare-billing        (healthcare billing)
  - /healthcare/medical-billing            (medical billing)

Uses Playwright for JS-rendered company cards.
Expected yield: 200–600 well-enriched company records.
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
from config import TARGET_METROS

DELAY_BETWEEN_PAGES = 4.0   # seconds — respectful crawl rate
MAX_PAGES_PER_CATEGORY = 8  # 30 companies/page × 8 = 240 per category

# Clutch directory URLs for RCM / medical billing companies
CLUTCH_URLS = [
    ("https://clutch.co/healthcare/revenue-cycle-management",  "RCM"),
    ("https://clutch.co/it-services/healthcare-billing",        "Healthcare Billing"),
    ("https://clutch.co/healthcare/medical-billing",            "Medical Billing"),
]

# Clutch employee range labels → estimated midpoint
EMPLOYEE_EST_MAP = {
    "freelancer": 1,
    "2-9": 5,
    "10-49": 25,
    "50-249": 125,
    "250-999": 500,
    "1,000-9,999": 2500,
    "10,000+": 15000,
}

# Build state lookup by metro name
_METRO_STATE: dict[str, str] = {m["metro"].lower(): m["state"] for m in TARGET_METROS}
_STATE_METROS: dict[str, str] = {m["state"]: m["metro"] for m in TARGET_METROS}


def _parse_employee_count(text: str) -> tuple[str | None, int | None]:
    """Return (range_label, estimated_midpoint) from a Clutch employee string."""
    text = text.strip()
    for label, est in EMPLOYEE_EST_MAP.items():
        if label.lower() in text.lower():
            return label, est
    # e.g. "250 - 999 employees"
    m = re.search(r"(\d[\d,]*)\s*[-–]\s*(\d[\d,]*)", text)
    if m:
        lo = int(m.group(1).replace(",", ""))
        hi = int(m.group(2).replace(",", ""))
        return f"{lo}-{hi}", (lo + hi) // 2
    return None, None


def _infer_state_metro(location: str) -> tuple[str, str]:
    """
    Parse 'Dallas, TX' or 'New York, NY, United States' → (state, metro).
    """
    if not location:
        return "", ""
    parts = [p.strip() for p in location.split(",")]
    state = ""
    city  = ""
    for part in reversed(parts):
        p = part.strip()
        if len(p) == 2 and p.isupper():
            state = p
        elif len(p) > 2 and p not in ("United States", "US"):
            city = p
    metro = ""
    if state:
        metro = _STATE_METROS.get(state, city)
    if not metro and city:
        metro = city
    return state, metro


def _parse_company_cards(html: str, category: str) -> list[dict]:
    """Parse Clutch HTML for one results page → list of raw company dicts."""
    soup = BeautifulSoup(html, "html.parser")
    companies: list[dict] = []

    # Clutch company cards — several possible selectors across site versions
    cards = (soup.select("li[data-test='provider-list-item']") or
             soup.select("li.provider-list-item") or
             soup.select("div.provider-row") or
             soup.select("article.directory-list-item"))

    for card in cards:
        # Name
        name_el = (card.select_one("h3.company_info--name") or
                   card.select_one("h3.company-name") or
                   card.select_one("h3") or
                   card.select_one("[data-test='company-name']"))
        name = name_el.get_text(strip=True) if name_el else ""
        if not name:
            continue

        # Location
        loc_el = (card.select_one(".locality") or
                  card.select_one("[data-test='location']") or
                  card.select_one(".location") or
                  card.select_one(".company_info--location"))
        location = loc_el.get_text(strip=True) if loc_el else ""
        state, metro = _infer_state_metro(location)

        # Employee count
        emp_el = (card.select_one("[data-test='employees']") or
                  card.select_one(".company_info--employees") or
                  card.select_one(".employees"))
        emp_text  = emp_el.get_text(strip=True) if emp_el else ""
        emp_range, emp_est = _parse_employee_count(emp_text)

        # Website (may be hidden behind Clutch redirect)
        link_el = card.select_one("a[data-test='provider-url'], a.website-link")
        website = ""
        if link_el:
            href = link_el.get("href", "")
            if href and not href.startswith("https://clutch.co"):
                website = href

        # Hourly rate (revenue proxy)
        rate_el = (card.select_one("[data-test='hourly-rate']") or
                   card.select_one(".company_info--rate"))
        hourly_rate = rate_el.get_text(strip=True) if rate_el else ""

        # Rating / review count (quality signal)
        rating_el = card.select_one(".rating, [data-test='rating']")
        rating = rating_el.get_text(strip=True) if rating_el else ""

        # Description / tagline
        desc_el = (card.select_one(".company-summary") or
                   card.select_one(".tagline") or
                   card.select_one("p.summary"))
        desc_text = desc_el.get_text(strip=True) if desc_el else ""

        companies.append({
            "company_name":       name,
            "website":            website,
            "city":               location.split(",")[0].strip() if location else "",
            "state":              state,
            "metro_region":       metro,
            "employee_count_range": emp_range,
            "employee_count_est": emp_est,
            "hourly_rate":        hourly_rate,
            "rating":             rating,
            "category":           category,
            "_website_text":      desc_text.lower(),
        })

    return companies


def _build_company_dict(raw: dict) -> dict:
    now = datetime.now(timezone.utc).isoformat()
    # Revenue band estimation from employee count
    emp_est = raw.get("employee_count_est") or 0
    if emp_est >= 500:
        revenue_band = "$30M-$50M"
    elif emp_est >= 125:
        revenue_band = "$15M-$30M"
    elif emp_est >= 25:
        revenue_band = "$5M-$15M"
    elif emp_est >= 5:
        revenue_band = "$2M-$5M"
    else:
        revenue_band = "Unknown"

    # Detect offshore / PE mentions in description
    desc = raw.get("_website_text", "")
    offshore_mentions = any(kw in desc for kw in [
        "offshore", "india", "philippines", "nearshore", "global delivery",
    ])
    pe_backed = any(kw in desc for kw in [
        "private equity", "backed by", "portfolio", "investment firm",
    ])

    return {
        "id":                   str(uuid.uuid4()),
        "company_name":         raw["company_name"],
        "website":              raw.get("website", ""),
        "phone":                "",
        "address":              "",
        "city":                 raw.get("city", ""),
        "state":                raw.get("state", ""),
        "metro_region":         raw.get("metro_region", ""),
        "zip":                  "",
        "estimated_revenue":    None,
        "revenue_band":         revenue_band,
        "employee_count_range": raw.get("employee_count_range"),
        "employee_count_est":   raw.get("employee_count_est"),
        "founded_year":         None,
        "company_age":          None,
        "owner_signals":        ["clutch_verified"],
        "specialties":          [raw.get("category", "RCM")],
        "technology_signals":   [],
        "pe_backed":            pe_backed,
        "offshore_mentions":    offshore_mentions,
        "multi_state":          False,
        "recent_funding":       False,
        "job_posting_count":    0,
        "job_titles_found":     [],
        "source":               ["clutch"],
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
        "_clutch_hourly_rate":  raw.get("hourly_rate", ""),
        "_clutch_rating":       raw.get("rating", ""),
        "_website_text":        raw.get("_website_text", ""),
    }


def scrape() -> list[dict]:
    """Scrape Clutch.co RCM company directories. No API key required."""
    all_raw:   list[dict] = []
    seen_names: set[str] = set()

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/121.0.0.0 Safari/537.36"
            ),
            viewport={"width": 1440, "height": 900},
        )

        for base_url, category in CLUTCH_URLS:
            print(f"  [clutch] Scraping category: {category} → {base_url}")

            for page_num in range(1, MAX_PAGES_PER_CATEGORY + 1):
                url = f"{base_url}?page={page_num}" if page_num > 1 else base_url
                print(f"    [clutch] Page {page_num}: {url}")

                try:
                    pg = context.new_page()
                    pg.goto(url, timeout=30_000, wait_until="domcontentloaded")
                    # Give JS-rendered cards time to load
                    pg.wait_for_timeout(3000)
                    html = pg.content()
                    pg.close()
                except PlaywrightTimeout:
                    print(f"    [clutch] Timeout on {url}")
                    try: pg.close()
                    except Exception: pass
                    break
                except Exception as e:
                    print(f"    [clutch] Error: {e}")
                    break

                companies = _parse_company_cards(html, category)
                if not companies:
                    print(f"    [clutch] No companies found on page {page_num} — stopping pagination")
                    break

                new_count = 0
                for raw in companies:
                    key = raw["company_name"].lower().strip()
                    if key not in seen_names:
                        seen_names.add(key)
                        all_raw.append(raw)
                        new_count += 1

                print(f"      → {new_count} new companies (running total: {len(all_raw)})")

                if new_count == 0:
                    break  # Duplicate page — we've hit the end

                time.sleep(DELAY_BETWEEN_PAGES + random.uniform(0, 1.5))

        browser.close()

    results = [_build_company_dict(r) for r in all_raw]
    print(f"[clutch] Complete: {len(results)} companies scraped")
    return results
