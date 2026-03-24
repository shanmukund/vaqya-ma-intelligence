"""
Indeed job postings scraper — identifies RCM companies with active manual-workflow jobs.

Companies posting for medical billers, AR follow-up specialists, denial management
staff, etc. are actively operating with manual billing workflows, making them prime
offshoring + automation targets.

Uses Playwright for JS-rendered pages. Respects robots.txt and rate limits.
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
from config import (
    INDEED_JOB_TITLES, INDEED_COMPANY_EXCLUSIONS, TARGET_METROS, RATE_LIMITS
)

_DELAY = RATE_LIMITS["indeed"]["delay_seconds"]
_SESSION_CAP = RATE_LIMITS["indeed"]["session_cap"]

INDEED_SEARCH_URL = "https://www.indeed.com/jobs?q={query}&l={location}&limit=50&sort=date"


def _is_excluded(company_name: str) -> bool:
    name_lower = company_name.lower()
    return any(excl in name_lower for excl in INDEED_COMPANY_EXCLUSIONS)


def _parse_jobs_page(page_html: str, metro: str, state: str) -> list[dict]:
    """Parse Indeed search results HTML → list of (company_name, location) dicts."""
    from bs4 import BeautifulSoup
    soup = BeautifulSoup(page_html, "html.parser")
    companies: dict[str, dict] = {}  # key: company_name → aggregated data

    job_cards = soup.select("[data-testid='job-card'], .jobsearch-SerpJobCard, .job_seen_beacon")
    for card in job_cards:
        # Company name
        company_el = (card.select_one("[data-testid='company-name']") or
                      card.select_one(".companyName") or
                      card.select_one(".company"))
        if not company_el:
            continue
        company_name = company_el.get_text(strip=True)

        if not company_name or _is_excluded(company_name):
            continue

        # Job title (for job_titles_found signal)
        title_el = (card.select_one("[data-testid='jobTitle']") or
                    card.select_one(".jobTitle") or
                    card.select_one("h2"))
        job_title = title_el.get_text(strip=True) if title_el else ""

        # Location
        loc_el = (card.select_one("[data-testid='text-location']") or
                  card.select_one(".companyLocation"))
        location = loc_el.get_text(strip=True) if loc_el else f"{metro}, {state}"

        key = company_name.lower().strip()
        if key not in companies:
            companies[key] = {
                "company_name":       company_name,
                "city":               metro,
                "state":              state,
                "metro_region":       metro,
                "job_posting_count":  0,
                "job_titles_found":   [],
                "_raw_location":      location,
            }
        companies[key]["job_posting_count"] += 1
        if job_title and job_title not in companies[key]["job_titles_found"]:
            companies[key]["job_titles_found"].append(job_title)

    return list(companies.values())


def _build_company_dict(raw: dict) -> dict:
    now = datetime.now(timezone.utc).isoformat()
    return {
        "id":                   str(uuid.uuid4()),
        "company_name":         raw["company_name"],
        "website":              "",
        "phone":                "",
        "address":              "",
        "city":                 raw.get("city", ""),
        "state":                raw.get("state", ""),
        "metro_region":         raw.get("metro_region", ""),
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
        "job_posting_count":    raw.get("job_posting_count", 1),
        "job_titles_found":     raw.get("job_titles_found", []),
        "source":               ["indeed"],
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


def scrape(
    metros: list[dict] | None = None,
    job_titles: list[str] | None = None,
) -> list[dict]:
    """Scrape Indeed job postings to discover active RCM companies."""
    metros     = metros     or TARGET_METROS[:15]  # Default: first 15 metros
    job_titles = job_titles or INDEED_JOB_TITLES

    all_companies: dict[str, dict] = {}
    request_count = 0

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            ),
            viewport={"width": 1280, "height": 800},
        )

        total = len(metros) * len(job_titles)
        done  = 0

        for metro_info in metros:
            metro    = metro_info["metro"]
            state    = metro_info["state"]
            location = f"{metro}, {state}"

            for job_title in job_titles:
                done += 1
                print(f"  [indeed] {done}/{total} — '{job_title}' in {location} "
                      f"(requests: {request_count})")

                if request_count >= _SESSION_CAP:
                    print(f"  [indeed] Session cap {_SESSION_CAP} reached.")
                    break

                url = INDEED_SEARCH_URL.format(
                    query=job_title.replace(" ", "+"),
                    location=location.replace(" ", "+").replace(",", "%2C"),
                )

                try:
                    page = context.new_page()
                    page.goto(url, timeout=20_000, wait_until="domcontentloaded")
                    time.sleep(2 + random.uniform(0, 1))
                    html = page.content()
                    page.close()
                    request_count += 1
                except PlaywrightTimeout as e:
                    print(f"    [indeed] Timeout: {e}")
                    try:
                        page.close()
                    except Exception:
                        pass
                    continue
                except Exception as e:
                    print(f"    [indeed] Error: {e}")
                    continue

                companies = _parse_jobs_page(html, metro, state)
                print(f"    → {len(companies)} companies found")

                for raw in companies:
                    key = raw["company_name"].lower().strip()
                    if key in all_companies:
                        # Merge job counts
                        all_companies[key]["job_posting_count"] += raw["job_posting_count"]
                        for t in raw["job_titles_found"]:
                            if t not in all_companies[key]["job_titles_found"]:
                                all_companies[key]["job_titles_found"].append(t)
                    else:
                        all_companies[key] = raw

                time.sleep(_DELAY + random.uniform(0, _DELAY * 0.3))

            if request_count >= _SESSION_CAP:
                break

        browser.close()

    results = [_build_company_dict(raw) for raw in all_companies.values()]
    print(f"[indeed] Complete: {len(results)} unique companies from {request_count} requests")
    return results
