"""
LinkedIn public company page scraper (no authentication required).

URL discovery priority (automatic, best available):
  1. Brave Search API  — $5/1,000 requests; $5 free credits auto-applied every month
                         = 1,000 free searches/month effectively
                         Actual use: ~20 calls/run × 4 runs = 80/mo ($0.40 of $5 credit)
                         Sign up: https://brave.com/search/api/
  2. DuckDuckGo HTML  — always free, no key, no account (fallback)
  3. SerpAPI          — Phase 2 optional paid upgrade (higher volume)

Then: Playwright fetches each LinkedIn company page to extract
  employee count range, founded year, specialties, description.

Rate limited to 50 requests/hour (15s delay). LinkedIn blocks aggressively —
the scraper degrades gracefully and skips blocked pages.

Brave cost math:
  $5/1,000 requests with $5 free credits auto-applied each month.
  4 queries × 5 pages each = ~20 calls/run × 4 runs/mo = 80 calls/mo = $0.40
  Hard run cap: 200 calls (800/mo max = $4.00 of $5.00 free credit).
"""

from __future__ import annotations
import time
import uuid
import random
import re
import requests as _requests
from datetime import datetime, timezone
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeout
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
from config import SERPAPI_KEY, BRAVE_API_KEY, RATE_LIMITS, TARGET_METROS, FREE_TIER_CAPS

_DELAY       = RATE_LIMITS["linkedin"]["delay_seconds"]
_HOURLY_CAP  = RATE_LIMITS["linkedin"]["hourly_cap"]
_BRAVE_CAP   = FREE_TIER_CAPS["brave_search"]["run_cap"]      # calls per run
_BRAVE_LIMIT = FREE_TIER_CAPS["brave_search"]["monthly_limit"] # for logging

LINKEDIN_SEARCH_QUERIES = [
    'site:linkedin.com/company "medical billing" "United States"',
    'site:linkedin.com/company "revenue cycle management" "United States"',
    'site:linkedin.com/company "physician billing" "United States"',
    'site:linkedin.com/company "healthcare billing" "United States"',
]


def _brave_search(query: str, offset: int = 0, count: int = 20,
                   call_counter: list | None = None) -> list[dict]:
    """
    Brave Search API — free 2,000 queries/month, best quality for LinkedIn discovery.
    Returns list of {link, title} dicts.
    call_counter is a mutable [int] for cross-call tracking against the run cap.
    """
    if not BRAVE_API_KEY:
        return []
    if call_counter is not None and call_counter[0] >= _BRAVE_CAP:
        print(f"  [linkedin/brave] Run cap ({_BRAVE_CAP}) reached — switching to DuckDuckGo.")
        return []
    try:
        resp = _requests.get(
            "https://api.search.brave.com/res/v1/web/search",
            params={"q": query, "count": count, "offset": offset},
            headers={
                "X-Subscription-Token": BRAVE_API_KEY,
                "Accept":               "application/json",
                "Accept-Encoding":      "gzip",
            },
            timeout=15,
        )
        resp.raise_for_status()
        if call_counter is not None:
            call_counter[0] += 1
        data = resp.json()
        items = (data.get("web") or {}).get("results") or []
        return [{"link": r.get("url", ""), "title": r.get("title", "")} for r in items]
    except Exception as e:
        print(f"  [linkedin/brave] Brave API error: {e}")
        return []


def _ddg_search(query: str, max_results: int = 20) -> list[dict]:
    """
    Free DuckDuckGo HTML search — no API key, no account.
    Posts to https://html.duckduckgo.com/html/ for clean, JS-free results.
    """
    import requests as _req
    from bs4 import BeautifulSoup as _BS
    try:
        resp = _req.post(
            "https://html.duckduckgo.com/html/",
            data={"q": query},
            headers={
                "User-Agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/121.0.0.0 Safari/537.36"
                ),
                "Content-Type": "application/x-www-form-urlencoded",
                "Accept-Language": "en-US,en;q=0.9",
            },
            timeout=15,
        )
        soup = _BS(resp.text, "html.parser")
        results = []
        for r in soup.select(".result")[:max_results]:
            link_el  = r.select_one(".result__url")
            title_el = r.select_one(".result__title")
            if not link_el:
                continue
            href = link_el.get_text(strip=True)
            if not href.startswith("http"):
                href = "https://" + href
            results.append({
                "link":  href,
                "title": title_el.get_text(strip=True) if title_el else "",
            })
        return results
    except Exception as e:
        print(f"  [linkedin] DuckDuckGo search error: {e}")
        return []


def _serpapi_search(query: str, start: int = 0) -> list[dict]:
    """Use SerpAPI to search Google for LinkedIn company pages (optional — paid)."""
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


def _search_linkedin_urls(query: str, start: int = 0) -> list[dict]:
    """Try SerpAPI first (if key present), fall back to free DuckDuckGo."""
    if SERPAPI_KEY != "YOUR_SERPAPI_KEY":
        return _serpapi_search(query, start)
    return _ddg_search(query)


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
    """
    Scrape LinkedIn public company pages.

    URL discovery priority (auto-selected):
      1. Brave Search API  (BRAVE_API_KEY set — free 2K/mo, best quality)
      2. DuckDuckGo HTML   (always free, no key, fallback)
      3. SerpAPI           (SERPAPI_KEY set — Phase 2 paid upgrade)
    """
    using_brave   = bool(BRAVE_API_KEY)
    using_serpapi = SERPAPI_KEY != "YOUR_SERPAPI_KEY"

    if using_brave:
        mode = "Brave Search API (free 2K/mo)"
    elif using_serpapi:
        mode = "SerpAPI (paid)"
    else:
        mode = "DuckDuckGo HTML (free, no key)"

    print(f"[linkedin] URL discovery via: {mode}")

    if using_brave:
        # Log projected monthly cost vs free credit
        projected_monthly = len(LINKEDIN_SEARCH_QUERIES) * 5 * 4  # 5 pages/query × 4 runs/mo
        projected_cost    = (projected_monthly / 1000) * 5         # $5 per 1,000 requests
        print(f"  [linkedin/brave] Projected: ~{projected_monthly} calls/month "
              f"≈ ${projected_cost:.2f}/mo of $5.00 free monthly credit")

    brave_counter = [0]   # mutable ref — tracks Brave API calls this run

    # Step 1: Collect LinkedIn URLs
    linkedin_urls: set[str] = set()

    for query in LINKEDIN_SEARCH_QUERIES:
        if using_brave:
            # Paginate up to 5 pages (20 results each = 100 results per query)
            for offset in range(0, 100, 20):
                results = _brave_search(query, offset=offset, count=20,
                                        call_counter=brave_counter)
                if not results:
                    break
                for r in results:
                    url = _extract_linkedin_url(r)
                    if url:
                        linkedin_urls.add(url)
                if brave_counter[0] >= _BRAVE_CAP:
                    break
                time.sleep(1.0)

        elif using_serpapi:
            for start in range(0, 50, 10):
                results = _serpapi_search(query, start)
                if not results:
                    break
                for r in results:
                    url = _extract_linkedin_url(r)
                    if url:
                        linkedin_urls.add(url)
                time.sleep(1)

        else:
            # DuckDuckGo — free, no key, ~20 results per call
            results = _ddg_search(query, max_results=20)
            for r in results:
                url = _extract_linkedin_url(r)
                if url:
                    linkedin_urls.add(url)
            time.sleep(2)

        if len(linkedin_urls) >= max_companies * 2:
            break

    if using_brave:
        print(f"  [linkedin/brave] Used {brave_counter[0]} Brave API calls this run")

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
