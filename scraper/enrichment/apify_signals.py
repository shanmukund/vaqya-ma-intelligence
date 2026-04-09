"""
Apify Website Content Crawler — offshore, tech, and PE signal detection.

Actor: apify/website-content-crawler
Pricing: ~$0.50-$2 per 1,000 pages crawled
Free tier: $5/month = ~2,500-10,000 pages

This replaces our basic tech_detector.py Playwright scraper with Apify's
managed crawler which handles anti-bot detection, JS rendering, and retries
automatically. Returns clean Markdown text for signal scanning.

What we detect per company website:
  - Offshore signals: "india office", "offshore team", "philippines", "bpo"
  - Legacy tech signals: "meditech", "paper claims", "fax submission", etc.
  - PE signals: "private equity", "portfolio company", "backed by"
  - Owner signals: "founded by", "family owned", "owner operated"
  - Multi-state signals: "nationwide", "multiple states", "across the country"

Strategy:
  - Only crawl companies WITH a website (skip NPPES-only records)
  - Limit to 1 page per domain (homepage only) to save credits
  - Cost cap: $3.00 per run leaves $2 for Google Maps
  - Batch companies into one Apify run (cheaper than multiple runs)

Sign up: https://apify.com/ (free, no card required for $5/mo credit)
"""

from __future__ import annotations
import time
import requests
from datetime import datetime, timezone
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
from config import (
    APIFY_API_TOKEN,
    OFFSHORE_SIGNALS, LEGACY_TECH_SIGNALS, MODERN_TECH_SIGNALS,
    PE_SIGNALS, OWNER_OPERATED_SIGNALS,
)

ACTOR_ID      = "apify~website-content-crawler"
APIFY_RUN_URL = f"https://api.apify.com/v2/acts/{ACTOR_ID}/runs"
APIFY_DS_URL  = "https://api.apify.com/v2/datasets/{dataset_id}/items"

MAX_CHARGE_USD   = 3.00   # per run cost cap
RUN_TIMEOUT_SECS = 600    # 10 min — crawling is slower than Maps
MAX_PAGES_PER_URL = 1     # homepage only — save credits


def _run_crawler(urls: list[str]) -> tuple[str | None, str | None]:
    """Submit Apify website crawler run. Returns (run_id, dataset_id)."""
    start_urls = [{"url": url} for url in urls]

    payload = {
        "startUrls":             start_urls,
        "maxCrawlPages":         len(urls) * MAX_PAGES_PER_URL,
        "maxCrawlDepth":         0,           # homepage only
        "crawlerType":           "cheerio",   # fast + cheap (no JS needed for signal scan)
        "outputFormats":         ["markdown"],
        "removeCookieWarnings":  True,
        "removeElementsCssSelector": "nav, footer, header, .cookie, #cookie, .menu, .sidebar",
    }

    params = {
        "token":             APIFY_API_TOKEN,
        "maxTotalChargeUsd": MAX_CHARGE_USD,
    }

    try:
        resp = requests.post(
            APIFY_RUN_URL,
            json=payload,
            params=params,
            timeout=30,
        )
        resp.raise_for_status()
        data       = resp.json().get("data") or {}
        run_id     = data.get("id")
        dataset_id = data.get("defaultDatasetId")
        print(f"  [apify_signals] Run started: {run_id} | {len(urls)} URLs")
        return run_id, dataset_id
    except Exception as e:
        print(f"  [apify_signals] Failed to start run: {e}")
        return None, None


def _wait_for_run(run_id: str, timeout: int = RUN_TIMEOUT_SECS) -> bool:
    """Poll run until SUCCEEDED or timeout."""
    status_url = f"https://api.apify.com/v2/actor-runs/{run_id}"
    params     = {"token": APIFY_API_TOKEN}
    elapsed    = 0
    interval   = 15

    while elapsed < timeout:
        try:
            resp   = requests.get(status_url, params=params, timeout=15)
            data   = resp.json().get("data") or {}
            status = data.get("status", "")
            usage  = data.get("usageTotalUsd", 0)
            done   = data.get("stats", {}).get("pagesFinished", 0)
            print(f"  [apify_signals] {status} | pages: {done} | cost: ${usage:.3f}")
            if status in ("SUCCEEDED", "FINISHED"):
                return True
            if status in ("FAILED", "ABORTED", "TIMED-OUT"):
                return False
        except Exception as e:
            print(f"  [apify_signals] Poll error: {e}")

        time.sleep(interval)
        elapsed += interval

    print(f"  [apify_signals] Timeout after {timeout}s")
    return False


def _fetch_results(dataset_id: str) -> list[dict]:
    """Download crawled page content from Apify dataset."""
    url    = APIFY_DS_URL.format(dataset_id=dataset_id)
    params = {"token": APIFY_API_TOKEN, "format": "json", "limit": 5000}
    try:
        resp = requests.get(url, params=params, timeout=30)
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        print(f"  [apify_signals] Failed to fetch dataset: {e}")
        return []


def _scan_signals(text: str) -> dict:
    """Scan page text for acquisition-relevant signals."""
    tl = text.lower()
    return {
        "offshore":    any(s in tl for s in OFFSHORE_SIGNALS),
        "legacy_tech": [s for s in LEGACY_TECH_SIGNALS  if s in tl],
        "modern_tech": [s for s in MODERN_TECH_SIGNALS  if s in tl],
        "pe_backed":   any(s in tl for s in PE_SIGNALS),
        "owner_op":    any(s in tl for s in OWNER_OPERATED_SIGNALS),
        "multi_state": any(s in tl for s in [
            "nationwide", "multiple states", "across the country",
            "all 50 states", "multi-state", "national clients",
        ]),
        "hiring":      any(s in tl for s in ["we're hiring", "join our team", "careers", "open positions"]),
        "distress":    any(s in tl for s in ["restructuring", "layoffs", "downsizing", "closing"]),
    }


def _normalize_domain(website: str) -> str:
    """Strip scheme and www to get bare domain."""
    url = website.strip().lower()
    url = url.replace("https://", "").replace("http://", "").replace("www.", "")
    return url.split("/")[0]


def enrich_signals(
    companies:      list[dict],
    max_sites:      int  = 500,    # max websites to visit per run
    cost_cap:       float = MAX_CHARGE_USD,
    skip_already_scanned: bool = True,
) -> list[dict]:
    """
    Visit company websites via Apify and update signal fields.

    companies — full company list (modifies in-place)
    max_sites — max URLs to crawl (cost control)
    cost_cap  — Apify cost cap in USD per run

    Returns same list with updated signal fields.
    """
    if not APIFY_API_TOKEN or APIFY_API_TOKEN == "YOUR_APIFY_API_TOKEN":
        print("[apify_signals] APIFY_API_TOKEN not set — skipping signal enrichment.")
        return companies

    # Build website → company index
    website_to_company: dict[str, dict] = {}
    for c in companies:
        website = (c.get("website") or "").strip()
        if not website or not website.startswith("http"):
            continue
        if skip_already_scanned and c.get("_apify_scanned"):
            continue
        domain = _normalize_domain(website)
        if domain and domain not in website_to_company:
            website_to_company[domain] = c

    if not website_to_company:
        print("[apify_signals] No websites to scan.")
        return companies

    # Cap to max_sites
    domains = list(website_to_company.keys())[:max_sites]
    urls    = [f"https://{d}" for d in domains]

    print(f"[apify_signals] Scanning {len(urls)} company websites for signals")
    print(f"  Signals: offshore, legacy_tech, PE-backed, owner-operated, multi-state")
    print(f"  Cost cap: ${cost_cap:.2f} | Est. cost: ${len(urls) * 0.001:.2f}")

    run_id, dataset_id = _run_crawler(urls)
    if not run_id:
        return companies

    success = _wait_for_run(run_id)
    if not success:
        print("[apify_signals] Crawl did not complete — partial results may be available")

    pages = _fetch_results(dataset_id)
    print(f"  [apify_signals] Pages crawled: {len(pages)}")

    updated   = 0
    now       = datetime.now(timezone.utc).isoformat()

    for page in pages:
        page_url = page.get("url") or page.get("loadedUrl") or ""
        content  = page.get("markdown") or page.get("text") or ""
        if not page_url or not content:
            continue

        domain = _normalize_domain(page_url)
        company = website_to_company.get(domain)
        if not company:
            continue

        signals = _scan_signals(content)

        # Update company record with detected signals
        if signals["offshore"]:
            company["offshore_mentions"] = True
        if signals["pe_backed"]:
            company["pe_backed"] = True
        if signals["multi_state"]:
            company["multi_state"] = True
        if signals["owner_op"]:
            if "owner_operated" not in (company.get("owner_signals") or []):
                company.setdefault("owner_signals", []).append("owner_operated")

        # Technology signals — add new ones, don't remove existing
        existing_tech = set(company.get("technology_signals") or [])
        for sig in signals["legacy_tech"]:
            existing_tech.add(f"legacy:{sig}")
        for sig in signals["modern_tech"]:
            existing_tech.add(f"modern:{sig}")
        company["technology_signals"] = list(existing_tech)

        # Store page text for scoring
        company["_website_text"]   = content[:3000].lower()  # first 3K chars
        company["_apify_scanned"]  = True
        company["date_updated"]    = now

        updated += 1

    offshore_found = sum(1 for c in companies if c.get("offshore_mentions"))
    pe_found       = sum(1 for c in companies if c.get("pe_backed"))
    owner_op_found = sum(1 for c in companies if "owner_operated" in (c.get("owner_signals") or []))

    print(f"\n[apify_signals] Complete: {updated} companies updated")
    print(f"  Offshore signals: {offshore_found} companies")
    print(f"  PE-backed:        {pe_found} companies")
    print(f"  Owner-operated:   {owner_op_found} companies")
    return companies
