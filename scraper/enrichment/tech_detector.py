"""
Website technology signal detector for RCM companies.

Visits each company website and extracts:
  - Legacy technology signals (manual/paper processes)
  - Modern tech signals (already automated)
  - Offshore/offshore-team mentions
  - PE backing signals
  - Owner-operated signals
  - Website text for scorer use

Stores results in company dict fields:
  technology_signals, offshore_mentions, pe_backed, owner_signals, _website_text
"""

from __future__ import annotations
import time
import random
import requests
from bs4 import BeautifulSoup
from fake_useragent import UserAgent
from tenacity import retry, wait_exponential, stop_after_attempt, retry_if_exception_type
from requests.exceptions import RequestException
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
from config import (
    LEGACY_TECH_SIGNALS, MODERN_TECH_SIGNALS, OFFSHORE_SIGNALS,
    PE_SIGNALS, OWNER_OPERATED_SIGNALS, RATE_LIMITS,
)

_ua = UserAgent()
_DELAY = RATE_LIMITS["tech_detector"]["delay_seconds"]
_DAILY_CAP = RATE_LIMITS["tech_detector"]["daily_cap"]
_visited_count = 0


def _get_headers() -> dict:
    return {
        "User-Agent": _ua.random,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.5",
        "Accept-Encoding": "gzip, deflate",
        "Connection": "keep-alive",
    }


@retry(
    wait=wait_exponential(min=4, max=30),
    stop=stop_after_attempt(2),
    retry=retry_if_exception_type(RequestException),
    reraise=False,
)
def _fetch_text(url: str, timeout: int = 10) -> str:
    """Fetch page text from a URL. Returns empty string on failure."""
    if not url.startswith("http"):
        url = "https://" + url
    resp = requests.get(url, headers=_get_headers(), timeout=timeout, allow_redirects=True)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "html.parser")
    # Remove scripts and styles
    for tag in soup(["script", "style", "noscript"]):
        tag.decompose()
    return soup.get_text(separator=" ", strip=True).lower()


def detect_signals(company: dict) -> dict:
    """
    Visit company website and populate technology/signal fields.
    Rate-limited. Returns updated company dict.
    """
    global _visited_count
    if _visited_count >= _DAILY_CAP:
        return company

    website = company.get("website")
    if not website:
        return company

    time.sleep(_DELAY + random.uniform(0, _DELAY * 0.2))

    try:
        text = _fetch_text(website)
    except Exception as e:
        print(f"  [tech_detector] Failed {website}: {e}")
        return company

    _visited_count += 1

    # Store full text for scorer (not persisted to JSON — private field)
    company["_website_text"] = text[:50_000]  # Cap at 50KB

    # Legacy tech signals
    found_legacy = [sig for sig in LEGACY_TECH_SIGNALS if sig in text]
    existing_tech = company.get("technology_signals") or []
    company["technology_signals"] = list(set(existing_tech + found_legacy))

    # Modern tech signals
    found_modern = [sig for sig in MODERN_TECH_SIGNALS if sig in text]
    if found_modern:
        company["technology_signals"] = list(
            set(company["technology_signals"] + [f"[modern] {s}" for s in found_modern])
        )

    # Offshore mentions
    offshore_found = any(sig in text for sig in OFFSHORE_SIGNALS)
    company["offshore_mentions"] = offshore_found

    # PE backing
    pe_found = any(sig in text for sig in PE_SIGNALS)
    if pe_found:
        company["pe_backed"] = True

    # Owner-operated signals
    owner_found = [sig for sig in OWNER_OPERATED_SIGNALS if sig in text]
    existing_owner = company.get("owner_signals") or []
    company["owner_signals"] = list(set(existing_owner + owner_found))

    # Specialty detection from website text
    if not company.get("specialties"):
        from config import SPECIALTY_KEYWORDS
        detected = []
        for spec, keywords in SPECIALTY_KEYWORDS.items():
            if any(kw in text for kw in keywords):
                detected.append(spec)
        if detected:
            company["specialties"] = detected

    return company


def detect_all(companies: list[dict], max_count: int | None = None) -> list[dict]:
    """Run tech detection on all companies. Respects daily cap."""
    cap = min(max_count or len(companies), _DAILY_CAP)
    for i, c in enumerate(companies):
        if i >= cap:
            break
        print(f"  [tech_detector] {i+1}/{cap} — {c.get('company_name', 'unknown')}")
        detect_signals(c)
    return companies
