"""
Quick Apify connectivity test — run this locally before triggering the full scrape.

Usage:
    python test_apify.py

Set your Apify token either:
  - In the APIFY_API_TOKEN environment variable
  - Or paste it below as TEST_TOKEN (never commit with a real token)
"""

import os
import sys
import requests

# ── Config ────────────────────────────────────────────────────────────────────
TEST_TOKEN = os.getenv("APIFY_API_TOKEN", "")   # set env var, don't paste here

WEBSITE_CRAWLER_ACTOR = "apify~website-content-crawler"
GMAPS_ACTOR           = "compass~crawler-google-places"

BASE = "https://api.apify.com/v2"


def check_token(token: str) -> bool:
    print("\n[1] Validating Apify token...")
    resp = requests.get(f"{BASE}/users/me", params={"token": token}, timeout=10)
    if resp.ok:
        data = resp.json().get("data") or {}
        print(f"    OK  — Username: {data.get('username')}")
        plan = (data.get("plan") or {})
        print(f"    Plan: {plan.get('id', '?')} | Monthly credits: ${plan.get('monthlyUsageCreditsUsd', '?')}")
        return True
    print(f"    FAIL — HTTP {resp.status_code}: {resp.text[:200]}")
    return False


def check_actor(token: str, actor_id: str) -> bool:
    print(f"\n[2] Checking actor: {actor_id} ...")
    actor_url_id = actor_id.replace("/", "~")
    resp = requests.get(
        f"{BASE}/acts/{actor_url_id}",
        params={"token": token},
        timeout=10,
    )
    if resp.ok:
        data = resp.json().get("data") or {}
        print(f"    OK  — {data.get('name')} (ID: {data.get('id', '?')})")
        print(f"    Pricing: {data.get('pricingInfos', [{}])[0].get('pricingModel', '?')}")
        return True
    print(f"    FAIL — HTTP {resp.status_code}: {resp.text[:300]}")
    return False


def test_signal_run(token: str) -> bool:
    """Submit a tiny 1-URL test run to website-content-crawler."""
    print(f"\n[3] Test run: website-content-crawler (1 URL, $0.01 cap)...")
    actor_id = WEBSITE_CRAWLER_ACTOR
    payload = {
        "startUrls":    [{"url": "https://example.com"}],
        "maxCrawlDepth": 0,
        "crawlerType":  "cheerio",
        "outputFormats": ["markdown", "text"],
        "maxResults":   1,
    }
    params = {
        "token":             token,
        "maxTotalChargeUsd": "0.01",
    }
    resp = requests.post(
        f"{BASE}/acts/{actor_id}/runs",
        json=payload,
        params=params,
        timeout=20,
    )
    if resp.ok:
        data = resp.json().get("data") or {}
        run_id = data.get("id")
        print(f"    OK  — Run started: {run_id}")
        print(f"    Status: {data.get('status')}")
        return True
    print(f"    FAIL — HTTP {resp.status_code}")
    print(f"    Response: {resp.text[:500]}")
    return False


def test_gmaps_run(token: str) -> bool:
    """Submit a tiny 1-query test run to compass~crawler-google-places."""
    print(f"\n[4] Test run: compass~crawler-google-places (1 query, $0.05 cap)...")
    actor_id = GMAPS_ACTOR
    payload = {
        "searchStringsArray":        ["medical billing company in Phoenix, AZ"],
        "maxCrawledPlacesPerSearch": 3,
        "language":                  "en",
        "skipClosedPlaces":          True,
        "includeWebResults":         False,
        "maxTotalChargeUsd":         0.05,
    }
    params = {
        "token":             token,
        "maxTotalChargeUsd": "0.05",
    }
    resp = requests.post(
        f"{BASE}/acts/{actor_id}/runs",
        json=payload,
        params=params,
        timeout=20,
    )
    if resp.ok:
        data = resp.json().get("data") or {}
        run_id = data.get("id")
        print(f"    OK  — Run started: {run_id}")
        print(f"    Status: {data.get('status')}")
        return True
    print(f"    FAIL — HTTP {resp.status_code}")
    print(f"    Response: {resp.text[:500]}")
    return False


def main():
    token = TEST_TOKEN
    if not token:
        print("ERROR: APIFY_API_TOKEN not set.")
        print("  Set it with:  set APIFY_API_TOKEN=apify_api_xxxx  (Windows)")
        print("  or:           export APIFY_API_TOKEN=apify_api_xxxx  (Mac/Linux)")
        sys.exit(1)

    print("=" * 55)
    print("  Apify Connectivity Test")
    print("=" * 55)

    ok = check_token(token)
    if not ok:
        print("\nToken invalid — fix token first.")
        sys.exit(1)

    check_actor(token, WEBSITE_CRAWLER_ACTOR)
    check_actor(token, GMAPS_ACTOR)

    # Optionally test actual runs (costs tiny amount of credits)
    if "--run" in sys.argv:
        test_signal_run(token)
        test_gmaps_run(token)

    print("\n" + "=" * 55)
    print("  Done — check output above for FAIL lines.")
    print("  Run with --run flag to also test live actor runs.")
    print("=" * 55)


if __name__ == "__main__":
    main()
