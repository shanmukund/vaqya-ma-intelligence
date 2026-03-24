"""
Google Places API scraper — highest yield source for RCM companies.

For each target metro + each search query, pages through Google Places
text search results and returns normalized company dicts.

Requires: GOOGLE_MAPS_API_KEY in config.py or environment.
Cost estimate: ~$0.032/request × ~1,500 requests = ~$48 per full run.
"""

from __future__ import annotations
import time
import uuid
import random
from datetime import datetime, timezone
from typing import Generator
import googlemaps
from googlemaps.exceptions import ApiError, HTTPError, TransportError
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
from config import GOOGLE_MAPS_API_KEY, GOOGLE_MAPS_QUERIES, TARGET_METROS, RATE_LIMITS

_DELAY = RATE_LIMITS["google_maps"]["delay_seconds"]
_DAILY_CAP = RATE_LIMITS["google_maps"]["daily_cap"]


def _build_client() -> googlemaps.Client:
    if GOOGLE_MAPS_API_KEY == "YOUR_GOOGLE_MAPS_API_KEY":
        raise ValueError(
            "Google Maps API key not set. "
            "Set GOOGLE_MAPS_API_KEY environment variable or update config.py."
        )
    return googlemaps.Client(key=GOOGLE_MAPS_API_KEY)


def _normalize_result(place: dict, metro: str, state: str) -> dict:
    """Convert a Google Places result to our standard company dict."""
    address_components = place.get("formatted_address", "")
    # Parse city/state/zip from formatted address
    city = ""
    parsed_state = state
    zip_code = ""
    parts = [p.strip() for p in address_components.split(",")]
    if len(parts) >= 3:
        city = parts[-3] if len(parts) >= 3 else ""
        state_zip = parts[-2].strip() if len(parts) >= 2 else ""
        sz_parts = state_zip.split()
        if sz_parts:
            parsed_state = sz_parts[0]
        if len(sz_parts) > 1:
            zip_code = sz_parts[1]

    return {
        "id":                   str(uuid.uuid4()),
        "company_name":         place.get("name", ""),
        "website":              place.get("website", ""),
        "phone":                place.get("formatted_phone_number", ""),
        "address":              place.get("vicinity", address_components),
        "city":                 city,
        "state":                parsed_state.upper()[:2] if parsed_state else state,
        "metro_region":         metro,
        "zip":                  zip_code,
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
        "job_posting_count":    0,
        "job_titles_found":     [],
        "source":               ["google_maps"],
        "data_confidence":      "medium",
        "pipeline_stage":       "Prospect",
        "assigned_to":          None,
        "priority":             None,
        "notes":                [],
        "next_action":          None,
        "next_action_due":      None,
        "contacts":             [],
        "date_added":           datetime.now(timezone.utc).isoformat(),
        "date_updated":         datetime.now(timezone.utc).isoformat(),
        "_google_place_id":     place.get("place_id", ""),
    }


def _scrape_metro_query(
    client: googlemaps.Client,
    query: str,
    metro: str,
    state: str,
    lat: float,
    lng: float,
    request_count: list,
) -> list[dict]:
    """Scrape all pages for one (metro, query) pair."""
    results = []
    full_query = f"{query} near {metro} {state}"

    try:
        response = client.places(
            query=full_query,
            location=(lat, lng),
            radius=40_000,  # 40km radius
            type="establishment",
        )
    except (ApiError, HTTPError, TransportError) as e:
        print(f"  [google_maps] API error for '{full_query}': {e}")
        return results

    request_count[0] += 1

    while True:
        for place in response.get("results", []):
            # Fetch place details for phone + website
            time.sleep(_DELAY + random.uniform(0, 0.3))
            request_count[0] += 1
            if request_count[0] >= _DAILY_CAP:
                print(f"  [google_maps] Daily cap {_DAILY_CAP} reached.")
                return results
            try:
                details = client.place(
                    place["place_id"],
                    fields=["name", "formatted_address", "formatted_phone_number",
                            "website", "vicinity", "place_id"],
                )
                results.append(_normalize_result(details.get("result", place), metro, state))
            except Exception as e:
                print(f"  [google_maps] Detail fetch failed: {e}")
                results.append(_normalize_result(place, metro, state))

        next_token = response.get("next_page_token")
        if not next_token or request_count[0] >= _DAILY_CAP:
            break
        time.sleep(2.5)  # Google requires delay before next_page_token is valid
        try:
            response = client.places(page_token=next_token)
            request_count[0] += 1
        except Exception as e:
            print(f"  [google_maps] Pagination error: {e}")
            break

    return results


def scrape(metros: list[dict] | None = None, queries: list[str] | None = None) -> list[dict]:
    """
    Scrape Google Places for RCM companies across target metros.

    metros  — subset of TARGET_METROS (default: all)
    queries — subset of GOOGLE_MAPS_QUERIES (default: all)
    """
    metros  = metros  or TARGET_METROS
    queries = queries or GOOGLE_MAPS_QUERIES

    try:
        client = _build_client()
    except ValueError as e:
        print(f"[google_maps] Skipping: {e}")
        return []

    all_results: list[dict] = []
    request_count = [0]  # mutable ref for tracking across calls

    total = len(metros) * len(queries)
    done  = 0

    for metro_info in metros:
        metro = metro_info["metro"]
        state = metro_info["state"]
        lat   = metro_info["lat"]
        lng   = metro_info["lng"]

        for query in queries:
            done += 1
            print(f"  [google_maps] {done}/{total} — {query} in {metro}, {state} "
                  f"(requests: {request_count[0]})")

            if request_count[0] >= _DAILY_CAP:
                print(f"  [google_maps] Daily cap reached. Stopping.")
                return all_results

            results = _scrape_metro_query(client, query, metro, state, lat, lng, request_count)
            all_results.extend(results)
            print(f"    → {len(results)} results (running total: {len(all_results)})")
            time.sleep(_DELAY)

    print(f"[google_maps] Complete: {len(all_results)} raw results, {request_count[0]} API calls")
    return all_results
