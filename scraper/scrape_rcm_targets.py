"""
Vaqya M&A RCM Target Scraper — Main Orchestrator
══════════════════════════════════════════════════

Usage:
    python scrape_rcm_targets.py [options]

Options:
    --sources       Comma-separated list of sources to run (default: all free)
                    Choices: nppes, indeed, sos, yellowpages, clutch, hfma_mgma,
                             linkedin, google_maps
    --metros        Comma-separated metro names to limit scope (default: all)
    --no-tech-scan  Skip website technology signal detection
    --dry-run       Run without writing output file

FREE sources (no API key needed):
    nppes, indeed, sos, yellowpages, clutch, hfma_mgma, linkedin
FREE sources (key needed — Brave API free 2K/mo):
    linkedin URL discovery upgrades automatically when BRAVE_API_KEY is set

OPTIONAL PAID sources — Phase 2 (skipped gracefully until key added):
    google_maps (~$48/run), linkedin (higher volume via SerpAPI)

Examples:
    python scrape_rcm_targets.py
    python scrape_rcm_targets.py --sources nppes,indeed --metros "Chicago,Dallas,Atlanta"
    python scrape_rcm_targets.py --sources nppes,clutch,yellowpages --no-tech-scan

Output:  scraper/rcm_targets.json
"""

from __future__ import annotations
import argparse
import json
import os
import sys
import time
from datetime import datetime, timezone
from typing import Any

# ─── Source modules ────────────────────────────────────────────────────────────
# Free (no API key required)
from sources import nppes, indeed_jobs, secretary_of_state
from sources import yellowpages, clutch, hfma_mgma, linkedin_public
# Phase 2 paid (gracefully skipped when key not set)
from sources import google_maps

# ─── Enrichment + scoring ─────────────────────────────────────────────────────
from enrichment.deduplicator   import deduplicate
from enrichment.revenue_estimator import estimate_revenue
from enrichment.tech_detector  import detect_all
from scoring.scorer            import score_all

from config import OUTPUT_FILE, TARGET_METROS, TIER2_METROS, BRAVE_API_KEY, SEARCHAPI_KEY

# ── Source tiers ───────────────────────────────────────────────────────────────
# Phase 1 — fully free (active now)
ALL_SOURCES  = ["nppes", "indeed", "sos", "yellowpages", "clutch", "hfma_mgma",
                "linkedin", "google_maps"]
FREE_SOURCES = ["nppes", "indeed", "sos", "yellowpages", "clutch", "hfma_mgma", "linkedin"]
# Phase 2 — paid (not in default run)
PAID_SOURCES = ["google_maps"]

def _default_sources() -> list[str]:
    """
    Build the default source list for this run.
    - Always includes all FREE_SOURCES.
    - Adds google_maps automatically if SEARCHAPI_KEY is set.
    - Brave key upgrades LinkedIn URL discovery internally (no extra source entry).
    """
    sources = list(FREE_SOURCES)
    if BRAVE_API_KEY:
        print("[main] BRAVE_API_KEY detected — LinkedIn will use Brave Search API "
              "($5 free credits/mo = 1,000 requests/mo)")
    if SEARCHAPI_KEY and SEARCHAPI_KEY != "YOUR_SEARCHAPI_KEY":
        sources.append("google_maps")
        print("[main] SEARCHAPI_KEY detected — Google Maps (tier-2 metros) ENABLED "
              "(100 lifetime free credits → use sparingly)")
    return sources


# ─── I/O helpers ──────────────────────────────────────────────────────────────
def load_existing() -> list[dict]:
    """Load existing rcm_targets.json, if present."""
    if not os.path.exists(OUTPUT_FILE):
        return []
    try:
        with open(OUTPUT_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        targets = data.get("targets") or []
        print(f"[main] Loaded {len(targets)} existing targets from {OUTPUT_FILE}")
        return targets
    except Exception as e:
        print(f"[main] Warning: could not load existing targets: {e}")
        return []


def save_output(targets: list[dict], dry_run: bool = False) -> None:
    """Write targets to rcm_targets.json."""
    # Strip internal-only fields before saving
    clean = []
    for t in targets:
        rec = {k: v for k, v in t.items() if not k.startswith("_")}
        clean.append(rec)

    # Sort by composite score descending
    clean.sort(key=lambda x: (x.get("scores") or {}).get("composite", 0), reverse=True)

    output = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "total_count":  len(clean),
        "targets":      clean,
    }

    if dry_run:
        print(f"[main] DRY RUN — would write {len(clean)} targets to {OUTPUT_FILE}")
        return

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(output, f, indent=2, ensure_ascii=False)
    print(f"[main] Saved {len(clean)} targets → {OUTPUT_FILE}")


def _filter_metros(metro_names: list[str]) -> list[dict]:
    """Filter TARGET_METROS by requested metro names."""
    names_lower = {n.lower().strip() for n in metro_names}
    return [m for m in TARGET_METROS if m["metro"].lower() in names_lower]


# ─── Main orchestration ────────────────────────────────────────────────────────
def run(
    sources:      list[str]  = ALL_SOURCES,
    metros:       list[dict] | None = None,
    tech_scan:    bool = True,
    dry_run:      bool = False,
) -> list[dict]:

    start_time = time.time()
    print(f"\n{'='*60}")
    print(f"  Vaqya M&A RCM Scraper -- {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print(f"  Sources: {', '.join(sources)}")
    print(f"  Metros:  {'all' if metros is None else len(metros)}")
    print(f"{'='*60}\n")

    # Step 1: Load existing targets (to preserve pipeline state)
    existing = load_existing()

    # Step 2: Run selected scrapers
    all_raw: list[dict] = []

    step = 0

    # ── FREE SOURCES (no key required) ────────────────────────────────────────
    if "nppes" in sources:
        step += 1
        print(f"\n[{step}] NPPES NPI Registry (FREE — CMS government database)...")
        results = nppes.scrape()
        all_raw.extend(results)
        print(f"      → {len(results)} records")

    if "indeed" in sources:
        step += 1
        print(f"\n[{step}] Indeed Job Postings (FREE)...")
        results = indeed_jobs.scrape(metros=metros or TARGET_METROS[:15])
        all_raw.extend(results)
        print(f"      → {len(results)} records")

    if "sos" in sources:
        step += 1
        print(f"\n[{step}] Secretary of State Registries (FREE)...")
        results = secretary_of_state.scrape()
        all_raw.extend(results)
        print(f"      → {len(results)} records")

    if "yellowpages" in sources:
        step += 1
        print(f"\n[{step}] Yellow Pages Directory (FREE)...")
        results = yellowpages.scrape(metros=metros)
        all_raw.extend(results)
        print(f"      → {len(results)} records")

    if "clutch" in sources:
        step += 1
        print(f"\n[{step}] Clutch.co RCM Directory (FREE — employee + revenue data)...")
        results = clutch.scrape()
        all_raw.extend(results)
        print(f"      → {len(results)} records")

    if "hfma_mgma" in sources:
        step += 1
        print(f"\n[{step}] HFMA/MGMA Chapter Directories (FREE)...")
        results = hfma_mgma.scrape()
        all_raw.extend(results)
        print(f"      → {len(results)} records")

    if "linkedin" in sources:
        step += 1
        print(f"\n[{step}] LinkedIn Public Pages (FREE via DuckDuckGo, or SerpAPI if key set)...")
        results = linkedin_public.scrape()
        all_raw.extend(results)
        print(f"      → {len(results)} records")

    # ── SEARCHAPI GOOGLE MAPS (free 100 lifetime credits) ─────────────────────
    if "google_maps" in sources:
        step += 1
        print(f"\n[{step}] Google Maps via SearchAPI.io (100 free lifetime credits)...")
        print(f"      Targeting tier-2 metros — owner-operated, lower PE competition")
        # Use caller-specified metros if given, else default to TIER2_METROS
        gm_metros = metros if metros is not None else None
        results = google_maps.scrape(metros=gm_metros)
        all_raw.extend(results)
        print(f"      → {len(results)} records")

    print(f"\n[main] Total raw records: {len(all_raw)}")

    if not all_raw and not existing:
        print("[main] No data collected. Check API keys and network connectivity.")
        return []

    # Step 3: Deduplicate across sources + merge with existing
    print("\n[enrich] Deduplicating...")
    merged = deduplicate(all_raw, existing)

    # Step 4: Technology signal detection (website visits)
    if tech_scan and merged:
        print(f"\n[enrich] Technology detection (visiting websites)...")
        detect_all(merged)

    # Step 5: Revenue estimation
    print("\n[enrich] Estimating revenues...")
    for company in merged:
        estimate_revenue(company)

    # Step 6: Score all companies
    print("\n[score] Scoring all companies...")
    score_all(merged)

    # Step 7: Print summary
    _print_summary(merged)

    # Step 8: Save output
    save_output(merged, dry_run=dry_run)

    elapsed = time.time() - start_time
    print(f"\n[main] Done in {elapsed/60:.1f} minutes.\n")
    return merged


def _print_summary(targets: list[dict]) -> None:
    tier_counts: dict[str, int] = {}
    state_counts: dict[str, int] = {}
    revenue_counts: dict[str, int] = {}

    for t in targets:
        scores = t.get("scores") or {}
        tier = scores.get("priority_tier", "—")
        tier_counts[tier] = tier_counts.get(tier, 0) + 1

        state = t.get("state", "Unknown")
        state_counts[state] = state_counts.get(state, 0) + 1

        band = t.get("revenue_band", "Unknown")
        revenue_counts[band] = revenue_counts.get(band, 0) + 1

    print(f"\n{'─'*50}")
    print(f"  SUMMARY — {len(targets)} total targets")
    print(f"{'─'*50}")
    print("  Priority Tiers:")
    for tier in ["A", "B", "C", "D", "—"]:
        count = tier_counts.get(tier, 0)
        bar = "█" * min(count, 30)
        print(f"    Tier {tier}: {bar} {count}")
    print(f"\n  Top States:")
    for state, count in sorted(state_counts.items(), key=lambda x: -x[1])[:8]:
        print(f"    {state:4s}: {count}")
    print(f"\n  Revenue Bands:")
    for band in ["$5M-$15M", "$15M-$30M", "$2M-$5M", "$30M-$50M", "$50M+", "Unknown"]:
        count = revenue_counts.get(band, 0)
        if count:
            print(f"    {band:15s}: {count}")
    print(f"{'─'*50}\n")


# ─── CLI entry point ──────────────────────────────────────────────────────────
def main() -> None:
    parser = argparse.ArgumentParser(description="Vaqya M&A RCM Target Scraper")
    parser.add_argument("--sources",      default="",
                        help="Comma-separated sources to run. "
                             "Default: auto (free + any keys detected). "
                             "Choices: nppes,indeed,sos,yellowpages,clutch,hfma_mgma,"
                             "linkedin,google_maps")
    parser.add_argument("--metros",       default="",
                        help="Comma-separated metro names to limit scope")
    parser.add_argument("--no-tech-scan", action="store_true",
                        help="Skip website technology detection")
    parser.add_argument("--dry-run",      action="store_true",
                        help="Run without writing output file")
    args = parser.parse_args()

    # If no --sources flag given, auto-detect based on keys present
    sources = [s.strip() for s in args.sources.split(",") if s.strip()]
    if not sources:
        sources = _default_sources()
    metros  = None
    if args.metros:
        metro_names = [m.strip() for m in args.metros.split(",")]
        metros = _filter_metros(metro_names)
        if not metros:
            print(f"[main] Warning: no matching metros found for: {metro_names}")
            sys.exit(1)

    run(
        sources=sources,
        metros=metros,
        tech_scan=not args.no_tech_scan,
        dry_run=args.dry_run,
    )


if __name__ == "__main__":
    main()
