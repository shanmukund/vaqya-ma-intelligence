"""
Apollo.io Bulk Export CSV → rcm_targets.json importer.

How to export from Apollo:
  1. apollo.io → Search → Companies
  2. Filters:
       Industry: "Medical Practice" or "Hospital & Health Care"
       Keywords: "medical billing" OR "revenue cycle" OR "rcm"
       Employees: 1–500 (sweet spot for acquisition targets)
  3. Select all → Export → CSV
  4. Save the file anywhere on your computer
  5. Run:
       python apollo_csv_import.py --csv path/to/apollo_export.csv

The script will:
  - Parse Apollo's CSV columns (name, domain, employees, revenue, etc.)
  - Normalize to our standard company dict format
  - Merge with existing rcm_targets.json (preserving pipeline stages/notes)
  - Re-score all records
  - Save updated rcm_targets.json

Apollo CSV columns we use (others are ignored):
  Company Name, Website, # Employees, Annual Revenue,
  City, State, Country, Founded Year, Industry, Keywords,
  LinkedIn URL, Phone, Person Name, Person Title, Email
"""

from __future__ import annotations
import argparse
import csv
import json
import uuid
import re
import sys
import os
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(__file__))
from config import OUTPUT_FILE
from enrichment.deduplicator import deduplicate
from scoring.scorer import score_all
from enrichment.revenue_estimator import estimate_revenue

# ─── Apollo CSV column name mappings ──────────────────────────────────────────
# Apollo may vary column names slightly by export type — we try all variants
COL_COMPANY     = ["Company Name", "company_name", "Name", "Organization Name"]
COL_WEBSITE     = ["Website", "website", "Domain", "Company Domain"]
COL_EMPLOYEES   = ["# Employees", "Employees", "Employee Count", "Number of Employees", "employees"]
COL_REVENUE     = ["Annual Revenue", "Revenue", "annual_revenue", "Estimated Annual Revenue"]
COL_CITY        = ["City", "city", "HQ City"]
COL_STATE       = ["State", "state", "HQ State"]
COL_COUNTRY     = ["Country", "country"]
COL_FOUNDED     = ["Founded Year", "Year Founded", "founded_year", "Founded"]
COL_INDUSTRY    = ["Industry", "industry", "Industries"]
COL_KEYWORDS    = ["Keywords", "keywords", "Specialties"]
COL_LINKEDIN    = ["LinkedIn URL", "linkedin_url", "LinkedIn", "Company Linkedin Url"]
COL_PHONE       = ["Phone", "phone", "Company Phone", "Phone Number"]
COL_PERSON_NAME = ["Person Name", "Contact Name", "First Name + Last Name"]
COL_PERSON_TITLE= ["Person Title", "Title", "Contact Title", "Job Title"]
COL_EMAIL       = ["Email", "email", "Person Email", "Contact Email"]


# ─── Revenue string → our band ────────────────────────────────────────────────
def _parse_revenue_string(rev_str: str) -> tuple[int | None, str]:
    """
    Parse Apollo revenue strings like '$5M', '$2.5M', '$12,500,000', '<$1M'.
    Returns (estimated_revenue_int, revenue_band_str).
    """
    if not rev_str or rev_str.strip() in ("", "-", "N/A", "Unknown"):
        return None, "Unknown"

    rev_str = rev_str.strip().upper().replace(",", "").replace("$", "")

    try:
        # Handle 'M' suffix: "5M", "2.5M", "12.5M"
        if rev_str.endswith("M"):
            val = float(rev_str[:-1]) * 1_000_000
        elif rev_str.endswith("B"):
            val = float(rev_str[:-1]) * 1_000_000_000
        elif rev_str.endswith("K"):
            val = float(rev_str[:-1]) * 1_000
        else:
            val = float(rev_str)

        val = int(val)

        # Map to our revenue bands
        if val >= 50_000_001:  band = "$50M+"
        elif val >= 30_000_001: band = "$30M-$50M"
        elif val >= 15_000_001: band = "$15M-$30M"
        elif val >= 5_000_001:  band = "$5M-$15M"
        elif val >= 2_000_001:  band = "$2M-$5M"
        elif val >= 1_000_001:  band = "$1M-$2M"
        else:                   band = "<$1M"

        return val, band

    except (ValueError, TypeError):
        return None, "Unknown"


def _parse_employee_string(emp_str: str) -> tuple[int | None, str | None]:
    """
    Parse Apollo employee strings like '11-50', '51-200', '200', '500+'.
    Returns (midpoint_int, range_str).
    """
    if not emp_str or emp_str.strip() in ("", "-", "N/A"):
        return None, None

    emp_str = emp_str.strip().replace(",", "")

    # Range: "11-50", "51-200"
    m = re.search(r"(\d+)\s*[-–]\s*(\d+)", emp_str)
    if m:
        lo, hi = int(m.group(1)), int(m.group(2))
        return (lo + hi) // 2, f"{lo}-{hi}"

    # Plus: "500+", "1000+"
    m = re.search(r"(\d+)\+", emp_str)
    if m:
        val = int(m.group(1))
        return val, f"{val}+"

    # Plain number
    m = re.search(r"(\d+)", emp_str)
    if m:
        val = int(m.group(1))
        return val, str(val)

    return None, None


def _get_col(row: dict, candidates: list[str]) -> str:
    """Return first matching column value from row, or empty string."""
    for col in candidates:
        if col in row and row[col]:
            return str(row[col]).strip()
    return ""


def _normalize_row(row: dict) -> dict | None:
    """Convert one Apollo CSV row to our standard company dict."""
    name = _get_col(row, COL_COMPANY)
    if not name:
        return None

    # Skip non-US companies
    country = _get_col(row, COL_COUNTRY).upper()
    if country and country not in ("US", "USA", "UNITED STATES", ""):
        return None

    website  = _get_col(row, COL_WEBSITE)
    phone    = _get_col(row, COL_PHONE)
    city     = _get_col(row, COL_CITY).title()
    state    = _get_col(row, COL_STATE).upper()[:2] if _get_col(row, COL_STATE) else ""
    linkedin = _get_col(row, COL_LINKEDIN)
    industry = _get_col(row, COL_INDUSTRY)
    keywords = _get_col(row, COL_KEYWORDS)

    # Employee count
    emp_str = _get_col(row, COL_EMPLOYEES)
    emp_est, emp_range = _parse_employee_string(emp_str)

    # Revenue
    rev_str = _get_col(row, COL_REVENUE)
    est_rev, rev_band = _parse_revenue_string(rev_str)

    # Founded year
    founded_str = _get_col(row, COL_FOUNDED)
    founded_year = None
    company_age  = None
    if founded_str:
        try:
            founded_year = int(re.search(r"\d{4}", founded_str).group())
            company_age  = datetime.now().year - founded_year
        except (AttributeError, ValueError):
            pass

    # Contact info (from Apollo people columns)
    contacts = []
    person_name  = _get_col(row, COL_PERSON_NAME)
    person_title = _get_col(row, COL_PERSON_TITLE)
    person_email = _get_col(row, COL_EMAIL)
    if person_name:
        contacts.append({
            "name":         person_name,
            "title":        person_title,
            "email":        person_email,
            "linkedin_url": "",
            "phone":        "",
            "source":       "apollo_csv",
            "date_added":   datetime.now(timezone.utc).isoformat(),
        })

    # Specialties from keywords
    spec_list = [s.strip() for s in keywords.split(",") if s.strip()] if keywords else []

    # PE / offshore signals from keywords/industry
    kw_lower = keywords.lower() + " " + industry.lower()
    pe_backed = any(sig in kw_lower for sig in [
        "private equity", "pe-backed", "portfolio", "venture", "growth equity"
    ])
    offshore = any(sig in kw_lower for sig in [
        "offshore", "india", "philippines", "bpo", "nearshore"
    ])

    now = datetime.now(timezone.utc).isoformat()

    # Best-effort metro from state
    from config import TARGET_METROS
    metro = next(
        (m["metro"] for m in TARGET_METROS if m["state"] == state),
        city or state
    )

    return {
        "id":                   str(uuid.uuid4()),
        "company_name":         name,
        "website":              website,
        "phone":                phone,
        "address":              "",
        "city":                 city,
        "state":                state,
        "metro_region":         metro,
        "zip":                  "",
        "estimated_revenue":    est_rev,
        "revenue_band":         rev_band,
        "employee_count_range": emp_range,
        "employee_count_est":   emp_est,
        "founded_year":         founded_year,
        "company_age":          company_age,
        "owner_signals":        [],
        "specialties":          spec_list,
        "technology_signals":   [],
        "pe_backed":            pe_backed,
        "offshore_mentions":    offshore,
        "multi_state":          False,
        "recent_funding":       False,
        "job_posting_count":    0,
        "job_titles_found":     [],
        "source":               ["apollo_csv"],
        "data_confidence":      "high" if est_rev else "medium",
        "pipeline_stage":       "Prospect",
        "assigned_to":          None,
        "priority":             None,
        "notes":                [],
        "next_action":          None,
        "next_action_due":      None,
        "contacts":             contacts,
        "date_added":           now,
        "date_updated":         now,
        "_linkedin_url":        linkedin,
        "_apollo_revenue_printed": rev_str,
    }


def import_csv(csv_path: str, dry_run: bool = False) -> list[dict]:
    """
    Parse Apollo CSV export and return list of normalized company dicts.
    """
    if not os.path.exists(csv_path):
        print(f"[apollo_csv] ERROR: File not found: {csv_path}")
        sys.exit(1)

    print(f"[apollo_csv] Reading: {csv_path}")
    companies = []
    skipped   = 0

    with open(csv_path, newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        rows   = list(reader)

    print(f"[apollo_csv] {len(rows)} rows in CSV, columns: {', '.join(reader.fieldnames or [])}")

    for row in rows:
        normalized = _normalize_row(row)
        if normalized:
            companies.append(normalized)
        else:
            skipped += 1

    print(f"[apollo_csv] Parsed: {len(companies)} valid US companies, {skipped} skipped")
    return companies


def merge_and_save(
    incoming:    list[dict],
    output_file: str = OUTPUT_FILE,
    dry_run:     bool = False,
) -> None:
    """Load existing rcm_targets.json, merge incoming, re-score, save."""

    # Load existing data
    existing = []
    if os.path.exists(output_file):
        with open(output_file) as f:
            data = json.load(f)
            existing = data.get("targets") or []
        print(f"[apollo_csv] Loaded {len(existing)} existing targets from {output_file}")
    else:
        print(f"[apollo_csv] No existing file — creating fresh dataset")

    # Estimate revenue for incoming records that have employee count but no revenue
    for c in incoming:
        estimate_revenue(c)

    # Deduplicate + merge
    print(f"[apollo_csv] Merging {len(incoming)} incoming with {len(existing)} existing...")
    merged = deduplicate(incoming, existing)

    # Score everything
    print(f"[apollo_csv] Scoring {len(merged)} companies...")
    score_all(merged)

    # Sort by composite score descending
    merged.sort(key=lambda c: (c.get("scores") or {}).get("composite", 0), reverse=True)

    # Count tier breakdown
    tiers: dict[str, int] = {}
    rev_known = 0
    contacts_count = 0
    for c in merged:
        tier = (c.get("scores") or {}).get("priority_tier", "—")
        tiers[tier] = tiers.get(tier, 0) + 1
        if (c.get("revenue_band") or "Unknown") != "Unknown":
            rev_known += 1
        if c.get("contacts"):
            contacts_count += 1

    output = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "total_count":  len(merged),
        "source":       "apollo_csv_import + scraper",
        "targets":      merged,
    }

    if dry_run:
        print(f"\n[apollo_csv] DRY RUN — would save {len(merged)} targets (not writing file)")
    else:
        with open(output_file, "w") as f:
            json.dump(output, f, indent=2, default=str)
        print(f"\n[apollo_csv] ✅ Saved {len(merged)} targets to {output_file}")

    print(f"\n{'='*55}")
    print(f"  Import Summary")
    print(f"{'='*55}")
    print(f"  Total companies:      {len(merged)}")
    print(f"  With revenue data:    {rev_known} ({rev_known*100//max(len(merged),1)}%)")
    print(f"  With contacts:        {contacts_count}")
    print(f"\n  Priority Tiers:")
    for tier in ["A", "B", "C", "D", "—"]:
        if tier in tiers:
            print(f"    Tier {tier}: {tiers[tier]}")
    print(f"{'='*55}\n")


def main():
    parser = argparse.ArgumentParser(
        description="Import Apollo.io bulk CSV export into rcm_targets.json"
    )
    parser.add_argument(
        "--csv",
        required=True,
        help="Path to Apollo CSV export file",
    )
    parser.add_argument(
        "--output",
        default=OUTPUT_FILE,
        help=f"Output JSON file (default: {OUTPUT_FILE})",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Parse and show stats without writing output file",
    )
    args = parser.parse_args()

    incoming = import_csv(args.csv, dry_run=args.dry_run)
    if incoming:
        merge_and_save(incoming, output_file=args.output, dry_run=args.dry_run)
    else:
        print("[apollo_csv] No valid companies parsed — check CSV format.")
        sys.exit(1)


if __name__ == "__main__":
    main()
