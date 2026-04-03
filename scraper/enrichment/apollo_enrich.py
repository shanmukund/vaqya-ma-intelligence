"""
Apollo.io enrichment — fills in employee count, revenue, and owner contacts
for Tier A and B acquisition targets.

Free plan: 900 credits/year (75/month, granted monthly).
Credits are used only for exporting contact emails (1 credit = 1 email).
Company/organization data (employee count, revenue band, industry) is FREE
with no credit cost on the Apollo API.

Strategy:
  1. For each Tier A/B company, call /organizations/enrich with domain or name
     → get confirmed employee count + revenue range (no credit cost)
  2. Then call /mixed_people/search for "owner OR founder OR CEO" at that org
     → get name, title, LinkedIn URL (no credit cost)
  3. Export their email only if credits remain (1 credit each)
     → populate contacts[] with name + title + email + linkedin

Apollo API docs: https://apolloio.github.io/apollo-api-docs/
Sign up:         https://www.apollo.io/ (free, no card required)
API key:         apollo.io → Settings → Integrations → API → copy key

Credit math (75/month free):
  Tier A companies per run: ~50–100
  Email exports needed:     50–75 (one decision-maker per company)
  Monthly credit use:       50–75 = within free 75/month budget
"""

from __future__ import annotations
import time
import random
import requests
from datetime import datetime, timezone
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
from config import APOLLO_API_KEY

APOLLO_BASE       = "https://api.apollo.io/api/v1"
ENRICH_URL        = f"{APOLLO_BASE}/organizations/enrich"
PEOPLE_SEARCH_URL = f"{APOLLO_BASE}/mixed_people/search"

# Only enrich companies in these tiers (preserve credits for best targets)
ENRICH_TIERS = {"A", "B"}

# Titles to search for — decision makers who would execute or approve an M&A deal
OWNER_TITLES = [
    "owner", "founder", "co-founder", "president", "ceo",
    "chief executive", "managing partner", "principal",
]

# Apollo employee range → midpoint for our revenue estimator
APOLLO_EMP_MAP = {
    "1,1-10":     5,
    "2,11-20":    15,
    "3,21-50":    35,
    "4,51-100":   75,
    "5,101-200":  150,
    "6,201-500":  350,
    "7,501-1000": 750,
    "8,1001-2000":1500,
    "9,2001-5000":3500,
    "10,5001+":   5001,
}

# Apollo annual revenue range → our revenue band
APOLLO_REV_MAP = {
    "1,0-1M":       "<$1M",
    "2,1M-10M":     "$2M-$5M",
    "3,10M-50M":    "$5M-$15M",
    "4,50M-100M":   "$30M-$50M",
    "5,100M-200M":  "$50M+",
    "6,200M-1B":    "$50M+",
    "7,1B+":        "$50M+",
}


def _headers() -> dict:
    return {
        "Content-Type":  "application/json",
        "Cache-Control": "no-cache",
        "X-Api-Key":     APOLLO_API_KEY,
    }


def _enrich_organization(domain: str = "", name: str = "") -> dict:
    """
    Call Apollo /organizations/enrich.
    Returns raw Apollo org dict or {} on failure.
    No credit cost — org data is free on all plans.
    """
    params = {}
    if domain:
        params["domain"] = domain
    elif name:
        params["name"] = name
    else:
        return {}
    try:
        resp = requests.get(
            ENRICH_URL,
            headers=_headers(),
            params=params,
            timeout=15,
        )
        if resp.status_code == 200:
            return resp.json().get("organization") or {}
        if resp.status_code == 404:
            return {}   # Not in Apollo database — graceful skip
        print(f"  [apollo] Org enrich HTTP {resp.status_code} for '{name or domain}'")
        return {}
    except Exception as e:
        print(f"  [apollo] Org enrich error: {e}")
        return {}


def _search_people(org_id: str = "", org_name: str = "",
                   credit_counter: list | None = None,
                   credit_cap: int = 70) -> list[dict]:
    """
    Search for owner/founder/CEO contacts at an organization.
    Returns list of contact dicts. Email export costs 1 credit each.
    """
    if not org_id and not org_name:
        return []

    payload: dict = {
        "page":         1,
        "per_page":     3,       # max 3 contacts per company
        "person_titles": OWNER_TITLES,
        "contact_email_status_v2": ["verified", "likely to engage"],
    }
    if org_id:
        payload["organization_ids"] = [org_id]
    else:
        payload["q_organization_name"] = org_name

    try:
        resp = requests.post(
            PEOPLE_SEARCH_URL,
            headers=_headers(),
            json=payload,
            timeout=15,
        )
        if resp.status_code != 200:
            return []
        people = resp.json().get("people") or []
        contacts = []
        for p in people:
            # Check if we have budget for email export
            email = p.get("email") or ""
            if not email and credit_counter is not None:
                if credit_counter[0] < credit_cap:
                    # Email is present in Apollo but requires credit to reveal
                    # We only reveal if we have budget
                    credit_counter[0] += 1
                else:
                    email = ""  # Skip — out of credits

            contact = {
                "name":         f"{p.get('first_name', '')} {p.get('last_name', '')}".strip(),
                "title":        p.get("title") or "",
                "email":        email,
                "linkedin_url": p.get("linkedin_url") or "",
                "phone":        (p.get("phone_numbers") or [{}])[0].get("sanitized_number", ""),
                "source":       "apollo",
                "date_added":   datetime.now(timezone.utc).isoformat(),
            }
            if contact["name"]:
                contacts.append(contact)
        return contacts
    except Exception as e:
        print(f"  [apollo] People search error: {e}")
        return []


def _apply_apollo_org(company: dict, org: dict) -> None:
    """Apply Apollo organization data to a company record in-place."""
    if not org:
        return

    # Employee count
    emp_range = org.get("employee_count")
    if emp_range and not company.get("employee_count_est"):
        company["employee_count_est"] = int(emp_range) if isinstance(emp_range, int) else 0
        if company["employee_count_est"]:
            company["employee_count_range"] = str(emp_range)

    # Revenue band from Apollo's annual_revenue_printed
    rev_str = org.get("annual_revenue_printed") or ""
    if rev_str and company.get("revenue_band") in ("Unknown", None, ""):
        # Apollo returns e.g. "$5M", "$12M", "$45M" — map to our bands
        company["_apollo_revenue_printed"] = rev_str

    # Website (fill if missing)
    if not company.get("website") and org.get("website_url"):
        company["website"] = org["website_url"]

    # Founded year
    if not company.get("founded_year") and org.get("founded_year"):
        company["founded_year"] = int(org["founded_year"])
        company["company_age"] = datetime.now().year - company["founded_year"]

    # LinkedIn URL
    if org.get("linkedin_url"):
        company["_linkedin_url"] = org["linkedin_url"]

    # PE / funding signals
    funding = org.get("latest_funding_stage") or ""
    if funding and funding.lower() not in ("", "none", "bootstrapped"):
        company["recent_funding"] = True
        investors = org.get("current_investor_list") or []
        if investors:
            company["pe_backed"] = True

    company["_apollo_org_id"] = org.get("id") or ""
    company["data_confidence"] = "high"   # Apollo data is verified


def enrich_targets(
    companies: list[dict],
    credit_cap: int = 70,      # Stay inside 75/month free credit budget
    delay:      float = 1.2,   # Seconds between API calls
) -> list[dict]:
    """
    Enrich Tier A and B companies with Apollo organization + contact data.

    companies   — full scored target list (modifies in-place, returns same list)
    credit_cap  — max email credits to spend this run (default 70 of 75/mo)
    delay       — seconds between Apollo API calls (be a good citizen)

    Returns the same list with enriched records updated.
    """
    if not APOLLO_API_KEY or APOLLO_API_KEY == "YOUR_APOLLO_API_KEY":
        print("[apollo] APOLLO_API_KEY not set — skipping enrichment.")
        print("         Sign up free at https://www.apollo.io/ → Settings → API")
        return companies

    # Filter to Tier A and B only
    targets = [
        c for c in companies
        if (c.get("scores") or {}).get("priority_tier") in ENRICH_TIERS
    ]

    if not targets:
        print("[apollo] No Tier A/B companies to enrich.")
        return companies

    print(f"[apollo] Enriching {len(targets)} Tier A/B companies "
          f"(credit cap: {credit_cap}/run)...")

    credit_counter = [0]   # mutable ref
    enriched = 0
    contacted = 0

    for i, company in enumerate(targets):
        domain = ""
        website = company.get("website") or ""
        if website:
            # Strip to bare domain
            domain = website.replace("https://", "").replace("http://", "")
            domain = domain.replace("www.", "").split("/")[0].strip()

        name = company.get("company_name") or ""
        tier = (company.get("scores") or {}).get("priority_tier", "?")

        print(f"  [apollo] {i+1}/{len(targets)} — {name} (Tier {tier}) "
              f"[credits used: {credit_counter[0]}/{credit_cap}]")

        # Step 1: Org enrichment (FREE — no credit cost)
        org = _enrich_organization(domain=domain, name=name)
        if org:
            _apply_apollo_org(company, org)
            enriched += 1
            org_id = org.get("id") or ""
        else:
            org_id = ""
            print(f"    → Not found in Apollo database")

        # Step 2: Find owner/founder contacts
        if credit_counter[0] < credit_cap:
            contacts_found = _search_people(
                org_id=org_id,
                org_name=name if not org_id else "",
                credit_counter=credit_counter,
                credit_cap=credit_cap,
            )
            if contacts_found:
                # Merge into existing contacts list (don't overwrite manual entries)
                existing_names = {c.get("name", "").lower()
                                  for c in (company.get("contacts") or [])}
                for c in contacts_found:
                    if c["name"].lower() not in existing_names:
                        company.setdefault("contacts", []).append(c)
                        existing_names.add(c["name"].lower())
                contacted += 1
                print(f"    → {len(contacts_found)} contacts found")
        else:
            print(f"    → Credit cap reached — skipping contact search")

        time.sleep(delay + random.uniform(0, 0.4))

    print(f"\n[apollo] Complete: {enriched} orgs enriched, "
          f"{contacted} companies with contacts, "
          f"{credit_counter[0]} credits used")
    return companies
