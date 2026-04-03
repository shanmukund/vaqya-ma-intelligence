"""
Vaqya M&A RCM Target Scoring Model
────────────────────────────────────
Composite score 0–100 across 4 strategic dimensions:

  1. Offshoring Opportunity   (max 30)  — Labor cost arbitrage potential
  2. Automation Opportunity   (max 25)  — Process modernization upside
  3. Deal Attractiveness      (max 25)  — Revenue band, specialty mix, growth
  4. Acquisition Feasibility  (max 20)  — Owner-operated, no PE, geographic fit

Adjust weights in WEIGHTS dict to recalibrate after M&A team feedback.
"""

from __future__ import annotations
from typing import Any
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
from config import (
    HIGH_COST_STATES, MEDIUM_COST_STATES, VAQYA_EXISTING_STATES,
    LEGACY_TECH_SIGNALS, MODERN_TECH_SIGNALS, OFFSHORE_SIGNALS, PE_SIGNALS,
)

# ─── Priority Tier Thresholds ──────────────────────────────────────────────────
TIERS = [
    (65, "A", "Contact within 30 days"),
    (50, "B", "Contact within 90 days"),
    (38, "C", "Watch list — revisit quarterly"),
    (25, "D", "Low priority"),
    (0,  "—", "Archive"),
]

# ─── Revenue Band Scoring ──────────────────────────────────────────────────────
REVENUE_BAND_SCORES: dict[str, int] = {
    "$5M-$15M":   22,  # Core target
    "$15M-$30M":  20,
    "$30M-$50M":  18,
    "$2M-$5M":    16,
    "$50M+":      12,
    "$1M-$2M":    10,
    "Unknown":    10,
    "<$1M":        5,
}


class RCMScorer:
    """Score an RCM company dict and return full score breakdown."""

    def score(self, company: dict[str, Any]) -> dict[str, Any]:
        o = self._offshoring_score(company)
        a = self._automation_score(company)
        d = self._deal_attractiveness(company)
        f = self._feasibility_score(company)
        composite = min(100, o + a + d + f)
        tier, action = self._tier(composite)
        return {
            "composite":              composite,
            "offshoring_opportunity": o,
            "automation_opportunity": a,
            "deal_attractiveness":    d,
            "acquisition_feasibility": f,
            "priority_tier":          tier,
            "recommended_action":     action,
        }

    # ── Dimension 1: Offshoring Opportunity (max 30) ────────────────────────
    def _offshoring_score(self, c: dict) -> int:
        pts = 0
        state = (c.get("state") or "").upper()

        if state in HIGH_COST_STATES:
            pts += 12
        elif state in MEDIUM_COST_STATES:
            pts += 6

        tech_signals  = [s.lower() for s in (c.get("technology_signals") or [])]
        owner_signals = [s.lower() for s in (c.get("owner_signals") or [])]
        website_text  = (c.get("_website_text") or "").lower()

        # No offshore mentions found
        offshore_found = any(sig in website_text for sig in OFFSHORE_SIGNALS) or \
                         c.get("offshore_mentions", False)
        if not offshore_found:
            pts += 8
        else:
            pts -= 8

        # Labor-heavy: employee/revenue ratio above median
        emp = c.get("employee_count_est") or 0
        rev = c.get("estimated_revenue") or 0
        if emp > 0 and rev > 0:
            rev_per_emp = rev / emp
            if rev_per_emp < 120_000:      # Below $120K revenue/employee → labor heavy
                pts += 6

        # Old company → entrenched manual workflows
        age = c.get("company_age") or 0
        if age >= 15:
            pts += 4

        # Job postings for manual billing roles
        if (c.get("job_posting_count") or 0) >= 2:
            pts += 4

        # Single location
        if not c.get("multi_state", False):
            pts += 3

        # PE backing reduces offshoring opportunity (they may already have it)
        if c.get("pe_backed", False):
            pts -= 5

        return max(0, min(30, pts))

    # ── Dimension 2: Automation Opportunity (max 25) ────────────────────────
    def _automation_score(self, c: dict) -> int:
        pts = 0
        tech_signals = [s.lower() for s in (c.get("technology_signals") or [])]
        website_text = (c.get("_website_text") or "").lower()
        job_titles   = [j.lower() for j in (c.get("job_titles_found") or [])]
        specialties  = [s.lower() for s in (c.get("specialties") or [])]
        age          = c.get("company_age") or 0

        # Legacy software detected
        legacy_found = any(sig in " ".join(tech_signals) or sig in website_text
                           for sig in LEGACY_TECH_SIGNALS)
        if legacy_found:
            pts += 8

        # Age-based automation opportunity
        if age >= 20:
            pts += 6
        elif age >= 10:
            pts += 4

        # Manual job postings signal manual workflows
        manual_job_keywords = ["biller", "ar follow", "charge entry", "denial", "coder"]
        manual_jobs_found = any(kw in " ".join(job_titles) for kw in manual_job_keywords)
        if manual_jobs_found:
            pts += 5

        # High-volume specialties = more automation upside
        if "physician" in specialties or "multi-specialty" in specialties:
            pts += 4
        elif any(s in specialties for s in ["multi-specialty", "multi_specialty"]):
            pts += 3

        # No API/integration mentions
        if "api" not in website_text and "integration" not in website_text:
            pts += 3

        # Fax / paper signals
        if "fax" in website_text or "paper eob" in website_text or "paper claims" in website_text:
            pts += 4

        # Modern tech stack — they already have automation tools
        modern_found = any(sig in " ".join(tech_signals) or sig in website_text
                           for sig in MODERN_TECH_SIGNALS)
        if modern_found:
            pts -= 4

        # If they advertise automation on their own site, upside is smaller
        if "automation" in website_text and "our automation" in website_text:
            pts -= 3

        return max(0, min(25, pts))

    # ── Dimension 3: Deal Attractiveness (max 25) ────────────────────────────
    def _deal_attractiveness(self, c: dict) -> int:
        pts = REVENUE_BAND_SCORES.get(c.get("revenue_band") or "Unknown", 10)

        specialties = [s.lower() for s in (c.get("specialties") or [])]

        # Specialty mix bonus
        if "multi-specialty" in specialties or "multi_specialty" in specialties:
            pts += 3
        elif len(specialties) == 1:
            pts -= 2  # Single specialty = concentration risk

        # Multi-state client footprint
        if c.get("multi_state", False):
            pts += 2

        # Growth vs decline signals (from website/job posting data)
        owner_signals = [s.lower() for s in (c.get("owner_signals") or [])]
        website_text  = (c.get("_website_text") or "").lower()

        if "hiring" in website_text or "expanding" in website_text or "new office" in website_text:
            pts += 2
        if "layoff" in website_text or "closing" in website_text or "restructur" in website_text:
            pts -= 3

        return max(0, min(25, pts))

    # ── Dimension 4: Acquisition Feasibility (max 20) ───────────────────────
    def _feasibility_score(self, c: dict) -> int:
        pts = 0
        state         = (c.get("state") or "").upper()
        age           = c.get("company_age") or 0
        owner_signals = [s.lower() for s in (c.get("owner_signals") or [])]
        website_text  = (c.get("_website_text") or "").lower()

        # Owner-operated signals
        if owner_signals:
            pts += 6

        # Founder may want liquidity
        if age >= 15:
            pts += 4

        # No PE backing
        if not c.get("pe_backed", False):
            pts += 4
        else:
            pts -= 8  # PE-backed is a major penalty

        # Geographic integration synergy
        if state in VAQYA_EXISTING_STATES:
            pts += 3

        # Bootstrapped — no recent funding rounds
        if not c.get("recent_funding", False):
            pts += 3
        else:
            pts -= 5

        # Distress signals → motivated seller
        if "negative reviews" in owner_signals or "attrition" in owner_signals:
            pts += 2

        # Enterprise complexity penalty
        if (c.get("estimated_revenue") or 0) > 50_000_000:
            pts -= 3

        return max(0, min(20, pts))

    # ── Tier Lookup ─────────────────────────────────────────────────────────
    @staticmethod
    def _tier(score: int) -> tuple[str, str]:
        for threshold, tier, action in TIERS:
            if score >= threshold:
                return tier, action
        return "—", "Archive"


# ─── Convenience wrapper ───────────────────────────────────────────────────────
_scorer = RCMScorer()

def score_company(company: dict) -> dict:
    """Score a single company dict. Returns scores sub-dict."""
    return _scorer.score(company)


def score_all(companies: list[dict]) -> list[dict]:
    """In-place score all companies. Returns the same list."""
    for c in companies:
        c["scores"] = _scorer.score(c)
    return companies
