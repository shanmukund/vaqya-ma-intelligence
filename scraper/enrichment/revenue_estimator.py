"""
Revenue estimation for private RCM companies.

Since revenue is almost never publicly disclosed for private billing companies,
we estimate from:
  1. Employee count (primary anchor)
  2. Specialty multiplier (hospital billing generates more revenue per employee)
  3. Geography discount (high-cost states need fewer staff per $ of revenue)

Output is a revenue_band string and estimated_revenue integer.
All estimates are marked with low data_confidence unless confirmed by a
public source.
"""

from __future__ import annotations
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
from config import HIGH_COST_STATES, MEDIUM_COST_STATES

# ─── Employee-count-to-revenue anchors ────────────────────────────────────────
# (min_emp, max_emp, rev_low, rev_high)  — all in USD
EMP_REV_TABLE = [
    (1,    5,    150_000,     800_000),
    (6,    10,   600_000,   1_500_000),
    (11,   25, 1_000_000,   5_000_000),
    (26,   50, 3_000_000,  10_000_000),
    (51,  100, 7_000_000,  20_000_000),
    (101, 200, 15_000_000, 40_000_000),
    (201, 500, 30_000_000, 100_000_000),
    (501, 99999, 80_000_000, 300_000_000),
]

# ─── Specialty revenue multipliers ────────────────────────────────────────────
SPECIALTY_MULTIPLIERS: dict[str, float] = {
    "hospital":          1.4,
    "ambulatory_surgery":1.3,
    "anesthesia":        1.2,
    "radiology":         1.1,
    "multi-specialty":   1.2,
    "multi_specialty":   1.2,
    "physician":         1.0,
    "emergency_medicine":1.0,
    "behavioral_health": 0.75,
    "chiropractic":      0.70,
    "dental":            0.65,
}

# ─── Geography adjustment ─────────────────────────────────────────────────────
# High-cost states: billing companies need fewer employees per $ (higher salaries)
# → estimate revenue higher for the same headcount
GEO_MULTIPLIER: dict[str, float] = {
    "high":   1.15,
    "medium": 1.0,
    "low":    0.90,
}

# ─── Revenue bands ────────────────────────────────────────────────────────────
REVENUE_BANDS = [
    (50_000_001, float("inf"), "$50M+"),
    (30_000_001, 50_000_000,  "$30M-$50M"),
    (15_000_001, 30_000_000,  "$15M-$30M"),
    (5_000_001,  15_000_000,  "$5M-$15M"),
    (2_000_001,  5_000_000,   "$2M-$5M"),
    (1_000_001,  2_000_000,   "$1M-$2M"),
    (0,          1_000_000,   "<$1M"),
]


def _emp_to_revenue_range(emp: int) -> tuple[int, int] | None:
    for min_e, max_e, rev_lo, rev_hi in EMP_REV_TABLE:
        if min_e <= emp <= max_e:
            return rev_lo, rev_hi
    return None


def _revenue_to_band(rev: int) -> str:
    for lo, hi, label in REVENUE_BANDS:
        if lo <= rev <= hi:
            return label
    return "Unknown"


def estimate_revenue(company: dict) -> dict:
    """
    Returns updated company dict with estimated_revenue and revenue_band set.
    Does NOT overwrite if already set with high confidence.
    """
    # If already confirmed from a reliable source, don't overwrite
    if company.get("estimated_revenue") and company.get("data_confidence") == "high":
        if not company.get("revenue_band"):
            company["revenue_band"] = _revenue_to_band(company["estimated_revenue"])
        return company

    emp_est = company.get("employee_count_est") or 0

    # Parse employee_count_range if employee_count_est not set
    if not emp_est and company.get("employee_count_range"):
        emp_est = _parse_emp_range(company["employee_count_range"])
        company["employee_count_est"] = emp_est

    # Proxy: use job posting count as a rough employee size signal
    # Companies posting billing jobs at scale are running meaningful operations.
    # This gives revenue estimates to NPPES/Indeed records with no employee data.
    if not emp_est and company.get("job_posting_count"):
        job_count = int(company.get("job_posting_count") or 0)
        if job_count >= 16:
            emp_est = 150   # 200+ staff — large regional shop
            range_str = "200+"
        elif job_count >= 6:
            emp_est = 75    # 51–200 staff — mid-size
            range_str = "51-200"
        elif job_count >= 3:
            emp_est = 25    # 11–50 staff — small-mid
            range_str = "11-50"
        else:
            emp_est = 5     # 1–10 staff — micro
            range_str = "1-10"
        # Only set if we didn't already have a range from another source
        if not company.get("employee_count_range"):
            company["employee_count_range"] = range_str
        company["employee_count_est"] = emp_est
        # Mark as low confidence — this is an indirect signal
        company["data_confidence"] = "low"

    if not emp_est:
        company.setdefault("revenue_band", "Unknown")
        company.setdefault("estimated_revenue", None)
        return company

    rev_range = _emp_to_revenue_range(emp_est)
    if not rev_range:
        company.setdefault("revenue_band", "Unknown")
        return company

    rev_lo, rev_hi = rev_range
    rev_mid = (rev_lo + rev_hi) // 2

    # Apply specialty multiplier (use highest-value specialty found)
    specialties = [s.lower() for s in (company.get("specialties") or [])]
    multiplier = 1.0
    for spec in specialties:
        multiplier = max(multiplier, SPECIALTY_MULTIPLIERS.get(spec, 1.0))

    # Apply geography multiplier
    state = (company.get("state") or "").upper()
    if state in HIGH_COST_STATES:
        geo_mult = GEO_MULTIPLIER["high"]
    elif state in MEDIUM_COST_STATES:
        geo_mult = GEO_MULTIPLIER["medium"]
    else:
        geo_mult = GEO_MULTIPLIER["low"]

    estimated = int(rev_mid * multiplier * geo_mult)
    company["estimated_revenue"] = estimated
    company["revenue_band"] = _revenue_to_band(estimated)
    # Mark confidence as low since this is an estimate
    if company.get("data_confidence") == "high":
        company["data_confidence"] = "medium"
    else:
        company.setdefault("data_confidence", "low")

    return company


def _parse_emp_range(emp_range: str) -> int:
    """Parse '11-50 employees' or '51-200' → midpoint integer."""
    if not emp_range:
        return 0
    # Patterns: "11-50", "11-50 employees", "1-10", "501+"
    import re
    match = re.search(r"(\d+)\s*[-–]\s*(\d+)", emp_range)
    if match:
        lo, hi = int(match.group(1)), int(match.group(2))
        return (lo + hi) // 2
    match = re.search(r"(\d+)\+", emp_range)
    if match:
        return int(match.group(1))
    match = re.search(r"(\d+)", emp_range)
    if match:
        return int(match.group(1))
    return 0
