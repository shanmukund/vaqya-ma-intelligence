"""
Cross-source deduplication and merge for RCM target companies.

Merge priority (highest confidence wins):
  1. Exact UUID match (same scraper run)
  2. Normalized domain match
  3. Normalized phone match
  4. Fuzzy company name match (Levenshtein distance < 0.15)

Pipeline fields (stage, notes, contacts, assigned_to) are ALWAYS preserved
from the existing record and never overwritten by incoming scraper data.
"""

from __future__ import annotations
import re
import uuid
from difflib import SequenceMatcher
from typing import Any
import tldextract


# ─── Pipeline fields that must never be overwritten on merge ──────────────────
PIPELINE_FIELDS = {
    "pipeline_stage", "assigned_to", "priority",
    "notes", "next_action", "next_action_due", "contacts",
    "date_added",
}

# Suffixes to strip when normalizing company names
COMPANY_SUFFIX_RE = re.compile(
    r"\b(llc|inc|corp|ltd|co|plc|pllc|pa|pc|lp|llp|dba|the)\b\.?$",
    re.IGNORECASE,
)

PHONE_DIGITS_RE = re.compile(r"\D")


def normalize_domain(url_or_domain: str) -> str:
    """Extract and normalize the registrable domain (e.g. 'medbill.com')."""
    if not url_or_domain:
        return ""
    extracted = tldextract.extract(url_or_domain)
    if extracted.domain and extracted.suffix:
        return f"{extracted.domain}.{extracted.suffix}".lower()
    # Fallback: strip scheme/www and take first segment
    cleaned = re.sub(r"^https?://", "", url_or_domain)
    cleaned = re.sub(r"^www\.", "", cleaned)
    return cleaned.split("/")[0].lower()


def normalize_phone(phone: str) -> str:
    """Return last 10 digits of a phone number (US)."""
    if not phone:
        return ""
    digits = PHONE_DIGITS_RE.sub("", phone)
    return digits[-10:] if len(digits) >= 10 else digits


def normalize_name(name: str) -> str:
    """Lowercase, strip legal suffixes, collapse whitespace."""
    if not name:
        return ""
    name = name.lower().strip()
    name = COMPANY_SUFFIX_RE.sub("", name).strip(", .")
    name = re.sub(r"\s+", " ", name)
    return name


def _name_similarity(a: str, b: str) -> float:
    """Return 0–1 similarity between two normalized company names."""
    return SequenceMatcher(None, normalize_name(a), normalize_name(b)).ratio()


def _merge_records(existing: dict, incoming: dict) -> dict:
    """
    Merge `incoming` into `existing`.
    Pipeline fields from `existing` are never overwritten.
    """
    merged = dict(existing)
    for key, value in incoming.items():
        if key in PIPELINE_FIELDS:
            continue  # Always keep existing pipeline state
        if value is not None and value != "" and value != [] and value != {}:
            # For list fields, union them (avoid duplicates)
            if isinstance(value, list) and isinstance(merged.get(key), list):
                existing_set = set(str(v) for v in merged[key])
                for item in value:
                    if str(item) not in existing_set:
                        merged[key].append(item)
                        existing_set.add(str(item))
            else:
                merged[key] = value
    merged["date_updated"] = incoming.get("date_updated") or existing.get("date_updated")
    return merged


def deduplicate(
    incoming: list[dict[str, Any]],
    existing: list[dict[str, Any]] | None = None,
) -> list[dict[str, Any]]:
    """
    Deduplicate incoming records against each other and against existing records.

    existing  — records already in rcm_targets.json (pipeline state preserved)
    incoming  — fresh records from all scrapers this run

    Returns the merged, deduplicated list.

    Performance note: fuzzy name matching is O(n²) and is SKIPPED when the
    batch is large (>500 records). Domain + phone O(1) dict lookups handle
    the vast majority of duplicates. Fuzzy is only valuable for the small
    number of Clutch/LinkedIn records that lack both domain and phone.
    """
    existing = existing or []

    # Only use fuzzy matching for small batches — avoids O(n²) with NPPES 5K+ records
    USE_FUZZY = len(incoming) <= 500

    if not USE_FUZZY:
        print(f"[dedup] Large batch ({len(incoming)} records) — "
              f"skipping fuzzy name match (domain+phone dedup only). Fast O(n) mode.")

    # Build lookup indexes on existing records
    existing_by_id:     dict[str, dict] = {}
    existing_by_domain: dict[str, dict] = {}
    existing_by_phone:  dict[str, dict] = {}
    # O(1) name lookup for existing (normalized)
    existing_by_name:   dict[str, dict] = {}

    for rec in existing:
        if rec.get("id"):
            existing_by_id[rec["id"]] = rec
        domain = normalize_domain(rec.get("website") or "")
        if domain:
            existing_by_domain[domain] = rec
        phone = normalize_phone(rec.get("phone") or "")
        if phone:
            existing_by_phone[phone] = rec
        name = normalize_name(rec.get("company_name") or "")
        if name:
            existing_by_name[name] = rec

    # Deduplicate incoming records among themselves first
    deduped_incoming: list[dict] = []
    seen_domains: dict[str, int] = {}
    seen_phones:  dict[str, int] = {}
    seen_names:   dict[str, int] = {}  # exact normalized name → index

    for rec in incoming:
        domain = normalize_domain(rec.get("website") or "")
        phone  = normalize_phone(rec.get("phone") or "")
        name   = normalize_name(rec.get("company_name") or "")

        # Check domain duplicate within this batch
        if domain and domain in seen_domains:
            idx = seen_domains[domain]
            deduped_incoming[idx] = _merge_records(deduped_incoming[idx], rec)
            continue

        # Check phone duplicate within this batch
        if phone and phone in seen_phones:
            idx = seen_phones[phone]
            deduped_incoming[idx] = _merge_records(deduped_incoming[idx], rec)
            continue

        # Exact normalized name match within this batch (O(1))
        if name and name in seen_names:
            idx = seen_names[name]
            deduped_incoming[idx] = _merge_records(deduped_incoming[idx], rec)
            continue

        # Fuzzy name match within this batch (O(n) — only for small batches)
        matched_idx = None
        if USE_FUZZY and name:
            for i, existing_rec in enumerate(deduped_incoming):
                sim = _name_similarity(name, existing_rec.get("company_name", ""))
                if sim >= 0.85:
                    matched_idx = i
                    break
        if matched_idx is not None:
            deduped_incoming[matched_idx] = _merge_records(deduped_incoming[matched_idx], rec)
            continue

        # New unique record
        idx = len(deduped_incoming)
        if not rec.get("id"):
            rec["id"] = str(uuid.uuid4())
        deduped_incoming.append(rec)
        if domain:
            seen_domains[domain] = idx
        if phone:
            seen_phones[phone] = idx
        if name:
            seen_names[name] = idx

    # Now merge deduped incoming with existing records
    # Use result_index for O(1) position lookup instead of result.index() which is O(n)
    result: list[dict] = list(existing)
    result_index: dict[str, int] = {rec.get("id", ""): i for i, rec in enumerate(result)}

    new_count     = 0
    updated_count = 0

    for inc in deduped_incoming:
        domain = normalize_domain(inc.get("website") or "")
        phone  = normalize_phone(inc.get("phone") or "")
        name   = normalize_name(inc.get("company_name") or "")

        matched_idx = None

        # Match by ID (O(1))
        if inc.get("id") and inc["id"] in existing_by_id:
            ex = existing_by_id[inc["id"]]
            matched_idx = result_index.get(ex.get("id", ""))

        # Match by domain (O(1))
        elif domain and domain in existing_by_domain:
            ex = existing_by_domain[domain]
            matched_idx = result_index.get(ex.get("id", ""))

        # Match by phone (O(1))
        elif phone and phone in existing_by_phone:
            ex = existing_by_phone[phone]
            matched_idx = result_index.get(ex.get("id", ""))

        # Exact normalized name match against existing (O(1))
        elif name and name in existing_by_name:
            ex = existing_by_name[name]
            matched_idx = result_index.get(ex.get("id", ""))

        # Fuzzy name match against existing (O(n) — small batches only)
        elif USE_FUZZY and name:
            for ex in existing:
                sim = _name_similarity(name, ex.get("company_name", ""))
                if sim >= 0.85:
                    matched_idx = result_index.get(ex.get("id", ""))
                    break

        if matched_idx is not None:
            result[matched_idx] = _merge_records(result[matched_idx], inc)
            updated_count += 1
        else:
            # Genuinely new — add to result and update index
            new_idx = len(result)
            result.append(inc)
            result_index[inc.get("id", "")] = new_idx
            new_count += 1

    print(f"[dedup] {new_count} new | {updated_count} updated | "
          f"{len(existing)} existing preserved | {len(result)} total")
    return result
