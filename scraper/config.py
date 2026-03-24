"""
Central configuration for the Vaqya M&A RCM Target Scraper.
Edit API keys and adjust weights here before running.
"""

import os

# ─── API Keys ──────────────────────────────────────────────────────────────────
# Set as environment variables or replace placeholders here.
GOOGLE_MAPS_API_KEY = os.getenv("GOOGLE_MAPS_API_KEY", "YOUR_GOOGLE_MAPS_API_KEY")
BING_MAPS_API_KEY   = os.getenv("BING_MAPS_API_KEY",   "YOUR_BING_MAPS_API_KEY")
SERPAPI_KEY         = os.getenv("SERPAPI_KEY",          "YOUR_SERPAPI_KEY")

# ─── Output ────────────────────────────────────────────────────────────────────
OUTPUT_FILE = os.path.join(os.path.dirname(__file__), "rcm_targets.json")

# ─── Rate Limits ───────────────────────────────────────────────────────────────
RATE_LIMITS = {
    "google_maps":          {"delay_seconds": 1.0,  "daily_cap": 500},
    "indeed":               {"delay_seconds": 5.0,  "session_cap": 200},
    "secretary_of_state":   {"delay_seconds": 3.0,  "daily_cap": 300},
    "linkedin":             {"delay_seconds": 15.0, "hourly_cap": 50},
    "bing_local":           {"delay_seconds": 2.0,  "daily_cap": 400},
    "hfma_mgma":            {"delay_seconds": 4.0,  "daily_cap": 100},
    "tech_detector":        {"delay_seconds": 2.0,  "daily_cap": 300},
}

# ─── State Classifications ─────────────────────────────────────────────────────
# Used for Offshoring Opportunity scoring
HIGH_COST_STATES   = {"CA", "NY", "MA", "WA", "IL", "NJ", "CT", "MD", "CO", "MN"}
MEDIUM_COST_STATES = {"TX", "FL", "GA", "NC", "VA", "AZ", "OH", "MI", "PA", "TN"}
LOW_COST_STATES    = {"AL", "AR", "ID", "IN", "IA", "KS", "KY", "LA", "MS",
                      "MO", "MT", "NE", "NM", "ND", "OK", "SC", "SD", "UT",
                      "WV", "WI", "WY"}

# States where Vaqya already operates — integration synergy bonus
VAQYA_EXISTING_STATES = {"AZ", "FL", "GA", "TX", "IL", "MO", "KY"}

# ─── Target Metro Areas (47 metros) ───────────────────────────────────────────
TARGET_METROS = [
    # High-cost, high-value markets
    {"metro": "New York",       "state": "NY", "lat": 40.7128, "lng": -74.0060},
    {"metro": "Los Angeles",    "state": "CA", "lat": 34.0522, "lng": -118.2437},
    {"metro": "Chicago",        "state": "IL", "lat": 41.8781, "lng": -87.6298},
    {"metro": "San Francisco",  "state": "CA", "lat": 37.7749, "lng": -122.4194},
    {"metro": "Boston",         "state": "MA", "lat": 42.3601, "lng": -71.0589},
    {"metro": "Seattle",        "state": "WA", "lat": 47.6062, "lng": -122.3321},
    {"metro": "Washington DC",  "state": "MD", "lat": 38.9072, "lng": -77.0369},
    {"metro": "Minneapolis",    "state": "MN", "lat": 44.9778, "lng": -93.2650},
    {"metro": "Denver",         "state": "CO", "lat": 39.7392, "lng": -104.9903},
    {"metro": "Newark",         "state": "NJ", "lat": 40.7357, "lng": -74.1724},
    # Vaqya existing states — integration synergy
    {"metro": "Phoenix",        "state": "AZ", "lat": 33.4484, "lng": -112.0740},
    {"metro": "Scottsdale",     "state": "AZ", "lat": 33.4942, "lng": -111.9261},
    {"metro": "Miami",          "state": "FL", "lat": 25.7617, "lng": -80.1918},
    {"metro": "Tampa",          "state": "FL", "lat": 27.9506, "lng": -82.4572},
    {"metro": "Orlando",        "state": "FL", "lat": 28.5383, "lng": -81.3792},
    {"metro": "Jacksonville",   "state": "FL", "lat": 30.3322, "lng": -81.6557},
    {"metro": "Atlanta",        "state": "GA", "lat": 33.7490, "lng": -84.3880},
    {"metro": "Savannah",       "state": "GA", "lat": 32.0835, "lng": -81.0998},
    {"metro": "Houston",        "state": "TX", "lat": 29.7604, "lng": -95.3698},
    {"metro": "Dallas",         "state": "TX", "lat": 32.7767, "lng": -96.7970},
    {"metro": "Austin",         "state": "TX", "lat": 30.2672, "lng": -97.7431},
    {"metro": "San Antonio",    "state": "TX", "lat": 29.4241, "lng": -98.4936},
    {"metro": "St. Louis",      "state": "MO", "lat": 38.6270, "lng": -90.1994},
    {"metro": "Kansas City",    "state": "MO", "lat": 39.0997, "lng": -94.5786},
    {"metro": "Louisville",     "state": "KY", "lat": 38.2527, "lng": -85.7585},
    {"metro": "Lexington",      "state": "KY", "lat": 38.0406, "lng": -84.5037},
    # High-yield supplemental markets
    {"metro": "Philadelphia",   "state": "PA", "lat": 39.9526, "lng": -75.1652},
    {"metro": "Charlotte",      "state": "NC", "lat": 35.2271, "lng": -80.8431},
    {"metro": "Raleigh",        "state": "NC", "lat": 35.7796, "lng": -78.6382},
    {"metro": "Nashville",      "state": "TN", "lat": 36.1627, "lng": -86.7816},
    {"metro": "Memphis",        "state": "TN", "lat": 35.1495, "lng": -90.0490},
    {"metro": "Columbus",       "state": "OH", "lat": 39.9612, "lng": -82.9988},
    {"metro": "Cleveland",      "state": "OH", "lat": 41.4993, "lng": -81.6944},
    {"metro": "Cincinnati",     "state": "OH", "lat": 39.1031, "lng": -84.5120},
    {"metro": "Detroit",        "state": "MI", "lat": 42.3314, "lng": -83.0458},
    {"metro": "Indianapolis",   "state": "IN", "lat": 39.7684, "lng": -86.1581},
    {"metro": "Milwaukee",      "state": "WI", "lat": 43.0389, "lng": -87.9065},
    {"metro": "Richmond",       "state": "VA", "lat": 37.5407, "lng": -77.4360},
    {"metro": "Virginia Beach", "state": "VA", "lat": 36.8529, "lng": -75.9780},
    {"metro": "Oklahoma City",  "state": "OK", "lat": 35.4676, "lng": -97.5164},
    {"metro": "Tulsa",          "state": "OK", "lat": 36.1540, "lng": -95.9928},
    {"metro": "New Orleans",    "state": "LA", "lat": 29.9511, "lng": -90.0715},
    {"metro": "Baton Rouge",    "state": "LA", "lat": 30.4515, "lng": -91.1871},
    {"metro": "Salt Lake City", "state": "UT", "lat": 40.7608, "lng": -111.8910},
    {"metro": "Albuquerque",    "state": "NM", "lat": 35.0844, "lng": -106.6504},
    {"metro": "Las Vegas",      "state": "NV", "lat": 36.1699, "lng": -115.1398},
    {"metro": "Pittsburgh",     "state": "PA", "lat": 40.4406, "lng": -79.9959},
]

# ─── Search Queries ────────────────────────────────────────────────────────────
GOOGLE_MAPS_QUERIES = [
    "medical billing company",
    "revenue cycle management company",
    "physician billing services",
    "healthcare billing services",
    "medical billing and coding",
    "RCM company",
    "medical accounts receivable",
    "healthcare revenue cycle",
]

INDEED_JOB_TITLES = [
    "medical biller",
    "AR follow-up specialist",
    "denial management specialist",
    "charge entry specialist",
    "revenue cycle specialist",
    "medical billing specialist",
    "insurance billing coordinator",
    "prior authorization specialist",
]

# Exclude staffing agencies from Indeed results
INDEED_COMPANY_EXCLUSIONS = [
    "staffing", "recruiting", "talent", "solutions group", "temp", "contract",
    "workforce", "careers", "hire", "placement", "manpower", "kelly services",
    "robert half", "adecco", "randstad",
]

# ─── Technology Signal Keywords ────────────────────────────────────────────────
# Found on company websites → indicates legacy / manual processes
LEGACY_TECH_SIGNALS = [
    "meditech", "mckesson", "lytec", "nuemd", "greenway", "healtheon",
    "paper claims", "paper eob", "paper remittance", "manual entry",
    "fax claims", "fax submission", "ub04", "cms 1500 paper",
    "no edi", "manual posting", "handwritten", "mail claims",
    "advantx", "medical manager", "medic", "amisys",
]

MODERN_TECH_SIGNALS = [
    "waystar", "availity api", "change healthcare api", "ehr integration",
    "automated eligibility", "robotic process automation", "rpa billing",
    "ai billing", "machine learning claims", "automated denial",
    "real-time eligibility", "clearinghouse api", "sftp automated",
]

# Found on websites → indicates they already have offshore operations
OFFSHORE_SIGNALS = [
    "offshore team", "india operations", "philippines team", "offshore billing",
    "global team", "nearshore", "offshore staff", "india office",
    "offshore coding", "remote team india", "bpo partner",
]

# Found on websites → indicates PE backing (harder/more expensive to acquire)
PE_SIGNALS = [
    "portfolio company", "backed by", "private equity", "investment firm",
    "growth equity", "recapitalization", "pe-backed", "venture backed",
    "our investors", "capital partners", "equity partners",
]

OWNER_OPERATED_SIGNALS = [
    "founded by", "family owned", "independently owned", "owner operated",
    "established by", "our founder", "started by",
]

# ─── Specialty Keywords ────────────────────────────────────────────────────────
SPECIALTY_KEYWORDS = {
    "physician":           ["physician billing", "physician practice", "medical practice billing", "doctor billing"],
    "multi-specialty":     ["multi-specialty", "multispecialty", "multiple specialties", "all specialties"],
    "hospital":            ["hospital billing", "facility billing", "inpatient billing", "outpatient facility"],
    "dental":              ["dental billing", "dental practice", "dentist billing"],
    "behavioral_health":   ["behavioral health billing", "mental health billing", "therapy billing", "psychiatric billing"],
    "chiropractic":        ["chiropractic billing", "chiropractor billing"],
    "ambulatory_surgery":  ["ambulatory surgery", "asc billing", "surgery center billing"],
    "radiology":           ["radiology billing", "imaging billing", "diagnostic billing"],
    "emergency_medicine":  ["emergency medicine billing", "er billing", "emergency department billing"],
    "anesthesia":          ["anesthesia billing", "anesthesiology billing"],
}

# ─── Secretary of State Configs ────────────────────────────────────────────────
SOS_CONFIGS = {
    "FL": {
        "url": "https://search.sunbiz.org/Inquiry/CorporationSearch/SearchResults",
        "naics_codes": ["621111", "621112", "524114", "524291"],
        "method": "html_form",
    },
    "TX": {
        "url": "https://mycpa.cpa.state.tx.us/coa/coaSales.do",
        "naics_codes": ["621111", "621112", "524114"],
        "method": "html_form",
    },
    "GA": {
        "url": "https://opencorporates.com/companies/us_ga",
        "naics_codes": ["621111", "524114"],
        "method": "opencorporates_api",
    },
}

OPENCORPORATES_BASE = "https://api.opencorporates.com/v0.4/companies/search"

# ─── HFMA/MGMA Chapter URLs ────────────────────────────────────────────────────
HFMA_CHAPTER_BASE = "https://www.hfma.org/chapters/"
MGMA_STATE_CHAPTERS = [
    "https://www.mgma.com/membership/state-affiliates",
]
