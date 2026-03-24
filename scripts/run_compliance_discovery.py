"""
NYC Compliance Discovery Pipeline v2 — Production-grade.

Architecture:
- 62 targeted queries across 7 role-family clusters
- Soft penalty system (only truly impossible roles hard-rejected)
- 10-component scoring model calibrated to Robin's profile
- 5-tier bucketing: Strong Target / Strong Bridge / Stretch / Maybe / Low Value
- Full audit trail with per-query yield, per-reason reject counts
- Confidence-tiered suppression (exact URL > fingerprint > fuzzy title)

Robin's profile (scoring calibration):
  Series 7 active, 8 months BD compliance at GWN Securities,
  AA in Business (expected May 2026), Excel/FINRA filing systems,
  no CPA/JD/CAMS/CFA. Target: NYC finance compliance/KYC/AML/onboarding/BD ops.
"""
from __future__ import annotations

import csv
import json
import random
import re
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

# ═══════════════════════════════════════════════════════════════════════════════
# CONFIG
# ═══════════════════════════════════════════════════════════════════════════════

INDEED_DOMAIN = "www.indeed.com"
INDEED_LOCATION = "New York, NY"
APPLIED_CSV = Path.home() / "Desktop" / "job apps" / "applied_jobs_cleaned.csv"
REPORT_OUT = ROOT / "docs" / "report.html"
AUDIT_OUT = ROOT / "data" / "audit_trail.json"

# ═══════════════════════════════════════════════════════════════════════════════
# QUERY CLUSTERS — 62 queries across 7 role families
# ═══════════════════════════════════════════════════════════════════════════════

QUERY_CLUSTERS = {
    "A_core_compliance": [
        "compliance analyst",
        "compliance associate",
        "compliance specialist bank",
        "compliance coordinator financial",
        "trade surveillance analyst",
        "surveillance analyst financial",
        "communications surveillance",
        "broker dealer compliance",
        "capital markets compliance",
        "regulatory compliance analyst",
        "compliance monitoring",
        "compliance testing",
        "transaction monitoring analyst",
    ],
    "B_risk_controls": [
        "risk analyst bank",
        "risk associate financial services",
        "first line risk",
        "controls analyst financial",
        "controls advisory",
        "operational risk analyst",
        "governance analyst bank",
        "regulatory reporting analyst",
        "internal controls analyst",
    ],
    "C_kyc_aml_fcc": [
        "KYC analyst",
        "KYC associate",
        "AML analyst",
        "AML associate",
        "financial crime analyst",
        "due diligence analyst",
        "sanctions analyst",
        "BSA analyst",
        "CDD analyst financial",
        "client onboarding analyst financial",
        "onboarding analyst bank",
        "enhanced due diligence analyst",
        "anti money laundering analyst",
    ],
    "D_buy_side": [
        "investment compliance analyst",
        "asset management compliance",
        "fund compliance analyst",
        "compliance operations financial",
        "investment adviser compliance",
    ],
    "E_operations": [
        "securities operations analyst",
        "middle office analyst",
        "trade support analyst",
        "operations analyst investment bank",
        "operations associate bank financial",
        "fund operations analyst",
        "fund accounting analyst",
        "client service associate investment",
        "account opening analyst financial",
        "licensing analyst FINRA",
        "registration analyst broker dealer",
        "regulatory operations analyst",
        "clearing operations analyst",
        "settlement operations analyst",
        "custody operations analyst",
    ],
    "F_analytics_adjacent": [
        "compliance analytics analyst",
        "risk analytics financial",
        "compliance data analyst",
    ],
    "G_broad_sweep": [
        "FINRA compliance",
        "financial crimes analyst",
        "trade compliance analyst",
        "market surveillance",
    ],
}

ALL_QUERIES = []
for cluster_queries in QUERY_CLUSTERS.values():
    ALL_QUERIES.extend(cluster_queries)

BROWSER_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": (
        "text/html,application/xhtml+xml,application/xml;q=0.9,"
        "image/avif,image/webp,*/*;q=0.8"
    ),
    "Accept-Language": "en-US,en;q=0.9",
    "Cache-Control": "no-cache",
    "Pragma": "no-cache",
    "Referer": "https://www.google.com/",
    "Upgrade-Insecure-Requests": "1",
}

# ═══════════════════════════════════════════════════════════════════════════════
# HARD REJECT PATTERNS — only truly impossible jobs
# ═══════════════════════════════════════════════════════════════════════════════

# These are HARD REJECTS — completely removed from pipeline
HARD_TITLE_RE = re.compile(
    r"\b(?:"
    # Seniority
    r"vice\s+president|(?<!\w)vp\b|avp\b|svp\b"
    r"|director(?!\s+of\s+(?:compliance|risk))"
    r"|head\s+of|chief|principal|managing\s+director"
    # Engineering / IT / Design (any "engineer" unless preceded by compliance/risk)
    r"|(?<!compliance\s)(?<!risk\s)(?<!regulatory\s)engineer(?:ing)?\b"
    r"|developer|architect(?!\s+(?:compliance|risk))|designer"
    r"|workday\s+(?:architect|consultant|engineer)"
    # Legal (not compliance)
    r"|(?<!\w)counsel\b|general\s+counsel|attorney|paralegal|lawyer"
    # Research / Science
    r"|(?:research\s+)?scientist|researcher"
    # Sales / Client-facing banking
    r"|financial\s+advisor|insurance\s+agent|sales\s+rep(?:resentative)?"
    r"|relationship\s+banker|private\s+(?:wealth|banking)\s+(?:associate|advisor)"
    r"|universal\s+banker"
    # Investment banking (IB roles, not ops)
    r"|investment\s+banking\s+(?:analyst|associate)"
    # Healthcare / Clinical
    r"|dental|nurse|physician|pharmacist|clinician"
    r"|(?:physical|respiratory|occupational)\s+therapist"
    r"|medical\s+(?:assistant|technician)|infection\s+control"
    r"|(?:speech|slp)\s+(?:language\s+)?(?:pathologist|therapist)"
    # Irrelevant
    r"|truck\s+driver|cdl\s+driver|bus\s+driver|warehouse|security\s+guard"
    r"|teacher|early\s+childhood|scheduler|secretary|receptionist"
    r"|food\s+safety"
    # Robin's current function he wants to leave
    r"|branch\s+examiner"
    # Programs / temp
    r"|summer\s+analyst|intern\b|internship"
    r"|part[-\s]?time"
    # Accounting / Controller (not compliance)
    r"|(?:financial\s+)?controller(?:ship)?"
    r"|(?:financial\s+)?accounting\s+(?:advisory|analyst|associate|manager)"
    # Claims / Insurance
    r"|claims\s+(?:analyst|adjuster|examiner|specialist)"
    # Product roles
    r"|product\s+(?:manager|designer|owner|associate)"
    # EEO / workplace compliance (not financial compliance)
    r"|eeo\s+compliance|equal\s+employment"
    # Speech / therapy / clinical
    r"|(?:virtual\s+)?slp\b|speech\s+(?:language\s+)?patholog"
    # Operations Manager at non-financial (too broad, handled by unclassified cap)
    r"|(?:hotel|hospitality)\s+(?:manager|operations)"
    r")\b",
    re.I,
)

HARD_INDUSTRY_RE = re.compile(
    r"\b(?:"
    r"healthcare|hospital|medical\s+center|nursing(?:\s+home)?|dental\s+(?:office|practice)"
    r"|food\s+safety|grocery|restaurant(?:\s+group)?"
    r"|school\s+district|k-12"
    r"|OPWDD|social\s+services|child\s+welfare"
    r"|sports\s+betting|igaming|online\s+casino|sportsbook"
    r"|pharmacy|construction\s+(?:company|contracting)"
    r"|nursery|nurseries|landscaping"
    r"|hotel(?!\s+(?:compliance|risk))|hospitality|resort"
    r"|health\s+plan|health\s+insurance|managed\s+care"
    r")\b",
    re.I,
)

# Government — OK if SEC/FINRA/OCC/FDIC/Federal Reserve
GOV_RE = re.compile(r"\b(?:state\s+of|city\s+of|department\s+of|public\s+health|government)\b", re.I)
FIN_GOV_RE = re.compile(r"\b(?:SEC|FINRA|OCC|FDIC|Federal\s+Reserve|Treasury\s+Department)\b", re.I)

CONTRACT_RE = re.compile(r"\b(?:seasonal|temporary|temp\b|freelance)\b", re.I)

NON_NYC_RE = re.compile(
    r"\b(?:white\s+plains|stamford|westchester|coral\s+gables"
    r"|parsippany|morristown|greenwich|darien|norwalk"
    r"|harrison|purchase|armonk|tarrytown|yonkers|jericho|melville"
    r"|princeton|florham\s+park|short\s+hills|woodbridge|iselin)\b",
    re.I,
)

NYC_RE = re.compile(
    r"\b(?:new\s+york|nyc|manhattan|brooklyn|queens|bronx|staten\s+island"
    r"|midtown|downtown|financial\s+district|wall\s+street|tribeca"
    r"|long\s+island\s+city|flatiron|soho|fidi)\b",
    re.I,
)

# Jersey City — allowed with soft penalty (many real finance jobs there)
JC_RE = re.compile(r"\bjersey\s+city\b", re.I)

BLOCKED_RE = re.compile(
    r"Authenticating\.\.\.|bot-detection-anonymous|Additional Verification Required",
    re.I,
)
JOB_CARD_RE = re.compile(
    r'<div class="job_seen_beacon">(.*?)(?=<div class="job_seen_beacon"|$)', re.S
)

# ═══════════════════════════════════════════════════════════════════════════════
# FIRM TIERS
# ═══════════════════════════════════════════════════════════════════════════════

TIER_1_FIRMS = {
    "goldman sachs", "morgan stanley", "jpmorgan", "jp morgan", "jpmorganchase",
    "citi", "citigroup", "citibank", "bank of america", "bofa",
    "barclays", "ubs", "deutsche bank", "hsbc", "bnp paribas",
    "blackstone", "kkr", "apollo", "carlyle", "citadel", "two sigma",
    "jane street", "man group", "bridgewater", "millennium", "point72",
    "d.e. shaw", "de shaw", "balyasny", "schonfeld",
    "blackrock", "vanguard", "fidelity", "pimco", "state street",
    "northern trust", "bny mellon", "nasdaq", "ice",
    "wells fargo", "mufg", "smbc", "societe generale", "credit agricole",
    "nomura", "lazard", "rothschild", "mizuho",
    "ares", "warburg pincus", "tpg", "bain capital", "general atlantic",
    "intercontinental exchange", "cboe", "cme group", "dtcc",
    "aqr", "renaissance", "elliott", "viking global",
    "wellington", "t. rowe price",
}

TIER_2_FIRMS = {
    "jefferies", "evercore", "cowen", "piper sandler", "stifel",
    "raymond james", "rbc", "scotiabank", "bmo", "houlihan lokey",
    "td bank", "td securities", "flagstar", "cibc", "william blair",
    "clear street", "virtu", "jane street", "drw", "susquehanna",
    "coinbase", "robinhood", "stripe", "ramp", "adyen", "webull",
    "interactive brokers", "drivewealth", "plaid", "sofi", "affirm",
    "neuberger berman", "invesco", "guggenheim", "franklin templeton",
    "centerview", "perella weinberg", "moelis", "cantor", "oppenheimer",
    "macquarie", "marex", "capital one", "synchrony",
    "kroll", "deloitte", "pwc", "ey", "kpmg", "crowe", "grant thornton",
    "alvarez", "fti consulting", "protiviti", "guidehouse",
    "capgemini", "accenture", "finra", "charles schwab",
    "pnc", "fifth third", "keycorp", "citizens", "m&t bank",
    "ally financial", "discover financial",
}

TIER_3_FIRMS = {
    "peloton", "greystone", "google", "amazon",
    "kraken", "gemini", "circle", "ripple", "block",
    "paypal", "chime", "betterment", "wealthfront",
    "oaktree", "pgim", "nuveen", "cohen & steers",
    "lpl financial", "ameriprise", "us bank",
    "copper", "athene", "sumitomo", "standard chartered",
    "metropolitan commercial bank", "new york life",
    "rockefeller", "pershing",
}

STAFFING_RE = re.compile(
    r"\b(?:staffing|recruiting|talent\s+(?:acquisition|solutions)"
    r"|search\s+partners|search\s+firm|manpower|adecco|robert\s+half"
    r"|randstad|hays|kelly\s+services|aston\s+carter|kforce|teksystems"
    r"|insight\s+global|beacon\s+hill|addison\s+group|phaxis|collabera"
    r"|apex\s+systems|prokatchers|cynet\s+systems|nextgen"
    r"|ascendo|hireminds|cardea|madison.davis|coda\s+search"
    r"|arrow\s+search|larson\s+maddox|barclay\s+simpson|ocr\s+alpha"
    r"|selby\s+jennings|harrington\s+starr|compliance\s+risk\s+concepts"
    r"|solomon\s+page|dynamic\s+search|odyssey\s+search"
    r"|options\s+group|glocap|landing\s+point|atlantic\s+group"
    r"|whitecap|social\s+capital|premium\s+technology)\b",
    re.I,
)

FINSERV_RE = re.compile(
    r"\b(?:bank(?:ing)?|broker[-\s]?dealer|securities|asset\s+management|investment"
    r"|hedge\s+fund|private\s+equity|capital\s+markets|fintech|wealth\s+management"
    r"|fund\s+(?:admin|operations|accounting)|financial\s+services"
    r"|compliance|regulatory|aml|kyc|bsa|finra|sec\b|cdd|due\s+diligence"
    r"|trading|equities|fixed\s+income|middle\s+office|back\s+office"
    r"|custody|clearing|settlement|prime\s+brokerage"
    r"|payments?|crypto|digital\s+assets)\b",
    re.I,
)

# ═══════════════════════════════════════════════════════════════════════════════
# ROLE-FAMILY CLASSIFICATION
# ═══════════════════════════════════════════════════════════════════════════════

# Ordered by specificity (most specific first)
ROLE_FAMILIES = [
    ("compliance_analyst", 95, re.compile(r"\b(?:compliance\s+(?:analyst|associate))\b", re.I)),
    ("registration_licensing", 95, re.compile(r"\b(?:(?:licensing|registration)\s+(?:analyst|specialist|associate|coordinator))\b", re.I)),
    ("kyc_aml", 90, re.compile(r"\b(?:(?:kyc|aml|bsa|cdd)\s+(?:analyst|associate|investigator|specialist))\b", re.I)),
    ("onboarding", 88, re.compile(r"\b(?:(?:onboarding|account\s+opening)\s+(?:analyst|specialist|associate|coordinator)|client\s+onboarding)\b", re.I)),
    ("regulatory_ops", 88, re.compile(r"\b(?:regulatory\s+(?:operations|compliance|affairs)\s+(?:analyst|associate|specialist|coordinator))\b", re.I)),
    ("financial_crime", 85, re.compile(r"\b(?:financial\s+crim(?:e|es)\s+(?:analyst|associate|specialist|investigator)|anti[-\s]?money\s+laundering)\b", re.I)),
    ("trade_surveillance", 85, re.compile(r"\b(?:(?:trade|market|communications?)\s+surveillance|transaction\s+monitoring)\b", re.I)),
    ("sanctions", 85, re.compile(r"\b(?:sanctions\s+(?:analyst|specialist|associate)|(?:ofac|sanctions)\s+(?:screening|compliance))\b", re.I)),
    ("bd_compliance", 85, re.compile(r"\b(?:broker[-\s]?dealer\s+compliance|(?:capital\s+markets|securities)\s+compliance)\b", re.I)),
    ("compliance_generic", 80, re.compile(r"\bcompliance\s+(?:officer|specialist|coordinator|testing|monitoring)\b", re.I)),
    ("due_diligence", 80, re.compile(r"\b(?:due\s+diligence|enhanced\s+due\s+diligence)\b", re.I)),
    ("risk_analyst", 65, re.compile(r"\b(?:risk\s+(?:analyst|associate|specialist)|(?:operational|credit|market)\s+risk\s+(?:analyst|associate))\b", re.I)),
    ("controls", 65, re.compile(r"\b(?:(?:controls?|governance)\s+(?:analyst|associate|advisory|specialist)|first\s+line\s+(?:risk|controls?))\b", re.I)),
    ("securities_ops", 70, re.compile(r"\b(?:securities\s+(?:operations|ops)|(?:clearing|settlement|custody)\s+(?:operations|analyst|associate))\b", re.I)),
    ("middle_office", 68, re.compile(r"\b(?:middle\s+office|trade\s+support)\b", re.I)),
    ("fund_ops", 65, re.compile(r"\b(?:fund\s+(?:operations|accounting|admin))\b", re.I)),
    ("investment_compliance", 82, re.compile(r"\b(?:investment\s+(?:compliance|adviser\s+compliance)|(?:asset\s+management|fund)\s+compliance)\b", re.I)),
    ("reg_reporting", 60, re.compile(r"\b(?:regulatory\s+reporting|prudential\s+reporting)\b", re.I)),
    ("ops_analyst", 50, re.compile(r"\b(?:operations\s+(?:analyst|associate|coordinator|specialist))\b", re.I)),
    ("client_service", 50, re.compile(r"\b(?:client\s+(?:service|services)\s+(?:analyst|associate|specialist|representative))\b", re.I)),
    ("compliance_analytics", 72, re.compile(r"\b(?:compliance\s+(?:analytics|data)|risk\s+analytics)\b", re.I)),
]

# Minimum relevance signal — title must match at least one of these to survive
RELEVANCE_RE = re.compile(
    r"\b(?:compliance|aml|kyc|bsa|regulatory|sanctions|financial\s+crim"
    r"|anti[-\s]?money|onboarding|account\s+opening|licensing|registration"
    r"|finra|surveillance|due\s+diligence|cdd|know\s+your\s+customer"
    r"|operations\s+(?:analyst|associate|coordinator|specialist)"
    r"|middle\s+office|trade\s+support|fund\s+(?:operations|accounting)"
    r"|securities|broker[-\s]?dealer|risk\s+(?:analyst|associate|operations|controls?)"
    r"|client\s+(?:service|onboarding)|controls?\s+(?:analyst|advisory)"
    r"|governance|clearing|settlement|custody"
    r"|transaction\s+monitoring|first\s+line)\b",
    re.I,
)

# ═══════════════════════════════════════════════════════════════════════════════
# HELPERS
# ═══════════════════════════════════════════════════════════════════════════════

def _clean(text: str) -> str:
    t = re.sub(r"<[^>]+>", " ", text or "")
    t = re.sub(r"&\w+;", " ", t)
    t = re.sub(r"&#x[0-9a-fA-F]+;", " ", t)
    return re.sub(r"\s+", " ", t).strip()


def _extract(block: str, pattern: str) -> str:
    m = re.search(pattern, block, re.S)
    return _clean(m.group(1)) if m else ""


def _fetch(url: str, params: dict | None = None, timeout: int = 20) -> str:
    if params:
        url = f"{url}?{urllib.parse.urlencode(params, doseq=True)}"
    req = urllib.request.Request(url, headers=BROWSER_HEADERS)
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return resp.read().decode("utf-8", errors="replace")


def _parse_salary_annual(salary_text: str) -> int | None:
    if not salary_text:
        return None
    s = salary_text.lower().replace(",", "").replace("$", "")
    nums = re.findall(r"([\d.]+)", s)
    if not nums:
        return None
    vals = [float(x) for x in nums]
    if "hour" in s:
        return int(max(vals) * 2080)
    if "year" in s:
        return int(max(vals))
    if "month" in s:
        return int(max(vals) * 12)
    top = max(vals)
    if top > 1000:
        return int(top)
    return None


# ═══════════════════════════════════════════════════════════════════════════════
# APPLIED SUPPRESSION — confidence-tiered
# ═══════════════════════════════════════════════════════════════════════════════

def _norm_company(text: str) -> str:
    t = re.sub(r"[^a-z0-9 ]", " ", (text or "").lower())
    for stop in ("inc", "llc", "ltd", "corp", "co", "company", "group", "holdings",
                 "international", "partners", "search", "recruiting", "staffing",
                 "solutions", "consulting", "services", "the", "n a", "lp", "l p"):
        t = re.sub(rf"\b{stop}\b", "", t)
    return re.sub(r"\s+", " ", t).strip()


def _norm_title(text: str) -> str:
    t = re.sub(r"[^a-z0-9 ]", " ", (text or "").lower())
    for pfx in ("senior ", "junior ", "sr ", "jr ", "lead ", "staff "):
        if t.startswith(pfx):
            t = t[len(pfx):]
    t = re.sub(r"\(.*?\)", "", t)
    return re.sub(r"\s+", " ", t).strip()


def load_applied_index() -> list[tuple[str, str]]:
    if not APPLIED_CSV.exists():
        return []
    pairs = []
    with open(APPLIED_CSV, "r", encoding="utf-8", errors="replace") as f:
        for row in csv.DictReader(f):
            c = _norm_company(row.get("company", "") or row.get("company_raw", ""))
            t = _norm_title(row.get("title", "") or row.get("title_raw", ""))
            if c and t:
                pairs.append((c, t))
    return pairs


def is_applied(company: str, title: str, index: list[tuple[str, str]]) -> str | None:
    """Return suppression reason string, or None if not suppressed.

    Confidence tiers:
      1. Exact normalized company+title match
      2. Company substring (≥4 chars both sides) + title overlap ≥60% of LARGER set
    """
    c = _norm_company(company)
    t = _norm_title(title)
    if not c or not t:
        return None
    t_words = set(t.split())

    for ac, at in index:
        # Company match: exact or safe substring
        company_match = None
        if c == ac:
            company_match = "exact"
        elif len(ac) >= 4 and len(c) >= 4 and (ac in c or c in ac):
            company_match = "substring"
        else:
            continue

        # Title match: exact or word overlap
        if t == at:
            return f"exact_match:{ac}|{at}"
        at_words = set(at.split())
        if not t_words or not at_words:
            continue
        overlap = t_words & at_words
        max_len = max(len(t_words), len(at_words))
        if max_len > 0 and len(overlap) / max_len >= 0.6:
            return f"{company_match}_fuzzy:{ac}|{at}"

    return None


# ═══════════════════════════════════════════════════════════════════════════════
# HARD REJECT GATE — only truly impossible jobs
# ═══════════════════════════════════════════════════════════════════════════════

def hard_reject(title: str, company: str, location: str,
                salary_text: str, snippet: str) -> str | None:
    """Return reject reason, or None if job should enter the scoring pipeline.

    Philosophy: only reject what is STRUCTURALLY IMPOSSIBLE. Everything else
    gets scored — seniority, experience, weak relevance are soft penalties.
    """
    # Impossible titles
    if HARD_TITLE_RE.search(title):
        return f"title:{title[:60]}"

    # Location — must have NYC signal (or JC, which gets soft penalty)
    loc = location.lower()
    if "remote" in loc and "hybrid" not in loc:
        return "remote_only"
    if NON_NYC_RE.search(location):
        return f"non_nyc:{location}"
    if not NYC_RE.search(location) and "ny" not in loc and not JC_RE.search(location):
        return f"not_nyc:{location}"

    # Impossible industries
    blob = f"{title} {company} {snippet}"
    if HARD_INDUSTRY_RE.search(blob) and not FIN_GOV_RE.search(blob):
        return f"industry:{company}"

    # Government (unless financial regulator)
    if GOV_RE.search(blob) and not FIN_GOV_RE.search(blob) and not FINSERV_RE.search(blob):
        return f"government:{company}"

    # Contract/temp/seasonal
    if CONTRACT_RE.search(title):
        return "contract_temp"

    # Salary floor: reject if stated max < $60k
    annual = _parse_salary_annual(salary_text)
    if annual is not None and annual < 60000:
        return f"low_salary:{annual}"

    # Zero relevance — no compliance/ops/finance signal in title at all
    if not RELEVANCE_RE.search(title):
        # Check snippet for signal before rejecting
        if not RELEVANCE_RE.search(snippet[:300]):
            return f"no_relevance:{title[:60]}"

    return None


# ═══════════════════════════════════════════════════════════════════════════════
# HIRE PROBABILITY SCORING ENGINE (3 components)
#   Seniority Fit  45%  — #1 predictor of whether Robin gets an interview
#   Category Match 35%  — how well the role matches Robin's compliance/ops lane
#   Comp Alignment 20%  — salary range sanity check
#
# NO firm prestige scoring. Robin evaluates firm fit himself.
# ═══════════════════════════════════════════════════════════════════════════════

# Category tiers: how well does this role match Robin's lane?
# Tier A (100): direct compliance ops / onboarding / licensing / BD ops / KYC-AML analyst
# Tier B (75):  adjacent (risk ops, controls, securities ops, middle office, fund ops)
# Tier C (50):  tangential (investigations, reg reporting, client service at fin firm)
# Tier D (25):  weak (generic ops, accounting, IT compliance, audit, credit)

_CAT_A = re.compile(
    r"\b(?:compliance\s+(?:analyst|associate|specialist|coordinator|testing|monitoring)"
    r"|(?:licensing|registration)\s+(?:analyst|specialist|associate|coordinator)"
    r"|(?:kyc|aml|bsa|cdd)\s+(?:analyst|associate|investigator|specialist)"
    r"|(?:onboarding|account\s+opening)\s+(?:analyst|specialist|associate|coordinator)"
    r"|client\s+onboarding"
    r"|regulatory\s+(?:operations|compliance|affairs)\s+(?:analyst|associate|specialist)"
    r"|broker[-\s]?dealer\s+(?:compliance|operations)"
    r"|(?:capital\s+markets|securities)\s+compliance"
    r"|investment\s+(?:compliance|adviser\s+compliance)"
    r"|(?:asset\s+management|fund)\s+compliance"
    r"|compliance\s+(?:analytics|data)"
    r"|advertising\s+compliance)\b", re.I)

_CAT_B = re.compile(
    r"\b(?:risk\s+(?:analyst|associate|specialist|operations)"
    r"|(?:operational|credit|market|enterprise)\s+risk"
    r"|(?:controls?|governance)\s+(?:analyst|associate|advisory|specialist)"
    r"|first\s+line\s+(?:risk|controls?)"
    r"|securities\s+(?:operations|ops)"
    r"|(?:clearing|settlement|custody)\s+(?:operations|analyst|associate)"
    r"|middle\s+office|trade\s+support"
    r"|fund\s+(?:operations|accounting|admin)"
    r"|sanctions\s+(?:analyst|specialist|associate)"
    r"|financial\s+crim(?:e|es)\s+(?:analyst|associate|specialist)"
    r"|(?:trade|market|communications?)\s+surveillance"
    r"|transaction\s+monitoring"
    r"|due\s+diligence"
    r"|anti[-\s]?money\s+laundering"
    r"|prime\s+(?:services|brokerage|finance|fin\s+svc))\b", re.I)

_CAT_C = re.compile(
    r"\b(?:operations\s+(?:analyst|associate|coordinator|specialist)"
    r"|client\s+(?:service|services)\s+(?:analyst|associate|specialist)"
    r"|regulatory\s+reporting|prudential\s+reporting"
    r"|transition\s+(?:analyst|associate|manager))\b", re.I)

_CAT_D = re.compile(
    r"\b(?:audit|accounting|tax|credit\s+(?:analyst|officer)"
    r"|IT\s+(?:compliance|risk|security)"
    r"|marketing|recruiting|talent)\b", re.I)

# Hard disqualifier categories — score = 0
_DISQUALIFY_RE = re.compile(
    r"\b(?:(?<!\w)director\b|vice\s+president|(?<!\w)vp\b|head\s+of"
    r"|managing\s+director|principal|partner\b"
    r"|(?<!\w)trading(?!\s+(?:support|operations|ops|surveillance|compliance))\b"
    r"|IT\s+security|(?:soc\s*2|nist|iso\s*27001)"
    r"|(?:capital\s+markets|investment)\s+banking"
    r"|commission[-\s]?based|cold\s+calling)\b", re.I)


def _classify_category(title: str, snippet: str) -> tuple[str, int]:
    """Return (category_label, base_score 0-100)."""
    if _CAT_A.search(title):
        return "direct_target", 100
    if _CAT_B.search(title):
        return "strong_adjacent", 75
    # Check snippet for A/B signals if title is generic
    if _CAT_A.search(snippet[:400]):
        return "direct_target_from_desc", 85
    if _CAT_B.search(snippet[:400]):
        return "adjacent_from_desc", 65
    if _CAT_C.search(title):
        return "tangential", 50
    if _CAT_D.search(title):
        return "weak_fit", 25
    return "unclassified", 20


def _seniority_score(title: str, snippet: str) -> tuple[int, list[str]]:
    """Return (score 0-100, list of penalty/boost notes).

    Full marks for entry/analyst/associate. Heavy penalties for senior+.
    """
    tl = title.lower()
    score = 70  # baseline
    notes = []

    # ── Boosts ──
    if any(kw in tl for kw in ("analyst", "associate")):
        score += 15
        notes.append("analyst/associate level")
    if any(kw in tl for kw in ("specialist", "coordinator")):
        score += 10
        notes.append("specialist/coordinator level")
    if "entry" in tl or "junior" in tl or "jr" in tl:
        score += 20
        notes.append("entry level")
    if re.search(r"\b(?:entry\s+level|0[-\s]?[12]\s*years?|1[-\s]?2\s*years?)\b", snippet, re.I):
        score += 12
        notes.append("low experience bar")
    if re.search(r"\b(?:series\s*7|SIE|finra\s+license)\b", snippet, re.I):
        score += 8
        notes.append("Series 7 valued")
    if re.search(r"\bbroker[-\s]?dealer\b", snippet, re.I):
        score += 5
        notes.append("BD experience match")

    # ── Penalties ──
    if re.search(r"\bsenior\b", title, re.I):
        score -= 25
        notes.append("senior title")
    if re.search(r"\b(?:manager|lead)\b", title, re.I):
        score -= 20
        notes.append("management/lead")
    if re.search(r"\bsupervisor\b", title, re.I):
        score -= 15
        notes.append("supervisory")
    if "officer" in tl:
        score -= 10
        notes.append("officer level")
    if "consultant" in tl:
        score -= 10
        notes.append("consultant role")
    if re.search(r"\b[3-4]\+?\s*(?:years?|yrs?)\s*(?:of\s+)?(?:experience|exp)\b", snippet, re.I):
        score -= 30
        notes.append("3-4yr experience req")
    if re.search(r"\b(?:[5-9]|1[0-9]|20)\+?\s*(?:years?|yrs?)\s*(?:of\s+)?(?:experience|exp)\b", snippet, re.I):
        score -= 40
        notes.append("5yr+ experience req")
    if re.search(r"\b(?:cpa|cfa|cams|acams|jd|ll\.?m|juris\s+doctor)\b", snippet, re.I):
        score -= 10
        notes.append("requires certification")
    # Data/quant/technical skills Robin doesn't have
    if re.search(r"\b(?:sql|python|r\b|tableau|power\s*bi|data\s+(?:science|engineer|analytics))\b", snippet, re.I):
        if "data" in tl or "analytics" in tl or "quant" in tl:
            score -= 25
            notes.append("data/quant skills required")
    # Even without snippet, "data" in title is a signal
    if "data" in tl and ("compliance" in tl or "risk" in tl):
        score -= 15
        notes.append("data-focused compliance role")

    return max(0, min(100, score)), notes


def _comp_score(salary_text: str) -> tuple[int, list[str]]:
    """Return (score 0-100, notes).

    $80-150K or unlisted = full. <$60K = penalty. >$200K = seniority red flag.
    """
    annual = _parse_salary_annual(salary_text)
    if annual is None:
        return 70, []  # unknown — neutral

    if annual >= 200000:
        return 30, [f"${annual//1000}K signals senior role"]
    if annual >= 150000:
        return 55, [f"${annual//1000}K — possibly senior"]
    if annual >= 100000:
        return 95, [f"${annual//1000}K in sweet spot"]
    if annual >= 80000:
        return 90, [f"${annual//1000}K in range"]
    if annual >= 65000:
        return 75, [f"${annual//1000}K — adequate"]
    if annual >= 60000:
        return 60, [f"${annual//1000}K — low but passable"]
    return 20, [f"${annual//1000}K — below floor"]


def score_job(title: str, company: str, snippet: str,
              salary_text: str, location: str) -> dict:
    """Hire probability scoring: how likely is Robin to get an interview?

    3 components:
      Seniority Fit  (45%)
      Category Match (35%)
      Comp Alignment (20%)

    No firm prestige scoring. Staffing penalty is the only firm-level signal.
    """
    title_lower = title.lower()
    penalties = []
    boosts = []

    # ── HARD DISQUALIFY ──────────────────────────────────────────────────
    if _DISQUALIFY_RE.search(title):
        return {
            "score": 0, "bucket": "Disqualified",
            "reason": f"hard disqualifier in title",
            "risk": title[:60], "role_family": "disqualified",
            "firm_tier": 0, "firm_label": "",
            "penalties": ["disqualified"], "boosts": [],
            "components": {"seniority": 0, "category": 0, "compensation": 0},
        }

    # ── 1. SENIORITY FIT (45%) ───────────────────────────────────────────
    sen_score, sen_notes = _seniority_score(title, snippet)
    penalties.extend([n for n in sen_notes if any(w in n for w in ("senior", "management", "lead", "officer", "supervisor", "experience", "certification", "consultant"))])
    boosts.extend([n for n in sen_notes if any(w in n for w in ("entry", "analyst", "specialist", "Series", "BD", "low exp"))])

    # ── 2. CATEGORY MATCH (35%) ──────────────────────────────────────────
    cat_label, cat_score = _classify_category(title, snippet)

    # Category-level adjustments
    if cat_label in ("weak_fit", "unclassified"):
        penalties.append(f"weak category fit ({cat_label})")

    # ── 3. COMPENSATION ALIGNMENT (20%) ──────────────────────────────────
    comp_s, comp_notes = _comp_score(salary_text)
    penalties.extend([n for n in comp_notes if "senior" in n or "below" in n or "low" in n])
    boosts.extend([n for n in comp_notes if "sweet spot" in n or "in range" in n])

    # ── WEIGHTED TOTAL ───────────────────────────────────────────────────
    total = round(
        sen_score * 0.45 +
        cat_score * 0.35 +
        comp_s * 0.20
    )
    total = max(0, min(100, total))

    # ── POST-SCORE ADJUSTMENTS (no firm prestige) ────────────────────────

    # Staffing agency: -10 penalty
    is_staffing = bool(STAFFING_RE.search(company))
    if is_staffing:
        total = max(0, total - 10)
        penalties.append("staffing agency")

    # Bonuses — only if seniority isn't already penalized
    if sen_score >= 65:
        if any(kw in title_lower for kw in ("analyst", "associate", "specialist", "coordinator")):
            total = min(100, total + 5)
            if "analyst/associate level" not in boosts:
                boosts.append("entry-level title")
        if cat_label == "direct_target":
            total = min(100, total + 5)
            boosts.append("direct compliance/ops match")

    # Compensation in $100K range bonus
    annual = _parse_salary_annual(salary_text)
    if annual and 80000 <= annual <= 150000:
        total = min(100, total + 3)

    total = max(0, min(100, total))

    # ── BUCKETING ────────────────────────────────────────────────────────
    if total >= 80:
        bucket = "Tier 1 — Apply Immediately"
    elif total >= 65:
        bucket = "Tier 2 — Review & Apply"
    elif total >= 50:
        bucket = "Tier 3 — Low Priority"
    else:
        bucket = "Below Threshold"

    # ── ONE-LINER REASON ─────────────────────────────────────────────────
    parts = []
    cat_labels = {
        "direct_target": "direct compliance/ops match",
        "direct_target_from_desc": "compliance signal in description",
        "strong_adjacent": "strong adjacent role",
        "adjacent_from_desc": "adjacent signal in description",
        "tangential": "tangential fit",
    }
    if cat_label in cat_labels:
        parts.append(cat_labels[cat_label])

    if sen_score >= 80:
        parts.append("realistic hire")
    elif sen_score >= 60:
        parts.append("plausible hire")
    elif sen_score < 40:
        parts.append("stretch — likely too senior")

    if annual:
        if 80000 <= annual <= 150000:
            parts.append(f"${annual:,}/yr")
        elif annual > 150000:
            parts.append(f"${annual:,}/yr (seniority signal)")
        else:
            parts.append(f"${annual:,}/yr (low)")

    if JC_RE.search(location):
        parts.append("Jersey City")
        penalties.append("Jersey City")

    reason = "; ".join(parts) if parts else "review posting"

    # ── RISK STRING ──────────────────────────────────────────────────────
    risk = "; ".join(penalties[:3]) if penalties else ""

    # Firm tier for downstream use (not in scoring)
    c = company.lower()
    if is_staffing:
        ft, fl = 5, "staffing"
    elif any(f in c for f in TIER_1_FIRMS):
        ft, fl = 1, "tier1"
    elif any(f in c for f in TIER_2_FIRMS):
        ft, fl = 2, "tier2"
    elif any(f in c for f in TIER_3_FIRMS):
        ft, fl = 3, "tier3"
    else:
        ft, fl = 4, "other"

    # Role family for downstream use
    role_family = cat_label

    return {
        "score": total,
        "bucket": bucket,
        "reason": reason,
        "risk": risk,
        "role_family": role_family,
        "firm_tier": ft,
        "firm_label": fl,
        "penalties": penalties,
        "boosts": boosts,
        "components": {
            "seniority": sen_score,
            "category": cat_score,
            "compensation": comp_s,
        },
    }


# ═══════════════════════════════════════════════════════════════════════════════
# INDEED SCRAPER
# ═══════════════════════════════════════════════════════════════════════════════

def scrape_indeed(queries: list[str] | None = None,
                  pages_per_query: int = 2) -> tuple[list[dict], dict]:
    """Scrape Indeed. Returns (jobs, audit_data)."""
    queries = queries or ALL_QUERIES
    all_jobs: list[dict] = []
    seen_keys: set[str] = set()
    query_audit: dict[str, dict] = {}
    errors: list[str] = []

    # Randomize order to avoid same queries always getting blocked
    shuffled = list(enumerate(queries))
    random.shuffle(shuffled)

    for orig_idx, query in shuffled:
        qa = {"query": query, "raw": 0, "unique": 0, "pages": 0, "error": None}

        for page in range(pages_per_query):
            try:
                html = _fetch(
                    f"https://{INDEED_DOMAIN}/jobs",
                    params={
                        "q": query,
                        "l": INDEED_LOCATION,
                        "fromage": "14",
                        "sort": "date",
                        "start": str(page * 10),
                    },
                )
            except Exception as exc:
                qa["error"] = str(exc)[:80]
                errors.append(f"{query}: {exc}")
                break

            if BLOCKED_RE.search(html):
                qa["error"] = "blocked"
                errors.append(f"{query}: blocked")
                break

            qa["pages"] += 1
            page_jobs = []
            for m in JOB_CARD_RE.finditer(html):
                block = m.group(1)
                job_key = _extract(block, r'data-jk="([^"]+)"')
                job_title = _extract(block, r'id="jobTitle-[^"]+">(.*?)</span>')
                job_company = _extract(block, r'data-testid="company-name"[^>]*>(.*?)</span>')
                job_location = _extract(block, r'data-testid="text-location"[^>]*>(.*?)</div>')
                job_salary = _extract(block, r'salary-snippet-container.*?<span[^>]*>(.*?)</span>')
                job_snippet = _extract(block, r'data-testid="belowJobSnippet"[^>]*>(.*?)</div>')

                if not job_key or not job_title or not job_company:
                    continue

                qa["raw"] += 1

                if job_key in seen_keys:
                    continue
                seen_keys.add(job_key)
                qa["unique"] += 1

                page_jobs.append({
                    "job_key": job_key,
                    "title": job_title,
                    "company": job_company,
                    "location": job_location,
                    "salary": job_salary,
                    "snippet": job_snippet or job_title,
                    "url": f"https://www.indeed.com/viewjob?jk={job_key}",
                    "query": query,
                })

            all_jobs.extend(page_jobs)
            if len(page_jobs) < 8:
                break
            time.sleep(random.uniform(0.8, 1.8))
        time.sleep(random.uniform(0.4, 1.0))

        query_audit[query] = qa

    if errors:
        print(f"  [scrape] {len(errors)} errors (first 3: {errors[:3]})")

    return all_jobs, {
        "query_count": len(queries),
        "total_raw": sum(q["raw"] for q in query_audit.values()),
        "total_unique": len(all_jobs),
        "blocked_queries": sum(1 for q in query_audit.values() if q["error"] == "blocked"),
        "failed_queries": sum(1 for q in query_audit.values() if q["error"] and q["error"] != "blocked"),
        "query_detail": query_audit,
    }


# ═══════════════════════════════════════════════════════════════════════════════
# PIPELINE ORCHESTRATOR
# ═══════════════════════════════════════════════════════════════════════════════

def _cross_source_dedup(jobs: list[dict]) -> list[dict]:
    """Deduplicate across sources. Same company+title = keep the one with better source."""
    SOURCE_PRIORITY = {"greenhouse": 1, "lever": 2, "efinancialcareers": 3, "indeed": 4}
    seen: dict[str, dict] = {}  # key -> best job
    for job in jobs:
        c = _norm_company(job["company"])
        t = _norm_title(job["title"])
        key = f"{c}|{t}"
        if key in seen:
            existing_prio = SOURCE_PRIORITY.get(seen[key].get("source", "indeed"), 5)
            new_prio = SOURCE_PRIORITY.get(job.get("source", "indeed"), 5)
            if new_prio < existing_prio:
                seen[key] = job  # keep better source
        else:
            seen[key] = job
    return list(seen.values())


def run_pipeline() -> dict:
    """Full multi-source pipeline: scrape all -> gate -> suppress -> score -> bucket -> audit."""

    print("[1/6] Loading applied jobs index...")
    applied_index = load_applied_index()
    print(f"  {len(applied_index)} applied pairs loaded")

    # ── MULTI-SOURCE SCRAPING ──────────────────────────────────────────────
    source_audits = {}

    print(f"\n[2/6] Scraping Indeed ({len(ALL_QUERIES)} queries, 7 clusters)...")
    indeed_jobs, indeed_audit = scrape_indeed()
    for j in indeed_jobs:
        j.setdefault("source", "indeed")
    source_audits["indeed"] = indeed_audit
    print(f"  Indeed: {indeed_audit['total_unique']} unique from {indeed_audit['total_raw']} raw")

    print("\n[2/6] Scraping Greenhouse + Lever (direct employer boards)...")
    try:
        from scripts.source_greenhouse_lever import scrape_all as scrape_gh_lv
        gh_lv_jobs, gh_lv_audit = scrape_gh_lv()
        source_audits["greenhouse"] = gh_lv_audit.get("greenhouse", {})
        source_audits["lever"] = gh_lv_audit.get("lever", {})
        print(f"  Greenhouse: {gh_lv_audit['greenhouse']['nyc']} NYC jobs from "
              f"{gh_lv_audit['greenhouse']['boards_checked']} boards "
              f"({gh_lv_audit['greenhouse']['errors']} errors)")
        print(f"  Lever: {gh_lv_audit['lever']['nyc']} NYC jobs from "
              f"{gh_lv_audit['lever']['boards_checked']} boards "
              f"({gh_lv_audit['lever']['errors']} errors)")
    except Exception as exc:
        gh_lv_jobs = []
        source_audits["greenhouse"] = {"error": str(exc)[:80]}
        source_audits["lever"] = {"error": str(exc)[:80]}
        print(f"  Greenhouse/Lever FAILED: {exc}")

    print("\n[2/6] Scraping eFinancialCareers...")
    try:
        from scripts.source_efinancialcareers import scrape as scrape_efc
        efc_jobs, efc_audit = scrape_efc()
        source_audits["efinancialcareers"] = efc_audit
        print(f"  eFinancialCareers: {len(efc_jobs)} NYC jobs "
              f"({efc_audit['raw']} raw, {efc_audit['errors']} errors, {efc_audit['blocked']} blocked)")
    except Exception as exc:
        efc_jobs = []
        source_audits["efinancialcareers"] = {"error": str(exc)[:80]}
        print(f"  eFinancialCareers FAILED: {exc}")

    # Combine all sources
    all_raw = indeed_jobs + gh_lv_jobs + efc_jobs
    print(f"\n  TOTAL RAW: {len(all_raw)} jobs across all sources")

    # Cross-source dedup
    all_raw = _cross_source_dedup(all_raw)
    print(f"  After cross-source dedup: {len(all_raw)}")

    # ── GATE + SUPPRESS ────────────────────────────────────────────────────
    print("\n[3/6] Hard reject gate + suppression...")
    reject_reasons: Counter = Counter()
    suppress_reasons: list[dict] = []
    passed: list[dict] = []

    for job in all_raw:
        source = job.get("source", "indeed")
        title = job["title"]

        # ALL sources get title rejection — no bypassing
        if HARD_TITLE_RE.search(title):
            reject_reasons["title"] += 1
            continue

        # Greenhouse/Lever are pre-filtered for NYC+relevance at ATS level,
        # so skip location/industry/relevance checks — but still check suppression
        if source in ("greenhouse", "lever"):
            supp = is_applied(job["company"], title, applied_index)
            if supp:
                suppress_reasons.append({
                    "title": title, "company": job["company"],
                    "source": source, "reason": supp,
                })
                continue
            passed.append(job)
            continue

        reason = hard_reject(
            job["title"], job["company"], job["location"],
            job.get("salary", ""), job.get("snippet", ""),
        )
        if reason:
            reject_reasons[reason.split(":")[0]] += 1
            continue

        supp = is_applied(job["company"], job["title"], applied_index)
        if supp:
            suppress_reasons.append({
                "title": job["title"], "company": job["company"],
                "source": source, "reason": supp,
            })
            print(f"  SUPPRESSED: [{job['title'][:50]}] at [{job['company'][:30]}] ({supp.split(':')[0]})")
            continue

        passed.append(job)

    print(f"  Passed: {len(passed)} | Rejected: {sum(reject_reasons.values())} | Suppressed: {len(suppress_reasons)}")
    print(f"  Reject breakdown: {dict(reject_reasons.most_common(8))}")

    # ── SCORING ────────────────────────────────────────────────────────────
    print("\n[4/6] Scoring (hire probability model)...")
    for job in passed:
        s = score_job(job["title"], job["company"], job.get("snippet", ""),
                      job.get("salary", ""), job.get("location", "New York, NY"))
        job.update(s)

    # Filter out disqualified jobs
    passed = [j for j in passed if j["bucket"] != "Disqualified"]

    passed.sort(key=lambda x: x["score"], reverse=True)

    buckets: dict[str, list] = defaultdict(list)
    for job in passed:
        buckets[job["bucket"]].append(job)

    t1 = "Tier 1 — Apply Immediately"
    t2 = "Tier 2 — Review & Apply"
    t3 = "Tier 3 — Low Priority"
    bt = "Below Threshold"
    print(f"  Tier 1: {len(buckets[t1])} | "
          f"Tier 2: {len(buckets[t2])} | "
          f"Tier 3: {len(buckets[t3])} | "
          f"Below: {len(buckets[bt])}")

    # ── SOURCE-BY-SOURCE YIELD ─────────────────────────────────────────────
    print("\n[5/6] Source-by-source yield:")
    source_yield = {}
    for src_name in ("indeed", "greenhouse", "lever", "efinancialcareers"):
        src_jobs = [j for j in passed if j.get("source", "indeed") == src_name]
        src_st = sum(1 for j in src_jobs if j["score"] >= 80)
        src_sb = sum(1 for j in src_jobs if 65 <= j["score"] < 80)
        src_staffing = sum(1 for j in src_jobs if j.get("firm_tier") == 5)
        src_direct = len(src_jobs) - src_staffing
        source_yield[src_name] = {
            "total": len(src_jobs),
            "strong_target": src_st,
            "strong_bridge": src_sb,
            "direct_employer": src_direct,
            "staffing": src_staffing,
        }
        if len(src_jobs) > 0:
            top_firms = Counter(j["company"] for j in src_jobs).most_common(5)
            source_yield[src_name]["top_firms"] = [f[0] for f in top_firms]
            print(f"  {src_name:22s} | {len(src_jobs):3d} total | {src_st:2d} strong | "
                  f"{src_sb:2d} bridge | {src_direct:3d} direct / {src_staffing:2d} staffing")
        else:
            source_yield[src_name]["top_firms"] = []
            print(f"  {src_name:22s} | 0 jobs")

    # ── REPORT + AUDIT ─────────────────────────────────────────────────────
    print("\n[6/6] Generating report...")
    visible = [j for j in passed if j["score"] >= 50]

    total_scraped = len(all_raw)
    total_rejected = sum(reject_reasons.values())
    total_suppressed = len(suppress_reasons)

    generate_report(visible, {
        "total_scraped": total_scraped,
        "rejected": total_rejected,
        "suppressed": total_suppressed,
        "strong_target": len(buckets.get(t1, [])),
        "strong_bridge": len(buckets.get(t2, [])),
        "stretch": len(buckets.get(t3, [])),
        "maybe": len(buckets.get(bt, [])),
        "low_value": 0,
        "generated": datetime.now().strftime("%Y-%m-%d %H:%M"),
    })
    print(f"  Report: {REPORT_OUT}")

    audit = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "sources": source_audits,
        "source_yield": source_yield,
        "reject_reasons": dict(reject_reasons.most_common()),
        "suppress_count": total_suppressed,
        "suppress_detail": suppress_reasons[:20],
        "buckets": {k: len(v) for k, v in buckets.items()},
        "total_scored": len(passed),
        "total_visible": len(visible),
        "top_10": [
            {"title": j["title"][:60], "company": j["company"][:30],
             "score": j["score"], "bucket": j["bucket"],
             "source": j.get("source", "indeed"),
             "role_family": j["role_family"], "firm_tier": j["firm_tier"]}
            for j in passed[:10]
        ],
    }
    AUDIT_OUT.parent.mkdir(parents=True, exist_ok=True)
    AUDIT_OUT.write_text(json.dumps(audit, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"  Audit trail: {AUDIT_OUT}")

    # Cluster yield (Indeed only)
    cluster_yield: dict[str, dict] = {}
    for cluster_name, cluster_queries in QUERY_CLUSTERS.items():
        cq_set = set(cluster_queries)
        cluster_jobs = [j for j in passed if j.get("query") in cq_set]
        cluster_yield[cluster_name] = {
            "queries": len(cluster_queries),
            "survivors": len(cluster_jobs),
            "strong_target": sum(1 for j in cluster_jobs if j["score"] >= 80),
        }
    print("\n  Indeed cluster yield:")
    for cn, cy in cluster_yield.items():
        print(f"    {cn:25s} | {cy['queries']:2d} queries -> {cy['survivors']:3d} survivors ({cy['strong_target']} strong)")

    return {
        "all": passed,
        "buckets": dict(buckets),
        "visible": visible,
        "meta": {
            "total_scraped": total_scraped,
            "rejected": total_rejected,
            "deduped": total_suppressed,
            "apply_count": len(buckets.get(t1, [])) + len(buckets.get(t2, [])),
            "maybe_count": len(buckets.get(t3, [])),
            "skip_count": len(buckets.get(bt, [])),
            "generated": datetime.now().strftime("%Y-%m-%d %H:%M"),
        },
        "audit": audit,
    }


# ═══════════════════════════════════════════════════════════════════════════════
# REPORT HTML — mobile-first, bucketed
# ═══════════════════════════════════════════════════════════════════════════════

def _esc(s):
    import html
    return html.escape(str(s) if s else "")


def generate_report(jobs: list[dict], meta: dict) -> None:
    REPORT_OUT.parent.mkdir(parents=True, exist_ok=True)

    bucket_colors = {
        "Tier 1 — Apply Immediately": ("t1apply", "#065f46", "#34d399", "#022c22", "#6ee7b7"),
        "Tier 2 — Review & Apply": ("t2review", "#1e3a5f", "#60a5fa", "#172554", "#93c5fd"),
        "Tier 3 — Low Priority": ("t3low", "#78350f", "#fbbf24", "#451a03", "#fcd34d"),
        "Below Threshold": ("below", "#3f3f46", "#71717a", "#27272a", "#a1a1aa"),
    }

    rows_html = ""
    for i, j in enumerate(jobs, 1):
        bucket = j.get("bucket", "Below Threshold")
        bkey, bg, fg, badge_bg, badge_fg = bucket_colors.get(bucket, bucket_colors["Below Threshold"])
        sal = j.get("salary") or "Not listed"
        risk = j.get("risk", "")
        firm_tag = ""
        if j.get("firm_tier") == 5:
            firm_tag = ' <span class="tag staffing">STAFFING</span>'
        elif j.get("firm_tier") == 1:
            firm_tag = ' <span class="tag t1">TOP FIRM</span>'
        elif j.get("firm_tier") == 2:
            firm_tag = ' <span class="tag t2">STRONG</span>'

        risk_html = f'<div class="card-risk">⚠ {_esc(risk)}</div>' if risk else ""

        rows_html += f"""
        <div class="card {bkey}" data-bucket="{bkey}" onclick="window.open('{_esc(j['url'])}','_blank')">
          <div class="card-header">
            <span class="rank">#{i}</span>
            <span class="score-badge" style="background:{bg};color:{fg}">{j['score']}</span>
            <span class="bucket-badge" style="background:{badge_bg};color:{badge_fg}">{bucket}</span>
            {firm_tag}
          </div>
          <div class="card-title">{_esc(j['title'])}</div>
          <div class="card-company">{_esc(j['company'])}</div>
          <div class="card-meta">
            <span>{_esc(j['location'])}</span>
            <span class="salary">{_esc(sal)}</span>
          </div>
          <div class="card-reason">{_esc(j['reason'])}</div>
          {risk_html}
          <a href="{_esc(j['url'])}" class="card-link" target="_blank"
             onclick="event.stopPropagation()">Open on Indeed →</a>
        </div>"""

    st = meta.get("strong_target", 0)
    sb = meta.get("strong_bridge", 0)
    sr = meta.get("stretch", 0)
    mb = meta.get("maybe", 0)

    html_content = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1,maximum-scale=1">
<title>NYC Compliance Jobs — {meta['generated']}</title>
<style>
*{{box-sizing:border-box;margin:0;padding:0}}
body{{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif;background:#0f172a;color:#e2e8f0;-webkit-font-smoothing:antialiased}}
.header{{background:linear-gradient(135deg,#1e293b,#334155);padding:20px 16px;position:sticky;top:0;z-index:10;border-bottom:1px solid #475569}}
.header h1{{font-size:18px;font-weight:700;color:#f8fafc;margin-bottom:4px}}
.stats{{font-size:12px;color:#94a3b8;display:flex;gap:10px;flex-wrap:wrap}}
.stats b{{color:#38bdf8}}
.filters{{display:flex;gap:6px;padding:10px 16px;overflow-x:auto;background:#1e293b;border-bottom:1px solid #334155}}
.filter-btn{{padding:7px 14px;border-radius:20px;border:1px solid #475569;background:transparent;color:#94a3b8;font-size:12px;font-weight:600;cursor:pointer;white-space:nowrap;-webkit-tap-highlight-color:transparent}}
.filter-btn.active{{background:#38bdf8;color:#0f172a;border-color:#38bdf8}}
.cards{{padding:12px;display:flex;flex-direction:column;gap:10px;max-width:680px;margin:0 auto}}
.card{{background:#1e293b;border-radius:14px;padding:14px;border:1px solid #334155;cursor:pointer;-webkit-tap-highlight-color:transparent;transition:border-color .15s}}
.card:active{{border-color:#38bdf8}}
.card-header{{display:flex;align-items:center;gap:6px;margin-bottom:8px;flex-wrap:wrap}}
.rank{{font-size:12px;color:#64748b;font-weight:700}}
.score-badge{{font-size:13px;font-weight:800;padding:2px 8px;border-radius:10px}}
.bucket-badge{{font-size:10px;font-weight:700;padding:2px 8px;border-radius:10px;text-transform:uppercase;letter-spacing:.03em}}
.tag{{font-size:9px;font-weight:700;padding:2px 6px;border-radius:8px;text-transform:uppercase;letter-spacing:.04em}}
.tag.t1{{background:#fef3c7;color:#92400e}}
.tag.t2{{background:#dbeafe;color:#1e40af}}
.tag.staffing{{background:#fecaca;color:#991b1b}}
.card-title{{font-size:15px;font-weight:700;color:#f1f5f9;line-height:1.3;margin-bottom:3px}}
.card-company{{font-size:13px;color:#38bdf8;font-weight:600;margin-bottom:5px}}
.card-meta{{display:flex;gap:10px;font-size:11px;color:#94a3b8;margin-bottom:6px;flex-wrap:wrap}}
.salary{{color:#a78bfa;font-weight:600}}
.card-reason{{font-size:12px;color:#cbd5e1;line-height:1.4;margin-bottom:6px;padding:6px 10px;background:#0f172a;border-radius:8px}}
.card-risk{{font-size:11px;color:#f87171;margin-bottom:6px;padding:4px 10px;background:rgba(239,68,68,.08);border-radius:6px}}
.card-link{{display:inline-block;font-size:12px;color:#38bdf8;font-weight:600;padding:6px 0;text-decoration:none}}
</style>
</head>
<body>

<div class="header">
  <h1>NYC Compliance Discovery</h1>
  <div class="stats">
    <span>Scraped <b>{meta['total_scraped']}</b></span>
    <span>Tier 1 <b>{st}</b></span>
    <span>Tier 2 <b>{sb}</b></span>
    <span>Tier 3 <b>{sr}</b></span>
    <span>Rejected <b>{meta['rejected']}</b></span>
    <span>Suppressed <b>{meta['suppressed']}</b></span>
    <span>{meta['generated']}</span>
  </div>
</div>

<div class="filters">
  <button class="filter-btn active" onclick="flt('all',this)">All ({st+sb+sr})</button>
  <button class="filter-btn" onclick="flt('t1apply',this)">Tier 1 — Apply ({st})</button>
  <button class="filter-btn" onclick="flt('t2review',this)">Tier 2 — Review ({sb})</button>
  <button class="filter-btn" onclick="flt('t3low',this)">Tier 3 — Low ({sr})</button>
</div>

<div class="cards" id="cards">
{rows_html}
</div>

<script>
function flt(b,btn){{
  document.querySelectorAll('.filter-btn').forEach(x=>x.classList.remove('active'));
  btn.classList.add('active');
  document.querySelectorAll('.card').forEach(c=>{{
    if(b==='all'){{c.style.display='';return}}
    c.style.display=c.dataset.bucket===b?'':'none';
  }});
}}
</script>
</body>
</html>"""

    REPORT_OUT.write_text(html_content, encoding="utf-8")


# ═══════════════════════════════════════════════════════════════════════════════
# EXPORTS for nuke_and_discover.py
# ═══════════════════════════════════════════════════════════════════════════════

# Keep backward-compatible function names
def scrape_indeed_compat():
    jobs, _ = scrape_indeed()
    return jobs


if __name__ == "__main__":
    results = run_pipeline()
    m = results["meta"]
    a = results["audit"]
    print(f"\n{'='*60}")
    print(f"PIPELINE COMPLETE")
    print(f"  Scraped: {m['total_scraped']}")
    print(f"  Tier 1 (Apply):  {a['buckets'].get('Tier 1 — Apply Immediately', 0)}")
    print(f"  Tier 2 (Review): {a['buckets'].get('Tier 2 — Review & Apply', 0)}")
    print(f"  Tier 3 (Low):    {a['buckets'].get('Tier 3 — Low Priority', 0)}")
    print(f"  Below Threshold: {a['buckets'].get('Below Threshold', 0)}")
    print(f"  Rejected: {m['rejected']} | Suppressed: {m['deduped']}")
    print(f"{'='*60}")
