"""Export jobs from SQLite to static JSON with power ranking for GitHub Pages."""
from __future__ import annotations

import json
import re
import sqlite3
from datetime import datetime, timezone, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parent
DB = ROOT / "data" / "jobengine.sqlite"
DOCS = ROOT / "docs"

STALE_YEARS = re.compile(r"\b(2019|2020|2021|2022|2023|2024|2025)\b")
_INTERN_RE = re.compile(r"\b(?:stage|stagiaire|intern|internship)\b", re.I)
_ALTERNANCE_RE = re.compile(r"\b(?:alternance|apprenti)", re.I)
_CONTRACT_RE = re.compile(r"\b(?:CDD|temp|temporary|contract|seasonal)\b", re.I)
COMPANY_SUFFIX = re.compile(
    r",?\s*\b(Inc\.?|LLC|Ltd\.?|Corp\.?|Co\.?|S\.?A\.?|S\.?A\.?S\.?|"
    r"SE|GmbH|PLC|N\.?V\.?|AG|SAS|SARL|SpA|Pty|Limited)\s*$",
    re.I,
)
INTERNAL_RE = re.compile(
    r"\b(bridge upside|borderline bridge|real bridge|prettier slop|mixed case)\b", re.I
)
GENERIC_ONE_LINER = re.compile(r"^(Apply|Maybe|Skip):\s", re.I)

TOP_PICK_LANES = {"Paris Direction", "NYC Direction", "Money / Platform Leap"}
BRIDGE_LANES = {
    "Strategic Internship / Traineeship", "Interesting Stretch",
    "Miami Option", "Top-Brand Wrong-Function Risk",
}
TOP_WORLDS = {"Top Luxury / Culture World"}
ADJACENT_WORLDS = {"Real Adjacent World", "Premium But Generic World"}

# ============================================================
# COMPLIANCE TAB DETECTION
# ============================================================

COMPLIANCE_KEYWORDS = [
    "compliance", "regulatory", "aml", "kyc", "risk", "securities",
    "broker-dealer", "broker dealer", "licensing", "registration",
    "account opening", "onboarding", "finra", "financial crimes",
    "bsa", "anti-money", "sanctions", "trade surveillance",
    "operations analyst", "risk analyst",
]
COMPLIANCE_SOURCES = {"nyc_compliance"}

# Location filters for compliance tab
_NYC_RE = re.compile(r"\b(?:new york|nyc|manhattan|brooklyn|jersey city|stamford|hoboken)\b", re.I)
_MIAMI_RE = re.compile(r"\b(?:miami|fort lauderdale|boca raton|palm beach|south florida)\b", re.I)
_PARIS_RE = re.compile(r"\bparis\b", re.I)

# ============================================================
# COMPLIANCE SCORING ENGINE
# ============================================================

# --- Role fit (0-40) ---

_ROLE_FIT_TIER1 = re.compile(
    r"\b(?:compliance\s+(?:analyst|associate)"
    r"|aml\s+(?:analyst|associate|investigator)"
    r"|kyc\s+(?:analyst|associate)"
    r"|regulatory\s+(?:operations|compliance)\s+(?:analyst|associate|specialist)"
    r"|broker.dealer\s+compliance"
    r"|securities\s+(?:operations|compliance)\s+(?:analyst|associate)"
    r"|licensing\s+(?:analyst|specialist|coordinator)"
    r"|registration\s+(?:analyst|specialist)"
    r"|account\s+opening\s+(?:analyst|specialist)"
    r"|onboarding\s+(?:analyst|specialist)"
    r"|financial\s+crimes\s+(?:analyst|associate)"
    r"|bsa\s+(?:analyst|associate))\b",
    re.I,
)

_ROLE_FIT_TIER2 = re.compile(
    r"\b(?:compliance\s+(?:specialist|coordinator|officer|advisor)"
    r"|regulatory\s+(?:analyst|specialist|coordinator|associate)"
    r"|aml\s+(?:specialist|coordinator)"
    r"|kyc\s+(?:specialist|coordinator)"
    r"|trade\s+surveillance\s+(?:analyst|associate)"
    r"|risk\s+(?:analyst|associate)"
    r"|operations\s+(?:analyst|associate|specialist|coordinator)"
    r"|sanctions\s+(?:analyst|specialist))\b",
    re.I,
)

_ROLE_FIT_TIER3 = re.compile(
    r"\b(?:compliance|regulatory|aml|kyc|risk|sanctions|bsa"
    r"|financial\s+crimes|trade\s+surveillance|securities"
    r"|broker.dealer|licensing|registration|account\s+opening"
    r"|onboarding|finra|anti.money)\b",
    re.I,
)

_WEAK_FIT_RE = re.compile(
    r"\b(?:data\s+analyst|general\s+counsel|eeo\s+compliance|sustainability"
    r"|environmental\s+compliance|health\s+safety|osha|hipaa"
    r"|privacy\s+(?:analyst|officer)|it\s+compliance"
    r"|information\s+security|cyber\s*security|soc\s+analyst)\b",
    re.I,
)

def _role_fit_score(title: str, description: str) -> tuple[int, str]:
    """Score role fit 0-40. Returns (score, label)."""
    hay = f"{title} {description}".strip()
    if not hay:
        return 0, ""
    title_lower = title.lower()

    # Weak/off-target roles first
    if _WEAK_FIT_RE.search(title):
        return 8, "weak fit"

    # Tier 1: exact role match in title
    if _ROLE_FIT_TIER1.search(title):
        return 40, "strong match"

    # Tier 2: good role match in title
    if _ROLE_FIT_TIER2.search(title):
        return 32, "good match"

    # Tier 3: compliance keyword in title
    if _ROLE_FIT_TIER3.search(title):
        return 22, "related"

    # Compliance keywords only in description, not title
    if _ROLE_FIT_TIER3.search(description):
        return 12, "tangential"

    return 5, "weak signal"


# --- Company quality (0-30) ---

_TIER1_COMPANIES = {
    # Investment banks
    "goldman sachs", "morgan stanley", "jpmorgan", "jp morgan", "j.p. morgan",
    "citigroup", "citi", "citibank", "ubs", "barclays", "deutsche bank",
    "credit suisse", "mufg", "mizuho", "nomura", "bank of america", "bofa",
    "wells fargo", "hsbc", "bnp paribas", "societe generale",
    # Hedge funds
    "point72", "citadel", "bridgewater", "two sigma", "de shaw", "d.e. shaw",
    "millennium", "man group", "aqr", "elliott", "renaissance",
    "balyasny", "viking global", "marshall wace", "brevan howard",
    # PE / VC
    "blackstone", "kkr", "apollo", "carlyle", "warburg pincus",
    "tpg", "vista equity", "thoma bravo", "advent international",
    "general atlantic", "silver lake", "hellman & friedman",
    # Top asset managers
    "blackrock", "vanguard", "fidelity", "franklin templeton",
    "neuberger berman", "pimco", "invesco", "t. rowe price", "state street",
    "northern trust", "bny mellon", "lazard",
    # Top exchanges / market infra
    "ice", "intercontinental exchange", "nasdaq", "cboe", "cme group",
    "dtcc", "finra",
}

_TIER2_COMPANIES = {
    # Strong mid-market / fintechs
    "clear street", "drivewealth", "adyen", "webull", "robinhood",
    "interactive brokers", "schwab", "charles schwab", "ameriprise",
    "raymond james", "stifel", "lpl financial", "edward jones",
    "jefferies", "cowen", "piper sandler", "evercore", "houlihan lokey",
    "william blair", "oppenheimer", "canaccord",
    # Consulting / advisory
    "kroll", "accenture", "deloitte", "kpmg", "ey", "ernst & young", "pwc",
    "alvarez & marsal", "protiviti", "guidehouse",
    # Established banks
    "flagstar", "synchrony", "td bank", "td securities", "bbva",
    "citizens", "m&t bank", "regions", "keycorp", "fifth third",
    "pnc", "us bank", "usaa", "ally financial", "discover financial",
    "capital one", "goldman sachs bank",
    # Strong crypto / fintech
    "coinbase", "kraken", "gemini", "circle", "ripple", "stripe",
    "plaid", "sofi", "affirm", "chime", "paypal", "square", "block",
    # Asset management mid-tier
    "ares management", "oaktree", "pgim", "manulife", "sun life",
    "nuveen", "cohen & steers", "artisan partners", "loomis sayles",
    "man group", "schroders", "aberdeen",
}

_TIER4_KEYWORDS = re.compile(
    r"\b(?:staffing|recruiting|talent\s+(?:acquisition|solutions)|"
    r"search\s+partners|search\s+firm|manpower|adecco|robert\s+half|"
    r"randstad|hays|kelly\s+services|aston\s+carter|kforce|teksystems|"
    r"insight\s+global|beacon\s+hill|addison\s+group)\b",
    re.I,
)

_INSURANCE_RE = re.compile(
    r"\b(?:insurance|underwriting|actuarial|claims\s+(?:analyst|adjuster)|"
    r"allstate|geico|progressive|state\s+farm|liberty\s+mutual|travelers|"
    r"metlife|prudential\s+insurance|aetna|cigna|humana|anthem|"
    r"hartford|zurich\s+insurance|chubb\s+insurance)\b",
    re.I,
)


def _norm_company(name: str) -> str:
    out = (name or "").lower().strip()
    out = COMPANY_SUFFIX.sub("", out).strip().rstrip(",").strip()
    return out


def _words_match(needle: str, haystack: str) -> bool:
    """Check if needle appears as a word-boundary match in haystack, or vice versa."""
    # Exact match
    if needle == haystack:
        return True
    # needle is a multi-word phrase — check as substring but require word boundaries
    pattern = r"\b" + re.escape(needle) + r"\b"
    if re.search(pattern, haystack):
        return True
    # haystack inside needle (e.g. company name "ubs" matching "ubs group")
    if len(haystack) >= 3:
        pattern2 = r"\b" + re.escape(haystack) + r"\b"
        if re.search(pattern2, needle):
            return True
    return False


def _company_tier(company: str) -> tuple[int, str]:
    """Score company quality 0-30. Returns (score, tier_label)."""
    norm = _norm_company(company)
    if not norm:
        return 10, "unknown"

    # Tier 4 checks first (staffing / insurance) — these override everything
    if _TIER4_KEYWORDS.search(company):
        return 0, "staffing"
    if _INSURANCE_RE.search(company):
        return 0, "insurance"

    # Tier 1
    for t1 in _TIER1_COMPANIES:
        if _words_match(t1, norm):
            return 30, "tier 1"

    # Tier 2
    for t2 in _TIER2_COMPANIES:
        if _words_match(t2, norm):
            return 20, "tier 2"

    # Default: tier 3 (unknown but legitimate)
    return 10, "tier 3"


# --- Practical adjustments ---

_SENIOR_RE = re.compile(
    r"\b(?:senior|sr\.?|director|vp|vice\s+president|head\s+of|chief|principal|managing\s+director)\b",
    re.I,
)
_HIGH_EXP_RE = re.compile(
    r"\b(?:[5-9]|1[0-9]|20)\+?\s*(?:years?|yrs?)\s*(?:of\s+)?(?:experience|exp)\b",
    re.I,
)
_LEGAL_RE = re.compile(r"\b(?:general\s+counsel|attorney|paralegal|legal\s+counsel|juris\s+doctor)\b", re.I)
_SALES_RE = re.compile(r"\b(?:commission.based|cold\s+calling|sales\s+representative|business\s+development\s+rep)\b", re.I)
_SERIES7_RE = re.compile(r"\b(?:series\s*7|SIE|series\s*63|series\s*66|series\s*24)\b", re.I)
_ENTRY_RE = re.compile(r"\b(?:entry.level|junior|jr\.?|associate|analyst)\b", re.I)
_HYBRID_RE = re.compile(r"\bhybrid\b", re.I)


def _practical_adjustments(title: str, description: str, location: str, company: str) -> tuple[int, list[str]]:
    """Compute practical adjustment points and collect reason notes."""
    adj = 0
    notes = []
    hay = f"{title} {description}"

    if _SENIOR_RE.search(title):
        adj -= 15
        notes.append("senior title")

    if _HIGH_EXP_RE.search(hay):
        adj -= 15
        notes.append("5yr+ exp req")

    if _LEGAL_RE.search(title):
        adj -= 20
        notes.append("legal role")

    if _SALES_RE.search(hay):
        adj -= 20
        notes.append("sales/commission")

    if _INSURANCE_RE.search(f"{company} {title} {description}"):
        adj -= 10
        notes.append("insurance")

    if _SERIES7_RE.search(hay):
        adj += 10
        notes.append("mentions Series 7")

    if _ENTRY_RE.search(title):
        adj += 5
        notes.append("entry/associate level")

    if _HYBRID_RE.search(f"{location} {title} {description}"):
        adj += 3
        notes.append("hybrid")

    return adj, notes


def compliance_score(title: str, company: str, description: str, location: str) -> tuple[int, str]:
    """Full compliance scoring. Returns (score 0-100, reason_string)."""
    role_raw, role_label = _role_fit_score(title, description)
    company_raw, company_tier = _company_tier(company)
    adj_raw, adj_notes = _practical_adjustments(title, description, location, company)

    # Raw total: role(0-40) + company(0-30) + adj(-50 to +18) → range roughly -50 to 88
    raw_total = role_raw + company_raw + adj_raw
    # Scale to 0-100
    score = max(0, min(100, round(raw_total * 100 / 88)))

    # Build reason string
    parts = []
    company_clean = clean_company(company)

    # Role description
    title_lower = title.lower()
    if "compliance associate" in title_lower:
        parts.append("Compliance associate")
    elif "compliance analyst" in title_lower:
        parts.append("Compliance analyst")
    elif "aml" in title_lower and ("analyst" in title_lower or "investigator" in title_lower):
        parts.append("AML analyst")
    elif "kyc" in title_lower and ("analyst" in title_lower or "specialist" in title_lower):
        parts.append("KYC analyst")
    elif "broker" in title_lower and "compliance" in title_lower:
        parts.append("BD compliance")
    elif "securities" in title_lower and ("operations" in title_lower or "compliance" in title_lower):
        parts.append("Securities ops")
    elif "regulatory" in title_lower:
        parts.append("Regulatory")
    elif "trade surveillance" in title_lower:
        parts.append("Trade surveillance")
    elif "risk" in title_lower and ("analyst" in title_lower or "associate" in title_lower):
        parts.append("Risk analyst")
    elif "operations" in title_lower and ("analyst" in title_lower or "associate" in title_lower):
        parts.append("Operations")
    elif "financial crimes" in title_lower:
        parts.append("Financial crimes")
    elif "compliance" in title_lower:
        parts.append("Compliance")
    else:
        parts.append(role_label.capitalize())

    # Company + tier
    tier_desc = {
        "tier 1": "top-tier firm",
        "tier 2": "strong firm",
        "tier 3": "",
        "staffing": "staffing agency",
        "insurance": "insurance co",
        "unknown": "",
    }
    td = tier_desc.get(company_tier, "")
    if td:
        parts.append(f"at {company_clean}, {td}")
    elif company_clean:
        parts.append(f"at {company_clean}")

    # Location
    loc = location or ""
    if _NYC_RE.search(loc):
        arrangement = "hybrid" if _HYBRID_RE.search(loc) else "on-site"
        parts.append(f"{arrangement} NYC")
    elif _MIAMI_RE.search(loc):
        parts.append("Miami")
    elif _PARIS_RE.search(loc):
        parts.append("Paris")
    else:
        short_loc = loc.split(",")[0].strip()[:20]
        if short_loc:
            parts.append(short_loc)

    # Practical flags
    neg_notes = [n for n in adj_notes if n in ("senior title", "5yr+ exp req", "legal role", "sales/commission", "insurance")]
    if neg_notes:
        parts.append("stretch \u2014 " + ", ".join(neg_notes))

    pos_notes = [n for n in adj_notes if n in ("mentions Series 7", "entry/associate level")]
    if pos_notes:
        parts.extend(pos_notes)

    reason = ", ".join(parts)
    return score, reason


# ============================================================
# FASHION SCORING (unchanged)
# ============================================================

def power_score(ev: dict) -> float:
    ss = ev.get("signal_scores", {})
    ds = ev.get("dimension_scores", {})
    raw = (
        ss.get("direction", 0) * 0.40
        + ss.get("bridge", 0) * 0.25
        + ds.get("function", 0) * 0.15
        + ds.get("escape", 0) * 0.10
        + ds.get("practicality", 0) * 0.10
        - ss.get("risk", 0) * 0.20
    )
    return round(raw, 1)


# ============================================================
# SHARED HELPERS
# ============================================================

def detect_type(title: str) -> str:
    if _ALTERNANCE_RE.search(title or ""):
        return "Alternance"
    if _INTERN_RE.search(title or ""):
        return "Intern"
    if _CONTRACT_RE.search(title or ""):
        return "Contract"
    return "Full-Time"


def clean_company(name: str) -> str:
    out = (name or "").strip()
    out = COMPANY_SUFFIX.sub("", out).strip().rstrip(",").strip()
    return out or name or ""


def clean_one_liner(one_liner: str, why_surfaced: str, path_logic: str) -> str:
    if not one_liner:
        return path_logic or why_surfaced or ""
    if INTERNAL_RE.search(one_liner):
        return path_logic or why_surfaced or ""
    if GENERIC_ONE_LINER.match(one_liner) and len(one_liner) < 80:
        return path_logic or why_surfaced or ""
    return one_liner


def city_label(city_lane: str) -> str:
    if not city_lane or city_lane == "Unknown":
        return "Other"
    if city_lane.startswith("Paris"):
        return "Paris"
    if city_lane == "NYC":
        return "NYC"
    if city_lane == "Miami":
        return "Miami"
    return "Other"


def tier_label(classification: str) -> str:
    if classification in TOP_PICK_LANES:
        return "Top Pick"
    if classification in BRIDGE_LANES:
        return "Bridge"
    return "Pass"


def world_simple(world_tier: str) -> str:
    if world_tier in TOP_WORLDS:
        return "Top World"
    if world_tier in ADJACENT_WORLDS:
        return "Adjacent"
    return "Other"


def detect_tab(source: str, title: str, description: str, location: str,
               function_family: str, classification: str, risk_flags: list) -> str:
    """Classify job into 'compliance' or 'fashion' tab."""
    if source in COMPLIANCE_SOURCES:
        return "compliance"
    hay = f"{title} {function_family} {classification}".lower()
    for kw in COMPLIANCE_KEYWORDS:
        if kw in hay:
            # Also check location — compliance roles outside NYC/Miami/Paris stay in fashion
            loc = (location or "").lower()
            if _NYC_RE.search(loc) or _MIAMI_RE.search(loc) or _PARIS_RE.search(loc):
                return "compliance"
            # Compliance keyword but not in target cities — still show in compliance tab
            return "compliance"
    for flag in (risk_flags or []):
        f = str(flag).lower()
        if "compliance" in f or "analyst_lane" in f:
            return "compliance"
    # Check description for compliance keywords
    desc_lower = (description or "").lower()[:2000]
    for kw in COMPLIANCE_KEYWORDS[:8]:  # core keywords only
        if kw in desc_lower:
            title_lower = title.lower()
            # Only if title also has some signal
            if any(w in title_lower for w in ["analyst", "associate", "specialist", "coordinator",
                                                "officer", "investigator", "operations"]):
                return "compliance"
    return "fashion"


# ============================================================
# MAIN EXPORT
# ============================================================

def main():
    DOCS.mkdir(exist_ok=True)
    conn = sqlite3.connect(str(DB))
    conn.row_factory = sqlite3.Row

    rows = conn.execute(
        """
        SELECT
            j.job_id, j.fingerprint, j.source, j.company, j.title,
            j.location_text, j.remote_type,
            j.compensation_min, j.compensation_max, j.compensation_text,
            j.url, j.apply_url, j.created_at_utc, j.description_text,
            d.queue, d.decision_reason, d.confidence,
            d.evidence_json, d.decided_at_utc
        FROM decisions d
        JOIN jobs_canonical j ON d.job_id = j.job_id
        """
    ).fetchall()
    conn.close()

    print(f"Raw rows from DB: {len(rows)}")

    all_jobs = []
    comp_passed = 0
    comp_rejected = 0

    for row in rows:
        ev = {}
        try:
            ev = json.loads(row["evidence_json"] or "{}")
        except Exception:
            pass

        classification = ev.get("classification", row["decision_reason"] or "")
        title = row["title"] or ""
        company = row["company"] or ""
        location = row["location_text"] or ""
        description = row["description_text"] or ""
        source = row["source"] or ""

        tab = detect_tab(
            source, title, description, location,
            ev.get("function_family", ""), classification, ev.get("risk_flags", []),
        )

        # Fashion tab: use existing tier filter
        if tab == "fashion":
            tier = tier_label(classification)
            if tier == "Pass":
                continue

        # Compliance tab: apply compliance-specific filters instead
        if tab == "compliance":
            # Hard reject: pure legal, sales, insurance-primary
            if _LEGAL_RE.search(title) and not _ROLE_FIT_TIER3.search(title):
                comp_rejected += 1
                continue
            if _SALES_RE.search(title):
                comp_rejected += 1
                continue
            comp_passed += 1

        world_raw = ev.get("world_tier", "")
        ws = world_simple(world_raw)
        why_surfaced = ev.get("why_surfaced", "")
        path_logic_raw = ev.get("path_logic", "")
        one_liner_raw = ev.get("one_line_recommendation", "")

        # Score differently per tab
        if tab == "compliance":
            comp_sc, comp_reason = compliance_score(title, company, description, location)
            ps = comp_sc
            one_liner_final = comp_reason
        else:
            ps = power_score(ev)
            one_liner_final = clean_one_liner(one_liner_raw, why_surfaced, path_logic_raw)

        tier_val = tier_label(classification) if tab == "fashion" else "Compliance"

        all_jobs.append({
            "job_id": row["job_id"],
            "fingerprint": row["fingerprint"],
            "source": source,
            "company": company,
            "company_clean": clean_company(company),
            "title": title,
            "location": location,
            "url": row["url"] or row["apply_url"] or "",
            "created_at": row["created_at_utc"] or "",
            "tier": tier_val,
            "lane": classification,
            "city": city_label(ev.get("city_lane", "")),
            "city_lane": ev.get("city_lane", "Unknown"),
            "world_simple": ws,
            "world_tier": world_raw,
            "function_family": ev.get("function_family", ""),
            "type": detect_type(title),
            "score": round(ps),
            "power_score": float(ps),
            "one_liner": one_liner_final,
            "path_logic": path_logic_raw,
            "main_risk": ev.get("main_risk", ""),
            "compensation": ev.get("comp_record", {}).get("comp_text_raw", "") or row["compensation_text"] or "",
            "dimension_scores": ev.get("dimension_scores", {}),
            "signal_scores": ev.get("signal_scores", {}),
            "tab": tab,
            "work_arrangement": ev.get("work_arrangement", ""),
            "_created": row["created_at_utc"] or "",
        })

    print(f"After tier/compliance filter: {len(all_jobs)} (compliance passed: {comp_passed}, rejected: {comp_rejected})")

    # --- DEDUPE ---
    groups: dict[tuple[str, str], list[dict]] = {}
    for job in all_jobs:
        key = (job["company"].lower().strip(), job["title"].lower().strip())
        groups.setdefault(key, []).append(job)

    deduped = []
    for group in groups.values():
        group.sort(key=lambda j: j["_created"] or "", reverse=True)
        deduped.append(group[0])

    print(f"After dedupe: {len(deduped)} (removed {len(all_jobs) - len(deduped)} dupes)")

    # --- DATE FILTER ---
    cutoff = (datetime.now(timezone.utc) - timedelta(days=60)).isoformat(timespec="seconds")
    before = len(deduped)
    filtered = []
    for job in deduped:
        if STALE_YEARS.search(job["title"]):
            if job["_created"] >= cutoff:
                filtered.append(job)
        else:
            filtered.append(job)

    print(f"After date filter: {len(filtered)} (removed {before - len(filtered)} expired)")

    # --- SORT & RANK (separate per tab) ---
    compliance_jobs = [j for j in filtered if j["tab"] == "compliance"]
    fashion_jobs = [j for j in filtered if j["tab"] == "fashion"]

    compliance_jobs.sort(key=lambda j: -j["power_score"])
    fashion_jobs.sort(key=lambda j: -j["power_score"])

    for i, job in enumerate(compliance_jobs):
        job["rank"] = i + 1
    for i, job in enumerate(fashion_jobs):
        job["rank"] = i + 1

    all_final = compliance_jobs + fashion_jobs
    for job in all_final:
        del job["_created"]

    # --- COUNTS ---
    tab_counts: dict[str, int] = {}
    city_counts: dict[str, int] = {}
    type_counts: dict[str, int] = {}
    for job in all_final:
        tab_counts[job["tab"]] = tab_counts.get(job["tab"], 0) + 1
        city_counts[job["city"]] = city_counts.get(job["city"], 0) + 1
        type_counts[job["type"]] = type_counts.get(job["type"], 0) + 1

    # --- WRITE ---
    (DOCS / "jobs.json").write_text(
        json.dumps(all_final, ensure_ascii=False, indent=1), encoding="utf-8"
    )
    meta = {
        "export_date": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "total_jobs": len(all_final),
        "jobs_by_tab": tab_counts,
        "jobs_by_city": city_counts,
        "jobs_by_type": type_counts,
    }
    (DOCS / "meta.json").write_text(
        json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8"
    )

    print(f"\nExported {len(all_final)} jobs\n")
    print("=== TAB ===")
    for k, v in sorted(tab_counts.items()):
        print(f"  {k}: {v}")
    print("\n=== CITY ===")
    for k, v in sorted(city_counts.items(), key=lambda x: -x[1]):
        print(f"  {k}: {v}")
    print("\n=== TYPE ===")
    for k, v in sorted(type_counts.items(), key=lambda x: -x[1]):
        print(f"  {k}: {v}")

    print(f"\n=== TOP 20 COMPLIANCE ===")
    for job in compliance_jobs[:20]:
        print(
            f"  #{job['rank']:>3} s={job['score']:3d} | {job['company_clean'][:25]:<25s} | {job['title'][:50]:<50s} | {job['one_liner'][:60]}"
        )

    print(f"\n=== TOP 20 FASHION ===")
    for job in fashion_jobs[:20]:
        print(
            f"  #{job['rank']:>3} s={job['score']:3d} | {job['company_clean'][:25]:<25s} | {job['title'][:55]}"
        )


if __name__ == "__main__":
    main()
