"""Export jobs from SQLite to static JSON for GitHub Pages.

Two completely separate scoring engines:
  NYC Compliance Lane: money/platform/prestige. Compliance identity is the goal.
  Paris Fashion Lane:  direction change into luxury/fashion/art/culture world.
"""
from __future__ import annotations

import json
import re
import sqlite3
from datetime import datetime, timezone, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parent
DB = ROOT / "data" / "jobengine.sqlite"
DOCS = ROOT / "docs"

# ============================================================
# SHARED CONSTANTS & HELPERS
# ============================================================

STALE_YEARS = re.compile(r"\b(2019|2020|2021|2022|2023|2024|2025)\b")
_INTERN_RE = re.compile(r"\b(?:stage|stagiaire|intern|internship)\b", re.I)
_ALTERNANCE_RE = re.compile(r"\b(?:alternance|apprenti)", re.I)
_CONTRACT_RE = re.compile(r"\b(?:CDD|temp|temporary|contract|seasonal)\b", re.I)
_CDI_RE = re.compile(r"\bCDI\b")
COMPANY_SUFFIX = re.compile(
    r",?\s*\b(Inc\.?|LLC|Ltd\.?|Corp\.?|Co\.?|S\.?A\.?|S\.?A\.?S\.?|"
    r"SE|GmbH|PLC|N\.?V\.?|AG|SAS|SARL|SpA|Pty|Limited)\s*$", re.I,
)
INTERNAL_RE = re.compile(r"\b(bridge upside|borderline bridge|real bridge|prettier slop|mixed case)\b", re.I)
GENERIC_ONE_LINER = re.compile(r"^(Apply|Maybe|Skip):\s", re.I)
_NYC_RE = re.compile(r"\b(?:new york|nyc|manhattan|brooklyn|jersey city|stamford|hoboken)\b", re.I)
_MIAMI_RE = re.compile(r"\b(?:miami|fort lauderdale|boca raton|palm beach|south florida)\b", re.I)
_PARIS_RE = re.compile(r"\bparis\b", re.I)
_HYBRID_RE = re.compile(r"\bhybrid\b", re.I)
_SENIOR_RE = re.compile(r"\b(?:senior|sr\.?|director|vp|vice\s+president|head\s+of|chief|principal|managing\s+director|lead)\b", re.I)
_HIGH_EXP_RE = re.compile(r"\b(?:[5-9]|1[0-9]|20)\+?\s*(?:years?|yrs?)\s*(?:of\s+)?(?:experience|exp)\b", re.I)
_LEGAL_RE = re.compile(r"\b(?:general\s+counsel|attorney|paralegal|legal\s+counsel|juris\s+doctor|counsel)\b", re.I)
_SALES_RE = re.compile(r"\b(?:commission.based|cold\s+calling|sales\s+representative|business\s+development\s+rep)\b", re.I)
_SERIES7_RE = re.compile(r"\b(?:series\s*7|SIE|series\s*63|series\s*66|series\s*24)\b", re.I)
_ENTRY_RE = re.compile(r"\b(?:entry.level|junior|jr\.?)\b", re.I)
_BILINGUAL_RE = re.compile(r"\b(?:bilingual|japanese|mandarin|cantonese|korean|spanish\s+required|fluent\s+(?:spanish|portuguese|arabic|hindi))\b", re.I)
_INSURANCE_RE = re.compile(
    r"\b(?:insurance|underwriting|actuarial|claims\s+(?:analyst|adjuster)|"
    r"allstate|geico|progressive|state\s+farm|liberty\s+mutual|travelers|"
    r"metlife|prudential\s+insurance|aetna|cigna|humana|anthem|"
    r"hartford|zurich\s+insurance|chubb\s+insurance)\b", re.I,
)
_STAFFING_RE = re.compile(
    r"\b(?:staffing|recruiting|talent\s+(?:acquisition|solutions)|"
    r"search\s+partners|search\s+firm|manpower|adecco|robert\s+half|"
    r"randstad|hays|kelly\s+services|aston\s+carter|kforce|teksystems|"
    r"insight\s+global|beacon\s+hill|addison\s+group|phaxis|collabera|"
    r"apex\s+systems|prokatchers|cynet\s+systems|nextgen)\b", re.I,
)
_COMP_RE = re.compile(r"\$\s*(\d[\d,]*)\s*k?", re.I)
_COMP_K_RE = re.compile(r"\$\s*(\d+)\s*[kK]", re.I)
TOP_PICK_LANES = {"Paris Direction", "NYC Direction", "Money / Platform Leap"}
BRIDGE_LANES = {"Strategic Internship / Traineeship", "Interesting Stretch", "Miami Option", "Top-Brand Wrong-Function Risk"}

COMPLIANCE_KEYWORDS = [
    "compliance", "regulatory", "aml", "kyc", "risk analyst", "risk associate",
    "securities", "broker-dealer", "broker dealer", "licensing", "registration",
    "account opening", "onboarding", "finra", "financial crimes",
    "bsa", "anti-money", "sanctions", "trade surveillance",
]


def _norm(name: str) -> str:
    out = (name or "").lower().strip()
    out = COMPANY_SUFFIX.sub("", out).strip().rstrip(",").strip()
    return out


def _words_match(needle: str, haystack: str) -> bool:
    if needle == haystack:
        return True
    if re.search(r"\b" + re.escape(needle) + r"\b", haystack):
        return True
    if len(haystack) >= 3 and re.search(r"\b" + re.escape(haystack) + r"\b", needle):
        return True
    return False


def clean_company(name: str) -> str:
    out = (name or "").strip()
    out = COMPANY_SUFFIX.sub("", out).strip().rstrip(",").strip()
    return out or name or ""


def detect_type(title: str) -> str:
    if _ALTERNANCE_RE.search(title or ""): return "Alternance"
    if _INTERN_RE.search(title or ""): return "Intern"
    if _CONTRACT_RE.search(title or ""): return "Contract"
    return "Full-Time"


def city_label(city_lane: str) -> str:
    if not city_lane or city_lane == "Unknown": return "Other"
    if city_lane.startswith("Paris"): return "Paris"
    if city_lane == "NYC": return "NYC"
    if city_lane == "Miami": return "Miami"
    return "Other"


def extract_comp_from_text(title: str, desc: str) -> str:
    """Try to extract salary from title or first 500 chars of description."""
    for text in [title, (desc or "")[:500]]:
        m = _COMP_K_RE.search(text)
        if m:
            val = int(m.group(1))
            if 30 <= val <= 500:
                return f"${val}K"
        m = _COMP_RE.search(text)
        if m:
            val = int(m.group(1).replace(",", ""))
            if 30000 <= val <= 500000:
                return f"${val // 1000}K"
    return ""


# ============================================================
# TAB ASSIGNMENT
# ============================================================

# Luxury/fashion world companies for Paris lane
_LUXURY_COMPANIES = {
    "chanel", "dior", "christian dior", "christian dior couture",
    "hermes", "louis vuitton", "cartier", "van cleef", "saint laurent",
    "balenciaga", "balmain", "givenchy", "celine", "loewe",
    "bottega veneta", "fendi", "prada", "gucci", "valentino",
    "alexander mcqueen", "berluti", "schiaparelli", "loro piana",
    "sephora", "kering", "lvmh", "richemont",
    "chloe", "polene", "sezane", "zimmermann", "roger vivier", "moynat",
    "parfums christian dior", "remy cointreau", "interparfums",
    "maison crivelli", "vestiaire collective",
    "christie", "christies", "sotheby", "sothebys",
    "centre pompidou", "fondation", "musee",
}

_LUXURY_WORLD_RE = re.compile(
    r"\b(?:luxury|luxe|fashion|mode|beauty|beaute|beaut[e\u00e9]|jewelry|jewellery|joaillerie"
    r"|gallery|galerie|auction|ench[e\u00e8]res|art\b|culture|cultural|foundation|fondation"
    r"|maison|boutique|haute\s+couture)\b", re.I,
)

_DIRECTION_FUNCTION_RE = re.compile(
    r"\b(?:chef\s+de\s+projet|project\s+(?:coordinator|assistant|manager)"
    r"|communication(?:s)?\s+(?:assistant|coordinator)"
    r"|event(?:s)?\s+(?:coordinator|assistant)"
    r"|content\s+(?:production|coordinator)"
    r"|visual\s+merchandising|vm\s+coordinator"
    r"|gallery\s+(?:coordinator|assistant)"
    r"|auction\s+(?:operations|coordinator)"
    r"|production\s+(?:assistant|coordinator)"
    r"|brand\s+(?:coordinator|assistant)"
    r"|studio\s+(?:coordinator|assistant))\b", re.I,
)


def _is_luxury_company(company: str) -> bool:
    norm = _norm(company)
    for lux in _LUXURY_COMPANIES:
        if _words_match(lux, norm):
            return True
    return False


def assign_tab(source: str, title: str, company: str, description: str,
               location: str, function_family: str, classification: str,
               risk_flags: list, world_tier: str) -> str:
    """Assign job to 'compliance' or 'fashion' tab."""
    title_lower = (title or "").lower()
    hay = f"{title} {function_family} {classification}".lower()

    # 1. If it's a compliance/risk/AML role at a NON-luxury company → compliance
    has_comp_signal = False
    for kw in COMPLIANCE_KEYWORDS:
        if kw in hay:
            has_comp_signal = True
            break
    if not has_comp_signal:
        for flag in (risk_flags or []):
            if "compliance" in str(flag).lower() or "analyst_lane" in str(flag).lower():
                has_comp_signal = True
                break

    if has_comp_signal:
        # If the company is a luxury house, keep in fashion (or both — we'll add to fashion)
        if _is_luxury_company(company):
            # Compliance at luxury house — fashion tab (it's about the direction context)
            # BUT only if the function is compliance-specific, not project/coordination
            if _DIRECTION_FUNCTION_RE.search(title):
                return "fashion"
            # It's compliance function at a luxury house — put in compliance
            # (user can see it there, the company name makes it interesting)
            return "compliance"
        return "compliance"

    # 2. Source-based
    if source == "nyc_compliance":
        return "compliance"

    # 3. If company is luxury/fashion/art/culture → fashion
    if _is_luxury_company(company):
        return "fashion"

    # 4. If world_tier indicates luxury/fashion
    wt = (world_tier or "").lower()
    if "luxury" in wt or "culture" in wt or "adjacent" in wt:
        return "fashion"

    # 5. If function/title is direction-change (creative/coordination/project at any company)
    if _LUXURY_WORLD_RE.search(f"{title} {company} {description[:300]}"):
        if _DIRECTION_FUNCTION_RE.search(title):
            return "fashion"

    # 6. Fallback: use the existing classification
    if classification in TOP_PICK_LANES or classification in BRIDGE_LANES:
        return "fashion"

    # If nothing matched and it has compliance signal from description only
    desc_lower = (description or "").lower()[:1500]
    for kw in COMPLIANCE_KEYWORDS[:6]:
        if kw in desc_lower:
            if any(w in title_lower for w in ["analyst", "associate", "specialist",
                                                "officer", "investigator", "operations"]):
                return "compliance"

    return "fashion"


# ============================================================
# NYC COMPLIANCE SCORING ENGINE
# ============================================================

# --- NYC Title fit (0-35) ---
_NYC_TITLE_T1 = re.compile(
    r"\b(?:compliance\s+(?:analyst|associate)"
    r"|aml\s+(?:analyst|associate|investigator)"
    r"|kyc\s+(?:analyst|associate))\b", re.I)
_NYC_TITLE_T2 = re.compile(
    r"\b(?:regulatory\s+(?:operations|compliance)\s+(?:analyst|associate|specialist)"
    r"|broker.dealer\s+compliance"
    r"|securities\s+(?:operations|compliance)"
    r"|licensing\s+(?:analyst|specialist)"
    r"|registration\s+(?:analyst|specialist))\b", re.I)
_NYC_TITLE_T3 = re.compile(
    r"\b(?:risk\s+(?:analyst|associate)"
    r"|operations\s+(?:analyst|associate)"
    r"|financial\s+crimes\s+(?:analyst|associate)"
    r"|trade\s+surveillance\s+(?:analyst|associate)"
    r"|sanctions\s+(?:analyst|specialist)"
    r"|bsa\s+(?:analyst|associate))\b", re.I)
_NYC_TITLE_T4 = re.compile(
    r"\b(?:onboarding\s+(?:analyst|specialist|coordinator)"
    r"|account\s+opening\s+(?:analyst|specialist)"
    r"|client\s+onboarding)\b", re.I)
_NYC_TITLE_WEAK = re.compile(
    r"\b(?:data\s+analyst|general\s+counsel|eeo\s+compliance|sustainability"
    r"|environmental|health\s+safety|osha|hipaa|privacy|it\s+compliance"
    r"|information\s+security|cyber|soc\s+analyst)\b", re.I)

# --- NYC Company prestige (0-35) ---
_NYC_T1 = {
    "goldman sachs", "morgan stanley", "jpmorgan", "jp morgan", "j.p. morgan",
    "citigroup", "citi", "citibank", "ubs", "barclays", "deutsche bank",
    "credit suisse", "mufg", "mizuho", "nomura", "bank of america", "bofa",
    "hsbc", "bnp paribas", "societe generale", "wells fargo",
    "point72", "citadel", "bridgewater", "two sigma", "de shaw", "d.e. shaw",
    "millennium", "man group", "aqr", "renaissance", "elliott",
    "balyasny", "viking global", "marshall wace", "brevan howard",
    "blackstone", "kkr", "apollo", "carlyle", "warburg pincus", "ares",
    "tpg", "bain capital", "advent international", "general atlantic",
    "blackrock", "vanguard", "fidelity", "pimco", "franklin templeton",
    "neuberger berman", "t. rowe price", "wellington", "invesco",
    "state street", "northern trust", "bny mellon", "lazard",
    "ice", "intercontinental exchange", "nasdaq", "cboe", "cme group", "dtcc",
}
_NYC_T2 = {
    "jefferies", "evercore", "cowen", "piper sandler", "william blair",
    "stifel", "raymond james", "rbc", "scotia", "bmo", "houlihan lokey",
    "clear street", "virtu", "jane street", "drw", "susquehanna", "jump trading",
    "obra capital", "rialto capital",
    "adyen", "stripe", "coinbase", "robinhood", "webull", "interactive brokers",
    "drivewealth", "plaid", "sofi", "affirm",
    "td bank", "td securities", "flagstar", "synchrony", "bbva",
    "pnc", "fifth third", "keycorp", "citizens", "m&t bank",
    "capital one", "ally financial", "discover financial",
    "kroll", "accenture", "deloitte", "kpmg", "ey", "ernst & young", "pwc",
    "alvarez & marsal", "protiviti", "guidehouse",
    "finra", "schwab", "charles schwab",
}
_NYC_T3 = {
    "peloton", "greystone", "capgemini", "google", "amazon",
    "kraken", "gemini", "circle", "ripple", "block",
    "paypal", "chime", "betterment", "wealthfront",
    "oaktree", "pgim", "nuveen", "cohen & steers",
    "oppenheimer", "canaccord", "lpl financial",
    "ameriprise", "edward jones",
    "usaa", "us bank",
}


def _nyc_title_fit(title: str, desc: str) -> tuple[int, str]:
    if _NYC_TITLE_WEAK.search(title):
        return 5, "weak fit"
    if _SENIOR_RE.search(title):
        return 10, "senior stretch"
    if _BILINGUAL_RE.search(title):
        return 10, "language req"
    if _NYC_TITLE_T1.search(title):
        return 35, ""
    if _NYC_TITLE_T2.search(title):
        return 30, ""
    if _NYC_TITLE_T3.search(title):
        return 25, ""
    if _NYC_TITLE_T4.search(title):
        return 22, ""
    # Generic compliance keyword in title
    title_l = title.lower()
    if "compliance" in title_l:
        return 20, ""
    if any(kw in title_l for kw in ["aml", "kyc", "regulatory", "risk"]):
        return 18, ""
    return 8, "tangential"


def _nyc_company_prestige(company: str) -> tuple[int, str]:
    norm = _norm(company)
    if not norm:
        return 8, "unknown"
    if _STAFFING_RE.search(company):
        return 0, "staffing agency"
    if _INSURANCE_RE.search(company):
        return 0, "insurance"
    for c in _NYC_T1:
        if _words_match(c, norm):
            return 35, "bulge bracket" if any(w in norm for w in ["goldman", "morgan stanley", "jpmorgan", "citi", "barclays", "ubs", "deutsche", "hsbc", "bnp", "bofa", "bank of america", "wells fargo", "nomura", "mizuho", "mufg", "credit suisse"]) else "top-tier firm"
    for c in _NYC_T2:
        if _words_match(c, norm):
            return 25, "strong firm"
    for c in _NYC_T3:
        if _words_match(c, norm):
            return 15, "solid firm"
    return 8, ""


def nyc_score(title: str, company: str, desc: str, location: str, comp_text: str) -> tuple[int, str, str]:
    """NYC compliance scoring. Returns (score 0-100, reason, risk)."""
    title_pts, title_flag = _nyc_title_fit(title, desc)
    company_pts, company_label = _nyc_company_prestige(company)
    hay = f"{title} {desc}"

    # Practical adjustments
    adj = 0
    risks = []

    if _HIGH_EXP_RE.search(hay):
        adj -= 20
        risks.append("5yr+ experience likely required")
    if _SENIOR_RE.search(title):
        adj -= 15
        risks.append("senior title -- expects 3-5yr experience")
    if _LEGAL_RE.search(title):
        adj -= 25
        risks.append("legal/counsel role, not compliance")
    if _SALES_RE.search(hay):
        adj -= 25
        risks.append("commission/sales-based")
    if _INSURANCE_RE.search(f"{company} {title}"):
        adj -= 15
        risks.append("insurance company, not finance")
    if _BILINGUAL_RE.search(f"{title} {desc[:500]}"):
        adj -= 25
        m = _BILINGUAL_RE.search(f"{title} {desc[:500]}")
        risks.append(f"requires {m.group(0).lower()}")
    if _SERIES7_RE.search(hay):
        adj += 10
    if _ENTRY_RE.search(title):
        adj += 5
    if _HYBRID_RE.search(f"{location} {title}"):
        adj += 3

    # Salary check
    comp = comp_text or extract_comp_from_text(title, desc)
    comp_val = 0
    m = _COMP_K_RE.search(comp)
    if m:
        comp_val = int(m.group(1)) * 1000
    else:
        m = _COMP_RE.search(comp)
        if m:
            comp_val = int(m.group(1).replace(",", ""))
    if comp_val >= 100000:
        adj += 5
    elif comp_val > 0 and comp_val < 60000:
        adj -= 10
        risks.append(f"low salary (${comp_val // 1000}K)")

    if title_flag == "staffing agency":
        risks.append("staffing agency posting, actual employer unclear")
    elif company_label == "staffing agency":
        risks.append("staffing agency posting, actual employer unclear")
    elif company_label == "insurance":
        if "insurance company, not finance" not in risks:
            risks.append("insurance company, not finance")

    raw = title_pts + company_pts + adj
    score = max(0, min(100, round(raw * 100 / 70)))

    # Build reason
    parts = []
    title_l = title.lower()
    if "compliance associate" in title_l: parts.append("Compliance associate")
    elif "compliance analyst" in title_l: parts.append("Compliance analyst")
    elif "aml" in title_l and any(w in title_l for w in ["analyst", "investigator", "associate"]): parts.append("AML analyst")
    elif "kyc" in title_l and any(w in title_l for w in ["analyst", "associate", "specialist"]): parts.append("KYC analyst")
    elif "broker" in title_l and "compliance" in title_l: parts.append("BD compliance")
    elif "securities" in title_l: parts.append("Securities ops")
    elif "regulatory" in title_l: parts.append("Regulatory")
    elif "trade surveillance" in title_l: parts.append("Trade surveillance")
    elif "risk" in title_l and any(w in title_l for w in ["analyst", "associate"]): parts.append("Risk analyst")
    elif "onboarding" in title_l or "account opening" in title_l: parts.append("Client onboarding")
    elif "financial crimes" in title_l: parts.append("Financial crimes")
    elif "compliance" in title_l: parts.append("Compliance")
    elif "operations" in title_l: parts.append("Operations")
    else: parts.append("Compliance-adjacent")

    cc = clean_company(company)
    if company_label and company_label not in ("", "unknown", "staffing agency", "insurance"):
        parts.append(f"at {cc}, {company_label}")
    elif cc:
        parts.append(f"at {cc}")

    loc = location or ""
    if _NYC_RE.search(loc):
        arr = "hybrid" if _HYBRID_RE.search(loc) else "on-site"
        parts.append(f"{arr} NYC")
    elif _MIAMI_RE.search(loc):
        parts.append("Miami")
    elif _PARIS_RE.search(loc):
        parts.append("Paris")
    else:
        short = loc.split(",")[0].strip()[:25]
        if short:
            parts.append(short)

    if _SERIES7_RE.search(hay):
        parts.append("mentions Series 7")

    reason = ", ".join(parts)
    risk = "; ".join(risks) if risks else ""
    return score, reason, risk


# ============================================================
# PARIS FASHION & DIRECTION SCORING ENGINE
# ============================================================

_PARIS_BRAND_T1 = {
    "chanel", "dior", "christian dior", "christian dior couture",
    "hermes", "louis vuitton", "cartier", "van cleef",
    "saint laurent", "balenciaga", "balmain", "givenchy", "celine",
    "loewe", "bottega veneta", "fendi", "prada", "gucci", "valentino",
    "alexander mcqueen", "berluti", "schiaparelli", "loro piana",
}
_PARIS_BRAND_T2 = {
    "sephora", "kering", "lvmh", "richemont",
    "chloe", "polene", "sezane", "zimmermann", "roger vivier", "moynat",
    "parfums christian dior", "remy cointreau", "interparfums",
    "maison crivelli",
}
_PARIS_BRAND_T3 = {
    "vestiaire collective", "christie", "christies", "sotheby", "sothebys",
    "centre pompidou", "fondation louis vuitton", "palais de tokyo",
    "musee", "galerie",
}

_PARIS_FUNC_T1 = re.compile(
    r"\b(?:chef\s+de\s+projet|project\s+(?:coordinator|assistant)"
    r"|production\s+(?:assistant|coordinator)"
    r"|content\s+(?:coordinator|production)"
    r"|coordination)\b", re.I)
_PARIS_FUNC_T2 = re.compile(
    r"\b(?:communication(?:s)?\s+(?:assistant|coordinator)"
    r"|event(?:s)?\s+(?:coordinator|assistant)"
    r"|visual\s+merchandising|vm\s+coordinator"
    r"|studio\s+(?:coordinator|assistant)"
    r"|gallery\s+(?:coordinator|assistant)"
    r"|auction\s+(?:operations|coordinator)"
    r"|creative\s+(?:operations|coordinator))\b", re.I)
_PARIS_FUNC_T3 = re.compile(
    r"\b(?:marketing\s+(?:assistant|coordinator)"
    r"|brand\s+(?:coordinator|assistant|strategy)"
    r"|pr\s+(?:assistant|coordinator)"
    r"|influence|relations\s+(?:presse|publiques))\b", re.I)
_PARIS_COMPLIANCE_FUNC = re.compile(
    r"\b(?:compliance|conformit|aml|kyc|lcb|risque|contr.le\s+interne|audit\s+interne)\b", re.I)
_PARIS_RETAIL_RE = re.compile(r"\b(?:vendeur|vendeuse|sales\s+associate|retail\s+sales|cashier|caissier)\b", re.I)
_FRENCH_HEAVY_RE = re.compile(r"\b(?:excellente?\s+ma.trise\s+du?\s+fran.ais|fran.ais\s+(?:courant|natif|maternel)|niveau\s+c[12]\s+(?:en\s+)?fran.ais|r.daction|press\s+attach|attach.e?\s+de\s+presse|copywriter|r.dacteur|concepteur.r.dacteur)\b", re.I)
_ENGLISH_POSTING_RE = re.compile(r"\b(?:we\s+are\s+looking|you\s+will|the\s+role|responsibilities|requirements|about\s+the\s+role|what\s+you|join\s+(?:us|our))\b", re.I)


def _paris_brand_score(company: str, world_tier: str) -> tuple[int, str]:
    norm = _norm(company)
    for b in _PARIS_BRAND_T1:
        if _words_match(b, norm):
            return 40, "top maison"
    for b in _PARIS_BRAND_T2:
        if _words_match(b, norm):
            return 32, "strong luxury brand"
    for b in _PARIS_BRAND_T3:
        if _words_match(b, norm):
            return 22, "respected institution"
    # Fall back to world_tier
    wt = (world_tier or "").lower()
    if "luxury" in wt or "culture" in wt:
        return 18, "luxury-adjacent"
    if "adjacent" in wt or "premium" in wt:
        return 12, "premium brand"
    return 5, ""


def _paris_role_score(title: str, desc: str) -> tuple[int, str]:
    if _PARIS_RETAIL_RE.search(title):
        return 8, "retail sales"
    if _PARIS_COMPLIANCE_FUNC.search(title):
        return 12, "compliance function"
    if _PARIS_FUNC_T1.search(title):
        return 35, "project/coordination"
    if _PARIS_FUNC_T2.search(title):
        return 32, "creative/events/VM"
    if _PARIS_FUNC_T3.search(title):
        return 28, "marketing/brand/PR"
    return 15, ""


def paris_score(title: str, company: str, desc: str, location: str,
                world_tier: str, comp_text: str) -> tuple[int, str, str]:
    """Paris fashion scoring. Returns (score 0-100, reason, risk)."""
    brand_pts, brand_label = _paris_brand_score(company, world_tier)
    role_pts, role_label = _paris_role_score(title, desc)
    adj = 0
    risks = []

    # Language risk
    desc_sample = (desc or "")[:2000]
    if _FRENCH_HEAVY_RE.search(f"{title} {desc_sample}"):
        adj -= 15
        risks.append("requires strong written French")
    elif _ENGLISH_POSTING_RE.search(desc_sample):
        adj += 5
    # Press/copywriter in title
    if re.search(r"\b(?:r.dacteur|copywriter|press\s+attach|attach.e?\s+de\s+presse)\b", title, re.I):
        adj -= 20
        risks.append("written French comms role")

    # Contract type
    title_l = title.lower()
    if _CDI_RE.search(title): adj += 10
    elif _CONTRACT_RE.search(title): adj += 5
    elif _ALTERNANCE_RE.search(title): adj += 0
    elif _INTERN_RE.search(title): adj += 0
    elif "freelance" in title_l: adj -= 5

    # Compliance at luxury house risk
    if _PARIS_COMPLIANCE_FUNC.search(title) and brand_pts >= 32:
        risks.append("compliance function at luxury house -- better wallpaper, same identity")

    # Retail risk
    if _PARIS_RETAIL_RE.search(title):
        risks.append("retail sales role dressed up by brand name")

    raw = brand_pts + role_pts + adj
    score = max(0, min(100, round(raw * 100 / 75)))

    # Build reason
    parts = []
    cc = clean_company(company)

    if role_label == "project/coordination":
        if "chef de projet" in title_l: parts.append(f"{cc} chef de projet")
        else: parts.append(f"{cc} project coordination")
    elif role_label == "creative/events/VM":
        if "visual merchandising" in title_l or "vm" in title_l: parts.append(f"{cc} visual merchandising")
        elif "event" in title_l: parts.append(f"{cc} events coordination")
        elif "gallery" in title_l: parts.append(f"{cc} gallery")
        elif "studio" in title_l: parts.append(f"{cc} studio coordination")
        else: parts.append(f"{cc} {role_label}")
    elif role_label == "marketing/brand/PR":
        parts.append(f"{cc} brand/marketing")
    elif role_label == "compliance function":
        parts.append(f"Compliance at {cc}")
    elif role_label == "retail sales":
        parts.append(f"Retail at {cc}")
    else:
        parts.append(f"{cc}" if cc else "Unknown brand")

    if brand_label:
        parts.append(brand_label)

    typ = detect_type(title)
    if typ == "Alternance": parts.append("alternance")
    elif typ == "Intern": parts.append("stage")
    elif typ == "Contract": parts.append("CDD")
    elif _CDI_RE.search(title): parts.append("CDI")

    if _ENGLISH_POSTING_RE.search(desc_sample) and not _FRENCH_HEAVY_RE.search(f"{title} {desc_sample}"):
        parts.append("English-friendly")

    reason = ", ".join(parts)
    risk = "; ".join(risks) if risks else ""
    return score, reason, risk


# ============================================================
# MAIN EXPORT
# ============================================================

def main():
    DOCS.mkdir(exist_ok=True)
    conn = sqlite3.connect(str(DB))
    conn.row_factory = sqlite3.Row

    rows = conn.execute("""
        SELECT j.job_id, j.fingerprint, j.source, j.company, j.title,
               j.location_text, j.remote_type,
               j.compensation_min, j.compensation_max, j.compensation_text,
               j.url, j.apply_url, j.created_at_utc, j.description_text,
               d.queue, d.decision_reason, d.confidence,
               d.evidence_json, d.decided_at_utc
        FROM decisions d
        JOIN jobs_canonical j ON d.job_id = j.job_id
    """).fetchall()
    conn.close()

    print(f"Raw rows from DB: {len(rows)}")
    all_jobs = []

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
        world_tier = ev.get("world_tier", "")
        comp_text = ev.get("comp_record", {}).get("comp_text_raw", "") or row["compensation_text"] or ""

        tab = assign_tab(source, title, company, description, location,
                         ev.get("function_family", ""), classification,
                         ev.get("risk_flags", []), world_tier)

        # Gate: fashion tab uses existing fashion-scorer tier filter
        if tab == "fashion":
            tier = "Top Pick" if classification in TOP_PICK_LANES else ("Bridge" if classification in BRIDGE_LANES else "Pass")
            if tier == "Pass":
                continue

        # Gate: compliance tab — reject pure legal and sales
        if tab == "compliance":
            if _LEGAL_RE.search(title) and "compliance" not in title.lower():
                continue
            if _SALES_RE.search(title):
                continue

        # Score
        if tab == "compliance":
            score, reason, risk = nyc_score(title, company, description, location, comp_text)
        else:
            score, reason, risk = paris_score(title, company, description, location, world_tier, comp_text)

        comp_display = comp_text
        if not comp_display:
            comp_display = extract_comp_from_text(title, description)

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
            "lane": classification,
            "city": city_label(ev.get("city_lane", "")),
            "city_lane": ev.get("city_lane", "Unknown"),
            "world_simple": "Top World" if world_tier in {"Top Luxury / Culture World"} else ("Adjacent" if world_tier in {"Real Adjacent World", "Premium But Generic World"} else "Other"),
            "world_tier": world_tier,
            "function_family": ev.get("function_family", ""),
            "type": detect_type(title),
            "score": score,
            "power_score": float(score),
            "one_liner": reason,
            "risk": risk,
            "path_logic": ev.get("path_logic", ""),
            "main_risk": risk,
            "compensation": comp_display,
            "tab": tab,
            "_created": row["created_at_utc"] or "",
        })

    print(f"After filters: {len(all_jobs)}")

    # Dedupe
    groups: dict[tuple[str, str], list[dict]] = {}
    for job in all_jobs:
        key = (job["company"].lower().strip(), job["title"].lower().strip())
        groups.setdefault(key, []).append(job)
    deduped = []
    for group in groups.values():
        group.sort(key=lambda j: j["_created"] or "", reverse=True)
        deduped.append(group[0])
    print(f"After dedupe: {len(deduped)} (removed {len(all_jobs) - len(deduped)})")

    # Date filter
    cutoff = (datetime.now(timezone.utc) - timedelta(days=60)).isoformat(timespec="seconds")
    filtered = [j for j in deduped if not STALE_YEARS.search(j["title"]) or j["_created"] >= cutoff]
    print(f"After date filter: {len(filtered)}")

    # Sort & rank per tab
    comp_jobs = sorted([j for j in filtered if j["tab"] == "compliance"], key=lambda j: -j["score"])
    fash_jobs = sorted([j for j in filtered if j["tab"] == "fashion"], key=lambda j: -j["score"])
    for i, j in enumerate(comp_jobs): j["rank"] = i + 1
    for i, j in enumerate(fash_jobs): j["rank"] = i + 1

    all_final = comp_jobs + fash_jobs
    for j in all_final:
        del j["_created"]

    # Write
    (DOCS / "jobs.json").write_text(json.dumps(all_final, ensure_ascii=False, indent=1), encoding="utf-8")
    tab_counts = {}
    for j in all_final:
        tab_counts[j["tab"]] = tab_counts.get(j["tab"], 0) + 1
    meta = {
        "export_date": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "total_jobs": len(all_final),
        "jobs_by_tab": tab_counts,
    }
    (DOCS / "meta.json").write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")

    # Summary
    print(f"\nExported {len(all_final)} jobs")
    print(f"  Compliance: {len(comp_jobs)}, Fashion: {len(fash_jobs)}")

    for label, jobs in [("COMPLIANCE", comp_jobs), ("FASHION", fash_jobs)]:
        scores = [j["score"] for j in jobs]
        if scores:
            scores.sort()
            med = scores[len(scores) // 2]
            print(f"\n=== {label} (min={scores[0]}, median={med}, max={scores[-1]}) ===")
        else:
            print(f"\n=== {label} (empty) ===")
        for j in jobs[:5]:
            print(f"  #{j['rank']:>3} s={j['score']:3d} | {j['company_clean'][:22]:<22s} | {j['title'][:45]:<45s}")
            print(f"         reason: {j['one_liner'][:75]}")
            if j.get("risk"):
                print(f"         risk:   {j['risk'][:75]}")


if __name__ == "__main__":
    main()
