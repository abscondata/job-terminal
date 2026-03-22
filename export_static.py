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

_INTERN_RE = re.compile(r"\b(?:stage|stagiaire|intern|internship)\b", re.I)
_ALTERNANCE_RE = re.compile(r"\b(?:alternance|apprenti)", re.I)
_CONTRACT_RE = re.compile(r"\b(?:CDD|temp|temporary|contract|seasonal)\b", re.I)
_CDI_RE = re.compile(r"\bCDI\b")
COMPANY_SUFFIX = re.compile(
    r",?\s*\b(Inc\.?|LLC|Ltd\.?|Corp\.?|Co\.?|S\.?A\.?|S\.?A\.?S\.?|"
    r"SE|GmbH|PLC|N\.?V\.?|AG|SAS|SARL|SpA|Pty|Limited)\s*$", re.I,
)
_NYC_RE = re.compile(r"\b(?:new york|nyc|manhattan|brooklyn|jersey city|stamford|hoboken|weehawken|newark|white plains)\b", re.I)
_MIAMI_RE = re.compile(r"\b(?:miami|fort lauderdale|boca raton|palm beach|south florida|coral gables|doral|aventura|hollywood,?\s*fl)\b", re.I)
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
TOP_PICK_LANES = {"Paris Direction", "NYC Direction", "Money / Platform Leap"}
BRIDGE_LANES = {"Strategic Internship / Traineeship", "Interesting Stretch", "Miami Option", "Top-Brand Wrong-Function Risk"}

COMPLIANCE_KEYWORDS = [
    "compliance", "regulatory", "aml", "kyc", "risk analyst", "risk associate",
    "securities", "broker-dealer", "broker dealer", "licensing", "registration",
    "account opening", "onboarding", "finra", "financial crimes",
    "bsa", "anti-money", "sanctions", "trade surveillance",
]

COMP_FLOOR = 35
FASH_FLOOR = 45
TODAY = datetime(2026, 3, 22, tzinfo=timezone.utc)

# Titles that should NEVER appear in compliance tab
_NON_COMPLIANCE_TITLE_RE = re.compile(
    r"\b(?:product\s+(?:manager|designer|lead|owner)"
    r"|software\s+engineer|backend\s+engineer|frontend\s+engineer|fullstack"
    r"|data\s+scien(?:ce|tist)|machine\s+learning|devops|sre\b"
    r"|corporate\s+finance\s+manager|technical\s+accounting\s+manager"
    r"|account\s+executive|account\s+manager|sales\s+(?:associate|manager|director)"
    r"|options\s+specialist|trading\s+specialist|trader\b"
    r"|marketing\s+manager|brand\s+manager|creative\s+director"
    r"|ui\s+designer|ux\s+designer|graphic\s+designer"
    r"|content\s+and\s+ai|content\s+moderator"
    r"|code\s+compliance\s+inspector|building\s+inspector"
    r"|safety\s+and\s+compliance|food\s+safety|fair\s+workweek"
    r"|reservations?\s+(?:compliance|specialist)"
    r"|capital\s+markets(?!\s+compliance|\s+operations)"
    r"|coverage\s+(?:analyst|associate)|grands?\s+comptes"
    r"|corporate\s+debt|debt\s+finance"
    r"|liquidity\s+sales"
    r"|caf[e\u00e9]\s+ambassador|banking\s+associate|universal\s+banker"
    r"|part[\s-]+time(?!\s+compliance)"
    r"|equity\s+capital(?!\s+compliance)"
    r"|eeo\s+compliance|equal\s+employment"
    r"|healthcare\s+analyst"
    r"|analytics\s+avp(?!\s+compliance|\s+risk)"
    r"|finance\s+(?:&|and)\s+strategy(?!\s+compliance)"
    r"|business\s+controller|tax\s+(?:information\s+)?reporting"
    r"|vendor\s+risk\s+management|procurement"
    r"|model\s+risk\s+program"
    r"|ogc\s+analyst|lobbying"
    r"|(?:ecm|dcm)\s+analyst)\b", re.I,
)

# Companies that are NOT financial services — exclude from compliance tab
_NON_FINANCIAL_COMPANY_RE = re.compile(
    r"\b(?:ikea|raising\s+cane|chicken\s+fingers?"
    r"|independent\s+living|city\s+of\s+miami|city\s+of\s+new\s+york"
    r"|ritz.carlton\s+yacht|marriott|hilton|hyatt"
    r"|amazon\s+web\s+services|aws\b|google\b"
    r"|walmart|target\b|costco|home\s+depot"
    r"|interstate\s+waste"
    r"|eramet|latham\s+.?\s*watkins|topstep)\b", re.I,
)

# Expanded staffing agency list for scoring penalty
_STAFFING_EXPANDED_RE = re.compile(
    r"\b(?:staffing|recruiting|talent\s+(?:acquisition|solutions)"
    r"|search\s+partners|search\s+firm|manpower|adecco|robert\s+half"
    r"|randstad|hays|kelly\s+services|aston\s+carter|kforce|teksystems"
    r"|insight\s+global|beacon\s+hill|addison\s+group|phaxis|collabera"
    r"|apex\s+systems|prokatchers|cynet\s+systems|nextgen"
    r"|ascendo\s+resources|hireminds|cardea\s+group|madison.davis"
    r"|coda\s+search|arrow\s+search|social\s+capital\s+resources"
    r"|larson\s+maddox|barclay\s+simpson|ocr\s+alpha|plona\s+partners"
    r"|selby\s+jennings|harrington\s+starr|compliance\s+risk\s+concepts"
    r"|citi\s*staffing|glocap|options\s+group|odyssey\s+search"
    r"|solomon\s+page|dynamic(?:s)?\s+(?:executive\s+)?search)\b", re.I,
)

# ============================================================
# COMP EXTRACTION
# ============================================================

_COMP_RANGE_RE = re.compile(r"\$\s*(\d[\d,]*)\s*[kK]?\s*[-\u2013]\s*\$?\s*(\d[\d,]*)\s*[kK]?", re.I)
_COMP_K_RE = re.compile(r"\$\s*(\d+)\s*[kK]", re.I)
_COMP_PLAIN_RE = re.compile(r"\$\s*(\d[\d,]+)", re.I)
_COMP_EURO_RE = re.compile(r"(\d[\d\s]*)\s*[kK]?\s*\u20ac|(\d[\d\s]*)\s*\u20ac\s*[kK]?", re.I)
_COMP_BONUS_RE = re.compile(r"\$\s*(\d+)\s*[kK]?\s*\+\s*(?:bonus|BONUS)", re.I)


def extract_comp(title: str, desc: str, comp_text: str) -> str:
    """Extract best salary string from all available text."""
    for text in [comp_text, title, (desc or "")[:600]]:
        if not text:
            continue
        # Bonus pattern: "$130+BONUS"
        m = _COMP_BONUS_RE.search(text)
        if m:
            v = int(m.group(1))
            if v < 1000:
                return f"${v}K+bonus"
            return f"${v // 1000}K+bonus"
        # Range: "$80,000-$120,000" or "$80K-$120K"
        m = _COMP_RANGE_RE.search(text)
        if m:
            a, b = m.group(1).replace(",", ""), m.group(2).replace(",", "")
            va, vb = int(a), int(b)
            if va < 1000: va *= 1000
            if vb < 1000: vb *= 1000
            if 30000 <= va <= 500000:
                return f"${va // 1000}K-${vb // 1000}K"
        # Single K: "$130K"
        m = _COMP_K_RE.search(text)
        if m:
            v = int(m.group(1))
            if 30 <= v <= 500:
                return f"${v}K"
        # Plain dollar: "$80,000"
        m = _COMP_PLAIN_RE.search(text)
        if m:
            v = int(m.group(1).replace(",", ""))
            if 30000 <= v <= 500000:
                return f"${v // 1000}K"
        # Euro
        m = _COMP_EURO_RE.search(text)
        if m:
            raw = (m.group(1) or m.group(2) or "").replace(" ", "")
            if raw:
                v = int(raw)
                if v < 1000:
                    return f"{v}K\u20ac"
                if 20000 <= v <= 200000:
                    return f"{v // 1000}K\u20ac"
    return ""


def _comp_val(comp_str: str) -> int:
    """Parse comp string to annual USD value for scoring adjustments."""
    m = _COMP_K_RE.search(comp_str)
    if m:
        return int(m.group(1)) * 1000
    m = _COMP_PLAIN_RE.search(comp_str)
    if m:
        return int(m.group(1).replace(",", ""))
    return 0


# ============================================================
# STALE / EXPIRED DETECTION
# ============================================================

_MONTH_YEAR_RE = re.compile(
    r"\b(janv(?:ier)?|f[e\u00e9]vr(?:ier)?|mars|avr(?:il)?|mai|juin|juill(?:et)?|ao[u\u00fb]t|sept(?:embre)?|oct(?:obre)?|nov(?:embre)?|d[e\u00e9]c(?:embre)?|"
    r"january|february|march|april|may|june|july|august|september|octobre|november|december)\s+"
    r"(20\d{2})\b", re.I,
)

_MONTH_MAP = {
    "janvier": 1, "janv": 1, "january": 1,
    "fevrier": 2, "f\u00e9vrier": 2, "february": 2, "f\u00e9vr": 2, "fevr": 2,
    "mars": 3, "march": 3,
    "avril": 4, "avr": 4, "april": 4,
    "mai": 5, "may": 5,
    "juin": 6, "june": 6,
    "juillet": 7, "juill": 7, "july": 7,
    "aout": 8, "ao\u00fbt": 8, "august": 8,
    "septembre": 9, "sept": 9, "september": 9,
    "octobre": 10, "oct": 10, "october": 10,
    "novembre": 11, "nov": 11, "november": 11,
    "decembre": 12, "d\u00e9cembre": 12, "d\u00e9c": 12, "dec": 12, "december": 12,
}


_SEASON_YEAR_RE = re.compile(
    r"\b([e\u00e9]t[e\u00e9]|printemps|automne|hiver|summer|spring|fall|autumn|winter)\s+(20\d{2})\b", re.I,
)
_SEASON_MONTH = {
    "ete": 7, "printemps": 4, "automne": 10, "hiver": 1,
    "summer": 7, "spring": 4, "fall": 10, "autumn": 10, "winter": 1,
}
_BARE_YEAR_RE = re.compile(r"\b(202[0-5])\b")


def check_stale(title: str) -> tuple[str, bool]:
    """Check if title references a past date. Returns (risk_note, should_hide).
    should_hide=True if >6 months in the past.
    """
    # Check month + year pattern
    for m in _MONTH_YEAR_RE.finditer(title):
        month_str = m.group(1).lower().replace("\u00e9", "e").replace("\u00fb", "u")
        year = int(m.group(2))
        month = _MONTH_MAP.get(month_str, 0)
        if not month:
            continue
        target = datetime(year, month, 1, tzinfo=timezone.utc)
        if target < TODAY:
            months_ago = (TODAY.year - target.year) * 12 + (TODAY.month - target.month)
            month_label = m.group(1).capitalize()
            if months_ago > 6:
                return f"Expired -- listed for {month_label} {year}", True
            return f"Possibly expired -- listed for {month_label} {year}", False

    # Check season + year pattern (été 2023, printemps 2024, etc.)
    for m in _SEASON_YEAR_RE.finditer(title):
        season_str = m.group(1).lower().replace("\u00e9", "e")
        year = int(m.group(2))
        month = _SEASON_MONTH.get(season_str, 6)
        target = datetime(year, month, 1, tzinfo=timezone.utc)
        if target < TODAY:
            months_ago = (TODAY.year - target.year) * 12 + (TODAY.month - target.month)
            label = m.group(1).capitalize()
            if months_ago > 6:
                return f"Expired -- listed for {label} {year}", True
            return f"Possibly expired -- listed for {label} {year}", False

    # Check bare year (2020-2025 in title, not 2026+)
    for m in _BARE_YEAR_RE.finditer(title):
        year = int(m.group(1))
        if year < TODAY.year:
            months_ago = (TODAY.year - year) * 12 + TODAY.month
            if months_ago > 12:
                return f"Expired -- references {year}", True
            return f"Possibly expired -- references {year}", False

    return "", False


# ============================================================
# FUZZY DEDUP
# ============================================================

_DEDUP_STRIP_RE = re.compile(
    r"\b(?:verified|h/f|h/f/x|f/h|m/f|m/w/d|m/f/d|"
    r"stage|alternance|intern|internship|cdi|cdd|"
    r"paris|new york|nyc|miami|france|united states|"
    r"\(\d+\)|[,-])\b", re.I,
)
_MULTI_SPACE = re.compile(r"\s+")


def _dedup_key(company: str, title: str) -> str:
    """Normalize company+title for fuzzy dedup."""
    c = COMPANY_SUFFIX.sub("", (company or "").lower().strip()).strip()
    t = _DEDUP_STRIP_RE.sub(" ", (title or "").lower())
    t = _MULTI_SPACE.sub(" ", t).strip()
    return f"{c}|{t}"


def _word_overlap(key_a: str, key_b: str) -> float:
    """Return fraction of word overlap between two dedup keys."""
    _, ta = key_a.split("|", 1) if "|" in key_a else ("", key_a)
    _, tb = key_b.split("|", 1) if "|" in key_b else ("", key_b)
    wa = set(ta.split())
    wb = set(tb.split())
    if not wa or not wb:
        return 0.0
    overlap = wa & wb
    return len(overlap) / min(len(wa), len(wb))


_COMPANY_ALIASES = {
    "sumitomo mitsui banking corporation": "smbc",
    "sumitomo mitsui financial group": "smbc",
    "smbc group": "smbc",
    "mufg bank": "mufg",
    "j.p. morgan": "jpmorgan",
    "jp morgan": "jpmorgan",
    "jpmorgan chase": "jpmorgan",
    "jpmorgan chase &": "jpmorgan",
    "bank of america": "bofa",
    "rothschild &": "rothschild",
    "rothschild & co": "rothschild",
    "ls power development": "ls power",
}


def _dedup_company_key(company: str) -> str:
    """Normalize company name for dedup grouping."""
    c = COMPANY_SUFFIX.sub("", (company or "").lower().strip()).strip()
    return _COMPANY_ALIASES.get(c, c)


def fuzzy_dedup(jobs: list[dict]) -> tuple[list[dict], int]:
    """Dedup by company + fuzzy title match (80%+ word overlap). Keep highest score.
    Returns (deduped_list, removed_count)."""
    by_company: dict[str, list[dict]] = {}
    for j in jobs:
        c = _dedup_company_key(j["company"])
        by_company.setdefault(c, []).append(j)

    kept = []
    removed = 0
    for c, group in by_company.items():
        if len(group) == 1:
            kept.append(group[0])
            continue
        # Sort by score desc so we keep best
        group.sort(key=lambda j: -j["score"])
        accepted: list[dict] = []
        for j in group:
            jkey = _dedup_key(j["company"], j["title"])
            is_dup = False
            for a in accepted:
                akey = _dedup_key(a["company"], a["title"])
                if _word_overlap(jkey, akey) >= 0.8:
                    is_dup = True
                    break
            if is_dup:
                removed += 1
            else:
                accepted.append(j)
        kept.extend(accepted)
    return kept, removed


# ============================================================
# SHARED HELPERS
# ============================================================

def _norm(name: str) -> str:
    out = (name or "").lower().strip()
    out = COMPANY_SUFFIX.sub("", out).strip().rstrip(",").strip()
    return out


def _words_match(needle: str, haystack: str) -> bool:
    if needle == haystack: return True
    if re.search(r"\b" + re.escape(needle) + r"\b", haystack): return True
    if len(haystack) >= 3 and re.search(r"\b" + re.escape(haystack) + r"\b", needle): return True
    return False


def clean_company(name: str) -> str:
    out = (name or "").strip()
    out = COMPANY_SUFFIX.sub("", out).strip().rstrip(",").strip()
    # Fix truncated names
    if out == "Rothschild &" or out == "Rothschild &amp;":
        out = "Rothschild & Co"
    if out == "JPMorgan Chase &":
        out = "JPMorgan Chase"
    return out or name or ""


def detect_type(title: str) -> str:
    if _ALTERNANCE_RE.search(title or ""): return "Alternance"
    if _INTERN_RE.search(title or ""): return "Intern"
    if _CONTRACT_RE.search(title or ""): return "Contract"
    return "Full-Time"


def city_label_from_lane(city_lane: str) -> str:
    if not city_lane or city_lane == "Unknown": return "Other"
    if city_lane.startswith("Paris"): return "Paris"
    if city_lane == "NYC": return "NYC"
    if city_lane == "Miami": return "Miami"
    return "Other"


def city_label(city_lane: str, location: str = "") -> str:
    """Resolve city from city_lane first, then fall back to location text parsing."""
    result = city_label_from_lane(city_lane)
    if result != "Other":
        return result
    # Fall back to location text
    if _NYC_RE.search(location): return "NYC"
    if _MIAMI_RE.search(location): return "Miami"
    if _PARIS_RE.search(location): return "Paris"
    return "Other"


# ============================================================
# TAB ASSIGNMENT (unchanged logic from previous version)
# ============================================================

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
        if _words_match(lux, norm): return True
    return False


def assign_tab(source, title, company, description, location, function_family, classification, risk_flags, world_tier):
    title_lower = (title or "").lower()
    hay = f"{title} {function_family} {classification}".lower()
    has_comp_signal = any(kw in hay for kw in COMPLIANCE_KEYWORDS)
    if not has_comp_signal:
        has_comp_signal = any("compliance" in str(f).lower() or "analyst_lane" in str(f).lower() for f in (risk_flags or []))
    if has_comp_signal:
        if _is_luxury_company(company) and _DIRECTION_FUNCTION_RE.search(title):
            return "fashion"
        return "compliance"
    if source == "nyc_compliance": return "compliance"
    if _is_luxury_company(company): return "fashion"
    wt = (world_tier or "").lower()
    if "luxury" in wt or "culture" in wt or "adjacent" in wt: return "fashion"
    if _LUXURY_WORLD_RE.search(f"{title} {company} {(description or '')[:300]}"):
        if _DIRECTION_FUNCTION_RE.search(title): return "fashion"
    if classification in TOP_PICK_LANES or classification in BRIDGE_LANES: return "fashion"
    desc_lower = (description or "").lower()[:1500]
    for kw in COMPLIANCE_KEYWORDS[:6]:
        if kw in desc_lower:
            if any(w in title_lower for w in ["analyst", "associate", "specialist", "officer", "investigator", "operations"]):
                return "compliance"
    return "fashion"


# ============================================================
# NYC COMPLIANCE SCORING ENGINE
# ============================================================

_NYC_TITLE_T1 = re.compile(r"\b(?:compliance\s+(?:analyst|associate)|aml\s+(?:analyst|associate|investigator)|kyc\s+(?:analyst|associate))\b", re.I)
_NYC_TITLE_T2 = re.compile(r"\b(?:regulatory\s+(?:operations|compliance)\s+(?:analyst|associate|specialist)|broker.dealer\s+compliance|securities\s+(?:operations|compliance)|licensing\s+(?:analyst|specialist)|registration\s+(?:analyst|specialist))\b", re.I)
_NYC_TITLE_T3 = re.compile(r"\b(?:risk\s+(?:analyst|associate)|operations\s+(?:analyst|associate)|financial\s+crimes\s+(?:analyst|associate)|trade\s+surveillance\s+(?:analyst|associate)|sanctions\s+(?:analyst|specialist)|bsa\s+(?:analyst|associate))\b", re.I)
_NYC_TITLE_T4 = re.compile(r"\b(?:onboarding\s+(?:analyst|specialist|coordinator)|account\s+opening\s+(?:analyst|specialist)|client\s+onboarding)\b", re.I)
_NYC_TITLE_WEAK = re.compile(r"\b(?:data\s+analyst|general\s+counsel|eeo\s+compliance|sustainability|environmental|health\s+safety|osha|hipaa|privacy|it\s+compliance|information\s+security|cyber|soc\s+analyst)\b", re.I)

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
    "ameriprise", "edward jones", "usaa", "us bank",
}
_BB_NAMES = {"goldman", "morgan stanley", "jpmorgan", "citi", "barclays", "ubs", "deutsche", "hsbc", "bnp", "bofa", "bank of america", "wells fargo", "nomura", "mizuho", "mufg", "credit suisse"}


def _nyc_title_fit(title):
    """Score title fit 0-35. Does NOT apply seniority penalties — those go in disqualifier layer."""
    if _NYC_TITLE_WEAK.search(title): return 5, "weak fit"
    if _BILINGUAL_RE.search(title): return 10, "language"
    if _NYC_TITLE_T1.search(title): return 35, ""
    if _NYC_TITLE_T2.search(title): return 30, ""
    if _NYC_TITLE_T3.search(title): return 25, ""
    if _NYC_TITLE_T4.search(title): return 22, ""
    tl = title.lower()
    if "compliance" in tl: return 20, ""
    if any(k in tl for k in ["aml", "kyc", "regulatory", "risk"]): return 18, ""
    return 8, "tangential"


def _nyc_company(company):
    norm = _norm(company)
    if not norm: return 8, "", 4
    if _STAFFING_EXPANDED_RE.search(company): return 0, "staffing agency", 5
    if _STAFFING_RE.search(company): return 0, "staffing agency", 5
    if _INSURANCE_RE.search(company): return 0, "insurance", 5
    for c in _NYC_T1:
        if _words_match(c, norm):
            lbl = "bulge bracket" if any(w in norm for w in _BB_NAMES) else "top-tier firm"
            return 35, lbl, 1
    for c in _NYC_T2:
        if _words_match(c, norm): return 25, "strong firm", 2
    for c in _NYC_T3:
        if _words_match(c, norm): return 15, "solid firm", 3
    return 8, "", 4


_EXEC_RE = re.compile(r"\b(?:SVP|MD\b|managing\s+director|chief|head\s+of|principal)\b", re.I)
_VP_RE = re.compile(r"\b(?:VP|vice\s+president|AVP)\b", re.I)
_SUPERVISOR_RE = re.compile(r"\b(?:supervisor|team\s+lead(?:er)?)\b", re.I)
_COUNSEL_RE = re.compile(r"\b(?:counsel|attorney|lawyer|juriste|avocat)\b", re.I)
_ACAMS_REQ_RE = re.compile(r"\bACAMS\s+(?:required|certification\s+required)\b", re.I)
_AUDIT_EXAM_RE = re.compile(r"\b(?:audit(?:or|eur)?|exam(?:iner|ination)|internal\s+audit)\b", re.I)
_BD_RE = re.compile(r"\b(?:broker.dealer|BD\s+compliance|broker\s+dealer)\b", re.I)
_ASSOCIATE_NEARBY_RE = re.compile(r"\bassociate\b", re.I)


def nyc_score(title, company, desc, location, comp_text):
    title_pts, title_flag = _nyc_title_fit(title)
    company_pts, company_label, company_tier = _nyc_company(company)
    hay = f"{title} {desc}"
    adj = 0
    risks = []
    tl_lower = title.lower()

    # === DISQUALIFIER PENALTIES (granular seniority) ===
    if _EXEC_RE.search(title):
        adj -= 30; risks.append("Executive level -- not realistic")
    elif _VP_RE.search(title) and not _ASSOCIATE_NEARBY_RE.search(title):
        adj -= 20; risks.append("VP+ title -- expects significant experience")
    elif _SENIOR_RE.search(title):
        adj -= 15; risks.append("Senior title -- likely expects 3-5yr experience")
    elif _SUPERVISOR_RE.search(title):
        adj -= 15; risks.append("Management role -- expects team leadership experience")

    if _COUNSEL_RE.search(title):
        adj -= 30; risks.append("Requires law degree")
    if _ACAMS_REQ_RE.search(hay):
        adj -= 10; risks.append("ACAMS certification required -- user does not have it")

    # === EXISTING PENALTIES ===
    if _HIGH_EXP_RE.search(hay):
        adj -= 20; risks.append("5yr+ experience likely required")
    if _SALES_RE.search(hay) and "compliance" not in tl_lower:
        adj -= 25; risks.append("commission/sales-based")
    if _INSURANCE_RE.search(f"{company} {title}"):
        adj -= 15; risks.append("insurance company, not finance")
    if _BILINGUAL_RE.search(f"{title} {desc[:500]}"):
        m = _BILINGUAL_RE.search(f"{title} {desc[:500]}")
        adj -= 25; risks.append(f"requires {m.group(0).lower()}")

    # === SOFT BOOSTS for realistic-level titles ===
    if _ENTRY_RE.search(title):
        adj += 10  # boosted from +5
    elif ("associate" in tl_lower or "analyst" in tl_lower) and not _SENIOR_RE.search(title) and not _VP_RE.search(title):
        adj += 5
    if _SERIES7_RE.search(hay):
        adj += 10  # direct credential match
    if _BD_RE.search(title):
        adj += 8  # direct experience match
    if _HYBRID_RE.search(f"{location} {title}"):
        adj += 3

    # === ROLE TYPE PREFERENCE ===
    if _AUDIT_EXAM_RE.search(title):
        adj -= 8; risks.append("Audit/examination role -- user wants to move away from this function")
    # Preferred categories get a small boost
    if any(kw in tl_lower for kw in ["compliance associate", "compliance analyst", "aml analyst",
                                       "kyc analyst", "regulatory ops", "onboarding", "securities ops"]):
        adj += 3

    # === COMP CHECK ===
    comp = comp_text or extract_comp(title, desc, "")
    cv = _comp_val(comp)
    if cv >= 100000: adj += 5
    elif cv > 0 and cv < 60000: adj -= 10; risks.append(f"low salary (${cv // 1000}K)")

    # === COMPANY FLAGS (no score penalty for staffing) ===
    if company_label == "staffing agency":
        risks.append("staffing agency posting, actual employer unclear")
    elif company_label == "insurance" and "insurance company, not finance" not in risks:
        risks.append("insurance company, not finance")

    raw = title_pts + company_pts + adj
    score = max(0, min(100, round(raw * 100 / 70)))

    # Build reason: company-first format
    cc = clean_company(company)
    tl = title.lower()
    # Role label
    rl = "Compliance"
    if "compliance associate" in tl: rl = "compliance associate"
    elif "compliance analyst" in tl: rl = "compliance analyst"
    elif "aml" in tl and any(w in tl for w in ["analyst", "investigator", "associate"]): rl = "AML analyst"
    elif "kyc" in tl and any(w in tl for w in ["analyst", "associate", "specialist"]): rl = "KYC analyst"
    elif "broker" in tl and "compliance" in tl: rl = "BD compliance"
    elif "securities" in tl: rl = "securities ops"
    elif "regulatory" in tl: rl = "regulatory"
    elif "trade surveillance" in tl: rl = "trade surveillance"
    elif "risk" in tl and any(w in tl for w in ["analyst", "associate"]): rl = "risk analyst"
    elif "onboarding" in tl or "account opening" in tl: rl = "client onboarding"
    elif "financial crimes" in tl: rl = "financial crimes"
    elif "operations" in tl: rl = "operations"

    parts = []
    if company_label and company_label not in ("staffing agency", "insurance"):
        parts.append(f"{cc} {rl}, {company_label}")
    elif cc:
        parts.append(f"{cc} {rl}")
    else:
        parts.append(rl.capitalize())

    loc = location or ""
    if _NYC_RE.search(loc):
        arr = "hybrid" if _HYBRID_RE.search(loc) else "on-site"
        parts.append(f"{arr} NYC")
    elif _MIAMI_RE.search(loc): parts.append("Miami")
    elif _PARIS_RE.search(loc): parts.append("Paris")
    else:
        short = loc.split(",")[0].strip()[:20]
        if short: parts.append(short)

    extras = []
    if _SERIES7_RE.search(hay): extras.append("mentions Series 7")
    if _ENTRY_RE.search(title): extras.append("entry-level friendly")
    # Niche identification for richer reasons
    norm_co = _norm(company)
    if company_label == "staffing agency":
        pass  # already in risk
    elif any(w in norm_co for w in ["crypto", "coinbase", "kraken", "gemini", "circle", "ripple", "block"]):
        extras.append("crypto/fintech")
    elif any(w in norm_co for w in ["hedge", "point72", "citadel", "millennium", "aqr", "two sigma", "de shaw", "bridgewater"]):
        extras.append("hedge fund")
    elif any(w in norm_co for w in ["blackstone", "kkr", "apollo", "carlyle", "warburg", "bain capital", "ares", "tpg"]):
        extras.append("PE firm")
    elif any(w in norm_co for w in ["bank", "citi", "jpmorgan", "goldman", "morgan stanley", "barclays", "ubs", "hsbc", "bnp", "wells fargo", "bofa"]):
        extras.append("banking")
    elif any(w in norm_co for w in ["asset", "blackrock", "vanguard", "fidelity", "pimco", "invesco", "neuberger", "wellington", "franklin"]):
        extras.append("asset management")
    if extras: parts.append(", ".join(extras))

    reason = ", ".join(parts)
    risk = "; ".join(risks) if risks else ""
    return score, reason, risk, company_tier, title_pts


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
    "parfums christian dior", "remy cointreau", "interparfums", "maison crivelli",
}
_PARIS_BRAND_T3 = {
    "vestiaire collective", "christie", "christies", "sotheby", "sothebys",
    "centre pompidou", "fondation louis vuitton", "fondation cartier",
    "palais de tokyo", "musee d'orsay", "petit palais",
    "maison europeenne de la photographie", "jeu de paume",
    "opera gallery", "pm gallery", "la galerie du 19m", "galerie du 19m",
    "pace gallery", "gagosian", "perrotin",
    "musee", "galerie",
}
_PARIS_FUNC_T1 = re.compile(r"\b(?:chef\s+de\s+projet|project\s+(?:coordinator|assistant)|production\s+(?:assistant|coordinator)|content\s+(?:coordinator|production)|coordination)\b", re.I)
_PARIS_FUNC_T2 = re.compile(r"\b(?:communication(?:s)?\s+(?:assistant|coordinator)|event(?:s)?\s+(?:coordinator|assistant)|visual\s+merchandising|vm\s+coordinator|studio\s+(?:coordinator|assistant)|gallery\s+(?:coordinator|assistant)|auction\s+(?:operations|coordinator)|creative\s+(?:operations|coordinator))\b", re.I)
_PARIS_FUNC_T3 = re.compile(r"\b(?:marketing\s+(?:assistant|coordinator)|brand\s+(?:coordinator|assistant|strategy)|pr\s+(?:assistant|coordinator)|influence|relations\s+(?:presse|publiques))\b", re.I)
_PARIS_COMPLIANCE_FUNC = re.compile(r"\b(?:compliance|conformit|aml|kyc|lcb|risque|contr.le\s+interne|audit\s+interne)\b", re.I)
_PARIS_RETAIL_RE = re.compile(
    r"\b(?:vendeur|vendeuse|conseill(?:er|[e\u00e8]re)\s+de\s+(?:vente|mode)"
    r"|assistant(?:e)?\s+de\s+vente|sales\s+(?:associate|advisor)"
    r"|client\s+advisor|h[o\u00f4]te(?:sse)?\s+de\s+caisse|cashier|caissier)\b", re.I)
_PARIS_STOCKROOM_RE = re.compile(r"\b(?:stockist[e]?|stock\s+(?:associate|coordinator)|magasinier|warehouse)\b", re.I)
_PARIS_CRAFT_RE = re.compile(r"\b(?:repasseu[r|se]|retoucheu[r|se]|couturi[e\u00e8]re?|teinturerie|d[e\u00e9]tacheu[r|se]|pressing|polisseu[r|se]|sertisseu[r|se])\b", re.I)
_PARIS_REPAIR_RE = re.compile(r"\b(?:service\s+entretien|service\s+r[e\u00e9]paration|apr[e\u00e8]s.vente|SAV\b|repair)\b", re.I)
_PARIS_BOUTIQUE_SVC_RE = re.compile(r"\b(?:service\s+client(?:s|e)?\s+boutique)\b", re.I)
_PARIS_SUPPLY_RE = re.compile(r"\b(?:supply\s+chain|omnistock|logistique|approvisionnement|demand\s+planning|inventory)\b", re.I)
_PARIS_SUPPLY_CREATIVE_RE = re.compile(r"\b(?:cr[e\u00e9]ation|design|visual|atelier|production\s+artistique)\b", re.I)
_PARIS_IT_RE = re.compile(r"(?:\bSI\b|\bIS\b|syst[e\u00e8]me(?:s)?\s+d.information|informatique|\bERP\b|\bSAP\b|data\s+engineer|\bBI\b|business\s+intelligence|d[e\u00e9]veloppeu[r|se]|developer|devops)", re.I)
_PARIS_HR_RE = re.compile(r"\b(?:learning\s+(?:&|and|et)\s+development|formation\s+RH|ressources\s+humaines|people\s+ops|talent\s+acquisition|recrutement)\b", re.I)
_PARIS_FINANCE_RE = re.compile(r"\b(?:contr[o\u00f4]le?\s+de\s+gestion|comptabilit|audit\s+interne|finance\s+manager|controller|accounts?\s+(?:payable|receivable))\b", re.I)
_FRENCH_HEAVY_RE = re.compile(r"\b(?:excellente?\s+ma.trise\s+du?\s+fran.ais|fran.ais\s+(?:courant|natif|maternel)|niveau\s+c[12]\s+(?:en\s+)?fran.ais|r.daction|press\s+attach|attach.e?\s+de\s+presse|copywriter|r.dacteur|concepteur.r.dacteur)\b", re.I)
_ENGLISH_POSTING_RE = re.compile(r"\b(?:we\s+are\s+looking|you\s+will|the\s+role|responsibilities|requirements|about\s+the\s+role|what\s+you|join\s+(?:us|our))\b", re.I)


def _paris_brand(company, world_tier):
    norm = _norm(company)
    for b in _PARIS_BRAND_T1:
        if _words_match(b, norm): return 40, "top maison", 1
    for b in _PARIS_BRAND_T2:
        if _words_match(b, norm): return 32, "strong luxury brand", 2
    for b in _PARIS_BRAND_T3:
        if _words_match(b, norm): return 22, "respected institution", 3
    # Only trust world_tier if explicitly tagged Luxury/Culture by the engine
    wt = (world_tier or "").lower()
    if "top luxury" in wt or "culture" in wt:
        return 18, "luxury-adjacent", 4
    # NOT RECOGNIZED — return negative tier to signal exclusion
    return -1, "", 99


def _paris_role(title):
    # Downgrade categories first (0-5 role quality)
    if _PARIS_RETAIL_RE.search(title): return 2, "retail sales", 8
    if _PARIS_STOCKROOM_RE.search(title): return 2, "stockroom", 8
    if _PARIS_CRAFT_RE.search(title): return 3, "skilled craft", 8
    if _PARIS_REPAIR_RE.search(title): return 2, "repair/after-sales", 8
    if _PARIS_BOUTIQUE_SVC_RE.search(title): return 3, "boutique client service", 8
    if _PARIS_IT_RE.search(title): return 3, "IT/systems", 7
    if _PARIS_HR_RE.search(title): return 4, "HR/training", 7
    if _PARIS_FINANCE_RE.search(title): return 4, "finance/accounting", 7
    # Supply chain — unless paired with creative
    if _PARIS_SUPPLY_RE.search(title) and not _PARIS_SUPPLY_CREATIVE_RE.search(title):
        return 5, "supply chain", 6
    # Compliance at luxury house
    if _PARIS_COMPLIANCE_FUNC.search(title): return 5, "compliance function", 6
    # Good creative/direction roles
    if _PARIS_FUNC_T1.search(title): return 35, "project/coordination", 1
    if _PARIS_FUNC_T2.search(title): return 32, "creative/events/VM", 2
    if _PARIS_FUNC_T3.search(title): return 28, "marketing/brand/PR", 3
    return 15, "", 4


def paris_score(title, company, desc, location, world_tier, comp_text):
    brand_pts, brand_label, brand_tier = _paris_brand(company, world_tier)
    role_pts, role_label, role_tier = _paris_role(title)
    adj = 0
    risks = []

    desc_sample = (desc or "")[:2000]
    if _FRENCH_HEAVY_RE.search(f"{title} {desc_sample}"):
        adj -= 15; risks.append("requires polished written French")
    elif _ENGLISH_POSTING_RE.search(desc_sample):
        adj += 5
    if re.search(r"\b(?:r.dacteur|copywriter|press\s+attach|attach.e?\s+de\s+presse)\b", title, re.I):
        adj -= 20; risks.append("written French comms role")

    title_l = title.lower()
    if _CDI_RE.search(title): adj += 10
    elif _CONTRACT_RE.search(title): adj += 5
    elif "freelance" in title_l: adj -= 5

    # Alternance school enrollment flag + score penalty
    if _ALTERNANCE_RE.search(title):
        adj -= 20
        risks.append("Alternance -- requires French school enrollment")

    # Downgrade category risk flags
    if _PARIS_RETAIL_RE.search(title):
        risks.append("Retail sales role at luxury house -- no direction change value")
    elif _PARIS_STOCKROOM_RE.search(title):
        risks.append("Stockroom role -- manual work, no path value")
    elif _PARIS_CRAFT_RE.search(title):
        risks.append("Skilled craft role -- requires specific trade training")
    elif _PARIS_REPAIR_RE.search(title):
        risks.append("Repair/after-sales service -- retail support, not creative")
    elif _PARIS_BOUTIQUE_SVC_RE.search(title):
        risks.append("Boutique client service -- retail support role")
    elif _PARIS_IT_RE.search(title):
        risks.append("IT/systems role at luxury house -- wrong function")
    elif _PARIS_HR_RE.search(title):
        risks.append("HR/training role -- not creative function")
    elif _PARIS_FINANCE_RE.search(title):
        risks.append("Finance/accounting at luxury house -- back office")
    elif _PARIS_SUPPLY_RE.search(title) and not _PARIS_SUPPLY_CREATIVE_RE.search(title):
        risks.append("Supply chain/logistics -- operations, not creative direction")
    elif _PARIS_COMPLIANCE_FUNC.search(title) and brand_pts >= 32:
        risks.append("compliance function at luxury house -- direction value unclear")

    raw = brand_pts + role_pts + adj
    score = max(0, min(100, round(raw * 100 / 75)))

    # Build reason: company-first
    cc = clean_company(company)
    parts = []
    if role_label == "project/coordination":
        lbl = "chef de projet" if "chef de projet" in title_l else "project coordination"
        parts.append(f"{cc} {lbl}")
    elif role_label == "creative/events/VM":
        if "visual merchandising" in title_l or "vm" in title_l: parts.append(f"{cc} visual merchandising")
        elif "event" in title_l: parts.append(f"{cc} events")
        elif "gallery" in title_l: parts.append(f"{cc} gallery")
        elif "studio" in title_l: parts.append(f"{cc} studio")
        else: parts.append(f"{cc} {role_label}")
    elif role_label == "marketing/brand/PR": parts.append(f"{cc} brand/marketing")
    elif role_label == "compliance function": parts.append(f"Compliance at {cc}")
    elif role_label == "retail sales": parts.append(f"Retail at {cc}")
    elif role_label == "stockroom": parts.append(f"Stockroom at {cc}")
    elif role_label == "skilled craft": parts.append(f"Craft/atelier at {cc}")
    elif role_label == "repair/after-sales": parts.append(f"Repair/SAV at {cc}")
    elif role_label == "boutique client service": parts.append(f"Client service at {cc}")
    elif role_label == "IT/systems": parts.append(f"IT/systems at {cc}")
    elif role_label == "HR/training": parts.append(f"HR at {cc}")
    elif role_label == "finance/accounting": parts.append(f"Finance at {cc}")
    elif role_label == "supply chain": parts.append(f"Supply chain at {cc}")
    else: parts.append(cc or "Unknown")

    if brand_label: parts.append(brand_label)

    typ = detect_type(title)
    if typ == "Alternance": parts.append("alternance")
    elif typ == "Intern": parts.append("stage")
    elif typ == "Contract": parts.append("CDD")
    elif _CDI_RE.search(title): parts.append("CDI")

    if _ENGLISH_POSTING_RE.search(desc_sample) and not _FRENCH_HEAVY_RE.search(f"{title} {desc_sample}"):
        parts.append("English-friendly")

    reason = ", ".join(parts)
    risk = "; ".join(risks) if risks else ""
    return score, reason, risk, brand_tier, role_tier


def _fix_function_family(func: str, title: str, tab: str) -> str:
    """Override bad function_family tags. Gallery/Cultural at a fintech is wrong."""
    if not func:
        return func
    func_l = func.lower()
    if tab == "compliance" and ("gallery" in func_l or "cultural" in func_l or "fashion" in func_l):
        # Derive from title instead
        tl = title.lower()
        if "compliance" in tl: return "Compliance"
        if "aml" in tl or "kyc" in tl: return "AML / KYC"
        if "risk" in tl: return "Risk"
        if "operations" in tl: return "Operations"
        if "regulatory" in tl: return "Regulatory"
        if "onboarding" in tl: return "Client Onboarding"
        return "Compliance"
    return func


# ============================================================
# APPLIED JOB MATCHING
# ============================================================

_GENERIC_COMPANY_WORDS = {
    "group", "inc", "the", "llc", "corp", "partners", "bank", "services",
    "solutions", "resources", "associates", "consulting", "financial",
    "capital", "search", "staffing", "advisors", "technologies", "systems",
    "international", "global", "management", "company", "de", "et", "la",
    "le", "les", "des", "du", "sa", "sas",
}
_GENERIC_TITLE_WORDS = {
    "associate", "analyst", "senior", "junior", "specialist", "officer",
    "manager", "intern", "stage", "alternance", "verified", "h/f", "h/f/x",
    "f/h", "m/f", "m/w/d", "cdi", "cdd", "h", "f", "x", "m", "w", "d",
}
_APPLIED_CSV = Path.home() / "Desktop" / "job apps" / "applied_jobs_cleaned.csv"


def _meaningful_words(text: str, generic: set) -> set:
    words = set(re.sub(r"[^a-z0-9 ]", " ", (text or "").lower()).split())
    return {w for w in words if len(w) >= 4 and w not in generic} | {w for w in words if len(w) >= 2 and w not in generic and w.isalpha()}


def _load_applied():
    if not _APPLIED_CSV.exists():
        return []
    import csv as _csv
    with open(_APPLIED_CSV, "r", encoding="utf-8", errors="replace") as f:
        return list(_csv.DictReader(f))


def _norm_company_for_match(c: str) -> str:
    """Normalize company name for matching: lowercase, strip punctuation and suffixes."""
    out = re.sub(r"[^a-z0-9 ]", " ", (c or "").lower())
    out = COMPANY_SUFFIX.sub("", out).strip()
    # Strip very common generic words that cause false matches
    for w in ("the", "de", "et", "la", "le", "du", "des"):
        out = re.sub(r"\b" + w + r"\b", "", out)
    return re.sub(r"\s+", " ", out).strip()


def _company_match(a: str, b: str) -> bool:
    """Containment-based company match: if either normalized name contains the other.
    Requires the shorter name to be at least 60% of the longer name's length
    to prevent 'Man Group' matching 'Neuberger Berman Group'."""
    na = _norm_company_for_match(a)
    nb = _norm_company_for_match(b)
    if len(na) < 3 or len(nb) < 3:
        return False
    shorter, longer = (na, nb) if len(na) <= len(nb) else (nb, na)
    if shorter not in longer:
        return False
    # Prevent short substrings from matching long names
    if len(shorter) / max(len(longer), 1) < 0.5:
        return False
    return True


def _match_applied(job_company: str, job_title: str, applied_rows: list) -> bool:
    jt_words = set(re.sub(r"[^a-z0-9 ]", " ", (job_title or "").lower()).split())
    jt_words = {w for w in jt_words if len(w) >= 3 and w not in _GENERIC_TITLE_WORDS}

    if not jt_words:
        return False

    for ar in applied_rows:
        ac = ar.get("company", "") or ar.get("company_raw", "")
        at = ar.get("title", "") or ar.get("title_raw", "")

        # Company: containment match
        if not _company_match(job_company, ac):
            continue

        # Title: 50%+ word overlap on shorter set
        at_words = set(re.sub(r"[^a-z0-9 ]", " ", (at or "").lower()).split())
        at_words = {w for w in at_words if len(w) >= 3 and w not in _GENERIC_TITLE_WORDS}
        if not at_words:
            continue
        overlap = jt_words & at_words
        shorter = min(len(jt_words), len(at_words))
        if shorter > 0 and len(overlap) / shorter >= 0.5:
            return True

    return False


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
        FROM decisions d JOIN jobs_canonical j ON d.job_id = j.job_id
    """).fetchall()
    conn.close()

    print(f"Raw rows from DB: {len(rows)}")

    # Load applied jobs for matching
    applied_rows = _load_applied()
    print(f"Applied jobs loaded: {len(applied_rows)}")

    all_jobs = []

    for row in rows:
        ev = {}
        try: ev = json.loads(row["evidence_json"] or "{}")
        except Exception: pass

        classification = ev.get("classification", row["decision_reason"] or "")
        title = row["title"] or ""
        company = row["company"] or ""
        location = row["location_text"] or ""
        description = row["description_text"] or ""
        source = row["source"] or ""
        world_tier = ev.get("world_tier", "")
        comp_raw = ev.get("comp_record", {}).get("comp_text_raw", "") or row["compensation_text"] or ""

        tab = assign_tab(source, title, company, description, location,
                         ev.get("function_family", ""), classification,
                         ev.get("risk_flags", []), world_tier)

        if tab == "fashion":
            tier = "Top Pick" if classification in TOP_PICK_LANES else ("Bridge" if classification in BRIDGE_LANES else "Pass")
            if tier == "Pass": continue
        if tab == "compliance":
            if _LEGAL_RE.search(title) and "compliance" not in title.lower(): continue
            if _SALES_RE.search(title) and "compliance" not in title.lower(): continue
            # Non-compliance function gate (expanded)
            if _NON_COMPLIANCE_TITLE_RE.search(title): continue
            # Non-financial company gate
            if _NON_FINANCIAL_COMPANY_RE.search(company): continue
            # Validate title has actual compliance signal — reject roles that only
            # got here because the company is financial
            tl_check = title.lower()
            has_compliance_title = any(kw in tl_check for kw in [
                "compliance", "aml", "kyc", "regulatory", "risk",
                "surveillance", "sanctions", "bsa", "financial crimes",
                "anti-money", "onboarding", "account opening",
                "licensing", "registration", "finra", "audit",
                "conformit", "lcb", "risque", "contr",
            ])
            if not has_compliance_title:
                continue
            # City gate: only NYC, Miami, Paris
            if not (_NYC_RE.search(location) or _MIAMI_RE.search(location) or _PARIS_RE.search(location)):
                continue

        comp_display = extract_comp(title, description, comp_raw)
        # Fix $0K / bad comp display
        if comp_display:
            stripped = comp_display.replace("$", "").replace("K", "").replace("k", "").replace(",", "").replace(" ", "").split("-")[0].split("+")[0]
            try:
                if not stripped or float(stripped) <= 0:
                    comp_display = ""
            except ValueError:
                pass
            if comp_display.lower() in ("$0k", "$0k-$0k", "0", "competitive", "n/a"):
                comp_display = ""

        if tab == "compliance":
            score, reason, risk, c_tier, t_fit = nyc_score(title, company, description, location, comp_raw)
            sort_secondary = (c_tier, t_fit)
        else:
            score, reason, risk, b_tier, r_tier = paris_score(title, company, description, location, world_tier, comp_raw)
            # Fix 2: If brand is not recognized (tier 99), skip this role
            if b_tier == 99:
                continue
            sort_secondary = (b_tier, r_tier)

        # Stale detection
        stale_note, stale_hide = check_stale(title)
        if stale_hide:
            continue  # >6 months in past, hide entirely
        if stale_note and stale_note not in risk:
            risk = f"{risk}; {stale_note}" if risk else stale_note

        resolved_city = city_label(ev.get("city_lane", ""), location)
        # Hide "Other" city compliance roles
        if tab == "compliance" and resolved_city == "Other":
            continue

        all_jobs.append({
            "job_id": row["job_id"], "fingerprint": row["fingerprint"],
            "source": source, "company": company, "company_clean": clean_company(company),
            "title": title, "location": location,
            "url": row["url"] or row["apply_url"] or "",
            "created_at": row["created_at_utc"] or "",
            "lane": classification,
            "city": resolved_city,
            "city_lane": ev.get("city_lane", "Unknown"),
            "world_simple": "Top World" if world_tier == "Top Luxury / Culture World" else ("Adjacent" if world_tier in {"Real Adjacent World", "Premium But Generic World"} else "Other"),
            "world_tier": world_tier,
            "function_family": _fix_function_family(ev.get("function_family", ""), title, tab),
            "type": detect_type(title),
            "score": score, "power_score": float(score),
            "one_liner": reason, "risk": risk, "main_risk": risk,
            "compensation": comp_display,
            "tab": tab,
            "is_alternance": bool(_ALTERNANCE_RE.search(title)),
            "applied": _match_applied(company, title, applied_rows),
            "_sort2": sort_secondary,
            "_created": row["created_at_utc"] or "",
        })

    print(f"After gate filters: {len(all_jobs)}")

    # Fuzzy dedup
    all_jobs, dedup_removed = fuzzy_dedup(all_jobs)
    print(f"After fuzzy dedup: {len(all_jobs)} (removed {dedup_removed})")

    # Sort per tab with secondary sort
    comp_jobs = [j for j in all_jobs if j["tab"] == "compliance"]
    fash_jobs = [j for j in all_jobs if j["tab"] == "fashion"]

    # Applied roles sort to bottom of their score tier
    comp_jobs.sort(key=lambda j: (j.get("applied", False), -j["score"], j["_sort2"][0], j["_sort2"][1]))
    fash_jobs.sort(key=lambda j: (j.get("applied", False), -j["score"], j["_sort2"][0], j["_sort2"][1]))

    for i, j in enumerate(comp_jobs): j["rank"] = i + 1
    for i, j in enumerate(fash_jobs): j["rank"] = i + 1

    all_final = comp_jobs + fash_jobs
    for j in all_final:
        del j["_sort2"]
        del j["_created"]

    # Stats
    comp_visible = [j for j in comp_jobs if j["score"] >= COMP_FLOOR]
    fash_visible = [j for j in fash_jobs if j["score"] >= FASH_FLOOR]
    comp_hidden = len(comp_jobs) - len(comp_visible)
    fash_hidden = len(fash_jobs) - len(fash_visible)

    # Write
    (DOCS / "jobs.json").write_text(json.dumps(all_final, ensure_ascii=False, indent=1), encoding="utf-8")
    meta = {
        "export_date": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "total_jobs": len(all_final),
        "comp_floor": COMP_FLOOR, "fash_floor": FASH_FLOOR,
        "compliance_visible": len(comp_visible), "fashion_visible": len(fash_visible),
        "compliance_total": len(comp_jobs), "fashion_total": len(fash_jobs),
    }
    (DOCS / "meta.json").write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")

    # Summary
    comp_applied = sum(1 for j in comp_visible if j.get("applied"))
    fash_applied = sum(1 for j in fash_visible if j.get("applied"))

    print(f"\nExported {len(all_final)} jobs")
    print(f"  Compliance: {len(comp_jobs)} total, {len(comp_visible)} visible (>={COMP_FLOOR}), {comp_applied} applied, {len(comp_visible) - comp_applied} unapplied")
    print(f"  Fashion:    {len(fash_jobs)} total, {len(fash_visible)} visible (>={FASH_FLOOR}), {fash_applied} applied, {len(fash_visible) - fash_applied} unapplied")
    print(f"  Dedup removed: {dedup_removed}")

    for label, visible in [("COMPLIANCE", comp_visible), ("FASHION", fash_visible)]:
        scores = sorted([j["score"] for j in visible])
        if not scores:
            print(f"\n=== {label} (empty) ==="); continue
        med = scores[len(scores) // 2]
        print(f"\n=== {label} visible: {len(visible)} roles (min={scores[0]}, median={med}, max={scores[-1]}) ===")
        n_top = 20 if label == "COMPLIANCE" else 3
        n_bot = 10 if label == "COMPLIANCE" else 3
        print(f"  TOP {n_top}:")
        for j in visible[:n_top]:
            print(f"    #{j['rank']:>3} s={j['score']:3d} | {j['company_clean'][:22]:<22s} | {j['title'][:42]}")
            print(f"           {j['one_liner'][:75]}")
            if j["risk"]: print(f"           RISK: {j['risk'][:70]}")
            if j["compensation"]: print(f"           COMP: {j['compensation']}")
        print(f"  BOTTOM {n_bot}:")
        for j in visible[-n_bot:]:
            print(f"    #{j['rank']:>3} s={j['score']:3d} | {j['company_clean'][:22]:<22s} | {j['title'][:42]}")
            print(f"           {j['one_liner'][:75]}")
            if j["risk"]: print(f"           RISK: {j['risk'][:70]}")


if __name__ == "__main__":
    main()
