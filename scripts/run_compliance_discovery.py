"""
NYC Compliance Discovery Pipeline — self-contained.
Scrapes Indeed, hard-rejects, dedupes against applied CSV, scores 0-100, generates mobile report.
"""
from __future__ import annotations

import csv
import json
import re
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

# ── Config ──────────────────────────────────────────────────────────────────

INDEED_DOMAIN = "www.indeed.com"
INDEED_LOCATION = "New York, NY"
APPLIED_CSV = Path.home() / "Desktop" / "job apps" / "applied_jobs_cleaned.csv"
REPORT_OUT = ROOT / "docs" / "report.html"

QUERIES = [
    "compliance analyst",
    "compliance associate",
    "KYC analyst",
    "AML analyst",
    "onboarding analyst",
    "regulatory operations",
    "securities operations",
    "broker dealer compliance",
    "financial crime analyst",
    "client onboarding financial services",
    "licensing registration FINRA",
    "operations associate investment bank",
    "middle office",
    "trade support",
    "fund operations",
    "compliance specialist bank",
    "BSA analyst",
    "sanctions analyst",
    "due diligence analyst",
    "risk operations",
    "account opening financial",
    "client service associate investment",
    "regulatory reporting",
    "operations analyst securities",
]

BROWSER_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": (
        "text/html,application/xhtml+xml,application/xml;q=0.9,"
        "image/avif,image/webp,*/*;q=0.8"
    ),
    "Accept-Language": "en-US,en;q=0.9,fr;q=0.8",
    "Cache-Control": "no-cache",
    "Pragma": "no-cache",
    "Referer": "https://www.google.com/",
    "Upgrade-Insecure-Requests": "1",
}

# ── Hard reject patterns ────────────────────────────────────────────────────

TITLE_REJECT_RE = re.compile(
    r"\b(?:"
    r"senior\s+(?:analyst|associate|officer|specialist|manager|director|compliance|aml|kyc|consultant)"
    r"|vice\s+president|vp\b|avp\b|svp\b|director|head\s+of|chief|principal"
    r"|managing\s+director|manager|lead\s+(?:analyst|associate)|supervisor"
    r"|financial\s+advisor|insurance\s+agent|sales\s+rep"
    r"|dental|nurse|medical|food\s+safety|scheduler|secretary"
    r"|intern\b|internship"
    r"|attorney|counsel|paralegal|lawyer"
    r"|data\s+scientist|data\s+engineer|machine\s+learning"
    r")\b",
    re.I,
)

INDUSTRY_REJECT_RE = re.compile(
    r"\b(?:"
    r"healthcare|hospital|medical\s+center|nursing|dental|ophthalmology"
    r"|food\s+safety|grocery|restaurant"
    r"|education|school\s+district|university(?!\s+(?:endowment|fund))"
    r"|nonprofit|non-profit|NGO"
    r"|OPWDD|social\s+services|child\s+welfare"
    r"|insurance\s+(?:company|agency|carrier)|life\s+insurance|health\s+insurance"
    r"|pharmaceutical|biotech"
    r"|sports\s+betting|igaming|online\s+casino|sportsbook"
    r"|cybersecurity\s+firm|information\s+security\s+(?:firm|company)"
    r"|real\s+estate\s+(?:brokerage|agency)"
    r"|pharmacy|construction\s+(?:company|contracting|inc)"
    r")\b",
    re.I,
)

# Government OK only if SEC/FINRA/OCC/FDIC
GOV_RE = re.compile(r"\b(?:state\s+of|city\s+of|department\s+of|public\s+health|government)\b", re.I)
FIN_GOV_RE = re.compile(r"\b(?:SEC|FINRA|OCC|FDIC|Federal\s+Reserve|Treasury)\b", re.I)

EXP_REJECT_RE = re.compile(
    r"\b(?:[5-9]|1[0-9]|20)\+?\s*(?:years?|yrs?)\s*(?:of\s+)?(?:experience|exp)\b", re.I
)

NON_NYC_RE = re.compile(
    r"\b(?:jersey\s+city|white\s+plains|stamford|westchester|coral\s+gables"
    r"|newark|hoboken|parsippany|morristown|greenwich|darien|norwalk"
    r"|harrison|purchase|armonk|tarrytown|yonkers|jericho|melville"
    r"|princeton|florham\s+park|short\s+hills|woodbridge|iselin)\b",
    re.I,
)

NYC_RE = re.compile(
    r"\b(?:new\s+york|nyc|manhattan|brooklyn|queens|bronx|staten\s+island"
    r"|midtown|downtown|financial\s+district|wall\s+street|tribeca)\b",
    re.I,
)

CONTRACT_RE = re.compile(r"\b(?:seasonal|contract|temp\b|temporary|freelance|part[-\s]?time)\b", re.I)

JOB_CARD_RE = re.compile(
    r'<div class="job_seen_beacon">(.*?)(?=<div class="job_seen_beacon"|$)', re.S
)
BLOCKED_RE = re.compile(
    r"Authenticating\.\.\.|bot-detection-anonymous|Additional Verification Required", re.I
)

# ── Financial services world keywords ───────────────────────────────────────

TOP_FIRMS = {
    "jp morgan", "jpmorgan", "jpmorganchase", "goldman sachs", "morgan stanley",
    "citi", "citigroup", "citibank", "bank of america", "barclays", "ubs",
    "deutsche bank", "hsbc", "bnp paribas", "societe generale", "mufg", "smbc",
    "jefferies", "credit agricole", "credit suisse", "blackstone", "kkr",
    "apollo", "carlyle", "citadel", "two sigma", "jane street", "man group",
    "nasdaq", "rothschild", "lazard", "coinbase", "robinhood", "vanguard",
    "fidelity", "pimco", "td bank", "td securities", "wells fargo", "ramp",
    "stripe", "klarna", "webull", "moomoo", "moody", "anchorage digital",
    "clear street", "scotiabank", "cibc", "nomura", "macquarie", "cantor",
    "oppenheimer", "piper sandler", "raymond james", "stifel", "cowen",
    "evercore", "centerview", "perella weinberg", "moelis", "houlihan lokey",
    "greenhill", "guggenheim", "neuberger berman", "d. e. shaw", "de shaw",
    "bridgewater", "millennium", "point72", "balyasny", "schonfeld",
    "new york life", "metlife", "prudential financial",
    "state street", "northern trust", "bny mellon", "pershing",
    "interactive brokers", "charles schwab", "ameritrade",
    "kroll", "deloitte", "pwc", "ey", "kpmg", "crowe", "grant thornton",
    "alvarez", "ftI consulting", "huron",
    "capgemini", "accenture",
    "lseg", "london stock exchange",
    "marex", "virtu", "flow traders",
    "metropolitan commercial bank",
}

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

ROLE_MATCH_RE = re.compile(
    r"\b(?:compliance|kyc|aml|bsa|anti[-\s]?money|financial\s+crime|sanctions"
    r"|onboarding|account\s+opening|client\s+(?:onboarding|service)"
    r"|due\s+diligence|cdd|know\s+your\s+customer"
    r"|regulatory\s+(?:operations|compliance|affairs)"
    r"|licensing|registration|finra"
    r"|securities\s+(?:operations|ops)|broker[-\s]?dealer\s+(?:operations|compliance|ops)"
    r"|middle\s+office|trade\s+(?:support|operations|processing)"
    r"|fund\s+(?:operations|accounting|admin)"
    r"|operations\s+(?:analyst|associate|coordinator)"
    r"|risk\s+(?:operations|analyst|associate))\b",
    re.I,
)


# ── Helpers ─────────────────────────────────────────────────────────────────

def _clean(text: str) -> str:
    t = re.sub(r"<[^>]+>", " ", text or "")
    t = re.sub(r"&\w+;", " ", t)
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
    """Parse salary text to annual number. Returns None if unparseable."""
    if not salary_text:
        return None
    s = salary_text.lower().replace(",", "").replace("$", "")
    # Check for annual
    nums = re.findall(r"([\d.]+)", s)
    if not nums:
        return None
    vals = [float(x) for x in nums]
    if "hour" in s:
        top = max(vals)
        return int(top * 2080)  # hourly -> annual
    if "year" in s:
        return int(max(vals))
    if "month" in s:
        return int(max(vals) * 12)
    # Assume annual if large number
    top = max(vals)
    if top > 1000:
        return int(top)
    return None


# ── Applied dedup ───────────────────────────────────────────────────────────

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


def is_applied(company: str, title: str, index: list[tuple[str, str]]) -> bool:
    """Check if this job was already applied to. Tightened to avoid over-suppression.

    Company match rules (must pass ONE):
      - Exact normalized match
      - One is a substring of the other, BUT only if the shorter side is ≥4 chars
        (prevents "td" matching everything containing "td")

    Title match rules:
      - Word overlap ≥ 60% of the LARGER set (not smaller — prevents short titles
        from matching everything)
      - OR exact normalized match
    """
    c = _norm_company(company)
    t = _norm_title(title)
    if not c or not t:
        return False
    t_words = set(t.split())
    for ac, at in index:
        # Company match: exact OR safe substring
        if c == ac:
            pass  # exact match
        elif len(ac) >= 4 and len(c) >= 4 and (ac in c or c in ac):
            pass  # substring match with min length guard
        else:
            continue

        # Title match: exact OR word overlap ≥ 60% of LARGER set
        if t == at:
            return True
        at_words = set(at.split())
        if not t_words or not at_words:
            continue
        overlap = t_words & at_words
        max_len = max(len(t_words), len(at_words))
        if max_len > 0 and len(overlap) / max_len >= 0.6:
            return True
    return False


# ── Hard reject gate ────────────────────────────────────────────────────────

def hard_reject(title: str, company: str, location: str, salary_text: str, snippet: str) -> str | None:
    """Return reject reason string, or None if passes."""
    # Title rejects
    if TITLE_REJECT_RE.search(title):
        return f"title_reject:{title}"

    # Location must be NYC
    loc = location.lower()
    if "remote" in loc and "hybrid" not in loc:
        return "remote_only"
    if NON_NYC_RE.search(location):
        return f"non_nyc:{location}"
    if not NYC_RE.search(location) and "ny" not in loc:
        return f"not_nyc:{location}"

    # Industry reject
    blob = f"{title} {company} {snippet}"
    if INDUSTRY_REJECT_RE.search(blob):
        # Allow financial regulators
        if not FIN_GOV_RE.search(blob):
            return f"wrong_industry:{company}"

    # Government reject unless financial regulator
    if GOV_RE.search(blob) and not FIN_GOV_RE.search(blob) and not FINSERV_RE.search(blob):
        return f"government:{company}"

    # Contract/seasonal/temp
    if CONTRACT_RE.search(title) or CONTRACT_RE.search(snippet[:200]):
        return "contract_temp"

    # 5+ years experience
    if EXP_REJECT_RE.search(snippet):
        return "5plus_years"

    # Salary floor: reject if max posted < $50k (generous floor for card data)
    annual = _parse_salary_annual(salary_text)
    if annual is not None and annual < 50000:
        return f"low_salary:{annual}"

    # Relevance gate: title must have at least one compliance/ops/finance signal.
    # Without this, broad Indeed queries like "middle office" or "operations analyst"
    # return software engineers, therapists, equity analysts, etc.
    title_lower = title.lower()
    has_relevance = ROLE_MATCH_RE.search(title) or any(kw in title_lower for kw in (
        "compliance", "aml", "kyc", "bsa", "regulatory", "sanctions",
        "financial crime", "anti-money", "onboarding", "account opening",
        "licensing", "registration", "finra", "surveillance",
        "operations analyst", "operations associate", "operations coordinator",
        "operations specialist", "middle office", "trade support",
        "fund operations", "fund accounting", "fund admin",
        "securities", "broker dealer", "broker-dealer",
        "risk analyst", "risk associate", "risk operations",
        "client service", "due diligence",
    ))
    if not has_relevance:
        return f"no_relevance:{title}"

    return None


# ── Scoring (0-100) ────────────────────────────────────────────────────────

def _is_top_firm(company: str) -> bool:
    c = company.lower()
    for firm in TOP_FIRMS:
        if firm in c or c in firm:
            return True
    return False


def score_job(title: str, company: str, snippet: str, salary_text: str) -> dict:
    """Score 0-100 based on hire probability for Robin's profile."""
    blob = f"{title} {company} {snippet}"

    # 40% role match
    role_score = 0
    role_matches = ROLE_MATCH_RE.findall(blob)
    if role_matches:
        role_score = min(100, 30 + len(role_matches) * 15)
    # Bonus for exact function matches
    title_lower = title.lower()
    if any(kw in title_lower for kw in ("compliance analyst", "compliance associate")):
        role_score = max(role_score, 90)
    elif any(kw in title_lower for kw in ("kyc analyst", "kyc associate", "aml analyst")):
        role_score = max(role_score, 85)
    elif any(kw in title_lower for kw in ("onboarding analyst", "onboarding associate", "client onboarding")):
        role_score = max(role_score, 85)
    elif any(kw in title_lower for kw in ("financial crime", "bsa analyst", "sanctions")):
        role_score = max(role_score, 80)
    elif any(kw in title_lower for kw in ("licensing", "registration")):
        role_score = max(role_score, 80)
    elif any(kw in title_lower for kw in ("regulatory operations", "compliance operations")):
        role_score = max(role_score, 85)
    elif any(kw in title_lower for kw in ("securities operations", "middle office", "trade support")):
        role_score = max(role_score, 70)
    elif any(kw in title_lower for kw in ("operations analyst", "operations associate")):
        role_score = max(role_score, 55)
    elif any(kw in title_lower for kw in ("risk analyst", "risk associate", "risk operations")):
        role_score = max(role_score, 60)
    elif any(kw in title_lower for kw in ("due diligence", "cdd analyst")):
        role_score = max(role_score, 75)
    elif any(kw in title_lower for kw in ("client service associate", "fund operations")):
        role_score = max(role_score, 50)
    # If no role match at all, very low
    if role_score == 0:
        role_score = 15

    # 30% realistic hire chance
    hire_score = 70  # baseline: entry-level compliance, Series 7, some experience
    if any(kw in title_lower for kw in ("associate", "analyst")):
        hire_score = 80
    if "specialist" in title_lower:
        hire_score = 70
    if "officer" in title_lower:
        hire_score = 50
    if "consultant" in title_lower:
        hire_score = 55
    # Penalty for stretch signals
    if re.search(r"\b[3-4]\+?\s*(?:years?|yrs?)", snippet, re.I):
        hire_score -= 15
    if re.search(r"\bcpa|cfa|cams|jd\b", snippet, re.I):
        hire_score -= 10
    # Bonus for "entry level" or "0-2 years"
    if re.search(r"\b(?:entry\s+level|0[-\s]?[12]\s*years?|1[-\s]?2\s*years?)\b", snippet, re.I):
        hire_score += 15
    hire_score = max(10, min(100, hire_score))

    # 20% firm prestige
    prestige_score = 50
    if _is_top_firm(company):
        prestige_score = 90
    elif FINSERV_RE.search(blob):
        prestige_score = 65

    # 10% salary
    annual = _parse_salary_annual(salary_text)
    salary_score = 40  # unknown
    if annual:
        if annual >= 120000:
            salary_score = 95
        elif annual >= 100000:
            salary_score = 85
        elif annual >= 80000:
            salary_score = 75
        elif annual >= 65000:
            salary_score = 60
        elif annual >= 50000:
            salary_score = 45
        else:
            salary_score = 25

    total = round(role_score * 0.40 + hire_score * 0.30 + prestige_score * 0.20 + salary_score * 0.10)
    total = max(0, min(100, total))

    # Determine tier
    if total >= 70:
        tier = "APPLY"
    elif total >= 50:
        tier = "MAYBE"
    else:
        tier = "SKIP"

    # One-line reason
    parts = []
    if role_score >= 80:
        parts.append("strong function match")
    elif role_score >= 55:
        parts.append("adjacent function")
    if _is_top_firm(company):
        parts.append("top-tier firm")
    if annual and annual >= 80000:
        parts.append(f"${annual:,}/yr")
    if hire_score >= 75:
        parts.append("realistic hire")
    elif hire_score < 50:
        parts.append("stretch on level")
    reason = "; ".join(parts) if parts else "review posting"

    return {
        "score": total,
        "tier": tier,
        "reason": reason,
        "role_score": role_score,
        "hire_score": hire_score,
        "prestige_score": prestige_score,
        "salary_score": salary_score,
    }


# ── Scrape ──────────────────────────────────────────────────────────────────

def scrape_indeed() -> list[dict]:
    all_jobs: list[dict] = []
    errors: list[str] = []
    seen_keys: set[str] = set()

    for query in QUERIES:
        for page in range(3):  # up to 3 pages per query
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
                errors.append(f"{query} p{page}: {exc}")
                break

            if BLOCKED_RE.search(html):
                errors.append(f"{query}: blocked")
                break

            page_jobs = []
            for m in JOB_CARD_RE.finditer(html):
                block = m.group(1)
                job_key = _extract(block, r'data-jk="([^"]+)"')
                title = _extract(block, r'id="jobTitle-[^"]+">(.*?)</span>')
                company = _extract(block, r'data-testid="company-name"[^>]*>(.*?)</span>')
                location = _extract(block, r'data-testid="text-location"[^>]*>(.*?)</div>')
                salary = _extract(block, r'salary-snippet-container.*?<span[^>]*>(.*?)</span>')
                snippet = _extract(block, r'data-testid="belowJobSnippet"[^>]*>(.*?)</div>')

                if not job_key or not title or not company:
                    continue
                if job_key in seen_keys:
                    continue
                seen_keys.add(job_key)

                page_jobs.append({
                    "job_key": job_key,
                    "title": title,
                    "company": company,
                    "location": location,
                    "salary": salary,
                    "snippet": snippet or title,
                    "url": f"https://www.indeed.com/viewjob?jk={job_key}",
                    "query": query,
                })

            all_jobs.extend(page_jobs)
            if len(page_jobs) < 8:
                break
            time.sleep(1.2)
        time.sleep(0.6)

    if errors:
        print(f"[scrape] {len(errors)} errors: {errors[:5]}")
    return all_jobs


# ── Pipeline ────────────────────────────────────────────────────────────────

def run() -> None:
    print("[1/5] Loading applied jobs index...")
    applied_index = load_applied_index()
    print(f"  Loaded {len(applied_index)} applied pairs")

    print("[2/5] Scraping Indeed ({} queries)...".format(len(QUERIES)))
    raw_jobs = scrape_indeed()
    print(f"  Scraped {len(raw_jobs)} raw jobs")

    print("[3/5] Hard reject gate + dedup...")
    passed: list[dict] = []
    reject_count = 0
    dedup_count = 0
    for job in raw_jobs:
        # Hard reject gate
        reason = hard_reject(job["title"], job["company"], job["location"], job["salary"], job["snippet"])
        if reason:
            reject_count += 1
            continue

        # Dedup against applied CSV
        if is_applied(job["company"], job["title"], applied_index):
            dedup_count += 1
            print(f"  DEDUP: Removed [{job['title']}] at [{job['company']}] — already applied")
            continue

        passed.append(job)

    print(f"  Passed: {len(passed)} | Rejected: {reject_count} | Deduped: {dedup_count}")

    print("[4/5] Scoring...")
    scored: list[dict] = []
    for job in passed:
        s = score_job(job["title"], job["company"], job["snippet"], job["salary"])
        job.update(s)
        scored.append(job)

    # Sort descending by score
    scored.sort(key=lambda x: x["score"], reverse=True)

    # Split tiers
    apply_jobs = [j for j in scored if j["tier"] == "APPLY"]
    maybe_jobs = [j for j in scored if j["tier"] == "MAYBE"]
    skip_jobs = [j for j in scored if j["tier"] == "SKIP"]

    print(f"  APPLY: {len(apply_jobs)} | MAYBE: {len(maybe_jobs)} | SKIP: {len(skip_jobs)}")

    # Final dedup check — verify none of the 10 specific applied jobs are in output
    check_names = [
        ("man group", "compliance associate"),
        ("nasdaq", "technical onboarding analyst"),
        ("capgemini", "financial crime compliance analyst"),
        ("jane street", "aml onboarding analyst"),
        ("ussa international", "compliance analyst"),
        ("current", "law enforcement compliance analyst"),
        ("rothschild", "global advisory legal compliance analyst"),
        ("whitecap", "kyc associate"),
        ("cardea group", "compliance and operations analyst"),
        ("anchorage digital", "member of kyc operations onboarding"),
    ]
    for job in apply_jobs + maybe_jobs:
        jc = job["company"].lower()
        jt = job["title"].lower()
        for cc, ct in check_names:
            if cc in jc and any(w in jt for w in ct.split()[:2]):
                print(f"  !! LEAK DETECTED: {job['title']} at {job['company']} — removing")
                job["tier"] = "SKIP"

    # Re-filter after leak check
    apply_jobs = [j for j in scored if j["tier"] == "APPLY"]
    maybe_jobs = [j for j in scored if j["tier"] == "MAYBE"]

    print("[5/5] Generating mobile report...")
    report_jobs = apply_jobs + maybe_jobs
    generate_report(report_jobs, {
        "total_scraped": len(raw_jobs),
        "rejected": reject_count,
        "deduped": dedup_count,
        "apply_count": len(apply_jobs),
        "maybe_count": len(maybe_jobs),
        "skip_count": len(skip_jobs),
        "generated": datetime.now().strftime("%Y-%m-%d %H:%M"),
    })
    print(f"  Report: {REPORT_OUT}")
    print(f"\n=== DONE === APPLY: {len(apply_jobs)} | MAYBE: {len(maybe_jobs)} | SKIP: {len(skip_jobs)}")


# ── Report HTML ─────────────────────────────────────────────────────────────

def _esc(s):
    import html
    return html.escape(str(s) if s else "")


def generate_report(jobs: list[dict], meta: dict) -> None:
    REPORT_OUT.parent.mkdir(parents=True, exist_ok=True)

    rows_html = ""
    for i, j in enumerate(jobs, 1):
        tier_class = "apply" if j["tier"] == "APPLY" else "maybe"
        sal = j.get("salary") or "Not listed"
        rows_html += f"""
        <div class="card {tier_class}" onclick="window.open('{_esc(j['url'])}','_blank')">
          <div class="card-header">
            <span class="rank">#{i}</span>
            <span class="score-badge {tier_class}">{j['score']}</span>
            <span class="tier-badge {tier_class}">{j['tier']}</span>
          </div>
          <div class="card-title">{_esc(j['title'])}</div>
          <div class="card-company">{_esc(j['company'])}</div>
          <div class="card-meta">
            <span>{_esc(j['location'])}</span>
            <span class="salary">{_esc(sal)}</span>
          </div>
          <div class="card-reason">{_esc(j['reason'])}</div>
          <a href="{_esc(j['url'])}" class="card-link" target="_blank" onclick="event.stopPropagation()">Open on Indeed →</a>
        </div>"""

    html_content = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1,maximum-scale=1">
<title>NYC Compliance Jobs — {meta['generated']}</title>
<style>
*{{box-sizing:border-box;margin:0;padding:0}}
body{{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif;background:#0f172a;color:#e2e8f0;padding:0;-webkit-font-smoothing:antialiased}}
.header{{background:linear-gradient(135deg,#1e293b,#334155);padding:20px 16px;position:sticky;top:0;z-index:10;border-bottom:1px solid #475569}}
.header h1{{font-size:18px;font-weight:700;color:#f8fafc;margin-bottom:4px}}
.header .stats{{font-size:13px;color:#94a3b8;display:flex;gap:12px;flex-wrap:wrap}}
.header .stats b{{color:#38bdf8}}
.filters{{display:flex;gap:8px;padding:12px 16px;overflow-x:auto;background:#1e293b;border-bottom:1px solid #334155}}
.filter-btn{{padding:8px 16px;border-radius:20px;border:1px solid #475569;background:transparent;color:#94a3b8;font-size:13px;font-weight:600;cursor:pointer;white-space:nowrap;-webkit-tap-highlight-color:transparent}}
.filter-btn.active{{background:#38bdf8;color:#0f172a;border-color:#38bdf8}}
.cards{{padding:12px;display:flex;flex-direction:column;gap:10px;max-width:680px;margin:0 auto}}
.card{{background:#1e293b;border-radius:14px;padding:16px;border:1px solid #334155;cursor:pointer;-webkit-tap-highlight-color:transparent;transition:border-color .15s}}
.card:active{{border-color:#38bdf8}}
.card-header{{display:flex;align-items:center;gap:8px;margin-bottom:8px}}
.rank{{font-size:13px;color:#64748b;font-weight:700}}
.score-badge{{font-size:14px;font-weight:800;padding:2px 10px;border-radius:12px}}
.score-badge.apply{{background:#065f46;color:#34d399}}
.score-badge.maybe{{background:#78350f;color:#fbbf24}}
.tier-badge{{font-size:11px;font-weight:700;padding:2px 8px;border-radius:10px;text-transform:uppercase;letter-spacing:.05em}}
.tier-badge.apply{{background:#022c22;color:#6ee7b7;border:1px solid #065f46}}
.tier-badge.maybe{{background:#451a03;color:#fcd34d;border:1px solid #78350f}}
.card-title{{font-size:16px;font-weight:700;color:#f1f5f9;line-height:1.3;margin-bottom:4px}}
.card-company{{font-size:14px;color:#38bdf8;font-weight:600;margin-bottom:6px}}
.card-meta{{display:flex;gap:12px;font-size:12px;color:#94a3b8;margin-bottom:8px;flex-wrap:wrap}}
.salary{{color:#a78bfa;font-weight:600}}
.card-reason{{font-size:13px;color:#cbd5e1;line-height:1.4;margin-bottom:8px;padding:8px 10px;background:#0f172a;border-radius:8px}}
.card-link{{display:inline-block;font-size:13px;color:#38bdf8;font-weight:600;padding:8px 0;text-decoration:none}}
.empty{{text-align:center;padding:40px 20px;color:#64748b;font-size:15px}}
</style>
</head>
<body>

<div class="header">
  <h1>NYC Compliance Discovery</h1>
  <div class="stats">
    <span>Scraped <b>{meta['total_scraped']}</b></span>
    <span>APPLY <b>{meta['apply_count']}</b></span>
    <span>MAYBE <b>{meta['maybe_count']}</b></span>
    <span>Rejected <b>{meta['rejected']}</b></span>
    <span>Deduped <b>{meta['deduped']}</b></span>
    <span>{meta['generated']}</span>
  </div>
</div>

<div class="filters">
  <button class="filter-btn active" onclick="filterTier('all',this)">All ({meta['apply_count'] + meta['maybe_count']})</button>
  <button class="filter-btn" onclick="filterTier('APPLY',this)">Apply ({meta['apply_count']})</button>
  <button class="filter-btn" onclick="filterTier('MAYBE',this)">Maybe ({meta['maybe_count']})</button>
</div>

<div class="cards" id="cards">
{rows_html}
</div>

<script>
function filterTier(tier, btn) {{
  document.querySelectorAll('.filter-btn').forEach(b => b.classList.remove('active'));
  btn.classList.add('active');
  document.querySelectorAll('.card').forEach(c => {{
    if (tier === 'all') {{ c.style.display = ''; return; }}
    c.style.display = c.classList.contains(tier.toLowerCase()) ? '' : 'none';
  }});
}}
</script>

</body>
</html>"""

    REPORT_OUT.write_text(html_content, encoding="utf-8")


if __name__ == "__main__":
    run()
