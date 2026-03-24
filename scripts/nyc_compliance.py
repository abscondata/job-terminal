"""NYC compliance/operations/KYC/onboarding job discovery via Indeed."""
from __future__ import annotations

import re
import urllib.error

from scripts.applied_dedup import build_applied_index, is_already_applied
from scripts.discovery_sources import clean_text, dedupe_jobs, fetch_text, pause

INDEED_DOMAIN = "www.indeed.com"
INDEED_LOCATION = "New York, NY"

QUERIES = [
    # Core compliance
    "compliance analyst",
    "compliance associate",
    "compliance associate broker dealer",
    "compliance analyst bank",
    "compliance associate asset management",
    "compliance associate private equity",
    "compliance associate hedge fund",
    "compliance operations analyst",
    # KYC / CDD / Onboarding
    "KYC analyst",
    "KYC associate",
    "CDD analyst",
    "client onboarding analyst",
    "client onboarding associate",
    "account opening analyst",
    "onboarding analyst financial",
    # AML
    "AML analyst",
    "AML associate",
    "AML KYC analyst",
    "financial crime analyst",
    # Securities operations
    "securities operations analyst",
    "broker dealer operations",
    "BD operations analyst",
    # Licensing & registration
    "licensing registration analyst",
    "licensing analyst FINRA",
    "registration analyst broker dealer",
    # Regulatory operations
    "regulatory operations analyst",
    "regulatory compliance associate",
    "regulatory affairs analyst financial",
    # Compliance operations
    "compliance operations",
    "compliance coordinator",
]

BLOCKED_RE = re.compile(
    r"Authenticating\.\.\.|bot-detection-anonymous|Additional Verification Required",
    re.I,
)

# Hard reject titles — per user spec
SKIP_TITLE_RE = re.compile(
    r"\b(?:"
    # Seniority — reject senior/VP/AVP/Director/Manager/Lead/Supervisor
    r"senior\s+(?:analyst|associate|officer|specialist|manager|director|vice|compliance|aml|kyc)"
    r"|(?:vice\s+president|vp\b|avp\b|svp\b|director|head\s+of|chief|principal"
    r"|manager|lead\s+(?:analyst|associate|officer|specialist)|supervisor)"
    # Wrong profession
    r"|insurance|actuary|underwriter|claims|nurse|physician|pharmacist"
    r"|attorney|counsel|paralegal"
    # Sales / commission
    r"|call center|customer service rep|commission.based|cold calling|sales representative"
    r"|business development representative|account executive"
    # IT / cybersecurity
    r"|information security|cybersecurity|soc\s*2|nist|iso\s*27001|it\s+compliance|it\s+risk"
    # Audit as primary
    r"|internal auditor|audit manager|examination|examiner"
    # Regulatory reporting
    r"|regulatory reporting|cftc|trade reporting|capital adequacy"
    # Data science / analytics
    r"|data scientist|data engineer|machine learning"
    # iGaming
    r"|igaming|sports betting|online casino|online gambling|sportsbook"
    # Internship
    r"|intern\b|internship"
    # Contract / temp / freelance
    r"|freelance|contractor|temporary|temp\b"
    r")\b",
    re.I,
)

SKIP_INDUSTRY_RE = re.compile(
    r"\b(?:insurance company|health insurance|life insurance"
    r"|pharmaceutical|biotech|healthcare"
    r"|sports betting|igaming|online casino|online gambling"
    r"|cybersecurity firm|information security)\b",
    re.I,
)

# Require 0-4 years experience (skip 5+)
HIGH_EXP_RE = re.compile(
    r"\b(?:[5-9]|1[0-9]|20)\+?\s*(?:years?|yrs?)\s*(?:of\s+)?(?:experience|exp)\b",
    re.I,
)

# Reject locations outside NYC 5 boroughs
NON_NYC_RE = re.compile(
    r"\b(?:jersey\s+city|white\s+plains|stamford|westchester|coral\s+gables"
    r"|newark|hoboken|parsippany|morristown|woodbridge|iselin"
    r"|greenwich|darien|norwalk|bridgeport"
    r"|harrison|purchase|armonk|tarrytown|yonkers"
    r"|jericho|melville|uniondale|garden\s+city"
    r"|princeton|florham\s+park|short\s+hills)\b",
    re.I,
)

# Reject language requirements
LANGUAGE_REQ_RE = re.compile(
    r"\b(?:fluent\s+(?:french|spanish|mandarin|cantonese|japanese|korean|portuguese|arabic)"
    r"|native\s+(?:french|spanish|mandarin|cantonese)"
    r"|bilingual\s+(?:french|spanish|mandarin|cantonese))\b",
    re.I,
)

# Skip data/analytics-primary roles
DATA_PRIMARY_RE = re.compile(
    r"\b(?:sql|python|r\b|tableau|power\s+bi|data\s+(?:analyst|scientist|engineer|analytics))\b.*"
    r"\b(?:required|must\s+have|proficiency|expertise)\b",
    re.I,
)

JOB_CARD_RE = re.compile(
    r'<div class="job_seen_beacon">(.*?)(?=<div class="job_seen_beacon"|$)', re.S
)


def _extract(block: str, pattern: str) -> str:
    match = re.search(pattern, block, re.S)
    return clean_text(match.group(1)) if match else ""


def _parse_page(html_text: str) -> list[dict]:
    jobs: list[dict] = []
    for match in JOB_CARD_RE.finditer(html_text):
        block = match.group(1)
        job_key = _extract(block, r'data-jk="([^"]+)"')
        title = _extract(block, r'id="jobTitle-[^"]+">(.*?)</span>')
        company = _extract(block, r'data-testid="company-name"[^>]*>(.*?)</span>')
        location = _extract(block, r'data-testid="text-location"[^>]*>(.*?)</div>')
        salary = _extract(block, r'salary-snippet-container.*?<span[^>]*>(.*?)</span>')
        snippet = _extract(block, r'data-testid="belowJobSnippet"[^>]*>(.*?)</div>')

        if not job_key or not title or not company:
            continue

        # Skip unwanted titles
        if SKIP_TITLE_RE.search(title):
            continue

        # Skip unwanted industries in snippet
        if snippet and SKIP_INDUSTRY_RE.search(snippet):
            continue

        # Skip high experience requirements in snippet
        if snippet and HIGH_EXP_RE.search(snippet):
            continue

        # Check location is NYC area (not remote-only, not suburbs)
        loc_lower = location.lower()
        if "remote" in loc_lower and "hybrid" not in loc_lower:
            continue

        # Reject non-NYC locations (Jersey City, White Plains, Stamford, etc.)
        if NON_NYC_RE.search(location):
            continue

        # Must contain NYC-area signals
        nyc_signals = re.compile(
            r"\b(?:new\s+york|nyc|manhattan|brooklyn|queens|bronx|staten\s+island"
            r"|long\s+island\s+city|midtown|downtown|financial\s+district"
            r"|wall\s+street|tribeca|soho|flatiron|times\s+square)\b",
            re.I,
        )
        if not nyc_signals.search(location) and "ny" not in loc_lower:
            continue

        url = f"https://www.indeed.com/viewjob?jk={job_key}"
        jobs.append(
            {
                "source": "nyc_compliance",
                "company": company,
                "title": title,
                "location_text": location,
                "url": url,
                "apply_url": url,
                "description_text": snippet or title,
                "compensation_text": salary or None,
                "source_job_id": f"nyc_compliance_{job_key}",
                "remote_hint": "hybrid" if "hybrid" in loc_lower else "onsite",
                "raw": {"job_key": job_key, "html": block[:4000]},
            }
        )
    return jobs


def scrape(
    max_pages: int = 5,
    quick: bool = False,
    *,
    city_scope: str = "all",
    applied_csv: str | None = None,
) -> list[dict]:
    """Scrape Indeed for NYC compliance/ops roles, dedup against applied jobs."""
    applied_index = build_applied_index(applied_csv)
    out: list[dict] = []
    errors: list[str] = []
    pages = 2 if quick else max_pages

    queries = QUERIES[::3] if quick else QUERIES

    for query in queries:
        for page in range(pages):
            try:
                html_text = fetch_text(
                    f"https://{INDEED_DOMAIN}/jobs",
                    params={
                        "q": query,
                        "l": INDEED_LOCATION,
                        "fromage": "14",
                        "sort": "date",
                        "start": str(page * 10),
                    },
                    timeout=20,
                )
            except urllib.error.HTTPError as exc:
                errors.append(f"NYC compliance {query}: HTTP {exc.code}")
                break
            except Exception as exc:
                errors.append(f"NYC compliance {query}: {type(exc).__name__}: {exc}")
                break

            if BLOCKED_RE.search(html_text):
                errors.append(f"NYC compliance {query}: blocked_by_bot_detection")
                break

            jobs = _parse_page(html_text)
            if not jobs:
                break
            out.extend(jobs)
            if len(jobs) < 8:
                break
            pause(1.2)
        pause(0.8)

    out = dedupe_jobs(out)

    # Dedup against applied jobs
    if applied_index:
        before = len(out)
        out = [
            j for j in out
            if not is_already_applied(j["company"], j["title"], applied_index)
        ]
        deduped = before - len(out)
        if deduped:
            print(f"[nyc_compliance] deduped {deduped} already-applied roles")

    if not out and errors:
        raise RuntimeError("; ".join(errors[:6]))
    return out


if __name__ == "__main__":
    results = scrape()
    print(f"NYC Compliance: {len(results)} jobs")
    for r in results[:5]:
        print(f"  {r['company']} | {r['title']} | {r['location_text']}")
