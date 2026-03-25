"""
LinkedIn Jobs scraper using the guest/public API.

No login required. Uses the public jobs-guest API that returns HTML fragments
of job cards, plus the public job detail pages for descriptions.
"""
from __future__ import annotations

import json
import random
import re
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent

# ═══════════════════════════════════════════════════════════════════════════════
# SEARCH QUERIES — all NYC-targeted
# ═══════════════════════════════════════════════════════════════════════════════

QUERIES = [
    # Core compliance
    "compliance analyst",
    "compliance associate",
    "compliance officer",
    "compliance specialist",
    # KYC / AML
    "KYC analyst",
    "KYC associate",
    "AML analyst",
    "AML associate",
    # Onboarding
    "onboarding analyst",
    "onboarding specialist",
    # Operations
    "operations analyst financial services",
    "operations associate broker dealer",
    "securities operations",
    "fund operations analyst",
    "trade operations",
    # Regulatory
    "licensing registration analyst",
    "regulatory operations",
    "regulatory reporting analyst",
    # Surveillance / Sanctions / Controls
    "sanctions analyst",
    "surveillance analyst",
    "controls analyst",
    # Due diligence
    "due diligence analyst",
    "due diligence associate",
    # BD-specific
    "FINRA compliance",
    "broker dealer compliance",
    "Series 7",
]

LOCATION = "New York, New York, United States"

# ═══════════════════════════════════════════════════════════════════════════════
# HTTP HELPERS
# ═══════════════════════════════════════════════════════════════════════════════

_USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:132.0) Gecko/20100101 Firefox/132.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.1 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36 Edg/131.0.0.0",
]


def _headers() -> dict:
    return {
        "User-Agent": random.choice(_USER_AGENTS),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "identity",
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "none",
        "DNT": "1",
    }


def _fetch(url: str, timeout: int = 15) -> str:
    req = urllib.request.Request(url, headers=_headers())
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.read().decode("utf-8", errors="replace")
    except urllib.error.HTTPError as e:
        if e.code == 429:
            time.sleep(random.uniform(10, 20))
            req2 = urllib.request.Request(url, headers=_headers())
            with urllib.request.urlopen(req2, timeout=timeout) as resp:
                return resp.read().decode("utf-8", errors="replace")
        raise


def _clean(text: str) -> str:
    t = re.sub(r"<[^>]+>", " ", text)
    t = re.sub(r"&\w+;", " ", t)
    t = re.sub(r"&#x[0-9a-fA-F]+;", " ", t)
    t = re.sub(r"&\#\d+;", " ", t)
    return re.sub(r"\s+", " ", t).strip()


# ═══════════════════════════════════════════════════════════════════════════════
# SEARCH API — paginated
# ═══════════════════════════════════════════════════════════════════════════════

_CARD_RE = re.compile(
    r'<div[^>]*class="[^"]*base-card[^"]*"[^>]*>(.*?)</div>\s*</div>\s*</div>',
    re.S,
)
_TITLE_RE = re.compile(r'<h3[^>]*base-search-card__title[^>]*>(.*?)</h3>', re.S)
_COMPANY_RE = re.compile(r'<h4[^>]*base-search-card__subtitle[^>]*>(.*?)</h4>', re.S)
_LOCATION_RE = re.compile(r'<span[^>]*job-search-card__location[^>]*>(.*?)</span>', re.S)
_DATE_RE = re.compile(r'<time[^>]*datetime="([^"]*)"', re.S)
_LINK_RE = re.compile(r'<a[^>]*class="base-card__full-link[^"]*"[^>]*href="([^"]*)"', re.S)
_JOB_ID_RE = re.compile(r'/view/[^/]*-(\d+)\?')
# Alternate ID pattern
_JOB_ID_ALT_RE = re.compile(r'data-entity-urn="urn:li:jobPosting:(\d+)"')


def _parse_search_page(html: str) -> list[dict]:
    """Parse LinkedIn job search HTML into job dicts."""
    jobs = []

    # Extract all titles, companies, locations, links
    titles = _TITLE_RE.findall(html)
    companies = _COMPANY_RE.findall(html)
    locations = _LOCATION_RE.findall(html)
    dates = _DATE_RE.findall(html)
    links = _LINK_RE.findall(html)

    # Also try to find job IDs from entity URNs
    all_ids = _JOB_ID_ALT_RE.findall(html)

    n = min(len(titles), len(companies), len(locations))
    for i in range(n):
        title = _clean(titles[i])
        company = _clean(companies[i])
        location = _clean(locations[i])
        date = dates[i] if i < len(dates) else ""
        link = links[i].split("?")[0] if i < len(links) else ""

        # Extract job ID
        job_id = ""
        if link:
            m = _JOB_ID_RE.search(link)
            if m:
                job_id = m.group(1)
        if not job_id and i < len(all_ids):
            job_id = all_ids[i]

        if not title or not company:
            continue

        jobs.append({
            "title": title,
            "company": company,
            "location": location,
            "date_posted": date,
            "url": link or f"https://www.linkedin.com/jobs/view/{job_id}" if job_id else "",
            "job_id": job_id,
        })

    return jobs


def search_linkedin(query: str, max_pages: int = 4) -> list[dict]:
    """Search LinkedIn for jobs matching query. Returns up to max_pages * 25 results."""
    all_jobs = []
    encoded_q = urllib.parse.quote(query)
    encoded_loc = urllib.parse.quote(LOCATION)

    for page in range(max_pages):
        start = page * 25
        url = (
            f"https://www.linkedin.com/jobs-guest/jobs/api/seeMoreJobPostings/search"
            f"?keywords={encoded_q}"
            f"&location={encoded_loc}"
            f"&f_TPR=r604800"  # Last 7 days
            f"&f_E=2%2C3"     # Entry + Associate level
            f"&f_JT=F"        # Full-time
            f"&start={start}"
        )

        try:
            html = _fetch(url)
        except Exception as e:
            break

        page_jobs = _parse_search_page(html)
        all_jobs.extend(page_jobs)

        if len(page_jobs) < 20:  # Less than a full page = last page
            break

        time.sleep(random.uniform(1.5, 3.0))

    return all_jobs


# ═══════════════════════════════════════════════════════════════════════════════
# JOB DETAIL FETCHER — get full description for scoring
# ═══════════════════════════════════════════════════════════════════════════════

_DESC_RE = re.compile(
    r'<div[^>]*class="[^"]*description__text[^"]*"[^>]*>(.*?)</div>',
    re.S,
)
_SALARY_RE = re.compile(
    r'<span[^>]*class="[^"]*compensation[^"]*"[^>]*>(.*?)</span>',
    re.S,
)
_EASY_APPLY_RE = re.compile(r'Easy\s+Apply', re.I)


def fetch_job_detail(job_id: str) -> dict:
    """Fetch full job description from LinkedIn public page."""
    if not job_id:
        return {}

    url = f"https://www.linkedin.com/jobs-guest/jobs/api/jobPosting/{job_id}"
    try:
        html = _fetch(url, timeout=12)
    except Exception:
        return {}

    result = {}

    # Description
    m = _DESC_RE.search(html)
    if m:
        result["description"] = _clean(m.group(1))[:2000]

    # Salary
    m = _SALARY_RE.search(html)
    if m:
        result["salary"] = _clean(m.group(1))

    # Easy Apply
    result["easy_apply"] = bool(_EASY_APPLY_RE.search(html))

    return result


# ═══════════════════════════════════════════════════════════════════════════════
# MAIN SCRAPER — runs all queries, dedupes, fetches descriptions
# ═══════════════════════════════════════════════════════════════════════════════

def scrape_all(max_pages_per_query: int = 3,
               max_detail_fetches: int = 150) -> tuple[list[dict], dict]:
    """Run full LinkedIn scrape. Returns (jobs, audit).

    1. Run all search queries with pagination
    2. Dedupe on job_id
    3. Fetch descriptions for top candidates
    4. Return formatted job dicts ready for pipeline
    """
    audit = {
        "queries_run": 0,
        "queries_with_results": 0,
        "total_raw": 0,
        "unique_after_dedup": 0,
        "descriptions_fetched": 0,
        "errors": 0,
        "blocked": 0,
        "query_detail": {},
    }

    # Phase 1: Search all queries
    raw_jobs: list[dict] = []
    seen_ids: set[str] = set()

    # Shuffle to avoid always hitting same queries first
    shuffled = list(QUERIES)
    random.shuffle(shuffled)

    for query in shuffled:
        audit["queries_run"] += 1
        try:
            results = search_linkedin(query, max_pages=max_pages_per_query)
        except Exception as e:
            audit["errors"] += 1
            audit["query_detail"][query] = {"error": str(e)[:60]}
            continue

        query_unique = 0
        for job in results:
            audit["total_raw"] += 1
            jid = job.get("job_id", "")
            if not jid:
                # Generate a fallback key from company+title
                jid = f"{job['company']}|{job['title']}"
            if jid in seen_ids:
                continue
            seen_ids.add(jid)
            query_unique += 1
            job["query"] = query
            raw_jobs.append(job)

        if query_unique > 0:
            audit["queries_with_results"] += 1
        audit["query_detail"][query] = {"results": len(results), "unique": query_unique}

        time.sleep(random.uniform(2.0, 4.0))

    audit["unique_after_dedup"] = len(raw_jobs)
    print(f"  LinkedIn search: {audit['total_raw']} raw -> {len(raw_jobs)} unique from {audit['queries_run']} queries")

    # Phase 2: Fetch descriptions for scoring (rate-limited)
    fetched = 0
    for job in raw_jobs:
        if fetched >= max_detail_fetches:
            break
        jid = job.get("job_id", "")
        if not jid or not jid.isdigit():
            continue
        try:
            detail = fetch_job_detail(jid)
            if detail.get("description"):
                job["description"] = detail["description"]
                job["easy_apply"] = detail.get("easy_apply", False)
                if detail.get("salary"):
                    job["salary"] = detail["salary"]
                fetched += 1
        except Exception:
            audit["errors"] += 1
        time.sleep(random.uniform(1.0, 2.0))

    audit["descriptions_fetched"] = fetched
    print(f"  LinkedIn details: {fetched} descriptions fetched")

    # Phase 3: Format for pipeline
    formatted: list[dict] = []
    for job in raw_jobs:
        desc = job.get("description", "")
        formatted.append({
            "job_key": f"li_{job.get('job_id', '')}",
            "title": job["title"],
            "company": job["company"],
            "location": job["location"],
            "salary": job.get("salary", ""),
            "snippet": desc[:400] if desc else job["title"],
            "url": job.get("url", ""),
            "source": "linkedin",
            "query": job.get("query", ""),
            "date_posted": job.get("date_posted", ""),
            "easy_apply": job.get("easy_apply", False),
        })

    return formatted, audit


if __name__ == "__main__":
    jobs, audit = scrape_all(max_pages_per_query=2, max_detail_fetches=10)
    print(f"\nTotal: {len(jobs)} jobs")
    print(f"Audit: {json.dumps({k: v for k, v in audit.items() if k != 'query_detail'}, indent=2)}")
    for j in jobs[:10]:
        print(f"  {j['company']:30s} | {j['title'][:50]:50s} | {j['location'][:30]}")
