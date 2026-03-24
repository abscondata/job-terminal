"""
eFinancialCareers scraper for NYC finance compliance/risk roles.

Scrapes the public search interface. Parses JSON-LD structured data
(JobPosting schema) when available, falls back to HTML parsing.
"""
from __future__ import annotations

import json
import re
import time
import urllib.error
import urllib.parse
import urllib.request

DOMAIN = "www.efinancialcareers.com"

QUERIES = [
    "compliance analyst",
    "compliance associate",
    "KYC analyst",
    "AML analyst",
    "regulatory compliance",
    "financial crime analyst",
    "sanctions analyst",
    "trade surveillance",
    "onboarding analyst",
    "risk analyst",
    "controls analyst",
    "securities operations",
    "middle office",
    "fund operations compliance",
    "BSA analyst",
    "broker dealer compliance",
    "due diligence analyst",
]

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.google.com/",
}

SENIOR_REJECT = re.compile(
    r"\b(?:vice\s+president|(?<!\w)vp\b|avp\b|svp\b|director|head\s+of|chief"
    r"|principal|managing\s+director|intern\b|internship)\b",
    re.I,
)

NYC_RE = re.compile(
    r"\b(?:new\s+york|nyc|manhattan|brooklyn|jersey\s+city)\b",
    re.I,
)

# JSON-LD JobPosting extractor
JSONLD_RE = re.compile(
    r'<script[^>]+type=["\']application/ld\+json["\'][^>]*>(.*?)</script>',
    re.I | re.S,
)

# HTML card patterns (fallback)
JOB_CARD_RE = re.compile(
    r'<article[^>]*class="[^"]*job[^"]*"[^>]*>(.*?)</article>',
    re.I | re.S,
)
TITLE_RE = re.compile(r'<a[^>]*class="[^"]*job-title[^"]*"[^>]*>(.*?)</a>', re.I | re.S)
COMPANY_RE = re.compile(r'<a[^>]*class="[^"]*company[^"]*"[^>]*>(.*?)</a>', re.I | re.S)
LOCATION_RE = re.compile(r'<span[^>]*class="[^"]*location[^"]*"[^>]*>(.*?)</span>', re.I | re.S)
LINK_RE = re.compile(r'<a[^>]*href="([^"]*job[^"]*)"[^>]*class="[^"]*job-title', re.I)


def _clean(text: str) -> str:
    t = re.sub(r"<[^>]+>", " ", text or "")
    t = re.sub(r"&\w+;", " ", t)
    return re.sub(r"\s+", " ", t).strip()


def _fetch(url: str, timeout: int = 20) -> str:
    req = urllib.request.Request(url, headers=HEADERS)
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return resp.read().decode("utf-8", errors="replace")


def _parse_jsonld(html: str) -> list[dict]:
    """Extract job postings from JSON-LD structured data."""
    jobs = []
    for m in JSONLD_RE.finditer(html):
        try:
            data = json.loads(m.group(1).strip())
        except (json.JSONDecodeError, ValueError):
            continue

        items = data if isinstance(data, list) else [data]
        for item in items:
            if not isinstance(item, dict):
                continue
            if str(item.get("@type", "")).lower() != "jobposting":
                continue

            hiring = item.get("hiringOrganization") or {}
            location = item.get("jobLocation") or {}
            if isinstance(location, list):
                location = location[0] if location else {}
            address = location.get("address") or {}

            title = _clean(item.get("title", ""))
            company = _clean(hiring.get("name", ""))
            loc = _clean(
                address.get("addressLocality", "")
                or address.get("addressRegion", "")
                or item.get("jobLocationType", "")
            )
            desc = _clean(item.get("description", ""))
            url = item.get("url", "")

            if title and company:
                jobs.append({
                    "title": title,
                    "company": company,
                    "location": loc,
                    "description": desc[:500],
                    "url": url,
                })
    return jobs


def _parse_html_cards(html: str) -> list[dict]:
    """Fallback: extract jobs from HTML cards."""
    jobs = []
    for card_match in JOB_CARD_RE.finditer(html):
        card = card_match.group(1)
        title_m = TITLE_RE.search(card)
        company_m = COMPANY_RE.search(card)
        loc_m = LOCATION_RE.search(card)
        link_m = LINK_RE.search(card)

        title = _clean(title_m.group(1)) if title_m else ""
        company = _clean(company_m.group(1)) if company_m else ""
        location = _clean(loc_m.group(1)) if loc_m else ""
        url = link_m.group(1) if link_m else ""

        if title and company:
            if url and not url.startswith("http"):
                url = f"https://{DOMAIN}{url}"
            jobs.append({
                "title": title,
                "company": company,
                "location": location,
                "description": title,
                "url": url,
            })
    return jobs


def scrape() -> tuple[list[dict], dict]:
    """Scrape eFinancialCareers for NYC compliance roles."""
    all_jobs: list[dict] = []
    seen_keys: set[str] = set()
    audit = {"queries": len(QUERIES), "raw": 0, "unique": 0, "nyc": 0,
             "errors": 0, "blocked": 0}

    for query in QUERIES:
        try:
            url = f"https://{DOMAIN}/search?" + urllib.parse.urlencode({
                "q": query,
                "location": "New York, NY",
                "radius": "25",
            })
            html = _fetch(url)
        except urllib.error.HTTPError as e:
            if e.code in (403, 429):
                audit["blocked"] += 1
            else:
                audit["errors"] += 1
            time.sleep(2.0)
            continue
        except Exception:
            audit["errors"] += 1
            time.sleep(1.0)
            continue

        # Try JSON-LD first, fall back to HTML cards
        page_jobs = _parse_jsonld(html)
        if not page_jobs:
            page_jobs = _parse_html_cards(html)

        for job in page_jobs:
            audit["raw"] += 1
            title = job["title"]
            company = job["company"]
            location = job["location"]

            # Dedup
            key = f"{company.lower()}|{title.lower()}"
            if key in seen_keys:
                continue
            seen_keys.add(key)

            # Hard reject
            if SENIOR_REJECT.search(title):
                continue

            # NYC filter
            loc_blob = f"{location} {job['description'][:200]}"
            if not NYC_RE.search(loc_blob) and "ny" not in location.lower():
                continue

            audit["unique"] += 1
            audit["nyc"] += 1

            all_jobs.append({
                "job_key": f"efc_{hash(key) & 0xFFFFFFFF:08x}",
                "title": title,
                "company": company,
                "location": location or "New York, NY",
                "salary": "",
                "snippet": job["description"][:400],
                "url": job["url"],
                "source": "efinancialcareers",
                "query": query,
            })

        time.sleep(1.5)

    return all_jobs, audit


if __name__ == "__main__":
    jobs, audit = scrape()
    print(f"eFinancialCareers: {len(jobs)} NYC jobs ({audit['raw']} raw, "
          f"{audit['errors']} errors, {audit['blocked']} blocked)")
    for j in jobs[:10]:
        print(f"  {j['company']:30s} | {j['title'][:50]}")
