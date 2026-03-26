"""Google Jobs scraper via SerpAPI-like public search.

Uses Google's job search with structured data extraction.
Searches google.com/search?q={query}+jobs+NYC&ibp=htl;jobs
"""
from __future__ import annotations

import json
import random
import re
import time
import urllib.error
import urllib.parse
import urllib.request
from typing import Any

# ═══════════════════════════════════════════════════════════════════════════════
# QUERIES — same core set, formatted for Google
# ═══════════════════════════════════════════════════════════════════════════════

QUERIES = [
    "compliance analyst NYC",
    "compliance associate New York",
    "KYC analyst NYC",
    "AML analyst New York",
    "onboarding analyst financial services NYC",
    "trade operations analyst New York",
    "securities operations NYC",
    "broker dealer compliance New York",
    "clearing operations analyst NYC",
    "middle office analyst New York",
    "regulatory operations NYC",
    "fund operations analyst New York",
    "compliance coordinator NYC",
    "licensing registration analyst New York",
    "trade support analyst NYC",
    "sanctions analyst New York",
]

_UA_LIST = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
]

NYC_RE = re.compile(
    r"\b(?:new\s+york|nyc|manhattan|brooklyn|queens|bronx|staten\s+island"
    r"|midtown|downtown|financial\s+district|wall\s+street)\b", re.I)

NON_NYC_RE = re.compile(
    r"\b(?:jersey\s+city|hoboken|newark|stamford|white\s+plains"
    r"|greenwich|parsippany|morristown)\b", re.I)


def _fetch_google_jobs(query: str) -> list[dict]:
    """Fetch job listings from Google Jobs search."""
    # Google Jobs uses a special endpoint that returns structured data
    encoded = urllib.parse.quote(query)
    url = f"https://www.google.com/search?q={encoded}+jobs&ibp=htl;jobs&hl=en"

    headers = {
        "User-Agent": random.choice(_UA_LIST),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
    }

    req = urllib.request.Request(url, headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=12) as resp:
            html = resp.read().decode("utf-8", errors="replace")
    except Exception:
        return []

    # Extract job data from Google's structured data
    jobs = []

    # Google embeds job data in script tags as JSON-LD or in data attributes
    # Try to find job listings in the HTML
    # Pattern: data-encoded-doc or class="BjJfJf" job cards
    title_pattern = re.compile(r'<div class="BjJfJf[^"]*"[^>]*>(.*?)</div>', re.S)
    company_pattern = re.compile(r'<div class="vNEEBe"[^>]*>(.*?)</div>', re.S)
    location_pattern = re.compile(r'<div class="Qk80Jf"[^>]*>(.*?)</div>', re.S)

    titles = title_pattern.findall(html)
    companies = company_pattern.findall(html)
    locations = location_pattern.findall(html)

    # Also try to extract from JSON-LD
    jsonld_pattern = re.compile(r'<script type="application/ld\+json">(.*?)</script>', re.S)
    for match in jsonld_pattern.findall(html):
        try:
            data = json.loads(match)
            if isinstance(data, dict) and data.get("@type") == "JobPosting":
                loc = data.get("jobLocation", {})
                if isinstance(loc, dict):
                    addr = loc.get("address", {})
                    loc_text = addr.get("addressLocality", "") if isinstance(addr, dict) else ""
                elif isinstance(loc, list) and loc:
                    addr = loc[0].get("address", {})
                    loc_text = addr.get("addressLocality", "") if isinstance(addr, dict) else ""
                else:
                    loc_text = ""

                jobs.append({
                    "title": _strip_html(data.get("title", "")),
                    "company": _strip_html(data.get("hiringOrganization", {}).get("name", "")),
                    "location": loc_text,
                    "url": data.get("url", ""),
                    "salary": data.get("baseSalary", {}).get("value", {}).get("value", "") if isinstance(data.get("baseSalary"), dict) else "",
                    "snippet": _strip_html(data.get("description", ""))[:500],
                    "source": "google_jobs",
                })
            elif isinstance(data, list):
                for item in data:
                    if isinstance(item, dict) and item.get("@type") == "JobPosting":
                        jobs.append({
                            "title": _strip_html(item.get("title", "")),
                            "company": _strip_html(item.get("hiringOrganization", {}).get("name", "")),
                            "location": item.get("jobLocation", {}).get("address", {}).get("addressLocality", ""),
                            "url": item.get("url", ""),
                            "salary": "",
                            "snippet": _strip_html(item.get("description", ""))[:500],
                            "source": "google_jobs",
                        })
        except (json.JSONDecodeError, TypeError, AttributeError):
            continue

    # Fallback: parse HTML cards
    for i in range(min(len(titles), len(companies))):
        title = _strip_html(titles[i])
        company = _strip_html(companies[i])
        location = _strip_html(locations[i]) if i < len(locations) else ""
        if title and company:
            jobs.append({
                "title": title,
                "company": company,
                "location": location,
                "url": "",
                "salary": "",
                "snippet": "",
                "source": "google_jobs",
            })

    return jobs


def _strip_html(text: str) -> str:
    return re.sub(r"<[^>]+>", "", text).strip()


def scrape_all() -> tuple[list[dict], dict]:
    """Run all Google Jobs queries, filter to NYC, return formatted jobs."""
    audit = {
        "total_raw": 0,
        "nyc": 0,
        "errors": 0,
        "queries_run": 0,
    }

    all_jobs: list[dict] = []
    seen: set[str] = set()

    shuffled = list(QUERIES)
    random.shuffle(shuffled)

    for query in shuffled:
        audit["queries_run"] += 1
        try:
            results = _fetch_google_jobs(query)
        except Exception:
            audit["errors"] += 1
            continue

        audit["total_raw"] += len(results)

        for job in results:
            title = job.get("title", "")
            company = job.get("company", "")
            location = job.get("location", "")

            # Dedup
            key = f"{company.lower().strip()}|{title.lower().strip()}"
            if key in seen:
                continue
            seen.add(key)

            # NYC filter
            loc_blob = f"{location} {title}"
            if NON_NYC_RE.search(loc_blob):
                continue
            if not NYC_RE.search(loc_blob) and "ny" not in location.lower():
                continue

            audit["nyc"] += 1
            all_jobs.append(job)

        time.sleep(random.uniform(1.5, 3.0))

    return all_jobs, audit


if __name__ == "__main__":
    jobs, audit = scrape_all()
    print(f"Google Jobs: {audit['nyc']} NYC from {audit['total_raw']} raw ({audit['errors']} errors)")
    for j in jobs[:10]:
        print(f"  {j['company']:30s} | {j['title'][:50]}")
