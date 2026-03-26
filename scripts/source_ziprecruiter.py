"""ZipRecruiter job scraper for NYC compliance/ops roles.

Uses ZipRecruiter's public search page (no auth required).
"""
from __future__ import annotations

import random
import re
import time
import urllib.error
import urllib.parse
import urllib.request

QUERIES = [
    "compliance analyst",
    "compliance associate",
    "KYC analyst",
    "AML analyst",
    "onboarding analyst financial services",
    "operations analyst broker dealer",
    "trade operations analyst",
    "middle office analyst",
    "securities operations",
    "clearing operations analyst",
    "regulatory operations",
    "FINRA compliance",
    "broker dealer compliance",
    "compliance coordinator",
    "licensing registration analyst",
]

_UA_LIST = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:132.0) Gecko/20100101 Firefox/132.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
]

NYC_RE = re.compile(
    r"\b(?:new\s+york|nyc|manhattan|brooklyn|queens|bronx|staten\s+island"
    r"|midtown|downtown|financial\s+district|wall\s+street)\b", re.I)

NON_NYC_RE = re.compile(
    r"\b(?:jersey\s+city|hoboken|newark|stamford|white\s+plains"
    r"|greenwich|parsippany|morristown)\b", re.I)


def _fetch(url: str) -> str:
    headers = {
        "User-Agent": random.choice(_UA_LIST),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": "https://www.google.com/",
    }
    req = urllib.request.Request(url, headers=headers)
    with urllib.request.urlopen(req, timeout=12) as resp:
        return resp.read().decode("utf-8", errors="replace")


def scrape_all() -> tuple[list[dict], dict]:
    """Scrape ZipRecruiter for NYC compliance/ops roles."""
    audit = {
        "total_raw": 0,
        "nyc": 0,
        "errors": 0,
        "blocked": 0,
        "queries_run": 0,
    }

    all_jobs: list[dict] = []
    seen: set[str] = set()

    shuffled = list(QUERIES)
    random.shuffle(shuffled)

    for query in shuffled:
        audit["queries_run"] += 1
        encoded = urllib.parse.quote(query)
        url = f"https://www.ziprecruiter.com/jobs-search?search={encoded}&location=New+York%2C+NY&days=7"

        try:
            html = _fetch(url)
        except Exception as e:
            audit["errors"] += 1
            continue

        if "captcha" in html.lower() or "verify you are human" in html.lower():
            audit["blocked"] += 1
            continue

        # Parse job cards — ZipRecruiter uses structured data
        # Try JSON-LD first
        import json
        jsonld_pattern = re.compile(r'<script type="application/ld\+json">(.*?)</script>', re.S)
        for match in jsonld_pattern.findall(html):
            try:
                data = json.loads(match)
                items = data if isinstance(data, list) else [data]
                for item in items:
                    if not isinstance(item, dict):
                        continue
                    if item.get("@type") != "JobPosting":
                        continue

                    title = item.get("title", "")
                    org = item.get("hiringOrganization", {})
                    company = org.get("name", "") if isinstance(org, dict) else ""
                    loc = item.get("jobLocation", {})
                    if isinstance(loc, dict):
                        addr = loc.get("address", {})
                        location = addr.get("addressLocality", "") if isinstance(addr, dict) else ""
                    elif isinstance(loc, list) and loc:
                        addr = loc[0].get("address", {}) if isinstance(loc[0], dict) else {}
                        location = addr.get("addressLocality", "") if isinstance(addr, dict) else ""
                    else:
                        location = ""

                    salary = ""
                    base = item.get("baseSalary", {})
                    if isinstance(base, dict):
                        val = base.get("value", {})
                        if isinstance(val, dict):
                            mn = val.get("minValue", "")
                            mx = val.get("maxValue", "")
                            if mn and mx:
                                salary = f"${mn}-${mx}"

                    desc = item.get("description", "")
                    url_job = item.get("url", "")

                    if not title or not company:
                        continue

                    key = f"{company.lower().strip()}|{title.lower().strip()}"
                    if key in seen:
                        continue
                    seen.add(key)
                    audit["total_raw"] += 1

                    # NYC filter
                    loc_blob = f"{location} {title}"
                    if NON_NYC_RE.search(loc_blob):
                        continue
                    if not NYC_RE.search(loc_blob) and "ny" not in location.lower():
                        continue

                    audit["nyc"] += 1
                    all_jobs.append({
                        "title": title,
                        "company": company,
                        "location": location,
                        "url": url_job,
                        "salary": salary,
                        "snippet": re.sub(r"<[^>]+>", "", desc)[:500],
                        "source": "ziprecruiter",
                    })
            except (json.JSONDecodeError, TypeError, AttributeError):
                continue

        # Also try HTML card parsing as fallback
        title_re = re.compile(r'<h2[^>]*class="[^"]*job_title[^"]*"[^>]*>\s*<a[^>]*>(.*?)</a>', re.S)
        company_re = re.compile(r'<a[^>]*class="[^"]*t_org_link[^"]*"[^>]*>(.*?)</a>', re.S)
        loc_re = re.compile(r'<a[^>]*class="[^"]*t_location_link[^"]*"[^>]*>(.*?)</a>', re.S)

        titles = title_re.findall(html)
        companies = company_re.findall(html)
        locs = loc_re.findall(html)

        for i in range(min(len(titles), len(companies))):
            title = re.sub(r"<[^>]+>", "", titles[i]).strip()
            company = re.sub(r"<[^>]+>", "", companies[i]).strip()
            location = re.sub(r"<[^>]+>", "", locs[i]).strip() if i < len(locs) else ""

            if not title or not company:
                continue

            key = f"{company.lower().strip()}|{title.lower().strip()}"
            if key in seen:
                continue
            seen.add(key)
            audit["total_raw"] += 1

            loc_blob = f"{location} {title}"
            if NON_NYC_RE.search(loc_blob):
                continue
            if not NYC_RE.search(loc_blob) and "ny" not in location.lower():
                continue

            audit["nyc"] += 1
            all_jobs.append({
                "title": title,
                "company": company,
                "location": location or "New York, NY",
                "url": "",
                "salary": "",
                "snippet": "",
                "source": "ziprecruiter",
            })

        time.sleep(random.uniform(2.0, 4.0))

    return all_jobs, audit


if __name__ == "__main__":
    jobs, audit = scrape_all()
    print(f"ZipRecruiter: {audit['nyc']} NYC from {audit['total_raw']} raw "
          f"({audit['errors']} errors, {audit['blocked']} blocked)")
    for j in jobs[:15]:
        print(f"  {j['company']:30s} | {j['title'][:50]}")
