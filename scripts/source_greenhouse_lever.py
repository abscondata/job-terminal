"""
Greenhouse + Lever direct-employer fetcher for target firms.

Fetches from public ATS APIs of curated NYC finance employers.
No auth required — these are public job board APIs.
"""
from __future__ import annotations

import json
import re
import time
import urllib.error
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent

# ═══════════════════════════════════════════════════════════════════════════════
# TARGET EMPLOYER ATS BOARDS
# Curated list: firm name -> (ats_type, board_id)
# ═══════════════════════════════════════════════════════════════════════════════

GREENHOUSE_BOARDS = {
    # Fintech / crypto
    "Coinbase": "coinbase",
    "Ramp": "ramp",
    "Robinhood": "robinhood",
    "Stripe": "stripe",
    "Plaid": "plaid",
    "Anchorage Digital": "anchoragedigital",
    "Circle": "circle",
    "Gemini": "gemini",
    "Ripple": "ripple",
    "Kraken": "krakendigital",
    "Paxos": "paxos",
    "Chainalysis": "chainalysis",
    "Fireblocks": "fireblocks",
    "Figure": "figure",
    # PE / HF / AM
    "Two Sigma": "twosigma",
    "Citadel": "citadel",
    "Point72": "point72",
    "D.E. Shaw": "deshaw",
    "Bridgewater": "bridgewaterassociates",
    "Man Group": "mangroup",
    "Millennium": "millenniummanagement",
    "Balyasny": "balyasnyassetmanagement",
    "Schonfeld": "schonfeld",
    "Ares Management": "aresmanagement",
    "General Atlantic": "generalatlantic",
    "Warburg Pincus": "warburgpincus",
    "Carlyle": "carlaborationhq",
    "Apollo": "apollo",
    "TPG": "tpg",
    # Mid-market / specialty
    "Clear Street": "clearstreet",
    "DriveWealth": "drivewealth",
    "Marqeta": "marqeta",
    "Brex": "brex",
    "SoFi": "solofinance",
    "Affirm": "affirm",
    "Adyen": "adyen",
    # Exchanges
    "Nasdaq": "nasdaq",
    "DTCC": "dtcc",
    "ICE": "theice",
    "Cboe": "cboe",
}

LEVER_BOARDS = {
    "Webull": "webull",
    "Virtu Financial": "virtufinancial",
    "Hudson River Trading": "hudson-river-trading",
    "Jane Street": "janestreet",
    "Flow Traders": "flowtraders",
    "Tower Research": "tower-research-capital",
    "Akuna Capital": "akunacapital",
    "Copper": "copperco",
    "BitGo": "bitgo",
    "Apex Fintech Solutions": "apexfintechsolutions",
}

# Role-family keywords for filtering ATS results
COMPLIANCE_KEYWORDS = re.compile(
    r"\b(?:compliance|regulatory|aml|kyc|bsa|sanctions|financial\s+crim"
    r"|anti[-\s]?money|onboarding|account\s+opening|licensing|registration"
    r"|surveillance|due\s+diligence|cdd|risk\s+(?:analyst|associate|operations|controls?)"
    r"|controls?\s+(?:analyst|advisory)|governance|middle\s+office|trade\s+support"
    r"|operations\s+(?:analyst|associate|coordinator)|fund\s+(?:operations|accounting)"
    r"|securities\s+(?:operations|compliance)|clearing|settlement|custody"
    r"|client\s+(?:service|onboarding)|first\s+line|transaction\s+monitoring"
    r"|finra|reg\s+reporting|regulatory\s+reporting)\b",
    re.I,
)

NYC_KEYWORDS = re.compile(
    r"\b(?:new\s+york|nyc|manhattan|brooklyn|jersey\s+city|ny\b)\b",
    re.I,
)

SENIOR_REJECT = re.compile(
    r"\b(?:vice\s+president|(?<!\w)vp\b|avp\b|svp\b|director|head\s+of|chief"
    r"|principal|managing\s+director|intern\b|internship)\b",
    re.I,
)

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept": "application/json",
}


def _fetch_json(url: str, timeout: int = 20) -> dict | list:
    req = urllib.request.Request(url, headers=HEADERS)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return json.loads(resp.read().decode("utf-8", errors="replace"))
    except (urllib.error.HTTPError, urllib.error.URLError, Exception) as e:
        return {"_error": str(e)}


def _clean_html(text: str) -> str:
    if not text:
        return ""
    t = re.sub(r"<[^>]+>", " ", text)
    t = re.sub(r"&\w+;", " ", t)
    t = re.sub(r"&#x[0-9a-fA-F]+;", " ", t)
    return re.sub(r"\s+", " ", t).strip()


def _greenhouse_location(job: dict) -> str:
    """Extract location string from Greenhouse job."""
    locs = job.get("location", {})
    if isinstance(locs, dict):
        return locs.get("name", "")
    offices = job.get("offices", [])
    if offices:
        return offices[0].get("name", "")
    return ""


def _lever_location(job: dict) -> str:
    """Extract location string from Lever posting."""
    cats = job.get("categories", {})
    return cats.get("location", "") or ""


def scrape_greenhouse() -> tuple[list[dict], dict]:
    """Fetch from all Greenhouse boards. Returns (jobs, audit)."""
    all_jobs = []
    audit = {"boards_checked": 0, "boards_with_jobs": 0, "raw": 0, "relevant": 0,
             "nyc": 0, "errors": 0, "firm_detail": {}}

    for firm_name, board_id in GREENHOUSE_BOARDS.items():
        url = f"https://boards-api.greenhouse.io/v1/boards/{board_id}/jobs?content=true"
        data = _fetch_json(url)
        audit["boards_checked"] += 1

        if isinstance(data, dict) and "_error" in data:
            audit["errors"] += 1
            audit["firm_detail"][firm_name] = {"error": data["_error"][:60]}
            time.sleep(0.3)
            continue

        jobs = data.get("jobs", []) if isinstance(data, dict) else []
        firm_stats = {"total": len(jobs), "relevant": 0, "nyc": 0}

        if jobs:
            audit["boards_with_jobs"] += 1

        for job in jobs:
            title = job.get("title", "")
            location = _greenhouse_location(job)
            content = _clean_html(job.get("content", ""))
            blob = f"{title} {content[:500]}"

            audit["raw"] += 1

            # Must have compliance/risk/ops relevance
            if not COMPLIANCE_KEYWORDS.search(blob):
                continue
            # Hard reject impossible seniority
            if SENIOR_REJECT.search(title):
                continue

            firm_stats["relevant"] += 1
            audit["relevant"] += 1

            # Must be NYC-area
            loc_blob = f"{location} {content[:300]}"
            if not NYC_KEYWORDS.search(loc_blob):
                continue

            firm_stats["nyc"] += 1
            audit["nyc"] += 1

            job_url = job.get("absolute_url", "")
            all_jobs.append({
                "job_key": f"gh_{board_id}_{job.get('id', '')}",
                "title": title,
                "company": firm_name,
                "location": location or "New York, NY",
                "salary": "",
                "snippet": content[:400] if content else title,
                "url": job_url,
                "source": "greenhouse",
                "source_board": board_id,
            })

        audit["firm_detail"][firm_name] = firm_stats
        time.sleep(0.3)

    return all_jobs, audit


def scrape_lever() -> tuple[list[dict], dict]:
    """Fetch from all Lever boards. Returns (jobs, audit)."""
    all_jobs = []
    audit = {"boards_checked": 0, "boards_with_jobs": 0, "raw": 0, "relevant": 0,
             "nyc": 0, "errors": 0, "firm_detail": {}}

    for firm_name, company_id in LEVER_BOARDS.items():
        url = f"https://api.lever.co/v0/postings/{company_id}?mode=json"
        data = _fetch_json(url)
        audit["boards_checked"] += 1

        if isinstance(data, dict) and "_error" in data:
            audit["errors"] += 1
            audit["firm_detail"][firm_name] = {"error": data["_error"][:60]}
            time.sleep(0.3)
            continue

        jobs = data if isinstance(data, list) else []
        firm_stats = {"total": len(jobs), "relevant": 0, "nyc": 0}

        if jobs:
            audit["boards_with_jobs"] += 1

        for job in jobs:
            title = job.get("text", "")
            location = _lever_location(job)
            desc = _clean_html(job.get("descriptionPlain", "") or job.get("description", ""))
            blob = f"{title} {desc[:500]}"

            audit["raw"] += 1

            if not COMPLIANCE_KEYWORDS.search(blob):
                continue
            if SENIOR_REJECT.search(title):
                continue

            firm_stats["relevant"] += 1
            audit["relevant"] += 1

            loc_blob = f"{location} {desc[:300]}"
            if not NYC_KEYWORDS.search(loc_blob):
                continue

            firm_stats["nyc"] += 1
            audit["nyc"] += 1

            job_url = job.get("hostedUrl", "") or job.get("applyUrl", "")
            all_jobs.append({
                "job_key": f"lv_{company_id}_{job.get('id', '')}",
                "title": title,
                "company": firm_name,
                "location": location or "New York, NY",
                "salary": "",
                "snippet": desc[:400] if desc else title,
                "url": job_url,
                "source": "lever",
                "source_board": company_id,
            })

        audit["firm_detail"][firm_name] = firm_stats
        time.sleep(0.3)

    return all_jobs, audit


def scrape_all() -> tuple[list[dict], dict]:
    """Run both Greenhouse and Lever. Returns (jobs, combined_audit)."""
    gh_jobs, gh_audit = scrape_greenhouse()
    lv_jobs, lv_audit = scrape_lever()

    all_jobs = gh_jobs + lv_jobs
    combined = {
        "greenhouse": gh_audit,
        "lever": lv_audit,
        "total_jobs": len(all_jobs),
        "total_boards": gh_audit["boards_checked"] + lv_audit["boards_checked"],
        "total_with_jobs": gh_audit["boards_with_jobs"] + lv_audit["boards_with_jobs"],
    }

    return all_jobs, combined


if __name__ == "__main__":
    jobs, audit = scrape_all()
    print(f"Greenhouse: {audit['greenhouse']['nyc']} NYC jobs from {audit['greenhouse']['boards_checked']} boards "
          f"({audit['greenhouse']['relevant']} relevant, {audit['greenhouse']['raw']} raw)")
    print(f"Lever: {audit['lever']['nyc']} NYC jobs from {audit['lever']['boards_checked']} boards "
          f"({audit['lever']['relevant']} relevant, {audit['lever']['raw']} raw)")
    print(f"Total: {len(jobs)} jobs")
    for j in jobs[:10]:
        print(f"  [{j['source']:10s}] {j['company']:25s} | {j['title'][:50]}")
