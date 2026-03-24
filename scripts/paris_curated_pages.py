from __future__ import annotations

import html
import re
from datetime import datetime, timezone, timedelta
from urllib.parse import urljoin

from connectors.greenhouse import fetch_jobs as fetch_greenhouse_jobs
from scripts.discovery_sources import clean_text, dedupe_jobs, fetch_text, looks_target_role, pause

KERING_URL = "https://www.kering.com/fr/talent/offres-d-emploi/"
CENTRE_POMPIDOU_PAGE = "https://www.centrepompidou.fr/fr/le-centre-pompidou/emplois-et-stages"
POMPIDOU_SEARCH = (
    "https://choisirleservicepublic.gouv.fr/nos-offres/filtres/mot-cles/"
    "Centre%20National%20d%27Art%20et%20de%20Culture%20Georges%20Pompidou/"
)
SOTHEBYS_COMPANY = "Sotheby's"

KERING_CARD_RE = re.compile(
    r"<h2[^>]*><a[^>]+title=\"([^\"]+)\" href=\"([^\"]+)\">(.*?)</a></h2>"
    r".*?<p class=\"t1ej2qn4\">(.*?)</p>",
    re.S,
)
KERING_BRAND_RE = re.compile(r"^([A-Z][A-Z &'\-]{3,})\s")
ANCHOR_RE = re.compile(r"<a[^>]+href=\"([^\"]+)\"[^>]*>(.*?)</a>", re.S)
POMPIDOU_OFFER_RE = re.compile(r"choisirleservicepublic\.gouv\.fr/offre-emploi/", re.I)


def scrape_sothebys(quick: bool = False, *, max_age_days: int | None = None) -> list[dict]:
    out: list[dict] = []
    cutoff = None
    if max_age_days:
        cutoff = datetime.now(timezone.utc) - timedelta(days=max_age_days)
    for job in fetch_greenhouse_jobs("sothebys"):
        updated = job.get("first_published") or job.get("updated_at") or ""
        if cutoff and updated:
            try:
                updated_dt = datetime.fromisoformat(updated.replace("Z", "+00:00"))
            except ValueError:
                updated_dt = None
            if updated_dt and updated_dt < cutoff:
                continue
        title = clean_text(job.get("title"))
        company = clean_text((job.get("company_name") or SOTHEBYS_COMPANY))
        location = clean_text((job.get("location") or {}).get("name"))
        desc = clean_text(job.get("content"))
        url = job.get("absolute_url") or ""
        source_job_id = str(job.get("id") or job.get("requisition_id") or job.get("internal_job_id") or "")
        if location:
            if "paris" not in location.lower():
                continue
        elif "paris" not in desc.lower():
            continue
        out.append(
            {
                "source": "sothebys",
                "company": company or SOTHEBYS_COMPANY,
                "title": title,
                "location_text": location or "Paris, France",
                "url": url,
                "apply_url": url,
                "description_text": desc,
                "compensation_text": None,
                "source_job_id": source_job_id or url,
                "raw": {"job_id": job.get("id")},
            }
        )
    return dedupe_jobs(out)


def scrape_kering(quick: bool = False) -> list[dict]:
    out: list[dict] = []
    pages = [KERING_URL] if quick else [KERING_URL, f"{KERING_URL}?page=2"]

    for page_url in pages:
        html_text = fetch_text(page_url, timeout=30)
        for match in KERING_CARD_RE.finditer(html_text):
            title = clean_text(match.group(1))
            href = html.unescape(match.group(2))
            details = clean_text(match.group(4))
            if "paris" not in details.lower():
                continue
            company_match = KERING_BRAND_RE.match(title)
            company = company_match.group(1).title() if company_match else "Kering"
            url = urljoin("https://www.kering.com", href)
            try:
                detail_html = fetch_text(url, timeout=25)
            except Exception:
                detail_html = ""
            desc = clean_text(detail_html)[:12000] or details

            source_job_id = href or url
            out.append(
                {
                    "source": "kering",
                    "company": company,
                    "title": title,
                    "location_text": "Paris, France",
                    "url": url,
                    "apply_url": url,
                    "description_text": desc,
                    "compensation_text": None,
                    "source_job_id": source_job_id,
                    "raw": {"details": details},
                }
            )
            pause(0.1)

    return dedupe_jobs(out)


def scrape_centre_pompidou(quick: bool = False) -> list[dict]:
    out: list[dict] = []
    seen: set[str] = set()

    # Keep a hard dependency on the official page so this source fails fast if
    # the public handoff to the state jobs board changes.
    official_html = fetch_text(CENTRE_POMPIDOU_PAGE, timeout=25)
    if "choisirleservicepublic" not in official_html.lower():
        return []

    pages = [POMPIDOU_SEARCH] if quick else [POMPIDOU_SEARCH, f"{POMPIDOU_SEARCH}page/2/"]
    for page_url in pages:
        listing_html = fetch_text(page_url, timeout=25)
        for match in ANCHOR_RE.finditer(listing_html):
            url = html.unescape(match.group(1))
            title = clean_text(match.group(2))
            if not title or url in seen or not POMPIDOU_OFFER_RE.search(url):
                continue
            seen.add(url)

            try:
                detail_html = fetch_text(url, timeout=25)
            except Exception:
                continue

            desc = clean_text(detail_html)[:12000]
            company = "Centre Pompidou"
            location = "Paris, France" if "paris" in desc.lower() else ""
            if not looks_target_role(title, desc, company):
                continue

            out.append(
                {
                    "source": "centre_pompidou",
                    "company": company,
                    "title": title,
                    "location_text": location,
                    "url": url,
                    "apply_url": url,
                    "description_text": desc,
                    "compensation_text": None,
                    "source_job_id": url,
                    "raw": {"official_page": CENTRE_POMPIDOU_PAGE},
                }
            )
            pause(0.1)

    return dedupe_jobs(out)
