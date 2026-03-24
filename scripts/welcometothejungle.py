from __future__ import annotations

import gzip
import html
import re
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone, timedelta

from scripts.discovery_sources import (
    dedupe_jobs,
    extract_job_posting,
    fetch_text,
    looks_target_role,
    pause,
)

SITEMAPS = [
    "https://www.welcometothejungle.com/sitemaps/job-listings.0.xml.gz",
    "https://www.welcometothejungle.com/sitemaps/job-listings.1.xml.gz",
]
URL_BLOCK_RE = re.compile(r"<url>(.*?)</url>", re.I | re.S)
LOC_RE = re.compile(r"<loc>(.*?)</loc>", re.I)
LASTMOD_RE = re.compile(r"<lastmod>(.*?)</lastmod>", re.I)
PARIS_URL_RE = re.compile(
    r"(?:/paris|paris[-_]|_paris|paris_|/puteaux|/courbevoie|/neuilly|"
    r"/boulogne(?:-billancourt)?|/levallois(?:-perret)?|/issy(?:-les-moulineaux)?|"
    r"/saint-denis|/montreuil|/clichy|/montrouge)\b",
    re.I,
)


def _fetch_gzip_text(url: str, timeout: int = 35) -> str:
    req = urllib.request.Request(
        url,
        headers={
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
            "Accept": "application/xml,text/xml;q=0.9,*/*;q=0.8",
        },
    )
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        data = resp.read()
        encoding = (resp.headers.get("Content-Encoding") or "").lower()
    if url.endswith(".gz") or "gzip" in encoding:
        data = gzip.decompress(data)
    return data.decode("utf-8", errors="replace")


def _candidate_urls(quick: bool, max_age_days: int | None = None) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    max_candidates = 60 if quick else 220
    cutoff = None
    if max_age_days:
        cutoff = datetime.now(timezone.utc) - timedelta(days=max_age_days)

    for sitemap in SITEMAPS[:1 if quick else len(SITEMAPS)]:
        xml_text = _fetch_gzip_text(sitemap, timeout=35)
        for block_match in URL_BLOCK_RE.finditer(xml_text):
            block = block_match.group(1)
            loc_match = LOC_RE.search(block or "")
            if not loc_match:
                continue
            url = html.unescape(loc_match.group(1))
            if cutoff:
                lastmod_match = LASTMOD_RE.search(block or "")
                if lastmod_match:
                    try:
                        lastmod = datetime.fromisoformat(lastmod_match.group(1).strip())
                        if lastmod.tzinfo is None:
                            lastmod = lastmod.replace(tzinfo=timezone.utc)
                        if lastmod < cutoff:
                            continue
                    except Exception:
                        pass
            if url in seen or not PARIS_URL_RE.search(url):
                continue
            seen.add(url)
            out.append(url)
            if len(out) >= max_candidates:
                return out
    return out


def _parse_url(url: str) -> dict | None:
    try:
        html_text = fetch_text(url, timeout=25)
    except Exception:
        return None

    parsed = extract_job_posting(url, html_text)
    title = parsed.get("title") or ""
    company = parsed.get("company") or ""
    location = parsed.get("location_text") or "Paris, France"
    desc = parsed.get("description_text") or ""

    if not company:
        match = re.search(r"/companies/([^/]+)/", url)
        if match:
            slug = match.group(1).replace("-", " ").strip()
            company = slug.title()

    if not title or not looks_target_role(title, desc, company, require_world_and_function=False):
        return None

    return {
        "source": "welcometothejungle",
        "company": company,
        "title": title,
        "location_text": location,
        "url": url,
        "apply_url": url,
        "description_text": desc,
        "compensation_text": None,
        "source_job_id": url,
        "raw": {"url": url},
    }


def scrape(quick: bool = False, *, max_age_days: int | None = None) -> list[dict]:
    urls = _candidate_urls(quick, max_age_days=max_age_days)
    if not urls:
        return []

    out: list[dict] = []
    with ThreadPoolExecutor(max_workers=4 if quick else 6) as executor:
        futures = {executor.submit(_parse_url, url): url for url in urls}
        for future in as_completed(futures):
            item = future.result()
            if item:
                out.append(item)
            pause(0.05)

    return dedupe_jobs(out)


if __name__ == "__main__":
    print(len(scrape()))
