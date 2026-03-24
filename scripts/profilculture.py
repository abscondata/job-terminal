from __future__ import annotations

import re

from scripts.discovery_sources import (
    clean_text,
    dedupe_jobs,
    extract_job_posting,
    fetch_text,
    looks_target_role,
    pause,
)

BASE_URL = "https://www.profilculture.com"
CATEGORY_PAGES = {
    "emploi": f"{BASE_URL}/annonce/category/emploi-culture-page1",
    "stage": f"{BASE_URL}/annonce/category/stage-alternance-culture-page1",
}

PAGE_RE = re.compile(r"https?://www\.profilculture\.com/annonce/category/[^\"]+-page\d+", re.I)
JOB_RE = re.compile(r"https?://www\.profilculture\.com/annonce/[^\" ]+?\.html", re.I)
TITLE_RE = re.compile(r"<title>(.*?)</title>", re.I | re.S)
PARIS_RE = re.compile(r"\bparis\b|\bile[-\\s]?de[-\\s]?france\b|\bidf\b", re.I)


def _split_title(raw_title: str) -> tuple[str, str, str]:
    cleaned = clean_text(raw_title).replace(" - ProfilCulture", "").strip()
    parts = [part.strip() for part in cleaned.split(",") if part.strip()]
    role = parts[0] if parts else cleaned
    company = parts[1] if len(parts) > 1 else ""
    location = parts[2] if len(parts) > 2 else ""
    return role, company, location


def _page_urls(base_url: str, max_pages: int) -> list[str]:
    urls = [base_url]
    try:
        html_text = fetch_text(base_url, timeout=25)
    except Exception:
        return urls
    for match in PAGE_RE.finditer(html_text):
        urls.append(match.group(0))
    unique = []
    seen = set()
    for url in urls:
        if url in seen:
            continue
        seen.add(url)
        unique.append(url)
    if max_pages and len(unique) > max_pages:
        return unique[:max_pages]
    return unique


def _job_urls(page_html: str) -> list[str]:
    seen = set()
    urls: list[str] = []
    for match in JOB_RE.finditer(page_html):
        url = match.group(0)
        if url in seen:
            continue
        seen.add(url)
        urls.append(url)
    return urls


def scrape(quick: bool = False) -> list[dict]:
    out: list[dict] = []
    seen: set[str] = set()
    max_pages = 1 if quick else 3
    stage_pages = 1 if quick else 2

    for key, base_url in CATEGORY_PAGES.items():
        pages = _page_urls(base_url, stage_pages if key == "stage" else max_pages)
        for page_url in pages:
            try:
                page_html = fetch_text(page_url, timeout=25)
            except Exception:
                continue
            for job_url in _job_urls(page_html):
                if job_url in seen:
                    continue
                seen.add(job_url)
                try:
                    detail_html = fetch_text(job_url, timeout=25)
                except Exception:
                    continue

                parsed = extract_job_posting(job_url, detail_html)
                title_tag = TITLE_RE.search(detail_html or "")
                title_hint = title_tag.group(1) if title_tag else ""
                role_title, company_hint, location_hint = _split_title(title_hint or parsed.get("title") or "")

                title = parsed.get("title") or role_title
                company = parsed.get("company") or company_hint
                location = parsed.get("location_text") or location_hint
                desc = parsed.get("description_text") or clean_text(detail_html)[:12000]

                if location and not PARIS_RE.search(location) and not PARIS_RE.search(desc):
                    continue

                if not looks_target_role(
                    title,
                    desc,
                    company,
                    require_world_and_function=False,
                ):
                    continue

                out.append(
                    {
                        "source": "profilculture",
                        "company": company or "ProfilCulture",
                        "title": title,
                        "location_text": location or "Paris, France",
                        "url": job_url,
                        "apply_url": job_url,
                        "description_text": desc,
                        "compensation_text": None,
                        "source_job_id": job_url,
                        "raw": {"category": key, "page_url": page_url},
                    }
                )
                pause(0.05)
            pause(0.2)

    return dedupe_jobs(out)


if __name__ == "__main__":
    print(len(scrape()))
