from __future__ import annotations

import re

from scripts.discovery_sources import (
    clean_text,
    dedupe_jobs,
    extract_job_posting,
    fetch_json_post,
    fetch_text,
    pause,
)


def _scrape_workday_site(
    *,
    source: str,
    company: str,
    api_url: str,
    base_url: str,
    quick: bool,
    max_age_days: int | None = None,
) -> list[dict]:
    out: list[dict] = []
    limit = 20
    max_pages = 1 if quick else 4
    offset = 0

    def _posted_days(posted_on: str) -> int | None:
        if not posted_on:
            return None
        if "30+" in posted_on:
            return 30
        match = re.search(r"(\\d+)", posted_on)
        if match:
            try:
                return int(match.group(1))
            except ValueError:
                return None
        return None

    for _page in range(max_pages):
        payload = {"limit": limit, "offset": offset, "searchText": "paris"}
        data = fetch_json_post(api_url, payload, timeout=25)
        postings = data.get("jobPostings") or []
        if not postings:
            break

        for posting in postings:
            title = clean_text(posting.get("title"))
            location = clean_text(posting.get("locationsText")) or "Paris, France"
            external_path = posting.get("externalPath") or ""
            posted_days = _posted_days(str(posting.get("postedOn") or ""))
            if max_age_days and posted_days is not None and posted_days > max_age_days:
                continue
            if not title or not external_path or "paris" not in location.lower():
                continue

            url = f"{base_url}{external_path}"
            try:
                detail_html = fetch_text(url, timeout=25)
            except Exception:
                continue

            parsed = extract_job_posting(url, detail_html)
            desc = parsed.get("description_text") or title

            source_job_id = ""
            bullet_fields = posting.get("bulletFields") or []
            if bullet_fields:
                source_job_id = str(bullet_fields[0])
            if not source_job_id:
                source_job_id = external_path.lstrip("/")

            out.append(
                {
                    "source": source,
                    "company": parsed.get("company") or company,
                    "title": parsed.get("title") or title,
                    "location_text": parsed.get("location_text") or location,
                    "url": url,
                    "apply_url": url,
                    "description_text": desc,
                    "compensation_text": None,
                    "source_job_id": source_job_id,
                    "raw": {"external_path": external_path},
                }
            )
            pause(0.1)

        if len(postings) < limit:
            break
        offset += limit

    return dedupe_jobs(out)


def scrape_chanel(quick: bool = False, *, max_age_days: int | None = None) -> list[dict]:
    return _scrape_workday_site(
        source="chanel",
        company="Chanel",
        api_url="https://cc.wd3.myworkdayjobs.com/wday/cxs/cc/ChanelCareers/jobs",
        base_url="https://cc.wd3.myworkdayjobs.com/en-US/ChanelCareers",
        quick=quick,
        max_age_days=max_age_days,
    )


def scrape_christies(quick: bool = False, *, max_age_days: int | None = None) -> list[dict]:
    return _scrape_workday_site(
        source="christies",
        company="Christie's",
        api_url="https://christies.wd3.myworkdayjobs.com/wday/cxs/christies/Christies_Careers/jobs",
        base_url="https://christies.wd3.myworkdayjobs.com/en-US/Christies_Careers",
        quick=quick,
        max_age_days=max_age_days,
    )
