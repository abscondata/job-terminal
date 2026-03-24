from __future__ import annotations

import re
import urllib.error

from scripts.discovery_sources import CITY_TARGETS, clean_text, dedupe_jobs, queries_for_city
from scripts.discovery_sources import fetch_text, looks_target_role, pause
from scripts.discovery_sources import quick_subset

SEARCH_URL = "https://www.linkedin.com/jobs-guest/jobs/api/seeMoreJobPostings/search"
WORKPLACE_TYPES = [("onsite", "1"), ("hybrid", "3")]

CARD_RE = re.compile(
    r'<div class="base-card.*?data-entity-urn="urn:li:jobPosting:(\d+)".*?</li>',
    re.S,
)


def _extract(block: str, pattern: str) -> str:
    match = re.search(pattern, block, re.S)
    return clean_text(match.group(1)) if match else ""


def _parse_cards(
    fragment: str,
    arrangement: str,
    query: str,
    *,
    require_world_and_function: bool = True,
) -> list[dict]:
    jobs: list[dict] = []
    for match in CARD_RE.finditer(fragment):
        block = match.group(0)
        job_id = match.group(1)
        title = _extract(block, r'base-search-card__title">\s*(.*?)\s*</h3>')
        company = _extract(block, r'base-search-card__subtitle">\s*<a[^>]*>(.*?)</a>')
        location = _extract(block, r'job-search-card__location">\s*(.*?)\s*</span>')
        url = _extract(block, r'base-card__full-link[^>]+href="([^"]+)"')
        snippet = _extract(block, r'job-search-card__snippet">\s*(.*?)\s*</div>')

        if not title or not company or not url:
            continue
        if not looks_target_role(
            title,
            snippet,
            company,
            require_world_and_function=require_world_and_function,
        ):
            continue

        jobs.append(
            {
                "source": "linkedin",
                "company": company,
                "title": title,
                "location_text": location,
                "url": url,
                "apply_url": url,
                "description_text": snippet or title,
                "compensation_text": None,
                "remote_hint": arrangement,
                "source_job_id": job_id,
                "raw": {
                    "job_id": job_id,
                    "query": query,
                    "arrangement": arrangement,
                    "html": block[:4000],
                },
            }
        )
    return jobs


def scrape(max_pages: int = 2, quick: bool = False, *, city_scope: str = "all") -> list[dict]:
    out: list[dict] = []
    errors: list[str] = []
    default_pages = 1 if quick else max_pages
    city_scope = (city_scope or "all").lower()
    city_targets = CITY_TARGETS
    if city_scope != "all":
        city_targets = [city for city in CITY_TARGETS if city.get("slug") == city_scope]

    for city in city_targets:
        city_queries = queries_for_city(city["slug"])
        pages = default_pages
        loose_targeting = city["slug"] == "nyc"
        if not quick and city["slug"] == "nyc":
            pages = max(pages, 5)
        for arrangement, work_type in WORKPLACE_TYPES:
            for query in quick_subset(city_queries, quick):
                for page in range(pages):
                    start = page * 25
                    try:
                        fragment = fetch_text(
                            SEARCH_URL,
                            params={
                                "keywords": query,
                                "location": city["linkedin"],
                                "start": start,
                                "f_TPR": "r2592000",
                                "f_WT": work_type,
                            },
                            timeout=20,
                        )
                    except urllib.error.HTTPError as exc:
                        errors.append(
                            f"{city['label']} {arrangement} {query}: HTTP {exc.code}"
                        )
                        break
                    except Exception as exc:
                        errors.append(
                            f"{city['label']} {arrangement} {query}: "
                            f"{type(exc).__name__}: {exc}"
                        )
                        break

                    jobs = _parse_cards(
                        fragment,
                        arrangement,
                        query,
                        require_world_and_function=not loose_targeting,
                    )
                    if not jobs:
                        break
                    out.extend(jobs)
                    if len(jobs) < 10:
                        break
                    pause(1.0)
                pause(0.4)

    out = dedupe_jobs(out)
    if not out and errors:
        raise RuntimeError("; ".join(errors[:6]))
    return out


if __name__ == "__main__":
    results = scrape()
    print(f"LinkedIn: {len(results)} jobs")
