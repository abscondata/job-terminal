from __future__ import annotations

import re
import urllib.error

from scripts.discovery_sources import CITY_TARGETS, clean_text, dedupe_jobs, queries_for_city
from scripts.discovery_sources import fetch_text, looks_target_role, pause
from scripts.discovery_sources import quick_subset

JOB_CARD_RE = re.compile(r'<div class="job_seen_beacon">(.*?)(?=<div class="job_seen_beacon"|$)', re.S)
BLOCKED_RE = re.compile(r"Authenticating\.\.\.|bot-detection-anonymous|Additional Verification Required", re.I)


def _extract(block: str, pattern: str) -> str:
    match = re.search(pattern, block, re.S)
    return clean_text(match.group(1)) if match else ""


def _parse_page(html_text: str, *, require_world_and_function: bool = True) -> list[dict]:
    jobs: list[dict] = []
    for match in JOB_CARD_RE.finditer(html_text):
        block = match.group(1)
        job_key = _extract(block, r'data-jk="([^"]+)"')
        title = _extract(block, r'id="jobTitle-[^"]+">(.*?)</span>')
        company = _extract(block, r'data-testid="company-name"[^>]*>(.*?)</span>')
        location = _extract(block, r'data-testid="text-location"[^>]*>(.*?)</div>')
        salary = _extract(block, r'salary-snippet-container.*?<span[^>]*>(.*?)</span>')
        snippet = _extract(block, r'data-testid="belowJobSnippet"[^>]*>(.*?)</div>')

        if not job_key or not title or not company:
            continue
        if not looks_target_role(
            title,
            snippet,
            company,
            require_world_and_function=require_world_and_function,
        ):
            continue

        url = f"https://www.indeed.com/viewjob?jk={job_key}"
        jobs.append(
            {
                "source": "indeed",
                "company": company,
                "title": title,
                "location_text": location,
                "url": url,
                "apply_url": url,
                "description_text": snippet or title,
                "compensation_text": salary or None,
                "source_job_id": job_key,
                "raw": {"job_key": job_key, "html": block[:4000]},
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
        domain = city["indeed_domain"]
        city_queries = quick_subset(queries_for_city(city["slug"]), quick)
        pages = default_pages
        loose_targeting = city["slug"] == "nyc"
        if not quick and city["slug"] == "nyc":
            pages = max(pages, 5)

        for query in city_queries:
            for page in range(pages):
                try:
                    html_text = fetch_text(
                        f"https://{domain}/jobs",
                        params={
                            "q": query,
                            "l": city["indeed_location"],
                            "fromage": "14",
                            "sort": "date",
                            "start": str(page * 10),
                        },
                        timeout=20,
                    )
                except urllib.error.HTTPError as exc:
                    errors.append(f"{city['label']} {query}: HTTP {exc.code}")
                    break
                except Exception as exc:
                    errors.append(f"{city['label']} {query}: {type(exc).__name__}: {exc}")
                    break

                if BLOCKED_RE.search(html_text):
                    errors.append(f"{city['label']} {query}: blocked_by_bot_detection")
                    break

                jobs = _parse_page(
                    html_text,
                    require_world_and_function=not loose_targeting,
                )
                if not jobs:
                    break
                out.extend(jobs)
                if len(jobs) < 8:
                    break
                pause(0.8)
            pause(0.4)

    out = dedupe_jobs(out)
    if not out and errors:
        raise RuntimeError("; ".join(errors[:6]))
    return out


if __name__ == "__main__":
    results = scrape()
    print(f"Indeed: {len(results)} jobs")
