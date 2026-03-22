from __future__ import annotations

import html
import json
import re
import time
import urllib.parse
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent

BROWSER_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": (
        "text/html,application/xhtml+xml,application/xml;q=0.9,"
        "image/avif,image/webp,*/*;q=0.8"
    ),
    "Accept-Language": "en-US,en;q=0.9,fr;q=0.8",
    "Cache-Control": "no-cache",
    "Pragma": "no-cache",
    "Referer": "https://www.google.com/",
    "Upgrade-Insecure-Requests": "1",
}

JSON_HEADERS = {
    **BROWSER_HEADERS,
    "Accept": "application/json,text/plain,*/*",
}

PARIS_CORE_QUERIES = [
    "assistant chef de projet communication luxe",
    "assistant chef de projet evenementiel luxe",
    "assistant chef de projet digital luxe",
    "assistant chef de projet marketing luxe",
    "assistant chef de projet luxe cdi",
    "charge de projet communication luxe",
    "charge de projet evenementiel luxe",
    "coordinateur projet luxe",
    "assistant project manager luxury",
    "project coordinator luxury brand",
    "communication coordinator luxury brand",
    "assistant coordination communication",
    "coordination contenu luxe",
    "production contenu luxe",
    "content production coordinator luxury",
    "visual merchandising coordinator luxe",
    "assistant visual merchandising",
    "retail excellence coordinator luxe",
    "client experience coordinator luxe",
    "assistant omnicanal",
    "brand operations coordinator luxe",
    "gallery coordinator paris",
    "auction operations coordinator",
    "foundation project coordinator",
]

PARIS_BRAND_QUERIES = [
    "dior assistant chef de projet",
    "louis vuitton project coordinator",
    "celine project coordinator",
    "sephora project coordinator",
    "cartier coordinator",
    "van cleef coordinator",
    "chloe communication assistant",
    "saint laurent coordinator",
    "balenciaga project assistant",
    "boucheron coordinator",
    "hermes project assistant",
]

NYC_CORE_QUERIES = [
    "project coordinator luxury brand",
    "assistant project manager luxury",
    "project coordinator fashion",
    "project coordinator beauty",
    "communication coordinator luxury",
    "content production coordinator",
    "production coordinator fashion",
    "visual merchandising coordinator",
    "visual merchandising assistant",
    "client experience coordinator",
    "retail excellence coordinator",
    "omnichannel coordinator",
    "brand operations coordinator",
    "events coordinator luxury",
    "gallery coordinator",
    "auction operations coordinator",
    "art operations coordinator",
]

NYC_BRAND_QUERIES = [
    "chanel coordinator",
    "cartier coordinator",
    "lvmh coordinator",
    "dior coordinator",
    "louis vuitton coordinator",
    "tiffany coordinator",
    "sephora project coordinator",
    "richemont coordinator",
    "van cleef coordinator",
]

NYC_QUERIES = NYC_CORE_QUERIES + NYC_BRAND_QUERIES

MIAMI_QUERIES = [
    "project coordinator luxury",
    "communication coordinator luxury",
    "client experience coordinator",
    "visual merchandising coordinator",
    "gallery operations assistant",
]

TARGET_QUERIES = PARIS_CORE_QUERIES + PARIS_BRAND_QUERIES

SENIOR_RE = re.compile(
    r"\b(?:manager|director|head|chief|vp|vice president|principal|partner|lead|senior manager)\b",
    re.I,
)
TARGET_FUNCTION_RE = re.compile(
    r"\b(?:assistant\s+chef\s+de\s+projet|chef\s+de\s+projet|charg[eÃ©]\s+de\s+projet|project\s+(?:assistant|support|coordinator)"
    r"|communication(?:s)?\s+(?:assistant|support|coordinator)"
    r"|charg[eÃ©]\s+de\s+communication"
    r"|event(?:s)?\s+(?:assistant|support|coordinator)"
    r"|charg[eÃ©]\s+de\s+(?:production|contenu|communication)"
    r"|content\s+(?:production|coordinator|coordination)"
    r"|visual\s+merchandising|vm\s+coordinator|merchandising\s+coordinator"
    r"|retail\s+excellence|client\s+experience|omnichannel"
    r"|gallery\s+(?:assistant|coordinator|operations)"
    r"|auction\s+(?:operations|assistant|coordinator)"
    r"|art\s+(?:operations|assistant|logistics)"
    r"|foundation\s+(?:assistant|coordinator)|cultural\s+(?:assistant|coordinator))\b",
    re.I,
)
TARGET_WORLD_RE = re.compile(
    r"\b(?:luxury|luxe|fashion|mode|beauty|beaute|beaut[eé]|jewelry|jewellery|joaillerie"
    r"|gallery|galerie|auction|ench[eè]res|art|culture|cultural|foundation|fondation"
    r"|premium|maison|boutique|retail excellence|client experience"
    r"|lvmh|dior|louis vuitton|celine|sephora|richemont|cartier|van cleef|chlo[ée]"
    r"|kering|saint laurent|balenciaga|boucheron|herm[eè]s|hermes|chanel)\b",
    re.I,
)
ANTI_TARGET_RE = re.compile(
    r"\b(?:compliance|aml|kyc|audit|legal|regulatory|trade surveillance|business development"
    r"|sales representative|copywriter|editorial|curator|cold calling|call center)\b",
    re.I,
)
OFF_LANE_RE = re.compile(
    r"\b(?:engineer|developer|architect|data scientist|machine learning|account executive"
    r"|financial analyst|operations analyst|internal audit|accountant|controller)\b",
    re.I,
)
TAG_RE = re.compile(r"<[^>]+>")
SCRIPT_JSON_RE = re.compile(
    r"<script[^>]+type=[\"']application/ld\\+json[\"'][^>]*>(.*?)</script>",
    re.I | re.S,
)
TITLE_RE = re.compile(r"<title[^>]*>(.*?)</title>", re.I | re.S)
META_RE = re.compile(
    r'<meta[^>]+(?:name|property)=["\']([^"\']+)["\'][^>]+content=["\']([^"\']*)["\']',
    re.I,
)

CITY_TARGETS = [
    {
        "slug": "paris",
        "label": "Paris",
        "linkedin": "Paris, France",
        "indeed_domain": "fr.indeed.com",
        "indeed_location": "Paris (75)",
    },
    {
        "slug": "nyc",
        "label": "NYC",
        "linkedin": "New York City Metropolitan Area",
        "indeed_domain": "www.indeed.com",
        "indeed_location": "New York, NY",
    },
    {
        "slug": "nyc",
        "label": "NYC Core",
        "linkedin": "New York, NY",
        "indeed_domain": "www.indeed.com",
        "indeed_location": "New York, NY",
    },
    {
        "slug": "miami",
        "label": "Miami",
        "linkedin": "Miami, FL",
        "indeed_domain": "www.indeed.com",
        "indeed_location": "Miami, FL",
    },
]

TARGET_FUNCTION_RE = re.compile(
    r"\b(?:assistant\s+chef\s+de\s+projet|chef\s+de\s+projet|charg[e\u00e9][\\.·]?\s*e?\s+de\s+projet|project\s+(?:assistant|support|coordinator)"
    r"|communication(?:s)?\s+(?:assistant|support|coordinator)"
    r"|charg[e\u00e9][\\.·]?\s*e?\s+de\s+communication"
    r"|event(?:s)?\s+(?:assistant|support|coordinator)"
    r"|charg[e\u00e9][\\.·]?\s*e?\s+de\s+(?:production|contenu|communication|evenementiel|\u00e9v\u00e9nementiel)"
    r"|content\s+(?:production|coordinator|coordination)"
    r"|visual\s+merchandising|vm\s+coordinator|merchandising\s+coordinator"
    r"|retail\s+excellence|client\s+experience|omnichannel"
    r"|gallery\s+(?:assistant|coordinator|operations)"
    r"|auction\s+(?:operations|assistant|coordinator)"
    r"|art\s+(?:operations|assistant|logistics)"
    r"|foundation\s+(?:assistant|coordinator)|cultural\s+(?:assistant|coordinator))\b",
    re.I,
)
TARGET_WORLD_RE = re.compile(
    r"\b(?:luxury|luxe|fashion|mode|beauty|beaute|beaut[e\u00e9]|jewelry|jewellery|joaillerie"
    r"|gallery|galerie|auction|ench[e\u00e8]res|art|culture|culturel|culturelle|cultural|foundation|fondation"
    r"|premium|maison|boutique|retail excellence|client experience"
    r"|lvmh|dior|louis vuitton|celine|sephora|richemont|cartier|van cleef|chlo[e\u00e9e]"
    r"|kering|saint laurent|balenciaga|boucheron|herm[e\u00e8]s|hermes|chanel)\b",
    re.I,
)


def queries_for_city(city_slug: str) -> list[str]:
    slug = (city_slug or "").lower()
    if slug == "nyc":
        return NYC_QUERIES
    if slug == "miami":
        return MIAMI_QUERIES
    return PARIS_CORE_QUERIES + PARIS_BRAND_QUERIES


def fetch_text(
    url: str,
    *,
    params: dict | None = None,
    headers: dict | None = None,
    timeout: int = 25,
) -> str:
    if params:
        query = urllib.parse.urlencode(params, doseq=True)
        sep = "&" if "?" in url else "?"
        url = f"{url}{sep}{query}"
    req = urllib.request.Request(url, headers={**BROWSER_HEADERS, **(headers or {})})
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return resp.read().decode("utf-8", errors="replace")


def fetch_json(
    url: str,
    *,
    params: dict | None = None,
    headers: dict | None = None,
    timeout: int = 25,
) -> dict | list:
    return json.loads(fetch_text(url, params=params, headers=headers, timeout=timeout))


def fetch_json_post(
    url: str,
    payload: dict,
    *,
    headers: dict | None = None,
    timeout: int = 25,
) -> dict | list:
    body = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(
        url,
        data=body,
        headers={
            **JSON_HEADERS,
            "Content-Type": "application/json",
            **(headers or {}),
        },
    )
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read().decode("utf-8", errors="replace"))


def clean_text(value: str | None) -> str:
    text = html.unescape(value or "")
    text = text.replace("·", " ")
    text = TAG_RE.sub(" ", text)
    return re.sub(r"\s+", " ", text).strip()


def extract_job_posting(url: str, html_text: str) -> dict:
    payload = {
        "url": url,
        "title": "",
        "company": "",
        "location_text": "",
        "description_text": "",
    }

    for match in SCRIPT_JSON_RE.finditer(html_text or ""):
        raw = html.unescape(match.group(1) or "").strip()
        if not raw:
            continue
        try:
            data = json.loads(raw)
        except Exception:
            continue
        candidates = data if isinstance(data, list) else [data]
        for candidate in candidates:
            if not isinstance(candidate, dict):
                continue
            if str(candidate.get("@type", "")).lower() != "jobposting":
                continue
            hiring = candidate.get("hiringOrganization") or {}
            location = candidate.get("jobLocation") or {}
            if isinstance(location, list):
                location = location[0] if location else {}
            address = location.get("address") or {}
            payload["title"] = clean_text(candidate.get("title"))
            payload["company"] = clean_text(hiring.get("name"))
            payload["location_text"] = clean_text(
                address.get("addressLocality")
                or address.get("addressRegion")
                or candidate.get("jobLocationType")
            )
            payload["description_text"] = clean_text(candidate.get("description"))
            return payload

    metas = {key.lower(): value for key, value in META_RE.findall(html_text or "")}
    title_match = TITLE_RE.search(html_text or "")
    payload["title"] = clean_text(
        metas.get("og:title")
        or metas.get("twitter:title")
        or (title_match.group(1) if title_match else "")
    )
    payload["description_text"] = clean_text(
        metas.get("description")
        or metas.get("og:description")
        or metas.get("twitter:description")
    )
    return payload


def dedupe_jobs(items: list[dict]) -> list[dict]:
    out: list[dict] = []
    seen: set[str] = set()
    for item in items:
        key = " | ".join(
            [
                (item.get("company") or "").strip().lower(),
                (item.get("title") or "").strip().lower(),
                (item.get("url") or item.get("apply_url") or "").strip().lower(),
            ]
        )
        if key in seen:
            continue
        seen.add(key)
        out.append(item)
    return out


def looks_target_role(
    title: str | None,
    description: str | None = None,
    company: str | None = None,
    *,
    require_world_and_function: bool = True,
) -> bool:
    title_text = clean_text(title)
    desc_text = clean_text(description)
    company_text = clean_text(company)
    hay = f"{title_text} {desc_text} {company_text}".strip()
    if not hay:
        return False
    world_match = bool(TARGET_WORLD_RE.search(hay))
    function_match = bool(TARGET_FUNCTION_RE.search(hay))
    if SENIOR_RE.search(title_text):
        return False
    if OFF_LANE_RE.search(hay):
        return False
    if ANTI_TARGET_RE.search(hay) and not world_match:
        return False
    if require_world_and_function:
        return function_match and world_match
    return function_match or world_match


def load_seed_lines(path: Path) -> list[str]:
    if not path.exists():
        return []
    lines: list[str] = []
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.split("#", 1)[0].strip()
        if line:
            lines.append(line)
    return lines


def quick_subset(items, quick: bool):
    seq = list(items)
    return seq[::2] if quick else seq


def pause(seconds: float) -> None:
    time.sleep(seconds)
