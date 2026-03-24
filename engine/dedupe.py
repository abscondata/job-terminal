from __future__ import annotations
import re
import sqlite3

_COMPANY_STOPWORDS = {
    "inc",
    "llc",
    "ltd",
    "co",
    "company",
    "corp",
    "corporation",
    "sa",
    "sas",
    "sarl",
    "groupe",
    "group",
}


def fingerprint_exists(conn: sqlite3.Connection, fingerprint: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM jobs_canonical WHERE fingerprint = ? LIMIT 1",
        (fingerprint,),
    ).fetchone()
    return row is not None


def normalize_company(text: str | None) -> str:
    blob = re.sub(r"[^a-z0-9]+", " ", (text or "").lower()).strip()
    parts = [part for part in blob.split() if part and part not in _COMPANY_STOPWORDS]
    return " ".join(parts)


def normalize_title(text: str | None) -> str:
    blob = re.sub(r"[^a-z0-9]+", " ", (text or "").lower()).strip()
    return " ".join(blob.split())


def normalize_location(text: str | None) -> str:
    blob = (text or "").lower()
    if "paris" in blob:
        return "paris"
    if "new york" in blob or "nyc" in blob:
        return "nyc"
    if "miami" in blob:
        return "miami"
    if "france" in blob:
        return "france"
    if "remote" in blob or "teletravail" in blob:
        return "remote"
    return ""


def fuzzy_key(company: str | None, title: str | None, location_text: str | None) -> str:
    return "|".join(
        [
            normalize_company(company),
            normalize_title(title),
            normalize_location(location_text),
        ]
    )
