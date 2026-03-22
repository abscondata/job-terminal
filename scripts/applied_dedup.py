"""Load applied jobs CSV and provide fuzzy matching for dedup."""
from __future__ import annotations

import csv
import re
from pathlib import Path

APPLIED_CSV = Path.home() / "Desktop" / "job apps" / "applied_jobs_cleaned.csv"

_NORM_RE = re.compile(r"[^a-z0-9 ]")
_MULTI_SPACE = re.compile(r"\s+")
_SUFFIX_RE = re.compile(
    r"\b(?:inc|llc|ltd|corp|co|company|group|holdings|international|partners|"
    r"search|recruiting|staffing|solutions|consulting|services|the)\b"
)


def _norm(text: str) -> str:
    t = (text or "").lower().strip()
    t = _NORM_RE.sub(" ", t)
    t = _SUFFIX_RE.sub("", t)
    return _MULTI_SPACE.sub(" ", t).strip()


def _title_core(title: str) -> str:
    """Extract the core role words, drop seniority prefixes and trailing qualifiers."""
    t = _norm(title)
    # drop common prefixes
    for prefix in ("senior ", "junior ", "sr ", "jr ", "lead ", "staff "):
        if t.startswith(prefix):
            t = t[len(prefix):]
    # drop parenthetical
    t = re.sub(r"\(.*?\)", "", t).strip()
    return t


def load_applied_pairs(csv_path: str | Path | None = None) -> list[tuple[str, str]]:
    """Return list of (normalized_company, normalized_title) from applied CSV."""
    path = Path(csv_path) if csv_path else APPLIED_CSV
    if not path.exists():
        return []
    pairs = []
    with open(path, "r", encoding="utf-8", errors="replace") as f:
        reader = csv.DictReader(f)
        for row in reader:
            company = _norm(row.get("company", "") or row.get("company_raw", ""))
            title = _title_core(row.get("title", "") or row.get("title_raw", ""))
            if company and title:
                pairs.append((company, title))
    return pairs


def build_applied_index(csv_path: str | Path | None = None) -> set[str]:
    """Build a set of 'company|title' keys for fast lookup."""
    pairs = load_applied_pairs(csv_path)
    return {f"{c}|{t}" for c, t in pairs}


def is_already_applied(
    company: str,
    title: str,
    applied_index: set[str],
) -> bool:
    """Check if a company+title pair fuzzy-matches anything in applied list."""
    c = _norm(company)
    t = _title_core(title)
    if not c or not t:
        return False

    # Exact match
    if f"{c}|{t}" in applied_index:
        return True

    # Check if applied company is a substring of discovered company or vice versa
    for key in applied_index:
        ac, at = key.split("|", 1)
        # Company must overlap
        if ac not in c and c not in ac:
            continue
        # Title must share significant words
        t_words = set(t.split())
        at_words = set(at.split())
        if not t_words or not at_words:
            continue
        overlap = t_words & at_words
        # If >50% of the smaller set overlaps, it's a match
        min_len = min(len(t_words), len(at_words))
        if min_len > 0 and len(overlap) / min_len >= 0.6:
            return True

    return False
