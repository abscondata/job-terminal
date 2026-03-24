from __future__ import annotations

import re
from dataclasses import dataclass, field

HOURS_PER_YEAR = 2080
DAYS_PER_YEAR = 260
WEEKS_PER_YEAR = 52
MONTHS_PER_YEAR = 12

_MOJI_EUR = "\u00e2\u201a\u00ac"
_MOJI_GBP = "\u00c2\u00a3"
_MOJI_ENDASH = "\u00e2\u20ac\u201c"
_MOJI_EMDASH = "\u00e2\u20ac\u201d"
_CUR = rf"(?:[$\u20ac\u00a3]|{re.escape(_MOJI_EUR)}|{re.escape(_MOJI_GBP)})"
_DASH = rf"(?:-|\u2013|\u2014|{re.escape(_MOJI_ENDASH)}|{re.escape(_MOJI_EMDASH)})"
NUM = re.compile(rf"(?:{_CUR})?\s*([\d]+(?:[.,]\d+)?)\s*(k)?\b", re.I)
RANGE = re.compile(
    rf"((?:{_CUR})?\s*[\d]+(?:[.,]\d+)?\s*(?:k)?)\s*(?:{_DASH}|to)\s*"
    rf"((?:{_CUR})?\s*[\d]+(?:[.,]\d+)?\s*(?:k)?)",
    re.I,
)
HOUR = re.compile(r"(?:/hr|/hour|per\s+hour|hourly|an?\s+hour)\b", re.I)
YEAR = re.compile(r"(?:/yr|/year|per\s+year|annual(?:ly)?|a\s+year)\b", re.I)
MONTH = re.compile(r"(?:/mo(?:nth)?|per\s+month|monthly|p\.m\.|a\s+month)\b", re.I)
WEEK = re.compile(r"(?:/wk|/week|per\s+week|weekly|a\s+week)\b", re.I)
DAY = re.compile(r"(?:/day|per\s+day|daily|a\s+day)\b", re.I)


def _to_int(tok: str) -> int | None:
    m = NUM.search(tok or "")
    if not m:
        return None

    raw = m.group(1)
    if "," in raw:
        parts = raw.split(",")
        if len(parts) == 2 and len(parts[1]) == 1:
            n = float(parts[0] + "." + parts[1])
        else:
            n = float(raw.replace(",", ""))
    elif "." in raw:
        n = float(raw)
    else:
        n = float(raw)

    n = int(n) if n == int(n) else n
    if m.group(2):
        n *= 1000
    return int(n)


def _detect_currency(text: str) -> str:
    t = text or ""
    if "\u20ac" in t or _MOJI_EUR in t or re.search(r"\bEUR\b", t, re.I):
        return "EUR"
    if "\u00a3" in t or _MOJI_GBP in t or re.search(r"\bGBP\b", t, re.I):
        return "GBP"
    return "USD"


def parse_compensation(text: str | None) -> tuple[int | None, int | None, str]:
    """Return annualized (min, max, unit)."""
    t = (text or "").strip()
    if not t:
        return None, None, "unknown"

    unit = "unknown"
    if HOUR.search(t):
        unit = "hourly"
    if DAY.search(t):
        unit = "daily"
    if WEEK.search(t):
        unit = "weekly"
    if MONTH.search(t):
        unit = "monthly"
    if YEAR.search(t):
        unit = "annual"

    lo = hi = None
    rm = RANGE.search(t)
    if rm:
        lo = _to_int(rm.group(1))
        hi = _to_int(rm.group(2))
    else:
        n = _to_int(t)
        lo = hi = n

    if unit == "hourly":
        if lo is not None:
            lo *= HOURS_PER_YEAR
        if hi is not None:
            hi *= HOURS_PER_YEAR
        return lo, hi, "hourly"

    if unit == "daily":
        if lo is not None:
            lo *= DAYS_PER_YEAR
        if hi is not None:
            hi *= DAYS_PER_YEAR
        return lo, hi, "daily"

    if unit == "weekly":
        if lo is not None:
            lo *= WEEKS_PER_YEAR
        if hi is not None:
            hi *= WEEKS_PER_YEAR
        return lo, hi, "weekly"

    if unit == "monthly":
        if lo is not None:
            lo *= MONTHS_PER_YEAR
        if hi is not None:
            hi *= MONTHS_PER_YEAR
        return lo, hi, "monthly"

    return lo, hi, ("annual" if (lo or 0) >= 20000 else unit)


_DESC_COMP_RE = re.compile(
    rf"(?:{_CUR})\s*([\d]+(?:[.,]\d+)?)\s*(k)?"
    rf"(?:\s*(?:{_DASH}|to)\s*(?:{_CUR})?\s*([\d]+(?:[.,]\d+)?)\s*(k)?)?"
    r"\s*(?:USD|CAD|EUR|GBP)?"
    r"\s*(?:per\s+|/|a\s+)"
    r"(month|mo|week|wk|hour|hr|year|yr|day)",
    re.I,
)


def _desc_to_int(raw: str, has_k: str | None) -> int:
    if "," in raw:
        parts = raw.split(",")
        if len(parts) == 2 and len(parts[1]) == 1:
            n = float(parts[0] + "." + parts[1])
        else:
            n = float(raw.replace(",", ""))
    else:
        n = float(raw)
    if has_k:
        n *= 1000
    return int(n)


def extract_comp_from_description(desc: str) -> tuple[int | None, int | None, str]:
    if not desc:
        return None, None, "unknown"

    m = _DESC_COMP_RE.search(desc)
    if not m:
        return None, None, "unknown"

    lo_raw = _desc_to_int(m.group(1), m.group(2))
    hi_raw = lo_raw
    if m.group(3):
        hi_raw = _desc_to_int(m.group(3), m.group(4))

    period = m.group(5).lower()
    if period in ("month", "mo"):
        return lo_raw * MONTHS_PER_YEAR, hi_raw * MONTHS_PER_YEAR, "monthly"
    if period in ("week", "wk"):
        return lo_raw * WEEKS_PER_YEAR, hi_raw * WEEKS_PER_YEAR, "weekly"
    if period in ("hour", "hr"):
        return lo_raw * HOURS_PER_YEAR, hi_raw * HOURS_PER_YEAR, "hourly"
    if period == "day":
        return lo_raw * DAYS_PER_YEAR, hi_raw * DAYS_PER_YEAR, "daily"
    if period in ("year", "yr"):
        return lo_raw, hi_raw, "annual"

    return None, None, "unknown"


@dataclass
class CompRecord:
    comp_text_raw: str = ""
    comp_period: str = "unknown"
    comp_currency: str = "USD"
    comp_min: int | None = None
    comp_max: int | None = None
    comp_annual_min: int | None = None
    comp_annual_max: int | None = None
    comp_annual_usd_min: int | None = None
    comp_annual_usd_max: int | None = None
    comp_source: str = "missing"
    comp_confidence: int = 0
    comp_parse_notes: list[str] = field(default_factory=list)
    comp_discrepancy: bool = False


def _estimate_comp_from_title(tl: str) -> int:
    estimates = [
        ("assistant", 45000),
        ("coordinator", 52000),
        ("specialist", 56000),
        ("analyst", 65000),
        ("associate", 55000),
        ("manager", 72000),
        ("director", 95000),
        ("vp", 120000),
        ("paralegal", 55000),
        ("accountant", 60000),
        ("auditor", 65000),
        ("compliance", 65000),
        ("operations", 58000),
        ("examiner", 60000),
    ]
    for keyword, est in estimates:
        if keyword in tl:
            return est
    return 55000


def build_comp_record(
    comp_text: str = "",
    comp_min: float | None = None,
    comp_max: float | None = None,
    description: str = "",
    title: str = "",
) -> CompRecord:
    rec = CompRecord()
    rec.comp_text_raw = (comp_text or "").strip()
    rec.comp_currency = _detect_currency(comp_text or description or "")

    primary_annual_min = None
    primary_annual_max = None

    clo, chi, unit = parse_compensation(comp_text)
    if clo is not None:
        rec.comp_source = "explicit"
        rec.comp_confidence = 85
        rec.comp_period = unit
        primary_annual_min = clo
        primary_annual_max = chi
        rec.comp_parse_notes.append(f"parsed_comp_text:{unit}")
    elif comp_min is not None or comp_max is not None:
        rec.comp_source = "explicit"
        rec.comp_confidence = 80
        primary_annual_min = int(comp_min) if comp_min else None
        primary_annual_max = int(comp_max) if comp_max else None
        rec.comp_parse_notes.append("from_job_fields")
    else:
        desc_lo, desc_hi, desc_unit = extract_comp_from_description(description)
        if desc_lo is not None and desc_unit != "unknown":
            rec.comp_source = "explicit"
            rec.comp_confidence = 70
            rec.comp_period = desc_unit
            primary_annual_min = desc_lo
            primary_annual_max = desc_hi
            rec.comp_parse_notes.append(f"from_description:{desc_unit}")
        elif rec.comp_text_raw:
            rec.comp_source = "explicit"
            rec.comp_confidence = 10
            rec.comp_parse_notes.append("unparseable_comp_text")
        else:
            estimate = _estimate_comp_from_title((title or "").lower())
            rec.comp_source = "inferred"
            rec.comp_confidence = 30
            primary_annual_min = estimate
            primary_annual_max = estimate
            rec.comp_parse_notes.append(f"title_estimate:{estimate}")

    rec.comp_annual_min = primary_annual_min
    rec.comp_annual_max = primary_annual_max

    if rec.comp_currency == "USD":
        rec.comp_annual_usd_min = primary_annual_min
        rec.comp_annual_usd_max = primary_annual_max
    else:
        rec.comp_annual_usd_min = None
        rec.comp_annual_usd_max = None
        if primary_annual_min is not None:
            rec.comp_parse_notes.append(
                f"no_fx:{rec.comp_currency}_annual={primary_annual_min}"
            )

    if primary_annual_min is not None:
        if rec.comp_period == "monthly":
            rec.comp_min = primary_annual_min // MONTHS_PER_YEAR
            rec.comp_max = (
                primary_annual_max // MONTHS_PER_YEAR if primary_annual_max else None
            )
        elif rec.comp_period == "hourly":
            rec.comp_min = primary_annual_min // HOURS_PER_YEAR
            rec.comp_max = (
                primary_annual_max // HOURS_PER_YEAR if primary_annual_max else None
            )
        elif rec.comp_period == "weekly":
            rec.comp_min = primary_annual_min // WEEKS_PER_YEAR
            rec.comp_max = (
                primary_annual_max // WEEKS_PER_YEAR if primary_annual_max else None
            )
        elif rec.comp_period == "daily":
            rec.comp_min = primary_annual_min // DAYS_PER_YEAR
            rec.comp_max = (
                primary_annual_max // DAYS_PER_YEAR if primary_annual_max else None
            )
        else:
            rec.comp_min = primary_annual_min
            rec.comp_max = primary_annual_max

    if rec.comp_source == "explicit" and rec.comp_confidence >= 70:
        desc_lo, desc_hi, desc_unit = extract_comp_from_description(description)
        if desc_lo is not None and desc_unit != "unknown" and primary_annual_min is not None:
            desc_mid = ((desc_lo or 0) + (desc_hi or desc_lo or 0)) / 2
            src_mid = (
                (primary_annual_min or 0)
                + (primary_annual_max or primary_annual_min or 0)
            ) / 2
            if src_mid > 0 and desc_mid > 0:
                ratio = max(src_mid, desc_mid) / min(src_mid, desc_mid)
                if ratio >= 2.0:
                    rec.comp_discrepancy = True
                    rec.comp_parse_notes.append(
                        f"discrepancy:src_mid={int(src_mid)},desc_mid={int(desc_mid)},ratio={ratio:.1f}"
                    )
                if desc_mid < src_mid * 0.7:
                    rec.comp_annual_min = desc_lo
                    rec.comp_annual_max = desc_hi
                    if rec.comp_currency == "USD":
                        rec.comp_annual_usd_min = desc_lo
                        rec.comp_annual_usd_max = desc_hi
                    rec.comp_period = desc_unit
                    rec.comp_parse_notes.append(
                        f"desc_override:{desc_unit}_${int(desc_mid):,}"
                    )

    return rec
