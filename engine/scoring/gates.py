"""Hard-gate detectors for location, fit blockers, and credentials."""
from __future__ import annotations

import re

from .models import GateResult, GateStatus
from .profile import UserProfile
from .targeting import assess_location_priority, assess_market_preference


SCAM_PHRASES = [
    "time tracking software",
    "keystroke logging",
    "keystroke monitoring",
    "screenshots of employee",
    "screenshots of employees",
    "screenshot employees",
    "webcam monitoring",
    "webcam required",
    "hubstaff",
    "teramind",
    "activtrak",
    "tmetric",
    "desktime",
    "employee monitoring software",
    "screen recording software",
    "activity tracking software",
    "call center",
    "inbound calls required",
    "high volume calls",
    "turbotax",
    "tax preparation",
    "tax season",
    "cold calling",
    "outbound sales",
    "commission-based",
    "commission only",
    "crypto trading",
    "forex trading",
    "day trading",
    "influencer",
    "content creator",
    "door to door",
    "door-to-door",
    "multi-level marketing",
    "mlm",
    "pyramid scheme",
]

_RESIDE_RE = re.compile(
    r"(?:must|required?\s+to|need\s+to)\s+"
    r"(?:reside|live|be\s+(?:located|based))\s+"
    r"(?:in|within)\s+(.{3,50}?)(?:\.|,|;|\n|$)",
    re.I,
)
_US_STATE_RE = re.compile(
    r"\b(?:Alabama|Alaska|Arizona|Arkansas|California|Colorado|Connecticut|"
    r"Delaware|Florida|Georgia|Hawaii|Idaho|Illinois|Indiana|Iowa|Kansas|"
    r"Kentucky|Louisiana|Maine|Maryland|Massachusetts|Michigan|Minnesota|"
    r"Mississippi|Missouri|Montana|Nebraska|Nevada|New\s+Hampshire|New\s+Jersey|"
    r"New\s+Mexico|New\s+York|North\s+Carolina|North\s+Dakota|Ohio|Oklahoma|"
    r"Oregon|Pennsylvania|Rhode\s+Island|South\s+Carolina|South\s+Dakota|"
    r"Tennessee|Texas|Utah|Vermont|Virginia|Washington|West\s+Virginia|"
    r"Wisconsin|Wyoming)\b",
    re.I,
)
_COUNTRY_GATE_RE = re.compile(
    r"(?:must\s+be\s+(?:authorized|eligible)\s+to\s+work\s+in|"
    r"work\s+(?:authorization|permit)\s+(?:required|needed)\s+(?:for|in)|"
    r"(?:legally|currently)\s+authorized\s+to\s+work\s+in|"
    r"eligible\s+to\s+work\s+in)\s+"
    r"(.{3,40}?)(?:\.|,|;|\n|$)",
    re.I,
)

_LANG_REQ_RE = re.compile(
    r"(?:fluency|fluent|proficien(?:t|cy)|native)\s+(?:in\s+)?(\w+)"
    r"|(\w+)\s+(?:fluency|proficiency|is\s+required|required|native\s+speaker)",
    re.I,
)
_LANG_NAMES = {
    "english",
    "french",
    "spanish",
    "german",
    "italian",
    "portuguese",
    "mandarin",
    "chinese",
    "japanese",
    "korean",
    "arabic",
    "hindi",
    "dutch",
    "russian",
    "polish",
    "turkish",
}

_LICENSE_PATTERNS = [
    (re.compile(r"\bseries\s*7\b", re.I), "series_7"),
    (re.compile(r"\bseries\s*66\b", re.I), "series_66"),
    (re.compile(r"\bseries\s*63\b", re.I), "series_63"),
    (re.compile(r"\bcpa\b|\bcertified\s+public\s+accountant\b", re.I), "cpa"),
    (re.compile(r"\bcfa\b", re.I), "cfa"),
    (re.compile(r"\bcams\b", re.I), "cams"),
    (re.compile(r"\bcrcm\b", re.I), "crcm"),
    (re.compile(r"\bjd\b|\bjuris\s+doctor\b", re.I), "jd"),
    (re.compile(r"\b(?:attorney|bar\s+admission|member\s+of\s+the\s+bar)\b", re.I), "attorney"),
]
_PREFERRED_CONTEXT_RE = re.compile(
    r"(?:prefer(?:red)?|nice\s+to\s+have|a\s+plus|bonus|desired|not\s+required|"
    r"willing\s+to\s+obtain|within\s+\d+\s+(?:months?|years?)|"
    r"ability\s+to\s+obtain|or\s+willingness\s+to\s+obtain)",
    re.I,
)

_REMOTE_BOARD_SOURCES = {"weworkremotely", "remoteok", "remotive", "himalayas", "jobspresso"}
_REMOTE_US_ONLY_SIGNALS = [
    "remote us only",
    "remote - us",
    "remote (us)",
    "remote, us",
    "united states only",
    "u.s. only",
    "us only",
    "usa only",
]
_REMOTE_ANYWHERE_SIGNALS = [
    "fully remote",
    "100% remote",
    "work from anywhere",
    "location agnostic",
    "global remote",
    "distributed team",
]
_HYBRID_SIGNALS = [
    "hybrid",
    "days in office",
    "days onsite",
    "days on-site",
    "in office",
    "in-office",
]
_ONSITE_SIGNALS = [
    "onsite",
    "on-site",
    "office-based",
    "in person",
    "in-person",
    "must report to office",
]
_REMOTE_NEGATION_RE = re.compile(
    r"\b(?:not\s+(?:a\s+)?remote|non[\s-]?remote|no\s+remote|remote\s+not\s+(?:available|offered))\b",
    re.I,
)


def infer_arrangement(job: dict) -> tuple[str, str]:
    arrangement = (job.get("remote_type") or "").lower().strip()
    if arrangement in {"remote_anywhere", "remote_us_only", "hybrid", "onsite"}:
        return arrangement, f"arrangement={arrangement}"

    desc = (job.get("description_text") or "").lower()
    title = (job.get("title") or "").lower()
    location = (job.get("location_text") or "").lower()
    blob = f"{title} {location} {desc}"
    source = (job.get("source") or "").lower()

    if _REMOTE_NEGATION_RE.search(blob):
        return "onsite", "onsite_signal:remote_negation"
    if any(signal in blob for signal in _ONSITE_SIGNALS):
        return "onsite", "onsite_signal:explicit"
    if any(signal in blob for signal in _HYBRID_SIGNALS):
        return "hybrid", "hybrid_signal:explicit"
    if any(signal in blob for signal in _REMOTE_US_ONLY_SIGNALS):
        return "remote_us_only", "remote_signal:us_only"
    if source in _REMOTE_BOARD_SOURCES:
        return "remote_anywhere", f"source_hint:{source}"
    if any(signal in blob for signal in _REMOTE_ANYWHERE_SIGNALS):
        return "remote_anywhere", "remote_signal:anywhere"
    if re.search(r"\bremote\b", blob):
        return "remote_anywhere", "remote_signal:generic"

    return "unknown", "arrangement_unknown"


def check_location(job: dict, profile: UserProfile) -> GateResult:
    arrangement, reason = infer_arrangement(job)
    priority = assess_location_priority(job.get("location_text") or "", arrangement, profile)

    if priority["label"] in {"paris_core", "paris_region"}:
        return GateResult("location", GateStatus.PASS, f"{priority['label']}:{reason}")

    if priority["label"] in {"unknown", "france_other"}:
        return GateResult("location", GateStatus.UNCLEAR, f"{priority['label']}:{reason}")

    return GateResult("location", GateStatus.FAIL, f"{priority['label']}:{reason}")


def check_residency(job: dict, profile: UserProfile) -> GateResult:
    desc = job.get("description_text") or ""
    loc = job.get("location_text") or ""
    blob = f"{desc} {loc}"

    match = _RESIDE_RE.search(blob)
    if match:
        location = match.group(1).strip()
        if _US_STATE_RE.search(location):
            state = _US_STATE_RE.search(location).group(0)
            if state.lower() == "florida":
                return GateResult("residency", GateStatus.PASS, "requires Florida; compatible with Miami lane")
            return GateResult("residency", GateStatus.UNCLEAR, f"state_restriction:{state}")
        if "us" in profile.work_auth_countries and re.search(r"\b(?:us|u\.s\.|united\s+states)\b", location, re.I):
            return GateResult("residency", GateStatus.PASS, "us_work_auth_ok")
        return GateResult("residency", GateStatus.UNCLEAR, f"location_restriction:{location}")

    country_match = _COUNTRY_GATE_RE.search(blob)
    if country_match:
        country = country_match.group(1).strip().lower()
        for auth in profile.work_auth_countries:
            if auth in country:
                return GateResult("residency", GateStatus.PASS, f"work_auth_ok:{country}")
        return GateResult("residency", GateStatus.UNCLEAR, f"work_auth_needed:{country}")

    location_market = assess_market_preference(loc, profile)
    if location_market["label"] == "other" and re.search(r"\b(?:must\s+be\s+local|commutable)\b", blob, re.I):
        return GateResult("residency", GateStatus.UNCLEAR, "local_commute_constraint")

    return GateResult("residency", GateStatus.PASS, "no_residency_constraint")


def check_language(job: dict, profile: UserProfile) -> GateResult:
    blob = f"{job.get('description_text') or ''} {job.get('language_requirements') or ''}"
    found_languages = set()
    for match in _LANG_REQ_RE.finditer(blob):
        language = (match.group(1) or match.group(2) or "").lower()
        if language in _LANG_NAMES:
            found_languages.add(language)

    if not found_languages:
        return GateResult("language", GateStatus.PASS, "no_language_requirement")

    user_languages = set(language.lower() for language in profile.languages)
    unsupported = found_languages - user_languages
    if not unsupported:
        return GateResult("language", GateStatus.PASS, "languages_met")

    lowered_blob = blob.lower()
    for language in unsupported:
        index = lowered_blob.find(language)
        if index >= 0:
            context = lowered_blob[max(0, index - 60): min(len(lowered_blob), index + 60)]
            if _PREFERRED_CONTEXT_RE.search(context):
                continue
            return GateResult("language", GateStatus.FAIL, f"requires_{language}")

    return GateResult("language", GateStatus.UNCLEAR, f"prefers_{','.join(sorted(unsupported))}")


def check_license(job: dict, profile: UserProfile) -> GateResult:
    blob = f"{job.get('title') or ''} {job.get('description_text') or ''}"
    required: list[str] = []
    preferred: list[str] = []

    for pattern, license_name in _LICENSE_PATTERNS:
        matches = list(pattern.finditer(blob))
        if not matches:
            continue
        is_preferred = False
        for match in matches:
            context = blob[max(0, match.start() - 80): min(len(blob), match.end() + 80)]
            if _PREFERRED_CONTEXT_RE.search(context):
                is_preferred = True
                break
        if is_preferred:
            preferred.append(license_name)
        else:
            required.append(license_name)

    user_licenses = set(license_name.lower() for license_name in profile.licenses_held)
    missing_required = [name for name in required if name not in user_licenses]
    missing_preferred = [name for name in preferred if name not in user_licenses]

    if not missing_required and not missing_preferred:
        return GateResult("license", GateStatus.PASS, "no_missing_credentials")

    if missing_required:
        hard_credentials = {"jd", "attorney", "cpa", "cfa"}
        if any(name in hard_credentials for name in missing_required):
            return GateResult("license", GateStatus.FAIL, f"hard_credential:{missing_required}")
        return GateResult("license", GateStatus.UNCLEAR, f"credential_gap:{missing_required}")

    return GateResult("license", GateStatus.PASS, f"preferred_credentials:{missing_preferred}")


def check_scam(job: dict, profile: UserProfile, extra_phrases: list[str] | None = None) -> GateResult:
    blob = " ".join(
        part.lower()
        for part in (
            job.get("title") or "",
            job.get("location_text") or "",
            job.get("description_text") or "",
        )
        if part
    )
    for phrase in SCAM_PHRASES + (extra_phrases or []):
        if phrase.lower() in blob:
            return GateResult("scam", GateStatus.FAIL, f"matched:{phrase}")
    return GateResult("scam", GateStatus.PASS, "no_scam_signals")


def check_red_flags(job: dict, profile: UserProfile) -> list[str]:
    blob = " ".join(
        part.lower() for part in (job.get("title") or "", job.get("description_text") or "") if part
    )
    flags: list[str] = []
    for phrase in profile.red_flag_phrases:
        pattern = r"\b" + re.escape(phrase.lower()) + r"\b"
        if re.search(pattern, blob):
            flags.append(phrase)
    return flags


def run_all_gates(
    job: dict,
    profile: UserProfile,
    extra_scam_phrases: list[str] | None = None,
) -> list[GateResult]:
    return [
        check_scam(job, profile, extra_scam_phrases),
        check_location(job, profile),
        check_residency(job, profile),
        check_language(job, profile),
        check_license(job, profile),
    ]
