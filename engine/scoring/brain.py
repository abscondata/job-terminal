"""Lane-based scorer for direction, platform, and slop review."""
from __future__ import annotations

from dataclasses import asdict
import re

from engine.compensation import build_comp_record

from .gates import infer_arrangement
from .models import EvalResult, GateResult, GateStatus, ScoreResult
from .profile import UserProfile
from .router import route
from .targeting import (
    assess_location_priority,
    assess_market_preference,
    city_lane,
    classify_exclusion,
    corporate_slop_hits,
    french_access,
    function_hits,
    primary_function_family,
    realism_risks,
    resolve_work_location,
    role_bucket,
    role_bucket_bonus,
    seniority_risk,
    work_type,
    world_hits,
    world_tier,
)


_SCAM_RE = re.compile(
    r"\b(?:call center|cold calling|commission(?:-based)?|outbound sales|"
    r"webcam monitoring|activity monitoring|time tracking software)\b",
    re.I,
)


def _clamp(value: float) -> int:
    return max(0, min(100, round(value)))


def _band(score: int) -> str:
    if score >= 80:
        return "High"
    if score >= 60:
        return "Medium"
    return "Low"


def _hard_reject_reason(title: str, desc: str, company: str, extra_phrases: list[str]) -> str | None:
    for blob in (title, desc, company):
        if _SCAM_RE.search(blob or ""):
            return "hard_reject:exploitative_or_sales"
    reason = classify_exclusion(title, desc)
    if reason:
        return reason
    full_blob = " ".join(part for part in (title, desc, company) if part).lower()
    for phrase in extra_phrases:
        if phrase and phrase in full_blob:
            return f"hard_reject:phrase:{phrase}"
    return None


def _comp_signal(comp_record) -> int:
    annual = (
        comp_record.comp_annual_usd_max
        or comp_record.comp_annual_usd_min
        or comp_record.comp_annual_max
        or comp_record.comp_annual_min
    )
    if annual is None:
        return 44 if comp_record.comp_source == "inferred" else 30
    if annual >= 110000:
        return 92
    if annual >= 90000:
        return 84
    if annual >= 75000:
        return 74
    if annual >= 60000:
        return 64
    if annual >= 45000:
        return 52
    return 38


def _platform_city_score(city_label: str) -> int:
    if city_label == "Paris":
        return 68
    if city_label == "Paris Region":
        return 64
    if city_label == "NYC":
        return 84
    if city_label == "Miami":
        return 56
    if city_label == "France Outside Paris":
        return 40
    if city_label == "US Other":
        return 42
    if city_label == "Remote":
        return 16
    return 28


def _escape_score(world_score: int, family_score: int, slop_hits: list[str], function_signals: list[str]) -> int:
    score = 18 + (world_score * 0.34) + (family_score * 0.30)
    if {"compliance", "kyc", "aml", "onboarding", "securities", "licensing", "regulatory"} & set(function_signals):
        score += 22
    if "support_center" in slop_hits:
        score -= 20
    if "wrong_industry" in slop_hits:
        score -= 18
    return _clamp(score)


def _practicality_score(
    bucket: dict,
    family_score: int,
    seniority: dict,
    french: dict,
    realism_flags: list[str],
) -> int:
    score = (role_bucket_bonus(bucket) * 0.28) + (family_score * 0.30) + (seniority["score"] * 0.20) + (french["score"] * 0.22)
    if "direct_luxury_experience_required" in realism_flags:
        score -= 12
    if "specialist_art_market_background" in realism_flags:
        score -= 18
    if "degree_gate" in realism_flags:
        score -= 10
    return _clamp(score)


def _direction_signal(
    city_score: int,
    world_score: int,
    family_score: int,
    escape_score: int,
    french_score: int,
) -> int:
    return _clamp((city_score * 0.28) + (world_score * 0.26) + (family_score * 0.22) + (escape_score * 0.16) + (french_score * 0.08))


def _bridge_signal(
    direction_signal: int,
    practicality_score: int,
    bucket: dict,
    seniority: dict,
) -> int:
    return _clamp((direction_signal * 0.45) + (practicality_score * 0.35) + (role_bucket_bonus(bucket) * 0.10) + (seniority["score"] * 0.10))


def _platform_signal(
    city_label: str,
    comp_signal: int,
    world_profile: dict,
    seniority: dict,
) -> int:
    prestige = 78 if world_profile["top_brand"] else world_profile["score"]
    return _clamp(
        (_platform_city_score(city_label) * 0.34)
        + (comp_signal * 0.30)
        + (prestige * 0.20)
        + (seniority["score"] * 0.16)
    )


def _risk_signal(
    seniority: dict,
    french: dict,
    realism_flags: list[str],
    slop_hits: list[str],
) -> int:
    score = 12
    if seniority["risk"] == "medium":
        score += 26
    elif seniority["risk"] == "high":
        score += 48
    if "degree_gate" in realism_flags:
        score += 12
    if "support_center" in slop_hits:
        score += 18
    if "wrong_industry" in slop_hits:
        score += 22
    return _clamp(score)


def _biggest_resume_gap(seniority: dict, french: dict, realism_flags: list[str], function_family: str) -> str:
    if seniority["risk"] == "high":
        return "Role requires more experience than current profile"
    if "degree_gate" in realism_flags:
        return "Degree requirement may screen out"
    if seniority["risk"] == "medium":
        return "Stretch on years — 3-4 preferred"
    return "No single fatal gap"


def _main_risk(
    classification: str,
    seniority: dict,
    french: dict,
    realism_flags: list[str],
    slop_hits: list[str],
) -> str:
    if classification == "Too Senior":
        return "Role level requires more years of experience than the current profile supports."
    if classification == "Low-Value Slop":
        return "Role is off-function or in a non-target industry."
    if classification == "Off-Mission":
        return "Location is outside NYC 5 boroughs."
    if classification == "Top-Brand Wrong-Function Risk":
        return "Strong brand but the function may not align with compliance/KYC/onboarding."
    if "degree_gate" in realism_flags:
        return "The posting signals a degree requirement that may screen out."
    if seniority["risk"] == "medium":
        return "Stretch on level — 3-4 years preferred, may still be reachable."
    if "wrong_industry" in slop_hits:
        return "Industry is outside financial services target."
    return "No single fatal risk identified — review posting details."


def _slop_verdict(classification: str, escape_score: int, top_brand_risk: bool, slop_hits: list[str]) -> str:
    if classification in {"Low-Value Slop", "Off-Mission"}:
        return "Skip"
    if classification == "Too Senior":
        return "Skip"
    if top_brand_risk:
        return "Mixed — strong brand, check function"
    if escape_score >= 65:
        return "Strong fit"
    if escape_score >= 45:
        return "Decent fit"
    return "Weak fit"


def _why_surfaced(city_label: str, world_label: str, function_family: str, work_type_label: str) -> str:
    return f"NYC {function_family.lower()} role in {world_label.lower()} ({work_type_label.lower()})."


def _why_fit(city_info: dict, world_profile: dict, family: dict, bucket: dict, work_type_profile: dict) -> str:
    parts: list[str] = []
    if city_info["label"] == "NYC":
        parts.append("NYC location matches target")
    if world_profile["top_brand"]:
        parts.append(f"top-tier firm ({world_profile['reason'].replace('_', ' ')})")
    elif world_profile["label"] in {"Financial Services World", "Fintech World"}:
        parts.append("financial services environment aligns with BD compliance background")
    if family["strength"] == "core":
        parts.append(f"function ({family['label']}) directly matches resume strengths")
    elif family["strength"] == "adjacent":
        parts.append(f"function ({family['label']}) is adjacent to compliance ops")
    return "; ".join(parts) or "Some fit signals but review posting details."


def _why_fail(
    classification: str,
    city_info: dict,
    french: dict,
    slop_hits: list[str],
    biggest_gap: str,
) -> str:
    parts: list[str] = []
    if classification == "Low-Value Slop":
        parts.append("function or industry does not match compliance target")
    if classification == "Off-Mission":
        parts.append("location is outside NYC 5 boroughs")
    if city_info["label"] != "NYC":
        parts.append(f"location ({city_info['label']}) is not in NYC")
    if "wrong_industry" in slop_hits:
        parts.append("industry is outside financial services")
    if biggest_gap != "No single fatal gap":
        parts.append(biggest_gap)
    return "; ".join(parts) or "No decisive failure point surfaced."


def _path_logic(
    classification: str,
    city_label: str,
    world_label: str,
    function_family: str,
) -> str:
    if classification == "NYC Direction":
        return f"Direct compliance/operations path in NYC. {function_family} at a {world_label.lower()} firm builds the resume for the next move."
    if classification == "Money / Platform Leap":
        return f"Platform upgrade through {world_label.lower()} — the brand and comp strengthen the resume even if function is adjacent."
    if classification == "Top-Brand Wrong-Function Risk":
        return f"The {world_label.lower()} brand helps, but the function ({function_family.lower()}) may not build compliance credentials."
    if classification == "Interesting Stretch":
        return f"Reachable stretch in {function_family.lower()} — worth applying if the posting doesn't hard-require 5+ years."
    if classification == "Off-Mission":
        return "Location is outside NYC — does not advance the current search."
    return "Review the posting to confirm fit with compliance/KYC/onboarding focus."


def _role_feel(classification: str) -> str:
    mapping = {
        "Paris Direction": "Strong direction leap",
        "NYC Direction": "Direction lane with better realism",
        "Money / Platform Leap": "Money/platform leap",
        "Strategic Internship / Traineeship": "Strategic junior reset",
        "Top-Brand Wrong-Function Risk": "Top-brand but wrong-function risk",
        "Interesting Stretch": "Interesting stretch",
        "French-Heavy Stretch": "French-heavy stretch",
        "Miami Option": "Tertiary city option",
        "Low-Value Slop": "Low-value slop",
        "Too Senior": "Too senior",
        "Off-Mission": "Off-mission",
        "Maybe Interesting": "Maybe interesting",
    }
    return mapping.get(classification, classification)


def _one_line_recommendation(classification: str, recommendation: str, slop_verdict: str) -> str:
    if recommendation == "apply":
        return f"Apply: {classification.lower()} with {slop_verdict.lower()} upside."
    if recommendation == "maybe":
        return f"Maybe: {classification.lower()} if you accept the tradeoffs."
    return f"Skip: {classification.lower()} and not worth the leap."


class ScoringBrain:
    """Stateless scorer for path analysis, triage, and review."""

    def __init__(self, cfg, profile: UserProfile | None = None):
        self.cfg = cfg
        self.profile = profile or UserProfile.from_config(cfg)
        self.queues = cfg.queues
        self._extra_scam = [phrase.lower() for phrase in getattr(cfg, "hard_reject_phrases", []) or []]

    def score(self, job: dict) -> ScoreResult:
        result = ScoreResult()
        title = job.get("title") or ""
        desc = job.get("description_text") or ""
        company = job.get("company") or ""
        location_text = job.get("location_text") or ""

        arrangement, arrangement_reason = infer_arrangement(job)
        location_resolution = resolve_work_location(location_text, desc, arrangement)
        market = assess_market_preference(location_resolution["resolved_location"] or location_text, self.profile)
        location_priority = assess_location_priority(
            location_resolution["resolved_location"] or location_text,
            arrangement,
            self.profile,
        )
        city_info = city_lane(location_priority["label"])
        comp_record = build_comp_record(
            job.get("compensation_text") or "",
            job.get("compensation_min"),
            job.get("compensation_max"),
            desc,
            title,
        )

        result.work_arrangement = arrangement
        result.raw_location = location_text
        result.resolved_location = location_resolution["resolved_location"]
        result.resolved_location_source = location_resolution["source"]
        result.resolved_location_reason = location_resolution["reason"]
        result.preferred_market = self.profile.target_market
        result.preferred_market_score = market["score"]
        result.target_geography = market["label"]
        result.location_priority = location_priority["label"]
        result.location_priority_score = location_priority["score"]
        result.city_lane = city_info["label"]
        result.city_priority_label = city_info["priority"]
        result.city_story = city_info["story"]
        result.comp_record = asdict(comp_record)

        hard_reject = _hard_reject_reason(title, desc, company, self._extra_scam)
        if hard_reject:
            result.gates = [GateResult("hard_reject", GateStatus.FAIL, hard_reject)]
            result.gate_pass = False
            result.gate_status = "fail"
            result.reject_kind = "hard"
            result.score = 0
            result.overall_score = 0
            result.qual_score = 0
            result.fit_score = 0
            result.bridge_score = 0
            result.p_qual = 0.0
            result.p_qual_confidence = 95
            result.classification = "Off-Mission"
            result.role_feel = _role_feel(result.classification)
            result.recommendation = "skip"
            result.explanation = hard_reject
            result.main_risk = "The role is off-mission or exploitative."
            result.why_fail = result.main_risk
            result.one_line_recommendation = _one_line_recommendation(result.classification, result.recommendation, "Prettier slop")
            result.reasons_neg = [hard_reject]
            result.factors = {
                "classification": result.classification,
                "recommendation": result.recommendation,
                "risk_flags": [],
                "role_bucket": result.role_bucket,
                "target_geography": result.target_geography,
                "location_priority": result.location_priority,
                "comp_record": result.comp_record,
                "arrangement_reason": arrangement_reason,
            }
            route(result, self.queues, self.profile)
            return result

        world = world_hits(f"{title} {desc}", company)
        world_profile = world_tier(f"{title} {desc}", company)
        function_signals = function_hits(f"{title} {desc}")
        family = primary_function_family(title, desc)
        slop_hits = corporate_slop_hits(f"{company} {title} {desc}")
        bucket = role_bucket(title, desc)
        french = french_access(desc, title)
        seniority = seniority_risk(title, desc)
        realism_flags = realism_risks(title, desc)
        work_type_profile = work_type(title, desc)

        city_score = city_info["score"]
        world_score = world_profile["score"]
        family_score = family["score"]
        comp_signal = _comp_signal(comp_record)
        escape_score = _escape_score(world_score, family_score, slop_hits, function_signals)
        practicality_score = _practicality_score(bucket, family_score, seniority, french, realism_flags)
        direction_signal = _direction_signal(city_score, world_score, family_score, escape_score, french["score"])
        bridge_signal = _bridge_signal(direction_signal, practicality_score, bucket, seniority)
        platform_signal = _platform_signal(city_info["label"], comp_signal, world_profile, seniority)
        risk_signal = _risk_signal(seniority, french, realism_flags, slop_hits)

        # --- NYC Compliance classification logic ---
        is_nyc = city_info["label"] == "NYC"
        is_core_function = family["strength"] == "core"
        is_adjacent_function = family["strength"] == "adjacent"
        is_financial_world = world_profile["label"] in {"Top Luxury / Culture World", "Financial Services World", "Fintech World"}
        is_top_brand = world_profile["top_brand"]
        sales_slop = family["label"] == "Sales / Client Service" or "sales_heavy" in realism_flags
        wrong_city = not is_nyc

        # Compliance-specific lane flags
        strong_compliance_fit = is_nyc and is_core_function and (is_financial_world or is_top_brand) and seniority["risk"] != "high"
        good_compliance_fit = is_nyc and is_core_function and seniority["risk"] != "high"
        adjacent_fit = is_nyc and is_adjacent_function and (is_financial_world or is_top_brand) and seniority["risk"] != "high"
        stretch_fit = is_nyc and is_core_function and seniority["risk"] == "medium"
        platform_move = is_nyc and is_top_brand and not is_core_function and not is_adjacent_function

        lanes: list[str] = []
        if strong_compliance_fit:
            lanes.append("NYC Direction")
        if good_compliance_fit and not strong_compliance_fit:
            lanes.append("NYC Direction")
        if adjacent_fit:
            lanes.append("Money / Platform Leap")
        if stretch_fit:
            lanes.append("Interesting Stretch")
        if platform_move:
            lanes.append("Top-Brand Wrong-Function Risk")
        if sales_slop:
            lanes.append("Low-Value Slop Risk")
        result.opportunity_lanes = lanes

        if sales_slop:
            classification = "Low-Value Slop"
            recommendation = "skip"
        elif seniority["risk"] == "high":
            classification = "Too Senior"
            recommendation = "skip"
        elif wrong_city:
            classification = "Off-Mission"
            recommendation = "skip"
        elif strong_compliance_fit:
            classification = "NYC Direction"
            recommendation = "apply" if bridge_signal >= 50 else "maybe"
        elif good_compliance_fit:
            classification = "NYC Direction"
            recommendation = "apply" if bridge_signal >= 55 else "maybe"
        elif adjacent_fit:
            classification = "Money / Platform Leap"
            recommendation = "apply" if bridge_signal >= 55 and platform_signal >= 60 else "maybe"
        elif stretch_fit:
            classification = "Interesting Stretch"
            recommendation = "maybe"
        elif platform_move:
            classification = "Top-Brand Wrong-Function Risk"
            recommendation = "maybe"
        elif is_nyc and family["strength"] != "off_lane":
            classification = "Maybe Interesting"
            recommendation = "maybe"
        else:
            classification = "Maybe Interesting"
            recommendation = "skip" if family["strength"] == "off_lane" and not is_top_brand else "maybe"

        # Guard: unknown world with no financial signals is suspect
        if recommendation in {"apply", "maybe"} and world_profile["label"] == "Unknown World" and family["strength"] == "off_lane":
            recommendation = "skip"
            classification = "Maybe Interesting"

        biggest_resume_gap = _biggest_resume_gap(seniority, french, realism_flags, family["label"])
        main_risk = _main_risk(classification, seniority, french, realism_flags, slop_hits)
        slop_verdict = _slop_verdict(classification, escape_score, is_top_brand and not is_core_function, slop_hits)
        why_fit = _why_fit(city_info, world_profile, family, bucket, work_type_profile)
        why_fail = _why_fail(classification, city_info, french, slop_hits, biggest_resume_gap)
        path_logic = _path_logic(classification, city_info["label"], world_profile["label"], family["label"])
        why_surfaced = _why_surfaced(city_info["label"], world_profile["label"], family["label"], work_type_profile["label"])
        role_feel = _role_feel(classification)
        one_line_recommendation = _one_line_recommendation(classification, recommendation, slop_verdict)

        result.gates = [GateResult("hard_reject", GateStatus.PASS, "passed")]
        result.gate_pass = True
        result.gate_status = "pass"
        result.p_qual = round(bridge_signal / 100, 4)
        result.p_qual_confidence = 80
        result.qual_score = direction_signal
        result.fit_score = practicality_score
        result.fit_score_absolute = practicality_score
        result.bridge_score = bridge_signal
        result.overall_score = bridge_signal
        result.score = bridge_signal
        result.role_family_match = round(family_score / 100, 3)
        result.scope = "apply_review" if recommendation == "apply" else "manual_review" if recommendation == "maybe" else "reject"
        result.role_bucket = bucket["label"]
        result.role_tier = bucket["tier"]
        result.classification = classification
        result.role_feel = role_feel
        result.recommendation = recommendation
        result.world_hits = world
        result.function_hits = function_signals
        result.risk_flags = list(dict.fromkeys(
            realism_flags
            + ([f"french:{french['reason']}"] if french["risk"] != "low" else [])
            + ([f"seniority:{seniority['reason']}"] if seniority["risk"] != "low" else [])
            + (["corporate_slop"] if "corporate" in slop_hits else [])
            + (["compliance_lane"] if "compliance" in slop_hits else [])
            + (["analyst_lane"] if "analyst" in slop_hits else [])
            + (["city_off_lane"] if city_info["priority"] == "off_lane" else [])
        ))
        result.dimension_scores = {
            "city": city_score,
            "world": world_score,
            "function": family_score,
            "escape": escape_score,
            "practicality": practicality_score,
            "compensation": comp_signal,
        }
        result.signal_scores = {
            "direction": direction_signal,
            "bridge": bridge_signal,
            "platform": platform_signal,
            "risk": risk_signal,
        }
        result.signal_bands = {key: _band(value) for key, value in result.signal_scores.items()}
        result.bridge_signal_band = result.signal_bands["bridge"]
        result.fit_signal_band = result.signal_bands["direction"]
        result.qual_reasoning = why_surfaced
        result.fit_reasoning = why_fit
        result.bridge_reasoning = path_logic
        result.bridge_story = path_logic
        result.slop_check = f"{slop_verdict}. {main_risk}"
        result.why_fit = why_fit
        result.why_fail = why_fail
        result.french_risk_label = {"high": "High French burden", "medium": "Medium French burden", "low": "Low French burden"}[french["risk"]]
        result.slop_verdict = slop_verdict
        result.biggest_resume_gap = biggest_resume_gap
        result.one_line_recommendation = one_line_recommendation
        result.explanation = f"{classification}: {why_surfaced} Main risk: {main_risk}"
        result.why_surfaced = why_surfaced
        result.why_could_matter = why_fit
        result.path_logic = path_logic
        result.main_risk = main_risk
        result.world_tier = world_profile["label"]
        result.world_story = world_profile["reason"].replace("_", " ")
        result.function_family = family["label"]
        result.function_family_detail = family["strength"].replace("_", " ")
        result.work_type = work_type_profile["key"]
        result.work_type_label = work_type_profile["label"]
        result.primary_lane = classification
        result.top_brand_risk = is_top_brand and not is_core_function
        result.reasons_pos = [
            f"city:{city_info['label']}",
            f"world:{world_profile['label']}",
            f"function:{family['label']}",
            f"direction:{result.signal_bands['direction']}",
        ]
        result.reasons_neg = result.risk_flags[:5]
        result.red_flags = result.risk_flags[:5]
        result.qual_evaluators = [
            EvalResult("city_lane", city_score, 0.92, city_info["story"], [city_info["label"]], city_info["priority"]),
            EvalResult("world_tier", world_score, 0.90, world_profile["label"], world, world_profile["reason"]),
            EvalResult("function_family", family_score, 0.88, family["label"], function_signals, family["strength"]),
            EvalResult("escape", escape_score, 0.84, slop_verdict, slop_hits, "escape"),
            EvalResult("practicality", practicality_score, 0.84, biggest_resume_gap, realism_flags, "practicality"),
        ]
        result.fit_evaluators = [
            EvalResult("direction", direction_signal, 0.88, why_fit, lanes, classification),
            EvalResult("platform", platform_signal, 0.82, path_logic, [city_info["label"]], "platform"),
            EvalResult("bridge", bridge_signal, 0.88, one_line_recommendation, lanes, classification),
        ]
        result.factors = {
            "classification": classification,
            "recommendation": recommendation,
            "city_lane": result.city_lane,
            "city_priority": result.city_priority_label,
            "opportunity_lanes": lanes,
            "world_tier": result.world_tier,
            "function_family": result.function_family,
            "work_type": result.work_type,
            "signal_scores": result.signal_scores,
            "signal_bands": result.signal_bands,
            "risk_flags": result.risk_flags,
            "bridge_story": result.bridge_story,
            "slop_check": result.slop_check,
            "why_fit": why_fit,
            "why_fail": why_fail,
            "french_risk_label": result.french_risk_label,
            "slop_verdict": slop_verdict,
            "biggest_resume_gap": biggest_resume_gap,
            "one_line_recommendation": one_line_recommendation,
            "explanation": result.explanation,
            "comp_record": result.comp_record,
            "arrangement_reason": arrangement_reason,
        }

        route(result, self.queues, self.profile)
        return result
