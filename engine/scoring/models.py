"""Dataclasses for life-direction review outputs."""
from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum


class GateStatus(str, Enum):
    PASS = "pass"
    FAIL = "fail"
    UNCLEAR = "unclear"


@dataclass
class GateResult:
    name: str
    status: GateStatus
    detail: str = ""

    def failed(self) -> bool:
        return self.status == GateStatus.FAIL

    def unclear(self) -> bool:
        return self.status == GateStatus.UNCLEAR


@dataclass
class EvalResult:
    name: str
    score: int
    confidence: float
    detail: str = ""
    signals: list[str] = field(default_factory=list)
    category: str = ""


@dataclass
class RequirementResult:
    name: str
    p_met: float
    weight: float = 1.0
    detail: str = ""
    hard: bool = False


@dataclass
class ScoreResult:
    gates: list[GateResult] = field(default_factory=list)
    gate_pass: bool = True
    gate_status: str = "pass"

    p_qual: float = 0.5
    p_qual_confidence: int = 50
    ev: float = 0.0
    salary_usd: int | None = None

    qual_score: int = 50
    fit_score: int = 50
    bridge_score: int = 50
    overall_score: int = 50
    score: int = 50
    fit_score_absolute: int = 50
    role_family_match: float = 0.0
    scope: str = "manual_review"

    qual_reasoning: str = ""
    fit_reasoning: str = ""
    bridge_reasoning: str = ""

    qual_evaluators: list[EvalResult] = field(default_factory=list)
    fit_evaluators: list[EvalResult] = field(default_factory=list)
    requirements: list[RequirementResult] = field(default_factory=list)

    queue: int = 3
    queue_reason: str = ""
    confidence: float = 0.5
    reject_kind: str = ""
    why_not_auto: str = "manual_only"

    preferred_market: str = "paris"
    preferred_market_score: int = 30
    target_geography: str = "unknown"
    raw_location: str = ""
    resolved_location: str = ""
    resolved_location_source: str = ""
    resolved_location_reason: str = ""
    work_arrangement: str = "unknown"
    location_priority: str = "unknown"
    location_priority_score: int = 30

    role_bucket: str = "unclassified"
    role_tier: int = 0
    classification: str = "Maybe Interesting"
    recommendation: str = "maybe"
    explanation: str = ""
    bridge_story: str = ""
    slop_check: str = ""
    why_fit: str = ""
    why_fail: str = ""
    french_risk_label: str = ""
    slop_verdict: str = ""
    biggest_resume_gap: str = ""
    one_line_recommendation: str = ""
    city_lane: str = "Off-Lane"
    city_priority_label: str = "off_lane"
    city_story: str = ""
    opportunity_lanes: list[str] = field(default_factory=list)
    primary_lane: str = ""
    world_tier: str = "Unknown World"
    world_story: str = ""
    function_family: str = "Unclear"
    function_family_detail: str = ""
    work_type: str = "full_time"
    work_type_label: str = "Full-time"
    role_feel: str = ""
    why_surfaced: str = ""
    why_could_matter: str = ""
    path_logic: str = ""
    main_risk: str = ""
    bridge_signal_band: str = ""
    fit_signal_band: str = ""
    signal_scores: dict[str, int] = field(default_factory=dict)
    signal_bands: dict[str, str] = field(default_factory=dict)
    top_brand_risk: bool = False

    risk_flags: list[str] = field(default_factory=list)
    world_hits: list[str] = field(default_factory=list)
    function_hits: list[str] = field(default_factory=list)
    dimension_scores: dict[str, int] = field(default_factory=dict)

    reasons_pos: list[str] = field(default_factory=list)
    reasons_neg: list[str] = field(default_factory=list)
    red_flags: list[str] = field(default_factory=list)

    comp_record: dict = field(default_factory=dict)
    factors: dict = field(default_factory=dict)

    def to_evidence(self) -> dict:
        return {
            "p_qual": round(self.p_qual, 4),
            "p_qual_confidence": self.p_qual_confidence,
            "gate_status": self.gate_status,
            "score": self.score,
            "qual_score": self.qual_score,
            "fit_score": self.fit_score,
            "bridge_score": self.bridge_score,
            "overall_score": self.overall_score,
            "ev": round(self.ev, 6),
            "salary_usd": self.salary_usd,
            "fit_score_absolute": self.fit_score_absolute,
            "role_family_match": self.role_family_match,
            "scope": self.scope,
            "qual_reasoning": self.qual_reasoning,
            "fit_reasoning": self.fit_reasoning,
            "bridge_reasoning": self.bridge_reasoning,
            "gates": {g.name: g.status.value for g in self.gates},
            "queue": self.queue,
            "queue_reason": self.queue_reason,
            "confidence": round(self.confidence, 3),
            "reject_kind": self.reject_kind,
            "why_not_auto": self.why_not_auto,
            "preferred_market": self.preferred_market,
            "preferred_market_score": self.preferred_market_score,
            "target_geography": self.target_geography,
            "raw_location": self.raw_location,
            "resolved_location": self.resolved_location,
            "resolved_location_source": self.resolved_location_source,
            "resolved_location_reason": self.resolved_location_reason,
            "work_arrangement": self.work_arrangement,
            "location_priority": self.location_priority,
            "location_priority_score": self.location_priority_score,
            "role_bucket": self.role_bucket,
            "role_tier": self.role_tier,
            "classification": self.classification,
            "recommendation": self.recommendation,
            "explanation": self.explanation,
            "bridge_story": self.bridge_story,
            "slop_check": self.slop_check,
            "why_fit": self.why_fit,
            "why_fail": self.why_fail,
            "french_risk_label": self.french_risk_label,
            "slop_verdict": self.slop_verdict,
            "biggest_resume_gap": self.biggest_resume_gap,
            "one_line_recommendation": self.one_line_recommendation,
            "city_lane": self.city_lane,
            "city_priority_label": self.city_priority_label,
            "city_story": self.city_story,
            "opportunity_lanes": self.opportunity_lanes,
            "primary_lane": self.primary_lane,
            "world_tier": self.world_tier,
            "world_story": self.world_story,
            "function_family": self.function_family,
            "function_family_detail": self.function_family_detail,
            "work_type": self.work_type,
            "work_type_label": self.work_type_label,
            "role_feel": self.role_feel,
            "why_surfaced": self.why_surfaced,
            "why_could_matter": self.why_could_matter,
            "path_logic": self.path_logic,
            "main_risk": self.main_risk,
            "bridge_signal_band": self.bridge_signal_band,
            "fit_signal_band": self.fit_signal_band,
            "signal_scores": self.signal_scores,
            "signal_bands": self.signal_bands,
            "top_brand_risk": self.top_brand_risk,
            "risk_flags": self.risk_flags,
            "world_hits": self.world_hits,
            "function_hits": self.function_hits,
            "dimension_scores": self.dimension_scores,
            "reasons_pos": self.reasons_pos[:10],
            "reasons_neg": self.reasons_neg[:10],
            "red_flags": self.red_flags,
            "qual_evaluators": [
                {
                    "name": evaluator.name,
                    "score": evaluator.score,
                    "confidence": round(evaluator.confidence, 3),
                    "detail": evaluator.detail,
                    "category": evaluator.category,
                    "signals": evaluator.signals,
                }
                for evaluator in self.qual_evaluators
            ],
            "fit_evaluators": [
                {
                    "name": evaluator.name,
                    "score": evaluator.score,
                    "confidence": round(evaluator.confidence, 3),
                    "detail": evaluator.detail,
                    "category": evaluator.category,
                    "signals": evaluator.signals,
                }
                for evaluator in self.fit_evaluators
            ],
            "requirements": [
                {
                    "name": req.name,
                    "p_met": round(req.p_met, 3),
                    "weight": req.weight,
                    "detail": req.detail,
                    "hard": req.hard,
                }
                for req in self.requirements
            ],
            "comp_record": self.comp_record,
            "score_factors": self.factors,
            "rescored": True,
        }
