"""Candidate profile loaded from config.json."""
from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class UserProfile:
    target_market: str = "paris"
    target_cities: list[str] = field(default_factory=lambda: ["paris", "new york city", "miami"])
    target_worlds: list[str] = field(
        default_factory=lambda: [
            "luxury",
            "fashion",
            "jewelry",
            "beauty",
            "art",
            "culture",
            "premium_brand",
        ]
    )
    target_brands: list[str] = field(
        default_factory=lambda: [
            "lvmh",
            "dior",
            "louis vuitton",
            "celine",
            "sephora",
            "richemont",
            "cartier",
            "van cleef & arpels",
            "chloé",
            "chloe",
            "kering",
            "saint laurent",
            "balenciaga",
            "boucheron",
            "hermès",
            "hermes",
            "chanel",
        ]
    )
    languages: list[str] = field(default_factory=lambda: ["english", "french"])
    english_level: str = "native"
    french_spoken_level: str = "conversational"
    french_written_level: str = "limited"
    work_auth_countries: list[str] = field(default_factory=lambda: ["fr", "us", "global"])
    years_experience: float = 2.0
    current_role: str = "Branch Examiner / Securities Coordination"
    current_employer: str = "GWN Securities"
    candidate_headline: str = "Project and operations professional pursuing a Paris-first direction leap"
    candidate_summary: str = (
        "Project and operations professional strong in documentation, tracking, follow-up, "
        "reporting, Excel, stakeholder communication, and cross-team coordination."
    )
    core_strengths: list[str] = field(
        default_factory=lambda: [
            "project coordination",
            "operations support",
            "documentation",
            "follow-up",
            "reporting",
            "excel",
            "stakeholder communication",
            "cross-team coordination",
            "task tracking",
            "operational execution",
        ]
    )
    transition_signals: list[str] = field(
        default_factory=lambda: [
            "art gallery business development support",
            "connecticut ballet outreach",
            "interest in art, design, and cultural organizations",
        ]
    )
    stronger_fit_functions: list[str] = field(
        default_factory=lambda: [
            "project support",
            "project coordination",
            "communication support",
            "event support",
            "content production coordination",
            "visual merchandising coordination",
            "reporting",
            "stakeholder execution",
        ]
    )
    weaker_fit_functions: list[str] = field(
        default_factory=lambda: [
            "deep direct luxury experience",
            "strong written french",
            "art market specialist",
            "finance analyst",
            "aml",
            "trade surveillance",
            "operations analyst",
            "generic us corporate role",
        ]
    )
    score_weights: dict[str, float] = field(
        default_factory=lambda: {
            "paris_location": 0.18,
            "brand_world": 0.16,
            "bridge_realism": 0.14,
            "functional_fit": 0.14,
            "resume_pride": 0.12,
            "french_access": 0.10,
            "culture_creativity": 0.08,
            "escape_slop": 0.08,
            "practicality": 0.10,
        }
    )
    location_priority_scores: dict[str, int] = field(
        default_factory=lambda: {
            "paris_core": 100,
            "paris_region": 90,
            "nyc": 78,
            "miami": 56,
            "france_other": 42,
            "us_other": 34,
            "remote": 14,
            "other": 24,
            "unknown": 28,
        }
    )
    red_flag_phrases: list[str] = field(
        default_factory=lambda: [
            "time tracking",
            "always on camera",
            "webcam monitoring",
            "activity monitoring",
            "call center",
            "cold calling",
            "commission only",
            "commission-based",
        ]
    )
    min_overall_threshold: int = 45
    allow_remote_fallback: bool = False
    determinism_required: bool = True

    @classmethod
    def from_config(cls, cfg) -> UserProfile:
        profile_data = {}
        if hasattr(cfg, "policy") and isinstance(cfg.policy, dict):
            profile_data = cfg.policy.get("profile", {})

        kw: dict[str, object] = {}

        for field_name in [
            "target_market",
            "english_level",
            "french_spoken_level",
            "french_written_level",
            "current_role",
            "current_employer",
            "candidate_headline",
            "candidate_summary",
        ]:
            if field_name in profile_data:
                kw[field_name] = str(profile_data[field_name])

        for field_name in [
            "allow_remote_fallback",
            "determinism_required",
        ]:
            if field_name in profile_data:
                kw[field_name] = bool(profile_data[field_name])

        for field_name in [
            "years_experience",
        ]:
            if field_name in profile_data:
                kw[field_name] = float(profile_data[field_name])

        for field_name in [
            "min_overall_threshold",
        ]:
            if field_name in profile_data:
                kw[field_name] = int(profile_data[field_name])

        for field_name in [
            "target_cities",
            "target_worlds",
            "target_brands",
            "languages",
            "work_auth_countries",
            "core_strengths",
            "transition_signals",
            "stronger_fit_functions",
            "weaker_fit_functions",
            "red_flag_phrases",
        ]:
            if field_name in profile_data:
                kw[field_name] = [str(x).lower() for x in profile_data[field_name]]

        for field_name in ["score_weights", "location_priority_scores"]:
            if field_name in profile_data and isinstance(profile_data[field_name], dict):
                kw[field_name] = {
                    str(k).lower(): float(v) if field_name == "score_weights" else int(v)
                    for k, v in profile_data[field_name].items()
                }

        return cls(**kw)
