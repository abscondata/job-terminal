"""Queue router for lane-based discovery and manual triage."""
from __future__ import annotations

from .models import GateStatus, ScoreResult
from .profile import UserProfile


def _has_hard_reject(result: ScoreResult) -> bool:
    if result.reject_kind == "hard":
        return True
    return any(gate.status == GateStatus.FAIL for gate in result.gates)


def route(result: ScoreResult, queues: dict, profile: UserProfile | None = None) -> None:
    q = queues

    if _has_hard_reject(result):
        result.queue = q["REJECT"]
        result.queue_reason = result.reject_kind or "hard_reject"
        result.confidence = 0.99
        result.why_not_auto = "manual_only"
        return

    if result.recommendation == "apply":
        result.queue = q["REVIEW"]
        result.queue_reason = result.classification
        result.confidence = 0.86
        result.why_not_auto = "manual_only"
        return

    if result.recommendation == "maybe":
        result.queue = q["MAYBE"]
        result.queue_reason = result.classification
        result.confidence = 0.70
        result.why_not_auto = "manual_only"
        return

    result.queue = q["REJECT"]
    result.queue_reason = result.classification
    result.confidence = 0.84
    result.why_not_auto = "manual_only"


def batch_route(
    results: list[ScoreResult],
    queues: dict,
    auto_rate: float = 0.0,
    min_auto: int = 0,
    max_auto: int = 0,
) -> dict:
    q = queues
    stats = {
        "total": len(results),
        "gate_pass": 0,
        "gate_fail": 0,
        "gate_unclear": 0,
        "review": 0,
        "maybe": 0,
        "reject": 0,
        "classification_counts": {},
    }

    for result in results:
        if result.gate_status == "pass":
            stats["gate_pass"] += 1
        elif result.gate_status == "fail":
            stats["gate_fail"] += 1
        else:
            stats["gate_unclear"] += 1

        stats["classification_counts"][result.classification] = (
            stats["classification_counts"].get(result.classification, 0) + 1
        )

        if result.queue == q["REJECT"]:
            stats["reject"] += 1
        elif result.queue == q["REVIEW"]:
            stats["review"] += 1
        elif result.queue == q["MAYBE"]:
            stats["maybe"] += 1

    return stats
