"""Export jobs from SQLite to static JSON for GitHub Pages terminal."""
from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent
DB = ROOT / "data" / "jobengine.sqlite"
DOCS = ROOT / "docs"


def main():
    DOCS.mkdir(exist_ok=True)
    conn = sqlite3.connect(str(DB))
    conn.row_factory = sqlite3.Row

    # Latest decision per job fingerprint
    rows = conn.execute(
        """
        SELECT
            j.job_id,
            j.fingerprint,
            j.source,
            j.company,
            j.title,
            j.location_text,
            j.remote_type,
            j.compensation_min,
            j.compensation_max,
            j.compensation_text,
            j.url,
            j.apply_url,
            SUBSTR(j.description_text, 1, 500) AS description_short,
            j.created_at_utc,
            d.queue,
            d.decision_reason,
            d.confidence,
            d.evidence_json,
            d.decided_at_utc
        FROM decisions d
        JOIN jobs_canonical j ON d.job_id = j.job_id
        WHERE d.decision_id IN (
            SELECT d2.decision_id
            FROM decisions d2
            JOIN jobs_canonical j2 ON d2.job_id = j2.job_id
            WHERE j2.fingerprint = j.fingerprint
            ORDER BY d2.decided_at_utc DESC
            LIMIT 1
        )
        GROUP BY j.fingerprint
        ORDER BY d.queue ASC, d.confidence DESC
        """
    ).fetchall()

    jobs = []
    queue_counts = {}
    city_counts = {}

    for row in rows:
        evidence = {}
        try:
            evidence = json.loads(row["evidence_json"] or "{}")
        except Exception:
            pass

        city_lane = evidence.get("city_lane", "Unknown")
        classification = evidence.get("classification", row["decision_reason"] or "")
        recommendation = evidence.get("recommendation", "")

        # Use recommendation from evidence if available, else map queue number
        if recommendation in {"apply", "maybe", "skip"}:
            queue_label = recommendation
        else:
            queue_label = {1: "apply", 2: "maybe", 3: "skip", 5: "skip"}.get(row["queue"], "skip")
        queue_counts[queue_label] = queue_counts.get(queue_label, 0) + 1
        city_counts[city_lane] = city_counts.get(city_lane, 0) + 1

        job = {
            "job_id": row["job_id"],
            "fingerprint": row["fingerprint"],
            "source": row["source"],
            "company": row["company"] or "",
            "title": row["title"] or "",
            "location_text": row["location_text"] or "",
            "remote_type": row["remote_type"] or "",
            "compensation_min": row["compensation_min"],
            "compensation_max": row["compensation_max"],
            "compensation_text": row["compensation_text"] or "",
            "url": row["url"] or row["apply_url"] or "",
            "apply_url": row["apply_url"] or row["url"] or "",
            "description_short": row["description_short"] or "",
            "created_at_utc": row["created_at_utc"] or "",
            "decided_at_utc": row["decided_at_utc"] or "",
            "queue": row["queue"],
            "queue_label": queue_label,
            "decision_reason": row["decision_reason"] or "",
            "confidence": row["confidence"],
            "classification": classification,
            "recommendation": recommendation,
            "city_lane": city_lane,
            "world_tier": evidence.get("world_tier", ""),
            "function_family": evidence.get("function_family", ""),
            "work_type_label": evidence.get("work_type_label", ""),
            "one_line_recommendation": evidence.get("one_line_recommendation", ""),
            "why_surfaced": evidence.get("why_surfaced", ""),
            "why_fit": evidence.get("why_fit", ""),
            "why_fail": evidence.get("why_fail", ""),
            "path_logic": evidence.get("path_logic", ""),
            "main_risk": evidence.get("main_risk", ""),
            "biggest_resume_gap": evidence.get("biggest_resume_gap", ""),
            "slop_verdict": evidence.get("slop_verdict", ""),
            "french_risk_label": evidence.get("french_risk_label", ""),
            "bridge_score": evidence.get("bridge_score", 0),
            "overall_score": evidence.get("overall_score", 0),
            "signal_scores": evidence.get("signal_scores", {}),
            "risk_flags": evidence.get("risk_flags", []),
            "opportunity_lanes": evidence.get("opportunity_lanes", []),
        }
        jobs.append(job)

    (DOCS / "jobs.json").write_text(
        json.dumps(jobs, ensure_ascii=False, indent=1), encoding="utf-8"
    )

    meta = {
        "export_date": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "total_jobs": len(jobs),
        "jobs_by_queue": queue_counts,
        "jobs_by_city": city_counts,
    }
    (DOCS / "meta.json").write_text(
        json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8"
    )

    conn.close()

    print(f"Exported {len(jobs)} jobs to docs/jobs.json")
    print(f"Queue breakdown: {queue_counts}")
    print(f"City breakdown: {city_counts}")
    print(f"Meta written to docs/meta.json")


if __name__ == "__main__":
    main()
