"""Re-score every job in jobs_canonical through the current ScoringBrain."""
from __future__ import annotations

import json
import sqlite3
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))

from engine.config import load_config
from engine.scoring import ScoringBrain


def utcnow():
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def main():
    cfg = load_config(str(ROOT / "config.json"))
    brain = ScoringBrain(cfg)

    conn = sqlite3.connect(cfg.db_path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA busy_timeout=5000;")

    jobs = conn.execute(
        """SELECT job_id, fingerprint, source, company, title, location_text,
                  remote_type, language_requirements, compensation_min,
                  compensation_max, compensation_text, url, apply_url,
                  description_text, created_at_utc
           FROM jobs_canonical"""
    ).fetchall()

    total = len(jobs)
    print(f"Loaded {total} jobs from jobs_canonical")

    # Find the latest run_id to associate new decisions with
    run_row = conn.execute(
        "SELECT run_id FROM runs ORDER BY started_at_utc DESC LIMIT 1"
    ).fetchone()
    run_id = run_row["run_id"] if run_row else str(uuid.uuid4())

    # Delete ALL existing decisions so we start clean
    conn.execute("DELETE FROM decisions")
    conn.commit()
    print("Cleared all existing decisions")

    lane_counts = {}
    queue_counts = {}
    scored_jobs = []
    now = utcnow()

    for i, row in enumerate(jobs):
        job_dict = {
            "title": row["title"] or "",
            "company": row["company"] or "",
            "location_text": row["location_text"] or "",
            "description_text": row["description_text"] or "",
            "remote_type": row["remote_type"] or "",
            "language_requirements": row["language_requirements"] or "",
            "compensation_text": row["compensation_text"] or "",
            "compensation_min": row["compensation_min"],
            "compensation_max": row["compensation_max"],
            "url": row["url"] or "",
            "apply_url": row["apply_url"] or "",
            "source": row["source"] or "",
        }

        result = brain.score(job_dict)
        evidence = result.to_evidence()

        decision_id = str(uuid.uuid4())
        conn.execute(
            """INSERT INTO decisions(decision_id, run_id, job_id, queue,
                   decision_reason, confidence, evidence_json, decided_at_utc)
               VALUES(?,?,?,?,?,?,?,?)""",
            (
                decision_id,
                run_id,
                row["job_id"],
                result.queue,
                result.classification,
                round(result.confidence, 4),
                json.dumps(evidence, ensure_ascii=False),
                now,
            ),
        )

        lane_counts[result.classification] = lane_counts.get(result.classification, 0) + 1
        ql = result.recommendation
        queue_counts[ql] = queue_counts.get(ql, 0) + 1

        scored_jobs.append({
            "title": row["title"],
            "company": row["company"],
            "classification": result.classification,
            "recommendation": result.recommendation,
            "overall_score": result.overall_score,
            "dimension_scores": result.dimension_scores,
            "signal_scores": result.signal_scores,
        })

        if (i + 1) % 100 == 0:
            conn.commit()
            print(f"  {i+1}/{total} scored...")

    conn.commit()
    conn.close()

    print(f"\nDone. Re-scored {total} jobs.\n")

    print("=== CLASSIFICATION LANE COUNTS ===")
    for lane, count in sorted(lane_counts.items(), key=lambda x: -x[1]):
        print(f"  {lane}: {count}")

    print(f"\n=== QUEUE COUNTS ===")
    for q, count in sorted(queue_counts.items(), key=lambda x: -x[1]):
        print(f"  {q}: {count}")

    print(f"\n=== TOP 10 BY OVERALL_SCORE ===")
    scored_jobs.sort(key=lambda j: -(j["overall_score"] or 0))
    for i, j in enumerate(scored_jobs[:10]):
        ds = j["dimension_scores"]
        ss = j["signal_scores"]
        print(
            f"  #{i+1} score={j['overall_score']} | {j['classification']} | "
            f"{j['company']} - {j['title']}"
        )
        print(f"       dim={ds}  sig={ss}")


if __name__ == "__main__":
    main()
