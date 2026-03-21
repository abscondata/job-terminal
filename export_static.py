"""Export jobs from SQLite to static JSON for GitHub Pages terminal."""
from __future__ import annotations

import json
import re
import sqlite3
from datetime import datetime, timezone, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parent
DB = ROOT / "data" / "jobengine.sqlite"
DOCS = ROOT / "docs"

STALE_YEARS = re.compile(r"\b(2019|2020|2021|2022|2023|2024|2025)\b")
COMPANY_SUFFIX = re.compile(
    r",?\s*\b(Inc\.?|LLC|Ltd\.?|Corp\.?|Co\.?|S\.?A\.?|S\.?A\.?S\.?|"
    r"SE|GmbH|PLC|N\.?V\.?|AG|SAS|SARL|SpA|Pty|Limited)\s*$",
    re.I,
)

LANE_PRIORITY = {
    "Paris Direction": 0,
    "NYC Direction": 1,
    "Strategic Internship / Traineeship": 2,
    "Money / Platform Leap": 3,
    "Interesting Stretch": 4,
}
QUEUE_MAP = {1: "apply", 2: "maybe", 3: "skip", 4: "skip", 5: "skip"}


def clean_company(name: str) -> str:
    out = (name or "").strip()
    out = COMPANY_SUFFIX.sub("", out).strip().rstrip(",").strip()
    return out or name or ""


def main():
    DOCS.mkdir(exist_ok=True)
    conn = sqlite3.connect(str(DB))
    conn.row_factory = sqlite3.Row

    rows = conn.execute(
        """
        SELECT
            j.job_id, j.fingerprint, j.source, j.company, j.title,
            j.location_text, j.remote_type,
            j.compensation_min, j.compensation_max, j.compensation_text,
            j.url, j.apply_url,
            SUBSTR(j.description_text, 1, 500) AS description_short,
            j.created_at_utc,
            d.queue, d.decision_reason, d.confidence,
            d.evidence_json, d.decided_at_utc
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
        """
    ).fetchall()
    conn.close()

    total_raw = len(rows)
    print(f"Raw rows from DB: {total_raw}")

    # Build job dicts
    all_jobs = []
    for row in rows:
        evidence = {}
        try:
            evidence = json.loads(row["evidence_json"] or "{}")
        except Exception:
            pass

        classification = evidence.get("classification", row["decision_reason"] or "")
        recommendation = evidence.get("recommendation", "")
        if recommendation in {"apply", "maybe", "skip"}:
            queue_label = recommendation
        else:
            queue_label = QUEUE_MAP.get(row["queue"], "skip")

        all_jobs.append({
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
            "confidence": row["confidence"],
            # Enriched fields
            "lane": classification,
            "classification": classification,
            "recommendation": recommendation,
            "city_lane": evidence.get("city_lane", "Unknown"),
            "world": evidence.get("world_tier", ""),
            "world_tier": evidence.get("world_tier", ""),
            "function_family": evidence.get("function_family", ""),
            "work_type_label": evidence.get("work_type_label", ""),
            "one_liner": evidence.get("one_line_recommendation", ""),
            "path_logic": evidence.get("path_logic", ""),
            "main_risk": evidence.get("main_risk", ""),
            "why_surfaced": evidence.get("why_surfaced", ""),
            "why_fit": evidence.get("why_fit", ""),
            "why_fail": evidence.get("why_fail", ""),
            "biggest_resume_gap": evidence.get("biggest_resume_gap", ""),
            "slop_verdict": evidence.get("slop_verdict", ""),
            "french_risk_label": evidence.get("french_risk_label", ""),
            "bridge_score": evidence.get("bridge_score", 0),
            "overall_score": evidence.get("overall_score", 0),
            "signal_scores": evidence.get("signal_scores", {}),
            "risk_flags": evidence.get("risk_flags", []),
            "opportunity_lanes": evidence.get("opportunity_lanes", []),
            "company_clean": clean_company(row["company"] or ""),
        })

    # --- DEDUPE by (company_lower, title_lower), keep newest ---
    groups: dict[tuple[str, str], list[dict]] = {}
    for job in all_jobs:
        key = (job["company"].lower().strip(), job["title"].lower().strip())
        groups.setdefault(key, []).append(job)

    deduped = []
    for key, group in groups.items():
        group.sort(key=lambda j: j["created_at_utc"] or "", reverse=True)
        deduped.append(group[0])

    print(f"After dedupe: {len(deduped)} (removed {len(all_jobs) - len(deduped)} dupes)")
    before_date = len(deduped)

    # --- DATE FILTERING: remove expired postings ---
    cutoff = datetime.now(timezone.utc) - timedelta(days=60)
    cutoff_str = cutoff.isoformat(timespec="seconds")
    filtered = []
    for job in deduped:
        title = job["title"]
        if STALE_YEARS.search(title):
            created = job["created_at_utc"] or ""
            if created >= cutoff_str:
                filtered.append(job)
            # else: expired, drop
        else:
            filtered.append(job)

    print(f"After date filter: {len(filtered)} (removed {before_date - len(filtered)} expired)")

    # --- SORTING ---
    def sort_key(job):
        q = {"apply": 0, "maybe": 1, "skip": 2}.get(job["queue_label"], 2)
        lane = LANE_PRIORITY.get(job["lane"], 99)
        conf = -(job["confidence"] or 0)
        return (q, lane, conf)

    filtered.sort(key=sort_key)

    # --- COUNTS ---
    queue_counts: dict[str, int] = {}
    city_counts: dict[str, int] = {}
    lane_counts: dict[str, int] = {}
    for job in filtered:
        ql = job["queue_label"]
        queue_counts[ql] = queue_counts.get(ql, 0) + 1
        cl = job["city_lane"]
        city_counts[cl] = city_counts.get(cl, 0) + 1
        ll = job["lane"]
        lane_counts[ll] = lane_counts.get(ll, 0) + 1

    # --- WRITE ---
    (DOCS / "jobs.json").write_text(
        json.dumps(filtered, ensure_ascii=False, indent=1), encoding="utf-8"
    )

    meta = {
        "export_date": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "total_jobs": len(filtered),
        "jobs_by_queue": queue_counts,
        "jobs_by_city": city_counts,
        "jobs_by_lane": lane_counts,
    }
    (DOCS / "meta.json").write_text(
        json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8"
    )

    print(f"\nFinal: {len(filtered)} jobs written to docs/jobs.json")
    print(f"Queue: {queue_counts}")
    print(f"City:  {city_counts}")
    print(f"Lane:  {lane_counts}")


if __name__ == "__main__":
    main()
