"""Export jobs from SQLite to static JSON with power ranking for GitHub Pages."""
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
INTERNAL_RE = re.compile(
    r"\b(bridge upside|borderline bridge|real bridge|prettier slop|mixed case)\b", re.I
)
GENERIC_ONE_LINER = re.compile(
    r"^(Apply|Maybe|Skip):\s", re.I
)


def clean_company(name: str) -> str:
    out = (name or "").strip()
    out = COMPANY_SUFFIX.sub("", out).strip().rstrip(",").strip()
    return out or name or ""


def clean_one_liner(one_liner: str, why_surfaced: str, path_logic: str) -> str:
    if not one_liner:
        return path_logic or why_surfaced or ""
    if INTERNAL_RE.search(one_liner):
        return path_logic or why_surfaced or ""
    if GENERIC_ONE_LINER.match(one_liner) and len(one_liner) < 80:
        return path_logic or why_surfaced or ""
    return one_liner


def city_label(city_lane: str) -> str:
    if not city_lane or city_lane == "Unknown":
        return "Other"
    if city_lane.startswith("Paris"):
        return "Paris"
    if city_lane == "NYC":
        return "NYC"
    if city_lane == "Miami":
        return "Miami"
    return "Other"


def power_score(ev: dict, queue_label: str) -> float:
    ss = ev.get("signal_scores", {})
    ds = ev.get("dimension_scores", {})
    direction = ss.get("direction", 0)
    bridge = ss.get("bridge", 0)
    risk = ss.get("risk", 0)
    func = ds.get("function", 0)
    escape = ds.get("escape", 0)
    prac = ds.get("practicality", 0)
    raw = (
        direction * 0.40
        + bridge * 0.25
        + func * 0.15
        + escape * 0.10
        + prac * 0.10
        - risk * 0.20
    )
    return round(raw, 1)


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
            j.created_at_utc,
            d.queue, d.decision_reason, d.confidence,
            d.evidence_json, d.decided_at_utc
        FROM decisions d
        JOIN jobs_canonical j ON d.job_id = j.job_id
        """
    ).fetchall()
    conn.close()

    total_raw = len(rows)
    print(f"Raw rows from DB: {total_raw}")

    # Build job dicts
    all_jobs = []
    for row in rows:
        ev = {}
        try:
            ev = json.loads(row["evidence_json"] or "{}")
        except Exception:
            pass

        recommendation = ev.get("recommendation", "")
        if recommendation in {"apply", "maybe", "skip"}:
            queue_label = recommendation
        else:
            queue_label = {1: "apply", 2: "maybe", 3: "skip"}.get(row["queue"], "skip")

        classification = ev.get("classification", row["decision_reason"] or "")
        world_raw = ev.get("world_tier", "")
        world_clean = world_raw.replace(" World", "") if world_raw else ""
        if world_clean == "Unknown":
            world_clean = ""

        why_surfaced = ev.get("why_surfaced", "")
        path_logic_raw = ev.get("path_logic", "")
        one_liner_raw = ev.get("one_line_recommendation", "")

        all_jobs.append({
            "job_id": row["job_id"],
            "fingerprint": row["fingerprint"],
            "source": row["source"],
            "company": row["company"] or "",
            "company_clean": clean_company(row["company"] or ""),
            "title": row["title"] or "",
            "location": row["location_text"] or "",
            "url": row["url"] or row["apply_url"] or "",
            "created_at": row["created_at_utc"] or "",
            "decided_at": row["decided_at_utc"] or "",
            "queue": queue_label,
            "confidence": row["confidence"],
            "lane": classification,
            "city": city_label(ev.get("city_lane", "")),
            "city_lane": ev.get("city_lane", "Unknown"),
            "world": world_clean,
            "world_tier": world_raw,
            "function_family": ev.get("function_family", ""),
            "work_type_label": ev.get("work_type_label", ""),
            "dimension_scores": ev.get("dimension_scores", {}),
            "signal_scores": ev.get("signal_scores", {}),
            "signal_bands": ev.get("signal_bands", {}),
            "one_liner": clean_one_liner(one_liner_raw, why_surfaced, path_logic_raw),
            "path_logic": path_logic_raw,
            "main_risk": ev.get("main_risk", ""),
            "slop_verdict": ev.get("slop_verdict", ""),
            "french_risk": ev.get("french_risk_label", ""),
            "biggest_resume_gap": ev.get("biggest_resume_gap", ""),
            "compensation": ev.get("comp_record", {}).get("comp_text_raw", "") or row["compensation_text"] or "",
            "compensation_min": row["compensation_min"],
            "compensation_max": row["compensation_max"],
            "why_surfaced": why_surfaced,
            "overall_score": ev.get("overall_score", 0),
            "bridge_score": ev.get("bridge_score", 0),
            "opportunity_lanes": ev.get("opportunity_lanes", []),
            "role_bucket": ev.get("role_bucket", ""),
            "risk_flags": ev.get("risk_flags", []),
            "_ev": ev,  # temp for power_score calc
        })

    # --- DEDUPE ---
    groups: dict[tuple[str, str], list[dict]] = {}
    for job in all_jobs:
        key = (job["company"].lower().strip(), job["title"].lower().strip())
        groups.setdefault(key, []).append(job)

    deduped = []
    for key, group in groups.items():
        group.sort(key=lambda j: j["created_at"] or "", reverse=True)
        deduped.append(group[0])

    print(f"After dedupe: {len(deduped)} (removed {len(all_jobs) - len(deduped)} dupes)")

    # --- DATE FILTER ---
    cutoff = datetime.now(timezone.utc) - timedelta(days=60)
    cutoff_str = cutoff.isoformat(timespec="seconds")
    before = len(deduped)
    filtered = []
    for job in deduped:
        if STALE_YEARS.search(job["title"]):
            if job["created_at"] >= cutoff_str:
                filtered.append(job)
        else:
            filtered.append(job)

    print(f"After date filter: {len(filtered)} (removed {before - len(filtered)} expired)")

    # --- COMPUTE POWER SCORE ---
    for job in filtered:
        job["power_score"] = power_score(job["_ev"], job["queue"])

    # --- SORT & RANK ---
    apply_jobs = [j for j in filtered if j["queue"] == "apply"]
    maybe_jobs = [j for j in filtered if j["queue"] == "maybe"]
    skip_jobs = [j for j in filtered if j["queue"] == "skip"]

    apply_jobs.sort(key=lambda j: -j["power_score"])
    maybe_jobs.sort(key=lambda j: -j["power_score"])
    skip_jobs.sort(key=lambda j: -(j["overall_score"] or 0))

    ranked = apply_jobs + maybe_jobs + skip_jobs
    for i, job in enumerate(ranked):
        job["rank"] = i + 1
        del job["_ev"]  # remove temp field

    # --- COUNTS ---
    queue_counts: dict[str, int] = {}
    city_counts: dict[str, int] = {}
    lane_counts: dict[str, int] = {}
    for job in ranked:
        queue_counts[job["queue"]] = queue_counts.get(job["queue"], 0) + 1
        city_counts[job["city"]] = city_counts.get(job["city"], 0) + 1
        lane_counts[job["lane"]] = lane_counts.get(job["lane"], 0) + 1

    # --- WRITE ---
    (DOCS / "jobs.json").write_text(
        json.dumps(ranked, ensure_ascii=False, indent=1), encoding="utf-8"
    )

    meta = {
        "export_date": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "total_jobs": len(ranked),
        "jobs_by_queue": queue_counts,
        "jobs_by_city": city_counts,
        "jobs_by_lane": lane_counts,
    }
    (DOCS / "meta.json").write_text(
        json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8"
    )

    print(f"\nFinal: {len(ranked)} jobs to docs/jobs.json\n")

    print("=== TOP 20 BY POWER RANK ===")
    for job in ranked[:20]:
        print(
            f"  #{job['rank']} pw={job['power_score']:5.1f} | {job['queue']:5s} | "
            f"{job['lane']:<40s} | {job['company_clean']} - {job['title'][:60]}"
        )

    print(f"\n=== QUEUE COUNTS ===")
    for q, c in sorted(queue_counts.items()):
        print(f"  {q}: {c}")

    print(f"\n=== LANE DISTRIBUTION ===")
    for lane, c in sorted(lane_counts.items(), key=lambda x: -x[1]):
        print(f"  {lane}: {c}")


if __name__ == "__main__":
    main()
