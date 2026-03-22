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
_INTERN_RE = re.compile(r"\b(?:stage|stagiaire|intern|internship)\b", re.I)
_ALTERNANCE_RE = re.compile(r"\b(?:alternance|apprenti)", re.I)
_CONTRACT_RE = re.compile(r"\b(?:CDD|temp|temporary|contract|seasonal)\b", re.I)
COMPANY_SUFFIX = re.compile(
    r",?\s*\b(Inc\.?|LLC|Ltd\.?|Corp\.?|Co\.?|S\.?A\.?|S\.?A\.?S\.?|"
    r"SE|GmbH|PLC|N\.?V\.?|AG|SAS|SARL|SpA|Pty|Limited)\s*$",
    re.I,
)
INTERNAL_RE = re.compile(
    r"\b(bridge upside|borderline bridge|real bridge|prettier slop|mixed case)\b", re.I
)
GENERIC_ONE_LINER = re.compile(r"^(Apply|Maybe|Skip):\s", re.I)

TOP_PICK_LANES = {"Paris Direction", "NYC Direction", "Money / Platform Leap"}
BRIDGE_LANES = {
    "Strategic Internship / Traineeship", "Interesting Stretch",
    "Miami Option", "Top-Brand Wrong-Function Risk",
}
TOP_WORLDS = {"Top Luxury / Culture World"}
ADJACENT_WORLDS = {"Real Adjacent World", "Premium But Generic World"}


def detect_type(title: str) -> str:
    if _ALTERNANCE_RE.search(title or ""):
        return "Alternance"
    if _INTERN_RE.search(title or ""):
        return "Intern"
    if _CONTRACT_RE.search(title or ""):
        return "Contract"
    return "Full-Time"


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


def tier_label(classification: str) -> str:
    if classification in TOP_PICK_LANES:
        return "Top Pick"
    if classification in BRIDGE_LANES:
        return "Bridge"
    return "Pass"


def world_simple(world_tier: str) -> str:
    if world_tier in TOP_WORLDS:
        return "Top World"
    if world_tier in ADJACENT_WORLDS:
        return "Adjacent"
    return "Other"


def power_score(ev: dict) -> float:
    ss = ev.get("signal_scores", {})
    ds = ev.get("dimension_scores", {})
    raw = (
        ss.get("direction", 0) * 0.40
        + ss.get("bridge", 0) * 0.25
        + ds.get("function", 0) * 0.15
        + ds.get("escape", 0) * 0.10
        + ds.get("practicality", 0) * 0.10
        - ss.get("risk", 0) * 0.20
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
            j.url, j.apply_url, j.created_at_utc,
            d.queue, d.decision_reason, d.confidence,
            d.evidence_json, d.decided_at_utc
        FROM decisions d
        JOIN jobs_canonical j ON d.job_id = j.job_id
        """
    ).fetchall()
    conn.close()

    print(f"Raw rows from DB: {len(rows)}")

    all_jobs = []
    for row in rows:
        ev = {}
        try:
            ev = json.loads(row["evidence_json"] or "{}")
        except Exception:
            pass

        classification = ev.get("classification", row["decision_reason"] or "")
        tier = tier_label(classification)
        if tier == "Pass":
            continue  # skip garbage

        world_raw = ev.get("world_tier", "")
        ws = world_simple(world_raw)
        why_surfaced = ev.get("why_surfaced", "")
        path_logic_raw = ev.get("path_logic", "")
        one_liner_raw = ev.get("one_line_recommendation", "")
        ps = power_score(ev)

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
            "tier": tier,
            "lane": classification,
            "city": city_label(ev.get("city_lane", "")),
            "city_lane": ev.get("city_lane", "Unknown"),
            "world_simple": ws,
            "world_tier": world_raw,
            "function_family": ev.get("function_family", ""),
            "type": detect_type(row["title"] or ""),
            "score": round(ps),
            "power_score": ps,
            "one_liner": clean_one_liner(one_liner_raw, why_surfaced, path_logic_raw),
            "path_logic": path_logic_raw,
            "main_risk": ev.get("main_risk", ""),
            "compensation": ev.get("comp_record", {}).get("comp_text_raw", "") or row["compensation_text"] or "",
            "dimension_scores": ev.get("dimension_scores", {}),
            "signal_scores": ev.get("signal_scores", {}),
            "_created": row["created_at_utc"] or "",
        })

    print(f"After tier filter (Top Pick + Bridge only): {len(all_jobs)}")

    # --- DEDUPE ---
    groups: dict[tuple[str, str], list[dict]] = {}
    for job in all_jobs:
        key = (job["company"].lower().strip(), job["title"].lower().strip())
        groups.setdefault(key, []).append(job)

    deduped = []
    for group in groups.values():
        group.sort(key=lambda j: j["_created"] or "", reverse=True)
        deduped.append(group[0])

    print(f"After dedupe: {len(deduped)} (removed {len(all_jobs) - len(deduped)} dupes)")

    # --- DATE FILTER ---
    cutoff = (datetime.now(timezone.utc) - timedelta(days=60)).isoformat(timespec="seconds")
    before = len(deduped)
    filtered = []
    for job in deduped:
        if STALE_YEARS.search(job["title"]):
            if job["_created"] >= cutoff:
                filtered.append(job)
        else:
            filtered.append(job)

    print(f"After date filter: {len(filtered)} (removed {before - len(filtered)} expired)")

    # --- SORT & RANK ---
    filtered.sort(key=lambda j: -j["power_score"])
    for i, job in enumerate(filtered):
        job["rank"] = i + 1
        del job["_created"]

    # --- COUNTS ---
    tier_counts: dict[str, int] = {}
    city_counts: dict[str, int] = {}
    world_counts: dict[str, int] = {}
    type_counts: dict[str, int] = {}
    for job in filtered:
        tier_counts[job["tier"]] = tier_counts.get(job["tier"], 0) + 1
        city_counts[job["city"]] = city_counts.get(job["city"], 0) + 1
        world_counts[job["world_simple"]] = world_counts.get(job["world_simple"], 0) + 1
        type_counts[job["type"]] = type_counts.get(job["type"], 0) + 1

    # --- WRITE ---
    (DOCS / "jobs.json").write_text(
        json.dumps(filtered, ensure_ascii=False, indent=1), encoding="utf-8"
    )
    meta = {
        "export_date": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "total_jobs": len(filtered),
        "jobs_by_tier": tier_counts,
        "jobs_by_city": city_counts,
        "jobs_by_world": world_counts,
        "jobs_by_type": type_counts,
    }
    (DOCS / "meta.json").write_text(
        json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8"
    )

    print(f"\nExported {len(filtered)} jobs\n")
    print("=== TIER ===")
    for k, v in sorted(tier_counts.items()):
        print(f"  {k}: {v}")
    print("\n=== WORLD ===")
    for k, v in sorted(world_counts.items(), key=lambda x: -x[1]):
        print(f"  {k}: {v}")
    print("\n=== CITY ===")
    for k, v in sorted(city_counts.items(), key=lambda x: -x[1]):
        print(f"  {k}: {v}")
    print("\n=== TYPE ===")
    for k, v in sorted(type_counts.items(), key=lambda x: -x[1]):
        print(f"  {k}: {v}")
    print("\n=== TOP 20 ===")
    for job in filtered[:20]:
        print(
            f"  #{job['rank']} s={job['score']:3d} | {job['tier']:<9s} | "
            f"{job['world_simple']:<10s} | {job['company_clean'][:25]:<25s} | {job['title'][:55]}"
        )


if __name__ == "__main__":
    main()
