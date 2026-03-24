"""
Nuke all stale discovery state and rebuild from scratch using v2 pipeline.

Architecture:
- Suppression lives in applied_jobs_cleaned.csv (single source of truth)
- Python dedup at scrape time enforces suppression BEFORE jobs enter the DB
- The DB only contains clean, unsuppressed jobs
- Rich evidence_json stores score components, bucket, penalties for export_static
"""
from __future__ import annotations

import json
import shutil
import sqlite3
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from engine.db import connect, init_schema

DB_PATH = ROOT / "data" / "jobengine.sqlite"
SCHEMA_PATH = ROOT / "engine" / "schema.sql"


def _norm_company(text: str) -> str:
    import re
    t = re.sub(r"[^a-z0-9 ]", " ", (text or "").lower())
    for stop in ("inc", "llc", "ltd", "corp", "co", "company", "group", "holdings",
                 "international", "partners", "search", "recruiting", "staffing",
                 "solutions", "consulting", "services", "the", "n a", "lp", "l p"):
        t = re.sub(rf"\b{stop}\b", "", t)
    return re.sub(r"\s+", " ", t).strip()


def _norm_title(text: str) -> str:
    import re
    t = re.sub(r"[^a-z0-9 ]", " ", (text or "").lower())
    for pfx in ("senior ", "junior ", "sr ", "jr ", "lead ", "staff "):
        if t.startswith(pfx):
            t = t[len(pfx):]
    t = re.sub(r"\(.*?\)", "", t)
    return re.sub(r"\s+", " ", t).strip()


def utcnow() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def nuke_all(conn: sqlite3.Connection) -> None:
    """Delete every row from every table that feeds jobs into the terminal."""
    print("[1/3] NUKING all discovery state...")
    for table in ("decisions", "jobs_canonical", "jobs_raw", "runs"):
        conn.execute(f"DELETE FROM {table}")
    try:
        conn.execute("DELETE FROM suppressed_jobs")
    except Exception:
        pass
    conn.commit()
    conn.execute("VACUUM")

    runs_dir = ROOT / "data" / "runs"
    if runs_dir.exists():
        for child in runs_dir.iterdir():
            if child.is_dir():
                shutil.rmtree(child, ignore_errors=True)

    for f in ("last_meta.json", "last_run.log", "source_health.json"):
        p = ROOT / "data" / f
        if p.exists():
            p.unlink()

    for rpt in (ROOT / "docs" / "report.html",):
        if rpt.exists():
            rpt.unlink()

    for ext in ("-wal", "-shm"):
        p = Path(str(DB_PATH) + ext)
        if p.exists():
            try:
                p.unlink()
            except PermissionError:
                pass

    for table in ("runs", "jobs_raw", "jobs_canonical", "decisions"):
        count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        assert count == 0, f"{table} still has {count} rows!"
    print("  All tables empty. State is clean.")


def run_fresh_discovery() -> dict:
    """Run the v2 compliance discovery pipeline."""
    print("[2/3] Running v2 compliance discovery...")
    from scripts.run_compliance_discovery import run_pipeline
    return run_pipeline()


def write_results(conn: sqlite3.Connection, results: dict) -> None:
    """Write results into DB with rich evidence for export_static."""
    print("[3/3] Writing to DB...")

    now = utcnow()
    run_id = str(uuid.uuid4())

    conn.execute(
        "INSERT INTO runs(run_id,started_at_utc,ended_at_utc,mode,config_snapshot) VALUES(?,?,?,?,?)",
        (run_id, now, now, "compliance_discovery_v2", "{}"),
    )

    all_jobs = results["all"]
    for job in all_jobs:
        job_id = str(uuid.uuid4())
        fp = f"{_norm_company(job['company'])}|{_norm_title(job['title'])}|{job.get('job_key', '')}"

        conn.execute(
            """INSERT INTO jobs_canonical(
                job_id,run_id,source,source_job_id,company,title,location_text,
                remote_type,language_requirements,compensation_min,compensation_max,
                compensation_text,url,apply_url,description_text,
                meetings_band,async_hint,relocation_hint,created_at_utc,fingerprint
            ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (job_id, run_id, "nyc_compliance", job.get("job_key", ""),
             job["company"], job["title"], job["location"], "", "",
             None, None, job.get("salary", ""),
             job["url"], job["url"], job.get("snippet", ""),
             "", 0, 0, now, fp),
        )

        # Rich evidence for export_static to use
        evidence = {
            "score": job["score"],
            "bucket": job["bucket"],
            "reason": job["reason"],
            "risk": job.get("risk", ""),
            "role_family": job["role_family"],
            "firm_tier": job["firm_tier"],
            "firm_label": job.get("firm_label", ""),
            "components": job.get("components", {}),
            "penalties": job.get("penalties", []),
            "boosts": job.get("boosts", []),
            # Fields expected by export_static
            "classification": job["bucket"],
            "recommendation": "apply" if job["score"] >= 60 else ("maybe" if job["score"] >= 38 else "skip"),
            "function_family": "Compliance / Risk",
            "city_lane": "NYC",
            "comp_record": {"comp_text_raw": job.get("salary", "")},
        }

        queue = 1 if job["score"] >= 72 else 2 if job["score"] >= 50 else 3
        conn.execute(
            "INSERT INTO decisions(decision_id,run_id,job_id,queue,decision_reason,confidence,evidence_json,decided_at_utc) VALUES(?,?,?,?,?,?,?,?)",
            (str(uuid.uuid4()), run_id, job_id, queue, job["bucket"],
             job["score"] / 100.0, json.dumps(evidence, ensure_ascii=False), now),
        )

    conn.commit()
    print(f"  DB: {len(all_jobs)} jobs under run {run_id[:8]}")


def main():
    print("=" * 60)
    print("NUKE AND REBUILD (v2)")
    print("=" * 60)

    conn = connect(str(DB_PATH))
    init_schema(conn, str(SCHEMA_PATH))

    nuke_all(conn)
    results = run_fresh_discovery()
    write_results(conn, results)
    conn.close()

    a = results.get("audit", {})
    buckets = a.get("buckets", {})
    print(f"\n{'=' * 60}")
    print("CLEAN STATE REBUILT")
    print(f"  Tier 1 (Apply):  {buckets.get('Tier 1 — Apply Immediately', 0)}")
    print(f"  Tier 2 (Review): {buckets.get('Tier 2 — Review & Apply', 0)}")
    print(f"  Tier 3 (Low):    {buckets.get('Tier 3 — Low Priority', 0)}")
    print(f"  Below Threshold: {buckets.get('Below Threshold', 0)}")
    print(f"  Suppressed: {a.get('suppress_count', 0)}")
    print(f"  Rejected: {sum(a.get('reject_reasons', {}).values())}")
    print(f"{'=' * 60}")


if __name__ == "__main__":
    main()
