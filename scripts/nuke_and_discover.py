"""
Nuke all stale discovery state and rebuild from scratch.

Architecture:
- Suppression lives in applied_jobs_cleaned.csv (the single source of truth)
- Python dedup at scrape time enforces suppression BEFORE jobs enter the DB
- The DB only contains clean, unsuppressed jobs
- No SQL-level suppression — that approach had a normalization mismatch bug

Steps:
1. DELETE all rows from runs, jobs_raw, jobs_canonical, decisions
2. Delete all run directories and stale state files
3. Run fresh discovery (scrape -> hard reject -> dedup against CSV -> score)
4. Write clean results to DB + generate report.html
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
    # Also clear suppressed_jobs if it exists (we don't use it anymore)
    try:
        conn.execute("DELETE FROM suppressed_jobs")
    except Exception:
        pass
    conn.commit()
    conn.execute("VACUUM")

    # Delete run directories
    runs_dir = ROOT / "data" / "runs"
    if runs_dir.exists():
        for child in runs_dir.iterdir():
            if child.is_dir():
                shutil.rmtree(child, ignore_errors=True)

    # Delete stale files
    for f in ("last_meta.json", "last_run.log", "source_health.json", "audit_report.txt"):
        p = ROOT / "data" / f
        if p.exists():
            p.unlink()

    # Delete old report
    for rpt in (ROOT / "docs" / "report.html",):
        if rpt.exists():
            rpt.unlink()

    # Delete WAL/SHM (may fail if DB connection holds them)
    for ext in ("-wal", "-shm"):
        p = Path(str(DB_PATH) + ext)
        if p.exists():
            try:
                p.unlink()
            except PermissionError:
                pass  # held by our connection, will be cleaned on close

    # Verify
    for table in ("runs", "jobs_raw", "jobs_canonical", "decisions"):
        count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        assert count == 0, f"{table} still has {count} rows!"
    print("  All tables empty. State is clean.")


def run_fresh_discovery() -> dict:
    """Run the compliance discovery pipeline. Returns results dict."""
    print("[2/3] Running fresh compliance discovery...")
    from scripts.run_compliance_discovery import (
        scrape_indeed, hard_reject, score_job, load_applied_index, is_applied,
    )

    applied_index = load_applied_index()
    print(f"  Applied index: {len(applied_index)} pairs (suppression source)")

    raw_jobs = scrape_indeed()
    print(f"  Scraped: {len(raw_jobs)} raw jobs")

    passed = []
    reject_count = 0
    dedup_count = 0

    for job in raw_jobs:
        reason = hard_reject(
            job["title"], job["company"], job["location"],
            job["salary"], job["snippet"],
        )
        if reason:
            reject_count += 1
            continue

        if is_applied(job["company"], job["title"], applied_index):
            dedup_count += 1
            print(f"  SUPPRESSED: [{job['title']}] at [{job['company']}]")
            continue

        passed.append(job)

    for job in passed:
        s = score_job(job["title"], job["company"], job["snippet"], job["salary"])
        job.update(s)

    passed.sort(key=lambda x: x["score"], reverse=True)

    apply_jobs = [j for j in passed if j["tier"] == "APPLY"]
    maybe_jobs = [j for j in passed if j["tier"] == "MAYBE"]
    skip_jobs = [j for j in passed if j["tier"] == "SKIP"]

    print(f"  Results: APPLY={len(apply_jobs)} MAYBE={len(maybe_jobs)} SKIP={len(skip_jobs)}")
    print(f"  Rejected={reject_count} Suppressed={dedup_count}")

    return {
        "all": passed,
        "apply": apply_jobs,
        "maybe": maybe_jobs,
        "skip": skip_jobs,
        "meta": {
            "total_scraped": len(raw_jobs),
            "rejected": reject_count,
            "deduped": dedup_count,
            "apply_count": len(apply_jobs),
            "maybe_count": len(maybe_jobs),
            "skip_count": len(skip_jobs),
            "generated": datetime.now().strftime("%Y-%m-%d %H:%M"),
        },
    }


def write_results(conn: sqlite3.Connection, results: dict) -> None:
    """Write clean results into DB and generate report.html."""
    print("[3/3] Writing to DB + generating report...")

    now = utcnow()
    run_id = str(uuid.uuid4())

    conn.execute(
        "INSERT INTO runs(run_id,started_at_utc,ended_at_utc,mode,config_snapshot) VALUES(?,?,?,?,?)",
        (run_id, now, now, "compliance_discovery", "{}"),
    )

    for job in results["all"]:
        job_id = str(uuid.uuid4())
        fp = f"{_norm_company(job['company'])}|{_norm_title(job['title'])}|{job.get('job_key','')}"

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

        evidence = {
            "score": job["score"],
            "tier": job["tier"],
            "reason": job["reason"],
            "recommendation": job["tier"].lower() if job["tier"] != "SKIP" else "skip",
            "classification": job["tier"],
            "company": job["company"],
            "title": job["title"],
            "location_text": job["location"],
            "url": job["url"],
            "salary": job.get("salary", ""),
        }

        queue = 1 if job["tier"] == "APPLY" else 2 if job["tier"] == "MAYBE" else 3
        conn.execute(
            "INSERT INTO decisions(decision_id,run_id,job_id,queue,decision_reason,confidence,evidence_json,decided_at_utc) VALUES(?,?,?,?,?,?,?,?)",
            (str(uuid.uuid4()), run_id, job_id, queue, job["tier"],
             job["score"] / 100.0, json.dumps(evidence, ensure_ascii=False), now),
        )

    conn.commit()
    print(f"  DB: {len(results['all'])} jobs under run {run_id[:8]}")

    # Generate report.html
    from scripts.run_compliance_discovery import generate_report
    report_jobs = results["apply"] + results["maybe"]
    generate_report(report_jobs, results["meta"])
    print(f"  Report: {ROOT / 'docs' / 'report.html'}")


def main():
    print("=" * 60)
    print("NUKE AND REBUILD")
    print("=" * 60)

    conn = connect(str(DB_PATH))
    init_schema(conn, str(SCHEMA_PATH))

    nuke_all(conn)
    results = run_fresh_discovery()
    write_results(conn, results)
    conn.close()

    m = results["meta"]
    print(f"\n{'=' * 60}")
    print(f"CLEAN STATE REBUILT")
    print(f"  APPLY: {m['apply_count']}")
    print(f"  MAYBE: {m['maybe_count']}")
    print(f"  Suppressed: {m['deduped']}")
    print(f"  Rejected: {m['rejected']}")
    print(f"{'=' * 60}")


if __name__ == "__main__":
    main()
