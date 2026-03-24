"""
Nuke stale discovery state and rebuild from scratch.

1. Load applied CSV -> populate suppressed_jobs table
2. DELETE all rows from runs, jobs_raw, jobs_canonical, decisions
3. Run fresh discovery via run_compliance_discovery
4. Post-filter: remove any job matching suppression from the report
5. Write clean report.html + update DB for Job Terminal
"""
from __future__ import annotations

import csv
import json
import re
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
APPLIED_CSV = Path.home() / "Desktop" / "job apps" / "applied_jobs_cleaned.csv"


def _norm_company(text: str) -> str:
    t = re.sub(r"[^a-z0-9 ]", " ", (text or "").lower())
    for stop in ("inc", "llc", "ltd", "corp", "co", "company", "group", "holdings",
                 "international", "partners", "search", "recruiting", "staffing",
                 "solutions", "consulting", "services", "the", "n a", "lp", "l p"):
        t = re.sub(rf"\b{stop}\b", "", t)
    return re.sub(r"\s+", " ", t).strip()


def _norm_title(text: str) -> str:
    t = re.sub(r"[^a-z0-9 ]", " ", (text or "").lower())
    for pfx in ("senior ", "junior ", "sr ", "jr ", "lead ", "staff "):
        if t.startswith(pfx):
            t = t[len(pfx):]
    t = re.sub(r"\(.*?\)", "", t)
    return re.sub(r"\s+", " ", t).strip()


def utcnow() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def step1_populate_suppression(conn: sqlite3.Connection) -> int:
    """Load applied CSV and populate suppressed_jobs table."""
    print("[NUKE 1/4] Populating suppression table from applied CSV...")

    # Ensure table exists
    conn.execute("""
        CREATE TABLE IF NOT EXISTS suppressed_jobs (
          suppression_id TEXT PRIMARY KEY,
          company_norm TEXT NOT NULL,
          title_norm TEXT NOT NULL,
          url TEXT,
          source_job_id TEXT,
          fingerprint TEXT,
          reason TEXT NOT NULL,
          suppressed_at_utc TEXT NOT NULL
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_suppressed_company_title ON suppressed_jobs(company_norm, title_norm)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_suppressed_fingerprint ON suppressed_jobs(fingerprint)")

    # Clear existing suppressions and rebuild
    conn.execute("DELETE FROM suppressed_jobs")

    if not APPLIED_CSV.exists():
        print("  WARNING: applied CSV not found")
        conn.commit()
        return 0

    count = 0
    now = utcnow()
    with open(APPLIED_CSV, "r", encoding="utf-8", errors="replace") as f:
        for row in csv.DictReader(f):
            company_raw = row.get("company", "") or row.get("company_raw", "")
            title_raw = row.get("title", "") or row.get("title_raw", "")
            cn = _norm_company(company_raw)
            tn = _norm_title(title_raw)
            if not cn or not tn:
                continue
            status = (row.get("status", "") or "").lower()
            reason = "applied" if status == "applied" else "reviewed"
            conn.execute(
                "INSERT OR IGNORE INTO suppressed_jobs(suppression_id,company_norm,title_norm,url,source_job_id,fingerprint,reason,suppressed_at_utc) VALUES(?,?,?,?,?,?,?,?)",
                (str(uuid.uuid4()), cn, tn, "", "", "", reason, now),
            )
            count += 1
    conn.commit()
    print(f"  Loaded {count} suppressed jobs")
    return count


def step2_nuke_discovery(conn: sqlite3.Connection) -> None:
    """Delete ALL discovery state."""
    print("[NUKE 2/4] Wiping discovery tables...")
    conn.execute("DELETE FROM decisions")
    conn.execute("DELETE FROM jobs_canonical")
    conn.execute("DELETE FROM jobs_raw")
    conn.execute("DELETE FROM runs")
    conn.commit()

    # Also clear run directories
    runs_dir = ROOT / "data" / "runs"
    if runs_dir.exists():
        import shutil
        for child in runs_dir.iterdir():
            if child.is_dir():
                shutil.rmtree(child, ignore_errors=True)

    # Clear stale state files
    for f in ["last_meta.json", "last_run.log", "source_health.json"]:
        p = ROOT / "data" / f
        if p.exists():
            p.unlink()

    print("  Discovery tables wiped clean")


def step3_run_fresh_discovery() -> dict:
    """Run the compliance discovery pipeline and return results."""
    print("[NUKE 3/4] Running fresh compliance discovery...")
    from scripts.run_compliance_discovery import scrape_indeed, hard_reject, score_job, load_applied_index, is_applied

    applied_index = load_applied_index()
    print(f"  Applied index: {len(applied_index)} pairs")

    raw_jobs = scrape_indeed()
    print(f"  Scraped: {raw_jobs and len(raw_jobs) or 0} raw jobs")

    passed = []
    reject_count = 0
    dedup_count = 0

    for job in raw_jobs:
        reason = hard_reject(job["title"], job["company"], job["location"], job["salary"], job["snippet"])
        if reason:
            reject_count += 1
            continue

        if is_applied(job["company"], job["title"], applied_index):
            dedup_count += 1
            print(f"  DEDUP: Removed [{job['title']}] at [{job['company']}] — already applied/reviewed")
            continue

        passed.append(job)

    # Score
    for job in passed:
        s = score_job(job["title"], job["company"], job["snippet"], job["salary"])
        job.update(s)

    passed.sort(key=lambda x: x["score"], reverse=True)

    apply_jobs = [j for j in passed if j["tier"] == "APPLY"]
    maybe_jobs = [j for j in passed if j["tier"] == "MAYBE"]
    skip_jobs = [j for j in passed if j["tier"] == "SKIP"]

    print(f"  APPLY: {len(apply_jobs)} | MAYBE: {len(maybe_jobs)} | SKIP: {len(skip_jobs)} | Rejected: {reject_count} | Deduped: {dedup_count}")

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


def step4_write_to_db_and_report(conn: sqlite3.Connection, results: dict) -> None:
    """Write fresh results into DB (so Job Terminal can serve them) and generate report.html."""
    print("[NUKE 4/4] Writing to DB + generating report...")

    now = utcnow()
    run_id = str(uuid.uuid4())

    conn.execute(
        "INSERT INTO runs(run_id,started_at_utc,ended_at_utc,mode,config_snapshot) VALUES(?,?,?,?,?)",
        (run_id, now, now, "compliance_discovery", "{}"),
    )

    for job in results["all"]:
        job_id = str(uuid.uuid4())
        fingerprint = f"{_norm_company(job['company'])}|{_norm_title(job['title'])}|{job.get('job_key','')}"

        conn.execute(
            """INSERT INTO jobs_canonical(job_id,run_id,source,source_job_id,company,title,location_text,
               remote_type,language_requirements,compensation_min,compensation_max,compensation_text,
               url,apply_url,description_text,meetings_band,async_hint,relocation_hint,created_at_utc,fingerprint)
               VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (job_id, run_id, "nyc_compliance", job.get("job_key", ""), job["company"], job["title"],
             job["location"], "", "", None, None, job.get("salary", ""),
             job["url"], job["url"], job.get("snippet", ""), "", 0, 0, now, fingerprint),
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
            "role_score": job.get("role_score", 0),
            "hire_score": job.get("hire_score", 0),
            "prestige_score": job.get("prestige_score", 0),
            "salary_score": job.get("salary_score", 0),
        }

        queue = 1 if job["tier"] == "APPLY" else 2 if job["tier"] == "MAYBE" else 3
        conn.execute(
            "INSERT INTO decisions(decision_id,run_id,job_id,queue,decision_reason,confidence,evidence_json,decided_at_utc) VALUES(?,?,?,?,?,?,?,?)",
            (str(uuid.uuid4()), run_id, job_id, queue, job["tier"], job["score"] / 100.0,
             json.dumps(evidence, ensure_ascii=False), now),
        )

    conn.commit()
    print(f"  Wrote {len(results['all'])} jobs to DB under run {run_id[:8]}")

    # Generate report.html
    from scripts.run_compliance_discovery import generate_report
    report_jobs = results["apply"] + results["maybe"]
    generate_report(report_jobs, results["meta"])
    print(f"  Report: {ROOT / 'docs' / 'report.html'}")


def main():
    print("=" * 60)
    print("NUKE AND REBUILD — wiping all stale discovery state")
    print("=" * 60)

    conn = connect(str(DB_PATH))
    init_schema(conn, str(SCHEMA_PATH))

    step1_populate_suppression(conn)
    step2_nuke_discovery(conn)
    results = step3_run_fresh_discovery()
    step4_write_to_db_and_report(conn, results)

    conn.close()

    meta = results["meta"]
    print(f"\n{'=' * 60}")
    print(f"DONE — Clean state rebuilt")
    print(f"  APPLY: {meta['apply_count']}")
    print(f"  MAYBE: {meta['maybe_count']}")
    print(f"  SKIP:  {meta['skip_count']}")
    print(f"  Deduped: {meta['deduped']}")
    print(f"  Rejected: {meta['rejected']}")
    print(f"{'=' * 60}")


if __name__ == "__main__":
    main()
