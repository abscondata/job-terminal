from __future__ import annotations

import argparse
import json
import re
import sys
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from engine.compensation import extract_comp_from_description, parse_compensation
from engine.config import load_config
from engine.db import connect, init_schema
from engine.dedupe import fingerprint_exists, fuzzy_key
from engine.language import detect_language_gate
from engine.models import make_fingerprint
from engine.report import write_report
from engine.scoring import ScoringBrain
from engine.scoring.router import batch_route
from scripts.indeed import scrape as pull_indeed_source
from scripts.linkedin import scrape as pull_linkedin_source
from scripts.official_workday import scrape_chanel, scrape_christies
from scripts.paris_curated_pages import (
    scrape_centre_pompidou,
    scrape_kering,
    scrape_sothebys,
)
from scripts.nyc_compliance import scrape as pull_nyc_compliance_source
from scripts.profilculture import scrape as scrape_profilculture
from scripts.welcometothejungle import scrape as pull_wttj_source

TAG_RE = re.compile(r"<[^>]+>")
_ALTERNANCE_RE = re.compile(r"\b(?:alternance|apprenti)", re.I)
DESC_LIMIT = 14000
ACTIVE_PATH_NOTE = (
    "Active path: targeted discovery -> lane-based path review -> manual triage and saved review. "
    "Paris is the dream lane, NYC is the realism/platform comparator, Miami is tertiary. "
    "No auto-apply. No generic score authority."
)
SOURCE_HEALTH_PATH = ROOT / "data" / "source_health.json"


def utcnow() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def snippet(text: str, limit: int = 800) -> str:
    cleaned = TAG_RE.sub(" ", text or "")
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    return cleaned[:limit]


def _load_source_health() -> dict:
    if not SOURCE_HEALTH_PATH.exists():
        return {"sources": {}}
    try:
        return json.loads(SOURCE_HEALTH_PATH.read_text(encoding="utf-8"))
    except Exception:
        return {"sources": {}}


def _save_source_health(payload: dict) -> None:
    SOURCE_HEALTH_PATH.parent.mkdir(parents=True, exist_ok=True)
    SOURCE_HEALTH_PATH.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _should_skip_source(
    name: str,
    health: dict,
    *,
    min_runs: int,
    min_returned: int,
    min_dup_rate: float,
    cooldown_hours: int,
    now: datetime,
) -> tuple[bool, str]:
    entry = (health.get("sources") or {}).get(name) or {}
    skip_until = entry.get("skip_until")
    if skip_until:
        try:
            if datetime.fromisoformat(skip_until) > now:
                return True, "cooldown"
        except Exception:
            pass
    history = entry.get("history") or []
    if len(history) < min_runs:
        return False, ""
    recent = history[-min_runs:]
    returned = sum(int(item.get("returned", 0)) for item in recent)
    new = sum(int(item.get("new", 0)) for item in recent)
    dupes = sum(int(item.get("dupes", 0)) for item in recent)
    if returned < min_returned:
        return False, ""
    dup_rate = dupes / max(1, returned)
    if new == 0 and dup_rate >= min_dup_rate:
        until = now + timedelta(hours=cooldown_hours)
        entry["skip_until"] = until.isoformat(timespec="seconds")
        health.setdefault("sources", {})[name] = entry
        return True, "stale_duplicate_only"
    return False, ""


def _update_source_health(health: dict, run_id: str, source_stats: dict) -> None:
    now = utcnow()
    sources = health.setdefault("sources", {})
    for name, stats in source_stats.items():
        entry = sources.setdefault(name, {"history": []})
        entry["history"].append(
            {
                "run_id": run_id,
                "ts": now,
                "returned": int(stats.get("returned", 0)),
                "new": int(stats.get("new", 0)),
                "dupes": int(stats.get("dupes", 0)),
                "rejected": int(stats.get("rejected", 0)),
                "capped": int(stats.get("capped", 0)),
            }
        )
        entry["history"] = entry["history"][-8:]
    health["sources"] = sources


def _elapsed_label(started_at: float) -> str:
    total = max(0, int(time.perf_counter() - started_at))
    mins, secs = divmod(total, 60)
    return f"{mins:02d}:{secs:02d}"


def _emit_progress(source: str, status: str, started_at: float, **extra) -> None:
    payload = {
        "source": source,
        "status": status,
        "elapsed": _elapsed_label(started_at),
        **extra,
    }
    print(f"DISCOVERY_PROGRESS={json.dumps(payload, ensure_ascii=False)}", flush=True)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Lane-based discovery for Paris/NYC/Miami paths")
    parser.add_argument("--quick", action="store_true", help="smaller source slices")
    parser.add_argument(
        "--sources",
        default="",
        help="comma-separated source names to run instead of config defaults",
    )
    parser.add_argument(
        "--city",
        default="all",
        choices=["all", "paris", "nyc", "miami"],
        help="limit city scope for LinkedIn/Indeed discovery",
    )
    parser.add_argument(
        "--audit-rejects",
        action="store_true",
        help="write JSONL audit log for rejected jobs",
    )
    return parser.parse_args()


def build_source_registry():
    return [
        ("welcometothejungle", pull_wttj_source),
        ("kering", scrape_kering),
        ("chanel", scrape_chanel),
        ("sothebys", scrape_sothebys),
        ("christies", scrape_christies),
        ("centre_pompidou", scrape_centre_pompidou),
        ("profilculture", scrape_profilculture),
        ("linkedin", pull_linkedin_source),
        ("indeed", pull_indeed_source),
        ("nyc_compliance", pull_nyc_compliance_source),
    ]


def _run_source(name: str, fn, quick: bool, source_kwargs: dict[str, dict]) -> tuple[str, list[dict]]:
    kwargs = source_kwargs.get(name, {})
    return name, fn(quick=quick, **kwargs)


def _base_source_stats() -> dict:
    return {
        "returned": 0,
        "new": 0,
        "dupes": 0,
        "fuzzy_dupes": 0,
        "rejected": 0,
        "capped": 0,
        "core_fit": 0,
    }


def main() -> None:
    args = parse_args()
    started_at = time.perf_counter()
    cfg = load_config(str(ROOT / "config.json"))
    conn = connect(cfg.db_path)
    init_schema(conn, str(ROOT / "engine" / "schema.sql"))

    run_id = str(uuid.uuid4())
    conn.execute(
        "INSERT INTO runs(run_id,started_at_utc,mode,config_snapshot) VALUES(?,?,?,?)",
        (run_id, utcnow(), "discover", json.dumps(cfg.policy, ensure_ascii=False)),
    )
    conn.commit()

    enabled = set((cfg.discovery or {}).get("enabled_sources") or [])
    all_sources = build_source_registry()
    registry = {name: fn for name, fn in all_sources}
    requested = [name.strip() for name in args.sources.split(",") if name.strip()]

    if requested:
        sources = [(name, registry[name]) for name in requested if name in registry]
    elif enabled:
        sources = [(name, fn) for name, fn in all_sources if name in enabled]
    else:
        sources = all_sources

    if not sources:
        available = ", ".join(name for name, _ in all_sources)
        raise SystemExit(f"No valid sources selected. Available sources: {available}")

    disc = cfg.discovery or {}
    city_scope = (args.city or disc.get("city_scope", "all")).lower()
    workday_max_age_days = int(disc.get("workday_max_age_days", 45))
    greenhouse_max_age_days = int(disc.get("greenhouse_max_age_days", 60))
    wttj_max_age_days = int(disc.get("wttj_max_age_days", 21))
    audit_rejects_enabled = args.audit_rejects or bool(disc.get("audit_rejects", True))
    include_alternance = bool(disc.get("include_alternance", True))
    fuzzy_enabled = bool(disc.get("fuzzy_dedupe", True))
    fuzzy_days = int(disc.get("fuzzy_dedupe_days", 120))
    fuzzy_allow_sources = set(disc.get("fuzzy_allow_sources") or [])
    stale_policy = disc.get("stale_source_policy") or {}
    stale_enabled = bool(stale_policy.get("enabled", True))
    stale_min_runs = int(stale_policy.get("min_runs", 3))
    stale_min_returned = int(stale_policy.get("min_returned", 5))
    stale_min_dup_rate = float(stale_policy.get("min_duplicate_rate", 0.9))
    stale_cooldown_hours = int(stale_policy.get("cooldown_hours", 12))
    scan_multiplier = float(disc.get("scan_cap_multiplier", 3))
    source_kwargs = {
        "welcometothejungle": {"max_age_days": wttj_max_age_days},
        "chanel": {"max_age_days": workday_max_age_days},
        "christies": {"max_age_days": workday_max_age_days},
        "sothebys": {"max_age_days": greenhouse_max_age_days},
        "linkedin": {"city_scope": city_scope},
        "indeed": {"city_scope": city_scope},
    }
    source_health = _load_source_health() if stale_enabled else {"sources": {}}
    stale_skipped: list[str] = []
    stale_skip_reasons: dict[str, str] = {}
    if stale_enabled and not requested:
        now_dt = datetime.now(timezone.utc)
        filtered: list[tuple[str, object]] = []
        for name, fn in sources:
            skip, reason = _should_skip_source(
                name,
                source_health,
                min_runs=stale_min_runs,
                min_returned=stale_min_returned,
                min_dup_rate=stale_min_dup_rate,
                cooldown_hours=stale_cooldown_hours,
                now=now_dt,
            )
            if skip:
                stale_skipped.append(name)
                stale_skip_reasons[name] = reason
                continue
            filtered.append((name, fn))
        if filtered:
            sources = filtered
        else:
            stale_skipped = []
            stale_skip_reasons = {}
    if stale_skipped:
        _save_source_health(source_health)
    fetch_cap = int(disc.get("fetch_cap", cfg.policy.get("caps", {}).get("fetch_per_run", 250)))
    priority_sources = {
        "welcometothejungle",
        "kering",
        "chanel",
        "sothebys",
        "christies",
        "centre_pompidou",
        "profilculture",
    }
    priority_cap = int(disc.get("priority_source_cap", 120))
    fallback_cap = int(disc.get("fallback_source_cap", 60))
    source_caps = {str(k): int(v) for k, v in (disc.get("source_caps") or {}).items()}
    internship_cap = disc.get("internship_cap_per_source")
    traineeship_cap = disc.get("traineeship_cap_per_source")
    company_cap = disc.get("company_cap")
    internship_cap_total = disc.get("internship_cap_total")
    traineeship_cap_total = disc.get("traineeship_cap_total")
    nyc_backfill_count = int(disc.get("nyc_backfill_count", 0) or 0)
    nyc_backfill_days = int(disc.get("nyc_backfill_days", 90) or 90)

    brain = ScoringBrain(cfg)
    rows: list[dict] = []
    score_results = []
    source_counts: dict[str, int] = {}
    source_stats: dict[str, dict] = {}
    source_errors: list[str] = []
    run_fingerprints: set[str] = set()
    source_work_counts: dict[str, dict[str, int]] = {}
    company_counts: dict[str, int] = {}
    global_work_counts: dict[str, int] = {}
    pulled_total = 0
    deduped_count = 0
    fuzzy_duped = 0
    new_count = 0

    audit_handle = None
    if audit_rejects_enabled:
        audit_dir = ROOT / "data" / "audit"
        audit_dir.mkdir(parents=True, exist_ok=True)
        audit_handle = (audit_dir / f"rejects_{run_id}.jsonl").open("w", encoding="utf-8")

    def _audit_reject(reason: str, item: dict, *, detail: dict | None = None) -> None:
        if audit_handle is None:
            return
        payload = {
            "run_id": run_id,
            "reason": reason,
            "source": item.get("source") or "unknown",
            "title": str(item.get("title") or ""),
            "company": str(item.get("company") or ""),
            "url": str(item.get("url") or item.get("apply_url") or ""),
            "detail": detail or {},
        }
        audit_handle.write(json.dumps(payload, ensure_ascii=False) + "\n")

    print(f"[config] quick={args.quick}")
    print(f"[config] city_scope={city_scope}")
    print(f"[config] audit_rejects={audit_rejects_enabled}")
    print(f"[config] include_alternance={include_alternance}")
    print(f"[config] fuzzy_dedupe={fuzzy_enabled} days={fuzzy_days}")
    print(f"[config] active_path={ACTIVE_PATH_NOTE}")
    print(f"[config] enabled_sources={sorted(enabled) if enabled else 'ALL'}")
    print(f"[config] dispatch_order={[name for name, _ in sources]}")
    if stale_skipped:
        print(f"[config] stale_skipped={stale_skipped}")

    existing_fuzzy_keys: set[str] = set()
    if fuzzy_enabled:
        cutoff = datetime.now(timezone.utc) - timedelta(days=fuzzy_days)
        for row in conn.execute(
            "SELECT company,title,location_text FROM jobs_canonical WHERE created_at_utc >= ?",
            (cutoff.isoformat(timespec="seconds"),),
        ):
            key = fuzzy_key(row["company"], row["title"], row["location_text"])
            if key and key != "||":
                existing_fuzzy_keys.add(key)

    def _row_from_result(
        job_id: str,
        fingerprint: str,
        source_name: str,
        job: dict,
        result,
        *,
        backfill: bool = False,
    ) -> dict:
        row = {
            "job_id": job_id,
            "fingerprint": fingerprint,
            "source": source_name,
            "company": job.get("company", ""),
            "title": job.get("title", ""),
            "location_text": job.get("location_text", ""),
            "target_geography": result.target_geography,
            "location_priority": result.location_priority,
            "url": job.get("url", ""),
            "apply_url": job.get("apply_url", ""),
            "queue": result.queue,
            "decision_reason": result.queue_reason,
            "confidence": result.confidence,
            "snippet": snippet(job.get("description_text", "")),
            "score": result.score,
            "fit_score": result.fit_score,
            "bridge_score": result.bridge_score,
            "city_lane": result.city_lane,
            "city_priority_label": result.city_priority_label,
            "opportunity_lanes": result.opportunity_lanes,
            "primary_lane": result.primary_lane,
            "world_tier": result.world_tier,
            "function_family": result.function_family,
            "work_type": result.work_type,
            "work_type_label": result.work_type_label,
            "role_feel": result.role_feel,
            "classification": result.classification,
            "recommendation": result.recommendation,
            "role_bucket": result.role_bucket,
            "role_tier": result.role_tier,
            "explanation": result.explanation,
            "bridge_story": result.bridge_story,
            "slop_check": result.slop_check,
            "why_fit": result.why_fit,
            "why_fail": result.why_fail,
            "french_risk_label": result.french_risk_label,
            "slop_verdict": result.slop_verdict,
            "biggest_resume_gap": result.biggest_resume_gap,
            "one_line_recommendation": result.one_line_recommendation,
            "why_surfaced": result.why_surfaced,
            "why_could_matter": result.why_could_matter,
            "path_logic": result.path_logic,
            "main_risk": result.main_risk,
            "bridge_signal_band": result.bridge_signal_band,
            "fit_signal_band": result.fit_signal_band,
            "risk_flags": result.risk_flags,
            "dimension_scores": result.dimension_scores,
            "signal_scores": result.signal_scores,
            "signal_bands": result.signal_bands,
            "world_hits": result.world_hits,
            "function_hits": result.function_hits,
            "p_qual": result.p_qual,
            "ev": result.ev,
            "score_factors": result.factors,
            "reasons_pos": result.reasons_pos,
            "reasons_neg": result.reasons_neg,
            "red_flags": result.red_flags,
            "gates": {gate.name: gate.status.value for gate in result.gates},
        }
        if backfill:
            row["backfill"] = True
        return row

    def _age_days(iso_ts: str | None) -> int | None:
        if not iso_ts:
            return None
        try:
            created = datetime.fromisoformat(iso_ts)
        except Exception:
            return None
        now = datetime.now(timezone.utc)
        if created.tzinfo is None:
            created = created.replace(tzinfo=timezone.utc)
        return max(0, (now - created).days)

    def _existing_by_source_job_id(source: str, source_job_id: str):
        if not source_job_id:
            return None
        return conn.execute(
            "SELECT job_id,created_at_utc,url,apply_url FROM jobs_canonical WHERE source=? AND source_job_id=? LIMIT 1",
            (source, source_job_id),
        ).fetchone()

    def _existing_by_url(url: str):
        if not url:
            return None
        return conn.execute(
            "SELECT job_id,created_at_utc,url,apply_url,source FROM jobs_canonical WHERE url=? OR apply_url=? LIMIT 1",
            (url, url),
        ).fetchone()

    def ingest(item: dict) -> bool:
        nonlocal deduped_count, new_count, fuzzy_duped
        source_name = (item.get("source") or "unknown").strip() or "unknown"
        stats = source_stats.setdefault(source_name, _base_source_stats())
        work_counts = source_work_counts.setdefault(source_name, {})

        title = str(item.get("title") or "").strip()
        company = str(item.get("company") or "").strip()
        url = str(item.get("url") or item.get("apply_url") or "").strip()
        desc = str(item.get("description_text") or "")[:DESC_LIMIT]
        location_text = str(item.get("location_text") or "").strip()
        remote_type = str(item.get("remote_type") or item.get("remote_hint") or "").strip()
        source_job_id = str(item.get("source_job_id") or "").strip()

        if not title:
            stats["rejected"] += 1
            _audit_reject(
                "missing_title",
                item,
                detail={"missing_fields": ["title"]},
            )
            return False

        if not (desc or url):
            stats["rejected"] += 1
            _audit_reject(
                "missing_description_or_url",
                item,
                detail={"missing_fields": ["description_text", "url"]},
            )
            return False

        if detect_language_gate(title, desc) == "block":
            stats["rejected"] += 1
            _audit_reject(
                "language_gate_block",
                item,
                detail={"gate": "language", "label": "block"},
            )
            return False

        if not include_alternance and _ALTERNANCE_RE.search(title):
            stats["rejected"] += 1
            _audit_reject(
                "alternance_excluded",
                item,
                detail={"reason": "include_alternance=false"},
            )
            return False

        existing = _existing_by_source_job_id(source_name, source_job_id)
        if existing:
            stats["dupes"] += 1
            deduped_count += 1
            _audit_reject(
                "duplicate_source_job_id",
                item,
                detail={
                    "source_job_id": source_job_id,
                    "existing_job_id": existing["job_id"],
                    "existing_age_days": _age_days(existing["created_at_utc"]),
                    "existing_url": existing["url"] or existing["apply_url"] or "",
                },
            )
            return False

        existing = _existing_by_url(url)
        if existing:
            stats["dupes"] += 1
            deduped_count += 1
            _audit_reject(
                "duplicate_url",
                item,
                detail={
                    "existing_job_id": existing["job_id"],
                    "existing_source": existing["source"],
                    "existing_age_days": _age_days(existing["created_at_utc"]),
                    "existing_url": existing["url"] or existing["apply_url"] or "",
                },
            )
            return False

        fingerprint = make_fingerprint(company, title, item.get("apply_url"), url)
        if fingerprint in run_fingerprints:
            stats["dupes"] += 1
            deduped_count += 1
            _audit_reject(
                "duplicate_run",
                item,
                detail={"fingerprint": fingerprint},
            )
            return False
        if fingerprint_exists(conn, fingerprint):
            existing = conn.execute(
                "SELECT job_id,created_at_utc,source,url,apply_url FROM jobs_canonical WHERE fingerprint=? LIMIT 1",
                (fingerprint,),
            ).fetchone()
            detail = {"fingerprint": fingerprint}
            if existing:
                detail.update(
                    {
                        "existing_job_id": existing["job_id"],
                        "existing_source": existing["source"],
                        "existing_age_days": _age_days(existing["created_at_utc"]),
                        "existing_url": existing["url"] or existing["apply_url"] or "",
                    }
                )
            stats["dupes"] += 1
            deduped_count += 1
            _audit_reject(
                "duplicate_db",
                item,
                detail=detail,
            )
            return False

        run_fingerprints.add(fingerprint)

        compensation_text = str(item.get("compensation_text") or "").strip()
        comp_min = item.get("compensation_min")
        comp_max = item.get("compensation_max")
        if comp_min is None and comp_max is None and compensation_text:
            comp_min, comp_max, _unit = parse_compensation(compensation_text)
        if comp_min is None and comp_max is None:
            desc_min, desc_max, _unit = extract_comp_from_description(desc)
            comp_min = desc_min
            comp_max = desc_max

        if fuzzy_enabled and source_name not in fuzzy_allow_sources:
            fkey = fuzzy_key(company, title, location_text)
            if fkey and fkey in existing_fuzzy_keys:
                stats["fuzzy_dupes"] += 1
                fuzzy_duped += 1
                _audit_reject(
                    "duplicate_fuzzy",
                    item,
                    detail={"fuzzy_key": fkey},
                )
                return False

        job = {
            "source": source_name,
            "company": company,
            "title": title,
            "location_text": location_text,
            "remote_type": remote_type,
            "url": url,
            "apply_url": str(item.get("apply_url") or url),
            "description_text": desc,
            "compensation_text": compensation_text,
            "compensation_min": comp_min,
            "compensation_max": comp_max,
            "language_requirements": str(item.get("language_requirements") or ""),
        }
        result = brain.score(job)

        work_label = result.work_type_label or "Full-time"
        company_key = company.lower()
        if company_cap is not None and company_key:
            if company_counts.get(company_key, 0) >= int(company_cap):
                stats["rejected"] += 1
                _audit_reject(
                    "company_cap",
                    item,
                    detail={"company": company, "cap": int(company_cap)},
                )
                return False
        if internship_cap is not None and work_label == "Internship":
            if work_counts.get(work_label, 0) >= int(internship_cap):
                stats["rejected"] += 1
                _audit_reject(
                    "work_type_cap",
                    item,
                    detail={"work_type": work_label, "cap": int(internship_cap)},
                )
                return False
        if traineeship_cap is not None and work_label == "Traineeship / Apprenticeship":
            if work_counts.get(work_label, 0) >= int(traineeship_cap):
                stats["rejected"] += 1
                _audit_reject(
                    "work_type_cap",
                    item,
                    detail={"work_type": work_label, "cap": int(traineeship_cap)},
                )
                return False
        if internship_cap_total is not None and work_label == "Internship":
            if global_work_counts.get(work_label, 0) >= int(internship_cap_total):
                stats["rejected"] += 1
                _audit_reject(
                    "work_type_global_cap",
                    item,
                    detail={"work_type": work_label, "cap": int(internship_cap_total)},
                )
                return False
        if traineeship_cap_total is not None and work_label == "Traineeship / Apprenticeship":
            if global_work_counts.get(work_label, 0) >= int(traineeship_cap_total):
                stats["rejected"] += 1
                _audit_reject(
                    "work_type_global_cap",
                    item,
                    detail={"work_type": work_label, "cap": int(traineeship_cap_total)},
                )
                return False
        evidence = result.to_evidence()

        raw_id = str(uuid.uuid4())
        job_id = str(uuid.uuid4())
        decision_id = str(uuid.uuid4())

        conn.execute(
            "INSERT INTO jobs_raw(raw_id,run_id,source,fetched_at_utc,payload_json) VALUES(?,?,?,?,?)",
            (
                raw_id,
                run_id,
                source_name,
                utcnow(),
                json.dumps(item, ensure_ascii=False, default=str),
            ),
        )
        conn.execute(
            """
            INSERT INTO jobs_canonical(
              job_id,run_id,source,source_job_id,company,title,location_text,remote_type,
              language_requirements,compensation_min,compensation_max,compensation_text,
              url,apply_url,description_text,meetings_band,async_hint,relocation_hint,
              created_at_utc,fingerprint
            ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                job_id,
                run_id,
                source_name,
                source_job_id,
                company,
                title,
                location_text,
                remote_type,
                job["language_requirements"],
                comp_min,
                comp_max,
                compensation_text,
                job["url"],
                job["apply_url"],
                desc,
                "",
                0,
                0,
                utcnow(),
                fingerprint,
            ),
        )
        conn.execute(
            """
            INSERT INTO decisions(
              decision_id,run_id,job_id,queue,decision_reason,confidence,evidence_json,decided_at_utc
            ) VALUES(?,?,?,?,?,?,?,?)
            """,
            (
                decision_id,
                run_id,
                job_id,
                result.queue,
                result.queue_reason,
                result.confidence,
                json.dumps(evidence, ensure_ascii=False),
                utcnow(),
            ),
        )

        row = _row_from_result(job_id, fingerprint, source_name, job, result)
        rows.append(row)
        score_results.append(result)
        if fuzzy_enabled and source_name not in fuzzy_allow_sources:
            fkey = fuzzy_key(company, title, location_text)
            if fkey:
                existing_fuzzy_keys.add(fkey)
        work_counts[work_label] = work_counts.get(work_label, 0) + 1
        global_work_counts[work_label] = global_work_counts.get(work_label, 0) + 1
        if company_key:
            company_counts[company_key] = company_counts.get(company_key, 0) + 1
        stats["new"] += 1
        if result.recommendation != "skip":
            stats["core_fit"] += 1
        source_counts[source_name] = source_counts.get(source_name, 0) + 1
        new_count += 1
        return True

    max_workers = min(len(sources), 6)
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_map = {}
        for name, fn in sources:
            _emit_progress(name, "running", started_at, pulled_total=pulled_total, cap=fetch_cap)
            future_map[executor.submit(_run_source, name, fn, args.quick, source_kwargs)] = name

        for future in as_completed(future_map):
            name = future_map[future]
            stats = source_stats.setdefault(name, _base_source_stats())
            try:
                _name, items = future.result()
                items = list(items or [])
                stats["returned"] = len(items)
                pulled_total += len(items)
                print(f"[source] {name}: returned {len(items)} jobs")
            except Exception as exc:
                error = f"{type(exc).__name__}: {exc}"
                source_errors.append(f"{name}:{error}")
                print(f"[source] {name}: error {error}")
                _emit_progress(
                    name,
                    "error",
                    started_at,
                    pulled_total=pulled_total,
                    cap=fetch_cap,
                    error=error,
                )
                continue

            source_cap = priority_cap if name in priority_sources else fallback_cap
            if name in source_caps:
                source_cap = min(source_cap, source_caps[name])
            ingested = 0
            scan_cap = int(source_cap * scan_multiplier) if source_cap else len(items)
            scan_cap = min(len(items), max(source_cap, scan_cap))
            attempted = 0
            for item in items:
                if attempted >= scan_cap:
                    break
                if new_count >= fetch_cap:
                    break
                if source_cap and ingested >= source_cap:
                    break
                attempted += 1
                if ingest(item):
                    ingested += 1
            stats["capped"] = max(0, len(items) - attempted)
            conn.commit()
            _emit_progress(
                name,
                "done",
                started_at,
                returned=len(items),
                ingested=ingested,
                pulled_total=pulled_total,
                cap=fetch_cap,
            )

    backfill_count = 0
    if nyc_backfill_count and not any(row.get("city_lane") == "NYC" for row in rows):
        cutoff = datetime.now(timezone.utc) - timedelta(days=nyc_backfill_days)
        cursor = conn.execute(
            """
            SELECT job_id,fingerprint,source,company,title,location_text,remote_type,
                   language_requirements,compensation_min,compensation_max,compensation_text,
                   url,apply_url,description_text,created_at_utc
            FROM jobs_canonical
            WHERE created_at_utc >= ?
            ORDER BY created_at_utc DESC
            LIMIT 300
            """,
            (cutoff.isoformat(timespec="seconds"),),
        )
        for row in cursor:
            if backfill_count >= nyc_backfill_count:
                break
            fingerprint = row["fingerprint"] or make_fingerprint(
                row["company"], row["title"], row["apply_url"], row["url"]
            )
            if fingerprint in run_fingerprints:
                continue
            job = {
                "source": row["source"],
                "company": row["company"],
                "title": row["title"],
                "location_text": row["location_text"],
                "remote_type": row["remote_type"],
                "url": row["url"],
                "apply_url": row["apply_url"],
                "description_text": row["description_text"],
                "compensation_text": row["compensation_text"],
                "compensation_min": row["compensation_min"],
                "compensation_max": row["compensation_max"],
                "language_requirements": row["language_requirements"],
            }
            result = brain.score(job)
            if result.city_lane != "NYC":
                continue
            rows.append(_row_from_result(row["job_id"], fingerprint, row["source"], job, result, backfill=True))
            score_results.append(result)
            run_fingerprints.add(fingerprint)
            backfill_count += 1

    batch_stats = batch_route(score_results, cfg.queues)
    classification_counts: dict[str, int] = {}
    recommendation_counts: dict[str, int] = {}
    geography_counts: dict[str, int] = {}
    city_lane_counts: dict[str, int] = {}
    lane_counts: dict[str, int] = {}
    world_tier_counts: dict[str, int] = {}
    work_type_counts: dict[str, int] = {}
    queue_counts: dict[int, int] = {}

    for row, result in zip(rows, score_results):
        queue_counts[result.queue] = queue_counts.get(result.queue, 0) + 1
        classification_counts[result.classification] = classification_counts.get(result.classification, 0) + 1
        recommendation_counts[result.recommendation] = recommendation_counts.get(result.recommendation, 0) + 1
        geography = row.get("target_geography") or "unknown"
        geography_counts[geography] = geography_counts.get(geography, 0) + 1
        city_lane_counts[result.city_lane] = city_lane_counts.get(result.city_lane, 0) + 1
        world_tier_counts[result.world_tier] = world_tier_counts.get(result.world_tier, 0) + 1
        work_type_counts[result.work_type_label] = work_type_counts.get(result.work_type_label, 0) + 1
        for lane in result.opportunity_lanes:
            lane_counts[lane] = lane_counts.get(lane, 0) + 1

    queue_labels = {
        cfg.queues["REVIEW"]: "Review",
        cfg.queues["MAYBE"]: "Maybe",
        cfg.queues["REJECT"]: "Reject",
    }

    print("\n=== SOURCE STATS ===")
    for name in [name for name, _ in sources]:
        stats = source_stats.get(name) or _base_source_stats()
        print(
            f"  {name:<20} returned={stats['returned']:>3}  new={stats['new']:>3}  "
            f"dupes={stats['dupes']:>3}  fuzzy={stats['fuzzy_dupes']:>3}  "
            f"rejected={stats['rejected']:>3}  capped={stats['capped']:>3}  "
            f"on_plan={stats['core_fit']:>3}"
        )
    print("====================\n")

    run_dir = str(Path(cfg.runs_dir) / run_id)
    meta = {
        "run_id": run_id,
        "run_dir": run_dir,
        "active_path_note": ACTIVE_PATH_NOTE,
        "pulled_total": pulled_total,
        "deduped_count": deduped_count,
        "fuzzy_duped": fuzzy_duped,
        "new_count": new_count,
        "backfill_count": backfill_count,
        "city_scope": city_scope,
        "stale_skipped_sources": stale_skipped,
        "stale_skip_reasons": stale_skip_reasons,
        "queue_counts": queue_counts,
        "tier_counts": {queue_labels.get(q, str(q)): count for q, count in queue_counts.items()},
        "geography_counts": geography_counts,
        "city_lane_counts": city_lane_counts,
        "lane_counts": lane_counts,
        "world_tier_counts": world_tier_counts,
        "work_type_counts": work_type_counts,
        "classification_counts": classification_counts,
        "recommendation_counts": recommendation_counts,
        "source_counts": source_counts,
        "source_stats": source_stats,
        "source_errors": source_errors,
        "batch_stats": batch_stats,
    }

    report_path = write_report(run_dir, run_id, rows, meta)
    meta["report_path"] = report_path
    print(f"REPORT={report_path}")

    data_dir = ROOT / "data"
    data_dir.mkdir(parents=True, exist_ok=True)
    (data_dir / "last_meta.json").write_text(
        json.dumps(meta, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    if stale_enabled:
        _update_source_health(source_health, run_id, source_stats)
        _save_source_health(source_health)

    conn.execute("UPDATE runs SET ended_at_utc=? WHERE run_id=?", (utcnow(), run_id))
    conn.commit()
    conn.close()
    if audit_handle is not None:
        audit_handle.close()

    print(f"RUN_ID={run_id}")
    print(f"PULLED_TOTAL={pulled_total}")
    print(f"NEW={new_count}")
    print(f"SEEN={deduped_count}")
    print(f"FUZZY_DUPES={fuzzy_duped}")
    print(f"SOURCE_COUNTS={json.dumps(source_counts)}")
    print(f"QUEUE_COUNTS={json.dumps(queue_counts)}")
    if source_errors:
        print(f"ERRORS={source_errors}")


if __name__ == "__main__":
    main()
