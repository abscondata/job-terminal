from __future__ import annotations

import html as htmlmod
import json
import os
import re
import sqlite3
import subprocess
import sys
import threading
import uuid
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path

import requests

from engine.config import load_config
from engine.models import make_fingerprint
from engine.scoring import ScoringBrain

ROOT = Path(__file__).parent.parent
PY = sys.executable

VALID_REVIEW_BUCKETS = {
    "Saved",
    "Applied",
    "Rejected",
    "Maybe",
    "Dream Bridge",
    "Strong Bridge",
    "Practical Paris Entry",
    "Skip",
}
REVIEW_BUCKET_ORDER = {
    "Applied": 0,
    "Saved": 1,
    "Maybe": 2,
    "Rejected": 3,
    "Dream Bridge": 4,
    "Strong Bridge": 5,
    "Practical Paris Entry": 6,
    "Skip": 7,
}
TAG_RE = re.compile(r"<[^>]+>")
SCRIPT_JSON_RE = re.compile(
    r"<script[^>]+type=[\"']application/ld\\+json[\"'][^>]*>(.*?)</script>",
    re.I | re.S,
)
TITLE_RE = re.compile(r"<title[^>]*>(.*?)</title>", re.I | re.S)
META_RE = re.compile(
    r'<meta[^>]+(?:name|property)=["\']([^"\']+)["\'][^>]+content=["\']([^"\']*)["\']',
    re.I,
)
TERMINAL_HTML_PATH = Path(__file__).with_name("job_terminal.html")


def utcnow():
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def ensure_schema(conn):
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS saved_roles(
          saved_id TEXT PRIMARY KEY,
          fingerprint TEXT NOT NULL UNIQUE,
          source TEXT,
          company TEXT,
          title TEXT,
          location_text TEXT,
          url TEXT,
          description_text TEXT,
          classification TEXT,
          recommendation TEXT,
          role_bucket TEXT,
          fit_score INTEGER,
          bridge_score INTEGER,
          overall_score INTEGER,
          review_bucket TEXT NOT NULL,
          evidence_json TEXT NOT NULL,
          saved_at_utc TEXT NOT NULL,
          updated_at_utc TEXT NOT NULL
        )
        """
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_saved_roles_bucket ON saved_roles(review_bucket)"
    )
    conn.commit()


def _clean_text(value: str | None) -> str:
    text = htmlmod.unescape(value or "")
    text = TAG_RE.sub(" ", text)
    return re.sub(r"\s+", " ", text).strip()


def _extract_job_posting_from_html(url: str, html_text: str) -> dict:
    payload = {
        "url": url,
        "title": "",
        "company": "",
        "location_text": "",
        "description_text": "",
    }

    for match in SCRIPT_JSON_RE.finditer(html_text or ""):
        raw = htmlmod.unescape(match.group(1) or "").strip()
        if not raw:
            continue
        try:
            data = json.loads(raw)
        except Exception:
            continue
        candidates = data if isinstance(data, list) else [data]
        for candidate in candidates:
            if not isinstance(candidate, dict):
                continue
            if str(candidate.get("@type", "")).lower() != "jobposting":
                continue
            hiring = candidate.get("hiringOrganization") or {}
            location = candidate.get("jobLocation") or {}
            if isinstance(location, list):
                location = location[0] if location else {}
            address = location.get("address") or {}
            payload["title"] = _clean_text(candidate.get("title"))
            payload["company"] = _clean_text(hiring.get("name"))
            payload["location_text"] = _clean_text(
                address.get("addressLocality")
                or address.get("addressRegion")
                or candidate.get("jobLocationType")
            )
            payload["description_text"] = _clean_text(candidate.get("description"))
            return payload

    metas = {key.lower(): value for key, value in META_RE.findall(html_text or "")}
    title_match = TITLE_RE.search(html_text or "")
    payload["title"] = _clean_text(
        metas.get("og:title")
        or metas.get("twitter:title")
        or (title_match.group(1) if title_match else "")
    )
    payload["description_text"] = _clean_text(
        metas.get("description")
        or metas.get("og:description")
        or metas.get("twitter:description")
    )
    return payload


def _triage_job_payload(data: dict) -> tuple[dict, str | None]:
    url = str(data.get("url", "")).strip()
    job = {
        "source": "manual_input",
        "title": str(data.get("title", "")).strip(),
        "company": str(data.get("company", "")).strip(),
        "location_text": str(data.get("location_text", "")).strip(),
        "description_text": str(data.get("description_text", "")).strip(),
        "remote_type": str(data.get("remote_type", "")).strip(),
        "language_requirements": "",
        "compensation_text": "",
        "compensation_min": None,
        "compensation_max": None,
        "url": url,
        "apply_url": url,
    }

    if url and (not job["description_text"] or not job["title"]):
        try:
            resp = requests.get(
                url,
                timeout=20,
                headers={
                    "User-Agent": (
                        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                        "AppleWebKit/537.36 (KHTML, like Gecko) "
                        "Chrome/122.0.0.0 Safari/537.36"
                    )
                },
            )
            if resp.ok:
                parsed = _extract_job_posting_from_html(url, resp.text)
                for key in ["title", "company", "location_text", "description_text"]:
                    if not job[key] and parsed.get(key):
                        job[key] = parsed[key]
        except Exception:
            pass

    if not job["title"] and not job["description_text"]:
        return job, "Paste job text or provide a publicly readable job URL."
    return job, None


def _normalize_saved_role(data: dict) -> tuple[dict | None, str | None]:
    review_bucket = str(data.get("review_bucket", "")).strip()
    if review_bucket not in VALID_REVIEW_BUCKETS:
        return None, (
            "review_bucket must be Saved, Applied, Rejected, Maybe, "
            "Dream Bridge, Strong Bridge, Practical Paris Entry, or Skip."
        )

    role = data.get("role") or {}
    if not role and data.get("job") and data.get("score"):
        job = data.get("job") or {}
        score = data.get("score") or {}
        role = {
            "source": job.get("source", "manual_input"),
            "company": job.get("company", ""),
            "title": job.get("title", ""),
            "location_text": job.get("location_text", ""),
            "url": job.get("url") or job.get("apply_url") or "",
            "apply_url": job.get("apply_url") or job.get("url") or "",
            "description_text": job.get("description_text", ""),
            "classification": score.get("classification", ""),
            "recommendation": score.get("recommendation", ""),
            "role_bucket": score.get("role_bucket", ""),
            "fit_score": score.get("fit_score", 0),
            "bridge_score": score.get("bridge_score", 0),
            "score": score.get("score", 0),
            "why_fit": score.get("why_fit", ""),
            "why_fail": score.get("why_fail", ""),
            "french_risk_label": score.get("french_risk_label", ""),
            "slop_verdict": score.get("slop_verdict", ""),
            "biggest_resume_gap": score.get("biggest_resume_gap", ""),
            "one_line_recommendation": score.get("one_line_recommendation", ""),
            "bridge_story": score.get("bridge_story", ""),
            "slop_check": score.get("slop_check", ""),
            "city_lane": score.get("city_lane", ""),
            "city_priority_label": score.get("city_priority_label", ""),
            "opportunity_lanes": score.get("opportunity_lanes", []),
            "primary_lane": score.get("primary_lane", ""),
            "world_tier": score.get("world_tier", ""),
            "function_family": score.get("function_family", ""),
            "work_type": score.get("work_type", ""),
            "work_type_label": score.get("work_type_label", ""),
            "role_feel": score.get("role_feel", ""),
            "why_surfaced": score.get("why_surfaced", ""),
            "why_could_matter": score.get("why_could_matter", ""),
            "path_logic": score.get("path_logic", ""),
            "main_risk": score.get("main_risk", ""),
            "bridge_signal_band": score.get("bridge_signal_band", ""),
            "fit_signal_band": score.get("fit_signal_band", ""),
            "top_brand_risk": score.get("top_brand_risk", False),
            "signal_scores": score.get("signal_scores", {}),
            "signal_bands": score.get("signal_bands", {}),
            "risk_flags": score.get("risk_flags", []),
            "dimension_scores": score.get("dimension_scores", {}),
        }

    if not role:
        return None, "No role payload provided."

    company = str(role.get("company", "")).strip()
    title = str(role.get("title", "")).strip()
    url = str(role.get("url") or role.get("apply_url") or "").strip()
    if not title and not url:
        return None, "Saved role needs a title or URL."

    fingerprint = str(role.get("fingerprint", "")).strip() or make_fingerprint(
        company,
        title,
        role.get("apply_url"),
        role.get("url"),
    )
    role["fingerprint"] = fingerprint
    role["review_bucket"] = review_bucket
    return role, None


def _saved_rows(conn) -> list[dict]:
    rows = conn.execute(
        """
        SELECT *
        FROM saved_roles
        ORDER BY updated_at_utc DESC
        """
    ).fetchall()
    out = []
    for row in rows:
        item = dict(row)
        try:
            evidence = json.loads(item.get("evidence_json") or "{}")
        except Exception:
            evidence = {}
        item["evidence"] = evidence
        item.update(evidence)
        out.append(item)
    out.sort(
        key=lambda item: (
            REVIEW_BUCKET_ORDER.get(item.get("review_bucket", "Skip"), 99),
            item.get("city_lane", ""),
            item.get("updated_at_utc", ""),
        )
    )
    return out


def _latest_run(conn: sqlite3.Connection) -> sqlite3.Row | None:
    row = conn.execute(
        """
        SELECT run_id, started_at_utc, ended_at_utc
        FROM runs
        WHERE ended_at_utc IS NOT NULL
        ORDER BY ended_at_utc DESC
        LIMIT 1
        """
    ).fetchone()
    if row:
        return row
    return conn.execute(
        "SELECT run_id, started_at_utc, ended_at_utc FROM runs ORDER BY started_at_utc DESC LIMIT 1"
    ).fetchone()


def _latest_jobs(conn: sqlite3.Connection, run_id: str) -> list[dict]:
    rows = conn.execute(
        """
        SELECT j.job_id,j.fingerprint,j.source,j.company,j.title,j.location_text,
               j.url,j.apply_url,j.description_text,d.evidence_json,d.decided_at_utc
        FROM decisions d
        JOIN jobs_canonical j ON d.job_id = j.job_id
        WHERE d.run_id = ?
        """,
        (run_id,),
    ).fetchall()
    out = []
    for row in rows:
        try:
            evidence = json.loads(row["evidence_json"] or "{}")
        except Exception:
            evidence = {}
        payload = dict(evidence)
        payload.update(
            {
                "job_id": row["job_id"],
                "fingerprint": row["fingerprint"],
                "source": row["source"],
                "company": row["company"] or "",
                "title": row["title"] or "",
                "location_text": row["location_text"] or "",
                "url": row["url"] or "",
                "apply_url": row["apply_url"] or "",
                "description_text": row["description_text"] or "",
                "decided_at_utc": row["decided_at_utc"] or "",
            }
        )
        if "score" not in payload:
            payload["score"] = payload.get("overall_score", 0)
        out.append(payload)
    return out


def start_state_server(host, port, db_path):
    conn = sqlite3.connect(db_path, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    ensure_schema(conn)
    refresh_state = {
        "running": False,
        "last_run_id": None,
        "last_started_at": None,
        "last_finished_at": None,
        "last_error": None,
        "last_exit_code": None,
    }
    cache_state = {
        "run": None,
        "jobs": [],
        "updated_at_utc": None,
    }
    refresh_lock = threading.Lock()

    def _open_db() -> sqlite3.Connection:
        db = sqlite3.connect(db_path, timeout=1.0)
        db.row_factory = sqlite3.Row
        try:
            db.execute("PRAGMA busy_timeout=1000;")
        except Exception:
            pass
        return db

    def _read_terminal_html() -> str:
        try:
            return TERMINAL_HTML_PATH.read_text(encoding="utf-8")
        except Exception:
            return "<html><body>Job Terminal not available.</body></html>"

    def _run_refresh():
        with refresh_lock:
            refresh_state["running"] = True
            refresh_state["last_started_at"] = utcnow()
            refresh_state["last_error"] = None
            refresh_state["last_exit_code"] = None
        run_id = None
        try:
            env = {**os.environ, "PYTHONPATH": str(ROOT)}
            proc = subprocess.Popen(
                [PY, str(ROOT / "scripts" / "run_discovery.py")],
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                env=env,
                cwd=str(ROOT),
                encoding="utf-8",
                errors="replace",
            )
            if proc.stdout:
                for raw_line in proc.stdout:
                    line = raw_line.strip()
                    if line.startswith("RUN_ID="):
                        run_id = line.split("=", 1)[1].strip()
            exit_code = proc.wait()
            with refresh_lock:
                refresh_state["last_exit_code"] = exit_code
                refresh_state["last_run_id"] = run_id
                if exit_code != 0:
                    refresh_state["last_error"] = "discovery_failed"
        except Exception as exc:
            with refresh_lock:
                refresh_state["last_error"] = str(exc)
        finally:
            with refresh_lock:
                refresh_state["running"] = False
                refresh_state["last_finished_at"] = utcnow()

    def _prime_cache():
        if not Path(db_path).exists():
            return
        try:
            with _open_db() as db:
                latest = _latest_run(db)
                if latest:
                    run_payload = {
                        "run_id": latest["run_id"],
                        "started_at_utc": latest["started_at_utc"],
                        "ended_at_utc": latest["ended_at_utc"],
                    }
                    jobs = _latest_jobs(db, latest["run_id"])
                    cache_state["run"] = run_payload
                    cache_state["jobs"] = jobs
                    cache_state["updated_at_utc"] = utcnow()
        except Exception:
            return

    _prime_cache()

    class H(BaseHTTPRequestHandler):
        def _cors(self):
            self.send_header("Access-Control-Allow-Origin", "*")
            self.send_header("Access-Control-Allow-Headers", "Content-Type")
            self.send_header("Access-Control-Allow-Methods", "GET,POST,OPTIONS")

        def _json(self, code, data):
            body = json.dumps(data, ensure_ascii=False, default=str).encode("utf-8")
            self.send_response(code)
            self._cors()
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def do_OPTIONS(self):
            self.send_response(204)
            self._cors()
            self.end_headers()

        def do_GET(self):
            if self.path in ("/", "/index.html"):
                html = _read_terminal_html()
                body = html.encode("utf-8")
                self.send_response(200)
                self.send_header("Content-Type", "text/html; charset=utf-8")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
                return

            if self.path.startswith("/jobs/latest"):
                run_payload = None
                jobs = []
                meta = {"db_path": str(db_path), "db_exists": Path(db_path).exists()}
                error = None
                if meta["db_exists"]:
                    try:
                        with _open_db() as db:
                            meta["run_count"] = int(
                                db.execute("SELECT COUNT(*) FROM runs").fetchone()[0]
                            )
                            latest = _latest_run(db)
                            if latest:
                                run_payload = {
                                    "run_id": latest["run_id"],
                                    "started_at_utc": latest["started_at_utc"],
                                    "ended_at_utc": latest["ended_at_utc"],
                                }
                                jobs = _latest_jobs(db, latest["run_id"])
                                meta["decision_count"] = len(jobs)
                                cache_state["run"] = run_payload
                                cache_state["jobs"] = jobs
                                cache_state["updated_at_utc"] = utcnow()
                    except Exception as exc:
                        error = str(exc)
                else:
                    meta["run_count"] = 0

                if (error or not run_payload) and cache_state["run"]:
                    meta["cache_used"] = True
                    run_payload = cache_state["run"]
                    jobs = cache_state["jobs"]
                with refresh_lock:
                    refresh_payload = dict(refresh_state)
                self._json(
                    200,
                    {
                        "run": run_payload,
                        "jobs": jobs,
                        "refresh": refresh_payload,
                        "meta": meta,
                        "error": error,
                    },
                )
                return

            if self.path.startswith("/refresh/status"):
                with refresh_lock:
                    payload = dict(refresh_state)
                self._json(200, payload)
                return

            if self.path.startswith("/saved/list"):
                self._json(200, {"items": _saved_rows(conn)})
                return

            self.send_response(404)
            self._cors()
            self.end_headers()

        def do_POST(self):
            n = int(self.headers.get("Content-Length", "0"))
            try:
                data = json.loads(self.rfile.read(n).decode())
            except Exception:
                data = {}

            if self.path.startswith("/refresh"):
                with refresh_lock:
                    if refresh_state["running"]:
                        self._json(409, {"error": "refresh_in_progress", "refresh": dict(refresh_state)})
                        return
                threading.Thread(target=_run_refresh, daemon=True).start()
                with refresh_lock:
                    payload = dict(refresh_state)
                self._json(200, {"ok": True, "refresh": payload})
                return

            if self.path.startswith("/triage/evaluate"):
                job, error = _triage_job_payload(data)
                if error:
                    self._json(400, {"error": error, "job": job})
                    return
                cfg = load_config(str(ROOT / "config.json"))
                brain = ScoringBrain(cfg)
                result = brain.score(job)
                fingerprint = make_fingerprint(job.get("company"), job.get("title"), job.get("apply_url"), job.get("url"))
                self._json(
                    200,
                    {
                        "ok": True,
                        "fingerprint": fingerprint,
                        "job": job,
                        "classification": result.classification,
                        "recommendation": result.recommendation,
                        "score": result.to_evidence(),
                    },
                )
                return

            if self.path.startswith("/saved/save"):
                role, error = _normalize_saved_role(data)
                if error:
                    self._json(400, {"error": error})
                    return

                now = utcnow()
                evidence_json = json.dumps(role, ensure_ascii=False)
                conn.execute(
                    """
                    INSERT INTO saved_roles(
                      saved_id,fingerprint,source,company,title,location_text,url,description_text,
                      classification,recommendation,role_bucket,fit_score,bridge_score,overall_score,
                      review_bucket,evidence_json,saved_at_utc,updated_at_utc
                    ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                    ON CONFLICT(fingerprint) DO UPDATE SET
                      source=excluded.source,
                      company=excluded.company,
                      title=excluded.title,
                      location_text=excluded.location_text,
                      url=excluded.url,
                      description_text=excluded.description_text,
                      classification=excluded.classification,
                      recommendation=excluded.recommendation,
                      role_bucket=excluded.role_bucket,
                      fit_score=excluded.fit_score,
                      bridge_score=excluded.bridge_score,
                      overall_score=excluded.overall_score,
                      review_bucket=excluded.review_bucket,
                      evidence_json=excluded.evidence_json,
                      updated_at_utc=excluded.updated_at_utc
                    """,
                    (
                        str(uuid.uuid4()),
                        role["fingerprint"],
                        str(role.get("source", "")),
                        str(role.get("company", "")),
                        str(role.get("title", "")),
                        str(role.get("location_text", "")),
                        str(role.get("url") or role.get("apply_url") or ""),
                        str(role.get("description_text", "")),
                        str(role.get("classification", "")),
                        str(role.get("recommendation", "")),
                        str(role.get("role_bucket", "")),
                        int(role.get("fit_score") or 0),
                        int(role.get("bridge_score") or 0),
                        int(role.get("score") or role.get("overall_score") or 0),
                        role["review_bucket"],
                        evidence_json,
                        now,
                        now,
                    ),
                )
                conn.commit()
                self._json(200, {"ok": True, "items": _saved_rows(conn)})
                return

            if self.path.startswith("/saved/delete"):
                fingerprint = str(data.get("fingerprint", "")).strip()
                if not fingerprint:
                    self._json(400, {"error": "fingerprint required"})
                    return
                conn.execute("DELETE FROM saved_roles WHERE fingerprint=?", (fingerprint,))
                conn.commit()
                self._json(200, {"ok": True, "items": _saved_rows(conn)})
                return

            self.send_response(404)
            self._cors()
            self.end_headers()

        def log_message(self, *args):
            return

    HTTPServer((host, port), H).serve_forever()
