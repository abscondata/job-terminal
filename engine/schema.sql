PRAGMA journal_mode=WAL;

CREATE TABLE IF NOT EXISTS runs (
  run_id TEXT PRIMARY KEY,
  started_at_utc TEXT NOT NULL,
  ended_at_utc TEXT,
  mode TEXT NOT NULL,
  config_snapshot TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS jobs_raw (
  raw_id TEXT PRIMARY KEY,
  run_id TEXT NOT NULL,
  source TEXT NOT NULL,
  fetched_at_utc TEXT NOT NULL,
  payload_json TEXT NOT NULL,
  FOREIGN KEY(run_id) REFERENCES runs(run_id)
);

CREATE TABLE IF NOT EXISTS jobs_canonical (
  job_id TEXT PRIMARY KEY,
  run_id TEXT NOT NULL,
  source TEXT NOT NULL,
  source_job_id TEXT,
  company TEXT,
  title TEXT,
  location_text TEXT,
  remote_type TEXT,
  language_requirements TEXT,
  compensation_min INTEGER,
  compensation_max INTEGER,
  compensation_text TEXT,
  url TEXT,
  apply_url TEXT,
  description_text TEXT,
  meetings_band TEXT,
  async_hint INTEGER,
  relocation_hint INTEGER,
  created_at_utc TEXT NOT NULL,
  fingerprint TEXT NOT NULL,
  FOREIGN KEY(run_id) REFERENCES runs(run_id)
);

CREATE TABLE IF NOT EXISTS decisions (
  decision_id TEXT PRIMARY KEY,
  run_id TEXT NOT NULL,
  job_id TEXT NOT NULL,
  queue INTEGER NOT NULL,
  decision_reason TEXT NOT NULL,
  confidence REAL NOT NULL,
  evidence_json TEXT NOT NULL,
  decided_at_utc TEXT NOT NULL,
  FOREIGN KEY(run_id) REFERENCES runs(run_id),
  FOREIGN KEY(job_id) REFERENCES jobs_canonical(job_id)
);

CREATE TABLE IF NOT EXISTS saved_roles (
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
);

CREATE TABLE IF NOT EXISTS suppressed_jobs (
  suppression_id TEXT PRIMARY KEY,
  company_norm TEXT NOT NULL,
  title_norm TEXT NOT NULL,
  url TEXT,
  source_job_id TEXT,
  fingerprint TEXT,
  reason TEXT NOT NULL,
  suppressed_at_utc TEXT NOT NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_jobs_fingerprint ON jobs_canonical(fingerprint);
CREATE INDEX IF NOT EXISTS idx_jobs_run ON jobs_canonical(run_id);
CREATE INDEX IF NOT EXISTS idx_decisions_run ON decisions(run_id);
CREATE INDEX IF NOT EXISTS idx_saved_roles_bucket ON saved_roles(review_bucket);
CREATE INDEX IF NOT EXISTS idx_saved_roles_updated ON saved_roles(updated_at_utc);
CREATE INDEX IF NOT EXISTS idx_suppressed_company_title ON suppressed_jobs(company_norm, title_norm);
CREATE INDEX IF NOT EXISTS idx_suppressed_fingerprint ON suppressed_jobs(fingerprint);
