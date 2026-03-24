# JobEngine

JobEngine is a life-direction review tool, not a generic job engine.

Its active question is:

`What path does this role create, and is it worth leaving for?`

## Active Product Shape

- Paris is the dream lane.
- NYC is the main realism / platform comparator.
- Miami is tertiary.
- Discovery is broad inside the right worlds, not broad across all jobs.
- Review is lane-based and explanation-first.
- Manual triage and saved review are core, not side utilities.

## What The Scorer Produces

Each role now carries:

- city lane
- primary path lane
- opportunity lanes
- world tier
- function family
- work type
- French burden
- bridge or slop verdict
- path logic
- main risk
- apply / maybe / skip guidance

Internal numeric signals still exist for routing and sorting, but they are no longer the visible product.

## Core Flow

```text
Discovery
  scripts/run_discovery.py
    -> engine/scoring/brain.py
    -> engine/report.py

Job Terminal UI (main)
  engine/state_server.py /
    -> shows ranked roles + actions

Manual triage (optional)
  engine/state_server.py /triage/evaluate
    -> engine/scoring/brain.py

Saved review
  engine/state_server.py /saved/*
```

## Entry Points

1. `C:\Users\Robin\Documents\Playground\paris_direction_engine\run\open_job_terminal.bat` (Job Terminal UI)
2. `C:\Users\Robin\Documents\Playground\paris_direction_engine\run\run_discovery_full.bat` (manual refresh only if needed)
3. `C:\Users\Robin\Documents\Playground\paris_direction_engine\run\run_discovery_quick.bat` (legacy/dev only)

Daily use:
Open Job Terminal → Refresh → review ranked jobs → apply/save/reject/maybe.

## Legacy/Secondary

- `C:\Users\Robin\Documents\Playground\paris_direction_engine\run\legacy_review_dashboard.bat` (legacy review UI)

## Debug Audit

To log rejected jobs with explicit reasons:

`python scripts\run_discovery.py --audit-rejects`

Audit output:

`C:\Users\Robin\Documents\Playground\paris_direction_engine\data\audit\rejects_<run_id>.jsonl`

## Active Resume

The active resume path is defined in:

`C:\Users\Robin\Documents\Playground\paris_direction_engine\config.json` -> `paths.resume_pdf`

Current value:

`C:\Users\Robin\Documents\Playground\paris_direction_engine\assets\Robin Resume.pdf`

## Main Files

- `C:\Users\Robin\Documents\Playground\paris_direction_engine\config.json`
- `C:\Users\Robin\Documents\Playground\paris_direction_engine\engine\scoring\brain.py`
- `C:\Users\Robin\Documents\Playground\paris_direction_engine\engine\scoring\targeting.py`
- `C:\Users\Robin\Documents\Playground\paris_direction_engine\engine\report.py`
- `C:\Users\Robin\Documents\Playground\paris_direction_engine\engine\state_server.py`
- `C:\Users\Robin\Documents\Playground\paris_direction_engine\scripts\run_discovery.py`
- `C:\Users\Robin\Documents\Playground\paris_direction_engine\scripts\discovery_sources.py`
- `C:\Users\Robin\Documents\Playground\paris_direction_engine\docs\direction_architecture.md`
