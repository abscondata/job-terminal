# JobEngine Direction Architecture

## Strategic Shift

JobEngine is no longer a score-led bridge-role sorter.

It is now a lane-based review system for one life move:

- find realistic Paris-direction roles
- preserve NYC as the strongest realism / platform comparator
- keep Miami visible only as a tertiary option
- separate direction leaps from money/platform leaps
- surface top-brand wrong-function risks instead of letting them disappear into the same bucket as generic slop

The user-facing product should answer:

`What path does this role create, and is it worth leaving for?`

## True Active Path

1. `C:\Users\Robin\Documents\Playground\paris_direction_engine\scripts\run_discovery.py` pulls targeted sources and stores raw/canonical jobs.
2. `C:\Users\Robin\Documents\Playground\paris_direction_engine\engine\scoring\brain.py` classifies each role into city lanes, path lanes, world tiers, function families, and review guidance.
3. `C:\Users\Robin\Documents\Playground\paris_direction_engine\engine\report.py` presents a filter-first review UI for discovery, manual triage, and saved review.
4. `C:\Users\Robin\Documents\Playground\paris_direction_engine\engine\state_server.py` powers `/triage/evaluate` plus `/saved/*`.

That is the product.

## What Was Rebuilt

### Classification

The active scorer now centers on:

- city lane
- path lane
- world tier
- function family
- work type
- French burden
- bridge or slop verdict
- path logic
- main risk

Internal signals still exist for ordering and routing, but the visible product no longer leads with hard numeric authority.

### City Logic

City priority is now:

1. Paris
2. NYC
3. Miami

Paris and NYC are preserved as different lanes instead of being merged into one generic geography score.

### Path Lanes

The active path labels are:

- Paris Direction
- NYC Direction
- Money / Platform Leap
- Strategic Internship / Traineeship
- Top-Brand Wrong-Function Risk
- Interesting Stretch
- French-Heavy Stretch
- Miami Option
- Low-Value Slop Risk

The primary classification is a quick read, not a claim that a single rigid ranking tells the whole story.

### Review UX

Discovery and saved review now filter by:

- decision
- city lane
- path lane
- world tier
- function family
- work type
- French burden
- bridge/slop verdict

Manual triage now returns explanation-first fields instead of visible score columns.

## Discovery Scope

The default automated source stack remains:

- Welcome to the Jungle
- Kering official careers
- Chanel official careers
- Sotheby's careers
- Christie's careers
- Centre Pompidou / public-sector handoff
- LinkedIn
- Indeed

LinkedIn and Indeed now search across Paris, NYC, and Miami with targeted role queries.

## Active Resume

The active resume path is set in:

`C:\Users\Robin\Documents\Playground\paris_direction_engine\config.json` -> `paths.resume_pdf`

Current value:

`C:\Users\Robin\Documents\Playground\paris_direction_engine\assets\Robin Resume.pdf`

## Reusable Abstractions Preserved

These remain active and intentionally reusable:

- `C:\Users\Robin\Documents\Playground\paris_direction_engine\engine\config.py`
- `C:\Users\Robin\Documents\Playground\paris_direction_engine\engine\db.py`
- `C:\Users\Robin\Documents\Playground\paris_direction_engine\engine\models.py`
- `C:\Users\Robin\Documents\Playground\paris_direction_engine\engine\compensation.py`
- `C:\Users\Robin\Documents\Playground\paris_direction_engine\engine\state_server.py`
- `C:\Users\Robin\Documents\Playground\paris_direction_engine\scripts\discovery_sources.py`

## Physically Removed Earlier In The Cleanup Pass

Legacy apply-first and finance-only scripts/modules were physically removed in the previous cleanup pass. The active path no longer depends on them.
