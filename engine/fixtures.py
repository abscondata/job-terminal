from __future__ import annotations
import json

def load_fixture_jobs(path: str) -> list[dict]:
    with open(path, "r", encoding="utf-8-sig") as f:
        return json.load(f)
