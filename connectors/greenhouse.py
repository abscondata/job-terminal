from __future__ import annotations
from engine.http import fetch_json

def fetch_jobs(board: str) -> list[dict]:
    url = f"https://boards-api.greenhouse.io/v1/boards/{board}/jobs?content=true"
    data = fetch_json(url)
    return data.get("jobs", [])
