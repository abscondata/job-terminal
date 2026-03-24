from __future__ import annotations
from engine.http import fetch_json

def fetch_jobs(company: str) -> list[dict]:
    url = f"https://api.lever.co/v0/postings/{company}?mode=json"
    data = fetch_json(url)
    return data if isinstance(data, list) else []
