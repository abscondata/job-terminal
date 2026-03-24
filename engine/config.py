from __future__ import annotations
from dataclasses import dataclass
import json
from pathlib import Path

@dataclass(frozen=True)
class EngineConfig:
    db_path: str
    runs_dir: str
    resume_pdf: str
    user_data_dir: str
    browser_channel: str
    browser_headless: bool
    policy: dict
    discovery: dict
    target_cities: list[str]
    hard_reject_phrases: list[str]
    queues: dict

def load_config(path: str) -> EngineConfig:
    # utf-8-sig handles files saved with BOM on Windows
    config_path = Path(path).resolve()
    base_dir = config_path.parent

    with open(config_path, "r", encoding="utf-8-sig") as f:
        raw = json.load(f)

    # Resolve relative paths against the directory containing config.json
    def _resolve(p: str) -> str:
        pp = Path(p)
        if not pp.is_absolute():
            pp = base_dir / pp
        return str(pp)

    paths = raw["paths"]
    browser = raw.get("browser", {})
    queues = dict(raw["queues"])
    if "MAYBE" not in queues and "APPROVAL" in queues:
        queues["MAYBE"] = queues["APPROVAL"]

    target_cities = raw.get("target_cities")
    if target_cities is None:
        target_cities = raw.get("relocation_places", [])

    return EngineConfig(
        db_path=_resolve(paths["db_path"]),
        runs_dir=_resolve(paths["runs_dir"]),
        resume_pdf=_resolve(paths["resume_pdf"]),
        user_data_dir=_resolve(paths["user_data_dir"]),
        browser_channel=browser.get("channel", "chrome"),
        browser_headless=bool(browser.get("headless", True)),
        policy=raw["policy"],
        discovery=raw.get("discovery", {}),
        target_cities=target_cities,
        hard_reject_phrases=raw.get("hard_reject_phrases", []),
        queues=queues,
    )
