from __future__ import annotations

import json
import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from scripts.run_discovery import build_source_registry


class TestDiscoveryRegistration(unittest.TestCase):
    def test_registry_is_paris_native_and_bridge_specific(self):
        names = [name for name, _fn in build_source_registry()]
        self.assertEqual(
            names,
            [
                "welcometothejungle",
                "kering",
                "chanel",
                "sothebys",
                "christies",
                "centre_pompidou",
                "profilculture",
                "linkedin",
                "indeed",
            ],
        )

    def test_config_enables_paris_native_defaults(self):
        cfg = json.loads((ROOT / "config.json").read_text(encoding="utf-8"))
        enabled = cfg.get("discovery", {}).get("enabled_sources", [])
        self.assertEqual(
            enabled,
            [
                "welcometothejungle",
                "kering",
                "chanel",
                "sothebys",
                "christies",
                "centre_pompidou",
                "profilculture",
                "linkedin",
                "indeed",
            ],
        )

    def test_config_is_paris_first(self):
        cfg = json.loads((ROOT / "config.json").read_text(encoding="utf-8"))
        self.assertEqual(cfg.get("policy", {}).get("profile", {}).get("target_market"), "paris")
        self.assertEqual(
            cfg.get("policy", {}).get("profile", {}).get("target_cities"),
            ["paris", "new york city", "miami"],
        )


if __name__ == "__main__":
    unittest.main()
