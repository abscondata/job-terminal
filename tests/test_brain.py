from __future__ import annotations

import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from engine.scoring import ScoringBrain
from engine.scoring.router import batch_route


def _make_cfg():
    class FakeCfg:
        policy = {"min_comp_floor": 38000}
        queues = {"REVIEW": 1, "MAYBE": 2, "REJECT": 3}
        hard_reject_phrases = ["call center", "cold calling", "commission only", "commission-based"]
        target_cities = ["Paris", "New York City", "Miami"]

    return FakeCfg()


def _make_job(**kw):
    job = {
        "company": "",
        "title": "",
        "description_text": "",
        "location_text": "",
        "remote_type": "",
        "compensation_text": "",
        "language_requirements": "",
        "compensation_min": None,
        "compensation_max": None,
        "source": "",
    }
    job.update(kw)
    return job


class TestLaneBasedScoring(unittest.TestCase):
    def setUp(self):
        self.brain = ScoringBrain(_make_cfg())

    def test_paris_luxury_comms_role_is_paris_direction(self):
        result = self.brain.score(
            _make_job(
                company="Sephora",
                title="Communication Coordinator",
                location_text="Paris, France",
                remote_type="hybrid",
                description_text=(
                    "Coordinate campaign calendars, stakeholder follow-up, event support, "
                    "and content production for a luxury beauty brand. Excel reporting and "
                    "cross-team project tracking required. International environment, English required."
                ),
            )
        )

        self.assertEqual(result.classification, "Paris Direction")
        self.assertEqual(result.recommendation, "apply")
        self.assertEqual(result.city_lane, "Paris")
        self.assertEqual(result.world_tier, "Top Luxury / Culture World")
        self.assertIn("Paris Direction", result.opportunity_lanes)
        self.assertEqual(result.slop_verdict, "Real bridge")
        self.assertTrue(result.one_line_recommendation)

    def test_gallery_operations_role_can_surface_as_paris_direction(self):
        result = self.brain.score(
            _make_job(
                company="Modern Art Gallery",
                title="Gallery Operations Assistant",
                location_text="Paris, France",
                remote_type="onsite",
                description_text=(
                    "Support exhibition logistics, documentation, scheduling, client follow-up, "
                    "and operational coordination for a Paris gallery."
                ),
            )
        )

        self.assertIn(result.classification, {"Paris Direction", "Interesting Stretch"})
        self.assertIn(result.recommendation, {"apply", "maybe"})
        self.assertIn("culture", result.world_hits)
        self.assertEqual(result.function_family, "Gallery / Cultural Coordination")

    def test_top_brand_compliance_role_is_preserved_as_wrong_function_risk(self):
        result = self.brain.score(
            _make_job(
                company="Dior",
                title="Compliance Analyst",
                location_text="Paris, France",
                remote_type="hybrid",
                description_text=(
                    "Support regulatory compliance, AML reviews, audit requests, and internal controls "
                    "for the Paris office of Dior."
                ),
            )
        )

        self.assertEqual(result.classification, "Top-Brand Wrong-Function Risk")
        self.assertEqual(result.recommendation, "maybe")
        self.assertTrue(result.top_brand_risk)
        self.assertEqual(result.slop_verdict, "Mixed case")

    def test_nyc_corporate_role_can_surface_as_platform_lane(self):
        result = self.brain.score(
            _make_job(
                company="Webull",
                title="Compliance Associate",
                location_text="New York, NY",
                remote_type="hybrid",
                description_text=(
                    "Support compliance monitoring, regulatory reviews, and surveillance for the New York office. "
                    "Salary $95,000 per year."
                ),
            )
        )

        self.assertEqual(result.classification, "Money / Platform Leap")
        self.assertEqual(result.recommendation, "maybe")
        self.assertIn("Money / Platform Leap", result.opportunity_lanes)
        self.assertEqual(result.city_lane, "NYC")

    def test_written_french_heavy_copy_role_is_french_heavy(self):
        result = self.brain.score(
            _make_job(
                company="Maison Culturelle",
                title="Content Editor",
                location_text="Paris, France",
                remote_type="onsite",
                description_text=(
                    "Native French required. Excellent written French and rédaction irréprochable "
                    "required for editorial content and copywriting."
                ),
            )
        )

        self.assertEqual(result.classification, "French-Heavy Stretch")
        self.assertEqual(result.recommendation, "skip")

    def test_senior_brand_role_is_too_senior(self):
        result = self.brain.score(
            _make_job(
                company="Chanel",
                title="Senior Brand Project Manager",
                location_text="Paris, France",
                remote_type="hybrid",
                description_text="Lead launch planning across regions. 6+ years of luxury project management required.",
            )
        )

        self.assertEqual(result.classification, "Too Senior")
        self.assertEqual(result.recommendation, "skip")

    def test_paris_direction_signal_beats_remote(self):
        paris = self.brain.score(
            _make_job(
                company="Sephora",
                title="Client Experience Coordinator",
                location_text="Paris, France",
                remote_type="hybrid",
                description_text="Luxury beauty client experience project support and reporting.",
            )
        )
        remote = self.brain.score(
            _make_job(
                company="Sephora",
                title="Client Experience Coordinator",
                location_text="Remote",
                remote_type="remote_anywhere",
                description_text="Luxury beauty client experience project support and reporting.",
            )
        )

        self.assertGreater(paris.signal_scores["direction"], remote.signal_scores["direction"])
        self.assertEqual(paris.city_lane, "Paris")
        self.assertEqual(remote.city_lane, "Remote")

    def test_batch_route_keeps_manual_review_workflow(self):
        results = [
            self.brain.score(
                _make_job(
                    company="Sephora",
                    title="Communication Coordinator",
                    location_text="Paris, France",
                    remote_type="hybrid",
                    description_text="Luxury beauty project coordination, content production, reporting, and events support.",
                )
            ),
            self.brain.score(
                _make_job(
                    company="Luxury Holding Group",
                    title="Compliance Analyst",
                    location_text="Paris, France",
                    remote_type="hybrid",
                    description_text="Regulatory compliance, AML and audit support.",
                )
            ),
        ]

        stats = batch_route(results, _make_cfg().queues)

        self.assertEqual(results[0].queue, 1)
        self.assertEqual(results[1].queue, 3)
        self.assertEqual(stats["review"], 1)
        self.assertEqual(stats["reject"], 1)


if __name__ == "__main__":
    unittest.main()
