import importlib.util
import json
import re
import tempfile
import unittest
from pathlib import Path


MODULE_PATH = Path(__file__).with_name("visualize-replay.py")
SPEC = importlib.util.spec_from_file_location("visualize_replay", MODULE_PATH)
visualize_replay = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(visualize_replay)


class VisualizeReplayVerdictTests(unittest.TestCase):
    def test_html_verdict_uses_scorecard_reason_without_flagging_skipped_requests(self):
        """HTML verdict follows the scorecard while skipped requests stay out of divergence filtering."""
        reason = "scorecard accepted skipped correlation"
        with tempfile.TemporaryDirectory() as tmp:
            state = self._write_state(
                Path(tmp),
                scorecard_pass=True,
                scorecard_reason=reason,
                include_mismatch=True,
            )

            data = visualize_replay.build(str(state))
            html_path = visualize_replay.write_html(str(state), data)
            html = Path(html_path).read_text(encoding="utf-8")

        embedded_data = self._embedded_data(html)
        requests_by_cid = {request["cid"]: request for request in embedded_data["requests"]}
        skipped = requests_by_cid["corr-b"]
        mismatch = requests_by_cid["corr-c"]

        self.assertEqual(embedded_data["overall_reason"], reason)
        self.assertIn(reason, html)
        self.assertIn(
            "D.overall_reason?' — '+D.overall_reason:' — byte-exact self-replay",
            html,
        )
        self.assertIsNone(skipped["sc"])
        self.assertFalse(skipped["matched"])
        self.assertEqual(self._html_data_div(skipped), 0)
        self.assertEqual(mismatch["sc"], 503)
        self.assertFalse(mismatch["matched"])
        self.assertEqual(self._html_data_div(mismatch), 1)
        self.assertIn('data-div="${r.sc!=null && !r.matched?1:0}"', html)

    def test_missing_scorecard_preserves_legacy_all_requests_matched_failure(self):
        """Old state dirs without scorecards still fail when any recorded request lacks an HTTP diff."""
        with tempfile.TemporaryDirectory() as tmp:
            state = self._write_state(Path(tmp), scorecard_pass=None)

            data = visualize_replay.build(str(state))

        self.assertEqual(data["run"], "run-scorecard")
        self.assertFalse(data["overall_pass"])
        self.assertEqual(
            [request["matched"] for request in data["requests"]],
            [True, False],
            "legacy fallback should still require every request to have a clean HTTP diff",
        )

    def test_explicit_run_id_builds_and_writes_each_run_without_latest_file_bleed(self):
        """Explicit run ids select their own scorecard and HTML output path."""
        passing_run = "run-a-pass"
        failing_run = "run-z-fail"
        with tempfile.TemporaryDirectory() as tmp:
            state = Path(tmp)
            (state / "recordings" / "rec-1").mkdir(parents=True)
            (state / "observed").mkdir()
            (state / "http-diffs").mkdir()
            (state / "runs").mkdir()

            recorded_events = [
                self._incoming_event("corr-a", 1, "/matched"),
                self._side_effect_event("corr-a", 2, "find_matched"),
            ]
            observations = [self._resolved_observation("corr-a", 2, "find_matched")]
            http_diffs = [
                {
                    "correlation_id": "corr-a",
                    "request_path": "/matched",
                    "status_baseline": 200,
                    "status_candidate": 200,
                    "body_diff": [],
                }
            ]
            self._write_jsonl(state / "recordings" / "rec-1" / "events.jsonl", recorded_events)
            for run_id, scorecard_pass in ((passing_run, True), (failing_run, False)):
                self._write_jsonl(state / "observed" / f"{run_id}.jsonl", observations)
                self._write_jsonl(state / "http-diffs" / f"{run_id}.jsonl", http_diffs)
                (state / "runs" / f"{run_id}.scorecard.json").write_text(
                    json.dumps({"verdict": {"pass": scorecard_pass}}),
                    encoding="utf-8",
                )

            passing_data = visualize_replay.build(str(state), passing_run)
            failing_data = visualize_replay.build(str(state), failing_run)
            passing_html_path = Path(visualize_replay.write_html(str(state), passing_data, passing_run))
            failing_html_path = Path(visualize_replay.write_html(str(state), failing_data, failing_run))
            passing_html_exists = passing_html_path.exists()
            failing_html_exists = failing_html_path.exists()

        self.assertTrue(passing_data["overall_pass"])
        self.assertFalse(failing_data["overall_pass"])
        self.assertNotEqual(passing_data["overall_pass"], failing_data["overall_pass"])
        self.assertEqual(passing_html_path.name, f"replay-visualization-{passing_run}.html")
        self.assertEqual(failing_html_path.name, f"replay-visualization-{failing_run}.html")
        self.assertNotEqual(passing_html_path, failing_html_path)
        self.assertTrue(passing_html_exists)
        self.assertTrue(failing_html_exists)


    def _write_state(self, state, scorecard_pass, scorecard_reason=None, include_mismatch=False):
        (state / "recordings" / "rec-1").mkdir(parents=True)
        (state / "observed").mkdir()
        (state / "http-diffs").mkdir()
        (state / "runs").mkdir()

        recorded_events = [
            self._incoming_event("corr-a", 1, "/matched"),
            self._side_effect_event("corr-a", 2, "find_matched"),
            self._incoming_event("corr-b", 3, "/not-driven"),
            self._side_effect_event("corr-b", 4, "find_not_driven"),
        ]
        observations = [
            self._resolved_observation("corr-a", 2, "find_matched"),
            self._resolved_observation("corr-b", 4, "find_not_driven"),
        ]
        http_diffs = [
            {
                "correlation_id": "corr-a",
                "request_path": "/matched",
                "status_baseline": 200,
                "status_candidate": 200,
                "body_diff": [],
            }
        ]
        if include_mismatch:
            recorded_events.extend(
                [
                    self._incoming_event("corr-c", 5, "/mismatch"),
                    self._side_effect_event("corr-c", 6, "find_mismatch"),
                ]
            )
            observations.append(self._resolved_observation("corr-c", 6, "find_mismatch"))
            http_diffs.append(
                {
                    "correlation_id": "corr-c",
                    "request_path": "/mismatch",
                    "status_baseline": 200,
                    "status_candidate": 503,
                    "body_diff": [
                        {
                            "json_path": "$.status",
                            "baseline": "ok",
                            "candidate": "down",
                        }
                    ],
                }
            )
        self._write_jsonl(state / "recordings" / "rec-1" / "events.jsonl", recorded_events)
        self._write_jsonl(state / "observed" / "run-scorecard.jsonl", observations)
        self._write_jsonl(state / "http-diffs" / "run-scorecard.jsonl", http_diffs)
        if scorecard_pass is not None:
            verdict = {"pass": scorecard_pass}
            if scorecard_reason is not None:
                verdict["reason"] = scorecard_reason
            (state / "runs" / "run-scorecard.scorecard.json").write_text(
                json.dumps({"verdict": verdict}),
                encoding="utf-8",
            )
        return state

    def _incoming_event(self, correlation_id, global_sequence, path):
        return {
            "correlation_id": correlation_id,
            "global_sequence": global_sequence,
            "request_sequence": 0,
            "boundary": "http_incoming",
            "method_name": path,
            "trait_name": "HttpServer",
            "args": {"path": path},
            "result": {"status": 200},
            "call_file": "router.rs",
            "call_line": 10 + global_sequence,
        }

    def _side_effect_event(self, correlation_id, global_sequence, method_name):
        return {
            "correlation_id": correlation_id,
            "global_sequence": global_sequence,
            "request_sequence": 1,
            "boundary": "db",
            "method_name": method_name,
            "trait_name": "Store",
            "args": {"id": correlation_id},
            "result": {"ok": True},
            "call_file": "store.rs",
            "call_line": 20 + global_sequence,
        }

    def _resolved_observation(self, correlation_id, source_sequence, method_name):
        return {
            "correlation_id": correlation_id,
            "source_event_global_sequence": source_sequence,
            "resolved": True,
            "resolved_rank": 1,
            "boundary": "db",
            "method_name": method_name,
            "trait_name": "Store",
            "args": {"id": correlation_id},
        }

    def _write_jsonl(self, path, rows):
        path.write_text(
            "".join(json.dumps(row, sort_keys=True) + "\n" for row in rows),
            encoding="utf-8",
        )

    def _embedded_data(self, html):
        match = re.search(r"const D = (.*?);\nconst BCOL =", html, re.S)
        self.assertIsNotNone(match, "write_html should embed replay data for the client renderer")
        return json.loads(match.group(1))

    def _html_data_div(self, request):
        return 1 if request["sc"] is not None and not request["matched"] else 0


if __name__ == "__main__":
    unittest.main()
