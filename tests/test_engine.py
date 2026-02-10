import unittest
from tempfile import TemporaryDirectory

from parallelx.engine import Engine, EngineConfig
from parallelx.loader import parse_workflow


class TestEngine(unittest.TestCase):
    def test_simple_run(self) -> None:
        wf = parse_workflow({
            "name": "sum",
            "tasks": [
                {"id": "a", "func": "parallelx.tasks:gen_numbers", "args": {"n": 10000, "seed": 1}},
                {"id": "b", "func": "parallelx.tasks:gen_numbers", "args": {"n": 10000, "seed": 2}},
                {"id": "sa", "func": "parallelx.tasks:sum_numbers", "deps": ["a"], "args": {"nums": {"ref": "a"}}},
                {"id": "sb", "func": "parallelx.tasks:sum_numbers", "deps": ["b"], "args": {"nums": {"ref": "b"}}},
                {"id": "t", "func": "parallelx.tasks:sum_numbers", "deps": ["sa","sb"], "args": {"nums": [{"ref":"sa"},{"ref":"sb"}]}},
            ]
        })
        with TemporaryDirectory() as d:
            cfg = EngineConfig(max_workers=2, executor="process", cache_dir=d, emit_logs=False)
            outcomes, summary = Engine(cfg).run(wf)
            self.assertEqual(outcomes["t"].status.value, "SUCCESS")
            self.assertGreater(float(outcomes["t"].value), 0.0)
            # cache should have at least some writes (misses)
            self.assertGreaterEqual(summary.cache_misses, 1)

    def test_retry(self) -> None:
        wf = parse_workflow({
            "name": "retry",
            "tasks": [
                {"id": "x", "func": "parallelx.tests_support:flaky_once", "args": {}, "retries": 1, "retry_backoff_seconds": 0.0}
            ]
        })
        cfg = EngineConfig(max_workers=1, executor="thread", emit_logs=False)
        outcomes, _ = Engine(cfg).run(wf)
        self.assertEqual(outcomes["x"].status.value, "SUCCESS")
        self.assertEqual(outcomes["x"].attempts, 2)
