import unittest

from parallelx.loader import WorkflowValidationError, parse_workflow


class TestLoader(unittest.TestCase):
    def test_cycle_detected(self) -> None:
        wf = {
            "name": "x",
            "tasks": [
                {"id": "a", "func": "parallelx.tasks:gen_numbers", "deps": ["b"], "args": {"n": 1}},
                {"id": "b", "func": "parallelx.tasks:gen_numbers", "deps": ["a"], "args": {"n": 1}},
            ],
        }
        with self.assertRaises(WorkflowValidationError):
            parse_workflow(wf)

    def test_missing_dep(self) -> None:
        wf = {
            "tasks": [{"id": "a", "func": "parallelx.tasks:gen_numbers", "deps": ["nope"], "args": {"n": 1}}]
        }
        with self.assertRaises(WorkflowValidationError):
            parse_workflow(wf)

    def test_negative_retry_values_rejected(self) -> None:
        wf = {
            "tasks": [
                {
                    "id": "a",
                    "func": "parallelx.tasks:gen_numbers",
                    "retries": -1,
                    "args": {"n": 1},
                }
            ]
        }
        with self.assertRaises(WorkflowValidationError):
            parse_workflow(wf)

    def test_invalid_numeric_fields_rejected(self) -> None:
        wf = {
            "tasks": [
                {
                    "id": "a",
                    "func": "parallelx.tasks:gen_numbers",
                    "retry_backoff_seconds": "oops",
                    "args": {"n": 1},
                }
            ]
        }
        with self.assertRaises(WorkflowValidationError):
            parse_workflow(wf)