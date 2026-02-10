import unittest

from parallelx.loader import parse_workflow, WorkflowValidationError


class TestLoader(unittest.TestCase):
    def test_cycle_detected(self):
        wf = {
            "name": "x",
            "tasks": [
                {"id": "a", "func": "parallelx.tasks:gen_numbers", "deps": ["b"], "args": {"n": 1}},
                {"id": "b", "func": "parallelx.tasks:gen_numbers", "deps": ["a"], "args": {"n": 1}},
            ],
        }
        with self.assertRaises(WorkflowValidationError):
            parse_workflow(wf)

    def test_missing_dep(self):
        wf = {
            "tasks": [{"id": "a", "func": "parallelx.tasks:gen_numbers", "deps": ["nope"], "args": {"n": 1}}]
        }
        with self.assertRaises(WorkflowValidationError):
            parse_workflow(wf)
