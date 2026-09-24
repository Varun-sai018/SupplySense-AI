import unittest
import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))
import importlib
evaluator_module = importlib.import_module('services.dependency-engine.evaluator')
evaluate_condition = evaluator_module.evaluate_condition

class TestDependencyEvaluator(unittest.TestCase):
    # ALL Condition
    def test_obj1_001_all_all_ready(self):
        decision, _ = evaluate_condition("ALL", ready_count=3, total_required=3)
        self.assertEqual(decision, "TRIGGER")
        
    def test_obj1_002_all_one_missing(self):
        decision, _ = evaluate_condition("ALL", ready_count=2, total_required=3)
        self.assertEqual(decision, "BLOCK")

    # ANY Condition
    def test_obj1_003_any_none_ready(self):
        decision, _ = evaluate_condition("ANY", ready_count=0, total_required=3)
        self.assertEqual(decision, "BLOCK")

    def test_obj1_004_any_one_ready(self):
        decision, _ = evaluate_condition("ANY", ready_count=1, total_required=3)
        self.assertEqual(decision, "TRIGGER")

    # QUORUM Condition
    def test_obj1_005_quorum_insufficient(self):
        decision, _ = evaluate_condition("QUORUM", ready_count=1, total_required=5, required_count=2)
        self.assertEqual(decision, "BLOCK")
        
    def test_obj1_006_quorum_exactly_satisfied(self):
        decision, _ = evaluate_condition("QUORUM", ready_count=2, total_required=5, required_count=2)
        self.assertEqual(decision, "TRIGGER")

    def test_obj1_007_quorum_above_threshold(self):
        decision, _ = evaluate_condition("QUORUM", ready_count=4, total_required=5, required_count=2)
        self.assertEqual(decision, "TRIGGER")

    # Invalid Condition Handling
    def test_unsupported_condition(self):
        decision, _ = evaluate_condition("INVALID", ready_count=1, total_required=1)
        self.assertEqual(decision, "BLOCK")

if __name__ == '__main__':
    unittest.main()
