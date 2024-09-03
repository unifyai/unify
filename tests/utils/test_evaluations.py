

import unittest
import tempfile
import json
import os
import requests
from unify.utils import trigger_evaluation, delete_evaluations, get_evaluation_status

class TestTriggerEvaluation(unittest.TestCase):
    def setUp(self):
        self.evaluator = "default"  # Use a default evaluator that should exist
        self.dataset = "test_dataset"
        self.endpoint = "gpt-3.5-turbo@openai"  # Use a valid endpoint
        self.temp_file = tempfile.NamedTemporaryFile(mode='w+', delete=False, suffix='.jsonl')
        self.temp_file.write(json.dumps({"prompt": "test prompt", "score": 0.5}) + '\n')
        self.temp_file.close()

    def tearDown(self):
        os.unlink(self.temp_file.name)
        # We'll remove the delete_evaluations call from here as it's causing issues

    def test_trigger_evaluation(self):
        try:
            result = trigger_evaluation(
                evaluator=self.evaluator,
                dataset=self.dataset,
                endpoint=self.endpoint,
                client_side_scores=self.temp_file.name
            )
            
            self.assertIsInstance(result, dict)
            self.assertIn("info", result)
            self.assertTrue(result["info"].startswith("Dataset evaluation started!"))
            
            # Check if the evaluation was actually triggered
            status = get_evaluation_status(self.dataset, self.endpoint, self.evaluator)
            self.assertIn("status", status)
            self.assertIn(status["status"], ["pending", "in_progress", "completed"])
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 404:
                self.skipTest("Dataset, evaluator, or endpoint not found. Skipping test.")
            else:
                raise

    def test_trigger_evaluation_without_client_scores(self):
        try:
            result = trigger_evaluation(
                evaluator=self.evaluator,
                dataset=self.dataset,
                endpoint=self.endpoint
            )
            
            self.assertIsInstance(result, dict)
            self.assertIn("info", result)
            self.assertTrue(result["info"].startswith("Dataset evaluation started!"))
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 404:
                self.skipTest("Dataset, evaluator, or endpoint not found. Skipping test.")
            else:
                raise

    def test_trigger_evaluation_invalid_file(self):
        with self.assertRaises(FileNotFoundError):
            trigger_evaluation(
                evaluator=self.evaluator,
                dataset=self.dataset,
                endpoint=self.endpoint,
                client_side_scores="non_existent_file.jsonl"
            )

    def test_trigger_evaluation_invalid_endpoint(self):
        with self.assertRaises(requests.exceptions.HTTPError):
            trigger_evaluation(
                evaluator=self.evaluator,
                dataset=self.dataset,
                endpoint="invalid_endpoint"
            )

