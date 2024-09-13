import unittest

import unify
from unify.utils import create_evaluator, trigger_evaluation, get_evaluations, delete_evaluator, delete_evaluations
from unify.utils.datasets import delete_dataset


class TestTriggerEvaluation(unittest.TestCase):
    def setUp(self):
        self.evaluator_config = {
            "name": "test_trigger_evaluator",
            "judge_models": "llama-3-8b-chat@aws-bedrock",
            "client_side": False,
        }
        try:
            create_evaluator(self.evaluator_config)
        except:
            pass

        entries = [
            {
                "prompt": {
                    "messages": [
                        {"role": "user", "content": "What is the capital of Spain?"}
                    ]
                },
                "ref_answer": "Madrid",
            },
            {
                "prompt": {
                    "messages": [
                        {
                            "role": "user",
                            "content": "What is the square root of 1009 to 1 decimal place",
                        }
                    ]
                },
                "ref_answer": "31.8",
            },
        ]
        try:
            unify.upload_dataset_from_dictionary("TestTrigger", entries)
        except:
            pass

    def test_trigger_evaluation(self):
        response = trigger_evaluation(
            evaluator="test_trigger_evaluator",
            dataset="TestTrigger",
            endpoint="llama-3-8b-chat@aws-bedrock",
        )
        self.assertIn("info", response)
        self.assertEqual(
            response["info"],
            "Dataset evaluation started! You will receive an email soon!",
        )

    def tearDown(self):
        try:
            delete_evaluations(dataset="TestTrigger")
            delete_evaluator("test_trigger_evaluator")
            delete_dataset("TestTrigger")
        except:
            pass
