import os
import unittest

from unify import AsyncMultiUnify, MultiUnify


class TestMultiUnify(unittest.TestCase):
    def setUp(self) -> None:
        self.valid_api_key = os.environ.get("UNIFY_KEY")

    def test_constructor(self) -> None:
        MultiUnify(
            api_key=self.valid_api_key,
            endpoints=["llama-3-8b-chat@together-ai", "gpt-4o@openai"],
        )

    def test_add_endpoints(self):
        endpoints = ("llama-3-8b-chat@together-ai", "gpt-4o@openai")
        client = MultiUnify(api_key=self.valid_api_key, endpoints=endpoints)
        self.assertEqual(client.endpoints, endpoints)
        self.assertEqual(tuple(client.clients.keys()), endpoints)
        client.add_endpoints("claude-3.5-sonnet@anthropic")
        endpoints = (
            "llama-3-8b-chat@together-ai",
            "gpt-4o@openai",
            "claude-3.5-sonnet@anthropic",
        )
        self.assertEqual(client.endpoints, endpoints)
        self.assertEqual(tuple(client.clients.keys()), endpoints)
        client.add_endpoints("claude-3.5-sonnet@anthropic")
        self.assertEqual(client.endpoints, endpoints)
        self.assertEqual(tuple(client.clients.keys()), endpoints)
        with self.assertRaises(Exception):
            client.add_endpoints("claude-3.5-sonnet@anthropic", ignore_duplicates=False)

    def test_remove_endpoints(self):
        endpoints = (
            "llama-3-8b-chat@together-ai",
            "gpt-4o@openai",
            "claude-3.5-sonnet@anthropic",
        )
        client = MultiUnify(api_key=self.valid_api_key, endpoints=endpoints)
        self.assertEqual(client.endpoints, endpoints)
        self.assertEqual(tuple(client.clients.keys()), endpoints)
        client.remove_endpoints("claude-3.5-sonnet@anthropic")
        endpoints = ("llama-3-8b-chat@together-ai", "gpt-4o@openai")
        self.assertEqual(client.endpoints, endpoints)
        self.assertEqual(tuple(client.clients.keys()), endpoints)
        client.remove_endpoints("claude-3.5-sonnet@anthropic")
        self.assertEqual(client.endpoints, endpoints)
        self.assertEqual(tuple(client.clients.keys()), endpoints)
        with self.assertRaises(Exception):
            client.remove_endpoints("claude-3.5-sonnet@anthropic", ignore_missing=False)

    def test_generate(self):
        endpoints = (
            "llama-3-8b-chat@together-ai",
            "gpt-4o@openai",
            "claude-3.5-sonnet@anthropic",
        )
        client = MultiUnify(api_key=self.valid_api_key, endpoints=endpoints)
        responses = client.generate("Hello, how it is going?")
        for endpoint, (response_endpoint, response) in zip(
            endpoints,
            responses.items(),
        ):
            self.assertEqual(endpoint, response_endpoint)
            self.assertIsInstance(response, str)
            self.assertGreater(len(response), 0)

    def test_default_prompt_handled_correctly(self):
        endpoints = ("gpt-4o@openai", "gpt-4@openai")
        client = MultiUnify(
            api_key=self.valid_api_key,
            endpoints=endpoints,
            n=2,
            return_full_completion=True,
        )
        responses = client.generate("Hello, how it is going?")
        for endpoint, (response_endpoint, response) in zip(
            endpoints,
            responses.items(),
        ):
            self.assertEqual(endpoint, response_endpoint)
            self.assertEqual(len(response.choices), 2)

    def test_multi_message_histories(self):
        endpoints = ("llama-3-8b-chat@together-ai", "gpt-4o@openai")
        messages = {
            "llama-3-8b-chat@together-ai": [
                {"role": "assistant", "content": "Let's talk about cats"},
            ],
            "gpt-4o@openai": [
                {"role": "assistant", "content": "Let's talk about dogs"},
            ],
        }
        animals = {"llama-3-8b-chat@together-ai": "cat", "gpt-4o@openai": "dog"}
        client = MultiUnify(
            api_key=self.valid_api_key,
            endpoints=endpoints,
            messages=messages,
        )
        responses = client.generate("What animal did you want to talk about?")
        for endpoint, (response_endpoint, response) in zip(
            endpoints,
            responses.items(),
        ):
            self.assertEqual(endpoint, response_endpoint)
            self.assertIsInstance(response, str)
            self.assertGreater(len(response), 0)
            self.assertIn(animals[endpoint], response.lower())

    def test_setter_chaining(self):
        endpoints = (
            "llama-3-8b-chat@together-ai",
            "gpt-4o@openai",
            "claude-3.5-sonnet@anthropic",
        )
        client = MultiUnify(endpoints=endpoints)
        client.add_endpoints(["gpt-4@openai", "gpt-4-turbo@openai"]).remove_endpoints(
            "claude-3.5-sonnet@anthropic",
        )
        assert set(client.endpoints) == {
            "llama-3-8b-chat@together-ai",
            "gpt-4o@openai",
            "gpt-4@openai",
            "gpt-4-turbo@openai",
        }


class TestAsyncMultiUnify(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.valid_api_key = os.environ.get("UNIFY_KEY")

    async def test_async_generate(self):
        endpoints = (
            "llama-3-8b-chat@together-ai",
            "gpt-4o@openai",
            "claude-3.5-sonnet@anthropic",
        )
        client = AsyncMultiUnify(api_key=self.valid_api_key, endpoints=endpoints)
        responses = await client.generate("Hello, how it is going?")
        for endpoint, (response_endpoint, response) in zip(
            endpoints,
            responses.items(),
        ):
            self.assertEqual(endpoint, response_endpoint)
            self.assertIsInstance(response, str)
            self.assertGreater(len(response), 0)

    async def test_default_prompt_handled_correctly(self):
        endpoints = ("gpt-4o@openai", "gpt-4@openai")
        client = AsyncMultiUnify(
            api_key=self.valid_api_key,
            endpoints=endpoints,
            n=2,
            return_full_completion=True,
        )
        responses = await client.generate("Hello, how it is going?")
        for endpoint, (response_endpoint, response) in zip(
            endpoints,
            responses.items(),
        ):
            self.assertEqual(endpoint, response_endpoint)
            self.assertEqual(len(response.choices), 2)
