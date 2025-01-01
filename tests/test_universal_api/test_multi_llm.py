import pytest

from unify import AsyncMultiUnify, MultiUnify


class TestMultiUnify:
    def test_constructor(self) -> None:
        MultiUnify(
            endpoints=["llama-3-8b-chat@together-ai", "gpt-4o@openai"],
            cache=True,
        )

    def test_add_endpoints(self):
        endpoints = ("llama-3-8b-chat@together-ai", "gpt-4o@openai")
        client = MultiUnify(endpoints=endpoints, cache=True)
        assert client.endpoints == endpoints
        assert tuple(client.clients.keys()) == endpoints
        client.add_endpoints("claude-3.5-sonnet@anthropic")
        endpoints = (
            "llama-3-8b-chat@together-ai",
            "gpt-4o@openai",
            "claude-3.5-sonnet@anthropic",
        )
        assert client.endpoints == endpoints
        assert tuple(client.clients.keys()) == endpoints
        client.add_endpoints("claude-3.5-sonnet@anthropic")
        assert client.endpoints == endpoints
        assert tuple(client.clients.keys()) == endpoints
        with pytest.raises(Exception):
            client.add_endpoints("claude-3.5-sonnet@anthropic", ignore_duplicates=False)

    def test_remove_endpoints(self):
        endpoints = (
            "llama-3-8b-chat@together-ai",
            "gpt-4o@openai",
            "claude-3.5-sonnet@anthropic",
        )
        client = MultiUnify(endpoints=endpoints, cache=True)
        assert client.endpoints == endpoints
        assert tuple(client.clients.keys()) == endpoints
        client.remove_endpoints("claude-3.5-sonnet@anthropic")
        endpoints = ("llama-3-8b-chat@together-ai", "gpt-4o@openai")
        assert client.endpoints == endpoints
        assert tuple(client.clients.keys()) == endpoints
        client.remove_endpoints("claude-3.5-sonnet@anthropic")
        assert client.endpoints == endpoints
        assert tuple(client.clients.keys()) == endpoints
        with pytest.raises(Exception):
            client.remove_endpoints("claude-3.5-sonnet@anthropic", ignore_missing=False)

    def test_generate(self):
        endpoints = (
            "llama-3-8b-chat@together-ai",
            "gpt-4o@openai",
            "claude-3.5-sonnet@anthropic",
        )
        client = MultiUnify(endpoints=endpoints, cache=True)
        responses = client.generate("Hello, how it is going?")
        for endpoint, (response_endpoint, response) in zip(
            endpoints,
            responses.items(),
        ):
            assert endpoint == response_endpoint
            assert isinstance(response, str)
            assert len(response) > 0

    def test_default_prompt_handled_correctly(self):
        endpoints = ("gpt-4o@openai", "gpt-4@openai")
        client = MultiUnify(
            endpoints=endpoints,
            n=2,
            return_full_completion=True,
            cache=True,
        )
        responses = client.generate("Hello, how it is going?")
        for endpoint, (response_endpoint, response) in zip(
            endpoints,
            responses.items(),
        ):
            assert endpoint == response_endpoint
            assert len(response.choices) == 2

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
            endpoints=endpoints,
            messages=messages,
            cache=True,
        )
        responses = client.generate("What animal did you want to talk about?")
        for endpoint, (response_endpoint, response) in zip(
            endpoints,
            responses.items(),
        ):
            assert endpoint == response_endpoint
            assert isinstance(response, str)
            assert len(response) > 0
            assert animals[endpoint] in response.lower()

    def test_setter_chaining(self):
        endpoints = (
            "llama-3-8b-chat@together-ai",
            "gpt-4o@openai",
            "claude-3.5-sonnet@anthropic",
        )
        client = MultiUnify(endpoints=endpoints, cache=True)
        client.add_endpoints(["gpt-4@openai", "gpt-4-turbo@openai"]).remove_endpoints(
            "claude-3.5-sonnet@anthropic",
        )
        assert set(client.endpoints) == {
            "llama-3-8b-chat@together-ai",
            "gpt-4o@openai",
            "gpt-4@openai",
            "gpt-4-turbo@openai",
        }


@pytest.mark.asyncio
class TestAsyncMultiUnify:
    async def test_async_generate(self):
        endpoints = (
            "llama-3-8b-chat@together-ai",
            "gpt-4o@openai",
            "claude-3.5-sonnet@anthropic",
        )
        client = AsyncMultiUnify(endpoints=endpoints, cache=True)
        responses = await client.generate("Hello, how it is going?")
        for endpoint, (response_endpoint, response) in zip(
            endpoints,
            responses.items(),
        ):
            assert endpoint == response_endpoint
            assert isinstance(response, str)
            assert len(response) > 0

    async def test_default_prompt_handled_correctly(self):
        endpoints = ("gpt-4o@openai", "gpt-4@openai")
        client = AsyncMultiUnify(
            endpoints=endpoints,
            n=2,
            return_full_completion=True,
            cache=True,
        )
        responses = await client.generate("Hello, how it is going?")
        for endpoint, (response_endpoint, response) in zip(
            endpoints,
            responses.items(),
        ):
            assert endpoint == response_endpoint
            assert len(response.choices) == 2


if __name__ == "__main__":
    pass
