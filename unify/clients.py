import os
from typing import AsyncGenerator, Generator, List, Optional, Union

import openai

from unify.Exceptions import UnifyError, status_error_map


def validate_api_key(api_key):
    if api_key is None:
        api_key = os.environ.get("UNIFY_KEY")
    if api_key is None:
        raise KeyError(
            "UNIFY_KEY is missing. Please make sure it is set correctly!",
        )
    return api_key


class Unify:
    """Class for interacting with the Unify API."""

    def __init__(
        self,
        api_key: Optional[str] = None,
    ) -> None:
        """Initialize the Unify client.

        Args:
            api_key (str, optional): API key for accessing the Unify API.
            If None, it attempts to retrieve the API key from the
            environment variable UNIFY_KEY.
            Defaults to None.

        Raises:
            UnifyError: If the API key is missing.
        """
        self.api_key = validate_api_key(api_key)
        try:
            self.client = openai.OpenAI(
                base_url="https://api.unify.ai/v0/",
                api_key=self.api_key,
            )
        except openai.OpenAIError as e:
            raise UnifyError(f"Failed to initialize Unify client: {str(e)}")

    def generate(
        self,
        messages: Union[str, List[dict]],
        model: str = "llama-2-13b-chat",
        provider: str = "anyscale",
        stream: bool = False,
    ) -> Union[Generator[str, None, None], str]:
        """Generate content using the Unify API.

        Args:
            messages (Union[str, List[dict]]): A single prompt as a string or a
              dictionary containing the conversation history.
            model (str): The name of the model. Defaults to "llama-2-13b-chat".
            provider (str): The provider of the model. Defaults to "anyscale".
            stream (bool): If True, generates content as a stream.
              If False, generates content as a single response.
              Defaults to False.

        Returns:
            Union[Generator[str, None, None], str]: If stream is True,
             returns a generator yielding chunks of content.
             If stream is False, returns a single string response.

        Raises:
            UnifyError: If an error occurs during content generation.
        """
        if isinstance(messages, str):
            contents = [{"role": "user", "content": messages}]
        else:
            contents = messages

        if stream:
            return self._generate_stream(contents, model, provider)
        else:
            return self._generate_non_stream(contents, model, provider)

    def _generate_stream(
        self,
        messages,
        model,
        provider,
    ) -> Generator[str, None, None]:
        try:
            chat_completion = self.client.chat.completions.create(
                model="@".join([model, provider]),
                messages=messages,
                stream=True,
            )
            for chunk in chat_completion:
                if chunk.choices[0].delta.content is not None:
                    yield chunk.choices[0].delta.content
        except openai.APIStatusError as e:
            raise status_error_map[e.status_code](e.message) from None

    def _generate_non_stream(
        self,
        messages,
        model,
        provider,
    ) -> str:
        try:
            chat_completion = self.client.chat.completions.create(
                model="@".join([model, provider]),
                messages=messages,
                stream=False,
            )
            return chat_completion.choices[0].message.content.strip(" ")  # type: ignore # noqa: E501
        except openai.APIStatusError as e:
            raise status_error_map[e.status_code](e.message) from None


class AsyncUnify:
    """Class for interacting asynchronously with the Unify API."""

    def __init__(
        self,
        api_key: Optional[str] = None,
    ) -> None:
        """Initialize the AsyncUnify client.

        Args:
            api_key (str, optional): API key for accessing the Unify API.
            If None, it attempts to retrieve the API key from
            the environment variable UNIFY_KEY.
            Defaults to None.

        Raises:
            UnifyError: If the API key is missing.
        """
        self.api_key = validate_api_key(api_key)
        try:
            self.client = openai.AsyncOpenAI(
                base_url="https://api.unify.ai/v0/",
                api_key=self.api_key,
            )
        except openai.APIStatusError as e:
            raise UnifyError(f"Failed to initialize Unify client: {str(e)}")

    async def generate(
        self,
        messages: Union[str, List[dict]],
        model: str = "llama-2-13b-chat",
        provider: str = "anyscale",
        stream: bool = False,
    ) -> Union[AsyncGenerator[str, None], str]:
        """Generate content asynchronously using the Unify API.

        Args:
            messages (Union[str, List[dict]]): A single prompt as a string
              or a dictionary containing the conversation history.
            model (str): The name of the model.
            provider (str): The provider of the model.
            stream (bool): If True, generates content as a stream.
            If False, generates content as a single response.
             Defaults to False.

        Returns:
            Union[AsyncGenerator[str, None], List[str]]: If stream is True,
            returns an asynchronous generator yielding chunks of content.
              If stream is False, returns a list of string responses.

        Raises:
            UnifyError: If an error occurs during content generation.
        """
        if isinstance(messages, str):
            contents = [{"role": "user", "content": messages}]
        else:
            contents = messages

        if stream:
            return self._generate_stream(contents, model, provider)
        else:
            return await self._generate_non_stream(contents, model, provider)

    async def _generate_stream(
        self,
        messages,
        model,
        provider,
    ) -> AsyncGenerator[str, None]:
        try:
            async with self.client as async_client:
                async_stream = await async_client.chat.completions.create(
                    model="@".join([model, provider]),
                    messages=messages,
                    stream=True,
                )
                async for chunk in async_stream:
                    yield chunk.choices[0].delta.content or ""
        except openai.APIStatusError as e:
            raise status_error_map[e.status_code](e.message) from None

    async def _generate_non_stream(
        self,
        messages,
        model,
        provider,
    ) -> str:
        try:
            async with self.client as async_client:
                async_response = await async_client.chat.completions.create(
                    model="@".join([model, provider]),
                    messages=messages,
                    stream=False,
                )
            return async_response.choices[0].message.content.strip(" ")  # type: ignore # noqa: E501
        except openai.APIStatusError as e:
            raise status_error_map[e.status_code](e.message) from None
