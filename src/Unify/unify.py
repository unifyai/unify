import openai
import os
from typing import Generator, AsyncGenerator, Union, Optional, List 

class UnifyError(Exception):
    """Exception raised for errors related to Unify."""
    pass

class Unify:
    """Class for interacting with the Unify API."""
    
    def __init__(
            self,
            api_key: Optional[str] = None,
    ) -> None:
        """Initialize the Unify client.

        Args:
            api_key (str, optional): API key for accessing the Unify API. If None, it attempts to retrieve the API key from the environment variable UNIFY_API_KEY.
              Defaults to None.

        Raises:
            UnifyError: If the API key is missing.
        """
        if api_key is None:
            api_key = os.environ.get("UNIFY_API_KEY")
        if api_key is None:
            raise UnifyError("UNIFY_API_KEY is missing. Please make sure it is set correctly!")
        try:
            self.client = openai.OpenAI(base_url="https://api.unify.ai/v0/", api_key=api_key)
        except openai.OpenAIError as e:
            raise UnifyError(f"Failed to initialize Unify client: {str(e)}")

    def generate(self, role: Union[str, List[str]], content: Union[str, List[str]], model: str, provider: str, stream: bool) -> Union[Generator[str, None, None], str]:
        """Generate content using the Unify API.

        Args:
            role (Union[str, List[str]]): The role(s) for the content.
            content (Union[str, List[str]]): The content(s) to generate.
            model (str): The name of the model.
            provider (str): The provider of the model.
            stream (bool): If True, generates content as a stream. If False, generates content as a single response.

        Returns:
            Union[Generator[str, None, None], str]: If stream is True, returns a generator yielding chunks of content. If stream is False, returns a single string response.

        Raises:
            UnifyError: If an error occurs during content generation.
        """
        if isinstance(role, str):
            roles = [role]
        else:
            roles = role
        
        if isinstance(content, str):
            contents = [content]
        else:
            contents = content
            
        if len(roles) != len(contents):
            raise UnifyError("Number of roles must match number of contents.")
        
        if stream:
            return self._generate_stream(roles, contents, model, provider)
        else:
            return self._generate_non_stream(roles, contents, model, provider)

    def _generate_stream(self, roles: List[str], contents: List[str], model: str, provider: str) -> Generator[str, None, None]:
        """Generate content as a stream using the Unify API.

        Args:
            roles (List[str]): The role(s) for the content.
            contents (List[str]): The content(s) to generate.
            model (str): The name of the model.
            provider (str): The provider of the model.

        Yields:
            Generator[str, None, None]: A generator yielding chunks of generated content.

        Raises:
            UnifyError: If an error occurs during content generation.
        """
        for role, content in zip(roles, contents):
            chat_completion = self.client.chat.completions.create(
                model='@'.join([model, provider]),
                messages=[{'role': role, "content": content}],
                stream=True
            )
            for chunk in chat_completion:
                if chunk.choices[0].delta.content is not None:
                    yield chunk.choices[0].delta.content

    def _generate_non_stream(self, roles: List[str], contents: List[str], model: str, provider: str) -> str:
        """Generate content as a single response using the Unify API.

        Args:
            roles (List[str]): The role(s) for the content.
            contents (List[str]): The content(s) to generate.
            model (str): The name of the model.
            provider (str): The provider of the model.

        Returns:
            str: The generated content as a single response.

        Raises:
            UnifyError: If an error occurs during content generation.
        """
        responses = []
        for role, content in zip(roles, contents):
            chat_completion = self.client.chat.completions.create(
                model='@'.join([model, provider]),
                messages=[{'role': role, "content": content}],
                stream=False
            )
            responses.append(chat_completion.choices[0].message.content.strip(" "))
        return responses


class AsyncUnify:
    """Class for interacting asynchronously with the Unify API."""

    def __init__(
            self,
            api_key: Optional[str] = None,
    ) -> None:
        """Initialize the AsyncUnify client.

        Args:
            api_key (str, optional): API key for accessing the Unify API. If None, it attempts to retrieve the API key from the environment variable UNIFY_API_KEY.
              Defaults to None.

        Raises:
            AsyncUnifyError: If the API key is missing.
        """
        if api_key is None:
            api_key = os.environ.get("UNIFY_API_KEY")
        if api_key is None:
            raise UnifyError("UNIFY_API_KEY is missing. Please make sure it is set correctly!")
        try:
            self.client = openai.AsyncOpenAI(base_url="https://api.unify.ai/v0/", api_key=api_key)
        except openai.OpenAIError as e:
            raise UnifyError(f"Failed to initialize AsyncUnify client: {str(e)}")

    async def generate(self, roles: Union[str, List[str]], contents: Union[str, List[str]], model: str, provider: str, stream: bool) -> Union[AsyncGenerator[str, None], List[str]]:
        """Generate content asynchronously using the Unify API.

        Args:
            roles (Union[str, List[str]]): The role(s) for the content.
            contents (Union[str, List[str]]): The content(s) to generate.
            model (str): The name of the model.
            stream (bool): If True, generates content as a stream. If False, generates content as a single response.

        Returns:
            Union[AsyncGenerator[str, None], List[str]]: If stream is True, returns an asynchronous generator yielding chunks of content.
              If stream is False, returns a list of string responses.

        Raises:
            AsyncUnifyError: If an error occurs during content generation.
        """
        if isinstance(roles, str):
            roles = [roles]
        if isinstance(contents, str):
            contents = [contents]

        if len(roles) != len(contents):
            raise UnifyError("Number of roles must match number of contents.")

        if stream:
            return self._generate_stream(roles, contents, model, provider)
        else:
            return await self._generate_non_stream(roles, contents, model, provider)

    async def _generate_stream(self, roles: List[str], contents: List[str], model: str,  provider:str) -> AsyncGenerator[str, None]:
        """Generate content as a stream asynchronously using the Unify API.

        Args:
            roles (List[str]): The role(s) for the content.
            contents (List[str]): The content(s) to generate.
            model (str): The name of the model.

        Yields:
            AsyncGenerator[str, None]: An asynchronous generator yielding chunks of generated content.

        Raises:
            AsyncUnifyError: If an error occurs during content generation.
        """
        async with self.client as async_client:
            for role, content in zip(roles, contents):
                async_stream = await async_client.chat.completions.create(
                    model='@'.join([model, provider]),
                    messages=[{"role": role, "content": content}],
                    stream=True,
                )
                async for chunk in async_stream:
                    yield chunk.choices[0].delta.content or ""

    async def _generate_non_stream(self, roles: List[str], contents: List[str], model: str, provider: str) -> List[str]:
        """Generate content as a single response asynchronously using the Unify API.

        Args:
            roles (List[str]): The role(s) for the content.
            contents (List[str]): The content(s) to generate.
            model (str): The name of the model.

        Returns:
            List[str]: The generated content as a list of string responses.

        Raises:
            AsyncUnifyError: If an error occurs during content generation.
        """
        responses = []
        async with self.client as async_client:
            for role, content in zip(roles, contents):
                async_response = await async_client.chat.completions.create(
                    model='@'.join([model, provider]),
                    messages=[{"role": role, "content": content}],
                    stream=False,
                )
                responses.append(async_response.choices[0].message.content.strip(" "))
        return responses
