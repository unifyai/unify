# global
import openai
from openai._types import Headers, Query
from openai.types.chat import (
    ChatCompletionToolParam,
    ChatCompletionToolChoiceOptionParam,
    ChatCompletionMessageParam,
    ChatCompletionStreamOptionsParam,
)
from openai.types.chat.completion_create_params import ResponseFormat
import requests
from abc import ABC, abstractmethod
from typing import AsyncGenerator, Dict, Generator, List, Optional, Union, Iterable

# local
import unify.utils
from unify import base_url
from unify._caching import _get_cache, _write_to_cache
from unify.utils.helpers import _validate_api_key
from unify.exceptions import BadRequestError, UnifyError, status_error_map


class Client(ABC):
    """Base Abstract class for interacting with the Unify chat completions endpoint."""

    @abstractmethod
    def _get_client(self):
        raise NotImplementedError

    @abstractmethod
    def generate(
        self,
        user_prompt: Optional[str] = None,
        system_prompt: Optional[str] = None,
        messages: Optional[Iterable[ChatCompletionMessageParam]] = None,
        *,
        # unified arguments
        max_tokens: Optional[int] = 1024,
        stop: Union[Optional[str], List[str]] = None,
        stream: Optional[bool] = False,
        temperature: Optional[float] = 1.0,
        # partially unified arguments
        frequency_penalty: Optional[float] = None,
        logit_bias: Optional[Dict[str, int]] = None,
        logprobs: Optional[bool] = None,
        top_logprobs: Optional[int] = None,
        n: Optional[int] = None,
        presence_penalty: Optional[float] = None,
        response_format: Optional[ResponseFormat] = None,
        seed: Optional[int] = None,
        stream_options: Optional[ChatCompletionStreamOptionsParam] = None,
        top_p: Optional[float] = None,
        tools: Optional[Iterable[ChatCompletionToolParam]] = None,
        tool_choice: Optional[ChatCompletionToolChoiceOptionParam] = None,
        parallel_tool_calls: Optional[bool] = None,
        # platform arguments
        use_custom_keys: bool = False,
        tags: Optional[List[str]] = None,
        # python client arguments
        message_content_only: bool = True,
        cache: bool = False,
        # passthrough arguments
        extra_headers: Optional[Headers] = None,
        extra_query: Optional[Query] = None,
        **kwargs,
    ):
        """Generate content using the Unify API.

        Args:
            user_prompt: A string containing the user prompt.
            If provided, messages must be None.

            system_prompt: An optional string containing the system prompt.

            messages: A list of messages comprising the conversation so far.
            If provided, user_prompt must be None.

            max_tokens: The maximum number of tokens that can be generated in the chat
            completion. The total length of input tokens and generated tokens is limited
            by the model's context length. Defaults to the provider's default max_tokens
            when the value is None.

            stop: Up to 4 sequences where the API will stop generating further tokens.

            stream: If True, generates content as a stream. If False, generates content
            as a single response. Defaults to False.

            temperature:  What sampling temperature to use, between 0 and 2.
            Higher values like 0.8 will make the output more random,
            while lower values like 0.2 will make it more focused and deterministic.
            It is generally recommended to alter this or top_p, but not both.
            Defaults to the provider's default max_tokens when the value is None.

            frequency_penalty: Number between -2.0 and 2.0. Positive values penalize new
            tokens based on their existing frequency in the text so far, decreasing the
            model's likelihood to repeat the same line verbatim.

            logit_bias: Modify the likelihood of specified tokens appearing in the
            completion. Accepts a JSON object that maps tokens (specified by their token
            ID in the tokenizer) to an associated bias value from -100 to 100.
            Mathematically, the bias is added to the logits generated by the model prior
            to sampling. The exact effect will vary per model, but values between -1 and
            1 should decrease or increase likelihood of selection; values like -100 or
            100 should result in a ban or exclusive selection of the relevant token.

            logprobs: Whether to return log probabilities of the output tokens or not.
            If true, returns the log probabilities of each output token returned in the
            content of message.

            top_logprobs: An integer between 0 and 20 specifying the number of most
            likely tokens to return at each token position, each with an associated log
            probability. logprobs must be set to true if this parameter is used.

            n: How many chat completion choices to generate for each input message. Note
            that you will be charged based on the number of generated tokens across all
            of the choices. Keep n as 1 to minimize costs.

            presence_penalty: Number between -2.0 and 2.0. Positive values penalize new
            tokens based on whether they appear in the text so far, increasing the
            model's likelihood to talk about new topics.

            response_format: An object specifying the format that the model must output.
            Setting to `{ "type": "json_schema", "json_schema": {...} }` enables
            Structured Outputs which ensures the model will match your supplied JSON
            schema. Learn more in the Structured Outputs guide. Setting to
            `{ "type": "json_object" }` enables JSON mode, which ensures the message the
            model generates is valid JSON.

            seed: If specified, a best effort attempt is made to sample
            deterministically, such that repeated requests with the same seed and
            parameters should return the same result. Determinism is not guaranteed, and
            you should refer to the system_fingerprint response parameter to monitor
            changes in the backend.

            stream_options: Options for streaming response. Only set this when you set
            stream: true.

            top_p: An alternative to sampling with temperature, called nucleus sampling,
            where the model considers the results of the tokens with top_p probability
            mass. So 0.1 means only the tokens comprising the top 10% probability mass
            are considered. Generally recommended to alter this or temperature, but not
            both.

            tools: A list of tools the model may call. Currently, only functions are
            supported as a tool. Use this to provide a list of functions the model may
            generate JSON inputs for. A max of 128 functions are supported.

            tool_choice: Controls which (if any) tool is called by the
            model. none means the model will not call any tool and instead generates a
            message. auto means the model can pick between generating a message or
            calling one or more tools. required means the model must call one or more
            tools. Specifying a particular tool via
            `{"type": "function", "function": {"name": "my_function"}}` forces the model
            to call that tool.
            none is the default when no tools are present. auto is the default if tools
            are present.

            parallel_tool_calls: Whether to enable parallel function calling during tool
            use.

            use_custom_keys:  Whether to use custom API keys or our unified API keys
            with the backend provider.

            tags: Arbitrary number of tags to classify this API query as needed. Helpful
            for generally grouping queries across tasks and users, for logging purposes.

            message_content_only: If True, only return the message content
            chat_completion.choices[0].message.content.strip(" ") from the OpenAI
            return. Otherwise, the full response chat_completion is returned.
            Defaults to True.

            cache: If True, then the arguments will be stored in a local cache file, and
            any future calls with identical arguments will read from the cache instead
            of running the LLM query. This can help to save costs and also debug
            multi-step LLM applications, while keeping early steps fixed.
            This argument only has any effect when stream=False.

            extra_headers: Additional "passthrough" headers for the request which are
            provider-specific, and are not part of the OpenAI standard. They are handled
            by the provider-specific API.

            extra_query: Additional "passthrough" query parameters for the request which
            are provider-specific, and are not part of the OpenAI standard. They are
            handled by the provider-specific API.

            kwargs: Additional "passthrough" JSON properties for the body of the
            request, which are provider-specific, and are not part of the OpenAI
            standard. They will be handled by the provider-specific API.

        Returns:
            If stream is True, returns a generator yielding chunks of content.
            If stream is False, returns a single string response.

        Raises:
            UnifyError: If an error occurs during content generation.
        """
        raise NotImplementedError

    def __init__(
        self,
        endpoint: Optional[str] = None,
        model: Optional[str] = None,
        provider: Optional[str] = None,
        api_key: Optional[str] = None,
    ) -> None:  # noqa: DAR101, DAR401
        """Initialize the Unify client.

        Args:
            endpoint: Endpoint name in OpenAI API format:
            <model_name>@<provider_name>
            Defaults to None.

            model: Name of the model.

            provider: Name of the provider.

            api_key: API key for accessing the Unify API.
                If None, it attempts to retrieve the API key from the
                environment variable UNIFY_KEY.
                Defaults to None.

        Raises:
            UnifyError: If the API key is missing.
        """
        self._api_key = _validate_api_key(api_key)
        if endpoint and (model or provider):
            raise UnifyError(
                "if the model or provider are passed, then the endpoint must not be"
                "passed."
            )
        self._endpoint, self._model, self._provider = None, None, None
        if endpoint:
            self.set_endpoint(endpoint)
        if provider:
            self.set_provider(provider)
        if model:
            self.set_model(model)
        self._client = self._get_client()

    @property
    def model(self) -> str:
        """
        Get the model name.  # noqa: DAR201.

        Returns:
            The model name.
        """
        return self._model

    def set_model(self, value: str) -> None:
        """
        Set the model name.  # noqa: DAR101.

        Args:
            value: The model name.
        """
        valid_models = unify.utils.list_models(self._provider, api_key=self._api_key)
        if value not in valid_models:
            if self._provider:
                raise UnifyError(
                    "Current provider {} does not support the specified model {},"
                    "please select one of: {}".format(
                        self._provider, value, valid_models
                    )
                )
            raise UnifyError(
                "The specified model {} is not one of the models supported by Unify: {}"
                .format(
                    value, valid_models
                )
            )
        self._model = value
        if self._provider:
            self._endpoint = "@".join([value, self._provider])

    @property
    def provider(self) -> Optional[str]:
        """
        Get the provider name.  # noqa: DAR201.

        Returns:
            The provider name.
        """
        return self._provider

    def set_provider(self, value: str) -> None:
        """
        Set the provider name.  # noqa: DAR101.

        Args:
            value: The provider name.
        """
        valid_providers = unify.utils.list_providers(self._model, api_key=self._api_key)
        if value not in valid_providers:
            if self._model:
                raise UnifyError(
                    "Current model {} does not support the specified provider {},"
                    "please select one of: {}".format(
                        self._model, value, valid_providers
                    )
                )
            raise UnifyError(
                "The specified provider {} is not one of the providers supported by "
                "Unify: {}".format(value, valid_providers)
            )
        self._provider = value
        if self._model:
            self._endpoint = "@".join([self._model, value])

    @property
    def endpoint(self) -> str:
        """
        Get the endpoint name.  # noqa: DAR201.

        Returns:
            The endpoint name.
        """
        return self._endpoint

    def set_endpoint(self, value: str) -> None:
        """
        Set the endpoint name.  # noqa: DAR101.

        Args:
            value: The endpoint name.
        """
        valid_endpoints = unify.utils.list_endpoints(api_key=self._api_key)
        if value not in valid_endpoints:
            raise UnifyError(
                "The specified endpoint {} is not one of the endpoints supported by "
                "Unify: {}".format(value, valid_endpoints)
            )
        self._endpoint = value
        self._model, self._provider = value.split("@")  # noqa: WPS414

    def get_credit_balance(self) -> Union[float, None]:
        # noqa: DAR201, DAR401
        """
        Get the remaining credits left on your account.

        Returns:
            The remaining credits on the account if successful, otherwise None.
        Raises:
            BadRequestError: If there was an HTTP error.
            ValueError: If there was an error parsing the JSON response.
        """
        url = f"{base_url()}/credits"
        headers = {
            "accept": "application/json",
            "Authorization": f"Bearer {self._api_key}",
        }
        try:
            response = requests.get(url, headers=headers, timeout=10)
            response.raise_for_status()
            return response.json()["credits"]
        except requests.RequestException as e:
            raise BadRequestError("There was an error with the request.") from e
        except (KeyError, ValueError) as e:
            raise ValueError("Error parsing JSON response.") from e


class Unify(Client):
    """Class for interacting with the Unify chat completions endpoint in a synchronous
    manner."""

    def _get_client(self):
        try:
            return openai.OpenAI(
                base_url=f"{base_url()}",
                api_key=self._api_key,
            )
        except openai.OpenAIError as e:
            raise UnifyError(f"Failed to initialize Unify client: {str(e)}")

    def _generate_stream(
        self,
        messages: List[Dict[str, str]],
        endpoint: str,
        # unified arguments
        max_tokens: Optional[int] = 1024,
        stop: Union[Optional[str], List[str]] = None,
        temperature: Optional[float] = 1.0,
        # partially unified arguments
        frequency_penalty: Optional[float] = None,
        logit_bias: Optional[Dict[str, int]] = None,
        logprobs: Optional[bool] = None,
        top_logprobs: Optional[int] = None,
        n: Optional[int] = None,
        presence_penalty: Optional[float] = None,
        response_format: Optional[ResponseFormat] = None,
        seed: Optional[int] = None,
        stream_options: Optional[ChatCompletionStreamOptionsParam] = None,
        top_p: Optional[float] = None,
        tools: Optional[Iterable[ChatCompletionToolParam]] = None,
        tool_choice: Optional[ChatCompletionToolChoiceOptionParam] = None,
        parallel_tool_calls: Optional[bool] = None,
        # platform arguments
        use_custom_keys: bool = False,
        tags: Optional[List[str]] = None,
        # python client arguments
        message_content_only: bool = True,
        # passthrough arguments
        extra_headers: Optional[Headers] = None,
        extra_query: Optional[Query] = None,
        **kwargs,
    ) -> Generator[str, None, None]:
        kw = dict(
            model=endpoint,
            messages=messages,  # type: ignore[arg-type]
            max_tokens=max_tokens,
            stop=stop,
            stream=True,
            temperature=temperature,
            # partially unified arguments
            frequency_penalty=frequency_penalty,
            logit_bias=logit_bias,
            logprobs=logprobs,
            top_logprobs=top_logprobs,
            n=n,
            presence_penalty=presence_penalty,
            response_format=response_format,
            seed=seed,
            stream_options=stream_options,
            top_p=top_p,
            tools=tools,
            tool_choice=tool_choice,
            parallel_tool_calls=parallel_tool_calls,
            extra_body={  # platform arguments
                "signature": "python",
                "use_custom_keys": use_custom_keys,
                "tags": tags,
                # passthrough json arguments
                **kwargs,
            },
            # other passthrough arguments
            extra_headers=extra_headers,
            extra_query=extra_query,
        )
        kw = {k: v for k, v in kw.items() if v is not None}
        try:
            chat_completion = self._client.chat.completions.create(**kw)
            for chunk in chat_completion:
                if message_content_only:
                    content = chunk.choices[0].delta.content  # type: ignore[union-attr]    # noqa: E501
                else:
                    content = chunk
                self.set_provider(chunk.model.split("@")[-1])  # type: ignore[union-attr]   # noqa: E501
                if content is not None:
                    yield content
        except openai.APIStatusError as e:
            raise status_error_map[e.status_code](e.message) from None

    def _generate_non_stream(
        self,
        messages: List[Dict[str, str]],
        endpoint: str,
        # unified arguments
        max_tokens: Optional[int] = 1024,
        stop: Union[Optional[str], List[str]] = None,
        temperature: Optional[float] = 1.0,
        # partially unified arguments
        frequency_penalty: Optional[float] = None,
        logit_bias: Optional[Dict[str, int]] = None,
        logprobs: Optional[bool] = None,
        top_logprobs: Optional[int] = None,
        n: Optional[int] = None,
        presence_penalty: Optional[float] = None,
        response_format: Optional[ResponseFormat] = None,
        seed: Optional[int] = None,
        stream_options: Optional[ChatCompletionStreamOptionsParam] = None,
        top_p: Optional[float] = None,
        tools: Optional[Iterable[ChatCompletionToolParam]] = None,
        tool_choice: Optional[ChatCompletionToolChoiceOptionParam] = None,
        parallel_tool_calls: Optional[bool] = None,
        # platform arguments
        use_custom_keys: bool = False,
        tags: Optional[List[str]] = None,
        # python client arguments
        message_content_only: bool = True,
        cache: bool = False,
        # passthrough arguments
        extra_headers: Optional[Headers] = None,
        extra_query: Optional[Query] = None,
        **kwargs,
    ) -> str:
        kw = dict(
            model=endpoint,
            messages=messages,  # type: ignore[arg-type]
            max_tokens=max_tokens,
            stop=stop,
            stream=False,
            temperature=temperature,
            # partially unified arguments
            frequency_penalty=frequency_penalty,
            logit_bias=logit_bias,
            logprobs=logprobs,
            top_logprobs=top_logprobs,
            n=n,
            presence_penalty=presence_penalty,
            response_format=response_format,
            seed=seed,
            stream_options=stream_options,
            top_p=top_p,
            tools=tools,
            tool_choice=tool_choice,
            parallel_tool_calls=parallel_tool_calls,
            extra_body={  # platform arguments
                "signature": "python",
                "use_custom_keys": use_custom_keys,
                "tags": tags,
                # passthrough json arguments
                **kwargs,
            },
            # other passthrough arguments
            extra_headers=extra_headers,
            extra_query=extra_query,
        )
        kw = {k: v for k, v in kw.items() if v is not None}
        chat_completion = None
        if cache:
            chat_completion = _get_cache(kw)
        if chat_completion is None:
            try:
                chat_completion = self._client.chat.completions.create(**kw)
            except openai.APIStatusError as e:
                raise status_error_map[e.status_code](e.message) from None
            if cache:
                _write_to_cache(kw, chat_completion)
        if "router" not in endpoint:
            self.set_provider(
                chat_completion.model.split(  # type: ignore[union-attr]
                    "@",
                )[-1]
            )
        if message_content_only:
            content = chat_completion.choices[0].message.content
            if content:
                return content.strip(" ")
            return ""
        return chat_completion

    def generate(  # noqa: WPS234, WPS211
        self,
        user_prompt: Optional[str] = None,
        system_prompt: Optional[str] = None,
        messages: Optional[Iterable[ChatCompletionMessageParam]] = None,
        *,
        # unified arguments
        max_tokens: Optional[int] = 1024,
        stop: Union[Optional[str], List[str]] = None,
        stream: Optional[bool] = False,
        temperature: Optional[float] = 1.0,
        # partially unified arguments
        frequency_penalty: Optional[float] = None,
        logit_bias: Optional[Dict[str, int]] = None,
        logprobs: Optional[bool] = None,
        top_logprobs: Optional[int] = None,
        n: Optional[int] = None,
        presence_penalty: Optional[float] = None,
        response_format: Optional[ResponseFormat] = None,
        seed: Optional[int] = None,
        stream_options: Optional[ChatCompletionStreamOptionsParam] = None,
        top_p: Optional[float] = None,
        tools: Optional[Iterable[ChatCompletionToolParam]] = None,
        tool_choice: Optional[ChatCompletionToolChoiceOptionParam] = None,
        parallel_tool_calls: Optional[bool] = None,
        # platform arguments
        use_custom_keys: bool = False,
        tags: Optional[List[str]] = None,
        # python client arguments
        message_content_only: bool = True,
        cache: bool = False,
        # passthrough arguments
        extra_headers: Optional[Headers] = None,
        extra_query: Optional[Query] = None,
        **kwargs,
    ) -> Union[Generator[str, None, None], str]:  # noqa: DAR101, DAR201, DAR401
        contents = []
        if system_prompt:
            contents.append({"role": "system", "content": system_prompt})
        if user_prompt:
            contents.append({"role": "user", "content": user_prompt})
        elif messages:
            contents.extend(messages)
        else:
            raise UnifyError("You must provider either the user_prompt or messages!")

        if tools:
            message_content_only = False

        if stream:
            return self._generate_stream(
                contents,
                self._endpoint,
                max_tokens=max_tokens,
                stop=stop,
                temperature=temperature,
                # partially unified arguments
                frequency_penalty=frequency_penalty,
                logit_bias=logit_bias,
                logprobs=logprobs,
                top_logprobs=top_logprobs,
                n=n,
                presence_penalty=presence_penalty,
                response_format=response_format,
                seed=seed,
                stream_options=stream_options,
                top_p=top_p,
                tools=tools,
                tool_choice=tool_choice,
                parallel_tool_calls=parallel_tool_calls,
                # platform arguments
                use_custom_keys=use_custom_keys,
                tags=tags,
                # python client arguments
                message_content_only=message_content_only,
                # passthrough arguments
                extra_headers=extra_headers,
                extra_query=extra_query,
                **kwargs,
            )
        return self._generate_non_stream(
            contents,
            self._endpoint,
            max_tokens=max_tokens,
            stop=stop,
            temperature=temperature,
            # partially unified arguments
            frequency_penalty=frequency_penalty,
            logit_bias=logit_bias,
            logprobs=logprobs,
            top_logprobs=top_logprobs,
            n=n,
            presence_penalty=presence_penalty,
            response_format=response_format,
            seed=seed,
            stream_options=stream_options,
            top_p=top_p,
            tools=tools,
            tool_choice=tool_choice,
            parallel_tool_calls=parallel_tool_calls,
            # platform arguments
            use_custom_keys=use_custom_keys,
            tags=tags,
            # python client arguments
            message_content_only=message_content_only,
            cache=cache,
            # passthrough arguments
            extra_headers=extra_headers,
            extra_query=extra_query,
            **kwargs,
        )


class AsyncUnify(Client):
    """Class for interacting with the Unify chat completions endpoint in a synchronous
    manner."""

    def _get_client(self):
        try:
            return openai.AsyncOpenAI(
                base_url=f"{base_url()}",
                api_key=self._api_key,
            )
        except openai.APIStatusError as e:
            raise UnifyError(f"Failed to initialize Unify client: {str(e)}")

    async def _generate_stream(
        self,
        messages: List[Dict[str, str]],
        endpoint: str,
        # unified arguments
        max_tokens: Optional[int] = 1024,
        stop: Union[Optional[str], List[str]] = None,
        temperature: Optional[float] = 1.0,
        # partially unified arguments
        frequency_penalty: Optional[float] = None,
        logit_bias: Optional[Dict[str, int]] = None,
        logprobs: Optional[bool] = None,
        top_logprobs: Optional[int] = None,
        n: Optional[int] = None,
        presence_penalty: Optional[float] = None,
        response_format: Optional[ResponseFormat] = None,
        seed: Optional[int] = None,
        stream_options: Optional[ChatCompletionStreamOptionsParam] = None,
        top_p: Optional[float] = None,
        tools: Optional[Iterable[ChatCompletionToolParam]] = None,
        tool_choice: Optional[ChatCompletionToolChoiceOptionParam] = None,
        parallel_tool_calls: Optional[bool] = None,
        # platform arguments
        use_custom_keys: bool = False,
        tags: Optional[List[str]] = None,
        # python client arguments
        message_content_only: bool = True,
        cache: bool = False,
        # passthrough arguments
        extra_headers: Optional[Headers] = None,
        extra_query: Optional[Query] = None,
        **kwargs,
    ) -> AsyncGenerator[str, None]:
        kw = dict(
            model=endpoint,
            messages=messages,  # type: ignore[arg-type]
            max_tokens=max_tokens,
            stop=stop,
            stream=True,
            temperature=temperature,
            # partially unified arguments
            frequency_penalty=frequency_penalty,
            logit_bias=logit_bias,
            logprobs=logprobs,
            top_logprobs=top_logprobs,
            n=n,
            presence_penalty=presence_penalty,
            response_format=response_format,
            seed=seed,
            stream_options=stream_options,
            top_p=top_p,
            tools=tools,
            tool_choice=tool_choice,
            parallel_tool_calls=parallel_tool_calls,
            extra_body={  # platform arguments
                "signature": "python",
                "use_custom_keys": use_custom_keys,
                "tags": tags,
                # passthrough json arguments
                **kwargs,
            },
            # other passthrough arguments
            extra_headers=extra_headers,
            extra_query=extra_query,
        )
        kw = {k: v for k, v in kw.items() if v is not None}
        try:
            async_stream = await self._client.chat.completions.create(**kw)
            async for chunk in async_stream:  # type: ignore[union-attr]
                self.set_provider(chunk.model.split("@")[-1])
                if message_content_only:
                    yield chunk.choices[0].delta.content or ""
                yield chunk
        except openai.APIStatusError as e:
            raise status_error_map[e.status_code](e.message) from None

    async def _generate_non_stream(
        self,
        messages: List[Dict[str, str]],
        endpoint: str,
        # unified arguments
        max_tokens: Optional[int] = 1024,
        stop: Union[Optional[str], List[str]] = None,
        temperature: Optional[float] = 1.0,
        # partially unified arguments
        frequency_penalty: Optional[float] = None,
        logit_bias: Optional[Dict[str, int]] = None,
        logprobs: Optional[bool] = None,
        top_logprobs: Optional[int] = None,
        n: Optional[int] = None,
        presence_penalty: Optional[float] = None,
        response_format: Optional[ResponseFormat] = None,
        seed: Optional[int] = None,
        stream_options: Optional[ChatCompletionStreamOptionsParam] = None,
        top_p: Optional[float] = None,
        tools: Optional[Iterable[ChatCompletionToolParam]] = None,
        tool_choice: Optional[ChatCompletionToolChoiceOptionParam] = None,
        parallel_tool_calls: Optional[bool] = None,
        # platform arguments
        use_custom_keys: bool = False,
        tags: Optional[List[str]] = None,
        # python client arguments
        message_content_only: bool = True,
        cache: bool = False,
        # passthrough arguments
        extra_headers: Optional[Headers] = None,
        extra_query: Optional[Query] = None,
        **kwargs,
    ) -> str:
        kw = dict(
            model=endpoint,
            messages=messages,  # type: ignore[arg-type]
            max_tokens=max_tokens,
            stop=stop,
            stream=False,
            temperature=temperature,
            # partially unified arguments
            frequency_penalty=frequency_penalty,
            logit_bias=logit_bias,
            logprobs=logprobs,
            top_logprobs=top_logprobs,
            n=n,
            presence_penalty=presence_penalty,
            response_format=response_format,
            seed=seed,
            stream_options=stream_options,
            top_p=top_p,
            tools=tools,
            tool_choice=tool_choice,
            parallel_tool_calls=parallel_tool_calls,
            extra_body={  # platform arguments
                "signature": "python",
                "use_custom_keys": use_custom_keys,
                "tags": tags,
                # passthrough json arguments
                **kwargs,
            },
            # other passthrough arguments
            extra_headers=extra_headers,
            extra_query=extra_query,
        )
        kw = {k: v for k, v in kw.items() if v is not None}
        chat_completion = None
        if cache:
            chat_completion = _get_cache(kw)
        if chat_completion is None:
            try:
                async_response = await self._client.chat.completions.create(**kw)
            except openai.APIStatusError as e:
                raise status_error_map[e.status_code](e.message) from None
            if cache:
                _write_to_cache(kw, chat_completion)
        self.set_provider(async_response.model.split("@")[-1])  # type: ignore
        if message_content_only:
            content = async_response.choices[0].message.content
            if content:
                return content.strip(" ")
            return ""
        return async_response

    async def generate(  # noqa: WPS234, WPS211
        self,
        user_prompt: Optional[str] = None,
        system_prompt: Optional[str] = None,
        messages: Optional[Iterable[ChatCompletionMessageParam]] = None,
        *,
        # unified arguments
        max_tokens: Optional[int] = 1024,
        stop: Union[Optional[str], List[str]] = None,
        stream: Optional[bool] = False,
        temperature: Optional[float] = 1.0,
        # partially unified arguments
        frequency_penalty: Optional[float] = None,
        logit_bias: Optional[Dict[str, int]] = None,
        logprobs: Optional[bool] = None,
        top_logprobs: Optional[int] = None,
        n: Optional[int] = None,
        presence_penalty: Optional[float] = None,
        response_format: Optional[ResponseFormat] = None,
        seed: Optional[int] = None,
        stream_options: Optional[ChatCompletionStreamOptionsParam] = None,
        top_p: Optional[float] = None,
        tools: Optional[Iterable[ChatCompletionToolParam]] = None,
        tool_choice: Optional[ChatCompletionToolChoiceOptionParam] = None,
        parallel_tool_calls: Optional[bool] = None,
        # platform arguments
        use_custom_keys: bool = False,
        tags: Optional[List[str]] = None,
        # python client arguments
        message_content_only: bool = True,
        cache: bool = False,
        # passthrough arguments
        extra_headers: Optional[Headers] = None,
        extra_query: Optional[Query] = None,
        **kwargs,
    ) -> Union[AsyncGenerator[str, None], str]:  # noqa: DAR101, DAR201, DAR401
        contents = []
        if system_prompt:
            contents.append({"role": "system", "content": system_prompt})

        if user_prompt:
            contents.append({"role": "user", "content": user_prompt})
        elif messages:
            contents.extend(messages)
        else:
            raise UnifyError("You must provide either the user_prompt or messages!")

        if stream:
            return self._generate_stream(
                contents,
                self._endpoint,
                max_tokens=max_tokens,
                stop=stop,
                temperature=temperature,
                # partially unified arguments
                frequency_penalty=frequency_penalty,
                logit_bias=logit_bias,
                logprobs=logprobs,
                top_logprobs=top_logprobs,
                n=n,
                presence_penalty=presence_penalty,
                response_format=response_format,
                seed=seed,
                stream_options=stream_options,
                top_p=top_p,
                tools=tools,
                tool_choice=tool_choice,
                parallel_tool_calls=parallel_tool_calls,
                # platform arguments
                use_custom_keys=use_custom_keys,
                tags=tags,
                # python client arguments
                message_content_only=message_content_only,
                # passthrough arguments
                extra_headers=extra_headers,
                extra_query=extra_query,
                **kwargs,
            )
        return await self._generate_non_stream(
            contents,
            self._endpoint,
            max_tokens=max_tokens,
            stop=stop,
            temperature=temperature,
            # partially unified arguments
            frequency_penalty=frequency_penalty,
            logit_bias=logit_bias,
            logprobs=logprobs,
            top_logprobs=top_logprobs,
            n=n,
            presence_penalty=presence_penalty,
            response_format=response_format,
            seed=seed,
            stream_options=stream_options,
            top_p=top_p,
            tools=tools,
            tool_choice=tool_choice,
            parallel_tool_calls=parallel_tool_calls,
            # platform arguments
            use_custom_keys=use_custom_keys,
            tags=tags,
            # python client arguments
            message_content_only=message_content_only,
            cache=cache,
            # passthrough arguments
            extra_headers=extra_headers,
            extra_query=extra_query,
            **kwargs,
        )
