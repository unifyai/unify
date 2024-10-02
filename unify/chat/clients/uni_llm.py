# global
import abc
import openai

# noinspection PyProtectedMember
from openai._types import Headers, Query
from openai.types.chat import (
    ChatCompletionToolParam,
    ChatCompletionToolChoiceOptionParam,
    ChatCompletionMessageParam,
    ChatCompletionStreamOptionsParam,
)
from openai.types.chat.completion_create_params import ResponseFormat
from typing_extensions import Self
from typing import AsyncGenerator, Dict, Generator, List, Optional, Union, Iterable

# local
import unify
from unify import BASE_URL, LOCAL_MODELS
from unify.chat.clients.base import _Client
from unify.types import Prompt, ChatCompletion
from unify._caching import _get_cache, _write_to_cache


class _UniLLMClient(_Client, abc.ABC):

    def __init__(
        self,
        endpoint: Optional[str] = None,
        *,
        model: Optional[str] = None,
        provider: Optional[str] = None,
        system_message: Optional[str] = None,
        messages: Optional[List[ChatCompletionMessageParam]] = None,
        frequency_penalty: Optional[float] = None,
        logit_bias: Optional[Dict[str, int]] = None,
        logprobs: Optional[bool] = None,
        top_logprobs: Optional[int] = None,
        max_completion_tokens: Optional[int] = 1024,
        n: Optional[int] = None,
        presence_penalty: Optional[float] = None,
        response_format: Optional[ResponseFormat] = None,
        seed: Optional[int] = None,
        stop: Union[Optional[str], List[str]] = None,
        stream: Optional[bool] = False,
        stream_options: Optional[ChatCompletionStreamOptionsParam] = None,
        temperature: Optional[float] = 1.0,
        top_p: Optional[float] = None,
        tools: Optional[Iterable[ChatCompletionToolParam]] = None,
        tool_choice: Optional[ChatCompletionToolChoiceOptionParam] = None,
        parallel_tool_calls: Optional[bool] = None,
        # platform arguments
        use_custom_keys: bool = False,
        tags: Optional[List[str]] = None,
        drop_params: Optional[bool] = True,
        region: Optional[str] = None,
        log_query_body: Optional[bool] = True,
        log_response_body: Optional[bool] = True,
        api_key: Optional[str] = None,
        # python client arguments
        return_full_completion: bool = False,
        cache: bool = False,
        # passthrough arguments
        extra_headers: Optional[Headers] = None,
        extra_query: Optional[Query] = None,
        **kwargs,
    ):
        """Initialize the Uni LLM Unify client.

        Args:
            endpoint: Endpoint name in OpenAI API format:
            <model_name>@<provider_name>
            Defaults to None.

            model: Name of the model. Should only be set if endpoint is not set.

            provider: Name of the provider. Should only be set if endpoint is not set.

            system_message: An optional string containing the system message. This
            always appears at the beginning of the list of messages.

            messages: A list of messages comprising the conversation so far. This will
            be appended to the system_message if it is not None, and any user_message
            will be appended if it is not None.

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

            max_completion_tokens: The maximum number of tokens that can be generated in
            the chat completion. The total length of input tokens and generated tokens
            is limited by the model's context length. Defaults to the provider's default
            max_completion_tokens when the value is None.

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

            stop: Up to 4 sequences where the API will stop generating further tokens.

            stream: If True, generates content as a stream. If False, generates content
            as a single response. Defaults to False.

            stream_options: Options for streaming response. Only set this when you set
            stream: true.

            temperature:  What sampling temperature to use, between 0 and 2.
            Higher values like 0.8 will make the output more random,
            while lower values like 0.2 will make it more focused and deterministic.
            It is generally recommended to alter this or top_p, but not both.
            Defaults to the provider's default max_completion_tokens when the value is
            None.

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
            `{ "type": "function", "function": {"name": "my_function"} }`
            forces the model to call that tool.
            none is the default when no tools are present. auto is the default if tools
            are present.

            parallel_tool_calls: Whether to enable parallel function calling during tool
            use.

            use_custom_keys:  Whether to use custom API keys or our unified API keys
            with the backend provider.

            tags: Arbitrary number of tags to classify this API query as needed. Helpful
            for generally grouping queries across tasks and users, for logging purposes.

            drop_params: Whether or not to drop unsupported OpenAI params by the
            provider you’re using.

            region: A string used to represent the region where the endpoint is
            accessed. Only relevant for on-prem deployments with certain providers like
            `vertex-ai`, `aws-bedrock` and `azure-ml`, where the endpoint is being
            accessed through a specified region.

            log_query_body: Whether to log the contents of the query json body.

            log_response_body: Whether to log the contents of the response json body.

            return_full_completion: If False, only return the message content
            chat_completion.choices[0].message.content.strip(" ") from the OpenAI
            return. Otherwise, the full response chat_completion is returned.
            Defaults to False.

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

        Raises:
            UnifyError: If the API key is missing.
        """
        self._constructor_args = dict(
            system_message=system_message,
            messages=messages,
            frequency_penalty=frequency_penalty,
            logit_bias=logit_bias,
            logprobs=logprobs,
            top_logprobs=top_logprobs,
            max_completion_tokens=max_completion_tokens,
            n=n,
            presence_penalty=presence_penalty,
            response_format=response_format,
            seed=seed,
            stop=stop,
            stream=stream,
            stream_options=stream_options,
            temperature=temperature,
            top_p=top_p,
            tools=tools,
            tool_choice=tool_choice,
            parallel_tool_calls=parallel_tool_calls,
            # platform arguments
            use_custom_keys=use_custom_keys,
            tags=tags,
            drop_params=drop_params,
            region=region,
            log_query_body=log_query_body,
            log_response_body=log_response_body,
            api_key=api_key,
            # python client arguments
            return_full_completion=return_full_completion,
            cache=cache,
            # passthrough arguments
            extra_headers=extra_headers,
            extra_query=extra_query,
            **kwargs,
        )
        super().__init__(**self._constructor_args)
        if endpoint and (model or provider):
            raise Exception(
                "if the model or provider are passed, then the endpoint must not be"
                "passed."
            )
        self._client = self._get_client()
        self._endpoint = None
        self._provider = None
        self._model = None
        if endpoint:
            self.set_endpoint(endpoint)
        if provider:
            self.set_provider(provider)
        if model:
            self.set_model(model)

    # Properties #
    # -----------#

    @property
    def endpoint(self) -> str:
        """
        Get the endpoint name.

        Returns:
            The endpoint name.
        """
        return self._endpoint

    @property
    def model(self) -> str:
        """
        Get the model name.

        Returns:
            The model name.
        """
        return self._model

    @property
    def provider(self) -> str:
        """
        Get the provider name.

        Returns:
            The provider name.
        """
        return self._provider

    # Setters #
    # --------#

    def set_endpoint(self, value: str) -> Self:
        """
        Set the endpoint name.  # noqa: DAR101.

        Args:
            value: The endpoint name.

        Returns:
            This client, useful for chaining inplace calls.
        """
        valid_endpoints = unify.list_endpoints(api_key=self._api_key)
        if value not in valid_endpoints and (
            "@custom" not in value
            and "@local" not in value
        ):
            raise Exception(
                "The specified endpoint {} is not one of the endpoints supported by "
                "Unify: {}".format(value, valid_endpoints)
            )
        self._endpoint = value
        self._model, self._provider = value.split("@")  # noqa: WPS414
        return self

    def set_model(self, value: str) -> Self:
        """
        Set the model name.  # noqa: DAR101.

        Args:
            value: The model name.

        Returns:
            This client, useful for chaining inplace calls.
        """
        valid_models = unify.list_models(self._provider, api_key=self._api_key)
        if value not in valid_models and (
            "custom" not in self._provider
            and self._provider != "local"
        ):
            if self._provider:
                raise Exception(
                    "Current provider {} does not support the specified model {},"
                    "please select one of: {}".format(
                        self._provider, value, valid_models
                    )
                )
            raise Exception(
                "The specified model {} is not one of the models supported by Unify: "
                "{}".format(value, valid_models)
            )
        self._model = value
        if self._provider:
            self._endpoint = "@".join([value, self._provider])
        return self

    def set_provider(self, value: str) -> Self:
        """
        Set the provider name.  # noqa: DAR101.

        Args:
            value: The provider name.

        Returns:
            This client, useful for chaining inplace calls.
        """
        valid_providers = unify.list_providers(self._model, api_key=self._api_key)
        if value not in valid_providers and (
            "custom" not in self._provider
            and self._provider != "local"
        ):
            if self._model:
                raise Exception(
                    "Current model {} does not support the specified provider {},"
                    "please select one of: {}".format(
                        self._model, value, valid_providers
                    )
                )
            raise Exception(
                "The specified provider {} is not one of the providers supported by "
                "Unify: {}".format(value, valid_providers)
            )
        self._provider = value
        if self._model:
            self._endpoint = "@".join([self._model, value])
        return self

    @staticmethod
    def _handle_kw(
        prompt,
        endpoint,
        stream,
        stream_options,
        use_custom_keys,
        tags,
        drop_params,
        region,
        log_query_body,
        log_response_body,
    ):
        prompt_dict = prompt.model_dump()
        if "extra_body" in prompt_dict:
            extra_body = prompt_dict["extra_body"]
            del prompt_dict["extra_body"]
        else:
            extra_body = {}
        kw = dict(
            model=endpoint,
            **prompt_dict,
            stream=stream,
            stream_options=stream_options,
            extra_body={  # platform arguments
                "signature": "python",
                "use_custom_keys": use_custom_keys,
                "tags": tags,
                "drop_params": drop_params,
                "region": region,
                "log_query_body": log_query_body,
                "log_response_body": log_response_body,
                # passthrough json arguments
                **extra_body,
            },
        )
        return {k: v for k, v in kw.items() if v is not None}

    # Representation #
    # ---------------#

    def __repr__(self):
        return "{}(endpoint={})".format(self.__class__.__name__, self._endpoint)

    def __str__(self):
        return "{}(endpoint={})".format(self.__class__.__name__, self._endpoint)

    # Abstract #
    # ---------#

    @abc.abstractmethod
    def _get_client(self):
        raise NotImplementedError


class Unify(_UniLLMClient):
    """Class for interacting with the Unify chat completions endpoint in a synchronous
    manner."""

    def _get_client(self):
        try:
            return openai.OpenAI(
                base_url=f"{BASE_URL}",
                api_key=self._api_key,
            )
        except openai.OpenAIError as e:
            raise Exception(f"Failed to initialize Unify client: {str(e)}")

    def _generate_stream(
        self,
        endpoint: str,
        prompt: Prompt,
        # stream
        stream_options: Optional[ChatCompletionStreamOptionsParam],
        # platform arguments
        use_custom_keys: bool,
        tags: Optional[List[str]],
        drop_params: Optional[bool],
        region: Optional[str],
        log_query_body: Optional[bool],
        log_response_body: Optional[bool],
        # python client arguments
        return_full_completion: bool,
    ) -> Generator[str, None, None]:
        kw = self._handle_kw(
            prompt=prompt,
            endpoint=endpoint,
            stream=True,
            stream_options=stream_options,
            use_custom_keys=use_custom_keys,
            tags=tags,
            drop_params=drop_params,
            region=region,
            log_query_body=log_query_body,
            log_response_body=log_response_body,
        )
        try:
            if endpoint in LOCAL_MODELS:
                kw.pop("extra_body")
                kw.pop("model")
                chat_completion = LOCAL_MODELS[endpoint](**kw)
            else:
                chat_completion = self._client.chat.completions.create(**kw)
            for chunk in chat_completion:
                if return_full_completion:
                    content = ChatCompletion(**(
                        chunk.model_dump()
                        if endpoint not in LOCAL_MODELS
                        else chunk.json()
                    ))
                else:
                    content = chunk.choices[0].delta.content  # type: ignore[union-attr]    # noqa: E501
                self.set_provider(chunk.model.split("@")[-1])  # type: ignore[union-attr]   # noqa: E501
                if content is not None:
                    yield content
        except openai.APIStatusError as e:
            raise Exception(e.message)

    def _generate_non_stream(
        self,
        endpoint: str,
        prompt: Prompt,
        # platform arguments
        use_custom_keys: bool,
        tags: Optional[List[str]],
        drop_params: Optional[bool],
        region: Optional[str],
        log_query_body: Optional[bool],
        log_response_body: Optional[bool],
        # python client arguments
        return_full_completion: bool,
        cache: bool,
    ) -> Union[str, ChatCompletion]:
        kw = self._handle_kw(
            prompt=prompt,
            endpoint=endpoint,
            stream=False,
            stream_options=None,
            use_custom_keys=use_custom_keys,
            tags=tags,
            drop_params=drop_params,
            region=region,
            log_query_body=log_query_body,
            log_response_body=log_response_body,
        )
        chat_completion = None
        if cache:
            chat_completion = _get_cache(kw)
        if chat_completion is None:
            try:
                if endpoint in LOCAL_MODELS:
                    kw.pop("extra_body")
                    kw.pop("model")
                    chat_completion = LOCAL_MODELS[endpoint](**kw).json()
                else:
                    chat_completion = self._client.chat.completions.create(**kw).model_dump()
                chat_completion = ChatCompletion(**chat_completion)
            except openai.APIStatusError as e:
                raise Exception(e.message)
            if cache:
                _write_to_cache(kw, chat_completion)
        if "router" not in endpoint:
            self.set_provider(
                chat_completion.model.split(  # type: ignore[union-attr]
                    "@",
                )[-1]
            )
        if return_full_completion:
            return chat_completion
        content = chat_completion.choices[0].message.content
        if content:
            return content.strip(" ")
        return ""

    def _generate(  # noqa: WPS234, WPS211
        self,
        user_message: Optional[str],
        system_message: Optional[str],
        messages: Optional[List[ChatCompletionMessageParam]],
        *,
        frequency_penalty: Optional[float],
        logit_bias: Optional[Dict[str, int]],
        logprobs: Optional[bool],
        top_logprobs: Optional[int],
        max_completion_tokens: Optional[int],
        n: Optional[int],
        presence_penalty: Optional[float],
        response_format: Optional[ResponseFormat],
        seed: Optional[int],
        stop: Union[Optional[str], List[str]],
        stream: Optional[bool],
        stream_options: Optional[ChatCompletionStreamOptionsParam],
        temperature: Optional[float],
        top_p: Optional[float],
        tools: Optional[Iterable[ChatCompletionToolParam]],
        tool_choice: Optional[ChatCompletionToolChoiceOptionParam],
        parallel_tool_calls: Optional[bool],
        # platform arguments
        use_custom_keys: bool,
        tags: Optional[List[str]],
        drop_params: Optional[bool],
        region: Optional[str],
        log_query_body: Optional[bool],
        log_response_body: Optional[bool],
        # python client arguments
        return_full_completion: bool,
        cache: bool,
        # passthrough arguments
        extra_headers: Optional[Headers],
        extra_query: Optional[Query],
        **kwargs,
    ) -> Union[Generator[str, None, None], str]:  # noqa: DAR101, DAR201, DAR401
        contents = []
        if system_message:
            contents.append({"role": "system", "content": system_message})
        if messages:
            contents.extend(messages)
        if user_message:
            contents.append({"role": "user", "content": user_message})

        prompt = Prompt(
            messages=contents,
            frequency_penalty=frequency_penalty,
            logit_bias=logit_bias,
            logprobs=logprobs,
            top_logprobs=top_logprobs,
            max_completion_tokens=max_completion_tokens,
            n=n,
            presence_penalty=presence_penalty,
            response_format=response_format,
            seed=seed,
            stop=stop,
            temperature=temperature,
            top_p=top_p,
            tools=tools,
            tool_choice=tool_choice,
            parallel_tool_calls=parallel_tool_calls,
            extra_headers=extra_headers,
            extra_query=extra_query,
            extra_body=kwargs,
        )
        if stream:
            return self._generate_stream(
                self._endpoint,
                prompt,
                # stream
                stream_options=stream_options,
                # platform arguments
                use_custom_keys=use_custom_keys,
                tags=tags,
                drop_params=drop_params,
                region=region,
                log_query_body=log_query_body,
                log_response_body=log_response_body,
                # python client arguments
                return_full_completion=return_full_completion,
            )
        return self._generate_non_stream(
            self._endpoint,
            prompt,
            # platform arguments
            use_custom_keys=use_custom_keys,
            tags=tags,
            drop_params=drop_params,
            region=region,
            log_query_body=log_query_body,
            log_response_body=log_response_body,
            # python client arguments
            return_full_completion=return_full_completion,
            cache=cache,
        )

    def to_async_client(self):
        """
        Return an asynchronous version of the client (`AsyncUnify` instance), with the
        exact same configuration as this synchronous (`Unify`) client.

        Returns:
            An `AsyncUnify` instance with the same configuration as this `Unify`
            instance.
        """
        return AsyncUnify(**self._constructor_args)


class AsyncUnify(_UniLLMClient):
    """Class for interacting with the Unify chat completions endpoint in a synchronous
    manner."""

    def _get_client(self):
        try:
            return openai.AsyncOpenAI(
                base_url=f"{BASE_URL}",
                api_key=self._api_key,
            )
        except openai.APIStatusError as e:
            raise Exception(f"Failed to initialize Unify client: {str(e)}")

    async def _generate_stream(
        self,
        endpoint: str,
        prompt: Prompt,
        # stream
        stream_options: Optional[ChatCompletionStreamOptionsParam],
        # platform arguments
        use_custom_keys: bool,
        tags: Optional[List[str]],
        drop_params: Optional[bool],
        region: Optional[str],
        log_query_body: Optional[bool],
        log_response_body: Optional[bool],
        # python client arguments
        return_full_completion: bool,
    ) -> AsyncGenerator[str, None]:
        kw = self._handle_kw(
            prompt=prompt,
            endpoint=endpoint,
            stream=True,
            stream_options=stream_options,
            use_custom_keys=use_custom_keys,
            tags=tags,
            drop_params=drop_params,
            region=region,
            log_query_body=log_query_body,
            log_response_body=log_response_body,
        )
        try:
            if endpoint in LOCAL_MODELS:
                kw.pop("extra_body")
                kw.pop("model")
                async_stream = await LOCAL_MODELS[endpoint](**kw)
            else:
                async_stream = await self._client.chat.completions.create(**kw)
            async for chunk in async_stream:  # type: ignore[union-attr]
                self.set_provider(chunk.model.split("@")[-1])
                if return_full_completion:
                    yield ChatCompletion(**(
                        chunk.model_dump()
                        if endpoint in LOCAL_MODELS
                        else chunk.json()
                    ))
                else:
                    yield chunk.choices[0].delta.content or ""
        except openai.APIStatusError as e:
            raise Exception(e.message)

    async def _generate_non_stream(
        self,
        endpoint: str,
        prompt: Prompt,
        # platform arguments
        use_custom_keys: bool,
        tags: Optional[List[str]],
        drop_params: Optional[bool],
        region: Optional[str],
        log_query_body: Optional[bool],
        log_response_body: Optional[bool],
        # python client arguments
        return_full_completion: bool,
        cache: bool,
    ) -> Union[str, ChatCompletion]:
        kw = self._handle_kw(
            prompt=prompt,
            endpoint=endpoint,
            stream=False,
            stream_options=None,
            use_custom_keys=use_custom_keys,
            tags=tags,
            drop_params=drop_params,
            region=region,
            log_query_body=log_query_body,
            log_response_body=log_response_body,
        )
        chat_completion, async_response = None, None
        if cache:
            chat_completion = _get_cache(kw)
        if chat_completion is None:
            try:
                if endpoint in LOCAL_MODELS:
                    kw.pop("extra_body")
                    kw.pop("model")
                    async_response = await LOCAL_MODELS[endpoint](**kw).json()
                else:
                    async_response = await self._client.chat.completions.create(**kw).model_dump()
                async_response = ChatCompletion(**async_response)
            except openai.APIStatusError as e:
                raise Exception(e.message)
            if cache:
                _write_to_cache(kw, chat_completion)
        self.set_provider(async_response.model.split("@")[-1])  # type: ignore
        if return_full_completion:
            return async_response
        content = async_response.choices[0].message.content
        if content:
            return content.strip(" ")
        return ""

    async def _generate(  # noqa: WPS234, WPS211
        self,
        user_message: Optional[str],
        system_message: Optional[str],
        messages: Optional[List[ChatCompletionMessageParam]],
        *,
        frequency_penalty: Optional[float],
        logit_bias: Optional[Dict[str, int]],
        logprobs: Optional[bool],
        top_logprobs: Optional[int],
        max_completion_tokens: Optional[int],
        n: Optional[int],
        presence_penalty: Optional[float],
        response_format: Optional[ResponseFormat],
        seed: Optional[int],
        stop: Union[Optional[str], List[str]],
        stream: Optional[bool],
        stream_options: Optional[ChatCompletionStreamOptionsParam],
        temperature: Optional[float],
        top_p: Optional[float],
        tools: Optional[Iterable[ChatCompletionToolParam]],
        tool_choice: Optional[ChatCompletionToolChoiceOptionParam],
        parallel_tool_calls: Optional[bool],
        # platform arguments
        use_custom_keys: bool,
        tags: Optional[List[str]],
        drop_params: Optional[bool],
        region: Optional[str],
        log_query_body: Optional[bool],
        log_response_body: Optional[bool],
        # python client arguments
        return_full_completion: bool,
        cache: bool,
        # passthrough arguments
        extra_headers: Optional[Headers],
        extra_query: Optional[Query],
        **kwargs,
    ) -> Union[AsyncGenerator[str, None], str]:  # noqa: DAR101, DAR201, DAR401
        contents = []
        assert (
            messages or user_message
        ), "You must provide either the user_message or messages!"
        if system_message:
            contents.append({"role": "system", "content": system_message})
        if messages:
            contents.extend(messages)
        if user_message:
            contents.append({"role": "user", "content": user_message})
        prompt = Prompt(
            messages=contents,
            frequency_penalty=frequency_penalty,
            logit_bias=logit_bias,
            logprobs=logprobs,
            top_logprobs=top_logprobs,
            max_completion_tokens=max_completion_tokens,
            n=n,
            presence_penalty=presence_penalty,
            response_format=response_format,
            seed=seed,
            stop=stop,
            temperature=temperature,
            top_p=top_p,
            tools=tools,
            tool_choice=tool_choice,
            parallel_tool_calls=parallel_tool_calls,
            extra_headers=extra_headers,
            extra_query=extra_query,
            extra_body=kwargs,
        )
        if stream:
            return self._generate_stream(
                self._endpoint,
                prompt,
                # stream
                stream_options=stream_options,
                # platform arguments
                use_custom_keys=use_custom_keys,
                tags=tags,
                drop_params=drop_params,
                region=region,
                log_query_body=log_query_body,
                log_response_body=log_response_body,
                # python client arguments
                return_full_completion=return_full_completion,
            )
        return await self._generate_non_stream(
            self._endpoint,
            prompt,
            # platform arguments
            use_custom_keys=use_custom_keys,
            tags=tags,
            drop_params=drop_params,
            region=region,
            log_query_body=log_query_body,
            log_response_body=log_response_body,
            # python client arguments
            return_full_completion=return_full_completion,
            cache=cache,
        )

    def to_sync_client(self):
        """
        Return a synchronous version of the client (`Unify` instance), with the
        exact same configuration as this asynchronous (`AsyncUnify`) client.

        Returns:
            A `Unify` instance with the same configuration as this `AsyncUnify`
            instance.
        """
        return Unify(**self._constructor_args)
