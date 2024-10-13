# global
import abc
import asyncio
from typing import Dict, Iterable, List, Optional, Tuple, Union

import requests

# local
import unify

# noinspection PyProtectedMember
from openai._types import Headers, Query
from openai.types.chat import (
    ChatCompletionMessageParam,
    ChatCompletionToolChoiceOptionParam,
    ChatCompletionToolParam,
)
from openai.types.chat.completion_create_params import ResponseFormat
from typing_extensions import Self
from unify import BASE_URL
from unify.chat.clients import AsyncUnify, _Client, _UniLLMClient
from unify.utils.endpoint_metrics import Metrics

# noinspection PyProtectedMember
from unify.utils.helpers import _validate_api_key


class _MultiLLMClient(_Client, abc.ABC):

    def __init__(
        self,
        endpoints: Optional[Iterable[str]] = None,
        *,
        system_message: Optional[str] = None,
        messages: Optional[
            Union[
                List[ChatCompletionMessageParam],
                Dict[str, List[ChatCompletionMessageParam]],
            ]
        ] = None,
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
    ) -> None:
        """Initialize the Multi LLM Unify client.

        Args:
            endpoints: A list of endpoint names, with each name in OpenAI API format:
            <model_name>@<provider_name>. Defaults to None.

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
            stream=False,
            stream_options=None,
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
        endpoints = list(endpoints)
        self._api_key = _validate_api_key(api_key)
        self._endpoints = endpoints
        self._client_class = AsyncUnify
        self._clients = self._create_clients(endpoints)

    def _create_clients(self, endpoints: List[str]) -> Dict[str, AsyncUnify]:
        return {
            endpoint: self._client_class(
                endpoint,
                system_message=self.system_message,
                messages=self.messages,
                frequency_penalty=self.frequency_penalty,
                logit_bias=self.logit_bias,
                logprobs=self.logprobs,
                top_logprobs=self.top_logprobs,
                max_completion_tokens=self.max_completion_tokens,
                n=self.n,
                presence_penalty=self.presence_penalty,
                response_format=self.response_format,
                seed=self.seed,
                stop=self.stop,
                temperature=self.temperature,
                top_p=self.top_p,
                tools=self.tools,
                tool_choice=self.tool_choice,
                parallel_tool_calls=self.parallel_tool_calls,
                # platform arguments
                use_custom_keys=self.use_custom_keys,
                tags=self.tags,
                drop_params=self.drop_params,
                region=self.region,
                log_query_body=self.log_query_body,
                log_response_body=self.log_response_body,
                api_key=self._api_key,
                # python client arguments
                return_full_completion=self.return_full_completion,
                cache=self.cache,
                # passthrough arguments
                extra_headers=self.extra_headers,
                extra_query=self.extra_query,
                **self.extra_body,
            )
            for endpoint in endpoints
        }

    def add_endpoints(
        self,
        endpoints: Union[List[str], str],
        ignore_duplicates: bool = True,
    ) -> Self:
        """
        Add extra endpoints to be queried for each call to generate.

        Args:
            endpoints: The extra endpoints to add.

            ignore_duplicates: Whether or not to ignore duplicate endpoints passed.

        Returns:
            This client, useful for chaining inplace calls.
        """
        if isinstance(endpoints, str):
            endpoints = [endpoints]
        # remove duplicates
        if ignore_duplicates:
            endpoints = [
                endpoint for endpoint in endpoints if endpoint not in self._endpoints
            ]
        elif len(self._endpoints + endpoints) != len(set(self._endpoints + endpoints)):
            raise Exception(
                "at least one of the provided endpoints to add {}"
                "was already set present in the endpoints {}."
                "Set ignore_duplicates to True to ignore errors like this".format(
                    endpoints,
                    self._endpoints,
                ),
            )
        # update endpoints
        self._endpoints = self._endpoints + endpoints
        # create new clients
        self._clients.update(self._create_clients(endpoints))
        return self

    def remove_endpoints(
        self,
        endpoints: Union[List[str], str],
        ignore_missing: bool = True,
    ) -> Self:
        """
        Remove endpoints from the current list, which are queried for each call to
        generate.

        Args:
            endpoints: The extra endpoints to add.

            ignore_missing: Whether or not to ignore endpoints passed which are not
            currently present in the client endpoint list.

        Returns:
            This client, useful for chaining inplace calls.
        """
        if isinstance(endpoints, str):
            endpoints = [endpoints]
        # remove irrelevant
        if ignore_missing:
            endpoints = [
                endpoint for endpoint in endpoints if endpoint in self._endpoints
            ]
        elif len(self._endpoints) != len(set(self._endpoints + endpoints)):
            raise Exception(
                "at least one of the provided endpoints to remove {}"
                "was not present in the current endpoints {}."
                "Set ignore_missing to True to ignore errors like this".format(
                    endpoints,
                    self._endpoints,
                ),
            )
        # update endpoints and clients
        for endpoint in endpoints:
            self._endpoints.remove(endpoint)
            del self._clients[endpoint]
        return self

    def get_credit_balance(self) -> Union[float, None]:
        """
        Get the remaining credits left on your account.

        Returns:
            The remaining credits on the account if successful, otherwise None.
        Raises:
            BadRequestError: If there was an HTTP error.
            ValueError: If there was an error parsing the JSON response.
        """
        url = f"{BASE_URL}/credits"
        headers = {
            "accept": "application/json",
            "Authorization": f"Bearer {self._api_key}",
        }
        try:
            response = requests.get(url, headers=headers, timeout=10)
            response.raise_for_status()
            return response.json()["credits"]
        except requests.RequestException as e:
            raise Exception("There was an error with the request.") from e
        except (KeyError, ValueError) as e:
            raise ValueError("Error parsing JSON response.") from e

    # Read-only Properties #
    # ---------------------#

    def _get_metrics(self) -> Dict[str, Metrics]:
        return {
            ep: unify.get_endpoint_metrics(ep, api_key=self._api_key)[0]
            for ep in self._endpoints
        }

    @property
    def input_cost(self) -> Dict[str, float]:
        return {
            ep: metrics["input_cost"] for ep, metrics in self._get_metrics().items()
        }

    @property
    def output_cost(self) -> Dict[str, float]:
        return {
            ep: metrics["output_cost"] for ep, metrics in self._get_metrics().items()
        }

    @property
    def time_to_first_token(self) -> Dict[str, float]:
        return {
            ep: metrics["time_to_first_token"]
            for ep, metrics in self._get_metrics().items()
        }

    @property
    def inter_token_latency(self) -> Dict[str, float]:
        return {
            ep: metrics["inter_token_latency"]
            for ep, metrics in self._get_metrics().items()
        }

    # Settable Properties #
    # --------------------#

    @property
    def endpoints(self) -> Tuple[str, ...]:
        """
        Get the current tuple of endpoints.

        Returns:
            The tuple of endpoints.
        """
        return tuple(self._endpoints)

    @property
    def clients(self) -> Dict[str, _UniLLMClient]:
        """
        Get the current dictionary of clients, with endpoint names as keys and
        Unify or AsyncUnify instances as values.

        Returns:
            The dictionary of clients.
        """
        return self._clients

    # Representation #
    # ---------------#

    def __repr__(self):
        return "{}(endpoints={})".format(self.__class__.__name__, self._endpoints)

    def __str__(self):
        return "{}(endpoints={})".format(self.__class__.__name__, self._endpoints)


class MultiLLM(_MultiLLMClient):

    def _generate(  # noqa: WPS234, WPS211
        self,
        user_message: Optional[str] = None,
        system_message: Optional[str] = None,
        messages: Optional[
            Union[
                List[ChatCompletionMessageParam],
                Dict[str, List[ChatCompletionMessageParam]],
            ]
        ] = None,
        *,
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
        # python client arguments
        return_full_completion: bool = False,
        # passthrough arguments
        extra_headers: Optional[Headers] = None,
        extra_query: Optional[Query] = None,
        **kwargs,
    ) -> Dict[str, str]:
        kw = dict(
            user_message=user_message,
            system_message=system_message,
            messages=messages,
            max_completion_tokens=max_completion_tokens,
            stop=stop,
            temperature=temperature,
            frequency_penalty=frequency_penalty,
            logit_bias=logit_bias,
            logprobs=logprobs,
            top_logprobs=top_logprobs,
            n=n,
            presence_penalty=presence_penalty,
            response_format=response_format,
            seed=seed,
            top_p=top_p,
            tools=tools,
            tool_choice=tool_choice,
            parallel_tool_calls=parallel_tool_calls,
            use_custom_keys=use_custom_keys,
            tags=tags,
            drop_params=drop_params,
            region=region,
            log_query_body=log_query_body,
            log_response_body=log_response_body,
            return_full_completion=return_full_completion,
            extra_headers=extra_headers,
            extra_query=extra_query,
            **kwargs,
        )

        # noinspection DuplicatedCode
        async def gen(kw_):
            multi_message = isinstance(messages, dict)
            kw_ = {k: v for k, v in kw_.items() if v is not None}
            responses = dict()
            for endpoint, client in self._clients.items():
                these_kw = kw_.copy()
                if multi_message:
                    these_kw["messages"] = these_kw["messages"][endpoint]
                responses[endpoint] = await client.generate(**these_kw)
            return responses

        return asyncio.run(gen(kw))

    def to_async_client(self):
        """
        Return an asynchronous version of the client (`AsyncMultiLLM` instance), with
        the exact same configuration as this synchronous (`MultiLLM`) client.

        Returns:
            An `AsyncMultiLLM` instance with the same configuration as this `MultiLLM`
            instance.
        """
        return AsyncMultiLLM(**self._constructor_args)


class AsyncMultiLLM(_MultiLLMClient):

    async def _generate(  # noqa: WPS234, WPS211
        self,
        user_message: Optional[str] = None,
        system_message: Optional[str] = None,
        messages: Optional[
            Union[
                List[ChatCompletionMessageParam],
                Dict[str, List[ChatCompletionMessageParam]],
            ]
        ] = None,
        *,
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
        # python client arguments
        return_full_completion: bool = False,
        # passthrough arguments
        extra_headers: Optional[Headers] = None,
        extra_query: Optional[Query] = None,
        **kwargs,
    ) -> Dict[str, str]:
        kw = dict(
            user_message=user_message,
            system_message=system_message,
            messages=messages,
            max_completion_tokens=max_completion_tokens,
            stop=stop,
            temperature=temperature,
            frequency_penalty=frequency_penalty,
            logit_bias=logit_bias,
            logprobs=logprobs,
            top_logprobs=top_logprobs,
            n=n,
            presence_penalty=presence_penalty,
            response_format=response_format,
            seed=seed,
            top_p=top_p,
            tools=tools,
            tool_choice=tool_choice,
            parallel_tool_calls=parallel_tool_calls,
            use_custom_keys=use_custom_keys,
            tags=tags,
            drop_params=drop_params,
            region=region,
            log_query_body=log_query_body,
            log_response_body=log_response_body,
            return_full_completion=return_full_completion,
            extra_headers=extra_headers,
            extra_query=extra_query,
            **kwargs,
        )
        multi_message = isinstance(messages, dict)
        kw = {k: v for k, v in kw.items() if v is not None}
        responses = dict()
        for endpoint, client in self._clients.items():
            these_kw = kw.copy()
            if multi_message:
                these_kw["messages"] = these_kw["messages"][endpoint]
            responses[endpoint] = await client.generate(**these_kw)
        return responses

    def to_sync_client(self):
        """
        Return a synchronous version of the client (`MultiLLM` instance), with the
        exact same configuration as this asynchronous (`AsyncMultiLLM`) client.

        Returns:
            A `MultiLLM` instance with the same configuration as this `AsyncMultiLLM`
            instance.
        """
        return MultiLLM(**self._constructor_args)
