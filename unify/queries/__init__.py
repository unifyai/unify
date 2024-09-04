from pydantic import BaseModel
from typing import Optional, Iterable, Union, List, Dict
from openai.types.chat import (
    ChatCompletionToolParam,
    ChatCompletionToolChoiceOptionParam,
    ChatCompletionMessageParam,
)
from openai.types.chat.completion_create_params import ResponseFormat


class Query(BaseModel):
    messages: Optional[Iterable[ChatCompletionMessageParam]] = None,
    frequency_penalty: Optional[float] = None,
    logit_bias: Optional[Dict[str, int]] = None,
    logprobs: Optional[bool] = None,
    top_logprobs: Optional[int] = None,
    max_tokens: Optional[int] = 1024,
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
