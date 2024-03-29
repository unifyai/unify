# Unify Python API Library
The Unify Python Package provides access to the [Unify](https://unify.ai) REST API, allowing you to query Large Language Models (LLMs)
from any Python 3.7.1+ application.
It includes Synchronous and Asynchronous clients with Streaming responses support.

Just like the REST API, you can:

- 🔑 **Use any endpoint with one key**: Access all LLMs at any provider with just one Unify API Key.

  
- 🚀 **Route to the best endpoint**: Each prompt is sent to the endpoint that will yield the best
  performance for your target metric, including high-throughput, low cost or low latency. See
  [the routing section](#dynamic-routing) to learn more about this!

## Installation
You can use pip to install the package as follows:
```bash
pip install .
```

## Basic Usage
```python
import os
from unify import Unify
unify = Unify(
    # This is the default and can be omitted
    api_key=os.environ.get("UNIFY_KEY")
)
response = unify.generate(messages="Hello Llama! Who was Isaac Newton?", model="llama-2-13b-chat", provider="anyscale")
```

Here, `response` is a string containing the model's output. 

### Supported models
The list of supported models and providers is available in [the platform](https://unify.ai/hub).

### API Key
You can get an API Key from [the Unify console](https://console.unify.ai/)

> [!NOTE]
> You can provide an `api_key` keyword argument, but
> we recommend using [python-dotenv](https://pypi.org/project/python-dotenv/)
> to add `UNIFY_KEY="My API Key"` to your `.env` file
> so that your API Key is not stored in source control.

### Sending multiple messages

 When a string is passed to the `messages` argument, it is assumed to be the user prompt. However, you can also pass a list of dictionaries containing the message history between
 the `user` and the `assistant`, as shown below:

 ```python
 messages=[
    {"role": "user", "content": "Who won the world series in 2020?"},
    {"role": "assistant", "content": "The Los Angeles Dodgers won the World Series in 2020."},
    {"role": "user", "content": "Where was it played?"}
]
res = unify.generate(messages=messages, model="llama-2-7b-chat", provider="anyscale")
 ```

## Dynamic Routing
TODO

## Async Usage
 Simply import `AsyncUnify` instead of `Unify` and use `await` with the `.generate` function.

 ```python
from unify import AsyncUnify
import os
import asyncio
async_unify = AsyncUnify(
    # This is the default and can be omitted
    api_key=os.environ.get("UNIFY_KEY")
)

async def main():
    responses = await async_unify.generate(messages="Hello Llama! Who was Isaac Newton?", model="llama-2-13b-chat", provider="anyscale")

asyncio.run(main())
```

Functionality between the synchronous and asynchronous clients is otherwise identical.

## Streaming Responses

We provide support for streaming responses using Server Side Events (SSE).

```python
import os
from unify import Unify
unify = Unify(
    # This is the default and can be omitted
    api_key=os.environ.get("UNIFY_KEY")
)
stream = unify.generate(messages="Hello Llama! Who was Isaac Newton?", model="llama-2-13b-chat", provider="anyscale", stream=True)
for chunk in stream:
    print(x, end="")
```


The async client uses the exact same interface.
 ```python
from unify import AsyncUnify
import os
import asyncio
async_unify = AsyncUnify(
    # This is the default and can be omitted
    api_key=os.environ.get("UNIFY_KEY")
)

async def main():
    async_stream = await async_unify.generate(messages="Hello Llama! Who was Isaac Newton?", model="llama-2-13b-chat", provider="anyscale", stream=True)
    async for chunk in async_stream:
        print(chunk, end="")

asyncio.run(main())
```
