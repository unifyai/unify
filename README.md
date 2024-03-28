# Unify Python API LIBRARY
The Unify Python Package provides convenient acceess to the Unify REST API from any Python 3.7.1+ application.
It includes both Synchronous and Asynchronous clients as well as support for Streaming.

## Installation
This project uses poetry. It's a modern dependency management tool. To run the project use this set of commands:
```bash
poetry install
```

## Basic Usage
```python
import os
from unify import Unify
unify = Unify(
    # This is the default and can be omitted
    api_key=os.environ.get("UNIFY_KEY")
)
response = Unify.generate(messages="Hello Llama! Who was Isaac Newton?", model="llama-2-13b-chat", provider="anyscale")
```
`response` is a string containing the model's output. You can explore our list of supported models and providers through the [benchmarks interface](https://unify.ai/hub).


NOTE: While you can provide an `api_key` keyword argument,
we recommend using [python-dotenv](https://pypi.org/project/python-dotenv/)
to add `UNIFY_KEY="My API Key"` to your `.env` file
so that your API Key is not stored in source control.


 When a string is passed to the `messages` argument, it is assumed to be the user prompt. However, you can also pass a list of dictonaries containing the message history between
 the `user` as the `assistant`, as shown below:

 ```python
 messages=[
    {"role": "user", "content": "Who won the world series in 2020?"},
    {"role": "assistant", "content": "The Los Angeles Dodgers won the World Series in 2020."},
    {"role": "user", "content": "Where was it played?"}
]
res = unify.generate(messages=messages, model="llama-2-7b-chat", provider="anyscale")
 ```

## Async Usage
 Simply import `AsyncUnify` instead of `Unify` and use `await` with the `.generate` function.

 ```python
from unify import AsyncUnify
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
stream = Unify.generate(messages="Hello Llama! Who was Isaac Newton?", model="llama-2-13b-chat", provider="anyscale", stream=True)
for chunk in stream:
    print(x, end="")
```


The async client uses the exact same interface.
 ```python
from unify import AsyncUnify
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
