#Unify Python API LIBRARY

The Unify Python Package provides convenient acceess to the Unify REST API from any Python 3.7+ application.
It includes Synchronous and Asynchronous clients as well as support for Streaming.

## Installation 
TODO

## Usage
```python
import os
from unify import Unify
unify = Unify(
    # This is the default and can be omitted
    api_key=os.environ.get("UNIFY_API_KEY")
)
response = Unify.generate("Hello Llama! Who was Isaac Newton?", "llama-2-13b-chat", "anyscale")

You can explore our list of supported models and providers through the [benchmarks interface](https://unify.ai/hub).

Note: While you can provide an `api_key` keyword argument,
we recommend using [python-dotenv](https://pypi.org/project/python-dotenv/)
to add `UNIFY_API_KEY="My API Key"` to your `.env` file
so that your API Key is not stored in source control.
```