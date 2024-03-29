import os

from unify import Unify

unify = Unify(
    # This is the default and optional to include.
    api_key=os.environ.get("UNIFY_KEY"),
)
response = unify.generate(
    messages="Hello Llama! Who was Isaac Newton?",
    model="llama-chat",
    provider="anyscale",
)
