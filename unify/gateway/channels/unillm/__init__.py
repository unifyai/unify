"""UniLLM channel: OpenAI-compatible chat completions proxy.

Mirrors ``communication/unillm/{views,schema}.py``. Single
``router`` with one endpoint (``POST /chat/completions``) that
authenticates the caller's user API key against Orchestra and
forwards the request to UniLLM with caching / cost tracking /
multi-provider support.

Smallest channel migration: 115 LOC views + 81 LOC pydantic
schemas. The two auth helpers
(``authenticate_user_api_key`` + ``extract_api_key``) are inlined
since unillm is the only public-facing user-API-keyed channel and
there's no second consumer yet to justify a common module.
"""

from unify.gateway.channels.unillm.views import router

__all__ = ["router"]
