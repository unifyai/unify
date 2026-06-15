from __future__ import annotations

import inspect
import json
import textwrap
from typing import Any

from pydantic import BaseModel

from unity.common.llm_client import new_llm_client

DEFAULT_REASONING_SYSTEM = (
    "You are a focused semantic reasoning subroutine inside a larger Python "
    "workflow. Make the requested judgment from the supplied evidence only. "
    "Prefer stable, concise answers that downstream symbolic code can use."
)


def _is_pydantic_model(response_format: Any) -> bool:
    return isinstance(response_format, type) and issubclass(response_format, BaseModel)


def _parse_response(result: Any, response_format: Any) -> Any:
    if response_format is None:
        return result

    if _is_pydantic_model(response_format):
        if isinstance(result, response_format):
            return result
        if isinstance(result, str):
            return response_format.model_validate_json(result)
        return response_format.model_validate(result)

    if isinstance(response_format, dict) and isinstance(result, str):
        return json.loads(result)

    return result


def get_reasoning_prompt_context() -> str:
    """Return actor-facing documentation for the sandbox reasoning helper."""

    doc = inspect.getdoc(reason) or ""
    prefix = "async def " if inspect.iscoroutinefunction(reason) else "def "
    signature = f"{prefix}{reason.__name__}{inspect.signature(reason)}"
    return (
        "### Semantic Reasoning Helper: `reason(...)`\n\n"
        "`reason(...)` is available inside `execute_code` Python sessions and "
        "stored Python functions. It is a normal sandbox helper, not a JSON "
        "tool call.\n\n"
        f"```python\n{signature}\n```\n\n"
        f"{doc}\n\n"
        f"{get_reasoning_model_selection_context()}"
    )


def _collect_supported_reasoning_endpoints() -> dict[str, list[str]]:
    """Return supported UniLLM endpoints grouped by provider.

    Prefer the public ``unillm.endpoints.list_endpoints`` helper. The fallback
    keeps prompt rendering compatible with older editable checkouts while the
    public helper rolls out across sibling repos.
    """

    try:
        import unillm.endpoints  # noqa: F401  # populate provider registries

        try:
            from unillm.endpoints import list_endpoints

            endpoints = list_endpoints()
        except (ImportError, AttributeError):
            from unillm.endpoints.utils import _MODEL_ALIAS_MAP

            endpoints = sorted(_MODEL_ALIAS_MAP)
    except Exception:
        return {}

    grouped: dict[str, list[str]] = {}
    for endpoint in endpoints:
        if "@" not in endpoint:
            continue
        _model, provider = endpoint.rsplit("@", 1)
        grouped.setdefault(provider, []).append(endpoint)
    return {provider: sorted(values) for provider, values in sorted(grouped.items())}


def _format_endpoint_catalog(grouped: dict[str, list[str]]) -> str:
    if not grouped:
        return (
            "Supported endpoint catalog could not be loaded in this runtime. "
            'Use known UniLLM endpoint strings in `model="model@provider"` form, '
            "or inspect `unillm.endpoints` before choosing a model."
        )

    lines = [
        "Supported UniLLM endpoints currently registered in this runtime:",
    ]
    for provider, endpoints in grouped.items():
        lines.append(f"- {provider}: {', '.join(endpoints)}")
    return "\n".join(lines)


def get_reasoning_model_selection_context() -> str:
    """Return model-selection guidance plus a dynamic UniLLM endpoint catalog."""

    endpoint_catalog = _format_endpoint_catalog(
        _collect_supported_reasoning_endpoints(),
    )
    guidance = textwrap.dedent(
        """
        ### Choosing A Model For `reason(...)`

        Pass model overrides as UniLLM endpoint strings, e.g.
        `model="gpt-4.1-nano@openai"`.

        For durable or recurring stored functions, choose `model=` deliberately.
        Do not silently inherit the default high-reasoning model for bounded,
        repeated classification/routing/extraction work unless that capability
        is genuinely needed.

        Use current external evidence when the model choice matters:
        - Artificial Analysis: https://artificialanalysis.ai/
        - ARC Prize leaderboard: https://arcprize.org/leaderboard
        - General web search for recent benchmark, pricing, latency, and
          reliability information.

        Do this research while authoring or storing the function, then bake the
        selected endpoint into the function. Do not put benchmark browsing or
        model shopping inside the hot path of a recurring task.

        Practical defaults:
        - Use cheap/fast models for bounded classification, routing, extraction,
          confidence scoring, and yes/no decisions after deterministic
          pre-filtering.
        - Use a mid-tier model for short user-facing synthesis or draft wording
          where quality matters but the task is still narrow.
        - Use the default strong model for ambiguous, high-stakes, policy-heavy,
          or poorly specified judgment, or as a fallback when cheaper models fail
          validation.
        - Prefer `temperature=0.0` and structured `response_format` for decisions
          that downstream Python branches on.
        - In stored functions, record the model-choice rationale in the docstring
          or a short code comment.
        """,
    ).strip()

    return f"{guidance}\n\n{endpoint_catalog}"


async def reason(
    prompt: str,
    *,
    system: str | None = None,
    response_format: type[BaseModel] | dict[str, Any] | None = None,
    model: str | None = None,
    origin: str = "CodeActActor.reason",
    temperature: float = 0.0,
    client_kwargs: dict[str, Any] | None = None,
    **generate_kwargs: Any,
) -> str | BaseModel | dict[str, Any]:
    """Run a one-shot semantic reasoning step from generated Python code.

    Use ``reason(...)`` when the code you are writing needs to process
    unstructured meaning, not merely manipulate exact values. Treat UniLLM as a
    first-class fuzzy processor inside a broader deterministic workflow:
    Python handles retrieval, iteration, batching, grouping, date arithmetic,
    API calls, validation, persistence, and side effects; ``reason(...)``
    handles bounded unstructured-data work that would be brittle if implemented
    as keyword matching or canned templates.

    Good uses
    ---------
    - Unstructured -> structured work: classify, extract, score, route, decide,
      summarize into fields, or choose an action from text, images, documents,
      tickets, emails, transcripts, notes, lead records, or web pages.
    - Unstructured -> unstructured work: draft, respond, rewrite, synthesize,
      explain, personalize, compress, or adapt human-facing text.
    - Classifying emails, tickets, documents, notes, or leads into broad
      categories based on meaning.
    - Deciding whether a message needs a reply, follow-up, escalation, or
      user review.
    - Judging relevance, intent, priority, sentiment, ambiguity, or policy fit
      from unstructured text.
    - Applying a stable rubric inside a loop, then feeding structured results
      back into deterministic Python control flow.

    Prefer direct symbolic code instead
    -----------------------------------
    Do not spend an LLM call for substeps where exact logic is enough: direct
    primitive calls, exact lookups, deterministic filtering, arithmetic, date
    comparisons, dedupe, schema reshaping, and simple transformations should
    stay in normal Python or use the relevant primitive directly. A generated
    function can freely mix deterministic substeps and semantic substeps; use
    ``reason(...)`` only where meaning-based judgment is doing real work.

    Examples
    --------
    Minimal judgment returning text::

        verdict = await reason(
            "Does this email require a reply? Answer yes, no, or unsure.\\n"
            f"Subject: {email['subject']}\\nBody: {email['body']}"
        )

    Structured output for downstream control flow::

        from pydantic import BaseModel, Field

        class EmailClassification(BaseModel):
            category: str = Field(description="billing, scheduling, hiring, personal, or other")
            needs_reply: bool
            confidence: float = Field(ge=0.0, le=1.0)
            rationale: str

        EmailClassification.model_rebuild()

        classification = await reason(
            f"Classify this email for inbox triage.\\nSubject: {subject}\\nBody: {body}",
            response_format=EmailClassification,
        )
        if classification.needs_reply and classification.confidence >= 0.8:
            to_reply.append(email)

    Structured triage plus draft generation::

        from pydantic import BaseModel, Field

        class EmailDraftDecision(BaseModel):
            category: str
            needs_reply: bool
            draft_reply: str | None = Field(
                description="Short human-reviewable draft, or null if no reply is needed"
            )
            confidence: float = Field(ge=0.0, le=1.0)
            rationale: str

        EmailDraftDecision.model_rebuild()

        decision = await reason(
            "Decide whether this email needs a reply. If it does, draft a concise reply "
            "in the user's voice. Return null for draft_reply when no reply is needed.\\n"
            f"Subject: {subject}\\nFrom: {sender}\\nBody: {body}",
            response_format=EmailDraftDecision,
            model="gpt-4.1-nano@openai",
        )

    Custom rubric with ``system`` for consistent bulk classification::

        system = (
            "Classify messages using the user's existing labels. "
            "Prefer 'needs_user_review' when the evidence is ambiguous."
        )
        decision = await reason(
            prompt=email_text,
            system=system,
            response_format=EmailClassification,
        )

    Model and generation options
    ----------------------------
    By default this uses ``new_llm_client(model, async_client=True,
    stateful=False, origin='CodeActActor.reason')`` and calls
    ``generate(..., temperature=0.0)`` for stable judgments. Override
    ``model`` only when the task has a real capability or cost reason. Raise
    ``temperature`` only when creative synthesis is more useful than stable
    classification. Pass advanced UniLLM generation options through
    ``generate_kwargs``; keep ordinary actor-written code simple.

    Anti-patterns
    -------------
    - Replacing exact deterministic substeps with ``reason(...)``.
    - Using substring checks as the whole classifier for semantic tasks, e.g.
      ``if "urgent" in subject.lower()`` for inbox triage. Exact lexical
      signals can help pre-filter, but they are not semantic judgment.
    - Replacing human-facing drafting, rewriting, or personalization with
      label-specific canned prose or templates unless the user explicitly asked
      for fixed deterministic templates.
    - Calling ``reason(...)`` for every item in a large set before cheap
      deterministic pre-filtering, sampling, or batching.

    Cost and observability
    ----------------------
    This performs a billable UniLLM call. Because it is built on
    ``new_llm_client``, normal UniLLM caching, logging, cost tracking, event
    hooks, spending limits, and billing attribution apply. Keep prompts compact
    and use structured outputs when Python needs to branch on the result.
    """

    client_config: dict[str, Any] = {
        "async_client": True,
        "stateful": False,
        "origin": origin,
    }
    if client_kwargs:
        client_config.update(client_kwargs)

    client = new_llm_client(model, **client_config)
    result = client.generate(
        user_message=prompt,
        system_message=system or DEFAULT_REASONING_SYSTEM,
        response_format=response_format,
        temperature=temperature,
        **generate_kwargs,
    )
    if inspect.isawaitable(result):
        result = await result

    return _parse_response(result, response_format)
