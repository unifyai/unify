"""Normalize and validate async-tool-loop ``response_format`` values.

Callers may pass:
- a Pydantic ``BaseModel`` subclass
- a JSON Schema dict (OpenAI-style ``type`` / ``properties`` / …)
- a JSON string encoding either of the above dict forms
- a simplified ``{field: type_name}`` dict (same shape as CM ``act``)

The tool loop uses the normalized form both to inject ``final_response`` /
``send_response`` and to validate the submitted payload.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Optional

import jsonschema
from pydantic import BaseModel, create_model

_SCHEMA_TYPE_MAP: dict[str, type] = {
    "string": str,
    "str": str,
    "integer": int,
    "int": int,
    "number": float,
    "float": float,
    "boolean": bool,
    "bool": bool,
}


def _looks_like_json_schema(schema: dict[str, Any]) -> bool:
    """Return True for OpenAI / JSON-Schema object shapes (not simplified maps)."""
    if any(
        key in schema
        for key in (
            "$schema",
            "$defs",
            "definitions",
            "properties",
            "items",
            "anyOf",
            "oneOf",
            "allOf",
            "additionalProperties",
        )
    ):
        return True
    schema_type = schema.get("type")
    if schema_type in (
        "object",
        "array",
        "string",
        "number",
        "integer",
        "boolean",
        "null",
    ):
        return True
    if isinstance(schema_type, list):
        return True
    return False


def _resolve_simplified_type(schema: Any, name_hint: str) -> type:
    if isinstance(schema, str):
        return _SCHEMA_TYPE_MAP.get(schema.lower(), str)
    if isinstance(schema, dict):
        return _simplified_dict_to_model(schema, name_hint)
    if isinstance(schema, list) and schema:
        item_type = _resolve_simplified_type(schema[0], f"{name_hint}Item")
        return list[item_type]  # type: ignore[valid-type]
    return str


def _simplified_dict_to_model(
    schema: dict[str, Any],
    model_name: str = "ResponseFormat",
) -> type[BaseModel]:
    fields: dict[str, Any] = {}
    for field_name, field_schema in schema.items():
        field_type = _resolve_simplified_type(
            field_schema,
            f"{model_name}{field_name.title().replace('_', '')}",
        )
        fields[field_name] = (field_type, ...)
    return create_model(model_name, **fields)  # type: ignore[call-overload]


@dataclass(frozen=True)
class NormalizedResponseFormat:
    """Resolved response format used by the async tool loop."""

    answer_json_schema: dict[str, Any]
    pydantic_model: Optional[type[BaseModel]] = None
    json_schema: Optional[dict[str, Any]] = None

    def validate(self, payload: Any) -> Any:
        """Validate *payload* and return the typed / canonical value."""
        if self.pydantic_model is not None:
            return self.pydantic_model.model_validate(payload)
        if self.json_schema is not None:
            jsonschema.validate(instance=payload, schema=self.json_schema)
            return payload
        raise TypeError("NormalizedResponseFormat has no validator configured.")

    def parse_result(self, raw: str) -> Any:
        """Parse a JSON string result from the loop into the typed value."""
        if self.pydantic_model is not None:
            return self.pydantic_model.model_validate_json(raw)
        data = json.loads(raw)
        return self.validate(data)


def normalize_response_format(
    response_format: Any,
) -> Optional[NormalizedResponseFormat]:
    """Normalize a caller- or LLM-supplied ``response_format`` value.

    Returns ``None`` when *response_format* is ``None``. Raises ``TypeError`` /
    ``ValueError`` / ``json.JSONDecodeError`` for unsupported values so the
    caller can decide whether to disable structured-output mode.
    """
    if response_format is None:
        return None

    if isinstance(response_format, type) and issubclass(response_format, BaseModel):
        return NormalizedResponseFormat(
            answer_json_schema=response_format.model_json_schema(),
            pydantic_model=response_format,
        )

    value: Any = response_format
    if isinstance(value, str):
        value = json.loads(value)

    if not isinstance(value, dict):
        raise TypeError(
            "response_format must be a Pydantic BaseModel subclass, a JSON "
            "Schema dict, a simplified {field: type} dict, or a JSON string "
            f"encoding one of those dicts; got {type(response_format)!r}.",
        )

    if _looks_like_json_schema(value):
        return NormalizedResponseFormat(
            answer_json_schema=value,
            json_schema=value,
        )

    model = _simplified_dict_to_model(value)
    return NormalizedResponseFormat(
        answer_json_schema=model.model_json_schema(),
        pydantic_model=model,
    )


def try_normalize_response_format(
    response_format: Any,
) -> Optional[NormalizedResponseFormat]:
    """Like ``normalize_response_format`` but returns ``None`` on failure."""
    if response_format is None:
        return None
    try:
        return normalize_response_format(response_format)
    except Exception:
        return None
