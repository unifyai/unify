"""Tests for response_format normalization and structured-output gating."""

from __future__ import annotations

import json

import pytest
from pydantic import BaseModel, Field

from unify.common._async_tool.loop import _check_valid_response_format
from unify.common._async_tool.response_format import (
    normalize_response_format,
    try_normalize_response_format,
)


class _Greeting(BaseModel):
    message: str = Field(...)
    number: int = Field(...)


ALICE_MATCHES_SCHEMA = {
    "type": "object",
    "properties": {
        "matches": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "contact_id": {"type": "integer"},
                    "first_name": {"type": ["string", "null"]},
                    "surname": {"type": ["string", "null"]},
                    "email_address": {"type": ["string", "null"]},
                },
                "required": ["contact_id", "first_name", "surname"],
            },
        },
    },
    "required": ["matches"],
}


def test_normalize_pydantic_model():
    norm = normalize_response_format(_Greeting)
    assert norm is not None
    assert norm.pydantic_model is _Greeting
    schema = norm.answer_json_schema
    assert "message" in schema["properties"]
    assert "number" in schema["properties"]
    validated = norm.validate({"message": "hi", "number": 7})
    assert isinstance(validated, _Greeting)
    assert validated.message == "hi"


def test_normalize_json_schema_dict_like_nested_ask_runaway():
    """Accept the OpenAI-style JSON Schema the update LLM passed into nested ask."""
    norm = normalize_response_format(ALICE_MATCHES_SCHEMA)
    assert norm is not None
    assert norm.pydantic_model is None
    assert norm.json_schema == ALICE_MATCHES_SCHEMA
    assert norm.answer_json_schema == ALICE_MATCHES_SCHEMA

    payload = {
        "matches": [
            {
                "contact_id": 6,
                "first_name": "Alice",
                "surname": "Wonder",
                "email_address": "alice.wonder@example.com",
            },
        ],
    }
    assert norm.validate(payload) == payload
    assert norm.parse_result(json.dumps(payload)) == payload


def test_normalize_json_schema_string():
    raw = json.dumps(ALICE_MATCHES_SCHEMA)
    norm = normalize_response_format(raw)
    assert norm is not None
    assert norm.json_schema == ALICE_MATCHES_SCHEMA
    # Schema extraction used by final_response injection must succeed.
    assert _check_valid_response_format(raw) == ALICE_MATCHES_SCHEMA


def test_normalize_simplified_field_map():
    norm = normalize_response_format({"email": "string", "age": "integer"})
    assert norm is not None
    assert norm.pydantic_model is not None
    validated = norm.validate({"email": "a@b.com", "age": 3})
    assert validated.email == "a@b.com"
    assert validated.age == 3


def test_normalize_rejects_unsupported_values():
    with pytest.raises((TypeError, ValueError, json.JSONDecodeError)):
        normalize_response_format(123)
    with pytest.raises((TypeError, ValueError, json.JSONDecodeError)):
        normalize_response_format("not-json")
    assert try_normalize_response_format(123) is None
    assert try_normalize_response_format(None) is None


def test_json_schema_validation_rejects_bad_payload():
    norm = normalize_response_format(ALICE_MATCHES_SCHEMA)
    assert norm is not None
    with pytest.raises(Exception):
        norm.validate({"matches": [{"first_name": "Alice"}]})  # missing required


def test_check_valid_response_format_accepts_schema_dict_and_model():
    assert _check_valid_response_format(ALICE_MATCHES_SCHEMA) == ALICE_MATCHES_SCHEMA
    schema = _check_valid_response_format(_Greeting)
    assert "message" in schema["properties"]
