import json
from unittest.mock import AsyncMock, MagicMock

import pytest
from pydantic import ValidationError

from unify.conversation_manager.medium_scripts.call import has_video_avatar_channel
from unify.conversation_manager.domains.fast_brain_mood import (
    FastBrainMood,
    FastBrainMoodClassification,
    FastBrainMoodClassifier,
)
from unify.conversation_manager.events import FastBrainMoodClassified
from unify.settings import SETTINGS


def test_mood_schema_accepts_requested_labels():
    for mood in (
        "neutral/happy",
        "apologetic/sad",
        "frustrated/angry",
        "bored",
    ):
        parsed = FastBrainMoodClassification.model_validate({"mood": mood})
        assert parsed.mood == FastBrainMood(mood)


def test_mood_schema_rejects_unknown_label():
    with pytest.raises(ValidationError):
        FastBrainMoodClassification.model_validate({"mood": "confused"})


def test_mood_classification_only_runs_for_video_avatar_channels():
    assert has_video_avatar_channel("unify_meet")
    assert has_video_avatar_channel("google_meet")
    assert has_video_avatar_channel("teams_meet")
    assert not has_video_avatar_channel("phone_call")
    assert not has_video_avatar_channel("whatsapp_call")
    assert not has_video_avatar_channel("phone")


def test_mood_classification_defaults_off():
    assert SETTINGS.conversation.FAST_BRAIN_MOOD_CLASSIFICATION_ENABLED is False


@pytest.mark.asyncio
async def test_classifier_uses_structured_output_and_user_message(monkeypatch):
    captured: dict = {}
    mock_client = MagicMock()
    mock_client.set_response_format = MagicMock()

    async def capture_generate(*, messages=None, **_kwargs):
        captured["messages"] = messages
        return '{"mood": "bored"}'

    mock_client.generate = AsyncMock(side_effect=capture_generate)

    def fake_new_llm_client(model, *, origin):
        captured["model"] = model
        captured["origin"] = origin
        return mock_client

    monkeypatch.setattr(
        "unify.conversation_manager.domains.fast_brain_mood.new_llm_client",
        fake_new_llm_client,
    )

    classifier = FastBrainMoodClassifier("gpt-5.5-mini@openai")
    result = await classifier.evaluate(
        transcript="User: hello\nAssistant: hi there",
        trigger_role="assistant",
        trigger_text="hi there",
    )

    assert result is not None
    assert result.mood == FastBrainMood.BORED
    assert result.avatar_mood == "bored"
    assert captured["model"] == "gpt-5.5-mini@openai"
    assert captured["origin"] == "FastBrain.mood_classification"
    mock_client.set_response_format.assert_called_once_with(FastBrainMoodClassification)
    assert any(msg["role"] == "user" for msg in captured["messages"])


def test_mood_event_payload_excludes_transcript():
    event = FastBrainMoodClassified(
        contact={"contact_id": 1},
        channel="unify_meet",
        mood="frustrated/angry",
        avatar_mood="frustrated",
        trigger_role="user",
        trigger_utterance_id="utt-123",
        turn_index=7,
        model="gpt-5.5-mini@openai",
    )

    payload = json.loads(event.to_json())["payload"]

    assert payload["mood"] == "frustrated/angry"
    assert payload["avatar_mood"] == "frustrated"
    assert payload["turn_index"] == 7
    assert "transcript" not in payload
