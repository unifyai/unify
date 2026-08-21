"""Unit tests for ``compact_reviewed_tool_results``.

After a storage review consolidates a stretch of a persistent session,
the raw tool payloads it covered are dead weight in every later dispatch.
The compactor stubs their *content* in place — identity, ordering and
tool_call pairing untouched — and re-baselines the watermark hash so the
append-only integrity check treats the rewrite as sanctioned.
"""

import json
from types import SimpleNamespace

from unify.common._async_tool.messages import (
    _REVIEW_COMPACTION_MARKER,
    compact_reviewed_tool_results,
)

BIG = "x" * 2000
SMALL = "ok"


def _client(messages):
    return SimpleNamespace(messages=messages)


def _tool_msg(content, name="execute_code", call_id="c1"):
    return {"role": "tool", "name": name, "tool_call_id": call_id, "content": content}


def test_stubs_large_final_tool_results_and_reports_savings():
    msgs = [
        {"role": "user", "content": "do it"},
        {"role": "assistant", "content": None, "tool_calls": [{"id": "c1"}]},
        _tool_msg(BIG),
        {"role": "assistant", "content": "done"},
    ]
    original_identity = msgs[2]
    saved = compact_reviewed_tool_results(_client(msgs), tool_message_count=1)
    assert saved > 1000
    assert msgs[2] is original_identity, "message identity must be preserved"
    assert _REVIEW_COMPACTION_MARKER in msgs[2]["content"]
    assert msgs[2]["content"].startswith("x" * 100)
    assert len(msgs[2]["content"]) < len(BIG)
    # User and assistant messages are never touched.
    assert msgs[0]["content"] == "do it"
    assert msgs[3]["content"] == "done"


def test_only_the_reviewed_prefix_is_compacted():
    msgs = [_tool_msg(BIG, call_id="c1"), _tool_msg(BIG, call_id="c2")]
    compact_reviewed_tool_results(_client(msgs), tool_message_count=1)
    assert _REVIEW_COMPACTION_MARKER in msgs[0]["content"]
    assert msgs[1]["content"] == BIG, "unreviewed tool results stay verbatim"


def test_small_placeholder_image_and_compacted_contents_are_skipped():
    placeholder = _tool_msg(json.dumps({"_placeholder": True, "pad": BIG}))
    already = _tool_msg(f"head\n… {_REVIEW_COMPACTION_MARKER} 1 chars omitted]" + BIG)
    with_image = _tool_msg(BIG + "[img:3]")
    small = _tool_msg(SMALL)
    msgs = [placeholder, already, with_image, small]
    before = [m["content"] for m in msgs]
    saved = compact_reviewed_tool_results(_client(msgs), tool_message_count=4)
    assert saved == 0
    assert [m["content"] for m in msgs] == before


def test_list_content_of_text_parts_is_flattened_and_stubbed():
    msgs = [
        _tool_msg(
            [
                {"type": "text", "text": BIG},
                {"type": "text", "text": "--- stdout ---"},
            ],
        ),
    ]
    saved = compact_reviewed_tool_results(_client(msgs), tool_message_count=1)
    assert saved > 1000
    assert isinstance(msgs[0]["content"], str)
    assert _REVIEW_COMPACTION_MARKER in msgs[0]["content"]


def test_list_content_with_non_text_parts_is_left_alone():
    content = [{"type": "text", "text": BIG}, {"type": "image_url", "url": "u"}]
    msgs = [_tool_msg(content)]
    saved = compact_reviewed_tool_results(_client(msgs), tool_message_count=1)
    assert saved == 0
    assert msgs[0]["content"] is content


def test_watermark_hash_rebaselined_under_invariant_checks(monkeypatch):
    from unify.common._async_tool import messages as messages_mod

    monkeypatch.setenv(messages_mod._INVARIANT_CHECKS_ENV, "1")
    msgs = [_tool_msg(BIG)]
    client = _client(msgs)
    client._sent_watermark = 1
    client._sent_watermark_hash = messages_mod._hash_msgs_slice(msgs[:1])
    stale = client._sent_watermark_hash

    compact_reviewed_tool_results(client, tool_message_count=1)

    assert client._sent_watermark_hash != stale
    assert client._sent_watermark_hash == messages_mod._hash_msgs_slice(msgs[:1])
