"""Tests for storage-review prompt compaction and layout.

The skill-librarian loops review what the actor *did*, not what it was
told it could do, and they can query the stores live with their own
tools. These tests pin the two guarantees that keep the review prompt
bounded and prompt-cache-friendly:

* ``_prepare_trajectory_for_storage_review`` — the trajectory snapshot
  drops the actor system prompt, collapses store-read results to entry
  counts (including results delivered via ``check_status_*``
  placeholders), and elides oversized tool output.
"""

from unify.actor.code_act_actor import (
    _prepare_trajectory_for_storage_review,
)

# ---------------------------------------------------------------------------
# Trajectory compaction
# ---------------------------------------------------------------------------


def test_large_system_prompt_is_stubbed_and_small_ones_kept():
    big = "### Role\n" + "x" * 10_000
    small = "## Parent Chat Context\nCame from a parent conversation."
    out = _prepare_trajectory_for_storage_review(
        [
            {"role": "system", "content": big},
            {"role": "system", "content": small},
            {"role": "user", "content": "do the thing"},
        ],
    )
    assert "x" * 100 not in out[0]["content"]
    assert "System prompt omitted" in out[0]["content"]
    assert f"{len(big):,} chars" in out[0]["content"]
    assert out[1]["content"] == small
    assert out[2]["content"] == "do the thing"


def test_store_read_results_collapse_to_entry_counts():
    entries = [
        {"guidance_id": i, "title": "t" * 40, "preview": "p" * 400} for i in range(12)
    ]
    import json as _json

    listing = _json.dumps(entries, indent=4)
    out = _prepare_trajectory_for_storage_review(
        [
            {
                "role": "assistant",
                "content": "",
                "tool_calls": [
                    {
                        "id": "call_1",
                        "function": {
                            "name": "GuidanceManager_search",
                            "arguments": "{}",
                        },
                    },
                ],
            },
            {
                "role": "tool",
                "tool_call_id": "call_1",
                "name": "GuidanceManager_search",
                "content": listing,
            },
        ],
    )
    stub = out[1]["content"]
    assert "GuidanceManager_search result omitted" in stub
    assert "12 entries" in stub
    assert "guidance_id" not in stub
    # The call itself stays visible — args carry the decision signal.
    assert out[0]["tool_calls"][0]["function"]["name"] == "GuidanceManager_search"


def test_short_store_read_results_stay_verbatim():
    out = _prepare_trajectory_for_storage_review(
        [
            {
                "role": "tool",
                "tool_call_id": "call_1",
                "name": "FunctionManager_search_functions",
                "content": "[]",
            },
        ],
    )
    # "Searched and found nothing" is the store-this signal; keep it.
    assert out[0]["content"] == "[]"


def test_check_status_indirection_resolves_to_store_read():
    listing = "[" + ",".join('{"name": "%d"}' % i for i in range(50)) + "]" + " " * 400
    out = _prepare_trajectory_for_storage_review(
        [
            {
                "role": "assistant",
                "content": "",
                "tool_calls": [
                    {
                        "id": "call_997376",
                        "function": {
                            "name": "FunctionManager_search_functions",
                            "arguments": '{"query": "settings"}',
                        },
                    },
                ],
            },
            {
                "role": "assistant",
                "content": "",
                "tool_calls": [
                    {
                        "id": "call_997376_completed",
                        "function": {
                            "name": "check_status_call_997376",
                            "arguments": "{}",
                        },
                    },
                ],
            },
            {
                "role": "tool",
                "tool_call_id": "call_997376_completed",
                "name": "check_status_call_997376",
                "content": listing,
            },
        ],
    )
    assert "FunctionManager_search_functions result omitted" in out[2]["content"]


def test_oversized_non_store_tool_results_keep_head_and_tail():
    content = "HEAD" + "m" * 20_000 + "TAIL"
    out = _prepare_trajectory_for_storage_review(
        [
            {
                "role": "tool",
                "tool_call_id": "call_9",
                "name": "check_status_call_9",
                "content": content,
            },
        ],
    )
    got = out[0]["content"]
    assert got.startswith("HEAD")
    assert got.endswith("TAIL")
    assert "chars omitted" in got
    assert len(got) < len(content)


def test_store_write_results_stay_verbatim():
    content = '{"stored": true, "function_id": 42}' + " " * 400
    out = _prepare_trajectory_for_storage_review(
        [
            {
                "role": "tool",
                "tool_call_id": "call_2",
                "name": "FunctionManager_add_functions",
                "content": content,
            },
        ],
    )
    assert out[0]["content"] == content


def test_oversized_list_content_tool_results_are_flattened_and_elided():
    # execute_code results arrive as OpenAI content-parts lists.
    big_text = "HEAD" + "m" * 20_000 + "TAIL"
    out = _prepare_trajectory_for_storage_review(
        [
            {
                "role": "tool",
                "tool_call_id": "call_7",
                "name": "check_status_call_7",
                "content": [{"type": "text", "text": big_text}],
            },
        ],
    )
    got = out[0]["content"]
    assert isinstance(got, str)
    assert got.startswith("HEAD")
    assert got.endswith("TAIL")
    assert "chars omitted" in got
    assert len(got) < len(big_text)


def test_small_list_content_tool_results_stay_untouched():
    content = [{"type": "text", "text": "ok: 42"}]
    out = _prepare_trajectory_for_storage_review(
        [
            {
                "role": "tool",
                "tool_call_id": "call_8",
                "name": "execute_code",
                "content": content,
            },
        ],
    )
    assert out[0]["content"] == content


def test_store_read_results_in_list_content_collapse():
    listing = "[" + ",".join('{"guidance_id": %d}' % i for i in range(9)) + "]"
    out = _prepare_trajectory_for_storage_review(
        [
            {
                "role": "tool",
                "tool_call_id": "call_9",
                "name": "GuidanceManager_search",
                "content": [{"type": "text", "text": listing + " " * 400}],
            },
        ],
    )
    assert "GuidanceManager_search result omitted" in out[0]["content"]


def test_input_messages_are_not_mutated():
    big = "### Role\n" + "x" * 10_000
    messages = [{"role": "system", "content": big}]
    _prepare_trajectory_for_storage_review(messages)
    assert messages[0]["content"] == big
