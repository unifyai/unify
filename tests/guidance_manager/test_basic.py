from __future__ import annotations

import pytest

from unify.guidance_manager.guidance_manager import GuidanceManager
from unify.guidance_manager.types.guidance import Guidance
from tests.helpers import _handle_project


def test_guidance_legacy_null_is_builtin_normalizes_to_false():
    row = Guidance(
        title="Legacy guidance",
        content="Created before builtins.",
        is_builtin=None,
    )

    assert row.is_builtin is False


@_handle_project
def test_create():
    gm = GuidanceManager()
    out = gm.add_guidance(
        title="Setup demo",
        content="Steps to set up the product demo.",
    )
    gid = out["details"]["guidance_id"]

    rows = gm.filter(filter=f"guidance_id == {gid}")
    assert rows and rows[0].guidance_id == gid
    assert rows[0].title == "Setup demo"
    assert rows[0].content.startswith("Steps to set up")
    assert rows[0].function_ids == []


@_handle_project
def test_update():
    gm = GuidanceManager()
    gid = gm.add_guidance(
        title="Onboarding Overview",
        content="We walk through onboarding steps.",
    )["details"]["guidance_id"]

    gm.update_guidance(
        guidance_id=gid,
        content="Updated walkthrough of onboarding steps for new users.",
    )

    rows = gm.filter(filter=f"guidance_id == {gid}")
    assert rows and rows[0].guidance_id == gid
    assert "Updated walkthrough" in rows[0].content
    assert rows[0].title == "Onboarding Overview"


@_handle_project
def test_delete():
    gm = GuidanceManager()
    gid = gm.add_guidance(
        title="Billing",
        content="Explains invoices and payment flows.",
    )["details"]["guidance_id"]

    # ensure present
    assert gm.filter(filter=f"guidance_id == {gid}")

    gm.delete_guidance(guidance_id=gid)
    assert len(gm.filter(filter=f"guidance_id == {gid}")) == 0


@_handle_project
def test_list_columns_and_filter():
    gm = GuidanceManager()
    cols = gm._list_columns()
    # Basic schema keys should be present
    for key in ("guidance_id", "title", "content", "function_ids"):
        assert key in cols

    gm.add_guidance(title="Comms", content="Prefer emails for updates")
    gm.add_guidance(title="Ops", content="Runbooks and SOPs")

    rows = gm.filter(filter="title == 'Comms'")
    assert rows and rows[0].title == "Comms"


@_handle_project
def test_add_requires_title_or_content():
    gm = GuidanceManager()

    with pytest.raises(ValueError):
        gm.add_guidance()


@_handle_project
def test_update_requires_a_field():
    gm = GuidanceManager()
    gid = gm.add_guidance(
        title="Docs",
        content="Documentation structure and guidelines.",
    )["details"]["guidance_id"]

    with pytest.raises(ValueError):
        gm.update_guidance(guidance_id=gid)


@_handle_project
def test_clear():
    gm = GuidanceManager()

    # Seed a couple of guidance entries
    out1 = gm.add_guidance(title="Alpha", content="First entry")
    out2 = gm.add_guidance(title="Beta", content="Second entry")
    gid1 = out1["details"]["guidance_id"]
    gid2 = out2["details"]["guidance_id"]

    # Sanity: entries present before clear
    assert gm.filter(filter=f"guidance_id == {gid1}")
    assert gm.filter(filter=f"guidance_id == {gid2}")

    # Execute clear
    gm.clear()

    # After clear: schema should be present again
    cols = gm._list_columns()
    for key in ("guidance_id", "title", "content", "function_ids"):
        assert key in cols

    # All prior guidance entries should be gone
    remaining_1 = gm.filter(filter=f"guidance_id == {gid1}")
    remaining_2 = gm.filter(filter=f"guidance_id == {gid2}")
    assert len(remaining_1) == 0
    assert len(remaining_2) == 0
