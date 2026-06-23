"""
tests/conversation_manager/core/test_prompt_builders.py
=======================================================

Unit tests for the ConversationManager prompt builder, focusing on
capability-gated sections (assistant phone / email).
"""

from __future__ import annotations

import pytest

from droid.conversation_manager.prompt_builders import (
    build_system_prompt,
    build_voice_agent_prompt,
)
from droid.session_details import TeamSummary

pytestmark = pytest.mark.no_unify_context

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_BASE_KWARGS: dict = {
    "bio": "A helpful assistant.",
    "contact_id": 1,
    "first_name": "Alice",
    "surname": "Smith",
}


def _build(**overrides: object) -> str:
    """Build a system prompt with sensible defaults, returning flat text."""
    kwargs = {**_BASE_KWARGS, **overrides}
    return build_system_prompt(**kwargs).flatten()


def _build_voice(**overrides: object) -> str:
    """Build a voice-agent prompt with stable defaults."""
    kwargs = {
        "bio": "I help Acme configure its Unify team.",
        "assistant_name": "Avery",
        "boss_first_name": "Dana",
        "boss_surname": "Owner",
        **overrides,
    }
    return build_voice_agent_prompt(**kwargs).flatten()


# ---------------------------------------------------------------------------
# Tests – tool listing
# ---------------------------------------------------------------------------


class TestCommsToolListing:
    """The output-format section lists only the comms tools the assistant can use."""

    def test_all_tools_listed_when_fully_configured(self):
        prompt = _build(assistant_has_phone=True, assistant_has_email=True)
        assert "`send_sms`" in prompt
        assert "`send_email`" in prompt
        assert "`make_call`" in prompt
        assert "`send_unify_message`" in prompt

    def test_phone_tools_absent_without_phone(self):
        prompt = _build(assistant_has_phone=False, assistant_has_email=True)
        assert "`send_sms`: Send an SMS" not in prompt
        assert "`make_call`: Start an outbound" not in prompt
        assert "`send_email`" in prompt
        assert "`send_unify_message`" in prompt

    def test_email_tool_absent_without_email(self):
        prompt = _build(assistant_has_phone=True, assistant_has_email=False)
        assert "`send_email`: Send an email" not in prompt
        assert "`send_sms`" in prompt
        assert "`make_call`" in prompt
        assert "`send_unify_message`" in prompt

    def test_only_unify_message_without_any_capabilities(self):
        prompt = _build(assistant_has_phone=False, assistant_has_email=False)
        assert "`send_sms`: Send an SMS" not in prompt
        assert "`send_email`: Send an email" not in prompt
        assert "`make_call`: Start an outbound" not in prompt
        assert "`send_unify_message`" in prompt

    def test_coordinator_comms_listing_is_boss_only(self):
        prompt = _build(
            is_coordinator=True,
            assistant_has_phone=True,
            assistant_has_email=True,
            assistant_has_whatsapp=True,
            assistant_has_discord=True,
            assistant_has_slack=True,
            assistant_has_teams=True,
        )

        assert "`send_sms`: Send an SMS message to my boss only" in prompt
        assert "`send_email`: Send an email to my boss only" in prompt
        assert (
            "`send_unify_message`: Send a Unify platform message to my boss only"
            in prompt
        )
        assert "`send_slack_message`: Send a Slack DM to my boss only" in prompt
        assert (
            "`send_teams_message`: Send a Teams direct message to my boss only"
            in prompt
        )
        assert (
            "`create_teams_meet`: Create a Microsoft Teams meeting with my boss only"
            in prompt
        )
        assert "`send_slack_channel_message`" not in prompt
        assert "`create_teams_channel`" not in prompt

    def test_regular_comms_listing_keeps_contact_targeting(self):
        prompt = _build(
            is_coordinator=False,
            assistant_has_phone=True,
            assistant_has_email=True,
            assistant_has_slack=True,
            assistant_has_teams=True,
        )

        assert "`send_sms`: Send an SMS message to a contact" in prompt
        assert "`send_email`: Send an email to a contact" in prompt
        assert "`send_slack_channel_message`: Post into a Slack channel" in prompt
        assert "`create_teams_channel`: Create a new channel" in prompt


class TestAccessibleSpacesBlock:
    """The system prompt contains shared-team routing guidance."""

    def test_block_renders_after_bio(self):
        prompt = _build(
            bio="Assistant biography.",
            team_summaries=[
                TeamSummary(
                    team_id=3,
                    name="Repairs",
                    description="South-East repairs patch daily operations.",
                ),
            ],
        )

        assert "Bio\n---\nAssistant biography." in prompt
        assert "Accessible shared teams" in prompt
        assert (
            '- team:3 "Repairs" - South-East repairs patch daily operations.' in prompt
        )
        assert prompt.index("Bio\n---") < prompt.index("Accessible shared teams")
        assert prompt.index("Accessible shared teams") < prompt.index(
            "Onboarding reference",
        )


class TestCoordinatorPrompt:
    """Coordinator sessions use a unified base prompt plus org-context surfaces."""

    def test_org_coordinator_prompt_lists_org_roster_and_admin_tools(self):
        prompt = _build(
            is_coordinator=True,
            authorized_humans=[
                {
                    "first_name": "Dana",
                    "surname": "Owner",
                    "email": "dana@acme.com",
                    "is_admin": True,
                },
                {
                    "first_name": "Francis",
                    "surname": "Lead",
                    "email": "francis@acme.com",
                    "is_admin": False,
                },
            ],
        )

        assert "Authorized humans" in prompt
        assert "Dana Owner; email: dana@acme.com; role: admin" in prompt
        assert "Francis Lead; email: francis@acme.com; role: member" in prompt
        assert "**Twin admin tools:**" in prompt
        assert "`primitives.coordinator.list_org_members`" in prompt
        assert "always target the active workspace organization" in prompt
        assert "Twin\n----" in prompt
        assert "Role / specialization: Coordinator." in prompt
        assert "My identity" in prompt
        assert "I am Twin, Alice Smith's personal, private assistant" in prompt

    def test_personal_coordinator_uses_boss_details_and_routes_org_work_to_switch(
        self,
    ):
        prompt = _build(is_coordinator=True, is_org_workspace=False)

        assert "Boss details" in prompt
        assert "Authorized humans\n-----------------" not in prompt
        assert "Organization membership actions are unavailable" in prompt
        assert "switch to that organization's Twin" in prompt
        assert "list_accessible_organizations" not in prompt

    def test_regular_assistant_gets_twin_reference_block(self):
        prompt = _build()

        assert "Twin identity" in prompt
        assert "Twin is Alice Smith's personal, private assistant" in prompt
        assert "I propose handing it to Twin explicitly" in prompt
        assert "inviting, removing, or changing roles for colleagues" in prompt
        assert "creating or removing teams" in prompt
        assert (
            "placing shared credentials, integrations, or other org-level setup"
            in prompt
        )
        assert "I cannot forward it automatically" not in prompt

    def test_twin_handoff_guidance_is_absent_on_twin_sessions(self):
        coordinator_prompt = _build(is_coordinator=True)

        assert "My identity" in coordinator_prompt
        assert (
            "I am Twin, Alice Smith's personal, private assistant" in coordinator_prompt
        )
        assert (
            "Twin is Alice Smith's personal, private assistant"
            not in coordinator_prompt
        )
        assert "My onboarding flow (UI reference)" in coordinator_prompt
        assert "Give me access to your workspace" in coordinator_prompt
        assert "Give Twin access to your workspace" not in coordinator_prompt
        assert "I propose handing it to Twin explicitly" not in coordinator_prompt

    def test_base_and_coordinator_share_restraint_but_keep_role_specific_sections(self):
        base_prompt = _build()
        coordinator_prompt = _build(is_coordinator=True)

        assert "Intent vs verified outcomes" in base_prompt
        assert "Intent vs verified outcomes" in coordinator_prompt
        assert "Console knowledge" in base_prompt
        assert "Console knowledge" not in coordinator_prompt
        assert "My Console literacy" in coordinator_prompt
        assert "Concurrent action and acknowledgment" in base_prompt
        assert "Concurrent action and acknowledgment" in coordinator_prompt
        assert "Onboarding reference" in base_prompt
        assert "Onboarding reference" not in coordinator_prompt

    def test_coordinator_direct_comms_guidance_is_boss_only(self):
        prompt = _build(
            is_coordinator=True,
            assistant_has_phone=True,
            assistant_has_email=True,
            assistant_has_whatsapp=True,
            assistant_has_discord=True,
            assistant_has_slack=True,
            assistant_has_teams=True,
        )

        assert "Boss-only direct communication" in prompt
        assert "only for communicating directly with my boss" in prompt
        assert "They do not accept ``contact_id``" in prompt
        assert "always target the boss contact (``contact_id==1``" in prompt
        assert (
            "Communication with anyone else is never handled by direct tools" in prompt
        )
        assert "delegated third-party communication work goes through ``act``" in prompt
        assert (
            "send a message, draft a reply, place a call, or invite someone else on their behalf"
            in prompt
        )
        assert "contact_id=5" not in prompt
        assert "Use the contact_id visible in active_conversations" not in prompt
        assert "send_slack_channel_message" not in prompt

    def test_regular_direct_comms_guidance_keeps_contact_id_examples(self):
        prompt = _build(
            is_coordinator=False,
            assistant_has_phone=True,
            assistant_has_email=True,
            assistant_has_teams=True,
        )

        assert "Contact-addressed communication tools" in prompt
        assert "Use the contact_id visible in active_conversations" in prompt
        assert 'send_sms(contact_id=5, content="Hi"' in prompt
        assert "Boss-only direct communication" not in prompt


class TestPromptSectionOwnershipMatrix:
    """Role/mode/org combinations keep prompt section ownership boundaries stable."""

    def test_system_prompt_section_ownership_matrix(self):
        cases = (
            {
                "name": "regular_non_demo_no_org",
                "kwargs": {},
                "present": (
                    "Act capabilities\n----------------",
                    "Concurrent action and acknowledgment\n------------------------------------",
                ),
                "absent": (
                    "**Twin admin tools:**",
                    "Authorized humans\n-----------------",
                    "Demo mode\n---------",
                ),
            },
            {
                "name": "regular_non_demo_with_org",
                "kwargs": {},
                "present": (
                    "Act capabilities\n----------------",
                    "Concurrent action and acknowledgment\n------------------------------------",
                    "Twin identity\n--------------",
                ),
                "absent": (
                    "**Twin admin tools:**",
                    "Authorized humans\n-----------------",
                    "Demo mode\n---------",
                ),
            },
            {
                "name": "regular_demo_no_org",
                "kwargs": {"demo_mode": True},
                "present": ("Demo mode\n---------",),
                "absent": (
                    "**Twin admin tools:**",
                    "Authorized humans\n-----------------",
                    "Act capabilities\n----------------",
                    "Concurrent action and acknowledgment\n------------------------------------",
                ),
            },
            {
                "name": "regular_demo_with_org",
                "kwargs": {
                    "demo_mode": True,
                },
                "present": (
                    "Demo mode\n---------",
                    "Twin identity\n--------------",
                ),
                "absent": (
                    "**Twin admin tools:**",
                    "Authorized humans\n-----------------",
                    "Act capabilities\n----------------",
                    "Concurrent action and acknowledgment\n------------------------------------",
                ),
            },
            {
                "name": "coordinator_non_demo_with_org",
                "kwargs": {
                    "is_coordinator": True,
                },
                "present": (
                    "**Twin admin tools:**",
                    "Authorized humans\n-----------------",
                    "Act capabilities\n----------------",
                    "Concurrent action and acknowledgment\n------------------------------------",
                    "Twin\n----",
                    "My identity\n-----------",
                    "My Console literacy\n----------------------",
                    "Console account & org administration",
                    "Proactive meeting offers\n------------------------",
                ),
                "absent": (
                    "Demo mode\n---------",
                    "Onboarding reference\n--------------------",
                    "Console knowledge\n-----------------",
                ),
            },
            {
                "name": "coordinator_demo_with_org",
                "kwargs": {
                    "is_coordinator": True,
                    "demo_mode": True,
                },
                "present": (
                    "Twin\n----",
                    "My identity\n-----------",
                    "Authorized humans\n-----------------",
                    "Demo mode\n---------",
                ),
                "absent": (
                    "**Twin admin tools:**",
                    "Act capabilities\n----------------",
                    "Concurrent action and acknowledgment\n------------------------------------",
                ),
            },
            {
                "name": "coordinator_non_demo_personal_workspace",
                "kwargs": {
                    "is_coordinator": True,
                    "is_org_workspace": False,
                },
                "present": (
                    "**Twin admin tools:**",
                    "Boss details\n------------",
                    "Organization membership actions are unavailable",
                    "switch to that organization's Twin",
                    "Twin\n----",
                    "My identity\n-----------",
                    "My Console literacy\n----------------------",
                    "Console account & org administration",
                    "Proactive meeting offers\n------------------------",
                ),
                "absent": (
                    "Authorized humans\n-----------------",
                    "Onboarding reference\n--------------------",
                    "Console knowledge\n-----------------",
                ),
            },
        )

        for case in cases:
            prompt = _build(**case["kwargs"])
            for marker in case["present"]:
                assert (
                    marker in prompt
                ), f"{case['name']} missing expected marker: {marker}"
            for marker in case["absent"]:
                assert (
                    marker not in prompt
                ), f"{case['name']} unexpectedly contains marker: {marker}"


class TestCoordinatorVoicePrompt:
    """Coordinator voice calls use Twin intro scaffolding plus optional user about."""

    def test_regular_voice_prompt_unchanged_when_flag_is_false(self):
        omitted = _build_voice()
        explicit_false = _build_voice(is_coordinator=False)

        assert omitted == explicit_false
        assert "Coordinator voice role" not in omitted

    def test_coordinator_voice_prompt_uses_twin_intro_and_optional_user_about(self):
        prompt = _build_voice(is_coordinator=True)

        assert "Twin\n----" in prompt
        assert "Role / specialization: Coordinator." in prompt
        assert "About me\n--------\nI help Acme configure its Unify team." in prompt
        assert "Bio\n---" not in prompt
        assert "Coordinator voice role" not in prompt
        assert prompt.index("Twin\n----") < prompt.index("Brevity\n-------")

    def test_coordinator_voice_prompt_omits_user_about_when_empty(self):
        prompt = _build_voice(is_coordinator=True, bio="")

        assert "Twin\n----" in prompt
        assert "About me\n--------" not in prompt

    def test_coordinator_voice_prompt_excludes_slow_brain_literacy(self):
        prompt = _build_voice(is_coordinator=True)

        assert "Twin admin tools" not in prompt
        assert "Unify system literacy" not in prompt
        assert "Requirements discovery workflow" not in prompt
        assert "Tasks/Activations" not in prompt
        assert "Context taxonomy" not in prompt
        assert "`create_assistant`" not in prompt
        assert "`delete_team`" not in prompt
        assert "`remove_team_member`" not in prompt

    def test_coordinator_voice_prompt_includes_console_literacy(self):
        prompt = _build_voice(is_coordinator=True)

        assert "My identity" in prompt
        assert "I am Twin, Dana Owner's personal, private assistant" in prompt
        assert "Twin is Dana Owner's personal, private assistant" not in prompt
        assert "My Console literacy" in prompt
        assert "Left sidebar — selection drives everything" in prompt
        assert "Shared workspaces (Teams in the left sidebar)" in prompt
        assert "Console account & org administration" in prompt
        assert "Two ways to accomplish org tasks" in prompt
        assert "Invite org member (both paths)" in prompt
        assert "mention **both in the same reply**" in prompt
        assert "Unify internal operator tools only" in prompt
        assert "My onboarding flow (UI reference)" in prompt
        assert "Give me access to your workspace" in prompt
        assert "Give Twin access to your workspace" not in prompt
        assert "Console knowledge\n-----------------" not in prompt


# ---------------------------------------------------------------------------
# Tests – missing-capability notices
# ---------------------------------------------------------------------------


class TestMissingCapabilityNotices:
    """The prompt includes notices explaining which channels are unavailable."""

    def test_no_notices_when_fully_configured(self):
        prompt = _build(assistant_has_phone=True, assistant_has_email=True)
        assert "do not currently have a phone number configured" not in prompt
        assert "do not currently have an email address configured" not in prompt

    def test_missing_phone_notice_present(self):
        prompt = _build(assistant_has_phone=False, assistant_has_email=True)
        assert "do not currently have a phone number configured" in prompt
        assert "cannot send SMS messages or make phone calls" in prompt
        assert "do not currently have an email address configured" not in prompt

    def test_missing_email_notice_present(self):
        prompt = _build(assistant_has_phone=True, assistant_has_email=False)
        assert "do not currently have an email address configured" in prompt
        assert "cannot send or receive emails" in prompt
        assert "do not currently have a phone number configured" not in prompt

    def test_both_notices_when_no_capabilities(self):
        prompt = _build(assistant_has_phone=False, assistant_has_email=False)
        assert "do not currently have a phone number configured" in prompt
        assert "do not currently have an email address configured" in prompt


# ---------------------------------------------------------------------------
# Tests – communication guidelines adapt
# ---------------------------------------------------------------------------


class TestCommunicationGuidelinesAdapt:
    """Contact-action examples and should_respond channels adjust."""

    def test_inline_sms_example_present_with_phone(self):
        prompt = _build(assistant_has_phone=True)
        assert "send_sms(contact_id=5" in prompt

    def test_inline_sms_example_absent_without_phone(self):
        prompt = _build(assistant_has_phone=False)
        assert "send_sms(contact_id=5" not in prompt

    def test_inline_email_example_present_with_email(self):
        prompt = _build(assistant_has_email=True)
        assert "send_email(to=[{" in prompt

    def test_inline_email_example_absent_without_email(self):
        prompt = _build(assistant_has_email=False)
        assert "send_email(to=[{" not in prompt

    def test_should_respond_lists_all_channels(self):
        prompt = _build(assistant_has_phone=True, assistant_has_email=True)
        assert "I can send SMS, emails, unify messages, calls" in prompt

    def test_should_respond_omits_phone_channels(self):
        prompt = _build(assistant_has_phone=False, assistant_has_email=True)
        assert "I can send emails, unify messages" in prompt
        assert "I can send SMS, emails, unify messages, calls" not in prompt

    def test_should_respond_omits_email_channel(self):
        prompt = _build(assistant_has_phone=True, assistant_has_email=False)
        assert "I can send SMS, unify messages, calls" in prompt
        assert "I can send SMS, emails" not in prompt

    def test_teams_workspace_actions_are_not_marked_contact_addressed(self):
        prompt = _build(
            assistant_has_phone=True,
            assistant_has_email=True,
            assistant_has_teams=True,
        )
        contact_actions = prompt.split("**Contact actions:**")[1].split(
            "- If the contact is NOT in active_conversations at all",
        )[0]
        contact_addressed_line = next(
            line
            for line in contact_actions.splitlines()
            if "Contact-addressed communication tools" in line
        )

        assert "send_teams_message" in contact_addressed_line
        assert "create_teams_channel" not in contact_addressed_line
        assert "create_teams_meet" not in contact_addressed_line
        assert (
            "`create_teams_channel` and `create_teams_meet` are Teams workspace actions"
            in contact_actions
        )


# ---------------------------------------------------------------------------
# Tests – external app integration
# ---------------------------------------------------------------------------


class TestExternalAppIntegration:
    """The prompt includes guidance for external app integration via credentials + SDK."""

    def test_onboarding_has_app_integration_qa(self):
        prompt = _build()
        assert "Can you help me manage my apps and online services?" in prompt
        assert "secure **Integrations** tab on the console" in prompt
        assert "pick the app from the gallery and authorize it" in prompt
        assert "service's Python SDK" in prompt

    def test_act_capabilities_has_external_apps_bullet(self):
        prompt = _build()
        assert "**External apps & services**" in prompt
        assert "stored credentials and the service's Python SDK" in prompt

    def test_onboarding_qa_present_in_demo_mode(self):
        prompt = _build(demo_mode=True)
        assert "Can you help me manage my apps and online services?" in prompt

    def test_org_assistant_onboarding_allows_direct_setup_with_shared_handoff(self):
        prompt = _build()

        assert "I can walk through app setup and day-to-day usage directly" in prompt
        assert (
            "If a credential must be shared across the team or org (rather than "
            "scoped to just me), Twin is the right person to place it"
        ) in prompt
        assert "Twin owns that setup" not in prompt
        assert "I cannot forward it automatically" not in prompt

    def test_act_capabilities_absent_in_demo_mode(self):
        prompt = _build(demo_mode=True)
        assert "**External apps & services**" not in prompt


# ---------------------------------------------------------------------------
# Tests – seeing/controlling the user's machine (screen-share → linked desktop)
# ---------------------------------------------------------------------------


class TestUserMachineAccess:
    """Precedence guidance for seeing/controlling the *user's* machine.

    Default (no linked desktop) is unchanged from the screen-share-only
    behaviour; a linked desktop unlocks the direct-control path while keeping
    screen share as the first option.
    """

    def test_block_absent_without_linked_desktop(self):
        prompt = _build(has_linked_user_desktop=False)
        assert "Seeing and controlling the user's machine" not in prompt
        # Default capability copy: assistant only controls its own computer.
        assert "I cannot control the user's computer — only my own" in prompt

    def test_linked_desktop_enables_direct_control_block(self):
        prompt = _build(has_linked_user_desktop=True)
        assert "Seeing and controlling the user's machine" in prompt
        # Screen share remains the first-priority option.
        assert "Active screen share / webcam first" in prompt
        assert "linked to me" in prompt
        # Capability bullet reflects the linked machine.
        assert "my boss's own machine, which they've linked to me" in prompt

    def test_screen_share_still_offered_with_linked_desktop(self):
        """Linking a desktop must not remove the screen-share-first guidance."""
        prompt = _build(has_linked_user_desktop=True)
        assert "Want to share your screen?" in prompt
        # Proactive meeting/screen-share offers are untouched.
        assert "Proactive meeting offers" in prompt

    def test_faq_reflects_linked_desktop(self):
        prompt = _build(has_linked_user_desktop=True)
        assert "you've linked a desktop to me" in prompt

    def test_acting_user_id_surfaced_for_targeting(self):
        """When linked + an acting user id is known, the block tells the model
        which user_id to target so a shared assistant drives the speaker's
        machine (not the owner's)."""
        prompt = _build(has_linked_user_desktop=True, acting_user_id="user-42")
        assert 'user_desktop.session(user_id="user-42")' in prompt
        assert "user_desktop.list_linked()" in prompt

    def test_acting_user_id_absent_keeps_block_generic(self):
        prompt = _build(has_linked_user_desktop=True, acting_user_id=None)
        assert "Seeing and controlling the user's machine" in prompt
        assert "user_desktop.session(user_id=" not in prompt


class TestPerUserDesktopResolution:
    """``AssistantDetails.user_desktop_for`` keys linked desktops by the acting
    user, so N users x M assistants resolves the speaker's own machine."""

    @staticmethod
    def _assistant_with_links() -> object:
        from droid.session_details import AssistantDetails, UserDesktopLink

        a = AssistantDetails()
        a.user_desktops = {
            "user-A": UserDesktopLink(
                owner_user_id="user-A",
                url="http://a",
                os="macos",
            ),
            "user-B": UserDesktopLink(
                owner_user_id="user-B",
                url="http://b",
                os="ubuntu",
            ),
        }
        return a

    def test_resolves_speakers_own_link(self):
        a = self._assistant_with_links()
        assert a.user_desktop_for("user-B").url == "http://b"
        assert a.user_desktop_for("user-A").url == "http://a"

    def test_unlinked_speaker_returns_none(self):
        a = self._assistant_with_links()
        assert a.user_desktop_for("user-C") is None

    def test_missing_user_id_returns_none(self):
        a = self._assistant_with_links()
        assert a.user_desktop_for(None) is None


# ---------------------------------------------------------------------------
# Tests – proactive meeting offers
# ---------------------------------------------------------------------------


class TestProactiveMeetingOffers:
    """The prompt encourages proactive meeting/screenshare suggestions."""

    def test_proactive_meeting_section_present(self):
        prompt = _build()
        assert "Proactive meeting offers" in prompt
        assert "screen sharing" in prompt.lower()

    def test_proactive_meeting_absent_in_demo_mode(self):
        prompt = _build(demo_mode=True)
        assert "Proactive meeting offers" not in prompt

    def test_proactive_meeting_absent_for_coordinator(self):
        prompt = _build(is_coordinator=True)
        assert "Proactive meeting offers" in prompt


# ---------------------------------------------------------------------------
# Tests – console knowledge
# ---------------------------------------------------------------------------


class TestConsoleKnowledge:
    """The prompt includes console UI knowledge for guiding users."""

    def test_console_knowledge_present(self):
        prompt = _build()
        assert "Console knowledge" in prompt
        assert "Integrations" in prompt
        assert "Contact Details" in prompt

    def test_console_knowledge_has_navigation_paths(self):
        prompt = _build()
        assert "open the **Integrations** tab" in prompt
        assert "⋮ → **Contact Details**" in prompt
        assert "profile menu" in prompt

    def test_console_knowledge_absent_in_demo_mode(self):
        prompt = _build(demo_mode=True)
        assert "Console knowledge" not in prompt

    def test_coordinator_uses_console_literacy_not_base_block(self):
        prompt = _build(is_coordinator=True)
        assert "My Console literacy" in prompt
        assert "hover over my name in the left sidebar → ⋮ → **Secrets**" not in prompt
        assert "Memory → Guidance" in prompt
        assert "Secrets (on the Integrations tab)" in prompt
        assert "Shared workspaces (Teams in the left sidebar)" in prompt


# ---------------------------------------------------------------------------
# Tests – demo mode adapts
# ---------------------------------------------------------------------------


class TestDemoModeAdapts:
    """Demo mode section adjusts available channel listing."""

    def test_demo_lists_all_channels(self):
        prompt = _build(
            demo_mode=True,
            assistant_has_phone=True,
            assistant_has_email=True,
        )
        assert "SMS, emails, unify messages, calls" in prompt

    def test_demo_omits_phone_channels(self):
        prompt = _build(
            demo_mode=True,
            assistant_has_phone=False,
            assistant_has_email=True,
        )
        assert "SMS" not in prompt.split("CAN do")[1].split("CANNOT do")[0]

    def test_demo_omits_email_channel(self):
        prompt = _build(
            demo_mode=True,
            assistant_has_phone=True,
            assistant_has_email=False,
        )
        assert "emails" not in prompt.split("CAN do")[1].split("CANNOT do")[0]


# ---------------------------------------------------------------------------
# Tests – Console-UI / onboarding gate (local mode)
# ---------------------------------------------------------------------------


class TestConsoleUIGate:
    """Console-UI knowledge and onboarding prompts are gated on
    ``console_ui_present`` so the public local install (no Console) gets a
    trimmed prompt with a local-mode note instead."""

    def test_regular_console_knowledge_present_by_default(self):
        prompt = _build(is_coordinator=False)
        assert "Console knowledge" in prompt
        assert "Interaction surface" not in prompt

    def test_regular_console_knowledge_absent_in_local_mode(self):
        prompt = _build(is_coordinator=False, console_ui_present=False)
        assert "Console knowledge" not in prompt
        assert "Interaction surface" in prompt

    def test_coordinator_console_blocks_present_by_default(self):
        prompt = _build(is_coordinator=True)
        assert "Console literacy" in prompt
        assert "onboarding flow (UI reference)" in prompt

    def test_coordinator_console_blocks_absent_in_local_mode(self):
        prompt = _build(is_coordinator=True, console_ui_present=False)
        assert "Console literacy" not in prompt
        assert "onboarding flow (UI reference)" not in prompt
        assert "Interaction surface" in prompt

    def test_voice_platform_knowledge_present_by_default(self):
        prompt = _build_voice(is_coordinator=False)
        assert "Platform knowledge" in prompt

    def test_voice_platform_knowledge_absent_in_local_mode(self):
        prompt = _build_voice(is_coordinator=False, console_ui_present=False)
        assert "Platform knowledge" not in prompt


_ONBOARDING_RENDER: dict = {
    "active_step_id": "email-reply",
    "steps": [
        {
            "id": "email-reference",
            "title": "Trigger email from Twin",
            "phase": "Quiz",
            "status": "done",
            "can_skip": False,
        },
        {
            "id": "email-reply",
            "title": "Reply to email",
            "phase": "Quiz",
            "status": "available",
            "can_skip": True,
        },
    ],
    "next_targets": [
        {
            "id": "email-reply",
            "title": "Reply to email",
            "nudge_chat": "Prompt them to reply with their guess to the email clue.",
            "nudge_voice": "replying with their guess for the email clue",
            "channel": "email",
        },
    ],
}


class TestCoordinatorOnboardingDeferGate:
    """The global "do onboarding later" switch drops onboarding-specific
    scaffolding (reactive narration, the checklist/flow map, and the live
    progress block) but keeps general Console literacy on so the
    Coordinator can still orient the user and nudge platform behaviours."""

    def test_deferred_drops_onboarding_blocks_but_keeps_console_literacy(self):
        prompt = _build(
            is_coordinator=True,
            coordinator_onboarding_deferred=True,
            coordinator_onboarding_render=_ONBOARDING_RENDER,
        )
        # General platform literacy stays on in every mode.
        assert "My Console literacy" in prompt
        # Onboarding-specific scaffolding is suppressed.
        assert "My onboarding narration" not in prompt
        assert "My onboarding flow (UI reference)" not in prompt
        assert "My onboarding progress (live)" not in prompt

    def test_not_deferred_keeps_all_coordinator_blocks(self):
        prompt = _build(
            is_coordinator=True,
            coordinator_onboarding_render=_ONBOARDING_RENDER,
        )
        assert "My Console literacy" in prompt
        assert "My onboarding narration" in prompt
        assert "My onboarding flow (UI reference)" in prompt

    # Body sentence unique to the standing progress block. The block title
    # also appears by name inside the narration block (which points the
    # brain at it), so assertions key off this body line instead.
    _PROGRESS_BLOCK_MARKER = "always-current picture of the user's onboarding"

    def test_progress_block_renders_next_targets_with_nudge_copy(self):
        prompt = _build(
            is_coordinator=True,
            coordinator_onboarding_render=_ONBOARDING_RENDER,
        )
        assert self._PROGRESS_BLOCK_MARKER in prompt
        # The standing block names the valid next target + its nudge copy
        # so the brain reads "what's next" rather than deriving it.
        assert "Reply to email" in prompt
        assert "Prompt them to reply with their guess to the email clue." in prompt

    def test_progress_block_absent_without_render(self):
        prompt = _build(is_coordinator=True)
        assert self._PROGRESS_BLOCK_MARKER not in prompt

    def test_voice_deferred_keeps_literacy_drops_flow_reference(self):
        prompt = _build_voice(is_coordinator=True, coordinator_onboarding_deferred=True)
        assert "My Console literacy" in prompt
        assert "My onboarding flow (UI reference)" not in prompt

    def test_voice_opener_pitches_server_next_target(self):
        prompt = _build_voice(
            is_coordinator=True,
            coordinator_onboarding_next_targets=[
                {
                    "id": "apps",
                    "title": "Connect me with your apps",
                    "nudge_chat": "Open Integrations and connect an app.",
                    "nudge_voice": "connecting one of their apps from the Integrations panel",
                    "channel": None,
                },
            ],
        )
        assert "connecting one of their apps from the Integrations panel" in prompt


# ---------------------------------------------------------------------------
# Tests – onboarding flow reference is driven by the fetched catalog
# ---------------------------------------------------------------------------


def _catalog_step(step_id: str, title: str, phase: str) -> dict:
    return {
        "id": step_id,
        "title": title,
        "phase": phase,
        "kind": "connect",
        "channel": None,
        "can_skip": True,
        "description": "",
        "estimated_time": "",
        "chips_chat": [],
        "chips_call": [],
    }


# A local-mode catalog: every phase present (mirrors the shape Orchestra's
# ``/assistant/onboarding/catalog`` returns on a self-host / dev deployment).
_CATALOG_LOCAL: dict = {
    "phases": [
        {
            "id": "communication",
            "phase": "Communication",
            "title": "Communication",
            "description": "Try communication channels.",
        },
        {
            "id": "workspace",
            "phase": "Workspace",
            "title": "Workspace",
            "description": "Give access to the workspace.",
        },
        {
            "id": "integrations",
            "phase": "Integrations",
            "title": "Integrations",
            "description": "Connect apps.",
        },
        {
            "id": "tasks",
            "phase": "Tasks",
            "title": "Tasks",
            "description": "Schedule work.",
        },
        {
            "id": "my-computer",
            "phase": "My Computer",
            "title": "My Computer",
            "description": "Ask me to operate from my computer.",
        },
    ],
    "steps": [
        _catalog_step("email-reference", "Trigger email from Twin", "Communication"),
        _catalog_step("workspace", "Give me access to your workspace", "Workspace"),
        _catalog_step("apps", "Connect me with your apps", "Integrations"),
        _catalog_step("schedule", "Schedule a task for later", "Tasks"),
        _catalog_step("my-computer-coming-soon", "[Coming soon]", "My Computer"),
    ],
}

# A hosted catalog: Orchestra returns the same phase structure as local mode.
_CATALOG_HOSTED: dict = {
    "phases": _CATALOG_LOCAL["phases"],
    "steps": _CATALOG_LOCAL["steps"],
}


class TestOnboardingCatalogDrivesFlowReference:
    """The flow-reference block sources its phase/step titles from the fetched
    catalog (Orchestra's single source of truth), and the hosted catalog —
    which omits the local_only phases — drops them from the prompt too."""

    def test_local_catalog_renders_all_phase_and_step_titles(self):
        prompt = _build(is_coordinator=True, onboarding_catalog=_CATALOG_LOCAL)
        assert "My onboarding flow (UI reference)" in prompt
        assert "Communication" in prompt
        assert "My Computer" in prompt
        assert "Give me access to your workspace" in prompt

    def test_hosted_catalog_renders_all_phase_and_step_titles(self):
        prompt = _build(is_coordinator=True, onboarding_catalog=_CATALOG_HOSTED)
        assert "My onboarding flow (UI reference)" in prompt
        assert "Communication" in prompt
        assert "My Computer" in prompt
        assert "Give me access to your workspace" in prompt

    def test_literacy_local_omits_removed_work_tour_hooks(self):
        prompt = _build(is_coordinator=True, onboarding_catalog=_CATALOG_LOCAL)
        assert "Onboarding phase 7 (My Computer) — tour hooks" not in prompt

    def test_literacy_hosted_omits_removed_work_tour_hooks(self):
        prompt = _build(is_coordinator=True, onboarding_catalog=_CATALOG_HOSTED)
        assert "Onboarding phase 7 (My Computer) — tour hooks" not in prompt

    def test_voice_flow_reference_uses_catalog(self):
        prompt = _build_voice(is_coordinator=True, onboarding_catalog=_CATALOG_HOSTED)
        assert "My onboarding flow (UI reference)" in prompt
        assert "My Computer" in prompt
        assert "Workspace" in prompt
