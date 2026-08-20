"""
tests/conversation_manager/core/test_prompt_builders.py
=======================================================

Unit tests for the ConversationManager prompt builder, focusing on
capability-gated sections (assistant phone / email).
"""

from __future__ import annotations

import pytest

from unify.conversation_manager.prompt_builders import (
    _build_voice_calls_guide,
    build_fast_brain_turn_guidance,
    build_system_prompt,
    build_voice_agent_prompt,
)
from unify.session_details import TeamSummary

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

    def test_slow_brain_prompt_documents_per_tool_call_thoughts(self):
        prompt = _build()
        assert "Tool-call reasoning" in prompt
        assert "optional `thoughts` argument" in prompt
        assert '"thoughts": [my concise thoughts before taking actions]' not in prompt

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

    def test_discord_channel_message_advertised_for_regular_assistant(self):
        prompt = _build(is_coordinator=False, assistant_has_discord=True)
        assert "`send_discord_message`: Send a Discord message to a contact" in prompt
        assert "`send_discord_channel_message`: Post into a Discord channel" in prompt

    def test_discord_channel_message_absent_for_coordinator(self):
        prompt = _build(is_coordinator=True, assistant_has_discord=True)
        assert "`send_discord_message`: Send a Discord direct message" in prompt
        assert "`send_discord_channel_message`" not in prompt


class TestVoiceSessionToolMasking:
    """While a voice session is live, call-starting tools are neither in the
    live tool set nor advertised, and a block explains they return on hang-up."""

    _CALL_TOOL_DESCRIPTIONS = (
        "Start an outbound phone call",
        "Start a WhatsApp voice call",
        "Join a Google Meet call via browser automation",
        "Join a Microsoft Teams meeting via browser automation",
    )

    def test_call_starting_tools_advertised_off_call(self):
        prompt = _build(
            assistant_has_phone=True,
            assistant_has_whatsapp=True,
            on_voice_call=False,
        )
        for desc in self._CALL_TOOL_DESCRIPTIONS:
            assert desc in prompt
        assert "Active voice session\n--------------------" not in prompt

    def test_call_starting_tools_withheld_on_call(self):
        prompt = _build(
            assistant_has_phone=True,
            assistant_has_whatsapp=True,
            is_voice_call=True,
            on_voice_call=True,
        )
        for desc in self._CALL_TOOL_DESCRIPTIONS:
            assert desc not in prompt
        # Text channels stay available mid-call.
        assert "`send_sms`: Send an SMS message to a contact" in prompt
        assert "`send_whatsapp`: Send a WhatsApp message to a contact" in prompt

    def test_active_voice_session_block_explains_return_on_hangup(self):
        prompt = _build(
            assistant_has_phone=True,
            assistant_has_whatsapp=True,
            is_voice_call=True,
            on_voice_call=True,
        )
        assert "Active voice session\n--------------------" in prompt
        assert "only be on ONE voice session at a time" in prompt
        assert "reappear automatically the moment this session ends" in prompt
        assert "`hang_up`" in prompt

    def test_coordinator_call_tools_withheld_on_call(self):
        prompt = _build(
            is_coordinator=True,
            assistant_has_phone=True,
            assistant_has_whatsapp=True,
            is_voice_call=True,
            on_voice_call=True,
        )
        assert "`make_call`: Start an outbound phone call to my boss only" not in prompt
        assert (
            "`make_whatsapp_call`: Start a WhatsApp voice call to my boss only"
            not in prompt
        )
        assert "Active voice session\n--------------------" in prompt

    def test_one_voice_session_rule_present_without_phone(self):
        # The mutual-exclusion rule is no longer gated on a stored phone number.
        prompt = _build(assistant_has_phone=False, assistant_has_whatsapp=False)
        assert "only be on ONE voice session at a time" in prompt


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
        assert "**T-W1N admin tools:**" in prompt
        assert "`primitives.coordinator.list_org_members`" in prompt
        assert "always target the active workspace organization" in prompt
        assert "T-W1N\n----" in prompt
        assert "Role / specialization: Coordinator." in prompt
        assert "My identity" in prompt
        assert "I am T-W1N, Alice Smith's personal, private assistant" in prompt

    def test_personal_coordinator_uses_boss_details_and_routes_org_work_to_switch(
        self,
    ):
        prompt = _build(is_coordinator=True, is_org_workspace=False)

        assert "Boss details" in prompt
        assert "Authorized humans\n-----------------" not in prompt
        assert "Organization membership actions are unavailable" in prompt
        assert "switch to that organization's T-W1N" in prompt
        assert "list_accessible_organizations" not in prompt

    def test_regular_assistant_gets_twin_reference_block(self):
        prompt = _build()

        assert "T-W1N identity" in prompt
        assert "T-W1N is Alice Smith's personal, private assistant" in prompt
        assert "I propose handing it to T-W1N explicitly" in prompt
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
            "I am T-W1N, Alice Smith's personal, private assistant"
            in coordinator_prompt
        )
        assert (
            "T-W1N is Alice Smith's personal, private assistant"
            not in coordinator_prompt
        )
        assert "Give T-W1N access to your workspace" not in coordinator_prompt
        assert "I propose handing it to T-W1N explicitly" not in coordinator_prompt

    def test_base_and_coordinator_share_restraint_but_keep_role_specific_sections(self):
        base_prompt = _build()
        coordinator_prompt = _build(is_coordinator=True)

        assert "Intent vs verified outcomes" in base_prompt
        assert "Intent vs verified outcomes" in coordinator_prompt
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

        assert "Boss-only communication" in prompt
        assert "only for communicating directly with my boss" in prompt
        assert "They do not accept ``contact_id``" in prompt
        assert "always target the boss contact (``contact_id==1``" in prompt
        assert "on ANY path — direct tools and ``act`` alike" in prompt
        assert "draft the content for my boss to send themselves" in prompt
        assert "Communication with anyone else is not possible for me" in prompt
        # The act escape hatch is gone: no prompt text routes third-party
        # communication through act any more.
        assert "delegated third-party communication work" not in prompt
        assert "through ``act`` instead of direct communication tools" not in prompt
        assert "Direct tools never accept inline contact details" in prompt
        assert "update the boss contact record first" in prompt
        assert "contact_id=5" not in prompt
        assert "Use the contact_id visible in active_conversations" not in prompt
        assert "send_slack_channel_message" not in prompt
        # Browser meetings are gone from the single-player surface entirely.
        assert "cannot join Google Meet or Microsoft Teams meetings" in prompt
        assert "`join_google_meet`: Join a Google Meet call" not in prompt

    def test_multiplayer_coordinator_gets_hire_comms_with_admin_surface(self):
        prompt = _build(
            is_coordinator=True,
            is_multiplayer=True,
            twin_name="Max Vector",
            assistant_has_phone=True,
            assistant_has_email=True,
            assistant_has_whatsapp=True,
            assistant_has_discord=True,
            assistant_has_slack=True,
            assistant_has_teams=True,
        )

        # Hire-like comms surface: open audience, channel posting, meets.
        assert "Contact actions:" in prompt
        assert "Boss-only communication" not in prompt
        assert "send_slack_channel_message" in prompt
        assert "`join_google_meet`: Join a Google Meet call" in prompt
        assert "I do not communicate with other people" not in prompt
        # Identity speaks under the twin's own name.
        assert "I am Max Vector" in prompt
        # The coordinator admin surface survives the flip.
        assert "primitives.coordinator" in prompt

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
                "name": "regular_no_org",
                "kwargs": {},
                "present": (
                    "Act capabilities\n----------------",
                    "Concurrent action and acknowledgment\n------------------------------------",
                ),
                "absent": (
                    "**T-W1N admin tools:**",
                    "Authorized humans\n-----------------",
                    "Demo mode\n---------",
                ),
            },
            {
                "name": "regular_with_org",
                "kwargs": {},
                "present": (
                    "Act capabilities\n----------------",
                    "Concurrent action and acknowledgment\n------------------------------------",
                    "T-W1N identity\n--------------",
                ),
                "absent": (
                    "**T-W1N admin tools:**",
                    "Authorized humans\n-----------------",
                    "Demo mode\n---------",
                ),
            },
            {
                "name": "coordinator_with_org",
                "kwargs": {
                    "is_coordinator": True,
                },
                "present": (
                    "**T-W1N admin tools:**",
                    "Authorized humans\n-----------------",
                    "Act capabilities\n----------------",
                    "Concurrent action and acknowledgment\n------------------------------------",
                    "T-W1N\n----",
                    "My identity\n-----------",
                    "Proactive meeting offers\n------------------------",
                ),
                "absent": (
                    "Demo mode\n---------",
                    "Onboarding reference\n--------------------",
                    "Console knowledge\n-----------------",
                ),
            },
            {
                "name": "coordinator_personal_workspace",
                "kwargs": {
                    "is_coordinator": True,
                    "is_org_workspace": False,
                },
                "present": (
                    "**T-W1N admin tools:**",
                    "Boss details\n------------",
                    "Organization membership actions are unavailable",
                    "switch to that organization's T-W1N",
                    "T-W1N\n----",
                    "My identity\n-----------",
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
    """Coordinator voice calls use T-W1N intro scaffolding plus optional user about."""

    def test_regular_voice_prompt_unchanged_when_flag_is_false(self):
        omitted = _build_voice()
        explicit_false = _build_voice(is_coordinator=False)

        assert omitted == explicit_false
        assert "Coordinator voice role" not in omitted

    def test_coordinator_voice_prompt_uses_twin_intro_and_optional_user_about(self):
        prompt = _build_voice(is_coordinator=True)

        assert "T-W1N\n----" in prompt
        assert "Role / specialization: Coordinator." in prompt
        assert "About me\n--------\nI help Acme configure its Unify team." in prompt
        assert "Bio\n---" not in prompt
        assert "Coordinator voice role" not in prompt
        assert prompt.index("T-W1N\n----") < prompt.index("Brevity\n-------")

    def test_coordinator_voice_prompt_omits_user_about_when_empty(self):
        prompt = _build_voice(is_coordinator=True, bio="")

        assert "T-W1N\n----" in prompt
        assert "About me\n--------" not in prompt

    def test_coordinator_voice_prompt_excludes_slow_brain_literacy(self):
        prompt = _build_voice(is_coordinator=True)

        assert "T-W1N admin tools" not in prompt
        assert "Unify system literacy" not in prompt
        assert "Requirements discovery workflow" not in prompt
        assert "Tasks/Activations" not in prompt
        assert "Context taxonomy" not in prompt
        assert "`create_assistant`" not in prompt
        assert "`delete_team`" not in prompt
        assert "`remove_team_member`" not in prompt

    def test_coordinator_voice_prompt_excludes_navigation_maps(self):
        prompt = _build_voice(is_coordinator=True)

        # The identity block remains on the fast brain.
        assert "My identity" in prompt
        assert "I am T-W1N, Dana Owner's personal, private assistant" in prompt
        assert "T-W1N is Dana Owner's personal, private assistant" not in prompt
        # The console-literacy and onboarding-flow maps are deliberately NOT
        # given to the fast brain: holding the same navigation knowledge as
        # the slow brain let the Voice Agent freelance contradictory
        # "what's next / where do I click" answers. Those questions now defer
        # to the slow brain (RULE 2), which owns onboarding navigation.
        assert "Console knowledge" not in prompt
        assert "My opening turn" not in prompt
        assert "Onboarding checklist" not in prompt
        assert "Step-by-step walkthrough pacing" not in prompt


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


def _concurrent_ack_block(prompt: str) -> str:
    start = prompt.index("Concurrent action and acknowledgment")
    end = prompt.index("**Acknowledgments should be brief:**", start)
    return prompt[start:end]


class TestConcurrentActionAckBlock:
    """Concurrent-action ack guidance names only outbound tools exposed this turn."""

    def test_ack_block_omits_send_sms_without_phone(self):
        prompt = _build(assistant_has_phone=False, assistant_has_email=False)
        block = _concurrent_ack_block(prompt)
        assert "`send_unify_message`" in block
        assert "send_sms" not in block
        assert "only outbound message tool available on this turn" in block

    def test_ack_block_includes_send_sms_with_phone(self):
        prompt = _build(assistant_has_phone=True, assistant_has_email=False)
        block = _concurrent_ack_block(prompt)
        assert "`send_sms`" in block
        assert "`send_unify_message`" in block
        assert "Pick whichever tool matches the active conversation thread" in block

    def test_ack_block_whatsapp_only_omits_send_sms(self):
        prompt = _build(
            assistant_has_phone=False,
            assistant_has_whatsapp=True,
            assistant_has_email=False,
        )
        block = _concurrent_ack_block(prompt)
        assert "`send_whatsapp`" in block
        assert "send_sms" not in block

    def test_ack_block_example_uses_default_outbound_tool(self):
        prompt = _build(assistant_has_phone=False, assistant_has_email=False)
        block = _concurrent_ack_block(prompt)
        assert 'send_unify_message(contact_id=1, content="Let me check.")' in block


class TestRoomChatEtiquette:
    """Team/group chat needs the opposite default to a 1:1 thread.

    The restraint section says never to leave a chat line unanswered, which is
    right when I am the only possible answerer. A room breaks that for two
    separate reasons: it delivers the same message to every member assistant, so
    all of them following that rule turns one human line into several replies;
    and most traffic in a busy room is people talking to each other, which is
    true even when I am the only assistant in it.
    """

    def test_the_room_etiquette_section_is_present(self):
        assert "Rooms: team and group chats" in _build()

    def test_it_covers_a_room_of_people_not_only_one_of_assistants(self):
        """A lone assistant among several humans has the same problem.

        Scoping this to multi-assistant rooms left the commonest group chat --
        one assistant, several people -- back on the 1:1 rule, answering every
        line two colleagues said to each other.
        """
        prompt = _build()
        assert "Two members talking to each other" in prompt
        assert "theirs to answer" in prompt

    def test_it_says_exactly_one_assistant_should_answer(self):
        prompt = _build()
        assert "Exactly one of us should answer a given message." in prompt

    def test_it_tells_me_to_stand_down_when_a_teammate_is_named(self):
        prompt = _build()
        assert "(not me)" in prompt
        assert "Stay quiet" in prompt

    def test_it_quotes_the_annotation_the_renderer_actually_emits(self):
        """Guidance keyed on wording the renderer never produces is guidance the
        model cannot act on — the annotation and the rule have to agree."""
        prompt = _build()
        assert "`addressed to me`" in prompt
        assert "`addressed to <name> (not me)`" in prompt

    def test_absent_addressing_is_not_treated_as_nobody(self):
        """An empty mention list also means the sender typed the "@" by hand."""
        prompt = _build()
        assert "NOT the same as nobody being addressed" in prompt
        assert 'my own name appears after an "@"' in prompt

    def test_it_says_silence_is_the_worse_failure_when_i_was_named(self):
        """The regression this block caused: standing down when asked directly."""
        prompt = _build()
        assert "staying quiet is the worse failure" in prompt

    def test_it_carves_the_room_exception_out_of_the_restraint_rule(self):
        """Both rules ship together, so the narrower one has to name the wider."""
        prompt = _build()
        assert "Never `wait` while their chat line is still unanswered" in prompt
        assert "It does not apply to a room" in prompt


class TestGroupCallEtiquette:
    """A call carrying several people is a room, and needs the room's default.

    Speech has no mention list: every utterance arrives as an ordinary user turn
    with nothing marking who it was aimed at, so the restraint rule reads as owed
    a reply on every line -- including the ones two other people are exchanging
    with each other. The section only appears while such a call is live, because
    on a 1:1 call every turn really is the assistant's to answer.
    """

    HEADING = "Group calls: deciding whether a turn is mine"

    def test_it_is_absent_off_a_call(self):
        assert self.HEADING not in _build(call_participant_names=["Ada", "Bo"])

    def test_it_is_absent_on_a_one_to_one_call(self):
        """The regression to avoid: telephony must keep answering everything."""
        assert self.HEADING not in _build(
            on_voice_call=True,
            call_participant_names=["Ada"],
        )

    def test_it_is_absent_when_no_roster_is_known(self):
        """Telephony reports nobody, which is an answer rather than a gap."""
        assert self.HEADING not in _build(on_voice_call=True)

    def test_it_appears_once_a_second_person_is_present(self):
        assert self.HEADING in _build(
            on_voice_call=True,
            call_participant_names=["Ada", "Bo"],
        )

    def test_blank_names_do_not_make_a_group(self):
        """A roster padded with empty entries is still one person."""
        assert self.HEADING not in _build(
            on_voice_call=True,
            call_participant_names=["Ada", "", "   "],
        )

    def test_it_names_who_is_on_the_call(self):
        """Deciding who was addressed needs to know who could have been."""
        prompt = _build(
            on_voice_call=True,
            call_participant_names=["Ada", "Bo"],
        )
        assert "On it right now: Ada, Bo." in prompt

    def test_it_tells_me_to_stay_out_of_their_exchange(self):
        prompt = _build(
            on_voice_call=True,
            call_participant_names=["Ada", "Bo"],
        )
        assert "Two participants talking between themselves" in prompt
        assert "`wait`" in prompt

    def test_absent_naming_is_not_evidence_the_turn_was_not_mine(self):
        """Mirrors the chat block: a missing signal must not read as a negative.

        People ask the assistant for things without saying its name, so keying
        the decision on hearing the name would drop every one of those.
        """
        prompt = _build(
            on_voice_call=True,
            call_participant_names=["Ada", "Bo"],
        )
        assert "NOT evidence that a turn was not mine" in prompt

    def test_it_says_silence_is_the_worse_failure_when_i_was_addressed(self):
        """The balance the chat block had to be corrected to state.

        Told only to stay quiet, the assistant stands down from questions put
        straight to it -- a worse failure than the over-answering it fixes.
        """
        prompt = _build(
            on_voice_call=True,
            call_participant_names=["Ada", "Bo"],
        )
        assert "answering is not optional" in prompt
        assert "staying quiet is the worse failure" in prompt

    def test_it_carves_the_exception_out_of_the_restraint_rule(self):
        """The wider rule ships in the same prompt, so this must name it."""
        prompt = _build(
            on_voice_call=True,
            call_participant_names=["Ada", "Bo"],
        )
        assert "group-call exception to the restraint rule" in prompt
        assert "does not change how I behave on a 1:1 call" in prompt

    def test_it_warns_off_replying_to_another_assistant(self):
        """The volley the fan-out brake bounds should be discouraged first."""
        prompt = _build()
        assert "Posted by an AI teammate" in prompt

    def test_it_points_at_the_annotation_that_carries_the_addressing(self):
        """Guidance is useless if it names a signal the renderer never emits."""
        prompt = _build()
        assert "[team chat" in prompt and "[group chat" in prompt

    def test_it_tells_me_standing_down_means_calling_wait(self):
        """Omitting the tool is not resting -- the runtime hands back a turn.

        A slow-brain turn that calls nothing is treated as unfinished and
        re-opened, and an assistant that keeps being re-offered a turn will
        eventually take it. So the block has to name the tool, not just the
        intent.
        """
        prompt = _build(
            on_voice_call=True,
            call_participant_names=["Ada", "Bo"],
        )
        assert "Standing down is an action I take, not an absence." in prompt
        assert "not producing no tool call at all" in prompt

    def test_it_reinterprets_the_wait_tools_answer_any_question_clause(self):
        """`wait`'s own description tells it not to wait when asked a question.

        That is written for a 1:1, where any question is necessarily the
        assistant's. Left unqualified on a group call it licenses answering a
        question overheard between two other people. The shared description is
        not edited -- it ships to every chat and every 1:1 call -- so the block
        narrows it here instead.
        """
        prompt = _build(
            on_voice_call=True,
            call_participant_names=["Ada", "Bo"],
        )
        assert "a question **aimed at me**" in prompt


class TestPeerAssistantCallEtiquette:
    """Which assistant takes the turn, when more than one is on the call.

    The fast brain gates the slow brain on a user turn, so this block is not
    what stops a duplicate answer there. It is for the routes the fast brain
    never sees: a notification relay, an action completing, a proactive line. On
    those the slow brain reaches the caller on its own decision.
    """

    HEADING = "Multi-assistant calls: which assistant takes the turn"

    def test_it_is_absent_off_a_call(self):
        assert self.HEADING not in _build(call_assistant_names=["A-DA"])

    def test_it_is_absent_with_no_teammate(self):
        """The 1:1 regression to avoid: nobody else can answer for the assistant."""
        assert self.HEADING not in _build(on_voice_call=True)
        assert self.HEADING not in _build(
            on_voice_call=True,
            call_participant_names=["Ada", "Bo"],
        )

    def test_blank_names_do_not_make_a_teammate(self):
        """A roster crosses a wire; padding must not stand the assistant down."""
        assert self.HEADING not in _build(
            on_voice_call=True,
            call_assistant_names=["", "   "],
        )

    def test_one_teammate_is_enough(self):
        """Not folded into the human count: one person plus two assistants is a
        room where a turn may belong to someone else, which counting humans
        alone reads as 1:1."""
        prompt = _build(on_voice_call=True, call_assistant_names=["A-DA"])
        assert self.HEADING in prompt
        assert "Also here: A-DA." in prompt

    def test_it_coexists_with_the_group_block(self):
        """Several humans and a teammate is one call, not a choice of two rules."""
        prompt = _build(
            on_voice_call=True,
            call_participant_names=["Ada", "Bo"],
            call_assistant_names=["A-DA"],
        )
        assert self.HEADING in prompt
        assert "Group calls: deciding whether a turn is mine" in prompt

    def test_it_says_a_quiet_question_is_not_an_unanswered_one(self):
        """The gap that makes an assistant pick up a teammate's question.

        Peer audio is not in this assistant's transcript, so a question it heard
        and no reply to reads as dropped when it was in fact answered.
        """
        prompt = _build(on_voice_call=True, call_assistant_names=["A-DA"])
        assert "I cannot hear my teammates' replies" in prompt
        assert "not** evidence it went unanswered" in prompt

    def test_unclear_turns_resolve_to_waiting(self):
        """The inversion the fast brain also makes: with a teammate present,
        an unclear turn is theirs to risk, not mine to claim."""
        prompt = _build(on_voice_call=True, call_assistant_names=["A-DA"])
        assert "Genuinely unclear whose it was" in prompt
        assert "two assistants answering the same question" in prompt

    def test_being_named_still_obliges_an_answer(self):
        """Told only to stand down, it stands down from questions put to it."""
        prompt = _build(on_voice_call=True, call_assistant_names=["A-DA"])
        assert "I was named, or a teammate handed the turn to me" in prompt
        assert "guide_voice_agent" in prompt

    def test_it_tells_me_standing_down_means_calling_wait(self):
        prompt = _build(on_voice_call=True, call_assistant_names=["A-DA"])
        assert "Standing down is an action I take, not an absence." in prompt
        assert "omitting the tool is read as an unfinished turn" in prompt

    def test_the_shared_wait_description_is_not_edited(self):
        """It ships to every chat and every 1:1 call, so it is narrowed in the
        block rather than rewritten at the tool."""
        from unify.conversation_manager.domains.brain_action_tools import (
            ConversationManagerBrainActionTools,
        )

        doc = ConversationManagerBrainActionTools.wait.__doc__ or ""
        assert "expresses confusion, or checks whether I" in doc


class TestCreateTeamsMeetShareTools:
    """create_teams_meet share guidance names only configured outbound tools."""

    def test_teams_only_omits_send_sms_and_send_email(self):
        prompt = _build(
            assistant_has_phone=False,
            assistant_has_email=False,
            assistant_has_teams=True,
        )
        idx = prompt.find("create_teams_meet")
        assert idx >= 0
        snippet = prompt[idx : idx + 900]
        assert "shared via `send_teams_message`" in snippet
        assert "send_sms" not in snippet
        assert "send_email" not in snippet

    def test_teams_with_phone_and_email_lists_all_share_tools(self):
        prompt = _build(
            assistant_has_phone=True,
            assistant_has_email=True,
            assistant_has_teams=True,
        )
        idx = prompt.find("create_teams_meet")
        snippet = prompt[idx : idx + 900]
        assert "shared via `send_teams_message` / `send_email` / `send_sms`" in snippet


# ---------------------------------------------------------------------------
# Tests – external app integration
# ---------------------------------------------------------------------------


class TestExternalAppIntegration:
    """The prompt includes guidance for external app integration via credentials + SDK."""

    def test_act_capabilities_has_external_apps_bullet(self):
        prompt = _build()
        assert "**External apps & services**" in prompt
        assert "stored credentials and the service's Python SDK" in prompt


class TestExternalResourcesActBlock:
    """External-resource work must go through ``act`` (not conversational mail)."""

    def test_external_resources_block_present(self):
        prompt = _build()
        assert "External resources (use ``act``)" in prompt
        assert "Ground truth rule" in prompt
        assert "I do not answer" in prompt and "from memory" in prompt
        assert "ordinary conversational replies" in prompt
        assert "programmatic mailbox/workspace automation" in prompt
        # Conversational inbox monitoring must not be blanket-forced into act.
        assert "API, inbox," not in prompt


class TestConversationalVsProgrammaticComms:
    """Standing reply instructions stay on CM tools; mailbox automation uses act."""

    def test_split_present_for_assistant(self):
        prompt = _build(is_coordinator=False)
        assert "Conversational messaging vs programmatic workspace" in prompt
        assert 'act("monitor for email and reply…")' in prompt
        assert "every Monday auto-label" in prompt
        assert "are **mine** (this assistant's)" in prompt

    def test_workspace_ownership_for_coordinator(self):
        prompt = _build(is_coordinator=True)
        assert "Connected Google/Microsoft Workspace is **my boss's**" in prompt


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
        from unify.session_details import AssistantDetails, UserDesktopLink

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

    def test_proactive_meeting_absent_for_coordinator(self):
        prompt = _build(is_coordinator=True)
        assert "Proactive meeting offers" in prompt


# ---------------------------------------------------------------------------
# Tests – console knowledge
# ---------------------------------------------------------------------------


_CONSOLE_BLOCK = "Console knowledge\n-----------------\nSurfaces go here."


class TestConsoleKnowledge:
    """Console orientation comes from the running Console, not from this module.

    The prompt carries whatever text Console publishes, verbatim, and carries
    nothing when Console publishes nothing.
    """

    def test_console_block_is_passed_through_verbatim(self):
        prompt = _build(console_guidance=_CONSOLE_BLOCK)
        assert _CONSOLE_BLOCK in prompt

    def test_no_console_block_without_guidance(self):
        prompt = _build()
        assert "Console knowledge" not in prompt

    def test_coordinator_takes_the_same_block(self):
        prompt = _build(is_coordinator=True, console_guidance=_CONSOLE_BLOCK)
        assert _CONSOLE_BLOCK in prompt


# ---------------------------------------------------------------------------
# Tests – Console-UI / onboarding gate (local mode)
# ---------------------------------------------------------------------------


class TestConsoleUIGate:
    """Onboarding prompts are gated on ``console_ui_present`` so the public
    local install (no Console) gets a trimmed prompt with a local-mode note.
    A Console-less deployment also never fetches guidance, so the orientation
    block is absent by the same token."""

    def test_regular_console_knowledge_present_by_default(self):
        prompt = _build(is_coordinator=False, console_guidance=_CONSOLE_BLOCK)
        assert "Console knowledge" in prompt
        assert "Interaction surface" not in prompt

    def test_regular_console_knowledge_absent_in_local_mode(self):
        prompt = _build(is_coordinator=False, console_ui_present=False)
        assert "Console knowledge" not in prompt
        assert "Interaction surface" in prompt

    def test_coordinator_console_block_present_by_default(self):
        prompt = _build(is_coordinator=True, console_guidance=_CONSOLE_BLOCK)
        assert "Console knowledge" in prompt

    def test_coordinator_console_block_absent_in_local_mode(self):
        prompt = _build(is_coordinator=True, console_ui_present=False)
        assert "Console knowledge" not in prompt
        assert "Interaction surface" in prompt

    def test_voice_platform_knowledge_present_by_default(self):
        prompt = _build_voice(is_coordinator=False)
        assert "Platform knowledge" in prompt

    def test_voice_platform_knowledge_absent_in_local_mode(self):
        prompt = _build_voice(is_coordinator=False, console_ui_present=False)
        assert "Platform knowledge" not in prompt

    def test_voice_prompt_never_carries_the_console_block(self):
        """Console orientation is slow-brain-only, for two reasons.

        The Coordinator's fast brain freelanced contradictory "where do I click"
        answers when it held the same navigation knowledge, and this prompt is
        built in the LiveKit worker subprocess, which cannot see the Console
        presence the block is gated on.
        """
        for is_coordinator in (False, True):
            prompt = _build_voice(is_coordinator=is_coordinator)
            assert "Console knowledge" not in prompt


# ---------------------------------------------------------------------------
# Tests - unified fast-brain turn prompt
# ---------------------------------------------------------------------------


class TestFastBrainTurnPrompt:
    """The unified fast-brain turn prompt covers social, defer, silence, and
    continuation rules formerly split across smalltalk/filler/continuation paths."""

    def test_build_fast_brain_turn_messages_structure(self):
        from unify.conversation_manager.domains.fast_brain_turn import (
            FAST_BRAIN_TURN_PROMPT,
            build_fast_brain_turn_messages,
        )

        history = [
            {"role": "assistant", "content": "Hi there!"},
            {"role": "user", "content": "what did you just say?"},
        ]
        msgs = build_fast_brain_turn_messages(
            system_prompt="PERSONA",
            history_messages=history,
            user_text="what did you just say?",
            pending_continuation=None,
            already_deferred=False,
            guidance="",
            idle_status_smalltalk=False,
            recent_assistant_text="",
        )
        assert msgs[0] == {"role": "system", "content": "PERSONA"}
        assert msgs[-1] == {"role": "user", "content": "what did you just say?"}
        assert any(
            m["role"] == "system" and m["content"] == FAST_BRAIN_TURN_PROMPT
            for m in msgs
        )
        assert {"role": "assistant", "content": "Hi there!"} in msgs

    def test_idle_status_smalltalk_guidance_is_absent_by_default(self):
        from unify.conversation_manager.domains.fast_brain_turn import (
            build_fast_brain_turn_messages,
        )

        msgs = build_fast_brain_turn_messages(
            system_prompt="PERSONA",
            history_messages=[],
            user_text="what are you doing?",
            pending_continuation=None,
            already_deferred=False,
            guidance="",
            idle_status_smalltalk=False,
            recent_assistant_text="",
        )

        system_text = "\n".join(m["content"] for m in msgs if m["role"] == "system")
        assert "Idle status small-talk is available" not in system_text
        assert "Mario Kart" not in system_text

    def test_idle_status_smalltalk_guidance_is_gated(self):
        from unify.conversation_manager.domains.fast_brain_turn import (
            build_fast_brain_turn_messages,
        )

        msgs = build_fast_brain_turn_messages(
            system_prompt="PERSONA",
            history_messages=[],
            user_text="what are you doing?",
            pending_continuation=None,
            already_deferred=False,
            guidance="",
            idle_status_smalltalk=True,
            recent_assistant_text="",
        )

        system_text = "\n".join(m["content"] for m in msgs if m["role"] == "system")
        assert "Idle status small-talk is available" in system_text
        assert "what are you doing" in system_text
        assert "Snake" in system_text
        assert "Sudoku" in system_text
        assert "Mario Kart" in system_text
        assert "Tetris" in system_text

    def test_fast_brain_turn_prompt_allows_social_bio_selfcontext_repeat(self):
        from unify.conversation_manager.domains.fast_brain_turn import (
            FAST_BRAIN_TURN_PROMPT,
        )

        g = FAST_BRAIN_TURN_PROMPT.lower()
        assert "smalltalk" in g
        assert "repeat" in g
        assert "persona" in g or "who you are" in g

    def test_fast_brain_turn_prompt_defers_substantive_and_mixed(self):
        from unify.conversation_manager.domains.fast_brain_turn import (
            FAST_BRAIN_TURN_PROMPT,
        )

        g = FAST_BRAIN_TURN_PROMPT
        assert "defer" in g
        low = g.lower()
        assert "data" in low and "tools" in low
        assert "when unsure" in low

    def test_fast_brain_turn_prompt_stays_silent_on_bare_acks(self):
        from unify.conversation_manager.domains.fast_brain_turn import (
            FAST_BRAIN_TURN_PROMPT,
        )

        g = FAST_BRAIN_TURN_PROMPT
        assert "silence" in g
        low = g.lower()
        assert "acknowledgement" in low
        assert "never echo" in low
        assert "authorises an action" in low or "authorizes an action" in low

    def test_fast_brain_turn_prompt_interrupted_question_ack_is_defer(self):
        from unify.conversation_manager.domains.fast_brain_turn import (
            FAST_BRAIN_TURN_PROMPT,
        )

        low = FAST_BRAIN_TURN_PROMPT.lower()
        assert "interrupted mid-sentence" in low and "question" in low
        assert "agreeing to proceed" in low

    def test_fast_brain_turn_prompt_defers_action_and_status_questions(self):
        from unify.conversation_manager.domains.fast_brain_turn import (
            FAST_BRAIN_TURN_PROMPT,
        )

        low = FAST_BRAIN_TURN_PROMPT.lower()
        assert "status of work you control" in low
        assert "idle status small-talk" not in low

    def test_slow_brain_voice_guide_knows_idle_smalltalk_exception(self):
        prompt = _build(is_voice_call=True)
        assert "Idle small-talk exception" in prompt
        assert "playing Snake" in prompt
        assert "no in-flight action" in prompt


class TestOnboardingPromptLeakageGuard:
    """Onboarding and general restraint blocks must not invite parroting."""

    def test_conversational_restraint_forbids_prompt_leakage(self):
        prompt = _build()
        assert "No prompt leakage" in prompt
        assert "never quote, paraphrase, or summarize" in prompt

    def test_coordinator_onboarding_narration_forbids_parroting(self):
        prompt = _build(is_coordinator=True)
        assert "My onboarding narration" in prompt
        assert "internal guidance — I never repeat it to the user" in prompt
        assert "No genre lists, franchise names" in prompt

    def test_coordinator_onboarding_scaffolding_omitted_when_inactive(self):
        prompt = _build(
            is_coordinator=True,
            coordinator_onboarding_active=False,
        )
        assert "My onboarding narration" not in prompt
        assert "My onboarding progress (live)" not in prompt

    def test_coordinator_onboarding_scaffolding_present_when_active(self):
        prompt = _build(
            is_coordinator=True,
            coordinator_onboarding_active=True,
            coordinator_onboarding_render={
                "steps": [],
                "next_targets": [],
            },
        )
        assert "My onboarding narration" in prompt

    def test_reference_quiz_rules_omit_parrotable_franchise_lists(self):
        prompt = _build(is_coordinator=True)
        assert "Star Wars" not in prompt
        assert "Blade Runner" not in prompt
        assert "quick sci-fi quiz" in prompt
        assert "I NEVER list genres, franchises" in prompt

    def test_reference_quiz_requires_checklist_click_not_verbal_consent(self):
        prompt = _build(
            is_coordinator=True,
            coordinator_onboarding_active=True,
            coordinator_onboarding_render={
                "steps": [
                    {
                        "id": "email-reference",
                        "title": "Trigger email from T-W1N",
                        "phase": "Communication",
                        "status": "available",
                        "kind": "trigger",
                        "interaction": {
                            "type": "reference_quiz",
                            "tool_name": "send_email",
                        },
                    },
                ],
                "next_targets": [
                    {
                        "id": "email-reference",
                        "title": "Trigger email from T-W1N",
                        "nudge_chat": "Click the email row.",
                    },
                ],
            },
        )
        assert "verbal ask" in prompt.lower() or "verbal consent" in prompt.lower()
        assert (
            "does not substitute" in prompt.lower()
            or "does not count" in prompt.lower()
        )
        assert "Trigger ... from T-W1N" in prompt

    def test_reference_quiz_treats_prior_session_clue_as_lost(self):
        """A clue dispatched before this conversation must be re-sent, not
        confirmed: pointing a returning user at a days-old email is a silent
        failure ("it's on its way") from their side of the mailbox."""
        prompt = _build(
            is_coordinator=True,
            coordinator_onboarding_active=True,
            coordinator_onboarding_render={
                "steps": [
                    {
                        "id": "email-reply",
                        "title": "Reply to email",
                        "phase": "Communication",
                        "status": "in_progress",
                        "dispatched_at": "2026-07-31T15:02:49+00:00",
                        "kind": "reply",
                    },
                ],
                "next_targets": [],
            },
        )
        assert "counts as sent if I sent it in THIS conversation" in prompt
        assert "predates this conversation" in prompt
        assert "re-send a fresh clue" in prompt
        assert "NEVER claim an old clue is on its way" in prompt
        assert "since 2026-07-31T15:02:49+00:00" in prompt

    def test_onboarding_requires_responsive_unify_message_chat(self):
        prompt = _build(is_coordinator=True, coordinator_onboarding_active=True)
        assert "Rules for unify_message during onboarding" in prompt
        assert "never `wait`" in prompt
        assert (
            "Do not leave chat silent while only the other channel carries the clue"
            in prompt
        )

    def test_conversational_restraint_keeps_unify_message_responsive(self):
        prompt = _build()
        assert "Unify message / Console chat is the live thread" in prompt
        assert "not over answering inbound chat" in prompt
        assert "Never `wait` while their chat line is still unanswered" in prompt

    def test_onboarding_progress_leads_with_whats_next_answer(self):
        prompt = _build(
            is_coordinator=True,
            coordinator_onboarding_active=True,
            coordinator_onboarding_render={
                "steps": [
                    {
                        "id": "whatsapp-number",
                        "title": "Add your WhatsApp number",
                        "phase": "Communication",
                        "status": "done",
                    },
                    {
                        "id": "whatsapp-message-reference",
                        "title": "Trigger WhatsApp message from T-W1N",
                        "phase": "Communication",
                        "status": "available",
                        "kind": "trigger",
                    },
                    {
                        "id": "phone-number",
                        "title": "Add your phone number",
                        "phase": "Communication",
                        "status": "available",
                        "kind": "setup",
                    },
                ],
                "next_targets": [
                    {
                        "id": "whatsapp-message-reference",
                        "title": "Trigger WhatsApp message from T-W1N",
                        "nudge_chat": "Click the WhatsApp message row.",
                    },
                    {
                        "id": "phone-number",
                        "title": "Add your phone number",
                        "nudge_chat": "Click the phone row.",
                    },
                ],
            },
        )
        whats_next_pos = prompt.index("When they ask what to do next")
        checklist_pos = prompt.index("Full checklist")
        assert whats_next_pos < checklist_pos
        assert "Primary answer: Trigger WhatsApp message from T-W1N" in prompt
        assert "Do NOT volunteer next steps unprompted" in prompt
        assert "collect all numbers first" in prompt
        assert "Startable steps right now" in prompt
        assert "1. Trigger WhatsApp message from T-W1N" in prompt
        assert "2. Add your phone number" in prompt


class TestFastBrainTurnGuidance:
    """The note handed to the slow brain after the Voice Agent finishes a turn.

    The Voice Agent's line is already in the caller's ears by the time the slow
    brain runs, so the note has to read as reported fact. Phrasing it as an
    intention invites a reply that re-answers the question and opens with a
    second "Yes".
    """

    @pytest.mark.parametrize(
        "classification",
        ["defer", "smalltalk", "continuation", "some_future_classification"],
    )
    def test_line_is_presented_as_already_spoken(self, classification):
        note = build_fast_brain_turn_guidance(
            classification=classification,
            intended_speech="Yes — I can take a look at that.",
        )
        assert "Yes — I can take a look at that." in note
        assert "intended speech" not in note.lower()
        assert "heard" in note.lower()

    def test_defer_asks_for_continuation_not_a_fresh_reply(self):
        note = build_fast_brain_turn_guidance(
            classification="defer",
            intended_speech="Yes — I can take a look at that.",
        )
        lowered = note.lower()
        assert "continues that same piece of speech" in lowered
        assert "do not restate it" in lowered
        assert "do not re-answer" in lowered
        assert "same yes/no" in lowered
        # The filler framing told the slow brain the line did not count.
        assert "filler" not in lowered

    def test_smalltalk_still_asks_for_silence(self):
        note = build_fast_brain_turn_guidance(
            classification="smalltalk",
            intended_speech="It's just gone nine here.",
        )
        assert "wait()" in note
        assert "do not repeat or paraphrase it" in note.lower()

    def test_undecided_says_nothing_was_spoken(self):
        """The one classification where the shared framing above is a lie.

        Every other branch reports a line the caller already heard. On an
        undecided turn the fast brain said nothing at all, so telling the slow
        brain to "continue from it" would have it resume a sentence that never
        existed — and open mid-thought to a caller still waiting on a first word.
        """
        note = build_fast_brain_turn_guidance(
            classification="undecided",
            intended_speech="",
        )
        lowered = note.lower()
        assert "nothing was said aloud" in lowered
        assert "no reply and no filler" in lowered
        assert "nothing to continue from" in lowered

    def test_undecided_asks_whose_turn_it_was(self):
        """It is handed over precisely because that question is still open."""
        note = build_fast_brain_turn_guidance(
            classification="undecided",
            intended_speech="",
        )
        assert "Another assistant is on this call" in note
        assert "answer it in full via guide_voice_agent" in note
        assert "wait()" in note

    def test_undecided_does_not_take_the_generic_already_spoken_branch(self):
        """A future classification falls through to "you just said this"; this
        one must not, so it is checked before the fall-through."""
        note = build_fast_brain_turn_guidance(
            classification="undecided",
            intended_speech="",
        )
        assert "You have just said this aloud" not in note

    def test_voice_guide_treats_the_spoken_line_as_delivered(self):
        guide = _build_voice_calls_guide()
        assert "mine and already delivered" in guide
        assert "Continue from what I just said." in guide
        assert 'never "Yes. The quickest way is…"' in guide


class TestVoiceMultiPartyBlock:
    """The room, in the voice agent's own prompt.

    Added as its own block rather than through ``participants``, which is
    exclusive with "Primary caller context" and reads keys the org roster does
    not carry — routing the roster there would strip a 1:1 meet's caller
    identity and render nothing in its place.
    """

    HEADING = "Who is on this call"

    def test_absent_on_a_one_to_one_call(self):
        assert self.HEADING not in _build_voice()

    def test_absent_with_one_other_person(self):
        assert self.HEADING not in _build_voice(call_participant_names=["Ada"])

    def test_blank_names_do_not_make_a_room(self):
        assert self.HEADING not in _build_voice(
            call_participant_names=["Ada", "", "  "],
        )

    def test_present_and_named_for_a_group(self):
        prompt = _build_voice(call_participant_names=["Ada", "Bo"])
        assert self.HEADING in prompt
        assert "Ada, Bo" in prompt

    def test_the_caller_identity_block_survives(self):
        """The regression routing the roster through `participants` would cause."""
        prompt = _build_voice(call_participant_names=["Ada", "Bo"])
        assert "Primary caller context" in prompt

    def test_it_separates_who_is_talking_from_who_is_addressed(self):
        """An attributed turn says who spoke; only the second decides the turn."""
        prompt = _build_voice(call_participant_names=["Ada", "Bo"])
        assert "who is TALKING" in prompt
        assert "who is being talked TO" in prompt

    def test_it_says_the_contact_is_not_the_only_speaker(self):
        """Otherwise a new voice reads as the same person under a new name."""
        prompt = _build_voice(call_participant_names=["Ada", "Bo"])
        assert "not the only person who might speak" in prompt
