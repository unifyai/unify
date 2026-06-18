"""Synthetic fixture corpus for the Droid vs Hermes artifact benchmark."""

from __future__ import annotations

from pathlib import PurePosixPath

from .models import EmailBatch, EmailMessage, ExpectedEmailOutcome, OutputContract

OUTPUT_CONTRACT = OutputContract(
    root_pattern="benchmark-output/{arm_id}/{seed}/{batch_id}",
    corpus_file=PurePosixPath("corpus.json"),
    drafts_file=PurePosixPath("drafts.json"),
    artifact_file=PurePosixPath("artifact.json"),
    trace_file=PurePosixPath("trace.json"),
    report_file=PurePosixPath("report.md"),
)


def synthetic_email_batches() -> tuple[EmailBatch, ...]:
    """Return fixed inbox slices used by every benchmark arm.

    The corpus intentionally mixes exact work (date filtering, reply/no-reply
    flags, JSON shaping) with semantic work (classification and draft tone).
    """

    return (
        EmailBatch(
            batch_id="batch-2026-06-04",
            since="2026-06-04T00:00:00+01:00",
            description=(
                "Initial calibration batch: mixed inbox since yesterday, "
                "including support, investor, calendar, newsletter, and spam."
            ),
            emails=(
                EmailMessage(
                    message_id="msg-001",
                    received_at="2026-06-04T08:12:00+01:00",
                    from_email="maya@northstar.ai",
                    from_name="Maya Chen",
                    subject="Pilot kickoff docs",
                    body=(
                        "Could you send the SOC2 summary and a short note on "
                        "how Droid handles mid-run steering before our pilot "
                        "kickoff tomorrow?"
                    ),
                    labels=["inbox", "customer"],
                ),
                EmailMessage(
                    message_id="msg-002",
                    received_at="2026-06-04T09:45:00+01:00",
                    from_email="calendar-notification@google.com",
                    from_name="Google Calendar",
                    subject="Updated invitation: Droid walkthrough",
                    body=(
                        "This event has been moved to Friday Jun 5 at 14:00. "
                        "No response is required."
                    ),
                    labels=["inbox", "calendar"],
                ),
                EmailMessage(
                    message_id="msg-003",
                    received_at="2026-06-04T13:20:00+01:00",
                    from_email="lee@seedfund.example",
                    from_name="Lee",
                    subject="Quick diligence question",
                    body=(
                        "Can you clarify whether FunctionManager functions are "
                        "stored as callable artifacts or just prompt snippets?"
                    ),
                    labels=["inbox", "investor"],
                ),
                EmailMessage(
                    message_id="msg-004",
                    received_at="2026-06-04T16:02:00+01:00",
                    from_email="digest@mlopsweekly.example",
                    from_name="MLOps Weekly",
                    subject="This week in agents",
                    body="Top stories: eval harnesses, vector databases, and agent sandboxes.",
                    labels=["inbox", "newsletter"],
                ),
                EmailMessage(
                    message_id="msg-005",
                    received_at="2026-06-04T21:17:00+01:00",
                    from_email="security@bigco.example",
                    from_name="BigCo Security",
                    subject="URGENT: access review due today",
                    body=(
                        "Our procurement portal says your vendor access review "
                        "is still incomplete. Please confirm the owner and send "
                        "the updated security contact today."
                    ),
                    labels=["inbox", "security"],
                ),
                EmailMessage(
                    message_id="msg-006",
                    received_at="2026-06-04T23:55:00+01:00",
                    from_email="promo@random-saas.example",
                    from_name="Random SaaS",
                    subject="Last chance to unlock seamless productivity",
                    body="Buy now and get 80% off. Click here.",
                    labels=["inbox", "promotion"],
                ),
            ),
            expected=(
                ExpectedEmailOutcome(
                    message_id="msg-001",
                    category="needs_reply",
                    needs_reply=True,
                    draft_reply=(
                        "Hi Maya - yes, I can send both. I will include the SOC2 "
                        "summary and a short note on how Droid keeps reusable "
                        "functions callable through FunctionManager."
                    ),
                    rationale="Customer asks for concrete docs before a scheduled kickoff.",
                ),
                ExpectedEmailOutcome(
                    message_id="msg-002",
                    category="meeting_or_calendar",
                    needs_reply=False,
                    draft_reply=None,
                    rationale="Calendar update explicitly says no response is required.",
                ),
                ExpectedEmailOutcome(
                    message_id="msg-003",
                    category="needs_reply",
                    needs_reply=True,
                    draft_reply=(
                        "Hi Lee - they are callable artifacts. The stored function "
                        "has a name, signature, implementation, and can be invoked "
                        "directly instead of being reread as procedural prompt text."
                    ),
                    rationale="Investor asks a substantive product question.",
                ),
                ExpectedEmailOutcome(
                    message_id="msg-004",
                    category="newsletter",
                    needs_reply=False,
                    draft_reply=None,
                    rationale="Bulk newsletter with no direct ask.",
                ),
                ExpectedEmailOutcome(
                    message_id="msg-005",
                    category="urgent_action",
                    needs_reply=True,
                    draft_reply=(
                        "Hi - thanks for the heads up. I am checking the owner and "
                        "security contact now and will send the update today."
                    ),
                    rationale="Time-sensitive access review with a direct request.",
                ),
                ExpectedEmailOutcome(
                    message_id="msg-006",
                    category="ignore",
                    needs_reply=False,
                    draft_reply=None,
                    rationale="Promotional spam with no business relevance.",
                ),
            ),
        ),
        EmailBatch(
            batch_id="batch-2026-06-05",
            since="2026-06-05T00:00:00+01:00",
            description="Repeat-run batch with the same task family but new messages.",
            emails=(
                EmailMessage(
                    message_id="msg-101",
                    received_at="2026-06-05T07:31:00+01:00",
                    from_email="danielle@acme.example",
                    from_name="Danielle",
                    subject="Can we move the integration review?",
                    body=(
                        "I have a conflict at 3. Could we move the integration "
                        "review to Monday morning?"
                    ),
                    labels=["inbox", "customer"],
                ),
                EmailMessage(
                    message_id="msg-102",
                    received_at="2026-06-05T08:03:00+01:00",
                    from_email="alerts@github.com",
                    from_name="GitHub",
                    subject="[unifyai/droid] CI fixed on main",
                    body="The previously failing build is now green.",
                    labels=["inbox", "notification"],
                ),
                EmailMessage(
                    message_id="msg-103",
                    received_at="2026-06-05T10:18:00+01:00",
                    from_email="sam@partner.example",
                    from_name="Sam",
                    subject="Draft MSA redlines",
                    body=(
                        "We left two comments in the MSA. Could you confirm "
                        "whether the DPA language is acceptable?"
                    ),
                    labels=["inbox", "legal"],
                ),
            ),
            expected=(
                ExpectedEmailOutcome(
                    message_id="msg-101",
                    category="meeting_or_calendar",
                    needs_reply=True,
                    draft_reply=(
                        "Hi Danielle - Monday morning works on my side. Send me "
                        "the time that is easiest for you and I will move it."
                    ),
                    rationale="Meeting reschedule request needs a reply.",
                ),
                ExpectedEmailOutcome(
                    message_id="msg-102",
                    category="fyi",
                    needs_reply=False,
                    draft_reply=None,
                    rationale="Notification only; no action requested.",
                ),
                ExpectedEmailOutcome(
                    message_id="msg-103",
                    category="needs_reply",
                    needs_reply=True,
                    draft_reply=(
                        "Hi Sam - thanks, I will look at the two comments and "
                        "come back on the DPA language shortly."
                    ),
                    rationale="Partner/legal thread asks for confirmation.",
                ),
            ),
        ),
    )


def workweek_email_batches() -> tuple[EmailBatch, ...]:
    """Return Monday-Friday inbox slices for the live recurring-task simulation."""

    return (
        EmailBatch(
            batch_id="monday-2026-06-01",
            since="2026-06-01T00:00:00+01:00",
            description="Monday 9am activation: customer ask, newsletter, and alert.",
            emails=(
                EmailMessage(
                    message_id="mon-001",
                    received_at="2026-06-01T07:12:00+01:00",
                    from_email="maya@northstar.ai",
                    from_name="Maya Chen",
                    subject="Pilot kickoff docs",
                    body=(
                        "Could you send the SOC2 summary and a short note on how "
                        "Droid handles mid-run steering before kickoff?"
                    ),
                    labels=["inbox", "customer"],
                ),
                EmailMessage(
                    message_id="mon-002",
                    received_at="2026-06-01T08:04:00+01:00",
                    from_email="digest@mlopsweekly.example",
                    from_name="MLOps Weekly",
                    subject="This week in agents",
                    body="Top stories: eval harnesses, sandboxes, and vector DBs.",
                    labels=["inbox", "newsletter"],
                ),
                EmailMessage(
                    message_id="mon-003",
                    received_at="2026-06-01T08:41:00+01:00",
                    from_email="security@bigco.example",
                    from_name="BigCo Security",
                    subject="URGENT: access review due today",
                    body=(
                        "The vendor access review is still incomplete. Please "
                        "confirm the owner and updated security contact today."
                    ),
                    labels=["inbox", "security"],
                ),
            ),
            expected=(
                ExpectedEmailOutcome(
                    message_id="mon-001",
                    category="needs_reply",
                    needs_reply=True,
                    draft_reply=(
                        "Hi Maya - yes, I can send both. I will include the SOC2 "
                        "summary and a short note on mid-run steering."
                    ),
                    rationale="Customer asks for concrete docs before kickoff.",
                ),
                ExpectedEmailOutcome(
                    message_id="mon-002",
                    category="newsletter",
                    needs_reply=False,
                    draft_reply=None,
                    rationale="Bulk newsletter with no direct ask.",
                ),
                ExpectedEmailOutcome(
                    message_id="mon-003",
                    category="urgent_action",
                    needs_reply=True,
                    draft_reply=(
                        "Hi - thanks for the heads up. I am checking the owner "
                        "and security contact now and will send the update today."
                    ),
                    rationale="Time-sensitive access-review request.",
                ),
            ),
        ),
        EmailBatch(
            batch_id="tuesday-2026-06-02",
            since="2026-06-02T00:00:00+01:00",
            description="Tuesday 9am activation: reschedule request and CI notice.",
            emails=(
                EmailMessage(
                    message_id="tue-001",
                    received_at="2026-06-02T07:31:00+01:00",
                    from_email="danielle@acme.example",
                    from_name="Danielle",
                    subject="Can we move the integration review?",
                    body=(
                        "I have a conflict at 3. Could we move the integration "
                        "review to Wednesday morning?"
                    ),
                    labels=["inbox", "customer"],
                ),
                EmailMessage(
                    message_id="tue-002",
                    received_at="2026-06-02T08:03:00+01:00",
                    from_email="alerts@github.com",
                    from_name="GitHub",
                    subject="[unifyai/droid] CI fixed on main",
                    body="The previously failing build is now green.",
                    labels=["inbox", "notification"],
                ),
            ),
            expected=(
                ExpectedEmailOutcome(
                    message_id="tue-001",
                    category="meeting_or_calendar",
                    needs_reply=True,
                    draft_reply=(
                        "Hi Danielle - Wednesday morning works on my side. Send "
                        "me the time that is easiest and I will move it."
                    ),
                    rationale="Meeting reschedule request needs a reply.",
                ),
                ExpectedEmailOutcome(
                    message_id="tue-002",
                    category="fyi",
                    needs_reply=False,
                    draft_reply=None,
                    rationale="Notification only; no action requested.",
                ),
            ),
        ),
        EmailBatch(
            batch_id="wednesday-2026-06-03",
            since="2026-06-03T00:00:00+01:00",
            description="Wednesday 9am activation: legal ask and spam.",
            emails=(
                EmailMessage(
                    message_id="wed-001",
                    received_at="2026-06-03T08:18:00+01:00",
                    from_email="sam@partner.example",
                    from_name="Sam",
                    subject="Draft MSA redlines",
                    body=(
                        "We left two comments in the MSA. Could you confirm "
                        "whether the DPA language is acceptable?"
                    ),
                    labels=["inbox", "legal"],
                ),
                EmailMessage(
                    message_id="wed-002",
                    received_at="2026-06-03T08:49:00+01:00",
                    from_email="promo@random-saas.example",
                    from_name="Random SaaS",
                    subject="Last chance to unlock seamless productivity",
                    body="Buy now and get 80% off. Click here.",
                    labels=["inbox", "promotion"],
                ),
            ),
            expected=(
                ExpectedEmailOutcome(
                    message_id="wed-001",
                    category="needs_reply",
                    needs_reply=True,
                    draft_reply=(
                        "Hi Sam - thanks, I will look at the two comments and "
                        "come back on the DPA language shortly."
                    ),
                    rationale="Partner/legal thread asks for confirmation.",
                ),
                ExpectedEmailOutcome(
                    message_id="wed-002",
                    category="ignore",
                    needs_reply=False,
                    draft_reply=None,
                    rationale="Promotional spam with no business relevance.",
                ),
            ),
        ),
        EmailBatch(
            batch_id="thursday-2026-06-04",
            since="2026-06-04T00:00:00+01:00",
            description="Thursday 9am activation: investor product question.",
            emails=(
                EmailMessage(
                    message_id="thu-001",
                    received_at="2026-06-04T08:20:00+01:00",
                    from_email="lee@seedfund.example",
                    from_name="Lee",
                    subject="Quick diligence question",
                    body=(
                        "Can you clarify whether FunctionManager functions are "
                        "stored as callable artifacts or just prompt snippets?"
                    ),
                    labels=["inbox", "investor"],
                ),
            ),
            expected=(
                ExpectedEmailOutcome(
                    message_id="thu-001",
                    category="needs_reply",
                    needs_reply=True,
                    draft_reply=(
                        "Hi Lee - they are callable artifacts with names, "
                        "signatures, and implementations, not prompt snippets."
                    ),
                    rationale="Investor asks a substantive product question.",
                ),
            ),
        ),
        EmailBatch(
            batch_id="friday-2026-06-05",
            since="2026-06-05T00:00:00+01:00",
            description="Friday 9am activation: calendar update and customer ask.",
            emails=(
                EmailMessage(
                    message_id="fri-001",
                    received_at="2026-06-05T07:12:00+01:00",
                    from_email="calendar-notification@google.com",
                    from_name="Google Calendar",
                    subject="Updated invitation: Droid walkthrough",
                    body="This event has been moved to Friday at 14:00. No response is required.",
                    labels=["inbox", "calendar"],
                ),
                EmailMessage(
                    message_id="fri-002",
                    received_at="2026-06-05T08:22:00+01:00",
                    from_email="maya@northstar.ai",
                    from_name="Maya Chen",
                    subject="One more pilot question",
                    body="Could you also send a sentence on how recurring task entrypoints work?",
                    labels=["inbox", "customer"],
                ),
            ),
            expected=(
                ExpectedEmailOutcome(
                    message_id="fri-001",
                    category="meeting_or_calendar",
                    needs_reply=False,
                    draft_reply=None,
                    rationale="Calendar update explicitly says no response required.",
                ),
                ExpectedEmailOutcome(
                    message_id="fri-002",
                    category="needs_reply",
                    needs_reply=True,
                    draft_reply=(
                        "Hi Maya - yes, I will include a short note that recurring "
                        "tasks can attach a stored function as the future entrypoint."
                    ),
                    rationale="Customer asks for one more concrete explanation.",
                ),
            ),
        ),
    )
