"""Bundled Learning onboarding bank-export fixtures and workspace provisioning.

The expenses-etl tutorial narrates exact numbers over fixed CSV rows shipped as
package data. The ``learning_beat_requested`` handler copies them into the twin
local workspace so ``act(persist=True)`` reads stable files — nothing is generated
at runtime.
"""

from __future__ import annotations

import csv
import shutil
from dataclasses import dataclass
from importlib import resources
from pathlib import Path

LEARNING_EXPENSES_BASE_DIR = "onboarding/learning/expenses"
LEARNING_EXPENSES_MONTH_N = "2026-01"
LEARNING_EXPENSES_MONTH_N_PLUS_1 = "2026-02"
LEARNING_EXPENSES_NAIVE_MARGIN = 400

# Narration copy mirrors :func:`compute_naive_spend_total` /
# :func:`compute_corrected_spend_total` — keep in sync if those rules change.
LEARNING_EXPENSES_NAIVE_MISTAKE_DESCRIPTION = (
    "sum every outflow as spend, add abs(Amount) again for each INTERNAL XFER "
    "row on either file (including card-side credits) so the transfer is "
    "double-counted, and ignore refunds"
)
LEARNING_EXPENSES_CORRECTION_RULE_DESCRIPTION = (
    "skip INTERNAL XFER rows; sum remaining outflows; net REFUND rows against spend"
)

LEARNING_EXPENSES_USER_CORRECTION_TEXT = (
    "Exclude internal transfer rows and net refunds against spend when computing "
    "monthly spend."
)

LEARNING_EXPENSES_GUIDANCE_TITLE = "Monthly bank export spend rules"
LEARNING_EXPENSES_FUNCTION_NAME = "compute_monthly_spend_from_bank_exports"


def learning_expenses_storage_check_nudge() -> str:
    """Explicit StorageCheck mandate for the learning correction interjection."""
    return (
        "StorageCheck memoization (for the post-act review loop — do NOT call "
        "GuidanceManager or FunctionManager store tools in the doing loop): "
        f"persist Guidance titled {LEARNING_EXPENSES_GUIDANCE_TITLE!r} with the "
        f"user's correction rule ({LEARNING_EXPENSES_CORRECTION_RULE_DESCRIPTION}) "
        f"and Function {LEARNING_EXPENSES_FUNCTION_NAME!r} for the corrected "
        "monthly spend pipeline from checking+card CSV exports."
    )


def learning_expenses_stop_act_for_storage_rule() -> str:
    """CM must end the persist act after the improved deliverable to run StorageCheck."""
    return (
        "After sending the improved deliverable tagged "
        "onboarding_learning_phase=improved, call stop_* on the running "
        "persist act in the SAME turn — StorageCheck only starts once the "
        "persist session ends, not while it sits in awaiting_input. Tell the "
        "user in plain language that you are stopping the action so Brain can "
        "save their rule (for example: stopping it now so your correction gets "
        "saved), then invoke stop_* before inviting February."
    )


def learning_expenses_user_facing_voice() -> str:
    """Plain-language rules for Learning demo chat messages (non-technical audience)."""
    return (
        "User-facing voice: the audience is non-technical. Keep every learning-demo "
        "chat message short and scannable — a headline dollar total plus one or two "
        "plain sentences. Do NOT send markdown tables, line-by-line row breakdowns, "
        "disposition/contribution columns, or accounting jargon (gross outflows, "
        "netted, phantom spending, rule 1/rule 2). The CSVs are attachments for "
        "anyone curious; do not recite every row in chat. "
        "Opening message: first teach Brain in 3–4 short concept bullets — "
        "corrections stick (learning), Guidance is my playbook for how to work, "
        "Functions are reusable skills, and together they mean less re-explaining "
        "on similar tasks. Keep concept lines domain-agnostic (no CSV/month/"
        "pipeline jargon). Then preview the hands-on demo in at most five short "
        "plan bullets; casual tone, not a compliance brief. "
        "Attachment captions: one sentence each (what the file is; mention the "
        "checking↔card transfer trap in plain English). "
        "First-attempt deliverable: state the naive total, then one sentence on "
        "the mistake (double-counted the internal transfer between checking and "
        "card), then the exact correction text to paste — nothing else. "
        "Improved deliverable: state the corrected total, one sentence on what "
        "changed (skipped transfers, counted refunds), optionally one contrast "
        "vs the naive total — say you are stopping the action so Brain can save "
        "their rule, then stop_* the persist act in the same turn, then "
        "Brain/StorageCheck nudge and invite February. "
        "Replay deliverable: corrected total for the new month in one line."
    )


_BUNDLED_ASSET_PACKAGE = "unify.assets.onboarding.learning.expenses"
_BUNDLED_FILENAMES: tuple[str, ...] = (
    f"checking-{LEARNING_EXPENSES_MONTH_N}.csv",
    f"card-{LEARNING_EXPENSES_MONTH_N}.csv",
    f"checking-{LEARNING_EXPENSES_MONTH_N_PLUS_1}.csv",
    f"card-{LEARNING_EXPENSES_MONTH_N_PLUS_1}.csv",
)


@dataclass(frozen=True)
class ExpenseRow:
    date: str
    description: str
    amount: float


def _csv_relative_path(kind: str, month: str) -> str:
    return f"{LEARNING_EXPENSES_BASE_DIR}/{kind}-{month}.csv"


def bundled_fixture_relative_paths() -> tuple[str, ...]:
    """Workspace-relative paths for all four bundled CSV fixtures."""
    return (
        _csv_relative_path("checking", LEARNING_EXPENSES_MONTH_N),
        _csv_relative_path("card", LEARNING_EXPENSES_MONTH_N),
        _csv_relative_path("checking", LEARNING_EXPENSES_MONTH_N_PLUS_1),
        _csv_relative_path("card", LEARNING_EXPENSES_MONTH_N_PLUS_1),
    )


def parse_expense_csv(content: str) -> list[ExpenseRow]:
    """Parse one bundled bank-export CSV into typed rows."""
    reader = csv.DictReader(content.strip().splitlines())
    rows: list[ExpenseRow] = []
    for raw in reader:
        date = (raw.get("Date") or "").strip()
        description = (raw.get("Description") or "").strip()
        amount_raw = (raw.get("Amount") or "").strip()
        if not date or not description or not amount_raw:
            raise ValueError(f"Invalid CSV row: {raw}")
        rows.append(
            ExpenseRow(
                date=date,
                description=description,
                amount=float(amount_raw),
            ),
        )
    return rows


def _is_internal_transfer(description: str) -> bool:
    return "internal xfer" in description.lower()


def _is_refund(description: str) -> bool:
    return "refund" in description.lower()


def compute_naive_spend_total(
    checking: list[ExpenseRow],
    card: list[ExpenseRow],
) -> float:
    """Naive spend: sum outflows, then add abs(Amount) again for each internal xfer row."""
    total = 0.0
    for row in [*checking, *card]:
        if row.amount < 0:
            total += abs(row.amount)
        if _is_internal_transfer(row.description):
            total += abs(row.amount)
    return total


def compute_corrected_spend_total(
    checking: list[ExpenseRow],
    card: list[ExpenseRow],
) -> float:
    """Corrected spend: exclude internal transfers; net refunds against prior spend."""
    total = 0.0
    for row in [*checking, *card]:
        if _is_internal_transfer(row.description):
            continue
        if row.amount < 0:
            total += abs(row.amount)
        elif _is_refund(row.description):
            total = max(0.0, total - row.amount)
    return total


def assert_month_invariants(
    checking: list[ExpenseRow],
    card: list[ExpenseRow],
    *,
    label: str,
) -> None:
    """Fail when bundled month-N or month-N+1 exports drift from tutorial needs."""
    transfer_rows = [
        row for row in [*checking, *card] if _is_internal_transfer(row.description)
    ]
    if len(transfer_rows) < 2:
        raise AssertionError(
            f"{label}: expected internal transfer rows on both exports",
        )
    if not any(_is_internal_transfer(row.description) for row in checking):
        raise AssertionError(
            f"{label}: internal transfer must appear in checking export",
        )
    if not any(_is_internal_transfer(row.description) for row in card):
        raise AssertionError(
            f"{label}: internal transfer must appear in card export",
        )

    if not any(_is_refund(row.description) for row in [*checking, *card]):
        raise AssertionError(f"{label}: expected at least one refund row")

    naive = compute_naive_spend_total(checking, card)
    corrected = compute_corrected_spend_total(checking, card)
    if naive - corrected < LEARNING_EXPENSES_NAIVE_MARGIN:
        raise AssertionError(
            f"{label}: naive spend {naive:.2f} must exceed corrected "
            f"{corrected:.2f} by >= {LEARNING_EXPENSES_NAIVE_MARGIN}",
        )


def assert_bundled_fixture_invariants() -> None:
    """Verify all four bundled CSVs satisfy the expenses-etl tutorial contract."""
    asset_root = resources.files(_BUNDLED_ASSET_PACKAGE)
    month_n_checking = parse_expense_csv(
        (asset_root / f"checking-{LEARNING_EXPENSES_MONTH_N}.csv").read_text(
            encoding="utf-8",
        ),
    )
    month_n_card = parse_expense_csv(
        (asset_root / f"card-{LEARNING_EXPENSES_MONTH_N}.csv").read_text(
            encoding="utf-8",
        ),
    )
    month_n1_checking = parse_expense_csv(
        (asset_root / f"checking-{LEARNING_EXPENSES_MONTH_N_PLUS_1}.csv").read_text(
            encoding="utf-8",
        ),
    )
    month_n1_card = parse_expense_csv(
        (asset_root / f"card-{LEARNING_EXPENSES_MONTH_N_PLUS_1}.csv").read_text(
            encoding="utf-8",
        ),
    )
    assert_month_invariants(month_n_checking, month_n_card, label="month N")
    assert_month_invariants(
        month_n1_checking,
        month_n1_card,
        label="month N+1",
    )


def provision_learning_expenses_fixtures(local_root: str | Path) -> list[str]:
    """Copy bundled CSV fixtures into the twin workspace when absent.

    Returns workspace-relative paths written on this call. Existing files are
    left untouched so a re-click or handler retry stays idempotent.
    """
    root = Path(local_root).expanduser().resolve()
    asset_root = resources.files(_BUNDLED_ASSET_PACKAGE)
    written: list[str] = []
    for filename in _BUNDLED_FILENAMES:
        relative_path = f"{LEARNING_EXPENSES_BASE_DIR}/{filename}"
        destination = root / relative_path
        if destination.exists():
            continue
        destination.parent.mkdir(parents=True, exist_ok=True)
        with resources.as_file(asset_root / filename) as bundled_path:
            shutil.copy2(bundled_path, destination)
        written.append(relative_path)
    return written


def learning_expenses_naive_algorithm_lines() -> tuple[str, ...]:
    """Step-by-step naive spend rules the first act(persist=True) pass must follow."""
    return (
        "For each row in both CSVs, if Amount < 0 add abs(Amount) to spend.",
        "For each row whose Description contains INTERNAL XFER, add abs(Amount) "
        "again — including card-side credits — so the same transfer is counted twice.",
        "Do not subtract refunds, payroll, or other credits.",
    )


def learning_expenses_corrected_algorithm_lines() -> tuple[str, ...]:
    """Step-by-step corrected spend rules for the improved deliverable and replay."""
    return (
        "Skip rows whose Description contains INTERNAL XFER.",
        "Add abs(Amount) for each remaining outflow (Amount < 0).",
        "Subtract rows whose Description contains REFUND from spend (floor at zero).",
    )


def learning_expenses_scenario_prompt_lines() -> tuple[str, ...]:
    """Fixed scenario facts for Learning tutorial narration.

    Built from module constants so prompt copy stays aligned if fixtures or
    paths change.
    """
    checking_month_n = _csv_relative_path("checking", LEARNING_EXPENSES_MONTH_N)
    card_month_n = _csv_relative_path("card", LEARNING_EXPENSES_MONTH_N)
    return (
        f"Two bank exports per month — `{checking_month_n}` and `{card_month_n}` "
        f"under `{LEARNING_EXPENSES_BASE_DIR}/`; month N = "
        f"{LEARNING_EXPENSES_MONTH_N}, month N+1 = "
        f"{LEARNING_EXPENSES_MONTH_N_PLUS_1} (reserved for the replay).",
        "Task: build a monthly spending report from those exports.",
        f"The deliberate naive mistake: {LEARNING_EXPENSES_NAIVE_MISTAKE_DESCRIPTION}.",
        "Naive algorithm for the first act pass:",
        *learning_expenses_naive_algorithm_lines(),
        f"The correction rule: {LEARNING_EXPENSES_CORRECTION_RULE_DESCRIPTION}.",
        "Corrected algorithm after the user sends this correction text:",
        f'"{LEARNING_EXPENSES_USER_CORRECTION_TEXT}"',
        *learning_expenses_corrected_algorithm_lines(),
        learning_expenses_storage_check_nudge(),
        learning_expenses_stop_act_for_storage_rule(),
        "Numbers are always computed from the files via act(persist=True), never "
        f"asserted — bundled fixtures guarantee naive vs corrected differ by at "
        f"least ${LEARNING_EXPENSES_NAIVE_MARGIN:.0f}.",
    )


def learning_expenses_first_attempt_act_query() -> str:
    """Query text for act(persist=True) on the month-N naive first attempt."""
    checking = _csv_relative_path("checking", LEARNING_EXPENSES_MONTH_N)
    card = _csv_relative_path("card", LEARNING_EXPENSES_MONTH_N)
    algo = "\n".join(f"- {line}" for line in learning_expenses_naive_algorithm_lines())
    return (
        f"Learning onboarding — first attempt (month N = {LEARNING_EXPENSES_MONTH_N}).\n\n"
        f"Read `{checking}` and `{card}` under `{LEARNING_EXPENSES_BASE_DIR}/`.\n"
        "Build the monthly spending report.\n\n"
        f"Apply this naive spend algorithm exactly:\n{algo}\n\n"
        "Parse the CSVs with execute_code and compute totals — never hardcode numbers.\n\n"
        "Return the naive total (numeric) and a brief internal summary (2–3 sentences, "
        "no markdown tables) for the ConversationManager to relay simply to the user."
    )


def learning_expenses_improved_act_query() -> str:
    """Query text for act after the user sends the correction."""
    checking = _csv_relative_path("checking", LEARNING_EXPENSES_MONTH_N)
    card = _csv_relative_path("card", LEARNING_EXPENSES_MONTH_N)
    algo = "\n".join(
        f"- {line}" for line in learning_expenses_corrected_algorithm_lines()
    )
    return (
        f"Learning onboarding — improved deliverable (month N = {LEARNING_EXPENSES_MONTH_N}).\n\n"
        f'User correction: "{LEARNING_EXPENSES_USER_CORRECTION_TEXT}"\n\n'
        f"Re-read `{checking}` and `{card}` under `{LEARNING_EXPENSES_BASE_DIR}/`.\n\n"
        f"Apply this corrected spend algorithm:\n{algo}\n\n"
        "1. Recompute corrected monthly spend via execute_code.\n\n"
        f"{learning_expenses_storage_check_nudge()}\n\n"
        "Return the corrected total (numeric) and a brief internal summary (2–3 "
        "sentences, no markdown tables) for the ConversationManager to relay "
        "simply to the user. Do not call "
        "GuidanceManager or FunctionManager store tools in the doing loop."
    )


def learning_expenses_replay_act_query() -> str:
    """Query text for the month-N+1 replay act pass."""
    checking = _csv_relative_path("checking", LEARNING_EXPENSES_MONTH_N_PLUS_1)
    card = _csv_relative_path("card", LEARNING_EXPENSES_MONTH_N_PLUS_1)
    return (
        f"Learning onboarding — replay (month N+1 = {LEARNING_EXPENSES_MONTH_N_PLUS_1}).\n\n"
        f"Build the monthly spending report from `{checking}` and `{card}` under "
        f"`{LEARNING_EXPENSES_BASE_DIR}/`.\n\n"
        "Use stored Guidance and Functions for this expense ETL workflow — search/list "
        "them and apply the stored pipeline rather than reinventing rules from scratch.\n\n"
        "Return the corrected total spend as a number."
    )
