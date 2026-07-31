"""Bundled Learning onboarding bill-split fixtures and workspace provisioning.

The billsplit-dinner tutorial narrates exact numbers over two fixed dinner
receipt CSVs shipped as package data. The ``learning_beat_requested``
handler copies them into the twin local workspace so ``act(persist=True)``
reads stable files — nothing is generated at runtime.
"""

from __future__ import annotations

import csv
import shutil
from dataclasses import dataclass
from importlib import resources
from pathlib import Path

LEARNING_BILLSPLIT_BASE_DIR = "onboarding/learning/billsplit"
LEARNING_BILLSPLIT_FRIDAY_FILENAME = "dinner-receipt-friday.csv"
LEARNING_BILLSPLIT_SATURDAY_FILENAME = "dinner-receipt-saturday.csv"

LEARNING_BILLSPLIT_NON_DRINKER = "Sam"
LEARNING_BILLSPLIT_FRIDAY_ATTENDEES: tuple[str, ...] = (
    "You",
    "Sam",
    "Priya",
    "Jordan",
)
LEARNING_BILLSPLIT_SATURDAY_ATTENDEES: tuple[str, ...] = (
    "You",
    "Sam",
    "Priya",
    "Marcus",
    "Dana",
)

# Sam's Friday overcharge under the naive even split, and the overcharge
# avoided on Saturday's zero-shot replay — the bundled fixtures guarantee
# these exact dollar amounts (the old bank-export tutorial's analogous
# constant was an "at least $N" floor; billsplit's numbers are fixed, not
# a threshold).
LEARNING_BILLSPLIT_FRIDAY_OVERCHARGE = 15.00
LEARNING_BILLSPLIT_SATURDAY_AVOIDED_OVERCHARGE = 10.40

# Narration copy mirrors :func:`compute_naive_even_split` /
# :func:`compute_corrected_split` — keep in sync if those rules change.
LEARNING_BILLSPLIT_NAIVE_MISTAKE_DESCRIPTION = (
    "split the total evenly across every attendee, including the alcohol, so "
    f"{LEARNING_BILLSPLIT_NON_DRINKER} pays for drinks they never touched"
)
LEARNING_BILLSPLIT_CORRECTION_RULE_DESCRIPTION = (
    "split food/dessert/drinks evenly across every attendee, but split "
    "alcohol only across the attendees who drink"
)

LEARNING_BILLSPLIT_USER_CORRECTION_TEXT = (
    "Sam doesn't drink — don't split alcohol across people who didn't drink."
)

LEARNING_BILLSPLIT_GUIDANCE_TITLE = "Bill splitting rules"
LEARNING_BILLSPLIT_FUNCTION_NAME = "split_dinner_bill"
LEARNING_BILLSPLIT_KNOWLEDGE_CLAIM = "Sam doesn't drink alcohol"


def learning_billsplit_storage_check_nudge() -> str:
    """Explicit StorageCheck mandate for the learning correction interjection."""
    return (
        "StorageCheck memoization (for the post-act review loop — do NOT call "
        "GuidanceManager or FunctionManager store tools in the doing loop): "
        f"persist Guidance titled {LEARNING_BILLSPLIT_GUIDANCE_TITLE!r} with the "
        f"rule ({LEARNING_BILLSPLIT_CORRECTION_RULE_DESCRIPTION}), a Knowledge "
        f"claim titled {LEARNING_BILLSPLIT_KNOWLEDGE_CLAIM!r}, and Function "
        f"{LEARNING_BILLSPLIT_FUNCTION_NAME!r} for the corrected bill-splitting "
        "computation."
    )


def learning_billsplit_stop_act_for_storage_rule() -> str:
    """CM must end the persist act after the corrected deliverable to run StorageCheck."""
    return (
        "After sending the corrected deliverable, call stop_* on the running "
        "persist act in the SAME turn — StorageCheck only starts once the "
        "persist session ends, not while it sits in awaiting_input. Tell the "
        "user in plain language that you are stopping the action so Brain can "
        "save their rule (for example: stopping it now so your correction "
        "gets saved), then invoke stop_*. Do NOT invite Saturday's dinner yet "
        "— that invite belongs to the Saved announcement, sent only once the "
        "save actually completes."
    )


def learning_billsplit_user_facing_voice() -> str:
    """Plain-language rules for Learning demo chat messages (non-technical audience)."""
    return (
        "User-facing voice: the audience is non-technical. Keep every "
        "learning-demo chat message short and scannable — a headline dollar "
        "total plus one or two plain sentences. Do NOT send markdown tables, "
        "line-by-line row breakdowns, or jargon (pools, pro-rata, "
        "allocation). Receipts are attachments for anyone curious; never "
        "recite every line item in chat. "
        "Opening message: first teach Brain in 3-4 short concept bullets — "
        "corrections stick (learning), Guidance is my playbook for how to "
        "work, Functions are reusable skills, and together they mean less "
        "re-explaining on similar tasks. Keep concept lines domain-agnostic "
        "(no receipt/split jargon). Then announce the trick: I'm going to "
        "get the first split wrong on purpose and the user's job is to catch "
        "me — tell them to open the Actions tab so they can watch me work. "
        "Attachment captions: one sentence each, naming the cast (who's at "
        "dinner) — one artifact to look at, never a second file. "
        "Naive deliverable: state the naive per-person total, then one "
        "sentence on the mistake (charged the non-drinker for alcohol they "
        "never touched), then the exact correction text to paste — nothing "
        "else. "
        "Corrected deliverable: state the corrected totals (non-drinker vs "
        "everyone else), say I am stopping the action so Brain can save the "
        "rule, then stop_* the persist act in the same turn — no replay "
        "invite here, that comes only after the save actually completes. "
        "Saved announcement: cite what actually got stored (a rule, a fact, "
        "a skill), point at the Brain tab, and invite the second dinner — "
        "sent exactly once, only after the save completes; on failure say so "
        "plainly and offer to retry. "
        "Replay deliverable: the new totals in one line, plus one sentence "
        "noting nobody had to remind me about the non-drinker this time."
    )


_BUNDLED_ASSET_PACKAGE = "unify.assets.onboarding.learning.billsplit"
_BUNDLED_FILENAMES: tuple[str, ...] = (
    LEARNING_BILLSPLIT_FRIDAY_FILENAME,
    LEARNING_BILLSPLIT_SATURDAY_FILENAME,
)


@dataclass(frozen=True)
class ReceiptRow:
    item: str
    category: str
    price: float


def _receipt_relative_path(filename: str) -> str:
    return f"{LEARNING_BILLSPLIT_BASE_DIR}/{filename}"


def bundled_fixture_relative_paths() -> tuple[str, ...]:
    """Workspace-relative paths for both bundled receipt CSV fixtures."""
    return (
        _receipt_relative_path(LEARNING_BILLSPLIT_FRIDAY_FILENAME),
        _receipt_relative_path(LEARNING_BILLSPLIT_SATURDAY_FILENAME),
    )


def parse_receipt_csv(content: str) -> list[ReceiptRow]:
    """Parse one bundled dinner-receipt CSV into typed rows."""
    reader = csv.DictReader(content.strip().splitlines())
    rows: list[ReceiptRow] = []
    for raw in reader:
        item = (raw.get("item") or "").strip()
        category = (raw.get("category") or "").strip()
        price_raw = (raw.get("price") or "").strip()
        if not item or not category or not price_raw:
            raise ValueError(f"Invalid CSV row: {raw}")
        rows.append(ReceiptRow(item=item, category=category, price=float(price_raw)))
    return rows


def _is_alcohol(row: ReceiptRow) -> bool:
    return row.category.strip().lower() == "alcohol"


def compute_naive_even_split(rows: list[ReceiptRow], attendee_count: int) -> float:
    """Naive per-person total: the full bill (including alcohol) split evenly."""
    total = sum(row.price for row in rows)
    return round(total / attendee_count, 2)


def compute_corrected_split(
    rows: list[ReceiptRow],
    *,
    attendee_count: int,
    non_drinker_count: int = 1,
) -> tuple[float, float]:
    """Corrected per-person totals: ``(non_drinker_share, drinker_share)``.

    Food/dessert/drinks split evenly across everyone; alcohol splits only
    across the attendees who drink.
    """
    shared = sum(row.price for row in rows if not _is_alcohol(row))
    alcohol = sum(row.price for row in rows if _is_alcohol(row))
    drinker_count = attendee_count - non_drinker_count
    shared_share = shared / attendee_count
    alcohol_share = alcohol / drinker_count
    return round(shared_share, 2), round(shared_share + alcohol_share, 2)


def assert_bundled_fixture_invariants() -> None:
    """Verify both bundled receipts reproduce the Core Flows math exactly."""
    asset_root = resources.files(_BUNDLED_ASSET_PACKAGE)
    friday = parse_receipt_csv(
        (asset_root / LEARNING_BILLSPLIT_FRIDAY_FILENAME).read_text(
            encoding="utf-8",
        ),
    )
    saturday = parse_receipt_csv(
        (asset_root / LEARNING_BILLSPLIT_SATURDAY_FILENAME).read_text(
            encoding="utf-8",
        ),
    )

    friday_attendee_count = len(LEARNING_BILLSPLIT_FRIDAY_ATTENDEES)
    friday_total = round(sum(row.price for row in friday), 2)
    if friday_total != 246.00:
        raise AssertionError(f"Friday receipt total {friday_total:.2f} != 246.00")
    friday_naive = compute_naive_even_split(friday, friday_attendee_count)
    if friday_naive != 61.50:
        raise AssertionError(f"Friday naive split {friday_naive:.2f} != 61.50")
    friday_non_drinker, friday_drinker = compute_corrected_split(
        friday,
        attendee_count=friday_attendee_count,
    )
    if friday_non_drinker != 46.50 or friday_drinker != 66.50:
        raise AssertionError(
            f"Friday corrected split {friday_non_drinker:.2f}/"
            f"{friday_drinker:.2f} != 46.50/66.50",
        )
    friday_overcharge = round(friday_naive - friday_non_drinker, 2)
    if friday_overcharge != LEARNING_BILLSPLIT_FRIDAY_OVERCHARGE:
        raise AssertionError(
            f"Friday overcharge {friday_overcharge:.2f} != "
            f"{LEARNING_BILLSPLIT_FRIDAY_OVERCHARGE:.2f}",
        )

    saturday_attendee_count = len(LEARNING_BILLSPLIT_SATURDAY_ATTENDEES)
    saturday_total = round(sum(row.price for row in saturday), 2)
    if saturday_total != 257.00:
        raise AssertionError(f"Saturday receipt total {saturday_total:.2f} != 257.00")
    saturday_naive = compute_naive_even_split(saturday, saturday_attendee_count)
    if saturday_naive != 51.40:
        raise AssertionError(f"Saturday naive split {saturday_naive:.2f} != 51.40")
    saturday_non_drinker, saturday_drinker = compute_corrected_split(
        saturday,
        attendee_count=saturday_attendee_count,
    )
    if saturday_non_drinker != 41.00 or saturday_drinker != 54.00:
        raise AssertionError(
            f"Saturday corrected split {saturday_non_drinker:.2f}/"
            f"{saturday_drinker:.2f} != 41.00/54.00",
        )
    saturday_avoided = round(saturday_naive - saturday_non_drinker, 2)
    if saturday_avoided != LEARNING_BILLSPLIT_SATURDAY_AVOIDED_OVERCHARGE:
        raise AssertionError(
            f"Saturday avoided overcharge {saturday_avoided:.2f} != "
            f"{LEARNING_BILLSPLIT_SATURDAY_AVOIDED_OVERCHARGE:.2f}",
        )


def provision_learning_billsplit_fixtures(local_root: str | Path) -> list[str]:
    """Copy bundled CSV fixtures into the twin workspace when absent.

    Returns workspace-relative paths written on this call. Existing files are
    left untouched so a re-click or handler retry stays idempotent.
    """
    root = Path(local_root).expanduser().resolve()
    asset_root = resources.files(_BUNDLED_ASSET_PACKAGE)
    written: list[str] = []
    for filename in _BUNDLED_FILENAMES:
        relative_path = _receipt_relative_path(filename)
        destination = root / relative_path
        if destination.exists():
            continue
        destination.parent.mkdir(parents=True, exist_ok=True)
        with resources.as_file(asset_root / filename) as bundled_path:
            shutil.copy2(bundled_path, destination)
        written.append(relative_path)
    return written


def learning_billsplit_naive_algorithm_lines() -> tuple[str, ...]:
    """Step-by-step naive split rules the first act(persist=True) pass must follow."""
    return (
        "Sum the price of every line item on the receipt, including alcohol.",
        "Divide that total evenly across every attendee.",
    )


def learning_billsplit_corrected_algorithm_lines() -> tuple[str, ...]:
    """Step-by-step corrected split rules for the corrected deliverable and replay."""
    return (
        "Sum the price of every non-alcohol line item (category != "
        "'alcohol'); divide that shared total evenly across every attendee.",
        "Sum the price of every alcohol line item (category == 'alcohol'); "
        "divide that alcohol total evenly across only the attendees who "
        "drink.",
        "Each drinker owes their shared share plus their alcohol share; each "
        "non-drinker owes their shared share only.",
    )


def learning_billsplit_scenario_prompt_lines() -> tuple[str, ...]:
    """Fixed scenario facts for Learning tutorial narration.

    Built from module constants so prompt copy stays aligned if fixtures or
    paths change.
    """
    friday_path = _receipt_relative_path(LEARNING_BILLSPLIT_FRIDAY_FILENAME)
    saturday_path = _receipt_relative_path(LEARNING_BILLSPLIT_SATURDAY_FILENAME)
    friday_attendees = ", ".join(LEARNING_BILLSPLIT_FRIDAY_ATTENDEES)
    saturday_attendees = ", ".join(LEARNING_BILLSPLIT_SATURDAY_ATTENDEES)
    return (
        f"Two dinner receipts bundled as package data — Friday's is "
        f"`{friday_path}` (attendees: {friday_attendees}) and Saturday's "
        f"(the replay) is `{saturday_path}` (attendees: {saturday_attendees}); "
        f"{LEARNING_BILLSPLIT_NON_DRINKER} is the non-drinker on both nights "
        "and recurs deliberately so the recurrence is the one moment "
        "Knowledge visibly pays off.",
        "Task: split each dinner bill across that night's attendees.",
        f"The deliberate naive mistake: {LEARNING_BILLSPLIT_NAIVE_MISTAKE_DESCRIPTION}.",
        "Naive algorithm for the first pass:",
        *learning_billsplit_naive_algorithm_lines(),
        f"The correction rule: {LEARNING_BILLSPLIT_CORRECTION_RULE_DESCRIPTION}.",
        "Corrected algorithm after the user sends this correction text:",
        f'"{LEARNING_BILLSPLIT_USER_CORRECTION_TEXT}"',
        *learning_billsplit_corrected_algorithm_lines(),
        learning_billsplit_storage_check_nudge(),
        learning_billsplit_stop_act_for_storage_rule(),
        "Numbers are always computed from the receipt file via "
        "act(persist=True), never asserted — bundled fixtures guarantee "
        "Friday's naive split is $61.50/person with "
        f"{LEARNING_BILLSPLIT_NON_DRINKER} overcharged "
        f"${LEARNING_BILLSPLIT_FRIDAY_OVERCHARGE:.2f} before the correction "
        f"(corrected: {LEARNING_BILLSPLIT_NON_DRINKER} $46.50, everyone else "
        "$66.50), and Saturday's zero-shot replay is "
        f"{LEARNING_BILLSPLIT_NON_DRINKER} $41.00 / everyone else $54.00 "
        f"(avoiding a ${LEARNING_BILLSPLIT_SATURDAY_AVOIDED_OVERCHARGE:.2f} "
        "overcharge with no reminder).",
    )


def learning_billsplit_naive_act_query() -> str:
    """Query text for act(persist=True) on Friday's naive first attempt."""
    friday_path = _receipt_relative_path(LEARNING_BILLSPLIT_FRIDAY_FILENAME)
    algo = "\n".join(f"- {line}" for line in learning_billsplit_naive_algorithm_lines())
    return (
        f"Learning onboarding — naive first pass on `{friday_path}` "
        f"(attendees: {', '.join(LEARNING_BILLSPLIT_FRIDAY_ATTENDEES)}).\n\n"
        f"Apply this naive split algorithm exactly:\n{algo}\n\n"
        "Parse the CSV with execute_code and compute totals — never hardcode "
        "numbers.\n\n"
        "Return the naive per-person total (numeric) and a brief internal "
        "summary (2-3 sentences, no markdown tables) for the "
        "ConversationManager to relay simply to the user."
    )


def learning_billsplit_corrected_act_query() -> str:
    """Query text for act after the user sends the correction."""
    friday_path = _receipt_relative_path(LEARNING_BILLSPLIT_FRIDAY_FILENAME)
    algo = "\n".join(
        f"- {line}" for line in learning_billsplit_corrected_algorithm_lines()
    )
    return (
        f"Learning onboarding — corrected deliverable on `{friday_path}`.\n\n"
        f'User correction: "{LEARNING_BILLSPLIT_USER_CORRECTION_TEXT}"\n\n'
        f"Apply this corrected split algorithm:\n{algo}\n\n"
        "Recompute the corrected per-person totals via execute_code.\n\n"
        f"{learning_billsplit_storage_check_nudge()}\n\n"
        "Return the corrected totals (numeric, non-drinker vs everyone else) "
        "and a brief internal summary (2-3 sentences, no markdown tables) "
        "for the ConversationManager to relay simply to the user. Do not "
        "call GuidanceManager or FunctionManager store tools in the doing "
        "loop."
    )


def learning_billsplit_replay_act_query() -> str:
    """Query text for the Saturday zero-shot replay act pass."""
    saturday_path = _receipt_relative_path(LEARNING_BILLSPLIT_SATURDAY_FILENAME)
    return (
        f"Learning onboarding — replay on `{saturday_path}` (attendees: "
        f"{', '.join(LEARNING_BILLSPLIT_SATURDAY_ATTENDEES)}).\n\n"
        "Use stored Guidance, Knowledge, and Functions for bill splitting — "
        "search/list them and apply the stored rule and fact rather than "
        "reinventing them from scratch. No reminder about the non-drinker "
        "is given this time.\n\n"
        "Return the corrected per-person totals (numeric, non-drinker vs "
        "everyone else) as numbers."
    )
