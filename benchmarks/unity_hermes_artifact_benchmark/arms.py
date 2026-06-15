"""Benchmark arm specifications for Unity and Hermes."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from .models import (
    ArtifactKind,
    ArtifactObservation,
    InspectionChecklist,
    PromptSet,
)


@dataclass(frozen=True)
class BenchmarkArm:
    arm_id: str
    display_name: str
    hypothesis: str
    prompts: PromptSet
    checklist: InspectionChecklist
    reference_artifact: ArtifactObservation

    def to_dict(self) -> dict[str, Any]:
        return {
            "arm_id": self.arm_id,
            "display_name": self.display_name,
            "hypothesis": self.hypothesis,
            "prompts": self.prompts.to_dict(),
            "checklist": self.checklist.to_dict(),
            "reference_artifact": self.reference_artifact.to_dict(),
        }


UNITY_FIRST_RUN_PROMPT = """\
You are running the controlled Unity artifact benchmark.

Task:
- Read the provided synthetic inbox batch.
- Only consider messages received since the batch's `since` timestamp.
- Classify every eligible email into one of:
  urgent_action, needs_reply, meeting_or_calendar, fyi, newsletter, ignore.
- Write a draft reply only when `needs_reply` is true.
- Do not touch Gmail or any external system. Write dry-run JSON only.

Implementation guidance:
- Start by searching FunctionManager and GuidanceManager.
- Use deterministic Python for date filtering, schema reshaping, and output files.
- Use focused semantic reasoning only for classification/draft judgment.
- If a reusable function already exists, call it via `execute_function`.
- Otherwise solve the batch once, then preserve enough trajectory detail for
  storage review to extract a reusable function.

Output contract:
- `drafts.json`: list of `{message_id, category, needs_reply, draft_reply, rationale}`.
- `trace.json`: tool calls and token usage where available.
- `artifact.json`: any reusable artifact proposed or stored.
"""


UNITY_CONSOLIDATION_PROMPT = """\
Review the completed trajectory and store the reusable workflow as a first-class
FunctionManager function if the run succeeded.

Target artifact:
- Name: `classify_and_draft_email_batch`.
- Input: `{since: str, emails: list[dict], dry_run: bool = true}`.
- Output: `{drafts: list[dict], skipped: list[dict], metrics: dict}`.
- Deterministic steps stay deterministic.
- Semantic classification and draft wording use a cheap model inside the function
  via `query_llm(...)` or `unillm`, not in the outer orchestration loop.
- The function must be callable with `execute_function(function_name=..., call_kwargs=...)`.

Also store GuidanceManager notes only if they explain non-obvious composition
strategy. Do not store a prose-only procedure as the main artifact.
"""


UNITY_REPEAT_RUN_PROMPT = """\
You are running the repeat phase of the Unity artifact benchmark.

Task:
- Process the new synthetic inbox batch since its `since` timestamp.
- Search FunctionManager and GuidanceManager first.
- If `classify_and_draft_email_batch` or an equivalent stored function exists,
  invoke it directly with `execute_function`.
- Do not regenerate the whole workflow unless no relevant function exists.
- Preserve trace evidence showing whether repeat execution was:
  `search function -> execute_function -> artifact-internal cheap LLM calls`.
"""


UNITY_CHECKLIST = InspectionChecklist(
    items=(
        "Stored artifact is a FunctionManager function, not only guidance text.",
        "Function has a stable name, typed-ish input contract, and JSON output contract.",
        "Repeat run can invoke it with `execute_function` by exact name.",
        "Date filtering and JSON shaping are deterministic code.",
        "Meaning-based classification/drafting happens inside the function with a cheap model.",
        "Dry-run mode prevents Gmail or external side effects.",
        "Scheduler can attach the function as a symbolic entrypoint candidate.",
    ),
)


UNITY_REFERENCE_ARTIFACT = ArtifactObservation(
    arm_id="unity",
    name="classify_and_draft_email_batch",
    kind=ArtifactKind.UNITY_FUNCTION,
    entrypoint="execute_function(function_name='classify_and_draft_email_batch', call_kwargs={...})",
    invocation_path=(
        "FunctionManager_search_functions",
        "execute_function",
        "classify_and_draft_email_batch",
    ),
    has_stable_input_schema=True,
    has_stable_output_schema=True,
    has_dry_run_mode=True,
    semantic_calls_inside_artifact=True,
    cheap_semantic_model="gpt-4.1-nano@openai",
    scheduler_binding="TaskScheduler entrypoint function_id candidate",
    requires_procedural_prompt_reread=False,
    exposes_supporting_script_directly=False,
    notes=(
        "Reference success case: a first-class FunctionManager artifact that "
        "future runs discover and invoke directly."
    ),
)


HERMES_FIRST_RUN_PROMPT = """\
You are running the controlled Hermes artifact benchmark.

Task:
- Read the provided synthetic inbox batch.
- Only consider messages received since the batch's `since` timestamp.
- Classify every eligible email into one of:
  urgent_action, needs_reply, meeting_or_calendar, fyi, newsletter, ignore.
- Write a draft reply only when `needs_reply` is true.
- Do not touch Gmail or any external system. Write dry-run JSON only.

Best-case artifact allowance:
- You may create a concise Hermes skill.
- You may add `scripts/classify_and_draft.py` under that skill.
- The first screenful of SKILL.md should explicitly say:
  "For recurring runs, execute `scripts/classify_and_draft.py` directly."
- The script should own deterministic filtering/schema work and cheap semantic
  model calls; the skill should be a thin launcher, not a long procedure.
"""


HERMES_CONSOLIDATION_PROMPT = """\
Create the most token-efficient Hermes reusable artifact you can for this task.

Target artifact:
- SKILL.md: concise launcher instructions, with the direct script instruction in
  the first screenful.
- `scripts/classify_and_draft.py`: dry-run processor with stable input/output
  JSON and cheap model calls inside the script.
- Avoid requiring the agent to reread a long procedure before every run.

Keep the artifact realistic: Hermes cron jobs with `skills: [...]` load full
SKILL.md content each tick, so the benchmark will penalize procedural prompt
rereads even when the inner script is good.
"""


HERMES_REPEAT_RUN_PROMPT = """\
You are running the repeat phase of the Hermes artifact benchmark.

Task:
- Process the new synthetic inbox batch since its `since` timestamp.
- Use the created skill/script in the most token-efficient realistic way.
- If the skill is attached or preloaded, trace the fact that full SKILL.md text
  enters the prompt before the agent decides to run the script.
- If you bypass the skill and call a fixed script path, trace that separately as
  a no-agent or prompt-only condition.
"""


HERMES_CHECKLIST = InspectionChecklist(
    items=(
        "SKILL.md is short and tells the agent to execute the script immediately.",
        "Supporting script has a stable CLI or JSON input/output contract.",
        "Script has dry-run mode and no Gmail side effects.",
        "Cheap semantic calls are inside the script rather than the outer agent.",
        "Repeat-run trace records whether SKILL.md was loaded in full.",
        "Cron/preload path is distinguished from a no-agent script path.",
        "Artifact quality does not assume cross-run skill prompt caching.",
    ),
)


HERMES_REFERENCE_ARTIFACT = ArtifactObservation(
    arm_id="hermes",
    name="email-triage-drafts skill + scripts/classify_and_draft.py",
    kind=ArtifactKind.HERMES_SKILL_WITH_SCRIPT,
    entrypoint="terminal python ${HERMES_SKILL_DIR}/scripts/classify_and_draft.py --input corpus.json --out drafts.json --dry-run",
    invocation_path=(
        "load_or_preload_skill_text",
        "infer_supporting_script_path",
        "terminal_run_script",
        "interpret_script_output",
    ),
    has_stable_input_schema=True,
    has_stable_output_schema=True,
    has_dry_run_mode=True,
    semantic_calls_inside_artifact=True,
    cheap_semantic_model="cheap provider model selected by script",
    scheduler_binding="Hermes cron `skills` field plus agent-run script, or separate no_agent script",
    requires_procedural_prompt_reread=True,
    exposes_supporting_script_directly=True,
    notes=(
        "Best-case Hermes artifact: the inner script is reusable, but the "
        "normal skill/preload/cron path still injects SKILL.md before use."
    ),
)


UNITY_ARM = BenchmarkArm(
    arm_id="unity",
    display_name="Unity FunctionManager",
    hypothesis=(
        "Unity should converge to a standalone FunctionManager function that "
        "future runs discover and invoke by exact name."
    ),
    prompts=PromptSet(
        first_run=UNITY_FIRST_RUN_PROMPT,
        consolidation=UNITY_CONSOLIDATION_PROMPT,
        repeat_run=UNITY_REPEAT_RUN_PROMPT,
    ),
    checklist=UNITY_CHECKLIST,
    reference_artifact=UNITY_REFERENCE_ARTIFACT,
)


HERMES_ARM = BenchmarkArm(
    arm_id="hermes",
    display_name="Hermes Skill With Supporting Script",
    hypothesis=(
        "Hermes can create a strong script, but the first-class reusable unit is "
        "still nested under skill prompt material unless using a separate no-agent script."
    ),
    prompts=PromptSet(
        first_run=HERMES_FIRST_RUN_PROMPT,
        consolidation=HERMES_CONSOLIDATION_PROMPT,
        repeat_run=HERMES_REPEAT_RUN_PROMPT,
    ),
    checklist=HERMES_CHECKLIST,
    reference_artifact=HERMES_REFERENCE_ARTIFACT,
)


ALL_ARMS = (UNITY_ARM, HERMES_ARM)
