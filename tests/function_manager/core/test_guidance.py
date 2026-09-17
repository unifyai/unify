from __future__ import annotations

from tests.helpers import _handle_project
from unify.function_manager.function_manager import FunctionManager
from unify.guidance_manager.guidance_manager import GuidanceManager


@_handle_project
def test_fetch_guidance_for_function_and_limits():
    # Seed functions
    fm = FunctionManager()
    src_a = "def alpha(x: int) -> int:\n    return x + 1\n"
    src_b = "def beta(y: int) -> int:\n    return y * 2\n"
    fm.add_functions(implementations=[src_a, src_b])
    listing = fm.list_functions()
    alpha_id = listing["alpha"]["function_id"]
    beta_id = listing["beta"]["function_id"]

    # Seed guidance that references alpha and beta
    gm = GuidanceManager()
    gm.add_guidance(
        title="Alpha Notes",
        content="How to use alpha",
        function_ids=[alpha_id],
    )
    gm.add_guidance(
        title="Beta Notes",
        content="How to use beta",
        function_ids=[beta_id],
    )

    # Fetch guidance for alpha
    alpha_guidance = fm._get_guidance_for_function(function_id=alpha_id)
    titles = {g["title"] for g in alpha_guidance}
    assert titles == {"Alpha Notes"}
    assert {"guidance_id", "title", "content"} <= set(alpha_guidance[0])

    # Limit behavior
    both = fm._get_guidance_for_function(function_id=beta_id, limit=1)
    assert len(both) == 1
