"""
Minimal conftest for parallel_run.sh fixture tests.

These tests exist only to verify parallel_run.sh behavior (session creation,
status prefixes, exit codes, etc.), not to test unify functionality.

Setting SKIP_UNIFY_TEST_INIT tells the shared harness to leave the store
closed and skip project activation, builtin seeding and per-test context
binding, so fixture tests complete in milliseconds instead of seconds and
parallel_run.sh itself can be exercised at high concurrency.
"""

import os


def pytest_configure(config):
    """Signal to the global conftest that this session wants no runtime setup."""
    os.environ["SKIP_UNIFY_TEST_INIT"] = "1"
    os.environ["UNIFY_TESTS_DELETE_PROJ_ON_START"] = "False"
    os.environ["UNIFY_TESTS_DELETE_PROJ_ON_EXIT"] = "False"
