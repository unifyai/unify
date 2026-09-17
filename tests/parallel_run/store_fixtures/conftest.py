"""
Minimal conftest for the store-plumbing fixtures.

SKIP_UNIFY_TEST_INIT keeps the shared harness from activating a project, so
the fixture alone decides what happens to the store parallel_run.sh handed
it and the run stays fast enough to spawn many sessions at once.
"""

import os


def pytest_configure(config):
    os.environ["SKIP_UNIFY_TEST_INIT"] = "1"
