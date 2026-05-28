#!/usr/bin/env python3
"""
Discover test paths using leaf-based algorithm for CI parallelism.

Usage:
    # Discover all leaf test directories (default)
    python discover_test_paths.py

    # Expand specific paths to their leaf directories
    python discover_test_paths.py tests/function_manager tests/actor

When explicit paths are provided:
- Files are kept as-is (no expansion)
- Directories are expanded to their leaf sub-directories using Option A algorithm

Option A algorithm:
- Leaf directories (have test files, no test subdirs) → one job per directory
- Mixed directories (test files AND test subdirs) → one bundled job for all
  direct test files (space-separated), plus recursive jobs for subdirs
"""

import os
import sys
from pathlib import Path

EXCLUDE_DIRS = {
    "__pycache__",
    ".pytest_cache",
    ".mypy_cache",
    "fixtures",
    ".git",
    ".venv",
    "venv",
}


# Directories whose tests take longer than a single CI job's 130-min
# timeout (i.e. parallel_run.sh can't drain them within the wall-clock
# budget). Each entry maps the directory path → a list of "bundled"
# test-file groups, where each group becomes its own matrix entry. Files
# within a group are run together as a single space-separated
# parallel_run.sh argument (same format as a Mixed-dir bundle).
#
# Why explicit chunking instead of a generic "split if > N files"
# heuristic: the runtime of a test cluster correlates with LLM-eval
# count, not file count. test_execute.py has 23 functions but most are
# parametrized into many tens of cases, each making real LLM calls — a
# generic "split into N file chunks" would not isolate it. The manual
# breakdown below keeps the heavy file alone and groups smaller files
# together.
#
# Add an entry here when a cluster starts hitting the job timeout and
# the natural fix is "split it across more matrix slots". Removing or
# editing entries reshapes the matrix; bumps to the GitHub Actions
# concurrency directive may also be needed if the matrix grows
# substantially.
SPLIT_DIRS: dict[str, list[list[str]]] = {
    "tests/task_scheduler": [
        # Group A — the heaviest single file, lives alone so the other
        # groups can finish well within timeout while it grinds.
        ["test_execute.py"],
        # Group B — the next-heaviest files, both LLM-eval-heavy.
        ["test_active_queue.py", "test_active_task.py"],
        # Group C — the remaining smaller files (~50 LLM calls
        # combined, comfortable for one job).
        [
            "test_all_ctx.py",
            "test_ask.py",
            "test_cancel.py",
            "test_contexts.py",
            "test_creation_deletion.py",
            "test_embedding.py",
            "test_event_logging.py",
            "test_failure_recovery.py",
            "test_foreign_keys.py",
            "test_info.py",
            "test_integration_contacts.py",
        ],
    ],
}


def has_test_files(directory):
    """Check if directory has test_*.py files directly in it."""
    return any(
        f.name.startswith("test_") and f.name.endswith(".py")
        for f in directory.iterdir()
        if f.is_file()
    )


def has_test_subdirs(directory):
    """Check if directory has subdirectories that contain test files (recursively)."""
    for subdir in directory.iterdir():
        if subdir.is_dir() and subdir.name not in EXCLUDE_DIRS:
            for root, dirs, files in os.walk(subdir):
                dirs[:] = [d for d in dirs if d not in EXCLUDE_DIRS]
                if any(f.startswith("test_") and f.endswith(".py") for f in files):
                    return True
    return False


def get_direct_test_files(directory):
    """Get test_*.py files directly in this directory (not recursive)."""
    return sorted(
        [
            f
            for f in directory.iterdir()
            if f.is_file() and f.name.startswith("test_") and f.name.endswith(".py")
        ],
    )


def collect_paths(directory, paths):
    """Recursively collect test paths using Option A algorithm."""
    if not directory.is_dir():
        return

    # Apply explicit split-config first: if this directory is registered
    # in SPLIT_DIRS, emit one matrix entry per pre-defined file group
    # rather than the single leaf-bundle the default algorithm would
    # produce.
    dir_key = str(directory)
    if dir_key in SPLIT_DIRS:
        for group in SPLIT_DIRS[dir_key]:
            files = [directory / fname for fname in group]
            missing = [f for f in files if not f.exists()]
            if missing:
                raise RuntimeError(
                    f"SPLIT_DIRS entry for {dir_key} references files "
                    f"that do not exist: {[str(m) for m in missing]}. "
                    f"Update SPLIT_DIRS in discover_test_paths.py or "
                    f"restore the files.",
                )
            paths.append(" ".join(str(f) for f in files))
        return

    has_files = has_test_files(directory)
    has_subdirs = has_test_subdirs(directory)

    if has_files and not has_subdirs:
        # Pure leaf: add the directory
        paths.append(str(directory))
    elif has_files and has_subdirs:
        # Mixed: bundle all direct files into one job, then recurse into subdirs
        direct_files = get_direct_test_files(directory)
        paths.append(" ".join(str(f) for f in direct_files))
        for subdir in sorted(directory.iterdir()):
            if subdir.is_dir() and subdir.name not in EXCLUDE_DIRS:
                collect_paths(subdir, paths)
    elif has_subdirs:
        # No direct test files, but has subdirs with tests: just recurse
        for subdir in sorted(directory.iterdir()):
            if subdir.is_dir() and subdir.name not in EXCLUDE_DIRS:
                collect_paths(subdir, paths)


def expand_path(path_str):
    """
    Expand a single path to its leaf test directories/files.

    - If path is a file: return it as-is
    - If path is a directory: apply Option A algorithm to find leaves
    """
    path = Path(path_str)
    paths = []

    if not path.exists():
        # Path doesn't exist - return as-is and let pytest handle the error
        return [path_str]

    if path.is_file():
        # Files are kept as-is
        return [path_str]

    # Directory: apply leaf discovery
    if path.is_dir():
        # SPLIT_DIRS override (see top-of-module rationale): emit one
        # matrix entry per pre-defined file group so a too-large cluster
        # fits in the per-job timeout budget.
        dir_key = str(path)
        if dir_key in SPLIT_DIRS:
            for group in SPLIT_DIRS[dir_key]:
                files = [path / fname for fname in group]
                missing = [f for f in files if not f.exists()]
                if missing:
                    raise RuntimeError(
                        f"SPLIT_DIRS entry for {dir_key} references files "
                        f"that do not exist: {[str(m) for m in missing]}. "
                        f"Update SPLIT_DIRS in discover_test_paths.py or "
                        f"restore the files.",
                    )
                paths.append(" ".join(str(f) for f in files))
            return paths

        # Check if this directory itself is a leaf or needs expansion
        has_files = has_test_files(path)
        has_subdirs = has_test_subdirs(path)

        if has_files and not has_subdirs:
            # Pure leaf directory - return as-is
            paths.append(str(path))
        elif has_files and has_subdirs:
            # Mixed: bundle all direct files into one job, then recurse into subdirs
            direct_files = get_direct_test_files(path)
            paths.append(" ".join(str(f) for f in direct_files))
            for subdir in sorted(path.iterdir()):
                if subdir.is_dir() and subdir.name not in EXCLUDE_DIRS:
                    collect_paths(subdir, paths)
        elif has_subdirs:
            # No direct test files, but has subdirs with tests: recurse
            for subdir in sorted(path.iterdir()):
                if subdir.is_dir() and subdir.name not in EXCLUDE_DIRS:
                    collect_paths(subdir, paths)
        else:
            # No test files at all - return as-is and let pytest handle it
            paths.append(str(path))

    return paths


def discover_all():
    """Discover all test paths from the tests/ root directory."""
    test_root = Path("tests")
    paths = []

    # Handle test files directly in tests/ root (e.g., test_settings.py)
    for item in sorted(test_root.iterdir()):
        if (
            item.is_file()
            and item.name.startswith("test_")
            and item.name.endswith(".py")
        ):
            paths.append(str(item))
        elif item.is_dir() and item.name not in EXCLUDE_DIRS:
            # Recurse into every non-excluded directory; collect_paths is itself
            # gated by has_test_files / has_test_subdirs, so non-test dirs are
            # no-ops. The previous `startswith("test")` filter accidentally
            # excluded every per-manager test directory (contact_manager/,
            # knowledge_manager/, actor/, etc.) since they don't carry the
            # `test_` prefix, collapsing the CI matrix to ~2 entries.
            collect_paths(item, paths)

    return paths


def main():
    if len(sys.argv) > 1:
        # Explicit paths provided - expand each one
        all_paths = []
        for arg in sys.argv[1:]:
            expanded = expand_path(arg)
            all_paths.extend(expanded)
        paths = all_paths
    else:
        # No arguments - discover all from tests/
        paths = discover_all()

    # Output unique paths, sorted
    for p in sorted(set(paths)):
        print(p)


if __name__ == "__main__":
    main()
