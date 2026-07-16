"""Unit tests for authored task resource requirement helpers."""

from __future__ import annotations

from unify.task_scheduler.resource_requirements import (
    desktop_required_for_resources,
    resolve_requires_computer,
    resolve_requires_filesystem,
    resolve_task_resource_requirements,
)


def test_resolve_requires_filesystem_defaults_false():
    assert resolve_requires_filesystem(None) is False
    assert resolve_requires_filesystem({}) is False
    assert resolve_requires_filesystem({"requires_filesystem": False}) is False


def test_resolve_requires_filesystem_coerces_truthy_strings():
    assert resolve_requires_filesystem({"requires_filesystem": True}) is True
    assert resolve_requires_filesystem({"requires_filesystem": "1"}) is True
    assert resolve_requires_filesystem({"requires_filesystem": "true"}) is True
    assert resolve_requires_filesystem({"requires_filesystem": "0"}) is False
    assert resolve_requires_filesystem({"requires_filesystem": "false"}) is False


def test_resolve_requires_computer_defaults_false():
    assert resolve_requires_computer(None) is False
    assert resolve_requires_computer({}) is False
    assert resolve_requires_computer({"requires_computer": False}) is False


def test_resolve_requires_computer_explicit_flag():
    assert resolve_requires_computer({"requires_computer": True}) is True
    assert resolve_requires_computer({"requires_computer": "1"}) is True
    assert resolve_requires_computer({"requires_computer": "true"}) is True


def test_resolve_requires_computer_legacy_browser_target():
    assert resolve_requires_computer({"browser_target": "assistant_desktop"}) is True
    assert (
        resolve_requires_computer(
            {"browser_target": " assistant_desktop ", "requires_computer": False},
        )
        is True
    )
    assert resolve_requires_computer({"browser_target": "other"}) is False


def test_resolve_task_resource_requirements_pair():
    assert resolve_task_resource_requirements(
        {
            "requires_filesystem": True,
            "browser_target": "assistant_desktop",
        },
    ) == (True, True)
    assert resolve_task_resource_requirements(
        {"requires_computer": True},
    ) == (False, True)
    assert resolve_task_resource_requirements({}) == (False, False)


def test_desktop_required_for_resources():
    assert (
        desktop_required_for_resources(
            requires_filesystem=False,
            requires_computer=False,
        )
        is False
    )
    assert (
        desktop_required_for_resources(
            requires_filesystem=True,
            requires_computer=False,
        )
        is True
    )
    assert (
        desktop_required_for_resources(
            requires_filesystem=False,
            requires_computer=True,
        )
        is True
    )
