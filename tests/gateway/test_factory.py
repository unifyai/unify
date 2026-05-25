"""Tests for ``create_ingress_transport_factory`` (Phase A.bis.6).

The factory's most important property is that the default
configuration ("" / "legacy" / unknown) returns ``None`` so that
``CommsManager`` falls through to its existing inline path. That is
the safety net that keeps hosted production unchanged when this code
ships, before anyone explicitly sets the env var. The other paths
exercise the inmemory and pubsub selectors and pin the resolver
contract.
"""

from __future__ import annotations

import pytest

from unity.gateway.factory import (
    KNOWN_TRANSPORT_KINDS,
    TRANSPORT_KIND_INMEMORY,
    TRANSPORT_KIND_LEGACY,
    TRANSPORT_KIND_PUBSUB,
    create_ingress_transport_factory,
)
from unity.gateway.ingress_inmemory import InMemoryIngressTransport
from unity.gateway.ingress_pubsub import PubSubIngressTransport

# ---------------------------------------------------------------------------
# Default / fallback behaviour -- the production safety net
# ---------------------------------------------------------------------------


def test_empty_kind_returns_none() -> None:
    """Default unset env var must produce no factory -> legacy path."""
    assert create_ingress_transport_factory(kind="") is None


def test_legacy_kind_returns_none() -> None:
    assert create_ingress_transport_factory(kind=TRANSPORT_KIND_LEGACY) is None


def test_unknown_kind_returns_none() -> None:
    """Misspelled env values must not crash; they fall back to legacy.

    A warning is emitted but Unity's custom logger setup intercepts log
    records, so we only assert the externally-observable behaviour
    (return value). The warning text is verified by reading the log
    output during deploy soak.
    """
    assert create_ingress_transport_factory(kind="poobsub") is None
    assert create_ingress_transport_factory(kind="PUBSUB") is None  # case-sensitive
    assert create_ingress_transport_factory(kind="not-a-real-kind") is None


def test_known_kinds_catalogue_is_exhaustive() -> None:
    assert KNOWN_TRANSPORT_KINDS == frozenset(
        {TRANSPORT_KIND_LEGACY, TRANSPORT_KIND_INMEMORY, TRANSPORT_KIND_PUBSUB},
    )


# ---------------------------------------------------------------------------
# inmemory path
# ---------------------------------------------------------------------------


def test_inmemory_kind_returns_factory_building_inmemory_transport() -> None:
    factory = create_ingress_transport_factory(kind=TRANSPORT_KIND_INMEMORY)
    assert factory is not None
    transport = factory()
    assert isinstance(transport, InMemoryIngressTransport)


def test_inmemory_factory_returns_a_fresh_transport_each_call() -> None:
    factory = create_ingress_transport_factory(kind=TRANSPORT_KIND_INMEMORY)
    assert factory is not None
    assert factory() is not factory()


# ---------------------------------------------------------------------------
# pubsub path
# ---------------------------------------------------------------------------


def test_pubsub_kind_requires_subscription_id_resolver() -> None:
    with pytest.raises(ValueError, match="subscription_id_resolver"):
        create_ingress_transport_factory(
            kind=TRANSPORT_KIND_PUBSUB,
            project_id="responsive-city-458413-a2",
        )


def test_pubsub_kind_requires_project_id() -> None:
    with pytest.raises(ValueError, match="project_id"):
        create_ingress_transport_factory(
            kind=TRANSPORT_KIND_PUBSUB,
            subscription_id_resolver=lambda: "unity-42-sub",
            project_id="",
        )


def test_pubsub_factory_invokes_resolver_lazily() -> None:
    """The resolver is called on factory() invocation, not at setup."""
    calls: list[int] = []

    def resolver() -> str:
        calls.append(1)
        return "unity-42-sub"

    factory = create_ingress_transport_factory(
        kind=TRANSPORT_KIND_PUBSUB,
        subscription_id_resolver=resolver,
        project_id="responsive-city-458413-a2",
    )
    assert factory is not None
    assert calls == []  # not yet resolved
    transport = factory()
    assert calls == [1]
    assert isinstance(transport, PubSubIngressTransport)
    assert transport.subscription_id == "unity-42-sub"
    assert transport.project_id == "responsive-city-458413-a2"


def test_pubsub_factory_returns_none_when_resolver_returns_empty() -> None:
    """A blank subscription_id at resolve time degrades to legacy gracefully.

    A warning is emitted (visible in stdout) but Unity's custom logger
    intercepts log records, so we only assert externally-observable
    behaviour (the factory's return value of ``None`` causes
    CommsManager to fall through to the legacy inline path).
    """
    factory = create_ingress_transport_factory(
        kind=TRANSPORT_KIND_PUBSUB,
        subscription_id_resolver=lambda: "",
        project_id="p",
    )
    assert factory is not None
    assert factory() is None


def test_pubsub_factory_passes_max_messages_through_to_transport() -> None:
    factory = create_ingress_transport_factory(
        kind=TRANSPORT_KIND_PUBSUB,
        subscription_id_resolver=lambda: "unity-42-sub",
        project_id="p",
        max_messages=25,
    )
    assert factory is not None
    transport = factory()
    assert isinstance(transport, PubSubIngressTransport)
    assert transport._max_messages == 25


def test_pubsub_factory_max_messages_defaults_to_ten() -> None:
    """Default max_messages mirrors the legacy subscribe_to_topic call."""
    factory = create_ingress_transport_factory(
        kind=TRANSPORT_KIND_PUBSUB,
        subscription_id_resolver=lambda: "unity-42-sub",
        project_id="p",
    )
    assert factory is not None
    transport = factory()
    assert isinstance(transport, PubSubIngressTransport)
    assert transport._max_messages == 10
