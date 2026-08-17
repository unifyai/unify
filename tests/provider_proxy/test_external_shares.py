"""Items shared from another tenant stay visible and stay decidable.

Two defects met on the same folder, and each one hid the other.

Graph omits externally-shared items from ``/me/drive/sharedWithMe`` unless the
caller passes ``allowexternal=true``. Nothing in this repo ever passed it, so a
folder a colleague at another company had shared -- correctly, with a direct
user grant -- came back as ``{"value":[]}``. An empty list is exactly what a
revoked grant looks like, so the omission read as the share being gone, and the
investigation went looking for a cause on the sender's side that did not exist.

Underneath that sat a second problem with the same shape. Microsoft does not
allow an external item to be addressed through the sending drive's id, so the
ancestry walk fails on its very first hop and returns an empty chain. An empty
chain matches no decision, so evaluation falls through to the provider default
-- and under the default-deny policy any assistant gets the moment someone
allowlists a folder, every externally shared item silently disappears, with an
explicit Console allow on that item unable to change the outcome. The one
assistant this was reproduced on happened to be default-allow, which is the
only reason the second defect stayed hidden behind the first.

Both were confirmed against live staging: with the parameter, Graph returned
the folder with ``shared.scope == "users"`` and ``folder.childCount == 8``.
"""

from __future__ import annotations

import pytest

from unify.provider_proxy import ancestry as anc
from unify.provider_proxy import proxy as pxy
from unify.provider_proxy.ancestry import WorkspaceFileNotFound
from unify.provider_proxy.policy import evaluate_access

# The live folder: Matt's tenant, shared to a guest in ours.
SENDER_DRIVE = "b!ohrf1yiVwUK-8etC9h4qwQOR-GAqYCtAqo_BlFsLYh9Q"
SHARED_ITEM = "01Y3CCPSMXRT3GKIQPOVCLHZAMEMK6CEGZ"


def _pxy_all_drives(query_string: str, *, listing: bool) -> str:
    return pxy._with_all_drives(query_string, listing=listing)


class TestTheExternalOptIn:
    def test_a_bare_listing_gains_the_opt_in(self):
        assert pxy._with_allow_external("") == "allowexternal=true"

    def test_existing_parameters_are_kept(self):
        out = pxy._with_allow_external("$top=200")
        assert "$top=200" in out
        assert "allowexternal=true" in out

    def test_a_caller_who_asked_for_it_is_not_duplicated(self):
        assert pxy._with_allow_external("allowexternal=true") == "allowexternal=true"

    def test_a_caller_narrowing_to_their_own_tenant_still_wins(self):
        # Opting out is a real request, not an omission to be corrected.
        assert pxy._with_allow_external("allowexternal=false") == "allowexternal=false"


class TestTheDriveEquivalent:
    """Drive makes the same omission Graph does, in its own vocabulary.

    ``supportsAllDrives`` and ``includeItemsFromAllDrives`` are set by every
    first-party Drive caller in this repo -- the gateway views, the ancestry
    probe, the scheduler's trigger resources -- and were absent from exactly one
    path: the proxy the sandbox uses. So the single caller composing requests ad
    hoc was the only one receiving a silently shortened list.
    """

    def test_a_listing_gains_both_opt_ins(self):
        out = _pxy_all_drives("", listing=True)
        assert "supportsAllDrives=true" in out
        assert "includeItemsFromAllDrives=true" in out

    def test_a_single_item_read_gains_only_the_support_flag(self):
        # includeItemsFromAllDrives is an enumeration concept; Drive rejects it
        # on a files.get.
        out = _pxy_all_drives("", listing=False)
        assert out == "supportsAllDrives=true"

    def test_caller_values_are_preserved(self):
        out = _pxy_all_drives("supportsAllDrives=false", listing=True)
        assert "supportsAllDrives=false" in out
        assert "supportsAllDrives=true" not in out

    def test_an_existing_query_is_kept(self):
        out = _pxy_all_drives("q=name+contains+'csv'", listing=True)
        assert "q=name+contains+'csv'" in out
        assert "supportsAllDrives=true" in out


class TestTheAccessCheckAsksTheSameQuestionAsTheRequest:
    """The probe and the forwarded call must agree on link redemption.

    Serving a ``/shares`` item runs two calls: the access-check probe, then the
    forwarded request. ``_dispatch`` set ``Prefer: redeemSharingLinkIfNecessary``
    on the forwarded one only, so an unredeemed link was refused by the probe and
    that refusal was returned as the answer -- the forwarded request that would
    have redeemed the link never ran. Confirmed on staging against a freshly
    minted link, which failed identically to a stale one.
    """

    def test_a_share_probe_asks_to_redeem_the_link(self, monkeypatch):
        monkeypatch.setattr(anc, "get_provider_access_token", lambda p: "tok")
        headers = anc._ms_probe_headers("share:u!abc")
        assert headers["Prefer"] == "redeemSharingLinkIfNecessary"

    def test_an_ordinary_drive_probe_does_not(self, monkeypatch):
        # Redemption is meaningless off a share and should not be advertised.
        monkeypatch.setattr(anc, "get_provider_access_token", lambda p: "tok")
        assert "Prefer" not in anc._ms_probe_headers(SENDER_DRIVE)

    def test_the_probe_still_carries_authorization(self, monkeypatch):
        monkeypatch.setattr(anc, "get_provider_access_token", lambda p: "tok")
        assert anc._ms_probe_headers("share:u!abc")["Authorization"] == "Bearer tok"


class TestAnUnresolvableAncestryStaysDecidable:
    """The walk cannot reach the sender's drive, so it must not come back empty."""

    @pytest.fixture(autouse=True)
    def _clear(self):
        anc.clear_ancestry_cache()
        yield
        anc.clear_ancestry_cache()

    @pytest.mark.asyncio
    async def test_the_item_stands_in_for_its_own_ancestry(self, monkeypatch):
        async def _refuse(provider, drive_id, item_id):
            raise WorkspaceFileNotFound(item_id)

        monkeypatch.setattr(anc, "get_node", _refuse)

        chain = await anc.ancestry_chain("microsoft", SENDER_DRIVE, SHARED_ITEM)

        assert chain == [(SENDER_DRIVE, SHARED_ITEM)]

    @pytest.mark.asyncio
    async def test_an_explicit_allow_on_a_shared_item_can_take_effect(
        self,
        monkeypatch,
    ):
        # The point of standing in: with an empty chain this decision could
        # never match, and a deny-by-default policy would win regardless.
        async def _refuse(provider, drive_id, item_id):
            raise WorkspaceFileNotFound(item_id)

        monkeypatch.setattr(anc, "get_node", _refuse)
        chain = await anc.ancestry_chain("microsoft", SENDER_DRIVE, SHARED_ITEM)

        decisions = ({"drive_id": SENDER_DRIVE, "item_id": SHARED_ITEM, "allow": True},)
        assert evaluate_access(decisions, False, chain) is True

    @pytest.mark.asyncio
    async def test_a_refusal_is_not_cached_as_a_decision(self, monkeypatch):
        calls: list[str] = []

        async def _refuse(provider, drive_id, item_id):
            calls.append(item_id)
            raise WorkspaceFileNotFound(item_id)

        monkeypatch.setattr(anc, "get_node", _refuse)
        await anc.ancestry_chain("microsoft", SENDER_DRIVE, SHARED_ITEM)
        await anc.ancestry_chain("microsoft", SENDER_DRIVE, SHARED_ITEM)

        # Caching the empty walk would make one refusal outlive itself, so a
        # share that becomes reachable stays invisible for the cache's life.
        assert len(calls) == 2

    @pytest.mark.asyncio
    async def test_a_resolved_chain_is_still_cached(self, monkeypatch):
        calls: list[str] = []

        async def _resolve(provider, drive_id, item_id):
            calls.append(item_id)
            return {"drive_id": drive_id, "item_id": item_id, "parent_id": None}

        monkeypatch.setattr(anc, "get_node", _resolve)
        first = await anc.ancestry_chain("microsoft", "my-drive", "item-1")
        second = await anc.ancestry_chain("microsoft", "my-drive", "item-1")

        assert first == second == [("my-drive", "item-1")]
        assert len(calls) == 1

    @pytest.mark.asyncio
    async def test_a_partial_walk_keeps_what_it_resolved(self, monkeypatch):
        # The item resolves, its parent does not: the answer is the real chain
        # as far as it got, not a synthesized one.
        async def _resolve_once(provider, drive_id, item_id):
            if item_id == "child":
                return {
                    "drive_id": drive_id,
                    "item_id": "child",
                    "parent_id": "unreachable",
                }
            raise WorkspaceFileNotFound(item_id)

        monkeypatch.setattr(anc, "get_node", _resolve_once)
        chain = await anc.ancestry_chain("microsoft", "my-drive", "child")

        assert chain == [("my-drive", "child")]
