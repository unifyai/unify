"""A request against another organisation's drive uses that organisation's token.

A delegated Graph token is issued for exactly one tenant. Pointed at a drive in
a different one it does not fail loudly -- Graph answers ``404 itemNotFound``,
which reads as "no such item" rather than "wrong credential". That is why a
folder shared correctly, by a direct user grant, with the right account and
ample scopes, was invisible for a week: every token we minted was stamped with
our own tenant.

The fix has two halves. Provenance is *learned* from the discovery call the
assistant already makes -- ``sharedWithMe`` names each item's owning tenant --
so routing costs no extra round trip. And when the owner is known but no token
can be had for them, the answer says so, because falling back to the home token
reproduces the original 404 and hides the real cause all over again.
"""

from __future__ import annotations

import json

import pytest

from unify.provider_proxy import proxy as pxy
from unify.provider_proxy import tenants
from unify.provider_proxy.classify import KIND_FILE_READ, Classification, Locator

HOME = "4fb2ad78-a61b-4c66-8d6a-0d69dc938dcf"
OTHER = "cfe84544-5e06-4f30-a4a0-5cf558a66462"
THEIR_DRIVE = "b!ohrf1yiVwUK-8etC9h4qwQOR-GAqYCtAqo_BlFsLYh9Q"
OUR_DRIVE = "b!niWeyYAMvUmOynYPxc_eOuyucLjbTEZJtllYpfRwCdp"


@pytest.fixture(autouse=True)
def _clean():
    tenants.clear()
    yield
    tenants.clear()


def _shared_with_me(drive_id: str, tenant_id: str) -> dict:
    """The shape Graph returns for an externally shared folder."""
    return {
        "value": [
            {
                "id": "01ABC",
                "name": "Unify access",
                "remoteItem": {
                    "id": "01ABC",
                    "name": "Unify access",
                    "parentReference": {"driveId": drive_id},
                    "sharepointIds": {"tenantId": tenant_id},
                },
            },
        ],
    }


class TestLearningWhoOwnsADrive:
    def test_a_shared_listing_teaches_the_owner(self):
        tenants.learn_from_listing(_shared_with_me(THEIR_DRIVE, OTHER))
        assert tenants.tenant_for_drive(THEIR_DRIVE) == OTHER

    def test_an_unknown_drive_reports_nothing(self):
        # Silence means "the ordinary credential applies", not "denied".
        assert tenants.tenant_for_drive("b!never-seen") is None

    def test_a_listing_without_provenance_is_harmless(self):
        tenants.learn_from_listing({"value": [{"id": "x", "name": "y"}]})
        assert tenants.tenant_for_drive("b!never-seen") is None

    def test_a_non_listing_payload_is_ignored(self):
        tenants.learn_from_listing(None)
        tenants.learn_from_listing({"error": {"code": "itemNotFound"}})

    def test_the_home_tenant_comes_from_the_token_itself(self):
        payload = json.dumps({"tid": HOME}).encode()
        import base64

        body = base64.urlsafe_b64encode(payload).decode().rstrip("=")
        assert tenants.home_tenant(f"hdr.{body}.sig") == HOME

    def test_an_opaque_token_yields_no_tenant(self):
        # Not every credential is a JWT; that must degrade to "unknown" rather
        # than raising inside a request path.
        assert tenants.home_tenant("not-a-jwt") == ""


class TestRoutingByOwner:
    def _classification(self, drive_id: str) -> Classification:
        return Classification(
            "microsoft",
            KIND_FILE_READ,
            "children",
            parent=Locator(drive_id, "01ABC"),
            is_listing=True,
        )

    def test_a_foreign_drive_selects_its_owning_tenant(self):
        tenants.learn_from_listing(_shared_with_me(THEIR_DRIVE, OTHER))
        assert pxy._resource_tenant_for(self._classification(THEIR_DRIVE)) == OTHER

    def test_our_own_drive_needs_no_special_routing(self):
        assert pxy._resource_tenant_for(self._classification(OUR_DRIVE)) is None

    def test_google_is_never_routed_by_tenant(self):
        # Drive has no tenant-scoped token; its external-sharing model differs
        # and must not inherit Microsoft's semantics.
        c = Classification(
            "google",
            KIND_FILE_READ,
            "children",
            parent=Locator(THEIR_DRIVE, "01ABC"),
            is_listing=True,
        )
        tenants.learn_from_listing(_shared_with_me(THEIR_DRIVE, OTHER))
        assert pxy._resource_tenant_for(c) is None


class TestAMissingApprovalIsNotAMissingFile:
    def test_the_refusal_names_the_organisation_and_the_remedy(self):
        resp = pxy._authorisation_required(OTHER)

        assert resp.status_code == 403
        body = resp.json()["error"]
        assert body["code"] == "authorisationRequired"
        assert body["tenantId"] == OTHER
        assert "administrator" in body["message"]

    def test_it_is_distinguishable_from_not_found(self):
        # The whole point: 404 was indistinguishable from the item not
        # existing, and that ambiguity cost a week.
        assert pxy._authorisation_required(OTHER).status_code != 404
        assert pxy._not_found().status_code == 404
