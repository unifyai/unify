"""A reference is classified before a route is chosen, not after one fails.

Every mechanism for reaching shared content fails in a way that resembles the
others -- an empty listing, a 404, a page of HTML where a file was expected --
so picking between them by trying each in turn yields a run of
indistinguishable failures and no diagnosis. That is exactly how a correctly
shared folder read as revoked for a week: five routes refused, each for its own
reason, and none of the refusals said which.

Classification here is offline and cheap. It decides from the shape of the
reference and from ownership already learned during discovery, so a route
documented as unable to reach a kind is never attempted at all.
"""

from __future__ import annotations

import pytest

from unify.provider_proxy.provenance import (
    KIND_LINK_ONLY,
    KIND_OWNED,
    KIND_PUBLIC_URL,
    KIND_SHARED_CROSS_ORG,
    KIND_SHARED_SAME_ORG,
    LANE_API,
    LANE_BROWSER,
    LANE_FETCH,
    classify_item,
    classify_url,
)

HOME = "4fb2ad78-a61b-4c66-8d6a-0d69dc938dcf"
THEIRS = "cfe84544-5e06-4f30-a4a0-5cf558a66462"
DRIVE = "b!ohrf1yiVwUK-8etC9h4qwQOR-GAqYCtAqo_BlFsLYh9Q"


class TestClassifyingAUrl:
    def test_an_ordinary_address_is_a_public_url(self):
        p = classify_url("https://example.com/data/repairs.csv")
        assert p.kind == KIND_PUBLIC_URL
        assert p.reachable_by == (LANE_FETCH,)

    def test_a_sharing_link_is_not_treated_as_an_ordinary_url(self):
        # It is an https address, but whether it can be fetched depends on how
        # it was scoped, which the link itself does not say.
        p = classify_url(
            "https://hiqcouk-my.sharepoint.com/:f:/g/personal/matt_h-iq_co_uk/IgCX?e=rVXb7u",
        )
        assert p.kind == KIND_LINK_ONLY
        assert p.provider == "microsoft"

    def test_a_sharing_link_tries_fetch_before_a_browser(self):
        # An anonymous link is just a public URL and that is the common case;
        # the browser is for links scoped to named people, where an
        # unauthenticated fetch returns a sign-in page that looks like success.
        p = classify_url(
            "https://hiqcouk-my.sharepoint.com/:x:/g/personal/someone/AbCd",
        )
        assert p.reachable_by == (LANE_FETCH, LANE_BROWSER)

    def test_a_canonical_provider_address_goes_through_the_api(self):
        p = classify_url(
            "https://hiqcouk-my.sharepoint.com/personal/matt_h-iq_co_uk/Documents/Unify%20access",
        )
        assert p.kind == KIND_OWNED
        assert p.reachable_by == (LANE_API,)

    def test_a_google_drive_address_is_recognised_as_google(self):
        p = classify_url("https://drive.google.com/drive/folders/1AbC")
        assert p.provider == "google"

    def test_classification_never_touches_the_network(self):
        # An address that cannot resolve still classifies, because nothing here
        # is permitted to make a request.
        assert classify_url("https://nonexistent.invalid/x").kind == KIND_PUBLIC_URL


class TestClassifyingADiscoveredItem:
    def test_another_organisations_content_is_marked_cross_org(self):
        p = classify_item("microsoft", DRIVE, home_org=HOME, owner_org=THEIRS)
        assert p.kind == KIND_SHARED_CROSS_ORG
        assert p.owner_org == THEIRS

    def test_cross_org_content_needs_someone_outside_to_act(self):
        p = classify_item("microsoft", DRIVE, home_org=HOME, owner_org=THEIRS)
        assert p.needs_authorisation is True

    def test_content_shared_inside_the_organisation_does_not(self):
        p = classify_item("microsoft", DRIVE, home_org=HOME, owner_org=HOME)
        assert p.kind == KIND_SHARED_SAME_ORG
        assert p.needs_authorisation is False

    def test_unknown_ownership_reads_as_our_own(self):
        # Silence is not suspicion: nothing has said otherwise, so the ordinary
        # credential applies and no approval is implied.
        p = classify_item("microsoft", DRIVE, home_org=HOME, owner_org="")
        assert p.kind == KIND_OWNED
        assert p.needs_authorisation is False

    def test_cross_org_is_still_an_api_route(self):
        # Reachable, but only with a credential the owning organisation issues
        # -- which is why it is an approval problem and not a browser problem.
        p = classify_item("microsoft", DRIVE, home_org=HOME, owner_org=THEIRS)
        assert p.reachable_by == (LANE_API,)

    @pytest.mark.parametrize("provider", ["microsoft", "google"])
    def test_the_contract_is_provider_agnostic(self, provider):
        # Google has no tenant-scoped credential, so it must be able to answer
        # these questions its own way rather than inherit tenant semantics.
        p = classify_item(provider, DRIVE, home_org="org-a", owner_org="org-b")
        assert p.kind == KIND_SHARED_CROSS_ORG
        assert p.provider == provider
