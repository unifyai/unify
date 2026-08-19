"""Sharing-link resolution through the proxy.

A provider sharing URL is resolved by encoding it into a share token and
walking ``/shares/{token}/driveItem`` — never by fetching the share page. The
first live attempt on staging failed at every step of that route: the
driveItem's ``content`` and colon-path forms classified as unknown (403), the
``children`` allow-check built the invalid URL ``/driveItem/root`` (Graph 400),
the share-token 308 was passed to the sandbox with a ``graph.microsoft.com``
Location it cannot use, and nothing redeemed an anyone-with-the-link share, so
a cross-tenant link stayed unresolvable even with a valid workspace token.
Each test here pins the fix for one of those failures.
"""

from __future__ import annotations

import httpx
import pytest

from unify.provider_proxy import proxy as pxy
from unify.provider_proxy.ancestry import _ms_item_url, _ms_path_url
from unify.provider_proxy.classify import (
    KIND_FILE_READ,
    KIND_FILE_WRITE,
    KIND_UNKNOWN,
    classify,
)
from unify.provider_proxy.policy import get_policy_store
from unify.provider_proxy.session import ProxySession, set_session

NONCE = "test-nonce"
SHARE = "u!c2hhcmVkLXVybA"


# ── Classification: the shared driveItem is an item, not a drive root ────────


def test_bare_share_is_metadata_read():
    c = classify("microsoft", "GET", f"v1.0/shares/{SHARE}", {})
    assert c.kind == KIND_FILE_READ
    assert c.operation == "get_share"
    assert c.target is None


def test_share_drive_item_is_an_allow_checked_item_read():
    c = classify("microsoft", "GET", f"v1.0/shares/{SHARE}/driveItem", {})
    assert c.kind == KIND_FILE_READ
    assert (c.target.drive_id, c.target.item_id) == (f"share:{SHARE}", "root")


def test_share_children_lists_under_the_drive_item():
    c = classify("microsoft", "GET", f"v1.0/shares/{SHARE}/driveItem/children", {})
    assert c.kind == KIND_FILE_READ
    assert c.is_listing is True
    assert (c.parent.drive_id, c.parent.item_id) == (f"share:{SHARE}", "root")


def test_share_content_is_a_content_read():
    # The canonical download route for a shared file. This was KIND_UNKNOWN
    # (403) because share tails were classified as drive-tail operations.
    c = classify("microsoft", "GET", f"v1.0/shares/{SHARE}/driveItem/content", {})
    assert c.kind == KIND_FILE_READ
    assert c.is_content is True
    assert (c.target.drive_id, c.target.item_id) == (f"share:{SHARE}", "root")


def test_share_colon_path_children_anchor_on_the_drive_item():
    c = classify(
        "microsoft",
        "GET",
        f"v1.0/shares/{SHARE}/driveItem:/MH data extract/Repairs:/children",
        {},
    )
    assert c.kind == KIND_FILE_READ
    assert c.is_listing is True
    assert c.parent.drive_id == f"share:{SHARE}"
    assert c.parent.path == "MH data extract/Repairs"
    assert c.parent.anchor_item_id == "root"


def test_share_colon_path_content_downloads():
    c = classify(
        "microsoft",
        "GET",
        f"v1.0/shares/{SHARE}/driveItem:/Repairs/FactAppointments.csv:/content",
        {},
    )
    assert c.kind == KIND_FILE_READ
    assert c.is_content is True
    assert c.target.path == "Repairs/FactAppointments.csv"


def test_share_write_is_classified_as_write():
    c = classify("microsoft", "POST", f"v1.0/shares/{SHARE}/driveItem/children", {})
    assert c.kind == KIND_FILE_WRITE


def test_unrecognized_share_tail_stays_denied():
    c = classify("microsoft", "GET", f"v1.0/shares/{SHARE}/permission/grant", {})
    assert c.kind == KIND_UNKNOWN


# ── Ancestry: share URLs never grow a /root segment ─────────────────────────


def test_share_item_url_is_the_drive_item_itself():
    # ``/shares/{id}/driveItem/root`` addresses a segment beneath an item,
    # which Graph rejects — this is what turned every share children listing
    # into an upstream 400.
    url = _ms_item_url(f"share:{SHARE}", "root")
    assert url.endswith(f"/shares/{SHARE}/driveItem")
    assert "/root" not in url


def test_share_path_url_anchors_with_a_colon():
    url = _ms_path_url(f"share:{SHARE}", "root", "Repairs")
    assert url.endswith(f"/shares/{SHARE}/driveItem:/Repairs")


def test_ordinary_drive_urls_are_unchanged():
    assert _ms_item_url("D1", "I1").endswith("/drives/D1/items/I1")
    assert _ms_path_url("D1", "root", "a/b").endswith("/drives/D1/root:/a/b")


# ── Dispatch: redemption, redirects, Location rewrite ────────────────────────


class _Forwarder:
    """Records forwarded calls; responds from a configurable table."""

    def __init__(self) -> None:
        self.calls: list[dict] = []
        self.response = httpx.Response(200, json={"ok": True})

    async def __call__(
        self,
        provider,
        method,
        rest_path,
        query_string,
        incoming_headers,
        body,
        *,
        follow_redirects=False,
        resource_tenant=None,
    ):
        self.calls.append(
            {
                "rest_path": rest_path,
                "headers": dict(incoming_headers),
                "follow_redirects": follow_redirects,
            },
        )
        return self.response


async def _allow_everything(provider, drive_id, item_id):
    return True


@pytest.fixture()
def client(monkeypatch):
    set_session(ProxySession(host="127.0.0.1", port=7777, nonce=NONCE))
    get_policy_store().clear()
    fwd = _Forwarder()
    monkeypatch.setattr(pxy, "_forward", fwd)
    monkeypatch.setattr(pxy, "is_allowed", _allow_everything)

    transport = httpx.ASGITransport(app=pxy.build_app())
    c = httpx.AsyncClient(
        transport=transport,
        base_url="http://proxy",
        headers={"Authorization": f"Bearer {NONCE}"},
    )
    c._forwarder = fwd  # type: ignore[attr-defined]
    return c


@pytest.mark.asyncio
async def test_share_requests_redeem_the_link(client):
    # An anyone-with-the-link share is unusable through /shares until the
    # recipient redeems it — the API equivalent of opening it in a browser.
    await client.get(f"/microsoft/v1.0/shares/{SHARE}/driveItem")
    headers = client._forwarder.calls[-1]["headers"]
    assert headers.get("prefer") == "redeemSharingLinkIfNecessary"


@pytest.mark.asyncio
async def test_caller_supplied_prefer_wins(client):
    await client.get(
        f"/microsoft/v1.0/shares/{SHARE}/driveItem",
        headers={"Prefer": "redeemSharingLink"},
    )
    headers = client._forwarder.calls[-1]["headers"]
    assert headers.get("prefer") == "redeemSharingLink"


@pytest.mark.asyncio
async def test_non_share_requests_are_not_redeemed(client):
    await client.get("/microsoft/v1.0/me/drive/root/children")
    for call in client._forwarder.calls:
        assert "prefer" not in call["headers"]


@pytest.mark.asyncio
async def test_share_reads_follow_redirects_upstream(client):
    # The share-token 308 must terminate inside the proxy, where the real
    # token lives — the sandbox cannot follow it.
    await client.get(f"/microsoft/v1.0/shares/{SHARE}/driveItem")
    assert client._forwarder.calls[-1]["follow_redirects"] is True


@pytest.mark.asyncio
async def test_share_listing_follows_redirects_upstream(client):
    await client.get(f"/microsoft/v1.0/shares/{SHARE}/driveItem/children")
    assert client._forwarder.calls[-1]["follow_redirects"] is True


@pytest.mark.asyncio
async def test_bare_share_metadata_follows_redirects_upstream(client):
    await client.get(f"/microsoft/v1.0/shares/{SHARE}")
    assert client._forwarder.calls[-1]["follow_redirects"] is True


@pytest.mark.asyncio
async def test_ordinary_metadata_reads_do_not_follow_redirects(client):
    await client.get("/microsoft/v1.0/drives/D1/items/I1")
    assert client._forwarder.calls[-1]["follow_redirects"] is False


@pytest.mark.asyncio
async def test_upstream_location_is_rewritten_to_the_proxy(client):
    # A residual redirect (e.g. from an endpoint the proxy does not follow
    # for) must point back at the proxy, where the nonce works.
    client._forwarder.response = httpx.Response(
        308,
        headers={
            "Location": "https://graph.microsoft.com/v1.0/shares/other/driveItem",
        },
    )
    resp = await client.get(f"/microsoft/v1.0/shares/{SHARE}")
    assert resp.status_code == 308
    assert (
        resp.headers["location"]
        == "http://127.0.0.1:7777/microsoft/v1.0/shares/other/driveItem"
    )


@pytest.mark.asyncio
async def test_non_upstream_location_is_left_alone(client):
    client._forwarder.response = httpx.Response(
        302,
        headers={"Location": "https://example.sharepoint.com/download?x=1"},
    )
    resp = await client.get(f"/microsoft/v1.0/shares/{SHARE}")
    assert resp.headers["location"] == "https://example.sharepoint.com/download?x=1"


# ── _forward: transport failures surface as a clean 502 ─────────────────────


class _RaisingClient:
    def __init__(self, exc: Exception) -> None:
        self._exc = exc

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        return False

    async def request(self, *args, **kwargs):
        raise self._exc


@pytest.mark.asyncio
async def test_redirect_without_location_becomes_a_502(monkeypatch):
    # httpx raises when told to follow a redirect that names no target — the
    # exact shape the staging share resolution died on. The sandbox must see
    # an upstream error it can report, not a proxy 500.
    monkeypatch.setattr(
        pxy.runtime_oauth,
        "get_provider_access_token_optimistic",
        lambda provider: "tok",
    )
    monkeypatch.setattr(
        pxy,
        "_make_client",
        lambda follow: _RaisingClient(
            httpx.RemoteProtocolError("redirect response missing Location"),
        ),
    )
    resp = await pxy._forward("microsoft", "GET", "v1.0/shares/x", "", {}, None)
    assert resp.status_code == 502
    body = resp.json()
    assert body["error"]["code"] == "upstreamUnreachable"
    assert "Location" in body["error"]["message"]
