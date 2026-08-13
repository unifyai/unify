"""What the sandbox is told when an access check cannot reach the provider.

Before an item is served, the proxy resolves its ancestry with the provider to
evaluate the file-access allowlist. That probe is a separate HTTP call from the
forwarded request, and it used to end in ``raise_for_status()`` with nothing
upstream to catch it -- so any unusual provider response became FastAPI's bare
500 with an empty body. That is the least useful answer available: it names
neither the cause nor the request, and it points at the proxy rather than at
the provider.

Both shapes here were observed live on staging resolving a cross-tenant
SharePoint sharing link:

* Graph answered ``/shares/{token}/driveItem`` with **308 and no Location**,
  which the client cannot follow, and the assistant saw ``500`` with an empty
  body across every variation it tried;
* Graph answered ``/sites/...`` with a 400 whose body said "This API is not
  supported for MSA accounts" -- the single most diagnostic sentence in the
  whole run, and one the sandbox would never have seen.
"""

from __future__ import annotations

import httpx
import pytest

from unify.provider_proxy import proxy as pxy
from unify.provider_proxy.ancestry import (
    WorkspaceProbeFailed,
    _raise_for_probe,
)
from unify.provider_proxy.policy import get_policy_store
from unify.provider_proxy.session import ProxySession, set_session

NONCE = "test-nonce"
GRAPH = "https://graph.microsoft.com/v1.0/shares/u!tok/driveItem"


def _response(status: int, **kwargs) -> httpx.Response:
    return httpx.Response(status, request=httpx.Request("GET", GRAPH), **kwargs)


class TestProbeTranslation:
    def test_a_redirect_with_no_target_is_a_dead_end_not_a_server_error(self):
        # The live failure. follow_redirects is on for these probes, so a 3xx
        # reaching the caller means the response named nowhere to go.
        with pytest.raises(WorkspaceProbeFailed) as caught:
            _raise_for_probe(_response(308))
        assert caught.value.status_code == 502
        assert "without saying where to" in caught.value.detail

    def test_the_dead_end_names_the_likely_cause(self):
        # A share that will not resolve is usually an identity mismatch, and
        # the assistant cannot see the identity from where it stands.
        with pytest.raises(WorkspaceProbeFailed) as caught:
            _raise_for_probe(_response(302))
        assert "personal Microsoft account" in caught.value.detail
        assert "connected identity" in caught.value.detail

    def test_a_provider_error_message_survives(self):
        # The sentence that explained the entire staging failure.
        body = {"error": {"message": "This API is not supported for MSA accounts"}}
        with pytest.raises(WorkspaceProbeFailed) as caught:
            _raise_for_probe(_response(400, json=body))
        assert caught.value.status_code == 400
        assert caught.value.detail == "This API is not supported for MSA accounts"

    def test_a_non_json_error_body_still_says_something(self):
        with pytest.raises(WorkspaceProbeFailed) as caught:
            _raise_for_probe(_response(503, text="upstream is down"))
        assert caught.value.status_code == 503
        assert "upstream is down" in caught.value.detail

    def test_an_empty_error_body_falls_back_to_the_status(self):
        with pytest.raises(WorkspaceProbeFailed) as caught:
            _raise_for_probe(_response(418, json={}))
        assert "418" in caught.value.detail

    def test_the_failing_request_is_recorded(self):
        with pytest.raises(WorkspaceProbeFailed) as caught:
            _raise_for_probe(_response(500, json={}))
        assert caught.value.url == GRAPH

    def test_success_passes_through(self):
        assert _raise_for_probe(_response(200, json={"id": "x"})) is None

    def test_a_redirect_the_client_did_follow_never_reaches_here(self):
        # httpx resolves a followable redirect itself, so what arrives is the
        # final response -- 200 here, and not an error.
        assert _raise_for_probe(_response(200, json={"id": "x"})) is None


class TestTheSandboxSeesTheReason:
    """The translation is worthless if it does not survive to the caller."""

    @pytest.fixture()
    def client(self, monkeypatch):
        set_session(ProxySession(host="127.0.0.1", port=7777, nonce=NONCE))
        get_policy_store().clear()

        async def _probe_fails(provider, drive_id, item_id):
            raise WorkspaceProbeFailed(
                502,
                "The provider redirected this request (308) without saying "
                "where to.",
                GRAPH,
            )

        monkeypatch.setattr(pxy, "is_allowed", _probe_fails)
        transport = httpx.ASGITransport(app=pxy.build_app())
        return httpx.AsyncClient(
            transport=transport,
            base_url="http://proxy",
            headers={"Authorization": f"Bearer {NONCE}"},
        )

    @pytest.mark.asyncio
    async def test_the_status_and_reason_reach_the_sandbox(self, client):
        resp = await client.get("/microsoft/v1.0/shares/u!tok/driveItem")
        assert resp.status_code == 502
        body = resp.json()
        assert body["error"]["code"] == "workspaceProbeFailed"
        assert "without saying where to" in body["error"]["message"]

    @pytest.mark.asyncio
    async def test_the_body_is_never_empty(self, client):
        # The whole defect in one assertion: the old answer was a 500 whose
        # body carried nothing, so the assistant swept every request shape it
        # could think of without ever learning why.
        resp = await client.get("/microsoft/v1.0/shares/u!tok/driveItem")
        assert resp.content
        assert resp.json()["error"]["message"].strip()
