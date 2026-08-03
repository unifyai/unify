"""The dispatch client's contract with the pipeline control plane.

Three properties matter here, and each has a concrete failure behind it:

* **Publish happens last.** A parse message that names bytes which are not
  staged yet fails on a missing object and burns a delivery attempt for a run
  that was otherwise fine.
* **The bytes go around this client where they can.** A signed target is written
  directly to the store; only a store that cannot sign sends them through the
  control plane. Getting this backwards would put every ingestion under an HTTP
  request-size ceiling.
* **A refusal keeps its reason.** The plane refuses for things a caller can act
  on -- another recovery owns this job -- and collapsing that to a status code
  turns an actionable answer into a number.
"""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any, Dict, List
from unittest.mock import patch

import pytest

from unify.ingestion_manager import dispatch as client
from unify.ingestion_manager.types.request import (
    CollectionTarget,
    FilesSource,
    IngestionRequest,
    TableTarget,
)


class _Response:
    def __init__(self, payload: Any = None, status_code: int = 200):
        self._payload = payload if payload is not None else {}
        self.status_code = status_code
        self.content = b"{}"
        self.text = str(payload)

    def json(self) -> Any:
        return self._payload


@pytest.fixture(autouse=True)
def _identity():
    """A pod that knows which assistant it is, with a key to prove it."""
    session = SimpleNamespace(
        unify_key="pod-key",
        assistant=SimpleNamespace(agent_id=42),
    )
    with (
        patch.object(client, "_assistant_key", lambda: "pod-key"),
        patch.object(
            client,
            "_assistant_id",
            lambda: 42,
        ),
    ):
        yield session


def _request(*, collection: bool = False) -> IngestionRequest:
    return IngestionRequest(
        source=FilesSource(paths=["/tmp/a.pdf"]),
        target=(
            CollectionTarget(name="Reports")
            if collection
            else TableTarget(context="Data/Deals")
        ),
    )


def _prepared(signed: bool) -> Dict[str, Any]:
    url = "https://signed.test/put" if signed else ""
    return {
        "dispatch_id": "run1",
        "request_upload": {"upload_url": url, "object_uri": "jobs/run1/request.json"},
        "sources": [
            {
                "logical_path": "/tmp/a.pdf",
                "upload_url": url,
                "object_uri": "jobs/run1/sources/0000-a.pdf",
            },
        ],
    }


def _run_dispatch(tmp_path, *, signed: bool, collection: bool = False):
    """Drive one dispatch, recording every call in order."""
    source = tmp_path / "a.pdf"
    source.write_bytes(b"%PDF-1.4 test")
    calls: List[Dict[str, Any]] = []

    def fake_post(url, json=None, headers=None, timeout=None):
        calls.append({"verb": "POST", "url": url, "json": json})
        if url.endswith("/submit"):
            return _Response(_prepared(signed))
        return _Response({"dispatch_id": "run1", "jobs": 1, "job_ids": ["run1-0000"]})

    def fake_put(url, data=None, headers=None, timeout=None, params=None):
        calls.append({"verb": "PUT", "url": url, "bytes": len(data or b"")})
        return _Response({})

    request = _request(collection=collection)
    with (
        patch.object(client.requests, "post", fake_post),
        patch.object(
            client.requests,
            "put",
            fake_put,
        ),
    ):
        dispatch_id = client.dispatch_run(
            base_url="http://plane.test",
            run_key="run1",
            request=request,
            request_key="jobs/run1/request.json",
            request_payload=request.model_dump(mode="json"),
            paths=[str(source)],
            observability={
                "run_key": "run1",
                "runs_context": "u/1/Ingestion/Runs",
                "events_context": "u/1/Ingestion/Events",
            },
        )
    return dispatch_id, calls


class TestOrdering:
    def test_everything_is_staged_before_anything_is_published(self, tmp_path):
        dispatch_id, calls = _run_dispatch(tmp_path, signed=True)
        assert dispatch_id == "run1"
        order = [
            (
                "prepare"
                if call["url"].endswith("/submit")
                else "publish" if call["url"].endswith("/submit/publish") else "upload"
            )
            for call in calls
        ]
        assert order[0] == "prepare"
        assert order[-1] == "publish"
        # Both the staged request and the source land before the publish, so a
        # worker can never pick up a message naming bytes that are not there.
        assert order.count("upload") == 2
        assert "upload" not in order[order.index("publish") :]


class TestUploadRouting:
    def test_a_signed_target_bypasses_the_control_plane(self, tmp_path):
        _, calls = _run_dispatch(tmp_path, signed=True)
        uploads = [call for call in calls if call["verb"] == "PUT"]
        assert all(call["url"] == "https://signed.test/put" for call in uploads)

    def test_an_unsigned_target_goes_through_the_plane(self, tmp_path):
        """Self-host: the store is a shared volume, so there is nothing to sign."""
        _, calls = _run_dispatch(tmp_path, signed=False)
        uploads = [call for call in calls if call["verb"] == "PUT"]
        assert uploads
        assert all("/infra/pipeline/upload/run1/" in call["url"] for call in uploads)

    def test_the_source_bytes_are_sent_verbatim(self, tmp_path):
        _, calls = _run_dispatch(tmp_path, signed=True)
        sizes = [call["bytes"] for call in calls if call["verb"] == "PUT"]
        assert len(b"%PDF-1.4 test") in sizes


class TestPublishPayload:
    def test_a_collection_target_dispatches_as_fm(self, tmp_path):
        _, calls = _run_dispatch(tmp_path, signed=True, collection=True)
        publish = next(c for c in calls if c["url"].endswith("/submit/publish"))
        # Documents stay whole for a collection; a table target merges them.
        assert publish["json"]["ingestion_mode"] == "fm"

    def test_a_table_target_dispatches_as_dm_and_names_the_context(self, tmp_path):
        _, calls = _run_dispatch(tmp_path, signed=True)
        publish = next(c for c in calls if c["url"].endswith("/submit/publish"))
        assert publish["json"]["ingestion_mode"] == "dm"
        assert publish["json"]["target_context"] == "Data/Deals"

    def test_the_observability_block_travels_with_the_run(self, tmp_path):
        """Without it a dispatched run journals nowhere the caller can read."""
        _, calls = _run_dispatch(tmp_path, signed=True)
        publish = next(c for c in calls if c["url"].endswith("/submit/publish"))
        assert publish["json"]["observability"]["events_context"].endswith(
            "Ingestion/Events",
        )

    def test_no_identity_crosses_the_wire(self, tmp_path):
        """The plane derives identity from the session, so sending it would be
        both redundant and an invitation to trust it."""
        _, calls = _run_dispatch(tmp_path, signed=True)
        publish = next(c for c in calls if c["url"].endswith("/submit/publish"))
        assert "user_id" not in publish["json"]


class TestRefusals:
    def test_a_target_count_mismatch_refuses_before_uploading(self, tmp_path):
        """Fewer targets than files means a partial run would be dispatched."""
        source = tmp_path / "a.pdf"
        source.write_bytes(b"x")

        def fake_post(url, json=None, headers=None, timeout=None):
            return _Response(
                {"dispatch_id": "run1", "request_upload": {}, "sources": []},
            )

        request = _request()
        with (
            patch.object(client.requests, "post", fake_post),
            patch.object(
                client.requests,
                "put",
                lambda *a, **k: _Response({}),
            ),
        ):
            with pytest.raises(RuntimeError, match="partial run"):
                client.dispatch_run(
                    base_url="http://plane.test",
                    run_key="run1",
                    request=request,
                    request_key="jobs/run1/request.json",
                    request_payload={},
                    paths=[str(source)],
                )

    def test_the_planes_own_reason_survives(self):
        busy = _Response({"detail": "Job run1-0000 is being recovered by x"}, 409)
        with patch.object(client.requests, "post", lambda *a, **k: busy):
            with pytest.raises(RuntimeError, match="being recovered by x"):
                client.request_retry(
                    base_url="http://plane.test",
                    dispatch_id="run1",
                    scope="dlq",
                )


class TestProbe:
    def test_an_unconfigured_plane_is_absent(self):
        assert client.probe(base_url="") is False

    def test_a_plane_that_cannot_reach_its_backends_reads_as_absent(self):
        """Configured is not reachable.

        Treating a broken plane as present would dispatch files to a fleet that
        never receives them, and the run would sit queued forever.
        """
        with patch.object(
            client.requests,
            "get",
            lambda *a, **k: _Response({"ok": False, "detail": "no bucket"}),
        ):
            assert client.probe(base_url="http://plane.test") is False

    def test_a_healthy_plane_is_present(self):
        with patch.object(
            client.requests,
            "get",
            lambda *a, **k: _Response({"ok": True}),
        ):
            assert client.probe(base_url="http://plane.test") is True
