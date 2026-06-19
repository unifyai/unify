from __future__ import annotations

import os

import httpx
import pytest

pytestmark = pytest.mark.skipif(
    os.environ.get("DROID_RUN_HOSTED_GATEWAY_SMOKE") != "true",
    reason="Hosted gateway smoke tests require live provider/backend credentials.",
)


def _gateway_url() -> str:
    url = os.environ.get("DROID_HOSTED_GATEWAY_URL", "").rstrip("/")
    if not url:
        pytest.skip("DROID_HOSTED_GATEWAY_URL is required")
    return url


def _admin_headers() -> dict[str, str]:
    admin_key = os.environ.get("ORCHESTRA_ADMIN_KEY", "")
    if not admin_key:
        pytest.skip("ORCHESTRA_ADMIN_KEY is required")
    return {"Authorization": f"Bearer {admin_key}"}


def test_hosted_gateway_health() -> None:
    response = httpx.get(f"{_gateway_url()}/health", timeout=10)

    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


@pytest.mark.parametrize(
    ("method", "path"),
    [
        ("GET", "/social/available-platforms"),
        ("POST", "/phone/send-text"),
        ("POST", "/whatsapp/send"),
        ("POST", "/gmail/send"),
        ("POST", "/outlook/send"),
        ("POST", "/teams/send"),
        ("POST", "/discord/send"),
    ],
)
def test_hosted_gateway_channel_auth_surface(method: str, path: str) -> None:
    response = httpx.request(
        method,
        f"{_gateway_url()}{path}",
        headers=_admin_headers(),
        json={},
        timeout=10,
    )

    assert response.status_code != 401
    assert response.status_code != 403
