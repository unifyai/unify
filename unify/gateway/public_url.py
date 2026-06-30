"""Callback URL construction for gateway routes."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal, Protocol, runtime_checkable
from urllib.parse import urljoin

GatewaySurface = Literal["comms", "adapters"]


@runtime_checkable
class PublicUrlProvider(Protocol):
    """Build externally reachable URLs for gateway callbacks."""

    def base_url(self, surface: GatewaySurface = "comms") -> str:
        """Return the public base URL for a gateway surface."""

    def url_for(self, path: str, *, surface: GatewaySurface = "comms") -> str:
        """Return a public URL for ``path`` on ``surface``."""


@dataclass(frozen=True)
class StaticPublicUrlProvider:
    """Static public URL provider configured by process settings."""

    comms_base_url: str
    adapters_base_url: str = ""

    def base_url(self, surface: GatewaySurface = "comms") -> str:
        if surface == "adapters" and self.adapters_base_url:
            return self.adapters_base_url.rstrip("/")
        return self.comms_base_url.rstrip("/")

    def url_for(self, path: str, *, surface: GatewaySurface = "comms") -> str:
        normalized_path = path.lstrip("/")
        return urljoin(f"{self.base_url(surface)}/", normalized_path)


__all__ = ["GatewaySurface", "PublicUrlProvider", "StaticPublicUrlProvider"]
