"""Dashboard layout operations for DashboardManager.

Helper functions for building dashboard records and serializing layouts.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any, List, Optional, Sequence

from unify.dashboard_manager.types.dashboard import (
    DashboardRecordRow,
    TilePosition,
)


def build_dashboard_record_row(
    token: str,
    title: str,
    tiles: List[TilePosition],
    description: Optional[str] = None,
) -> DashboardRecordRow:
    """Build a DashboardRecordRow ready for insertion into the Unify context."""
    now = datetime.now(timezone.utc).isoformat()
    return DashboardRecordRow(
        token=token,
        title=title,
        description=description,
        layout=serialize_layout(tiles),
        tile_count=len(tiles),
        created_at=now,
        updated_at=now,
    )


def coerce_tile_positions(tiles: Sequence[Any]) -> List[TilePosition]:
    """Normalize actor-supplied layout entries into ``TilePosition`` models."""

    coerced: List[TilePosition] = []
    for tile in tiles:
        if isinstance(tile, TilePosition):
            coerced.append(tile)
        elif isinstance(tile, dict):
            coerced.append(TilePosition.model_validate(tile))
        else:
            raise TypeError(f"Unsupported tile position type: {type(tile)!r}")
    return coerced


def serialize_layout(tiles: Sequence[Any]) -> str:
    """Serialize a list of TilePosition objects to a JSON string."""
    typed = coerce_tile_positions(tiles)
    return json.dumps([t.model_dump() for t in typed])


def deserialize_layout(json_str: str) -> List[TilePosition]:
    """Deserialize a JSON string to a list of TilePosition objects."""
    data = json.loads(json_str)
    return [TilePosition(**item) for item in data]
