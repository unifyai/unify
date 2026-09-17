"""
Pydantic model for the Functions/Meta context.

Stores the primitives sync state.
"""

from typing import Dict

from pydantic import BaseModel, Field


class FunctionsMeta(BaseModel):
    """
    Metadata record for the Functions context.

    Stores sync hashes to detect when primitives have changed and need
    re-synchronization.
    """

    meta_id: int = Field(
        1,
        description="Fixed ID for the single metadata row.",
    )
    primitives_hash_by_manager: Dict[str, str] = Field(
        default_factory=dict,
        description=(
            "Per-manager hash of primitive signatures and docstrings. "
            "Keys are primitive namespace aliases (e.g., 'actor'). "
            "Enables scoped primitive sync without global recomputation."
        ),
    )
