"""Settings for the CanvasManager module."""

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class CanvasSettings(BaseSettings):
    """CanvasManager configuration.

    Supports both 'real' and 'simulated' implementations.
    """

    IMPL: str = Field(
        default="real",
        description="CanvasManager implementation: 'real' or 'simulated'.",
    )

    ENABLED: bool = Field(
        default=True,
        description="Whether the CanvasManager is enabled.",
    )

    REVIEW_ENABLED: bool = Field(
        default=True,
        description=(
            "Whether create_view/update_view render the canvas headlessly and "
            "critique it. Disable only where no browser is available; a render "
            "failure is otherwise a publication gate."
        ),
    )

    KIT_VERSION: str = Field(
        default="0.1.0",
        description=(
            "Version of @unity/canvas-kit canvases are compiled against. Recorded "
            "on each canvas so an existing one keeps rendering on the runtime it "
            "was reviewed against rather than silently upgrading."
        ),
    )

    TOOLCHAIN_ROOT: str = Field(
        default="",
        description=(
            "Node workspace holding esbuild, typescript and the vendored kit "
            "declarations. Empty means probe the standard image locations."
        ),
    )

    HOST_ROOT: str = Field(
        default="",
        description=(
            "Vendored runtime host used by the author-time render gate. The "
            "assets are byte-identical to what the canvas origin serves, so a "
            "preview shows what a viewer will see. Empty means probe the "
            "standard image locations."
        ),
    )

    MAX_BUNDLE_BYTES: int = Field(
        default=512_000,
        description=(
            "Ceiling on a compiled bundle. The bytes live on the canvas row, so "
            "this bounds row size. React and the kit are externals supplied by "
            "the runtime host, which puts a realistic canvas in single-digit "
            "kilobytes; anything near this limit is inlining data that belongs "
            "in a binding."
        ),
    )

    model_config = SettingsConfigDict(
        env_prefix="UNITY_CANVAS_",
        case_sensitive=True,
        extra="ignore",
    )
