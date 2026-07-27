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

    model_config = SettingsConfigDict(
        env_prefix="UNITY_CANVAS_",
        case_sensitive=True,
        extra="ignore",
    )
