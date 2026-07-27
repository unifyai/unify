"""Fixtures for CanvasManager tests."""

import pytest

from unify.canvas_manager.simulated import SimulatedCanvasManager

# Minimal canvas that passes every authoring gate. Tests that care about a
# specific gate mutate a copy of this rather than restating a whole component.
VALID_TSX = (
    'import { Canvas, Stack } from "@unity/canvas-kit";\n'
    "export default function View({ canvas }) {\n"
    '  return <Canvas><Stack gap="md" /></Canvas>;\n'
    "}\n"
)


@pytest.fixture
def canvas_manager():
    """Fresh in-memory CanvasManager."""
    return SimulatedCanvasManager()


@pytest.fixture
def valid_tsx():
    """Canvas source that compiles cleanly."""
    return VALID_TSX
