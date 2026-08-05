"""Tests for the canvas authoring gates.

The linter is the only place the no-colour rule can be applied to a canvas.
Authored TSX never passes through console's lint-staged or its production build,
so a gap here ships an off-palette canvas that looks correct in one theme and
wrong in the other. Each case below is a way that has previously been possible
to get colour or an unresolvable import past a naive check.
"""

import pytest

from unify.canvas_manager.ops.build_ops import (
    allowed_imports,
    build_canvas,
    lint_source,
    toolchain_available,
)

CLEAN = (
    'import * as React from "react";\n'
    'import { Canvas, cn } from "@unity/canvas-kit";\n'
    "export default function View({ canvas }) {\n"
    '  return <Canvas><span className={cn("bg-primary text-primary-foreground")}>ok</span></Canvas>;\n'
    "}\n"
)


class TestColourRejection:
    def test_clean_source_passes(self):
        assert lint_source(CLEAN) == []

    def test_hex_literal_is_rejected(self):
        problems = lint_source('const style = { color: "#ff0000" };')
        assert len(problems) == 1
        assert "hex colour" in problems[0]

    def test_functional_colour_is_rejected(self):
        assert any(
            "rgb()" in problem for problem in lint_source("const c = 'rgba(1,2,3,.5)';")
        )

    def test_named_utility_class_is_rejected(self):
        # These are the dangerous case: they look right in review but the canvas
        # stylesheet ships no colour utilities, so they silently do nothing.
        problems = lint_source('<div className="bg-red-500" />')
        assert len(problems) == 1
        assert "no effect" in problems[0]

    def test_arbitrary_colour_class_is_rejected(self):
        assert lint_source('<div className="text-[#aabbcc]" />')

    def test_css_font_family_is_rejected(self):
        assert any(
            "font-family" in problem
            for problem in lint_source("<style>{`a { font-family: serif }`}</style>")
        )


class TestFalsePositives:
    """Cases a blunt regex would reject, and which must keep working."""

    def test_svg_fragment_reference_is_not_a_colour(self):
        # `url(#gradient)` is an SVG reference; treating the `#` as a hex colour
        # would make gradients and clip paths unusable.
        assert lint_source('<rect fill="url(#gradient-1)" />') == []

    def test_hex_inside_a_comment_is_ignored(self):
        assert (
            lint_source("// the brand green is #1c6460\nexport default () => null;")
            == []
        )

    def test_tone_props_are_untouched(self):
        assert (
            lint_source('<div className="bg-destructive text-muted-foreground" />')
            == []
        )


class TestImports:
    def test_every_allowed_import_passes(self):
        source = "\n".join(
            f'import x{i} from "{name}";'
            for i, name in enumerate(sorted(allowed_imports()))
        )
        assert lint_source(source) == []

    def test_unavailable_package_is_rejected(self):
        # There is no bundler and no network at view time, so this would be a
        # load failure in front of the user rather than a slow import.
        problems = lint_source('import axios from "axios";')
        assert len(problems) == 1
        assert "not available at view time" in problems[0]

    def test_relative_import_is_rejected(self):
        problems = lint_source('import { helper } from "./helper";')
        assert len(problems) == 1
        assert "single module" in problems[0]

    def test_bare_side_effect_import_is_checked(self):
        assert lint_source('import "some-polyfill";')


class TestDiagnostics:
    def test_problems_name_the_line(self):
        source = "const a = 1;\nconst b = 2;\nconst c = '#abc';\n"
        problems = lint_source(source)
        assert len(problems) == 1
        assert problems[0].startswith("line 3:")

    def test_every_violation_is_reported_not_just_the_first(self):
        # The author fixes these in one pass, so reporting one at a time would
        # turn a single revision into several.
        source = (
            "const a = '#fff';\nconst b = 'rgb(0,0,0)';\nimport x from \"lodash\";\n"
        )
        assert len(lint_source(source)) == 3


@pytest.mark.skipif(
    not toolchain_available(),
    reason="needs the vendored canvas build toolchain",
)
class TestBundleCeiling:
    """The compiled bundle is stored on the canvas row, so its size is bounded."""

    def test_an_inlined_dataset_is_rejected(self):
        # Lints and typechecks cleanly; only the ceiling stands between this and
        # a canvas row holding most of a megabyte. Inlining is also the wrong
        # shape -- the data would be frozen at author time rather than live.
        filler = ",".join(f'{{ label: "row {n}", value: {n} }}' for n in range(20_000))
        source = (
            "import * as React from 'react';\n"
            "import { Canvas } from '@unity/canvas-kit';\n"
            f"const rows = [{filler}];\n"
            "export default function View() {\n"
            "  return <Canvas><span>{rows.length}</span></Canvas>;\n"
            "}\n"
        )

        report, code = build_canvas(source)

        assert not report.ok
        assert report.failed_stage == "bundle"
        # Nothing is returned to store, and the message points at the fix rather
        # than just stating the limit.
        assert code == ""
        assert any("binding" in problem for problem in report.diagnostics)
