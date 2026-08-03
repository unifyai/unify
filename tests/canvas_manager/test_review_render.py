"""Tests for the author-time render gate.

The gate exists to catch a canvas that lints clean, typechecks clean and bundles
clean, then throws on mount — the one failure class neither tsc nor esbuild can
see. That end-to-end assertion needs a browser and the vendored runtime host, so
it is skipped where those are absent (the image has both; a plain checkout has
neither, and branding's own smoke test covers the same protocol from the other
side).

The checks that need nothing are run unconditionally, because two of them are
load-bearing in a way that is easy to get backwards:

* A missing gate must not block authoring. If it did, every environment without
  a browser would silently stop being able to publish a canvas.
* The harness must not hand the frame anything the real parent would not. It
  plays console's role, so a shortcut here would let a canvas pass a gate that
  tested a more permissive contract than production.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from unify.canvas_manager.ops import review_ops
from unify.canvas_manager.ops.build_ops import build_canvas, toolchain_available
from unify.canvas_manager.types.view import ReviewReport

WORKING = """
import * as React from 'react';
import { Canvas, Card, CardContent, Stat, type CanvasViewProps } from '@unity/canvas-kit';

export default function View({ canvas }: CanvasViewProps) {
  const rows = canvas.data.tasks ?? [];
  return (
    <Canvas>
      <Card>
        <CardContent>
          <Stat label="Open" value={rows.length} />
        </CardContent>
      </Card>
    </Canvas>
  );
}
"""

# Typechecks: the cast launders away the `| undefined` that
# noUncheckedIndexedAccess would otherwise force the author to handle. This is
# the shape of the mistake an assistant makes when it assumes a binding returned
# rows, and it throws the moment the binding comes back empty.
THROWS_ON_MOUNT = """
import * as React from 'react';
import { Canvas, Card, CardContent, Text, type CanvasViewProps } from '@unity/canvas-kit';

interface Row extends Record<string, unknown> {
  nested: { label: string };
}

export default function View({ canvas }: CanvasViewProps) {
  const rows = (canvas.data.tasks ?? []) as Row[];
  const first = rows[0] as Row;
  return (
    <Canvas>
      <Card>
        <CardContent>
          <Text>{first.nested.label}</Text>
        </CardContent>
      </Card>
    </Canvas>
  );
}
"""

renderable = pytest.mark.skipif(
    not review_ops.gate_available() or not toolchain_available(),
    reason="needs the vendored canvas host, a chromium and the build toolchain",
)


class TestGateAvailability:
    def test_a_missing_host_does_not_block_authoring(self, monkeypatch):
        # Inverting this would stop every browserless environment from ever
        # publishing a canvas, which is a far worse failure than skipping a
        # visual check.
        monkeypatch.setattr(review_ops, "_host_root", lambda: None)

        report = review_ops.render_and_review(token="abc123", bundle="", props={})

        assert report.rendered is True
        assert "skipped" in report.verdict


class TestHarnessContract:
    """The harness stands in for console, so it must not be more permissive."""

    def test_only_the_named_aliases_are_offered(self):
        html = review_ops._parent_html(
            host_origin="http://127.0.0.1:1",
            source="export default () => null",
            props={"title": "T"},
            rows={"tasks": [{"a": 1}], "people": []},
        )
        assert 'aliases: ["people", "tasks"]' in html

    def test_no_action_targets_are_handed_over(self):
        # A function or task id crossing this boundary is the thing the whole
        # action design exists to prevent; the harness must not be the exception.
        html = review_ops._parent_html(
            host_origin="http://127.0.0.1:1",
            source="export default () => null",
            props={},
            rows={},
        )
        assert "function_id" not in html
        assert "task_id" not in html
        assert "actions: []" in html

    def test_the_frame_is_sandboxed_the_way_console_sandboxes_it(self):
        html = review_ops._parent_html(
            host_origin="http://127.0.0.1:1",
            source="",
            props={},
            rows={},
        )
        assert 'sandbox="allow-scripts"' in html
        # An opaque origin is the entire isolation premise; granting this would
        # give the frame cookies, storage and the parent DOM.
        assert "allow-same-origin" not in html

    def test_a_bundle_cannot_break_out_of_the_script_tag(self):
        # The HTML parser ends a script block at the first literal `</script`
        # whatever the JS context, and JSON does not escape forward slashes. A
        # canvas that merely renders that text would otherwise take the harness
        # down and be reported as a render failure it did not cause.
        html = review_ops._parent_html(
            host_origin="http://127.0.0.1:1",
            source='const s = "</script><script>alert(1)</script>";',
            props={"note": "</script>"},
            rows={"a": [{"html": "<!-- </script> -->"}]},
        )

        body, _, tail = html.partition("<script>")
        assert tail.count("</script>") == 1
        assert tail.endswith("</script></body>")


@renderable
class TestRender:
    def test_a_working_canvas_renders_in_both_themes(self, tmp_path):
        report, bundle = build_canvas(WORKING, kit_version="0.1.0")
        assert report.ok, report.diagnostics

        result = review_ops.render_and_review(
            token="renderok0001",
            bundle=bundle,
            props={},
            rows={"tasks": [{"title": "one"}, {"title": "two"}]},
            out_dir=tmp_path,
        )

        assert result.rendered, result.error
        # Both themes, because a canvas can be legible in one and unreadable in
        # the other and only a screenshot of each shows it.
        assert len(result.screenshots) == 2
        assert all(
            tmp_path.joinpath(shot).exists() for shot in map(str, result.screenshots)
        )

    def test_a_canvas_that_throws_on_mount_is_rejected(self, tmp_path):
        report, bundle = build_canvas(THROWS_ON_MOUNT, kit_version="0.1.0")
        # The premise: the compiler is happy with it.
        assert report.ok, report.diagnostics

        result = review_ops.render_and_review(
            token="renderbad001",
            bundle=bundle,
            props={},
            rows={"tasks": []},
            out_dir=tmp_path,
        )

        assert result.rendered is False
        # The actor has to be able to fix it, so the failure must say what broke.
        assert result.error


class TestCritique:
    """The critique is advisory and must stay that way.

    The gate is whether a canvas renders; whether it looks good is a note back to
    the actor. Confusing the two would either block publication on an opinion or
    let a genuinely broken canvas be described as fine.
    """

    def test_no_screenshots_yields_a_neutral_verdict(self):
        # Nothing to look at is not a criticism.
        verdict, issues = review_ops._critique([])

        assert verdict == "rendered"
        assert issues == []

    def test_an_unavailable_model_does_not_block_publication(self, monkeypatch):
        # No vision model configured, or no network. Publishing must proceed: the
        # render already established the only thing being gated.
        def explode(*args, **kwargs):
            raise RuntimeError("no model configured")

        monkeypatch.setattr("unify.common.reasoning.query_llm", explode)

        verdict, issues = review_ops._critique(["/nonexistent/shot.png"])

        assert verdict == "rendered"
        assert issues == []

    def test_a_failed_render_is_not_critiqued(self, monkeypatch):
        """A canvas that never mounted must not come back with a visual opinion.

        Critiquing it would either describe screenshots that do not exist or
        overwrite the error that says what actually broke.
        """
        monkeypatch.setattr(
            review_ops,
            "_render",
            lambda **kwargs: ReviewReport(rendered=False, error="threw on mount"),
        )
        monkeypatch.setattr(
            review_ops,
            "_critique",
            lambda shots: pytest.fail("critiqued a render that failed"),
        )

        report = review_ops._render_and_critique(
            host=Path("/unused"),
            token="t",
            source="",
            props={},
            rows={},
            out_dir=Path("/unused"),
        )

        assert report.rendered is False
        assert report.error == "threw on mount"
