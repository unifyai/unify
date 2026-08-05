"""Exercise the author-time render gate against a vendored runtime host.

The check that matters is the last one: a canvas that lints clean, typechecks
clean and bundles clean, then throws on mount. Neither tsc nor esbuild can see
that class of bug, and it is the entire reason the render is a hard gate rather
than advisory.

    UNITY_CANVAS_TOOLCHAIN_ROOT=... UNITY_CANVAS_HOST_ROOT=... \
        .venv/bin/python tests/_verify_canvas_render.py
"""

from __future__ import annotations

import sys
from pathlib import Path

from unify.canvas_manager.ops.build_ops import build_canvas, toolchain_available
from unify.canvas_manager.ops.review_ops import _host_root, render_and_review

WORKS = """
import * as React from 'react';
import { Canvas, cn, type CanvasViewProps } from '@unity/canvas-kit';

interface Row extends Record<string, unknown> {
  title: string;
  status: string;
}

function Card({ className, ...props }: React.HTMLAttributes<HTMLDivElement>) {
  return <div className={cn('rounded-xl border bg-card p-6 text-card-foreground shadow', className)} {...props} />;
}

export default function Tracker({ canvas }: CanvasViewProps) {
  const rows = (canvas.data.tasks ?? []) as Row[];
  return (
    <Canvas>
      <div className="flex flex-col gap-4">
        <h2 className="text-2xl font-semibold">{String(canvas.props.title ?? 'Tasks')}</h2>
        <Card>
          <p className="mb-2 font-semibold">Open ({rows.length})</p>
          <table className="w-full text-sm">
            <tbody>
              {rows.map((row) => (
                <tr key={row.title} className="border-b">
                  <td className="p-2">{row.title}</td>
                  <td className="p-2 text-muted-foreground">{row.status}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </Card>
      </div>
    </Canvas>
  );
}
"""

# Compiles and typechecks: `rows[0]` is `Row | undefined` under
# noUncheckedIndexedAccess, but the cast launders that away -- exactly the shape
# of mistake an assistant makes when it assumes a binding returned rows.
THROWS_ON_MOUNT = """
import * as React from 'react';
import { Canvas, type CanvasViewProps } from '@unity/canvas-kit';

interface Row extends Record<string, unknown> {
  nested: { label: string };
}

export default function Broken({ canvas }: CanvasViewProps) {
  const rows = (canvas.data.empty ?? []) as Row[];
  const first = rows[0] as Row;
  return (
    <Canvas>
      <div className="rounded-xl border bg-card p-6">
        <p>{first.nested.label}</p>
      </div>
    </Canvas>
  );
}
"""

ROWS = {
    "tasks": [
        {"title": "Ship the canvas kit", "status": "open"},
        {"title": "Wire the data plane", "status": "open"},
        {"title": "Review the render gate", "status": "done"},
    ],
    "empty": [],
}


def main() -> int:
    if not toolchain_available():
        print("SKIP: no canvas toolchain installed")
        return 0
    if _host_root() is None:
        print("SKIP: no canvas host installed")
        return 0

    failures = 0

    def check(name: str, ok: bool, detail: str = "") -> None:
        nonlocal failures
        if ok:
            print(f"  ok   {name}")
        else:
            failures += 1
            print(f"  FAIL {name}{f' - {detail}' if detail else ''}")

    out = Path(sys.argv[1]) if len(sys.argv) > 1 else None

    report, bundle = build_canvas(WORKS, kit_version="0.1.0")
    check("fixture builds", report.ok, "; ".join(report.diagnostics))
    if not report.ok:
        return 1

    good = render_and_review(
        token="verify0000ok",
        bundle=bundle,
        props={"title": "Pending tasks"},
        rows=ROWS,
        out_dir=out,
    )
    check("working canvas renders", good.rendered, good.error or "")
    sizes = [Path(shot).stat().st_size for shot in good.screenshots]
    check(
        "both themes captured, and neither is blank",
        len(sizes) == 2 and all(size > 2000 for size in sizes),
        f"{good.screenshots} {sizes}",
    )

    broken_report, broken_bundle = build_canvas(THROWS_ON_MOUNT, kit_version="0.1.0")
    check(
        "a canvas that throws on mount still compiles",
        broken_report.ok,
        "; ".join(broken_report.diagnostics),
    )
    if broken_report.ok:
        bad = render_and_review(
            token="verify0000no",
            bundle=broken_bundle,
            props={},
            rows=ROWS,
            out_dir=out,
        )
        check("render gate rejects it", not bad.rendered, bad.verdict)
        check("failure names the cause", bool(bad.error), bad.error or "")

    print(
        f"\n{'ALL CHECKS PASSED' if failures == 0 else f'{failures} CHECK(S) FAILED'}",
    )
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
