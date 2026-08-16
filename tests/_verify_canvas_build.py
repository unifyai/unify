"""Exercise the real lint -> tsc -> esbuild chain against an installed toolchain.

Runs in seconds and needs no LLM, no backend and no browser, so it is the fast
feedback loop while working on the build pipeline. Deleted once the pipeline is
stable; the durable coverage lives in tests/canvas_manager/test_build_ops.py.

    UNIFY_CANVAS_TOOLCHAIN_ROOT=/opt/canvas-toolchain \
        .venv/bin/python tests/_verify_canvas_build.py
"""

from __future__ import annotations

import sys

from unify.canvas_manager.ops.build_ops import build_canvas, toolchain_available

CLEAN = """
import * as React from 'react';
import { Canvas, Freshness, cn, type CanvasViewProps } from '@unity/canvas-kit';
import { ChevronDown } from 'lucide-react';

interface Row extends Record<string, unknown> {
  title: string;
  status: string;
}

function Card({ className, ...props }: React.HTMLAttributes<HTMLDivElement>) {
  return <div className={cn('rounded-xl border bg-card text-card-foreground shadow', className)} {...props} />;
}

export default function Tracker({ canvas }: CanvasViewProps) {
  const rows = (canvas.data.tasks ?? []) as Row[];
  const open = React.useMemo(() => rows.filter((r) => r.status !== 'done'), [rows]);

  return (
    <Canvas>
      <Card className="p-6">
        <div className="flex items-center justify-between">
          <span className="font-semibold">Open ({open.length})</span>
          <ChevronDown className="h-4 w-4 text-muted-foreground" />
        </div>
        <table className="w-full text-sm">
          <tbody>
            {open.map((row) => (
              <tr key={row.title} className="border-b">
                <td className="p-2">{row.title}</td>
                <td className="p-2 text-muted-foreground">{row.status}</td>
              </tr>
            ))}
          </tbody>
        </table>
        <Freshness synced={Date.now()} />
      </Card>
    </Canvas>
  );
}
"""

# A wrong prop name. esbuild compiles this happily, so only the typecheck stage
# can reject it -- which is the whole reason tsc runs.
TYPE_ERROR = CLEAN.replace("synced={Date.now()}", "synced={Date.now()} staleAfter={1}")

HEX = CLEAN.replace(
    'className="font-semibold"',
    'className="font-semibold" style={{ color: "#ff0000" }}',
)

# A class the shipped stylesheet does not contain: it would silently style
# nothing at view time, which is exactly why the lint refuses it.
GHOST_CLASS = CLEAN.replace('className="p-6"', 'className="p-6 backdrop-hue-rotate-15"')

FORBIDDEN_IMPORT = "import axios from 'axios';\n" + CLEAN

# Inlining a dataset instead of binding to it. Lints and typechecks; the only
# thing standing between this and a canvas row holding a megabyte is the ceiling.
_FILLER = ",".join(f'{{ title: "row {n}", status: "open" }}' for n in range(20_000))
OVERSIZED = CLEAN.replace(
    "const rows = (canvas.data.tasks ?? []) as Row[];",
    f"const rows = [{_FILLER}] as Row[];",
)


def main() -> int:
    if not toolchain_available():
        print("SKIP: no canvas toolchain installed")
        return 0

    failures = 0

    def check(name: str, ok: bool, detail: str = "") -> None:
        nonlocal failures
        if ok:
            print(f"  ok   {name}")
        else:
            failures += 1
            print(f"  FAIL {name}{f' - {detail}' if detail else ''}")

    report, code = build_canvas(CLEAN, kit_version="0.1.0")
    check("clean canvas builds", report.ok, "; ".join(report.diagnostics))
    check("bundle emitted", bool(code) and "export" in code)
    check(
        "react stays external",
        'from "react"' in code or "from'react'" in code,
        code[:200],
    )
    check("kit stays external", "@unity/canvas-kit" in code)
    check("sha recorded", len(report.bundle_sha) == 64)

    again, code_again = build_canvas(CLEAN, kit_version="0.1.0")
    check(
        "build is byte-reproducible",
        again.bundle_sha == report.bundle_sha and code_again == code,
        f"{report.bundle_sha[:12]} != {again.bundle_sha[:12]}",
    )

    bad_type, _ = build_canvas(TYPE_ERROR)
    check(
        "type error blocks publish",
        not bad_type.ok and bad_type.failed_stage == "typecheck",
        bad_type.failed_stage,
    )

    bad_hex, _ = build_canvas(HEX)
    check(
        "hex colour blocks publish",
        not bad_hex.ok and bad_hex.failed_stage == "lint",
    )

    bad_import, _ = build_canvas(FORBIDDEN_IMPORT)
    check(
        "unavailable import blocks publish",
        not bad_import.ok and bad_import.failed_stage == "lint",
    )

    ghost, _ = build_canvas(GHOST_CLASS)
    check(
        "class outside the shipped stylesheet blocks publish",
        not ghost.ok and ghost.failed_stage == "lint",
        "; ".join(ghost.diagnostics)[:120],
    )

    oversized, oversized_code = build_canvas(OVERSIZED)
    check(
        "oversized bundle blocks publish",
        not oversized.ok and oversized.failed_stage == "bundle" and not oversized_code,
        f"{oversized.failed_stage} {oversized.bytes}",
    )
    check(
        "the size failure names the remedy",
        any("binding" in problem for problem in oversized.diagnostics),
        "; ".join(oversized.diagnostics)[:200],
    )

    print(
        f"\n{'ALL CHECKS PASSED' if failures == 0 else f'{failures} CHECK(S) FAILED'}",
    )
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
