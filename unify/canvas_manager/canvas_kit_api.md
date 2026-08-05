# @unity/canvas-kit

The protocol and brand layer a canvas is authored against. Generated
from the kit's type declarations by `scripts/generate_canvas_kit_api.py`
— do not edit by hand.

The kit carries no presentational vocabulary. Cards, tables, badges,
dialogs, inputs and charts are shadcn component source INLINED into the
canvas module and compiled against the vendored substrate — the
`@radix-ui/react-*` set, `class-variance-authority`, `clsx`,
`tailwind-merge`, `lucide-react` and `recharts` are importable, and
nothing else resolves at view time. When inlining shadcn source,
rewrite `@/lib/utils` to `@unity/canvas-kit` (for `cn`) and inline any
`@/components/ui/*` sibling into the same module. Colour enters only
through semantic token utilities (`bg-primary`, `text-muted-foreground`,
`bg-destructive`, ...) and `seriesColor(n)` / `var(--chart-N)` for
chart fills; a class the shipped stylesheet lacks fails lint.

## Canvas root

### `<Canvas>`
Root wrapper for a canvas.

- `padding?: Pad` — Outer padding. Defaults to `lg`; use `none` when embedding a canvas inside another surface.
- plus standard element attributes except title

## Freshness

### `<Freshness>`
How old a materialised value is, stated instead of implied.

- `synced: string | number` — When the data was produced — ISO-8601 string or epoch milliseconds.
- `label?: string` — Prefix label; defaults to "Updated".
- `staleAfterMinutes?: number` — Minutes after which the age renders in the warning tone.
- plus standard element attributes except children

## Actions

### `<ActionButton>`
Button that runs one declared action.

- `canvas: CanvasRuntime`
- `action: string`
- `args?: Record<string, unknown>` — Arguments to submit. Omit for an action that takes none.
- `label?: React.ReactNode` — Overrides the label the action was declared with.
- plus standard element attributes except onClick

### `<ActionResult>`
Outcome of an action's most recent run.

- `canvas: CanvasRuntime`
- `action: string`
- `successMessage?: React.ReactNode` — Shown on success. Defaults to a generic acknowledgement.
- plus standard element attributes except title

### `<ActionForm>`

- `canvas: CanvasRuntime`
- `action: string`
- `submitLabel?: React.ReactNode` — Overrides the submit label the action was declared with.
- plus standard element attributes except title

### `useCanvasAction(canvas, name)`
Look up one declared action and its latest run.

<!-- 6 components -->
