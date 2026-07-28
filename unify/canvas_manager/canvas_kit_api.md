# @unity/canvas-kit

The component vocabulary a canvas is written against. Generated from the
kit's type declarations by `scripts/generate_canvas_kit_api.py` — do not
edit by hand.

Import everything from `@unity/canvas-kit`. Two rules the API enforces:
no component takes a raw colour (only `tone` and chart series indices),
and layout props are enumerated scales rather than class strings.

## Scales

- `Align` = 'start' | 'center' | 'end' | 'stretch' | 'baseline'
- `Gap` = 'none' | 'xs' | 'sm' | 'md' | 'lg' | 'xl'
- `Justify` = 'start' | 'center' | 'end' | 'between' | 'around'
- `Pad` = Gap
- `Tone` = 'default' | 'muted' | 'success' | 'info' | 'warning' | 'danger'

## Layout

### `<Canvas>`
Root wrapper for a canvas.

- `padding?: Pad` — Outer padding. Defaults to `lg`; use `none` when embedding a canvas inside another surface.
- plus standard element attributes except title

### `<Stack>`
Vertical flow.

- `gap?: Gap`
- `align?: Align`
- `justify?: Justify`
- plus standard element attributes

### `<Row>`
Horizontal flow.

- `gap?: Gap`
- `align?: Align`
- `justify?: Justify`
- `wrap?: boolean` — Set false to keep items on one line and allow horizontal overflow.
- plus standard element attributes

### `<Grid>`
Responsive column grid.

- `columns?: 1 | 2 | 3 | 4`
- `gap?: Gap`
- plus standard element attributes

### `<Box>`
Neutral padded container, for when a Card's border and shadow are too heavy.

- `padding?: Pad`
- plus standard element attributes

### `<Section>`
A titled region of a canvas.

- `title?: React.ReactNode`
- `description?: React.ReactNode`
- `actions?: React.ReactNode` — Rendered opposite the title — filters, a refresh control, a link.
- `gap?: Gap`
- plus standard element attributes except title

## Cards

### `<Card>`
Surface container.

### `<CardHeader>`

### `<CardTitle>`

### `<CardDescription>`

### `<CardContent>`

### `<CardFooter>`

## Typography

### `<Heading>`
Section heading.

- `level?: 1 | 2 | 3 | 4`
- `tone?: Tone`
- plus standard element attributes

### `<Text>`
Body copy.

- `size?: 'sm' | 'md' | 'lg'`
- `tone?: Tone`
- `numeric?: boolean` — Tabular figures, for numbers that should align in a column.
- `as?: 'p' | 'span' | 'div'` — Render inline rather than as a paragraph.
- plus standard element attributes

### `<Code>`
Monospace run, for identifiers, tokens and code fragments.

## Indicators

### `<Badge>`
Compact status pill.

- `tone?: Tone`
- plus standard element attributes

### `<Stat>`
A single headline figure with its label.

- `label: React.ReactNode`
- `value: React.ReactNode`
- `hint?: React.ReactNode` — Secondary line under the value — a delta, a period, a denominator.
- `tone?: Tone`
- plus standard element attributes except children

### `<KpiRow>`
A row of Stats in equal-width cards.

- `items: StatProps[]`
- plus standard element attributes except children

### `<Progress>`
Determinate progress bar.

- `value: number`
- `tone?: Tone`
- `showValue?: boolean` — Show the percentage to the right of the track.
- plus standard element attributes except children

## Tables

### `<Table>`
Read-only data table.

- `columns: Column<Row>[]`
- `rows: Row[]`
- `rowKey?: (row: Row, index: number) => string | number` — Stable row key. Falls back to the array index.
- `emptyMessage?: string` — Shown in place of the table body when `rows` is empty.
- `maxHeight?: number` — Cap the body height and scroll inside it, keeping the header pinned.
- plus standard element attributes except children

## Lists

### `<List>`

- `items: ListItem[]`
- `emptyMessage?: string`
- `bordered?: boolean` — Wrap in a bordered card. Off when the list already sits inside one.
- plus standard element attributes except children

### `<Checklist>`

- `items: ChecklistItem[]`
- `emptyMessage?: string`
- `onToggle?: (item: ChecklistItem, index: number) => void` — Called when an item is toggled. Omit for a read-only checklist — without it the boxes render disabled, so a display-only list cannot look interactive.
- plus standard element attributes except children, onToggle

## States

### `<Empty>`
Placeholder for a surface with no data.

- `message?: string`
- `hint?: string`
- plus standard element attributes except children

### `<Skeleton>`
Skeleton block.

### `<Loading>`
Loading placeholder shaped like a list of rows.

- `rows?: number`
- `label?: string`
- plus standard element attributes except children

### `<ErrorState>`
Failure state.

- `message?: string`
- `detail?: string` — Technical detail. Rendered monospaced and clipped; safe to pass a stack.
- plus standard element attributes except children

## Charts

### `<BarChart>`
Grouped bar chart.

- `data: Datum[]`
- `x: string` — Field plotted along the category axis.
- `y: string | string[]` — Field(s) plotted as values. Each gets the next `--chart-N` colour.
- `height?: number`
- `showLegend?: boolean` — Legend is hidden for a single series, where it only adds noise.
- `showGrid?: boolean`
- `emptyMessage?: string`
- plus standard element attributes except children

### `<LineChart>`
Line chart.

- `data: Datum[]`
- `x: string` — Field plotted along the category axis.
- `y: string | string[]` — Field(s) plotted as values. Each gets the next `--chart-N` colour.
- `height?: number`
- `showLegend?: boolean` — Legend is hidden for a single series, where it only adds noise.
- `showGrid?: boolean`
- `emptyMessage?: string`
- plus standard element attributes except children

### `<AreaChart>`
Area chart.

- `data: Datum[]`
- `x: string` — Field plotted along the category axis.
- `y: string | string[]` — Field(s) plotted as values. Each gets the next `--chart-N` colour.
- `height?: number`
- `showLegend?: boolean` — Legend is hidden for a single series, where it only adds noise.
- `showGrid?: boolean`
- `emptyMessage?: string`
- plus standard element attributes except children

### `<PieChart>`
Share-of-total chart.

- `data: Datum[]`
- `nameKey: string` — Field holding the slice label.
- `valueKey: string` — Field holding the slice magnitude.
- `height?: number`
- `donut?: boolean` — Render as a donut.
- `showLegend?: boolean`
- `emptyMessage?: string`
- plus standard element attributes except children

<!-- 30 components -->
