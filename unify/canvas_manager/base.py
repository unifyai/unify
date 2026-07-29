"""
Base abstract class for CanvasManager.

Defines the public contract for authoring and managing canvases. All docstrings
are defined here and inherited by concrete implementations via
``@functools.wraps``.

IMPORTANT: Do not duplicate docstrings in concrete implementations.
"""

from __future__ import annotations

from abc import abstractmethod
from typing import Any, Dict, List, Optional

from unify.canvas_manager.types.action import CanvasAction, CanvasInvocationRecord
from unify.canvas_manager.types.binding import PrimitiveBinding
from unify.canvas_manager.types.view import (
    CanvasResult,
    CanvasViewRecord,
    ReviewReport,
)

DEFAULT_VISIBILITY = "private"


class BaseCanvasManager:
    """
    Public contract for authoring generative user interfaces.

    CanvasManager turns a natural-language request for a view -- "a tracker for
    my pending tasks", "a dashboard over last quarter's sales", "a to-do list I
    can tick off" -- into a real, running React interface the user can open,
    read and interact with.

    A canvas is **one whole view**: a single TSX module that renders the entire
    surface. There is no tile-and-layout split and no placement grid, because
    React already expresses composition. Write one component; it is the page.

    What a canvas is made of
    ------------------------
    - **Code.** TSX written against ``@unity/canvas-kit``, a fixed component
      vocabulary covering layout, cards, tables, lists, checklists, stats,
      badges and charts.
    - **Data.** Declarative ``bindings`` that read from any manager, re-executed
      server-side each time the canvas is viewed, so a canvas is live rather
      than a snapshot.
    - **Static values.** A ``props`` dictionary for anything computed once at
      authoring time.
    - **Actions.** Declared operations a viewer can trigger, which run stored
      functions or tasks and stream their progress back into the view.

    Data: bindings versus props
    ---------------------------
    Use a **binding** whenever the answer is a query -- rows, counts,
    aggregates, joins -- over a table. Bindings re-run on every view, so the
    canvas is always current, and the query itself never travels to the browser.

    Use **props** for values that took reasoning to produce: a summary of a
    conversation, an answer distilled from a document, anything that needed a
    language model. Those cannot re-run per view, so compute them once, pass
    them as props, and refresh them on a schedule with ``refresh_props``.

    Data from connected apps
    ------------------------
    A canvas cannot call a connected app while someone is looking at it. A
    provider call takes seconds rather than milliseconds, rate limits are per
    account rather than per viewer, and a call can come back needing a
    reconnection that a rendered surface has no way to resolve.

    So app data is **stored first, displayed second**. Fetch it with the
    integration tools, write it to a table, keep it fresh with a scheduled task,
    and bind the canvas to that table like any other data::

        rows = await primitives.integrations.github.list_issues(state="open")
        await primitives.data.ingest(rows, "Data/GitHubIssues")
        # ...schedule a task to repeat that, then bind the canvas to the table.

    This is also the only way to show two apps together. Providers cannot be
    joined against each other directly; once both are stored, joining them is an
    ordinary query.

    Binding to a table that has not been stored yet fails when the canvas is
    created, naming the table, rather than producing a view that renders empty.

    Interactivity
    -------------
    Declare an ``action`` when the viewer should be able to *do* something --
    send the emails, re-run the report, mark the batch complete. Give it an
    ``input_schema`` when it takes input, and the kit will render the form; the
    arguments are validated server-side before anything runs. Anything
    irreversible must set ``destructive=True`` and supply ``confirm`` text.

    Authoring loop
    --------------
    Every write is checked before it is published: the source is linted, type
    checked against the kit, compiled, its bindings dry-run, and the result
    rendered in a real browser and screenshotted. Compile and render failures
    block publication and come back as diagnostics. The screenshots come back
    too, so the view can be inspected and revised rather than assumed correct.

    Docstring Requirements for Subclass Methods
    -------------------------------------------
    All public methods MUST include comprehensive docstrings with:

    1. **One-line summary** -- What the method does
    2. **Extended description** -- When to use, contrasted with similar methods
    3. **Parameters section** -- EVERY parameter with type, semantics, defaults
       and example values
    4. **Returns section** -- Return type AND field-level structure
    5. **Raises section** -- Exceptions that may be raised
    6. **Usage Examples** -- MULTIPLE concrete examples
    7. **Anti-patterns section** -- What NOT to do and why
    8. **Notes section** -- Invariants and edge cases
    9. **See Also section** -- Cross-references to related methods

    This is CRITICAL because these docstrings ARE the documentation the caller
    reads before writing code against these methods.
    """

    _as_caller_description: str = (
        "a CanvasManager, authoring generative user interfaces"
    )

    # ──────────────────────────────────────────────────────────────────────────
    # Authoring
    # ──────────────────────────────────────────────────────────────────────────

    @abstractmethod
    def create_view(
        self,
        tsx: str,
        *,
        title: str,
        description: Optional[str] = None,
        bindings: Optional[List[PrimitiveBinding]] = None,
        props: Optional[Dict[str, Any]] = None,
        actions: Optional[List[CanvasAction]] = None,
        destination: Optional[str] = None,
        visibility: str = DEFAULT_VISIBILITY,
        review: bool = True,
    ) -> CanvasResult:
        """
        Author a new canvas and publish it.

        This is the entry point for every generated interface. Write one TSX
        module that renders the whole view, declare the data it needs, and this
        compiles it, checks it, renders it and returns a shareable URL.

        The module must ``export default`` a component taking a single
        ``{ canvas }`` prop::

            export default function MyView({ canvas }) { ... }

        That ``canvas`` object carries everything the view can reach:

        - ``canvas.data[alias]`` -- rows for a declared binding, ``undefined``
          until they arrive
        - ``canvas.props`` -- the static values passed here
        - ``canvas.invoke(name, args)`` -- trigger a declared action
        - ``canvas.invocations`` -- status of actions already triggered
        - ``canvas.ask(text)`` -- send the viewer's change request back

        Parameters
        ----------
        tsx : str
            The canvas source. Imports may reference ``react`` and
            ``@unity/canvas-kit`` only; both are provided at runtime. Any other
            import fails the build, because nothing else exists in the frame.

            Colour is not available: there is no colour prop and no colour class
            works. Use ``tone="success" | "warning" | "danger" | "muted"`` and
            chart series indices. This is what keeps a canvas matching the
            surrounding application in both light and dark themes.

        title : str
            Short human-readable name, shown in listings and in the frame
            header. Examples: ``"Pending tasks"``, ``"Q3 pipeline"``.

        description : str | None, default ``None``
            Longer explanation of what the view shows. Helps future retrieval
            via ``list_views``.

        bindings : list[PrimitiveBinding] | None, default ``None``
            Live queries, each with an ``alias`` the canvas reads by. Every
            binding is dry-run before the canvas is stored, so a bad filter, an
            unknown column, or a table that does not exist yet fails here rather
            than in front of the user.

            A canvas can only display data that lives in a table. For connected
            apps that means storing it first — see "Data from connected apps".

        props : dict | None, default ``None``
            Static values, JSON-serialisable. Use for anything that needed
            reasoning to produce.

        actions : list[CanvasAction] | None, default ``None``
            Operations the viewer may trigger. Each resolves to exactly one
            target and is verified to exist before the canvas is stored.

        destination : str | None, default ``None``
            Where to store it. ``None`` or ``"personal"`` for the private
            workspace, ``"team:<id>"`` for a shared one.

        visibility : str, default ``"private"``
            ``"private"``, ``"team"``, or ``"public_link"`` for anyone holding
            the URL.

        review : bool, default ``True``
            Whether to render and critique the canvas after building. Leave on:
            a canvas that compiles but throws on mount is only caught here.

        Returns
        -------
        CanvasResult
            ``token`` and ``url`` for the published canvas, plus:

            - ``build`` -- ``ok``, and on failure ``failed_stage`` (one of
              ``lint``, ``typecheck``, ``bundle``, ``render``) with
              ``diagnostics`` carrying the compiler's own messages
            - ``review`` -- ``screenshots`` of the rendered result, and any
              issues a look at it turned up
            - ``error`` -- set when the call failed outright; read this first

            On a build failure nothing is stored and ``token`` is empty.

        Raises
        ------
        ToolErrorException
            If ``destination`` is malformed, or names a team the assistant is
            not a member of.

        Examples
        --------
        A live task tracker. The binding re-runs on every view, so it never
        goes stale::

            result = primitives.canvas.create_view(
                tsx='''
                import { Canvas, Stack, Section, KpiRow, Table } from "@unity/canvas-kit";

                export default function Tracker({ canvas }) {
                  const tasks = canvas.data.tasks ?? [];
                  return (
                    <Canvas>
                      <Stack gap="lg">
                        <KpiRow items={[
                          { label: "Open", value: tasks.length },
                        ]} />
                        <Section title="Pending">
                          <Table
                            columns={[
                              { key: "name", header: "Task" },
                              { key: "due", header: "Due" },
                            ]}
                            rows={tasks}
                          />
                        </Section>
                      </Stack>
                    </Canvas>
                  );
                }
                ''',
                title="Pending tasks",
                bindings=[
                    PrimitiveBinding(
                        alias="tasks",
                        manager="tasks",
                        table="Tasks",
                        args={"operation": "filter",
                              "filter": "status != 'done'",
                              "order_by": "due",
                              "limit": 100},
                    ),
                ],
            )

        A dashboard over two connected apps. Each app's data is stored first,
        which is what lets the canvas read both -- and what would let a single
        query join them::

            # Store, on a schedule, well before the canvas is built.
            issues = await primitives.integrations.github.list_issues(state="open")
            await primitives.data.ingest(issues, "Data/GitHubIssues")
            deals = await primitives.integrations.hubspot.list_deals()
            await primitives.data.ingest(deals, "Data/HubSpotDeals")

            result = primitives.canvas.create_view(
                tsx='''
                import { Canvas, Grid, Card, CardHeader, CardTitle,
                         CardContent, BarChart, Table } from "@unity/canvas-kit";

                export default function Delivery({ canvas }) {
                  return (
                    <Canvas>
                      <Grid columns={2}>
                        <Card>
                          <CardHeader><CardTitle>Open issues</CardTitle></CardHeader>
                          <CardContent>
                            <BarChart data={canvas.data.issues ?? []}
                                      x="repo" y="count" />
                          </CardContent>
                        </Card>
                        <Card>
                          <CardHeader><CardTitle>Pipeline</CardTitle></CardHeader>
                          <CardContent>
                            <Table
                              columns={[
                                { key: "name", header: "Deal" },
                                { key: "amount", header: "Amount", numeric: true },
                              ]}
                              rows={canvas.data.deals ?? []}
                            />
                          </CardContent>
                        </Card>
                      </Grid>
                    </Canvas>
                  );
                }
                ''',
                title="Delivery and pipeline",
                bindings=[
                    PrimitiveBinding(
                        alias="issues", manager="data", table="Data/GitHubIssues",
                        args={"operation": "filter", "limit": 200},
                    ),
                    PrimitiveBinding(
                        alias="deals", manager="data", table="Data/HubSpotDeals",
                        args={"operation": "filter", "order_by": "amount",
                              "descending": True, "limit": 50},
                    ),
                ],
            )

        A canvas that does something. The form is rendered from the schema, and
        the arguments are checked server-side before the function runs::

            result = primitives.canvas.create_view(
                tsx='''
                import { Canvas, Section, ActionForm } from "@unity/canvas-kit";

                export default function BulkMail({ canvas }) {
                  return (
                    <Canvas>
                      <Section title="Send an update">
                        <ActionForm action="bulk_send" canvas={canvas} />
                      </Section>
                    </Canvas>
                  );
                }
                ''',
                title="Bulk email",
                actions=[
                    CanvasAction(
                        name="bulk_send",
                        label="Send to everyone listed",
                        function_name="send_bulk_email",
                        input_schema={
                            "type": "object",
                            "required": ["recipients", "subject", "body"],
                            "properties": {
                                "recipients": {
                                    "type": "array",
                                    "maxItems": 200,
                                    "items": {"type": "string", "format": "email"},
                                },
                                "subject": {"type": "string", "maxLength": 200},
                                "body": {"type": "string", "maxLength": 10000},
                            },
                        },
                        destructive=True,
                        confirm="This sends real email to everyone listed.",
                    ),
                ],
            )

        Anti-patterns
        -------------
        - **Do not embed query results in the source.** Interpolating rows into
          the TSX produces a snapshot that is wrong by the time anyone opens it.
          Declare a binding.
        - **Do not import anything beyond react and the kit.** There is no
          bundler at view time and no network in the frame; the import fails.
        - **Do not write colours.** Hex, ``rgb()`` and colour utility classes
          are rejected by the linter, and would be wrong in one of the two
          themes even if they were not.
        - **Do not put a language-model call behind a binding.** Bindings are
          queries. Compute the answer once and pass it in ``props``.
        - **Do not call a connected app from the canvas source.** There is no
          network in the frame, so it cannot work. Store the data in a table
          first and bind to that.
        - **Do not paste app data into ``props``.** That is a snapshot with no
          refresh path, and it cannot be joined against a second app. Store it
          in a table and schedule the refresh.
        - **Do not describe an action vaguely.** ``label`` is what the viewer
          reads before consenting, so "Send to everyone listed" beats "Submit".
        - **Do not skip ``review``** to save time. It is the only check that
          catches a canvas which compiles and then throws on mount.

        Notes
        -----
        - The canvas is stored only if every gate passes.
        - ``token`` and ``url`` are stable across later edits, so a URL already
          shared keeps working.
        - Bindings are stored with their context paths already resolved, and
          only the alias travels to the browser.

        See Also
        --------
        update_view : Revise an existing canvas.
        refresh_props : Recompute static values without recompiling.
        preview : Re-render and re-inspect without changing anything.
        """
        raise NotImplementedError

    @abstractmethod
    def update_view(
        self,
        token: str,
        *,
        tsx: Optional[str] = None,
        title: Optional[str] = None,
        description: Optional[str] = None,
        bindings: Optional[List[PrimitiveBinding]] = None,
        props: Optional[Dict[str, Any]] = None,
        actions: Optional[List[CanvasAction]] = None,
        visibility: Optional[str] = None,
        review: bool = True,
    ) -> CanvasResult:
        """
        Revise an existing canvas in place.

        Use this for every change after the first version -- responding to "make
        the chart bigger", adding a column, fixing something the review pass
        turned up. The token and URL are preserved, so anywhere the canvas has
        already been shared keeps working.

        Partial by design: omitted parameters are left as they are. Passing
        ``tsx`` recompiles and re-reviews; passing only ``title`` does neither.

        Parameters
        ----------
        token : str
            Identifier of the canvas to revise.
        tsx : str | None, default ``None``
            Replacement source. Triggers the full build and review pass.
        title, description : str | None, default ``None``
            Replacement metadata.
        bindings : list[PrimitiveBinding] | None, default ``None``
            Replaces the binding set wholesale; each is re-validated. Pass an
            empty list to remove all bindings.
        props : dict | None, default ``None``
            Replaces the static values wholesale.
        actions : list[CanvasAction] | None, default ``None``
            Replaces the action set wholesale. Pass an empty list to remove all
            actions, which is the way to make a canvas read-only.
        visibility : str | None, default ``None``
            New visibility.
        review : bool, default ``True``
            Whether to re-render and critique. Only meaningful with ``tsx``.

        Returns
        -------
        CanvasResult
            Same shape as ``create_view``. On a build failure the **previous
            version stays published** and nothing is overwritten.

        Raises
        ------
        ToolErrorException
            If the token does not resolve.

        Examples
        --------
        Responding to feedback on the rendered result::

            result = primitives.canvas.update_view(token, tsx=revised_source)
            if not result.build.ok:
                print(result.build.diagnostics)

        Making a canvas read-only without touching its code::

            primitives.canvas.update_view(token, actions=[])

        Anti-patterns
        -------------
        - **Do not create a second canvas to make a change.** That leaves the
          user with two URLs and no idea which is current.
        - **Do not pass ``bindings`` expecting a merge.** The list replaces the
          existing set; include the ones being kept.

        Notes
        -----
        - A failed build leaves the stored canvas untouched.
        - ``updated_at`` is stamped on every successful call.

        See Also
        --------
        create_view : Author a new canvas.
        refresh_props : Update static values without recompiling.
        """
        raise NotImplementedError

    @abstractmethod
    def refresh_props(self, token: str, *, props: Dict[str, Any]) -> CanvasResult:
        """
        Replace a canvas's static values without recompiling it.

        The counterpart to bindings. Bindings re-run themselves on every view;
        values that needed reasoning cannot, so they are computed here and
        written into the canvas. Call this from a scheduled task to keep a
        summary, a digest or a distilled answer current.

        Parameters
        ----------
        token : str
            Identifier of the canvas to update.
        props : dict
            Replacement values, JSON-serialisable. Replaces the whole set.

        Returns
        -------
        CanvasResult
            With ``token`` and ``url``. No ``build`` or ``review``, since the
            code is untouched.

        Raises
        ------
        ToolErrorException
            If the token does not resolve.

        Examples
        --------
        A weekly refresh of a summary the canvas displays::

            summary = primitives.transcripts.ask("Summarise this week's calls")
            primitives.canvas.refresh_props(token, props={"summary": summary})

        Anti-patterns
        -------------
        - **Do not use this for data a binding could fetch.** Rows, counts and
          aggregates belong in bindings, which stay current without a scheduled
          job.
        - **Do not put large payloads here.** Props are stored on the record and
          sent with every view; a table of rows belongs in a binding.

        Notes
        -----
        - Cheap: no compile, no render, no review.

        See Also
        --------
        create_view : Where props are first set.
        update_view : For changing code, bindings or actions.
        """
        raise NotImplementedError

    # ──────────────────────────────────────────────────────────────────────────
    # Retrieval
    # ──────────────────────────────────────────────────────────────────────────

    @abstractmethod
    def get_view(self, token: str) -> Optional[CanvasViewRecord]:
        """
        Read one canvas in full, including its source.

        The only method that returns ``tsx_source``. Use it before revising a
        canvas, to edit from what is actually there rather than from memory.

        Parameters
        ----------
        token : str
            Identifier of the canvas.

        Returns
        -------
        CanvasViewRecord | None
            The full record -- source, bindings, props, build metadata,
            visibility, status -- or ``None`` if no such canvas is visible.

        Examples
        --------
        Edit an existing canvas from its current source::

            record = primitives.canvas.get_view(token)
            primitives.canvas.update_view(
                token,
                tsx=record.tsx_source.replace('height={200}', 'height={360}'),
            )

        Anti-patterns
        -------------
        - **Do not call this in a loop over ``list_views``.** It returns full
          source per canvas; filter the listing instead.

        Notes
        -----
        - Reads span the personal workspace and every team the assistant belongs
          to, so a token from a shared workspace resolves without extra work.

        See Also
        --------
        list_views : Discovery without source.
        """
        raise NotImplementedError

    @abstractmethod
    def list_views(
        self,
        *,
        filter: Optional[str] = None,
        limit: int = 50,
    ) -> List[CanvasViewRecord]:
        """
        List canvases without their source.

        The discovery method: use it to find what already exists before building
        something new, and to answer "what views do I have?". Source is omitted
        so the result stays small.

        Parameters
        ----------
        filter : str | None, default ``None``
            Filter expression over the stored fields, e.g.
            ``"status == 'published'"`` or ``"'sales' in title"``.
        limit : int, default ``50``
            Maximum number returned.

        Returns
        -------
        list[CanvasViewRecord]
            Matching records with ``tsx_source`` empty.

        Examples
        --------
        Check for an existing view before authoring a new one::

            existing = primitives.canvas.list_views(filter="'tasks' in title")
            if existing:
                primitives.canvas.update_view(existing[0].token, tsx=source)

        Anti-patterns
        -------------
        - **Do not filter in Python after fetching everything.** Push the
          predicate into ``filter``.

        Notes
        -----
        - Results span the personal workspace and every team workspace.

        See Also
        --------
        get_view : Full record including source.
        """
        raise NotImplementedError

    @abstractmethod
    def delete_view(self, token: str, *, destination: Optional[str] = None) -> bool:
        """
        Delete a canvas and everything attached to it.

        Removes the record, its actions and its invocation history, and revokes
        the token so the URL stops resolving. Irreversible.

        Parameters
        ----------
        token : str
            Identifier of the canvas to delete.
        destination : str | None, default ``None``
            Which workspace to delete from, when the canvas lives in a team one.

        Returns
        -------
        bool
            ``True`` if a canvas was deleted or already absent. Idempotent.

        Examples
        --------
        ::

            primitives.canvas.delete_view(token)

        Anti-patterns
        -------------
        - **Do not delete to make changes.** ``update_view`` preserves the URL;
          deleting breaks every link already shared.

        Notes
        -----
        - Stored functions that actions pointed at are left alone. They may be
          in use elsewhere.

        See Also
        --------
        update_view : Revise instead of replacing.
        """
        raise NotImplementedError

    # ──────────────────────────────────────────────────────────────────────────
    # Inspection
    # ──────────────────────────────────────────────────────────────────────────

    @abstractmethod
    def preview(self, token: str) -> ReviewReport:
        """
        Render a stored canvas and look at the result.

        Renders in a real browser, in both themes, and returns the screenshots
        together with a critique. Use it to check a canvas still renders after
        its underlying data has shifted, or to see the current state before
        acting on a change request.

        Parameters
        ----------
        token : str
            Identifier of the canvas to render.

        Returns
        -------
        ReviewReport
            ``rendered`` for whether it mounted at all, ``screenshots`` for the
            light and dark captures, ``verdict`` and ``issues`` for what a look
            at it turned up, and ``error`` when it failed to render.

        Examples
        --------
        Look before revising::

            report = primitives.canvas.preview(token)
            if not report.rendered:
                print(report.error)

        Anti-patterns
        -------------
        - **Do not call this in a polling loop.** It launches a browser.

        Notes
        -----
        - Renders against the binding data available at call time, so what comes
          back reflects the data as it is now.

        See Also
        --------
        create_view : Runs this automatically unless ``review=False``.
        """
        raise NotImplementedError

    @abstractmethod
    def run_invocation(
        self,
        invocation_id: int,
        *,
        token: str,
    ) -> CanvasInvocationRecord:
        """
        Execute one action run a viewer requested from a canvas.

        Called when a viewer presses a control, not by a plan. The request was
        already validated and recorded before it reached here, so this resolves
        the declared target, runs it, and records what happened.

        The arguments come from the stored run rather than from a caller, which is
        the whole reason the run is persisted: neither available execution path
        accepts a payload, so the record carries them.

        Parameters
        ----------
        invocation_id : int
            Identifier of the recorded run to execute.
        token : str
            Canvas the run belongs to. Runs are scoped to their canvas, so an
            identifier from one canvas cannot be executed through another.

        Returns
        -------
        CanvasInvocationRecord
            The completed run, with ``status`` ``succeeded`` or ``failed``,
            ``result`` or ``error``, and ``finished_at`` set.

        Raises
        ------
        ValueError
            If the run does not exist on this canvas, or the action it names is no
            longer declared.

        Examples
        --------
        Re-run a failed action after fixing its cause::

            failed = [
                run for run in primitives.canvas.list_invocations(token)
                if run.status == "failed"
            ]
            for run in failed:
                primitives.canvas.run_invocation(run.invocation_id, token=token)

        Anti-patterns
        -------------
        - **Do not call this to perform work of your own.** It executes what a
          viewer asked for, with their arguments. To do something yourself, call
          the function or task directly.
        - **Do not use it to retry a run that is still going.** A run already in
          flight is left alone; check ``status`` first.

        Notes
        -----
        A run that has already succeeded is returned unchanged rather than
        repeated. Re-running a completed send because a message was redelivered is
        the failure this prevents.

        See Also
        --------
        list_invocations : See what viewers have triggered and how it went.
        """
        raise NotImplementedError

    @abstractmethod
    def list_invocations(
        self,
        token: str,
        *,
        limit: int = 20,
    ) -> List[CanvasInvocationRecord]:
        """
        List recent action runs for a canvas.

        Shows what viewers have actually triggered and how it went. Use it to
        answer "did the send go out?" and to diagnose a failed action.

        Parameters
        ----------
        token : str
            Identifier of the canvas.
        limit : int, default ``20``
            Maximum number returned, newest first.

        Returns
        -------
        list[CanvasInvocationRecord]
            Each with ``action_name``, the ``args`` supplied, ``status``
            (``pending``, ``running``, ``succeeded``, ``failed``), ``result`` or
            ``error``, and timestamps.

        Examples
        --------
        Check whether anything failed::

            failed = [
                run for run in primitives.canvas.list_invocations(token)
                if run.status == "failed"
            ]

        Anti-patterns
        -------------
        - **Do not poll this to wait for completion.** Progress is streamed to
          the canvas already.

        Notes
        -----
        - Invocations are retained after the run so they remain an audit trail
          of who triggered what, with which arguments.

        See Also
        --------
        create_view : Where actions are declared.
        """
        raise NotImplementedError
