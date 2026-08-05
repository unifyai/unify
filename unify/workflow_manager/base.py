from __future__ import annotations

from abc import abstractmethod
from typing import Any, Dict, Optional

from ..common.state_managers import BaseStateManager
from ..manager_registry import SingletonABCMeta


class BaseWorkflowManager(BaseStateManager, metaclass=SingletonABCMeta):
    """
    Public contract for a catalogue of installable workflows.

    Overview
    --------
    A workflow is a bundle: one named, versioned package that plants
    content across several surfaces at once — procedures, durable tasks,
    typed claims, functions — so that a job the assistant does regularly
    arrives set up rather than assembled by hand. "Draft my email replies
    every morning" is a workflow: it is not one task but a mailbox pull, a
    contact backfill, a set of procedures for tone and triage, and a
    recurring task that depends on all of them.

    Installing a workflow writes its content into the ordinary surfaces.
    There is no separate place workflow content lives and no second
    lookup path: procedures a workflow planted are found by the same
    search as any other, and tasks it planted run like any other. What
    installation adds is provenance — every planted row records which
    workflow put it there — so a workflow can later be updated or removed
    as a unit.

    Upkeep
    ------
    Installed content is kept matched to the catalogue: reinstalling or
    reconciling brings the planted rows to the current bundle version,
    overwriting local edits to them and pruning entries the bundle
    dropped. Content the user wants to diverge from the workflow belongs
    in a copy of their own, alongside the planted original.

    Requirements
    ------------
    A bundle declares which integrations its jobs need. Installing with a
    requirement unmet still plants everything, but the workflow's
    recurring jobs stay disarmed — planted and visible, firing nothing —
    until the connection lands. Reinstalling after connecting arms them.

    Parameters
    ----------
    A bundle may declare install-time settings — which mailbox to read,
    which calendar to write to. These are recorded on the installation
    and read by the workflow's own tasks when they run. They are never
    written into the planted rows: two people installing one workflow get
    byte-identical content and differ only in their settings.

    Data Model
    ----------
    Installations conform to
    ``unify.workflow_manager.types.workflow.WorkflowInstallation``.
    Implementations may return instances of this model or JSON-serialisable
    dictionaries matching its schema.

    Shared-Space Semantics
    ----------------------
    Reads cover personal installations plus every accessible shared team
    installation. Writes default to personal. Installing to a team plants
    the workflow's content for every member of that team, so it carries
    wider consequences than a personal install.
    """

    _as_caller_description: str = "the WorkflowManager, managing installed workflows"

    @abstractmethod
    def list_workflows(
        self,
        *,
        installed: Optional[bool] = None,
        offset: int = 0,
        limit: int = 100,
    ) -> Dict[str, Any]:
        """
        List workflows, installed and available.

        Use this before installing anything, both to check whether the
        capability the user is asking for is already set up and to find
        the exact ``slug`` to install.

        Parameters
        ----------
        installed:
            ``True`` for installed workflows only, ``False`` for available
            but not installed, ``None`` (default) for both.
        offset:
            Number of entries to skip.
        limit:
            Maximum entries to return.

        Returns
        -------
        A dict with ``workflows`` (a list of entries, each carrying at
        least ``slug``, ``name``, ``description``, and ``installed``) and
        ``total``.
        """
        raise NotImplementedError

    @abstractmethod
    def install_workflow(
        self,
        *,
        slug: str,
        params: Optional[Dict[str, Any]] = None,
        destination: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Install a workflow, planting its content across the surfaces it covers.

        This writes real content the user will see: new procedures, new
        recurring tasks that will start firing on their schedule, new
        typed claims. Confirm with the user before installing anything
        they did not explicitly ask for, and tell them what it set up
        afterwards — particularly any recurring task, since that will act
        on its own later.

        Installing a workflow that is already installed reconciles it to
        the current bundle instead of duplicating it — a repeat install is
        also how a partial install is retried and how jobs held on a
        missing connection are armed once it lands.

        Parameters
        ----------
        slug:
            Bundle identifier from ``list_workflows``.
        params:
            Install-time settings declared by the bundle, e.g. which
            mailbox to work from. Ask the user for any the bundle requires
            rather than guessing.
        destination:
            ``None`` for personal, or ``team:<id>`` to install for a whole
            team. A team install plants content for every member.

        Returns
        -------
        A dict with the resulting installation and a ``planted`` summary
        of what was written per surface. When a declared integration is
        not connected it also carries ``connect_required`` naming what is
        missing; the workflow's jobs stay disarmed until then.
        """
        raise NotImplementedError

    @abstractmethod
    def reconcile_installed(
        self,
        *,
        slugs: Optional[list[str]] = None,
        destination: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Bring installed workflows back in line with the current catalogue.

        Use this to apply a newer version of a workflow the user already
        has, or to re-check all of them at once — it reconciles each
        installation's content to its bundle and re-arms or holds its
        jobs according to what is currently connected. Settings recorded
        on each installation are kept.

        Parameters
        ----------
        slugs:
            Limit the pass to these workflows; ``None`` reconciles every
            installed one.
        destination:
            ``None`` for personal installations, or ``team:<id>``.

        Returns
        -------
        A dict with ``reconciled`` (per-slug install results) and, when
        some installations no longer have a bundle in the catalogue,
        ``orphaned`` naming them; those are left untouched.
        """
        raise NotImplementedError

    @abstractmethod
    def get_installation_params(
        self,
        *,
        slug: str,
        destination: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Read the settings an installation was configured with.

        This is how a workflow's own recurring tasks resolve their
        per-install settings at run time — which mailbox to read, which
        calendar to write — since settings are never written into the
        planted content itself.

        Parameters
        ----------
        slug:
            Bundle identifier of the installation.
        destination:
            ``None`` for personal, or ``team:<id>``.

        Returns
        -------
        The installation's settings as a dict; an error result if the
        workflow is not installed at that destination.
        """
        raise NotImplementedError

    @abstractmethod
    def uninstall_workflow(
        self,
        *,
        slug: str,
        destination: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Remove a workflow and the content it planted.

        This deletes the rows the workflow put in place, including any
        recurring tasks it created, so those stop firing. Content the user
        added themselves is untouched, and so is anything another workflow
        planted. Confirm before calling: the user may have spent time
        editing the planted rows, and those edits go too.

        Parameters
        ----------
        slug:
            Bundle identifier of the installation to remove.
        destination:
            ``None`` for personal, or ``team:<id>``.

        Returns
        -------
        A dict with a ``removed`` summary of what was deleted per surface.
        """
        raise NotImplementedError

    @abstractmethod
    def get_workflow(self, *, slug: str) -> Dict[str, Any]:
        """
        Read one workflow's full record: its description, whether it is
        installed and at which version, its current settings, its
        requirements and their connection state, and which surfaces it
        covers.

        Use this to answer questions about what a workflow does or what a
        given installation is currently set to, and to read back the
        settings before changing one.

        Parameters
        ----------
        slug:
            Bundle identifier.

        Returns
        -------
        The workflow record, or a not-found result if no such bundle exists.
        """
        raise NotImplementedError
