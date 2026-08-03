"""IngestionManager: store data and files from any source, observably.

Intentionally exports nothing. ``unify.settings`` imports this package's settings
module, so re-exporting the base class or the manager here would pull DataManager
and the file pipeline into that import and close a cycle back onto settings.
Import from the submodules directly.
"""
