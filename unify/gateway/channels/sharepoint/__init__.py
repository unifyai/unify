"""SharePoint channel: sites / drives / files / folders / search via MS Graph.

Mirrors ``communication/sharepoint/views.py``. Single ``router``,
11 endpoints all admin-authed. Read-heavy compared to the other MS
Graph channels (mostly enumerations and downloads); the only
write paths are upload, create-folder, and delete.

Simplest channel migration so far in terms of translation: the only
``communication``-side import is ``get_graph_client``, which is
already in ``unify.gateway.common.graph`` (promoted alongside the
other Graph helpers in Phase B.4.prep).
"""

from unify.gateway.channels.sharepoint.views import router

__all__ = ["router"]
