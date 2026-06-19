"""Google Drive browse channel: roots / children / item / search.

Read-only enumeration of a connected BYOD Google account's Drive, used by the
Console workspace file-access picker and by allowlist enforcement in the
assistant runtime. Single ``router``, admin-authed at the aggregator.
"""

from droid.gateway.channels.drive.views import router

__all__ = ["router"]
