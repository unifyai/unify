"""Teams channel: chats, channels, watch subscriptions, meeting creation.

Mirrors ``communication/teams/views.py`` and
``communication/teams/create_meeting.py``. Single ``router`` (12
endpoints, all admin-authed).

Largest channel migration yet -- 1139 LOC of routes + 203 LOC of
meeting-creation helpers. Built on top of the MS Graph + Orchestra
helpers in ``droid.gateway.common`` (landed alongside outlook in
Phase B.4.prep).
"""

from droid.gateway.channels.teams.views import router

__all__ = ["router"]
