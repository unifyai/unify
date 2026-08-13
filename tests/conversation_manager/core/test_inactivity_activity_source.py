"""What counts as somebody needing the assistant.

An idle pod is retired by the inactivity timer, and that timer used to
advance on *any* message off the bus — including the system's own chatter.
One recurring internal event was enough to keep a pod alive indefinitely:
it never idled out, so it never picked up a newly deployed image, and a
staging pod served seven hours of requests from a five-hour-old build while
three deploys went by.

The distinction is presence, not verbosity. `loggable` asks whether an
event is worth writing down, which is a different question: editing an
assistant or reading the chat is somebody being there, even when neither is
worth tracing.
"""

from unify.conversation_manager.events import (
    AssistantUpdateEvent,
    Error,
    Event,
    GetBusEventsResponse,
    GetChatHistory,
    InitializationComplete,
    IntegrationToolsSyncFailed,
    OpenSlowBrainTurn,
    Ping,
    StartupEvent,
)


def test_only_provably_internal_events_stop_counting_as_presence():
    # A keepalive says the process is up, never that anyone wants anything.
    assert Ping.counts_as_activity is False
    # The pod telling itself it booted, and a reply to its own query.
    assert InitializationComplete.counts_as_activity is False
    assert GetBusEventsResponse.counts_as_activity is False


def test_a_pod_cannot_hold_itself_open_by_failing():
    """Failure output is the pod talking to itself, however loud.

    Whatever these were a response to already counted when it arrived -- a
    message, a due task, an assignment -- so discounting them drops nothing
    a person did. Counting them let a pod with no managers stay alive on its
    own noise: nine integration syncs failed at boot and were its last
    recorded activity for forty-five minutes, and later a send it could not
    complete was retried seven times and reset the clock each time.
    """
    assert Error.counts_as_activity is False
    assert IntegrationToolsSyncFailed.counts_as_activity is False


def test_a_person_doing_something_still_keeps_the_pod_alive():
    # These are all `loggable = False`, so gating on that flag would have
    # treated them as noise — someone editing their assistant config, or
    # reading the conversation, would have stopped holding the pod open.
    for event_class in (AssistantUpdateEvent, GetChatHistory, OpenSlowBrainTurn):
        assert event_class.loggable is False
        assert (
            event_class.counts_as_activity is True
        ), f"{event_class.__name__} is somebody doing something"


def test_assignment_starts_the_clock_for_a_pod_that_waited():
    """A pod can wait in the warm pool for hours before it is assigned.

    Nothing it sees while unassigned counts as presence, so its clock is
    stale by the time somebody arrives. The assignment itself is what
    restarts it: were `StartupEvent` to stop counting, a pod would take its
    first message already past the timeout and shut down under the session
    it had only just been handed.
    """
    assert StartupEvent.counts_as_activity is True


def test_the_default_is_to_count():
    """A new event counts until someone proves it should not.

    The safe direction is keeping a live pod alive: wrongly counting costs
    an idle pod some extra minutes, wrongly discounting drops a session
    somebody is in the middle of.

    That asymmetry survives the pod that stayed up for three hours unable to
    serve, because what was wrong there was not the tagging: a pod that
    cannot serve now retires on `unserviceable_reason` whatever its clock
    says. Inverting this default would trade a bounded cost for the one
    failure this flag can actually cause.
    """
    assert Event.counts_as_activity is True

    class _NewlyAddedEvent(Event):
        pass

    assert _NewlyAddedEvent.counts_as_activity is True
