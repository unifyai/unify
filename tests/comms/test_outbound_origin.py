from unify.comms.outbound_origin import (
    mark_slow_brain_direct_outbound,
    reset_slow_brain_direct_outbound,
    slow_brain_direct_outbound_active,
)


def test_slow_brain_direct_outbound_defaults_false():
    assert slow_brain_direct_outbound_active() is False


def test_mark_and_reset_restores_default():
    token = mark_slow_brain_direct_outbound()
    assert slow_brain_direct_outbound_active() is True
    reset_slow_brain_direct_outbound(token)
    assert slow_brain_direct_outbound_active() is False


def test_nested_mark_reset_restores_outer_scope():
    outer = mark_slow_brain_direct_outbound()
    assert slow_brain_direct_outbound_active() is True
    inner = mark_slow_brain_direct_outbound()
    assert slow_brain_direct_outbound_active() is True
    reset_slow_brain_direct_outbound(inner)
    assert slow_brain_direct_outbound_active() is True
    reset_slow_brain_direct_outbound(outer)
    assert slow_brain_direct_outbound_active() is False
