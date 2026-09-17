"""Identity verification for provisioned contexts.

A context created without unique keys or auto-counting stays bare: the store
cannot retrofit that configuration later, and rows inserted into it receive
no identity column, which is silent corruption. These tests pin the store
shape and prove `create_context_checked` refuses to treat such a context as
provisioned, and that a row write never creates a context implicitly.
"""

from __future__ import annotations

import pytest
from unify import db
from tests.helpers import _handle_project
from unify.common.context_store import (
    ContextIdentityError,
    create_context_checked,
)
from unify.common.log_utils import log as unity_log


@_handle_project
def test_write_to_missing_context_is_refused():
    base = db.get_active_context()["write"]
    name = f"{base}/MissingTable"

    with pytest.raises(db.NotFound):
        db.log(context=name, new=True, payload="row")


@_handle_project
def test_bare_context_assigns_no_identity():
    base = db.get_active_context()["write"]
    name = f"{base}/BareTable"

    db.create_context(name)
    db.log(context=name, new=True, payload="row")

    live = db.get_context(name)
    assert not live.get("unique_keys")
    assert not live.get("auto_counting")

    rows = db.get_logs(context=name)
    assert rows and "row_id" not in rows[0].entries

    with pytest.raises(ContextIdentityError):
        create_context_checked(
            name,
            unique_keys={"row_id": "int"},
            auto_counting={"row_id": None},
        )


@_handle_project
def test_configured_context_passes_verification_and_assigns_ids():
    base = db.get_active_context()["write"]
    name = f"{base}/ConfiguredTable"

    create_context_checked(
        name,
        unique_keys={"row_id": "int"},
        auto_counting={"row_id": None},
    )
    # Re-ensuring an existing, correctly configured context stays idempotent.
    create_context_checked(
        name,
        unique_keys={"row_id": "int"},
        auto_counting={"row_id": None},
    )

    log = unity_log(context=name, new=True, payload="row")
    assert log.entries["row_id"] == 0
