"""Every value the dispatch path sends must be one the model accepts.

Three separate outages came from the same shape in one week. A field is typed as
a ``Literal`` enumerating the values known when it was written; a new caller
appears and sends something outside the set; nothing catches it until a live
dispatch fails validation. In order:

* ``execution_target="hosted"`` -- not a member, so every brokered publish was
  refused before parsing;
* environment detection answering ``"development"`` -- same field, would have
  broken every self-host deployment the moment the first was fixed;
* ``source="ingestion_manager"`` -- the assistant's own ingestion path, the one
  callers actually reach, missing from the manifest's origin list.

Each cost a full round trip through deploy-and-retest to discover, and each
looked like a platform outage rather than a bad field. These tests pin the
contract from the sending side, so a value the senders use but the models reject
fails here instead of in production.
"""

from __future__ import annotations

from typing import get_args

import pytest

from unify.common.pipeline.deployment.types import (
    DeploymentExecutionTarget,
    DeploymentRunMode,
    DispatchManifest,
)


class TestTheManifestAcceptsEveryRealOrigin:
    @pytest.mark.parametrize(
        "origin",
        [
            "dispatch_pipeline",  # the batch script
            "pipeline_control",  # the operator CLI
            "ingestion_manager",  # the assistant's own ingestion path
        ],
    )
    def test_a_known_publisher_is_accepted(self, origin):
        assert DispatchManifest(dispatch_id="d1", source=origin).source == origin

    def test_an_unknown_origin_is_still_rejected(self):
        # The set is a contract, not a formality: an unrecognised origin should
        # fail loudly here rather than be written into a manifest nobody can
        # attribute later.
        with pytest.raises(Exception):
            DispatchManifest(dispatch_id="d1", source="somewhere_new")

    def test_the_assistant_path_is_named_in_the_type(self):
        # Guards the specific regression: the brokered path publishing as
        # ingestion_manager is the one users reach.
        assert "ingestion_manager" in get_args(
            DispatchManifest.model_fields["source"].annotation,
        )


class TestExecutionTargetsAreEnvironmentAgnostic:
    """The deploy layer maps environments onto these; both sides must agree."""

    def test_the_target_set_is_what_the_mapper_targets(self):
        # unify-deploy's _execution_target maps every environment it can detect
        # into this set. If a value is added or renamed here without updating
        # that mapping, dispatches fail validation at runtime -- which is
        # exactly how "hosted" and "development" reached production.
        assert set(get_args(DeploymentExecutionTarget)) == {
            "local",
            "local_with_gcp",
            "staging",
            "production",
        }

    @pytest.mark.parametrize("bad", ["hosted", "development", "", "cloud"])
    def test_values_that_have_been_sent_by_mistake_are_not_members(self, bad):
        # Each of these was sent in earnest at some point. Keeping them named
        # here makes a future re-introduction a failing test rather than a
        # failed dispatch.
        assert bad not in get_args(DeploymentExecutionTarget)


class TestRunModesCoverBothIngestionShapes:
    def test_file_and_data_manager_are_both_addressable(self):
        # publish_submit picks between these from the ingestion mode; a missing
        # member would fail the same way as the fields above.
        modes = get_args(DeploymentRunMode)
        assert "file_manager" in modes
        assert "data_manager" in modes
