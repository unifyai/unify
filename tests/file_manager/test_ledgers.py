"""Run ledger tests.

Validates JSONL serialization round-trips for the run, file and stage manifests
that make a pipeline run inspectable after the fact.
"""

from __future__ import annotations

import json


from unify.common.pipeline import (
    JsonlRunLedger,
    PipelineFileManifest,
    PipelineRunManifest,
    PipelineStageManifest,
)


class TestRunLedgerJSONL:

    def test_writes_and_reads_stage_manifest(self, tmp_path):
        ledger_path = tmp_path / "run.jsonl"
        ledger = JsonlRunLedger(path=ledger_path)

        manifest = PipelineStageManifest(
            run_id="run-001",
            file_path="repairs.csv",
            stage_name="parse",
            status="success",
            duration_ms=1234.5,
        )
        ledger.write(manifest)
        ledger.flush()
        ledger.close()

        lines = ledger_path.read_text(encoding="utf-8").strip().split("\n")
        assert len(lines) == 1
        loaded = json.loads(lines[0])
        assert loaded["record_type"] == "stage"
        assert loaded["run_id"] == "run-001"
        assert loaded["stage_name"] == "parse"
        assert loaded["status"] == "success"

    def test_writes_file_and_run_manifests(self, tmp_path):
        ledger_path = tmp_path / "run.jsonl"
        ledger = JsonlRunLedger(path=ledger_path)

        ledger.write(
            PipelineFileManifest(
                run_id="run-002",
                file_path="data.xlsx",
                status="success",
                total_duration_ms=5000.0,
            ),
        )
        ledger.write(
            PipelineRunManifest(
                run_id="run-002",
                status="completed",
                file_count=1,
                success_count=1,
            ),
        )
        ledger.close()

        lines = ledger_path.read_text(encoding="utf-8").strip().split("\n")
        assert len(lines) == 2
        assert json.loads(lines[0])["record_type"] == "file"
        assert json.loads(lines[1])["record_type"] == "run"
