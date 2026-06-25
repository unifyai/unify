"""Controlled Unity vs Hermes recurring-task artifact benchmark."""

from .arms import HERMES_ARM, UNITY_ARM, BenchmarkArm
from .fixtures import OUTPUT_CONTRACT, synthetic_email_batches
from .scoring import analyze_results, score_artifact

__all__ = [
    "BenchmarkArm",
    "HERMES_ARM",
    "OUTPUT_CONTRACT",
    "UNITY_ARM",
    "analyze_results",
    "score_artifact",
    "synthetic_email_batches",
]
