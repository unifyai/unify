"""Compatibility wrappers for deploy-specific metrics backends."""

from unify.deploy_runtime import init_metrics, shutdown_metrics

__all__ = ["init_metrics", "shutdown_metrics"]
