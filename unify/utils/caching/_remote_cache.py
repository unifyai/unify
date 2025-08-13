import json
from typing import Any, Dict, List, Optional

from unify.utils.caching._base_cache import BaseCache


class RemoteCache(BaseCache):
    _remote_context = "UNIFY_CACHE"

    @staticmethod
    def _get_filter_expr(cache_key: str):
        return f"key == {json.dumps(cache_key)}"

    @classmethod
    def set_filename(cls, filename: str) -> None:
        cls._remote_context = filename

    @classmethod
    def get_filename(cls) -> str:
        return cls._remote_context

    @classmethod
    def update_entry(
        cls,
        *,
        key: str,
        value: Any,
        res_types: Optional[Dict[str, Any]] = None,
    ) -> None:
        from unify import delete_logs, get_logs, log

        logs = get_logs(
            context=cls._remote_context,
            filter=cls._get_filter_expr(key),
            return_ids_only=True,
        )

        if len(logs) > 0:
            delete_logs(logs=logs, context=cls._remote_context)

        entries = {"value": value}
        if res_types:
            entries["res_types"] = json.dumps(res_types)
        log(key=key, context=cls._remote_context, **entries)

    @classmethod
    def write(cls, filename: str = None) -> None:
        # Do nothing
        pass

    @classmethod
    def create_or_load(cls, filename: str = None) -> None:
        from unify import create_context, get_contexts

        if cls._remote_context not in get_contexts():
            create_context(cls._remote_context)

    @classmethod
    def get_keys(cls) -> List[str]:
        from unify import get_logs

        logs = get_logs(context=cls._remote_context)
        return [log.entries["key"] for log in logs]

    @classmethod
    def get_entry(cls, cache_key: str) -> Optional[Any]:
        from unify import get_logs

        value = res_types = None
        logs = get_logs(
            context=cls._remote_context,
            filter=cls._get_filter_expr(cache_key),
        )
        if len(logs) > 0:
            entry = logs[0].entries
            value = json.loads(entry["value"])
            if "res_types" in entry:
                res_types = json.loads(entry["res_types"])
        return value, res_types

    @classmethod
    def key_exists(cls, cache_key: str) -> bool:
        from unify import get_logs

        logs = get_logs(
            context=cls._remote_context,
            filter=cls._get_filter_expr(cache_key),
            return_ids_only=True,
        )
        return len(logs) > 0

    @classmethod
    def delete(cls, cache_key: str) -> None:
        from unify import delete_logs, get_logs

        logs = get_logs(
            context=cls._remote_context,
            filter=cls._get_filter_expr(cache_key),
            return_ids_only=True,
        )
        delete_logs(context=cls._remote_context, logs=logs)
