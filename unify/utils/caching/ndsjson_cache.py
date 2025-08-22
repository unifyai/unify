import json
import os
from typing import Any, List, Optional


def _load_ndjson_cache(filepath: str):
    cache = {}
    if not os.path.exists(filepath):
        return cache

    with open(filepath, "r") as f:
        for line in f:
            item = json.loads(line)
            cache[item["key"]] = {
                "value": item["value"],
                "res_types": item["res_types"],
            }

    return cache


def _write_to_ndjson_cache(
    filepath: str,
    key: str,
    value: Any,
    res_types: Optional[List[str]] = None,
):
    with open(filepath, "a") as f:
        f.write(
            json.dumps(
                {
                    "key": key,
                    "value": value,
                    "res_types": res_types,
                },
            )
            + "\n",
        )
