import json
import os
import threading
from typing import Any, Dict, List, Optional

from unify.utils.caching._base_cache import BaseCache


class LocalCache(BaseCache):
    _cache: Optional[Dict] = None
    _cache_dir: str = (
        os.environ["UNIFY_CACHE_DIR"]
        if "UNIFY_CACHE_DIR" in os.environ
        else os.getcwd()
    )
    _cache_lock: threading.Lock = threading.Lock()
    _cache_fname: str = ".cache.json"
    _enabled: bool = False

    @classmethod
    def set_filename(cls, filename: str) -> None:
        cls._cache_fname = filename
        cls._cache = None  # Force a reload of the cache

    @classmethod
    def get_filename(cls) -> str:
        return cls._cache_fname

    @classmethod
    def get_cache_filepath(cls, filename: str = None) -> str:
        if filename is None:
            filename = cls.get_filename()

        return os.path.join(cls._cache_dir, filename)

    @classmethod
    def is_enabled(cls) -> bool:
        return cls._enabled

    @classmethod
    def update_entry(
        cls,
        *,
        key: str,
        value: Any,
        res_types: Optional[Dict[str, Any]] = None,
    ) -> None:
        if res_types:
            cls._cache[key + "_res_types"] = res_types
        cls._cache[key] = value

    @classmethod
    def write(cls, filename: str = None) -> None:
        cache_fpath = cls.get_cache_filepath(filename)
        with open(cache_fpath, "w") as outfile:
            json.dump(cls._cache, outfile)

    @classmethod
    def create_or_load(cls, filename: str = None) -> None:
        cache_fpath = cls.get_cache_filepath(filename)
        if cls._cache is None:
            if not os.path.exists(cache_fpath):
                with open(cache_fpath, "w") as outfile:
                    json.dump({}, outfile)

            # Check if file is empty or contains invalid JSON
            try:
                with open(cache_fpath) as outfile:
                    content = outfile.read().strip()
                    if not content:
                        # File is empty, initialize with empty dict
                        with open(cache_fpath, "w") as outfile:
                            json.dump({}, outfile)
                        cls._cache = {}
                    else:
                        # Try to parse the JSON
                        cls._cache = json.loads(content)
            except json.JSONDecodeError:
                # File contains invalid JSON, reinitialize
                with open(cache_fpath, "w") as outfile:
                    json.dump({}, outfile)
                cls._cache = {}

    @classmethod
    def get_keys(cls) -> List[str]:
        return list(cls._cache.keys())

    @classmethod
    def get_entry(cls, cache_key: str) -> Optional[Any]:
        return cls._cache.get(cache_key), cls._cache.get(f"{cache_key}_res_types")

    @classmethod
    def key_exists(cls, cache_key: str) -> bool:
        return cache_key in cls._cache

    @classmethod
    def delete(cls, cache_key: str) -> None:
        del cls._cache[cache_key]
        del cls._cache[cache_key + "_res_types"]
