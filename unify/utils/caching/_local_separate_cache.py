import json
import os
from typing import Any, Dict, List, Optional

from unify.utils.caching._base_cache import BaseCache


class LocalSeparateCache(BaseCache):
    _cache_read: Optional[Dict] = None
    _cache_write: Optional[Dict] = None
    _cache_dir: str = (
        os.environ["UNIFY_CACHE_DIR"]
        if "UNIFY_CACHE_DIR" in os.environ
        else os.getcwd()
    )
    _cache_fname_read: str = ".cache.json"
    _cache_fname_write: str = ".cache_write.json"
    _enabled: bool = False

    @classmethod
    def set_filename(cls, filename: str) -> None:
        cls._cache_fname_read = filename + "_read"
        cls._cache_fname_write = filename + "_write"
        cls._cache_read = None  # Force a reload of the cache
        cls._cache_write = None  # Force a reload of the cache

    @classmethod
    def get_filename(cls) -> str:
        return cls._cache_fname_read

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
            cls._cache_write[key + "_res_types"] = res_types
        cls._cache_write[key] = value
        cls.write()

    @classmethod
    def write(cls, filename: str = None) -> None:
        cache_fpath = cls._cache_fname_write
        with open(cache_fpath, "w") as outfile:
            json.dump(cls._cache_write, outfile)

    @classmethod
    def create_or_load(cls, filename: str = None) -> None:
        # Always create the write cache
        if cls._cache_write is None:
            cls._cache_write = {}
            filepath = cls._cache_fname_write
            with open(filepath, "w") as outfile:
                json.dump({}, outfile)

        if cls._cache_read is None:
            filepath = cls._cache_fname_read
            if not os.path.exists(filepath):
                with open(filepath, "w") as outfile:
                    json.dump({}, outfile)

            # Check if file is empty or contains invalid JSON
            try:
                with open(filepath) as outfile:
                    content = outfile.read().strip()
                    if not content:
                        # File is empty, initialize with empty dict
                        with open(filepath, "w") as outfile:
                            json.dump({}, outfile)
                        cls._cache_read = {}
                    else:
                        # Try to parse the JSON
                        cls._cache_read = json.loads(content)
            except json.JSONDecodeError:
                # File contains invalid JSON, reinitialize
                with open(filepath, "w") as outfile:
                    json.dump({}, outfile)
                cls._cache_read = {}

    @classmethod
    def get_keys(cls) -> List[str]:
        return list(cls._cache_read.keys()) + list(cls._cache_write.keys())

    @classmethod
    def get_entry(cls, cache_key: str) -> Optional[Any]:
        # First check the write cache
        value = cls._cache_write.get(cache_key)
        if value is not None:
            return json.loads(value), cls._cache_write.get(f"{cache_key}_res_types")

        # If not found, check the read cache
        value = cls._cache_read.get(cache_key)
        value = json.loads(value) if value else None
        if value is not None:
            cls.update_entry(
                key=cache_key,
                value=value,
                res_types=cls._cache_read.get(f"{cache_key}_res_types"),
            )
        return value, cls._cache_read.get(f"{cache_key}_res_types")

    @classmethod
    def key_exists(cls, cache_key: str) -> bool:
        return cache_key in cls._cache_read or cache_key in cls._cache_write

    @classmethod
    def delete(cls, cache_key: str) -> None:
        del cls._cache_write[cache_key]
        del cls._cache_write[cache_key + "_res_types"]
        del cls._cache_read[cache_key]
        del cls._cache_read[cache_key + "_res_types"]
