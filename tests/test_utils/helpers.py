import os

import unify
from unify.utils.caching._local_cache import LocalCache


class _CacheHandler:
    def __init__(self, fname=".test_cache.json"):
        self._old_cache_fpath = LocalCache.get_cache_filepath(fname)
        self._fname = fname
        self.test_path = ""

    def __enter__(self):
        unify.set_caching_fname(self._fname)
        self.test_path = LocalCache.get_cache_filepath(self._fname)
        if os.path.exists(self.test_path):
            os.remove(self.test_path)
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        if os.path.exists(self.test_path):
            os.remove(self.test_path)
        unify.set_caching_fname(self._old_cache_fpath)
