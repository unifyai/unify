import importlib
import sys

import unify


class TraceLoader(importlib.abc.Loader):
    def __init__(self, original_loader, filter=None):
        self._original_loader = original_loader
        self.filter = filter

    def create_module(self, spec):
        return self._original_loader.create_module(spec)

    def exec_module(self, module):
        self._original_loader.exec_module(module)
        unify.traced(module, filter=self.filter)


class TraceFinder(importlib.abc.MetaPathFinder):
    def __init__(self, targets=[], filter=None):
        self.targets = targets
        self.filter = filter
        self._original_finder = importlib.machinery.PathFinder

    def find_spec(self, fullname, path, target=None):
        for target_module in self.targets:
            if not fullname.startswith(target_module):
                return None

        spec = self._original_finder.find_spec(fullname, path)
        if not spec or not spec.loader:
            return None

        if spec.origin is None or not spec.origin.endswith(".py"):
            return None

        spec.loader = TraceLoader(spec.loader, filter=self.filter)
        return spec


def install_tracing_hook(targets=[], filter=None):
    if not any(isinstance(finder, TraceFinder) for finder in sys.meta_path):
        sys.meta_path.insert(0, TraceFinder(targets, filter))


def disable_tracing_hook():
    for finder in sys.meta_path:
        if isinstance(finder, TraceFinder):
            sys.meta_path.remove(finder)
