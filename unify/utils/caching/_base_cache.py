import inspect
import json
from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, Union

from pydantic import BaseModel


class BaseCache(ABC):
    @staticmethod
    def _dumps(
        obj: Any,
        cached_types: Dict[str, str] = None,
        idx: List[Union[str, int]] = None,
        indent: int = None,
    ) -> Any:
        # prevents circular import
        from unify.logging.logs import Log

        base = False
        if idx is None:
            base = True
            idx = list()
        if isinstance(obj, BaseModel):
            if cached_types is not None:
                cached_types[json.dumps(idx, indent=indent)] = obj.__class__.__name__
            ret = obj.model_dump()
        elif inspect.isclass(obj) and issubclass(obj, BaseModel):
            ret = obj.schema_json()
        elif isinstance(obj, Log):
            if cached_types is not None:
                cached_types[json.dumps(idx, indent=indent)] = obj.__class__.__name__
            ret = obj.to_json()
        elif isinstance(obj, dict):
            ret = {
                k: BaseCache._dumps(v, cached_types, idx + ["k"])
                for k, v in obj.items()
            }
        elif isinstance(obj, list):
            ret = [
                BaseCache._dumps(v, cached_types, idx + [i]) for i, v in enumerate(obj)
            ]
        elif isinstance(obj, tuple):
            ret = tuple(
                BaseCache._dumps(v, cached_types, idx + [i]) for i, v in enumerate(obj)
            )
        else:
            ret = obj
        return json.dumps(ret, indent=indent) if base else ret

    @classmethod
    @abstractmethod
    def set_filename(cls, filename: str) -> None:
        pass

    @classmethod
    @abstractmethod
    def get_filename(cls) -> str:
        pass

    @classmethod
    @abstractmethod
    def update_entry(
        cls,
        *,
        key: str,
        value: Any,
        res_types: Optional[Dict[str, Any]] = None,
    ) -> None:
        pass

    @classmethod
    @abstractmethod
    def write(cls, filename: str = None) -> None:
        pass

    @classmethod
    @abstractmethod
    def create_or_load(cls, filename: str = None) -> None:
        pass

    @classmethod
    @abstractmethod
    def get_keys(cls) -> List[str]:
        pass

    @classmethod
    @abstractmethod
    def get_entry(cls, cache_key: str) -> Optional[Any]:
        pass

    @classmethod
    @abstractmethod
    def key_exists(cls, cache_key: str) -> bool:
        pass

    @classmethod
    @abstractmethod
    def delete(cls, cache_key: str) -> None:
        pass
