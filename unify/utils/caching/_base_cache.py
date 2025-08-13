from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional


class BaseCache(ABC):
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
