from typing import Any, Dict


def get_model_alias_map(provider: str, model_map: Dict[str, Any]) -> Dict[str, str]:
    return {f"{model}@{provider}": alias for model, alias in model_map.items()}
