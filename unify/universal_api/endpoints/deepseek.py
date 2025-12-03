import litellm

from .utils import get_model_alias_map

models = {
    "deepseek-v3": "deepseek/deepseek-chat",
    "deepseek-r1": "deepseek/deepseek-reasoner",
}

litellm.model_alias_map.update(get_model_alias_map("deepseek", models))
