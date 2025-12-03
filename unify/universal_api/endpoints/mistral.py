import litellm

from .utils import get_model_alias_map

models = {
    "mistral-large": "mistral/mistral-large-latest",
    "mistral-medium": "mistral/mistral-medium-latest",
    "mistral-small": "mistral/mistral-small-latest",
}

litellm.model_alias_map.update(get_model_alias_map("mistral", models))
