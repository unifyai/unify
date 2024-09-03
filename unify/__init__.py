"""Unify python module."""

_local_api = False  # for development


def base_url():
    if _local_api:
        return "http://127.0.0.1:8000/v0"
    return "https://api.unify.ai/v0"


from . import queries, utils
from .queries.chat import *
from .queries.clients import *
from .queries.multi_llm import *
from .utils.credits import *
from .utils.custom_api_keys import *
from .utils.custom_endpoints import *
from .utils.datasets import *
from .utils.efficiency_benchmarks import *
from .utils.evaluations import *
from .utils.evaluators import *
from .utils.helpers import *
from .utils.logging import *
from .utils.router_configurations import *
from .utils.router_deployment import *
from .utils.router_training import *
from .utils.supported_endpoints import *
