from __future__ import annotations
import json

from ...utils.helpers import _validate_api_key
from .logs import *


# Parameters #
# -----------#


def get_param_by_version(
    field: str,
    version: Union[str, int],
    api_key: Optional[str] = None,
) -> Any:
    """
    Gets the parameter by version.

    Args:
        field: The field of the parameter to get.

        version: The version of the parameter to get.

        api_key: If specified, unify API key to be used. Defaults to the value in the
        `UNIFY_KEY` environment variable.

    Returns:
        The parameter by version.
    """
    api_key = _validate_api_key(api_key)
    version = str(version)
    filter_exp = f"version({field}) == {version}"
    return get_logs(filter=filter_exp, limit=1, api_key=api_key)[0].params[field][1]


def get_param_by_value(
    field: str,
    value: Any,
    api_key: Optional[str] = None,
) -> Any:
    """
    Gets the parameter by value.

    Args:
        field: The field of the parameter to get.

        value: The value of the parameter to get.

        api_key: If specified, unify API key to be used. Defaults to the value in the
        `UNIFY_KEY` environment variable.

    Returns:
        The parameter by version.
    """
    api_key = _validate_api_key(api_key)
    filter_exp = f"{field} == {json.dumps(value)}"
    return get_logs(filter=filter_exp, limit=1, api_key=api_key)[0].params[field][0]


def get_source() -> str:
    """
    Extracts the source code for the file from where this function was called.

    Returns:
        The source code for the file, as a string.
    """
    frame = inspect.getouterframes(inspect.currentframe())[1]
    with open(frame.filename, "r") as file:
        source = file.read()
    return f"```python\n{source}\n```"


# Experiments #
# ------------#


def get_experiment_name(version: int, api_key: Optional[str] = None) -> str:
    """
    Gets the experiment name (by version).

    Args:
        version: The version of the experiment to get.

        api_key: If specified, unify API key to be used. Defaults to the value in the
        `UNIFY_KEY` environment variable.

    Returns:
        The experiment name with said version.
    """
    logs = get_logs_with_fields("experiment", api_key=api_key)
    if not logs:
        return None
    if version < 0:
        latest_version = max(
            [max([int(param[0]) for param in lg.params.values()]) for lg in logs],
        )
        version = latest_version + version + 1
    return get_param_by_version("experiment", version, api_key)


def get_experiment_version(name: str, api_key: Optional[str] = None) -> int:
    """
    Gets the experiment version (by name).

    Args:
        name: The name of the experiment to get.

        api_key: If specified, unify API key to be used. Defaults to the value in the
        `UNIFY_KEY` environment variable.

    Returns:
        The experiment version with said name.
    """
    logs = get_logs_with_fields("experiment", api_key=api_key)
    if not logs:
        return None
    return get_param_by_value("experiment", name, api_key)
