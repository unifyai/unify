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


# Fields (Both) #
# --------------#


def get_logs_with_fields(
    *fields: str,
    mode: str = "all",
    project: Optional[str] = None,
    api_key: Optional[str] = None,
) -> List[unify.Log]:
    """
    Returns a list of logs which contain the specified fields, either the logs which
    contain all of them ("all") or the logs which contain any of the fields ("any").

    Args:
        fields: The fields to retrieve logs for.

        mode: The retrieval mode, either returning the logs with all of the fields or
        the logs with any of the fields.

        project: Name of the project to get logs from.

        api_key: If specified, unify API key to be used. Defaults to the value in the
        `UNIFY_KEY` environment variable.

    Returns:
        A list of logs which contain the specified fields.
    """
    api_key = _validate_api_key(api_key)
    mode = {"any": "or", "all": "and"}[mode]
    filter_exp = f" {mode} ".join([f"exists({field})" for field in fields])
    return get_logs(project=project, filter=filter_exp, api_key=api_key)


def get_logs_without_fields(
    *fields: str,
    mode: str = "all",
    project: Optional[str] = None,
    api_key: Optional[str] = None,
) -> List[unify.Log]:
    """
    Returns a list of logs which do not contain the specified fields, either the logs
    which do not contain all of the fields ("all") or the logs which do not contain any
    of the fields ("any").

    Args:
        fields: The fields to not retrieve logs for.

        mode: The retrieval mode, either returning the logs with do not contain all of
        the fields or the logs which do not contain any of the fields.

        project: Name of the project to get logs from.

        api_key: If specified, unify API key to be used. Defaults to the value in the
        `UNIFY_KEY` environment variable.

    Returns:
        A list of logs which do not contain the specified fields.
    """
    api_key = _validate_api_key(api_key)
    mode = {"any": "and", "all": "or"}[mode]
    filter_exp = f" {mode} ".join([f"(not exists({field}))" for field in fields])
    return get_logs(project=project, filter=filter_exp, api_key=api_key)


def get_logs_by_id(
    ids: Union[int, List[int]],
    *,
    api_key: Optional[str] = None,
) -> List[unify.Log]:
    """
    Returns the logs associated with a given ids.

    Args:
        ids: IDs of the logs to fetch.

        api_key: If specified, unify API key to be used. Defaults to the value in the
        `UNIFY_KEY` environment variable.

    Returns:
        The full set of logs matching the given ids.
    """
    if isinstance(ids, int):
        ids = [ids]
    return [get_log_by_id(id=i, api_key=api_key) for i in ids]


def get_logs_by_value(
    *,
    project: Optional[str] = None,
    api_key: Optional[str] = None,
    **data,
) -> List[unify.Log]:
    """
    Returns the logs with the data matching exactly if it exists,
    otherwise returns None.

    Args:
        project: Name of the project to get logs from.

        api_key: If specified, unify API key to be used. Defaults to the value in the
        `UNIFY_KEY` environment variable.

        data: The data to search the upstream logs for.

    Returns:
        The list of logs which match the data, if any exist.
    """
    filter_str = " and ".join(
        [
            f"({k} == {json.dumps(v) if isinstance(v, str) else v})"
            for k, v in data.items()
        ],
    )
    return get_logs(project=project, filter=filter_str, api_key=api_key)


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
