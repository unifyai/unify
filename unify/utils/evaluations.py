import requests
from typing import Dict, Optional, Any, Union, List

import unify
from unify import BASE_URL
from .helpers import _validate_api_key


# Helpers #
# --------#


def _get_and_maybe_create_project(project: str, api_key: Optional[str] = None) -> str:
    api_key = _validate_api_key(api_key)
    if project is None:
        project = unify.active_project
        if project is None:
            raise Exception(
                "No project specified in the arguments, and no globally set project "
                "either. A project must be passed in the argument, or set globally via "
                "unify.activate('project_name')"
            )
        if project not in list_projects(api_key):
            create_project(project, api_key)
    return project


# Projects #
# ---------#


def create_project(name: str, api_key: Optional[str] = None) -> Dict[str, str]:
    """
    Creates a logging project and adds this to your account. This project will have
    a set of logs associated with it.

    Args:
        name: A unique, user-defined name used when referencing the project.

        api_key: If specified, unify API key to be used. Defaults to the value in the
        `UNIFY_KEY` environment variable.

    Returns:
        A message indicating whether the project was created successfully.
    """
    api_key = _validate_api_key(api_key)
    headers = {
        "accept": "application/json",
        "Authorization": f"Bearer {api_key}",
    }
    body = {"name": name}
    response = requests.post(BASE_URL + "/project", headers=headers, json=body)
    response.raise_for_status()
    return response.json()


def rename_project(
    name: str, new_name: str, api_key: Optional[str] = None
) -> Dict[str, str]:
    """
    Renames a project from `name` to `new_name` in your account.

    Args:
        name: Name of the project to rename.

        new_name: A unique, user-defined name used when referencing the project.

        api_key: If specified, unify API key to be used. Defaults to the value in the
        `UNIFY_KEY` environment variable.

    Returns:
        A message indicating whether the project was successfully renamed.
    """
    api_key = _validate_api_key(api_key)
    headers = {
        "accept": "application/json",
        "Authorization": f"Bearer {api_key}",
    }
    body = {"name": new_name}
    response = requests.patch(BASE_URL + f"/project/{name}", headers=headers, json=body)
    response.raise_for_status()
    return response.json()


def delete_project(name: str, api_key: Optional[str] = None) -> str:
    """
    Deletes a project from your account.

    Args:
        name: Name of the project to delete.

        api_key: If specified, unify API key to be used. Defaults to the value in the
        `UNIFY_KEY` environment variable.

    Returns:
        Whether the project was successfully deleted.
    """
    api_key = _validate_api_key(api_key)
    headers = {
        "accept": "application/json",
        "Authorization": f"Bearer {api_key}",
    }
    response = requests.delete(BASE_URL + f"/project/{name}", headers=headers)
    response.raise_for_status()
    return response.json()


def list_projects(api_key: Optional[str] = None) -> List[str]:
    """
    Returns the names of all projects stored in your account.

    Args:
        api_key: If specified, unify API key to be used. Defaults to the value in the
        `UNIFY_KEY` environment variable.

    Returns:
        List of all project names.
    """
    api_key = _validate_api_key(api_key)
    headers = {
        "accept": "application/json",
        "Authorization": f"Bearer {api_key}",
    }
    response = requests.get(BASE_URL + "/projects", headers=headers)
    response.raise_for_status()
    return response.json()


# Artifacts #
# ----------#


def add_artifacts(
    project: Optional[str] = None, api_key: Optional[str] = None, **kwargs
) -> Dict[str, str]:
    """
    Creates one or more artifacts associated to a project. Artifacts are project-level
    metadata that don’t depend on other variables.

    Args:
        project: Name of the project the artifacts belong to.

        api_key: If specified, unify API key to be used. Defaults to the value in the
        `UNIFY_KEY` environment variable.

        kwargs: Dictionary containing one or more key:value pairs that will be stored
        as artifacts.

    Returns:
        A message indicating whether the artifacts were successfully added.
    """
    api_key = _validate_api_key(api_key)
    headers = {
        "accept": "application/json",
        "Authorization": f"Bearer {api_key}",
    }
    body = {"artifacts": kwargs}
    project = _get_and_maybe_create_project(project, api_key)
    response = requests.post(
        BASE_URL + f"/project/{project}/artifacts", headers=headers, json=body
    )
    response.raise_for_status()
    return response.json()


def delete_artifact(
    key: str, project: Optional[str] = None, api_key: Optional[str] = None
) -> str:
    """
    Deletes an artifact from a project.

    Args:
        project: Name of the project to delete an artifact from.

        key: Key of the artifact to delete.

        api_key: If specified, unify API key to be used. Defaults to the value in the
        `UNIFY_KEY` environment variable.

    Returns:
        Whether the artifacts were successfully deleted.
    """
    api_key = _validate_api_key(api_key)
    headers = {
        "accept": "application/json",
        "Authorization": f"Bearer {api_key}",
    }
    project = _get_and_maybe_create_project(project, api_key)
    response = requests.delete(
        BASE_URL + f"/project/{project}/artifacts/{key}", headers=headers
    )
    response.raise_for_status()
    return response.json()


def get_artifacts(
    project: Optional[str] = None, api_key: Optional[str] = None
) -> Dict[str, Any]:
    """
    Returns the key-value pairs of all artifacts in a project.

    Args:
        project: Name of the project to delete an artifact from.

        api_key: If specified, unify API key to be used. Defaults to the value in the
        `UNIFY_KEY` environment variable.

    Returns:
        A dictionary of all artifacts associated with the project, with keys for
        artifact names and values for the artifacts themselves.
    """
    api_key = _validate_api_key(api_key)
    headers = {
        "accept": "application/json",
        "Authorization": f"Bearer {api_key}",
    }
    project = _get_and_maybe_create_project(project, api_key)
    response = requests.get(BASE_URL + f"/project/{project}/artifacts", headers=headers)
    response.raise_for_status()
    return response.json()


# Logs #
# -----#


class Log:

    def __init__(
        self,
        project: Optional[str] = None,
        version: Optional[Dict[str, str]] = None,
        api_key: Optional[str] = None,
        **kwargs,
    ):
        self._api_key = _validate_api_key(api_key)
        headers = {
            "accept": "application/json",
            "Authorization": f"Bearer {self._api_key}",
        }
        project = _get_and_maybe_create_project(project)
        if version:
            kwargs = {
                k + "/" + version[k] if k in version else k: v
                for k, v in kwargs.items()
            }
        body = {"project": project, "entries": kwargs}
        response = requests.post(BASE_URL + "/log", headers=headers, json=body)
        response.raise_for_status()
        self._id = response.json()

    # Properties

    @property
    def id(self) -> int:
        return self._id

    # Public methods

    def add_entries(self, **kwargs) -> None:
        add_log_entries(self._id, self._api_key, **kwargs)

    def delete_entries(
        self,
        keys_to_delete: List[str],
    ) -> None:
        for key in keys_to_delete:
            delete_log_entry(key, self._id, self._api_key)

    def delete(self) -> None:
        delete_log(self._id, self._api_key)


def log(
    project: Optional[str] = None,
    version: Optional[Dict[str, str]] = None,
    api_key: Optional[str] = None,
    **kwargs,
) -> Log:
    """
    Creates one or more logs associated to a project. Logs are LLM-call-level data
    that might depend on other variables. This method returns the id of the new
    stored log.

    Args:
        project: Name of the project the stored logs will be associated to.

        version: Optional version parameters which are associated with each key being
        logged, with the keys of this version dict being the keys being logged, and the
        values being the name of this particular version.

        api_key: If specified, unify API key to be used. Defaults to the value in the
        `UNIFY_KEY` environment variable.

        kwargs: Dictionary containing one or more key:value pairs that will be logged
        into the platform.

    Returns:
        The unique id of newly created log entry.
    """
    return Log(project, version, api_key, **kwargs)


# ToDo: endpoint not available yet
def add_log_entries(id: int, api_key: Optional[str] = None, **kwargs) -> Dict[str, str]:
    """
    Returns the data (id and values) by querying the data based on their values.

    Args:
        id: The log id to update with extra data.

        api_key: If specified, unify API key to be used. Defaults to the value in the
        `UNIFY_KEY` environment variable.

        kwargs: The data to log into the console.

    Returns:
        A message indicating whether the log was successfully updated.
    """
    api_key = _validate_api_key(api_key)
    headers = {
        "accept": "application/json",
        "Authorization": f"Bearer {api_key}",
    }
    body = {"data": kwargs, "id": id}
    response = requests.put(BASE_URL + "/log", headers=headers, json=body)
    response.raise_for_status()
    return response.json()


def delete_log(
    id: Union[int, List[int]], api_key: Optional[str] = None
) -> Dict[str, str]:
    """
    Deletes logs from a project.

    Args:
        id: IDs of the log to delete from a project.

        api_key: If specified, unify API key to be used. Defaults to the value in the
        `UNIFY_KEY` environment variable.

    Returns:
        A message indicating whether the logs were successfully deleted.
    """
    api_key = _validate_api_key(api_key)
    headers = {
        "accept": "application/json",
        "Authorization": f"Bearer {api_key}",
    }
    response = requests.delete(BASE_URL + f"/log/{id}", headers=headers)
    response.raise_for_status()
    return response.json()


def delete_log_entry(
    entry: str, id: str, api_key: Optional[str] = None
) -> Dict[str, str]:
    """
    Deletes an entry from a log.

    Args:
        entry: Name of the entries to delete from a given log.

        id: ID of the log to delete an entry from.

        api_key: If specified, unify API key to be used. Defaults to the value in the
        `UNIFY_KEY` environment variable.

    Returns:
        A message indicating whether the log entries were successfully deleted.
    """
    api_key = _validate_api_key(api_key)
    headers = {
        "accept": "application/json",
        "Authorization": f"Bearer {api_key}",
    }
    response = requests.delete(BASE_URL + f"/log/{id}/entry/{entry}", headers=headers)
    response.raise_for_status()
    return response.json()


def get_log(id: int, api_key: Optional[str] = None) -> List[Dict[str, Any]]:
    """
    Returns the log associated with a given id.

    Args:
        id: IDs of the logs to fetch.

        api_key: If specified, unify API key to be used. Defaults to the value in the
        `UNIFY_KEY` environment variable.

    Returns:
        The full set of log data.
    """
    api_key = _validate_api_key(api_key)
    headers = {
        "accept": "application/json",
        "Authorization": f"Bearer {api_key}",
    }
    response = requests.get(BASE_URL + f"/log/{id}", headers=headers)
    response.raise_for_status()
    return response.json()


def get_logs(
    project: Optional[str] = None,
    filter: Optional[str] = None,
    api_key: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """
    Returns a list of filtered logs from a project.

    Args:
        project: Name of the project to get logs from.

        filter: Boolean string to filter logs, for example:
        "(temperature > 0.5 and (len(system_msg) < 100 or 'no' in usr_response))"

        api_key: If specified, unify API key to be used. Defaults to the value in the
        `UNIFY_KEY` environment variable.

    Returns:
        The full set of log data for the project, after optionally applying filtering.
    """
    api_key = _validate_api_key(api_key)
    headers = {
        "accept": "application/json",
        "Authorization": f"Bearer {api_key}",
    }
    project = _get_and_maybe_create_project(project, api_key)
    params = {"project": project, "filter": filter}
    response = requests.get(BASE_URL + "/logs", headers=headers, params=params)
    response.raise_for_status()
    return response.json()


def get_groups(
    key: str, project: Optional[str] = None, api_key: Optional[str] = None
) -> Dict[str, List[Dict[str, Any]]]:
    """
    Returns a list of the different version/values of one entry within a given project
    based on its key.

    Args:
        key: Name of the log entry to do equality matching for.

        project: Name of the project to get logs from.

        api_key: If specified, unify API key to be used. Defaults to the value in the
        `UNIFY_KEY` environment variable.

    Returns:
        A dict containing the grouped logs, with each key of the dict representing the
        version of the log key with equal values, and the value being the equal value.
    """
    api_key = _validate_api_key(api_key)
    headers = {
        "accept": "application/json",
        "Authorization": f"Bearer {api_key}",
    }
    project = _get_and_maybe_create_project(project, api_key)
    params = {"project": project, "key": key}
    response = requests.get(BASE_URL + "/logs/groups", headers=headers, params=params)
    response.raise_for_status()
    return response.json()


def group_logs(key: str, project: Optional[str] = None, api_key: Optional[str] = None):
    """
    Groups logs based on equality '==' of the values for the specified key, returning a
    dict with group indices as the keys and the list of logs as the values. If the keys
    are not versioned, then the indices are simply incrementing integers.

    Args:
        key: Name of the log entry to do equality matching for.

        project: Name of the project to get logs from.

        api_key: If specified, unify API key to be used. Defaults to the value in the
        `UNIFY_KEY` environment variable.

    Returns:
        A dict containing the grouped logs, with each key of the dict representing the
        version of the log key with equal values, and the value being a list of logs.
    """
    api_key = _validate_api_key(api_key)
    project = _get_and_maybe_create_project(project, api_key)
    return {
        k: get_logs(project, "{} == {}".format(key, v), api_key)
        for k, v in get_groups(key, project, api_key).items()
    }


def get_logs_metric(
    metric: str,
    key: str,
    filter: Optional[str] = None,
    project: Optional[str] = None,
    api_key: Optional[str] = None,
) -> Union[float, int, bool]:
    """
    Retrieve a set of log metrics across a project, after applying the filtering.

    Args:
        metric: The reduction metric to compute for the specified key. Supported are:
        sum, mean, var, std, min, max, median, mode.

        key: The key to compute the reduction statistic for.

        filter: The filtering to apply to the various log values, expressed as a string,
        for example:
        "(temperature > 0.5 and (len(system_msg) < 100 or 'no' in usr_response))"

        project: The id of the project to retrieve the logs for.

        api_key: If specified, unify API key to be used. Defaults to the value in the
        `UNIFY_KEY` environment variable.

    Returns:
        The full set of reduced log metrics for the project, after optionally applying
        the optional filtering.
    """
    api_key = _validate_api_key(api_key)
    headers = {
        "accept": "application/json",
        "Authorization": f"Bearer {api_key}",
    }
    project = _get_and_maybe_create_project(project, api_key)
    params = {"project": project, "filter": filter}
    response = requests.get(
        BASE_URL + f"/logs/metric/{metric}/{key}", headers=headers, params=params
    )
    response.raise_for_status()
    return response.json()
