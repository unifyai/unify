from typing import Dict, List, Optional

from unify import BASE_URL
from unify.utils import _requests

from ...utils.helpers import _check_response, _validate_api_key

# Projects #
# ---------#


def create_project(
    name: str,
    *,
    overwrite: bool = False,
    api_key: Optional[str] = None,
    is_versioned: bool = True,
) -> Dict[str, str]:
    """
    Creates a logging project and adds this to your account. This project will have
    a set of logs associated with it.

    Args:
        name: A unique, user-defined name used when referencing the project.

        overwrite: Whether to overwrite an existing project if is already exists.

        api_key: If specified, unify API key to be used. Defaults to the value in the
        `UNIFY_KEY` environment variable.

        is_versioned: Whether the project is tracked via version control.

    Returns:
        A message indicating whether the project was created successfully.
    """
    api_key = _validate_api_key(api_key)
    headers = {
        "accept": "application/json",
        "Authorization": f"Bearer {api_key}",
    }
    body = {"name": name, "is_versioned": is_versioned}
    if overwrite:
        if name in list_projects(api_key=api_key):
            delete_project(name=name, api_key=api_key)
    response = _requests.post(BASE_URL + "/project", headers=headers, json=body)
    _check_response(response)
    return response.json()


def rename_project(
    name: str,
    new_name: str,
    *,
    api_key: Optional[str] = None,
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
    response = _requests.patch(
        BASE_URL + f"/project/{name}",
        headers=headers,
        json=body,
    )
    _check_response(response)
    return response.json()


def delete_project(
    name: str,
    *,
    api_key: Optional[str] = None,
) -> str:
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
    response = _requests.delete(BASE_URL + f"/project/{name}", headers=headers)
    _check_response(response)
    return response.json()


def delete_project_logs(
    name: str,
    *,
    api_key: Optional[str] = None,
) -> None:
    """
    Deletes all logs from a project.

    Args:
        name: Name of the project to delete logs from.

        api_key: If specified, unify API key to be used. Defaults to the value in the
        `UNIFY_KEY` environment variable.
    """
    api_key = _validate_api_key(api_key)
    headers = {
        "accept": "application/json",
        "Authorization": f"Bearer {api_key}",
    }
    response = _requests.delete(BASE_URL + f"/project/{name}/logs", headers=headers)
    _check_response(response)
    return response.json()


def delete_project_contexts(
    name: str,
    *,
    api_key: Optional[str] = None,
) -> None:
    """
    Deletes all contexts and their associated logs from a project

    Args:
        name: Name of the project to delete contexts from.

        api_key: If specified, unify API key to be used. Defaults to the value in the
        `UNIFY_KEY` environment variable.
    """
    api_key = _validate_api_key(api_key)
    headers = {
        "accept": "application/json",
        "Authorization": f"Bearer {api_key}",
    }
    response = _requests.delete(BASE_URL + f"/project/{name}/contexts", headers=headers)
    _check_response(response)
    return response.json()


def list_projects(
    *,
    api_key: Optional[str] = None,
) -> List[str]:
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
    response = _requests.get(BASE_URL + "/projects", headers=headers)
    _check_response(response)
    return response.json()


def commit_project(
    name: str,
    commit_message: str,
    *,
    api_key: Optional[str] = None,
) -> Dict[str, str]:
    """
    Creates a commit for the entire project, saving a snapshot of all versioned contexts.

    Args:
        name: Name of the project to commit.
        commit_message: A description of the changes being saved.
        api_key: If specified, unify API key to be used. Defaults to the value in the
        `UNIFY_KEY` environment variable.

    Returns:
        A dictionary containing the new commit_hash.
    """
    api_key = _validate_api_key(api_key)
    headers = {
        "accept": "application/json",
        "Authorization": f"Bearer {api_key}",
    }
    body = {"commit_message": commit_message}
    response = _requests.post(
        BASE_URL + f"/project/{name}/commit",
        headers=headers,
        json=body,
    )
    _check_response(response)
    return response.json()


def rollback_project(
    name: str,
    commit_hash: str,
    *,
    api_key: Optional[str] = None,
) -> Dict[str, str]:
    """
    Rolls back the entire project to a specific commit.

    Args:
        name: Name of the project to roll back.
        commit_hash: The hash of the commit to restore.
        api_key: If specified, unify API key to be used. Defaults to the value in the
        `UNIFY_KEY` environment variable.

    Returns:
        A message indicating the success of the rollback operation.
    """
    api_key = _validate_api_key(api_key)
    headers = {
        "accept": "application/json",
        "Authorization": f"Bearer {api_key}",
    }
    body = {"commit_hash": commit_hash}
    response = _requests.post(
        BASE_URL + f"/project/{name}/rollback",
        headers=headers,
        json=body,
    )
    _check_response(response)
    return response.json()


def get_project_commits(name: str, *, api_key: Optional[str] = None) -> List[Dict]:
    """
    Retrieves the commit history for a project.

    Args:
        name: Name of the project.
        api_key: If specified, unify API key to be used. Defaults to the value in the
        `UNIFY_KEY` environment variable.

    Returns:
        A list of dictionaries, each representing a commit.
    """
    api_key = _validate_api_key(api_key)
    headers = {
        "accept": "application/json",
        "Authorization": f"Bearer {api_key}",
    }
    response = _requests.get(BASE_URL + f"/project/{name}/commits", headers=headers)
    _check_response(response)
    return response.json()
