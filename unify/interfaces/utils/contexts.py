import requests
from typing import Optional, Dict, Any
from ...utils.helpers import _validate_api_key, _get_and_maybe_create_project

from unify import BASE_URL


# Contexts #
# ---------#


def get_contexts(
    *,
    prefix: Optional[str] = None,
    project: Optional[str] = None,
    api_key: Optional[str] = None,
) -> Dict[str, str]:
    """
    Gets all contexts associated with a project, with the corresponding prefix.

    Args:
        prefix: Prefix of the contexts to get.

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
    project = _get_and_maybe_create_project(
        project,
        api_key=api_key,
        create_if_missing=False,
    )
    response = requests.get(
        BASE_URL + f"/project/{project}/contexts",
        headers=headers,
    )
    if response.status_code != 200:
        raise Exception(response.json())
    contexts = response.json()
    contexts = {context["name"]: context["description"] for context in contexts}
    if prefix:
        contexts = {
            context: description
            for context, description in contexts.items()
            if context.startswith(prefix)
        }
    return contexts


def delete_context(
    name: str,
    *,
    project: Optional[str] = None,
    api_key: Optional[str] = None,
) -> None:
    """
    Delete a context from the server.

    Args:
        name: Name of the context to delete.

        project: Name of the project the context belongs to.

        api_key: If specified, unify API key to be used. Defaults to the value in the
        `UNIFY_KEY` environment variable.
    """
    api_key = _validate_api_key(api_key)
    project = _get_and_maybe_create_project(
        project,
        api_key=api_key,
        create_if_missing=False,
    )
    headers = {
        "accept": "application/json",
        "Authorization": f"Bearer {api_key}",
    }
    response = requests.delete(
        BASE_URL + f"/project/{project}/contexts/{name}",
        headers=headers,
    )
    if response.status_code != 200:
        raise Exception(response.json())
    return response.json()
