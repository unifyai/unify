import requests
from typing import Optional, List, Any, Dict, Union
import os

from unify import base_url
from .helpers import _validate_api_key, _res_to_list


def trigger_evaluation(
    evaluator: str,
    dataset: str,
    endpoint: str,
    client_side_scores: Optional[str] = None,
    api_key: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Trigger an evaluation for a specific dataset using a given evaluator and endpoint.

    Args:
        evaluator: Name of the evaluator to use.
        dataset: Name of the uploaded dataset to evaluate.
        endpoint: Name of the endpoint to evaluate. Must be specified using the `model@provider` format.
        client_side_scores: Optional path to a JSONL file containing client-side scores.
        api_key: If specified, unify API key to be used. Defaults to the value in the `UNIFY_KEY` environment variable.

    Returns:
        A dictionary containing the response from the API.

    Raises:
        requests.HTTPError: If the API request fails.
        KeyError: If the API key is not provided and not set in the environment.
        FileNotFoundError: If the client_side_scores file is specified but not found.
    """
    api_key = _validate_api_key(api_key)
    headers = {
        "accept": "application/json",
        "Authorization": f"Bearer {api_key}",
    }
    url = f"{base_url()}/evaluation"

    params = {
        "evaluator": evaluator,
        "dataset": dataset,
        "endpoint": endpoint,
    }

    files = {}
    if client_side_scores:
        if not os.path.exists(client_side_scores):
            raise FileNotFoundError(
                f"Client-side scores file not found: {client_side_scores}"
            )
        files["client_side_scores"] = (
            "client_scores.jsonl",
            open(client_side_scores, "rb"),
            "application/json",
        )

    response = requests.post(url, headers=headers, params=params, files=files)
    response.raise_for_status()

    return response.json()


def admin_trigger_eval(
    user_id: str, name: str, dataset: str, endpoint: str, api_key: Optional[str] = None
) -> Dict[str, Any]:
    """
    Trigger an evaluation as an admin for a specific user.

    Args:
        user_id: ID of the user that will own the triggered eval.
        name: Name of the eval to use.
        dataset: Name of the uploaded dataset to evaluate.
        endpoint: Name of the endpoint to evaluate. Must be specified using the `model@provider` format.
        api_key: If specified, unify API key to be used. Defaults to the value in the `UNIFY_KEY` environment variable.

    Returns:
        A dictionary containing the response from the API.

    Raises:
        requests.HTTPError: If the API request fails.
    """
    api_key = _validate_api_key(api_key)
    headers = {
        "accept": "application/json",
        "Authorization": f"Bearer {api_key}",
    }
    url = f"{base_url()}/evals/admin_trigger"

    params = {
        "user_id": user_id,
        "name": name,
        "dataset": dataset,
        "endpoint": endpoint,
    }

    response = requests.post(url, headers=headers, params=params)
    response.raise_for_status()

    return response.json()


def get_evaluations(
    dataset: str,
    endpoint: Optional[str] = None,
    evaluator: Optional[str] = None,
    per_prompt: bool = False,
    api_key: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Get evaluations for a specific dataset, optionally filtered by endpoint and evaluator.

    Args:
        dataset: Name of the dataset to fetch evaluation from.
        endpoint: The endpoint to fetch the evaluation for. If None, returns evaluations for all endpoints.
        evaluator: Name of the evaluator to fetch the evaluation for. If None, returns all available evaluations for the dataset and endpoint pair.
        per_prompt: If True, returns the scores on a per-prompt level. By default set to False. If True requires an eval name and endpoint to be set.
        api_key: If specified, unify API key to be used. Defaults to the value in the `UNIFY_KEY` environment variable.

    Returns:
        A dictionary containing the evaluation results.

    """
    api_key = _validate_api_key(api_key)
    headers = {
        "accept": "application/json",
        "Authorization": f"Bearer {api_key}",
    }

    url = f"{base_url()}/evaluation"

    params = {"dataset": dataset, "per_prompt": per_prompt}

    if endpoint:
        params["endpoint"] = endpoint
    if evaluator:
        params["evaluator"] = evaluator

    response = requests.get(url, headers=headers, params=params)

    if response.status_code == 200:
        return response.json()
    else:
        response.raise_for_status()


def delete_evaluations(
    dataset: str,
    endpoint: Optional[str] = None,
    evaluator: Optional[str] = None,
    api_key: Optional[str] = None,
) -> Dict[str, str]:
    """
    Delete evaluations for a specific dataset, optionally filtered by endpoint and evaluator.

    Args:
        dataset: Name of the dataset to delete the evaluation for.
        endpoint: The endpoint to delete the evaluation for. If None, deletes the evaluations for all endpoints.
        evaluator: Name of the evaluator to delete the evaluation for. If None, deletes all available evaluations for the dataset and endpoint pair.
        api_key: If specified, unify API key to be used. Defaults to the value in the `UNIFY_KEY` environment variable.

    Returns:
        A dictionary containing information about the deletion operation.

    Raises:
        requests.exceptions.RequestException: If the API request fails.
    """
    api_key = _validate_api_key(api_key)
    headers = {
        "accept": "application/json",
        "Authorization": f"Bearer {api_key}",
    }

    url = f"{base_url()}/evaluation"

    params = {
        "dataset": dataset,
    }

    if endpoint:
        params["endpoint"] = endpoint
    if evaluator:
        params["evaluator"] = evaluator

    response = requests.delete(url, headers=headers, params=params)
    response.raise_for_status()

    return response.json()


def get_evaluation_status(
    dataset: str, endpoint: str, evaluator: str, api_key: Optional[str] = None
) -> Dict[str, Any]:
    """
    Get the evaluation status for a specific dataset, endpoint, and evaluator.

    Args:
        dataset: Name of the dataset to get evaluation status of.
        endpoint: Endpoint to get evaluation status of.
        evaluator: Name of the evaluator to get status of.
        api_key: If specified, unify API key to be used. Defaults
                 to the value in the `UNIFY_KEY` environment variable.

    Returns:
        A dictionary containing the evaluation status information.

    """
    api_key = _validate_api_key(api_key)
    headers = {
        "accept": "application/json",
        "Authorization": f"Bearer {api_key}",
    }
    params = {
        "dataset": dataset,
        "endpoint": endpoint,
        "evaluator": evaluator,
    }
    url = f"{base_url()}/evaluation/status"

    response = requests.get(url, headers=headers, params=params)
    response.raise_for_status()

    return response.json()
