import json
import logging
import os

import requests
from requests.adapters import HTTPAdapter
from urllib3 import Retry

_logger = logging.getLogger("unify_requests")
_log_enabled = os.getenv("UNIFY_REQUESTS_DEBUG", "false").lower() in ("true", "1")
_logger.setLevel(logging.DEBUG if _log_enabled else logging.WARNING)

_session = requests.Session()
_retries = Retry(total=5, backoff_factor=0.1, status_forcelist=[500, 502, 503, 504])
_session.mount("http://", HTTPAdapter(max_retries=_retries))
_session.mount("https://", HTTPAdapter(max_retries=_retries))


class RequestError(Exception):
    def __init__(self, url: str, r_type: str, response: requests.Response, /, **kwargs):
        super().__init__(
            f"{r_type}:{url} with {kwargs} failed with status code {response.status_code}: {response.text}",
        )
        self.response = response


def _log(type: str, url: str, mask_key: bool = True, /, **kwargs):
    if not _log_enabled:
        return
    _kwargs_str = ""
    if mask_key and "headers" in kwargs:
        key = kwargs["headers"]["Authorization"]
        kwargs["headers"]["Authorization"] = "***"

    for k, v in kwargs.items():
        if isinstance(v, dict):
            _kwargs_str += f"{k:}:{json.dumps(v, indent=2)},\n"
        else:
            _kwargs_str += f"{k}:{v},\n"

    if mask_key and "headers" in kwargs:
        kwargs["headers"]["Authorization"] = key

    log_msg = f"""
====== {type} =======
url:{url}
{_kwargs_str}
"""
    _logger.debug(log_msg)


def _mask_auth_key(kwargs: dict):
    if "headers" in kwargs:
        kwargs["headers"]["Authorization"] = "***"
    return kwargs


def request(method, url, **kwargs):
    _log(f"{method}", url, True, **kwargs)
    try:
        res = _session.request(method, url, **kwargs)
        res.raise_for_status()
    except requests.exceptions.HTTPError as e:
        kwargs = _mask_auth_key(kwargs)
        raise RequestError(url, method, e.response, **kwargs)

    try:
        _log(f"{method} response:{res.status_code}", url, response=res.json())
    except requests.exceptions.JSONDecodeError:
        _log(f"{method} response:{res.status_code}", url, response=res.text)
    return res


def get(url, params=None, **kwargs):
    return request("GET", url, params=params, **kwargs)


def options(url, **kwargs):
    return request("OPTIONS", url, **kwargs)


def head(url, **kwargs):
    return request("HEAD", url, **kwargs)


def post(url, data=None, json=None, **kwargs):
    return request("POST", url, data=data, json=json, **kwargs)


def put(url, data=None, **kwargs):
    return request("PUT", url, data=data, **kwargs)


def patch(url, data=None, **kwargs):
    return request("PATCH", url, data=data, **kwargs)


def delete(url, **kwargs):
    return request("DELETE", url, **kwargs)
