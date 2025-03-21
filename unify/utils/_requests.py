import json
import logging
import os

import requests

_logger = logging.getLogger("unify_requests")
_log_enabled = os.getenv("UNIFY_REQUESTS_DEBUG", "false").lower() in ("true", "1")
if _log_enabled:
    _type = os.getenv("UNIFY_REQUESTS_TYPE", "file").lower()
    if _type == "file":
        _handler = logging.FileHandler("requests.log", "w")
    elif _type == "console":
        _handler = logging.StreamHandler()
    else:
        raise ValueError(
            f"Invalid value for UNIFY_REQUESTS_TYPE, must be 'file' or 'console' got '{_type}'",
        )
    _handler.setLevel(logging.DEBUG)
    _handler.formatter = logging.Formatter(
        "%(levelname)s:%(name)s:%(asctime)s:%(message)s",
    )

    _logger.setLevel(logging.DEBUG)
    _logger.addHandler(_handler)
    _logger.propagate = False


def _log(type, url: str, /, **kwargs):
    if not _log_enabled:
        return
    _kwargs_str = ""
    for k, v in kwargs.items():
        if isinstance(v, dict):
            _kwargs_str += f"{k:}:{json.dumps(v, indent=2)},\n"
        else:
            _kwargs_str += f"{k}:{v},\n"
    log_msg = f"""
====== {type} =======
url:{url}
{_kwargs_str}
"""
    _logger.debug(log_msg)


def request(method, url, **kwargs):
    _log(f"request:{method}", url, **kwargs)
    res = requests.request(method, url, **kwargs)
    _log(f"response:{res.status_code}", url, **kwargs)
    return res


def get(url, params=None, **kwargs):
    _log("GET", url, params=params, **kwargs)
    res = requests.get(url, params=params, **kwargs)
    _log(f"response:{res.status_code}", url, params=params, **kwargs)
    return res


def options(url, **kwargs):
    _log("OPTIONS", url, **kwargs)
    res = requests.options(url, **kwargs)
    _log(f"response:{res.status_code}", url, **kwargs)
    return res


def head(url, **kwargs):
    _log("HEAD", url, **kwargs)
    res = requests.head(url, **kwargs)
    _log(f"response:{res.status_code}", url, **kwargs)
    return res


def post(url, data=None, json=None, **kwargs):
    _log("POST", url, data=data, json=json, **kwargs)
    res = requests.post(url, data=data, json=json, **kwargs)
    _log(f"response:{res.status_code}", url, data=data, json=json, **kwargs)
    return res


def put(url, data=None, **kwargs):
    _log("PUT", url, data=data, **kwargs)
    res = requests.put(url, data=data, **kwargs)
    _log(f"response:{res.status_code}", url, data=data, **kwargs)
    return res


def patch(url, data=None, **kwargs):
    _log("PATCH", url, data=data, **kwargs)
    res = requests.patch(url, data=data, **kwargs)
    _log(f"response:{res.status_code}", url, data=data, **kwargs)
    return res


def delete(url, **kwargs):
    _log("DELETE", url, **kwargs)
    res = requests.delete(url, **kwargs)
    _log(f"response:{res.status_code}", url, **kwargs)
    return res
