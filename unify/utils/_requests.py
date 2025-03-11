import logging

import requests

_handler = logging.FileHandler("requests.log", mode="w")
_handler.setLevel(logging.DEBUG)
_handler.formatter = logging.Formatter("%(levelname)s:%(name)s:%(asctime)s:%(message)s")

_logger = logging.getLogger("unify_requests")
_logger.setLevel(logging.DEBUG)
_logger.addHandler(_handler)


def _log(type, url: str, /, **kwargs):
    log_msg = f"""
    ====== {type} =======
    url:{url}
    msg:{kwargs}
"""
    _logger.debug(log_msg)


def request(method, url, **kwargs):
    _log(f"request:{method}", url, **kwargs)
    return requests.request(method, url, **kwargs)


def get(url, params=None, **kwargs):
    _log("get", url, params=params, **kwargs)
    return requests.get(url, params=params, **kwargs)


def options(url, **kwargs):
    return requests.options(url, **kwargs)


def head(url, **kwargs):
    return requests.head(url, **kwargs)


def post(url, data=None, json=None, **kwargs):
    _log("post", url, data=data, json=json, **kwargs)
    return requests.post(url, data=data, json=json, **kwargs)


def put(url, data=None, **kwargs):
    _log("put", url, data=data, **kwargs)
    return requests.put(url, data=data, **kwargs)


def patch(url, data=None, **kwargs):
    return requests.patch(url, data=data, **kwargs)


def delete(url, **kwargs):
    _log("delete", url, **kwargs)
    return requests.delete(url, **kwargs)
