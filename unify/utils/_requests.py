import requests


def request(method, url, **kwargs):
    return requests.request(method, url, **kwargs)


def get(url, params=None, **kwargs):
    return requests.get(url, params=params, **kwargs)


def options(url, **kwargs):
    return requests.options(url, **kwargs)


def head(url, **kwargs):
    return requests.head(url, **kwargs)


def post(url, data=None, json=None, **kwargs):
    return requests.post(url, data=data, json=json, **kwargs)


def put(url, data=None, **kwargs):
    return requests.put(url, data=data, **kwargs)


def patch(url, data=None, **kwargs):
    return requests.patch(url, data=data, **kwargs)


def delete(url, **kwargs):
    return requests.delete(url, **kwargs)
