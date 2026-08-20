"""A caller-supplied URL must not become a request into our own network.

The dangerous half of fetching a URL is the request, not the file. A URL is an
instruction to open a connection from inside the trusted runtime, so
``http://169.254.169.254/`` reads cloud instance credentials and
``http://127.0.0.1:8000/`` reaches services written on the assumption that only
local callers can see them. Neither looks like an attack at the call site --
both are ordinary fetches that return 200.

Content is a separate concern and handled elsewhere: parsing runs on the
dispatched fleet, off the assistant's process. What is asserted here is that
the *request* can only go somewhere public, cannot be redirected somewhere
private, and cannot write without bound.
"""

from __future__ import annotations

import ipaddress

import pytest

from unify.web_searcher import url_fetch
from unify.web_searcher.url_fetch import FetchRejected, assert_fetchable, filename_for


def _resolving_to(*addresses: str):
    """Stand in for DNS so the guard can be tested without the network."""

    def _fake(host: str):
        return [ipaddress.ip_address(a) for a in addresses]

    return _fake


class TestWhereARequestMayGo:
    def test_a_public_address_is_allowed(self, monkeypatch):
        monkeypatch.setattr(url_fetch, "_addresses_for", _resolving_to("93.184.216.34"))
        assert_fetchable("https://example.com/data.csv")

    @pytest.mark.parametrize(
        "address,what",
        [
            ("169.254.169.254", "cloud instance metadata"),
            ("127.0.0.1", "loopback"),
            ("10.0.0.5", "private RFC1918"),
            ("192.168.1.10", "home/office LAN"),
            ("172.16.4.4", "private RFC1918"),
            ("0.0.0.0", "unspecified"),
            ("::1", "IPv6 loopback"),
            ("fd00::1", "IPv6 unique-local"),
            ("fe80::1", "IPv6 link-local"),
        ],
    )
    def test_a_non_public_address_is_refused(self, monkeypatch, address, what):
        monkeypatch.setattr(url_fetch, "_addresses_for", _resolving_to(address))
        with pytest.raises(FetchRejected) as excinfo:
            assert_fetchable("https://looks-fine.example/x")
        assert "not a public address" in str(excinfo.value), what

    def test_one_private_answer_among_public_ones_is_still_refused(self, monkeypatch):
        # A host answering with both would otherwise pass the check and then be
        # free to connect to either.
        monkeypatch.setattr(
            url_fetch,
            "_addresses_for",
            _resolving_to("93.184.216.34", "127.0.0.1"),
        )
        with pytest.raises(FetchRejected):
            assert_fetchable("https://split-horizon.example/x")

    def test_an_unresolvable_host_is_refused_not_attempted(self, monkeypatch):
        def _boom(host: str):
            raise FetchRejected(f"Could not resolve host {host!r}")

        monkeypatch.setattr(url_fetch, "_addresses_for", _boom)
        with pytest.raises(FetchRejected):
            assert_fetchable("https://nope.invalid/x")


class TestWhatCountsAsAFetchableUrl:
    @pytest.mark.parametrize(
        "url",
        [
            "file:///etc/passwd",
            "ftp://example.com/x",
            "gopher://example.com/x",
            "data:text/plain;base64,aGk=",
        ],
    )
    def test_only_http_and_https_are_fetchable(self, url):
        with pytest.raises(FetchRejected) as excinfo:
            assert_fetchable(url)
        assert "http and https" in str(excinfo.value)

    def test_a_url_with_no_host_is_refused(self):
        with pytest.raises(FetchRejected):
            assert_fetchable("http:///just/a/path")

    def test_embedded_credentials_are_refused(self, monkeypatch):
        # Silently dropping them would fetch something other than what the URL
        # asked for; forwarding them would leak a credential to whatever the
        # host turns out to be.
        monkeypatch.setattr(url_fetch, "_addresses_for", _resolving_to("93.184.216.34"))
        with pytest.raises(FetchRejected) as excinfo:
            assert_fetchable(
                "https://user:secret@example.com/x",  # pragma: allowlist secret
            )
        assert "Credentials" in str(excinfo.value)


class TestNamingTheDownload:
    def test_a_name_is_taken_from_the_url(self):
        assert filename_for("https://example.com/reports/q3.csv") == "q3.csv"

    def test_a_percent_encoded_name_is_decoded(self):
        assert filename_for("https://example.com/MH%20data.csv") == "MH_data.csv"

    def test_a_server_suggested_traversal_cannot_escape(self):
        # Content-Disposition is attacker-controlled; trusting it verbatim is a
        # write outside the destination directory.
        name = filename_for(
            "https://example.com/x",
            'attachment; filename="../../etc/passwd"',
        )
        assert "/" not in name and ".." not in name
        assert name == "passwd"

    def test_a_url_with_no_path_still_yields_a_name(self):
        assert filename_for("https://example.com") == "download"

    def test_separators_in_a_suggested_name_are_neutralised(self):
        name = filename_for("https://example.com/x", 'attachment; filename="a/b/c.csv"')
        assert name == "c.csv"
