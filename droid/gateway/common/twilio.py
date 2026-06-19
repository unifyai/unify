"""Shared Twilio REST Client factories for gateway channels.

Promoted from the inline ``_build_twilio_client`` / ``_build_twilio_wa_client``
helpers in ``droid.gateway.channels.social.views`` once ``phone/``
became the second channel needing the same surface.

Two distinct factories because the SMS path and the WhatsApp path
have separate Twilio sub-accounts in hosted Droid (different
``TWILIO_ACCOUNT_SID`` / ``TWILIO_WA_ACCOUNT_SID``). The WhatsApp
variant is here for future migrations even though only ``social/``
currently calls it -- one factory per credential pair beats a
parameterised helper.
"""

from __future__ import annotations

from typing import Any

from droid.gateway.credentials import CredentialNotFoundError, CredentialStore


def build_twilio_client(credentials: CredentialStore) -> Any:
    """Construct a Twilio REST client using the SMS credentials.

    Reads ``TWILIO_ACCOUNT_SID`` and ``TWILIO_AUTH_TOKEN`` from the
    supplied credential store. Lazy-imports the Twilio SDK so unit
    tests that mock the client don't pay the import cost on every
    collection.

    Raises ``RuntimeError`` with a clear message naming both required
    credentials when either is missing.
    """
    from twilio.rest import Client as TwilioClient

    try:
        sid = credentials.get("TWILIO_ACCOUNT_SID")
        token = credentials.get("TWILIO_AUTH_TOKEN")
    except CredentialNotFoundError as exc:
        raise RuntimeError(
            "TWILIO_ACCOUNT_SID and TWILIO_AUTH_TOKEN must be set",
        ) from exc
    return TwilioClient(sid, token)


def build_twilio_wa_client(credentials: CredentialStore) -> Any:
    """Construct a Twilio REST client using the WhatsApp credentials.

    Reads ``TWILIO_WA_ACCOUNT_SID`` and ``TWILIO_WA_AUTH_TOKEN`` from
    the supplied credential store. The WhatsApp sub-account is
    distinct from the SMS account in hosted Droid, so SMS creds will
    not satisfy a WhatsApp send and vice versa.
    """
    from twilio.rest import Client as TwilioClient

    try:
        sid = credentials.get("TWILIO_WA_ACCOUNT_SID")
        token = credentials.get("TWILIO_WA_AUTH_TOKEN")
    except CredentialNotFoundError as exc:
        raise RuntimeError(
            "TWILIO_WA_ACCOUNT_SID and TWILIO_WA_AUTH_TOKEN must be set",
        ) from exc
    return TwilioClient(sid, token)


__all__ = ["build_twilio_client", "build_twilio_wa_client"]
