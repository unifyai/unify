"""Pluggable object storage for gateway attachments and artifacts.

Used today to back inbound email/SMS/WhatsApp attachments and outbound
artifacts (call recordings, downloaded files). The hosted code path
stores these in Google Cloud Storage; the self-hosted path keeps them on
local disk under a configured base directory.

This package defines the ``Storage`` protocol and ships the local-disk
implementation. The GCS implementation is intentionally stubbed pending
the Phase B channel migration, which is where call sites actually need
the GCS backend wired in.
"""

from unity.gateway.storage.base import Storage, StorageError, StorageObject
from unity.gateway.storage.local import LocalDiskStorage

__all__ = ["LocalDiskStorage", "Storage", "StorageError", "StorageObject"]
