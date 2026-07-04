"""Voice-enrollment storage on contact rows.

Enrollment data lives in private (underscore-prefixed) Orchestra columns on
the Contacts context (see ``VOICE_ENROLLMENT_FIELDS``), so it is invisible to
all LLM-facing contact tools while remaining queryable programmatically.
"""

from __future__ import annotations

import base64
from datetime import datetime, timezone
from typing import Iterable

import unisdk

VOICE_ENROLLMENT_SOURCE_AUTO = "auto_call"
VOICE_ENROLLMENT_SOURCE_MANUAL = "manual_upload"


def get_voice_profiles(self, contact_ids: Iterable[int]) -> dict[int, list[float]]:
    """Return {contact_id: embedding} for contacts with a voice enrollment."""
    ids = sorted({int(cid) for cid in contact_ids})
    if not ids:
        return {}
    if len(ids) == 1:
        filt = f"contact_id == {ids[0]}"
    else:
        filt = f"contact_id in [{', '.join(str(x) for x in ids)}]"
    rows = unisdk.get_logs(
        context=self._ctx,
        filter=filt,
        limit=len(ids),
        from_fields=["contact_id", "_voice_embedding"],
    )
    profiles: dict[int, list[float]] = {}
    for lg in rows:
        entries = lg.entries
        embedding = entries.get("_voice_embedding")
        if embedding:
            profiles[int(entries["contact_id"])] = [float(x) for x in embedding]
    return profiles


def get_voice_enrollment_info(self, contact_id: int) -> dict:
    """Return enrollment metadata (without the sample) for one contact."""
    rows = unisdk.get_logs(
        context=self._ctx,
        filter=f"contact_id == {int(contact_id)}",
        limit=1,
        from_fields=[
            "contact_id",
            "_voice_embedding",
            "_voice_enrolled_at",
            "_voice_enrollment_source",
        ],
    )
    if not rows:
        return {}
    entries = rows[0].entries
    return {
        "enrolled": bool(entries.get("_voice_embedding")),
        "enrolled_at": entries.get("_voice_enrolled_at"),
        "source": entries.get("_voice_enrollment_source"),
    }


def sync_manual_voice_enrollment(self) -> None:
    """Sync the boss user's manually recorded voice sample onto the boss contact.

    Reads the user's Orchestra record; when a voice sample exists that is newer
    than the boss contact's current enrollment (manual uploads always win over
    auto-call enrollments), downloads it, computes the speaker embedding, and
    stores both on the contact row.
    """
    import requests

    from unify.session_details import SESSION_DETAILS
    from unify.settings import SETTINGS

    resp = requests.get(
        f"{SETTINGS.ORCHESTRA_URL}/user/basic-info",
        headers={"Authorization": f"Bearer {SESSION_DETAILS.unify_key}"},
        timeout=15,
    )
    if resp.status_code != 200:
        return
    info = resp.json()
    gcs_uri = info.get("voice_sample")
    uploaded_at_raw = info.get("voice_sample_uploaded_at")
    if not gcs_uri:
        return

    boss_contact_id = int(SESSION_DETAILS.boss_contact_id)
    current = get_voice_enrollment_info(self, boss_contact_id)
    if current.get("enrolled"):
        uploaded_at = _parse_ts(uploaded_at_raw)
        enrolled_at = _parse_ts(current.get("enrolled_at"))
        is_manual = current.get("source") == VOICE_ENROLLMENT_SOURCE_MANUAL
        # Skip only when the current enrollment is already this manual sample
        # (or a newer one); auto-call enrollments are always superseded.
        if is_manual and enrolled_at is not None:
            if uploaded_at is None or uploaded_at <= enrolled_at:
                return

    wav_bytes = unisdk.download_object(gcs_uri)

    from unify.conversation_manager import speaker_id

    model_path = speaker_id.ensure_speaker_model()
    if model_path is None:
        raise RuntimeError(
            "Speaker-embedding model unavailable; cannot sync manual voice enrollment",
        )
    embedder = speaker_id.SpeakerEmbedder(model_path)
    embedding = embedder.embed_wav_sync(wav_bytes)

    set_voice_enrollment(
        self,
        contact_id=boss_contact_id,
        embedding=[float(x) for x in embedding],
        wav_bytes=wav_bytes,
        source=VOICE_ENROLLMENT_SOURCE_MANUAL,
    )


def _parse_ts(value) -> datetime | None:
    """Parse an ISO timestamp (tolerating a trailing Z); None when absent."""
    if not value:
        return None
    if isinstance(value, datetime):
        return value
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None


def set_voice_enrollment(
    self,
    *,
    contact_id: int,
    embedding: list[float],
    wav_bytes: bytes | None = None,
    source: str,
) -> None:
    """Persist a voice enrollment onto a contact row."""
    target_ids = unisdk.get_logs(
        context=self._ctx,
        filter=f"contact_id == {int(contact_id)}",
        return_ids_only=True,
    )
    if not target_ids:
        raise ValueError(
            f"No contact found with contact_id {contact_id} for voice enrollment.",
        )
    entries: dict = {
        "_voice_embedding": [float(x) for x in embedding],
        "_voice_enrolled_at": datetime.now(timezone.utc).isoformat(),
        "_voice_enrollment_source": source,
    }
    if wav_bytes is not None:
        entries["_voice_sample"] = base64.b64encode(wav_bytes).decode("ascii")
    unisdk.update_logs(
        logs=[target_ids[0]],
        context=self._ctx,
        entries=entries,
        overwrite=True,
    )
