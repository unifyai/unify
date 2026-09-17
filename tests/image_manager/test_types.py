from __future__ import annotations

import pytest
from unify import db
from datetime import datetime, UTC
from pydantic import BaseModel, Field
from unify.common.context_store import TableStore
from unify.common.model_to_fields import model_to_fields
from unify.image_manager.utils import make_solid_png_base64
from unify.image_manager.types.image import Image

from tests.helpers import _handle_project
from pydantic import ValidationError
from unify.image_manager.types import (
    RawImageRef,
    AnnotatedImageRef,
    ImageRefs,
    RawImageRefs,
    AnnotatedImageRefs,
)


@_handle_project
def test_annotated_ref_basic():
    ann = AnnotatedImageRef(
        raw_image_ref=RawImageRef(image_id=123),
        annotation="training set example: high relevance",
    )

    assert ann.raw_image_ref.image_id == 123
    assert ann.annotation.startswith("training set example")


@_handle_project
def test_container_mixed_types():
    base1_ref = RawImageRef(image_id=1)
    base2_ref = RawImageRef(image_id=2)
    annotated = AnnotatedImageRef(
        raw_image_ref=base2_ref,
        annotation="used in alerting scenario",
    )

    images = ImageRefs.model_validate([base1_ref, annotated, base2_ref])

    assert len(images.root) == 3
    assert isinstance(images.root[0], RawImageRef)
    assert isinstance(images.root[1], AnnotatedImageRef)
    assert isinstance(images.root[2], RawImageRef)


@_handle_project
def test_raw_refs_accepts_only_raw():
    r1 = RawImageRef(image_id=10)
    r2 = RawImageRef(image_id=11)

    refs = RawImageRefs.model_validate([r1, r2])

    assert len(refs.root) == 2
    assert all(isinstance(x, RawImageRef) for x in refs.root)


@_handle_project
def test_raw_refs_rejects_annotated():
    ann = AnnotatedImageRef(
        raw_image_ref=RawImageRef(image_id=5),
        annotation="note",
    )

    with pytest.raises(ValidationError):
        RawImageRefs.model_validate([ann])


@_handle_project
def test_annotated_refs_accepts_only_annotated():
    ann1 = AnnotatedImageRef(
        raw_image_ref=RawImageRef(image_id=21),
        annotation="first",
    )
    ann2 = AnnotatedImageRef(
        raw_image_ref=RawImageRef(image_id=22),
        annotation="second",
    )

    refs = AnnotatedImageRefs.model_validate([ann1, ann2])

    assert len(refs.root) == 2
    assert all(isinstance(x, AnnotatedImageRef) for x in refs.root)


@_handle_project
def test_annotated_refs_rejects_raw():
    raw = RawImageRef(image_id=99)

    with pytest.raises(ValidationError):
        AnnotatedImageRefs.model_validate([raw])


# --------------------------------------------------------------------------- #
#  Filepath field on RawImageRef / AnnotatedImageRef                           #
# --------------------------------------------------------------------------- #


@_handle_project
def test_raw_ref_with_filepath_only():
    ref = RawImageRef(filepath="/tmp/images/step1.png")
    assert ref.image_id is None
    assert ref.filepath == "/tmp/images/step1.png"


@_handle_project
def test_raw_ref_with_both():
    ref = RawImageRef(image_id=42, filepath="/tmp/images/step1.png")
    assert ref.image_id == 42
    assert ref.filepath == "/tmp/images/step1.png"


@_handle_project
def test_annotated_ref_with_filepath():
    ann = AnnotatedImageRef(
        raw_image_ref=RawImageRef(filepath="/tmp/images/step2.png"),
        annotation="shows the deploy button",
    )
    assert ann.raw_image_ref.image_id is None
    assert ann.raw_image_ref.filepath == "/tmp/images/step2.png"
    assert ann.annotation == "shows the deploy button"


@_handle_project
def test_raw_ref_serialization_with_filepath():
    ref = RawImageRef(filepath="/tmp/images/round_trip.png")
    dumped = ref.model_dump(mode="json")
    assert dumped == {"image_id": None, "filepath": "/tmp/images/round_trip.png"}
    restored = RawImageRef.model_validate(dumped)
    assert restored.filepath == "/tmp/images/round_trip.png"
    assert restored.image_id is None


# --------------------------------------------------------------------------- #
#  Backend schema shape for each Pydantic model in types/                    #
# --------------------------------------------------------------------------- #


class _RowIdModel(BaseModel):
    row_id: int = Field(default=-1, ge=-1)


@_handle_project
def test_backend_schema_raw_ref_field_shape():
    class _RawRefRow(_RowIdModel):
        ref: RawImageRef

    # Provision context with nested schema for RawImageRef
    try:
        ctxs = db.get_active_context()
        base_ctx = ctxs.get("write") if isinstance(ctxs, dict) else None
    except Exception:
        base_ctx = None
    ctx = f"{base_ctx}/SchemaRawImageRef" if base_ctx else "SchemaRawImageRef"
    store = TableStore(
        ctx,
        unique_keys={"row_id": "int"},
        auto_counting={"row_id": None},
        description="Schema test for RawImageRef field",
        fields=model_to_fields(_RawRefRow),
    )
    store.ensure_context()

    fields = db.get_fields(context=ctx)
    assert "ref" in fields and "image_id" in str(fields["ref"].get("data_type"))

    # A payload matching the nested schema is accepted
    valid = {"ref": {"image_id": 123}}
    _ = db.log(context=ctx, **valid, new=True, mutable=True)


@_handle_project
def test_backend_schema_annotated_ref_field_shape():
    class _AnnRefRow(_RowIdModel):
        ref: AnnotatedImageRef

    try:
        ctxs = db.get_active_context()
        base_ctx = ctxs.get("write") if isinstance(ctxs, dict) else None
    except Exception:
        base_ctx = None
    ctx = (
        f"{base_ctx}/SchemaAnnotatedImageRef" if base_ctx else "SchemaAnnotatedImageRef"
    )
    store = TableStore(
        ctx,
        unique_keys={"row_id": "int"},
        auto_counting={"row_id": None},
        description="Schema test for AnnotatedImageRef field",
        fields=model_to_fields(_AnnRefRow),
    )
    store.ensure_context()

    fields = db.get_fields(context=ctx)
    dtype = str(fields["ref"].get("data_type"))
    assert "raw_image_ref" in dtype and "annotation" in dtype and "image_id" in dtype

    # A payload matching the nested schema is accepted
    valid = {"ref": {"raw_image_ref": {"image_id": 5}, "annotation": "note"}}
    _ = db.log(context=ctx, **valid, new=True, mutable=True)


@_handle_project
def test_backend_schema_refs_field_shape():
    class _ImageRefsRow(_RowIdModel):
        refs: ImageRefs

    try:
        ctxs = db.get_active_context()
        base_ctx = ctxs.get("write") if isinstance(ctxs, dict) else None
    except Exception:
        base_ctx = None
    ctx = f"{base_ctx}/SchemaImageRefs" if base_ctx else "SchemaImageRefs"
    store = TableStore(
        ctx,
        unique_keys={"row_id": "int"},
        auto_counting={"row_id": None},
        description="Schema test for ImageRefs field",
        fields=model_to_fields(_ImageRefsRow),
    )
    store.ensure_context()

    fields = db.get_fields(context=ctx)
    dtype = str(fields["refs"].get("data_type"))
    assert "raw_image_ref" in dtype and "annotation" in dtype and "image_id" in dtype

    # A payload matching the nested schema is accepted
    valid = {
        "refs": [
            {"image_id": 1},
            {"raw_image_ref": {"image_id": 2}, "annotation": "a"},
        ],
    }
    _ = db.log(context=ctx, **valid, new=True, mutable=True)


@_handle_project
def test_backend_schema_raw_refs_field_shape():
    class _RawImageRefsRow(_RowIdModel):
        refs: RawImageRefs

    try:
        ctxs = db.get_active_context()
        base_ctx = ctxs.get("write") if isinstance(ctxs, dict) else None
    except Exception:
        base_ctx = None
    ctx = f"{base_ctx}/SchemaRawImageRefs" if base_ctx else "SchemaRawImageRefs"
    store = TableStore(
        ctx,
        unique_keys={"row_id": "int"},
        auto_counting={"row_id": None},
        description="Schema test for RawImageRefs field",
        fields=model_to_fields(_RawImageRefsRow),
    )
    store.ensure_context()

    fields = db.get_fields(context=ctx)
    dtype = str(fields["refs"].get("data_type"))
    assert "image_id" in dtype and "raw_image_ref" not in dtype  # raw-only entries

    # A payload matching the nested schema is accepted
    valid = {"refs": [{"image_id": 10}, {"image_id": 11}]}
    _ = db.log(context=ctx, **valid, new=True, mutable=True)


@_handle_project
def test_backend_schema_annotated_refs_field_shape():
    class _AnnotatedImageRefsRow(_RowIdModel):
        refs: AnnotatedImageRefs

    try:
        ctxs = db.get_active_context()
        base_ctx = ctxs.get("write") if isinstance(ctxs, dict) else None
    except Exception:
        base_ctx = None
    ctx = (
        f"{base_ctx}/SchemaAnnotatedImageRefs"
        if base_ctx
        else "SchemaAnnotatedImageRefs"
    )
    store = TableStore(
        ctx,
        unique_keys={"row_id": "int"},
        auto_counting={"row_id": None},
        description="Schema test for AnnotatedImageRefs field",
        fields=model_to_fields(_AnnotatedImageRefsRow),
    )
    store.ensure_context()

    fields = db.get_fields(context=ctx)
    dtype = str(fields["refs"].get("data_type"))
    assert "raw_image_ref" in dtype and "annotation" in dtype and "image_id" in dtype

    # A payload matching the nested schema is accepted
    valid = {"refs": [{"raw_image_ref": {"image_id": 20}, "annotation": "z"}]}
    _ = db.log(context=ctx, **valid, new=True, mutable=True)


@_handle_project
def test_backend_schema_image_field_shape():
    class _ImageRow(_RowIdModel):
        entry: Image

    try:
        ctxs = db.get_active_context()
        base_ctx = ctxs.get("write") if isinstance(ctxs, dict) else None
    except Exception:
        base_ctx = None
    ctx = f"{base_ctx}/SchemaImageField" if base_ctx else "SchemaImageField"
    store = TableStore(
        ctx,
        unique_keys={"row_id": "int"},
        auto_counting={"row_id": None},
        description="Schema test for Image field",
        fields=model_to_fields(_ImageRow),
    )
    store.ensure_context()

    fields = db.get_fields(context=ctx)
    dtype = str(fields["entry"].get("data_type"))
    assert "timestamp" in dtype and "data" in dtype

    # A payload matching the nested schema is accepted (deterministic base64 PNG)
    png_b64 = make_solid_png_base64(32, 32, (1, 2, 3))
    valid = {
        "entry": {
            "timestamp": datetime.now(UTC).isoformat(),
            "caption": "tiny sample",
            "data": png_b64,
        },
    }
    _ = db.log(context=ctx, **valid, new=True, mutable=True)
