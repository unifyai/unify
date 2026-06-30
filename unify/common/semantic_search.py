from __future__ import annotations

from typing import List, Tuple, Dict, Optional
import json
import hashlib

import unisdk

from .embed_utils import (
    EMBED_MODEL,
    ensure_vector_column,
    ensure_derived_column,
    list_private_fields,
    escape_single_quotes,
)


def is_plain_identifier(expr: str) -> bool:
    return ("{" not in expr) and ("}" not in expr) and any(c.isalpha() for c in expr)


def _build_term_equations(embed_col: str, ref_text: str) -> tuple[str, str]:
    """Return (numerator_term, denominator_term) equations for one (col, text)."""
    escaped_ref = escape_single_quotes(ref_text)
    num = (
        "((cosine({lg:"
        + embed_col
        + "}, "
        + f"embed('{escaped_ref}', model='{EMBED_MODEL}'))) "
        + f"if exists({{lg:{embed_col}}}) else 0)"
    )
    den = f"(1 if exists({{lg:{embed_col}}}) else 0)"
    return num, den


def wrap_str_placeholders(expr: str) -> str:
    """Wrap str({field}) → ((str({field})) if exists({field}) else '') to avoid 'None'."""
    import re as _re

    pattern = _re.compile(r"str\(\{\s*([a-zA-Z_][\w]*)\s*\}\)")

    def _repl(m: _re.Match[str]) -> str:
        fld = m.group(1)
        return f"((str({{{fld}}})) if exists({{{fld}}}) else '')"

    return pattern.sub(_repl, expr)


def ensure_vector_for_source(
    context: str,
    source_expr: str,
    *,
    project: Optional[str] = None,
) -> str:
    """Ensure an embedding column exists for source_expr within context and return its name.

    - If source_expr is a plain identifier, use that column directly and create `_{col}_emb`.
    - If source_expr is an expression, create a stable derived source column `_expr_<hash>` and
      then create the embedding for it.
    """
    if is_plain_identifier(source_expr):
        source_column_name = source_expr
        embed_column_name = f"_{source_column_name}_emb"
        ensure_vector_column(
            context,
            embed_column=embed_column_name,
            source_column=source_column_name,
            derived_expr=None,
            project=project,
        )
    else:
        expr_hash = hashlib.sha256(source_expr.encode("utf-8")).hexdigest()[:10]
        source_column_name = f"_expr_{expr_hash}"
        embed_column_name = f"{source_column_name}_emb"
        sanitized_expr = wrap_str_placeholders(source_expr)
        ensure_vector_column(
            context,
            embed_column=embed_column_name,
            source_column=source_column_name,
            derived_expr=sanitized_expr,
            project=project,
        )
    return embed_column_name


def embed_column_for_source(source_expr: str) -> str:
    """Return the canonical embedding column name for a source expression."""
    if is_plain_identifier(source_expr):
        return f"_{source_expr}_emb"
    expr_hash = hashlib.sha256(source_expr.encode("utf-8")).hexdigest()[:10]
    return f"_expr_{expr_hash}_emb"


def resolve_existing_vector_for_source(
    context: str,
    source_expr: str,
    *,
    project: Optional[str] = None,
) -> Optional[str]:
    """Return the embedding column for ``source_expr`` only if it already exists.

    Read-only counterpart to :func:`ensure_vector_for_source` for contexts the
    caller cannot write to (public-read catalogues): the column is looked up
    but never created. Returns ``None`` when absent, meaning the term simply
    has no embeddings in this context.
    """
    embed_column_name = embed_column_for_source(source_expr)
    fields = unisdk.get_fields(context=context, project=project)
    return embed_column_name if embed_column_name in fields else None


def extract_placeholders(expr: str) -> list[str]:
    """Return placeholder field names inside a source expression.

    Example: "str({content}).lower()" -> ["content"].
    """
    import re as _re

    return _re.findall(r"\{\s*([a-zA-Z_][\w]*)\s*\}", expr or "")


def ensure_join_context(
    *,
    left_ctx: str,
    right_ctx: str,
    join_expr: str,
    new_context: str,
    columns: Dict[str, str],
    mode: str = "inner",
    copy: bool = True,
) -> str:
    """Materialize a joined context with aliased columns and return its name.

    This small wrapper standardizes aliasing/copy semantics so downstream code can
    reference bare column names in the joined context without fully qualified paths.
    """
    unisdk.join_logs(
        pair_of_args=(
            {"context": left_ctx},
            {"context": right_ctx},
        ),
        join_expr=join_expr,
        mode=mode,
        new_context=new_context,
        columns=columns,
        copy=copy,
    )
    return new_context


def ensure_mean_cosine_column(
    context: str,
    terms: List[Tuple[str, str]],
    seed: str,
    *,
    project: Optional[str] = None,
) -> str:
    """Create a mean-of-cosines derived column over provided (embed_col, ref_text) terms.

    - Each term contributes its cosine distance when the embedding exists, otherwise 0.
    - The final value is (sum of present cosines) / (count of present cosines).
    - If no term is present for a row, the value is 2 (maximal distance).

    Returns the created (or existing) column key.
    """
    # Build numerator and denominator parts
    num_terms: list[str] = []
    den_terms: list[str] = []
    for embed_col, ref_text in terms:
        num, den = _build_term_equations(embed_col, ref_text)
        num_terms.append(num)
        den_terms.append(den)

    sum_key = f"_sum_cos_{seed}"
    numerator = " + ".join(num_terms) if num_terms else "0"
    denominator = " + ".join(den_terms) if den_terms else "0"
    # mean if denom>0 else 2
    sum_equation = f"(({numerator}) / ({denominator})) if (({denominator}) > 0) else 2"

    ensure_derived_column(
        context=context,
        key=sum_key,
        equation=sum_equation,
        project=project,
    )

    return sum_key


def ensure_mean_cosine_column_piecewise(
    context: str,
    terms: List[Tuple[str, str]],
    seed: str,
    *,
    project: Optional[str] = None,
) -> str:
    """Create a mean-of-cosines derived column using a single zero-shot equation.

    For backend compatibility and simplicity, this delegates to
    ``ensure_mean_cosine_column`` so that the full equation is evaluated
    server-side without creating any intermediate columns.

    Returns the created (or existing) mean column key.
    """
    return ensure_mean_cosine_column(context, terms, seed, project=project)


def ensure_mean_cosine_column_piecewise_named(
    context: str,
    terms: List[Tuple[str, str]],
    seed: str,
    *,
    public_prefix: str = "score_",
    project: Optional[str] = None,
) -> str:
    """Create a private mean-of-cosines column for the provided terms.

    Backward-compatible wrapper that now delegates to ``ensure_mean_cosine_column``
    to build a single zero-shot equation and returns its private key (underscored).
    The ``public_prefix`` parameter is ignored.

    Returns the created (or existing) private sum column key.
    """
    return ensure_mean_cosine_column(context, terms, seed, project=project)


SORT_DISTANCE_KEY = "_sort_distance"
COMBINED_COSINE_KEY = "_combined_cosine"


def _fetch_single_term_scored(
    context: str,
    embed_col: str,
    ref_text: str,
    *,
    k: int,
    row_filter: Optional[str] = None,
    allowed_fields: Optional[List[str]] = None,
    project: Optional[str] = None,
) -> list[dict]:
    """Fetch top-k rows ranked by inline cosine with the distance per row.

    Fully read-only: ranking uses the backend ANN sort with
    ``return_sort_distance`` instead of a derived score column, so it works
    against public-read contexts and creates no per-query column bloat.
    """
    escaped_ref = escape_single_quotes(ref_text)
    sorting = {
        f"cosine({embed_col}, embed('{escaped_ref}', model='{EMBED_MODEL}'))": "ascending",
    }
    if allowed_fields is not None:
        from_fields = list(dict.fromkeys([*allowed_fields, SORT_DISTANCE_KEY]))
        logs = unisdk.get_logs(
            context=context,
            project=project,
            filter=row_filter,
            sorting=sorting,
            limit=k,
            from_fields=from_fields,
            return_sort_distance=True,
        )
    else:
        exclude_fields = [
            f
            for f in list_private_fields(context, project=project)
            if f != SORT_DISTANCE_KEY
        ]
        logs = unisdk.get_logs(
            context=context,
            project=project,
            filter=row_filter,
            sorting=sorting,
            limit=k,
            exclude_fields=exclude_fields,
            return_sort_distance=True,
        )
    return [lg.entries for lg in logs]


def _context_unique_id_field(context: str, *, project: Optional[str] = None) -> str:
    """Return the unique-key column for a context (required for combining)."""
    ctx_info = unisdk.get_context(context, project=project)
    unique_keys = ctx_info.get("unique_keys")
    if isinstance(unique_keys, dict):
        unique_keys = list(unique_keys)
    if isinstance(unique_keys, list):
        unique_keys = unique_keys[0] if unique_keys else None
    if not unique_keys:
        raise ValueError(
            f"Context {context!r} has no unique key; multi-term client-side "
            "score combination requires a per-row identity column.",
        )
    return str(unique_keys)


def fetch_top_k_by_terms_combined_client_side(
    context: str,
    terms: List[Tuple[str, str]],
    *,
    k: int = 10,
    row_filter: Optional[str] = None,
    allowed_fields: Optional[List[str]] = None,
    project: Optional[str] = None,
) -> tuple[list[dict], str]:
    """Multi-term top-k via per-term read-only queries combined client-side.

    Mirrors the mean-with-missing-penalty semantics of
    ``ensure_mean_cosine_column`` (mean of cosines over terms whose embedding
    exists for the row; maximal distance 2 when none exist) without creating
    any derived column, so it works against public-read contexts.

    Candidate generation fetches each term's top-k; a row outside every
    per-term window cannot be retrieved, so results are exact whenever the
    true top-k rows each rank within the window of at least one term — the
    standard top-k union approximation.
    """
    id_field = _context_unique_id_field(context, project=project)
    fetch_fields = (
        list(dict.fromkeys([*allowed_fields, id_field]))
        if allowed_fields is not None
        else None
    )

    payload_by_id: dict[object, dict] = {}
    scores_by_id: dict[object, dict[int, float]] = {}
    for term_index, (embed_col, ref_text) in enumerate(terms):
        rows = _fetch_single_term_scored(
            context,
            embed_col,
            ref_text,
            k=k,
            row_filter=row_filter,
            allowed_fields=fetch_fields,
            project=project,
        )
        for row in rows:
            row_id = row.get(id_field)
            if row_id is None:
                continue
            distance = row.pop(SORT_DISTANCE_KEY, None)
            payload_by_id.setdefault(row_id, row)
            if distance is not None:
                scores_by_id.setdefault(row_id, {})[term_index] = float(distance)

    # Fill in exact scores for candidates that fell outside a term's window.
    for term_index, (embed_col, ref_text) in enumerate(terms):
        missing = [
            row_id
            for row_id in payload_by_id
            if term_index not in scores_by_id.get(row_id, {})
        ]
        if not missing:
            continue
        ids_expr = ", ".join(repr(row_id) for row_id in missing)
        id_filter = f"{id_field} in [{ids_expr}]"
        combined_filter = (
            f"({row_filter}) and ({id_filter})" if row_filter else id_filter
        )
        rows = _fetch_single_term_scored(
            context,
            embed_col,
            ref_text,
            k=len(missing),
            row_filter=combined_filter,
            allowed_fields=[id_field],
            project=project,
        )
        for row in rows:
            row_id = row.get(id_field)
            distance = row.get(SORT_DISTANCE_KEY)
            if row_id is None or distance is None:
                continue
            scores_by_id.setdefault(row_id, {})[term_index] = float(distance)

    ranked: list[dict] = []
    for row_id, payload in payload_by_id.items():
        present = scores_by_id.get(row_id, {})
        mean = sum(present.values()) / len(present) if present else 2.0
        combined = dict(payload)
        combined[COMBINED_COSINE_KEY] = mean
        ranked.append(combined)
    ranked.sort(key=lambda row: row[COMBINED_COSINE_KEY])
    return ranked[:k], COMBINED_COSINE_KEY


def fetch_top_k_by_terms_with_score(
    context: str,
    terms: List[Tuple[str, str]],
    *,
    k: int = 10,
    row_filter: Optional[str] = None,
    allowed_fields: Optional[List[str]] = None,
    project: Optional[str] = None,
) -> tuple[list[dict], str]:
    """Return top-k rows plus the private score column key for provided terms.

    The score column remains private (underscored). Internally, this function
    includes that private column in the returned payload to enable downstream
    consumers to combine scores, while still excluding all other private fields.

    Multi-term searches against a foreign ``project`` (a public-read context
    such as the builtins catalogue, where derived score columns cannot be
    written) combine per-term read-only queries client-side instead.
    """
    if len(terms) == 0:
        return [], ""

    if len(terms) == 1:
        embed_col, ref_text = terms[0]
        rows = _fetch_single_term_scored(
            context,
            embed_col,
            ref_text,
            k=k,
            row_filter=row_filter,
            allowed_fields=allowed_fields,
            project=project,
        )
        return rows, SORT_DISTANCE_KEY

    if project is not None:
        return fetch_top_k_by_terms_combined_client_side(
            context,
            terms,
            k=k,
            row_filter=row_filter,
            allowed_fields=allowed_fields,
            project=project,
        )

    canonical = "|".join(f"{i}:{col}=>{txt}" for i, (col, txt) in enumerate(terms))
    import hashlib as _hashlib

    sum_hash = _hashlib.sha1(canonical.encode("utf-8")).hexdigest()[:12]
    sum_key = ensure_mean_cosine_column_piecewise_named(
        context,
        terms,
        sum_hash,
        project=project,
    )

    # Build read projection
    if allowed_fields is not None:
        # Ensure the private score column is included in the payload
        from_fields = list(dict.fromkeys([*allowed_fields, sum_key]))
        logs = unisdk.get_logs(
            context=context,
            project=project,
            filter=row_filter,
            sorting={sum_key: "ascending"},
            limit=k,
            from_fields=from_fields,
        )
    else:
        # Exclude all private fields except the score key we need to read
        exclude_fields = [
            f for f in list_private_fields(context, project=project) if f != sum_key
        ]
        logs = unisdk.get_logs(
            context=context,
            project=project,
            filter=row_filter,
            sorting={sum_key: "ascending"},
            limit=k,
            exclude_fields=exclude_fields,
        )
    return [lg.entries for lg in logs], sum_key


def fetch_scores_for_ids(
    context: str,
    terms: List[Tuple[str, str]],
    *,
    id_field: str,
    ids: list[int],
) -> tuple[dict[int, float], str]:
    """Fetch scores for a specific set of IDs using a private score column.

    Returns a mapping from id -> score and the score column key used.
    """
    if len(terms) == 0 or len(ids) == 0:
        return {}, ""

    canonical = "|".join(f"{i}:{col}=>{txt}" for i, (col, txt) in enumerate(terms))
    import hashlib as _hashlib

    sum_hash = _hashlib.sha1(canonical.encode("utf-8")).hexdigest()[:12]
    sum_key = ensure_mean_cosine_column_piecewise_named(context, terms, sum_hash)

    ids_expr = ", ".join(str(int(v)) for v in ids)
    id_filter = f"{id_field} in [{ids_expr}]"

    # Exclude all private fields except the score key we need to read
    exclude_fields = [f for f in list_private_fields(context) if f != sum_key]

    rows = unisdk.get_logs(
        context=context,
        filter=id_filter,
        limit=len(ids),
        exclude_fields=exclude_fields,
    )
    out: dict[int, float] = {}
    for lg in rows:
        e = lg.entries
        try:
            out[int(e[id_field])] = float(e.get(sum_key, 0))
        except Exception:
            continue
    return out, sum_key


def fetch_top_k_by_terms(
    context: str,
    terms: List[Tuple[str, str]],
    *,
    k: int = 10,
    row_filter: Optional[str] = None,
    allowed_fields: Optional[List[str]] = None,
    project: Optional[str] = None,
) -> List[dict]:
    """Return top-k rows ranked by semantic similarity given pre-embedded terms.

    terms is a list of (embed_column_name, reference_text) pairs that already exist
    in the provided context.
    """

    if len(terms) == 0:
        return []

    if len(terms) == 1:
        embed_col, ref_text = terms[0]
        escaped_ref = ref_text.replace("'", "\\'")
        if allowed_fields is not None:
            logs = unisdk.get_logs(
                context=context,
                project=project,
                filter=row_filter,
                sorting={
                    f"cosine({embed_col}, embed('{escaped_ref}', model='{EMBED_MODEL}'))": "ascending",
                },
                limit=k,
                from_fields=allowed_fields,
            )
        else:
            logs = unisdk.get_logs(
                context=context,
                project=project,
                filter=row_filter,
                sorting={
                    f"cosine({embed_col}, embed('{escaped_ref}', model='{EMBED_MODEL}'))": "ascending",
                },
                limit=k,
                exclude_fields=list_private_fields(context, project=project),
            )
        return [lg.entries for lg in logs]

    # If multiple terms are provided but the filter excludes all rows, avoid
    # creating a summed-cosine derived column which can fail on empty contexts.
    try:
        if row_filter is not None:
            any_rows = unisdk.get_logs(
                context=context,
                project=project,
                filter=row_filter,
                limit=1,
                exclude_fields=list_private_fields(context, project=project),
            )
            if not any_rows:
                return []
    except Exception:
        # If introspection fails, fall back to attempting the derived approach
        pass

    canonical = "|".join(f"{i}:{col}=>{txt}" for i, (col, txt) in enumerate(terms))
    import hashlib as _hashlib

    sum_hash = _hashlib.sha256(canonical.encode("utf-8")).hexdigest()[:12]
    sum_key = ensure_mean_cosine_column_piecewise(
        context,
        terms,
        sum_hash,
        project=project,
    )

    if allowed_fields is not None:
        logs = unisdk.get_logs(
            context=context,
            project=project,
            filter=row_filter,
            sorting={sum_key: "ascending"},
            limit=k,
            from_fields=allowed_fields,
        )
    else:
        logs = unisdk.get_logs(
            context=context,
            project=project,
            filter=row_filter,
            sorting={sum_key: "ascending"},
            limit=k,
            exclude_fields=list_private_fields(context, project=project),
        )
    return [lg.entries for lg in logs]


def fetch_top_k_by_references(
    context: str,
    references: Optional[Dict[str, str]],
    *,
    k: int = 10,
    row_filter: Optional[str] = None,
    allowed_fields: Optional[List[str]] = None,
    project: Optional[str] = None,
) -> List[dict]:
    """Return top-k rows from a context ranked by semantic similarity to reference text(s).

    This helper abstracts the common flow used by KnowledgeManager's semantic search methods:
    - Ensure an embedding column exists for each source expression (plain column or derived expr)
    - Rank by cosine when a single source is provided
    - Rank by the sum of cosine distances across multiple sources when more than one is provided
    - Exclude embedding columns ("*_emb") from the result payloads
    """
    # When no references are provided, skip semantic search entirely and
    # let the caller's backfill logic drive the result set.
    if not references:
        return []

    # Be tolerant to callers accidentally passing a JSON string for `references`
    # instead of an object. Parse it when it looks like JSON.
    if isinstance(references, str):
        s = references.strip()
        if s.startswith("{") or s.startswith("["):
            try:
                parsed = json.loads(s)
                if isinstance(parsed, dict):
                    # Ensure values are strings for the embedding reference text
                    references = {str(k): (v if isinstance(v, str) else str(v)) for k, v in parsed.items()}  # type: ignore[assignment]
                else:
                    raise TypeError
            except Exception:
                raise TypeError(
                    "`references` must be a mapping (e.g., {'bio': 'text'}) or a JSON string of such a mapping.",
                )
        else:
            raise TypeError(
                "`references` must be a mapping (e.g., {'bio': 'text'}) or a JSON string of such a mapping.",
            )

    if not isinstance(references, dict):  # defensive guard
        raise TypeError(
            "`references` must be a mapping (e.g., {'bio': 'text'}) or a JSON string of such a mapping.",
        )

    # Collect (embed_col, ref_text) pairs
    terms: List[Tuple[str, str]] = []
    for source_expr, ref_text in references.items():
        embed_col = ensure_vector_for_source(context, source_expr, project=project)
        terms.append((embed_col, ref_text))

    return fetch_top_k_by_terms(
        context,
        terms,
        k=k,
        row_filter=row_filter,
        allowed_fields=allowed_fields,
        project=project,
    )


def backfill_rows(
    context: str,
    initial_rows: List[dict],
    k: int,
    *,
    row_filter: Optional[str] = None,
    unique_id_field: Optional[str] = None,
    allowed_fields: Optional[List[str]] = None,
    project: Optional[str] = None,
) -> List[dict]:
    """Backfill similarity results with additional rows to reach k.

    Parameters
    ----------
    context : str
        Unify context to read fallback rows from.
    initial_rows : list[dict]
        Rows already obtained from semantic similarity (ordered best-first).
    k : int
        Desired total number of rows.
    row_filter : str | None, default None
        Optional row-level predicate to apply while backfilling (e.g., exclude system rows).
    unique_id_field : str | None, default None
        Column used to deduplicate rows when backfilling. When ``None``, attempts to infer
        from the context's ``unique_keys``.

    Returns
    -------
    list[dict]
        Up to ``k`` rows with original similarity results first, followed by backfilled rows.
    """
    results: List[dict] = list(initial_rows)
    if len(results) >= k:
        return results

    # Determine unique id column if not supplied
    if unique_id_field is None:
        try:
            ctx_info = unisdk.get_context(context, project=project)
            unique_id_field = ctx_info.get("unique_keys")
            if isinstance(unique_id_field, list):
                unique_id_field = unique_id_field[0] if unique_id_field else None
        except Exception:
            unique_id_field = None

    # Track already included ids to avoid duplicates
    seen_ids: set = set()
    if unique_id_field:
        for r in results:
            if unique_id_field in r and r.get(unique_id_field) is not None:
                try:
                    seen_ids.add(int(r.get(unique_id_field)))
                except Exception:
                    seen_ids.add(r.get(unique_id_field))

    # Exclude embedding/vector columns in payloads
    needed = k - len(results)
    offset = 0
    while needed > 0:
        batch_size = needed
        # Use deterministic sorting by unique_id_field to ensure consistent ordering
        # across test runs. Without explicit sorting, results depend on log_event.id
        # which can vary between sessions even for identically-seeded data.
        sorting = {unique_id_field: "descending"} if unique_id_field else None

        if allowed_fields is not None:
            # Ensure we can deduplicate
            from_fields = list(
                dict.fromkeys(
                    (
                        [*(allowed_fields or []), unique_id_field]
                        if unique_id_field
                        else (allowed_fields or [])
                    ),
                ),
            )
            fallback_logs = unisdk.get_logs(
                context=context,
                project=project,
                filter=row_filter,
                offset=offset,
                limit=batch_size,
                from_fields=from_fields,
                sorting=sorting,
            )
        else:
            try:
                fallback_logs = unisdk.get_logs(
                    context=context,
                    project=project,
                    filter=row_filter,
                    offset=offset,
                    limit=batch_size,
                    exclude_fields=list_private_fields(context, project=project),
                    sorting=sorting,
                )
            except Exception as e:
                # If the context does not exist yet (common in tests where tables are
                # created lazily), treat as "no rows to backfill" rather than failing
                # the caller's semantic search.
                try:
                    from unisdk.utils.http import RequestError as _UnifyRequestError

                    if isinstance(e, _UnifyRequestError):
                        status = getattr(
                            getattr(e, "response", None),
                            "status_code",
                            None,
                        )
                        if status == 404:
                            break
                except Exception:
                    pass
                raise
        if not fallback_logs:
            break

        fetched = 0
        for lg in fallback_logs:
            entries = getattr(lg, "entries", lg)
            if not isinstance(entries, dict):
                continue
            uid_val = entries.get(unique_id_field) if unique_id_field else None
            if unique_id_field is not None:
                try:
                    comp_val = int(uid_val) if uid_val is not None else None
                except Exception:
                    comp_val = uid_val
                if comp_val is not None and comp_val in seen_ids:
                    continue
            results.append(entries)
            fetched += 1
            if unique_id_field is not None and uid_val is not None:
                try:
                    seen_ids.add(int(uid_val))
                except Exception:
                    seen_ids.add(uid_val)
            needed -= 1
            if needed == 0:
                break

        # Advance by the number returned by the backend to avoid re-reading
        # the same page repeatedly when many rows are skipped as duplicates.
        try:
            returned = len(fallback_logs)
        except Exception:
            returned = fetched
        if returned <= 0:
            break
        offset += returned

    return results
