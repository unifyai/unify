# Provider proxy: supported endpoint templates

The workspace file proxy sits in front of the full Microsoft Graph and Google Drive
REST APIs. It classifies each request into one of four enforcement modes:

| Classification | Behavior |
|---|---|
| `non_file` | Forward unchanged (calendar, mail, contacts, …) |
| `unknown_file` | **403** default-deny — shape looks like file/drive but is not in the allowlist |
| `file_read` | Gate reads; filter listings; masked items → **404** |
| `file_write` | Gate mutations; masked targets → **403** or **404** |

Gating is **opt-in**: with no Console file-access policy configured, all recognized
file endpoints pass through unrestricted. Once a policy exists, items are evaluated
via ancestry against the allowlist.

Gmail, Calendar, People, and other Google APIs reached via `GOOGLE_API_BASE`
(`…/google/calendar/v3/…`, etc.) are **`non_file`** passthrough — not gated by
this module. Only `drive/v3` and `upload/drive/v3` paths are file-domain.

## Finite template model

Real APIs expose infinitely many `(driveId, itemId)` pairs. The classifier only
recognizes a **finite set of path templates** (drive base + item suffix). Each
template maps to a gating strategy; anything else is `unknown_file`.

### Microsoft Graph drive bases

| Base pattern | `drive_id` sentinel | Example |
|---|---|---|
| `/me/drive` | `me` | `me/drive/root/children` |
| `/me/drives` | — (root listing) | `me/drives` |
| `/drives` | — (root listing) | `drives` |
| `/drives/{drive-id}` | `{drive-id}` | `drives/D1/items/I1/content` |
| `/sites/{site-id}/drive` | `site:{site-id}` | `sites/S1/drive/root/delta` |
| `/sites/{site-id}/drives` | — (root listing) | `sites/S1/drives` |
| `/sites/{site-id}/drives/{drive-id}` | `{drive-id}` | `sites/S1/drives/D1/root/children` |
| `/groups/{group-id}/drive` | `group:{group-id}` | `groups/G1/drive/root/children` |
| `/groups/{group-id}/drives` | — (root listing) | `groups/G1/drives` |
| `/users/{user-id}/drive` | `user:{user-id}` | `users/U1/drive/items/I1` |
| `/users/{user-id}/drives` | — (root listing) | `users/U1/drives` |
| `/shares/{share-id}` | `share:{share-id}` | metadata only |
| `/shares/{share-id}/driveItem` | `share:{share-id}` | `…/driveItem/children` |

Path-addressing forms (`root:/HR:/children`, `items/{id}:/rel:/content`) are
supported for all drive bases above, including site/group/user/share containers.

### Microsoft item suffixes (same across all bases)

| Suffix | Gating |
|---|---|
| `` (bare drive), `root` | Drive metadata; no item gate |
| `root/children`, `items/{id}/children`, `driveItem/children` | Listing → per-item filter |
| `items/{id}`, `root` | Item read/write → `is_allowed` |
| `items/{id}/content`, path `:/content` | Content read/write → `is_allowed` |
| `root/delta`, `items/{id}/delta` | Listing → per-item filter |
| `recent`, `sharedWithMe`, `following`, `bundles` | Listing → per-item filter |
| `root/search(…)`, `items/{id}/search(…)` | Search listing → per-item filter |
| `special`, `special/{name}` | Listing / item metadata |
| `items/{id}/copy`, `createUploadSession`, `createLink`, `invite`, … | Write → `is_allowed` |
| `items/{id}/permissions`, `versions`, `thumbnails`, `preview`, … | Read/write → `is_allowed` |

Pagination: `@odata.nextLink` and `@odata.deltaLink` in listing responses are
rewritten to point back through the proxy.

### Google Drive v3

Paths are relative to `drive/v3` or `upload/drive/v3` (upload uses the same
templates and gating as the non-upload host).

#### Top-level resources

| Path | Gating |
|---|---|
| `about` | `non_file` |
| `apps`, `apps/{appId}` | `non_file` |
| `channels/stop` | `non_file` |
| `operations/{name}` | `non_file` |
| `files/generateIds`, `files/generateCseToken` | `non_file` |
| `files` (list) | Listing → per-item filter |
| `files` (POST create) | Write; parent(s) in body/query checked |
| `DELETE files/trash` | Write (empty trash) |
| `changes` (list) | Listing via `filter_changes` on embedded `file` |
| `changes/startPageToken` | Read; no item gate |
| `changes/watch` | Write |
| `drives` (list) | Root listing (shared drives) |
| `POST drives` | Write (create shared drive) |
| `drives/{driveId}` | Read/write (drive metadata) |
| `drives/{driveId}/hide`, `/unhide` | Write |
| `drives/{driveId}/emptyTrash` | Write |

#### `files/{fileId}` sub-resources

| Suffix | Gating |
|---|---|
| `` (bare file) | Read/write; `?alt=media` → content |
| `/export` | Content read → `is_allowed` |
| `/copy`, `/trash`, `/watch`, `/download` | Write → `is_allowed` |
| `/permissions`, `/permissions/{id}` | Read/write → `is_allowed` |
| `/revisions`, `/revisions/{id}` | Read/write → `is_allowed` |
| `/listLabels` | Read → `is_allowed` |
| `/modifyLabels` | Write → `is_allowed` |
| `/comments`, `/comments/{id}` | Read/write → `is_allowed` |
| `/comments/{id}/replies`, `…/replies/{id}` | Read/write → `is_allowed` |
| `/accessproposals`, `/accessproposals/{id}`, `…/{id}:resolve` | Read/write → `is_allowed` |
| `/approvals`, `/approvals/{id}`, `/approvals:start`, `…/{id}:approve`, etc. | Read/write → `is_allowed` |

Upload host examples: `POST upload/drive/v3/files`, `PATCH upload/drive/v3/files/{fileId}`.

## Adding a new pattern

1. Add the template to `_ms_parse_drive_base` / `_classify_ms_drive_tail` or the
   `_google_*` helpers in `classify.py`.
2. If the response is a listing with a non-standard shape, add or extend a filter
   in `filter.py` (and wire it in `proxy.py`).
3. If Microsoft path resolution needs a new container, extend `_ms_drive_base` in
   `ancestry.py`.
4. Add parametrized tests in `tests/provider_proxy/test_classify.py` and, when
   filtering applies, `test_proxy_enforcement.py` / `test_filter.py`.

Unknown suffixes remain **default-deny** until explicitly allowlisted.
