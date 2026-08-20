# Cross-tenant and cross-domain shared content

How the assistant reaches a file or folder that someone at **another
organisation** has shared with the connected workspace account.

Everything marked **proven** below was measured against live infrastructure,
not inferred from documentation. Everything marked **untested** is explicitly
not yet known, and the steps to settle it are given.

## The shape of the problem

A user says "read the folder Matt shared with me". Four things have to line up:

1. the item has to be **discoverable** — it appears in a listing;
2. its **provenance** has to be known — whose drive is it actually in;
3. a credential valid **for that owner** has to exist;
4. the bytes have to land somewhere ingestion can address.

Each of these failed independently at least once, and every failure produced
the same symptom: a successful HTTP response that was quietly missing things.
That is the defining hazard of this area — nothing errors.

## Microsoft 365

### Discovery

`GET /me/drive/sharedWithMe` **omits items shared from other tenants** unless
the caller passes `allowexternal=true`. Without it a live, correctly shared
folder returns `{"value":[]}`, which is indistinguishable from a revoked grant.

The parameter is applied in the provider proxy, not left to callers: an
omission that returns `200 OK` cannot be detected at the call site, and the
only caller composing these requests is a model writing code ad hoc.

**Discovery must use the home-tenant token.** `/me` has to mean the connected
account's own drive.

### Provenance

Every item returned carries `sharepointIds.tenantId`. Comparing it to the home
tenant classifies the item with no network round-trip:

| `tenantId` | kind | route |
|---|---|---|
| equals home tenant | `owned` / `shared_same_tenant` | Graph, home token |
| differs | `shared_cross_tenant` | Graph, **resource-tenant token** |
| absent (anonymous link) | `link_only` | browser lane |

Route selection is a fact check, never a ladder of hopeful attempts.

### Credentials — the resource-tenant token

**Proven.** A refresh token minted through the `/common` authority is bound to
(user, client) and **not** to a tenant, so the *same* refresh token can be
redeemed against a **foreign** tenant's token endpoint:

```
POST https://login.microsoftonline.com/{resourceTenantId}/oauth2/v2.0/token
  client_id, client_secret, grant_type=refresh_token,
  refresh_token=<the one we already store>,
  scope=https://graph.microsoft.com/Files.Read.All offline_access
```

Measured result against a real customer tenant:

```
tid : cfe84544-…            (the resource tenant, not ours)
upn : haris@unifyailtd123.onmicrosoft.com
scp : Files.Read.All profile openid email
GET /drives/{driveId}/items/{itemId}/children  ->  200, 8 children
```

Consequences for the design:

- **No new stored secrets.** One refresh token serves every tenant. Only
  short-lived access tokens are cached, keyed by tenant, in the proxy.
- **Discovery and traversal need different tokens.** With the guest token,
  `/me/drive/sharedWithMe` returns **zero** items, because `/me` now resolves
  inside the *resource* tenant where the guest has no drive of their own.
  Building on the assumption that one token does both fails silently — a
  successful call returning nothing, the same shape as every other bug here.

### Consent

The resource tenant must have consented to the app. Without it:

```
AADSTS65001: The user or administrator has not consented …
```

This is **not** a rejection of the mechanism — the token and the user both
resolved; only the grant was missing. In a real enterprise this is
**admin**-gated, so a counterparty's IT approves it once, permanently, and it
covers every environment because staging and production share one app
registration.

Two rules follow, both learned by getting them wrong first:

- **Never request `.default` for a counterparty tenant.** It asks for every
  permission the app has ever registered — mail-send, chat, site-wide write —
  and no competent admin approves that. Request the scopes for the *job*:
  `Files.Read.All offline_access` and nothing else.
- **The callback must handle the admin-consent response.** `/v2.0/adminconsent`
  redirects with `admin_consent=True&tenant=…` and **no `code`**. Our callback
  is written for the authorization-code flow, so an admin who does exactly what
  we asked lands on "Missing authorization code" — reading as failure after a
  successful grant.

### Account-type matrix

Personal Microsoft accounts (MSA) have **no tenant and no administrator**, so
the consent step above has nowhere to happen. That makes the matrix uneven:

| Owner of the content | Connected account | Mechanism | Status |
|---|---|---|---|
| Work tenant A | Work tenant A | Graph, home token | works today |
| Work tenant B | Work tenant A | guest token + admin consent in B | **proven** |
| Work tenant B | Personal (MSA) | MSA can be a B2B guest; token issued by B? | **untested** |
| Personal (MSA) | Work tenant A | no tenant to consent in; likely link-only | **untested** |
| Personal (MSA) | Personal (MSA) | consumer Graph endpoints | **untested** |

Only row two is settled. The rows involving a personal account are not
variations of it — they are a different mechanism, because the thing that makes
row two work (a tenant with an admin who can grant consent) does not exist.

Do not generalise row two to them without testing.

## Google Workspace

Google's model is **per-file ACL and user-centric**, not tenant-centric. A
user's OAuth token reaches whatever that user can see, and consent is between
the *user* and the *app* — there is no counterparty-domain approval step
equivalent to Microsoft's.

If that holds, cross-domain sharing needs **no** consent dance at all, and the
Microsoft design does not transfer. That is a strong claim resting on the
model rather than on measurement, and this area has already punished one
assumption of symmetry between the two providers. It is **untested**.

Known asymmetries to account for regardless:

- **Shared Drives** are a separate construct from "My Drive" and need
  `supportsAllDrives` / `includeItemsFromAllDrives`. These were set by every
  first-party Drive caller in the repo *except* the proxy the sandbox uses —
  the same omission as `allowexternal`, in Drive's vocabulary.
- A Workspace admin can restrict which apps **their own** users may authorise.
  That gates the *recipient's* domain, not the owner's — the mirror image of
  Microsoft, where the *owner's* tenant gates.

### Test plan

Mirrors what settled Microsoft. Two accounts are needed: a personal
`@gmail.com` and a Workspace account on a domain.

1. **Connect** the assistant's workspace to the **Workspace** account.
2. From the **personal** account, create a Drive folder with two or three
   files and share it to the Workspace account with "Restricted — specific
   people".
3. **Discover**: `files.list` with `q=sharedWithMe` plus `supportsAllDrives`
   and `includeItemsFromAllDrives`. Record whether the folder appears, and
   what owner/domain metadata comes back — this is the Drive analogue of
   `sharepointIds.tenantId` and determines how provenance is detected.
4. **Traverse**: `files.list` with `q='<folderId>' in parents`. Success here
   with the ordinary connected token is the whole question: it would mean
   cross-domain needs no consent step.
5. **Download**: `files.get?alt=media` on one file, confirming bytes and not
   just metadata.
6. **Reverse the direction**: connect the assistant to the **personal**
   account, share from the **Workspace** account, repeat 3–5. Workspace
   domains often restrict external sharing, so this direction can fail where
   the first succeeds.
7. **Shared Drive**: repeat with a file in a Shared Drive rather than My
   Drive, to confirm the all-drives opt-ins are load-bearing.

Record the exact request, status and body for each step, and do not stop on
the first failure — the pattern across all seven is what identifies the
mechanism.

## Landing the bytes

Every lane converges on the same finish: local paths handed to
`primitives.ingestion.submit`. Two things make that reliable.

The managed desktop's `/Unity/Downloads` is a **symlink into the synced tree**
(`/Unity/Local/Downloads`), so a browser download reaches the pod workspace by
construction rather than by the assistant remembering to move it. Both
spellings of the path resolve to the same bytes.

Sync otherwise runs around desktop *execution*, so a download followed
immediately by a read can race it.
`primitives.computer.collect_downloads()` forces the sync and returns the
local paths, newest first, ready to parse or ingest.

## The recurring failure mode

Four separate defects in this area produced the identical symptom: **a
successful response that was missing things**. A missing `allowexternal`, the
missing Drive opt-ins, an access-check probe that omitted the header the
request it gated would send, and an ancestry walk that returned empty for
items whose ancestry is structurally unresolvable.

None errored. Each was invisible at the call site, and each hid the others.

The rule that prevents the class: **capability opt-ins belong in the
infrastructure that knows the provider, never in code a model composes.** An
opt-in an LLM has to remember is an opt-in that silently does not happen, and
the result looks exactly like an empty result set.
