#!/usr/bin/env bash
#
# Provision the two artifacts CanvasManager needs in order to publish a canvas:
#
#   $TOOLCHAIN_ROOT  a node workspace with typescript, esbuild and the kit's
#                    type declarations, used by `build_ops.build_canvas`
#   $HOST_ROOT       the built runtime host plus its header helper, used by
#                    `review_ops.render_and_review`
#
# Both come out of the branding monorepo, which owns @unity/canvas-kit and the
# runtime host. They are installed rather than fetched at runtime, so authoring a
# canvas needs no network and no separate build service.
#
# The host assets are byte-identical to what the canvas origin serves and the
# response headers are computed by the same script the deploy uses. That is the
# point of vendoring rather than reimplementing: an author-time render running a
# different runtime or a laxer CSP than production would pass canvases that then
# fail for real viewers.
#
# Usage:
#   scripts/install-canvas-toolchain.sh [options]
#
#     --branding <path>   existing branding checkout; cloned when omitted
#     --branch <ref>      branch to clone (default: main)
#     --toolchain <dir>   default /opt/canvas-toolchain
#     --host <dir>        default /opt/canvas-host
#     --keep-sources      leave the clone in place (debugging)
#
set -euo pipefail

BRANDING_PATH=""
BRANCH="main"
TOOLCHAIN_ROOT="/opt/canvas-toolchain"
HOST_ROOT="/opt/canvas-host"
KEEP_SOURCES=0
CLONED_DIR=""

# Pinned to the versions branding resolves, so a canvas is typechecked and
# bundled by the same toolchain that built the kit it links against.
TYPESCRIPT_VERSION="5.9.3"
ESBUILD_VERSION="0.28.1"
# React 18 deliberately, not the 19 hoisted at the branding root: the runtime
# host and console both ship 18, and typechecking against newer declarations
# would admit APIs that are absent at view time.
REACT_VERSION="18.3.1"
REACT_TYPES_VERSION="18.3.12"
RECHARTS_VERSION="3.10.1"
CLSX_VERSION="2.1.1"
TAILWIND_MERGE_VERSION="2.6.1"
# The vocabulary substrate: what inlined shadcn component source compiles
# against. Kept in step with apps/canvas-host/package.json in branding — the
# toolchain typechecks against the same versions the runtime host vendors.
CVA_VERSION="0.7.1"
LUCIDE_VERSION="0.475.0"
RADIX_PACKAGES="accordion alert-dialog aspect-ratio avatar checkbox collapsible dialog dropdown-menu hover-card label menubar navigation-menu popover progress radio-group scroll-area select separator slider slot switch tabs toggle toggle-group tooltip"

while [[ $# -gt 0 ]]; do
    case "$1" in
        --branding) BRANDING_PATH="$2"; shift 2 ;;
        --branch) BRANCH="$2"; shift 2 ;;
        --toolchain) TOOLCHAIN_ROOT="$2"; shift 2 ;;
        --host) HOST_ROOT="$2"; shift 2 ;;
        --keep-sources) KEEP_SOURCES=1; shift ;;
        *) echo "unknown option: $1" >&2; exit 2 ;;
    esac
done

command -v node >/dev/null || { echo "node is required" >&2; exit 1; }
command -v npm >/dev/null || { echo "npm is required" >&2; exit 1; }

cleanup() {
    if [[ -n "$CLONED_DIR" && "$KEEP_SOURCES" -eq 0 ]]; then
        rm -rf "$CLONED_DIR"
    fi
    cleanup_build
}

# Defined early so the EXIT trap can call it before the host build assigns it.
BUILD_DIR=""
cleanup_build() { :; }
trap cleanup EXIT

if [[ -z "$BRANDING_PATH" ]]; then
    CLONED_DIR="$(mktemp -d)/branding"
    echo ">> cloning branding@${BRANCH}"
    git clone --depth 1 --branch "$BRANCH" \
        https://github.com/unifyai/branding.git "$CLONED_DIR"
    # packages/iso is a submodule, so a plain clone leaves it as an empty
    # directory and npm then resolves @unity/iso from the public registry (404)
    # instead of the workspace. Only this one is initialised; the repo's other
    # submodule is editor tooling the build does not need.
    git -C "$CLONED_DIR" submodule update --init --depth 1 packages/iso
    BRANDING_PATH="$CLONED_DIR"
fi

BRANDING_PATH="$(cd "$BRANDING_PATH" && pwd)"
KIT_SRC="$BRANDING_PATH/packages/canvas-kit"
HOST_SRC="$BRANDING_PATH/apps/canvas-host"

[[ -d "$KIT_SRC" ]] || { echo "missing $KIT_SRC" >&2; exit 1; }
[[ -d "$HOST_SRC" ]] || { echo "missing $HOST_SRC" >&2; exit 1; }

KIT_VERSION="$(node -p "require('$KIT_SRC/package.json').version")"

# --------------------------------------------------------------------------
# Runtime host
# --------------------------------------------------------------------------
# Built in a copy, never in the checkout we were pointed at. Installing in place
# leaves a `node_modules` behind, and when the checkout is console's `branding`
# submodule that tree carries React 19 types which shadow console's React 18 and
# break `tsc` across the whole console repo. It is gitignored, so it looks clean
# while doing it.
#
# The copy is only the four workspaces the host needs — a few megabytes out of a
# half-gigabyte checkout — so this is also faster than installing the full
# workspace set. `--ignore-scripts` skips native postinstall builds (potrace,
# fontkit) that the host does not need and that do not compile in a slim image.
BUILD_DIR=""
cleanup_build() {
    if [[ -n "$BUILD_DIR" && "$KEEP_SOURCES" -eq 0 ]]; then
        rm -rf "$BUILD_DIR"
    fi
}

echo ">> building the canvas runtime host"
BUILD_DIR="$(mktemp -d)/branding-build"
mkdir -p "$BUILD_DIR/packages" "$BUILD_DIR/apps"
cp "$BRANDING_PATH/package.json" "$BUILD_DIR/package.json"
for workspace in packages/iso packages/brand packages/canvas-kit apps/canvas-host; do
    # package.json, not just the directory: an uninitialised submodule is an
    # empty directory that passes -d and then fails npm resolution far away.
    if [[ ! -f "$BRANDING_PATH/$workspace/package.json" ]]; then
        echo "workspace $workspace in $BRANDING_PATH is missing or empty" \
             "(uninitialised submodule?)" >&2
        cleanup_build
        exit 1
    fi
    # Excludes rather than a bare copy so a stale dist or an unrelated
    # node_modules in the source cannot leak into the build.
    tar -C "$BRANDING_PATH" \
        --exclude=node_modules --exclude=dist --exclude=.turbo \
        -cf - "$workspace" | tar -C "$BUILD_DIR" -xf -
done

(cd "$BUILD_DIR" && npm install --ignore-scripts --no-audit --no-fund) || {
    cleanup_build
    exit 1
}
(cd "$BUILD_DIR" && npm run build --workspace @unity/canvas-host) || {
    cleanup_build
    exit 1
}

rm -rf "$HOST_ROOT"
mkdir -p "$HOST_ROOT/scripts"
cp -R "$BUILD_DIR/apps/canvas-host/dist/host" "$HOST_ROOT/host"
cp "$BUILD_DIR/apps/canvas-host/scripts/headers.mjs" "$HOST_ROOT/scripts/headers.mjs"
cleanup_build
BUILD_DIR=""

# --------------------------------------------------------------------------
# Build toolchain
# --------------------------------------------------------------------------
echo ">> installing the canvas build toolchain"
rm -rf "$TOOLCHAIN_ROOT"
mkdir -p "$TOOLCHAIN_ROOT"

RADIX_DEPS=""
for pkg in $RADIX_PACKAGES; do
    RADIX_DEPS="${RADIX_DEPS},
    \"@radix-ui/react-${pkg}\": \"^1\""
done

cat > "$TOOLCHAIN_ROOT/package.json" <<JSON
{
  "name": "@unity/canvas-toolchain",
  "version": "${KIT_VERSION}",
  "private": true,
  "description": "Pinned toolchain CanvasManager compiles authored canvases with.",
  "dependencies": {
    "typescript": "${TYPESCRIPT_VERSION}",
    "esbuild": "${ESBUILD_VERSION}",
    "@types/react": "${REACT_TYPES_VERSION}",
    "react": "${REACT_VERSION}",
    "react-dom": "${REACT_VERSION}",
    "clsx": "${CLSX_VERSION}",
    "recharts": "${RECHARTS_VERSION}",
    "tailwind-merge": "${TAILWIND_MERGE_VERSION}",
    "class-variance-authority": "${CVA_VERSION}",
    "lucide-react": "${LUCIDE_VERSION}"${RADIX_DEPS}
  }
}
JSON

# recharts, clsx and tailwind-merge are here only so the kit's declarations can
# be emitted and then resolved; a canvas never imports them itself. react-dom is
# pinned alongside react because recharts declares both as peers, and letting npm
# resolve react-dom alone would pull in a major that disagrees with react.
(cd "$TOOLCHAIN_ROOT" && npm install --no-audit --no-fund --loglevel=error)

# The kit is distributed as TypeScript source, so declarations are emitted here
# rather than shipped. Typechecking a canvas against `.d.ts` files under
# `skipLibCheck` keeps each build near a second; pointing tsc at the kit's source
# would recompile the kit and recharts on every canvas.
echo ">> emitting @unity/canvas-kit declarations"
KIT_PKG="$TOOLCHAIN_ROOT/node_modules/@unity/canvas-kit"
STAGE="$TOOLCHAIN_ROOT/.kit-src"
rm -rf "$STAGE" "$KIT_PKG"
mkdir -p "$STAGE" "$KIT_PKG"
cp -R "$KIT_SRC/src" "$STAGE/src"

"$TOOLCHAIN_ROOT/node_modules/.bin/tsc" \
    --declaration --emitDeclarationOnly \
    --rootDir "$STAGE/src" --outDir "$KIT_PKG" \
    --target ES2020 --lib ES2020,DOM,DOM.Iterable \
    --module ESNext --moduleResolution bundler --jsx react-jsx \
    --strict --noUncheckedIndexedAccess --esModuleInterop --skipLibCheck \
    "$STAGE/src/index.ts" "$STAGE/src/protocol.ts"

rm -rf "$STAGE"

cat > "$KIT_PKG/package.json" <<JSON
{
  "name": "@unity/canvas-kit",
  "version": "${KIT_VERSION}",
  "private": true,
  "type": "module",
  "types": "./index.d.ts",
  "exports": {
    ".": { "types": "./index.d.ts" },
    "./protocol": { "types": "./protocol.d.ts" }
  }
}
JSON

# The authoring contract artifacts. build_ops reads the externals list (one
# source of truth with the host's import map) and the class manifest (a class
# the shipped stylesheet lacks silently styles nothing, so the lint refuses
# it); the vocabulary corpus is the reference source authoring guidance quotes.
node -e "import('$BRANDING_PATH/apps/canvas-host/canvas-specifiers.mjs').then(m => console.log(JSON.stringify(Object.keys(m.CANVAS_SPECIFIERS), null, 1)))" \
    > "$TOOLCHAIN_ROOT/canvas-externals.json"
cp "$HOST_ROOT"/host/*/classes.json "$TOOLCHAIN_ROOT/classes.json"
cp -R "$BRANDING_PATH/apps/canvas-host/vocabulary" "$TOOLCHAIN_ROOT/vocabulary"

# Canvas builds happen in a subdirectory of the toolchain so ordinary node
# resolution walks up into its node_modules; no baseUrl or path mapping needed.
mkdir -p "$TOOLCHAIN_ROOT/.builds"

echo
echo "canvas toolchain : $TOOLCHAIN_ROOT  (kit ${KIT_VERSION})"
echo "canvas host      : $HOST_ROOT"
