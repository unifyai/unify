# ---------------------------------------------------------------------------
# Canvas toolchain and runtime host
# ---------------------------------------------------------------------------
# Built in a separate stage so the branding monorepo's node_modules — around a
# gigabyte, most of it irrelevant here — never reaches the runtime image. Only
# two directories are copied forward.
#
# CanvasManager needs both: the toolchain to lint/typecheck/bundle authored TSX,
# and the runtime host to render the result headlessly before publishing. The
# host assets are the same bytes the canvas origin serves, so what the assistant
# reviews is what a viewer sees.
FROM node:22-slim AS canvas
ARG BRANCH=main
ARG CANVAS_BRANDING_SHA=unknown

RUN apt-get update && apt-get install -y --no-install-recommends git ca-certificates openssh-client \
    && rm -rf /var/lib/apt/lists/*

COPY scripts/install-canvas-toolchain.sh /tmp/install-canvas-toolchain.sh

# CANVAS_BRANDING_SHA busts this layer when branding moves; without it a stale
# kit would be baked in indefinitely. Branch logic mirrors the dependency clones
# below: main→main, everything else→staging.
#
# branding and its packages/iso submodule are private and are each read with
# their own repo-scoped read-only deploy key. A deploy key authorises exactly
# one repository and github binds the ssh session to the first key that
# authenticates, so offering both on one connection would let whichever
# answered first decide what the rest of the clone could reach. Each repository
# therefore gets its own host alias bound to its own identity.
#
# The keys arrive as mounted secrets rather than build arguments: an argument is
# recorded verbatim in the metadata of every layer that follows it, and a global
# git config would persist the credential in the image itself. Both the aliases
# and the rewrites are removed before the layer ends.
RUN --mount=type=secret,id=branding_deploy_key \
    --mount=type=secret,id=iso_deploy_key \
    echo "canvas_branding_sha=${CANVAS_BRANDING_SHA}" && \
    install -m 600 /run/secrets/branding_deploy_key /tmp/k_branding && \
    install -m 600 /run/secrets/iso_deploy_key /tmp/k_iso && \
    mkdir -p /root/.ssh && printf '%s\n' \
        "Host branding.gh" "  HostName github.com" "  User git" "  IdentityFile /tmp/k_branding" "  IdentitiesOnly yes" "  StrictHostKeyChecking accept-new" \
        "Host iso.gh" "  HostName github.com" "  User git" "  IdentityFile /tmp/k_iso" "  IdentitiesOnly yes" "  StrictHostKeyChecking accept-new" \
        > /root/.ssh/config && \
    git config --global url."git@branding.gh:unifyai/branding".insteadOf "https://github.com/unifyai/branding" && \
    git config --global url."git@iso.gh:unifyai/iso-animation".insteadOf "git@github.com:unifyai/iso-animation" && \
    chmod +x /tmp/install-canvas-toolchain.sh && \
    /tmp/install-canvas-toolchain.sh \
        --branch "$([ "$BRANCH" = "main" ] && echo main || echo staging)" \
        --toolchain /opt/canvas-toolchain \
        --host /opt/canvas-host; \
    status=$?; \
    git config --global --unset-all url."git@branding.gh:unifyai/branding".insteadOf; \
    git config --global --unset-all url."git@iso.gh:unifyai/iso-animation".insteadOf; \
    rm -f /tmp/k_branding /tmp/k_iso /root/.ssh/config; \
    exit $status


# Use Python 3.12 slim image as base
FROM python:3.12-slim

# Accept build arguments
ARG UNIFY_KEY
ARG BRANCH=main

# Set working directory
WORKDIR /app

# Build environment setup
ENV DEBIAN_FRONTEND=noninteractive

# Install all system dependencies from shared script (full production set)
COPY scripts/install-system-deps.sh /tmp/
RUN chmod +x /tmp/install-system-deps.sh && /tmp/install-system-deps.sh && rm /tmp/install-system-deps.sh

# Set locale environment
ENV LANG=en_US.UTF-8
ENV LC_ALL=en_US.UTF-8


# Install Node.js & npm for agent-service
RUN curl -fsSL https://deb.nodesource.com/setup_22.x | bash - \
    && apt-get install -y nodejs

# Install uv using official installation script
RUN curl -LsSf https://astral.sh/uv/install.sh | sh && \
    mv /root/.local/bin/uv /usr/local/bin/ && \
    mv /root/.local/bin/uvx /usr/local/bin/

# Copy dependency files
COPY pyproject.toml uv.lock ./

# The dependency repositories cloned below are public, so no credential is
# configured here. Do not reintroduce one: only the canvas stage reaches a
# private repository, and it uses a repo-scoped deploy key mounted for the
# single command that needs it.

# Install PyTorch CPU-only first (smaller and faster for containers)
RUN uv pip install --system --no-cache torch torchvision torchaudio --index-url https://download.pytorch.org/whl/cpu

# Clone unify and unillm repos (no PyPI releases; pyproject.toml references ../unisdk and ../unillm)
# Branch logic mirrors CI: main→main, non-main branches→staging.
# DEP_REPOS_SHA is set at build time to the latest commit SHAs of unify+unillm;
# changing it busts the Docker layer cache so stale clones are never reused.
# The value is referenced in the RUN to ensure Docker includes it in the cache key.
ARG DEP_REPOS_SHA=unknown
RUN echo "dep_repos_sha=${DEP_REPOS_SHA}" && \
    DEP_BRANCH=$([ "$BRANCH" = "main" ] && echo "main" || echo "staging") && \
    git clone --depth 1 --branch $DEP_BRANCH https://github.com/unifyai/unisdk.git /unisdk && \
    git clone --depth 1 --branch $DEP_BRANCH https://github.com/unifyai/unillm.git /unillm

# Clone and build magnitude before COPY so this expensive layer is cached when
# only application source changes.  Placed outside /app to avoid being overwritten
# by COPY; symlinked back in after COPY for agent-service's file: references.
# --ignore-scripts skips the root postinstall (turbo run build) which requires bun.
# The committed lockfile only resolves platform-specific optional deps (sharp, baml, etc.)
# for the OS it was generated on, so we delete it and let npm resolve for the target platform.
RUN git clone --depth 1 --branch main https://github.com/unifyai/magnitude.git /opt/magnitude
WORKDIR /opt/magnitude
RUN rm -f package-lock.json && npm install --ignore-scripts
RUN cd packages/magnitude-extract && npx tsup
RUN cd packages/magnitude-core && npx baml-cli generate && npx pkgroll
WORKDIR /app

# Copy source and install unify with all dependencies
COPY . /app
RUN uv pip install --system --no-cache .

# Link pre-built magnitude into the workspace (agent-service references ../magnitude)
RUN ln -sfn /opt/magnitude magnitude

RUN chmod +x /app/deploy/entrypoint.sh /app/deploy/selfhost/*.sh 2>/dev/null || true

# Build agent-service
WORKDIR /app/agent-service
RUN npm ci && npx tsc --skipLibCheck
WORKDIR /app

# Set environment variables
ENV PYTHONPATH=/app
ENV UNIFY_KEY=${UNIFY_KEY}

RUN install -m 0755 /app/scripts/sandbox-dpkg /usr/local/bin/sandbox-dpkg

# Download the turn detector model files to a fixed, user-agnostic path
# so the entrypoint can seed the emptyDir /tmp volume regardless of which user runs.
ENV OMP_NUM_THREADS=1
ENV MKL_NUM_THREADS=1
RUN HF_HOME=/opt/hf-cache python unify/conversation_manager/medium_scripts/call.py download-files

# Browsers install once into a single user-owned cache. The CM spawns agent-service
# with PLAYWRIGHT_BROWSERS_PATH set to this path, and the runtime user resolves the
# same path, so no duplicate copy is needed. Chromium is the only browser used.
ENV PLAYWRIGHT_BROWSERS_PATH=/home/unity/.cache/ms-playwright
RUN mkdir -p /home/unity/.cache/ms-playwright && playwright install chromium

# Canvas authoring artifacts from the builder stage. The render gate drives the
# chromium installed just above, which is why this follows it: without a browser
# CanvasManager would compile a canvas and then decline to publish it.
COPY --from=canvas /opt/canvas-toolchain /opt/canvas-toolchain
COPY --from=canvas /opt/canvas-host /opt/canvas-host
ENV UNITY_CANVAS_TOOLCHAIN_ROOT=/opt/canvas-toolchain
ENV UNITY_CANVAS_HOST_ROOT=/opt/canvas-host

# Install Patchright Chromium (same as deploy/desktop/Dockerfile) for non-headless
# browser sessions (e.g., Google Meet join via local browser with virtual display).
RUN cd /opt/magnitude/packages/magnitude-core && npx patchright install --with-deps chromium

# Configure Chromium as default browser so XFCE and GUI apps can find it
RUN CHROME_PATH=$(find /home/unity/.cache/ms-playwright -name "chrome" -type f -executable 2>/dev/null | head -1) && \
    printf '#!/bin/bash\nexec "%s" --no-sandbox "$@"\n' "$CHROME_PATH" > /usr/local/bin/chromium-browser && \
    chmod +x /usr/local/bin/chromium-browser && \
    printf '[Desktop Entry]\nVersion=1.0\nName=Chromium Web Browser\nExec=/usr/local/bin/chromium-browser %%U\nTerminal=false\nType=Application\nIcon=web-browser\nCategories=Network;WebBrowser;\nMimeType=text/html;text/xml;application/xhtml+xml;x-scheme-handler/http;x-scheme-handler/https;\nStartupNotify=true\n' \
        > /usr/share/applications/chromium-browser.desktop && \
    update-alternatives --install /usr/bin/x-www-browser x-www-browser /usr/local/bin/chromium-browser 100 && \
    update-alternatives --set x-www-browser /usr/local/bin/chromium-browser && \
    update-desktop-database /usr/share/applications/ 2>/dev/null || true && \
    xdg-mime default chromium-browser.desktop x-scheme-handler/http 2>/dev/null || true && \
    xdg-mime default chromium-browser.desktop x-scheme-handler/https 2>/dev/null || true && \
    xdg-mime default chromium-browser.desktop text/html 2>/dev/null || true

# XFCE helper definition so exo-open recognizes Chromium as a browser option
RUN mkdir -p /usr/share/xfce4/helpers && \
    printf '[Desktop Entry]\nVersion=1.0\nType=X-XFCE-Helper\nName=Chromium Web Browser\nIcon=web-browser\nX-XFCE-Binaries=chromium-browser;/usr/local/bin/chromium-browser;\nX-XFCE-Category=WebBrowser\nX-XFCE-Commands=%%B;\nX-XFCE-CommandsWithParameter=%%B %%s;\n' \
        > /usr/share/xfce4/helpers/chromium-browser.desktop

# XFCE configuration: suppress first-run dialogs, disable screensaver/power mgmt
RUN mkdir -p /root/.config/xfce4/xfconf/xfce-perchannel-xml && \
    printf '<?xml version="1.0" encoding="UTF-8"?>\n<channel name="xfce4-power-manager" version="1.0">\n  <property name="xfce4-power-manager" type="empty">\n    <property name="dpms-enabled" type="bool" value="false"/>\n    <property name="blank-on-ac" type="int" value="0"/>\n    <property name="dpms-on-ac-sleep" type="uint" value="0"/>\n    <property name="dpms-on-ac-off" type="uint" value="0"/>\n  </property>\n</channel>\n' \
        > /root/.config/xfce4/xfconf/xfce-perchannel-xml/xfce4-power-manager.xml && \
    printf '<?xml version="1.0" encoding="UTF-8"?>\n<channel name="xfce4-screensaver" version="1.0">\n  <property name="saver" type="empty">\n    <property name="enabled" type="bool" value="false"/>\n  </property>\n  <property name="lock" type="empty">\n    <property name="enabled" type="bool" value="false"/>\n  </property>\n</channel>\n' \
        > /root/.config/xfce4/xfconf/xfce-perchannel-xml/xfce4-screensaver.xml && \
    printf 'WebBrowser=chromium-browser\nTerminalEmulator=xfce4-terminal\n' \
        > /root/.config/xfce4/helpers.rc

# Virtual display for non-headless browser sessions (Google Meet, etc.)
ENV DISPLAY=:99

# Set runtime environment variables for memory optimization
ENV PYTHONUNBUFFERED=1
ENV OMP_NUM_THREADS=1
ENV MKL_NUM_THREADS=1
ENV TOKENIZERS_PARALLELISM=false

# Logging: Unity emoji logs to terminal, unify/unillm quiet (file traces only)
ENV UNITY_TERMINAL_LOG=true
ENV UNISDK_TERMINAL_LOG=false
ENV UNILLM_TERMINAL_LOG=false
ENV UNITY_LOG_DIR=/var/log/unity
ENV UNISDK_LOG_DIR=/var/log/unisdk
ENV UNILLM_LOG_DIR=/var/log/unillm
ENV MAGNITUDE_LOG_DIR=/var/log/magnitude
ENV MAGNITUDE_DEBUG=true

# Pre-create directories that the desktop stack needs but can't create as non-root
RUN mkdir -p /root/.vnc && chmod 777 /root /root/.vnc && \
    mkdir -p /tmp/.X11-unix && chmod 1777 /tmp/.X11-unix && \
    mkdir -p /tmp/.ICE-unix && chmod 1777 /tmp/.ICE-unix

# Create non-root user for runtime
RUN useradd -m -u 1000 unity && \
    mkdir -p /var/log/unity /var/log/unisdk /var/log/unillm /var/log/magnitude /home/unity/.cache /home/unity/.config && \
    mkdir -p /run/dbus && chmod 777 /run/dbus && \
    mkdir -p /tmp/runtime-unity && chown unity:unity /tmp/runtime-unity && chmod 700 /tmp/runtime-unity && \
    chown -R unity:unity /app /var/log/unity /var/log/unisdk /var/log/unillm /var/log/magnitude && \
    cp -r /root/.config/xfce4 /home/unity/.config/xfce4 2>/dev/null || true && \
    chown -R unity:unity /home/unity

USER unity

# Expose the ports that the applications use
# 8000: conversation manager, 3000: agent-service (Magnitude)
EXPOSE 8000 3000

# Use Tini as init system to handle signals properly
ENTRYPOINT ["/usr/bin/tini", "--"]

# Use bash to run the startup script
CMD ["/bin/bash", "/app/deploy/entrypoint.sh"]
