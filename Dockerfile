# syntax=docker/dockerfile:1
#
# Drop-in replacement for the official `postgres` Docker image
# (docker-library/postgres), backed by the pgrust `postgres` binary (a Rust
# port of PostgreSQL 18.3) instead of C postgres.
#
# Anyone can swap `postgres` -> `pgrust` in their `docker run` / compose and
# get the same contract: POSTGRES_PASSWORD / POSTGRES_USER / POSTGRES_DB /
# POSTGRES_INITDB_ARGS / POSTGRES_HOST_AUTH_METHOD / PGDATA env vars,
# /docker-entrypoint-initdb.d first-init scripts, PGDATA at
# /var/lib/postgresql/data (declared a VOLUME), the `postgres` unix user (uid
# 999), the /var/run/postgresql socket dir, EXPOSE 5432, STOPSIGNAL SIGINT,
# ENTRYPOINT ["docker-entrypoint.sh"], CMD ["postgres"], and gosu step-down.
#
# The ONE substitution: the user-facing SERVER is the pgrust binary. pgrust
# does not ship its own initdb/psql — the catalog bootstrap (`initdb`) and the
# init-script client (`psql`) come from the bundled PostgreSQL 18 PGDG
# packages, exactly as the official image's tooling. The C `postgres` backend
# is present too, but it is invoked ONLY by initdb's bootstrap — never as the
# user-facing server (pgrust refuses to run as root and is always launched by
# the entrypoint as the unprivileged `postgres` user).
#
# Three stages:
#   1. rustbuild — builds the pgrust `postgres` binary from this workspace
#                  (toolchain pinned by rust-toolchain.toml; rustup in the
#                  rust image resolves it automatically).
#   2. pgtools  — harvests C initdb / psql / postgres + loadable modules +
#                 libpq + the PostgreSQL share tree from the PGDG apt repo.
#   3. final    — official-postgres-compatible runtime.
#
# Build:  docker build -t pgrust .
#         (BuildKit recommended: the cargo registry/target use cache mounts,
#          so incremental rebuilds only recompile changed crates.)
# Run:    docker run --rm -e POSTGRES_PASSWORD=secret -p 5432:5432 pgrust
# Connect: psql -h localhost -U postgres
# Data dir: PGDATA defaults to /var/lib/postgresql/data (a VOLUME); mount your
#          own with -v mydata:/var/lib/postgresql/data or override -e PGDATA.
# Port:   the server listens on 5432 in-container; publish any host port with
#         -p HOSTPORT:5432 (or pass `-p <port>` server args after `postgres`).

# The C tools (initdb / psql / the bootstrap-only postgres backend), their
# loadable modules, and the share tree are kept at their PGDG-native paths in
# the final image, because the C `postgres` backend computes $libdir and the
# bki/template share dir RELATIVE to its own executable location; relocating
# it would make initdb's bootstrap backend look for dict_snowball.so etc. in
# the wrong place.
ARG PG_MAJOR=18
ARG PG_LIBROOT=/usr/lib/postgresql/18
ARG PG_SHAREDIR=/usr/share/postgresql/18

# ---------------------------------------------------------------------------
# Stage 1: build the pgrust `postgres` binary
# ---------------------------------------------------------------------------
FROM rust:1-bookworm AS rustbuild

ARG PG_SHAREDIR
ENV DEBIAN_FRONTEND=noninteractive
# Build deps:
#   build-essential/pkg-config — C toolchain (the `ssl` feature vendors and
#                                compiles OpenSSL from source via openssl-src)
#   libre2-dev — the RE2 regex engine; release-family builds REFUSE to compile
#                without it (a Spencer-only binary is a silent multi-x
#                regression on regexp-heavy shapes). Linked statically.
# ICU and libxml2 are dlopen'd at runtime — no build-time dependency.
RUN apt-get update && apt-get install -y --no-install-recommends \
        build-essential pkg-config libre2-dev \
    && rm -rf /var/lib/apt/lists/*

# Bake the runtime share dir (timezone/timezonesets/tsearch) location into the
# binary so it resolves the share tree shipped in the final image (the
# compiled-in default is /usr/local/pgsql/share, which the image doesn't have).
ENV PGRUST_PGSHAREDIR=${PG_SHAREDIR}
# Dedicated target dir, off the source tree.
ENV CARGO_TARGET_DIR=/build/target

WORKDIR /src
COPY . /src

# Cache mounts keep the cargo registry and the target dir across builds, so a
# source-only change recompiles just the changed crates. The binary and the
# staged share tree (contrib extension control/SQL files + tsearch data, staged
# by main_main's build.rs next to the binary) are copied OUT of the cache mount
# within the same RUN, because cache mounts are not part of the image.
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/build/target \
    cargo build --release --locked --bin postgres \
    && cp /build/target/release/postgres /opt/pgrust-postgres \
    && cp -r /build/target/release/share /opt/pgrust-share

# ---------------------------------------------------------------------------
# Stage 2: obtain the minimal C tools (initdb, psql, libpq, share) from PGDG
# ---------------------------------------------------------------------------
FROM debian:bookworm-slim AS pgtools

ARG PG_LIBROOT
ARG PG_SHAREDIR
ENV DEBIAN_FRONTEND=noninteractive

# Add the PGDG apt repo and install the PostgreSQL 18 server + client packages
# (server package carries initdb + postgres + the loadable modules + the
# share/ tree; client package carries psql + libpq).
RUN apt-get update && apt-get install -y --no-install-recommends \
        ca-certificates gnupg wget \
    && install -d /usr/share/postgresql-common/pgdg \
    && wget -qO /usr/share/postgresql-common/pgdg/apt.postgresql.org.asc \
        https://www.postgresql.org/media/keys/ACCC4CF8.asc \
    && echo "deb [signed-by=/usr/share/postgresql-common/pgdg/apt.postgresql.org.asc] http://apt.postgresql.org/pub/repos/apt bookworm-pgdg main" \
        > /etc/apt/sources.list.d/pgdg.list \
    && apt-get update \
    && apt-get install -y --no-install-recommends \
        postgresql-18 postgresql-client-18 \
    && rm -rf /var/lib/apt/lists/*

# Sanity-check the layout the final stage relies on, and gather the non-glibc
# shared libs the binaries pull in (libpq, libicu, libldap, ...) into a single
# dir we can copy wholesale. Everything else stays at its native path.
RUN set -eux; \
    test -x "${PG_LIBROOT}/bin/initdb"; \
    test -x "${PG_LIBROOT}/bin/psql"; \
    test -x "${PG_LIBROOT}/bin/postgres"; \
    test -f "${PG_LIBROOT}/lib/dict_snowball.so"; \
    test -f "${PG_SHAREDIR}/postgres.bki"; \
    test -d "${PG_SHAREDIR}/timezonesets"; \
    mkdir -p /opt/runlibs; \
    for b in "${PG_LIBROOT}"/bin/initdb "${PG_LIBROOT}"/bin/psql "${PG_LIBROOT}"/bin/postgres; do \
        ldd "$b" | awk '/=> \//{print $3}'; \
    done | sort -u | while read -r so; do \
        case "$so" in /lib/*|/usr/lib/*) cp -L "$so" /opt/runlibs/ || true;; esac; \
    done

# ---------------------------------------------------------------------------
# Stage 3: runtime image — drop-in compatible with the official postgres image
# ---------------------------------------------------------------------------
FROM debian:bookworm-slim AS final

ARG PG_MAJOR
ARG PG_LIBROOT
ARG PG_SHAREDIR
ENV DEBIAN_FRONTEND=noninteractive

# The "postgres" user/group with uid/gid 999, exactly like the official image —
# so a host mounting the data volume sees identical ownership.
RUN set -eux; \
    groupadd -r postgres --gid=999; \
    useradd -r -g postgres --uid=999 --home-dir=/var/lib/postgresql --shell=/bin/bash postgres; \
    install --verbose --directory --owner postgres --group postgres --mode 1777 /var/lib/postgresql

# Runtime packages:
#   libicu72        — dlopen'd by the pgrust binary (ICU collation provider)
#   libxml2         — dlopen'd by the pgrust binary (xml type support)
#   tzdata          — system zoneinfo (Debian PG is --with-system-tzdata;
#                     pgrust reads it via PGRUST_TZDIR)
#   locales         — en_US.utf8 (matches the official image's LANG)
#   gosu            — root -> postgres step-down (Debian package, like
#                     upstream; pgrust refuses to run as root)
#   libnss-wrapper  — fakes the current uid in /etc/passwd for initdb under --user
#   xz-utils/zstd/gzip — decompress *.sql.{xz,zst,gz} init scripts
RUN set -eux; \
    apt-get update; \
    apt-get install -y --no-install-recommends \
        libicu72 libxml2 tzdata locales libnss-wrapper xz-utils zstd gzip \
        gosu \
    ; \
    rm -rf /var/lib/apt/lists/*; \
    # verify gosu works (and that the "nobody" user resolves) like the official image \
    gosu nobody true; \
    localedef -i en_US -c -f UTF-8 -A /usr/share/locale/locale.alias en_US.UTF-8 || true
ENV LANG=en_US.utf8

ENV PG_MAJOR=${PG_MAJOR}

# The pgrust binary (this IS the user-facing server). It is named
# `pgrust-postgres` so the C `postgres` (needed by initdb) keeps the bare
# `postgres` name on PATH; the entrypoint launches the server by this path.
COPY --from=rustbuild /opt/pgrust-postgres /usr/local/bin/pgrust-postgres
# pgrust's own contrib extension files + tsearch data, staged by the build
# next to the binary (CREATE EXTENSION resolves <exe dir>/share/extension
# first, before the PGDG share tree).
COPY --from=rustbuild /opt/pgrust-share /usr/local/bin/share

# The C tools at their PGDG-native paths (so the bootstrap backend's relative
# $libdir / share-dir computation resolves correctly):
#   ${PG_LIBROOT}/bin   — initdb, psql, the bootstrap-only postgres backend
#   ${PG_LIBROOT}/lib   — loadable modules dlopen'd during initdb (dict_snowball)
#   ${PG_SHAREDIR}      — timezonesets + initdb bootstrap templates (postgres.bki, ...)
COPY --from=pgtools ${PG_LIBROOT} ${PG_LIBROOT}
COPY --from=pgtools ${PG_SHAREDIR} ${PG_SHAREDIR}
# The non-glibc shared libs those binaries link (libpq, libicu, libldap, ...).
COPY --from=pgtools /opt/runlibs /opt/runlibs
ENV PATH=${PG_LIBROOT}/bin:$PATH
ENV LD_LIBRARY_PATH=/opt/runlibs

# Debian's PostgreSQL is built --with-system-tzdata, so the package share dir
# ships NO `timezone/` subtree. The pgrust binary resolves the tz database via
# PGRUST_TZDIR (and ${PGRUST_PGSHAREDIR}/timezone), so point both at the
# system tzdata; timezonesets ships in the PGDG share dir. The
# /usr/share/timezonesets symlink covers the PGRUST_TZDIR-parent fallback for
# anyone overriding the env.
ENV PGRUST_PGSHAREDIR=${PG_SHAREDIR}
ENV PGRUST_TZDIR=/usr/share/zoneinfo
RUN test -d /usr/share/zoneinfo \
    && ln -snf /usr/share/zoneinfo "${PG_SHAREDIR}/timezone" \
    && ln -snf "${PG_SHAREDIR}/timezonesets" /usr/share/timezonesets

# pgrust's per-statement frames are large; the Rust default main/thread stacks
# refuse to boot (the entrypoint also raises `ulimit -s`).
ENV RUST_MIN_STACK=33554432

# Official-image data dir + socket dir conventions.
ENV PGDATA=/var/lib/postgresql/data
RUN install --verbose --directory --owner postgres --group postgres --mode 1777 /var/lib/postgresql/data
RUN mkdir -p /var/run/postgresql && chown -R postgres:postgres /var/run/postgresql && chmod 3777 /var/run/postgresql

# The entrypoint, installed under the official name so `docker-entrypoint.sh`
# on PATH works for users who reference it explicitly.
COPY docker/entrypoint.sh /usr/local/bin/docker-entrypoint.sh
RUN chmod 0755 /usr/local/bin/docker-entrypoint.sh \
    && ln -snf /usr/local/bin/docker-entrypoint.sh /docker-entrypoint.sh # backwards compat

# First-init scripts dir (official image convention).
RUN mkdir -p /docker-entrypoint-initdb.d

VOLUME /var/lib/postgresql/data

EXPOSE 5432
STOPSIGNAL SIGINT
ENTRYPOINT ["docker-entrypoint.sh"]
# Default: run the server (the entrypoint rewrites this to the pgrust binary).
CMD ["postgres"]
