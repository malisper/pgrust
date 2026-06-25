# syntax=docker/dockerfile:1
#
# Drop-in replacement for the official `postgres` Docker image
# (docker-library/postgres), backed by the pgrust `postgres` binary (a Rust port
# of PostgreSQL 18.3) instead of C postgres.
#
# Anyone can swap `postgres` -> `pgrust` in their `docker run` / compose and get
# the same contract: POSTGRES_PASSWORD / POSTGRES_USER / POSTGRES_DB /
# POSTGRES_INITDB_ARGS / POSTGRES_HOST_AUTH_METHOD / PGDATA env vars,
# /docker-entrypoint-initdb.d first-init scripts, PGDATA at
# /var/lib/postgresql/data (declared a VOLUME), the `postgres` unix user (uid
# 999), the /var/run/postgresql socket dir, EXPOSE 5432, STOPSIGNAL SIGINT,
# ENTRYPOINT ["docker-entrypoint.sh"], CMD ["postgres"], and gosu step-down.
#
# The substitution: BOTH the user-facing SERVER and the catalog bootstrap
# (`initdb`) are the pgrust binary — pgrust's own `--initdb` driver (a faithful
# port of initdb.c) now does the bootstrap. The only C tool left is `psql`, the
# init-script / healthcheck client (pgrust has no psql replacement); it comes
# from the bundled PostgreSQL 18 PGDG client package. No C `postgres` backend and
# no C `initdb` are present in the final image.
#
# Three stages:
#   1. rustbuild — builds the pgrust `postgres` binary. `nodetags.h` is vendored
#                  (crates/_support/types/nodes/vendor/nodetags.h), so this stage
#                  needs no PostgreSQL source — just Rust + libicu-dev.
#   2. pgtools  — harvests C psql + libpq from the PGDG client package, plus the
#                 PostgreSQL share tree (postgres.bki, system_*.sql,
#                 system_views.sql, information_schema.sql, sql_features.txt,
#                 snowball_create.sql, config samples, tz data) that pgrust's
#                 --initdb / --boot read. NO C postgres backend, NO C initdb, NO
#                 bootstrap loadable modules.
#   3. final    — official-postgres-compatible runtime.
#
# Build:  docker build -t pgrust .
# Run:    docker run --rm -e POSTGRES_PASSWORD=secret -p 5432:5432 pgrust

# psql + libpq are kept at their PGDG-native paths in the final image; the share
# tree is kept at its PGDG-native path too (PG_SHAREDIR) because the pgrust
# binary's baked-in PGRUST_PGSHAREDIR points there for the tz tree, and the
# entrypoint passes it to `--initdb` via `-L`.
ARG PG_MAJOR=18
ARG PG_LIBROOT=/usr/lib/postgresql/18
ARG PG_SHAREDIR=/usr/share/postgresql/18

# ---------------------------------------------------------------------------
# Stage 1: build the pgrust `postgres` binary (self-contained — vendored nodetags.h)
# ---------------------------------------------------------------------------
FROM rust:1-bookworm AS rustbuild

ARG PG_SHAREDIR
ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update && apt-get install -y --no-install-recommends \
        build-essential pkg-config libicu-dev libldap2-dev libpam0g-dev libssl-dev \
    && rm -rf /var/lib/apt/lists/*

# Bake the runtime share dir (timezone/timezonesets) location into the binary so
# it resolves the tz tree shipped in the final image.
ENV PGRUST_PGSHAREDIR=${PG_SHAREDIR}
# Dedicated target dir, off the source tree.
ENV CARGO_TARGET_DIR=/build/target

WORKDIR /src
COPY . /src

RUN cargo build --release --locked --bin postgres \
    && cp /build/target/release/postgres /opt/pgrust-postgres

# ---------------------------------------------------------------------------
# Stage 2: obtain psql + libpq + the share tree from PGDG
# ---------------------------------------------------------------------------
FROM debian:bookworm-slim AS pgtools

ARG PG_LIBROOT
ARG PG_SHAREDIR
ENV DEBIAN_FRONTEND=noninteractive

# Add the PGDG apt repo and install the PostgreSQL 18 server + client packages.
# We need the CLIENT package for psql + libpq, and the SERVER package only for
# its share/ tree (postgres.bki, system_*.sql, system_views.sql,
# information_schema.sql, sql_features.txt, snowball_create.sql, config samples)
# — pgrust's --initdb / --boot read those. The server package's binaries and
# loadable modules are NOT copied into the final image.
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

# Stage the C tools the final image keeps (psql only) into a clean bin dir,
# verify the share files pgrust's --initdb needs are present, and gather the
# non-glibc shared libs psql pulls in (libpq, libldap, ...) into a single dir we
# can copy wholesale. The C `postgres` backend, C `initdb`, and the bootstrap
# loadable modules are deliberately left behind.
RUN set -eux; \
    test -x "${PG_LIBROOT}/bin/psql"; \
    test -f "${PG_SHAREDIR}/postgres.bki"; \
    test -f "${PG_SHAREDIR}/system_constraints.sql"; \
    test -f "${PG_SHAREDIR}/system_functions.sql"; \
    test -f "${PG_SHAREDIR}/system_views.sql"; \
    test -f "${PG_SHAREDIR}/information_schema.sql"; \
    test -f "${PG_SHAREDIR}/sql_features.txt"; \
    test -f "${PG_SHAREDIR}/snowball_create.sql"; \
    test -f "${PG_SHAREDIR}/pg_hba.conf.sample"; \
    test -f "${PG_SHAREDIR}/postgresql.conf.sample"; \
    test -f "${PG_SHAREDIR}/pg_ident.conf.sample"; \
    mkdir -p /opt/pgbin /opt/runlibs; \
    cp -L "${PG_LIBROOT}/bin/psql" /opt/pgbin/psql; \
    ldd "${PG_LIBROOT}/bin/psql" | awk '/=> \//{print $3}' \
    | sort -u | while read -r so; do \
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
#   libicu72        — linked directly by the pgrust binary (ICU collation provider)
#   libxml2         — linked directly by the pgrust binary (xml type support)
#   tzdata          — system zoneinfo (Debian PG is --with-system-tzdata)
#   locales         — en_US.utf8 (matches the official image's LANG)
#   gosu            — root -> postgres step-down (Debian package, like upstream)
#   xz-utils/zstd/gzip — decompress *.sql.{xz,zst,gz} init scripts
# (the official image's libnss-wrapper is NOT needed here: only C initdb
#  consulted /etc/passwd; pgrust initdb takes the superuser name from --username.)
RUN set -eux; \
    apt-get update; \
    apt-get install -y --no-install-recommends \
        libicu72 libxml2 tzdata locales xz-utils zstd gzip \
        libldap-2.5-0 libpam0g libssl3 \
        gosu \
    ; \
    rm -rf /var/lib/apt/lists/*; \
    # verify gosu works (and that the "nobody" user resolves) like the official image \
    gosu nobody true; \
    localedef -i en_US -c -f UTF-8 -A /usr/share/locale/locale.alias en_US.UTF-8 || true
ENV LANG=en_US.utf8

ENV PG_MAJOR=${PG_MAJOR}

# The pgrust binary. This IS both the user-facing server and the catalog
# bootstrap driver (`pgrust-postgres --initdb`). It is named `pgrust-postgres`;
# the entrypoint always launches it by this absolute path.
COPY --from=rustbuild /opt/pgrust-postgres /usr/local/bin/pgrust-postgres

# psql — the only C tool retained (init-script / healthcheck client; pgrust has
# no psql replacement). Placed on PATH at the PGDG-native bin location.
COPY --from=pgtools /opt/pgbin/psql ${PG_LIBROOT}/bin/psql
# The PostgreSQL share tree: tz data + the initdb bootstrap templates
# (postgres.bki, system_*.sql, system_views.sql, information_schema.sql,
# sql_features.txt, snowball_create.sql, config samples) that pgrust --initdb /
# --boot read.
COPY --from=pgtools ${PG_SHAREDIR} ${PG_SHAREDIR}
# The non-glibc shared libs psql links (libpq, libldap, ...).
COPY --from=pgtools /opt/runlibs /opt/runlibs
ENV PATH=${PG_LIBROOT}/bin:$PATH
ENV LD_LIBRARY_PATH=/opt/runlibs
# pgrust's --initdb passes this to its --boot/--single phases via `-L`, and the
# binary's baked-in PGRUST_PGSHAREDIR points here too.
ENV PG_SHAREDIR=${PG_SHAREDIR}

# Debian's PostgreSQL is built --with-system-tzdata, so the package share dir
# ships NO `timezone/` subtree. The pgrust binary looks for
# ${PGRUST_PGSHAREDIR}/timezone, so point it at the system tzdata.
RUN test -d /usr/share/zoneinfo \
    && ln -snf /usr/share/zoneinfo "${PG_SHAREDIR}/timezone"

# Official-image data dir + socket dir conventions.
ENV PGDATA=/var/lib/postgresql/data
RUN install --verbose --directory --owner postgres --group postgres --mode 1777 /var/lib/postgresql/data
RUN mkdir -p /var/run/postgresql && chown -R postgres:postgres /var/run/postgresql && chmod 3777 /var/run/postgresql

# The entrypoint, installed under the official name so `docker-entrypoint.sh` on
# PATH works for users who reference it explicitly.
COPY docker/entrypoint.sh /usr/local/bin/docker-entrypoint.sh
RUN ln -snf /usr/local/bin/docker-entrypoint.sh /docker-entrypoint.sh # backwards compat

# First-init scripts dir (official image convention).
RUN mkdir -p /docker-entrypoint-initdb.d

VOLUME /var/lib/postgresql/data

EXPOSE 5432
STOPSIGNAL SIGINT
ENTRYPOINT ["docker-entrypoint.sh"]
# Default: run the server (the entrypoint rewrites this to the pgrust binary).
CMD ["postgres"]
