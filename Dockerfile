# syntax=docker/dockerfile:1
#
# Reproducible build + run image for pgrust (a Rust port of PostgreSQL 18.3).
#
# Three stages:
#   1. rustbuild — builds the pgrust `postgres` binary. `nodetags.h` is vendored
#                  in the repo (crates/_support/types/nodes/vendor/nodetags.h),
#                  so this stage needs no PostgreSQL source at all — just Rust +
#                  libicu-dev (the ICU collation provider links system ICU via
#                  pkg-config).
#   2. pgtools  — pulls the C `initdb` / `psql` / `postgres` + loadable modules
#                 + libpq + the PostgreSQL share tree from the PGDG apt repo.
#                 pgrust's own initdb is unported, so a datadir must be created
#                 by the C `initdb` (which runs the C `postgres` backend to
#                 bootstrap the catalog); psql is the client. The C `postgres`
#                 is used ONLY by initdb — it is NOT the user-facing server.
#   3. final    — slim runtime: the pgrust binary (the server) + those minimal C
#                 tools + libicu, with an entrypoint that initdb's a datadir,
#                 boots pgrust by absolute path, and (default CMD) runs a query.
#
# Build:  docker build -t pgrust .
# Run:    docker run --rm pgrust            # boots + prints version + a query
#         docker run --rm -it pgrust psql   # interactive psql against pgrust
#
# No host mounts are required for the build.

# The C tools (initdb / psql / the bootstrap-only postgres backend), their
# loadable modules, and the share tree are all kept at their PGDG-native paths
# in the final image. This matters because the C `postgres` backend computes
# $libdir and the bki/template share dir RELATIVE to its own executable
# location (configure-time PGBINDIR/PKGLIBDIR/PGSHAREDIR); relocating the binary
# would make initdb's bootstrap backend look for dict_snowball.so etc. in the
# wrong place. So we leave the whole /usr/lib/postgresql/18 + /usr/share/...
# layout untouched.
ARG PG_LIBROOT=/usr/lib/postgresql/18
ARG PG_SHAREDIR=/usr/share/postgresql/18

# ---------------------------------------------------------------------------
# Stage 1: build the pgrust `postgres` binary (self-contained — vendored nodetags.h)
# ---------------------------------------------------------------------------
FROM rust:1-bookworm AS rustbuild

ARG PG_SHAREDIR
ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update && apt-get install -y --no-install-recommends \
        build-essential pkg-config libicu-dev \
    && rm -rf /var/lib/apt/lists/*

# Bake the runtime share dir (timezone/timezonesets) location into the binary so
# it resolves the tz tree shipped in the final image.
ENV PGRUST_PGSHAREDIR=${PG_SHAREDIR}
# Dedicated target dir, off the source tree.
ENV CARGO_TARGET_DIR=/build/target

WORKDIR /src
COPY . /src

RUN cargo build --release --locked --bin postgres \
    && cp /build/target/release/postgres /opt/postgres

# ---------------------------------------------------------------------------
# Stage 2: obtain the minimal C tools (initdb, psql, libpq, share) from PGDG
# ---------------------------------------------------------------------------
FROM debian:bookworm-slim AS pgtools

ARG PG_LIBROOT
ARG PG_SHAREDIR
ENV DEBIAN_FRONTEND=noninteractive

# Add the PostgreSQL Global Development Group (PGDG) apt repo and install the
# PostgreSQL 18 server + client packages (server package carries initdb +
# postgres + the loadable modules + the share/ tree; client package carries
# psql + libpq).
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
# shared libs the three binaries pull in (libpq, libicu, libldap, ...) into a
# single dir we can copy wholesale. Everything else stays at its native path.
RUN set -eux; \
    test -x "${PG_LIBROOT}/bin/initdb"; \
    test -x "${PG_LIBROOT}/bin/psql"; \
    test -x "${PG_LIBROOT}/bin/postgres"; \
    test -f "${PG_LIBROOT}/lib/dict_snowball.so"; \
    test -f "${PG_SHAREDIR}/postgres.bki"; \
    mkdir -p /opt/runlibs; \
    for b in "${PG_LIBROOT}"/bin/initdb "${PG_LIBROOT}"/bin/psql "${PG_LIBROOT}"/bin/postgres; do \
        ldd "$b" | awk '/=> \//{print $3}'; \
    done | sort -u | while read -r so; do \
        case "$so" in /lib/*|/usr/lib/*) cp -L "$so" /opt/runlibs/ || true;; esac; \
    done

# ---------------------------------------------------------------------------
# Stage 3: runtime image
# ---------------------------------------------------------------------------
FROM debian:bookworm-slim AS final

ARG PG_LIBROOT
ARG PG_SHAREDIR
ENV DEBIAN_FRONTEND=noninteractive
# libicu72 is the only runtime package we install (the pgrust binary links it
# directly for the ICU collation provider). initdb/psql/postgres's other libs
# are copied from the pgtools stage into /opt/runlibs.
RUN apt-get update && apt-get install -y --no-install-recommends \
        libicu72 tzdata \
    && rm -rf /var/lib/apt/lists/*

# The pgrust binary (this IS the user-facing server).
COPY --from=rustbuild /opt/postgres /usr/local/bin/postgres

# The C tools at their PGDG-native paths (so the bootstrap backend's relative
# $libdir / share-dir computation resolves correctly):
#   ${PG_LIBROOT}/bin   — initdb, psql, the bootstrap-only postgres backend
#   ${PG_LIBROOT}/lib   — loadable modules dlopen'd during initdb (dict_snowball)
#   ${PG_SHAREDIR}      — tz data + initdb bootstrap templates (postgres.bki, ...)
COPY --from=pgtools ${PG_LIBROOT} ${PG_LIBROOT}
COPY --from=pgtools ${PG_SHAREDIR} ${PG_SHAREDIR}
# The non-glibc shared libs those binaries link (libpq, libicu, libldap, ...).
COPY --from=pgtools /opt/runlibs /opt/runlibs
ENV PATH=${PG_LIBROOT}/bin:$PATH
ENV LD_LIBRARY_PATH=/opt/runlibs

# Debian's PostgreSQL is built --with-system-tzdata, so the package share dir
# ships NO `timezone/` subtree — PG reads the system zoneinfo. The pgrust binary
# still looks for ${PGRUST_PGSHAREDIR}/timezone, so point it at the system
# tzdata (identical TZif/zic format). `tzdata` (/usr/share/zoneinfo) is already
# present via the base image's dependencies.
RUN test -d /usr/share/zoneinfo \
    && ln -s /usr/share/zoneinfo "${PG_SHAREDIR}/timezone"

# Non-root runtime user (PostgreSQL refuses to run as root).
RUN useradd -m -u 1000 pgrust \
    && mkdir -p /var/lib/pgrust \
    && chown -R pgrust:pgrust /var/lib/pgrust
USER pgrust

ENV PGDATA=/var/lib/pgrust/data \
    PGSOCKDIR=/tmp \
    PGPORT=5432

COPY --chown=pgrust:pgrust docker/entrypoint.sh /usr/local/bin/entrypoint.sh

EXPOSE 5432
ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]
# Default: boot pgrust and run a sanity query, then exit.
CMD ["query"]
