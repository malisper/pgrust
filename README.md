<h1 align="center">pgrust</h1>

<p align="center">
  <strong>A Postgres rewrite in Rust.</strong>
</p>

<p align="center">
  <img alt="Postgres 18.3" src="https://img.shields.io/badge/Postgres-18.3-336791">
  <img alt="Regression queries: 46k+" src="https://img.shields.io/badge/regression_queries-46k%2B-brightgreen">
  <a href="https://github.com/malisper/pgrust/blob/main/LICENSE">
    <img alt="License: AGPL-3.0" src="https://img.shields.io/badge/license-AGPL--3.0-blue">
  </a>
</p>

<div align="center">
  <a href="https://pgrust.com">Browser demo</a>
  <span>&nbsp;&nbsp;|&nbsp;&nbsp;</span>
  <a href="https://discord.gg/FZZ4dbdvwU">Discord</a>
  <span>&nbsp;&nbsp;|&nbsp;&nbsp;</span>
  <a href="https://malisper.me/subscribe/">Mailing list</a>
  <span>&nbsp;&nbsp;|&nbsp;&nbsp;</span>
  <a href="https://github.com/malisper/pgrust/issues">Issues</a>
</div>

<br />

pgrust targets compatibility with Postgres 18.3 and matches Postgres's
expected output across more than 46,000 regression queries.

pgrust is disk compatible with Postgres and can boot from an existing Postgres
18.3 data directory.

The goal is to make Postgres easier to change from the inside: keep the behavior
Postgres-shaped, keep the real Postgres tests as the oracle, and use Rust plus
AI-assisted programming to explore deeper server changes.

## Status

pgrust is not production-ready yet. It is not performance optimized yet.

Existing Postgres extensions and procedural language extensions such as
PL/Python, PL/Perl, and PL/Tcl are not generally compatible yet. Some bundled
contrib modules are already ported, and more compatibility may be possible over
time.

## Roadmap

- multithreaded Postgres internals
- built-in connection pooling
- better JSON-heavy workload support
- fast forking and branching workflows
- storage experiments, including no-vacuum designs
- runtime guardrails for bad queries and AI-generated SQL
- fewer sudden bad plan switches

## Try It

Try the WebAssembly demo at https://pgrust.com.

Docker:

```bash
docker run -d --name pgrust -e POSTGRES_PASSWORD=secret malisper/pgrust:v0.1 && until docker exec -e PGPASSWORD=secret pgrust psql -h 127.0.0.1 -U postgres -c '\q' >/dev/null 2>&1; do sleep 1; done && docker exec -it -e PGPASSWORD=secret pgrust psql -h 127.0.0.1 -U postgres; docker rm -f pgrust
```

This uses the `psql` client inside the Docker image.

`malisper/pgrust:latest` currently points at the same release, but `v0.1` is the
pinned launch image.

## Build From Source

macOS:

```bash
brew install icu4c openssl@3 libpq

export LIBRARY_PATH="$(brew --prefix openssl@3)/lib:${LIBRARY_PATH:-}"
export PKG_CONFIG_PATH="$(brew --prefix openssl@3)/lib/pkgconfig:$(brew --prefix icu4c)/lib/pkgconfig:${PKG_CONFIG_PATH:-}"
export PATH="$(brew --prefix libpq)/bin:$PATH"
```

Debian/Ubuntu:

```bash
sudo apt-get update
sudo apt-get install -y build-essential pkg-config libicu-dev libssl-dev libldap2-dev libpam0g-dev postgresql-client-18
```

Build:

```bash
PGRUST_PGSHAREDIR="$PWD/vendor/postgres-18.3/share" \
cargo build --release --locked --bin postgres
```

Create a data directory:

```bash
target/release/postgres --initdb \
  -D /tmp/pgrust-data \
  -L "$PWD/vendor/postgres-18.3/share" \
  --no-locale \
  --encoding UTF8 \
  -U postgres
```

Run pgrust:

```bash
ulimit -s 65520

RUST_MIN_STACK=33554432 target/release/postgres \
  -D /tmp/pgrust-data \
  -F \
  -c listen_addresses= \
  -k /tmp \
  -p 5432 \
  -c io_method=sync \
  -c max_stack_depth=60000
```

Connect:

```bash
psql -h /tmp -p 5432 -U postgres -d postgres \
  -c "select version(), 1 + 1 as two"
```

## Regression Tests

Run the Postgres regression tests against pgrust:

```bash
PGRUST_BIN="$PWD/target/release/postgres" \
scripts/run-regression
```

The runner uses pgrust's own `--initdb` plus the vendored Postgres 18.3 test
files in this repository. It needs a Postgres 18 `psql` client on `PATH`; if
`psql` is somewhere else, set `PGRUST_PSQL=/path/to/psql`.

Verified launch result: pgrust matched Postgres's expected output across more
than 46,000 regression queries.

## History

This repository now contains the newer pgrust implementation that reached the
regression-test milestone.

The older public implementation is archived on
`archive/pre-fabled-2026-06-23`.

Background:

- Original pgrust launch: https://malisper.me/pgrust-rebuilding-postgres-in-rust-with-ai/
- 67% regression update: https://malisper.me/pgrust-update-at-67-postgres-compatibility-and-accelerating/
- Four Horsemen roadmap: https://malisper.me/the-four-horsemen-behind-thousands-of-postgres-outages/

## Feedback

Please open an issue if something breaks, if setup is confusing, or if there is
a Postgres improvement you want to see first.

## Contact

- Email: maintainers@pgrust.com
- Discord: https://discord.gg/FZZ4dbdvwU
- Mailing list: https://malisper.me/subscribe/

## License

pgrust is licensed under AGPL-3.0. See `LICENSE`.
