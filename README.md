# pgrust

**Multi-threaded PostgreSQL, rewritten from scratch in Rust.**

Built-in columnar scans. Built-in vector search. Auto-tuning ambitions. Wire-protocol compatible.

> 200,000 lines of Rust. 14 days. ~34% of PostgreSQL regression tests passing at the [`v0.1.0-baseline`](../../releases/tag/v0.1.0-baseline) tag.
> Core systems present: query planner, buffer cache, storage engine, indexes.

**[Try it in your browser → pgrust.com](https://pgrust.com)**

---

## Status: V1 / experimental

This is **not** production-ready. Core systems exist and the wire protocol is compatible enough that `psql` connects and most basic SQL works. Many features are still missing, performance is unoptimized, and there are rough edges everywhere. See [What works](#what-works) and [What doesn't work yet](#what-doesnt-work-yet) below.

The full launch write-up is here: [TBD: blog post URL].

## Why?

PostgreSQL has been the best database for 30 years. But some architectural decisions from 1996 keep showing up as operational pain:

- **Single-threaded per connection** — one OS process per session. Modern hardware stays idle.
- **Manual tuning** — 350+ parameters. PGTune exists because Postgres won't tune itself.
- **Stack-glue** — production deployments end up running Postgres + DuckDB + pgvector + PgBouncer + ClickHouse just to cover OLTP + OLAP + vectors + connections + analytics.
- **Vacuum** — enough said.

pgrust is an experiment: what does Postgres look like if you start from scratch with modern primitives — Rust, multi-threading, columnar storage, vector search — and keep wire-protocol compatibility?

## What works

- Query planner
- Buffer cache
- Storage engine
- B-tree indexes
- Multi-threaded query execution
- Wire protocol (psql connects, any Postgres driver should)
- Basic SQL: SELECT / INSERT / UPDATE / DELETE / CREATE TABLE / CREATE INDEX / transactions / JSON and JSONB / basic aggregates / joins
- ~34% of PostgreSQL regression tests passing (tagged [`v0.1.0-baseline`](../../releases/tag/v0.1.0-baseline))

## What doesn't work yet

- `VACUUM` (the architecture doesn't need it the same way, but the SQL isn't implemented)
- Schemas and databases (the SQL concepts — single schema per cluster right now)
- Performance optimization (feature-first; many queries are slow)
- Many SQL features (window functions, full partitioning, etc.)
- Production-readiness of any kind (don't put real data in this)

## Quick start

### In-browser (no install)

[pgrust.com](https://pgrust.com) runs pgrust compiled to WebAssembly in your browser. Paste in queries and watch them run. Same engine as the native build.

### Native (clone and run)

```bash
git clone https://github.com/malisper/pgrust.git
cd pgrust
cargo run --release --bin pgrust_server
```

Then from another terminal:

```bash
psql -h localhost -p 5432 -d postgres
```

```sql
CREATE TABLE t (id INT, name TEXT);
INSERT INTO t VALUES (1, 'hello'), (2, 'world');
SELECT * FROM t WHERE id = 1;
```

### Docker

```bash
docker pull malisper/pgrust:nightly
docker run -p 5432:5432 malisper/pgrust:nightly
```

## How it was built

pgrust was built by [Michael Malis](https://michaelmalis.com) over 14 days using AI coding agents (Claude Code, Cursor), directed by years of PostgreSQL internals experience from leading Heap's database team at petabyte scale and founding [Freshpaint](https://www.freshpaint.io/) (YC S19).

**Architecture and verification: human-directed.**
**Implementation velocity: AI-assisted.**

The PostgreSQL regression test suite is used as an executable spec — roughly "oracle-based verification": run the same query on real PostgreSQL and on pgrust, diff the results. If they diverge, that's a bug.

Full story: [TBD: blog post URL].

## Architecture

pgrust uses a shared-buffer multi-threaded architecture with a Rust async runtime. Connections are lightweight tasks, not full OS processes. The buffer cache, storage engine, and query execution operate across threads. The file layout mirrors PostgreSQL's directory structure (`src/backend/parser`, `src/backend/executor`, `src/backend/storage`, etc.) on purpose — it makes cross-referencing with Postgres easier.

See [CLAUDE.md](./CLAUDE.md) for a more detailed module-by-module layout.

## Roadmap

- [ ] 34% → 50% → 75% PostgreSQL regression tests
- [ ] Performance pass (benchmarks follow-up post)
- [ ] Schema + database SQL support
- [ ] Auto-tuning for core parameters
- [ ] Built-in columnar scan for OLAP
- [ ] Built-in vector similarity search
- [ ] Production-grade durability story

## Stay updated

- **Blog post & mailing list:** [TBD: mailing list link] — progress updates, plus we'd love to hear what PostgreSQL problems you'd want solved.
- **Discord:** [TBD: discord invite] — general questions, bug reports, feature ideas.
- **GitHub issues:** open for bug reports and design discussion.

## License

AGPL-3.0-only. Same license as Grafana, MinIO, and ParadeDB.

If you want to use pgrust commercially under different terms, [reach out](mailto:TBD).

## Contributing

Early stage. Not accepting code PRs yet — the codebase is still evolving too fast for external contributions to be stable. Issues and feedback are very welcome, and contributions will open once the architecture stabilizes.
