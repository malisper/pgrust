# **pgrust**

**PostgreSQL, rewritten from scratch in Rust.**

[**Try it in your browser → pgrust.com**](https://pgrust.com/)

---

## **Status: V1 / experimental**

**This is not production-ready. pgrust is now matching about 67% of the 54,000+ query blocks in PostgreSQL's regression suite. Core systems exist, `psql` connects, and the browser demo runs the same engine compiled to WebAssembly. Many features are still missing, performance is uneven, and correctness work is ongoing.**

[**Original launch write-up.**](https://malisper.me/pgrust-rebuilding-postgres-in-rust-with-ai)

[**Latest update: 67% Postgres compatibility and accelerating.**](https://malisper.me/pgrust-update-at-67-postgres-compatibility-and-accelerating/)

## **Why?**

**PostgreSQL has been in development for 40 years. But some architectural decisions from 80s keep showing up as operational pain:**

- **Single-threaded per connection — one OS process per connection. Limited parallelism in queries.**
- **Manual tuning — 350+ parameters. PGTune exists because Postgres won't tune itself.**
- **Vacuum — Has maybe caused thousands of outages, potentially much more.**

**pgrust is an experiment: what does Postgres look like if you start from scratch with modern primitives?**

## **What works**

- **Query planner**
- **Buffer cache**
- **Storage engine**
- **B-tree indexes**
- **Wire protocol compatibility (`psql` connects)**
- **JSON and JSONB**
- **Window functions**
- **Foreign keys**
- **EXPLAIN / EXPLAIN ANALYZE**
- **Regex support**
- **PL/pgSQL pieces**
- **Basic SQL: SELECT / INSERT / UPDATE / DELETE / CREATE TABLE / CREATE INDEX / transactions / aggregates / joins**
- **~67% of PostgreSQL regression query blocks match expected output**

## **Current snapshot**

The public `main` branch moves forward with intentionally published updates. The 2026-04-27 regression snapshot that matched about 67% of PostgreSQL's regression query blocks is tagged separately so it remains easy to find even as `main` advances.

## **Quick start**

### **In-browser (no install)**

[pgrust.com](https://pgrust.com/) runs pgrust compiled to WebAssembly in your browser, with examples for window functions, JSONB, foreign keys, EXPLAIN ANALYZE, regex, and a recursive-CTE Lisp interpreter.

### **Native (clone and run)**

```bash
git clone <https://github.com/malisper/pgrust.git>
cd pgrust
cargo run --release --bin pgrust_server -- --port 54321
```

**Then from another terminal:**

```bash
# If you have psql installed
psql -h localhost -p 54321 -d postgres
```

```bash
# If you don't have psql installed
docker run -it --rm postgres:18 psql -h host.docker.internal -p 54321 -U postgres
```

### **Docker**

```bash
docker run --rm --name pgrust -p 54321:5432 malisper/pgrust:nightly
```

```bash
# If you have psql installed
psql -h localhost -p 54321 -d postgres
```

```bash
# If you don't have psql installed
docker run -it --rm postgres:18 psql -h host.docker.internal -p 54321 -U postgres
```

## **Stay updated**

- [Blog](https://malisper.me/)
- [Mailing List](https://malisper.me/subscribe/)
- [Discord](https://discord.gg/FZZ4dbdvwU)

## Contributing

Early stage. Not accepting code PRs yet, the codebase is still evolving too fast for external contributions to be stable. Issues and feedback are very welcome, and contributions will open once the architecture stabilizes.
