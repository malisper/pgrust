# **pgrust**

**PostgreSQL, rewritten from scratch in Rust.**

[**Try it in your browser → pgrust.com**](https://pgrust.com/)

---

## **Status: V1 / experimental**

**This is not production-ready. pgrust is now passing about 96% of PostgreSQL's regression suite. Core systems exist, `psql` connects, and the browser demo runs the same engine compiled to WebAssembly. Many features are still missing, performance is uneven, and correctness work is ongoing.**

[**Original launch write-up.**](https://malisper.me/pgrust-rebuilding-postgres-in-rust-with-ai)

[**Latest update: The four horsemen behind thousands of Postgres outages.**](https://malisper.me/the-four-horsemen-behind-thousands-of-postgres-outages/)

[**Previous update: 67% Postgres compatibility and accelerating.**](https://malisper.me/pgrust-update-at-67-postgres-compatibility-and-accelerating/)

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
- **~96% of PostgreSQL regression tests match expected output**

## **Current snapshot**

The public `main` branch moves forward with intentionally published updates. The 96% compatibility snapshot is tagged as [`compat-96pct-20260507`](https://github.com/malisper/pgrust/releases/tag/compat-96pct-20260507), and the previous 67% snapshot remains tagged as [`compat-67pct-20260427`](https://github.com/malisper/pgrust/releases/tag/compat-67pct-20260427).

## **Roadmap**

- **Push the remaining PostgreSQL compatibility gaps toward 100%.**
- **Stability and bug bashing so pgrust can move from experimental to trustworthy.**
- **Explore architectural fixes for common Postgres outage sources: 64-bit transaction IDs, VACUUM alternatives, better connection/query parallelism, adaptive planning, and JSON statistics/compression.**

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
