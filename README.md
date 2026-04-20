# **pgrust**

**PostgreSQL, rewritten from scratch in Rust.**

[**Try it in your browser → pgrust.com**](https://pgrust.com/)

---

## **Status: V1 / experimental**

**This is not production-ready. Core systems exist and the wire protocol is compatible enough that `psql` connects and most basic SQL works. Many features are still missing, performance is unoptimized, and there are rough edges everywhere.**

[**The full launch write-up is here.**](https://malisper.me/pgrust-rebuilding-postgres-in-rust-with-ai)

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
- **Wire protocol compatibility (psql connects)**
- **Basic SQL: SELECT / INSERT / UPDATE / DELETE / CREATE TABLE / CREATE INDEX / transactions / JSON and JSONB / basic aggregates / joins**
- **~34% of PostgreSQL regression tests passi**

## **Quick start**

### **In-browser (no install)**

[pgrust.com](https://pgrust.com/) runs pgrust compiled to WebAssembly in your browser. Paste in queries and watch them run. Same engine as the native build.**

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