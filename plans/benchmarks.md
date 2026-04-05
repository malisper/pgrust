# Benchmarks

## Autocommit INSERT (over wire, 10K rows)

Benchmark: `bench/bench_insert_pgrust_wire.sh 10000` and `bench/bench_insert_postgres_autocommit.sh 10000`

| Engine | inserts/sec |
|--------|------------|
| pgrust (wire, F_FULLFSYNC) | 119 |
| pgrust (wire, POSIX fsync) | 478 |
| pgrust (wire, POSIX fsync + BufWriter) | **520** |
| PostgreSQL 18 | 513 |

## Autocommit INSERT (in-process, 1M rows)

Benchmark: `cargo run --release --bin bench_insert -- --autocommit --rows 1000000`

| Optimization | inserts/sec |
|-------------|------------|
| Before CLOG cache (fsync disabled) | 108,484 |
| After CLOG cache (fsync disabled) | 185,962 |

## Single-txn INSERT (in-process, PL/pgSQL, 1M rows)

Benchmark: `bench/bench_insert_postgres.sh 1000000` / `bench/bench_insert_postgres_dynamic.sh 1000000`

| Engine | inserts/sec |
|--------|------------|
| PostgreSQL (plan-cached PL/pgSQL) | 329,780 |

## Profile breakdown (autocommit insert, in-process, fsync disabled, 1M rows)

After all optimizations:

| % | Component |
|---|-----------|
| 25% | pest parser |
| 11% | memmove |
| 7% | memcmp |
| 2% | malloc |
| 1.4% | txn begin |
| 1.1% | snapshot |
| 0.9% | commit |
| 0.7% | write syscalls |

## Profile breakdown (autocommit insert, wire protocol, 10K rows)

After POSIX fsync + BufWriter:

| % | Component |
|---|-----------|
| 27% | fsync (per-commit WAL) |
| 20% | WAL write |
| 17% | network send |
| 9% | network recv |
| 5% | pest parser |
| 2% | malloc/free |

## Key optimizations applied

1. **in_progress Vec** — O(active_txns) snapshots instead of O(all_txns). 6.7x speedup at 100K rows.
2. **In-memory CLOG buffer** — Eliminated ~20% CLOG seek+read+write syscalls. 1.7x speedup (108K→186K inserts/sec).
3. **POSIX fsync** — Replace macOS F_FULLFSYNC with POSIX fsync, matching PostgreSQL's default. 4x speedup over wire (119→478).
4. **BufWriter for network writes** — Batch multiple small write_all calls into one sendto per response. 478→520 inserts/sec.
5. **Buffer pool eviction race fix** — Clear dirty flag before dropping locks, re-check is_dirty on re-acquire.
6. **2-bit CLOG format** — Matches PostgreSQL's pg_xact layout. 4 transactions per byte.
7. **WAL delta records** — After first FPI, subsequent inserts to same page write only tuple data (~100 bytes vs 8KB).
8. **WAL hole compression** — FPI records omit the zero-filled hole between pd_lower and pd_upper.
9. **Background WAL writer** — Periodic BufWriter flush to reduce commit-path write syscalls.
10. **Prepared insert path** — Skip SQL parsing on repeated inserts.
