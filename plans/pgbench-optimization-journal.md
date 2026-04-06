# Pgbench Optimization Journal

Benchmark command used for all experiments unless noted otherwise:

```bash
cargo run --release --bin pgbench_like -- \
  --base-dir /tmp/pgrust-pgbench-opt \
  --pool-size 128 \
  --init \
  --scale 1 \
  --clients 10 \
  --time 10
```

## Baseline

- Branch: `experiment/pgbench-tuning`
- Commit: `0100d58`
- Result:
  - transactions: `191`
  - failures: `0`
  - wall time: `10.350 s`
  - avg latency: `537.474 ms`
  - TPS: `18.453`

## Experiments

### 1. Remove `parking_lot` deadlock detection from release builds

- Change: moved `deadlock_detection` off the normal `parking_lot` dependency.
- Result:
  - transactions: `183`
  - avg latency: `551.314 ms`
  - TPS: `17.949`
- Impact vs baseline: slower (`-2.7% TPS`)
- Decision: reverted

### 2. Cache one heap page per scan instead of repinning for every tuple

- Change: `HeapScan` now copies a page image once per block and iterates all tuples from that cached page before advancing.
- Result:
  - transactions: `220`
  - avg latency: `463.849 ms`
  - TPS: `21.492`
- Impact vs baseline: faster (`+16.5% TPS`)
- Decision: kept

### 3. Replace the buffer lookup `RwLock` with a plain `Mutex`

- Change: made the buffer lookup table a `Mutex<HashMap<...>>` instead of a write-preferring `RwLock`.
- Result:
  - transactions: `228`
  - avg latency: `447.324 ms`
  - TPS: `22.208`
- Impact vs baseline: faster (`+20.3% TPS`)
- Impact vs previous kept state: faster (`+3.3% TPS`)
- Decision: kept

### 4. Switch the buffer lookup table to a faster hasher

- Change: replaced the lookup map hasher with `rustc-hash`.
- Result:
  - transactions: `220`
  - avg latency: `457.687 ms`
  - TPS: `21.824`
- Impact vs current best: slower (`-1.7% TPS`)
- Decision: reverted

### 5. Reuse the physical tuple slot for assignment evaluation

- Change: evaluated `UPDATE` assignments directly against the materialized physical slot instead of building a separate virtual slot.
- Result:
  - transactions: `220`
  - avg latency: `464.635 ms`
  - TPS: `21.469`
- Impact vs current best: slower (`-3.3% TPS`)
- Decision: reverted

### 6. Hold the transaction manager read lock across an entire scan

- PostgreSQL comparison: PostgreSQL does not take a single heavyweight lock around all tuple visibility checks in a scan; its visibility path is much more fine-grained.
- Change: held `ctx.txns.read()` across the whole `UPDATE`/`DELETE` scan loop instead of reacquiring it per tuple.
- Result:
  - transactions: `196`
  - avg latency: `514.686 ms`
  - TPS: `19.350`
- Impact vs current best: slower (`-12.9% TPS`)
- Decision: reverted

### 7. Skip tuple materialization in EXPLAIN ANALYZE

- Change: EXPLAIN ANALYZE now discards tuples as they are produced instead of decoding and collecting them into a `Vec<Vec<Value>>`. Only a row counter is incremented. This matches PostgreSQL's behavior.
- Benchmark: `bench/bench_explain.sh --port 5444 --rows 10000 --iterations 10`
- Before (materializes all rows):
  - avg: `0.728 ms`
  - min: `0.681 ms`
  - median: `0.723 ms`
- After (discard tuples):
  - avg: `0.689 ms`
  - min: `0.648 ms`
  - median: `0.689 ms`
- Impact: ~5% faster. Modest because the scan and visibility checks dominate; tuple decoding is not the bottleneck at this row count.
- Note: the `CompiledTupleDecoder` on `SeqScanState` is built at plan time but never used — the `values()` path still goes through `deform` + `decode_value`. Wiring up the compiled decoder would benefit all scan paths, not just EXPLAIN ANALYZE.
- Decision: kept

### 8. Pending

### 9. Pending

### 10. Pending
