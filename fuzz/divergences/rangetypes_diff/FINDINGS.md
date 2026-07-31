# rangetypes_diff — divergences (lane p1-laneac)

Oracle: verbatim PostgreSQL 18.3 (upstream sha 62d6c7d3df) in
`fuzz/core/csrc/pg_rangetypes_io.c`. Ground-truth: docker `postgres:18.3`.

## P1 (RELEASE BLOCKER) — `range_recv` SEGV on a zero-length bound

**Reproducer**: `P1-range_recv-zero-length-bound-SEGV.bin`
(`cargo +nightly fuzz run rangetypes_diff --release <file>`).
Wire decoded: `flags=0x00` (both bounds present), lower `bound_len=0`.

**Defect**: `crates/backend/utils/adt/rangetypes/src/io.rs:443`

```rust
let bound_len = ::pqformat::pq_getmsgint(buf, 4)? as usize;
let mut bound_buf = ::stringinfo::StringInfo::with_capacity_in(mcx, bound_len)?;
```

`StringInfo::with_capacity_in` (`crates/_support/types/stringinfo/src/lib.rs:25`)
writes the NUL terminator unconditionally:

```rust
debug_assert!(initsize >= 1 && initsize <= MAX_ALLOC_SIZE);
let mut data = PgVec::new_in(mcx);
data.try_reserve_exact(initsize)...;
unsafe { *data.as_mut_ptr() = 0 };   // initsize == 0 => dangling ptr (0x1)
```

With `initsize == 0` the reserve is a no-op and the write lands on `PgVec`'s
zero-capacity dangling sentinel: **SEGV in release builds** (ASan:
`SEGV on unknown address 0x000000000001 in StringInfo::with_capacity_in`).
Debug builds trip the `debug_assert` instead — the debug-assert masking
class: the guard is debug-only while the defect is release-live.

**C behavior** (verbatim `range_recv`) validates before allocating:
`pq_getmsgbytes(buf, bound_len)` runs FIRST, then `initStringInfo` (which
always allocates 1024). A zero-length bound therefore reaches the element
receive function with an empty buffer and errors normally.

**Ground truth (`postgres:18.3`, binary COPY of `int8range`)**:
`gt_good.bin` -> `COPY 1`, value `[1,9)`.
`gt_bad.bin` (the same zero-length-bound wire) ->
`ERROR: insufficient data left in message` (08P01), **server stays alive**
(`select 1` succeeds afterwards).

**Reachability**: SQL-reachable by any client that can send a binary range
value — `COPY ... WITH (FORMAT binary)` or an extended-protocol binary
parameter of a range type. Unprivileged post-auth => backend crash.

**Classification**: pgrust-bug, release blocker.

**Siblings of the same shape** (same lane, in-scope for adt/multirangetypes):
- `crates/backend/utils/adt/multirangetypes/src/io.rs:262` —
  `StringInfo::with_capacity_in(mcx, range_len)` with wire-controlled
  `range_len`, identical zero case.
- `crates/backend/utils/adt/multirangetypes/src/io.rs:258` —
  `vec_with_capacity_in(mcx, range_count)` with a wire-controlled count
  (P2 shape below, no zero case).
Out of lane: `crates/backend/utils/adt/arrayfuncs/src/io.rs:818` uses
`itemlen + 1`, so it is immune to the zero case but shares P2's shape
(lane p1-lanex owns adt/arrayfuncs).

## P2 — `range_recv` allocates a wire-controlled size before validating

**Reproducer**: `P2-range_recv-unbounded-prealloc.bin`
(wire: `flags=0x00`, `bound_len=0xEBFFFFFF` = 3.96 GiB, 5 bytes available).

Same call site: pgrust reserves `bound_len` bytes, C first calls
`pq_getmsgbytes(buf, bound_len)` which raises 08P01 because the message is
short — C never allocates. A 10-byte message therefore makes pgrust attempt
a ~4 GiB allocation (libFuzzer reports OOM; `MAX_ALLOC_SIZE` = 0x3FFFFFFF is
only checked by the debug-only assert).

With allocation limits raised the comparison planes AGREE (both sides end at
08P01), so P2 is purely a resource-behavior divergence — DoS-shaped, not a
value/errcode divergence. Recorded separately from P1 for that reason.

**Classification**: pgrust-bug (resource), same fix site as P1.

## Fix note (NOT applied by this lane)

The C-faithful ordering is: read the length, `pq_getmsgbytes` it (which
bounds it by the remaining message), and only then build the buffer — that
fixes P1 and P2 together and needs no `StringInfo` change. Hardening
`with_capacity_in` against `initsize == 0` is a defensible second layer but
would leave the pre-validation over-allocation in place.
