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

---

# Harness-defect log (lane p1-laneac)

P1/P2 above are the only SHIPPED-CODE defects this target found (both fixed by
the coordinator in 3c129c2bb6). Every other stop was a defect in the harness
itself, and in each case the shipped Rust was correct. Recorded because the
FAILED-is-not-a-verdict law is only useful if the decode outcomes are written
down — four of these five would have been mis-reported as pgrust bugs if the
oracle had been trusted.

| # | Symptom | Real cause | Decode signal |
|---|---|---|---|
| H1 | SEGV in `pg_diff_range_accessors`, addr 0x0 | flags `RANGE_LB_NULL` leaves a bound ABSENT but not infinite, so C's `range_lower` returns an unflagged `(Datum) 0`; for a BYREF subtype that is a NULL element pointer that C's own accessors/comparators dereference. rangetypes.h marks both NULL bits "(NOT USED)" and no C path sets them, so C cannot construct the input. | Discriminating test: identical input passes at int4range/int8range, SEGVs only at numrange. (The initial hypothesis — a NULL typcache entry — was WRONG; the oid was mocked all along.) |
| H2 | `numrange_subdiff`: C err 2 vs Rust err 1 | `pg_float_io.c` and `pg_rangetypes_io.c` share one `_Thread_local pg_diff_errcode` but number classes INVERSELY (float_io 1=22P02/2=22003). Calling the vendored `float8in_internal` stamped the other file's numbering into the channel. | Rust's message named a genuine out-of-range double; two files, one channel, two tables. |
| H3 | `range_out`: "C err -1" | -1 is the oracle's buffer-capacity sentinel, not an error class, and the arm compared it as one. `numeric_out` can emit ~147k digits and `range_bound_escape` can double every char, so a 20-byte literal blows a 4 KiB cap. | Sentinel value appearing where a sqlstate class belongs. |
| H4 | int8range `[..-9223372036854775808,X]`: C 22003 vs Rust 22P02 | I HAND-WROTE `pg_neg_u64_overflow` instead of vendoring it, casting uint64→int64 before the subtraction so -(-2^63) wrapped and the legal bigint INT64_MIN was rejected. Upstream's arm is `__builtin_sub_overflow(0, a, result)` with `a` unsigned (infinite precision). | i64::MIN is legal bigint; the oracle rejecting it indicts the oracle. |
| H5 | `range_eq` divergence at flags 06/04 | One errcode for all 15 bundled operators: `range_adjacent`'s legitimate 22003 (via `int4range_canonical` at INT32_MAX — already recorded in the proofs ledger for that row) aborted the bundle and was compared against `range_eq`'s good result. | The named operator cannot raise 22003 at all; the erroring sibling can. |

Durable lessons worth carrying to sibling lanes:

1. **The never-fabricate-C-bodies rule covers HEADER static inlines**, not just
   the `.c` bodies the scaffold gates with `#error`. H4 was a two-line helper.
   All of `common/int.h`, `port/pg_bitutils.h`, `common/stringinfo.c`,
   `libpq/pqformat.c(+.h)` are now extracted verbatim by the assembler.
2. **A shared errcode channel needs ONE numbering.** If a target links another
   csrc TU, either share its table or translate at the seam (H2). The
   translation is now written down in both directions.
3. **Capacity sentinels must never share a namespace with error classes** (H3),
   and every arm must assert on them loudly.
4. **A bundled multi-call entry needs per-call errcodes** (H5). Beyond
   correctness this strengthened the error plane: previously only the FIRST
   erroring operator of 15 was ever compared per iteration.
5. **Fence the input domain at the choke point, not per arm.** The H1 fence
   lives in `build_image`, so all image-consuming arms inherit it.
6. **Oracle-side `assert()` must be release-effective.** Probed and confirmed:
   `cc-rs` does NOT define `NDEBUG` in the release fuzz build here, so the
   oracle's loud guards are live in exactly the build the campaign runs. Had
   they been compiled out, every guard would have been a debug-only tripwire on
   a release-live path — the debug-assert-masking class, self-inflicted.

## H6 — byref image layout: a COVERAGE GAP, not the SEGV it was reported as

Raised by the sibling multirange lane (`proofs/p1-laneac-mr`) as a malformed-image
SEGV blocking this campaign. Decoded differently, with evidence:

* **Their banked reproducer passes here unchanged.** Its flags are `0x30` =
  `RANGE_LB_NULL|RANGE_UB_INF` — exactly the case `fence_flags` already handles
  (H1, committed ac6d2da619) — and the SEGV site they cite (`numeric_cmp`) is
  H1's NULL byref bound deref. With `LB_NULL` fenced that input writes a single
  lower bound and never pads at all.
* **The layout observation is real.** Measured for numrange `[1.5,2.5)`:

      serializer : 5c000000 420f0000 0f 80800100 8813 0f 80800200 8813 02
      hand-built : 7c000000 420f0000 28000000 80800100 8813 0000 28000000 ...

  numeric is packable (`typstorage 'm'`), so `datum_write` converts a small
  bound to a 1-byte SHORT header with no alignment; the hand builder writes a
  4-byte header plus pad bytes.
* **But it is not a defect.** The C accessors entry reads the hand-built image
  with `ret=0` and all seven per-call errcodes `0`, returning exactly the
  4-byte-header bounds written. Both sides deserialize it identically, so
  comparisons over it were sound — they simply never exercised
  `fetch_att`/`att_addlength_pointer`/`att_align_pointer`'s `VARATT_IS_1B` arms.

**Resolution:** their fix adopted as an ADDITION, not a replacement. Both layouts
are now fuzzed on a payload bit — the hand builder keeps the arbitrary-flags
domain the constructor cannot express, and `build_image_ctor` covers the
packed-short layout where builder/serializer skew is structurally impossible.
Replacing the hand builder outright would have traded one coverage gap for
another.

**Lesson (the one worth carrying):** "the oracle crashed on my input" and "my
input is malformed" are different claims, and the second does not follow from the
first. Read what the reader actually does with the bytes before rewriting the
writer — and check whether an already-committed fence covers the reproducer.

## H7 — leaked fn_expr carrier killed the first 10M campaign (my defect, my bad evidence)

`ops_flinfo` minted its `fn_expr` rettype carrier with
`Box::leak(Box::new(AggFnArgTypes { .. }))`, i.e. 24 bytes per call, never freed.
The fleet's LeakSanitizer killed campaign
`pgrust-fuzz-campaign-1785516178-4344-37961` at 8 execs: 360 bytes in 15 objects.

Fixed by making the carriers statics — `PINS` is const and there are exactly
three instantiations, so nothing needs allocating. Named `RNG_RETTYPE` to match
the sibling multirange target's spelling so the two reconcile at merge. Two gates
added: one pinning `RNG_RETTYPE[t].rettype == PINS[t].rngtypid` (a silent
reordering would hand every constructor the wrong result type, which no value
plane could catch), and one asserting successive `ops_flinfo()` calls hand back
the SAME carrier pointer, so a return to per-call allocation fails a test rather
than a campaign.

Sweep for the same shape across the driver: no remaining `Box::leak`,
`into_raw`, `mem::forget`, `ManuallyDrop` or `.leak()`. Everything else is either
owned and dropped per iteration (`Vec` buffers, `CString`, the per-exec
`MemoryContext` backing `StringInfo`/minted numerics) or reclaimed by `Drop`
(`FmgrInfo`'s `FnExtra` frees its box). Oracle-side allocations sit in the
`pg_diff_arena` static array, which LSan sees as a live global root — that is
precisely why the scaffold routes `palloc` through it instead of bare `malloc`.

**The evidence failure is the durable part.** I had claimed this class clean from
a filtered grep whose tail showed only libFuzzer frames, plus a `quote_diff`
control showing the same trace. Neither supports the conclusion: the report was
never enumerated, and a control sharing a libFuzzer-side allocation cannot speak
to this target's own. LSan on this laptop names `ops_flinfo` directly once the
full report is read, so nothing about the platform prevented finding it.
Enumerate the whole report; never generalize from a control that cannot exhibit
your defect.
