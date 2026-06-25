# Zero-copy heap-scan slot — design / decision doc

Status: PROPOSAL (read-only analysis, no code changed)
Target query that motivates this: `SELECT count(*) FROM ints WHERE i%2=0` (10M rows)
Branch: `perf/zero-copy-bufferslot-plan`

## TL;DR

The premise "pgrust has no buffer-borrowing slot, port C's `ExecStoreBufferHeapTuple`" is
only *half* right. pgrust **already has** `BufferHeapTupleTableSlot` with a real `Buffer`
pin, `ExecStoreBufferHeapTuple` already exists, and the pin lifecycle (same-page
optimization, release-on-clear) is already faithful to C. The seqscan path already routes
through it (`heap_getnextslot` → `exec_store_buffer_heap_tuple`).

The actual problem is different and narrower: pgrust's slot stores an **owned**
`FormedTuple` (header `PgBox` + a separate owned `PgVec<u8>` user-data area) instead of a
**borrow into the pinned page**. As a result the per-returned-row path performs *two* full
tuple materializations where C performs *zero*:

1. `read_on_page_full` (`common_heaptuple.rs:528`) copies header + user-data off the page
   (`slice_in` at `:554`), then
2. `heap_getnextslot` (`scan.rs:1461`) calls `FormedTuple::clone_in` — a *second* full deep
   copy — before `exec_store_buffer_heap_tuple` moves it into the slot.

C does neither: `loctup.t_data = (HeapTupleHeader) PageGetItem(page, lpp)` is a pointer into
the pinned buffer, and `ExecStoreBufferHeapTuple` just stores that pointer + bumps the pin
refcount. The copy in C happens only at `ExecMaterializeSlot` / `heap_copytuple`.

So the real work is: **make the slot's physical tuple a borrow into the pinned page, force
materialization at the existing `ExecMaterializeSlot` choke point, and delete the two copies.**
This is a substrate change (the slot's stored-tuple representation gains a borrow), but it is
much smaller than "introduce a new buffer slot from scratch," because the slot *variant*, the
pin, and the materialize dispatcher already exist.

---

## 1. Current state (grounded in files)

### Slot types — crate `types_slot`
`crates/_support/types/types_slot/src/lib.rs`

- `TupleTableSlot<'mcx>` base struct (`lib.rs:59`): `tts_flags`, `tts_nvalid`, `tts_ops:
  TupleSlotKind`, `tts_tupleDescriptor`, `tts_values: PgVec<Datum>`, `tts_isnull`, `tts_tid`,
  `tts_tableOid`. Flag constants `TTS_FLAG_SHOULDFREE/EMPTY/SLOW/FIXED` at `:42`.
- `TupleSlotKind` enum (`lib.rs:136`): `Virtual | HeapTuple | MinimalTuple | BufferHeapTuple`
  — the "tts_ops identity" token replacing C's function-pointer vtable.
- Four superstructures: `VirtualTupleTableSlot` (`:185`), `HeapTupleTableSlot` (`:194`,
  fields `tuple: Option<FormedTuple>`, `off: u32`, `tupdata: HeapTupleData`),
  **`BufferHeapTupleTableSlot` (`:211`, `base: HeapTupleTableSlot` + `buffer: Buffer`)**,
  `MinimalTupleTableSlot` (`:220`).
- `SlotData<'mcx>` enum (`lib.rs:246`): the runtime `TupleTableSlot *` — `Virtual | Heap |
  Minimal | BufferHeap`. Helpers `kind()/base()/base_mut()`; the BufferHeap downcast to
  `&base.base` is at `:272/:282`.
- `TupleTableSlotOps` is a **data-only** struct (`lib.rs:294`): `base_slot_size`,
  `has_get_heap_tuple`, `has_get_minimal_tuple`. NOT a trait. Per-kind population in
  `crates/backend/executor/execTuples/src/slot_payload_model.rs:48` (`ops_for_kind`).

The vtable *behavior* is free functions in
`crates/backend/executor/execTuples/src/slot_ops_vtables.rs` (`tts_virtual_* / tts_heap_* /
tts_minimal_* / tts_buffer_heap_*`), dispatched by `match` over `SlotData`
(`slot_clear/slot_release/slot_materialize/slot_ops_getsomeattrs/slot_getsysattr/slot_copyslot`
at `:1732`+).

### Physical-tuple carrier — crate `types_tuple`
- `HeapTupleData<'mcx>` (`heaptuple.rs:541`): `t_len`, `t_self`, `t_tableOid`, `t_data:
  Option<PgBox<HeapTupleHeaderData>>`. `clone_in` at `:549` (deep copy = `heap_copytuple`).
- `FormedTuple<'mcx>` (`common_heaptuple.rs:500`): `tuple: PgBox<HeapTupleData>` + `data:
  PgVec<u8>` (the user-data area at `td + t_hoff`). This is the **owned-body** carrier the
  slot stores. `clone_in` at `:506`.
  - The owned model splits what C keeps as one contiguous palloc block: C's `t_data` is a
    single pointer into either the page or a copied block; pgrust has a boxed header **plus**
    a separate `PgVec<u8>` body. The null bitmap `t_bits` is *also* copied into the owned
    header (`read_on_page_full:545-549`), whereas C leaves it aliased on the page.

### The copy/clone sites on the per-tuple path (the bug)
For `SELECT count(*) FROM t WHERE i%2=0` the qual is an executor filter (not a scankey), so
`nkeys==0` and `read_on_page_full` runs once per *returned* tuple; the page visibility pass
uses `read_on_page_header_only` (`common_heaptuple.rs:584`) which deliberately skips the
user-data `slice_in` (good — that optimization already exists).

Per returned row, page → slot:

| # | site | file:line | allocates |
|---|------|-----------|-----------|
| a | `read_on_page_full` user-data | `common_heaptuple.rs:554` (`slice_in`) | user-data area bytes, in `es_query_cxt` |
| b | `read_on_page_full` t_bits | `common_heaptuple.rs:548` | null bitmap (only if HEAP_HASNULL) |
| c | `read_on_page_full` header box | `common_heaptuple.rs:563` (`alloc_in`) | `HeapTupleHeaderData` |
| d | `read_on_page_full` tuple box | `common_heaptuple.rs:567` (`alloc_in`) | `HeapTupleData` |
| e | `FormedTuple::clone_in` in getnextslot | `scan.rs:1461` → `common_heaptuple.rs:509` | clones tuple box + `slice_in` data **again** |
| f | `HeapTupleData::clone_in` header | `heaptuple.rs:549` | header box + t_bits copy |

`slice_in` itself: `crates/_support/mcx/src/lib.rs:1352` — `vec_with_capacity_in` +
`extend_from_slice` (the `palloc`+`memcpy` idiom). The `mcx` is `estate.es_query_cxt`
(set in `nodeSeqscan/src/lib.rs:156`).

So: **two full materializations per returned row** (sites a–d, then e–f). C does zero on
this path; it copies only when `ExecMaterializeSlot` is forced.

### The deform path (downstream consumer of the body)
`slot_deform_heap_tuple` (`slot_deform.rs:293`) borrows `formed.tuple.t_data.t_bits` and
`formed.data` as `&[u8]` and deforms in place into `tts_values/tts_isnull` — no per-call Vec
copy. For `count(*) WHERE i%2=0`, column `i` is `int4` (by-value `Datum::ByVal`), so deform
allocates only the per-call `PgVec<DeformedColumn>` (`heaptuple.rs` deform), no per-column
`slice_in`. **Crucially, this deform path already only needs `&[u8]` slices** — it does not
care whether those slices point into the page or into owned storage. That is what makes the
borrow feasible: the hot consumer is already byte-slice-oriented.

### Lifetimes — already present
`TupleTableSlot`, all four superstructures, `SlotData`, `Datum`, `HeapTupleData`,
`FormedTuple` are all parameterized by a single arena lifetime `'mcx`. The bodies are
**owned** (`PgVec<u8>`). There is currently **no** lifetime tying a tuple to a page/pin.

### Pin handling — already faithful
`tts_buffer_heap_store_tuple` (`slot_store_fetch.rs:578`) mirrors C exactly: same-page
optimization (`if slot.buffer != buffer` → `ReleaseBuffer(old)` + `IncrBufferRefCount(new)`,
`:615-632`), `transfer_pin` handling, `should_free` free-on-overwrite. `tts_buffer_heap_clear`
(`slot_ops_vtables.rs:1336`) releases the pin and resets to `InvalidBuffer`. `ExecStoreBuffer
HeapTuple` (`slot_store_fetch.rs:68`) + `ExecStorePinnedBufferHeapTuple` (`:103`) exist.
`ExecMaterializeSlot` (`slot_store_fetch.rs:402`) delegates to `slot_materialize` and is the
natural materialize choke point.

---

## 2. C reference (PG 18.3) and the pgrust mapping

| C mechanism | what it does | pgrust counterpart |
|---|---|---|
| `loctup.t_data = PageGetItem(...)` | tuple "is" a pointer into the pinned page | **MISSING** — pgrust copies via `read_on_page_full` |
| `ExecStoreBufferHeapTuple` | store page-pointer tuple, bump pin | PRESENT (`slot_store_fetch.rs:68`) but stores an *owned* `FormedTuple` |
| `BufferHeapTupleTableSlot.tts_buffer` | the pin held while a page tuple is in the slot | PRESENT (`buffer: Buffer`, `types_slot lib.rs:215`) |
| same-page pin optimization | avoid re-pin churn across rows on a page | PRESENT (`slot_store_fetch.rs:615`) |
| lazy deform from page bytes | `slot_deform_heap_tuple` reads off `t_data` | PRESENT but reads off owned `formed.data` (`slot_deform.rs:347`) |
| `ExecMaterializeSlot` = `heap_copytuple` only when needed | copy page→palloc only when the tuple must outlive the pin | PRESENT (`slot_store_fetch.rs:402`); but today the tuple is *always* already owned, so it is a no-op-ish copy |
| pin released on `ExecClearTuple`/store of new page | bounded pin lifetime | PRESENT (`slot_ops_vtables.rs:1336`, `:615`) |

The single missing mechanism is **the borrow itself**: the slot's tuple must be able to be a
`&page[..]` view rather than owned bytes.

---

## 3. The design

### 3.1 Core idea
Add a borrowed-body case to the BufferHeap slot's stored tuple. The deform consumer already
takes `&[u8]`, so deform works unchanged regardless of where the bytes live. Two viable
representations:

**Option A — enum body on the BufferHeap slot (recommended).**
Introduce a `TupleBody` that is either borrowed or owned:

```rust
// conceptual — in types_tuple
enum HeapTupleBody<'mcx, 'page> {
    OnPage { header: HeapTupleHeaderRef<'page>, data: &'page [u8] }, // points into pinned buffer
    Owned(FormedTuple<'mcx>),                                        // materialized
}
```

The `OnPage` arm carries `'page`, a lifetime tied to the pinned buffer page. The slot's
`buffer: Buffer` field is the *runtime* pin that keeps that page valid; `'page` is the
*compile-time* witness that the borrow does not outlive that pin.

**Option B — keep `FormedTuple` owned but make its body a `Cow`-like.** Rejected: a `Cow`
still needs the borrow lifetime, and `PgVec<u8>` vs `&[u8]` unification gains nothing over A
while obscuring the two states.

Recommendation: **Option A**, but confined to the BufferHeap path. The `Heap`/`Minimal`/
`Virtual` variants stay owned exactly as today.

### 3.2 The hard part — lifetime threading vs. self-reference
The naive `TupleTableSlot<'mcx, 'page>` is **not** viable: slots are long-lived (allocated
once per scan node, in `ExecInitScanTupleSlot`) and reused across *many* page reads with
*different* `'page`s. A second lifetime parameter on the slot type would (a) need to be
re-instantiated every page (impossible for a stored field) and (b) propagate through all
~175 files that name `TupleTableSlot`. That is the trap that makes this "risky substrate."

Two ways out, in order of preference:

**Approach 1 — runtime pin + `unsafe` borrow, lifetime erased at the slot boundary
(matches C, recommended).**
C is exactly this: `t_data` is a raw pointer, validity guaranteed by the runtime pin, not the
type system. Mirror that: the `OnPage` arm holds a raw view (offset+len into the buffer's
page image, or a `NonNull<u8>`+len) rather than a checked `&'page [u8]`. Safety is upheld by
an **invariant**, not the borrow checker:

> INVARIANT: while a BufferHeap slot holds an `OnPage` body, `slot.buffer` is a valid pin on
> the page those bytes live in, AND the body is never read after the pin is released.

The pin is released only in `tts_buffer_heap_clear` and the same-page-overwrite branch of
`tts_buffer_heap_store_tuple` — both of which already drop/replace the body. So the invariant
is *local* to the BufferHeap ops in `execTuples`. This keeps the change off the 175-file
surface entirely: `SlotData<'mcx>`'s signature does not change. The cost is an `unsafe`
block + a tightly-documented invariant in one file. This is the honest, C-faithful choice and
the only one that does not explode the lifetime surface.

**Approach 2 — short-lived borrowed wrapper, never stored.** Keep the slot owned, but add a
borrowing *accessor* that yields a `BorrowedHeapTuple<'page>` for the *current* tuple only,
consumed within the `heap_getnextslot` → deform window, never stored across a page advance.
This is safer (real lifetimes) but does **not** eliminate the per-row copy unless deform is
driven directly from the borrow before the store — i.e. it restructures the scan so the slot
never holds page bytes. That is a bigger executor change (the slot abstraction assumes the
slot *owns* its current tuple). Lower payoff-to-risk than Approach 1.

**Decision: Approach 1.** It is the minimal faithful port: same invariant C relies on, same
pin field that already exists, confined to the BufferHeap ops.

### 3.3 Where materialization must be forced
A borrowed body must be converted to `Owned` (copy page→`es_query_cxt`) before any of:

1. The pin could be released while the bytes are still referenced — handled by making
   `tts_buffer_heap_materialize` the converter and ensuring every pin-release path either
   clears or materializes first (the clear path already drops the body).
2. The tuple crosses into a sink that stores it (tuplestore/tuplesort/hashtable). These are
   already `ExecCopySlotMinimalTuple` / `tuplestore_puttupleslot` call sites (see §4) which
   *form a minimal tuple from the slot* — they read the body via deform/`get_minimal_tuple`,
   which copies anyway. The borrow is consumed (read) there, not stored, so these are safe as
   long as they run while the pin is held (they do — they run on the current slot tuple).
3. The tuple is returned "upward" past a scan boundary where the next `getnextslot` would
   overwrite the page — the executor already calls `ExecMaterializeSlot` / `ExecCopySlot` at
   those boundaries (e.g. EPQ, junction into ModifyTable). Those paths route through
   `slot_materialize`, which becomes the converter.

The key realization: **the existing `ExecMaterializeSlot` choke point is exactly C's, and the
sinks already copy-out via deform/minimal-tuple.** So forcing materialization = implementing
`tts_buffer_heap_materialize` to copy the `OnPage` body into an `Owned` body (it currently is
a near-noop because the body is always owned). No new choke points needed.

### 3.4 Pin-leak avoidance
Unchanged from today: the pin is acquired in `tts_buffer_heap_store_tuple` and released in
`tts_buffer_heap_clear` / same-page-overwrite. The only new rule is that `materialize` must
**also release the pin** after copying (C does: once materialized, `tts_buffer = Invalid`),
which `tts_buffer_heap_materialize` must now actually do (today there is no page borrow so the
pin release semantics need an audit — see Phase 1).

---

## 4. Blast radius

- Slot type crate: `types_slot` — 1 file (`lib.rs`). With Approach 1, **no signature change**
  to `SlotData`/`TupleTableSlot`; only the BufferHeap body representation changes (or a new
  `HeapTupleBody` type in `types_tuple`).
- Ops crate: `crates/backend/executor/execTuples/` — the real edit surface:
  `slot_ops_vtables.rs` (`tts_buffer_heap_*`: getsomeattrs/materialize/clear/store/copy),
  `slot_store_fetch.rs` (`tts_buffer_heap_store_tuple`, `ExecStoreBufferHeapTuple`,
  `ExecMaterializeSlot`), `slot_deform.rs` (`slot_deform_heap_tuple` — point it at the borrow
  for the `OnPage` arm).
- Producer: `crates/backend/access/heap/heapam/src/scan.rs` — `heap_getnextslot` (`:1434`,
  delete the `clone_in` at `:1461`, store a borrow), `read_on_page_full` callers in
  `heapgettup_pagemode` (`:984`), and the bitmap-heap-scan store path (`scan.rs:1462`-region
  analog). `common_heaptuple.rs` — add an `OnPage`/borrow constructor next to
  `read_on_page_full`.
- Sinks (must be verified to materialize/copy-out before the pin moves on): the
  `tuplestore_puttupleslot` sites (nodeMaterial `:166`, nodeWindowAgg `:1605/:1691`,
  nodeRecursiveunion, nodeCtescan, tstoreReceiver, trigger firing), the
  `ExecCopySlotMinimalTuple` sites (nodeMemoize, nodeSort/IncrementalSort, nodeSetOp,
  nodeAgg), and nodeHash/nodeHashjoin (route through `ExecStoreMinimalTuple` — closer look
  needed). These are READ sites, not store sites, so they are already correct as long as they
  run under the pin — but each must be confirmed.

Scale: ~175 files name `TupleTableSlot`, but with Approach 1 the **changed** set is ~6–8
files (the producer + the execTuples ops + the body type). The other ~167 are *audited, not
edited* (the correctness gate is the regression suite, since the change is in shared
substrate behavior, not in their signatures).

---

## 5. Risks + phased plan

Silent result corruption (a tuple read after its pin moved → garbage bytes from a recycled
buffer) is the danger. Every phase gates on the full 230-file regression suite (this is a
shared-substrate change → MEMORY policy says full suite, not targeted).

**Phase 0 — instrument & confirm (no behavior change).**
Add a debug-build assertion in `tts_buffer_heap_*` read paths that the pin is valid, and a
counter for materializations. Confirm the current `clone_in`-at-`scan.rs:1461` double-copy in
a profile. Gate: build only.

**Phase 1 — make materialize real, keep owned body (de-risk the choke point first).**
Audit `tts_buffer_heap_materialize` to ensure it copies + releases the pin (C semantics),
even though the body is still always owned. This makes the materialize path correct *before*
we introduce a borrow that depends on it. Gate: full suite (must stay 230/230).

**Phase 2 — introduce the borrowed body behind the seqscan only.**
Add the `OnPage` arm to the BufferHeap body. Add a borrow constructor in `common_heaptuple.rs`
(view into the page item, no `slice_in`). In `heap_getnextslot`, store the borrow instead of
`clone_in` — but **immediately materialize** at the top of `slot_getsomeattrs`/before any
return (aggressive materialization). This deletes copy `e/f` (the second clone) while keeping
the borrow window tiny and provably-pinned. Point `slot_deform_heap_tuple` at the borrow for
the `OnPage` arm so the deform reads the page directly (deletes copy `a/b` for the deform
read). Gate: full suite. Highest-risk step — measure correctness *and* perf here.

**Phase 3 — relax materialization.**
Once Phase 2 is green, stop materializing on every `getsomeattrs` and let the borrow live for
the natural slot lifetime (until clear / next-page store), matching C. Verify each §4 sink
materializes/copies before the pin moves. Gate: full suite + isolation suite (concurrent
buffer recycling is where a pin bug surfaces) + the recovery TAP (buffer pin accounting).

**Phase 4 — extend to bitmap heap scan + index-fetch heap path.**
Apply the same borrow to `heapam_scan_bitmap_next_tuple` and the heap-fetch in index scans
(they also go through `ExecStoreBufferHeapTuple`). Gate: full suite.

Highest-risk steps: **Phase 2** (introduces the unsafe borrow + deletes the copy) and
**Phase 3** (lets the borrow live across executor calls — the place a pin-accounting bug
becomes silent corruption). Both are guarded by the full + isolation suites.

---

## 6. Expected payoff

The profile attributes ~47% of serial CPU to per-tuple heap materialization, comprising the
two full copies (sites a–f). Breakdown of what this plan reclaims:

- **Copy e/f (the `clone_in` at `scan.rs:1461`)** — a *full redundant deep copy* of every
  returned tuple. Deleting it (Phase 2) is pure win with no downside; this alone should remove
  roughly *half* of the 47% (one of the two materializations) → ~20–25% of serial CPU.
- **Copy a–d (the `read_on_page_full` materialization)** — eliminated for the deform read once
  the borrow feeds deform directly (Phases 2–3). For `count(*) WHERE i%2=0` the body is never
  needed beyond deform (no materialize forced — count doesn't store the tuple), so the borrow
  survives and a–d disappear too → most of the remaining ~22%.

Realistic estimate: this plan can reclaim the **bulk of the ~47%** for scan-and-discard
queries (count/agg/filter where the tuple never crosses a materializing sink), narrowing the
39.5×-vs-C serial gap substantially — though not to parity, because pgrust still pays for
`PgVec<DeformedColumn>` per deform, `Datum` enum tagging, and seam-dispatch overhead that C
does not.

What it will **NOT** help:
- Queries that materialize anyway (sort/hash/tuplestore-bound, `SELECT *` into a tuplestore,
  CTE/window/recursive) — they copy at the sink regardless; the borrow just moves *when* the
  copy happens, not *whether*.
- By-reference (varlena/text) heavy columns where deform already `slice_in`s per-column — the
  borrow removes the *body* copy but per-column detoasting/copy on materialize remains.
- The deform-side allocations (`PgVec<DeformedColumn>`) and `Datum`/seam overhead — separate
  levers.

---

## Recommendation

Do it, via **Approach 1, Phases 0→3**, starting with the trivially-safe deletion of the
redundant `clone_in` at `scan.rs:1461` (Phase 2 first half) which is ~half the win for a tiny,
low-risk change. Treat Phase 3 (live borrow) as the real substrate commitment, gated by the
full + isolation + recovery suites. The change is far smaller than "new buffer slot" because
the slot variant, the pin, and the materialize choke point already exist and are faithful to
C — the only missing piece is the page borrow itself, which C implements with the same
runtime-pin invariant (not the type system) that Approach 1 adopts.
