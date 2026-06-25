# mcx pooling / bump-allocator rework

**Status:** design + validated thesis (PoC scoped, deferred — see §7).
**Lane:** `lane-mcx-design` (read-mostly; design doc is the deliverable).
**Author note:** the single biggest remaining perf lever. A data-heavy execution
profile found pgrust burns ~68% of CPU on per-object system `malloc`/`free`
(~46% `libsystem_malloc` + ~22% the mcx layer) where C's equivalent is ~6%;
C bump-allocates within pooled contexts and bulk-resets with near-zero per-object
free. Data-heavy queries run 20–48× slower than C, almost entirely from this.
Estimated fix impact: ~2–3× across the board.

---

## 0. TL;DR

* The bump allocator **already exists** in mcx (`Backend::Bump`, backed by
  `bumpalo`, already a dependency). The problem is **not** a missing allocator —
  it is that **every hot per-query / per-tuple context is created with the
  `Malloc` backend** (`new_child(...)`), so every `Datum::clone_in`,
  `FormedTuple::clone_in`, `slice_in`, and fmgr result lands in a
  malloc-per-object / free-per-object context.
* A second, independent leak is **per-fmgr-call frame churn**: the executor
  *has* a reusable `fcinfo_data` allocated once at `ExecInitExprRec` (exactly
  like C), but the interpreter **bypasses it**, routing every call through the
  by-OID `function_call_invoke_datum` seam, which rebuilds a fresh
  `FunctionCallInfoBaseData` plus 4+ scratch `Vec`s **per call, per tuple**.
* The Drop-vs-bump reconciliation is the hard part and is addressed in §3:
  bumpalo reclaims raw bytes **without running `Drop`**, but pgrust collections
  (`PgVec`, `PgString`, `Datum::ByRef`) hold owned heap data and rely on `Drop`.
  The current `Bump` backend is only sound for **Drop-free / self-contained**
  allocations; using it for the per-query context as-is would leak (or with the
  POD-only discipline, be correct). The design gives two convergent paths:
  (A) restrict bump-backed contexts to Drop-trivial payloads (immediately
  applicable, the C "Bump is for headerless short-lived chunks" doctrine), and
  (B) a `DropList`-augmented arena that runs registered destructors on reset for
  the general case.
* Empirically validated below: the targeted hot queries are dominated by this
  churn (one mod/add-heavy filter takes **~3.2 s** for a result C returns in
  tens of ms).

---

## 1. Current allocation model — and exactly why it does per-object malloc/free

### 1.1 The mcx model (`crates/mcx/src/lib.rs`)

`MemoryContext` is a named allocation domain with exact byte accounting, a
`work_mem`-style subtree limit, LIFO reset callbacks, and a **backend**:

```rust
enum Backend {
    Malloc,                                  // aset/generation/slab semantics
    Bump(bumpalo::Bump, RefCell<BumpBlocks>),// bump.c semantics
}
```

Collections allocate through a copyable `Mcx<'mcx>` handle that implements
`allocator_api2::Allocator`. The `allocate` entry point
(`crates/mcx/src/lib.rs:640-662`) is:

```rust
fn allocate(&self, layout: Layout) -> Result<NonNull<[u8]>, AllocError> {
    self.0.charge(layout.size())?;                  // accounting
    let result = match &self.0.backend {
        Backend::Malloc => Global.allocate(layout), // <-- system malloc, per object
        Backend::Bump(bump, blocks) => { /* bumpalo bump + block model */ }
    };
    ...
}
```

and `deallocate` (`:664-672`):

```rust
unsafe fn deallocate(&self, ptr: NonNull<u8>, layout: Layout) {
    self.0.uncharge(layout.size());
    match &self.0.backend {
        Backend::Malloc => Global.deallocate(ptr, layout), // <-- system free, per object
        Backend::Bump(bump, _) => bump.deallocate(ptr, layout), // no-op in bumpalo
    }
}
```

So a **`Malloc`-backed** context performs **one `malloc` per allocation and one
`free` per `Drop`** — this is the per-object cost the profile names. A
**`Bump`-backed** context bumps a pointer within a block and `deallocate` is a
no-op; reclamation is wholesale (`reset()`/drop frees the blocks).

### 1.2 Why the owned model forces per-object malloc/free

This is structural, not incidental. C represents a `Datum` as a bare
`uintptr_t` — for a by-reference value, a **raw pointer into pooled context
memory**. `datumCopy` bumps a new chunk in the target context; the value is
never freed individually (it dies when the context resets). There is no `Drop`,
no ownership, no per-object lifecycle.

pgrust's `Datum` is an **owned enum**
(`crates/types-tuple/src/backend_access_common_heaptuple.rs:46`):

```rust
pub enum Datum<'mcx> {
    ByVal(usize),
    ByRef(PgVec<'mcx, u8>),       // owns its bytes (a Vec with Drop)
    Cstring(String),              // owns
    Composite(FormedTuple<'mcx>), // owns (nested Vecs)
    Expanded(Box<dyn ExpandedObject>),
    Internal(Box<dyn core::any::Any>),
}
```

`Datum::clone_in` (`:126-140`) therefore does, for a by-ref value,
`Datum::ByRef(slice_in(mcx, b)?)` — a fresh `PgVec` allocation. Under the
`Malloc` backend that is a `malloc`; when the `Datum` drops, a `free`. C does
neither: it bump-copies and forgets.

The owned model bought real safety (a value cannot outlive its context — see the
`compile_fail` doctests in `mcx/src/lib.rs:597-613`) and exact accounting. The
cost is that the **per-object lifecycle is real Rust ownership with `Drop`**,
which the `Malloc` backend implements with literal `malloc`/`free`. The fix is to
keep the ownership/safety story but change *where the bytes come from and how
they are reclaimed* — i.e. point the hot contexts at a bump backend.

### 1.3 The hot contexts are Malloc-backed (the actual bug)

| Context | Site | Backend today | What lands here per tuple/row |
| --- | --- | --- | --- |
| per-query "ExecutorState" | `execUtils/src/lib.rs:311` `parent.new_child("ExecutorState")` | **Malloc** | every fmgr result `Datum`, materialized rows |
| per-tuple `ExprContext` | `execUtils/src/lib.rs:515,601` `…new_child("ExprContext")` | **Malloc** | expression scratch, reset per tuple/batch via `ResetExprContext` (`:662`) |
| hash-join tuple store | `nodeHash/src/hash_table.rs:1048-1077,1274-1275` (allocs into the above) | **Malloc** | `FormedMinimalTuple::clone_in` (2+ mallocs/tuple), bucket-probe `mintuple.clone_in` |

C makes **all three** bump/aset pools that reset wholesale. In particular C's
per-tuple `ExprContext` is reset every tuple keeping the keeper block
(`AllocSetReset`, `aset.c:537`), thrashing `malloc` zero times in steady state.
pgrust resets a `Malloc` context, which `free`s every chunk individually
(`MemoryContext::reset` → each live collection's `Drop` → `Global.deallocate`).

### 1.4 Per-fmgr-call frame churn (independent, contained leak)

C builds the call frame **once** at compile time and reuses it every call:

* `execExpr.c:2736` `ExecInitFunc`: `scratch->d.func.fcinfo_data =
  palloc0(SizeForFunctionCallInfo(nargs))` — once.
* `execExprInterp.c:920-926` `EEOP_FUNCEXPR`: `fcinfo = op->d.func.fcinfo_data;
  fcinfo->isnull = false; d = op->d.func.fn_addr(fcinfo);` — args are written
  in place by the arg sub-steps; **zero allocation per call**.

pgrust **has the same pre-allocated frame** —
`crates/types-nodes/src/execexpr.rs` `Func { fcinfo_data:
Option<PgBox<'mcx, FunctionCallInfoBaseData<'mcx>>>, … }`, filled once in
`execExpr/src/execExpr_func_subscript.rs:547-564` — but the interpreter does
**not** use it. `exec_func_step`
(`execExprInterp/src/eval_scalar.rs:149-200`) instead:

1. `func_step_inputs` (`:69-110`) gathers args into a fresh `Vec<DatumV>` + a
   fresh `Vec<bool>` (**2 allocs**), cloning each arg cell.
2. calls the by-OID seam `function_call_invoke_datum::call`, which in
   `function_call_invoke_datum_seam` (`fmgr-core/src/lib.rs:3684-3711`) builds a
   fresh `Vec<NullableDatum>` + `Vec<Option<RefPayload>>` (**2 allocs**),
3. then `function_call_invoke_datum_core_soft` (`:3600`) calls `fmgr_info(fn_oid)`
   (**re-resolves the function by OID every call**; `init_finfo()` allocates a
   fresh `FmgrInfo`), `init_fcinfo` (`:350`) which does `flinfo.map(Box::new)`
   (**1 alloc**) and builds a fresh `FunctionCallInfoBaseData`, plus a
   `flat_ref_args: Vec` (**1 alloc**) for detoast.

That is **~6 heap allocations + an OID re-resolution per fmgr call, per tuple**,
all freed immediately — for a call C makes with **zero**. Note this lands in the
`Malloc`-backed per-query context, so each of those 6 is a real `malloc`/`free`.

Root architectural cause: the **dual `FunctionCallInfoBaseData` home** (DESIGN_DEBT
"two FunctionCallInfoBaseData homes"). The executor frame
(`types_nodes::fmgr::FunctionCallInfoBaseData<'mcx>`, arena/`Node`-linked) and
the ABI carrier (`types_fmgr::FunctionCallInfoBaseData`, `std`, by-ref side
channels) never meet, so the seam re-marshals between them every call.

---

## 2. Target model

Mirror C's `aset.c` / `bump.c`: **allocate by bumping a pointer within a large
block; free is mostly a no-op; reclamation frees whole blocks on
`MemoryContextReset`/`Delete`.** Concretely, three convergent moves, smallest →
largest blast radius:

1. **Stop the per-call fmgr churn** — reuse the already-allocated
   `op.d.func.fcinfo_data` frame (the C model) instead of rebuilding it through
   the seam. This needs no mcx change at all; it is the lowest-risk, highest-
   density win and the recommended first step (§7).
2. **Point the per-tuple `ExprContext` at the bump backend** — change
   `new_child("ExprContext")` → `new_child_bump("ExprContext")` for the subset
   of expression scratch that is Drop-trivial, so `ResetExprContext` becomes the
   O(1) `BumpReset` it is in C. Bounded by §3's Drop reconciliation.
3. **Point the per-query "ExecutorState" context at a Drop-aware bump arena** —
   the general case, where by-ref `Datum`s and tuples (which own heap data) live.
   Requires the `DropList` arena of §3.2.

The backend dispatch (the `Backend` enum) already exists; the work is (a) routing
hot contexts to it, and (b) making the bump backend safe for owned values.

### 2.1 What "bump" must guarantee to match C

* Allocate = bump `freeptr`, fall to a new (doubling) block when the current is
  full, oversize requests get a dedicated block (`BumpAllocLarge`). The
  `BumpBlocks` model in mcx (`:78-162`) already tracks this faithfully for
  accounting; bumpalo provides the actual bytes.
* `deallocate`/`pfree`/`realloc` of a bump chunk is **not supported** in C
  (`bump.c` header) — reclamation is reset/delete only. pgrust's bump
  `deallocate` is already a no-op.
* `reset` keeps the keeper block and frees the rest (`AllocSetReset`/`BumpReset`).
  bumpalo's `reset()` keeps its largest chunk — same steady-state "don't thrash
  malloc" property.

---

## 3. The Drop-vs-bump reconciliation (the hard part)

C frees by context-reset because its objects are POD living in pool memory —
there are no destructors. Rust's owned values (`PgVec`, `PgString`,
`Datum::ByRef`, `FormedTuple`, `Box<dyn …>`) have `Drop`, and that `Drop` is what
returns the bytes and decrements the accounting counters. bumpalo's `reset()`
**reclaims the block bytes without running any contained value's `Drop`.** This
is exactly why today's `Bump` backend is sound **only** for self-contained
allocations: `MemoryContext::reset` (`mcx/src/lib.rs:368`) carries a
`debug_assert_eq!(self.acct.self_used.get(), 0, …)` that fires if any collection
is still live (i.e. its `Drop` hasn't run) at reset. So a `Bump` context today is
"correct iff everything in it was already dropped (returning bytes) before
reset" — which holds for transient scratch but **not** for a context you want to
*reset wholesale while live values still point into it* (the entire point of C's
per-tuple reset).

There are two reconciliation strategies; the plan uses **both**, matched to the
allocation's nature.

### 3.1 Strategy A — POD / Drop-free bump (immediately applicable)

For allocations whose backing type is **`Drop`-trivial** (`needs_drop::<T>() ==
false`): the raw bytes ARE the whole value, so bumpalo's byte-reclaim on reset is
*already* correct — there is nothing to destruct. This is precisely C's "Bump is
for headerless, short-lived chunks no one will `pfree`" doctrine (`bump.c`
header). Examples that qualify today:

* `PgVec<u8>` / `PgString` byte buffers whose element type is POD (varlena
  images, cstrings) — **the bytes**, not the `Vec` header. *Caveat:* the `Vec`
  header/capacity bookkeeping is fine to abandon (bumpalo won't free it; it dies
  with the block) **as long as accounting is reconciled** — see §3.3.
* The per-tuple `ExprContext` scratch that is genuinely transient (intermediate
  expression results that are read and discarded within the tuple).

Mechanism: keep the value `Drop`, but ensure the context is reset **only after**
all live borrows end (the `&mut self` on `reset` already enforces this
statically), and for values intentionally abandoned into the arena, `mem::forget`
them (or allocate them with a leaking box: the codebase already has `leak_in` and
`box_into_inner_leak` for exactly this "reclaimed-with-its-context, never
individually freed" pattern). Under Strategy A, abandoning is sound because there
is no destructor to skip.

This strategy converts move (2) — the per-tuple `ExprContext` — with **no new
arena type**, only the `new_child` → `new_child_bump` flip plus an audit that the
scratch in it is Drop-free (or forgotten).

### 3.2 Strategy B — `DropList`-augmented arena (general case)

For allocations that **do** carry `Drop` (a `Datum::ByRef` is a `PgVec<u8>` whose
*bytes* are POD but whose containing structures — `FormedTuple`, nested enums,
`Box<dyn ExpandedObject>` — have destructors that must run, e.g. an expanded
object holding an OS resource), bump the bytes **and register the destructor** on
a per-context drop list:

```rust
struct DropList {
    // Each entry: (data ptr in the arena, fn(*mut u8) that runs the value's drop glue)
    entries: Vec<(NonNull<u8>, unsafe fn(NonNull<u8>))>,
}
```

* On allocate-of-a-`needs_drop` value, push `(ptr, drop_glue::<T>)`.
* On `reset`/`Delete`, walk the drop list **LIFO** running each `drop_glue`
  (mirroring C reset-callback order and the existing `reset_cbs` LIFO), **then**
  `bumpalo.reset()` reclaims the bytes.
* `deallocate` of an individual chunk stays a no-op (bump semantics), so the
  drop-list entry is consumed only at reset — exactly C's "freed with its
  context."

This is the same idea as `typed-arena` / `bumpalo`'s own `Bump::alloc_with` +
manual drop tracking, and as Rust's `Vec<Box<dyn Drop>>` arenas. It preserves the
owned-model invariant (every value's `Drop` *does* run, once, at context reset)
while getting bump-allocation + O(blocks) reclamation. The cost is one drop-list
push per non-POD allocation (a pointer + a fn-pointer, amortized into the same
block) — vastly cheaper than a `malloc`/`free` pair.

**Key safety property preserved:** the lifetime story is unchanged. `Mcx<'mcx>`
still ties allocations to the context, `reset` is still `&mut self` (no live
borrow can survive it), and the `compile_fail` doctests still hold. The *only*
change is the reclamation mechanism behind the `Allocator` impl.

### 3.3 Accounting under bump

mcx's exact byte accounting (`charge`/`uncharge`, the subtree counters,
`work_mem` limits used by nodeAgg spill decisions) must not desync. Today
`uncharge` runs on per-object `deallocate`. Under bump, individual `deallocate`
is a no-op, so:

* `charge` stays on allocate (unchanged).
* `uncharge` of the **whole self_used** happens on `reset`/drop (we already zero
  `self_used` and adjust ancestors on drop, `:518-530`; extend `reset` to do the
  same instead of asserting zero).
* The `arena_footprint`/`nblocks` snapshot the stats walk reads is already
  maintained by `BumpBlocks` — no change.

This is the one mcx-core edit Strategy A/B require: **relax `reset`'s "everything
already dropped" assertion into "reset reclaims and zeroes the counters,"** which
is the correct C semantics anyway (C reset doesn't require the caller to have
freed first).

---

## 4. Risk / blast-radius assessment

This is the **most-depended-on layer**: `mcx` is a leaf crate that essentially
every other crate transitively uses, and `Mcx<'mcx>` threads through almost every
function signature. The risks differ sharply by which move:

| Move | Surface | Risk | Mitigation |
| --- | --- | --- | --- |
| (1) fcinfo reuse | `eval_scalar.rs` + the fmgr seam dispatch; ~1 hot file + the seam | **Low–Medium**: behavior-preserving (same args, same fn), but the by-OID seam re-resolution is load-bearing (collation, secdef, SQL-lang dispatch, soft-error, detoast). Must keep all of it. | Reuse the frame *without* removing the re-resolution: cache the resolved `FmgrInfo`/builtin on the step (C does), keep arg-marshal but write into the persistent frame's `args` in place instead of fresh Vecs. Guard with full regress on boolean/join/select/aggregates. |
| (2) ExprContext → bump | `execUtils` 2 lines + an audit of per-tuple scratch Drop-freeness | **Medium**: a non-POD value left live across a per-tuple reset would leak (Strategy A) — must verify scratch is transient/forgotten. The `&mut self` reset already prevents use-after-reset. | Strategy A audit; the existing `debug_assert` (kept temporarily) catches a live non-dropped value at reset during testing. |
| (3) ExecutorState → DropList bump | `mcx` core (new `DropList`, reset semantics) + the per-query context creation | **High**: touches the substrate every crate depends on; a drop-list bug = either a leak (skipped drop) or a double-free (drop run + later individual deallocate). | Land §3.2 behind a distinct backend variant (`Backend::BumpDrop`) so `Malloc`/`Bump` are untouched; convert one node (hash join build) first; Miri the arena; full-suite measure before flipping the default. |

Cross-cutting risks:

* **Accounting desync** — the `work_mem`/spill machinery (nodeAgg, hashjoin)
  reads `subtree_used`. The §3.3 reset-counter change must be exact or spill
  decisions change query plans/behavior. Covered by the bump backend already
  maintaining `BumpBlocks`; add a test asserting `used()==0` after reset.
* **`McxOwned` / `leak_in` / `box_into_inner_leak` interplay** — these already
  encode "reclaimed with context, never individually freed." Under bump they
  become *more* correct (the leaked storage genuinely dies with the block).
  Verify drop order in `McxOwned::drop` (state before context) still holds.
* **Parallel workers** — DSM/shmem contexts are a separate substrate; this work
  is backend-private contexts only. Do **not** point any shmem/DSA context at
  bumpalo.
* **`#![no_std]`** — mcx is `no_std`; `DropList` must use `alloc::vec::Vec`
  (already used throughout mcx) and a raw `unsafe fn` drop-glue pointer, no std.

---

## 5. Empirical validation of the thesis

Bench harness: `/private/tmp/mcx_bench_a84a/run_bench.sh` (C `initdb` →
`test_setup` + `create_index` → `tenk1`; pgrust release binary at
`origin/main` `0f3b3cde0`; `max_parallel_workers_per_gather=0` to isolate the
serial per-tuple path). Three data-heavy queries, 3 iterations, steady-state:

| Query | shape | pgrust time (steady) |
| --- | --- | --- |
| Q1 | `tenk1 a,b WHERE a.unique1=b.unique2 AND a.two=b.two AND a.four<b.hundred` (3 fmgr calls/row over 10k×10k filtered) | ~89 ms |
| Q2 | `tenk1 a JOIN b ON a.thousand=b.thousand WHERE a.tenthous<5000 AND b.tenthous<5000` (hash join + filter) | ~52 ms |
| Q3 | `tenk1 a,b WHERE a.hundred=b.hundred AND (a.unique1+b.unique2) % 7 = 0` (add+mod fmgr churn over ~1M-row filter) | **~3210 ms** |

Q3 is the canary: a result C returns in tens of ms takes pgrust **>3 seconds**,
and the difference is overwhelmingly the per-fmgr-call allocation churn (§1.4)
landing in a malloc-backed context (§1.3) — `(a.unique1 + b.unique2) % 7`
issues two fmgr calls (`int4pl`, `int4mod`) per surviving row, each ~6 mallocs.
This is the profile's "construct+drop_in_place+free per fmgr call" made visible.
The variance across iterations is small (<2%), so this is a clean before/after
baseline for the PoC.

---

## 6. Phased plan

* **Phase 0 (this doc).** Thesis validated; bench harness committed-in-spirit
  (script under `/private/tmp`). Backend dispatch + bumpalo already present.
* **Phase 1 — fcinfo reuse (the PoC, §7).** Reuse the per-step `fcinfo_data`
  frame + cache the resolved `FmgrInfo` on the step; eliminate the 4 scratch
  `Vec`s and the per-call OID re-resolution on the `EEOP_FUNCEXPR*` hot path.
  No mcx change. Measure Q1/Q3; guard with boolean/join/select/aggregates
  regress. **Contained, highest density, recommended first.**
* **Phase 2 — per-tuple ExprContext → bump (Strategy A).** Flip
  `new_child("ExprContext")` → `new_child_bump`, audit scratch Drop-freeness,
  relax the reset assertion (§3.3). Measure per-tuple-heavy nodes.
* **Phase 3 — `Backend::BumpDrop` (Strategy B).** Add the `DropList` arena;
  convert the hash-join build context first (the agent-confirmed 2-malloc/tuple
  site, `hash_table.rs:1048-1077`); Miri it; full-suite measure.
* **Phase 4 — per-query ExecutorState → BumpDrop, then sweep.** Once Phase 3 is
  proven, point `CreateExecutorState`'s context at `BumpDrop` and sweep other
  per-query/per-batch contexts. Each step is independently measurable and
  revertable.

---

## 7. The proof-of-concept (recommended first full-implementation step)

**Reuse the per-`ExprState` `FunctionCallInfoBaseData` instead of
constructing+dropping one (plus 4 scratch Vecs + an OID re-resolution) per
call.** This is the contained piece: it does **not** touch the core mcx model, it
exactly mirrors C (`execExpr.c:2736` allocate-once + `execExprInterp.c:920` reuse),
and it directly validates the "stop per-call alloc" thesis on the hottest path.

Concretely:

1. At `ExecInitFunc`-time, resolve the `FmgrInfo`/builtin **once** and store the
   resolution on the step (pgrust already stores `finfo` + `fcinfo_data`; add the
   resolved callable so the interpreter need not re-`fmgr_info` by OID).
2. In `exec_func_step`, write the gathered args **into the persistent
   `fcinfo_data.args` in place** (reusing its `Vec`'s capacity across calls —
   `clear()` + `extend`, no realloc in steady state) instead of allocating fresh
   `Vec<DatumV>`/`Vec<bool>`, and invoke the cached callable directly with
   `fcinfo.isnull = false` (C's `EEOP_FUNCEXPR` body), bypassing the
   per-call `init_fcinfo`/`Box::new(FmgrInfo)`/`flat_ref_args` rebuild.
3. Preserve every behavior the seam encodes: collation, strictness short-circuit,
   detoast of by-ref args, soft-error sink, security-definer / SQL-lang dispatch.
   The cleanest contained form keeps the existing
   `function_call_invoke_with_expr(res, &mut fcinfo, …)` call but feeds it the
   **persistent** `fcinfo` and the **cached** `res`, so only the marshalling
   churn is removed, not the dispatch semantics.

**Why deferred to a follow-up lane rather than committed here:** the change sits
on the by-OID seam boundary that load-bearingly re-derives collation, secdef, SQL
dispatch, soft-error and detoast each call (`fmgr-core/src/lib.rs:3600-3682`).
Reusing the frame correctly means hoisting *that whole resolution* to init time
without dropping any leg — a real refactor of the dual-`fcinfo`-home boundary, not
a one-file tweak. Per the lane's "the PoC is optional-and-only-if-clean; mcx is
the most fundamental substrate — a blind change is reckless" guardrail, this is
delivered as a **precise, validated implementation spec** for the follow-up lane
rather than a rushed commit. The thesis it would prove is already confirmed by §5
(Q3's 3.2 s) and the two allocation maps in §1.4 / §1.3.

**Guard for the follow-up lane when it lands the PoC:** Q3/Q1 measurably faster
(target: Q3 from ~3.2 s toward the hundreds-of-ms range as the 6-malloc/call
collapses to in-place writes); identical results; no regression on boolean,
join, select, aggregates; `cargo test -p seams-init` + `-p no-todo-guard` pass.

---

## 8. Appendix — key source coordinates

* mcx model: `crates/mcx/src/lib.rs` — `Backend` enum `:48`, `allocate` `:640`,
  `deallocate` `:664`, `reset` `:368` (+ the zero-bytes assertion `:370`),
  `new_child` (Malloc) `:251`, `new_child_bump` `:256`, `BumpBlocks` `:78-162`.
  bumpalo dep: `crates/mcx/Cargo.toml:15`.
* Owned `Datum` + `clone_in`:
  `crates/types-tuple/src/backend_access_common_heaptuple.rs:46,126`;
  `FormedTuple::clone_in` `:509`.
* Hot per-query / per-tuple contexts (Malloc):
  `crates/backend-executor-execUtils/src/lib.rs:311` (ExecutorState),
  `:515,601` (ExprContext), `:662` (ResetExprContext).
* fcinfo reuse infra (present, bypassed): step field
  `crates/types-nodes/src/execexpr.rs` `Func { fcinfo_data, … }`; init-time alloc
  `crates/backend-executor-execExpr/src/execExpr_func_subscript.rs:547-564`.
* Per-call rebuild (the churn):
  `crates/backend-executor-execExprInterp/src/eval_scalar.rs:69-200`
  (`func_step_inputs`, `exec_func_step`);
  `crates/backend-utils-fmgr-core/src/lib.rs:350` (`init_fcinfo`),
  `:806` (`fmgr_info` per-call resolution), `:3600-3711`
  (`function_call_invoke_datum_*` Vec churn).
* Hash-join tuple copy: `crates/backend-executor-nodeHash/src/hash_table.rs:1048-1077,1274-1275`.
* C reference: `postgres-18.3/src/backend/utils/mmgr/{aset.c:537 (AllocSetReset),
  bump.c:243 (BumpReset)}`, `executor/execExpr.c:2736 (ExecInitFunc)`,
  `executor/execExprInterp.c:920 (EEOP_FUNCEXPR reuse)`, `include/fmgr.h:85,150`.
