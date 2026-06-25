# mctx — memory contexts with allocator-tied lifetimes

Design for this repo's memory-context system. Based on src-idiomatic's
`backend-utils-mctx` pilot, with one structural change: **the collection types
carry a lifetime tied to their allocator** (`PgVec<'mcx, T>`), so an allocation
can never outlive its context, accounting is exact by construction, and
backends can be real arena allocators (bump) instead of accounting shims.

## Why change src-idiomatic's model

src-idiomatic's pilot (3,245 lines, `../pgrust/src-idiomatic/crates/backend-utils-mctx`)
made contexts pure *accounting*: wrappers do not store their context, every
allocating method takes `&MemoryContext`, and the context owns none of the
memory. Consequences we fix:

1. **Accounting is only as accurate as call-site discipline.** Pass the wrong
   context to `free`/`push` and counters silently desync (their docs admit the
   "fragile recharge-every-survivor dance"). With a stored `&'mcx` allocator,
   charge/uncharge happen inside the allocator on every alloc/grow/shrink/free
   — there is no wrong-context argument to pass.
2. **No real arenas.** Accounting-only contexts can't do PG's `bump.c`/`aset.c`
   block behavior or cheap bulk reset. Owning backends (incl. bumpalo) can.
3. **API noise.** `vec.push(ctx, x)` everywhere; with the lifetime, `vec.push(x)`.

What we keep from the pilot: contexts as a named tree with limits and reset
callbacks; `PgVec`/`PgString`/`PgBox`/`PgHashMap` as the vocabulary; the rule
that consumer state structs are not self-referential.

## Core design

```rust
// crates/mcx (infrastructure crate, like seam-core)

pub struct MemoryContext {
    name: &'static str,
    backend: ContextBackend,        // aset | generation | slab | bump
    used: Cell<usize>,              // exact, maintained by the Allocator impl
    peak: Cell<usize>,
    limit: usize,                   // usize::MAX = unlimited (the default);
                                    // exceeded => Err(OOM). 0 is a real,
                                    // always-full limit.
    // tree + reset-callback fields as in the pilot
}

// The allocator handle every collection stores.
#[derive(Clone, Copy)]
pub struct Mcx<'mcx>(&'mcx MemoryContext);

impl allocator_api2::alloc::Allocator for Mcx<'_> { /* charge + delegate + uncharge */ }

pub type PgVec<'mcx, T> = allocator_api2::vec::Vec<T, Mcx<'mcx>>;
pub type PgBox<'mcx, T> = allocator_api2::boxed::Box<T, Mcx<'mcx>>;
pub type PgHashMap<'mcx, K, V> = hashbrown::HashMap<K, V, DefaultHashBuilder, Mcx<'mcx>>;
pub struct PgString<'mcx>(PgVec<'mcx, u8>);
```

Key choices:

- **`allocator-api2` for the allocator plumbing** (stable polyfill of the
  unstable `std` Allocator API). hashbrown already integrates with it natively,
  and **bumpalo implements its `Allocator` trait** — so collections-over-
  custom-allocators work on stable Rust with no unsafe of our own in the
  collection layer. External deps: `allocator-api2`, `hashbrown`, `bumpalo`
  (first non-trivial external deps in the workspace — flagged as a decision).
- **Backends mirror C's `mcxt_methods[]` table** as an enum (aset, generation,
  slab, bump), not a user-extensible trait — PG has exactly these implementations
  and an enum keeps `MemoryContext` object-safe-free and `Copy`-friendly to
  reference. `bump` is backed by bumpalo; `aset`/`generation`/`slab` start as
  malloc-per-chunk + exact accounting (semantics-first), with block-structured
  behavior an internal upgrade later — the API doesn't change.
- **Exact accounting & limits**: every allocate/grow/shrink/deallocate passes
  through `Mcx::allocate(..)`, which updates `used`/`peak` and enforces
  `limit`, failing allocation. OOM surfaces as `Err(PgError)` with
  `ERRCODE_OUT_OF_MEMORY` (`types-error`) at fallible-API call sites
  (`try_reserve`-style), matching PG's `ereport(ERROR, "out of memory")`.
- **Reset/delete are `&mut self`**: with collections borrowing `&'mcx`, the
  borrow checker statically proves no allocation survives a reset — the bug
  class PG handles by discipline (and src-idiomatic by counter games) becomes
  a compile error. Bump reset is O(1).
- **No ambient `CurrentMemoryContext`.** Lifetimes make a thread-local
  current-context unrepresentable, and that's the right call anyway: C call
  sites that `palloc` in the current context translate to functions taking
  `Mcx<'mcx>` (or allocating into a caller-provided collection). Long-lived
  roots (`TopMemoryContext` equivalent) are owned by the eventual process
  entry point, not a global. `MemoryContextSwitchTo` call sites restructure to
  explicit handles — this is a per-port translation rule, documented here so
  audits judge against it.

## C API mapping (audit reference)

| C | Here |
|---|---|
| `palloc`/`palloc0` (current ctx) | allocate via the `Mcx` the function receives |
| `MemoryContextAlloc(cx, n)` | `PgBox::new_in(v, mcx)` / collection in `mcx` |
| `repalloc` | `Vec` grow via the same allocator |
| `pfree` | drop |
| `MemoryContextSwitchTo` | explicit handle threading (no equivalent) |
| `MemoryContextReset` | `ctx.reset()` (`&mut`) |
| `MemoryContextDelete` | drop the context (subtree drops with it) |
| `GetMemoryChunkContext` | unnecessary — the lifetime/handle carries it |
| `MemoryContextMemAllocated` | `ctx.used()` (exact) |
| OOM `ereport(ERROR, …)` | `Err(PgError)` `ERRCODE_OUT_OF_MEMORY` |
| `MemoryContextStats*` | `ctx.stats()` returning a report; LOG emission deferred until elog lands (then direct dep, acyclic) |
| `AssertNotInCriticalSection` | debug_assert hook, no-op until critical-section state exists |
| context-metadata allocation (`AllocSetContextCreate`, `MemoryContextSetIdentifier`, `MemoryContextRegisterResetCallback`) | infallible global-allocator allocation (`Rc::new`, `String::from`, `Box::new`, children `push`) — **sanctioned divergence**: C's counterparts allocate context metadata with malloc/palloc whose failure is `ereport(ERROR)`; here the few bytes of accounting metadata abort the process on OOM instead of erroring. Only *user data* routed through `Mcx` must use the fallible (`try_`) APIs |

## Crate/catalog mapping

One infra crate `crates/mcx` covering catalog units `backend-utils-mmgr-mcxt`,
`-aset`, `-generation`, `-slab`, `-bump`, plus `alignedalloc`/`memdebug`
(`backend-utils-mmgr-small`): mark `combined-into:mcx`-style rows with notes.
Deps: `types-error` only (+ external `allocator-api2`, `hashbrown`, `bumpalo`).
Audit is function-by-function against the C with the table above as the
sanctioned divergence list; behavior parity is required for accounting,
limits, reset callback ordering (LIFO), and OOM conditions.

## Decisions (settled 2026-06-12)

1. **External deps approved**: `allocator-api2` + `hashbrown` + `bumpalo`.
   The collections are type aliases of real Vec/Box/HashMap; the only custom
   collection is `PgString` (allocator-api2 has no String).
2. **Accounting counts requested bytes** (capacity bytes flowing through the
   `Allocator` impl), not C block-level `MemoryContextMemAllocated` parity;
   the divergence is documented for audits. `ContextStats.arena_footprint`
   reports backend-held bytes where it differs (bump).
3. **Existing merged crates migrate eagerly** once the implementation is
   reviewed: one migration pass replacing owned Vec/String allocation with
   threaded `Mcx` handles. There is no ambient-context API to migrate away
   from in this repo — the rule is that none is ever introduced.
4. **Sanctioned divergence — `Tuplestorestate`'s type-erased payload.**
   `types_nodes::funcapi::Tuplestorestate` holds its engine state as
   `Option<Box<dyn Any>>` on the global allocator, while C pallocs the
   tuplestore's state into the captured `state->context`. The carrier itself
   crosses seams as `PgBox<'mcx, Tuplestorestate>` (so the *handle* is
   context-charged); the inner engine bytes are not. When the tuplestore
   owner (`utils/sort/tuplestore.c`) lands, it must either move the payload
   to a `'mcx`-carrying representation or re-justify this entry; until then
   the divergence is accepted here so accounting audits don't flag it
   per-consumer.
5. **Backend-lifetime global structs are an explicit owned-allocation
   exception.** Structs stored in `thread_local!` backend globals (`Port` and
   its `HbaLine` in `MyProcPort`; `DataDir`/`DatabasePath` strings) use plain
   owned `String`/`Vec`/`Box`, not `'mcx`-parameterized collections. In C
   these live in backend-lifetime contexts (`PostmasterContext`,
   `hbacontext`) that are reset only by wholesale replacement, which is
   exactly Rust drop-on-reassign; a `'mcx` parameter on the type would force
   a borrowed lifetime into every `thread_local!` cell, which `thread_local!`
   cannot express. The exception covers only values whose single owner is a
   backend-global cell — types threaded through query-lifetime call paths
   still take `Mcx`.
