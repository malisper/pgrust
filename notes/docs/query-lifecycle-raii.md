# Query-lifecycle resource management: owner values + RAII, no registries

The standing model for everything in Postgres that must be cleaned up at
statement/portal/transaction boundaries — including on the error path. This
replaces a faithful port of `resowner.c` and the ambient lifecycle globals.
Decided 2026-06-12; the mcx allocator design (`docs/mctx-design.md`) is the
same architecture applied to memory.

## What C is solving, and how that maps here

C has four ambient stacks that abort processing unwinds as a group:

| C ambient state | Here |
|---|---|
| `CurrentMemoryContext` (+ `MemoryContextSwitchTo`) | gone — explicit `Mcx<'mcx>` threading (`docs/mctx-design.md`) |
| `CurrentResourceOwner` + the resowner registry | gone — RAII guards + explicit owner values (this doc) |
| `ActiveSnapshot` stack (`Push/PopActiveSnapshot`) | a `SnapshotStack` owned by the transaction state, a `Ctx` facet |
| `error_context_stack` (`ErrorContextCallback`) | attach-on-propagation: `result.map_err(\|e\| e.add_context(...))` at the boundaries where C pushed callbacks |

All four exist in C because its only unwinding mechanism is
longjmp-to-sigsetjmp plus registries the abort path walks. Here errors return
through `?`, so destructors run on the error path — RAII is real, and the
registries' reason to exist disappears.

## Requirements (what any design must answer)

- **R1 — scoped release on error**: every resource released exactly once when
  its owning scope unwinds, never another scope's resources.
- **R2 — commit/abort asymmetry**: at commit, a still-held resource is a bug
  to WARN about (C parity); at abort, release is silent. `Drop` alone cannot
  distinguish these.
- **R3 — cross-kind ordering**: transaction end releases in phases (pins
  before locks, etc.) — needs exactly one home.
- **R4 — scope promotion**: some resources legitimately move to a longer-lived
  owner mid-flight (cache entries, held portals).
- **R5 — single authority**: exactly one thing may release a resource.

## Evidence: the registry approach failed in src-idiomatic

`../pgrust/TECH_DEBT.md` TD-17 and commit `36b392866` document the resowner
port's trajectory: pin bookkeeping first wired as silent no-ops (error-edge
buffer pins leaked for the backend's lifetime — heap_insert alone has ~16 such
edges); then rebuilt as a flat thread-local registry **without owner scoping**,
whose abort sweep over-released pins held by longer-lived owners (CREATE INDEX
died; sweep shipped disabled, leaks ledgered). A live `RefCell` re-borrow
panic in the release path was structural: release walks a registry that
release-callbacks mutate. The failure was not "resowner is hard" — a flat
registry without scoping fails in both directions at once, and scoping is
exactly what Rust ownership provides natively. Do not re-run this experiment.

## The model

Three composable layers:

**1. Frame-local resources: plain RAII guards.** Acquisition returns a guard
(`BufferPin`, `CatcacheRef`, file/DSM guards, ...); `Drop` releases; the guard
lives in whatever state struct needs it (a pin held across `scan_next` calls
lives in the scan state). `?` releases exactly this frame's resources — R1 and
R5 by construction. Guards reach their manager through a **cheap owned handle**
(`Rc` to the per-backend singleton), not a borrow — see "owned handles" below.

**2. Transaction/portal spine: owner values, commit by consumption.**

```rust
pub struct TxnResources {
    pins: Vec<BufferPin>,        // released first ─┐
    locks: Vec<LockGuard>,       //  then locks      ├─ R3 lives here, once
    files: Vec<FileGuard>,       //  then the rest  ─┘
    snapshots: SnapshotStack,
}

impl TxnResources {
    /// Commit consumes the owner: ordered teardown, WARN on anything
    /// still held (R2, C's commit-time leak warnings).
    pub fn commit(mut self) -> PgResult<()> { ... }
}
impl Drop for TxnResources {
    /// The abort path: same order, silent. Reached by `?` anywhere
    /// between transaction start and commit().
    fn drop(&mut self) { ... }
}
```

Commit is a method that consumes the owner; abort is simply not calling it.
Promotion (R4) is `txn.pins.push(guard)` — a checked move. Live-count tallies
for richer leak diagnostics can be added inside the owner without changing its
API (the observer pattern, as mcx's accounting tree).

**3. The threading currency: one `Ctx<'q>` value.** This is PG's own pattern
made universal — `EState`/`ExprContext` are exactly this bundle in C; the
ambient globals were the workaround for paths without an estate in hand.

```rust
/// Public fields so field-level split borrows work.
pub struct Ctx<'q> {
    pub mcx: Mcx<'q>,                 // current allocation target (Copy)
    pub res: &'q mut TxnResources,
    pub snap: &'q mut SnapshotStack,
}

impl<'q> Ctx<'q> {
    /// Reborrow to pass down the call tree. Ctx contains &mut fields, so it
    /// is not Copy and Rust does not auto-reborrow custom structs; rb() is
    /// the manual `&mut *` at each descent point.
    pub fn rb(&mut self) -> Ctx<'_> {
        Ctx { mcx: self.mcx, res: self.res, snap: self.snap }
    }
    /// Derived context with a different allocation target — the explicit
    /// MemoryContextSwitchTo (e.g. per-tuple bump context in a loop).
    pub fn with_mcx<'b>(&'b mut self, mcx: Mcx<'b>) -> Ctx<'b> {
        Ctx { mcx, res: self.res, snap: self.snap }
    }
}
```

## Rules

1. **Narrowest capability wins.** A function that only allocates takes
   `Mcx<'_>`, not `Ctx` — the signature documents that it cannot pin, lock,
   or touch snapshots. Take `Ctx` only when you need two or more facets (or
   pass through to something that does). `ctx.mcx` is `Copy`, so handing the
   narrow capability down is free.
2. **`Ctx`'s facet set is closed**: memory, resources, snapshots. Adding a
   facet is an AGENTS.md-level design decision, not a convenience edit — this
   is the types-god-crate lesson applied to context structs.
3. **Owned handles, single lifetime.** `TxnResources` must stay lifetime-free
   (guards hold `Rc`-style handles to per-backend managers, not borrows);
   otherwise `Ctx` needs `&'r mut TxnResources<'bm>` and the
   `&'a mut T<'a>` invariance trap multiplies lifetimes through every
   signature. One `'q` everywhere is the deliberate trade.
4. **No registries with release authority.** If a design wants "remember this
   resource so something else can release it later," the answer is moving the
   guard into an owner value, never a side table (R5; TD-17).
5. **Error context attaches on propagation**, at the same boundaries where C
   pushed `ErrorContextCallback`s: `.map_err(|e| e.add_context("while …"))`.
   No ambient callback chain.
6. **C mapping for ports/audits**: functions taking `EState`/`ExprContext` map
   to `Ctx`; functions using the ambient four get `Ctx` (or narrower) added as
   a parameter; `PG_TRY`/`PG_CATCH` cleanup blocks become guard `Drop` impls;
   `ResourceOwnerRemember/Forget` pairs become guard construction/drop;
   `RESOURCE_RELEASE_*` phase ordering is `TxnResources`' field order.

## Catalog consequences

`backend-utils-resowner-all` is not ported as a unit: its semantics dissolve
into per-subsystem guard types plus the owner values above (recorded in
CATALOG.tsv notes). `backend-utils-time-snapmgr`'s ActiveSnapshot stack ports
as the `SnapshotStack` facet, owned, not ambient. The first forcing case for
the guard layer is the bufmgr port (pins) — build `BufferPin` + the owned-handle
manager link there, with this doc as the acceptance criteria.
