# aset.c port — status & the heap-corruption it surfaces

Branch `mcx-aset` (off `origin/main` `0c6fdf338`). Commit `760ba4a5f`.

## What landed

A faithful port of PostgreSQL's `aset.c` block-pooling allocator as a new
`Backend::Aset(RefCell<AllocSet>)` in `crates/mcx`, wired as the default for
`MemoryContext::new` / `new_child` (i.e. ~every backend-private context). Motive:
the boolean.sql profiles show allocator churn is the #1 pgrust-vs-C gap (24–38% of
backend CPU in `libsystem_malloc` vs C's ~3%), because every default context was
`Backend::Malloc` — one `malloc` per chunk, one `free` per `Drop`.

`crates/mcx/src/aset.rs`: 11 power-of-two size-class freelists, keeper block,
doubling block sizes, dedicated `Global` blocks for `>8KiB`/`>8-align` requests,
`AllocSetReset` semantics. No per-chunk header (the context is `self`). Uniform
8-byte alignment. aset's per-chunk free maps cleanly onto Rust `Drop`→`deallocate`
— no bumpalo-style Drop reconciliation needed.

## Validation that PASSES

- 6 aset unit tests + full mcx suite green.
- **Miri clean** on the aset tests and all 23 non-`owned` mcx tests on the Aset
  backend. (The 3 `owned` tests hit a PRE-EXISTING `McxOwned` self-reference
  Stacked/Tree-Borrows violation present unchanged on `origin/main`.)
- Full `postgres` binary builds; boots; serves thousands of allocations.

## The blocker: heap corruption on real workloads

Booting the aset binary and running `boolean.sql` **segfaults** (SIGSEGV) where the
unmodified `origin/main` (perf-P2, system malloc) binary does **not**. Investigation:

| test | result |
|---|---|
| `boolean.sql` single-user, aset | **SIGSEGV** (deterministic) |
| same on perf-P2 (system malloc) | clean |
| crash site | `<Mcx as Allocator>::deallocate`, writing the freelist head into a chunk at address `0x1` |
| freelist **reuse disabled** (always carve fresh) | **still SIGSEGV** → not a freelist/UAF-of-next-pointer issue |
| bisect | clean at `boolean.sql:66`, crashes by `:68`; but `:68` (`pg_input_error_info`) **in isolation is clean** → trigger and victim are *decoupled* |
| **double every chunk** (slack after each alloc) | crash **moves later** (17 results vs crashing at line 68) but **still SIGSEGV** → not a simple small overrun; address-sensitive |

**Signature = classic heap corruption**: address-sensitive, decoupled
trigger/victim, masked by system malloc's size-rounding/red-zones, fatal under
aset's tightly-packed power-of-two chunks. The corrupt value `0x1` looks like a
small integer written into a freed/adjacent chunk (a length, bool, or refcount).

## Assessment

Two possibilities, not yet definitively separated:
1. **(More likely)** a latent memory bug in pgrust (buffer overrun, use-after-free,
   or type-confusion) that system malloc masks and aset's exact packing exposes.
   Supports: perf-P2 is clean; aset is Miri-clean in isolation; corruption is
   address-sensitive and decoupled — the hallmark of surfacing a pre-existing bug.
2. A subtle bug in the new `unsafe` aset code (realloc/carve) not covered by the
   unit/Miri tests (which don't replicate the exact real-workload allocation mix).

## Definitive next step (not yet done)

Build a **guard-page / red-zone debugging variant** of the AllocSet: place each
pooled chunk so its end abuts an `mprotect(PROT_NONE)` guard page (or write a
canary in the `csize - size` slack and verify it on every `deallocate`,
`panic!`-ing with `RUST_BACKTRACE` on mismatch). That converts the silent
corruption into a fault **at the exact offending write**, naming the pgrust (or
aset) call site. This is the clean way to decide (1) vs (2) and find the bug.

## RESOLVED — root cause: missing zero-sized (ZST) deallocation handling

The guard-page debug variant (`--features mcx/aset-guard`, see `aset.rs::guard`)
plus an `lldb` `process handle SIGSEGV --stop` run on the *plain* aset binary
pinned the faulting write:

```
EXC_BAD_ACCESS (code=1, address=0x1)
  frame #0 core::ptr::write<Option<NonNull<u8>>>   (writing the freelist head)
  frame #1 aset::dealloc                aset.rs:319
  frame #2 mcx::deallocate              lib.rs:696
  frame #3 drop_in_place<Box<dyn NodePayload, Mcx>>   <- A_Star node payload
  ...      drop_in_place<ColumnRef>  (the `*` in `SELECT * FROM pg_input_error_info(...)`)
  frame    transformTargetList / transformSelectStmt
```

`deallocate` was called with `ptr = 0x1` and `layout.size() == 0`. The chunk
being freed is a **zero-sized type** — the `A_Star` node (`pub struct A_Star;`,
a unit struct) inside `ColumnRef.fields` for the `*` in boolean.sql line 67
(`SELECT * FROM pg_input_error_info('junk','bool')`). `0x1` is the *dangling*
pointer an align-1 ZST `Box` carries.

`allocator_api2`'s `Box::new_in` **skips `allocate`** for a ZST (uses
`NonNull::dangling()`), but `Box::drop` **unconditionally** calls
`deallocate(dangling_ptr, Layout::for_value)` with `size == 0`. aset's `dealloc`
then did `core::ptr::write(ptr as *mut Option<NonNull<u8>>, head)` — an 8-byte
write into address `0x1` → SIGSEGV. System malloc (`alloc::Global`) masked this
because its `deallocate` is a no-op for size-0 layouts (per the `Allocator`
contract); aset's tightly-packed port omitted that case. The decoupled
trigger/victim and "still crashes with freelist reuse disabled" both follow:
the corruption is the write *into the dangling pointer itself*, independent of
the freelist.

**Verdict: an aset bug, not a latent pgrust bug.** It is the only corruption on
this path — boolean.sql now runs to completion (exit 0, correct output) under
the pooling allocator.

**Fix (this commit):** `aset.rs::alloc` returns a dangling, aligned, zero-length
pointer for `layout.size() == 0` (touching no pool), and `aset.rs::dealloc`
no-ops on `layout.size() == 0`, mirroring `alloc::Global`. Regression test
`zero_sized_alloc_dealloc_is_a_noop_on_dangling_ptr` reproduces the exact
ZST-`Box` drop pattern. The guard-page instrumentation is retained behind the
off-by-default `aset-guard` feature for future corruption hunts.
