# Audit: mcx (backend-utils-mmgr-mcxt, -aset, -generation, -slab, -bump, -small)

Crate: `crates/mcx` (`src/lib.rs`, `src/owned.rs`, `src/string.rs`).
C ground truth: `../pgrust/postgres-18.3/src/backend/utils/mmgr/{mcxt,aset,generation,slab,bump,alignedalloc,memdebug}.c`.
Cross-checked against `../pgrust/c2rust-runs/backend-utils-mmgr-{mcxt,aset,generation,slab,bump,small}` —
function lists agree (c2rust's `memdebug.rs` confirms `randomize_mem` is outside the build config).

Sanctioned divergence list: the "C API mapping" table and "Decisions" in
`docs/mctx-design.md`. Verdicts below are adapted for an infrastructure crate:

- **MATCH** — behavior preserved through the design's mapping.
- **SANCTIONED** — covered by a cited design-doc row/decision.
- **DEFERRED** — block-structured aset/generation/slab internals; the design says
  malloc-backed semantics-first. The *semantics* were verified; the block
  mechanics are the deferred part (full list at the end).
- **FIXED** — real finding, fixed in this audit, re-verified from scratch.

Design-doc rows cited below: `[palloc]`, `[ctxalloc]`=`MemoryContextAlloc`,
`[repalloc]`, `[pfree]`, `[switch]`=`MemoryContextSwitchTo`, `[reset]`,
`[delete]`=`MemoryContextDelete→drop`, `[chunkctx]`=`GetMemoryChunkContext`,
`[memalloc]`=`MemoryContextMemAllocated`, `[oom]`, `[stats]`=`MemoryContextStats*`,
`[critsec]`=`AssertNotInCriticalSection`, `[D2]`=Decision 2 (requested-bytes
accounting), `[bumpalo]`=design "bump is backed by bumpalo".

## mcxt.c (53 functions)

| C function | Port location | Verdict | Notes |
|---|---|---|---|
| GetMemoryChunkMethodID | — | SANCTIONED | `[chunkctx]` — no chunk headers; the `Mcx` handle carries the backend |
| GetMemoryChunkHeader | — | SANCTIONED | `[chunkctx]`; MEMORY_CONTEXT_CHECKING-only |
| MemoryContextTraverseNext | `stats_tree` recursion | MATCH | non-recursive C iteration ↔ recursion bounded by context-tree depth |
| BogusFree | — | SANCTIONED | `[chunkctx]` — bogus-pointer detection unneeded; types prevent freeing into the wrong allocator |
| BogusRealloc | — | SANCTIONED | same |
| BogusGetChunkContext | — | SANCTIONED | same |
| BogusGetChunkSpace | — | SANCTIONED | same |
| MemoryContextInit | — | SANCTIONED | `[switch]`/design: roots are owned by the eventual entry point, no globals; ErrorContext's reserved 8KB is that owner's job |
| MemoryContextReset | `reset()` | MATCH | child cascade is ownership (`[delete]`); self path verified below |
| MemoryContextResetOnly | `reset()` | MATCH | callbacks fire, all memory provably returned (`&mut` borrow), bump arena reclaimed O(1); C's `isReset` short-circuit is equivalent (empty callback list ⇒ no-op) |
| MemoryContextResetChildren | — | SANCTIONED | `[delete]` — children are independently owned values |
| MemoryContextDelete | `Drop for MemoryContext` | SANCTIONED | `[delete]`; bottom-up subtree delete = nested ownership drop order |
| MemoryContextDeleteOnly | `Drop for MemoryContext` | MATCH | callbacks first (popped-before-call, so an erroring/panicking callback can't double-fire), ident cleared (as C does), then backend released; residual-bytes return keeps ancestor counters exact |
| MemoryContextDeleteChildren | — | SANCTIONED | `[delete]` |
| MemoryContextRegisterResetCallback | `register_reset_callback` | MATCH | push-on-head ↔ Vec push; "called in reverse order of registration" preserved |
| MemoryContextCallResetCallbacks | `fire_reset_callbacks` | MATCH | pop-before-call loop is exactly C's `while ((cb = list)) { list = cb->next; call }`; verified re-entrant registration fires (and stays LIFO) in both |
| MemoryContextSetIdentifier | `set_ident` / `ident()` | **FIXED** | was MISSING; added `set_ident(Option<&str>)` (None forgets, as C NULL), ident surfaces in `ContextStats`/`TreeStats`, cleared on drop like `MemoryContextDeleteOnly`. Owned copy instead of caller-kept pointer (strictly safer, same observable). Re-audited + tested |
| MemoryContextSetParent | `McxOwned` move | SANCTIONED | design §McxOwned: lifetime extension by moving the bundle, not reparenting pointers; `(context, NULL)` delink case is the Drop path |
| MemoryContextAllowInCriticalSection | — | SANCTIONED | `[critsec]` — no-op until critical-section state exists |
| GetMemoryChunkContext | — | SANCTIONED | `[chunkctx]` |
| GetMemoryChunkSpace | `used()` granularity | SANCTIONED | `[D2]` — requested bytes, not chunk+header space |
| MemoryContextGetParent | — | SANCTIONED | `[delete]`/ownership — accounting tree deliberately exposes no parent handle |
| MemoryContextIsEmpty | `used()==0` / `subtree_used()==0` | MATCH | C: any children ⇒ nonempty, else backend inquiry; exact counters subsume both |
| MemoryContextMemAllocated | `used()` / `subtree_used()` | SANCTIONED | `[memalloc]` + `[D2]`: requested bytes, not block bytes; recurse=true is `subtree_used()`, O(1) by eager propagation |
| MemoryContextMemConsumed | `stats()` / `subtree_used`/`subtree_peak` | MATCH | totals counters through the stats mapping `[stats]` |
| MemoryContextStats | `stats()` | SANCTIONED | `[stats]` — report now, LOG emission when elog lands |
| MemoryContextStatsDetail | `stats_tree()` | SANCTIONED | `[stats]` |
| MemoryContextStatsInternal | `stats_tree()` internals | SANCTIONED | `[stats]`; max_children summarization belongs to the deferred emission |
| MemoryContextStatsPrint | — | SANCTIONED | `[stats]` (pure formatting/emission) |
| MemoryContextCheck | — | DEFERRED | MEMORY_CONTEXT_CHECKING sentinel walk — chunk mechanics |
| MemoryContextCreate | `with_backend` | MATCH | common init: name, zeroed counters, parent link (Rc, with amortized dead-Weak pruning), no callbacks |
| MemoryContextAllocationFailure | `oom(request)` | MATCH | verified vs C: message `out of memory`, SQLSTATE 53200 (`ERRCODE_OUT_OF_MEMORY`), detail `Failed on request of size %zu in memory context "%s".` — byte-identical shape (test `oom_error_shape_matches_mcxt_c`); `MCXT_ALLOC_NO_OOM` ↔ `AllocError` from `try_*`; the `MemoryContextStats(TopMemoryContext)` dump is `[stats]`-deferred |
| MemoryContextSizeFailure | — | SANCTIONED | `[palloc]`/`[ctxalloc]` shape: the Rust API has no flags/size-class surface; every allocation takes the checked (huge-capable) path, `Layout` caps at `isize::MAX`. The 1GB `MaxAllocSize` guard is chunk-header-encoding hygiene with no analogue; divergence documented here deliberately |
| MemoryContextAlloc | `alloc_in` / `PgBox::try_new_in` | MATCH | OOM error shape as above |
| MemoryContextAllocZero | value init | MATCH | Rust initializes; `MemSetAligned` has no uninitialized-alloc counterpart |
| MemoryContextAllocExtended | `try_*` APIs | MATCH | ZERO=init, NO_OOM=`try_*` `Result`, HUGE=see SizeFailure row |
| HandleLogMemoryContextInterrupt | — | SANCTIONED | `[stats]` — signal-driven stats logging deferred with emission (procsignal unported) |
| ProcessLogMemoryContextInterrupt | — | SANCTIONED | same |
| palloc | take `Mcx<'mcx>` param | SANCTIONED | `[palloc]` — no ambient context |
| palloc0 | same + value init | SANCTIONED | `[palloc]` |
| palloc_extended | same + `try_*` | SANCTIONED | `[palloc]` |
| MemoryContextAllocAligned | `allocate(Layout)` | MATCH | `Layout` carries alignment natively; Global and bumpalo honor it. The redirection-MemoryChunk machinery exists only because C `pfree` must rediscover the unaligned base — `deallocate(ptr, layout)` makes it unnecessary |
| palloc_aligned | same | SANCTIONED | `[palloc]` ambient + above |
| pfree | drop | SANCTIONED | `[pfree]` |
| repalloc | `Vec` grow via stored allocator | SANCTIONED | `[repalloc]` |
| repalloc_extended | `try_reserve` | SANCTIONED | `[repalloc]` + NO_OOM=`try_*` |
| repalloc0 | `resize(n, 0)` / grow+init | MATCH | zero-fills the extension exactly as C memsets `[oldsize, size)` |
| MemoryContextAllocHuge | normal allocation | MATCH | no size classes; all allocations are huge-capable (see SizeFailure) |
| repalloc_huge | normal grow | MATCH | same |
| MemoryContextStrdup | `PgString::from_str_in`/`clone_in` | MATCH | |
| pstrdup | `PgString::from_str_in` | SANCTIONED | `[palloc]` ambient |
| pnstrdup | slice + `from_str_in` | MATCH | C strnlen-stops at NUL within len; `&str` has no embedded-NUL convention — callers slice what they mean |
| pchomp | `PgString::chomp_in` | **FIXED** | was MISSING; added — copies with all trailing `'\n'` removed (`trim_end_matches('\n')` ≡ C's strlen-decrement loop). Re-audited + tested incl. only-trailing / all-newline / no-newline cases |

## aset.c (16 functions)

Semantics verified for the whole file: allocation returns usable memory or the
mcxt.c OOM error shape; free returns the bytes immediately; realloc preserves
contents and charges/uncharges the exact delta; reset/delete return everything;
accounting is exact per `[D2]`. The block/freelist mechanics are the deferred part.

| C function | Port location | Verdict | Notes |
|---|---|---|---|
| AllocSetFreeIndex | — | DEFERRED | freelist bucketing |
| AllocSetContextCreateInternal | `MemoryContext::new`/`new_child` | DEFERRED | create semantics MATCH; block sizes, keeper block, context freelists deferred |
| AllocSetReset | `reset()` | DEFERRED | reset semantics MATCH (callbacks, full reclaim); keeper-block retention deferred |
| AllocSetDelete | Drop | DEFERRED | delete semantics MATCH; context-freelist recycling deferred |
| AllocSetAllocLarge | `Mcx::allocate` | DEFERRED | oversize-chunk path; OOM shape MATCH |
| AllocSetAllocChunkFromBlock | `Mcx::allocate` | DEFERRED | |
| AllocSetAllocFromNewBlock | `Mcx::allocate` | DEFERRED | block-doubling growth |
| AllocSetAlloc | `Mcx::allocate` | DEFERRED | alloc semantics MATCH |
| AllocSetFree | `Mcx::deallocate` | DEFERRED | free semantics MATCH; `could not find block containing chunk` guard unrepresentable (types prevent the bug) |
| AllocSetRealloc | `Mcx::grow`/`shrink` | DEFERRED | content-preservation + exact delta accounting MATCH; in-place chunk reuse deferred |
| AllocSetGetChunkContext | — | SANCTIONED | `[chunkctx]` |
| AllocSetGetChunkSpace | — | SANCTIONED | `[D2]` |
| AllocSetIsEmpty | `used()==0` | MATCH | exact, vs C's `isReset` approximation |
| AllocSetStats | `stats()` | DEFERRED | used/peak exact; nblocks/freechunks need the block backend; emission `[stats]` |
| AllocSetCheck | — | DEFERRED | debug chunk-structure walk |

## generation.c (18 functions)

Caller-visible semantics are identical to aset (alloc/free/realloc/reset);
generation's distinction is purely block-lifecycle mechanics, all deferred.

| C function | Port location | Verdict | Notes |
|---|---|---|---|
| GenerationContextCreate | `MemoryContext::new`/`new_child` | DEFERRED | semantics MATCH via malloc backend |
| GenerationReset | `reset()` | DEFERRED | semantics MATCH |
| GenerationDelete | Drop | DEFERRED | semantics MATCH |
| GenerationAllocLarge | `Mcx::allocate` | DEFERRED | |
| GenerationAllocChunkFromBlock | `Mcx::allocate` | DEFERRED | |
| GenerationAllocFromNewBlock | `Mcx::allocate` | DEFERRED | |
| GenerationAlloc | `Mcx::allocate` | DEFERRED | semantics MATCH |
| GenerationBlockInit | — | DEFERRED | block helper |
| GenerationBlockMarkEmpty | — | DEFERRED | block helper |
| GenerationBlockFreeBytes | — | DEFERRED | block helper |
| GenerationBlockFree | — | DEFERRED | block helper |
| GenerationFree | `Mcx::deallocate` | DEFERRED | drop returns bytes immediately — strictly prompter than C's free-when-block-empties; accounting per `[D2]` |
| GenerationRealloc | `Mcx::grow` | DEFERRED | alloc-copy-discard semantics MATCH |
| GenerationGetChunkContext | — | SANCTIONED | `[chunkctx]` |
| GenerationGetChunkSpace | — | SANCTIONED | `[D2]` |
| GenerationIsEmpty | `used()==0` | MATCH | |
| GenerationStats | `stats()` | DEFERRED | |
| GenerationCheck | — | DEFERRED | debug |

## slab.c (17 functions)

No dedicated slab constructor exists yet (callers use `MemoryContext::new`);
the fixed-chunk-size contract and its enforcement are deferred with the backend.

| C function | Port location | Verdict | Notes |
|---|---|---|---|
| SlabBlocklistIndex | — | DEFERRED | blocklist bucketing |
| SlabFindNextBlockListIndex | — | DEFERRED | |
| SlabGetNextFreeChunk | — | DEFERRED | free-chunk pointer chain |
| SlabContextCreate | `MemoryContext::new` | DEFERRED | chunk-size capture/rounding deferred |
| SlabReset | `reset()` | DEFERRED | semantics MATCH |
| SlabDelete | Drop | DEFERRED | semantics MATCH |
| SlabAllocSetupNewChunk | `Mcx::allocate` | DEFERRED | |
| SlabAllocFromNewBlock | `Mcx::allocate` | DEFERRED | |
| SlabAllocInvalidSize | — | DEFERRED | the **runtime** `size != chunkSize` → `unexpected alloc chunk size %zu (expected %u)` ERROR is unenforceable without a slab backend — explicitly on the deferred list |
| SlabAlloc | `Mcx::allocate` | DEFERRED | alloc semantics MATCH; size check deferred (above) |
| SlabFree | `Mcx::deallocate` | DEFERRED | semantics MATCH; blocklist movement deferred |
| SlabRealloc | `Mcx::grow` | DEFERRED | C errors unless size==chunkSize (`slab allocator does not support realloc()`); malloc backend currently permits grow — restored when the slab backend lands (deferred list) |
| SlabGetChunkContext | — | SANCTIONED | `[chunkctx]` |
| SlabGetChunkSpace | — | SANCTIONED | `[D2]` |
| SlabIsEmpty | `used()==0` | MATCH | |
| SlabStats | `stats()` | DEFERRED | |
| SlabCheck | — | DEFERRED | debug |

## bump.c (19 functions)

The bump backend is a *real* arena (bumpalo), not deferred. Verified hard
against C semantics: chunks are headerless (so are bumpalo's), per-chunk free
does not reclaim (bumpalo dealloc reclaims only a trailing allocation — a
strict superset, invisible to callers), reset reclaims wholesale in O(1) and
the arena is reusable without regrowth (test `bump_context_reset_reclaims_and_reuses`).

| C function | Port location | Verdict | Notes |
|---|---|---|---|
| BumpContextCreate | `new_bump`/`new_child_bump` | MATCH | minContextSize/init/maxBlockSize knobs are internal block sizing, managed by bumpalo's doubling |
| BumpReset | `reset()` | MATCH | O(1) wholesale reclaim; keeper-block retention ↔ bumpalo retains its largest block |
| BumpDelete | Drop | MATCH | arena freed |
| BumpAllocLarge | bumpalo oversize path | MATCH | |
| BumpAllocChunkFromBlock | bumpalo pointer bump | MATCH | |
| BumpAllocFromNewBlock | bumpalo block growth | MATCH | |
| BumpAlloc | `Mcx::allocate` → bumpalo | MATCH | headerless chunks, exactly bump.c's layout choice; OOM error shape from `oom()` |
| BumpBlockInit | bumpalo internal | MATCH | |
| BumpBlockIsEmpty | bumpalo internal | MATCH | |
| BumpBlockMarkEmpty | bumpalo internal | MATCH | |
| BumpBlockFreeBytes | bumpalo internal | MATCH | |
| BumpBlockFree | bumpalo internal | MATCH | |
| BumpFree | `Mcx::deallocate` (no-op reclaim) | SANCTIONED | `[pfree]` maps pfree→drop universally and `Drop` cannot fail, so C's `pfree is not supported by the bump memory allocator` ERROR is unrepresentable; memory stays in the arena until reset (C's intent), accounting uncharges per `[D2]` (exact where C cannot track at all) |
| BumpRealloc | `Mcx::grow` → bumpalo alloc+copy | SANCTIONED | `[repalloc]` maps repalloc→grow on the stored allocator; old bytes remain arena-held (visible as `arena_footprint` − `used`) |
| BumpGetChunkContext | — | SANCTIONED | `[chunkctx]`; C also ERRORs (headerless) |
| BumpGetChunkSpace | — | SANCTIONED | same + `[D2]` |
| BumpIsEmpty | `used()==0` | MATCH | exact, vs C's block-scan approximation |
| BumpStats | `stats()` | SANCTIONED | `[stats]`; `arena_footprint` (bumpalo `allocated_bytes`) reports backend-held bytes per `[D2]` |
| BumpCheck | — | DEFERRED | debug walk of bumpalo internals |

## alignedalloc.c (4) and memdebug.c (1)

| C function | Port location | Verdict | Notes |
|---|---|---|---|
| AlignedAllocFree | `deallocate(ptr, layout)` | SANCTIONED | `[chunkctx]` — redirection chunks exist only to rediscover the unaligned base; `Layout` carries it |
| AlignedAllocRealloc | `grow`/`shrink` with aligned `Layout` | SANCTIONED | alignment preserved by `Layout`; C's alloc-copy-free fallback ≡ `Allocator` default behavior |
| AlignedAllocGetChunkContext | — | SANCTIONED | `[chunkctx]` |
| AlignedAllocGetChunkSpace | — | SANCTIONED | `[D2]` |
| randomize_mem | — | DEFERRED | `RANDOMIZE_ALLOCATED_MEMORY`-only; outside build config (absent from the c2rust unit); poisoning of recycled raw chunks has no counterpart while collections initialize values |

## Hard-verification notes (re-derived, not taken from the port's claims)

- **Reset-callback LIFO + re-entrancy**: C pops the head before calling
  (`MemoryContextCallResetCallbacks`); a callback registering a new callback
  prepends it, so it fires next. `fire_reset_callbacks` re-borrows and pops the
  Vec tail per iteration — identical order, identical re-entrant behavior,
  identical never-call-twice-on-error property. Test
  `reset_callbacks_fire_lifo_on_reset_and_drop` covers reset *and* drop firing.
- **OOM shape**: message/SQLSTATE/detail byte-compared against
  `MemoryContextAllocationFailure` (mcxt.c:1154); `ERRCODE_OUT_OF_MEMORY` =
  53200 confirmed against `errcodes.txt:409`.
- **Limit enforcement points**: limits are this design's addition (C mmgr has
  none; PG enforces work_mem caller-side via `MemoryContextMemAllocated`).
  `charge()` validates **every ancestor** (checked_add, so `usize::MAX` =
  unlimited falls out) before applying anything — a failed charge mutates no
  counter at any level (tests `limit_enforced_via_try_reserve`,
  `ancestor_limit_caps_descendants` assert the no-mutation property).
- **Allocator charge/uncharge on every path**: `allocate` charges then undoes
  on backend failure; `deallocate` uncharges unconditionally; `grow` charges
  only the delta and undoes on failure; `shrink` uncharges the delta **only on
  success** (a failed shrink keeps the old block, so counters stay truthful).
  Delta arithmetic cannot underflow (Allocator contract: grow ⇒ new ≥ old,
  shrink ⇒ new ≤ old). Charge overflow is caught by `checked_add` on the
  self node (first `ancestors()` element), and `self_used ≤ subtree_used`
  makes the unchecked self_used add safe.
- **Accounting exactness invariants**: every byte flows through the stored
  allocator, so `used()` == sum of live capacities — asserted per-push in
  `accounting_tracks_capacity_exactly` and on shrink_to_fit/drop; residual
  bytes from `mem::forget` are returned to ancestors by `Drop` so the tree
  never holds phantom bytes (debug_assert flags it on `reset`).
- **McxOwned drop order**: explicit `Drop` drops state before context
  (both `ManuallyDrop`, so field order is not load-bearing — the impl is).
  State destructors deallocate into the still-live context; then the context's
  own drop fires reset callbacks. vs C `MemoryContextDeleteOnly`
  (callbacks **before** freeing): observationally equivalent because Rust
  callbacks are `'static` closures stored outside the context and cannot
  reference context allocations — verified the `for<'mcx>` builder/accessor
  bounds make any such borrow ill-typed. The lifetime-erasure soundness
  argument (universal quantification at build and `with_mut`, heap-pinned
  box for address stability) was re-derived and holds.
- **No-ambient-context rule**: no thread-local, no statics in the crate;
  grep-verified.

## Seam audit

`mcx` is an infrastructure crate: dependencies are `types-error` plus external
`allocator-api2`/`hashbrown`/`bumpalo` (exactly the design-doc approved set).
It owns no `-seams` crate, declares no seams, installs none, and `seams-init`
does not reference it — consumers use the direct cargo edge, per the "direct
dependency by default" rule. No findings.

## Findings and resolution

1. **MemoryContextSetIdentifier MISSING** — no ident support existed and the
   sanctioned table does not cover it. Fixed: `set_ident(Option<&str>)` /
   `ident()`, ident included in `ContextStats`/`TreeStats` (Copy dropped from
   `ContextStats`), cleared on drop as `MemoryContextDeleteOnly` does.
   Re-audited from scratch against mcxt.c:615 and the delete path; test
   `ident_set_forget_and_stats`.
2. **pchomp MISSING** — fixed: `PgString::chomp_in` strips all trailing `'\n'`
   then copies, matching mcxt.c:1753's strlen-decrement loop on every input
   (only-trailing, all-newlines, none). Test `chomp_strips_only_trailing_newlines`.

## Deferred-mechanics list (block-structured internals, per design)

Upgrading `Backend::Malloc` to real aset/generation/slab backends must restore:

- aset.c: AllocSetFreeIndex, AllocSetContextCreateInternal (block sizing,
  keeper block, context freelists), AllocSetReset (keeper retention),
  AllocSetDelete (freelist recycling), AllocSetAllocLarge,
  AllocSetAllocChunkFromBlock, AllocSetAllocFromNewBlock, AllocSetAlloc,
  AllocSetFree, AllocSetRealloc (in-place reuse), AllocSetStats (block
  counters), AllocSetCheck.
- generation.c: GenerationContextCreate, GenerationReset, GenerationDelete,
  GenerationAllocLarge, GenerationAllocChunkFromBlock,
  GenerationAllocFromNewBlock, GenerationAlloc, GenerationBlockInit,
  GenerationBlockMarkEmpty, GenerationBlockFreeBytes, GenerationBlockFree,
  GenerationFree (free-when-block-empties), GenerationRealloc,
  GenerationStats, GenerationCheck.
- slab.c: SlabBlocklistIndex, SlabFindNextBlockListIndex, SlabGetNextFreeChunk,
  SlabContextCreate (fixed chunkSize), SlabReset, SlabDelete,
  SlabAllocSetupNewChunk, SlabAllocFromNewBlock, **SlabAllocInvalidSize /
  SlabAlloc's runtime size==chunkSize check**, SlabFree, **SlabRealloc's
  reject-unless-same-size semantics**, SlabStats, SlabCheck.
- mcxt.c: MemoryContextCheck. bump.c: BumpCheck. memdebug.c: randomize_mem
  (debug-only, outside build config).

## Verdict

**PASS** (after fixes). 128 C functions inventoried:
**37 MATCH** (2 of them FIXED in this audit), **46 SANCTIONED** (each citing a
design-doc row/decision), **45 DEFERRED** (block mechanics above; semantics
verified), **0 MISSING / 0 DIVERGES** remaining. `cargo test --workspace`
green (120 suites). Spot-checked MATCH verdicts re-derived in detail:
reset-callback re-entrancy, OOM shape vs mcxt.c:1154, charge/uncharge on all
four Allocator methods including failure paths, McxOwned drop order.
