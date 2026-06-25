# Audit: backend-utils-mmgr-dsa (`src/backend/utils/mmgr/dsa.c`)

Audited independently against the C (`postgres-18.3/src/backend/utils/mmgr/dsa.c`,
2352 lines), the c2rust rendering (`c2rust-runs/backend-utils-mmgr-dsa/src/dsa.rs`),
and the port (`crates/backend-utils-mmgr-dsa/src/{lib,runtime,wire}.rs`,
`crates/types-dsa/src/lib.rs`).

## Function inventory and verdicts

The C file defines 38 functions plus `contiguous_pages_to_segment_bin`
(static inline). `c2rust_pg_read_barrier` (dsa.c:63) is a `C2RUST_TRANSPILE`-only
no-op shim, not a real function — excluded.

| # | C function | C loc | Port loc | Verdict | Notes |
|---|---|---|---|---|---|
| 1 | `contiguous_pages_to_segment_bin` | 129 | lib.rs:123 | MATCH | `n==0 → 0`, else `leftmost_one_pos(n)+1` via `Size::BITS - leading_zeros`, capped at `DSA_NUM_SEGMENT_BINS-1`. Test covers 0/1/2/3/4/2^20/MAX. |
| 2 | `dsa_create_ext` | 431 | runtime.rs `dsa_create_ext` | MATCH | dsm_create→pin→create_internal→on_dsm_detach. dsm_create's NULL (non-NO_OOM) → Err. Takes Mcx (dsm allocates). |
| 3 | `dsa_create_in_place_ext` | 481 | runtime.rs | MATCH | create_internal(DSM_HANDLE_INVALID,None); on_dsm_detach only if segment provided. |
| 4 | `dsa_get_handle` | 508 | runtime.rs | MATCH | returns control->handle; Assert→debug_assert_ne. |
| 5 | `dsa_attach` | 520 | runtime.rs | MATCH | dsm_attach; NULL→`ereport(ERROR, ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)` → Err "could not attach to dynamic shared area"; attach_internal; on_dsm_detach. |
| 6 | `dsa_attach_in_place` | 555 | runtime.rs | MATCH | attach_internal(NULL, DSA_HANDLE_INVALID); on_dsm_detach iff segment. |
| 7 | `dsa_on_dsm_detach_release_in_place` | 586 | runtime.rs | MATCH | signature mirrors `OnDsmDetachCallback = fn(DsmSegmentId, Datum) -> PgResult<()>`; delegates to dsa_release_in_place. |
| 8 | `dsa_on_shmem_exit_release_in_place` | 600 | runtime.rs | MATCH | `(code, place)`; code ignored; delegates. (src-idiomatic had dropped this.) |
| 9 | `dsa_release_in_place` | 615 | runtime.rs | MATCH | area lock; magic+refcnt asserts; `--refcnt==0` → unpin every live segment_handle. lock released via guard.release(). |
| 10 | `dsa_pin_mapping` | 645 | runtime.rs | MATCH (idiomatic) | C clears `resowner` then `dsm_pin_mapping` each segment. The registry holds segment *ids* (`into_id`, already detached from any resowner guard → session-lifetime), so clearing `resowner_set` is the whole effect; per-segment pin is a no-op here. Infallible (C void). Documented in the fn doc. |
| 11 | `dsa_allocate_extended` | 681 | runtime.rs | MATCH | size>0 debug_assert; validate_alloc_request (HUGE/normal); large path (alloc_object span, get_best/make_new segment, FreePageManagerGet FATAL, init_span large, pagemap_set, ZERO memset); small path (size_class, alloc_object, ZERO). OOM via `oom()`. |
| 12 | `dsa_free` | 836 | runtime.rs | MATCH | check_for_freed_segments; locate span/superblock/object; SPAN_LARGE special case (CLOBBER_FREED_MEMORY `#ifdef` not built — excluded; fpm_put+rebin under area lock, unlink_span under span lock, recursive dsa_free); normal: freelist push, nallocatable++, two fullness-class transitions. |
| 13 | `dsa_get_address` | 952 | runtime.rs | MATCH | InvalidDsaPointer→0(NULL); check_for_freed_segments; split; map-in via get_segment_by_index; return base+offset. |
| 14 | `dsa_pin` | 985 | runtime.rs | MATCH | area lock; already-pinned → `elog(ERROR,"dsa_area already pinned")`→Err; set pinned, ++refcnt. |
| 15 | `dsa_unpin` | 1004 | runtime.rs | MATCH | area lock; refcnt>1 assert; not-pinned → Err "dsa_area not pinned"; clear pinned, --refcnt. |
| 16 | `dsa_set_size_limit` | 1028 | runtime.rs | MATCH | area lock; set max_total_segment_size. |
| 17 | `dsa_get_total_size` | 1037 | runtime.rs | MATCH | area lock; read total_segment_size. |
| 18 | `dsa_trim` | 1053 | runtime.rs | MATCH | reverse pool order; skip SPAN_LARGE; per class fullness-1 scan; destroy_superblock when nallocatable==nmax. (src-idiomatic had dropped this.) |
| 19 | `dsa_dump` | 1098 | runtime.rs | MATCH | stderr→eprintln!; area-lock segment-bins walk + per-pool fullness walk, same fields/format intent. (src-idiomatic had dropped this.) |
| 20 | `dsa_minimum_size` | 1206 | runtime.rs | MATCH | `MAXALIGN(sizeof control)+MAXALIGN(sizeof FPM)` then grow by `sizeof(dsa_pointer)` per page until pages cover it. |
| 21 | `create_internal` | 1228 | runtime.rs | MATCH | size<minimum → `elog(ERROR,"dsa_area space must be at least %zu...")`; metadata_bytes calc + page-pad; memset control; all control fields incl magic=`MAGIC^handle^0`, max_total=`(size_t)-1`=Size::MAX, bins=NONE, refcnt=1; LWLockInitialize lock + every pool lock; segment_maps[0]; fpm init + put(usable>0); bin. |
| 22 | `attach_internal` | 1336 | runtime.rs | MATCH | magic/handle asserts; segment_maps[0]; area lock; refcnt==0 → Err "could not attach..."; ++refcnt; copy freed_segment_counter. |
| 23 | `init_span` | 1387 | runtime.rs | MATCH | head->prevspan link; push to fullness-1; fields; BLOCK_OF_SPANS (ninitialized=1, nallocatable=PAGE/obsize-1) vs non-SPAN_LARGE (SUPERBLOCK/obsize); firstfree=NOTHING_FREE; nmax; fclass=1. C `Assert(LWLockHeldByMe)` is debug-only and has no main seam — dropped (see Design conformance). |
| 24 | `transfer_first_span` | 1442 | runtime.rs | MATCH | empty source→false; unlink head from source, relink head of target, fix prevspans, set fclass. |
| 25 | `alloc_object` | 1482 | runtime.rs | MATCH | sclass lock; no active span & !ensure_active_superblock → Invalid; else pop firstfree (read NextFreeObjectIndex via resolved addr) or initialize new; --nallocatable; full→transfer to class N-1. |
| 26 | `ensure_active_superblock` | 1570 | runtime.rs | MATCH | nmax calc; fclass 2..N-1 rebalance (tfclass, list surgery); transfer fallbacks (2..N-1 then 0); alloc span for non-BLOCK_OF_SPANS; area lock get_best/make_new; FreePageManagerGet FATAL; BLOCK_OF_SPANS carves span inline; init_span + pagemap fill. |
| 27 | `get_segment_by_index` | 1767 | runtime.rs | MATCH | unmapped→attach; DSM_HANDLE_INVALID→`elog(ERROR,"...segment that has been freed")`→Err; NULL→Err "could not attach to segment"; set map; high_segment_index; magic assert. C resowner swap noted as comment (registry owns mapping). |
| 28 | `destroy_superblock` | 1847 | runtime.rs | MATCH | unlink_span; area lock + check_for_freed_segments_locked; fpm_put; if fully free & index!=0 → unlink_segment, mark freed, total-=size, unpin+detach, slot=INVALID, ++freed_counter, zero map; else rebin; recursive dsa_free for non-BLOCK_OF_SPANS. |
| 29 | `unlink_span` | 1916 | runtime.rs | MATCH | next/prev relink; no-prev → pool->spans[fclass]=next via pool dsa_pointer. |
| 30 | `add_span_to_fullness_class` | 1939 | runtime.rs | MATCH | head->prevspan; push front; set fclass. |
| 31 | `dsa_detach` | 1962 | runtime.rs | MATCH | detach each mapped segment; free backend-local area (registry slot → None). |
| 32 | `unlink_segment` | 1988 | runtime.rs | MATCH | prev relink or segment_bins[bin]=next (with assert); next relink. |
| 33 | `get_best_segment` | 2020 | runtime.rs | MATCH | check_for_freed_segments_locked; bin scan from `contiguous_pages_to_segment_bin(npages)`; threshold `1<<(bin-1)`; skip-in-bin / rebin / done logic. |
| 34 | `make_new_segment` | 2091 | runtime.rs | MATCH | free-slot scan; total>=max_total→None; geometric total_size (min max_segment_size, min remaining); metadata+pad; too-small→None; usable_pages; odd-sized retry when requested>usable (DSA_MAX_SEGMENT_SIZE / remaining checks→None); dsm_create→None on NULL; pin; record handle/high/total; map; fpm init+put; header magic=`MAGIC^handle^index`; bin link. |
| 35 | `check_for_freed_segments` | 2262 | runtime.rs | MATCH | `pg_read_barrier` (the c2rust shim is a no-op; on this single-process target the load ordering the barrier guarantees is implied) then counter compare; lock + locked variant. |
| 36 | `check_for_freed_segments_locked` | 2298 | runtime.rs | MATCH | counter compare; detach every mapped+freed segment, zero map; store counter. |
| 37 | `rebin_segment` | 2326 | runtime.rs | MATCH | new_bin from fpm_largest; same-bin→return; unlink; push front of new bin; next->prev fix. |
| 38 | (macro) `DsaAreaPoolToDsaPointer` | 332 | runtime.rs `dsa_area_pool_to_dsa_pointer` | MATCH | `DSA_MAKE_POINTER(0, offsetof(control,pools)+class*sizeof(pool))`. |

Helper inlines `DSA_MAKE_POINTER`/`DSA_EXTRACT_*`/`fpm_size_to_pages`/
`NextFreeObjectIndex`/`get_segment_index`/`DsaPointerIsValid`/`fpm_largest` are
all reproduced (lib.rs `make_pointer`/`fpm_size_to_pages`/`dsa_pointer_is_valid`,
runtime.rs `next_free_object_index_*`, `fpm_largest` reads `contiguous_pages`).

## Constants (verified against headers, not memory)

- `DSA_OFFSET_WIDTH=40`, `DSA_MAX_SEGMENT_SIZE=1<<40`, `DSA_MAX_SEGMENTS=1024`,
  `DSA_NUM_SEGMENT_BINS=16`, `DSA_NUM_SEGMENTS_AT_EACH_SIZE=2`,
  `DSA_PAGES_PER_SUPERBLOCK=16`, `DSA_FULLNESS_CLASSES=4`,
  `DSA_SEGMENT_HEADER_MAGIC=0x0ce26608`, `DSA_SPAN_NOTHING_FREE=u16::MAX`,
  `DSA_SCLASS_BLOCK_OF_SPANS=0`, `DSA_SCLASS_SPAN_LARGE=1`,
  `DSA_SIZE_CLASS_MAP_QUANTUM=8` — all MATCH (`utils/dsa.h` / dsa.c top).
- `dsa_size_classes[]` (40 entries) and `dsa_size_class_map[]` (128 entries) —
  transcribed and verified element-by-element against dsa.c:235-267.
  `dsa_size_classes[0]=56=sizeof(dsa_area_span)`, guarded by a const assertion
  `size_of::<DsaAreaSpan>() <= 56`.
- `MaxAllocSize=0x3fffffff`, `MaxAllocHugeSize=SIZE_MAX/2` (memutils.h) — MATCH.
- `FPM_PAGE_SIZE=4096` (re-exported from types-freepage) — MATCH.
- SQLSTATE: attach failures → `ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE`
  (`55000`); OOM → `ERRCODE_OUT_OF_MEMORY` (`53200`) with
  errdetail "Failed on DSA request of size N." — MATCH (errcodes.h).

## Seam audit

Owned seam crate (by C-source coverage): `crates/backend-utils-mmgr-dsa-seams`
(declared earlier by `backend-executor-execParallel`; its `c_sources` is
`dsa.c`, so this unit owns it). Its 7 declarations
(`dsa_minimum_size`, `dsa_create_in_place`, `dsa_attach_in_place`, `dsa_detach`,
`dsa_allocate`, `dsa_free`, `dsa_get_address`) are **all** installed by
`crate::init_seams() → wire::install_dsa_seams()`. `seams-init::init_all()`
calls `backend_utils_mmgr_dsa::init_seams()`. No uninstalled seam; no `set()`
outside the owner.

The seam vocabulary (`types_execparallel`: `DsaAreaHandle`/`DsmSegmentHandle`/
`SerializeCursor`/`DsaPointer`/`Size`) is the frozen boundary the already-merged
execParallel consumes; consumed as-is per the freeze-vocabulary rule. The
adapters are thin: convert the execParallel token, source the per-backend DSA
top `Mcx`, call one runtime function, convert the result. `dsa_create_in_place`
expands the C macro's default init/max segment sizes (a constant pair, not
logic). The error→panic at the boundary is the C `ereport(ERROR)` the infallible
seam cannot surface.

Outward calls — all justified direct deps (no cycle), thin:
- DSM lifecycle → `backend-storage-ipc-dsm-core` (direct dep; acyclic).
- FreePageManager ops → `backend-utils-mmgr-freepage-seams` (the real
  `*mut FreePageManager` seam; the owner installs it). `fpm_largest` is a direct
  `contiguous_pages` field read (the C macro), no seam.
- LWLock ops → `backend-storage-lmgr-lwlock-seams` (`&LWLock` guard, owner
  installed). `my_proc_number` → `backend-utils-init-small-seams`.

No function body was replaced by a "somewhere else" seam call; all dsa.c logic
lives in this crate.

## Design conformance

- **Allocating fns take `Mcx<'static>` + return `PgResult`**: `dsa_create_ext`,
  `dsa_attach`, `dsa_allocate_extended`, `dsa_free`, `dsa_get_address`,
  `dsa_trim`, `dsa_dump`, and the internal allocator helpers thread `Mcx` and
  return `PgResult` (OOM = `mcx`-style ERRCODE_OUT_OF_MEMORY). The non-allocating
  pin/unpin/size queries are `PgResult` only because they take the in-segment
  LWLock (whose acquire is fallible — "too many LWLocks taken"). `dsa_pin_mapping`
  and `dsa_get_handle` are infallible (C void / no lock).
- **Per-backend globals → thread_local**: `DSA_STATE` (the `dsa_area` registry,
  the C palloc'd per-backend object) and the wire `TOP` (DSA top context) are
  `thread_local`, never shared statics.
- **Opacity inherited, not invented**: `DsaPointer=u64`/`DsaHandle=u32` are C's
  own typedefs (`uint64`/`dsm_handle`), not stand-ins. The in-segment
  aggregates are real `repr(C)` structs embedding the real
  `types_freepage::FreePageManager` and `types_storage::LWLock` (the
  shared-memory substrate exception), not byte blobs. dsm-core gained
  `DsmSegmentId::{as_u64,from_u64}` (accessor, non-restructuring).
- **Locks via guards, never held across `?` bare**: every `LWLockAcquire`
  returns an `LWLockGuard`; the C `LWLockRelease` sites call `.release()?`; an
  early `?` between acquire and release drops the guard (the C
  `LWLockReleaseAll` abort backstop).
- **Panics**: `dsa_fatal` = the C `elog(FATAL)` ("dsa_allocate could not find N
  free pages[ for superblock]") — process death modeled as panic. The wire
  boundary panic is the documented ereport-at-infallible-seam case.
- No ambient-global seams, no registry-with-release-authority, no unledgered
  divergence markers.

## Excluded by build config (verified, not skipped)

- `c2rust_pg_read_barrier` (dsa.c:63): `C2RUST_TRANSPILE`-only no-op shim.
- `CLOBBER_FREED_MEMORY` memsets in `dsa_free` (dsa.c:868, 892): `#ifdef`
  not in the default build.
- `pg_read_barrier()` in `check_for_freed_segments`: the c2rust run renders it
  as a no-op; the load-ordering it guards is implied on the single-process
  target. Noted for the eventual threaded-server revisit.

## Out-of-scope substrate left to its owner

src-idiomatic folded a parallel-hash DSA substrate (the
`dsa_pointer_atomic_*` ops, `ExecParallelHashPushTuple`, and the
`HashMemoryChunkData`/`HashJoinTupleData` carve codec) into its dsa crate. None
of that is in `dsa.c` (it is nodeHash.c / hashjoin.h); it is correctly **not**
ported here and belongs with `backend-executor-nodeHash`.

## Verdict: PASS

All 38 dsa.c functions plus `contiguous_pages_to_segment_bin` are present and
MATCH; all seams (7) are installed by the owner's `init_seams()` and wired into
`seams-init`; design-conformance checks pass. Panics stand only for C
`elog(FATAL)` / the infallible-seam `ereport(ERROR)` boundary, never for absent
logic.
