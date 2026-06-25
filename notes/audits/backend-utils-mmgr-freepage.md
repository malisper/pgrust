# Audit: backend-utils-mmgr-freepage

- **Unit:** `backend-utils-mmgr-freepage` (`src/backend/utils/mmgr/freepage.c`, 18.3)
- **Crates:** `crates/backend-utils-mmgr-freepage`, `crates/backend-utils-mmgr-freepage-seams`,
  `crates/backend-utils-misc-stack-depth-seams` (new pending-seam crate),
  `crates/types-freepage` (layout vocabulary)
- **C sources:** `postgres-18.3/src/backend/utils/mmgr/freepage.c` plus headers
  `src/include/utils/freepage.h`, `src/include/utils/relptr.h`
- **c2rust:** `c2rust-runs/backend-utils-mmgr-freepage/src/freepage.rs`
- **Audit date:** 2026-06-12

## Inventory

freepage.c defines 31 functions; 29 survive the default build (cross-checked
against the c2rust rendering, which contains exactly those 29 plus the
`relptr_store_eval` inline from relptr.h). `sum_free_pages` /
`sum_free_pages_recurse` are inside `#ifdef FPM_EXTRA_ASSERTS` and are absent
from the build configuration (confirmed: not present in the c2rust output),
so they are correctly not ported; likewise the `FPM_EXTRA_ASSERTS` blocks in
`FreePageManagerInitialize` / `Get` / `Put` and the `free_pages` struct field.

Header macros ported as functions: `relptr_access`, `relptr_store`,
`relptr_is_null`, `relptr_offset`, `relptr_copy` (rendered as direct `RelPtr`
struct assignment — `RelPtr` is a one-field `Copy` struct, identical
semantics), `fpm_page_to_pointer`, `fpm_pointer_to_page`,
`fpm_pointer_is_page_aligned`, `fpm_segment_base`, `fpm_size_to_pages`,
`fpm_largest`.

## Constants (verified against headers, not memory)

| Constant | C value | Port | Verdict |
|---|---|---|---|
| `FPM_PAGE_SIZE` | 4096 (freepage.h) | `types_freepage::FPM_PAGE_SIZE = 4096` | MATCH |
| `FPM_NUM_FREELISTS` | 129 (freepage.h) | 129 | MATCH |
| `FREE_PAGE_SPAN_LEADER_MAGIC` | 0xea4020f0 | 0xea40_20f0 | MATCH |
| `FREE_PAGE_LEAF_MAGIC` | 0x98eae728 | 0x98ea_e728 | MATCH |
| `FREE_PAGE_INTERNAL_MAGIC` | 0x19aa32c9 | 0x19aa_32c9 | MATCH |
| `FPM_ITEMS_PER_INTERNAL/LEAF_PAGE` | `(4096 - sizeof(hdr)) / sizeof(key)` = 254 on LP64 | same formula over `repr(C)` structs; test pins 254 | MATCH |
| relptr encoding | `off = val - base + 1`, 0 = NULL (relptr.h) | identical | MATCH |
| `FATAL` | 22 (elog.h; c2rust shows literal 22) | `types_error::FATAL = ErrorLevel(22)` | MATCH |
| `FreePageManager` layout | self, btree_root, btree_recycle, `unsigned` ×2, `Size` ×3, `bool`, freelist[129] | `repr(C)` with `u32` ×2 — field-for-field | MATCH |

## Per-function table

| C function (freepage.c) | Port (`crates/backend-utils-mmgr-freepage/src/lib.rs`) | Verdict | Notes |
|---|---|---|---|
| `FreePageManagerInitialize` (182) | `free_page_manager_initialize` (186) | MATCH | All fields zeroed/nulled identically; 129 freelists nulled. |
| `FreePageManagerGet` (209) | `free_page_manager_get` (211) | MATCH | bool + out-param → `Option<Size>`; GetInternal → Cleanup → max(contiguous) → UpdateLargest, same order; returns the pre-cleanup result. |
| `sum_free_pages_recurse` (251) | — | MATCH (not ported) | `FPM_EXTRA_ASSERTS` only; absent from build (c2rust confirms). |
| `sum_free_pages` (273) | — | MATCH (not ported) | Same. |
| `FreePageManagerLargestContiguous` (323) | `free_page_manager_largest_contiguous` (344) | MATCH | Oversized-list scan, else `do { --f; ... } while (f > 0)` descent reproduced exactly (checks 127..0, breaks on first non-empty). |
| `FreePageManagerUpdateLargest` (365) | `free_page_manager_update_largest` (378) | MATCH | |
| `FreePageManagerPut` (378) | `free_page_manager_put` (235) | MATCH | `contiguous_pages > npages` → cleanup; max; UpdateLargest. PutInternal's `elog(FATAL)` surfaces as `Err` (see seam notes). |
| `FreePageManagerDump` (423) | `free_page_manager_dump` (271) | MATCH | Byte-identical output: "metadata: self %zu max contiguous pages = %zu\n", "btree depth %u:\n", "singleton: %zu(%zu)\n", "btree recycle:", "freelists:\n", "  %zu:". StringInfo-in-CurrentMemoryContext → explicit `Mcx` + `PgResult<PgString>` per repo convention; `push_size` reproduces `%zu`. |
| `FreePageBtreeAdjustAncestorKeys` (500) | `free_page_btree_adjust_ancestor_keys` (388) | MATCH | s/s-1 disambiguation, `USE_ASSERT_CHECKING` double-check as debug_asserts, `if (s > 0) break; child = parent;` loop identical. |
| `FreePageBtreeCleanup` (579) | `free_page_btree_cleanup` (448) | MATCH | Depth-shrink loop (nused==1 leaf→singleton / internal→child-root; nused==2 leaf bridge-across-root case incl. `end_of_first == root_page` test) and recycle-drain loop with soft put. Soft put `Err` is provably unreachable (both FATAL sites are behind `if (soft) return 0` / recycle-nonempty), handled as `unreachable!` with citation. |
| `FreePageBtreeConsolidate` (694) | `free_page_btree_consolidate` (547) | MATCH | `nused >= max/3` early-out; right-sibling then left-sibling merge; memcpy → `copy_nonoverlapping` (distinct pages, valid); parent-pointer update for internal merges; RemovePage of the emptied page. |
| `FreePageBtreeFindLeftSibling` (773) | `free_page_btree_find_left_sibling` (613) | MATCH | Ascend while index==0, step to `index-1`, descend via `nused-1` children. |
| `FreePageBtreeFindRightSibling` (818) | `free_page_btree_find_right_sibling` (652) | MATCH | Mirror image, descend via child 0. |
| `FreePageBtreeFirstKey` (862) | `free_page_btree_first_key` (690) | MATCH | |
| `FreePageBtreeGetRecycled` (879) | `free_page_btree_get_recycled` (703) | MATCH | `relptr_copy(newhead->prev, victim->prev)` = struct assign; count decrement. |
| `FreePageBtreeInsertInternal` (899) | `free_page_btree_insert_internal` (720) | MATCH | memmove → `ptr::copy` (overlapping-safe), `nused - index` elements. |
| `FreePageBtreeInsertLeaf` (916) | `free_page_btree_insert_leaf` (739) | MATCH | Same. |
| `FreePageBtreeRecycle` (933) | `free_page_btree_recycle` (756) | MATCH | magic/npages=1/list push/count increment. |
| `FreePageBtreeRemove` (954) | `free_page_btree_remove` (778) | MATCH | nused==1 → RemovePage; shift; index==0 → AdjustAncestorKeys; Consolidate. |
| `FreePageBtreeRemovePage` (986) | `free_page_btree_remove_page` (810) | MATCH | Root-removal early return (depth=0, root nulled); ascend while parent nused==1 recycling each level; leaf/internal downlink removal; recycle; index==0 adjust; consolidate parent. `for(;;)`+break → `loop { break parent }`. |
| `FreePageBtreeSearch` (1063) | `free_page_btree_search` (883) | MATCH | split_pages starts 1; empty-root early return; internal descent with exact-match/left-bias `--index`; split_pages++ on full page else reset to 0 (both internal and leaf phases); found predicate identical. C's leaf-phase assert against `FPM_ITEMS_PER_INTERNAL_PAGE` (line 1118) reproduced as-is with a `sic` note (constants equal). |
| `FreePageBtreeSearchInternal` (1139) | `free_page_btree_search_internal` (952) | MATCH | Same binary search incl. equal-return-mid. |
| `FreePageBtreeSearchLeaf` (1169) | `free_page_btree_search_leaf` (977) | MATCH | Same. |
| `FreePageBtreeSplitPage` (1200) | `free_page_btree_split_page` (1003) | MATCH | nused/2 to new sibling; parent relptr copied; parent-pointer fixup for internal pages. |
| `FreePageBtreeUpdateParentPointers` (1231) | `free_page_btree_update_parent_pointers` (1035) | MATCH | |
| `FreePageManagerDumpBtree` (1249) | `free_page_manager_dump_btree` (1044) | MATCH | `check_stack_depth()` via stack-depth seam (`PgResult` propagated — the C ereport(ERROR) longjmps; same surface). Format "  %zu@%d %c", "[actual parent %zu, expected %zu]", " %zu->%zu" / " %zu(%zu)" byte-identical; recursion over children identical. NULL `parent`/`check_parent` page-number arithmetic wraps the same way as the C pointer subtraction. |
| `FreePageManagerDumpSpans` (1295) | `free_page_manager_dump_spans` (1103) | MATCH | " %zu(%zu)" vs " %zu" branch restructured to common prefix + conditional suffix — identical bytes. |
| `FreePageManagerGetInternal` (1318) | `free_page_manager_get_internal` (1128) | MATCH | `f = Min(npages, FPM_NUM_FREELISTS) - 1` with npages=0 → `wrapping_sub` → loop skipped → None, same as C unsigned wrap. Best-fit scan of last list with early break on exact size; unlink from freelist `f` (saved as `chosen_f`, used only when victim found, as in C); both contiguous_pages_dirty predicates; singleton path vs btree path (exact-size remove / in-place shrink + AdjustAncestorKeys at index 0 + push remainder); returns victim's page. |
| `FreePageManagerPutInternal` (1475) | `free_page_manager_put_internal` (1251) | MATCH | All four singleton cases incl. btree bootstrap (recycled / soft-bail `Ok(0)` / GetInternal / FATAL→`Err`), zero-page-root corner case; search + prev/next key location (right-sibling fallback with nindex=0); consolidate-with-prev (incl. also-next, pop/push, deferred `FreePageBtreeRemove` last); consolidate-with-next (pop/push, in-place key update, nindex==0 adjust); split path: soft bail `Ok(0)`, recycle-stock loop with FATAL→`Err`, re-search, split loop (leaf vs downlink insert, `insert_into == split_target` ancestor adjust, root split with new internal root nused=2 and depth++, parent insert with index==0 adjust), final push; plain leaf insert + index==0 adjust + push. The C's union read `newsibling->u.internal_key[0].first_page` for a possibly-leaf sibling reproduced with a layout note (first_page at offset 0 in both arms). |
| `FreePagePopSpanLeader` (1842) | `free_page_pop_span_leader` (1556) | MATCH | prev==NULL → unlink from freelist `Min(npages, FPM_NUM_FREELISTS) - 1`, with the C Assert as debug_assert. |
| `FreePagePushSpanLeader` (1870) | `free_page_push_span_leader` (1577) | MATCH | |
| `fpm_size_to_pages` (macro) | `fpm_size_to_pages` (162) | MATCH | |
| `fpm_largest` (macro) | `fpm_largest` (167) | MATCH | |

Spot-checks re-derived in full detail: `FreePageManagerGetInternal` (freelist
index bookkeeping, npages=0 wrap, dirty predicates),
`FreePageManagerPutInternal` (all consolidation/split/bootstrap paths against
both the C and the c2rust rendering, including the FATAL sites at C lines
1534 and 1689), `FreePageBtreeCleanup` (soft-put unreachability of `Err`),
`FreePageManagerDump*` (byte-level format comparison), and
`FreePageBtreeAdjustAncestorKeys`.

## Error paths

- `elog(FATAL, "free page manager btree is corrupt")` (freepage.c:1534,
  1689) → `PgError::new(FATAL, "free page manager btree is corrupt")`;
  `FATAL` = 22 matches elog.h (c2rust emits literal 22); default
  internal-error sqlstate matches `elog`. Reachable exactly when `soft ==
  false`, i.e. via `FreePageManagerPut` — hence the `PgResult<()>` on the
  Put seam, with all three dsm.c call sites in
  `backend-storage-ipc-dsm-core/src/dsm.rs` now propagating with `?` (the C
  FATAL longjmps through them identically).
- `free_page_manager_get` stays infallible like the C `bool` API: the only
  PutInternal invocation under Get is the soft one in Cleanup, which returns
  before either FATAL site; the port documents and `unreachable!`s that arm.
- `check_stack_depth()` in `FreePageManagerDumpBtree` → seam returning
  `PgResult<()>`, propagated; matches the C `ereport(ERROR)` unwind.

## Seam audit

- `crates/backend-utils-mmgr-freepage-seams` declares 3 seams
  (`free_page_manager_initialize` / `_get` / `_put`); all 3 installed by this
  crate's `init_seams()`, which contains nothing but `set()` calls;
  `seams-init::init_all()` calls it (`crates/seams-init/src/lib.rs:48`). The
  pre-existing consumer (dsm-core) uses them as thin `::call` delegates with
  no logic in the seam path. The `_put` signature change (`()` →
  `PgResult<()>`) is a parity fix, not new seam logic.
- `crates/backend-utils-misc-stack-depth-seams` is a new pending-seam crate
  for the unported `backend-utils-misc-stack-depth` unit (CATALOG row 562,
  status `todo`); it declares only `check_stack_depth() -> PgResult<()>`
  (matches the C `void` + ereport(ERROR) contract). No `set()` in production
  code outside the (future) owner; the freepage test module stubs it under
  `#[cfg(test)]`, which is the established convention (e.g.
  `backend-access-hashvalidate/src/tests.rs`).
- Outward dependency justified: `check_stack_depth` lives in an unported
  neighbor unit; seam-crate-per-owner is the prescribed AGENTS.md mechanism.
  The call is marshal-free (zero args, one call, `?`).
- No body-replaced-by-seam functions: every freepage.c function's logic lives
  in this crate.

## Design conformance

- No invented opacity: `FreePageManager` is the full `repr(C)` C layout in
  `types-freepage` (consumers size reservations with `size_of`); page-resident
  structs (`FreePageSpanLeader`, `FreePageBtree*`) stay private to the crate,
  exactly as they are file-private in freepage.c.
- The only allocating function (`free_page_manager_dump`) takes `Mcx<'mcx>`
  and returns `PgResult<PgString<'mcx>>`.
- No per-backend globals, shared statics, registries, or locks in this unit
  (state lives in caller-provided shared memory, as in C).
- Build: workspace `cargo build` clean; `cargo test -p
  backend-utils-mmgr-freepage` 8/8 pass (incl. a 2048-page scattered-free
  stress driving btree growth, splits, recycling, and full re-merge).

## Verdict

**PASS.** All 29 built functions (plus the two header macros exposed as API)
verdict MATCH; the two `FPM_EXTRA_ASSERTS`-only functions are correctly
outside the build; zero seam findings; zero design findings.
