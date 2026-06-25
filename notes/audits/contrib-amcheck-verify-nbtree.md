# Audit: contrib-amcheck-verify-nbtree

C source: `contrib/amcheck/verify_nbtree.c` (PG 18.3)
Crate: `crates/contrib-amcheck-verify-nbtree` (+ owned seam crate
`crates/contrib-amcheck-verify-nbtree-seams`)
Assembly: keystone scaffold `decomp/contrib-amcheck-verify-nbtree` (7ccb0309) +
F0 (`-f0`, Cargo.lock only) + F2 (`-f2`, target_page engine) merged.

**Verdict: FAIL → NEEDS_DECOMP.** Residual families F1 (`entry.rs`) and F3
(`linkage.rs`) are unfilled panic stubs, and — the hard blocker — the crate's
index-tuple model (`IndexTuple<'mcx> = Option<PgBox<IndexTupleData>>`, a
header-only box: `t_tid` + `t_info` with no variable-length body) is
contract-divergent with the byte-slice (`PgVec<'mcx,u8>`) lane the keystone's
own seams and the real `backend-access-nbtree-nbtree` consumer use. F2 was
filled against this divergent model and therefore carries the divergence forward
at its tuple handoffs (see findings F-1/F-2 below). The crate compiles (panicking
stubs) and the gate is green, but it is not a faithful port.

## Function inventory & verdicts

| C function (line) | Family / port location | Verdict | Notes |
|---|---|---|---|
| `bt_index_check` (252) | F1 entry.rs:28 | MISSING (stub) | panic!("not yet filled"); SQL entry point |
| `bt_index_parent_check` (284) | F1 entry.rs:36 | MISSING (stub) | panic! |
| `bt_index_check_callback` (312) | F1 entry.rs:49 | MISSING (stub) | panic! |
| `bt_check_every_level` (378) | F1 entry.rs:63 | MISSING (stub) | panic! |
| `bt_check_level_from_leftmost` (624) | F1 entry.rs:87 | MISSING (stub) | panic!; also needs page_opaque btpo_prev/level + page-LSN reads not yet exposed |
| `heap_entry_is_visible` (852) | F1 entry.rs:97 | MISSING (stub) | panic! |
| `bt_report_duplicate` (871) | F1 entry.rs:107 | MISSING (stub) | panic! |
| `bt_entry_unique_check` (911) | F1 entry.rs:122 | MISSING (stub) | panic!; called by F2 |
| `bt_tuple_present_callback` (2782) | F1 entry.rs:137 | MISSING (stub) | panic!; fingerprints IndexTupleSize(norm) bytes — needs byte tuple model |
| `bt_normalize_tuple` (2850) | F1 entry.rs:152 | MISSING (stub) | panic!; returns IndexTuple; index_form_tuple over re-formed datums — header-only box cannot hold reformed body |
| `palloc_btree_page` (3292) | F1 entry.rs:163 | MISSING (stub) | panic!; needs ReadBufferExtended/LockBuffer/_bt_checkpage seams |
| `PageGetItemIdCareful` (3494) | F1 entry.rs:174 | MISSING (stub) | panic!; called heavily by F2 |
| `BTreeTupleGetHeapTIDCareful` (3534) | F1 entry.rs:187 | MISSING (stub) | panic!; takes &IndexTuple |
| `bt_mkscankey_pivotsearch` (3470) | F1 entry.rs:199 | MISSING (stub) | panic!; takes Option<&IndexTuple> -> BTScanInsert |
| `bt_target_page_check` (1239) | F2 target_page.rs:238 | PARTIAL/DIVERGES | item-by-item engine is structurally faithful, but built on `index_tuple_header()` (8-byte header only) and threads header-only `index_tuple_box` into the F1/F3 helpers; reaches divergent `bloom_add_index_tuple` (F-1) |
| `bt_right_page_check_scankey` (1867) | F2 target_page.rs:729 | MATCH* | right-link walk, p_ignore loop, first-data-item selection mirror C; *depends on header-only itup box handed to mkscankey |
| `offset_is_negative_infinity` (3075) | F2 target_page.rs:798 | MATCH | `!P_ISLEAF && offset == P_FIRSTDATAKEY`, faithful |
| `bt_posting_plain_tuple` (2978) | F2 target_page.rs:809 | MISSING (stub) | panic!; returns IndexTuple — blocked by byte model |
| `invariant_l_offset` (3110) | F2 target_page.rs:816 | MISSING (stub) | panic! (declared F2, body deferred) |
| `invariant_leq_offset` (3173) | F2 target_page.rs:827 | MISSING (stub) | panic! |
| `invariant_g_offset` (3196) | F2 target_page.rs:838 | MISSING (stub) | panic! |
| `invariant_l_nontarget_offset` (3232) | F2 target_page.rs:851 | MISSING (stub) | panic! |
| `bt_leftmost_ignoring_half_dead` (1010) | F3 linkage.rs:77 | MISSING (stub) | panic! |
| `bt_recheck_sibling_links` (1099) | F3 linkage.rs:89 | MISSING (stub) | panic!; needs Buffer/ReadBufferExtended/LockBuffer + _bt_checkpage; crate has no buffer dep |
| `bt_child_check` (2394) | F3 linkage.rs:26 | MISSING (stub) | panic!; called by F2 |
| `bt_child_highkey_check` (2147) | F3 linkage.rs:39 | MISSING (stub) | panic!; called by F2 |
| `bt_downlink_missing_check` (2559) | F3 linkage.rs:52 | MISSING (stub) | panic! |
| `bt_pivot_tuple_identical` (2074) | F3 linkage.rs:65 | MISSING (stub) | panic!; C memcmp over IndexTupleSize bytes — impossible with header-only box |
| `bt_rootdescend` (3011) | F3 linkage.rs:101 | MISSING (stub) | panic!; needs _bt_mkscankey over full tuple bytes |
| `BTreeTupleGetDownLink` (nbtree.h) | F3 linkage.rs:111 | MISSING (stub) | panic!; pure byte-math, fillable once model fixed |
| `BTreeTupleGetTopParent` (nbtree.h) | F3 linkage.rs:118 | MISSING (stub) | panic!; pure byte-math, fillable once model fixed |

(The static inline `offset_is_negative_infinity` / `BTreeTupleGetPointsToTID`
local helpers in F2 are MATCH; `item_pointer_compare`, `btree_tuple_get_*`,
`fmt_tid`, `fmt_lsn` local helpers mirror the C inline macros faithfully.)

## Findings

### F-1 (DIVERGES, merge-blocking, latent): `bloom_add_index_tuple` fingerprints 8 bytes, not the tuple

target_page.rs:928. C (`bt_target_page_check`, lines 1500/1511) calls
`bloom_add_element(state->filter, (unsigned char *) norm, IndexTupleSize(norm))`
— it hashes the *entire* normalized index tuple (header + variable-length body).
The port hashes only the serialized 8-byte `IndexTupleData` header. Different
input bytes → different Bloom membership set → the heapallindexed check would
accept/reject the wrong heap tuples. The author's own comment concedes the body
"is [not] addressable here" because `bt_normalize_tuple` returns a header-only
`IndexTuple`. This is the tuple-model divergence surfacing as own logic, not a
sanctioned seam-and-panic. (Currently unreachable because `bt_normalize_tuple`
panics first, but absent/wrong logic still fails the audit.)

### F-2 (root cause, multi-family): header-only IndexTuple model

`types_tuple::heaptuple::IndexTuple = Option<PgBox<IndexTupleData>>` carries only
`t_tid` + `t_info`. The keystone's own seams are byte-oriented
(`index_form_tuple -> PgVec<u8>`, `page_get_item -> PgVec<u8>`,
`bt_mkscankey(Option<&[u8]>)`, `tuple_heap_tid(&[u8])`, `bt_form_posting`), and
the proven consumer `backend-access-nbtree-nbtree` carries tuples as `PgVec<u8>`
end-to-end. Faithful F1/F3 fills (`bt_normalize_tuple`, `bt_pivot_tuple_identical`
memcmp over `IndexTupleSize` bytes, `bt_rootdescend` / `bt_mkscankey_pivotsearch`
over full tuple bytes, posting-list materialization) all require the body. F2
papers over this by parsing only the 8-byte header out of the byte slice
(`index_tuple_header` / `index_tuple_box`) and discarding the body. The remedy is
a keystone re-scaffold: retype `IndexTuple -> PgVec<'mcx,u8>` / `&[u8]` in
`BtreeCheckState.lowkey` (lib.rs) and all three family modules, matching the seam
contract — out of single-family scope, so F1/F3 correctly STOPped rather than
fill against the wrong model.

### F-3 (minor, error-detail only): BTreeTupleGetNAtts non-pivot path

target_page.rs:877 returns `rel.rd_att.natts` for the non-pivot case; C's
`BTreeTupleGetNAtts` macro uses `IndexRelationGetNumberOfAttributes(rel)` =
`rd_index->indnatts`. Equal for non-INCLUDE indexes; divergent value printed in
the corruption-report detail string for INCLUDE indexes. Subordinate to F-2.

### Secondary gap (not own-logic): missing page/buffer machinery

`bt_check_level_from_leftmost` needs `btpo_prev`/`btpo_level`/page-LSN reads; the
`page_opaque` seam exposes only `(btpo_flags, btpo_cycleid, btpo_next)`. F2 added
the `page_btpo_level(page)->u32` decl to `backend-access-nbtree-core-seams`
(owner `backend-access-nbtree-core`, still `todo`); `btpo_prev`/page-LSN remain
to be declared. `bt_recheck_sibling_links`/`palloc_btree_page` need
`ReadBufferExtended`/`LockBuffer`/`_bt_checkpage` + a `backend-storage-buffer`
dep. These are wireable (bufmgr-seams / snapmgr-seams / index-seams exist) and are
genuine cross-subsystem seam-and-panics, not own-logic absence.

## Seam audit

Owned seam crate: `contrib-amcheck-verify-nbtree-seams`, declaring the two fmgr
entry points `bt_index_check` / `bt_index_parent_check`. Both are installed by
`init_seams()` (lib.rs:146) and wired into `seams-init::init_all`. Both
`recurrence_guard` tests pass (`every_seam_installing_crate_is_wired_into_init_all`,
`every_declared_seam_is_installed_by_its_owner`).

Outward seams consumed (all into unported owners; correctly panic-until-owner,
not installed here): `backend-access-nbtree-core-seams` (page_get_item, page_opaque,
page_btpo_level [F2-added], page_get_max_offset_number, bt_check_natts, bt_compare,
bt_mkscankey, tuple_is_pivot/is_posting/heap_tid/n_posting/posting_tid),
`backend-lib-bloomfilter-seams::bloom_add_element`.

No `todo!`/`unimplemented!` anywhere. All deferrals are `panic!("decomp: …")`
mirror-and-panic stubs.

## Pre-sync reconciliation (assembler)

Merging current `refs/heads/main` surfaced a keystone/main contract collision:
the F0 keystone added `BTScanInsertData { scankeys: Vec<ScanKeyData> }` to
`types-nbtree` against the pre-lifetime `ScanKeyData`, while main concurrently
made `types_scan::scankey::ScanKeyData` lifetime-parameterized (`<'mcx>`).
Resolved additively per repo convention (consumer must compile): parameterized
`BTScanInsertData<'mcx>` / `BTScanInsert<'mcx>`, threaded `'mcx` through
`BTInsertStateData.itup_key`, the four `backend-access-nbtree-core-seams`
signatures (bt_mkscankey/bt_compare/bt_search/bt_moveright/bt_binsrch), and the
amcheck consumers (entry.rs, target_page.rs, linkage.rs). No logic change.

## Gate (post-sync, isolated target)

- `cargo check --workspace` — pass (warnings only)
- `cargo test -p contrib-amcheck-verify-nbtree` — pass (0 tests; compiles)
- `cargo test -p seams-init` — pass (2 recurrence guards)
