//! B-tree core cluster (PostgreSQL 18.3) — the mutually-recursive
//! `nbtsearch.c` / `nbtinsert.c` / `nbtpage.c` / `nbtutils.c` cluster plus the
//! dependent `nbtsplitloc.c` and `nbtpreprocesskeys.c`, ported as one crate.
//!
//! Module map (one Rust module per C file):
//! - [`search`]        — nbtsearch.c (descent / compare / scan-runtime)
//! - [`insert`]        — nbtinsert.c (insert / unique-check / split / parent)
//! - [`page`]          — nbtpage.c   (metapage / buffers / page-deletion / FSM)
//! - [`utils`]         — nbtutils.c  (scankeys / array keys / checkkeys / vacuum)
//! - [`splitloc`]      — nbtsplitloc.c (page-split-point choosing)
//! - [`preprocesskeys`]— nbtpreprocesskeys.c (scankey preprocessing)
//! - [`helpers`]       — nbtree.h / bufpage.h inline page/tuple-format reads
//!
//! [`init_seams`] installs the unit's owned seams declared in
//! `backend-access-nbtree-core-seams` (all 47 except `bt_form_posting`'s C body,
//! which lives in nbtdedup.c — installed here as a thin wrapper over nbtdedup's
//! pure builder, since nbtdedup itself owns no inward seam crate).

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(clippy::too_many_arguments)]

pub mod helpers;
pub mod insert;
pub mod page;
pub mod preprocesskeys;
pub mod search;
pub mod splitloc;
pub mod utils;

use backend_access_nbtree_core_seams as seams;

/// Install every seam this unit owns. Wired into `seams-init` by the parent.
///
/// Grouping mirrors the owning C file:
/// - page.rs (nbtpage.c): metapage / buffer / page-deletion / FSM / vacuum
/// - search.rs (nbtsearch.c): descent / compare / binary-search / scan-runtime
/// - utils.rs (nbtutils.c): scankeys / array keys / vacuum cycle-id / validation
/// - insert.rs (nbtinsert.c): the single `bt_doinsert` entry point
/// - helpers.rs (nbtree.h / bufpage.h inlines): page/tuple-format reads, and
///   the `bt_form_posting` wrapper over nbtdedup's builder.
pub fn init_seams() {
    // --- nbtpage.c (15) ---
    seams::build_empty_metapage::set(page::build_empty_metapage);
    seams::bt_getrootheight::set(page::bt_getrootheight);
    seams::bt_vacuum_needs_cleanup::set(page::bt_vacuum_needs_cleanup);
    seams::bt_set_cleanup_info::set(page::bt_set_cleanup_info);
    seams::bt_pendingfsm_init::set(page::bt_pendingfsm_init);
    seams::bt_pendingfsm_finalize::set(page::bt_pendingfsm_finalize);
    seams::bt_lockbuf::set(page::bt_lockbuf);
    seams::bt_relbuf::set(page::bt_relbuf);
    seams::bt_checkpage::set(page::bt_checkpage);
    seams::bt_upgradelockbufcleanup::set(page::bt_upgradelockbufcleanup);
    seams::bt_page_is_recyclable::set(page::bt_page_is_recyclable);
    seams::bt_delitems_vacuum::set(page::bt_delitems_vacuum);
    seams::bt_pagedel::set(page::bt_pagedel);
    seams::bt_delitems_delete_check::set(page::bt_delitems_delete_check);
    seams::bt_metaversion::set(page::bt_metaversion);

    // --- nbtsearch.c (8) ---
    seams::bt_search::set(search::bt_search);
    seams::bt_moveright::set(search::bt_moveright);
    seams::bt_binsrch::set(search::bt_binsrch);
    seams::bt_binsrch_insert::set(search::bt_binsrch_insert);
    seams::bt_compare::set(search::bt_compare);
    seams::bt_first::set(search::bt_first);
    seams::bt_next::set(search::bt_next);
    seams::current_heaptid::set(search::current_heaptid);
    seams::current_itup::set(search::current_itup);

    // --- nbtutils.c (11) ---
    seams::bt_mkscankey::set(utils::bt_mkscankey);
    seams::bt_freestack::set(utils::bt_freestack);
    seams::bt_start_array_keys::set(utils::bt_start_array_keys);
    seams::bt_start_prim_scan::set(utils::bt_start_prim_scan);
    seams::bt_killitems::set(utils::bt_killitems);
    seams::bt_keep_natts_fast::set(utils::bt_keep_natts_fast);
    seams::bt_check_natts::set(utils::bt_check_natts);
    seams::bt_allequalimage::set(utils::bt_allequalimage);
    seams::bt_allequalimage_dbg::set(utils::bt_allequalimage_dbg);
    seams::bt_start_vacuum::set(utils::bt_start_vacuum);
    seams::bt_end_vacuum::set(utils::bt_end_vacuum);

    // BTVacInfo shared state (nbtutils.c) — the ipci.c `CalculateShmemSize` /
    // `CreateOrAttachShmemStructs` entry points.
    backend_access_nbtree_seams::btree_shmem_size::set(utils::bt_shmem_size);
    backend_access_nbtree_seams::btree_shmem_init::set(utils::bt_shmem_init);

    // --- nbtinsert.c (1) ---
    seams::bt_doinsert::set(insert::bt_doinsert);

    // --- nbtree.h / bufpage.h inline page/tuple-format reads (11) ---
    seams::page_is_new::set(helpers::page_is_new);
    seams::page_opaque::set(helpers::page_opaque);
    seams::page_btpo_level::set(helpers::page_btpo_level);
    seams::page_clear_cycleid::set(helpers::page_clear_cycleid);
    seams::page_get_max_offset_number::set(helpers::page_get_max_offset_number);
    seams::page_get_item::set(helpers::page_get_item);
    seams::tuple_is_pivot::set(helpers::tuple_is_pivot);
    seams::tuple_is_posting::set(helpers::tuple_is_posting);
    seams::tuple_heap_tid::set(helpers::tuple_heap_tid);
    seams::tuple_n_posting::set(helpers::tuple_n_posting);
    seams::tuple_posting_tid::set(helpers::tuple_posting_tid);

    // --- nbtsort.c build helpers (nbtutils.c bodies, declared in the
    //     backend-access-nbtree-build-seams crate) (3) ---
    // C's `BTScanInsert itup_key` is a non-null pointer; the seam carries it
    // Option-wrapped (nbtsort stores it as such), so the install unwraps it to
    // the `&BTScanInsertData` the in-crate `_bt_truncate` body takes — a thin
    // marshal, mirroring insert.rs's `itup_key.as_ref().unwrap()`.
    backend_access_nbtree_build_seams::bt_truncate::set(|mcx, rel, lastleft, firstright, itup_key| {
        utils::bt_truncate(
            mcx,
            rel,
            lastleft,
            firstright,
            itup_key.as_ref().expect("_bt_truncate: itup_key is NULL"),
        )
    });
    backend_access_nbtree_build_seams::bt_check_third_page::set(utils::bt_check_third_page);
    backend_access_nbtree_build_seams::bt_load_compare_index_tuples::set(
        utils::bt_load_compare_index_tuples,
    );

    // --- nbtdedup.c body, owned here as a wrapper (1) ---
    // nbtdedup owns no inward seam crate, so its `_bt_form_posting` C body is
    // installed from this unit (the cluster that includes nbtdedup's family).
    seams::bt_form_posting::set(helpers::bt_form_posting);
}
