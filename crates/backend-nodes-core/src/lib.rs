//! `backend-nodes-core` — the node-support core (`src/backend/nodes/*.c`).
//!
//! This unit came back NEEDS_DECOMP from the frontier wave and is decomposed
//! into a KEYSTONE family plus ten coherent function-cluster families, each a
//! module here:
//!
//! | family            | module                | C source              |
//! |-------------------|-----------------------|-----------------------|
//! | **bitmapset** (KEYSTONE) | [`bitmapset`]   | `bitmapset.c`         |
//! | value + core      | [`value_core`]        | `value.c` + nodes.h   |
//! | list              | [`list`]              | `list.c`              |
//! | makefuncs         | [`makefuncs`]         | `makefuncs.c`         |
//! | tidbitmap         | [`tidbitmap`]         | `tidbitmap.c`         |
//! | print             | [`print`]             | `print.c`             |
//! | params            | [`params`]            | `params.c`            |
//! | multibitmapset    | [`multibitmapset`]    | `multibitmapset.c`    |
//! | read              | [`read`]              | `read.c`              |
//! | nodefuncs         | [`nodefuncs`]         | `nodeFuncs.c` (sub-decomp) |
//!
//! `extensible.c` is carved out as the already-ported `backend-nodes-extensible`
//! unit and is NOT part of this crate.
//!
//! ## Keystone
//!
//! The **bitmapset** family is the shared ABI/lifetime foundation: it owns the
//! `bms_*` operations over the owned `types_nodes::Bitmapset<'mcx>`, and it owns
//! and installs `backend-nodes-core-seams` — already depended on by merged
//! executor/optimizer units (`execUtils`, `nodeAppend`, `nodeBitmapHeapscan`,
//! `nodeMemoize`, `nbtree`, …). It is ported with full logic in THIS scaffold
//! phase so the crate compiles and those consumers get a real implementation.
//! The remaining families are module skeletons (fixed C-faithful signatures +
//! `todo!()` bodies) filled in follow-up passes.

pub mod bitmapset;
pub mod list;
pub mod makefuncs;
pub mod multibitmapset;
pub mod nodefuncs;
pub mod params;
pub mod print;
pub mod read;
pub mod tidbitmap;
pub mod value_core;

/// Install this unit's inward seams.
///
/// The KEYSTONE bitmapset family is fully ported, so its seams in
/// `backend-nodes-core-seams` are installed here. The remaining `bms_*` seam
/// surface is all backed by [`bitmapset`]. The **makefuncs** family is filled,
/// so its three canonical seams in `backend-nodes-makefuncs-seams`
/// (`make_const_node`, `make_and_boolexpr`, `make_type_name_from_name_list`)
/// are installed here too.
///
/// `tbm_add_tuple` (in the same seams crate) and every seam in
/// `backend-nodes-core-tidbitmap-seams` / `-params-seams` / `-read-seams` /
/// `-nodeFuncs-seams` stay UNINSTALLED (they panic on call) until their
/// families are filled — `mirror-pg-and-panic`.
pub fn init_seams() {
    use backend_nodes_core_seams as seams;

    seams::bms_is_member::set(bitmapset::bms_is_member);
    seams::bms_add_member::set(bitmapset::bms_add_member);
    seams::bms_next_member::set(bitmapset::bms_next_member);
    seams::bms_is_empty::set(bitmapset::bms_is_empty);
    seams::bms_intersect::set(bitmapset::bms_intersect);
    seams::bms_join::set(bitmapset::bms_join);
    seams::bms_union::set(bitmapset::bms_union);
    seams::bms_nonempty_difference::set(bitmapset::bms_nonempty_difference);
    seams::bms_copy::set(bitmapset::bms_copy);
    seams::bms_add_members::set(bitmapset::bms_add_members);
    seams::bms_del_member::set(bitmapset::bms_del_member);
    seams::bms_num_members::set(bitmapset::bms_num_members);
    seams::bms_prev_member::set(bitmapset::bms_prev_member);
    seams::bms_overlap::set(bitmapset::bms_overlap);
    seams::bms_add_range::set(bitmapset::bms_add_range);
    seams::bms_del_members::set(bitmapset::bms_del_members);
    seams::bms_equal::set(bitmapset::bms_equal);
    seams::bms_free::set(bitmapset::bms_free);

    // makefuncs family — the three canonical constructor seams.
    use backend_nodes_makefuncs_seams as makefuncs_seams;
    makefuncs_seams::make_const_node::set(makefuncs::make_const_node_seam);
    makefuncs_seams::make_and_boolexpr::set(makefuncs::make_and_boolexpr_seam);
    makefuncs_seams::make_type_name_from_name_list::set(
        makefuncs::make_type_name_from_name_list_seam,
    );
}
