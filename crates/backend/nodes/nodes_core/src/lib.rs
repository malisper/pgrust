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
//! `bms_*` operations over the owned `nodes::Bitmapset<'mcx>`, and it owns
//! and installs `backend-nodes-core-seams` — already depended on by merged
//! executor/optimizer units (`execUtils`, `nodeAppend`, `nodeBitmapHeapscan`,
//! `nodeMemoize`, `nbtree`, …). It is ported with full logic in THIS scaffold
//! phase so the crate compiles and those consumers get a real implementation.
//! The remaining families are module skeletons (fixed C-faithful signatures +
//! stub bodies) filled in follow-up passes.
//!
//! The **params** family ([`params`]) is filled: the full `nodes/params.c`
//! machinery over a handle-keyed `ParamListInfoData` store, with the owned
//! `make_param_list` seam installed.

pub mod bitmapset;
pub mod list;
pub mod makefuncs;
pub mod multibitmapset;
pub mod node_walker;
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
/// The **tidbitmap** family is also filled: `tbm_add_tuple` (in the same seams
/// crate) and the `backend-nodes-core-tidbitmap-seams` surface are installed via
/// [`tidbitmap::init_seams`]. The **params** family (`nodes/params.c`) is filled,
/// so its `backend-nodes-params-seams::make_param_list` seam is installed here.
/// The **read** family is now filled, so its
/// `backend-nodes-read-seams::string_to_node` is installed here; `string_to_node`
/// of a node tree still routes the body through the unported
/// `backend-nodes-readfuncs-seams::parse_node_string`, which panics until the
/// readfuncs unit lands. Every seam in `-nodeFuncs-seams` stays UNINSTALLED (they
/// panic on call) until their families are filled — `mirror-pg-and-panic`.
pub fn init_seams() {
    use nodes_core_seams as seams;

    // tidbitmap family: tbm_add_tuple + the tidbitmap-seams surface.
    tidbitmap::init_seams();

    // params family (nodes/params.c): the canonical `make_param_list` seam +
    // the Bind-message param-slot writer.
    params_seams::make_param_list::set(params::make_param_list_value);
    params_seams::store_param_extern::set(params::store_param_extern);

    // read family (nodes/read.c): string_to_node.
    read_seams::string_to_node::set(read::string_to_node);
    read_seams::string_to_node_opt::set(read::string_to_node_opt);

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

    // expression_tree_walker (node_walker.rs) — generic expression recursion.
    seams::expression_tree_walker::set(node_walker::expression_tree_walker);

    // makefuncs family — the three canonical constructor seams.
    use makefuncs_seams as makefuncs_seams;
    makefuncs_seams::make_const_node::set(makefuncs::make_const_node_seam);
    makefuncs_seams::make_and_boolexpr::set(makefuncs::make_and_boolexpr_seam);
    makefuncs_seams::make_type_name_from_name_list::set(
        makefuncs::make_type_name_from_name_list_seam,
    );

    // The nodefuncs family (`nodeFuncs.c`) owns `backend-nodes-nodeFuncs-seams`
    // and installs its expression-inspection seams.
    nodefuncs::init_seams();

    // print family (`nodes/print.c`): the three ad-hoc printers this crate owns
    // and can fully evaluate (`print_rt`/`print_expr`/`print_tl`). `print_pathkeys`
    // and `print_slot` stay UNINSTALLED — they route to their genuine unported
    // owners (the planner EC arena and the printtup slot runtime) and panic on
    // call (`mirror-pg-and-panic`).
    seams::print_rt::set(print::print_rt);
    seams::print_expr::set(print::print_expr);
    seams::print_tl::set(print::print_tl);

    install_execparallel_support_bms_seams();
}

/// Install the bitmapset-membership accessors the parallel executor reaches
/// over the `execParallel-support` seam (`bms_next_member`/`bms_num_members`/
/// `bms_is_empty`). These are the same `bitmapset.c` bodies this crate owns;
/// the support seam carries a non-`Option` `&Bitmapset` (the parallel param
/// set is always present when these are called), so adapt to the owner's
/// `Option<&Bitmapset>` C-nullability surface.
fn install_execparallel_support_bms_seams() {
    use execParallel_support_seams as sup;
    sup::bms_next_member::set(|a, prevbit| bitmapset::bms_next_member(Some(a), prevbit));
    sup::bms_num_members::set(|a| bitmapset::bms_num_members(Some(a)));
    sup::bms_is_empty::set(|a| bitmapset::bms_is_empty(Some(a)));

    // `params.c` (de)serialization the parallel executor reaches over the
    // `execParallel-support` seam. `SerializeCursor` is a real address into the
    // mapped DSM segment (== C's `char *start_address`); convert to/from the raw
    // pointer the body threads.
    sup::estimate_param_list_space::set(|param_li| {
        // C's `EstimateParamListSpace` returns `Size` with no error path; its
        // `get_typlenbyval` lookups are over already-resolved param types, so
        // an error is a programming error (matches the seam's infallible
        // contract).
        params::EstimateParamListSpace(param_li.as_deref()).expect("EstimateParamListSpace")
    });
    sup::serialize_param_list::set(|param_li, chunk| {
        // SAFETY: `chunk` addresses a leader-reserved chunk of at least
        // `EstimateParamListSpace(param_li)` bytes (execParallel allocated it).
        let advanced =
            unsafe { params::SerializeParamList(param_li.as_deref(), chunk.0 as *mut u8) }?;
        Ok(execparallel::SerializeCursor(advanced as usize))
    });
    sup::restore_param_list::set(|chunk| {
        // SAFETY: `chunk` addresses a `SerializeParamList` image in the worker's
        // mapped DSM segment. C discards the advanced cursor here (the chunk is
        // looked up by key), so only the rebuilt value is returned.
        let (param_li, _advanced) =
            unsafe { params::RestoreParamList(chunk.0 as *mut u8) }
                .expect("RestoreParamList");
        param_li
    });
}
