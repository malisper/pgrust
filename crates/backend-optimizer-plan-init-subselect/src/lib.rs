//! `backend/optimizer/plan/initsplan.c` — target list, group by, qualification
//! and joininfo initialization, ported 1:1 over this repo's planner arena +
//! [`PlannerRun`](types_pathnodes::planner_run::PlannerRun) resolver model.
//!
//! Every `RelOptInfo *` is a [`RelId`](types_pathnodes::RelId), every
//! `RestrictInfo *` a [`RinfoId`](types_pathnodes::RinfoId), and every
//! expression a [`NodeId`](types_pathnodes::NodeId) into the planner node arena.
//! `Relids` set algebra is reached through `backend-optimizer-util-relnode`
//! seams; equivclass / joininfo / lsyscache / nodeFuncs through their seam
//! crates; and the parse tree (`parse->jointree`/`rtable`/`targetList`) through
//! the `PlannerRun` resolver (#264), passed alongside `&mut PlannerInfo`.
//!
//! The eight outward-facing `initsplan.c` entry points consumed by
//! `query_planner` (planmain.c) are installed into
//! `backend-optimizer-plan-small-seams`. The `add_vars_to_targetlist` /
//! `add_vars_to_attr_needed` / `distribute_restrictinfo_to_rels` /
//! `process_implied_equality` / `build_implied_join_equality` /
//! `restriction_is_always_true` / `restriction_is_always_false` functions
//! `initsplan.c` owns (but which equivclass / joininfo call) are installed into
//! the `*-ext-seams` consumer crates.

#![allow(non_snake_case)]
#![allow(clippy::too_many_arguments)]

extern crate alloc;

use types_pathnodes::planner_run::PlannerRun;
use types_pathnodes::{PlannerInfo, Relids, SpecialJoinInfo};

pub mod baserels;
pub mod fkeys;
pub mod groupby;
pub mod jointree;
pub mod lateral;
pub mod mergehash;
pub mod outerjoin;
pub mod quals;
pub mod targetlist;

// subselect.c (SubPlan-building half).
pub mod correlation;
pub mod finalize;
pub mod subplan;

/// `enable_material` (cost.h GUC) — default true. Read by
/// `build_subplan` when deciding whether to add a `Material` node. The GUC table
/// lives in an unported owner; mirror the boot-time default as a local read.
pub static mut ENABLE_MATERIAL: bool = true;

#[inline]
pub fn enable_material() -> bool {
    // SAFETY: single-threaded planner; mirrors the C global read.
    unsafe { core::ptr::addr_of!(ENABLE_MATERIAL).read() }
}

/* ==========================================================================
 * GUC globals (optimizer.h externs) read by deconstruct_recurse.
 * ======================================================================== */

/// `int from_collapse_limit` (GUC) backing storage (optimizer.h extern;
/// allocgocon.c `conf->variable`). FROM-list collapse threshold; boot_val 8.
/// This is the owner-held `conf->variable` the GUC engine reads/writes through
/// the installed [`GucVarAccessors`](backend_utils_misc_guc_tables::GucVarAccessors).
pub static mut FROM_COLLAPSE_LIMIT: i32 = 8;
/// `int join_collapse_limit` (GUC) backing storage. Explicit-JOIN collapse
/// threshold; boot_val 8. Owner-held `conf->variable` for the GUC engine.
pub static mut JOIN_COLLAPSE_LIMIT: i32 = 8;

#[inline]
pub fn from_collapse_limit() -> i32 {
    // SAFETY: single-threaded planner; mirrors the C global read.
    unsafe { core::ptr::addr_of!(FROM_COLLAPSE_LIMIT).read() }
}

#[inline]
pub fn join_collapse_limit() -> i32 {
    unsafe { core::ptr::addr_of!(JOIN_COLLAPSE_LIMIT).read() }
}

/* ==========================================================================
 * JoinTreeItem — the transient per-jointree-node working struct that
 * deconstruct_jointree threads through its three passes, modelled as a
 * Vec<JoinTreeItem> arena keyed by a JtId index (the owned-tree analogue of the
 * C List * of palloc'd JoinTreeItem * whose jti_parent links form a tree).
 * ======================================================================== */

/// Handle into the [`JoinTreeItem`] arena built by `deconstruct_recurse`.
pub type JtId = usize;

/// `JoinTreeItem` (initsplan.c) — transient per-jointree-node working struct.
#[derive(Clone, Debug, Default)]
pub struct JoinTreeItem {
    /// kind of the jointree node this item describes
    pub kind: JtNodeKind,
    /// depth-first post-order rank (the order C appends to `item_list` and the
    /// order the distribute passes run, bottom-up)
    pub post_order: usize,
    /// `JoinDomain *jdomain` — index into `root.join_domains`
    pub jdomain: usize,
    /// `JoinTreeItem *jti_parent` — arena index, or `None` at the top
    pub jti_parent: Option<JtId>,
    /// `Relids qualscope`
    pub qualscope: Relids,
    /// `Relids inner_join_rels`
    pub inner_join_rels: Relids,
    /// `Relids left_rels` (join nodes)
    pub left_rels: Relids,
    /// `Relids right_rels` (join nodes)
    pub right_rels: Relids,
    /// `Relids nonnullable_rels` (outer joins)
    pub nonnullable_rels: Relids,
    /// `SpecialJoinInfo *sjinfo` (outer joins) — filled during distribute
    pub sjinfo: Option<alloc::boxed::Box<SpecialJoinInfo>>,
    /// `List *oj_joinclauses` — postponed outer-join quals (owned `Expr`s)
    pub oj_joinclauses: alloc::vec::Vec<types_nodes::primnodes::Expr>,
    /// `List *lateral_clauses` — quals postponed from children (owned `Expr`s)
    pub lateral_clauses: alloc::vec::Vec<types_nodes::primnodes::Expr>,
}

/// Which jointree node a [`JoinTreeItem`] describes, plus the scalars the
/// distribute passes read off it.
#[derive(Clone, Debug)]
pub enum JtNodeKind {
    /// `RangeTblRef` — carries the RT index.
    RangeTblRef {
        rtindex: i32,
    },
    /// `FromExpr` — carries an owned clone of its `quals` (implicit-AND list).
    FromExpr {
        quals: alloc::vec::Vec<types_nodes::primnodes::Expr>,
    },
    /// `JoinExpr` — carries its `jointype`, `rtindex`, and an owned clone of its
    /// `quals` (implicit-AND list).
    JoinExpr {
        jointype: types_pathnodes::JoinType,
        rtindex: i32,
        quals: alloc::vec::Vec<types_nodes::primnodes::Expr>,
    },
}

impl Default for JtNodeKind {
    fn default() -> Self {
        JtNodeKind::RangeTblRef { rtindex: 0 }
    }
}

/* ==========================================================================
 * Constants transcribed from the C headers.
 * ======================================================================== */

/// `BMS_EMPTY_SET` / `BMS_SINGLETON` / `BMS_MULTIPLE` membership codes returned
/// by `relids_membership` (`bms_membership`).
pub const BMS_EMPTY_SET: i32 = 0;
pub const BMS_SINGLETON: i32 = 1;
pub const BMS_MULTIPLE: i32 = 2;

/// `BOOLOID` (catalog/pg_type_d.h).
pub const BOOLOID: types_core::primitive::Oid = 16;
/// `InvalidOid`.
pub const INVALID_OID: types_core::primitive::Oid = 0;
/// `FirstLowInvalidHeapAttributeNumber` (access/sysattr.h).
pub const FIRST_LOW_INVALID_HEAP_ATTRIBUTE_NUMBER: i32 = -8;
/// `PG_INT32_MAX`.
pub const PG_INT32_MAX: i32 = i32::MAX;

/* ==========================================================================
 * Seam installation. The eight planmain.c entry points carry an extra
 * `run: &PlannerRun` (the resolver model #264) because the owner must reach the
 * parse jointree / rtable / targetList; `query_planner` (the sole consumer)
 * already holds the run and passes it. The ext-seam-owned functions match the
 * declared owned-value signatures and marshal to the C-faithful internal fns.
 * ======================================================================== */

pub fn init_seams() {
    use backend_optimizer_plan_small_seams as psmall;
    use backend_optimizer_path_equivclass_ext_seams as eqext;
    use backend_optimizer_util_joininfo_ext_seams as jiext;

    /* ---- GUC int var accessors owned here (initsplan.c globals) --------- *
     * `int from_collapse_limit` / `int join_collapse_limit` are plain USERSET
     * GUC ints (guc_tables.c, QUERY_TUNING_OTHER, boot_val 8). The C reads them
     * directly off the `conf->variable` global at deconstruct_jointree time
     * (initsplan.c) — they are NOT seeded from ControlFile. The GUC engine seeds
     * the owner-held storage from boot_val and assigns user/config changes
     * through these accessors. */
    {
        use backend_utils_misc_guc_tables::{vars, GucVarAccessors};
        vars::from_collapse_limit.install(GucVarAccessors {
            get: from_collapse_limit,
            set: |v| {
                // SAFETY: single-threaded GUC assignment; mirrors `conf->variable = v`.
                unsafe { *core::ptr::addr_of_mut!(FROM_COLLAPSE_LIMIT) = v }
            },
        });
        vars::join_collapse_limit.install(GucVarAccessors {
            get: join_collapse_limit,
            set: |v| unsafe { *core::ptr::addr_of_mut!(JOIN_COLLAPSE_LIMIT) = v },
        });
    }

    /* ---- planmain.c entry points (plan-small-seams) ------------------- */
    psmall::add_base_rels_to_query::set(|root, run, jtnode| {
        baserels::add_base_rels_to_query(root, run, jtnode).expect("add_base_rels_to_query")
    });
    psmall::remove_useless_groupby_columns::set(|root, run| {
        groupby::remove_useless_groupby_columns(root, run)
    });
    psmall::build_base_rel_tlists::set(|root, run| {
        targetlist::build_base_rel_tlists(root, run).expect("build_base_rel_tlists")
    });
    psmall::find_lateral_references::set(|root, run| {
        lateral::find_lateral_references(root, run)
    });
    psmall::deconstruct_jointree::set(|root, run| {
        jointree::deconstruct_jointree(root, run)
    });
    psmall::create_lateral_join_info::set(|root, run| {
        lateral::create_lateral_join_info(root, run)
    });
    psmall::match_foreign_keys_to_quals::set(|root| {
        fkeys::match_foreign_keys_to_quals(root)
    });
    psmall::add_other_rels_to_query::set(|root| {
        baserels::add_other_rels_to_query(root).expect("add_other_rels_to_query")
    });
    // rebuild_lateral_attr_needed is ported (lateral.rs); analyzejoins calls it
    // via this seam after a join removal. rebuild_joinclause_attr_needed is NOT
    // yet ported, so it is left to panic via its default seam body.
    psmall::rebuild_lateral_attr_needed::set(|root, run| {
        lateral::rebuild_lateral_attr_needed(root, run)
    });

    /* ---- initsplan.c-owned fns called by equivclass (equivclass-ext-seams) */
    eqext::add_vars_to_targetlist::set(|root, vars, where_needed| {
        targetlist::add_vars_to_targetlist(root, vars, where_needed)
    });
    eqext::add_vars_to_attr_needed::set(|root, vars, where_needed| {
        targetlist::add_vars_to_attr_needed(root, vars, where_needed)
    });
    eqext::distribute_restrictinfo_to_rels::set(|run, root, restrictinfo| {
        quals::distribute_restrictinfo_to_rels(run, root, restrictinfo)
    });
    eqext::process_implied_equality::set(
        |run, root, opno, collation, item1, item2, qualscope, security_level, both_const| {
            quals::process_implied_equality(
                run, root, opno, collation, &item1, &item2, &qualscope, security_level, both_const,
            )
        },
    );
    eqext::build_implied_join_equality::set(
        |run, root, opno, collation, item1, item2, qualscope, security_level| {
            quals::build_implied_join_equality(
                run, root, opno, collation, &item1, &item2, &qualscope, security_level,
            )
        },
    );

    /* ---- initsplan.c-owned fns called by joininfo (joininfo-ext-seams) -- */
    jiext::add_vars_to_targetlist::set(|root, vars, where_needed| {
        targetlist::add_vars_to_targetlist(root, vars, where_needed)
    });
    jiext::add_vars_to_attr_needed::set(|root, vars, where_needed| {
        targetlist::add_vars_to_attr_needed(root, vars, where_needed)
    });
    jiext::restriction_is_always_true::set(|root, clause| {
        quals::restriction_is_always_true(root, clause)
    });
    jiext::restriction_is_always_false::set(|root, clause| {
        quals::restriction_is_always_false(root, clause)
    });

    /* ---- subselect.c-owned SS_attach_initplans (createplan-seams) -------- */
    // `SS_attach_initplans(root, plan)` — createplan.c calls this at the top of
    // `create_plan`. subselect.c owns it; install over the finalize module fn.
    backend_optimizer_plan_createplan_seams::ss_attach_initplans::set(|mcx, root, plan| {
        finalize::SS_attach_initplans(mcx, root, plan)
    });
}

// Suppress "unused" until consumers reference these re-exports.
#[allow(unused_imports)]
use {PlannerInfo as _PlannerInfo, PlannerRun as _PlannerRun};
