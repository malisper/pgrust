//! `backend/optimizer/plan/initsplan.c` — target list, group by, qualification
//! and joininfo initialization, ported 1:1 over this repo's planner arena +
//! [`PlannerRun`](::pathnodes::planner_run::PlannerRun) resolver model.
//!
//! Every `RelOptInfo *` is a [`RelId`](::pathnodes::RelId), every
//! `RestrictInfo *` a [`RinfoId`](::pathnodes::RinfoId), and every
//! expression a [`NodeId`](::pathnodes::NodeId) into the planner node arena.
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

use ::pathnodes::planner_run::PlannerRun;
use pathnodes::{PlannerInfo, Relids, SpecialJoinInfo};

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
/// `build_subplan` when deciding whether to add a `Material` node. The GUC's
/// backing store is owned by costsize.c (`guc.rs` installs the slot accessor);
/// read the live value through the guc-table slot so `SET enable_material`
/// takes effect here too, rather than a never-written local mirror.
#[inline]
pub fn enable_material() -> bool {
    ::guc_tables::vars::enable_material.read()
}

/* ==========================================================================
 * GUC globals (optimizer.h externs) read by deconstruct_recurse.
 * ======================================================================== */

/// `int from_collapse_limit` (GUC) backing storage (optimizer.h extern;
/// allocgocon.c `conf->variable`). FROM-list collapse threshold; boot_val 8.
/// This is the owner-held `conf->variable` the GUC engine reads/writes through
/// the installed [`GucVarAccessors`](::guc_tables::GucVarAccessors).
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
pub struct JoinTreeItem<'mcx> {
    /// kind of the jointree node this item describes
    pub kind: JtNodeKind<'mcx>,
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
    pub oj_joinclauses: alloc::vec::Vec<nodes::primnodes::Expr<'mcx>>,
    /// `List *lateral_clauses` — quals postponed from children (owned `Expr`s)
    pub lateral_clauses: alloc::vec::Vec<nodes::primnodes::Expr<'mcx>>,
}

/// Which jointree node a [`JoinTreeItem`] describes, plus the scalars the
/// distribute passes read off it.
#[derive(Clone, Debug)]
pub enum JtNodeKind<'mcx> {
    /// `RangeTblRef` — carries the RT index.
    RangeTblRef {
        rtindex: i32,
    },
    /// `FromExpr` — carries an owned clone of its `quals` (implicit-AND list).
    FromExpr {
        quals: alloc::vec::Vec<nodes::primnodes::Expr<'mcx>>,
    },
    /// `JoinExpr` — carries its `jointype`, `rtindex`, and an owned clone of its
    /// `quals` (implicit-AND list).
    JoinExpr {
        jointype: ::pathnodes::JoinType,
        rtindex: i32,
        quals: alloc::vec::Vec<nodes::primnodes::Expr<'mcx>>,
    },
}

impl Default for JtNodeKind<'_> {
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
    use plan_small_seams as psmall;
    use equivclass_ext_seams as eqext;
    use joininfo_ext_seams as jiext;

    /* ---- GUC int var accessors owned here (initsplan.c globals) --------- *
     * `int from_collapse_limit` / `int join_collapse_limit` are plain USERSET
     * GUC ints (guc_tables.c, QUERY_TUNING_OTHER, boot_val 8). The C reads them
     * directly off the `conf->variable` global at deconstruct_jointree time
     * (initsplan.c) — they are NOT seeded from ControlFile. The GUC engine seeds
     * the owner-held storage from boot_val and assigns user/config changes
     * through these accessors. */
    {
        use guc_tables::{vars, GucVarAccessors};
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
        baserels::add_base_rels_to_query(root, run, jtnode)
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
    psmall::add_other_rels_to_query::set(|root, run| {
        baserels::add_other_rels_to_query(root, run).expect("add_other_rels_to_query")
    });
    // rebuild_lateral_attr_needed / rebuild_joinclause_attr_needed are ported
    // here (lateral.rs / targetlist.rs); analyzejoins calls them via these seams
    // after a join removal (outer-join or self-join elimination).
    psmall::rebuild_lateral_attr_needed::set(|root, run| {
        lateral::rebuild_lateral_attr_needed(root, run)
    });
    psmall::rebuild_joinclause_attr_needed::set(|root, run| {
        targetlist::rebuild_joinclause_attr_needed(root, run)
            .expect("rebuild_joinclause_attr_needed")
    });

    /* ---- initsplan.c-owned fns called by equivclass (equivclass-ext-seams) */
    eqext::add_vars_to_targetlist::set(|mcx, root, vars, where_needed| {
        targetlist::add_vars_to_targetlist(mcx, root, vars, where_needed)
    });
    eqext::add_vars_to_attr_needed::set(|mcx, root, vars, where_needed| {
        targetlist::add_vars_to_attr_needed(mcx, root, vars, where_needed)
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
    jiext::add_vars_to_targetlist::set(|mcx, root, vars, where_needed| {
        targetlist::add_vars_to_targetlist(mcx, root, vars, where_needed)
    });
    jiext::add_vars_to_attr_needed::set(|mcx, root, vars, where_needed| {
        targetlist::add_vars_to_attr_needed(mcx, root, vars, where_needed)
    });
    jiext::restriction_is_always_true::set(|root, ri| {
        quals::restriction_is_always_true_for(root, ri)
    });
    jiext::restriction_is_always_false::set(|root, ri| {
        quals::restriction_is_always_false_for(root, ri)
    });

    /* ---- subselect.c-owned SS_attach_initplans (createplan-seams) -------- */
    // `SS_attach_initplans(root, plan)` — createplan.c calls this at the top of
    // `create_plan`. subselect.c owns it; install over the finalize module fn.
    createplan_seams::ss_attach_initplans::set(|mcx, root, plan| {
        finalize::SS_attach_initplans(mcx, root, plan)
    });

    // `resolve_cte_subplan` / `resolve_worktable_param` — the SubPlan-init /
    // wt_param_id resolution legs of create_ctescan_plan / create_worktablescan_plan
    // (createplan.c:3884 / :4055). They dereference the init SubPlans and
    // `wt_param_id` that SS_process_ctes builds here, so subselect.c owns them.
    createplan_seams::resolve_cte_subplan::set(
        subplan::resolve_cte_subplan,
    );
    createplan_seams::resolve_worktable_param::set(
        subplan::resolve_worktable_param,
    );

    // `multiexpr_param_lookup` (setrefs-seams) — resolve a PARAM_MULTIEXPR Param
    // to its replacement expression. The replacement Params are interned into the
    // node arena by SS_process_sublinks here, so subselect.c owns the lookup.
    setrefs_seams::multiexpr_param_lookup::set(
        subplan::resolve_multiexpr_param,
    );
}

// Suppress "unused" until consumers reference these re-exports.
#[allow(unused_imports)]
use {PlannerInfo as _PlannerInfo, PlannerRun as _PlannerRun};
