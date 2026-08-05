use core::cell::RefCell;
use core::ptr::NonNull;

use mcx::{Mcx, PgVec};
use types_core::Oid;
use types_nodes::list::NodeList;
use types_pathnodes::{
    JoinType, NodeId, QualCost, RelId, Relids, RinfoId, UpperRelationKind, JOIN_INNER,
    UPPERREL_SETOP,
};

// PgFdwRelationInfo (postgres_fdw.h). Divergence: the C struct caches the
// ForeignTable/Server/UserMapping catalog rows; here only `serverid` (the
// shippability cache key) is retained — table/server options are re-fetched
// via Get* where needed (deparse re-opens the table anyway), which keeps the
// carrier ForgetSafe (arena-leaked, never dropped; att_stats_memo discipline).
pub struct PgFdwRelationInfo<'mcx> {
    pub pushdown_safe: bool,
    pub remote_conds: PgVec<'mcx, RinfoId>,
    pub local_conds: PgVec<'mcx, RinfoId>,
    pub final_remote_exprs: PgVec<'mcx, NodeId>,
    pub attrs_used: types_nodes::Bitmapset<'mcx>,
    pub qp_is_pushdown_safe: bool,
    pub local_conds_cost: QualCost,
    pub local_conds_sel: f64,
    pub joinclause_sel: f64,
    pub rows: f64,
    pub width: i32,
    pub disabled_nodes: i32,
    pub startup_cost: f64,
    pub total_cost: f64,
    pub retrieved_rows: f64,
    pub rel_startup_cost: f64,
    pub rel_total_cost: f64,
    pub use_remote_estimate: bool,
    pub fdw_startup_cost: f64,
    pub fdw_tuple_cost: f64,
    pub shippable_extensions: PgVec<'mcx, Oid>,
    pub async_capable: bool,
    pub serverid: Oid,
    pub fetch_size: i32,
    pub relation_name: &'mcx str,
    pub outerrel: Option<RelId>,
    pub innerrel: Option<RelId>,
    pub jointype: JoinType,
    pub joinclauses: PgVec<'mcx, RinfoId>,
    pub stage: UpperRelationKind,
    pub grouped_tlist: NodeList<'mcx>,
    pub make_outerrel_subquery: bool,
    pub make_innerrel_subquery: bool,
    pub lower_subquery_rels: Relids<'mcx>,
    pub hidden_subquery_rels: Relids<'mcx>,
    pub relation_index: i32,
}

mcx::forget_safe_struct!(
    PgFdwRelationInfo<'_> {
        pushdown_safe, remote_conds, local_conds, final_remote_exprs, attrs_used,
        qp_is_pushdown_safe, local_conds_cost, local_conds_sel, joinclause_sel,
        rows, width, disabled_nodes, startup_cost, total_cost, retrieved_rows,
        rel_startup_cost, rel_total_cost, use_remote_estimate, fdw_startup_cost,
        fdw_tuple_cost, shippable_extensions, async_capable, serverid, fetch_size,
        relation_name, outerrel, innerrel, jointype, joinclauses, stage,
        grouped_tlist, make_outerrel_subquery, make_innerrel_subquery,
        lower_subquery_rels, hidden_subquery_rels, relation_index
    },
);

impl<'mcx> PgFdwRelationInfo<'mcx> {
    // palloc0(sizeof(PgFdwRelationInfo)) shape.
    pub fn new(mcx: Mcx<'mcx>) -> Self {
        PgFdwRelationInfo {
            pushdown_safe: false,
            remote_conds: PgVec::new_in(mcx),
            local_conds: PgVec::new_in(mcx),
            final_remote_exprs: PgVec::new_in(mcx),
            attrs_used: types_nodes::Bitmapset::empty(),
            qp_is_pushdown_safe: false,
            local_conds_cost: QualCost::default(),
            local_conds_sel: 0.0,
            joinclause_sel: 0.0,
            rows: 0.0,
            width: 0,
            disabled_nodes: 0,
            startup_cost: 0.0,
            total_cost: 0.0,
            retrieved_rows: 0.0,
            rel_startup_cost: 0.0,
            rel_total_cost: 0.0,
            use_remote_estimate: false,
            fdw_startup_cost: 0.0,
            fdw_tuple_cost: 0.0,
            shippable_extensions: PgVec::new_in(mcx),
            async_capable: false,
            serverid: types_core::InvalidOid,
            fetch_size: 0,
            relation_name: "",
            outerrel: None,
            innerrel: None,
            jointype: JOIN_INNER,
            joinclauses: PgVec::new_in(mcx),
            stage: UPPERREL_SETOP,
            grouped_tlist: NodeList::nil(),
            make_outerrel_subquery: false,
            make_innerrel_subquery: false,
            lower_subquery_rels: types_pathnodes::relids::relids_empty(),
            hidden_subquery_rels: types_pathnodes::relids::relids_empty(),
            relation_index: 0,
        }
    }

    pub fn serverid(&self) -> Oid {
        self.serverid
    }
}

pub(crate) fn attach_fpinfo<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &mut types_pathnodes::RelOptInfo<'mcx>,
    fpinfo: PgFdwRelationInfo<'mcx>,
) -> types_error::PgResult<()> {
    let cell = mcx::forget_box_in(mcx, RefCell::new(fpinfo))?;
    rel.fdw_state = Some(NonNull::from(cell).cast());
    Ok(())
}

/// C `(PgFdwRelationInfo *) rel->fdw_private`. SAFETY: `fdw_state` is written
/// only by [`attach_fpinfo`] in this crate, from an allocation in the run's
/// arena — same lifetime as the RelOptInfo that points at it; single-threaded
/// planning, aliasing policed by the RefCell.
pub(crate) fn fpinfo<'mcx>(
    rel: &types_pathnodes::RelOptInfo<'mcx>,
) -> &'mcx RefCell<PgFdwRelationInfo<'mcx>> {
    let p = rel.fdw_state.expect("postgres_fdw fpinfo attached");
    // SAFETY: see item doc.
    unsafe { p.cast::<RefCell<PgFdwRelationInfo<'mcx>>>().as_ref() }
}
