use core::cell::RefCell;
use core::ptr::NonNull;

use foreigncmds::foreign::{ForeignServer, ForeignTable, UserMapping};
use mcx::{Mcx, PgVec};
use types_core::Oid;
use types_nodes::list::NodeList;
use types_nodes::JoinType;
use types_pathnodes::{
    NodeId, QualCost, RelId, Relids, RinfoId, UpperRelationKind, UPPERREL_SETOP,
};

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
    pub table: Option<ForeignTable<'mcx>>,
    pub server: Option<ForeignServer<'mcx>>,
    pub user: Option<UserMapping<'mcx>>,
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

const _: () = assert!(!core::mem::needs_drop::<PgFdwRelationInfo<'static>>());

impl<'mcx> PgFdwRelationInfo<'mcx> {
    // palloc0(sizeof(PgFdwRelationInfo)) shape: zeroes everywhere C relies
    // on them (retrieved_rows/rel_*_cost sentinels are set by callers).
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
            table: None,
            server: None,
            user: None,
            fetch_size: 0,
            relation_name: "",
            outerrel: None,
            innerrel: None,
            jointype: JoinType::JOIN_INNER,
            joinclauses: PgVec::new_in(mcx),
            stage: UPPERREL_SETOP,
            grouped_tlist: NodeList::nil(),
            make_outerrel_subquery: false,
            make_innerrel_subquery: false,
            lower_subquery_rels: None,
            hidden_subquery_rels: None,
            relation_index: 0,
        }
    }

    pub fn serverid(&self) -> Oid {
        self.server.as_ref().expect("fpinfo.server set by GetForeignRelSize").serverid
    }
}

pub(crate) fn attach_fpinfo<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &mut types_pathnodes::RelOptInfo<'mcx>,
    fpinfo: PgFdwRelationInfo<'mcx>,
) -> types_error::PgResult<()> {
    let cell = mcx::leak_in(mcx::alloc_in(mcx, RefCell::new(fpinfo))?);
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
    unsafe { p.cast::<RefCell<PgFdwRelationInfo<'mcx>>>().as_ref() }
}

pub(crate) fn has_fpinfo(rel: &types_pathnodes::RelOptInfo<'_>) -> bool {
    rel.fdw_state.is_some()
}
