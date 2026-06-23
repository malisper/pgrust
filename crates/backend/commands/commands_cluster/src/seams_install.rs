//! Install every seam this crate owns (`backend-commands-cluster-seams`).
//! `seams-init` calls [`init_seams`] once at startup.

/// Install the CLUSTER / VACUUM FULL entry-point seams.
pub fn init_seams() {
    // ProcessUtility dispatch arm (utility.c: case T_ClusterStmt: cluster()).
    utility_out_seams::cluster::set(cluster_arm);

    cluster_seams::cluster_rel::set(crate::cluster_rel);
    cluster_seams::check_index_is_clusterable::set(crate::check_index_is_clusterable);
    cluster_seams::mark_index_clustered::set(crate::mark_index_clustered);
    cluster_seams::make_new_heap::set(crate::make_new_heap);
    cluster_seams::finish_heap_swap::set(crate::finish_heap_swap);

    // matview.c reaches make_new_heap / finish_heap_swap (cluster.c) through its
    // outward frontier seam crate; cluster owns the bodies. The constant
    // arguments C passes at the matview call sites (ExclusiveLock; the
    // is_system_catalog/swap_toast_by_content/check_constraints/is_internal
    // bools; RecentXmin and ReadNextMultiXactId() as the freeze cutoffs) are
    // marshaled here.
    {
        use matview_deps_seams as m;
        use types_storage::lock::ExclusiveLock;
        m::make_new_heap::set(|matview_oid, table_space, relam, relpersistence| {
            let ctx = mcx::MemoryContext::new("make_new_heap");
            crate::make_new_heap(
                ctx.mcx(),
                matview_oid,
                table_space,
                relam,
                relpersistence as u8,
                ExclusiveLock,
            )
        });
        m::finish_heap_swap::set(|matview_oid, oid_new_heap, relpersistence| {
            let ctx = mcx::MemoryContext::new("finish_heap_swap");
            let frozen_xid = snapmgr::RecentXmin();
            let cutoff_multi =
                multixact_seams::read_next_multixact_id::call()?;
            crate::finish_heap_swap(
                ctx.mcx(),
                matview_oid,
                oid_new_heap,
                false,
                false,
                true,
                true,
                frozen_xid,
                cutoff_multi,
                relpersistence as u8,
            )
        });
    }
}

use mcx::Mcx;
use types_error::PgResult;
use ::nodes::nodes::Node;
use ::nodes::parsestmt::ParseState;

/// `case T_ClusterStmt: cluster(pstate, (ClusterStmt *) parsetree, isTopLevel)`
/// (utility.c). The dispatch carries the parse tree as `&Node`; extract the
/// `ClusterStmt` variant from the node-opaque payload and project it onto the
/// command-view `types_cluster::ClusterStmt` (concrete `RangeVar`/`DefElem`/
/// option-string), which `cluster()` consumes.
fn cluster_arm<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &mut ParseState<'mcx>,
    stmt: &Node<'mcx>,
    _is_top_level: bool,
) -> PgResult<()> {
    let Some(cs) = stmt.as_clusterstmt() else {
        panic!("cluster: parse tree is not a ClusterStmt");
    };
    let view = node_clusterstmt_to_view(cs)?;
    // `cluster()` ignores isTopLevel (C: it is consumed only by the multi-table
    // PreventInTransactionBlock path, which is checked there); the owner reads
    // it through its own seam, so we drop the bool here.
    crate::cluster(mcx, pstate, &view, _is_top_level)
}

/// Project the node-tree `ClusterStmt` (NodePtr children) onto the command-view
/// `types_cluster::ClusterStmt` the `cluster()` body reads.
fn node_clusterstmt_to_view(
    cs: &::nodes::ddlnodes::ClusterStmt<'_>,
) -> PgResult<types_cluster::ClusterStmt> {
    use alloc::string::ToString;
    use alloc::vec::Vec;

    let relation = match &cs.relation {
        None => None,
        Some(n) => {
            let Some(rv) = n.as_rangevar() else {
                panic!("cluster: ClusterStmt.relation is not a RangeVar");
            };
            Some(node_rangevar_to_access(rv))
        }
    };

    let indexname = cs.indexname.as_ref().map(|s| s.as_str().to_string());

    let mut params: Vec<types_cluster::DefElem> = Vec::with_capacity(cs.params.len());
    for n in cs.params.iter() {
        let Some(de) = n.as_defelem() else {
            panic!("cluster: ClusterStmt.params element is not a DefElem");
        };
        params.push(node_defelem_to_view(de)?);
    }

    Ok(types_cluster::ClusterStmt {
        relation,
        indexname,
        params,
    })
}

/// `RangeVar` (node) → `types_tuple::access::RangeVar` (command view), mirroring
/// the shared `to_access_range_var` projection used by the other command crates.
fn node_rangevar_to_access(
    rv: &::nodes::rawnodes::RangeVar<'_>,
) -> types_tuple::access::RangeVar {
    use alloc::string::ToString;
    types_tuple::access::RangeVar {
        catalogname: rv.catalogname.as_ref().map(|s| s.as_str().to_string()),
        schemaname: rv.schemaname.as_ref().map(|s| s.as_str().to_string()),
        relname: rv
            .relname
            .as_ref()
            .map(|s| s.as_str().to_string())
            .unwrap_or_default(),
        inh: rv.inh,
        relpersistence: rv.relpersistence as u8,
        location: rv.location,
    }
}

/// `DefElem` (node) → `types_cluster::DefElem`. The `arg` is one of the value
/// nodes the CLUSTER option parse reads through `defGetBoolean`.
fn node_defelem_to_view(
    de: &::nodes::ddlnodes::DefElem<'_>,
) -> PgResult<types_cluster::DefElem> {
    use alloc::string::ToString;
    use types_cluster::DefElemArg;

    let arg = match &de.arg {
        None => None,
        Some(n) => {
            if let Some(i) = n.as_integer() {
                Some(DefElemArg::Integer(i.ival as i64))
            } else if let Some(f) = n.as_float() {
                Some(DefElemArg::Float(f.fval.as_str().to_string()))
            } else if let Some(b) = n.as_boolean() {
                Some(DefElemArg::Boolean(b.boolval))
            } else if let Some(s) = n.as_string() {
                Some(DefElemArg::String(s.sval.as_str().to_string()))
            } else {
                return Err(types_error::PgError::error(alloc::format!(
                    "unrecognized DefElem arg node (tag {:?})",
                    n.node_tag()
                )));
            }
        }
    };

    Ok(types_cluster::DefElem {
        defnamespace: de.defnamespace.as_ref().map(|s| s.as_str().to_string()),
        defname: de
            .defname
            .as_ref()
            .map(|s| s.as_str().to_string())
            .unwrap_or_default(),
        arg,
        location: de.location,
    })
}
