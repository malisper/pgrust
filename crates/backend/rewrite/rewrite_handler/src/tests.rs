use std::cell::RefCell;
use std::rc::Rc;
use std::sync::Once;

use datum::Datum;
use mcx::{alloc_in, leak_in, Mcx, MemoryContext, PgVec};
use types_core::{Oid, INVALID_PROC_NUMBER, RELPERSISTENCE_PERMANENT};
use types_error::{PgError, PgResult};
use types_nodes::list::NodeList;
use types_nodes::node_tree::Node;
use types_nodes::nodes_enums::CmdType;
use types_nodes::parsenodes::{Query, QuerySource, RTEKind, RangeTblEntry};
use types_rel::{
    AccessShareLock, FormData_pg_class, LockInfoData, LockRelId, NoLock, Relation, RelationData,
    RowExclusiveLock, RowShareLock, LOCKMODE, RELKIND_MATVIEW, RELKIND_RELATION, RELKIND_VIEW,
    REPLICA_IDENTITY_DEFAULT,
};
use types_tuple::{NameData, TupleDescData};

use crate::{AcquireRewriteLocks, QueryRewrite};

const TBL: Oid = 1;
const VIEW: Oid = 2;
const RLS_TBL: Oid = 3;
const MATVIEW: Oid = 4;

thread_local! {
    static OPENS: RefCell<Vec<(Oid, LOCKMODE)>> = const { RefCell::new(Vec::new()) };
}

fn opens() -> Vec<(Oid, LOCKMODE)> {
    OPENS.with_borrow(|v| v.clone())
}

fn reset_opens() {
    OPENS.with_borrow_mut(|v| v.clear());
}

fn entry(oid: Oid) -> Option<(&'static str, u8, bool)> {
    match oid {
        TBL => Some(("tbl", RELKIND_RELATION, false)),
        VIEW => Some(("vw", RELKIND_VIEW, false)),
        RLS_TBL => Some(("rls_tbl", RELKIND_RELATION, true)),
        MATVIEW => Some(("mv", RELKIND_MATVIEW, false)),
        _ => None,
    }
}

fn make<'mcx>(mcx: Mcx<'mcx>, oid: Oid, name: &str, relkind: u8, rls: bool) -> Relation<'mcx> {
    let mut relname = NameData::default();
    relname.namestrcpy(name);
    let data = RelationData {
        rd_id: oid,
        rd_backend: INVALID_PROC_NUMBER,
        rd_islocaltemp: false,
        rd_isvalid: std::cell::Cell::new(true),
        rd_createSubid: std::cell::Cell::new(0),
        rd_newRelfilelocatorSubid: std::cell::Cell::new(0),
        rd_firstRelfilelocatorSubid: std::cell::Cell::new(0),
        rd_droppedSubid: std::cell::Cell::new(0),
        rd_lockInfo: LockInfoData {
            lockRelId: LockRelId { relId: oid, dbId: 5 },
        },
        rd_rel: FormData_pg_class {
            relname,
            relnamespace: 2200,
            reltype: 0,
            relowner: 10,
            relam: 2,
            relfilenode: oid,
            reltablespace: 0,
            relpages: 0,
            reltuples: -1.0,
            relallvisible: 0,
            reltoastrelid: 0,
            relhasindex: false,
            relisshared: false,
            relpersistence: RELPERSISTENCE_PERMANENT,
            relkind,
            relhassubclass: false,
            relrowsecurity: rls,
            relispopulated: true,
            relreplident: REPLICA_IDENTITY_DEFAULT,
            relispartition: false,
            relfrozenxid: 3,
            relminmxid: 1,
        },
        rd_att: Rc::new(TupleDescData {
            natts: 0,
            tdtypeid: 0,
            tdtypmod: -1,
            tdrefcount: 1,
            constr: None,
            compact_attrs: PgVec::new_in(mcx),
            attrs: PgVec::new_in(mcx),
        }),
        rd_index: None,
        rd_opcintype: PgVec::new_in(mcx),
        rd_opfamily: PgVec::new_in(mcx),
        rd_indoption: PgVec::new_in(mcx),
        rd_indcollation: PgVec::new_in(mcx),
        rd_options: None,
        pgstat_enabled: std::cell::Cell::new(false),
        rd_amcache: Default::default(),
        rd_supportinfo: Default::default(),
    };
    Relation::open(data, None)
}

fn fake_relation_open(mcx: Mcx<'_>, oid: Oid, lockmode: LOCKMODE) -> PgResult<Relation<'_>> {
    OPENS.with_borrow_mut(|v| v.push((oid, lockmode)));
    match entry(oid) {
        Some((name, relkind, rls)) => Ok(make(mcx, oid, name, relkind, rls)),
        None => Err(PgError::error(format!("relation {oid} does not exist")).into()),
    }
}

fn install() {
    static INIT: Once = Once::new();
    INIT.call_once(|| {
        relation_seams::relation_open::set(fake_relation_open);
        table::init_seams();
        crate::init_seams();
    });
}

fn select1<'mcx>(mcx: Mcx<'mcx>) -> Query<'mcx> {
    let one = Node::mk_const(mcx, 23, -1, 0, 4, Datum::from_i32(1), false, true).unwrap();
    let te = Node::mk_target_entry(mcx, one, 1, Some("?column?"), false).unwrap();
    Query {
        commandType: CmdType::CMD_SELECT,
        querySource: QuerySource::QSRC_ORIGINAL,
        queryId: 42,
        canSetTag: true,
        targetList: NodeList::make1(mcx, te).unwrap(),
        stmt_len: 8,
        ..Default::default()
    }
}

fn rte_node<'mcx>(mcx: Mcx<'mcx>, rte: RangeTblEntry<'mcx>) -> Node<'mcx> {
    Node::mk(mcx, rte).unwrap()
}

fn relation_rte<'mcx>(mcx: Mcx<'mcx>, relid: Oid, relkind: u8, rellockmode: LOCKMODE) -> Node<'mcx> {
    rte_node(
        mcx,
        RangeTblEntry {
            rtekind: RTEKind::RTE_RELATION,
            relid,
            relkind,
            rellockmode,
            inFromCl: true,
            ..Default::default()
        },
    )
}

#[test]
fn no_rules_select1_passes_through_byte_stable() {
    install();
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let query = select1(mcx);

    let tl_ptr = query.targetList.as_slice().as_ptr();
    let te_before = query.targetList.nth(0).as_target_entry().unwrap() as *const _;

    let results = QueryRewrite(mcx, query).unwrap();
    assert_eq!(results.len(), 1);

    let q = &results[0];
    assert_eq!(q.commandType, CmdType::CMD_SELECT);
    assert_eq!(q.querySource, QuerySource::QSRC_ORIGINAL);
    assert_eq!(q.queryId, 42);
    assert!(q.canSetTag);
    assert_eq!(q.resultRelation, 0);
    assert!(!q.hasAggs && !q.hasWindowFuncs && !q.hasTargetSRFs && !q.hasSubLinks);
    assert!(!q.hasDistinctOn && !q.hasRecursive && !q.hasModifyingCTE && !q.hasForUpdate);
    assert!(!q.hasRowSecurity && !q.hasGroupRTE && !q.isReturn);
    assert!(q.utilityStmt.is_none() && q.onConflict.is_none());
    assert!(q.jointree.is_none() && q.setOperations.is_none() && q.havingQual.is_none());
    assert!(q.limitOffset.is_none() && q.limitCount.is_none());
    assert!(q.cteList.is_nil() && q.rtable.is_nil() && q.rteperminfos.is_nil());
    assert!(q.returningList.is_nil() && q.groupClause.is_nil() && q.groupingSets.is_nil());
    assert!(q.windowClause.is_nil() && q.distinctClause.is_nil() && q.sortClause.is_nil());
    assert!(q.rowMarks.is_nil() && q.constraintDeps.is_nil() && q.withCheckOptions.is_nil());
    assert_eq!(q.stmt_location, 0);
    assert_eq!(q.stmt_len, 8);
    assert_eq!(q.targetList.len(), 1);
    assert_eq!(q.targetList.as_slice().as_ptr(), tl_ptr);
    let te_after = q.targetList.nth(0).as_target_entry().unwrap() as *const _;
    assert_eq!(te_before, te_after);
}

#[test]
fn no_rules_table_query_passes_through() {
    install();
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let mut query = select1(mcx);
    query.rtable = NodeList::make1(mcx, relation_rte(mcx, TBL, RELKIND_RELATION, AccessShareLock))
        .unwrap();
    let rt_ptr = query.rtable.as_slice().as_ptr();

    reset_opens();
    let results = QueryRewrite(mcx, query).unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].rtable.as_slice().as_ptr(), rt_ptr);
    // fireRIRrules: one rules probe + one RLS probe, both NoLock.
    assert_eq!(opens(), vec![(TBL, NoLock), (TBL, NoLock)]);
}

#[test]
fn matview_rte_is_skipped_by_rir_probe() {
    install();
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let mut query = select1(mcx);
    query.rtable =
        NodeList::make1(mcx, relation_rte(mcx, MATVIEW, RELKIND_MATVIEW, AccessShareLock)).unwrap();

    reset_opens();
    let results = QueryRewrite(mcx, query).unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(opens(), vec![]);
}

#[test]
fn acquire_locks_not_for_execute_uses_access_share() {
    install();
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let mut query = select1(mcx);
    query.rtable =
        NodeList::make1(mcx, relation_rte(mcx, TBL, 0, RowExclusiveLock)).unwrap();

    reset_opens();
    AcquireRewriteLocks(mcx, &query, false, false).unwrap();
    assert_eq!(opens(), vec![(TBL, AccessShareLock)]);
    let rte = query.rtable.nth(0).as_range_tbl_entry().unwrap();
    assert_eq!(rte.relkind, RELKIND_RELATION);
    assert_eq!(rte.rellockmode, RowExclusiveLock);
}

#[test]
fn acquire_locks_for_execute_uses_rellockmode() {
    install();
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let mut query = select1(mcx);
    query.rtable =
        NodeList::make1(mcx, relation_rte(mcx, TBL, 0, RowExclusiveLock)).unwrap();

    reset_opens();
    AcquireRewriteLocks(mcx, &query, true, false).unwrap();
    assert_eq!(opens(), vec![(TBL, RowExclusiveLock)]);
}

#[test]
fn acquire_locks_pushed_down_upgrades_access_share_to_row_share() {
    install();
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let mut query = select1(mcx);
    query.rtable =
        NodeList::make1(mcx, relation_rte(mcx, TBL, 0, AccessShareLock)).unwrap();

    reset_opens();
    AcquireRewriteLocks(mcx, &query, true, true).unwrap();
    assert_eq!(opens(), vec![(TBL, RowShareLock)]);
    let rte = query.rtable.nth(0).as_range_tbl_entry().unwrap();
    assert_eq!(rte.rellockmode, RowShareLock);

    // A stronger pre-existing mode is kept as-is.
    let mut query2 = select1(mcx);
    query2.rtable =
        NodeList::make1(mcx, relation_rte(mcx, TBL, 0, RowExclusiveLock)).unwrap();
    reset_opens();
    AcquireRewriteLocks(mcx, &query2, true, true).unwrap();
    assert_eq!(opens(), vec![(TBL, RowExclusiveLock)]);
}

#[test]
fn acquire_locks_recurses_into_subquery_rte() {
    install();
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let mut sub = select1(mcx);
    sub.rtable = NodeList::make1(mcx, relation_rte(mcx, TBL, 0, AccessShareLock)).unwrap();
    let sub: &Query = leak_in(alloc_in(mcx, sub).unwrap());

    let mut query = select1(mcx);
    query.rtable = NodeList::make1(
        mcx,
        rte_node(
            mcx,
            RangeTblEntry {
                rtekind: RTEKind::RTE_SUBQUERY,
                subquery: Some(sub),
                inFromCl: true,
                ..Default::default()
            },
        ),
    )
    .unwrap();

    reset_opens();
    AcquireRewriteLocks(mcx, &query, true, false).unwrap();
    assert_eq!(opens(), vec![(TBL, AccessShareLock)]);
    let inner = sub.rtable.nth(0).as_range_tbl_entry().unwrap();
    assert_eq!(inner.relkind, RELKIND_RELATION);
}

#[test]
#[should_panic(expected = "INSERT/UPDATE/DELETE/MERGE rewrite")]
fn dml_rewrite_defers_loud() {
    install();
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let mut query = select1(mcx);
    query.commandType = CmdType::CMD_INSERT;
    query.resultRelation = 1;
    let _ = QueryRewrite(mcx, query);
}

#[test]
#[should_panic(expected = "WITH-clause rewrite needs CommonTableExpr")]
fn with_clause_defers_loud() {
    install();
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let mut query = select1(mcx);
    query.cteList = NodeList::make1(mcx, Node::mk_integer(mcx, 0).unwrap()).unwrap();
    let _ = QueryRewrite(mcx, query);
}

#[test]
#[should_panic(expected = "sublink descent needs the walker's T_SubLink arm")]
fn sublinks_defer_loud() {
    install();
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let mut query = select1(mcx);
    query.hasSubLinks = true;
    let _ = QueryRewrite(mcx, query);
}

#[test]
#[should_panic(expected = "view expansion needs")]
fn view_expansion_defers_loud() {
    install();
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let mut query = select1(mcx);
    query.rtable =
        NodeList::make1(mcx, relation_rte(mcx, VIEW, RELKIND_VIEW, AccessShareLock)).unwrap();
    let _ = QueryRewrite(mcx, query);
}

#[test]
#[should_panic(expected = "row-level security needs")]
fn row_security_defers_loud() {
    install();
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let mut query = select1(mcx);
    query.rtable =
        NodeList::make1(mcx, relation_rte(mcx, RLS_TBL, RELKIND_RELATION, AccessShareLock))
            .unwrap();
    let _ = QueryRewrite(mcx, query);
}

#[test]
#[should_panic(expected = "dropped-column fixup of joinaliasvars")]
fn join_alias_fixup_defers_loud() {
    install();
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let mut query = select1(mcx);
    query.rtable = NodeList::make1(
        mcx,
        rte_node(
            mcx,
            RangeTblEntry { rtekind: RTEKind::RTE_JOIN, ..Default::default() },
        ),
    )
    .unwrap();
    let _ = AcquireRewriteLocks(mcx, &query, true, false);
}

#[test]
#[should_panic(expected = "FOR UPDATE/SHARE pushdown needs get_parse_rowmark")]
fn rowmarks_pushdown_defers_loud() {
    install();
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let sub = leak_in(alloc_in(mcx, select1(mcx)).unwrap());
    let mut query = select1(mcx);
    query.rowMarks = NodeList::make1(mcx, Node::mk_integer(mcx, 0).unwrap()).unwrap();
    query.rtable = NodeList::make1(
        mcx,
        rte_node(
            mcx,
            RangeTblEntry {
                rtekind: RTEKind::RTE_SUBQUERY,
                subquery: Some(sub),
                ..Default::default()
            },
        ),
    )
    .unwrap();
    let _ = AcquireRewriteLocks(mcx, &query, true, false);
}

#[test]
fn seam_installed_and_callable() {
    install();
    assert!(rewrite_handler_seams::query_rewrite::is_installed());
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let results = rewrite_handler_seams::query_rewrite::call(mcx, select1(mcx)).unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].queryId, 42);
}
