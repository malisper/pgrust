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
use types_nodes::primnodes::{CoercionForm, FuncExpr, Var};
use types_rel::{
    AccessShareLock, FormData_pg_class, LockInfoData, LockRelId, NoLock, Relation, RelationData,
    RowExclusiveLock, RowShareLock, LOCKMODE, RELKIND_MATVIEW, RELKIND_RELATION, RELKIND_VIEW,
    REPLICA_IDENTITY_DEFAULT,
};
use types_tuple::{NameData, TupleDescData};

use crate::{rte_of, AcquireRewriteLocks, QueryRewrite};

const TBL: Oid = 1;
const VIEW: Oid = 2;
const RLS_TBL: Oid = 3;
const MATVIEW: Oid = 4;
const SELF_VIEW: Oid = 5;
const RLS_REC: Oid = 6;

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
        RLS_REC => Some(("rls_rec", RELKIND_RELATION, true)),
        MATVIEW => Some(("mv", RELKIND_MATVIEW, false)),
        SELF_VIEW => Some(("self_vw", RELKIND_VIEW, false)),
        _ => None,
    }
}

// Shape of a live-PG-18.3-captured ev_action (see readfuncs tests for the
// verbatim capture) for a one-column "SELECT a FROM <rel>" rule body.
fn ev_action(relid: Oid) -> String {
    format!(
        r#"({{QUERY :commandType 1 :querySource 0 :canSetTag true :utilityStmt <> :resultRelation 0 :hasAggs false :hasWindowFuncs false :hasTargetSRFs false :hasSubLinks false :hasDistinctOn false :hasRecursive false :hasModifyingCTE false :hasForUpdate false :hasRowSecurity false :hasGroupRTE false :isReturn false :cteList <> :rtable ({{RANGETBLENTRY :alias <> :eref {{ALIAS :aliasname t :colnames ("a")}} :rtekind 0 :relid {relid} :inh true :relkind r :rellockmode 1 :perminfoindex 1 :tablesample <> :lateral false :inFromCl true :securityQuals <>}}) :rteperminfos ({{RTEPERMISSIONINFO :relid {relid} :inh true :requiredPerms 2 :checkAsUser 0 :selectedCols (b 8) :insertedCols (b) :updatedCols (b)}}) :jointree {{FROMEXPR :fromlist ({{RANGETBLREF :rtindex 1}}) :quals <>}} :mergeActionList <> :mergeTargetRelation 0 :mergeJoinCondition <> :targetList ({{TARGETENTRY :expr {{VAR :varno 1 :varattno 1 :vartype 23 :vartypmod -1 :varcollid 0 :varnullingrels (b) :varlevelsup 0 :varreturningtype 0 :varnosyn 1 :varattnosyn 1 :location -1}} :resno 1 :resname a :ressortgroupref 0 :resorigtbl {relid} :resorigcol 1 :resjunk false}}) :override 0 :onConflict <> :returningOldAlias <> :returningNewAlias <> :returningList <> :groupClause <> :groupDistinct false :groupingSets <> :havingQual <> :windowClause <> :distinctClause <> :sortClause <> :limitOffset <> :limitCount <> :limitOption 0 :rowMarks <> :setOperations <> :constraintDeps <> :withCheckOptions <> :stmt_location -1 :stmt_len -1}})"#
    )
}

fn fake_scan_pg_rewrite<'mcx>(
    mcx: Mcx<'mcx>,
    ev_class: Oid,
) -> PgResult<PgVec<'mcx, relcache_build_seams::PgRewriteRuleShape<'mcx>>> {
    let mut rows = mcx::vec_with_capacity_in(mcx, 1)?;
    let body: Option<Oid> = match ev_class {
        VIEW => Some(TBL),
        SELF_VIEW => Some(SELF_VIEW),
        _ => None,
    };
    if let Some(base) = body {
        rows.push(relcache_build_seams::PgRewriteRuleShape {
            rule_id: 30000 + ev_class,
            ev_type: b'1',
            ev_enabled: b'O',
            is_instead: true,
            ev_qual: "<>",
            ev_action: Box::leak(ev_action(base).into_boxed_str()),
        });
    }
    Ok(rows)
}

fn make<'mcx>(mcx: Mcx<'mcx>, oid: Oid, name: &str, relkind: u8, rls: bool) -> Relation<'mcx> {
    let mut relname = NameData::default();
    relname.namestrcpy(name);
    let data = RelationData { rd_locator: Default::default(), rd_smgr: Default::default(),
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
        pgstat_link: core::cell::Cell::new((0, core::ptr::null_mut())),
        rd_amcache: Default::default(),
        rd_amcache_hash: Default::default(), rd_amcache_gin: Default::default(), rd_amcache_spgist: Default::default(),
        rd_support: PgVec::new_in(mcx),
        rd_supportinfo: Default::default(),
        rd_opcoptions: Default::default(),
        rd_indexlist: Default::default(),
            rd_trigdesc: Default::default(),
            rd_hastriggers: false,
            rd_hasrules: relkind == RELKIND_VIEW,
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

fn fake_check_enable_rls(
    relid: Oid,
    _check_as_user: Oid,
    _no_error: bool,
) -> PgResult<rls_seams::CheckEnableRls> {
    Ok(if matches!(relid, RLS_TBL | RLS_REC) {
        rls_seams::CheckEnableRls::RlsEnabled
    } else {
        rls_seams::CheckEnableRls::RlsNone
    })
}

const POLICY_QUAL_A_EQ_5: &str = r#"{OPEXPR :opno 96 :opfuncid 65 :opresulttype 16 :opretset false :opcollid 0 :inputcollid 0 :args ({VAR :varno 1 :varattno 1 :vartype 23 :vartypmod -1 :varcollid 0 :varnullingrels (b) :varlevelsup 0 :varreturningtype 0 :varnosyn 1 :varattnosyn 1 :location -1} {CONST :consttype 23 :consttypmod -1 :constcollid 0 :constlen 4 :constbyval true :constisnull false :location -1 :constvalue 4 [ 5 0 0 0 0 0 0 0 ]}) :location -1}"#;

fn recursive_policy_qual() -> String {
    let action = ev_action(RLS_REC);
    let inner = &action[1..action.len() - 1];
    format!(
        "{{SUBLINK :subLinkType 0 :subLinkId 0 :testexpr <> :operName <> \
         :subselect {inner} :location -1}}"
    )
}

fn fake_scan_pg_policy<'mcx>(
    mcx: Mcx<'mcx>,
    polrelid: Oid,
) -> PgResult<PgVec<'mcx, relcache_build_seams::PgPolicyShape<'mcx>>> {
    let mut rows = mcx::vec_with_capacity_in(mcx, 1)?;
    let qual: Option<&'static str> = match polrelid {
        RLS_TBL => Some(POLICY_QUAL_A_EQ_5),
        RLS_REC => Some(Box::leak(recursive_policy_qual().into_boxed_str())),
        _ => None,
    };
    if let Some(q) = qual {
        let mut roles = mcx::vec_with_capacity_in(mcx, 1)?;
        roles.push(0);
        rows.push(relcache_build_seams::PgPolicyShape {
            polname: "p1",
            polcmd: b'*',
            polpermissive: true,
            polroles: roles.leak(),
            polqual: Some(q),
            polwithcheck: None,
        });
    }
    Ok(rows)
}

fn install() {
    static INIT: Once = Once::new();
    INIT.call_once(|| {
        relation_seams::relation_open::set(fake_relation_open);
        relcache_build_seams::scan_pg_rewrite::set(fake_scan_pg_rewrite);
        relcache_build_seams::scan_pg_policy::set(fake_scan_pg_policy);
        rls_seams::check_enable_rls::set(fake_check_enable_rls);
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
    query.jointree = Some(
        leak_in(alloc_in(mcx, types_nodes::primnodes::FromExpr {
            fromlist: NodeList::make1(mcx, Node::mk_range_tbl_ref(mcx, 1).unwrap()).unwrap(),
            quals: None,
        }).unwrap()),
    );
    let rt_ptr = query.rtable.as_slice().as_ptr();

    reset_opens();
    let results = QueryRewrite(mcx, query).unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].rtable.as_slice().as_ptr(), rt_ptr);
    // fireRIRrules: one rules probe + one RLS probe, both NoLock.
    assert_eq!(opens(), vec![(TBL, NoLock), (TBL, NoLock)]);
}

#[test]
fn unreferenced_rte_skips_rules_probe() {
    install();
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let mut query = select1(mcx);
    query.rtable = NodeList::make1(mcx, relation_rte(mcx, TBL, RELKIND_RELATION, AccessShareLock))
        .unwrap();

    reset_opens();
    let results = QueryRewrite(mcx, query).unwrap();
    assert_eq!(results.len(), 1);
    // rangeTableEntry_used = false: only the RLS probe remains.
    assert_eq!(opens(), vec![(TBL, NoLock)]);
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
fn merge_rewrite_plain_table_passes() {
    install();
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let mut query = select1(mcx);
    query.commandType = CmdType::CMD_MERGE;
    query.resultRelation = 1;
    query.rtable =
        NodeList::make1(mcx, relation_rte(mcx, TBL, RELKIND_RELATION, RowExclusiveLock))
            .unwrap();
    let out = QueryRewrite(mcx, query).unwrap();
    assert_eq!(out.len(), 1);
}

#[test]
fn select_cte_passes_rewrite_unchanged() {
    install();
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let mut query = select1(mcx);
    let cte = types_nodes::parsenodes::CommonTableExpr {
        ctename: Some("x"),
        ctequery: Some(Node::mk(mcx, select1(mcx)).unwrap()),
        ..Default::default()
    };
    query.cteList = NodeList::make1(mcx, Node::mk(mcx, cte).unwrap()).unwrap();
    let out = QueryRewrite(mcx, query).unwrap();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0].cteList.len(), 1);
}

#[test]
fn sublink_subselect_gets_rir_descent() {
    install();
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let mut query = select1(mcx);
    let sublink = types_nodes::primnodes::SubLink {
        subLinkType: types_nodes::primnodes::SubLinkType::EXPR_SUBLINK,
        subLinkId: 0,
        testexpr: None,
        operName: NodeList::nil(),
        subselect: Node::mk(mcx, select1(mcx)).unwrap(),
        location: -1,
    };
    let te = Node::mk_target_entry(mcx, Node::mk(mcx, sublink).unwrap(), 1, None, false).unwrap();
    query.targetList = NodeList::make1(mcx, te).unwrap();
    query.hasSubLinks = true;
    let out = QueryRewrite(mcx, query).unwrap();
    assert_eq!(out.len(), 1);
}

fn view_query<'mcx>(mcx: Mcx<'mcx>, view_oid: Oid) -> Query<'mcx> {
    let var = Node::mk(
        mcx,
        types_nodes::primnodes::Var { varno: 1, varattno: 1, vartype: 23, ..Default::default() },
    )
    .unwrap();
    let te = Node::mk_target_entry(mcx, var, 1, Some("a"), false).unwrap();
    let mut colnames = NodeList::nil();
    colnames.lappend(mcx, Node::mk_string(mcx, "a").unwrap()).unwrap();
    let eref = leak_in(
        alloc_in(mcx, types_nodes::primnodes::Alias { aliasname: Some("v"), colnames }).unwrap(),
    );
    let rte = rte_node(
        mcx,
        RangeTblEntry {
            rtekind: RTEKind::RTE_RELATION,
            relid: view_oid,
            relkind: RELKIND_VIEW,
            rellockmode: AccessShareLock,
            perminfoindex: 0,
            eref: Some(eref),
            inFromCl: true,
            ..Default::default()
        },
    );
    let mut query = select1(mcx);
    query.targetList = NodeList::make1(mcx, te).unwrap();
    query.rtable = NodeList::make1(mcx, rte).unwrap();
    query.jointree = Some(leak_in(
        alloc_in(
            mcx,
            types_nodes::primnodes::FromExpr {
                fromlist: NodeList::make1(mcx, Node::mk_range_tbl_ref(mcx, 1).unwrap()).unwrap(),
                quals: None,
            },
        )
        .unwrap(),
    ));
    query
}

#[test]
fn view_select_expands_to_subquery_rte() {
    install();
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let query = view_query(mcx, VIEW);

    reset_opens();
    let results = QueryRewrite(mcx, query).unwrap();
    assert_eq!(results.len(), 1);
    let q = &results[0];

    let rte = q.rtable.nth(0).as_range_tbl_entry().unwrap();
    assert_eq!(rte.rtekind, RTEKind::RTE_SUBQUERY);
    // relid/relkind/rellockmode/perminfoindex survive for executor lock+ACL.
    assert_eq!(rte.relid, VIEW);
    assert_eq!(rte.relkind, RELKIND_VIEW);
    assert_eq!(rte.rellockmode, AccessShareLock);
    assert!(!rte.inh && !rte.security_barrier && rte.tablesample.is_none());

    let sub = rte.subquery.expect("expanded view rule query");
    assert_eq!(sub.commandType, CmdType::CMD_SELECT);
    assert_eq!(sub.rtable.len(), 1);
    let base = sub.rtable.nth(0).as_range_tbl_entry().unwrap();
    assert_eq!(base.rtekind, RTEKind::RTE_RELATION);
    assert_eq!(base.relid, TBL);
    assert_eq!(base.relkind, RELKIND_RELATION);
    assert_eq!(sub.targetList.len(), 1);
    let sub_var = sub.targetList.nth(0).as_target_entry().unwrap().expr.as_var().unwrap();
    assert_eq!((sub_var.varno, sub_var.varattno, sub_var.vartype), (1, 1, 23));

    // setRuleCheckAsUser: view owner (fake relowner = 10) on all perminfos.
    let p = sub.rteperminfos.nth(0).as_rte_permission_info().unwrap();
    assert_eq!(p.checkAsUser, 10);

    // view probe, base-table lock (AcquireRewriteLocks, rellockmode), then
    // the recursion's rules + RLS probes on the base table.
    assert_eq!(
        opens(),
        vec![(VIEW, NoLock), (TBL, AccessShareLock), (TBL, NoLock), (TBL, NoLock)]
    );
}

#[test]
fn self_referential_view_reports_infinite_recursion() {
    install();
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let query = view_query(mcx, SELF_VIEW);

    let Err(err) = QueryRewrite(mcx, query) else {
        panic!("self-referential view must fail")
    };
    assert!(err
        .message()
        .contains("infinite recursion detected in rules for relation \"self_vw\""));
}

fn rls_query<'mcx>(mcx: Mcx<'mcx>, relid: Oid) -> Query<'mcx> {
    let mut query = select1(mcx);
    query.rtable = NodeList::make1(
        mcx,
        rte_node(
            mcx,
            RangeTblEntry {
                rtekind: RTEKind::RTE_RELATION,
                relid,
                relkind: RELKIND_RELATION,
                rellockmode: AccessShareLock,
                perminfoindex: 1,
                inFromCl: true,
                ..Default::default()
            },
        ),
    )
    .unwrap();
    query.rteperminfos = NodeList::make1(
        mcx,
        Node::mk(
            mcx,
            types_nodes::parsenodes::RTEPermissionInfo {
                relid,
                inh: true,
                requiredPerms: 2,
                checkAsUser: 10,
                ..Default::default()
            },
        )
        .unwrap(),
    )
    .unwrap();
    query.jointree = Some(leak_in(
        alloc_in(
            mcx,
            types_nodes::primnodes::FromExpr {
                fromlist: NodeList::make1(mcx, Node::mk_range_tbl_ref(mcx, 1).unwrap()).unwrap(),
                quals: None,
            },
        )
        .unwrap(),
    ));
    query
}

#[test]
fn row_security_applies_policy_quals() {
    install();
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let query = rls_query(mcx, RLS_TBL);

    let results = QueryRewrite(mcx, query).unwrap();
    assert_eq!(results.len(), 1);
    let q = &results[0];
    assert!(q.hasRowSecurity);
    assert!(!q.hasSubLinks);
    let rte = rte_of(q.rtable.nth(0));
    assert_eq!(rte.securityQuals.len(), 1);
    assert_eq!(rte.securityQuals.nth(0).node_tag(), types_nodes::NodeTag::T_OpExpr);
    assert!(q.withCheckOptions.is_nil());
}

#[test]
fn self_referential_policy_reports_infinite_recursion() {
    install();
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let query = rls_query(mcx, RLS_REC);

    let Err(err) = QueryRewrite(mcx, query) else {
        panic!("self-referential policy must fail")
    };
    assert!(err
        .message()
        .contains("infinite recursion detected in policy for relation \"rls_rec\""));
}

fn join_query<'mcx>(mcx: Mcx<'mcx>, aliasvars: NodeList<'mcx>) -> Query<'mcx> {
    let values = rte_node(
        mcx,
        RangeTblEntry { rtekind: RTEKind::RTE_VALUES, inFromCl: true, ..Default::default() },
    );
    let join = rte_node(
        mcx,
        RangeTblEntry {
            rtekind: RTEKind::RTE_JOIN,
            joinaliasvars: aliasvars,
            ..Default::default()
        },
    );
    let mut query = select1(mcx);
    query.rtable = NodeList::make2(mcx, values, join).unwrap();
    query
}

fn alias_var<'mcx>(mcx: Mcx<'mcx>, varno: i32, varattno: i16) -> Node<'mcx> {
    Node::mk(mcx, Var { varno, varattno, vartype: 23, vartypmod: -1, ..Default::default() })
        .unwrap()
}

#[test]
fn acquire_locks_join_rte_scans_aliasvars() {
    install();
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let coerced = Node::mk(
        mcx,
        FuncExpr {
            funcid: 480,
            funcresulttype: 20,
            funcformat: CoercionForm::COERCE_IMPLICIT_CAST,
            args: NodeList::make1(mcx, alias_var(mcx, 1, 2)).unwrap(),
            ..Default::default()
        },
    )
    .unwrap();
    let aliasvars = NodeList::make2(mcx, alias_var(mcx, 1, 1), coerced).unwrap();
    let query = join_query(mcx, aliasvars);
    AcquireRewriteLocks(mcx, &query, true, false).unwrap();
    assert_eq!(rte_of(query.rtable.nth(1)).joinaliasvars.len(), 2);
}

#[test]
fn join_rte_forward_varno_is_internal_error() {
    install();
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let aliasvars = NodeList::make1(mcx, alias_var(mcx, 2, 1)).unwrap();
    let query = join_query(mcx, aliasvars);
    let err = AcquireRewriteLocks(mcx, &query, true, false).unwrap_err();
    assert!(err.message.contains("unexpected varno 2 in JOIN RTE 2"));
}

#[test]
fn rowmarks_pushdown_recurses_quietly() {
    install();
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let sub = leak_in(alloc_in(mcx, select1(mcx)).unwrap());
    let mut query = select1(mcx);
    query.rowMarks = NodeList::make1(
        mcx,
        Node::mk(
            mcx,
            types_nodes::parsenodes::RowMarkClause {
                rti: 1,
                strength: types_nodes::LockClauseStrength::LCS_FORUPDATE,
                waitPolicy: types_nodes::LockWaitPolicy::LockWaitBlock,
                pushedDown: false,
            },
        )
        .unwrap(),
    )
    .unwrap();
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
    AcquireRewriteLocks(mcx, &query, true, false).unwrap();
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
