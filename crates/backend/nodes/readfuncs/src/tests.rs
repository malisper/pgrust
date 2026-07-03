use mcx::MemoryContext;
use types_nodes::nodes_enums::CmdType;
use types_nodes::parsenodes::{QuerySource, RTEKind};

use crate::stringToNode;

// Captured from live PostgreSQL 18.3: CREATE TABLE t(a int, b text);
// CREATE VIEW v AS SELECT a, b FROM t; SELECT ev_action FROM pg_rewrite.
pub const EV_ACTION_V: &str = r#"({QUERY :commandType 1 :querySource 0 :canSetTag true :utilityStmt <> :resultRelation 0 :hasAggs false :hasWindowFuncs false :hasTargetSRFs false :hasSubLinks false :hasDistinctOn false :hasRecursive false :hasModifyingCTE false :hasForUpdate false :hasRowSecurity false :hasGroupRTE false :isReturn false :cteList <> :rtable ({RANGETBLENTRY :alias <> :eref {ALIAS :aliasname t :colnames ("a" "b")} :rtekind 0 :relid 16384 :inh true :relkind r :rellockmode 1 :perminfoindex 1 :tablesample <> :lateral false :inFromCl true :securityQuals <>}) :rteperminfos ({RTEPERMISSIONINFO :relid 16384 :inh true :requiredPerms 2 :checkAsUser 0 :selectedCols (b 8 9) :insertedCols (b) :updatedCols (b)}) :jointree {FROMEXPR :fromlist ({RANGETBLREF :rtindex 1}) :quals <>} :mergeActionList <> :mergeTargetRelation 0 :mergeJoinCondition <> :targetList ({TARGETENTRY :expr {VAR :varno 1 :varattno 1 :vartype 23 :vartypmod -1 :varcollid 0 :varnullingrels (b) :varlevelsup 0 :varreturningtype 0 :varnosyn 1 :varattnosyn 1 :location -1} :resno 1 :resname a :ressortgroupref 0 :resorigtbl 16384 :resorigcol 1 :resjunk false} {TARGETENTRY :expr {VAR :varno 1 :varattno 2 :vartype 25 :vartypmod -1 :varcollid 100 :varnullingrels (b) :varlevelsup 0 :varreturningtype 0 :varnosyn 1 :varattnosyn 2 :location -1} :resno 2 :resname b :ressortgroupref 0 :resorigtbl 16384 :resorigcol 2 :resjunk false}) :override 0 :onConflict <> :returningOldAlias <> :returningNewAlias <> :returningList <> :groupClause <> :groupDistinct false :groupingSets <> :havingQual <> :windowClause <> :distinctClause <> :sortClause <> :limitOffset <> :limitCount <> :limitOption 0 :rowMarks <> :setOperations <> :constraintDeps <> :withCheckOptions <> :stmt_location -1 :stmt_len -1})"#;

#[test]
fn reads_live_captured_view_rule() {
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let node = stringToNode(mcx, EV_ACTION_V).unwrap();
    let actions = node.as_list().expect("ev_action is a List");
    assert_eq!(actions.len(), 1);
    let q = actions.nth(0).as_query().expect("rule action is a Query");

    assert_eq!(q.commandType, CmdType::CMD_SELECT);
    assert_eq!(q.querySource, QuerySource::QSRC_ORIGINAL);
    assert_eq!(q.queryId, 0);
    assert!(q.canSetTag);
    assert_eq!(q.resultRelation, 0);
    assert!(!q.hasAggs && !q.hasSubLinks && !q.hasForUpdate && !q.hasRowSecurity);
    assert!(q.cteList.is_nil() && q.groupClause.is_nil() && q.sortClause.is_nil());
    assert!(q.limitOffset.is_none() && q.limitCount.is_none() && q.setOperations.is_none());
    assert_eq!(q.stmt_location, -1);

    assert_eq!(q.rtable.len(), 1);
    let rte = q.rtable.nth(0).as_range_tbl_entry().unwrap();
    assert_eq!(rte.rtekind, RTEKind::RTE_RELATION);
    assert_eq!(rte.relid, 16384);
    assert!(rte.inh);
    assert_eq!(rte.relkind, b'r');
    assert_eq!(rte.rellockmode, 1);
    assert_eq!(rte.perminfoindex, 1);
    assert!(rte.alias.is_none() && rte.tablesample.is_none());
    assert!(!rte.lateral && rte.inFromCl);
    let eref = rte.eref.expect("eref");
    assert_eq!(eref.aliasname, Some("t"));
    assert_eq!(eref.colnames.len(), 2);
    assert_eq!(eref.colnames.nth(0).as_string().unwrap().sval, "a");
    assert_eq!(eref.colnames.nth(1).as_string().unwrap().sval, "b");

    assert_eq!(q.rteperminfos.len(), 1);
    let p = q.rteperminfos.nth(0).as_rte_permission_info().unwrap();
    assert_eq!(p.relid, 16384);
    assert!(p.inh);
    assert_eq!(p.requiredPerms, 2);
    assert_eq!(p.checkAsUser, 0);
    assert!(p.selectedCols.is_member(8) && p.selectedCols.is_member(9));
    assert_eq!(p.selectedCols.num_members(), 2);
    assert!(p.insertedCols.is_empty() && p.updatedCols.is_empty());

    let jt = q.jointree.expect("jointree");
    assert_eq!(jt.fromlist.len(), 1);
    assert_eq!(jt.fromlist.nth(0).as_range_tbl_ref().unwrap().rtindex, 1);
    assert!(jt.quals.is_none());

    assert_eq!(q.targetList.len(), 2);
    let te0 = q.targetList.nth(0).as_target_entry().unwrap();
    assert_eq!(te0.resno, 1);
    assert_eq!(te0.resname, Some("a"));
    assert_eq!((te0.resorigtbl, te0.resorigcol), (16384, 1));
    assert!(!te0.resjunk);
    let v0 = te0.expr.as_var().unwrap();
    assert_eq!((v0.varno, v0.varattno, v0.vartype, v0.vartypmod), (1, 1, 23, -1));
    assert_eq!(v0.varcollid, 0);
    assert!(v0.varnullingrels.is_empty());
    assert_eq!(v0.varlevelsup, 0);
    assert_eq!((v0.varnosyn, v0.varattnosyn), (1, 1));
    assert_eq!(v0.location, -1);
    let v1 = q.targetList.nth(1).as_target_entry().unwrap().expr.as_var().unwrap();
    assert_eq!((v1.varno, v1.varattno, v1.vartype, v1.varcollid), (1, 2, 25, 100));
}

#[test]
fn reads_const_with_byval_datum() {
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    // outfuncs shape of (Const int4 5), captured format per _outConst/_outDatum.
    let s = "{CONST :consttype 23 :consttypmod -1 :constcollid 0 :constlen 4 \
             :constbyval true :constisnull false :location 12 :constvalue 4 [ 5 0 0 0 0 0 0 0 ]}";
    let node = stringToNode(mcx, s).unwrap();
    let c = node.as_const().unwrap();
    assert_eq!((c.consttype, c.consttypmod, c.constlen), (23, -1, 4));
    assert!(c.constbyval && !c.constisnull);
    assert_eq!(c.location, -1);
    assert_eq!(c.constvalue.as_u64(), 5);
}

#[test]
fn null_const_and_escaped_strings() {
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let s = "{CONST :consttype 25 :consttypmod -1 :constcollid 100 :constlen -1 \
             :constbyval false :constisnull true :location -1 :constvalue <>}";
    let c = stringToNode(mcx, s).unwrap();
    assert!(c.as_const().unwrap().constisnull);

    let s = r#"{ALIAS :aliasname my\ table :colnames ("col\"x" "")}"#;
    let a = stringToNode(mcx, s).unwrap();
    let a = a.as_alias().unwrap();
    assert_eq!(a.aliasname, Some("my table"));
    assert_eq!(a.colnames.nth(0).as_string().unwrap().sval, "col\"x");
    assert_eq!(a.colnames.nth(1).as_string().unwrap().sval, "");
}

#[test]
#[should_panic(expected = "read arm unported")]
fn unknown_node_label_is_loud() {
    let ctx = MemoryContext::new("t");
    let _ = stringToNode(ctx.mcx(), "{WINDOWCLAUSE :name <>}");
}

#[test]
#[should_panic(expected = "arm unported (view SELECT-rule set)")]
fn rte_values_arm_is_loud() {
    let ctx = MemoryContext::new("t");
    let _ = stringToNode(
        ctx.mcx(),
        "{RANGETBLENTRY :alias <> :eref <> :rtekind 5 :values_lists <>}",
    );
}
