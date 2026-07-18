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

// Captured from live PostgreSQL 18.3 initdb:
// SELECT ev_action FROM pg_rewrite WHERE ev_class = 'pg_stat_activity'::regclass.
pub const EV_ACTION_PG_STAT_ACTIVITY: &str = include_str!("fixtures/pg_stat_activity.ev_action");

#[test]
fn reads_pg_stat_activity_view_rule() {
    use types_nodes::jointype::JoinType;

    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let node = stringToNode(mcx, EV_ACTION_PG_STAT_ACTIVITY).unwrap();
    let actions = node.as_list().expect("ev_action is a List");
    assert_eq!(actions.len(), 1);
    let q = actions.nth(0).as_query().expect("rule action is a Query");

    assert_eq!(q.commandType, CmdType::CMD_SELECT);
    assert_eq!(q.rtable.len(), 5);
    let kinds: Vec<RTEKind> = q
        .rtable
        .iter()
        .map(|n| n.as_range_tbl_entry().unwrap().rtekind)
        .collect();
    assert_eq!(
        kinds,
        [
            RTEKind::RTE_FUNCTION,
            RTEKind::RTE_RELATION,
            RTEKind::RTE_JOIN,
            RTEKind::RTE_RELATION,
            RTEKind::RTE_JOIN
        ]
    );

    let func_rte = q.rtable.nth(0).as_range_tbl_entry().unwrap();
    assert!(!func_rte.funcordinality);
    assert_eq!(func_rte.functions.len(), 1);
    let rtf = func_rte.functions.nth(0).as_range_tbl_function().unwrap();
    assert_eq!(rtf.funccolcount, 31);
    assert!(rtf.funccolnames.is_nil() && rtf.funccoltypes.is_nil());
    assert!(rtf.funccoltypmods.is_nil() && rtf.funccolcollations.is_nil());
    assert!(rtf.funcparams.is_empty());
    let fe = rtf.funcexpr.expect("funcexpr").as_func_expr().unwrap();
    assert_eq!(fe.funcid, 2022);
    assert!(fe.funcretset);
    assert_eq!(fe.args.len(), 1);
    assert!(fe.args.nth(0).as_const().unwrap().constisnull);

    assert_eq!(q.rtable.nth(1).as_range_tbl_entry().unwrap().relid, 1262);
    assert_eq!(q.rtable.nth(3).as_range_tbl_entry().unwrap().relid, 1260);

    let j3 = q.rtable.nth(2).as_range_tbl_entry().unwrap();
    assert_eq!(j3.jointype, JoinType::JOIN_LEFT);
    assert_eq!(j3.joinmergedcols, 0);
    assert_eq!(j3.joinaliasvars.len(), 49);
    assert_eq!(j3.joinleftcols.len(), 31);
    assert_eq!(j3.joinrightcols.len(), 18);
    assert!(j3.join_using_alias.is_none());
    let av = j3.joinaliasvars.nth(0).as_var().unwrap();
    assert_eq!((av.varno, av.varattno), (1, 1));

    let j5 = q.rtable.nth(4).as_range_tbl_entry().unwrap();
    assert_eq!(j5.jointype, JoinType::JOIN_LEFT);
    assert_eq!(j5.joinaliasvars.len(), 61);
    assert_eq!(j5.joinleftcols.len(), 49);
    assert_eq!(j5.joinrightcols.len(), 12);

    assert_eq!(q.rteperminfos.len(), 2);

    let jt = q.jointree.expect("jointree");
    assert_eq!(jt.fromlist.len(), 1);
    let outer = jt.fromlist.nth(0).as_join_expr().expect("outer JoinExpr");
    assert_eq!(outer.jointype, JoinType::JOIN_LEFT);
    assert!(!outer.isNatural);
    assert_eq!(outer.rtindex, 5);
    assert!(outer.usingClause.is_nil() && outer.join_using_alias.is_none());
    assert!(outer.alias.is_none());
    assert!(outer.quals.expect("outer join quals").as_op_expr().is_some());
    let inner = outer.larg.as_join_expr().expect("inner JoinExpr");
    assert_eq!(inner.jointype, JoinType::JOIN_LEFT);
    assert_eq!(inner.rtindex, 3);
    assert_eq!(inner.larg.as_range_tbl_ref().unwrap().rtindex, 1);
    assert_eq!(inner.rarg.as_range_tbl_ref().unwrap().rtindex, 2);
    assert_eq!(inner.quals.expect("inner join quals").as_op_expr().unwrap().opno, 607);
    assert_eq!(outer.rarg.as_range_tbl_ref().unwrap().rtindex, 4);

    assert_eq!(q.targetList.len(), 22);
    let te = q.targetList.nth(21).as_target_entry().unwrap();
    assert_eq!(te.resname, Some("backend_type"));
    let v = te.expr.as_var().unwrap();
    assert_eq!((v.varno, v.varattno, v.vartype), (1, 18, 25));
    assert!(q
        .targetList
        .iter()
        .flat_map(|te| te.as_target_entry().unwrap().expr.as_var())
        .any(|v| !v.varnullingrels.is_empty()));
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
    let _ = stringToNode(ctx.mcx(), "{PLANNEDSTMT :commandType 1}");
}

#[test]
fn rte_values_roundtrips() {
    let ctx = MemoryContext::new("t");
    let n = stringToNode(
        ctx.mcx(),
        "{RANGETBLENTRY :alias <> :eref {ALIAS :aliasname *VALUES* :colnames (\"column1\")} \
         :rtekind 5 :values_lists (({CONST :consttype 23 :consttypmod -1 :constcollid 0 \
         :constlen 4 :constbyval true :constisnull false :location -1 \
         :constvalue 4 [ 7 0 0 0 0 0 0 0 ]})) :coltypes (o 23) :coltypmods (i -1) \
         :colcollations (o 0) :lateral false :inFromCl true :securityQuals <>}",
    )
    .expect("VALUES RTE reads");
    let rte = n.as_range_tbl_entry().expect("RangeTblEntry");
    assert_eq!(rte.values_lists.len(), 1);
    assert_eq!(rte.coltypes.nth(0), 23);
    assert_eq!(rte.coltypmods.nth(0), -1);
}

#[test]
fn coerce_to_domain_value_conbin() {
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let s = "{OPEXPR :opno 521 :opfuncid 147 :opresulttype 16 :opretset false \
             :opcollid 0 :inputcollid 0 :args ({COERCETODOMAINVALUE :typeId 23 \
             :typeMod -1 :collation 0 :location 47} {CONST :consttype 23 \
             :consttypmod -1 :constcollid 0 :constlen 4 :constbyval true \
             :constisnull false :location 55 :constvalue 4 [ 0 0 0 0 0 0 0 0 ]}) \
             :location 53}";
    let n = stringToNode(mcx, s).unwrap();
    let op = n.as_op_expr().unwrap();
    assert_eq!((op.opno, op.opfuncid), (521, 147));
    let dv = op.args.nth(0).as_coerce_to_domain_value().unwrap();
    assert_eq!((dv.typeId, dv.typeMod, dv.collation, dv.location), (23, -1, 0, -1));
}

#[test]
fn coerce_to_domain_node() {
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let s = "{COERCETODOMAIN :arg {CONST :consttype 23 :consttypmod -1 \
             :constcollid 0 :constlen 4 :constbyval true :constisnull false \
             :location -1 :constvalue 4 [ 5 0 0 0 0 0 0 0 ]} :resulttype 90001 \
             :resulttypmod -1 :resultcollid 0 :coercionformat 2 :location -1}";
    let n = stringToNode(mcx, s).unwrap();
    let cd = n.as_coerce_to_domain().unwrap();
    assert_eq!((cd.resulttype, cd.resulttypmod), (90001, -1));
    assert_eq!(cd.arg.as_const().unwrap().constvalue.as_u64(), 5);
}

#[test]
fn merge_in_cte_ev_action() {
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let s = include_str!("../fixtures/merge_cte_ev_action.txt").trim();
    let n = stringToNode(mcx, s).unwrap();
    let q = n.as_list().unwrap().nth(0).as_query().unwrap();
    let cte = q.cteList.nth(0).as_common_table_expr().unwrap();
    let mq = cte.ctequery.unwrap().as_query().unwrap();
    assert_eq!(mq.commandType, types_nodes::nodes_enums::CmdType::CMD_MERGE);
    assert!(mq.mergeJoinCondition.is_some());
    let kinds: Vec<_> = mq
        .mergeActionList
        .iter()
        .map(|a| a.as_merge_action().unwrap().commandType)
        .collect();
    assert_eq!(
        kinds,
        [
            types_nodes::nodes_enums::CmdType::CMD_UPDATE,
            types_nodes::nodes_enums::CmdType::CMD_INSERT
        ]
    );
    assert!(!mq.returningList.is_nil());
}

#[test]
fn search_cycle_ev_action() {
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let s = include_str!("../fixtures/search_cycle_ev_action.txt").trim();
    let n = stringToNode(mcx, s).unwrap();
    let q = n.as_list().unwrap().nth(0).as_query().unwrap();
    let cte = q.cteList.nth(0).as_common_table_expr().unwrap();
    let sc = cte.search_clause.unwrap().as_cte_search_clause().unwrap();
    assert!(!sc.search_breadth_first);
    assert_eq!(sc.search_seq_column, Some("ord"));
    assert_eq!(sc.search_col_list.len(), 1);
    let cc = cte.cycle_clause.unwrap().as_cte_cycle_clause().unwrap();
    assert_eq!(cc.cycle_mark_column, Some("is_c"));
    assert_eq!(cc.cycle_path_column, Some("pth"));
    assert!(cc.cycle_mark_value.unwrap().as_const().unwrap().constvalue.as_bool());
    assert_eq!(cc.cycle_mark_type, 16);
}

// C nodeToString(list_make1(NIL)) — the stored prosqlbody of an empty
// BEGIN ATOMIC body — reads back as a list holding one empty list.
#[test]
fn reads_nil_list_element_as_empty_list() {
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let node = stringToNode(mcx, "(<>)").unwrap();
    let outer = node.as_list().expect("outer List");
    assert_eq!(outer.len(), 1);
    let inner = outer.nth(0).as_list().expect("NIL element reads as a List");
    assert!(inner.is_nil());
}

// planagg's minmax probe round-trips the post-expansion Query: GROUP BY ()
// serializes :groupingSets (<>) — a NIL member inside a node-list field.
#[test]
fn reads_nil_element_in_node_list_field() {
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let s = EV_ACTION_V.replace(":groupingSets <>", ":groupingSets ((i 1) <>)");
    let node = stringToNode(mcx, &s).unwrap();
    let q = node.as_list().unwrap().nth(0).as_query().unwrap();
    assert_eq!(q.groupingSets.len(), 2);
    assert_eq!(q.groupingSets.nth(0).as_int_list().unwrap().iter().collect::<Vec<_>>(), [1]);
    let empty = q.groupingSets.nth(1).as_list().expect("NIL member reads as a List");
    assert!(empty.is_nil());
}

// stringToNode("<>") is C's NULL return (read.c OTHER_TOKEN, tok_len == 0):
// pg_rewrite.ev_qual holds the bare marker on every unconditional rule, and
// pg_get_expr reads it straight from SQL (public issue #18).
#[test]
fn bare_null_marker_reads_as_none() {
    let ctx = MemoryContext::new("t");
    assert!(crate::stringToNodeNullable(ctx.mcx(), "<>").unwrap().is_none());
}

// The non-null entry keeps the loud panic for columns that never hold "<>"
// (their C readers dereference the NULL unconditionally).
#[test]
#[should_panic(expected = "null node")]
fn nonnull_entry_panics_on_bare_null_marker() {
    let ctx = MemoryContext::new("t");
    let _ = stringToNode(ctx.mcx(), "<>");
}
