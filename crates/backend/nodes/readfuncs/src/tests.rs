use mcx::MemoryContext;
use types_error::{PgError, ERRCODE_FEATURE_NOT_SUPPORTED, ERRCODE_INTERNAL_ERROR};
use types_nodes::nodes_enums::CmdType;
use types_nodes::parsenodes::{QuerySource, RTEKind};
use types_nodes::NodeTag;

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

// An unported read arm is a typed refusal (ERRCODE_FEATURE_NOT_SUPPORTED)
// naming the arm — never a panic, never disguised as C's malformed-string
// message; a token that is not even shaped like a C node label takes
// parseNodeString's elog verbatim (readfuncs.c:587 `"%.32s"` over the
// un-NUL-terminated pg_strtok token, i.e. the next 32 bytes of input).
#[test]
fn unknown_node_label_is_a_typed_refusal() {
    for text in ["{PLANNEDSTMT :commandType 1}", "{UNRECOGNIZED_NODE}"] {
        let e = err_of(text);
        assert_eq!(e.sqlstate(), ERRCODE_FEATURE_NOT_SUPPORTED, "{text:?}");
        assert!(e.message().contains("read arm unported"), "{text:?}: {}", e.message());
    }
    let e = err_of("{foo bar}");
    assert_eq!(e.sqlstate(), ERRCODE_INTERNAL_ERROR);
    assert_eq!(e.message(), "badly formatted node string \"foo bar}\"...");
    let e = err_of("{abcdefghijklmnopqrstuvwxyz0123456789 :x 1}");
    assert_eq!(e.message(), "badly formatted node string \"abcdefghijklmnopqrstuvwxyz012345\"...");
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

// ---- lane p1-nodes fix witnesses (nodesfam_diff differential findings) ----

// C PARITY: stringToNode("") runs nodeRead(NULL,0); pg_strtok returns NULL
// immediately and the result is NULL — C treats empty/whitespace node text as
// the NULL node, exactly like "<>". The port used to panic
// ("stringToNode: empty input").
#[test]
fn empty_and_whitespace_input_is_the_null_node() {
    for text in ["", " ", "\t", "\n", "   \t\n  "] {
        let ctx = MemoryContext::new("t");
        assert!(
            crate::stringToNodeNullable(ctx.mcx(), text).unwrap().is_none(),
            "{text:?} should read as the NULL node (C parity)"
        );
    }
}

// DATA-CORRUPTION FIX: the value-node path used `parse_int(t) as i32`, which
// silently WRAPPED — the token `9992999999` built an Integer node holding
// 1403065407 where C builds a Float node printing "9992999999". The port now
// applies C nodeTokenType's own rule: strtoint over the UNSIGNED magnitude
// (so INT32_MIN's magnitude ERANGEs exactly as in C), everything else numeric
// is a T_Float value node, outside this crate's charter -> loud panic.
#[test]
fn in_range_integer_tokens_read_as_integer_nodes() {
    for t in ["0", "-1", "2147483647", "-2147483647"] {
        let ctx = MemoryContext::new("t");
        let m = ctx.mcx();
        let n = crate::stringToNodeNullable(m, t).unwrap().unwrap();
        assert_eq!(n.node_tag(), types_nodes::NodeTag::T_Integer, "{t:?}");
        assert_eq!(
            n.as_integer().unwrap().ival,
            t.parse::<i32>().unwrap(),
            "{t:?} must read to its exact value, never a wrapped one"
        );
    }
}

#[test]
fn out_of_range_integer_token_does_not_wrap() {
    let ctx = MemoryContext::new("t");
    // used to silently build Integer(1403065407); C builds Float "9992999999"
    let n = crate::stringToNodeNullable(ctx.mcx(), "9992999999").unwrap().unwrap();
    assert_eq!(n.as_float().expect("T_Float").fval, "9992999999");
}

#[test]
fn int32_min_token_follows_cs_magnitude_rule() {
    let ctx = MemoryContext::new("t");
    // C's nodeTokenType strips the sign first, so INT32_MIN's magnitude
    // ERANGEs and C builds a FLOAT node; the port must not build an Integer.
    let n = crate::stringToNodeNullable(ctx.mcx(), "-2147483648").unwrap().unwrap();
    assert_eq!(n.as_float().expect("T_Float").fval, "-2147483648");
}

// ---- audit-18.6 remediation b017 (read.c / readfuncs.c error surface) ----
//
// pg_node_tree_in refuses every SQL value in both engines ("cannot accept a
// value of type pg_node_tree", live pair IDENTICAL), so these shapes are
// reachable only through engine-written or corrupted catalog text; they are
// witnessed here at the library level. Each expected message is the C elog
// byte-for-byte; SQLSTATE XX000 is elog(ERROR)'s default.

fn err_of(text: &str) -> Box<PgError> {
    let ctx = MemoryContext::new("t");
    match crate::stringToNodeNullable(ctx.mcx(), text) {
        Err(e) => e,
        Ok(n) => panic!("{text:?} parsed (some node: {}) instead of raising", n.is_some()),
    }
}

fn assert_elog(text: &str, message: &str) {
    let e = err_of(text);
    assert_eq!(e.sqlstate(), ERRCODE_INTERNAL_ERROR, "{text:?}: {}", e.message());
    assert_eq!(e.message(), message, "{text:?}");
}

// read.c:484 nodeRead T_Float: a numeric-leading token strtoint does not
// consume entirely, or that ERANGEs, becomes a Float node carrying the raw
// token (before: panic "T_Float value node").
#[test]
fn float_value_tokens_read_as_float_nodes() {
    for t in ["1.5", "+1.5", "-0.25", ".5", "1e5", "12abc", "9992999999", "-2147483648"] {
        let ctx = MemoryContext::new("t");
        let n = crate::stringToNodeNullable(ctx.mcx(), t).unwrap().unwrap();
        assert_eq!(n.node_tag(), NodeTag::T_Float, "{t:?}");
        assert_eq!(n.as_float().unwrap().fval, t, "{t:?} keeps the raw token");
    }
    let ctx = MemoryContext::new("t");
    let l = crate::stringToNode(ctx.mcx(), "(1.5)").unwrap();
    assert_eq!(l.as_list().unwrap().nth(0).as_float().unwrap().fval, "1.5");
}

// read.c:257 nodeTokenType accepts an explicit '+' sign; the value is atoi.
#[test]
fn plus_signed_integer_tokens_read_as_integer_nodes() {
    for (t, v) in [("+1", 1), ("+2147483647", i32::MAX), ("-0", 0), ("+007", 7)] {
        let ctx = MemoryContext::new("t");
        let n = crate::stringToNodeNullable(ctx.mcx(), t).unwrap().unwrap();
        assert_eq!(n.as_integer().unwrap_or_else(|| panic!("{t:?} is T_Integer")).ival, v);
    }
    let ctx = MemoryContext::new("t");
    let l = crate::stringToNode(ctx.mcx(), "(+1)").unwrap();
    assert_eq!(l.as_list().unwrap().nth(0).as_integer().unwrap().ival, 1);
}

// read.c:493 T_Boolean and read.c:498 T_BitString value tokens.
#[test]
fn boolean_and_bitstring_value_tokens() {
    let ctx = MemoryContext::new("t");
    let l = crate::stringToNode(ctx.mcx(), "(true false b101 x1F)").unwrap();
    let l = l.as_list().unwrap();
    assert!(l.nth(0).as_boolean().unwrap().boolval);
    assert!(!l.nth(1).as_boolean().unwrap().boolval);
    assert_eq!(l.nth(2).as_bitstring().unwrap().bsval, "b101");
    assert_eq!(l.nth(3).as_bitstring().unwrap().bsval, "x1F");
}

// read.c:400 "(x ...)" TransactionId lists and read.c:421 "(b ...)"
// Bitmapsets met by nodeRead itself (not via READ_BITMAPSET_FIELD).
#[test]
fn xid_lists_and_bitmapsets_in_node_read() {
    let ctx = MemoryContext::new("t");
    let m = ctx.mcx();
    let x = crate::stringToNode(m, "(x 100 200)").unwrap();
    let x = x.as_xid_list().expect("XidList");
    assert_eq!((x.len(), x.nth(0), x.nth(1)), (2, 100, 200));
    let b = crate::stringToNode(m, "(b 1 2)").unwrap();
    let b = b.as_bitmapset().expect("Bitmapset");
    assert!(b.is_member(1) && b.is_member(2) && b.num_members() == 2);
    let e = crate::stringToNode(m, "(b)").unwrap();
    assert!(e.as_bitmapset().unwrap().is_empty());
    let nested = crate::stringToNode(m, "((b 3) (x 7))").unwrap();
    let nested = nested.as_list().unwrap();
    assert!(nested.nth(0).as_bitmapset().unwrap().is_member(3));
    assert_eq!(nested.nth(1).as_xid_list().unwrap().nth(0), 7);
}

// read.c:357/368/389/410 "unterminated List structure", read.c:433
// "unterminated Bitmapset structure" (before: panic "unterminated input").
#[test]
fn unterminated_structures_raise_cs_elog() {
    for t in ["(", "(i 1 2", "(o 1", "(x 1", "(1 2", "({RANGETBLREF :rtindex 1}"] {
        assert_elog(t, "unterminated List structure");
    }
    assert_elog("(b 1", "unterminated Bitmapset structure");
    assert_elog("(b", "unterminated Bitmapset structure");
}

// read.c:462 RIGHT_PAREN and read.c:473 OTHER_TOKEN.
#[test]
fn stray_tokens_raise_cs_elog() {
    assert_elog(")", "unexpected right parenthesis");
    assert_elog("foobar", "unrecognized token: \"foobar\"");
    assert_elog("(1 foobar)", "unrecognized token: \"foobar\"");
    assert_elog("+-5", "unrecognized token: \"+-5\"");
}

// read.c:373/394/417/438: a list member strtol/strtoul does not consume
// entirely (before: panic "bad integer token").
#[test]
fn bad_typed_list_members_raise_cs_elog() {
    assert_elog("(i notanint)", "unrecognized integer: \"notanint\"");
    assert_elog("(i 12abc)", "unrecognized integer: \"12abc\"");
    assert_elog("(o 1x)", "unrecognized OID: \"1x\"");
    assert_elog("(x zz)", "unrecognized Xid: \"zz\"");
    assert_elog("(b notanint)", "unrecognized integer: \"notanint\"");
    // (int) strtol / (Oid) strtoul: libc saturation, then C's narrowing.
    let ctx = MemoryContext::new("t");
    let l = crate::stringToNode(ctx.mcx(), "(i 99999999999 -1)").unwrap();
    let l = l.as_int_list().unwrap();
    assert_eq!((l.nth(0), l.nth(1)), (99999999999i64 as i32, -1));
    let o = crate::stringToNode(ctx.mcx(), "(o -1 4294967296)").unwrap();
    let o = o.as_oid_list().unwrap();
    assert_eq!((o.nth(0), o.nth(1)), (u32::MAX, 0));
}

// read.c:341 "did not find '}' at end of input node" (before: assert! in
// expect("}")). A node whose LAST field is a node (READ_NODE_FIELD) reads
// NULL at end of input exactly like C's nodeRead, then hits this check.
#[test]
fn missing_close_brace_raises_cs_elog() {
    assert_elog("{RANGETBLREF :rtindex 1", "did not find '}' at end of input node");
    assert_elog("{RANGETBLREF :rtindex 1 )", "did not find '}' at end of input node");
    assert_elog("{FROMEXPR :fromlist <> :quals", "did not find '}' at end of input node");
}

// readfuncs.c:209 _readBitmapset (READ_BITMAPSET_FIELD): "incomplete
// Bitmapset structure", "unrecognized token", "unterminated Bitmapset
// structure", "unrecognized integer" (before: assert!/panic).
#[test]
fn malformed_bitmapset_field_raises_cs_elog() {
    let head = "{VAR :varno 1 :varattno 1 :vartype 23 :vartypmod -1 :varcollid 0 :varnullingrels ";
    let tail = " :varlevelsup 0 :location -1}";
    assert_elog(&format!("{head}(b notanint){tail}"), "unrecognized integer: \"notanint\"");
    assert_elog(&format!("{head}<>{tail}"), "unrecognized token: \"\"");
    assert_elog(&format!("{head}(i 1){tail}"), "unrecognized token: \"i\"");
    assert_elog(&format!("{head}(b 1"), "unterminated Bitmapset structure");
    assert_elog(&format!("{head}(b"), "unterminated Bitmapset structure");
    assert_elog(&format!("{head}("), "incomplete Bitmapset structure");
    assert_elog(head.trim_end(), "incomplete Bitmapset structure");
}

// readfuncs.c:600 readDatum: the "[" / "]" checks print the token with
// "%s" — pg_strtok does not NUL-terminate, so C shows the REST of the node
// string from that token — and byval length > sizeof(Datum) is an elog
// (before: assert!/expect panics).
#[test]
fn malformed_datum_raises_cs_elog() {
    let head = "{CONST :consttype 23 :consttypmod -1 :constcollid 0 :constlen 4 \
                :constbyval true :constisnull false :location -1 :constvalue ";
    assert_elog(&format!("{head}9 [ 0 0 0 0 0 0 0 0 ]}}"), "byval datum but length = 9");
    assert_elog(
        &format!("{head}4 x 1 0 0 0 0 0 0 0 ]}}"),
        "expected \"[\" to start datum, but got \"x 1 0 0 0 0 0 0 0 ]}\"; length = 4",
    );
    assert_elog(
        &format!("{head}4 <> 1 0 0 0 0 0 0 0 ]}}"),
        "expected \"[\" to start datum, but got \"<> 1 0 0 0 0 0 0 0 ]}\"; length = 4",
    );
    assert_elog(
        &format!("{head}4"),
        "expected \"[\" to start datum, but got \"[NULL]\"; length = 4",
    );
    assert_elog(
        &format!("{head}4 [ 1 0 0 0 0 0 0 0 }}"),
        "expected \"]\" to end datum, but got \"}\"; length = 4",
    );
    // atoui/atoi semantics for the length and the bytes: never an error.
    let ctx = MemoryContext::new("t");
    let c = crate::stringToNode(ctx.mcx(), &format!("{head}4 [ 5x 0 zz 0 0 0 0 0 ]}}")).unwrap();
    assert_eq!(c.as_const().unwrap().constvalue.as_u64(), 5);
}

// readfuncs.c:435 _readRangeTblEntry default: "unrecognized RTE kind: %d"
// (before: panic "bad RTEKind"). READ_ENUM_FIELD is atoi, so -1 prints -1.
#[test]
fn unrecognized_rte_kind_raises_cs_elog() {
    assert_elog(
        "{RANGETBLENTRY :alias <> :eref <> :rtekind 99 :lateral false :inFromCl false :securityQuals ()}",
        "unrecognized RTE kind: 99",
    );
    assert_elog("{RANGETBLENTRY :alias <> :eref <> :rtekind -1", "unrecognized RTE kind: -1");
}

// readfuncs.c:301 _readBoolExpr: "unrecognized boolop \"%.*s\"".
#[test]
fn unrecognized_boolop_raises_cs_elog() {
    assert_elog("{BOOLEXPR :boolop badop :args () :location -1}", "unrecognized boolop \"badop\"");
    assert_elog("{BOOLEXPR :boolop <> :args () :location -1}", "unrecognized boolop \"\"");
}

// READ_INT_FIELD is atoi(token) and READ_BOOL_FIELD is strtobool(token) ==
// (*token == 't'): neither ever errors in C (before: panic "bad integer
// token" / "bad bool").
#[test]
fn scalar_fields_follow_cs_atoi_and_strtobool() {
    for (t, v) in [("12abc", 12), ("zz", 0), ("99999999999", 99999999999i64 as i32), ("+3", 3)] {
        let ctx = MemoryContext::new("t");
        let n = crate::stringToNode(ctx.mcx(), &format!("{{RANGETBLREF :rtindex {t}}}")).unwrap();
        assert_eq!(n.as_range_tbl_ref().unwrap().rtindex, v, "{t:?}");
    }
    for (t, v) in [("true", true), ("tru", true), ("t", true), ("false", false), ("yes", false), ("<>", false)] {
        let ctx = MemoryContext::new("t");
        let n = crate::stringToNode(
            ctx.mcx(),
            &format!("{{NULLTEST :arg <> :nulltesttype 0 :argisrow {t} :location -1}}"),
        )
        .unwrap();
        assert_eq!(n.as_null_test().unwrap().argisrow, v, "{t:?}");
    }
}

// _readConst: for a null Const C skips the constvalue token without looking
// at it (readfuncs.c:279 "skip <>"); before: assert! that it was "<>".
#[test]
fn null_const_skips_the_value_token_unchecked() {
    let ctx = MemoryContext::new("t");
    let s = "{CONST :consttype 25 :consttypmod -1 :constcollid 100 :constlen -1 \
             :constbyval false :constisnull true :location -1 :constvalue 4}";
    let c = crate::stringToNode(ctx.mcx(), s).unwrap();
    assert!(c.as_const().unwrap().constisnull);
}

// RECURSION GUARD (C parity: readfuncs.c:578 parseNodeString calls
// check_stack_depth). With the guard armed and the limit lowered, deep {}
// nesting must come back as a STRUCTURED 54001, never a stack overflow.
// MUST-FAIL CONTROL inverted: the same input parses fine at the default
// limit, so the 54001 witness is the guard firing, not a parse error.
#[test]
fn deep_nesting_raises_54001_with_the_guard_armed() {
    let depth = 4000;
    let mut text = String::new();
    for _ in 0..depth {
        text.push_str("{BOOLEXPR :boolop and :args (");
    }
    text.push_str("<>");
    for _ in 0..depth {
        text.push_str(") :location -1}");
    }

    std::thread::Builder::new()
        .stack_size(32 * 1024 * 1024)
        .spawn(move || {
            stack_depth_core::set_stack_base();

            // control: parses clean under a generous limit
            stack_depth_core::assign_max_stack_depth(16 * 1024);
            let ctx = MemoryContext::new("t");
            assert!(
                crate::stringToNodeNullable(ctx.mcx(), &text).is_ok(),
                "control failed: the witness input must parse at a high limit"
            );

            // guard: a low limit turns the same input into 54001 (exact
            // bytes, scale-independent — this pins the guard mechanism)
            stack_depth_core::set_enforced_stack_budget_for_tests(200 * 1024);
            let ctx2 = MemoryContext::new("t");
            let err = crate::stringToNodeNullable(ctx2.mcx(), &text)
                .expect_err("the guard must fire at a 200kB limit");
            assert_eq!(
                err.sqlstate(),
                types_error::ERRCODE_STATEMENT_TOO_COMPLEX,
                "guard must raise 54001, got {:?}",
                err.sqlstate()
            );
            // restore for other tests on this thread (none, but be tidy)
            stack_depth_core::assign_max_stack_depth(100);
        })
        .unwrap()
        .join()
        .unwrap();
}
