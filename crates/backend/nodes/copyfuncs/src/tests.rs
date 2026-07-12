use datum::Datum;
use mcx::MemoryContext;
use types_nodes::bitmapset::Bitmapset;
use types_nodes::equal::equal;
use types_nodes::list::{NodeList, OidList};
use types_nodes::plannodes::{Plan, PlannedStmt, Scan, SeqScan, Sort};
use types_nodes::primnodes::{Alias, Const, RangeVar, Var, VarReturningType};
use types_nodes::rawnodes::{DistinctClause, SelectStmt};
use types_nodes::{Node, NodeTag};

use crate::copy_object;

// COPY_PARSE_PLAN_TREES pattern: the copy must equal() the original and share
// no arena storage with it. The fixture is a full transformed Query captured
// from live PostgreSQL 18.3 (rules, RTEs, joins, target lists, quals).
#[test]
fn fixture_query_copy_roundtrip() {
    let src_ctx = MemoryContext::new("src");
    let dst_ctx = MemoryContext::new("dst");
    let (smcx, dmcx) = (src_ctx.mcx(), dst_ctx.mcx());
    const EV_ACTION: &str =
        include_str!("../../readfuncs/src/fixtures/pg_stat_activity.ev_action");
    let node = readfuncs::stringToNode(smcx, EV_ACTION).unwrap();
    let copy = copy_object(dmcx, node).unwrap();
    assert!(!node.ptr_eq(copy));
    // equal() would be C's COPY_PARSE_PLAN_TREES assertion, but its match
    // lacks T_RangeTblFunction (equalfuncs residual); the byte-identical
    // out-serialization below is the stricter check anyway (it also compares
    // the location fields equal() ignores).
    let s = outfuncs::nodeToString(smcx, node).unwrap().as_str().to_string();
    let c = outfuncs::nodeToString(dmcx, copy).unwrap().as_str().to_string();
    assert_eq!(s, c, "copy must serialize identically");
    // The copy must survive the source arena: serialize again after drop.
    drop(src_ctx);
    let c2 = outfuncs::nodeToString(dmcx, copy).unwrap().as_str().to_string();
    assert_eq!(c, c2);
}

// The plancache BuildCachedPlan boundary uses copy_query on transformed
// Queries: every fixture Query in the corpus must serialize identically after
// the structural copy and survive its source arena.
#[test]
fn query_corpus_copy_differential() {
    const CORPUS: &[&str] = &[
        include_str!("../../readfuncs/src/fixtures/pg_stat_activity.ev_action"),
        include_str!("../../../utils/adt/ruleutils/src/fixtures/v1_action.txt"),
        include_str!("../../../utils/adt/ruleutils/src/fixtures/v11_action.txt"),
    ];
    for &fixture in CORPUS {
        let src_ctx = MemoryContext::new("src");
        let dst_ctx = MemoryContext::new("dst");
        let (smcx, dmcx) = (src_ctx.mcx(), dst_ctx.mcx());
        let list = readfuncs::stringToNode(smcx, fixture).unwrap();
        let mut copies: Vec<(String, Node<'_>)> = Vec::new();
        for q in list.as_list().expect("action list").iter() {
            let query = q.as_query().expect("Query member");
            let copy = crate::copy_query(dmcx, query).unwrap();
            let copy = Node::mk(dmcx, copy).unwrap();
            let s = outfuncs::nodeToString(smcx, q).unwrap().as_str().to_string();
            let c = outfuncs::nodeToString(dmcx, copy).unwrap().as_str().to_string();
            assert_eq!(s, c, "copy_query must serialize identically");
            copies.push((c, copy));
        }
        drop(src_ctx);
        for (before, copy) in copies {
            let after = outfuncs::nodeToString(dmcx, copy).unwrap().as_str().to_string();
            assert_eq!(before, after, "copy must not reference the source arena");
        }
    }
}

#[test]
fn const_byref_datum_is_deep_copied() {
    let src_ctx = MemoryContext::new("src");
    let dst_ctx = MemoryContext::new("dst");
    let (smcx, dmcx) = (src_ctx.mcx(), dst_ctx.mcx());
    // text 'hi': 1-byte-header varlena would be fine too; use 4-byte header.
    let mut payload = vec![0u8; 6];
    let vl_len = (6u32) << 2; // 4-byte header varlena, len = header + 2
    payload[..4].copy_from_slice(&vl_len.to_le_bytes());
    payload[4] = b'h';
    payload[5] = b'i';
    let vl = mcx::slice_in(smcx, &payload).unwrap().leak();
    let c = Const {
        consttype: 25,
        consttypmod: -1,
        constcollid: 100,
        constlen: -1,
        constvalue: Datum::from_usize(vl.as_ptr() as usize),
        constisnull: false,
        constbyval: false,
        location: -1,
    };
    let node = Node::mk(smcx, c).unwrap();
    let copy = copy_object(dmcx, node).unwrap();
    let cc = copy.as_variant::<Const>().unwrap();
    assert_ne!(cc.constvalue.as_usize(), c.constvalue.as_usize(), "byref datum reallocated");
    assert!(equal(node, copy));
    drop(src_ctx);
    let p = cc.constvalue.as_usize() as *const u8;
    // SAFETY: freshly copied 6-byte varlena in dst arena.
    let bytes = unsafe { core::slice::from_raw_parts(p, 6) };
    assert_eq!(&bytes[4..], b"hi");
}

#[test]
fn plan_tree_copy() {
    let src_ctx = MemoryContext::new("src");
    let dst_ctx = MemoryContext::new("dst");
    let (smcx, dmcx) = (src_ctx.mcx(), dst_ctx.mcx());
    let var = Node::mk(
        smcx,
        Var {
            varno: 1,
            varattno: 1,
            vartype: 23,
            vartypmod: -1,
            varcollid: 0,
            varnullingrels: Bitmapset::empty(),
            varlevelsup: 0,
            varreturningtype: VarReturningType::VAR_RETURNING_DEFAULT,
            varnosyn: 1,
            varattnosyn: 1,
            location: -1,
        },
    )
    .unwrap();
    let mut ext = Bitmapset::empty();
    ext.add_member(smcx, 3).unwrap();
    ext.add_member(smcx, 77).unwrap();
    let seqscan = Node::mk(
        smcx,
        SeqScan {
            cb_scan_cols: None,
            scan: Scan {
                plan: Plan {
                    plan_rows: 100.0,
                    plan_width: 4,
                    plan_node_id: 1,
                    targetlist: NodeList::make1(smcx, var).unwrap(),
                    extParam: ext,
                    ..Plan::default()
                },
                scanrelid: 1,
            },
        },
    )
    .unwrap();
    let sort = Node::mk(
        smcx,
        Sort {
            plan: Plan { lefttree: Some(seqscan), ..Plan::default() },
            numCols: 1,
            sortColIdx: mcx::slice_in(smcx, &[1i16]).unwrap().leak(),
            sortOperators: mcx::slice_in(smcx, &[97u32]).unwrap().leak(),
            collations: mcx::slice_in(smcx, &[0u32]).unwrap().leak(),
            nullsFirst: mcx::slice_in(smcx, &[false]).unwrap().leak(),
        },
    )
    .unwrap();
    let copy = copy_object(dmcx, sort).unwrap();
    drop(src_ctx);
    let s = copy.as_variant::<Sort>().unwrap();
    assert_eq!(s.numCols, 1);
    assert_eq!(s.sortColIdx, &[1i16]);
    assert_eq!(s.sortOperators, &[97u32]);
    assert_eq!(s.nullsFirst, &[false]);
    let inner = s.plan.lefttree.expect("lefttree survives");
    assert_eq!(inner.node_tag(), NodeTag::T_SeqScan);
    let ss = inner.as_variant::<SeqScan>().unwrap();
    assert_eq!(ss.scan.scanrelid, 1);
    assert_eq!(ss.scan.plan.plan_rows, 100.0);
    assert_eq!(ss.scan.plan.targetlist.len(), 1);
    let v = ss.scan.plan.targetlist.first().unwrap().as_variant::<Var>().unwrap();
    assert_eq!(v.vartype, 23);
    assert!(ss.scan.plan.extParam.is_member(3));
    assert!(ss.scan.plan.extParam.is_member(77));
    assert_eq!(ss.scan.plan.extParam.num_members(), 2);
}

#[test]
fn utility_planned_stmt_copy() {
    let src_ctx = MemoryContext::new("src");
    let dst_ctx = MemoryContext::new("dst");
    let (smcx, dmcx) = (src_ctx.mcx(), dst_ctx.mcx());
    let alias = Node::mk(smcx, Alias { aliasname: Some("al"), colnames: NodeList::nil() })
        .unwrap()
        .as_variant::<Alias>()
        .unwrap();
    let rv = Node::mk(
        smcx,
        RangeVar {
            catalogname: None,
            schemaname: Some("public"),
            relname: Some("t"),
            inh: true,
            relpersistence: b'p',
            alias: Some(alias),
            location: 3,
        },
    )
    .unwrap();
    let rel = Node::mk(
        smcx,
        types_nodes::parsenodes::VacuumRelation {
            relation: Some(rv),
            oid: 0,
            va_cols: NodeList::nil(),
        },
    )
    .unwrap();
    let vacuum = Node::mk(
        smcx,
        types_nodes::parsenodes::VacuumStmt {
            options: NodeList::nil(),
            rels: NodeList::make1(smcx, rel).unwrap(),
            is_vacuumcmd: true,
        },
    )
    .unwrap();
    let pstmt = PlannedStmt {
        commandType: types_nodes::nodes_enums::CmdType::CMD_UTILITY,
        canSetTag: true,
        utilityStmt: Some(vacuum),
        stmt_len: 9,
        ..PlannedStmt::default()
    };
    let copy = crate::copy_utility_planned_stmt(dmcx, &pstmt).unwrap();
    drop(src_ctx);
    assert!(copy.canSetTag);
    let stmt = copy.utilityStmt.expect("utilityStmt copied");
    let vs = stmt.as_variant::<types_nodes::parsenodes::VacuumStmt>().unwrap();
    assert!(vs.is_vacuumcmd);
    let rel = vs.rels.first().unwrap();
    let vr = rel.as_variant::<types_nodes::parsenodes::VacuumRelation>().unwrap();
    let rv = vr.relation.expect("relation").as_variant::<RangeVar>().unwrap();
    assert_eq!(rv.schemaname, Some("public"));
    assert_eq!(rv.relname, Some("t"));
    assert_eq!(rv.alias.expect("alias").aliasname, Some("al"));
}

#[test]
fn select_stmt_distinct_and_lists() {
    let src_ctx = MemoryContext::new("src");
    let dst_ctx = MemoryContext::new("dst");
    let (smcx, dmcx) = (src_ctx.mcx(), dst_ctx.mcx());
    let col = Node::mk(
        smcx,
        types_nodes::rawnodes::ColumnRef {
            fields: NodeList::make1(smcx, Node::mk_string(smcx, "a").unwrap()).unwrap(),
            location: 16,
        },
    )
    .unwrap();
    let sel = Node::mk(
        smcx,
        SelectStmt {
            distinctClause: DistinctClause::On(NodeList::make1(smcx, col).unwrap()),
            ..SelectStmt::default()
        },
    )
    .unwrap();
    let copy = copy_object(dmcx, sel).unwrap();
    drop(src_ctx);
    let s = copy.as_variant::<SelectStmt>().unwrap();
    let DistinctClause::On(l) = &s.distinctClause else {
        panic!("DISTINCT ON list must survive the copy");
    };
    let cr = l.first().unwrap().as_variant::<types_nodes::rawnodes::ColumnRef>().unwrap();
    assert_eq!(cr.fields.first().unwrap().as_string().unwrap().sval, "a");
}

#[test]
fn value_and_scalar_list_nodes() {
    let src_ctx = MemoryContext::new("src");
    let dst_ctx = MemoryContext::new("dst");
    let (smcx, dmcx) = (src_ctx.mcx(), dst_ctx.mcx());
    let oids = OidList::from_slice(smcx, &[16u32, 25, 1700]).unwrap();
    let n = Node::mk_oid_list(smcx, oids).unwrap();
    let copy = copy_object(dmcx, n).unwrap();
    let s = Node::mk_string(smcx, "xyzzy").unwrap();
    let sc = copy_object(dmcx, s).unwrap();
    drop(src_ctx);
    assert_eq!(copy.as_oid_list().unwrap().as_slice(), &[16u32, 25, 1700]);
    assert_eq!(sc.as_string().unwrap().sval, "xyzzy");
}
