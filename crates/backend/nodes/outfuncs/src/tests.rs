use datum::Datum;
use mcx::MemoryContext;
use types_nodes::bitmapset::Bitmapset;
use types_nodes::list::NodeList;
use types_nodes::primnodes::{Const, OpExpr, Var};
use types_nodes::Node;

use crate::nodeToString;

// Captured from live PostgreSQL 18.3:
// CREATE TABLE dctest (a int DEFAULT 42, b int CHECK (b > 0)).
const ADBIN_DEFAULT_42: &str = "{CONST :consttype 23 :consttypmod -1 :constcollid 0 \
    :constlen 4 :constbyval true :constisnull false :location -1 :constvalue 4 \
    [ 42 0 0 0 0 0 0 0 ]}";
const CONBIN_B_GT_0: &str = "{OPEXPR :opno 521 :opfuncid 147 :opresulttype 16 \
    :opretset false :opcollid 0 :inputcollid 0 :args ({VAR :varno 1 :varattno 2 \
    :vartype 23 :vartypmod -1 :varcollid 0 :varnullingrels (b) :varlevelsup 0 \
    :varreturningtype 0 :varnosyn 1 :varattnosyn 2 :location -1} {CONST \
    :consttype 23 :consttypmod -1 :constcollid 0 :constlen 4 :constbyval true \
    :constisnull false :location -1 :constvalue 4 [ 0 0 0 0 0 0 0 0 ]}) \
    :location -1}";

fn int4_const(v: i32) -> Const {
    Const {
        consttype: 23,
        consttypmod: -1,
        constcollid: 0,
        constlen: 4,
        constvalue: Datum::from_i32(v),
        constisnull: false,
        constbyval: true,
        location: 7,
    }
}

#[test]
fn const_matches_live_adbin() {
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let node = Node::mk(mcx, int4_const(42)).unwrap();
    assert_eq!(nodeToString(mcx, node).unwrap().as_str(), ADBIN_DEFAULT_42);
}

#[test]
fn opexpr_matches_live_conbin() {
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let var = Node::mk(
        mcx,
        Var {
            varno: 1,
            varattno: 2,
            vartype: 23,
            vartypmod: -1,
            varcollid: 0,
            varnullingrels: Bitmapset::empty(),
            varlevelsup: 0,
            varreturningtype: types_nodes::primnodes::VarReturningType::VAR_RETURNING_DEFAULT,
            varnosyn: 1,
            varattnosyn: 2,
            location: 33,
        },
    )
    .unwrap();
    let zero = Node::mk(mcx, int4_const(0)).unwrap();
    let mut args = NodeList::nil();
    args.lappend(mcx, var).unwrap();
    args.lappend(mcx, zero).unwrap();
    let op = Node::mk(
        mcx,
        OpExpr {
            opno: 521,
            opfuncid: 147,
            opresulttype: 16,
            opretset: false,
            opcollid: 0,
            inputcollid: 0,
            args,
            location: 35,
        },
    )
    .unwrap();
    assert_eq!(nodeToString(mcx, op).unwrap().as_str(), CONBIN_B_GT_0);
}

#[test]
fn round_trips_through_readfuncs_scanner_shape() {
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let node = Node::mk(mcx, int4_const(-7)).unwrap();
    let s = nodeToString(mcx, node).unwrap();
    assert!(s.as_str().contains(":constvalue 4 [ -7 -1 -1 -1 -1 -1 -1 -1 ]"));
}

// Captured from live PostgreSQL 18.3: CREATE TABLE (e bigint DEFAULT 42).
const ADBIN_BIGINT_DEFAULT_42: &str = "{FUNCEXPR :funcid 481 :funcresulttype 20 \
    :funcretset false :funcvariadic false :funcformat 2 :funccollid 0 \
    :inputcollid 0 :args ({CONST :consttype 23 :consttypmod -1 :constcollid 0 \
    :constlen 4 :constbyval true :constisnull false :location -1 :constvalue 4 \
    [ 42 0 0 0 0 0 0 0 ]}) :location -1}";

#[test]
fn funcexpr_matches_live_adbin_and_round_trips() {
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let mut args = NodeList::nil();
    args.lappend(mcx, Node::mk(mcx, int4_const(42)).unwrap()).unwrap();
    let f = Node::mk(
        mcx,
        types_nodes::primnodes::FuncExpr {
            funcid: 481,
            funcresulttype: 20,
            funcretset: false,
            funcvariadic: false,
            funcformat: types_nodes::primnodes::CoercionForm::COERCE_IMPLICIT_CAST,
            funccollid: 0,
            inputcollid: 0,
            args,
            location: 30,
        },
    )
    .unwrap();
    let s = nodeToString(mcx, f).unwrap();
    assert_eq!(s.as_str(), ADBIN_BIGINT_DEFAULT_42);
    let back = readfuncs::stringToNode(mcx, s.as_str()).unwrap();
    let fx = back.as_variant::<types_nodes::primnodes::FuncExpr>().unwrap();
    assert_eq!(fx.funcid, 481);
    assert_eq!(fx.args.len(), 1);
}
