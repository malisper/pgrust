//! Logic tests for the FunctionScan node — drive the real `ExecInitFunctionScan`
//! / `ExecEndFunctionScan` / `ExecReScanFunctionScan` against mock installs of
//! the unported owners' seams (execUtils, execSRF, execScan, execTuples,
//! execExpr, tupdesc/toastdesc), and verify the new
//! `PlanStateNode::FunctionScan` variant is reachable + downcastable through
//! `as_function_scan_state` (mirroring the #165 AggState-as-PlanStateNode
//! pattern: the node is a live, tag-checked member of the central dispatch
//! enum).
//!
//! The runtime per-row SRF evaluation (`ExecMakeTableFunctionResult`) is the
//! #349 K2 keystone (the frame-based SRF invoke seam owned by the not-yet-ported
//! `execSRF.c`); the final test confirms that call is a documented loud
//! seam-panic, NOT a fake/`todo!`.

use std::sync::Once;

use mcx::MemoryContext;
use types_nodes::nodes::Node;
use types_nodes::primnodes::{Expr, Var};
use types_nodes::rawnodes::RangeTblFunction;
use types_nodes::nodefunctionscan::FunctionScan;
use types_nodes::value::StringNode;
use types_nodes::{EcxtId, PlanStateNode};

use super::*;

static INSTALL: Once = Once::new();

/// Build a 1-attribute non-NULL `TupleDescData` (the mock function result
/// descriptor). `natts = 1`, no attrs needed for the slot-init / copy mocks.
fn one_col_desc<'mcx>(mcx: mcx::Mcx<'mcx>) -> types_tuple::heaptuple::TupleDesc<'mcx> {
    let td = types_tuple::heaptuple::TupleDescData {
        natts: 1,
        tdtypeid: RECORDOID,
        tdtypmod: -1,
        tdrefcount: -1,
        constr: None,
        compact_attrs: PgVec::new_in(mcx),
        attrs: PgVec::new_in(mcx),
    };
    Some(mcx::alloc_in(mcx, td).unwrap())
}

/// Install the init-path seams as recording / passthrough mocks. The seam
/// registry is process-global, so a `Once` guards the install.
fn install() {
    // execUtils: ExecAssignExprContext — assign the node a per-node ExprContext
    // id (the real seam pushes an ExprContext into the EState pool; the mock
    // just stamps a stable id so `ExecInitFunctionScan` can thread it into the
    // setexpr init).
    execUtils::exec_assign_expr_context::set(|_estate, planstate| {
        planstate.ps_ExprContext = Some(EcxtId(0));
        Ok(())
    });

    // execSRF: ExecInitTableFunctionResult — return a default SetExprState box
    // (the real owner compiles the function's arg expressions; the init path
    // only needs a non-NULL state).
    execSRF::exec_init_table_function_result::set(|_expr, _econtext, _parent, estate| {
        mcx::alloc_in(
            estate.es_query_cxt,
            types_nodes::execexpr::SetExprState::default(),
        )
    });

    // execSRF: ExecMakeTableFunctionResult — the #349 K2 runtime keystone. Left
    // as the declared seam's loud default panic (NOT installed here), so the
    // runtime-SRF test exercises the documented seam boundary.

    // toastdesc / tupdesc descriptor construction.
    toastdesc::build_desc_from_lists::set(|mcx, _names, _types, _typmods, _colls| {
        Ok(one_col_desc(mcx))
    });
    execTuples::bless_tuple_desc::set(|_mcx, td| Ok(td));
    tupdesc::create_tupledesc_copy::set(|mcx, td| Ok(mcx::alloc_in(mcx, td.clone_in(mcx)?)?));

    // execTuples / execScan slot + result-type + projection setup.
    execTuples::exec_init_scan_tuple_slot::set(|_estate, _ss, _td, _ops| Ok(()));
    execTuples::exec_init_result_type_tl::set(|_ps, _estate| Ok(()));
    execScan::exec_assign_scan_projection_info::set(|_ss, _estate| Ok(()));

    // execExpr: ExecInitQual over the (NIL) scan qual.
    execExpr::exec_init_qual::set(|_qual, _ps, _estate| Ok(None));
}

fn setup() {
    INSTALL.call_once(install);
}

/// A FunctionScan plan node: one function with a 1-column coldeflist (so the
/// init path takes the `BuildDescFromLists` branch, no `get_expr_result_type`),
/// no ordinality => `simple == true`.
fn simple_function_scan<'mcx>(mcx: mcx::Mcx<'mcx>) -> Node<'mcx> {
    // funcexpr: a trivial Expr node (the mocked ExecInitTableFunctionResult
    // never inspects it beyond `as_expr`).
    let funcexpr = mcx::alloc_in(mcx, Node::Expr(Expr::Var(Var::default()))).unwrap();

    // funccolnames = list_make1(makeString("c")).
    let mut funccolnames: PgVec<'mcx, types_nodes::nodes::NodePtr<'mcx>> = PgVec::new_in(mcx);
    funccolnames.push(
        mcx::alloc_in(
            mcx,
            Node::String(StringNode {
                sval: mcx::PgString::from_str_in("c", mcx).unwrap(),
            }),
        )
        .unwrap(),
    );

    let mut funccoltypes: PgVec<'mcx, types_core::Oid> = PgVec::new_in(mcx);
    funccoltypes.push(23); // int4
    let mut funccoltypmods: PgVec<'mcx, i32> = PgVec::new_in(mcx);
    funccoltypmods.push(-1);
    let mut funccolcollations: PgVec<'mcx, types_core::Oid> = PgVec::new_in(mcx);
    funccolcollations.push(0);

    let rtfunc = RangeTblFunction {
        funcexpr: Some(funcexpr),
        funccolcount: 1,
        funccolnames,
        funccoltypes,
        funccoltypmods,
        funccolcollations,
        funcparams: None,
    };

    let mut functions: PgVec<'mcx, RangeTblFunction<'mcx>> = PgVec::new_in(mcx);
    functions.push(rtfunc);

    Node::FunctionScan(FunctionScan {
        scan: Default::default(),
        functions: Some(functions),
        funcordinality: false,
    })
}

#[test]
fn init_builds_simple_function_scan_state() {
    setup();
    let ctx = MemoryContext::new("per-query");
    let mcx = ctx.mcx();
    let node = mcx::alloc_in(mcx, simple_function_scan(mcx)).unwrap();
    let mut estate = EStateData::new_in(mcx);

    let scanstate = ExecInitFunctionScan(&node, &mut estate, 0).unwrap();

    // Single function, no ordinality => the fast `simple` path.
    assert!(scanstate.simple);
    assert!(!scanstate.ordinality);
    assert_eq!(scanstate.nfuncs, 1);
    assert_eq!(scanstate.ordinal, 0);
    assert_eq!(scanstate.funcstates.len(), 1);
    // The per-function state was initialized: setexpr present, tstore NULL
    // (not called yet), rowcount -1, and — simple path — no separate func_slot.
    assert!(scanstate.funcstates[0].setexpr.is_some());
    assert!(scanstate.funcstates[0].tstore.is_none());
    assert_eq!(scanstate.funcstates[0].rowcount, -1);
    assert!(scanstate.funcstates[0].func_slot.is_none());
    assert!(scanstate.funcstates[0].tupdesc.is_some());
    // The arg-evaluation context was created.
    assert!(scanstate.argcontext.is_some());
    // The ExecProcNode callback was armed.
    assert!(scanstate.ss.ps.ExecProcNode.is_some());
}

#[test]
fn plan_state_node_function_scan_downcast() {
    setup();
    let ctx = MemoryContext::new("per-query");
    let mcx = ctx.mcx();
    let node = mcx::alloc_in(mcx, simple_function_scan(mcx)).unwrap();
    let mut estate = EStateData::new_in(mcx);

    let scanstate = ExecInitFunctionScan(&node, &mut estate, 0).unwrap();

    // Wrap the concrete state into the central PlanStateNode dispatch enum (the
    // #165 AggState-as-PlanStateNode discipline; here a direct by-value variant
    // since FunctionScanState lives in types-nodes).
    let mut ps = mcx::alloc_in(mcx, PlanStateNode::FunctionScan(scanstate)).unwrap();

    // tag() is T_FunctionScanState.
    assert_eq!(ps.tag(), types_nodes::nodefunctionscan::T_FunctionScanState);
    // The node is a relation-scan node: as_scan_state() reaches its ScanState.
    assert!(ps.as_scan_state().is_some());
    // castNode(FunctionScanState, node) recovers the concrete state.
    let recovered = ps.as_function_scan_state().expect("downcast to FunctionScanState");
    assert!(recovered.simple);
    assert_eq!(recovered.nfuncs, 1);
    // The &mut accessor works too.
    assert!(ps.as_function_scan_state_mut().is_some());
    // A non-FunctionScan downcast on this node is None.
    assert!(ps.as_agg_state().is_none());
}

#[test]
fn end_releases_state_without_tuplestores() {
    setup();
    let ctx = MemoryContext::new("per-query");
    let mcx = ctx.mcx();
    let node = mcx::alloc_in(mcx, simple_function_scan(mcx)).unwrap();
    let mut estate = EStateData::new_in(mcx);

    let mut scanstate = ExecInitFunctionScan(&node, &mut estate, 0).unwrap();
    // No function has been called, so no tuplestore to release — End is a clean
    // no-op (and must not panic into the tuplestore_end seam).
    ExecEndFunctionScan(&mut scanstate).unwrap();
    assert!(scanstate.funcstates[0].tstore.is_none());
}

#[test]
#[should_panic(expected = "exec_make_table_function_result")]
fn runtime_srf_call_hits_documented_k2_seam_boundary() {
    // The runtime per-row SRF evaluation calls ExecMakeTableFunctionResult,
    // whose owner (execSRF.c, #349 K2) is not yet ported, so the seam is the
    // declared loud panic. This proves the runtime path is a genuine documented
    // seam-panic into the K2 owner — NOT a fake row / todo!.
    setup();
    let ctx = MemoryContext::new("per-query");
    let mcx = ctx.mcx();
    let node = mcx::alloc_in(mcx, simple_function_scan(mcx)).unwrap();
    let mut estate = EStateData::new_in(mcx);

    let mut scanstate = ExecInitFunctionScan(&node, &mut estate, 0).unwrap();
    // Give the scan a scan slot so FunctionNext reaches the SRF call.
    let qcxt = estate.es_query_cxt;
    let slot = estate
        .make_slot(types_nodes::TupleTableSlot::new_in(qcxt))
        .unwrap();
    scanstate.ss.ss_ScanTupleSlot = Some(slot);

    // FunctionNext's first call reads all rows via ExecMakeTableFunctionResult
    // => the not-yet-installed K2 seam panics (documented boundary).
    let _ = FunctionNext(&mut scanstate, &mut estate);
}
