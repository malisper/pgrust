use std::rc::Rc;

use ::datum::Datum;
use ::executils::EStateData;
use ::mcx::{Mcx, MemoryContext, PgVec};
use ::types_error::PgResult;
use ::types_fmgr::{FmgrInfo, FunctionCallInfoBaseData};
use ::types_slot::TupleSlotKind;
use ::types_tuple::{
    CompactAttribute, FormData_pg_attribute, TupleDescData, TYPALIGN_INT, TYPSTORAGE_PLAIN,
};

use crate::*;

fn int4_desc(mcx: Mcx<'_>, natts: i32) -> TupleDescData<'_> {
    let mut attrs = PgVec::new_in(mcx);
    let mut compact = PgVec::new_in(mcx);
    for i in 0..natts {
        let att = FormData_pg_attribute {
            attnum: (i + 1) as i16,
            atttypid: 23,
            attlen: 4,
            attbyval: true,
            attalign: TYPALIGN_INT,
            attstorage: TYPSTORAGE_PLAIN,
            ..Default::default()
        };
        compact.push(CompactAttribute::populate_from(&att));
        attrs.push(att);
    }
    TupleDescData {
        natts,
        tdtypeid: 2249,
        tdtypmod: -1,
        tdrefcount: -1,
        constr: None,
        compact_attrs: compact,
        attrs,
    }
}

fn mat_srf(
    flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut FunctionCallInfoBaseData,
) -> PgResult<Datum> {
    // SAFETY: the executor armed es_query_cxt, which outlives this call.
    let mcx = unsafe { fcinfo.result_mcx_detached() };
    let mut srf = funcapi::InitMaterializedSRF(
        mcx,
        flinfo.expect("invoked with flinfo"),
        fcinfo,
        funcapi::MAT_SRF_USE_EXPECTED_DESC,
    )?;
    srf.putvalues(&[Datum::from_i32(10), Datum::from_i32(20)], &[false, false])?;
    srf.putvalues(&[Datum::from_i32(30), Datum::from_i32(40)], &[false, true])?;
    Ok(srf.finish(fcinfo))
}

fn setexpr_for(mcx: Mcx<'_>, returns_set: bool) -> SetExprState<'_> {
    SetExprState {
        flinfo: Some(FmgrInfo::new(mat_srf, 4242, 0, false, returns_set)),
        args: PgVec::new_in(mcx),
        collation: 0,
        returns_set,
        returns_tuple: false,
        elided_func_state: None,
    }
}

// C elidedFuncState leg: a planner-folded non-FuncExpr item yields exactly
// one row through the generic ExecEvalExpr path.
#[test]
fn elided_expression_stores_one_row() {
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let mut estate = EStateData::new_in(mcx);
    let ecxt = estate.exec_assign_expr_context();
    let desc = int4_desc(mcx, 1);

    let konst = ::types_nodes::node_tree::Node::mk_const(
        mcx,
        23,
        -1,
        0,
        4,
        Datum::from_i32(7),
        false,
        true,
    )
    .unwrap();
    let elided = crate::exec_init_expr(mcx, Some(konst), estate.param_bind()).unwrap().unwrap();
    let mut setexpr = SetExprState {
        flinfo: None,
        args: PgVec::new_in(mcx),
        collation: 0,
        returns_set: false,
        returns_tuple: false,
        elided_func_state: Some(elided),
    };

    let mut arg_mcx = MemoryContext::new("t-args");
    let mut store =
        exec_make_table_function_result(&mut setexpr, &desc, false, &mut estate, ecxt, &mut arg_mcx)
            .unwrap();
    assert_eq!(store.tuple_count(), 1);
    store.rescan();
    let mut slot =
        exectuples::make_tuple_table_slot(mcx, TupleSlotKind::MinimalTuple, Some(Rc::new(desc)));
    assert!(store.gettupleslot(true, false, &mut slot, mcx).unwrap());
    exectuples::slot_getallattrs(&mut slot);
    assert_eq!(slot.base().tts_values[0].as_i32(), 7);
    assert!(!slot.base().tts_isnull[0]);
    assert!(!store.gettupleslot(true, false, &mut slot, mcx).unwrap());
    store.end();
}

#[test]
fn materialize_mode_srf_feeds_the_scan_store() {
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let mut estate = EStateData::new_in(mcx);
    let ecxt = estate.exec_assign_expr_context();
    let desc = int4_desc(mcx, 2);
    let mut setexpr = setexpr_for(mcx, true);

    let mut arg_mcx = MemoryContext::new("t-args");
    let mut store =
        exec_make_table_function_result(&mut setexpr, &desc, false, &mut estate, ecxt, &mut arg_mcx)
            .unwrap();
    assert_eq!(store.tuple_count(), 2);

    let mut slot =
        exectuples::make_tuple_table_slot(mcx, TupleSlotKind::MinimalTuple, Some(Rc::new(desc)));
    assert!(store.gettupleslot(true, false, &mut slot, mcx).unwrap());
    exectuples::slot_getallattrs(&mut slot);
    assert_eq!(slot.base().tts_values[0].as_i32(), 10);
    assert_eq!(slot.base().tts_values[1].as_i32(), 20);
    assert!(store.gettupleslot(true, false, &mut slot, mcx).unwrap());
    exectuples::slot_getallattrs(&mut slot);
    assert_eq!(slot.base().tts_values[0].as_i32(), 30);
    assert!(slot.base().tts_isnull[1]);
    assert!(!store.gettupleslot(true, false, &mut slot, mcx).unwrap());
    store.end();
}

#[test]
fn materialize_mode_from_non_srf_violates_protocol() {
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let mut estate = EStateData::new_in(mcx);
    let ecxt = estate.exec_assign_expr_context();
    let desc = int4_desc(mcx, 2);
    let mut setexpr = setexpr_for(mcx, false);

    let mut arg_mcx = MemoryContext::new("t-args");
    let err = match exec_make_table_function_result(
        &mut setexpr,
        &desc,
        false,
        &mut estate,
        ecxt,
        &mut arg_mcx,
    ) {
        Err(e) => e,
        Ok(_) => panic!("non-SRF materialize return must violate the protocol"),
    };
    assert!(err
        .message()
        .contains("table-function protocol for materialize mode was not followed"));
}
