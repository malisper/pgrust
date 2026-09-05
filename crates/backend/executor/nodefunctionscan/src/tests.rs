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
    let elided = ::execexpr::exec_init_expr(mcx, Some(konst), estate.param_bind()).unwrap().unwrap();
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
    store.rescan().unwrap();
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

fn empty_vpc_srf(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut FunctionCallInfoBaseData,
) -> PgResult<Datum> {
    if let Some(rsinfo) = fcinfo.rsinfo_mut() {
        rsinfo.isDone = ExprDoneCond::ExprEndResult;
    }
    Ok(Datum::null())
}

#[test]
fn eight_arg_table_srf_does_not_panic() {
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let mut estate = EStateData::new_in(mcx);
    let ecxt = estate.exec_assign_expr_context();
    let desc = int4_desc(mcx, 1);

    let mut args = PgVec::new_in(mcx);
    for i in 0..8 {
        let konst = ::types_nodes::node_tree::Node::mk_const(
            mcx,
            23,
            -1,
            0,
            4,
            Datum::from_i32(i),
            false,
            true,
        )
        .unwrap();
        args.push(
            ::execexpr::exec_init_expr(mcx, Some(konst), estate.param_bind())
                .unwrap()
                .unwrap(),
        );
    }
    let mut setexpr = SetExprState {
        flinfo: Some(FmgrInfo::new(empty_vpc_srf, 4243, 8, false, true)),
        args,
        collation: 0,
        returns_set: true,
        returns_tuple: false,
        elided_func_state: None,
    };

    let mut arg_mcx = MemoryContext::new("t-args");
    let store =
        exec_make_table_function_result(&mut setexpr, &desc, false, &mut estate, ecxt, &mut arg_mcx)
            .unwrap();
    assert_eq!(store.tuple_count(), 0);
    store.end();
}

// ---------------------------------------------------------------------------
// audit-18.6 b154: init_sexpr / ExecInitFunctionScan / no_function_result
// witnesses (execSRF.c, nodeFunctionscan.c). The catalog reads the init path
// makes are served by process-wide test seams (set-once, so one installer
// serves every test in this binary).
// ---------------------------------------------------------------------------
mod init_sexpr_witness {
    use super::*;
    use std::sync::{Mutex, Once};

    use ::objectaccess::{ObjectAccessArg, ObjectAccessType, OAT_FUNCTION_EXECUTE};
    use ::syscache_seams::{PgProcFmgrShape, PgProcResultArraysShape, PgProcShape};
    use ::types_core::catalog::{PROCEDURE_RELATION_ID, RECORDOID};
    use ::types_core::Oid;
    use ::types_nodes::primnodes::FuncExpr;
    use ::types_nodes::{NodeList, RangeTblFunction};

    const INT4OID: Oid = 23;
    // pg_proc says SETOF, the planned FuncExpr says not (init_sexpr allowSRF).
    const FID_PROC_RETSET: Oid = 4301;
    // A plain function fed FUNC_MAX_ARGS + 1 arguments.
    const FID_MANY_ARGS: Oid = 4302;
    // RECORD-returning, no OUT params, no coldeflist.
    const FID_RECORD: Oid = 4303;
    // The object-access execute hook witness.
    const FID_HOOKED: Oid = 4304;

    static SEAMS: Once = Once::new();
    static HOOK_EVENTS: Mutex<Vec<(ObjectAccessType, Oid, Oid, i32)>> = Mutex::new(Vec::new());

    fn proc_fmgr(funcid: Oid) -> PgResult<Option<PgProcFmgrShape>> {
        let retset = match funcid {
            FID_PROC_RETSET => true,
            FID_MANY_ARGS | FID_HOOKED => false,
            _ => return Ok(None),
        };
        // prosecdef routes fmgr_info through the security-definer handler:
        // no prosrc/probin/pg_language reads, exactly the pg_proc fields
        // FmgrInfo carries (fn_retset among them).
        Ok(Some(PgProcFmgrShape {
            prolang: 12,
            prorettype: INT4OID,
            pronargs: 0,
            proisstrict: false,
            proretset: retset,
            prosecdef: true,
            proconfig_isnull: true,
            xmin: 0,
            tid: Default::default(),
        }))
    }

    fn proc_shape(funcid: Oid) -> PgResult<Option<PgProcShape>> {
        Ok((funcid == FID_RECORD).then(|| PgProcShape {
            pronamespace: 11,
            prorettype: RECORDOID,
            provariadic: types_core::InvalidOid,
            prosupport: types_core::InvalidOid,
            prolang: 12,
            pronargs: 0,
            prokind: b'f' as i8,
            provolatile: b'i' as i8,
            proparallel: b's' as i8,
            proretset: false,
            proisstrict: false,
            proleakproof: false,
            prosecdef: false,
            proconfig_isnull: true,
        }))
    }

    fn proc_result_arrays<'mcx>(
        _mcx: Mcx<'mcx>,
        funcid: Oid,
    ) -> PgResult<Option<PgProcResultArraysShape<'mcx>>> {
        Ok((funcid == FID_RECORD).then(|| PgProcResultArraysShape {
            proallargtypes: None,
            proargmodes: None,
            proargnames: None,
        }))
    }

    fn type_typtype(typid: Oid) -> PgResult<Option<i8>> {
        Ok(match typid {
            RECORDOID => Some(b'p' as i8),
            INT4OID => Some(b'b' as i8),
            _ => None,
        })
    }

    fn install_seams() {
        SEAMS.call_once(|| {
            ::syscache_seams::lookup_pg_proc_fmgr::set(proc_fmgr);
            ::syscache_seams::lookup_pg_proc_shape::set(proc_shape);
            ::syscache_seams::pg_proc_result_arrays::set(proc_result_arrays);
            ::syscache_seams::pg_type_typtype::set(type_typtype);
            ::aclchk_seams::object_aclcheck::set(|classid, _objid, _roleid, _mode| {
                assert_eq!(classid, PROCEDURE_RELATION_ID);
                Ok(0)
            });
            ::miscinit_seams::get_user_id::set(|| 10);
            ::mbutils_seams::pg_mbstrlen_with_len::set(|s| Ok(s.len() as i32));
        });
    }

    fn recording_hook(
        access: ObjectAccessType,
        class_id: Oid,
        object_id: Oid,
        sub_id: i32,
        _arg: &mut ObjectAccessArg<'_>,
    ) -> PgResult<()> {
        HOOK_EVENTS.lock().unwrap().push((access, class_id, object_id, sub_id));
        Ok(())
    }

    fn int4_const(mcx: Mcx<'_>, v: i32) -> ::types_nodes::node_tree::Node<'_> {
        ::types_nodes::node_tree::Node::mk_const(mcx, INT4OID, -1, 0, 4, Datum::from_i32(v), false, true)
            .unwrap()
    }

    fn rtfunc_for<'mcx>(
        mcx: Mcx<'mcx>,
        funcid: Oid,
        funcresulttype: Oid,
        funcretset: bool,
        nargs: i32,
        location: i32,
    ) -> RangeTblFunction<'mcx> {
        let mut args = NodeList::nil();
        for i in 0..nargs {
            args.lappend(mcx, int4_const(mcx, i)).unwrap();
        }
        let fexpr = ::types_nodes::node_tree::Node::mk(
            mcx,
            FuncExpr {
                funcid,
                funcresulttype,
                funcretset,
                funcvariadic: false,
                funcformat: Default::default(),
                funccollid: 0,
                inputcollid: 0,
                args,
                location,
            },
        )
        .unwrap();
        RangeTblFunction { funcexpr: Some(fexpr), ..Default::default() }
    }

    // execSRF.c:403-411 no_function_result: a non-set-returning function
    // that reports ExprEndResult on its first call produced nothing, and C
    // manufactures ONE all-nulls row shaped by expectedDesc (the SRF case
    // stays empty).
    #[test]
    fn non_srf_end_result_yields_one_all_nulls_row() {
        let ctx = MemoryContext::new("t");
        let mcx = ctx.mcx();
        let mut estate = EStateData::new_in(mcx);
        let ecxt = estate.exec_assign_expr_context();
        let desc = int4_desc(mcx, 2);
        let mut setexpr = SetExprState {
            flinfo: Some(FmgrInfo::new(empty_vpc_srf, 4244, 0, false, false)),
            args: PgVec::new_in(mcx),
            collation: 0,
            returns_set: false,
            returns_tuple: false,
            elided_func_state: None,
        };

        let mut arg_mcx = MemoryContext::new("t-args");
        let mut store = exec_make_table_function_result(
            &mut setexpr,
            &desc,
            false,
            &mut estate,
            ecxt,
            &mut arg_mcx,
        )
        .unwrap();
        assert_eq!(store.tuple_count(), 1, "C stores one all-nulls row for a non-SRF");
        let mut slot = exectuples::make_tuple_table_slot(
            mcx,
            TupleSlotKind::MinimalTuple,
            Some(Rc::new(desc)),
        );
        assert!(store.gettupleslot(true, false, &mut slot, mcx).unwrap());
        exectuples::slot_getallattrs(&mut slot);
        assert!(slot.base().tts_isnull[0]);
        assert!(slot.base().tts_isnull[1]);
        assert!(!store.gettupleslot(true, false, &mut slot, mcx).unwrap());
        store.end();
    }

    // execSRF.c:736-741 init_sexpr: fn_retset (pg_proc) with allowSRF false
    // (the planned FuncExpr.funcretset) is ERRCODE_FEATURE_NOT_SUPPORTED with
    // the executor cursor at the call's location.
    #[test]
    fn set_returning_proc_in_non_set_funcexpr_is_refused_at_init() {
        install_seams();
        let ctx = MemoryContext::new("t");
        let mcx = ctx.mcx();
        let mut estate = EStateData::new_in(mcx);
        estate.es_sourceText = Some("SELECT * FROM test_srf()");
        let rtfunc = rtfunc_for(mcx, FID_PROC_RETSET, INT4OID, false, 0, 14);
        let err = match exec_init_table_function_result(mcx, &rtfunc, &mut estate) {
            Err(e) => e,
            Ok(_) => panic!("init_sexpr must refuse a set-valued function where the caller disallows sets"),
        };
        assert_eq!(err.message(), "set-valued function called in context that cannot accept a set");
        assert_eq!(err.sqlstate(), ::types_error::ERRCODE_FEATURE_NOT_SUPPORTED);
        assert_eq!(err.cursor_position(), Some(15));
    }

    // execSRF.c:715-721 init_sexpr: more than FUNC_MAX_ARGS arguments is
    // ERRCODE_TOO_MANY_ARGUMENTS at init, never a fcinfo overrun later.
    #[test]
    fn more_than_func_max_args_is_refused_at_init() {
        install_seams();
        let ctx = MemoryContext::new("t");
        let mcx = ctx.mcx();
        let mut estate = EStateData::new_in(mcx);
        let nargs = (types_core::FUNC_MAX_ARGS + 1) as i32;
        let rtfunc = rtfunc_for(mcx, FID_MANY_ARGS, INT4OID, false, nargs, -1);
        let err = match exec_init_table_function_result(mcx, &rtfunc, &mut estate) {
            Err(e) => e,
            Ok(_) => panic!("init_sexpr must refuse more than FUNC_MAX_ARGS arguments"),
        };
        assert_eq!(err.message(), "cannot pass more than 100 arguments to a function");
        assert_eq!(err.sqlstate(), ::types_error::ERRCODE_TOO_MANY_ARGUMENTS);
    }

    // nodeFunctionscan.c:421 ExecInitFunctionScan: a result class that is
    // neither composite nor scalar (RECORD without a coldeflist) is C's
    // elog(ERROR) — an XX000 error, not a process abort.
    #[test]
    fn unsupported_return_type_is_an_internal_error_not_a_panic() {
        install_seams();
        let ctx = MemoryContext::new("t");
        let mcx = ctx.mcx();
        let rtfunc = rtfunc_for(mcx, FID_RECORD, RECORDOID, false, 0, -1);
        let err = match build_function_tupdesc(mcx, &rtfunc) {
            Err(e) => e,
            Ok(_) => panic!("RECORD without a coldeflist has no tupdesc"),
        };
        assert_eq!(err.message(), "function in FROM has unsupported return type");
        assert_eq!(err.sqlstate(), ::types_error::ERRCODE_INTERNAL_ERROR);
    }

    // execSRF.c:707 init_sexpr: InvokeFunctionExecuteHook(foid) fires
    // OAT_FUNCTION_EXECUTE for the table function after the ACL check.
    #[test]
    fn table_function_init_invokes_the_function_execute_hook() {
        install_seams();
        let ctx = MemoryContext::new("t");
        let mcx = ctx.mcx();
        let mut estate = EStateData::new_in(mcx);
        let rtfunc = rtfunc_for(mcx, FID_HOOKED, INT4OID, false, 0, -1);
        let prev = ::objectaccess::set_object_access_hook(Some(recording_hook));
        assert!(prev.is_none(), "another hook is installed on this thread");
        let init = exec_init_table_function_result(mcx, &rtfunc, &mut estate);
        ::objectaccess::set_object_access_hook(None);
        init.expect("hooked init succeeds");
        let events: Vec<_> = HOOK_EVENTS
            .lock()
            .unwrap()
            .iter()
            .copied()
            .filter(|e| e.2 == FID_HOOKED)
            .collect();
        assert_eq!(events, vec![(OAT_FUNCTION_EXECUTE, PROCEDURE_RELATION_ID, FID_HOOKED, 0)]);
    }
}
