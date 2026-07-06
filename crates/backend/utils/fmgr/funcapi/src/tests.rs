use super::*;
use ::datum::Datum;
use ::fmgr::{FmNode, LocalFcinfo};
use ::mcx::MemoryContext;
use ::types_core::{INT4OID, TEXTOID};
use ::types_tuple::{NameData, PgTypeShape, TYPALIGN_INT, TYPSTORAGE_EXTENDED, TYPSTORAGE_PLAIN};
use std::sync::Once;

const COMPOSITE_TYPE: Oid = 7777;
const F_SCALAR: Oid = 1001;
const F_COMPOSITE: Oid = 1002;
const F_RECORD_BARE: Oid = 1003;
const F_RECORD_OUT: Oid = 1004;
const F_RECORD_ONE_OUT: Oid = 1005;
const P_ONE_OUT: Oid = 1006;
const F_POLY: Oid = 1007;

fn proc_shape(prorettype: Oid, prokind: i8, pronargs: i16) -> PgProcShape {
    PgProcShape {
        prolang: 12,
        prosecdef: false,
        proconfig_isnull: true,
        pronamespace: 11,
        prorettype,
        provariadic: InvalidOid,
        prosupport: InvalidOid,
        pronargs,
        prokind,
        provolatile: b'i' as i8,
        proparallel: b's' as i8,
        proretset: false,
        proisstrict: true,
        proleakproof: false,
    }
}

// vec_with_capacity_in forbids droppy T; PgString elements own allocations.
fn vec_droppy_with_capacity<'mcx, T>(mcx: Mcx<'mcx>, n: usize) -> PgResult<PgVec<'mcx, T>> {
    let mut v = PgVec::new_in(mcx);
    v.try_reserve_exact(n)
        .map_err(|_| Box::new(mcx.oom(n.saturating_mul(core::mem::size_of::<T>()))))?;
    Ok(v)
}

static SEAMS: Once = Once::new();

fn install_seams() {
    SEAMS.call_once(|| {
        // The rig models a live session over pg_catalog-band builtins:
        // always visible, so error strings stay unqualified (C TypeIsVisible).
        namespace_seams::type_is_visible::set(|_| Ok(true));
        syscache_seams::lookup_pg_proc_shape::set(|funcid| {
            Ok(match funcid {
                F_SCALAR => Some(proc_shape(INT4OID, PROKIND_FUNCTION, 1)),
                F_COMPOSITE => Some(proc_shape(COMPOSITE_TYPE, PROKIND_FUNCTION, 0)),
                F_RECORD_BARE => Some(proc_shape(RECORDOID, PROKIND_FUNCTION, 0)),
                F_RECORD_OUT => Some(proc_shape(RECORDOID, PROKIND_FUNCTION, 1)),
                F_RECORD_ONE_OUT => Some(proc_shape(RECORDOID, PROKIND_FUNCTION, 1)),
                P_ONE_OUT => Some(proc_shape(RECORDOID, PROKIND_PROCEDURE, 1)),
                F_POLY => Some(proc_shape(ANYELEMENTOID, PROKIND_FUNCTION, 1)),
                _ => None,
            })
        });
        syscache_seams::pg_proc_proname::set(|funcid| {
            let mut name = NameData::default();
            name.namestrcpy(match funcid {
                F_POLY => "poly_fn",
                _ => "test_fn",
            });
            Ok(Some(name))
        });
        syscache_seams::lookup_pg_proc_signature::set(|mcx, funcid| {
            let mut args: PgVec<'_, Oid> = vec_with_capacity_in(mcx, 2)?;
            match funcid {
                F_SCALAR | F_RECORD_OUT | F_RECORD_ONE_OUT | P_ONE_OUT => args.push(INT4OID),
                F_POLY => args.push(ANYELEMENTOID),
                _ => {}
            }
            Ok(Some((InvalidOid, args)))
        });
        syscache_seams::pg_proc_result_arrays::set(|mcx, funcid| {
            let arrays = |types: &[Oid],
                          modes: &[i8],
                          names: Option<&[&str]>|
             -> PgResult<syscache_seams::PgProcResultArraysShape<'_>> {
                let mut t: PgVec<'_, Oid> = vec_with_capacity_in(mcx, types.len())?;
                t.extend_from_slice(types);
                let mut m: PgVec<'_, i8> = vec_with_capacity_in(mcx, modes.len())?;
                m.extend_from_slice(modes);
                let n = match names {
                    Some(names) => {
                        let mut v: PgVec<'_, PgString<'_>> =
                            vec_droppy_with_capacity(mcx, names.len())?;
                        for s in names {
                            v.push(PgString::from_str_in(s, mcx)?);
                        }
                        Some(v)
                    }
                    None => None,
                };
                Ok(syscache_seams::PgProcResultArraysShape {
                    proallargtypes: Some(t),
                    proargmodes: Some(m),
                    proargnames: n,
                })
            };
            Ok(match funcid {
                F_RECORD_OUT => Some(arrays(
                    &[INT4OID, INT4OID, TEXTOID],
                    &[PROARGMODE_IN, PROARGMODE_OUT, PROARGMODE_OUT],
                    Some(&["a", "b", ""]),
                )?),
                F_RECORD_ONE_OUT | P_ONE_OUT => Some(arrays(
                    &[INT4OID, INT4OID],
                    &[PROARGMODE_IN, PROARGMODE_OUT],
                    None,
                )?),
                F_RECORD_BARE | F_SCALAR | F_COMPOSITE | F_POLY => {
                    Some(syscache_seams::PgProcResultArraysShape {
                        proallargtypes: None,
                        proargmodes: None,
                        proargnames: None,
                    })
                }
                _ => None,
            })
        });
        syscache_seams::pg_type_typtype::set(|typid| {
            Ok(match typid {
                INT4OID | TEXTOID => Some(b'b' as i8),
                COMPOSITE_TYPE => Some(b'c' as i8),
                RECORDOID | VOIDOID | CSTRINGOID => Some(b'p' as i8),
                _ => None,
            })
        });
        syscache_seams::lookup_pg_type_shape::set(|typid| {
            Ok(match typid {
                INT4OID => Some(PgTypeShape {
                    typlen: 4,
                    typbyval: true,
                    typalign: TYPALIGN_INT,
                    typstorage: TYPSTORAGE_PLAIN,
                    typcollation: InvalidOid,
                }),
                TEXTOID => Some(PgTypeShape {
                    typlen: -1,
                    typbyval: false,
                    typalign: TYPALIGN_INT,
                    typstorage: TYPSTORAGE_EXTENDED,
                    typcollation: 100,
                }),
                _ => None,
            })
        });
        syscache_seams::lookup_pg_type_typcache_shape::set(|typid| {
            Ok((typid == ANYELEMENTOID).then(|| {
                let mut name = NameData::default();
                name.namestrcpy("anyelement");
                syscache_seams::PgTypeTypcacheShape {
                    typname: name,
                    typlen: 4,
                    typbyval: true,
                    typalign: TYPALIGN_INT,
                    typstorage: TYPSTORAGE_PLAIN,
                    typtype: b'p' as i8,
                    typisdefined: true,
                    typrelid: InvalidOid,
                    typsubscript: InvalidOid,
                    typelem: InvalidOid,
                    typarray: InvalidOid,
                    typcollation: InvalidOid,
                }
            }))
        });
        typcache_seams::assign_record_type_typmod::set(|desc| {
            desc.tdtypmod = 42;
            Ok(())
        });
        typcache_seams::lookup_rowtype_tupdesc_copy::set(|mcx, type_id, typmod| {
            assert_eq!(type_id, COMPOSITE_TYPE);
            assert_eq!(typmod, -1);
            let mut desc = tupdesc::CreateTemplateTupleDesc(mcx, 1)?;
            tupdesc::TupleDescInitEntry(&mut desc, 1, Some("x"), INT4OID, -1, 0)?;
            desc.tdtypeid = COMPOSITE_TYPE;
            desc.tdtypmod = 0;
            Ok(desc)
        });
    });
}

fn dummy(
    _flinfo: Option<&mut FmgrInfo>,
    _fcinfo: &mut ::fmgr::FunctionCallInfoBaseData,
) -> PgResult<Datum> {
    Ok(Datum::from_i32(0))
}

fn flinfo_for(oid: Oid) -> FmgrInfo {
    FmgrInfo::new(dummy, oid, 1, true, false)
}

#[test]
fn scalar_function() {
    install_seams();
    let ctx = MemoryContext::new("t");
    let r = get_call_result_type(ctx.mcx(), &flinfo_for(F_SCALAR), None).unwrap();
    assert_eq!(r.class, TypeFuncClass::Scalar);
    assert_eq!(r.result_type_id, INT4OID);
    assert!(r.result_tuple_desc.is_none());
}

#[test]
fn composite_function_copies_rowtype() {
    install_seams();
    let ctx = MemoryContext::new("t");
    let r = get_func_result_type(ctx.mcx(), F_COMPOSITE).unwrap();
    assert_eq!(r.class, TypeFuncClass::Composite);
    assert_eq!(r.result_type_id, COMPOSITE_TYPE);
    let desc = r.result_tuple_desc.unwrap();
    assert_eq!(desc.natts, 1);
    assert_eq!(desc.attr(0).atttypid, INT4OID);
}

#[test]
fn bare_record_without_context_stays_record() {
    install_seams();
    let ctx = MemoryContext::new("t");
    let r = get_call_result_type(ctx.mcx(), &flinfo_for(F_RECORD_BARE), None).unwrap();
    assert_eq!(r.class, TypeFuncClass::Record);
    assert_eq!(r.result_type_id, RECORDOID);
    assert!(r.result_tuple_desc.is_none());
}

#[test]
fn bare_record_with_expected_desc_resolves_composite() {
    install_seams();
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let mut expected = tupdesc::CreateTemplateTupleDesc(mcx, 1).unwrap();
    tupdesc::TupleDescInitEntry(&mut expected, 1, Some("n"), INT4OID, -1, 0).unwrap();
    let r = get_call_result_type(mcx, &flinfo_for(F_RECORD_BARE), Some(&expected)).unwrap();
    assert_eq!(r.class, TypeFuncClass::Composite);
    let desc = r.result_tuple_desc.unwrap();
    assert_eq!(desc.natts, 1);
    assert_eq!(desc.attr(0).attname.name_str(), b"n");
}

#[test]
fn out_params_build_record_tupdesc() {
    install_seams();
    let ctx = MemoryContext::new("t");
    let r = get_call_result_type(ctx.mcx(), &flinfo_for(F_RECORD_OUT), None).unwrap();
    assert_eq!(r.class, TypeFuncClass::Composite);
    assert_eq!(r.result_type_id, RECORDOID);
    let desc = r.result_tuple_desc.unwrap();
    assert_eq!(desc.natts, 2);
    assert_eq!(desc.attr(0).attname.name_str(), b"b");
    assert_eq!(desc.attr(0).atttypid, INT4OID);
    // Unnamed OUT column gins up "column2" (build_function_result_tupdesc_d).
    assert_eq!(desc.attr(1).attname.name_str(), b"column2");
    assert_eq!(desc.attr(1).atttypid, TEXTOID);
    assert_eq!(desc.tdtypmod, 42, "assign_record_type_typmod stamped the typmod");
}

#[test]
fn single_out_function_is_not_composite() {
    install_seams();
    let ctx = MemoryContext::new("t");
    let r = get_func_result_type(ctx.mcx(), F_RECORD_ONE_OUT).unwrap();
    assert_eq!(r.class, TypeFuncClass::Record);
    assert!(r.result_tuple_desc.is_none());
}

#[test]
fn single_out_procedure_is_composite() {
    install_seams();
    let ctx = MemoryContext::new("t");
    let r = get_func_result_type(ctx.mcx(), P_ONE_OUT).unwrap();
    assert_eq!(r.class, TypeFuncClass::Composite);
    let desc = r.result_tuple_desc.unwrap();
    assert_eq!(desc.natts, 1);
    assert_eq!(desc.attr(0).attname.name_str(), b"column1");
}

#[test]
fn polymorphic_rettype_without_call_expr_errors() {
    install_seams();
    let ctx = MemoryContext::new("t");
    let err = get_func_result_type(ctx.mcx(), F_POLY).unwrap_err();
    assert_eq!(
        err.message(),
        "could not determine actual result type for function \"poly_fn\" \
         declared to return type anyelement"
    );
}

#[test]
fn missing_function_errors() {
    install_seams();
    let ctx = MemoryContext::new("t");
    let err = get_func_result_type(ctx.mcx(), 999999).unwrap_err();
    assert!(err.message().contains("cache lookup failed for function 999999"));
}

#[test]
fn get_type_func_class_pseudo_scalars() {
    install_seams();
    assert_eq!(get_type_func_class(VOIDOID).unwrap().0, TypeFuncClass::Scalar);
    assert_eq!(get_type_func_class(CSTRINGOID).unwrap().0, TypeFuncClass::Scalar);
    assert_eq!(get_type_func_class(RECORDOID).unwrap().0, TypeFuncClass::Record);
}

#[test]
fn multi_func_call_lifecycle() {
    install_seams();
    let mut flinfo = flinfo_for(F_SCALAR);
    let mut fcinfo = LocalFcinfo::<0>::new(0);
    let mut rsinfo = FmNode { tag: NodeTag::T_ReturnSetInfo as u32 };
    fcinfo.resultinfo = Some(core::ptr::NonNull::from(&mut rsinfo));

    let fctx = init_MultiFuncCall(&mut flinfo, &fcinfo).unwrap();
    fctx.max_calls = 3;
    fctx.user_fctx = Some(Box::new(7i32));

    let again = per_MultiFuncCall(&mut flinfo);
    again.call_cntr += 1;
    assert_eq!(again.call_cntr, 1);
    assert_eq!(again.max_calls, 3);
    assert_eq!(*again.user_fctx.as_ref().unwrap().downcast_ref::<i32>().unwrap(), 7);

    let err = init_MultiFuncCall(&mut flinfo, &fcinfo).unwrap_err();
    assert!(err.message().contains("cannot be called more than once"));

    end_MultiFuncCall(&mut flinfo);
    assert!(!flinfo.has_fn_extra());
}

#[test]
fn multi_func_call_requires_rsinfo() {
    install_seams();
    let mut flinfo = flinfo_for(F_SCALAR);
    let fcinfo = LocalFcinfo::<0>::new(0);
    let err = init_MultiFuncCall(&mut flinfo, &fcinfo).unwrap_err();
    assert!(err
        .message()
        .contains("set-valued function called in context that cannot accept a set"));
}

fn materialize_rsinfo(allowed: u32) -> ::fmgr::ReturnSetInfo {
    ::fmgr::ReturnSetInfo::new(allowed)
}

fn int4_pair_desc(mcx: Mcx<'_>) -> TupleDescData<'_> {
    let mut d = tupdesc::CreateTemplateTupleDesc(mcx, 2).unwrap();
    tupdesc::TupleDescInitEntry(&mut d, 1, Some("a"), INT4OID, -1, 0).unwrap();
    tupdesc::TupleDescInitEntry(&mut d, 2, Some("b"), INT4OID, -1, 0).unwrap();
    d
}

#[test]
fn materialized_srf_expected_desc_roundtrip() {
    use ::fmgr::{SetFunctionReturnMode, SFRM_Materialize, SFRM_ValuePerCall};
    install_seams();
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let desc = int4_pair_desc(mcx);

    let mut rsinfo = materialize_rsinfo(SFRM_ValuePerCall | SFRM_Materialize);
    rsinfo.expectedDesc = Some(core::ptr::NonNull::from(&desc).cast());
    let mut flinfo = flinfo_for(F_RECORD_OUT);
    let mut fcinfo = LocalFcinfo::<0>::new(InvalidOid);
    fcinfo.resultinfo = rsinfo.as_fmnode_ptr();

    let mut srf =
        InitMaterializedSRF(mcx, &mut flinfo, &mut fcinfo, MAT_SRF_USE_EXPECTED_DESC).unwrap();
    assert_eq!(srf.tupdesc.natts, 2);
    srf.putvalues(&[Datum::from_i32(1), Datum::from_i32(2)], &[false, false]).unwrap();
    srf.putvalues(&[Datum::from_i32(3), Datum::from_i32(4)], &[false, true]).unwrap();
    let result = srf.finish(&mut fcinfo);
    assert_eq!(result.as_usize(), 0);
    assert!(!fcinfo.isnull);

    assert_eq!(rsinfo.returnMode, SetFunctionReturnMode::Materialize);
    let mut store = *rsinfo
        .setResult
        .take()
        .expect("finish armed setResult")
        .downcast::<::tuplestore::Tuplestore>()
        .expect("setResult is a Tuplestore");
    assert_eq!(store.tuple_count(), 2);

    let mut slot = exectuples::make_tuple_table_slot(
        mcx,
        types_slot::TupleSlotKind::MinimalTuple,
        Some(std::rc::Rc::new(desc)),
    );
    assert!(store.gettupleslot(true, false, &mut slot, mcx).unwrap());
    exectuples::slot_getallattrs(&mut slot);
    assert_eq!(slot.base().tts_values[0].as_i32(), 1);
    assert_eq!(slot.base().tts_values[1].as_i32(), 2);
    assert!(store.gettupleslot(true, false, &mut slot, mcx).unwrap());
    exectuples::slot_getallattrs(&mut slot);
    assert_eq!(slot.base().tts_values[0].as_i32(), 3);
    assert!(slot.base().tts_isnull[1]);
    assert!(!store.gettupleslot(true, false, &mut slot, mcx).unwrap());
    store.end();
}

#[test]
fn materialized_srf_derives_tupdesc_from_pg_proc() {
    use ::fmgr::SFRM_Materialize;
    install_seams();
    let ctx = MemoryContext::new("t");
    let mut rsinfo = materialize_rsinfo(SFRM_Materialize);
    let mut flinfo = flinfo_for(F_RECORD_OUT);
    let mut fcinfo = LocalFcinfo::<0>::new(InvalidOid);
    fcinfo.resultinfo = rsinfo.as_fmnode_ptr();

    let srf = InitMaterializedSRF(ctx.mcx(), &mut flinfo, &mut fcinfo, 0).unwrap();
    assert_eq!(srf.tupdesc.natts, 2);
    assert_eq!(srf.tupdesc.attr(0).attname.name_str(), b"b");
    assert_eq!(srf.tupdesc.attr(1).attname.name_str(), b"column2");
}

#[test]
fn materialized_srf_requires_rsinfo() {
    install_seams();
    let ctx = MemoryContext::new("t");
    let mut flinfo = flinfo_for(F_RECORD_OUT);
    let mut fcinfo = LocalFcinfo::<0>::new(InvalidOid);
    let err = InitMaterializedSRF(ctx.mcx(), &mut flinfo, &mut fcinfo, 0).unwrap_err();
    assert!(err
        .message()
        .contains("set-valued function called in context that cannot accept a set"));
}

#[test]
fn materialized_srf_requires_materialize_mode() {
    use ::fmgr::{SFRM_Materialize, SFRM_ValuePerCall};
    install_seams();
    let ctx = MemoryContext::new("t");
    let mut flinfo = flinfo_for(F_RECORD_OUT);

    let mut rsinfo = materialize_rsinfo(SFRM_ValuePerCall);
    let mut fcinfo = LocalFcinfo::<0>::new(InvalidOid);
    fcinfo.resultinfo = rsinfo.as_fmnode_ptr();
    let err = InitMaterializedSRF(ctx.mcx(), &mut flinfo, &mut fcinfo, 0).unwrap_err();
    assert!(err.message().contains("materialize mode required"));

    // MAT_SRF_USE_EXPECTED_DESC with no expectedDesc is the same error.
    let mut rsinfo = materialize_rsinfo(SFRM_ValuePerCall | SFRM_Materialize);
    let mut fcinfo = LocalFcinfo::<0>::new(InvalidOid);
    fcinfo.resultinfo = rsinfo.as_fmnode_ptr();
    let err = InitMaterializedSRF(ctx.mcx(), &mut flinfo, &mut fcinfo, MAT_SRF_USE_EXPECTED_DESC)
        .unwrap_err();
    assert!(err.message().contains("materialize mode required"));
}

#[test]
fn materialized_srf_rejects_scalar_result() {
    use ::fmgr::SFRM_Materialize;
    install_seams();
    let ctx = MemoryContext::new("t");
    let mut flinfo = flinfo_for(F_SCALAR);
    let mut rsinfo = materialize_rsinfo(SFRM_Materialize);
    let mut fcinfo = LocalFcinfo::<0>::new(InvalidOid);
    fcinfo.resultinfo = rsinfo.as_fmnode_ptr();
    let err = InitMaterializedSRF(ctx.mcx(), &mut flinfo, &mut fcinfo, 0).unwrap_err();
    assert!(err.message().contains("return type must be a row type"));
}

#[test]
fn row_expr_record_builds_blessed_tupdesc() {
    install_seams();
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let args = nodes::NodeList::make2(
        mcx,
        Node::mk_const(mcx, INT4OID, -1, 0, 4, Datum::from_i32(1), false, true).unwrap(),
        Node::mk_const(mcx, TEXTOID, -1, 100, -1, Datum::from_usize(0), true, false).unwrap(),
    )
    .unwrap();
    let colnames = nodes::NodeList::make2(
        mcx,
        Node::mk_string(mcx, "a").unwrap(),
        Node::mk_string(mcx, "b").unwrap(),
    )
    .unwrap();
    let re = Node::mk(
        mcx,
        nodes::primnodes::RowExpr {
            args,
            row_typeid: RECORDOID,
            row_format: nodes::primnodes::CoercionForm::COERCE_EXPLICIT_CALL,
            colnames,
            location: -1,
        },
    )
    .unwrap();
    let r = get_expr_result_type(mcx, Some(re)).unwrap();
    assert_eq!(r.class, TypeFuncClass::Composite);
    assert_eq!(r.result_type_id, RECORDOID);
    let desc = r.result_tuple_desc.unwrap();
    assert_eq!(desc.natts, 2);
    assert_eq!(desc.attr(0).attname.name_str(), b"a");
    assert_eq!(desc.attr(0).atttypid, INT4OID);
    assert_eq!(desc.attr(1).attname.name_str(), b"b");
    assert_eq!(desc.attr(1).atttypid, TEXTOID);
    assert_eq!(desc.attr(1).attcollation, 100);
    // BlessTupleDesc went through the assign_record_type_typmod seam.
    assert_eq!(desc.tdtypmod, 42);
}

#[test]
fn polymorphic_rettype_resolves_via_agg_carrier() {
    install_seams();
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let mut flinfo = flinfo_for(F_POLY);
    static CARRIER_ARGS: [Oid; 1] = [INT4OID];
    let carrier = ::mcx::alloc_leak_in(
        mcx,
        types_core::fmgr::AggFnArgTypes { rettype: INT4OID, argtypes: &CARRIER_ARGS },
    )
    .unwrap();
    // SAFETY: carrier is arena-backed and outlives the flinfo below.
    flinfo.fn_expr =
        Some(unsafe { types_core::fmgr::FnExprErased::from_node_ref(carrier) });
    assert_eq!(get_fn_expr_rettype(&flinfo), INT4OID);
    let r = get_call_result_type(mcx, &flinfo, None).unwrap();
    assert_eq!(r.class, TypeFuncClass::Scalar);
    assert_eq!(r.result_type_id, INT4OID);
}
