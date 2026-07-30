use alloc::rc::Rc;
use std::sync::Once;

use ::datum::Datum;
use ::mcx::{Mcx, MemoryContext, PgBox, PgVec};
use ::types_nodes::list::NodeList;
use ::types_nodes::node_tree::Node;
use ::types_nodes::primnodes::OpExpr;
use ::types_slot::{SlotData, TupleSlotKind};
use ::types_tuple::{
    CompactAttribute, FormData_pg_attribute, PgTypeShape, TupleDescData, TYPALIGN_INT,
    TYPSTORAGE_PLAIN,
};

use crate::compile::{exec_build_projection_info, exec_init_expr, exec_init_qual};
use ::types_portal::params::ParamBind;
use crate::interp::{exec_eval_expr, exec_project, exec_project_returning, exec_qual, EvalSlots, RetSlot, RetSlots};
use crate::steps::{CmpOp, ExprState, Kernel, SlotSrc, Step};

const INT4OID: u32 = 23;
const INT8OID: u32 = 20;
const BOOLOID: u32 = 16;

static SEAMS: Once = Once::new();

fn install_seams() {
    SEAMS.call_once(|| {
        miscinit_seams::get_user_id::set(|| 10);
        namespace_seams::is_temp_namespace::set(|_| false);
        syscache_seams::pg_type_typnamespace::set(|_| Ok(Some(11)));
        syscache_seams::pg_type_element_shape::set(|typid| {
            Ok((typid == 1007).then(|| syscache_seams::PgTypeElementShape {
                typelem: 23,
                typsubscript: lsyscache::F_ARRAY_SUBSCRIPT_HANDLER,
            }))
        });
        aclchk_seams::object_aclcheck::set(|_classid, _objid, _roleid, _mode| Ok(0));
        syscache_seams::lookup_pg_type_shape::set(|typid| {
            Ok(match typid {
                INT4OID => Some(PgTypeShape {
                    typlen: 4,
                    typbyval: true,
                    typalign: TYPALIGN_INT,
                    typstorage: TYPSTORAGE_PLAIN,
                    typcollation: 0,
                }),
                BOOLOID => Some(PgTypeShape {
                    typlen: 1,
                    typbyval: true,
                    typalign: b'c' as i8,
                    typstorage: TYPSTORAGE_PLAIN,
                    typcollation: 0,
                }),
                _ => None,
            })
        });
        // Minimal typcache backing for TYPECACHE_CMP_PROC on int4 (MinMax).
        const INT4_BTREE_OPCLASS: u32 = 1978;
        const INT_BTREE_FAM: u32 = 1976;
        const F_BTINT4CMP: u32 = 351;
        syscache_seams::lookup_pg_type_typcache_shape::set(|typid| {
            Ok(match typid {
                INT4OID | DOMAIN_OID => {
                    let mut name = ::types_tuple::NameData::default();
                    name.namestrcpy(if typid == INT4OID { "int4" } else { "posint" });
                    Some(syscache_seams::PgTypeTypcacheShape {
                        typname: name,
                        typlen: 4,
                        typbyval: true,
                        typalign: b'i' as i8,
                        typstorage: b'p' as i8,
                        typtype: if typid == INT4OID { b'b' as i8 } else { b'd' as i8 },
                        typisdefined: true,
                        typrelid: 0,
                        typsubscript: 0,
                        typelem: 0,
                        typarray: 0,
                        typcollation: 0,
                    })
                }
                _ => None,
            })
        });
        syscache_seams::syscache_hash_value_typeoid::set(|typid| Ok(typid.wrapping_mul(0x9e3779b1)));
        syscache_seams::lookup_pg_opclass_shape::set(|opclass| {
            Ok((opclass == INT4_BTREE_OPCLASS).then_some(syscache_seams::PgOpclassShape {
                opcmethod: ::types_core::BTREE_AM_OID,
                opcfamily: INT_BTREE_FAM,
                opcintype: INT4OID,
                // int4_ops stores no separate key type (pg_opclass: 0).
                opckeytype: ::types_core::InvalidOid,
            }))
        });
        syscache_seams::lookup_pg_amproc::set(|opfamily, _l, _r, procnum| {
            Ok(if opfamily == INT_BTREE_FAM && procnum == 1 { F_BTINT4CMP } else { 0 })
        });
        indexcmds_seams::get_default_opclass::set(|type_id, am_id| {
            Ok(if type_id == INT4OID && am_id == ::types_core::BTREE_AM_OID {
                INT4_BTREE_OPCLASS
            } else {
                0
            })
        });
        install_domain_seams();
        install_json_seams();
    });
}

const TEXTOID_T: u32 = 25;
const JSONBOID_T: u32 = 3802;
const JSONPATHOID_T: u32 = 4072;

fn install_json_seams() {
    let _ = mbutils::SetDatabaseEncoding(wchar::PG_UTF8);
    mbutils::init_seams();
    // json_populate_type resolves input functions through fmgr_seams.
    fmgr_core::init_seams();
    postgres_seams::check_for_interrupts::set(|| Ok(()));
    syscache_seams::pg_type_typtype::set(|typid| {
        Ok(match typid {
            INT4OID | BOOLOID | TEXTOID_T | JSONBOID_T | JSONPATHOID_T => Some(b'b' as i8),
            DOMAIN_OID => Some(b'd' as i8),
            _ => None,
        })
    });
    syscache_seams::pg_type_base_shape::set(|typid| {
        Ok(matches!(typid, INT4OID | BOOLOID | TEXTOID_T | JSONBOID_T).then_some(
            syscache_seams::PgTypeBaseShape {
                typtype: b'b' as i8,
                typbasetype: 0,
                typtypmod: -1,
                typelem: 0,
                typsubscript: 0,
            },
        ))
    });
    syscache_seams::pg_type_io_shape::set(|typid| {
        let mk = |typinput, typoutput, typlen, typbyval| syscache_seams::PgTypeIoShape {
            oid: typid,
            typinput,
            typoutput,
            typreceive: 0,
            typsend: 0,
            typmodin: 0,
            typmodout: 0,
            typelem: 0,
            typlen,
            typbyval,
            typalign: b'i' as i8,
            typdelim: b',' as i8,
            typisdefined: true,
        };
        Ok(match typid {
            INT4OID => Some(mk(42, 43, 4, true)),
            TEXTOID_T => Some(mk(46, 47, -1, false)),
            JSONBOID_T => Some(mk(3806, 3805, -1, false)),
            _ => None,
        })
    });
}

const DOMAIN_OID: u32 = 90001;
const CONBIN_VALUE_GT_0: &str = "{OPEXPR :opno 521 :opfuncid 147 :opresulttype 16 \
    :opretset false :opcollid 0 :inputcollid 0 :args ({COERCETODOMAINVALUE \
    :typeId 23 :typeMod -1 :collation 0 :location 47} {CONST :consttype 23 \
    :consttypmod -1 :constcollid 0 :constlen 4 :constbyval true :constisnull \
    false :location 55 :constvalue 4 [ 0 0 0 0 0 0 0 0 ]}) :location 53}";

fn install_domain_seams() {
    clauses::init_seams();
    syscache_seams::pg_type_domain_shape::set(|typid| {
        let mk = |nm: &str, nsp, tt, nn, base| {
            let mut n = ::types_tuple::NameData::default();
            n.namestrcpy(nm);
            syscache_seams::PgTypeDomainShape {
                typname: n,
                typnamespace: nsp,
                typtype: tt,
                typnotnull: nn,
                typbasetype: base,
            }
        };
        Ok(match typid {
            DOMAIN_OID => Some(mk("posint", 2200, b'd' as i8, true, INT4OID)),
            INT4OID => Some(mk("int4", 11, b'b' as i8, false, 0)),
            _ => None,
        })
    });
    typcache_seams::scan_domain_check_constraints::set(|mcx, contypid| {
        let mut rows = ::mcx::vec_with_capacity_in(mcx, 1)?;
        if contypid == DOMAIN_OID {
            let mut cn = ::types_tuple::NameData::default();
            cn.namestrcpy("posint_check");
            rows.push(typcache_seams::DomainCheckRow { conname: cn, conbin: CONBIN_VALUE_GT_0 });
        }
        Ok(rows)
    });
    syscache_seams::lookup_pg_proc_shape::set(|funcid| {
        Ok((funcid == 147).then_some(syscache_seams::PgProcShape {
            prolang: 12,
            prosecdef: false,
            proconfig_isnull: true,
            pronamespace: 11,
            prorettype: BOOLOID,
            provariadic: 0,
            prosupport: 0,
            pronargs: 2,
            prokind: b'f' as i8,
            provolatile: b'i' as i8,
            proparallel: b's' as i8,
            proretset: false,
            proisstrict: true,
            proleakproof: false,
        }))
    });
    namespace_seams::type_is_visible::set(|typid| Ok(typid == DOMAIN_OID));
    syscache_seams::pg_namespace_nspname::set(|nspid| {
        let mut n = ::types_tuple::NameData::default();
        n.namestrcpy(if nspid == 2200 { "public" } else { "pg_catalog" });
        Ok(Some(n))
    });
}

fn with_mcx<R>(f: impl for<'m> FnOnce(Mcx<'m>) -> R) -> R {
    install_seams();
    let ctx = MemoryContext::new("execexpr-test");
    f(ctx.mcx())
}

fn desc_int4<'mcx>(mcx: Mcx<'mcx>, natts: i32) -> Rc<TupleDescData<'mcx>> {
    let mut attrs = PgVec::new_in(mcx);
    let mut compact = PgVec::new_in(mcx);
    for i in 0..natts {
        let att = FormData_pg_attribute {
            attnum: (i + 1) as i16,
            atttypid: INT4OID,
            attlen: 4,
            attbyval: true,
            attalign: TYPALIGN_INT,
            attstorage: TYPSTORAGE_PLAIN,
            ..Default::default()
        };
        compact.push(CompactAttribute::populate_from(&att));
        attrs.push(att);
    }
    Rc::new(TupleDescData {
        natts,
        tdtypeid: 0,
        tdtypmod: -1,
        tdrefcount: -1,
        constr: None,
        compact_attrs: compact,
        attrs,
    })
}

fn virtual_slot<'mcx>(mcx: Mcx<'mcx>, values: &[Option<i32>]) -> SlotData<'mcx> {
    let mut slot = exectuples::make_tuple_table_slot(
        mcx,
        TupleSlotKind::Virtual,
        Some(desc_int4(mcx, values.len() as i32)),
    );
    {
        let base = slot.base_mut();
        for (i, v) in values.iter().enumerate() {
            match v {
                Some(x) => {
                    base.tts_values[i] = Datum::from_i32(*x);
                    base.tts_isnull[i] = false;
                }
                None => {
                    base.tts_values[i] = Datum::null();
                    base.tts_isnull[i] = true;
                }
            }
        }
    }
    exectuples::exec_store_virtual_tuple(&mut slot);
    slot
}

fn heap_slot<'mcx>(mcx: Mcx<'mcx>, values: &[Option<i32>]) -> SlotData<'mcx> {
    let desc = desc_int4(mcx, values.len() as i32);
    let mut vals = PgVec::new_in(mcx);
    let mut nulls = PgVec::new_in(mcx);
    for v in values {
        vals.push(v.map_or(Datum::null(), Datum::from_i32));
        nulls.push(v.is_none());
    }
    let tuple = heaptuple::heap_form_tuple(mcx, &desc, &vals, &nulls).unwrap();
    let mut slot = exectuples::make_tuple_table_slot(mcx, TupleSlotKind::HeapTuple, Some(desc));
    exectuples::exec_store_heap_tuple_owned(&mut slot, mcx, tuple);
    slot
}

fn mk_scan_var<'mcx>(mcx: Mcx<'mcx>, attno: i16, typ: u32) -> Node<'mcx> {
    Node::mk_var(mcx, 1, attno, typ, -1, 0, 0).unwrap()
}

fn mk_ret_var<'mcx>(
    mcx: Mcx<'mcx>,
    attno: i16,
    rtype: ::types_nodes::primnodes::VarReturningType,
) -> Node<'mcx> {
    use ::types_nodes::bitmapset::Bitmapset;
    use ::types_nodes::primnodes::Var;
    Node::mk(
        mcx,
        Var {
            varno: 1,
            varattno: attno,
            vartype: INT4OID,
            vartypmod: -1,
            varcollid: 0,
            varnullingrels: Bitmapset::empty(),
            varlevelsup: 0,
            varreturningtype: rtype,
            varnosyn: 1,
            varattnosyn: attno,
            location: -1,
        },
    )
    .unwrap()
}

fn mk_int4_const<'mcx>(mcx: Mcx<'mcx>, v: Option<i32>) -> Node<'mcx> {
    Node::mk_const(
        mcx,
        INT4OID,
        -1,
        0,
        4,
        v.map_or(Datum::null(), Datum::from_i32),
        v.is_none(),
        true,
    )
    .unwrap()
}

fn mk_opexpr<'mcx>(
    mcx: Mcx<'mcx>,
    opfuncid: u32,
    resulttype: u32,
    args: NodeList<'mcx>,
) -> Node<'mcx> {
    Node::mk(
        mcx,
        OpExpr {
            opno: 0,
            opfuncid,
            opresulttype: resulttype,
            opretset: false,
            opcollid: 0,
            inputcollid: 0,
            args,
            location: -1,
        },
    )
    .unwrap()
}

fn mk_null_if_expr<'mcx>(
    mcx: Mcx<'mcx>,
    opfuncid: u32,
    resulttype: u32,
    args: NodeList<'mcx>,
) -> Node<'mcx> {
    Node::mk(
        mcx,
        ::types_nodes::NullIfExpr {
            opno: 0,
            opfuncid,
            opresulttype: resulttype,
            opretset: false,
            opcollid: 0,
            inputcollid: 0,
            args,
            location: -1,
        },
    )
    .unwrap()
}

fn qual_state<'mcx>(mcx: Mcx<'mcx>, expr: Node<'mcx>) -> PgBox<'mcx, ExprState<'mcx>> {
    let qual = NodeList::make1(mcx, expr).unwrap();
    exec_init_qual(mcx, &qual, ParamBind::NONE).unwrap().unwrap()
}

fn run_qual<'mcx>(mcx: Mcx<'mcx>, state: &mut ExprState<'mcx>, values: &[Option<i32>]) -> bool {
    let mut slot = virtual_slot(mcx, values);
    let mut slots = EvalSlots { scan: Some(&mut slot), inner: None, outer: None };
    exec_qual(Some(state), &mut slots).unwrap()
}

fn run_qual_heap<'mcx>(
    mcx: Mcx<'mcx>,
    state: &mut ExprState<'mcx>,
    values: &[Option<i32>],
) -> bool {
    let mut slot = heap_slot(mcx, values);
    let mut slots = EvalSlots { scan: Some(&mut slot), inner: None, outer: None };
    exec_qual(Some(state), &mut slots).unwrap()
}

#[test]
fn empty_qual_is_true() {
    with_mcx(|mcx| {
        let qual = NodeList::default();
        assert!(exec_init_qual(mcx, &qual, ParamBind::NONE).unwrap().is_none());
        let mut slots = EvalSlots::default();
        assert!(exec_qual(None, &mut slots).unwrap());
    });
}

#[test]
fn just_const_expr() {
    with_mcx(|mcx| {
        let mut state = exec_init_expr(mcx, Some(mk_int4_const(mcx, Some(42))), ParamBind::NONE).unwrap().unwrap();
        assert!(matches!(state.kernel(), Kernel::JustConst { .. }));
        assert_eq!(state.steps().len(), 2);
        let mut slots = EvalSlots::default();
        let r = exec_eval_expr(&mut state, &mut slots).unwrap();
        assert!(!r.isnull);
        assert_eq!(r.value.as_i32(), 42);
    });
}

#[test]
fn select1_projection_fused_kernel() {
    with_mcx(|mcx| {
        let tle = Node::mk_target_entry(mcx, mk_int4_const(mcx, Some(1)), 1, None, false).unwrap();
        let tlist = NodeList::make1(mcx, tle).unwrap();
        let mut state = exec_build_projection_info(mcx, &tlist, None, ParamBind::NONE).unwrap();
        assert!(matches!(state.kernel(), Kernel::JustConstAssign { resultnum: 0, .. }));

        let mut result =
            exectuples::make_tuple_table_slot(mcx, TupleSlotKind::Virtual, Some(desc_int4(mcx, 1)));
        let mut slots = EvalSlots::default();
        exec_project(&mut state, &mut slots, &mut result, mcx).unwrap();
        let base = result.base();
        assert_eq!(base.tts_nvalid, 1);
        assert_eq!(base.tts_values[0].as_i32(), 1);
        assert!(!base.tts_isnull[0]);
        assert!(!base.is_empty());
    });
}

#[test]
fn fused_qual_kernel_var_eq_const() {
    with_mcx(|mcx| {
        let args = NodeList::make2(mcx, mk_scan_var(mcx, 1, INT4OID), mk_int4_const(mcx, Some(7)))
            .unwrap();
        let mut state = qual_state(mcx, mk_opexpr(mcx, 65, BOOLOID, args));
        assert!(matches!(
            state.kernel(),
            Kernel::QualScanVarCmpConst { attnum: 0, cmp: CmpOp::Int4Eq, .. }
        ));
        assert!(run_qual(mcx, &mut state, &[Some(7)]));
        assert!(!run_qual(mcx, &mut state, &[Some(8)]));
        assert!(!run_qual(mcx, &mut state, &[None]));
    });
}

#[test]
fn fused_qual_kernel_commuted_const_lt_var() {
    with_mcx(|mcx| {
        let args = NodeList::make2(mcx, mk_int4_const(mcx, Some(5)), mk_scan_var(mcx, 1, INT4OID))
            .unwrap();
        let mut state = qual_state(mcx, mk_opexpr(mcx, 66, BOOLOID, args));
        assert!(matches!(
            state.kernel(),
            Kernel::QualScanVarCmpConst { cmp: CmpOp::Int4Gt, .. }
        ));
        assert!(run_qual(mcx, &mut state, &[Some(6)]));
        assert!(!run_qual(mcx, &mut state, &[Some(5)]));
        assert!(!run_qual(mcx, &mut state, &[Some(4)]));
    });
}

#[test]
fn interpreter_path_matches_fused_kernel() {
    with_mcx(|mcx| {
        for vals in [Some(7), Some(8), Some(-7), None] {
            let args =
                NodeList::make2(mcx, mk_scan_var(mcx, 1, INT4OID), mk_int4_const(mcx, Some(7)))
                    .unwrap();
            let mut fused = qual_state(mcx, mk_opexpr(mcx, 65, BOOLOID, args));
            let args =
                NodeList::make2(mcx, mk_scan_var(mcx, 1, INT4OID), mk_int4_const(mcx, Some(7)))
                    .unwrap();
            let mut interp = qual_state(mcx, mk_opexpr(mcx, 65, BOOLOID, args));
            interp.force_program_kernel();
            assert_eq!(
                run_qual(mcx, &mut fused, &[vals]),
                run_qual(mcx, &mut interp, &[vals]),
                "value {vals:?}"
            );
        }
    });
}

#[test]
fn qual_deforms_heap_tuple_through_slot_lanes() {
    with_mcx(|mcx| {
        let args = NodeList::make2(mcx, mk_scan_var(mcx, 3, INT4OID), mk_int4_const(mcx, Some(9)))
            .unwrap();
        let mut state = qual_state(mcx, mk_opexpr(mcx, 65, BOOLOID, args));
        assert!(run_qual_heap(mcx, &mut state, &[Some(1), Some(2), Some(9)]));
        assert!(!run_qual_heap(mcx, &mut state, &[Some(1), Some(2), Some(8)]));
        assert!(!run_qual_heap(mcx, &mut state, &[Some(1), None, None]));
    });
}

#[test]
fn multi_qual_short_circuits() {
    with_mcx(|mcx| {
        let q1 = mk_opexpr(
            mcx,
            65,
            BOOLOID,
            NodeList::make2(mcx, mk_scan_var(mcx, 1, INT4OID), mk_int4_const(mcx, Some(1)))
                .unwrap(),
        );
        let q2 = mk_opexpr(
            mcx,
            147,
            BOOLOID,
            NodeList::make2(mcx, mk_scan_var(mcx, 2, INT4OID), mk_int4_const(mcx, Some(10)))
                .unwrap(),
        );
        let qual = NodeList::make2(mcx, q1, q2).unwrap();
        let mut state = exec_init_qual(mcx, &qual, ParamBind::NONE).unwrap().unwrap();
        assert!(matches!(state.kernel(), Kernel::Program));
        assert!(run_qual(mcx, &mut state, &[Some(1), Some(11)]));
        assert!(!run_qual(mcx, &mut state, &[Some(1), Some(10)]));
        assert!(!run_qual(mcx, &mut state, &[Some(2), Some(11)]));
        assert!(!run_qual(mcx, &mut state, &[None, Some(11)]));
    });
}

#[test]
fn just_func_kernel_const_args() {
    with_mcx(|mcx| {
        let args =
            NodeList::make2(mcx, mk_int4_const(mcx, Some(40)), mk_int4_const(mcx, Some(2)))
                .unwrap();
        let mut state = exec_init_expr(mcx, Some(mk_opexpr(mcx, 177, INT4OID, args)), ParamBind::NONE)
            .unwrap()
            .unwrap();
        assert!(matches!(state.kernel(), Kernel::JustFunc { nargs: 2, strict: true, .. }));
        let mut slots = EvalSlots::default();
        let r = exec_eval_expr(&mut state, &mut slots).unwrap();
        assert_eq!(r.value.as_i32(), 42);
        assert!(!r.isnull);
    });
}

#[test]
fn nullif_equal_args_returns_null() {
    with_mcx(|mcx| {
        let args =
            NodeList::make2(mcx, mk_int4_const(mcx, Some(1)), mk_int4_const(mcx, Some(1)))
                .unwrap();
        let mut state =
            exec_init_expr(mcx, Some(mk_null_if_expr(mcx, 65, INT4OID, args)), ParamBind::NONE)
                .unwrap()
                .unwrap();
        assert!(matches!(state.steps()[0], Step::NullIf { .. }));
        let mut slots = EvalSlots::default();
        let r = exec_eval_expr(&mut state, &mut slots).unwrap();
        assert!(r.isnull);
    });
}

#[test]
fn nullif_unequal_args_returns_first() {
    with_mcx(|mcx| {
        let args =
            NodeList::make2(mcx, mk_int4_const(mcx, Some(1)), mk_int4_const(mcx, Some(2)))
                .unwrap();
        let mut state =
            exec_init_expr(mcx, Some(mk_null_if_expr(mcx, 65, INT4OID, args)), ParamBind::NONE)
                .unwrap()
                .unwrap();
        let mut slots = EvalSlots::default();
        let r = exec_eval_expr(&mut state, &mut slots).unwrap();
        assert!(!r.isnull);
        assert_eq!(r.value.as_i32(), 1);
    });
}

#[test]
fn nullif_null_arg_returns_first_unevaluated() {
    with_mcx(|mcx| {
        let args =
            NodeList::make2(mcx, mk_int4_const(mcx, None), mk_int4_const(mcx, Some(2))).unwrap();
        let mut state =
            exec_init_expr(mcx, Some(mk_null_if_expr(mcx, 65, INT4OID, args)), ParamBind::NONE)
                .unwrap()
                .unwrap();
        let mut slots = EvalSlots::default();
        let r = exec_eval_expr(&mut state, &mut slots).unwrap();
        assert!(r.isnull);
    });
}

#[test]
fn just_func_kernel_strict_null_const() {
    with_mcx(|mcx| {
        let args =
            NodeList::make2(mcx, mk_int4_const(mcx, Some(40)), mk_int4_const(mcx, None)).unwrap();
        let mut state = exec_init_expr(mcx, Some(mk_opexpr(mcx, 177, INT4OID, args)), ParamBind::NONE)
            .unwrap()
            .unwrap();
        let mut slots = EvalSlots::default();
        let r = exec_eval_expr(&mut state, &mut slots).unwrap();
        assert!(r.isnull);
    });
}

// Miri repro: the Hash32Var arg write must not invalidate the fcinfo reborrow.
#[test]
fn hash32_var_kernel_arg_write_then_call() {
    with_mcx(|mcx| {
        let desc = desc_int4(mcx, 1);
        let mut state =
            crate::compile::exec_build_hash32_from_attrs(mcx, &desc, &[450], &[0], &[1], 0)
                .unwrap();
        assert!(matches!(state.kernel(), Kernel::Hash32Var { .. }));
        fn hash_of<'m>(mcx: Mcx<'m>, state: &mut ExprState<'m>, v: Option<i32>) -> u32 {
            let mut slot = virtual_slot(mcx, &[v]);
            let mut slots = EvalSlots { scan: None, inner: Some(&mut slot), outer: None };
            let r = exec_eval_expr(state, &mut slots).unwrap();
            assert!(!r.isnull);
            r.value.as_u32()
        }
        let h42 = hash_of(mcx, &mut state, Some(42));
        assert_eq!(h42, hash_of(mcx, &mut state, Some(42)));
        assert_ne!(h42, hash_of(mcx, &mut state, Some(7)));
        assert_eq!(hash_of(mcx, &mut state, None), 0);
    });
}

#[test]
fn func_strict2_with_var_arg_null_propagation() {
    with_mcx(|mcx| {
        let args = NodeList::make2(mcx, mk_scan_var(mcx, 1, INT4OID), mk_int4_const(mcx, Some(2)))
            .unwrap();
        let mut state = exec_init_expr(mcx, Some(mk_opexpr(mcx, 177, INT4OID, args)), ParamBind::NONE)
            .unwrap()
            .unwrap();
        assert!(matches!(state.kernel(), Kernel::Program));

        let mut slot = virtual_slot(mcx, &[Some(40)]);
        let mut slots = EvalSlots { scan: Some(&mut slot), inner: None, outer: None };
        let r = exec_eval_expr(&mut state, &mut slots).unwrap();
        assert_eq!(r.value.as_i32(), 42);

        let mut slot = virtual_slot(mcx, &[None]);
        let mut slots = EvalSlots { scan: Some(&mut slot), inner: None, outer: None };
        let r = exec_eval_expr(&mut state, &mut slots).unwrap();
        assert!(r.isnull);
    });
}

#[test]
fn nested_funcexpr_two_frames() {
    with_mcx(|mcx| {
        let inner_args =
            NodeList::make2(mcx, mk_scan_var(mcx, 1, INT4OID), mk_int4_const(mcx, Some(1)))
                .unwrap();
        let inner = mk_opexpr(mcx, 177, INT4OID, inner_args);
        let outer_args = NodeList::make2(mcx, inner, mk_int4_const(mcx, Some(2))).unwrap();
        let mut state = exec_init_expr(mcx, Some(mk_opexpr(mcx, 177, INT4OID, outer_args)), ParamBind::NONE)
            .unwrap()
            .unwrap();

        let mut slot = virtual_slot(mcx, &[Some(39)]);
        let mut slots = EvalSlots { scan: Some(&mut slot), inner: None, outer: None };
        let r = exec_eval_expr(&mut state, &mut slots).unwrap();
        assert_eq!(r.value.as_i32(), 42);
    });
}

#[test]
fn just_var_kernel_reads_deformed_lane() {
    with_mcx(|mcx| {
        let mut state = exec_init_expr(mcx, Some(mk_scan_var(mcx, 2, INT4OID)), ParamBind::NONE)
            .unwrap()
            .unwrap();
        assert!(matches!(state.kernel(), Kernel::JustVar { src: SlotSrc::Scan, attnum: 1 }));

        let mut slot = heap_slot(mcx, &[Some(5), Some(6)]);
        let mut slots = EvalSlots { scan: Some(&mut slot), inner: None, outer: None };
        let r = exec_eval_expr(&mut state, &mut slots).unwrap();
        assert_eq!(r.value.as_i32(), 6);
    });
}

#[test]
fn projection_safe_var_kernel_and_assign_tmp_path() {
    with_mcx(|mcx| {
        let desc = desc_int4(mcx, 2);
        let tle = Node::mk_target_entry(mcx, mk_scan_var(mcx, 2, INT4OID), 1, None, false).unwrap();
        let tlist = NodeList::make1(mcx, tle).unwrap();
        let mut state = exec_build_projection_info(mcx, &tlist, Some(&desc), ParamBind::NONE).unwrap();
        assert!(matches!(
            state.kernel(),
            Kernel::JustAssignVar { src: SlotSrc::Scan, attnum: 1, resultnum: 0 }
        ));

        let mut scan = heap_slot(mcx, &[Some(3), Some(4)]);
        let mut result =
            exectuples::make_tuple_table_slot(mcx, TupleSlotKind::Virtual, Some(desc_int4(mcx, 1)));
        let mut slots = EvalSlots { scan: Some(&mut scan), inner: None, outer: None };
        exec_project(&mut state, &mut slots, &mut result, mcx).unwrap();
        assert_eq!(result.base().tts_values[0].as_i32(), 4);

        // vartype mismatch vs input desc -> generic ASSIGN_TMP path.
        let tle =
            Node::mk_target_entry(mcx, mk_scan_var(mcx, 2, INT8OID), 1, None, false).unwrap();
        let tlist = NodeList::make1(mcx, tle).unwrap();
        let state = exec_build_projection_info(mcx, &tlist, Some(&desc), ParamBind::NONE).unwrap();
        assert!(matches!(state.steps()[2], Step::AssignTmp { resultnum: 0 }));
    });
}

#[test]
fn still_valid_check_rejects_type_mismatch() {
    with_mcx(|mcx| {
        let mut state = exec_init_expr(mcx, Some(mk_scan_var(mcx, 1, INT8OID)), ParamBind::NONE)
            .unwrap()
            .unwrap();
        let mut slot = virtual_slot(mcx, &[Some(1)]);
        let mut slots = EvalSlots { scan: Some(&mut slot), inner: None, outer: None };
        let err = exec_eval_expr(&mut state, &mut slots).unwrap_err();
        // CheckVarSlotCompatibility's C-exact wrong-type message (B5).
        assert!(err.message().contains("has wrong type"), "got: {}", err.message());
        assert!(
            err.detail().is_some_and(|d| d.starts_with("Table has type ")
                && d.contains(", but query expects ")),
            "got detail: {:?}",
            err.detail()
        );
    });
}

#[test]
fn step_footprint_and_program_shapes() {
    assert!(core::mem::size_of::<Step>() <= 64);
    assert!(core::mem::size_of::<Kernel>() <= 48);
    with_mcx(|mcx| {
        let args = NodeList::make2(mcx, mk_scan_var(mcx, 1, INT4OID), mk_int4_const(mcx, Some(7)))
            .unwrap();
        let mut state = qual_state(mcx, mk_opexpr(mcx, 65, BOOLOID, args));
        assert_eq!(state.steps().len(), 5);
        assert!(matches!(state.steps()[2], Step::FuncExprStrict2 { .. }));
        state.force_program_kernel();
        let shapes: alloc::vec::Vec<core::mem::Discriminant<Step>> =
            state.steps().iter().map(core::mem::discriminant).collect();
        assert_eq!(state.steps().len(), 4);
        assert!(matches!(state.steps()[0], Step::ScanFetchSome { last_var: 1 }));
        assert!(matches!(
            state.steps()[1],
            Step::ScanVarFuncStrict2Thin { attnum: 0, argno: 0, .. }
        ));
        assert!(matches!(state.steps()[2], Step::Qual { jumpdone: 3 }));
        assert!(matches!(state.steps()[3], Step::DoneReturn));
        assert_eq!(shapes.len(), 4);
    });
}

#[test]
fn cmp_op_semantics_match_int_c() {
    assert!(CmpOp::Int4Eq.eval(Datum::from_i32(-1), Datum::from_i32(-1)));
    assert!(CmpOp::Int4Lt.eval(Datum::from_i32(i32::MIN), Datum::from_i32(i32::MAX)));
    assert!(!CmpOp::Int4Gt.eval(Datum::from_i32(i32::MIN), Datum::from_i32(i32::MAX)));
    assert!(CmpOp::Int8Le.eval(Datum::from_i64(i64::MIN), Datum::from_i64(i64::MIN)));
    assert!(CmpOp::Int84Gt.eval(Datum::from_i64(1 << 40), Datum::from_i32(5)));
    assert!(CmpOp::Int48Lt.eval(Datum::from_i32(5), Datum::from_i64(1 << 40)));
    assert!(CmpOp::Int2Ge.eval(Datum::from_i16(-5), Datum::from_i16(-5)));
    for (op, com) in [
        (CmpOp::Int4Lt, CmpOp::Int4Gt),
        (CmpOp::Int84Lt, CmpOp::Int48Gt),
        (CmpOp::Int48Eq, CmpOp::Int84Eq),
    ] {
        assert_eq!(op.commuted(), com);
    }
}

// New agg steps under Miri: trans program (strict count + non-strict sum
// shapes) advancing pergroup in place, and AggrefEval projecting the results.
#[test]
fn agg_trans_and_aggref_eval_steps() {
    use core::ptr::NonNull;

    use crate::compile::{
        exec_build_agg_projection_info, exec_build_agg_trans, AggBind, AggTransSpec,
    };
    use crate::steps::AggPerGroup;
    use ::types_nodes::primnodes::{Aggref, OUTER_VAR};

    with_mcx(|mcx| {
        let mut pergroup = [
            AggPerGroup {
                trans_value: Datum::from_i64(0),
                trans_value_is_null: false,
                no_trans_value: false,
            },
            AggPerGroup {
                trans_value: Datum::null(),
                trans_value_is_null: true,
                no_trans_value: true,
            },
        ];
        let base = NonNull::new(pergroup.as_mut_ptr()).unwrap();
        let empty_args = NodeList::nil();
        let var = Node::mk_var(mcx, OUTER_VAR, 1, INT4OID, -1, 0, 0).unwrap();
        let arg_tle = Node::mk_target_entry(mcx, var, 1, None, false).unwrap();
        let sum_args = NodeList::make1(mcx, arg_tle).unwrap();
        let specs = [
            // count(*): int8inc (1219), strict, non-null init, 0 inputs.
            AggTransSpec {
                combine: false,
                deserialfn_oid: 0,
                arg_types: &[],
                transtype_byval: true,
                transtype_len: 8,
                transfn_oid: 1219,
                inputcollid: 0,
                init_value_is_null: false,
                args: &empty_args,
                aggfilter: None,
                pergroup: base,
                ordered: None,
                cur_agg: None,
            },
            // sum(int4): int4_sum (1841), non-strict, null init, 1 input.
            AggTransSpec {
                combine: false,
                deserialfn_oid: 0,
                arg_types: &[],
                transtype_byval: true,
                transtype_len: 8,
                transfn_oid: 1841,
                inputcollid: 0,
                init_value_is_null: true,
                args: &sum_args,
                aggfilter: None,
                // SAFETY: index 1 of the 2-element local array.
                pergroup: unsafe { NonNull::new_unchecked(base.as_ptr().add(1)) },
                ordered: None,
                cur_agg: None,
            },
        ];
        let mut trans = exec_build_agg_trans(mcx, &specs, None, ParamBind::NONE).unwrap();
        for v in [7i32, 35] {
            let mut outer = virtual_slot(mcx, &[Some(v)]);
            let mut slots =
                EvalSlots { scan: None, inner: None, outer: Some(&mut outer) };
            crate::exec_eval_expr(&mut trans, &mut slots).unwrap();
        }
        assert_eq!(pergroup[0].trans_value.as_i64(), 2);
        assert!(!pergroup[0].trans_value_is_null);
        assert_eq!(pergroup[1].trans_value.as_i64(), 42);
        assert!(!pergroup[1].trans_value_is_null);

        let mut aggvalues = [pergroup[0].trans_value, pergroup[1].trans_value];
        let mut aggnulls = [false, false];
        let bind = AggBind {
            values: NonNull::new(aggvalues.as_mut_ptr()).unwrap(),
            nulls: NonNull::new(aggnulls.as_mut_ptr()).unwrap(),
            naggs: 2,
            grouping: None,
        };
        let mut agg0 = Node::build::<Aggref>(mcx).unwrap();
        agg0.aggfnoid = 2803;
        agg0.aggtype = INT8OID;
        agg0.aggno = 0;
        let mut agg1 = Node::build::<Aggref>(mcx).unwrap();
        agg1.aggfnoid = 2108;
        agg1.aggtype = INT8OID;
        agg1.aggno = 1;
        let tle0 = Node::mk_target_entry(mcx, agg0.seal(), 1, None, false).unwrap();
        let tle1 = Node::mk_target_entry(mcx, agg1.seal(), 2, None, false).unwrap();
        let tlist = NodeList::make2(mcx, tle0, tle1).unwrap();
        let mut proj = exec_build_agg_projection_info(mcx, &tlist, None, bind, ParamBind::NONE).unwrap();
        let mut result = exectuples::make_tuple_table_slot(
            mcx,
            TupleSlotKind::Virtual,
            Some(desc_int4(mcx, 2)),
        );
        let mut slots = EvalSlots { scan: None, inner: None, outer: None };
        crate::exec_project(&mut proj, &mut slots, &mut result, mcx).unwrap();
        let rbase = result.base();
        assert_eq!(rbase.tts_values[0].as_i64(), 2);
        assert_eq!(rbase.tts_values[1].as_i64(), 42);
        assert!(!rbase.tts_isnull[0] && !rbase.tts_isnull[1]);
    });
}

#[test]
fn agg_trans_strict_input_check_skips_nulls() {
    use core::ptr::NonNull;

    use crate::compile::{exec_build_agg_trans, AggTransSpec};
    use crate::steps::AggPerGroup;
    use ::types_nodes::primnodes::OUTER_VAR;

    with_mcx(|mcx| {
        let mut pergroup = [
            AggPerGroup {
                trans_value: Datum::from_i64(0),
                trans_value_is_null: false,
                no_trans_value: false,
            },
            AggPerGroup {
                trans_value: Datum::null(),
                trans_value_is_null: true,
                no_trans_value: true,
            },
        ];
        let base = NonNull::new(pergroup.as_mut_ptr()).unwrap();
        let var = Node::mk_var(mcx, OUTER_VAR, 1, INT4OID, -1, 0, 0).unwrap();
        let count_args =
            NodeList::make1(mcx, Node::mk_target_entry(mcx, var, 1, None, false).unwrap())
                .unwrap();
        let var2 = Node::mk_var(mcx, OUTER_VAR, 1, INT4OID, -1, 0, 0).unwrap();
        let sum_args =
            NodeList::make1(mcx, Node::mk_target_entry(mcx, var2, 1, None, false).unwrap())
                .unwrap();
        let specs = [
            // count(a): int8inc_any (2804), strict, 1 input, non-null init.
            AggTransSpec {
                combine: false,
                deserialfn_oid: 0,
                arg_types: &[],
                transtype_byval: true,
                transtype_len: 8,
                transfn_oid: 2804,
                inputcollid: 0,
                init_value_is_null: false,
                args: &count_args,
                aggfilter: None,
                pergroup: base,
                ordered: None,
                cur_agg: None,
            },
            // sum(int4): int4_sum (1841), non-strict, null init.
            AggTransSpec {
                combine: false,
                deserialfn_oid: 0,
                arg_types: &[],
                transtype_byval: true,
                transtype_len: 8,
                transfn_oid: 1841,
                inputcollid: 0,
                init_value_is_null: true,
                args: &sum_args,
                aggfilter: None,
                // SAFETY: index 1 of the 2-element local array.
                pergroup: unsafe { NonNull::new_unchecked(base.as_ptr().add(1)) },
                ordered: None,
                cur_agg: None,
            },
        ];
        let mut trans = exec_build_agg_trans(mcx, &specs, None, ParamBind::NONE).unwrap();
        for v in [Some(7i32), None, Some(35)] {
            let mut outer = virtual_slot(mcx, &[v]);
            let mut slots = EvalSlots { scan: None, inner: None, outer: Some(&mut outer) };
            crate::exec_eval_expr(&mut trans, &mut slots).unwrap();
        }
        assert_eq!(pergroup[0].trans_value.as_i64(), 2);
        assert!(!pergroup[0].trans_value_is_null);
        assert_eq!(pergroup[1].trans_value.as_i64(), 42);
        assert!(!pergroup[1].trans_value_is_null);
    });
}

fn eval_sysvar<'m>(
    mcx: Mcx<'m>,
    slot: &mut SlotData<'m>,
    attno: i16,
    typ: u32,
) -> ::types_error::PgResult<::datum::NullableDatum> {
    let mut state = exec_init_expr(mcx, Some(mk_scan_var(mcx, attno, typ)), ParamBind::NONE).unwrap().unwrap();
    assert!(matches!(state.kernel(), Kernel::Program));
    let mut slots = EvalSlots { scan: Some(slot), inner: None, outer: None };
    exec_eval_expr(&mut state, &mut slots)
}

#[test]
fn sysvar_steps_read_slot_and_tuple_header() {
    with_mcx(|mcx| {
        let desc = desc_int4(mcx, 1);
        let vals = [Datum::from_i32(9)];
        let nulls = [false];
        let mut tuple = heaptuple::heap_form_tuple(mcx, &desc, &vals, &nulls).unwrap();
        tuple.t_data_mut().set_xmin(77);
        tuple.t_data_mut().set_cmin(5);
        let mut slot = exectuples::make_tuple_table_slot(mcx, TupleSlotKind::HeapTuple, Some(desc));
        exectuples::exec_store_heap_tuple_owned(&mut slot, mcx, tuple);
        slot.base_mut().tts_tableOid = 424242;
        slot.base_mut().tts_tid = ::types_tuple::ItemPointerData::new(7, 3);

        let ctid = eval_sysvar(mcx, &mut slot, -1, 27).unwrap();
        assert!(!ctid.isnull);
        let tid = unsafe { &*(ctid.value.as_usize() as *const ::types_tuple::ItemPointerData) };
        assert_eq!(*tid, ::types_tuple::ItemPointerData::new(7, 3));

        assert_eq!(eval_sysvar(mcx, &mut slot, -2, 28).unwrap().value.as_u32(), 77);
        assert_eq!(eval_sysvar(mcx, &mut slot, -3, 29).unwrap().value.as_u32(), 5);
        assert_eq!(eval_sysvar(mcx, &mut slot, -5, 29).unwrap().value.as_u32(), 5);
        assert_eq!(eval_sysvar(mcx, &mut slot, -6, 26).unwrap().value.as_oid(), 424242);

        // Virtual slots surface xmin only through the 0A000 arm.
        let mut vslot = virtual_slot(mcx, &[Some(1)]);
        vslot.base_mut().tts_tableOid = 7;
        assert_eq!(eval_sysvar(mcx, &mut vslot, -6, 26).unwrap().value.as_oid(), 7);
        let err = eval_sysvar(mcx, &mut vslot, -2, 28).unwrap_err();
        assert_eq!(err.message, "cannot retrieve a system column in this context");
    });
}

fn mk_param<'mcx>(
    mcx: Mcx<'mcx>,
    kind: ::types_nodes::primnodes::ParamKind,
    paramid: i32,
    typ: u32,
) -> Node<'mcx> {
    Node::mk(
        mcx,
        ::types_nodes::primnodes::Param {
            paramkind: kind,
            paramid,
            paramtype: typ,
            paramtypmod: -1,
            paramcollid: 0,
            location: -1,
        },
    )
    .unwrap()
}

#[test]
fn param_extern_step_is_one_resolved_load() {
    use ::types_nodes::primnodes::ParamKind;
    use ::types_portal::params::{ParamExternData, PARAM_FLAG_CONST};
    with_mcx(|mcx| {
        let externs = [ParamExternData {
            value: Datum::from_i32(42),
            isnull: false,
            pflags: PARAM_FLAG_CONST,
            ptype: INT4OID,
        }];
        let bind = ParamBind { extern_params: Some(&externs), ..ParamBind::NONE };
        let node = mk_param(mcx, ParamKind::PARAM_EXTERN, 1, INT4OID);
        let mut state = exec_init_expr(mcx, Some(node), bind).unwrap().unwrap();
        assert!(matches!(state.steps()[0], Step::ParamExtern { .. }));
        let mut slots = EvalSlots::default();
        let r = exec_eval_expr(&mut state, &mut slots).unwrap();
        assert!(!r.isnull);
        assert_eq!(r.value.as_i32(), 42);
    });
}

#[test]
fn param_extern_missing_value_errors_42704() {
    use ::types_nodes::primnodes::ParamKind;
    with_mcx(|mcx| {
        let node = mk_param(mcx, ParamKind::PARAM_EXTERN, 2, INT4OID);
        // C errors at evaluation, not init (EXPLAIN GENERIC_PLAN inits only).
        let mut state = exec_init_expr(mcx, Some(node), ParamBind::NONE).unwrap().unwrap();
        assert!(matches!(state.steps()[0], Step::ParamExternMissing { .. }));
        let mut slots = EvalSlots::default();
        let err = exec_eval_expr(&mut state, &mut slots).unwrap_err();
        assert_eq!(err.message, "no value found for parameter 2");
        assert_eq!(err.sqlstate, ::types_error::ERRCODE_UNDEFINED_OBJECT);
    });
}

#[test]
fn param_exec_step_reads_estate_slot() {
    use ::types_nodes::primnodes::ParamKind;
    use ::types_portal::params::ParamExecData;
    with_mcx(|mcx| {
        let mut vals = [ParamExecData::EMPTY, ParamExecData::EMPTY];
        let base = vals.as_mut_ptr();
        // SAFETY: in-bounds writes through the same pointer the steps read.
        unsafe {
            *base.add(1) =
                ParamExecData { value: Datum::from_i32(7), isnull: false, exec_plan: false };
        }
        let bind = ParamBind {
            extern_params: None,
            exec_vals: core::ptr::NonNull::new(base),
            n_exec: 2,
        };
        let node = mk_param(mcx, ParamKind::PARAM_EXEC, 1, INT4OID);
        let mut state = exec_init_expr(mcx, Some(node), bind).unwrap().unwrap();
        assert!(matches!(state.steps()[0], Step::ParamExec { .. }));
        let mut slots = EvalSlots::default();
        let r = exec_eval_expr(&mut state, &mut slots).unwrap();
        assert_eq!(r.value.as_i32(), 7);

        // ExecSetParamPlan's write side is the subplan lane; a pending
        // initplan must be loud, not a stale read.
        // SAFETY: as above; the interp must observe the pending-plan bit.
        unsafe { (*base.add(1)).exec_plan = true };
        let panicked = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let mut slots = EvalSlots::default();
            let _ = exec_eval_expr(&mut state, &mut slots);
        }));
        assert!(panicked.is_err());
    });
}

#[test]
#[should_panic(expected = "must not reach the executor")]
fn param_sublink_is_loud() {
    use ::types_nodes::primnodes::ParamKind;
    with_mcx(|mcx| {
        let node = mk_param(mcx, ParamKind::PARAM_SUBLINK, 1, INT4OID);
        let _ = exec_init_expr(mcx, Some(node), ParamBind::NONE);
    });
}

fn mk_minmax<'mcx>(mcx: Mcx<'mcx>, least: bool, vals: &[Option<i32>]) -> Node<'mcx> {
    use ::types_nodes::primnodes::{MinMaxExpr, MinMaxOp};
    let args: alloc::vec::Vec<Node<'mcx>> =
        vals.iter().map(|v| mk_int4_const(mcx, *v)).collect();
    Node::mk(
        mcx,
        MinMaxExpr {
            minmaxtype: INT4OID,
            minmaxcollid: 0,
            inputcollid: 0,
            op: if least { MinMaxOp::IS_LEAST } else { MinMaxOp::IS_GREATEST },
            args: NodeList::from_slice(mcx, &args).unwrap(),
            location: -1,
        },
    )
    .unwrap()
}

#[test]
fn minmax_greatest_least_and_null_handling() {
    with_mcx(|mcx| {
        let out = crate::evaluate_expr(mcx, mk_minmax(mcx, false, &[Some(1), Some(2), Some(3)]), INT4OID, -1, 0)
            .unwrap();
        let c = out.as_const().unwrap();
        assert!(!c.constisnull);
        assert_eq!(c.constvalue.as_i32(), 3);

        let out = crate::evaluate_expr(mcx, mk_minmax(mcx, true, &[Some(1), Some(2), Some(3)]), INT4OID, -1, 0)
            .unwrap();
        assert_eq!(out.as_const().unwrap().constvalue.as_i32(), 1);

        // NULL inputs are ignored (C ExecEvalMinMax).
        let out = crate::evaluate_expr(
            mcx,
            mk_minmax(mcx, false, &[None, Some(-5), None, Some(4)]),
            INT4OID,
            -1,
            0,
        )
        .unwrap();
        let c = out.as_const().unwrap();
        assert!(!c.constisnull);
        assert_eq!(c.constvalue.as_i32(), 4);

        // All-NULL result is NULL.
        let out =
            crate::evaluate_expr(mcx, mk_minmax(mcx, true, &[None, None]), INT4OID, -1, 0).unwrap();
        assert!(out.as_const().unwrap().constisnull);
    });
}

fn mk_bool_const<'mcx>(mcx: Mcx<'mcx>, v: Option<bool>) -> Node<'mcx> {
    Node::mk_const(
        mcx,
        16,
        -1,
        0,
        1,
        v.map_or(Datum::null(), Datum::from_bool),
        v.is_none(),
        true,
    )
    .unwrap()
}

fn mk_boolexpr<'mcx>(
    mcx: Mcx<'mcx>,
    op: ::types_nodes::primnodes::BoolExprType,
    args: &[Option<bool>],
) -> Node<'mcx> {
    let mut list = NodeList::nil();
    for &a in args {
        list.lappend(mcx, mk_bool_const(mcx, a)).unwrap();
    }
    Node::mk(mcx, ::types_nodes::primnodes::BoolExpr { boolop: op, args: list, location: -1 })
        .unwrap()
}

fn eval_bool<'mcx>(mcx: Mcx<'mcx>, expr: Node<'mcx>) -> Option<bool> {
    let mut state = exec_init_expr(mcx, Some(expr), ParamBind::NONE).unwrap().unwrap();
    let mut slots = EvalSlots::default();
    let r = exec_eval_expr(&mut state, &mut slots).unwrap();
    if r.isnull {
        None
    } else {
        Some(r.value.as_bool())
    }
}

#[test]
fn boolexpr_three_valued_truth_tables() {
    use ::types_nodes::primnodes::BoolExprType::{AND_EXPR, NOT_EXPR, OR_EXPR};
    with_mcx(|mcx| {
        let vals = [Some(true), Some(false), None];
        for a in vals {
            for b in vals {
                let and = eval_bool(mcx, mk_boolexpr(mcx, AND_EXPR, &[a, b]));
                let expect_and = match (a, b) {
                    (Some(false), _) | (_, Some(false)) => Some(false),
                    (Some(true), Some(true)) => Some(true),
                    _ => None,
                };
                assert_eq!(and, expect_and, "AND {a:?} {b:?}");
                let or = eval_bool(mcx, mk_boolexpr(mcx, OR_EXPR, &[a, b]));
                let expect_or = match (a, b) {
                    (Some(true), _) | (_, Some(true)) => Some(true),
                    (Some(false), Some(false)) => Some(false),
                    _ => None,
                };
                assert_eq!(or, expect_or, "OR {a:?} {b:?}");
                for c in vals {
                    let and3 = eval_bool(mcx, mk_boolexpr(mcx, AND_EXPR, &[a, b, c]));
                    let expect3 = match (expect_and, c) {
                        (Some(false), _) | (_, Some(false)) => Some(false),
                        (Some(true), Some(true)) => Some(true),
                        _ => None,
                    };
                    assert_eq!(and3, expect3, "AND {a:?} {b:?} {c:?}");
                }
            }
            let not = eval_bool(mcx, mk_boolexpr(mcx, NOT_EXPR, &[a]));
            assert_eq!(not, a.map(|v| !v), "NOT {a:?}");
        }
    });
}

fn mk_svf<'mcx>(
    mcx: Mcx<'mcx>,
    op: ::types_nodes::primnodes::SQLValueFunctionOp,
    typ: u32,
    typmod: i32,
) -> Node<'mcx> {
    use ::types_nodes::primnodes::SQLValueFunction;
    Node::mk(mcx, SQLValueFunction { op, r#type: typ, typmod, location: -1 }).unwrap()
}

#[test]
fn sql_value_function_datetime_ops() {
    use ::types_nodes::primnodes::SQLValueFunctionOp as Op;
    static TZ: Once = Once::new();
    TZ.call_once(|| {
        // SAFETY: single-threaded test init, before any getenv (adt_date
        // tests' precedent).
        unsafe { std::env::set_var("PGRUST_TZDIR", "/usr/share/zoneinfo") };
        pgtz::init_seams();
        adt_timestamp::init_seams();
        guc_tables::init_seams();
        elog::init_seams();
        fd::init_seams();
        xact_seams::get_current_sub_transaction_id::set(|| 1);
    });
    adt_datetime::tz::pg_timezone_initialize();

    with_mcx(|mcx| {
        let mut eval = |node| {
            let mut state = exec_init_expr(mcx, Some(node), ParamBind::NONE).unwrap().unwrap();
            let mut slots = EvalSlots::default();
            exec_eval_expr(&mut state, &mut slots).unwrap()
        };

        let r = eval(mk_svf(mcx, Op::SVFOP_CURRENT_TIMESTAMP, 1184, -1));
        assert!(!r.isnull);
        assert_eq!(r.value.as_i64(), adt_timestamp::GetSQLCurrentTimestamp(-1));

        // Statement start is fixed, so typmod-0 rounding matches exactly.
        let r = eval(mk_svf(mcx, Op::SVFOP_CURRENT_TIMESTAMP_N, 1184, 0));
        assert_eq!(r.value.as_i64(), adt_timestamp::GetSQLCurrentTimestamp(0));
        assert_eq!(r.value.as_i64() % 1_000_000, 0);

        let r = eval(mk_svf(mcx, Op::SVFOP_LOCALTIMESTAMP, 1114, -1));
        assert_eq!(r.value.as_i64(), adt_timestamp::GetSQLLocalTimestamp(-1).unwrap());

        let r = eval(mk_svf(mcx, Op::SVFOP_CURRENT_DATE, 1082, -1));
        assert_eq!(r.value.as_i32(), adt_date::GetSQLCurrentDate());

        let r = eval(mk_svf(mcx, Op::SVFOP_LOCALTIME_N, 1083, 0));
        assert_eq!(r.value.as_i64() % 1_000_000, 0);

        // CURRENT_TIME yields a by-ref TimeTz image (time i64, zone i32).
        let r = eval(mk_svf(mcx, Op::SVFOP_CURRENT_TIME, 1266, -1));
        assert!(!r.isnull);
        let p = r.value.as_usize() as *const u8;
        // SAFETY: step-owned 12-byte image written by the eval above.
        let (time, zone) = unsafe {
            (
                p.cast::<i64>().read(),
                p.add(8).cast::<i32>().read(),
            )
        };
        assert!((0..86_400_000_000).contains(&time));
        // GMT session zone (pg_timezone_initialize default).
        assert_eq!(zone, 0);
    });
}

#[test]
fn case_expr_arg_form() {
    with_mcx(|mcx| {
        // CASE scanvar WHEN 1 THEN 10 WHEN 2 THEN 20 ELSE 30 END, in the
        // parser's expanded shape: int4eq(CaseTestExpr, k) conditions.
        let case_test = || {
            Node::mk(
                mcx,
                ::types_nodes::primnodes::CaseTestExpr {
                    typeId: INT4OID,
                    typeMod: -1,
                    collation: 0,
                },
            )
            .unwrap()
        };
        let when = |k: i32, r: i32| {
            let mut args = NodeList::nil();
            args.lappend(mcx, case_test()).unwrap();
            args.lappend(mcx, mk_int4_const(mcx, Some(k))).unwrap();
            Node::mk(
                mcx,
                ::types_nodes::primnodes::CaseWhen {
                    expr: Some(mk_opexpr(mcx, 65, BOOLOID, args)),
                    result: Some(mk_int4_const(mcx, Some(r))),
                    location: -1,
                },
            )
            .unwrap()
        };
        let mut whens = NodeList::nil();
        whens.lappend(mcx, when(1, 10)).unwrap();
        whens.lappend(mcx, when(2, 20)).unwrap();
        let case = Node::mk(
            mcx,
            ::types_nodes::primnodes::CaseExpr {
                casetype: INT4OID,
                casecollid: 0,
                arg: Some(mk_scan_var(mcx, 1, INT4OID)),
                args: whens,
                defresult: Some(mk_int4_const(mcx, Some(30))),
                location: -1,
            },
        )
        .unwrap();
        let mut state = exec_init_expr(mcx, Some(case), ParamBind::NONE).unwrap().unwrap();
        let mut eval = |v: Option<i32>| {
            let mut slot = virtual_slot(mcx, &[v]);
            let mut slots = EvalSlots { scan: Some(&mut slot), inner: None, outer: None };
            exec_eval_expr(&mut state, &mut slots).unwrap()
        };
        assert_eq!(eval(Some(1)).value.as_i32(), 10);
        assert_eq!(eval(Some(2)).value.as_i32(), 20);
        assert_eq!(eval(Some(7)).value.as_i32(), 30);
        // NULL arg: strict equality yields NULL -> no match -> ELSE.
        assert_eq!(eval(None).value.as_i32(), 30);
    });
}

#[test]
fn case_expr_searched_form() {
    with_mcx(|mcx| {
        // CASE WHEN var = 1 THEN 10 END (implicit-NULL default as a Const).
        let mut args = NodeList::nil();
        args.lappend(mcx, mk_scan_var(mcx, 1, INT4OID)).unwrap();
        args.lappend(mcx, mk_int4_const(mcx, Some(1))).unwrap();
        let mut whens = NodeList::nil();
        whens
            .lappend(
                mcx,
                Node::mk(
                    mcx,
                    ::types_nodes::primnodes::CaseWhen {
                        expr: Some(mk_opexpr(mcx, 65, BOOLOID, args)),
                        result: Some(mk_int4_const(mcx, Some(10))),
                        location: -1,
                    },
                )
                .unwrap(),
            )
            .unwrap();
        let case = Node::mk(
            mcx,
            ::types_nodes::primnodes::CaseExpr {
                casetype: INT4OID,
                casecollid: 0,
                arg: None,
                args: whens,
                defresult: Some(mk_int4_const(mcx, None)),
                location: -1,
            },
        )
        .unwrap();
        let mut state = exec_init_expr(mcx, Some(case), ParamBind::NONE).unwrap().unwrap();
        let mut eval = |v: Option<i32>| {
            let mut slot = virtual_slot(mcx, &[v]);
            let mut slots = EvalSlots { scan: Some(&mut slot), inner: None, outer: None };
            exec_eval_expr(&mut state, &mut slots).unwrap()
        };
        assert_eq!(eval(Some(1)).value.as_i32(), 10);
        assert!(eval(Some(2)).isnull);
        assert!(eval(None).isnull);
    });
}

fn mk_domain_coercion(mcx: Mcx<'_>, value: Option<i32>) -> Node<'_> {
    let konst = Node::mk_const(
        mcx,
        INT4OID,
        -1,
        0,
        4,
        value.map_or(Datum::null(), Datum::from_i32),
        value.is_none(),
        true,
    )
    .unwrap();
    Node::mk(
        mcx,
        ::types_nodes::CoerceToDomain {
            arg: konst,
            resulttype: DOMAIN_OID,
            resulttypmod: -1,
            resultcollid: 0,
            coercionformat: ::types_nodes::CoercionForm::COERCE_IMPLICIT_CAST,
            location: -1,
        },
    )
    .unwrap()
}

const INT4ARRAYOID: u32 = 1007;

const F_INT4EQ: u32 = 65;

fn mk_int4_array_const<'mcx>(mcx: Mcx<'mcx>, elems: &[Option<i32>]) -> Node<'mcx> {
    let values: Vec<Datum> =
        elems.iter().map(|v| v.map_or(Datum::null(), Datum::from_i32)).collect();
    let nulls: Vec<bool> = elems.iter().map(|v| v.is_none()).collect();
    let dims = [elems.len() as i32];
    let img = arrayfuncs::construct_md_array(
        mcx, &values, Some(&nulls), 1, &dims, &[1], INT4OID, 4, true, b'i',
    )
    .unwrap();
    let d = Datum::from_usize(img.leak().as_ptr() as usize);
    Node::mk_const(mcx, INT4ARRAYOID, -1, 0, -1, d, false, false).unwrap()
}

fn mk_saop<'mcx>(
    mcx: Mcx<'mcx>,
    use_or: bool,
    scalar: Node<'mcx>,
    array: Node<'mcx>,
) -> Node<'mcx> {
    let mut args = NodeList::make1(mcx, scalar).unwrap();
    args.lappend(mcx, array).unwrap();
    Node::mk(
        mcx,
        ::types_nodes::ScalarArrayOpExpr {
            opno: 96,
            opfuncid: F_INT4EQ,
            hashfuncid: 0,
            negfuncid: 0,
            useOr: use_or,
            inputcollid: 0,
            args,
            location: -1,
        },
    )
    .unwrap()
}

fn eval_domain(value: Option<i32>) -> Result<::datum::NullableDatum, Box<::types_error::PgError>> {
    install_seams();
    with_mcx(|mcx| {
        let expr = mk_domain_coercion(mcx, value);
        let mut state = exec_init_expr(mcx, Some(expr), ParamBind::NONE).unwrap().unwrap();
        state.arm_result_mcx(mcx);
        exec_eval_expr(&mut state, &mut EvalSlots::default())
    })
}

fn eval_saop(
    use_or: bool,
    scalar: Option<i32>,
    elems: &[Option<i32>],
) -> Option<bool> {
    with_mcx(|mcx| {
        let node = mk_saop(
            mcx,
            use_or,
            mk_int4_const(mcx, scalar),
            mk_int4_array_const(mcx, elems),
        );
        let mut state = exec_init_expr(mcx, Some(node), ParamBind::NONE).unwrap().unwrap();
        state.arm_result_mcx(mcx);
        let mut slots = EvalSlots::default();
        let r = exec_eval_expr(&mut state, &mut slots).unwrap();
        (!r.isnull).then(|| r.value.as_bool())
    })
}

#[test]
fn coerce_to_domain_valid_value_passes() {
    let r = eval_domain(Some(5)).unwrap();
    assert!(!r.isnull);
    assert_eq!(r.value.as_i32(), 5);
}

#[test]
fn coerce_to_domain_check_violation_is_23514() {
    let e = eval_domain(Some(0)).unwrap_err();
    assert_eq!(
        e.message(),
        "value for domain posint violates check constraint \"posint_check\""
    );
    assert_eq!(e.sqlstate(), ::types_error::ERRCODE_CHECK_VIOLATION);
    assert_eq!(e.constraint_name(), Some("posint_check"));
    assert_eq!(e.datatype_name(), Some("posint"));
}

#[test]
fn coerce_to_domain_null_is_23502() {
    let e = eval_domain(None).unwrap_err();
    assert_eq!(e.message(), "domain posint does not allow null values");
    assert_eq!(e.sqlstate(), ::types_error::ERRCODE_NOT_NULL_VIOLATION);
}

#[test]
fn domain_check_input_engine_matches() {
    install_seams();
    assert!(crate::domain::domain_check_input(Datum::from_i32(7), false, DOMAIN_OID, None).is_ok());
    let e = crate::domain::domain_check_input(Datum::from_i32(-1), false, DOMAIN_OID, None).unwrap_err();
    assert_eq!(e.sqlstate(), ::types_error::ERRCODE_CHECK_VIOLATION);
    let e = crate::domain::domain_check_input(Datum::null(), true, DOMAIN_OID, None).unwrap_err();
    assert_eq!(e.sqlstate(), ::types_error::ERRCODE_NOT_NULL_VIOLATION);
}
#[test]
fn scalar_array_op_any_and_all() {
    assert_eq!(eval_saop(true, Some(2), &[Some(1), Some(2), Some(3)]), Some(true));
    assert_eq!(eval_saop(true, Some(5), &[Some(1), Some(2), Some(3)]), Some(false));
    assert_eq!(eval_saop(true, Some(5), &[]), Some(false));
    assert_eq!(eval_saop(false, Some(5), &[]), Some(true));
    assert_eq!(eval_saop(false, Some(2), &[Some(2), Some(2)]), Some(true));
    assert_eq!(eval_saop(false, Some(2), &[Some(2), Some(3)]), Some(false));
    // Strict fn + NULL scalar -> NULL; NULL element leaves NULL unless decided.
    assert_eq!(eval_saop(true, None, &[Some(1)]), None);
    assert_eq!(eval_saop(true, Some(2), &[Some(1), None]), None);
    assert_eq!(eval_saop(true, Some(2), &[None, Some(2)]), Some(true));
    assert_eq!(eval_saop(false, Some(2), &[Some(2), None]), None);
    assert_eq!(eval_saop(false, Some(2), &[None, Some(3)]), Some(false));
}

#[test]
fn scalar_array_op_null_array_is_null() {
    with_mcx(|mcx| {
        let arr = Node::mk_const(mcx, INT4ARRAYOID, -1, 0, -1, Datum::null(), true, false)
            .unwrap();
        let node = mk_saop(mcx, true, mk_int4_const(mcx, Some(2)), arr);
        let mut state = exec_init_expr(mcx, Some(node), ParamBind::NONE).unwrap().unwrap();
        state.arm_result_mcx(mcx);
        let mut slots = EvalSlots::default();
        assert!(exec_eval_expr(&mut state, &mut slots).unwrap().isnull);
    });
}

#[test]
fn array_expr_builds_array_consumable_by_saop() {
    with_mcx(|mcx| {
        let mut elems = NodeList::make1(mcx, mk_int4_const(mcx, Some(7))).unwrap();
        elems.lappend(mcx, mk_int4_const(mcx, Some(8))).unwrap();
        elems.lappend(mcx, mk_int4_const(mcx, None)).unwrap();
        let ae = Node::mk(
            mcx,
            ::types_nodes::ArrayExpr {
                array_typeid: INT4ARRAYOID,
                array_collid: 0,
                element_typeid: INT4OID,
                elements: elems,
                multidims: false,
                list_start: -1,
                list_end: -1,
                location: -1,
            },
        )
        .unwrap();
        let node = mk_saop(mcx, true, mk_int4_const(mcx, Some(8)), ae);
        let mut state = exec_init_expr(mcx, Some(node), ParamBind::NONE).unwrap().unwrap();
        state.arm_result_mcx(mcx);
        let mut slots = EvalSlots::default();
        let r = exec_eval_expr(&mut state, &mut slots).unwrap();
        assert!(!r.isnull);
        assert!(r.value.as_bool());
    });
}

/// ArrayExprStep.nelems was u16, so an ARRAY[] literal with 65536 elements
/// truncated to 0 and silently evaluated to an EMPTY array — no error, just a
/// wrong answer (found by pgjdbc's 64K-binds test; `array_length(...)` came
/// back NULL against pgrust and 65536 against C). C's execExpr.h keeps this
/// count in an `int`, so there is no 16-bit ceiling to honour. Walk the
/// boundary: 65535 was already correct, 65536/65537 are the regression.
#[test]
fn array_expr_element_count_survives_the_16_bit_boundary() {
    for n in [65535usize, 65536, 65537] {
        with_mcx(|mcx| {
            let mut elems = NodeList::make1(mcx, mk_int4_const(mcx, Some(1))).unwrap();
            for _ in 1..n {
                elems.lappend(mcx, mk_int4_const(mcx, Some(1))).unwrap();
            }
            let ae = Node::mk(
                mcx,
                ::types_nodes::ArrayExpr {
                    array_typeid: INT4ARRAYOID,
                    array_collid: 0,
                    element_typeid: INT4OID,
                    elements: elems,
                    multidims: false,
                    list_start: -1,
                    list_end: -1,
                    location: -1,
                },
            )
            .unwrap();
            let mut state = exec_init_expr(mcx, Some(ae), ParamBind::NONE).unwrap().unwrap();
            // The step must carry the true count, not a wrapped one. Under the
            // old u16 this was 0 for 65536 and 1 for 65537.
            let carried = state
                .steps()
                .iter()
                .find_map(|s| match s {
                    Step::ArrayExprStep { nelems, .. } => Some(*nelems),
                    _ => None,
                })
                .expect("ArrayExpr compiles to an ArrayExprStep");
            assert_eq!(carried as usize, n, "ArrayExprStep.nelems wrapped at {n} elements");

            // And the evaluated array must actually hold n elements.
            state.arm_result_mcx(mcx);
            let mut slots = EvalSlots::default();
            let r = exec_eval_expr(&mut state, &mut slots).unwrap();
            assert!(!r.isnull, "ARRAY[] of {n} elements evaluated to NULL");
            // SAFETY: a non-null array datum addresses a live varlena; the
            // ARRAY[] result is built with a plain 4-byte header.
            let img = unsafe {
                let p = r.value.as_usize() as *const u8;
                core::slice::from_raw_parts(p, ::types_tuple::varatt::varsize_any(p))
            };
            assert_eq!(::arrayfuncs::arr_ndim(img), 1);
            assert_eq!(
                ::arrayfuncs::arr_dim(img, 0) as usize,
                n,
                "ARRAY[] of {n} elements built the wrong length"
            );
        });
    }
}

#[test]
fn fused_func_chain_evaluates_like_unfused() {
    with_mcx(|mcx| {
        let mut expr = mk_scan_var(mcx, 1, INT4OID);
        for k in 1..=8 {
            let args = NodeList::make2(mcx, expr, mk_int4_const(mcx, Some(k))).unwrap();
            expr = mk_opexpr(mcx, 177, INT4OID, args);
        }
        let mut state = exec_init_expr(mcx, Some(expr), ParamBind::NONE).unwrap().unwrap();
        assert_eq!(state.steps().len(), 7);
        assert!(matches!(state.steps()[1], Step::ScanVarFuncStrict2Thin { attnum: 0, argno: 0, .. }));
        assert!(matches!(state.steps()[2], Step::FuncFuncStrict2Thin { argno: 0, .. }));
        assert!(matches!(state.steps()[4], Step::FuncFuncStrict2Thin { .. }));
        assert!(matches!(state.steps()[5], Step::FuncExprStrict2Thin { .. }));
        for v in [Some(5), Some(-1000), None] {
            let mut slot = virtual_slot(mcx, &[v]);
            let mut slots = EvalSlots { scan: Some(&mut slot), inner: None, outer: None };
            let r = exec_eval_expr(&mut state, &mut slots).unwrap();
            match v {
                Some(x) => {
                    assert!(!r.isnull);
                    assert_eq!(r.value.as_i32(), x + 36);
                }
                None => assert!(r.isnull),
            }
        }
    });
}

// PROCPERF P2 compile economy: under the window the pair-fusion peephole and
// the lane-v2 censuses are skipped, the thin-ABI single rewrite is kept, and
// evaluation results are identical to the fused program.
#[test]
fn economy_window_skips_fusion_keeps_thin_and_results() {
    with_mcx(|mcx| {
        let build = |mcx| {
            let mut expr = mk_scan_var(mcx, 1, INT4OID);
            for k in 1..=8 {
                let args = NodeList::make2(mcx, expr, mk_int4_const(mcx, Some(k))).unwrap();
                expr = mk_opexpr(mcx, 177, INT4OID, args);
            }
            expr
        };
        let mut economy_state = {
            let _w = crate::compile::economy_window(true);
            exec_init_expr(mcx, Some(build(mcx)), ParamBind::NONE).unwrap().unwrap()
        };
        // Window restored: a post-window compile fuses again.
        let fused_state = exec_init_expr(mcx, Some(build(mcx)), ParamBind::NONE).unwrap().unwrap();
        assert!(fused_state
            .steps()
            .iter()
            .any(|s| matches!(s, Step::ScanVarFuncStrict2Thin { .. } | Step::FuncFuncStrict2Thin { .. })));
        // Economy program: no pair-fused variants, thin singles retained.
        assert!(!economy_state
            .steps()
            .iter()
            .any(|s| matches!(s, Step::ScanVarFuncStrict2Thin { .. } | Step::FuncFuncStrict2Thin { .. })));
        assert!(economy_state
            .steps()
            .iter()
            .any(|s| matches!(s, Step::FuncExprStrict2Thin { .. })));
        for v in [Some(5), Some(-1000), None] {
            let mut slot = virtual_slot(mcx, &[v]);
            let mut slots = EvalSlots { scan: Some(&mut slot), inner: None, outer: None };
            let r = exec_eval_expr(&mut economy_state, &mut slots).unwrap();
            match v {
                Some(x) => {
                    assert!(!r.isnull);
                    assert_eq!(r.value.as_i32(), x + 36);
                }
                None => assert!(r.isnull),
            }
        }
    });
}

// Economy skips the lane-v2 qual censuses (scan_cmp_clauses stays None) and
// the fused-qual rewrite, while the qual still evaluates correctly.
#[test]
fn economy_window_skips_qual_census() {
    with_mcx(|mcx| {
        let build_qual = |mcx| {
            let a_lt0 = {
                let args =
                    NodeList::make2(mcx, mk_scan_var(mcx, 1, INT4OID), mk_int4_const(mcx, Some(0)))
                        .unwrap();
                mk_opexpr(mcx, 66, BOOLOID, args)
            };
            let b_gt5 = {
                let args =
                    NodeList::make2(mcx, mk_scan_var(mcx, 2, INT4OID), mk_int4_const(mcx, Some(5)))
                        .unwrap();
                mk_opexpr(mcx, 147, BOOLOID, args)
            };
            NodeList::make2(mcx, a_lt0, b_gt5).unwrap()
        };
        let control = exec_init_qual(mcx, &build_qual(mcx), ParamBind::NONE).unwrap().unwrap();
        assert!(control.scan_cmp_const_clauses().is_some());
        let mut state = {
            let _w = crate::compile::economy_window(true);
            exec_init_qual(mcx, &build_qual(mcx), ParamBind::NONE).unwrap().unwrap()
        };
        assert!(state.scan_cmp_const_clauses().is_none());
        assert!(!state
            .steps()
            .iter()
            .any(|s| matches!(s, Step::ScanVarFuncStrict2Thin { .. })));
        for (a, b, want) in [
            (Some(-1), Some(6), true),
            (Some(-1), Some(5), false),
            (Some(1), Some(6), false),
            (None, Some(6), false),
            (Some(-1), None, false),
        ] {
            assert_eq!(run_qual(mcx, &mut state, &[a, b]), want, "a={a:?} b={b:?}");
        }
    });
}

#[test]
fn fused_two_clause_qual_matches() {
    with_mcx(|mcx| {
        let a_lt0 = {
            let args =
                NodeList::make2(mcx, mk_scan_var(mcx, 1, INT4OID), mk_int4_const(mcx, Some(0)))
                    .unwrap();
            mk_opexpr(mcx, 66, BOOLOID, args)
        };
        let b_gt5 = {
            let args =
                NodeList::make2(mcx, mk_scan_var(mcx, 2, INT4OID), mk_int4_const(mcx, Some(5)))
                    .unwrap();
            mk_opexpr(mcx, 147, BOOLOID, args)
        };
        let qual = NodeList::make2(mcx, a_lt0, b_gt5).unwrap();
        let mut state = exec_init_qual(mcx, &qual, ParamBind::NONE).unwrap().unwrap();
        assert!(matches!(state.kernel(), Kernel::Program));
        assert!(state
            .steps()
            .iter()
            .any(|s| matches!(s, Step::ScanVarFuncStrict2Thin { .. })));
        for (a, b, want) in [
            (Some(-1), Some(6), true),
            (Some(-1), Some(5), false),
            (Some(1), Some(6), false),
            (None, Some(6), false),
            (Some(-1), None, false),
        ] {
            assert_eq!(run_qual(mcx, &mut state, &[a, b]), want, "a={a:?} b={b:?}");
        }
    });
}

#[test]
fn thin_fused_chain_overflow_error_intact() {
    with_mcx(|mcx| {
        let args = NodeList::make2(
            mcx,
            mk_scan_var(mcx, 1, INT4OID),
            mk_int4_const(mcx, Some(i32::MAX)),
        )
        .unwrap();
        let expr = mk_opexpr(mcx, 177, INT4OID, args);
        let mut state = exec_init_expr(mcx, Some(expr), ParamBind::NONE).unwrap().unwrap();
        assert!(state
            .steps()
            .iter()
            .any(|s| matches!(s, Step::ScanVarFuncStrict2Thin { .. })));
        let mut slot = virtual_slot(mcx, &[Some(1)]);
        let mut slots = EvalSlots { scan: Some(&mut slot), inner: None, outer: None };
        let e = exec_eval_expr(&mut state, &mut slots).unwrap_err();
        assert_eq!(e.sqlstate(), ::types_error::ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE);
        let mut slot = virtual_slot(mcx, &[Some(-1)]);
        let mut slots = EvalSlots { scan: Some(&mut slot), inner: None, outer: None };
        let r = exec_eval_expr(&mut state, &mut slots).unwrap();
        assert_eq!((r.isnull, r.value.as_i32()), (false, i32::MAX - 1));
    });
}

#[test]
fn thin_qual_matches_general_path() {
    with_mcx(|mcx| {
        // int4lt is thin-registered; the fused qual selects a thin arm and
        // must agree with the kernel path on every null/value combination.
        let args =
            NodeList::make2(mcx, mk_scan_var(mcx, 1, INT4OID), mk_int4_const(mcx, Some(0)))
                .unwrap();
        let mut state = qual_state(mcx, mk_opexpr(mcx, 66, BOOLOID, args));
        state.force_program_kernel();
        assert!(state.steps().iter().any(|s| matches!(
            s,
            Step::FuncStrict2QualThin { .. } | Step::ScanVarFuncStrict2Thin { .. }
        )));
        for (v, want) in [(Some(-1), true), (Some(0), false), (Some(1), false), (None, false)] {
            assert_eq!(run_qual(mcx, &mut state, &[v]), want, "v={v:?}");
        }
    });
}

#[test]
fn thin_strict1_single_rewrite() {
    with_mcx(|mcx| {
        // int4um (212) is thin-registered at arity 1 and errors on INT32_MIN.
        let args = NodeList::make1(mcx, mk_scan_var(mcx, 1, INT4OID)).unwrap();
        let expr = mk_opexpr(mcx, 212, INT4OID, args);
        let mut state = exec_init_expr(mcx, Some(expr), ParamBind::NONE).unwrap().unwrap();
        state.force_program_kernel();
        assert!(state
            .steps()
            .iter()
            .any(|s| matches!(s, Step::FuncExprStrict1Thin { .. })));
        for (v, want) in [(Some(5), Some(-5)), (None, None)] {
            let mut slot = virtual_slot(mcx, &[v]);
            let mut slots = EvalSlots { scan: Some(&mut slot), inner: None, outer: None };
            let r = exec_eval_expr(&mut state, &mut slots).unwrap();
            match want {
                Some(x) => assert_eq!((r.isnull, r.value.as_i32()), (false, x)),
                None => assert!(r.isnull),
            }
        }
        let mut slot = virtual_slot(mcx, &[Some(i32::MIN)]);
        let mut slots = EvalSlots { scan: Some(&mut slot), inner: None, outer: None };
        let e = exec_eval_expr(&mut state, &mut slots).unwrap_err();
        assert_eq!(e.sqlstate(), ::types_error::ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE);
    });
}

#[test]
fn thin_agg_count_star_kernel() {
    use core::ptr::NonNull;

    use crate::compile::{exec_build_agg_trans, AggTransSpec};
    use crate::steps::AggPerGroup;

    with_mcx(|mcx| {
        let mut pergroup = [AggPerGroup {
            trans_value: Datum::from_i64(0),
            trans_value_is_null: false,
            no_trans_value: false,
        }];
        let base = NonNull::new(pergroup.as_mut_ptr()).unwrap();
        let empty_args = NodeList::nil();
        let specs = [AggTransSpec {
            combine: false,
            deserialfn_oid: 0,
            arg_types: &[],
            transtype_byval: true,
            transtype_len: 8,
            transfn_oid: 1219,
            inputcollid: 0,
            init_value_is_null: false,
            args: &empty_args,
            aggfilter: None,
            pergroup: base,
            ordered: None,
            cur_agg: None,
        }];
        let mut trans = exec_build_agg_trans(mcx, &specs, None, ParamBind::NONE).unwrap();
        assert!(matches!(trans.kernel(), Kernel::AggTransByValThin { strict: true, .. }));
        for _ in 0..3 {
            let mut slots = EvalSlots::default();
            crate::exec_eval_expr(&mut trans, &mut slots).unwrap();
        }
        assert_eq!(pergroup[0].trans_value.as_i64(), 3);
        assert!(!pergroup[0].trans_value_is_null);

        // Batched drive seam: the count(*) shape is detected and one advance
        // equals n per-row transitions.
        let (pg, strict) = trans.agg_count_star().expect("count(*) trans detected");
        assert!(strict);
        assert!(crate::steps::agg_count_star_advance(pg, strict, 291));
        assert_eq!(pergroup[0].trans_value.as_i64(), 294);

        // In-batch overflow refuses the advance (per-row walk owns the error).
        // Seed through `base`: a safe `pergroup[0]` write would invalidate
        // the kernel's pointer, which shares base's provenance (miri F3).
        // SAFETY: base points at the live local above; no other reference is
        // active across these writes.
        unsafe { (*base.as_ptr()).trans_value = Datum::from_i64(i64::MAX - 2) };
        assert!(!crate::steps::agg_count_star_advance(pg, strict, 3));
        assert_eq!(pergroup[0].trans_value.as_i64(), i64::MAX - 2);

        // Strict + null transvalue: all n calls are skipped.
        // SAFETY: as above (miri F3).
        unsafe { (*base.as_ptr()).trans_value_is_null = true };
        assert!(crate::steps::agg_count_star_advance(pg, true, 7));
        assert!(pergroup[0].trans_value_is_null);
        // Non-strict + null: refused (per-row resolves it).
        assert!(!crate::steps::agg_count_star_advance(pg, false, 7));
    });
}

#[test]
fn fusion_skips_jump_targets() {
    with_mcx(|mcx| {
        // CASE arm heads are jump targets; results stay correct across arms.
        let cond = {
            let args =
                NodeList::make2(mcx, mk_scan_var(mcx, 1, INT4OID), mk_int4_const(mcx, Some(0)))
                    .unwrap();
            mk_opexpr(mcx, 66, BOOLOID, args)
        };
        let then_expr = {
            let a1 =
                NodeList::make2(mcx, mk_scan_var(mcx, 1, INT4OID), mk_int4_const(mcx, Some(1)))
                    .unwrap();
            let inner = mk_opexpr(mcx, 177, INT4OID, a1);
            let a2 = NodeList::make2(mcx, inner, mk_int4_const(mcx, Some(2))).unwrap();
            mk_opexpr(mcx, 177, INT4OID, a2)
        };
        let when = Node::mk(
            mcx,
            ::types_nodes::primnodes::CaseWhen {
                expr: Some(cond),
                result: Some(then_expr),
                location: -1,
            },
        )
        .unwrap();
        let case = Node::mk(
            mcx,
            ::types_nodes::primnodes::CaseExpr {
                casetype: INT4OID,
                casecollid: 0,
                arg: None,
                args: NodeList::make1(mcx, when).unwrap(),
                defresult: Some(mk_int4_const(mcx, Some(7))),
                location: -1,
            },
        )
        .unwrap();
        let mut state = exec_init_expr(mcx, Some(case), ParamBind::NONE).unwrap().unwrap();
        for (v, want) in [(Some(-4), Some(-1)), (Some(3), Some(7)), (None, Some(7))] {
            let mut slot = virtual_slot(mcx, &[v]);
            let mut slots = EvalSlots { scan: Some(&mut slot), inner: None, outer: None };
            let r = exec_eval_expr(&mut state, &mut slots).unwrap();
            assert_eq!((r.isnull, r.value.as_i32()), (false, want.unwrap()), "v={v:?}");
        }
    });
}

#[test]
fn qual_bitmap_matches_scalar_cmp() {
    use crate::steps::qual_bitmap_cmp_const;
    let n = 291usize;
    let mut values = alloc::vec::Vec::new();
    let mut isnull = alloc::vec::Vec::new();
    for i in 0..n {
        values.push(Datum::from_i32((i as i32 % 7) - 3));
        isnull.push(i % 11 == 0);
    }
    let konst = Datum::from_i32(0);
    for cmp in [
        CmpOp::Int4Eq,
        CmpOp::Int4Ne,
        CmpOp::Int4Lt,
        CmpOp::Int4Le,
        CmpOp::Int4Gt,
        CmpOp::Int4Ge,
    ] {
        let mut sel = [0u64; 5];
        qual_bitmap_cmp_const(cmp, konst, &values, &isnull, &mut sel);
        for i in 0..n {
            let want = !isnull[i] && cmp.eval(values[i], konst);
            let got = sel[i / 64] & (1u64 << (i % 64)) != 0;
            assert_eq!(got, want, "{cmp:?} row {i}");
        }
        for i in n..5 * 64 {
            assert!(sel[i / 64] & (1u64 << (i % 64)) == 0, "tail bit {i}");
        }
    }
    let mut sel = [0u64; 5];
    qual_bitmap_cmp_const(
        CmpOp::Int84Lt,
        Datum::from_i32(1),
        &[Datum::from_i64(-9), Datum::from_i64(1), Datum::from_i64(0)],
        &[false, false, false],
        &mut sel,
    );
    assert_eq!(sel[0], 0b101);
}

mod json {
    use super::*;
    use ::datum::NullableDatum;
    use ::types_nodes::primnodes::{
        CaseTestExpr, JsonBehavior, JsonBehaviorType as JBT, JsonExpr, JsonExprOp as JOP,
        JsonReturning, JsonWrapper as JW,
    };

    use crate::compile::exec_init_expr_with_case_test;

    fn jsonb_datum<'m>(mcx: Mcx<'m>, json: &str) -> Datum {
        let img = adt_jsonb::io::jsonb_in(mcx, json.as_bytes(), None)
            .unwrap_or_else(|e| panic!("jsonb_in({json:?}): {}", e.message()))
            .expect("hard path returns Some");
        let d = Datum::from_usize(img.as_ptr() as usize);
        core::mem::forget(img);
        d
    }

    fn jsonb_const<'m>(mcx: Mcx<'m>, json: &str) -> Node<'m> {
        Node::mk_const(mcx, JSONBOID_T, -1, 0, -1, jsonb_datum(mcx, json), false, false).unwrap()
    }

    fn path_const<'m>(mcx: Mcx<'m>, path: &str) -> Node<'m> {
        let img = adt_jsonpath::path::jsonpath_in(mcx, path.as_bytes(), None)
            .unwrap_or_else(|e| panic!("jsonpath_in({path:?}): {}", e.message()))
            .expect("hard path returns Some");
        let d = Datum::from_usize(img.as_ptr() as usize);
        core::mem::forget(img);
        Node::mk_const(mcx, JSONPATHOID_T, -1, 0, -1, d, false, false).unwrap()
    }

    fn behavior<'m>(mcx: Mcx<'m>, btype: JBT, expr: Node<'m>) -> Node<'m> {
        Node::mk(mcx, JsonBehavior { btype, expr: Some(expr), coerce: false, location: -1 })
            .unwrap()
    }

    fn null_const<'m>(mcx: Mcx<'m>, typid: u32) -> Node<'m> {
        let (len, byval) = match typid {
            INT4OID => (4, true),
            BOOLOID => (1, true),
            _ => (-1, false),
        };
        Node::mk_const(mcx, typid, -1, 0, len, Datum::null(), true, byval).unwrap()
    }

    fn bool_const<'m>(mcx: Mcx<'m>, b: bool) -> Node<'m> {
        Node::mk_const(mcx, BOOLOID, -1, 0, 1, Datum::from_bool(b), false, true).unwrap()
    }

    struct Spec<'m> {
        op: JOP,
        formatted: Node<'m>,
        path: Node<'m>,
        ret_typid: u32,
        use_io: bool,
        use_json: bool,
        wrapper: JW,
        omit_quotes: bool,
        on_empty: Option<Node<'m>>,
        on_error: Node<'m>,
        passing: &'m [(&'m str, Node<'m>)],
    }

    fn mk_json_expr<'m>(mcx: Mcx<'m>, spec: Spec<'m>) -> Node<'m> {
        let returning: &JsonReturning<'_> = ::mcx::leak_in(
            ::mcx::alloc_in(
                mcx,
                JsonReturning { format: None, typid: spec.ret_typid, typmod: -1 },
            )
            .unwrap(),
        );
        let mut names = PgVec::new_in(mcx);
        let mut values = PgVec::new_in(mcx);
        for &(n, v) in spec.passing {
            names.push(Node::mk_string(mcx, n).unwrap());
            values.push(v);
        }
        Node::mk(
            mcx,
            JsonExpr {
                op: spec.op,
                column_name: None,
                formatted_expr: Some(spec.formatted),
                format: None,
                path_spec: Some(spec.path),
                returning: Some(returning),
                passing_names: NodeList::from_slice(mcx, &names).unwrap(),
                passing_values: NodeList::from_slice(mcx, &values).unwrap(),
                on_empty: spec.on_empty,
                on_error: Some(spec.on_error),
                use_io_coercion: spec.use_io,
                use_json_coercion: spec.use_json,
                wrapper: spec.wrapper,
                omit_quotes: spec.omit_quotes,
                collation: 0,
                location: -1,
            },
        )
        .unwrap()
    }

    fn eval<'m>(mcx: Mcx<'m>, expr: Node<'m>) -> ::types_error::PgResult<NullableDatum> {
        let mut state = exec_init_expr(mcx, Some(expr), ParamBind::NONE).unwrap().unwrap();
        state.arm_result_mcx(mcx);
        let mut slots = EvalSlots::default();
        exec_eval_expr(&mut state, &mut slots)
    }

    fn jsonb_datum_string(mcx: Mcx<'_>, d: Datum) -> std::string::String {
        // SAFETY: a live 4B-header jsonb image produced by this crate's steps.
        let payload = unsafe {
            let p = d.as_usize() as *const u8;
            let total = ::types_tuple::varatt::varsize_4b(p);
            core::slice::from_raw_parts(p.add(4), total - 4)
        };
        let v = adt_jsonb::io::jsonb_out(mcx, payload).unwrap();
        std::string::String::from_utf8(v[..v.len() - 1].to_vec()).unwrap()
    }

    fn text_datum_string(d: Datum) -> std::string::String {
        // SAFETY: a live 4B-header text image produced by this crate's steps.
        let bytes = unsafe {
            let p = d.as_usize() as *const u8;
            let total = ::types_tuple::varatt::varsize_4b(p);
            core::slice::from_raw_parts(p.add(4), total - 4)
        };
        std::string::String::from_utf8(bytes.to_vec()).unwrap()
    }

    fn exists_spec<'m>(mcx: Mcx<'m>, doc: &str, path: &str, on_error: Node<'m>) -> Spec<'m> {
        Spec {
            op: JOP::JSON_EXISTS_OP,
            formatted: jsonb_const(mcx, doc),
            path: path_const(mcx, path),
            ret_typid: BOOLOID,
            use_io: false,
            use_json: false,
            wrapper: JW::JSW_UNSPEC,
            omit_quotes: false,
            on_empty: None,
            on_error,
            passing: &[],
        }
    }

    #[test]
    fn json_exists_true_false() {
        with_mcx(|mcx| {
            for (path, want) in [("$.a", true), ("$.nope", false)] {
                let on_error = behavior(mcx, JBT::JSON_BEHAVIOR_FALSE, bool_const(mcx, false));
                let expr = mk_json_expr(mcx, exists_spec(mcx, r#"{"a": 1}"#, path, on_error));
                let r = eval(mcx, expr).unwrap();
                assert_eq!((r.isnull, r.value.as_bool()), (false, want), "{path}");
            }
        });
    }

    #[test]
    fn json_exists_error_suppressed_to_false() {
        with_mcx(|mcx| {
            let on_error = behavior(mcx, JBT::JSON_BEHAVIOR_FALSE, bool_const(mcx, false));
            let expr =
                mk_json_expr(mcx, exists_spec(mcx, r#"{"a": 1}"#, "strict $.a.b", on_error));
            let r = eval(mcx, expr).unwrap();
            assert_eq!((r.isnull, r.value.as_bool()), (false, false));
        });
    }

    #[test]
    fn json_exists_error_on_error_throws() {
        with_mcx(|mcx| {
            let on_error = behavior(mcx, JBT::JSON_BEHAVIOR_ERROR, null_const(mcx, BOOLOID));
            let expr =
                mk_json_expr(mcx, exists_spec(mcx, r#"{"a": 1}"#, "strict $.a.b", on_error));
            assert!(eval(mcx, expr).is_err());
        });
    }

    #[test]
    fn json_exists_passing_vars() {
        with_mcx(|mcx| {
            for (v, want) in [(5, true), (1, false)] {
                let passing: &[(&str, Node<'_>)] =
                    ::mcx::leak_in(::mcx::alloc_in(mcx, [("x", mk_int4_const(mcx, Some(v)))]).unwrap());
                let on_error = behavior(mcx, JBT::JSON_BEHAVIOR_FALSE, bool_const(mcx, false));
                let mut spec = exists_spec(mcx, "3", "$ ? (@ < $x)", on_error);
                spec.passing = passing;
                let r = eval(mcx, mk_json_expr(mcx, spec)).unwrap();
                assert_eq!((r.isnull, r.value.as_bool()), (false, want), "x={v}");
            }
        });
    }

    #[test]
    fn json_exists_int_coercion() {
        with_mcx(|mcx| {
            for (path, want) in [("$.a", 1), ("$.nope", 0)] {
                let on_error = behavior(mcx, JBT::JSON_BEHAVIOR_FALSE, mk_int4_const(mcx, Some(0)));
                let mut spec = exists_spec(mcx, r#"{"a": 1}"#, path, on_error);
                spec.ret_typid = INT4OID;
                spec.use_json = true;
                let r = eval(mcx, mk_json_expr(mcx, spec)).unwrap();
                assert_eq!((r.isnull, r.value.as_i32()), (false, want), "{path}");
            }
        });
    }

    fn query_spec<'m>(mcx: Mcx<'m>, doc: &str, path: &str, wrapper: JW) -> Spec<'m> {
        Spec {
            op: JOP::JSON_QUERY_OP,
            formatted: jsonb_const(mcx, doc),
            path: path_const(mcx, path),
            ret_typid: JSONBOID_T,
            use_io: false,
            use_json: false,
            wrapper,
            omit_quotes: false,
            on_empty: Some(behavior(mcx, JBT::JSON_BEHAVIOR_NULL, null_const(mcx, JSONBOID_T))),
            on_error: behavior(mcx, JBT::JSON_BEHAVIOR_NULL, null_const(mcx, JSONBOID_T)),
            passing: &[],
        }
    }

    #[test]
    fn json_query_wrapper_modes() {
        with_mcx(|mcx| {
            let doc = r#"{"a": [1, 2, 3]}"#;
            let cases = [
                ("$.a[*]", JW::JSW_UNCONDITIONAL, Some("[1, 2, 3]")),
                ("$.a[*]", JW::JSW_CONDITIONAL, Some("[1, 2, 3]")),
                ("$.a[0]", JW::JSW_CONDITIONAL, Some("1")),
                ("$.a", JW::JSW_NONE, Some("[1, 2, 3]")),
                // multiple items, no wrapper: error, suppressed to NULL ON ERROR
                ("$.a[*]", JW::JSW_NONE, None),
            ];
            for (path, wrapper, want) in cases {
                let r = eval(mcx, mk_json_expr(mcx, query_spec(mcx, doc, path, wrapper))).unwrap();
                match want {
                    Some(s) => {
                        assert!(!r.isnull, "{path} {wrapper:?}");
                        assert_eq!(jsonb_datum_string(mcx, r.value), s, "{path} {wrapper:?}");
                    }
                    None => assert!(r.isnull, "{path} {wrapper:?}"),
                }
            }
        });
    }

    #[test]
    fn json_query_on_empty_null() {
        with_mcx(|mcx| {
            let r = eval(
                mcx,
                mk_json_expr(mcx, query_spec(mcx, r#"{"a": 1}"#, "$.nope", JW::JSW_UNSPEC)),
            )
            .unwrap();
            assert!(r.isnull);
        });
    }

    #[test]
    fn json_query_omit_quotes_returning_jsonb() {
        with_mcx(|mcx| {
            // "hi" unquoted is not valid jsonb: soft error, NULL ON ERROR.
            let mut spec = query_spec(mcx, r#"{"a": "hi"}"#, "$.a", JW::JSW_UNSPEC);
            spec.omit_quotes = true;
            spec.use_json = true;
            let r = eval(mcx, mk_json_expr(mcx, spec)).unwrap();
            assert!(r.isnull);
            // "1" unquoted parses as the jsonb number 1.
            let mut spec = query_spec(mcx, r#"{"a": "1"}"#, "$.a", JW::JSW_UNSPEC);
            spec.omit_quotes = true;
            spec.use_json = true;
            let r = eval(mcx, mk_json_expr(mcx, spec)).unwrap();
            assert!(!r.isnull);
            assert_eq!(jsonb_datum_string(mcx, r.value), "1");
        });
    }

    #[test]
    fn json_query_coercion_to_int4() {
        with_mcx(|mcx| {
            let mut spec = query_spec(mcx, r#"{"a": 7}"#, "$.a", JW::JSW_UNSPEC);
            spec.ret_typid = INT4OID;
            spec.use_json = true;
            spec.on_empty = Some(behavior(mcx, JBT::JSON_BEHAVIOR_NULL, null_const(mcx, INT4OID)));
            spec.on_error = behavior(mcx, JBT::JSON_BEHAVIOR_NULL, null_const(mcx, INT4OID));
            let r = eval(mcx, mk_json_expr(mcx, spec)).unwrap();
            assert_eq!((r.isnull, r.value.as_i32()), (false, 7));
        });
    }

    #[test]
    fn json_query_coercion_identity_jsonb() {
        with_mcx(|mcx| {
            let mut spec = query_spec(mcx, r#"{"a": [1, 2]}"#, "$.a", JW::JSW_UNSPEC);
            spec.use_json = true;
            let r = eval(mcx, mk_json_expr(mcx, spec)).unwrap();
            assert!(!r.isnull);
            assert_eq!(jsonb_datum_string(mcx, r.value), "[1, 2]");
        });
    }

    fn value_spec<'m>(mcx: Mcx<'m>, doc: &str, path: &str, ret_typid: u32) -> Spec<'m> {
        Spec {
            op: JOP::JSON_VALUE_OP,
            formatted: jsonb_const(mcx, doc),
            path: path_const(mcx, path),
            ret_typid,
            use_io: ret_typid != TEXTOID_T,
            use_json: false,
            wrapper: JW::JSW_UNSPEC,
            omit_quotes: true,
            on_empty: Some(behavior(mcx, JBT::JSON_BEHAVIOR_NULL, null_const(mcx, ret_typid))),
            on_error: behavior(mcx, JBT::JSON_BEHAVIOR_NULL, null_const(mcx, ret_typid)),
            passing: &[],
        }
    }

    #[test]
    fn json_value_returning_text() {
        with_mcx(|mcx| {
            for (doc, path, want) in [
                (r#"{"a": "hi"}"#, "$.a", "hi"),
                (r#"{"a": 1.50}"#, "$.a", "1.50"),
                // C boolout: JSON_VALUE of a boolean renders "t"/"f".
                (r#"{"a": true}"#, "$.a", "t"),
            ] {
                let r = eval(mcx, mk_json_expr(mcx, value_spec(mcx, doc, path, TEXTOID_T)))
                    .unwrap();
                assert!(!r.isnull, "{path}");
                assert_eq!(text_datum_string(r.value), want, "{doc} {path}");
            }
        });
    }

    #[test]
    fn json_value_returning_int4_io_coercion() {
        with_mcx(|mcx| {
            let r = eval(
                mcx,
                mk_json_expr(mcx, value_spec(mcx, r#"{"a": 42}"#, "$.a", INT4OID)),
            )
            .unwrap();
            assert_eq!((r.isnull, r.value.as_i32()), (false, 42));
        });
    }

    #[test]
    fn json_value_returning_jsonb_io_coercion() {
        with_mcx(|mcx| {
            let r = eval(
                mcx,
                mk_json_expr(mcx, value_spec(mcx, r#"{"a": "hi"}"#, "$.a", JSONBOID_T)),
            )
            .unwrap();
            assert!(!r.isnull);
            assert_eq!(jsonb_datum_string(mcx, r.value), "\"hi\"");
        });
    }

    #[test]
    fn json_value_io_error_suppressed_to_null() {
        with_mcx(|mcx| {
            let r = eval(
                mcx,
                mk_json_expr(mcx, value_spec(mcx, r#"{"a": "abc"}"#, "$.a", INT4OID)),
            )
            .unwrap();
            assert!(r.isnull);
        });
    }

    #[test]
    fn json_value_io_error_throws_with_error_on_error() {
        with_mcx(|mcx| {
            let mut spec = value_spec(mcx, r#"{"a": "abc"}"#, "$.a", INT4OID);
            spec.on_error = behavior(mcx, JBT::JSON_BEHAVIOR_ERROR, null_const(mcx, INT4OID));
            let e = eval(mcx, mk_json_expr(mcx, spec)).unwrap_err();
            assert_eq!(e.sqlstate(), ::types_error::ERRCODE_INVALID_TEXT_REPRESENTATION);
        });
    }

    #[test]
    fn json_value_on_error_default_expr() {
        with_mcx(|mcx| {
            let mut spec = value_spec(mcx, r#"{"a": "abc"}"#, "$.a", INT4OID);
            spec.on_error =
                behavior(mcx, JBT::JSON_BEHAVIOR_DEFAULT, mk_int4_const(mcx, Some(7)));
            let r = eval(mcx, mk_json_expr(mcx, spec)).unwrap();
            assert_eq!((r.isnull, r.value.as_i32()), (false, 7));
        });
    }

    #[test]
    fn json_value_on_empty_null_and_default() {
        with_mcx(|mcx| {
            let spec = value_spec(mcx, r#"{"a": 1}"#, "$.nope", INT4OID);
            let r = eval(mcx, mk_json_expr(mcx, spec)).unwrap();
            assert!(r.isnull);

            let mut spec = value_spec(mcx, r#"{"a": 1}"#, "$.nope", INT4OID);
            spec.on_empty = Some(behavior(
                mcx,
                JBT::JSON_BEHAVIOR_DEFAULT,
                mk_int4_const(mcx, Some(5)),
            ));
            let r = eval(mcx, mk_json_expr(mcx, spec)).unwrap();
            assert_eq!((r.isnull, r.value.as_i32()), (false, 5));
        });
    }

    #[test]
    fn json_value_error_on_empty_throws_22035() {
        with_mcx(|mcx| {
            let mut spec = value_spec(mcx, r#"{"a": 1}"#, "$.nope", INT4OID);
            spec.on_empty =
                Some(behavior(mcx, JBT::JSON_BEHAVIOR_ERROR, null_const(mcx, INT4OID)));
            let e = eval(mcx, mk_json_expr(mcx, spec)).unwrap_err();
            assert_eq!(e.sqlstate(), ::types_error::ERRCODE_NO_SQL_JSON_ITEM);
            assert_eq!(e.message(), "no SQL/JSON item found for specified path");
        });
    }

    #[test]
    fn json_value_strict_structural_error_suppressed() {
        with_mcx(|mcx| {
            let spec = value_spec(mcx, r#"{"a": 1}"#, "strict $.a.b.c", TEXTOID_T);
            let r = eval(mcx, mk_json_expr(mcx, spec)).unwrap();
            assert!(r.isnull);
        });
    }

    #[test]
    fn ext_case_test_value_feeds_expression() {
        with_mcx(|mcx| {
            let ct = Node::mk(
                mcx,
                CaseTestExpr { typeId: INT4OID, typeMod: -1, collation: 0 },
            )
            .unwrap();
            let mut state =
                exec_init_expr_with_case_test(mcx, Some(ct), ParamBind::NONE).unwrap().unwrap();
            state.arm_result_mcx(mcx);
            for v in [3i32, -8] {
                state.set_case_test(NullableDatum { value: Datum::from_i32(v), isnull: false });
                let mut slots = EvalSlots::default();
                let r = exec_eval_expr(&mut state, &mut slots).unwrap();
                assert_eq!((r.isnull, r.value.as_i32()), (false, v));
            }
            state.set_case_test(NullableDatum::null());
            let mut slots = EvalSlots::default();
            let r = exec_eval_expr(&mut state, &mut slots).unwrap();
            assert!(r.isnull);
        });
    }

    #[test]
    fn ext_case_test_feeds_json_expr_formatted_expr() {
        with_mcx(|mcx| {
            let ct = Node::mk(
                mcx,
                CaseTestExpr { typeId: JSONBOID_T, typeMod: -1, collation: 0 },
            )
            .unwrap();
            let mut spec = value_spec(mcx, "{}", "$.a", TEXTOID_T);
            spec.formatted = ct;
            let expr = mk_json_expr(mcx, spec);
            let mut state =
                exec_init_expr_with_case_test(mcx, Some(expr), ParamBind::NONE).unwrap().unwrap();
            state.arm_result_mcx(mcx);
            for (doc, want) in [(r#"{"a": "x"}"#, Some("x")), (r#"{"a": "y"}"#, Some("y")), (r#"{"b": 1}"#, None)]
            {
                state.set_case_test(NullableDatum {
                    value: jsonb_datum(mcx, doc),
                    isnull: false,
                });
                let mut slots = EvalSlots::default();
                let r = exec_eval_expr(&mut state, &mut slots).unwrap();
                match want {
                    Some(s) => {
                        assert!(!r.isnull, "{doc}");
                        assert_eq!(text_datum_string(r.value), s, "{doc}");
                    }
                    None => assert!(r.isnull, "{doc}"),
                }
            }
            // NULL input document -> NULL result via the jump-return-null path.
            state.set_case_test(NullableDatum::null());
            let mut slots = EvalSlots::default();
            let r = exec_eval_expr(&mut state, &mut slots).unwrap();
            assert!(r.isnull);
        });
    }

    #[test]
    fn ext_case_test_without_permission_is_clean_feature_error() {
        with_mcx(|mcx| {
            let ct = Node::mk(
                mcx,
                CaseTestExpr { typeId: INT4OID, typeMod: -1, collation: 0 },
            )
            .unwrap();
            let err = match exec_init_expr(mcx, Some(ct), ParamBind::NONE) {
                Ok(_) => panic!("expected a feature error"),
                Err(e) => e,
            };
            assert_eq!(
                err.sqlstate(),
                ::types_error::ERRCODE_FEATURE_NOT_SUPPORTED
            );
            assert!(
                err.message().contains("not yet implemented"),
                "{}",
                err.message()
            );
        });
    }
}
// jit-qual cross-check: drives an unfused program through
// interp::exec_one_step, emulating the emitter's stenciled opcodes (the
// helper refuses those by contract) and following StepFlow for the rest.
fn run_stepwise<'mcx>(
    state: &mut ExprState<'mcx>,
    slots: &mut EvalSlots<'_, 'mcx>,
    mut result_slot: Option<&mut SlotData<'mcx>>,
) -> ::types_error::PgResult<::datum::NullableDatum> {
    use ::datum::NullableDatum;
    use crate::interp::StepFlow;
    let res = state.resnd;
    let mut ix: u32 = 0;
    loop {
        let step = state.steps.as_slice()[ix as usize];
        match step {
            Step::DoneReturn => return Ok(unsafe { res.read() }),
            Step::DoneNoReturn => return Ok(NullableDatum::null()),
            Step::Const { value, isnull, out } => unsafe {
                out.0.write(NullableDatum { value, isnull });
            },
            Step::ScanVar { attnum, out, .. } => {
                let base = slots.scan.as_deref_mut().unwrap().base();
                let nd = NullableDatum {
                    value: base.tts_values[attnum as usize],
                    isnull: base.tts_isnull[attnum as usize],
                };
                unsafe { out.0.write(nd) };
            }
            Step::CaseTestVal { slot, out } => unsafe { out.0.write(slot.read()) },
            Step::FuncExprStrict2 { call, out } => {
                let (a0, a1) = unsafe {
                    (
                        crate::steps::arg_slot_of(call.fcinfo, 0).read(),
                        crate::steps::arg_slot_of(call.fcinfo, 1).read(),
                    )
                };
                let nd = if a0.isnull || a1.isnull {
                    NullableDatum::null()
                } else {
                    let (v, isnull) = crate::interp::invoke(&call)?;
                    NullableDatum { value: v, isnull }
                };
                unsafe { out.0.write(nd) };
            }
            Step::Qual { jumpdone } => {
                let r = unsafe { res.read() };
                if r.isnull || !r.value.as_bool() {
                    unsafe {
                        res.write(NullableDatum { value: Datum::from_bool(false), isnull: false })
                    };
                    ix = jumpdone;
                    continue;
                }
            }
            Step::Jump { jumpdone } => {
                ix = jumpdone;
                continue;
            }
            Step::JumpIfNull { jumpdone, out } => {
                if unsafe { out.0.read() }.isnull {
                    ix = jumpdone;
                    continue;
                }
            }
            Step::JumpIfNotNull { jumpdone, out } => {
                if !unsafe { out.0.read() }.isnull {
                    ix = jumpdone;
                    continue;
                }
            }
            Step::JumpIfNotTrue { jumpdone, out } => {
                let r = unsafe { out.0.read() };
                if r.isnull || !r.value.as_bool() {
                    ix = jumpdone;
                    continue;
                }
            }
            other => {
                assert!(
                    crate::interp::step_has_helper(&other),
                    "stencil {other:?} not emulated by the test driver"
                );
                let mut ret = crate::interp::RetSlots::none();
                match crate::interp::exec_one_step(
                    state,
                    slots,
                    &mut ret,
                    result_slot.as_deref_mut(),
                    ix,
                )? {
                    StepFlow::Next => {}
                    StepFlow::Jump(t) => {
                        ix = t;
                        continue;
                    }
                    StepFlow::Suspend(_) => panic!("unexpected SubPlan suspension"),
                }
            }
        }
        ix += 1;
    }
}

#[test]
fn jit_single_step_matches_run_program() {
    with_mcx(|mcx| {
        crate::compile::SKIP_FUSE_FOR_TESTS.with(|c| c.set(true));

        // Qual: ScanFetchSome via the helper, var/cmp/qual as stencils.
        for vals in [Some(7), Some(8), Some(-7), None] {
            let mk_state = || {
                let args = NodeList::make2(
                    mcx,
                    mk_scan_var(mcx, 1, INT4OID),
                    mk_int4_const(mcx, Some(7)),
                )
                .unwrap();
                let mut s = qual_state(mcx, mk_opexpr(mcx, 147, BOOLOID, args));
                s.force_program_kernel();
                s
            };
            let expected = run_qual(mcx, &mut mk_state(), &[vals]);
            let mut slot = virtual_slot(mcx, &[vals]);
            let mut slots = EvalSlots { scan: Some(&mut slot), inner: None, outer: None };
            let r = run_stepwise(&mut mk_state(), &mut slots, None).unwrap();
            assert!(!r.isnull);
            assert_eq!(r.value.as_bool(), expected, "qual {vals:?}");
        }

        // MinMax helper arm.
        for (least, vals, want) in [
            (true, &[Some(3), Some(1), Some(2)][..], Some(1)),
            (false, &[None, Some(-5), None, Some(4)][..], Some(4)),
            (true, &[None, None][..], None),
        ] {
            let mk_state = || {
                let mut s = exec_init_expr(mcx, Some(mk_minmax(mcx, least, vals)), ParamBind::NONE)
                    .unwrap()
                    .unwrap();
                s.arm_result_mcx(mcx);
                s.force_program_kernel();
                s
            };
            let expected = exec_eval_expr(&mut mk_state(), &mut EvalSlots::default()).unwrap();
            let r = run_stepwise(&mut mk_state(), &mut EvalSlots::default(), None).unwrap();
            assert_eq!(r.isnull, expected.isnull, "minmax {least} {vals:?}");
            assert_eq!(r.isnull, want.is_none());
            if let Some(w) = want {
                assert_eq!(expected.value.as_i32(), w);
                assert_eq!(r.value.as_i32(), w);
            }
        }

        // ScalarArrayOp helper arm (found / not found / null element).
        for (scalar, elems) in [
            (Some(2), &[Some(1), Some(2)][..]),
            (Some(5), &[Some(1), Some(2)][..]),
            (Some(2), &[Some(1), None][..]),
            (None, &[Some(1)][..]),
        ] {
            let mk_state = || {
                let node = mk_saop(
                    mcx,
                    true,
                    mk_int4_const(mcx, scalar),
                    mk_int4_array_const(mcx, elems),
                );
                let mut s =
                    exec_init_expr(mcx, Some(node), ParamBind::NONE).unwrap().unwrap();
                s.arm_result_mcx(mcx);
                s.force_program_kernel();
                s
            };
            let expected = exec_eval_expr(&mut mk_state(), &mut EvalSlots::default()).unwrap();
            let r = run_stepwise(&mut mk_state(), &mut EvalSlots::default(), None).unwrap();
            assert_eq!(r.isnull, expected.isnull, "saop {scalar:?} {elems:?}");
            if !r.isnull {
                assert_eq!(r.value.as_bool(), expected.value.as_bool());
            }
        }

        // Domain family: DomainTestval/DomainNotNull/DomainCheck incl errors.
        for v in [Some(5), Some(0), None] {
            let mk_state = || {
                let mut s =
                    exec_init_expr(mcx, Some(mk_domain_coercion(mcx, v)), ParamBind::NONE)
                        .unwrap()
                        .unwrap();
                s.arm_result_mcx(mcx);
                s.force_program_kernel();
                s
            };
            let expected = exec_eval_expr(&mut mk_state(), &mut EvalSlots::default());
            let got = run_stepwise(&mut mk_state(), &mut EvalSlots::default(), None);
            match (expected, got) {
                (Ok(e), Ok(g)) => {
                    assert_eq!(e.isnull, g.isnull, "domain {v:?}");
                    if !e.isnull {
                        assert_eq!(e.value.as_i32(), g.value.as_i32());
                    }
                }
                (Err(e), Err(g)) => assert_eq!(e.sqlstate(), g.sqlstate(), "domain {v:?}"),
                (e, g) => panic!(
                    "domain {v:?} outcome mismatch: {:?} vs {:?}",
                    e.map(|n| n.isnull),
                    g.map(|n| n.isnull)
                ),
            }
        }

        // Projection: FetchSome + AssignScanVar helpers + DoneNoReturn.
        {
            let desc = desc_int4(mcx, 2);
            let mk_state = || {
                let tle1 =
                    Node::mk_target_entry(mcx, mk_scan_var(mcx, 2, INT4OID), 1, None, false)
                        .unwrap();
                let tle2 =
                    Node::mk_target_entry(mcx, mk_scan_var(mcx, 1, INT4OID), 2, None, false)
                        .unwrap();
                let mut tlist = NodeList::make1(mcx, tle1).unwrap();
                tlist.lappend(mcx, tle2).unwrap();
                let mut s = exec_build_projection_info(mcx, &tlist, Some(&desc), ParamBind::NONE)
                    .unwrap();
                s.force_program_kernel();
                s
            };
            let mut scan = heap_slot(mcx, &[Some(3), Some(4)]);
            let mut result_a = exectuples::make_tuple_table_slot(
                mcx,
                TupleSlotKind::Virtual,
                Some(desc_int4(mcx, 2)),
            );
            {
                let mut slots = EvalSlots { scan: Some(&mut scan), inner: None, outer: None };
                exec_project(&mut mk_state(), &mut slots, &mut result_a, mcx).unwrap();
            }
            let mut scan2 = heap_slot(mcx, &[Some(3), Some(4)]);
            let mut result_b = exectuples::make_tuple_table_slot(
                mcx,
                TupleSlotKind::Virtual,
                Some(desc_int4(mcx, 2)),
            );
            {
                let mut slots = EvalSlots { scan: Some(&mut scan2), inner: None, outer: None };
                let r = run_stepwise(&mut mk_state(), &mut slots, Some(&mut result_b)).unwrap();
                assert!(r.isnull);
            }
            for i in 0..2 {
                assert_eq!(
                    result_a.base().tts_values[i].as_i32(),
                    result_b.base().tts_values[i].as_i32(),
                    "projection col {i}"
                );
                assert!(!result_b.base().tts_isnull[i]);
            }
        }

        crate::compile::SKIP_FUSE_FOR_TESTS.with(|c| c.set(false));
    });
}

// ---- Copy-and-patch JIT parity fuzz (jit.rs) ----
//
// Random expression programs from the census distribution (bool trees, int
// cmp/arith with overflow, NULL mixes, CASE/COALESCE jumps, null/bool
// tests), JIT vs interpreter byte-compared on (value, isnull) and on error
// (message + sqlstate). Off-aarch64 the JIT never engages and the test
// degrades to interpreter self-comparison.

struct Lcg(u64);

impl Lcg {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
        self.0 >> 11
    }

    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
}

const FUZZ_COLS: usize = 3;

fn fuzz_i32(rng: &mut Lcg) -> i32 {
    match rng.below(8) {
        0 => i32::MAX,
        1 => i32::MIN,
        2 => 0,
        3 => 7,
        4 => -500_000,
        5 => 500_000,
        6 => 0x4000_0000,
        _ => rng.next() as i32,
    }
}

fn fuzz_int_expr<'mcx>(mcx: Mcx<'mcx>, rng: &mut Lcg, depth: u32) -> Node<'mcx> {
    match if depth == 0 { rng.below(2) } else { rng.below(6) } {
        0 => mk_scan_var(mcx, (rng.below(FUZZ_COLS as u64) + 1) as i16, INT4OID),
        1 => mk_int4_const(mcx, (rng.below(5) != 0).then(|| fuzz_i32(rng))),
        // int4pl/int4mi/int4mul: the emitter's inline-arith stencils with
        // overflow falling into the real fmgr call.
        2 | 3 => {
            let f = [177u32, 181, 141][rng.below(3) as usize];
            let mut args = NodeList::nil();
            args.lappend(mcx, fuzz_int_expr(mcx, rng, depth - 1)).unwrap();
            args.lappend(mcx, fuzz_int_expr(mcx, rng, depth - 1)).unwrap();
            mk_opexpr(mcx, f, INT4OID, args)
        }
        // CASE WHEN b THEN x ELSE y (JumpIfNotTrue/Jump skeleton).
        4 => {
            let when = ::types_nodes::primnodes::CaseWhen {
                expr: Some(fuzz_bool_expr(mcx, rng, depth - 1)),
                result: Some(fuzz_int_expr(mcx, rng, depth - 1)),
                location: -1,
            };
            let mut args = NodeList::nil();
            args.lappend(mcx, Node::mk(mcx, when).unwrap()).unwrap();
            Node::mk(
                mcx,
                ::types_nodes::primnodes::CaseExpr {
                    casetype: INT4OID,
                    casecollid: 0,
                    arg: None,
                    args,
                    defresult: Some(fuzz_int_expr(mcx, rng, depth - 1)),
                    location: -1,
                },
            )
            .unwrap()
        }
        // COALESCE(x, y) (JumpIfNotNull skeleton).
        _ => {
            let mut args = NodeList::nil();
            args.lappend(mcx, fuzz_int_expr(mcx, rng, depth - 1)).unwrap();
            args.lappend(mcx, fuzz_int_expr(mcx, rng, depth - 1)).unwrap();
            Node::mk(
                mcx,
                ::types_nodes::primnodes::CoalesceExpr {
                    coalescetype: INT4OID,
                    coalescecollid: 0,
                    args,
                    location: -1,
                },
            )
            .unwrap()
        }
    }
}

fn fuzz_bool_expr<'mcx>(mcx: Mcx<'mcx>, rng: &mut Lcg, depth: u32) -> Node<'mcx> {
    use ::types_nodes::primnodes::BoolExprType::{AND_EXPR, NOT_EXPR, OR_EXPR};
    match if depth == 0 { rng.below(2) } else { rng.below(6) } {
        // int4 cmp over int subtrees (CmpOp inline stencils).
        0 | 1 => {
            let f = [65u32, 144, 66, 149, 147, 150][rng.below(6) as usize];
            let mut args = NodeList::nil();
            args.lappend(mcx, fuzz_int_expr(mcx, rng, depth.saturating_sub(1))).unwrap();
            args.lappend(mcx, fuzz_int_expr(mcx, rng, depth.saturating_sub(1))).unwrap();
            mk_opexpr(mcx, f, BOOLOID, args)
        }
        2 => {
            let op = [AND_EXPR, OR_EXPR][rng.below(2) as usize];
            let mut args = NodeList::nil();
            for _ in 0..2 + rng.below(2) {
                args.lappend(mcx, fuzz_bool_expr(mcx, rng, depth - 1)).unwrap();
            }
            Node::mk(
                mcx,
                ::types_nodes::primnodes::BoolExpr { boolop: op, args, location: -1 },
            )
            .unwrap()
        }
        3 => {
            let mut args = NodeList::nil();
            args.lappend(mcx, fuzz_bool_expr(mcx, rng, depth - 1)).unwrap();
            Node::mk(
                mcx,
                ::types_nodes::primnodes::BoolExpr { boolop: NOT_EXPR, args, location: -1 },
            )
            .unwrap()
        }
        4 => Node::mk(
            mcx,
            ::types_nodes::primnodes::NullTest {
                arg: Some(fuzz_int_expr(mcx, rng, depth - 1)),
                nulltesttype: if rng.below(2) == 0 {
                    ::types_nodes::primnodes::NullTestType::IS_NULL
                } else {
                    ::types_nodes::primnodes::NullTestType::IS_NOT_NULL
                },
                argisrow: false,
                location: -1,
            },
        )
        .unwrap(),
        _ => Node::mk(
            mcx,
            ::types_nodes::primnodes::BooleanTest {
                arg: Some(fuzz_bool_expr(mcx, rng, depth - 1)),
                booltesttype: match rng.below(4) {
                    0 => ::types_nodes::primnodes::BoolTestType::IS_TRUE,
                    1 => ::types_nodes::primnodes::BoolTestType::IS_NOT_TRUE,
                    2 => ::types_nodes::primnodes::BoolTestType::IS_FALSE,
                    _ => ::types_nodes::primnodes::BoolTestType::IS_NOT_FALSE,
                },
                location: -1,
            },
        )
        .unwrap(),
    }
}

type FuzzOutcome = Result<(bool, usize), (String, String)>;

fn fuzz_eval<'mcx>(
    mcx: Mcx<'mcx>,
    state: &mut ExprState<'mcx>,
    row: &[Option<i32>],
) -> FuzzOutcome {
    let mut slot = virtual_slot(mcx, row);
    let mut slots = EvalSlots { scan: Some(&mut slot), inner: None, outer: None };
    match exec_eval_expr(state, &mut slots) {
        Ok(nd) => Ok((nd.isnull, if nd.isnull { 0 } else { nd.value.as_usize() })),
        Err(e) => Err((e.message.clone(), format!("{:?}", e.sqlstate))),
    }
}

#[test]
fn jit_parity_fuzz() {
    with_mcx(|mcx| {
        let mut rng = Lcg(0x9E37_79B9_7F4A_7C15);
        let mut jitted = 0usize;
        for tree in 0..300u32 {
            let expr = fuzz_bool_expr(mcx, &mut rng, 3);
            let mut interp =
                exec_init_expr(mcx, Some(expr), ParamBind::NONE).unwrap().unwrap();
            interp.arm_result_mcx(mcx);
            crate::jit::session_begin(crate::jit::PGJIT_PERFORM | crate::jit::PGJIT_EXPR);
            let mut jit = exec_init_expr(mcx, Some(expr), ParamBind::NONE).unwrap().unwrap();
            jit.arm_result_mcx(mcx);
            // Kernels stay alive for the eval loop (estate-collector analog).
            let col = crate::jit::session_end();
            #[cfg(target_arch = "aarch64")]
            if matches!(jit.kernel(), Kernel::Program) {
                assert!(jit.jit.is_some(), "tree {tree}: Program shape refused by the emitter");
            }
            if jit.jit.is_some() {
                jitted += 1;
            }
            for _row in 0..64u32 {
                let row: alloc::vec::Vec<Option<i32>> = (0..FUZZ_COLS)
                    .map(|_| (rng.below(5) != 0).then(|| fuzz_i32(&mut rng)))
                    .collect();
                let want = fuzz_eval(mcx, &mut interp, &row);
                let got = fuzz_eval(mcx, &mut jit, &row);
                assert_eq!(want, got, "tree {tree} row {row:?}");
            }
            drop(col);
        }
        #[cfg(target_arch = "aarch64")]
        assert!(jitted > 0, "no jitted programs in the whole fuzz corpus");
        let _ = jitted;
    });
}

#[test]
fn old_new_var_projection_reads_ret_slots() {
    use ::types_nodes::primnodes::VarReturningType;
    with_mcx(|mcx| {
        let t1 = Node::mk_target_entry(
            mcx,
            mk_ret_var(mcx, 1, VarReturningType::VAR_RETURNING_OLD),
            1,
            None,
            false,
        )
        .unwrap();
        let t2 = Node::mk_target_entry(
            mcx,
            mk_ret_var(mcx, 1, VarReturningType::VAR_RETURNING_NEW),
            2,
            None,
            false,
        )
        .unwrap();
        let tlist = NodeList::make2(mcx, t1, t2).unwrap();
        let desc = desc_int4(mcx, 2);
        let mut state =
            exec_build_projection_info(mcx, &tlist, Some(&desc_int4(mcx, 1)), ParamBind::NONE)
                .unwrap();
        assert!(state.has_old() && state.has_new());
        assert!(state
            .steps()
            .iter()
            .any(|s| matches!(s, Step::OldFetchSome { last_var: 1 })));
        assert!(state
            .steps()
            .iter()
            .any(|s| matches!(s, Step::NewFetchSome { last_var: 1 })));

        let mut scan = heap_slot(mcx, &[Some(10)]);
        let mut old = heap_slot(mcx, &[Some(5)]);
        let mut result =
            exectuples::make_tuple_table_slot(mcx, TupleSlotKind::Virtual, Some(desc));
        state.set_old_new_null(false, false);
        let mut slots = EvalSlots { scan: Some(&mut scan), inner: None, outer: None };
        let mut ret = RetSlots { old: RetSlot::Slot(&mut old), new: RetSlot::Scan };
        exec_project_returning(&mut state, &mut slots, &mut ret, &mut result, mcx).unwrap();
        assert_eq!(result.base().tts_values[0].as_i32(), 5);
        assert_eq!(result.base().tts_values[1].as_i32(), 10);
    });
}

#[test]
fn jit_parity_qual_lists() {
    // Multi-clause qual programs: the Qual stencil's jumpdone legs, false-on-
    // NULL semantics, and the heap-slot FETCHSOME helper path.
    with_mcx(|mcx| {
        let mut rng = Lcg(0xC0FF_EE00_D15E_A5E5);
        for tree in 0..150u32 {
            let mut qual = NodeList::nil();
            for _ in 0..1 + rng.below(3) {
                qual.lappend(mcx, fuzz_bool_expr(mcx, &mut rng, 2)).unwrap();
            }
            let mut interp = exec_init_qual(mcx, &qual, ParamBind::NONE).unwrap().unwrap();
            crate::jit::session_begin(crate::jit::PGJIT_PERFORM | crate::jit::PGJIT_EXPR);
            let mut jit = exec_init_qual(mcx, &qual, ParamBind::NONE).unwrap().unwrap();
            let col = crate::jit::session_end();
            for _row in 0..32u32 {
                let row: alloc::vec::Vec<Option<i32>> = (0..FUZZ_COLS)
                    .map(|_| (rng.below(5) != 0).then(|| fuzz_i32(&mut rng)))
                    .collect();
                let heap = rng.below(2) == 0;
                fn run_one<'mcx>(
                    mcx: Mcx<'mcx>,
                    heap: bool,
                    row: &[Option<i32>],
                    state: &mut ExprState<'mcx>,
                ) -> Result<bool, (String, String)> {
                    let mut slot =
                        if heap { heap_slot(mcx, row) } else { virtual_slot(mcx, row) };
                    let mut slots =
                        EvalSlots { scan: Some(&mut slot), inner: None, outer: None };
                    match exec_qual(Some(state), &mut slots) {
                        Ok(b) => Ok(b),
                        Err(e) => Err((e.message.clone(), format!("{:?}", e.sqlstate))),
                    }
                }
                let want = run_one(mcx, heap, &row, &mut interp);
                let got = run_one(mcx, heap, &row, &mut jit);
                assert_eq!(want, got, "tree {tree} row {row:?} heap={heap}");
            }
            drop(col);
        }
    });
}

#[test]
fn old_var_reads_all_null_substitute() {
    use ::types_nodes::primnodes::VarReturningType;
    with_mcx(|mcx| {
        let tle = Node::mk_target_entry(
            mcx,
            mk_ret_var(mcx, 1, VarReturningType::VAR_RETURNING_OLD),
            1,
            None,
            false,
        )
        .unwrap();
        let tlist = NodeList::make1(mcx, tle).unwrap();
        let mut state =
            exec_build_projection_info(mcx, &tlist, Some(&desc_int4(mcx, 1)), ParamBind::NONE)
                .unwrap();
        let mut scan = heap_slot(mcx, &[Some(10)]);
        let mut allnull = virtual_slot(mcx, &[None]);
        let mut result =
            exectuples::make_tuple_table_slot(mcx, TupleSlotKind::Virtual, Some(desc_int4(mcx, 1)));
        state.set_old_new_null(true, false);
        let mut slots = EvalSlots { scan: Some(&mut scan), inner: None, outer: None };
        let mut ret = RetSlots { old: RetSlot::Slot(&mut allnull), new: RetSlot::Scan };
        exec_project_returning(&mut state, &mut slots, &mut ret, &mut result, mcx).unwrap();
        assert!(result.base().tts_isnull[0]);
    });
}

#[test]
fn returning_expr_step_null_flag_short_circuits() {
    with_mcx(|mcx| {
        let rexpr = Node::mk(
            mcx,
            ::types_nodes::primnodes::ReturningExpr {
                retlevelsup: 0,
                retold: true,
                retexpr: mk_int4_const(mcx, Some(7)),
            },
        )
        .unwrap();
        let mut state = exec_init_expr(mcx, Some(rexpr), ParamBind::NONE).unwrap().unwrap();
        assert!(state.has_old());
        assert!(state
            .steps()
            .iter()
            .any(|s| matches!(s, Step::ReturningExprStep { .. })));
        state.arm_result_mcx(mcx);

        state.set_old_new_null(false, false);
        let mut slots = EvalSlots { scan: None, inner: None, outer: None };
        let r = exec_eval_expr(&mut state, &mut slots).unwrap();
        assert_eq!((r.isnull, r.value.as_i32()), (false, 7));

        state.set_old_new_null(true, false);
        let mut slots = EvalSlots { scan: None, inner: None, outer: None };
        let r = exec_eval_expr(&mut state, &mut slots).unwrap();
        assert!(r.isnull);
    });
}

#[test]
fn whole_row_record_var_blesses_slot_descriptor() {
    with_mcx(|mcx| {
        // Whole-row Var of RECORD type over the scan slot (subquery shape).
        let var = Node::mk_var(mcx, 1, 0, ::types_core::catalog::RECORDOID, -1, 0, 0).unwrap();
        let mut state = exec_init_expr(mcx, Some(var), ParamBind::NONE).unwrap().unwrap();
        assert!(state.steps().iter().any(|s| matches!(s, Step::WholeRow { .. })));
        state.arm_result_mcx(mcx);

        let mut scan = virtual_slot(mcx, &[Some(7), Some(8)]);
        let mut slots = EvalSlots { scan: Some(&mut scan), inner: None, outer: None };
        let r = exec_eval_expr(&mut state, &mut slots).unwrap();
        assert!(!r.isnull);
        // SAFETY: the eval returns a flattened in-memory composite datum.
        let td = unsafe { &*(r.value.as_usize() as *const ::types_tuple::HeapTupleHeaderData) };
        assert_eq!(td.type_id(), ::types_core::catalog::RECORDOID);
        let typmod = td.typmod();
        assert!(typmod >= 0, "RECORD whole-row output must carry a blessed typmod");
        // The blessed typmod resolves back to the slot's shape.
        let desc = ::typcache::lookup_rowtype_tupdesc_copy(
            mcx,
            ::types_core::catalog::RECORDOID,
            typmod,
        )
        .unwrap();
        assert_eq!(desc.natts, 2);
        assert_eq!(desc.attrs[0].atttypid, INT4OID);

        // Second eval reuses the blessed descriptor (first-eval split).
        let mut slots = EvalSlots { scan: Some(&mut scan), inner: None, outer: None };
        let r2 = exec_eval_expr(&mut state, &mut slots).unwrap();
        // SAFETY: as above.
        let td2 = unsafe { &*(r2.value.as_usize() as *const ::types_tuple::HeapTupleHeaderData) };
        assert_eq!(td2.typmod(), typmod);
    });
}

#[test]
fn exec_type_set_col_names_skips_empty_and_dropped() {
    with_mcx(|mcx| {
        let rc = desc_int4(mcx, 3);
        let mut desc = ::tupdesc::CreateTupleDescCopy(mcx, rc.as_ref()).unwrap();
        desc.attr_mut(1).attisdropped = true;
        let mut names = NodeList::nil();
        names.lappend(mcx, Node::mk_string(mcx, "a").unwrap()).unwrap();
        names.lappend(mcx, Node::mk_string(mcx, "b").unwrap()).unwrap();
        names.lappend(mcx, Node::mk_string(mcx, "").unwrap()).unwrap();
        let before2 = desc.attrs[2].attname;
        crate::interp::exec_type_set_col_names(&mut desc, &names);
        assert_eq!(desc.attrs[0].attname.name_str(), b"a");
        // Dropped column keeps its name; empty alias keeps the original.
        assert_ne!(desc.attrs[1].attname.name_str(), b"b");
        assert_eq!(desc.attrs[2].attname.name_str(), before2.name_str());
    });
}

#[test]
fn multiexpr_subplan_compiles_to_setup_steps_and_dummy_const() {
    use ::types_nodes::primnodes::{ParamKind, SubLinkType, SubPlan};
    use ::types_portal::params::ParamExecData;
    use core::ptr::NonNull;

    // C ExecInitSubPlanExpr's stand-in: the token flows into the SubPlan step.
    unsafe fn stub_init(
        _estate: NonNull<()>,
        _node: Node<'_>,
        _agg: Option<crate::compile::AggBind>,
    ) -> ::types_error::PgResult<NonNull<()>> {
        Ok(NonNull::<u8>::dangling().cast())
    }

    with_mcx(|mcx| {
        let mut vals = [ParamExecData::EMPTY, ParamExecData::EMPTY];
        let base = vals.as_mut_ptr();
        let bind = ParamBind {
            extern_params: None,
            exec_vals: core::ptr::NonNull::new(base),
            n_exec: 2,
        };
        let env = crate::compile::SubplanCompileEnv {
            estate: NonNull::<u8>::dangling().cast(),
            init: Some(stub_init),
            agg: None,
            rtable: None,
            parent_subplan_tlist: None,
        };
        // Correlated MULTIEXPR shape: parParam [0] fed by Const 42,
        // setParam [1] written by the (suspended) subplan run.
        let subplan = Node::mk(
            mcx,
            SubPlan {
                subLinkType: SubLinkType::MULTIEXPR_SUBLINK,
                testexpr: None,
                paramIds: ::types_nodes::list::IntList::nil(),
                plan_id: 1,
                plan_name: Some("SubPlan 1"),
                firstColType: INT4OID,
                firstColTypmod: -1,
                firstColCollation: 0,
                useHashTable: false,
                unknownEqFalse: false,
                parallel_safe: false,
                setParam: ::types_nodes::list::IntList::make1(mcx, 1).unwrap(),
                parParam: ::types_nodes::list::IntList::make1(mcx, 0).unwrap(),
                args: NodeList::make1(mcx, mk_int4_const(mcx, Some(42))).unwrap(),
                startup_cost: 0.0,
                per_call_cost: 0.0,
            },
        )
        .unwrap();
        let mut state = crate::compile::exec_init_expr_subplans(mcx, Some(subplan), bind, Some(env))
            .unwrap()
            .unwrap();
        // Program shape: setup steps run the SubPlan (arg eval + PARAM_SET +
        // SUBPLAN) before the body; the in-tree SubPlan is a dummy NULL const.
        assert!(matches!(state.steps()[0], Step::Const { .. }));
        assert!(matches!(state.steps()[1], Step::ParamSet { .. }));
        assert!(matches!(state.steps()[2], Step::SubPlan { .. }));
        assert!(
            matches!(state.steps()[3], Step::Const { isnull: true, .. }),
            "in-tree MULTIEXPR SubPlan is a dummy NULL"
        );

        let mut slots = EvalSlots::default();
        let outcome = crate::interp::exec_eval_expr_outcome(&mut state, &mut slots, None).unwrap();
        let crate::interp::EvalOutcome::Suspended(s) = outcome else {
            panic!("MULTIEXPR setup must suspend on the SubPlan step");
        };
        // The arg eval + EEOP_PARAM_SET ran before the suspension.
        assert_eq!(vals[0].value.as_i32(), 42);
        assert!(!vals[0].isnull);
        // The driver (nodeSubplan) would fill the setParams; the resumed
        // program then reads them via the referencing PARAM_EXEC Params.
        let outcome = crate::interp::exec_eval_expr_outcome(
            &mut state,
            &mut slots,
            Some(s.resume_with(::datum::NullableDatum::null())),
        )
        .unwrap();
        let crate::interp::EvalOutcome::Done(r) = outcome else {
            panic!("resume must complete the program");
        };
        assert!(r.isnull, "in-tree MULTIEXPR SubPlan yields NULL::record");
    });
}

// --- lanereg conformance (design §3a batch-function registry) ---------------
// `CmpOp::for_fn_oid` and the JIT arithmetic admission are now driven by the
// central `lanereg` census. These tests pin the registry-backed lookups to the
// exact golden OID tables (the legacy 30 comparator families plus the 42
// censusgaps additions), so the registry cannot drift from this consumer.

// The golden OID→CmpOp table: the pre-registry 30-arm literal table verbatim,
// extended by the censusgaps int24/int42/oid/float families and the
// ne-admission census-close date/timestamp/timestamptz aliases (plain int
// compares incl. infinity sentinels — date.c / timestamp.c; the mapping
// laneexec's translate whitelist has carried since the harvest).
fn golden_for_fn_oid(oid: ::types_core::Oid) -> Option<CmpOp> {
    Some(match oid {
        65 => CmpOp::Int4Eq,
        144 => CmpOp::Int4Ne,
        66 => CmpOp::Int4Lt,
        149 => CmpOp::Int4Le,
        147 => CmpOp::Int4Gt,
        150 => CmpOp::Int4Ge,
        467 => CmpOp::Int8Eq,
        468 => CmpOp::Int8Ne,
        469 => CmpOp::Int8Lt,
        471 => CmpOp::Int8Le,
        470 => CmpOp::Int8Gt,
        472 => CmpOp::Int8Ge,
        63 => CmpOp::Int2Eq,
        145 => CmpOp::Int2Ne,
        64 => CmpOp::Int2Lt,
        148 => CmpOp::Int2Le,
        146 => CmpOp::Int2Gt,
        151 => CmpOp::Int2Ge,
        474 => CmpOp::Int84Eq,
        475 => CmpOp::Int84Ne,
        476 => CmpOp::Int84Lt,
        478 => CmpOp::Int84Le,
        477 => CmpOp::Int84Gt,
        479 => CmpOp::Int84Ge,
        852 => CmpOp::Int48Eq,
        853 => CmpOp::Int48Ne,
        854 => CmpOp::Int48Lt,
        856 => CmpOp::Int48Le,
        855 => CmpOp::Int48Gt,
        857 => CmpOp::Int48Ge,
        158 => CmpOp::Int24Eq,
        164 => CmpOp::Int24Ne,
        160 => CmpOp::Int24Lt,
        166 => CmpOp::Int24Le,
        162 => CmpOp::Int24Gt,
        168 => CmpOp::Int24Ge,
        159 => CmpOp::Int42Eq,
        165 => CmpOp::Int42Ne,
        161 => CmpOp::Int42Lt,
        167 => CmpOp::Int42Le,
        163 => CmpOp::Int42Gt,
        169 => CmpOp::Int42Ge,
        184 => CmpOp::OidEq,
        185 => CmpOp::OidNe,
        716 => CmpOp::OidLt,
        717 => CmpOp::OidLe,
        1638 => CmpOp::OidGt,
        1639 => CmpOp::OidGe,
        287 => CmpOp::Float4Eq,
        288 => CmpOp::Float4Ne,
        289 => CmpOp::Float4Lt,
        290 => CmpOp::Float4Le,
        291 => CmpOp::Float4Gt,
        292 => CmpOp::Float4Ge,
        293 => CmpOp::Float8Eq,
        294 => CmpOp::Float8Ne,
        295 => CmpOp::Float8Lt,
        296 => CmpOp::Float8Le,
        297 => CmpOp::Float8Gt,
        298 => CmpOp::Float8Ge,
        299 => CmpOp::Float48Eq,
        300 => CmpOp::Float48Ne,
        301 => CmpOp::Float48Lt,
        302 => CmpOp::Float48Le,
        303 => CmpOp::Float48Gt,
        304 => CmpOp::Float48Ge,
        305 => CmpOp::Float84Eq,
        306 => CmpOp::Float84Ne,
        307 => CmpOp::Float84Lt,
        308 => CmpOp::Float84Le,
        309 => CmpOp::Float84Gt,
        310 => CmpOp::Float84Ge,
        // date (int32 days)
        1086 => CmpOp::Int4Eq,
        1091 => CmpOp::Int4Ne,
        1087 => CmpOp::Int4Lt,
        1088 => CmpOp::Int4Le,
        1089 => CmpOp::Int4Gt,
        1090 => CmpOp::Int4Ge,
        // timestamp / timestamptz (int64 microseconds)
        2052 | 1152 => CmpOp::Int8Eq,
        2053 | 1153 => CmpOp::Int8Ne,
        2054 | 1154 => CmpOp::Int8Lt,
        2055 | 1155 => CmpOp::Int8Le,
        2057 | 1157 => CmpOp::Int8Gt,
        2056 | 1156 => CmpOp::Int8Ge,
        _ => return None,
    })
}

#[test]
fn for_fn_oid_matches_golden_over_full_oid_sweep() {
    // Covers every comparator OID plus the fold/arith OID neighborhoods, so a
    // stray admission (or dropped one) anywhere in 0..=3000 fails loud.
    for oid in 0u32..=3000 {
        assert_eq!(
            CmpOp::for_fn_oid(oid),
            golden_for_fn_oid(oid),
            "for_fn_oid drifted from golden at oid {oid}"
        );
    }
}

#[test]
fn jit_inline_arith_matches_golden_set() {
    // The six pre-registry arithmetic OIDs + the censusgaps int2/int4 mixed
    // family (int24/int42 pl/mi/mul, int24div).
    let golden: &[u32] = &[177, 181, 141, 463, 464, 465, 178, 179, 182, 183, 170, 171, 172];
    for oid in 0u32..=3000 {
        let admitted = ::lanereg::jit_arith(oid).is_some();
        assert_eq!(admitted, golden.contains(&oid), "jit arith admission drifted at oid {oid}");
    }
}

// --- censusgaps parity: the 42 new CmpOp families vs the ported per-row ------
// functions, over boundary pools (INT_MIN/MAX, NaN/±0/±inf, NULL handling).
// The ported fns (adt_int / adt_scalar / adt_float) are the audited C bodies,
// so equality here IS C parity for the batch kernels.

fn pool_i16() -> Vec<i16> {
    vec![i16::MIN, i16::MIN + 1, -7, -1, 0, 1, 7, i16::MAX - 1, i16::MAX]
}

fn pool_i32() -> Vec<i32> {
    vec![
        i32::MIN,
        i32::MIN + 1,
        -32769,
        -32768,
        -7,
        -1,
        0,
        1,
        7,
        32767,
        32768,
        i32::MAX - 1,
        i32::MAX,
    ]
}

fn pool_i64() -> Vec<i64> {
    vec![
        i64::MIN,
        i64::MIN + 1,
        i32::MIN as i64 - 1,
        i32::MIN as i64,
        -1,
        0,
        1,
        i32::MAX as i64,
        i32::MAX as i64 + 1,
        i64::MAX - 1,
        i64::MAX,
    ]
}

fn pool_u32() -> Vec<u32> {
    vec![0, 1, 2, 7, 0x7FFF_FFFE, 0x7FFF_FFFF, 0x8000_0000, 0x8000_0001, u32::MAX - 1, u32::MAX]
}

fn pool_f32() -> Vec<f32> {
    vec![
        f32::NAN,
        -f32::NAN, // distinct NaN payload/sign: all NaNs must compare equal
        f32::INFINITY,
        f32::NEG_INFINITY,
        0.0,
        -0.0,
        1.0,
        -1.0,
        1.5,
        f32::MIN,
        f32::MAX,
        f32::MIN_POSITIVE,
        1e-45, // subnormal
        -1e-45,
    ]
}

fn pool_f64() -> Vec<f64> {
    vec![
        f64::NAN,
        -f64::NAN,
        f64::INFINITY,
        f64::NEG_INFINITY,
        0.0,
        -0.0,
        1.0,
        -1.0,
        1.5,
        // straddle the f32 range so f32-vs-f64 cross compares see values a
        // float4 cannot represent
        f32::MAX as f64 * 2.0,
        f32::MIN as f64 * 2.0,
        f64::MIN,
        f64::MAX,
        f64::MIN_POSITIVE,
        5e-324, // subnormal
    ]
}

// eval + commuted parity in one pass: the kernel evaluates a fused
// (const, var) call as commuted(cmp)(var, const), so for every op we also
// assert op.eval(a, b) == op.commuted().eval(b, a).
macro_rules! cmp_parity {
    ($a:expr, $b:expr, $da:expr, $db:expr, $($op:path => $ported:expr;)*) => {{
        $(
            assert_eq!(
                $op.eval($da, $db),
                $ported,
                "{:?} eval parity at ({:?}, {:?})", $op, $a, $b
            );
            assert_eq!(
                $op.commuted().eval($db, $da),
                $op.eval($da, $db),
                "{:?} commuted parity at ({:?}, {:?})", $op, $a, $b
            );
        )*
    }};
}

#[test]
fn cmp_eval_int24_int42_matches_ported() {
    for &a in &pool_i16() {
        for &b in &pool_i32() {
            let (da, db) = (Datum::from_i16(a), Datum::from_i32(b));
            cmp_parity!(a, b, da, db,
                CmpOp::Int24Eq => ::adt_int::int24eq(a, b);
                CmpOp::Int24Ne => ::adt_int::int24ne(a, b);
                CmpOp::Int24Lt => ::adt_int::int24lt(a, b);
                CmpOp::Int24Le => ::adt_int::int24le(a, b);
                CmpOp::Int24Gt => ::adt_int::int24gt(a, b);
                CmpOp::Int24Ge => ::adt_int::int24ge(a, b);
            );
            let (da, db) = (Datum::from_i32(b), Datum::from_i16(a));
            cmp_parity!(b, a, da, db,
                CmpOp::Int42Eq => ::adt_int::int42eq(b, a);
                CmpOp::Int42Ne => ::adt_int::int42ne(b, a);
                CmpOp::Int42Lt => ::adt_int::int42lt(b, a);
                CmpOp::Int42Le => ::adt_int::int42le(b, a);
                CmpOp::Int42Gt => ::adt_int::int42gt(b, a);
                CmpOp::Int42Ge => ::adt_int::int42ge(b, a);
            );
        }
    }
}

#[test]
fn cmp_eval_oid_matches_ported() {
    for &a in &pool_u32() {
        for &b in &pool_u32() {
            let (da, db) = (Datum::from_u32(a), Datum::from_u32(b));
            cmp_parity!(a, b, da, db,
                CmpOp::OidEq => ::adt_scalar::oideq(a, b);
                CmpOp::OidNe => ::adt_scalar::oidne(a, b);
                CmpOp::OidLt => ::adt_scalar::oidlt(a, b);
                CmpOp::OidLe => ::adt_scalar::oidle(a, b);
                CmpOp::OidGt => ::adt_scalar::oidgt(a, b);
                CmpOp::OidGe => ::adt_scalar::oidge(a, b);
            );
        }
    }
}

#[test]
fn cmp_eval_float_matches_ported() {
    for &a in &pool_f32() {
        for &b in &pool_f32() {
            let (da, db) = (Datum::from_f32(a), Datum::from_f32(b));
            cmp_parity!(a, b, da, db,
                CmpOp::Float4Eq => ::adt_float::float4_eq(a, b);
                CmpOp::Float4Ne => ::adt_float::float4_ne(a, b);
                CmpOp::Float4Lt => ::adt_float::float4_lt(a, b);
                CmpOp::Float4Le => ::adt_float::float4_le(a, b);
                CmpOp::Float4Gt => ::adt_float::float4_gt(a, b);
                CmpOp::Float4Ge => ::adt_float::float4_ge(a, b);
            );
        }
    }
    for &a in &pool_f64() {
        for &b in &pool_f64() {
            let (da, db) = (Datum::from_f64(a), Datum::from_f64(b));
            cmp_parity!(a, b, da, db,
                CmpOp::Float8Eq => ::adt_float::float8_eq(a, b);
                CmpOp::Float8Ne => ::adt_float::float8_ne(a, b);
                CmpOp::Float8Lt => ::adt_float::float8_lt(a, b);
                CmpOp::Float8Le => ::adt_float::float8_le(a, b);
                CmpOp::Float8Gt => ::adt_float::float8_gt(a, b);
                CmpOp::Float8Ge => ::adt_float::float8_ge(a, b);
            );
        }
    }
    for &a in &pool_f32() {
        for &b in &pool_f64() {
            let (da, db) = (Datum::from_f32(a), Datum::from_f64(b));
            cmp_parity!(a, b, da, db,
                CmpOp::Float48Eq => ::adt_float::float48eq(a, b);
                CmpOp::Float48Ne => ::adt_float::float48ne(a, b);
                CmpOp::Float48Lt => ::adt_float::float48lt(a, b);
                CmpOp::Float48Le => ::adt_float::float48le(a, b);
                CmpOp::Float48Gt => ::adt_float::float48gt(a, b);
                CmpOp::Float48Ge => ::adt_float::float48ge(a, b);
            );
            let (da, db) = (Datum::from_f64(b), Datum::from_f32(a));
            cmp_parity!(b, a, da, db,
                CmpOp::Float84Eq => ::adt_float::float84eq(b, a);
                CmpOp::Float84Ne => ::adt_float::float84ne(b, a);
                CmpOp::Float84Lt => ::adt_float::float84lt(b, a);
                CmpOp::Float84Le => ::adt_float::float84le(b, a);
                CmpOp::Float84Gt => ::adt_float::float84gt(b, a);
                CmpOp::Float84Ge => ::adt_float::float84ge(b, a);
            );
        }
    }
}

// Legacy int families: eval was already conformance-pinned; bind commuted()
// over the boundary pools too so the (const, var) flip stays C-exact for
// every census family.
#[test]
fn cmp_commuted_matches_swapped_args_int_families() {
    use CmpOp::*;
    let i16s = pool_i16();
    let i32s = pool_i32();
    let i64s = pool_i64();
    for &op in &[Int2Eq, Int2Ne, Int2Lt, Int2Le, Int2Gt, Int2Ge] {
        for &a in &i16s {
            for &b in &i16s {
                let (da, db) = (Datum::from_i16(a), Datum::from_i16(b));
                assert_eq!(op.commuted().eval(db, da), op.eval(da, db), "{op:?} ({a}, {b})");
            }
        }
    }
    for &op in &[Int4Eq, Int4Ne, Int4Lt, Int4Le, Int4Gt, Int4Ge] {
        for &a in &i32s {
            for &b in &i32s {
                let (da, db) = (Datum::from_i32(a), Datum::from_i32(b));
                assert_eq!(op.commuted().eval(db, da), op.eval(da, db), "{op:?} ({a}, {b})");
            }
        }
    }
    for &op in &[Int8Eq, Int8Ne, Int8Lt, Int8Le, Int8Gt, Int8Ge] {
        for &a in &i64s {
            for &b in &i64s {
                let (da, db) = (Datum::from_i64(a), Datum::from_i64(b));
                assert_eq!(op.commuted().eval(db, da), op.eval(da, db), "{op:?} ({a}, {b})");
            }
        }
    }
    for &op in &[Int84Eq, Int84Ne, Int84Lt, Int84Le, Int84Gt, Int84Ge] {
        for &a in &i64s {
            for &b in &i32s {
                let (da, db) = (Datum::from_i64(a), Datum::from_i32(b));
                assert_eq!(op.commuted().eval(db, da), op.eval(da, db), "{op:?} ({a}, {b})");
            }
        }
    }
    for &op in &[Int48Eq, Int48Ne, Int48Lt, Int48Le, Int48Gt, Int48Ge] {
        for &a in &i32s {
            for &b in &i64s {
                let (da, db) = (Datum::from_i32(a), Datum::from_i64(b));
                assert_eq!(op.commuted().eval(db, da), op.eval(da, db), "{op:?} ({a}, {b})");
            }
        }
    }
}

// Bitmap-kernel parity: for EVERY census comparator (all 72), the packed
// selection word must equal the per-row `!isnull && op.eval(v, konst)` for
// each konst in the family pool — including a >64-row batch so word packing
// and tail lanes are exercised.
#[test]
fn qual_bitmap_matches_eval_for_all_census_ops() {
    use crate::steps::qual_bitmap_cmp_const;
    // (var-side pool, konst-side pool) per census op, as canonical Datums.
    fn pools(op: CmpOp) -> (Vec<Datum>, Vec<Datum>) {
        use CmpOp::*;
        let i16d: Vec<Datum> = pool_i16().iter().map(|&v| Datum::from_i16(v)).collect();
        let i32d: Vec<Datum> = pool_i32().iter().map(|&v| Datum::from_i32(v)).collect();
        let i64d: Vec<Datum> = pool_i64().iter().map(|&v| Datum::from_i64(v)).collect();
        let u32d: Vec<Datum> = pool_u32().iter().map(|&v| Datum::from_u32(v)).collect();
        let f32d: Vec<Datum> = pool_f32().iter().map(|&v| Datum::from_f32(v)).collect();
        let f64d: Vec<Datum> = pool_f64().iter().map(|&v| Datum::from_f64(v)).collect();
        match op {
            Int2Eq | Int2Ne | Int2Lt | Int2Le | Int2Gt | Int2Ge => (i16d.clone(), i16d),
            Int4Eq | Int4Ne | Int4Lt | Int4Le | Int4Gt | Int4Ge => (i32d.clone(), i32d),
            Int8Eq | Int8Ne | Int8Lt | Int8Le | Int8Gt | Int8Ge => (i64d.clone(), i64d),
            Int84Eq | Int84Ne | Int84Lt | Int84Le | Int84Gt | Int84Ge => (i64d, i32d),
            Int48Eq | Int48Ne | Int48Lt | Int48Le | Int48Gt | Int48Ge => (i32d, i64d),
            Int24Eq | Int24Ne | Int24Lt | Int24Le | Int24Gt | Int24Ge => (i16d, i32d),
            Int42Eq | Int42Ne | Int42Lt | Int42Le | Int42Gt | Int42Ge => (i32d, i16d),
            OidEq | OidNe | OidLt | OidLe | OidGt | OidGe => (u32d.clone(), u32d),
            Float4Eq | Float4Ne | Float4Lt | Float4Le | Float4Gt | Float4Ge => {
                (f32d.clone(), f32d)
            }
            Float8Eq | Float8Ne | Float8Lt | Float8Le | Float8Gt | Float8Ge => {
                (f64d.clone(), f64d)
            }
            Float48Eq | Float48Ne | Float48Lt | Float48Le | Float48Gt | Float48Ge => (f32d, f64d),
            Float84Eq | Float84Ne | Float84Lt | Float84Le | Float84Gt | Float84Ge => (f64d, f32d),
        }
    }
    for oid in 0u32..=3000 {
        let Some(op) = CmpOp::for_fn_oid(oid) else { continue };
        let (vpool, kpool) = pools(op);
        // >64 rows: cycle the pool with a null every 5th row.
        let n = 150usize;
        let values: Vec<Datum> = (0..n).map(|i| vpool[i % vpool.len()]).collect();
        let isnull: Vec<bool> = (0..n).map(|i| i % 5 == 4).collect();
        for &konst in &kpool {
            let mut sel = vec![0u64; n.div_ceil(64)];
            qual_bitmap_cmp_const(op, konst, &values, &isnull, &mut sel);
            for i in 0..n {
                let want = !isnull[i] && op.eval(values[i], konst);
                let got = sel[i / 64] >> (i % 64) & 1 == 1;
                assert_eq!(got, want, "{op:?} row {i} konst {konst:?}");
            }
        }
    }
}

// --- censusgaps engagement: fused qual kernels for the new families ----------

fn mk_const_typed<'mcx>(mcx: Mcx<'mcx>, typ: u32, len: i16, v: Datum) -> Node<'mcx> {
    Node::mk_const(mcx, typ, -1, 0, len as i32, v, false, true).unwrap()
}

// Tuple desc over arbitrary fixed-width byval columns: (typid, len) per att.
fn desc_typed<'mcx>(mcx: Mcx<'mcx>, cols: &[(u32, i16)]) -> Rc<TupleDescData<'mcx>> {
    let mut attrs = PgVec::new_in(mcx);
    let mut compact = PgVec::new_in(mcx);
    for (i, &(typid, len)) in cols.iter().enumerate() {
        let att = FormData_pg_attribute {
            attnum: (i + 1) as i16,
            atttypid: typid,
            attlen: len,
            attbyval: true,
            attalign: match len {
                2 => b's' as i8,
                8 => b'd' as i8,
                _ => TYPALIGN_INT,
            },
            attstorage: TYPSTORAGE_PLAIN,
            ..Default::default()
        };
        compact.push(CompactAttribute::populate_from(&att));
        attrs.push(att);
    }
    Rc::new(TupleDescData {
        natts: cols.len() as i32,
        tdtypeid: 0,
        tdtypmod: -1,
        tdrefcount: -1,
        constr: None,
        compact_attrs: compact,
        attrs,
    })
}

fn virtual_slot_typed<'mcx>(
    mcx: Mcx<'mcx>,
    cols: &[(u32, i16, Option<Datum>)],
) -> SlotData<'mcx> {
    let shape: Vec<(u32, i16)> = cols.iter().map(|&(t, l, _)| (t, l)).collect();
    let mut slot = exectuples::make_tuple_table_slot(
        mcx,
        TupleSlotKind::Virtual,
        Some(desc_typed(mcx, &shape)),
    );
    {
        let base = slot.base_mut();
        for (i, (_, _, v)) in cols.iter().enumerate() {
            match v {
                Some(x) => {
                    base.tts_values[i] = *x;
                    base.tts_isnull[i] = false;
                }
                None => {
                    base.tts_values[i] = Datum::null();
                    base.tts_isnull[i] = true;
                }
            }
        }
    }
    exectuples::exec_store_virtual_tuple(&mut slot);
    slot
}

fn run_qual_d<'mcx>(
    mcx: Mcx<'mcx>,
    state: &mut ExprState<'mcx>,
    cols: &[(u32, i16, Option<Datum>)],
) -> bool {
    let mut slot = virtual_slot_typed(mcx, cols);
    let mut slots = EvalSlots { scan: Some(&mut slot), inner: None, outer: None };
    exec_qual(Some(state), &mut slots).unwrap()
}

// A float8 qual that previously refused the fused kernel (scalar fmgr per
// row) now takes the AOT bitmap tier's kernel — the engagement smoke for the
// censusgaps comparator families, NaN semantics included.
#[test]
fn fused_qual_kernel_float8_var_lt_const() {
    const FLOAT8OID: u32 = 701;
    with_mcx(|mcx| {
        let args = NodeList::make2(
            mcx,
            mk_scan_var(mcx, 1, FLOAT8OID),
            mk_const_typed(mcx, FLOAT8OID, 8, Datum::from_f64(1.5)),
        )
        .unwrap();
        let mut state = qual_state(mcx, mk_opexpr(mcx, 295, BOOLOID, args));
        assert!(matches!(
            state.kernel(),
            Kernel::QualScanVarCmpConst { attnum: 0, cmp: CmpOp::Float8Lt, .. }
        ));
        let row = |v: Option<f64>| [(FLOAT8OID, 8i16, v.map(Datum::from_f64))];
        assert!(run_qual_d(mcx, &mut state, &row(Some(1.0))));
        assert!(run_qual_d(mcx, &mut state, &row(Some(-0.0))));
        assert!(!run_qual_d(mcx, &mut state, &row(Some(1.5))));
        assert!(!run_qual_d(mcx, &mut state, &row(Some(2.0))));
        // NaN sorts greatest: NaN < 1.5 is false.
        assert!(!run_qual_d(mcx, &mut state, &row(Some(f64::NAN))));
        assert!(run_qual_d(mcx, &mut state, &row(Some(f64::NEG_INFINITY))));
        assert!(!run_qual_d(mcx, &mut state, &row(None)));
    });
}

// (const float4) > (var float8): float48gt commutes to Float84Lt on the var.
#[test]
fn fused_qual_kernel_commuted_float48() {
    const FLOAT4OID: u32 = 700;
    const FLOAT8OID: u32 = 701;
    with_mcx(|mcx| {
        let args = NodeList::make2(
            mcx,
            mk_const_typed(mcx, FLOAT4OID, 4, Datum::from_f32(2.5)),
            mk_scan_var(mcx, 1, FLOAT8OID),
        )
        .unwrap();
        let mut state = qual_state(mcx, mk_opexpr(mcx, 303, BOOLOID, args));
        assert!(matches!(
            state.kernel(),
            Kernel::QualScanVarCmpConst { cmp: CmpOp::Float84Lt, .. }
        ));
        let row = |v: Option<f64>| [(FLOAT8OID, 8i16, v.map(Datum::from_f64))];
        assert!(run_qual_d(mcx, &mut state, &row(Some(1.0))));
        assert!(!run_qual_d(mcx, &mut state, &row(Some(2.5))));
        assert!(!run_qual_d(mcx, &mut state, &row(Some(3.0))));
        // 2.5 > NaN is false (NaN sorts greatest).
        assert!(!run_qual_d(mcx, &mut state, &row(Some(f64::NAN))));
    });
}

#[test]
fn fused_qual_kernel_oid_unsigned() {
    const OIDOID: u32 = 26;
    with_mcx(|mcx| {
        let args = NodeList::make2(
            mcx,
            mk_scan_var(mcx, 1, OIDOID),
            mk_const_typed(mcx, OIDOID, 4, Datum::from_u32(0x8000_0000)),
        )
        .unwrap();
        let mut state = qual_state(mcx, mk_opexpr(mcx, 716, BOOLOID, args));
        assert!(matches!(
            state.kernel(),
            Kernel::QualScanVarCmpConst { attnum: 0, cmp: CmpOp::OidLt, .. }
        ));
        // Unsigned order: 0x7FFFFFFF < 0x80000000 but NOT as signed i32.
        let row = |v: Option<u32>| [(OIDOID, 4i16, v.map(Datum::from_u32))];
        assert!(run_qual_d(mcx, &mut state, &row(Some(0x7FFF_FFFF))));
        assert!(run_qual_d(mcx, &mut state, &row(Some(0))));
        assert!(!run_qual_d(mcx, &mut state, &row(Some(0x8000_0000))));
        assert!(!run_qual_d(mcx, &mut state, &row(Some(u32::MAX))));
    });
}

#[test]
fn fused_qual_kernel_int24_and_int42() {
    const INT2OID: u32 = 21;
    with_mcx(|mcx| {
        // var int2 < const int4 (int24lt, oid 160).
        let args = NodeList::make2(
            mcx,
            mk_scan_var(mcx, 1, INT2OID),
            mk_int4_const(mcx, Some(100_000)),
        )
        .unwrap();
        let mut state = qual_state(mcx, mk_opexpr(mcx, 160, BOOLOID, args));
        assert!(matches!(
            state.kernel(),
            Kernel::QualScanVarCmpConst { attnum: 0, cmp: CmpOp::Int24Lt, .. }
        ));
        // Every int2 value is < 100000 after C's promotion.
        let row = |v: Option<i16>| [(INT2OID, 2i16, v.map(Datum::from_i16))];
        assert!(run_qual_d(mcx, &mut state, &row(Some(i16::MAX))));
        assert!(run_qual_d(mcx, &mut state, &row(Some(i16::MIN))));
        assert!(!run_qual_d(mcx, &mut state, &row(None)));
        // (const int4) < (var int2): int42lt (oid 161) commutes to Int24Gt
        // on the var.
        let args = NodeList::make2(
            mcx,
            mk_int4_const(mcx, Some(-7)),
            mk_scan_var(mcx, 1, INT2OID),
        )
        .unwrap();
        let mut state = qual_state(mcx, mk_opexpr(mcx, 161, BOOLOID, args));
        assert!(matches!(
            state.kernel(),
            Kernel::QualScanVarCmpConst { cmp: CmpOp::Int24Gt, .. }
        ));
        assert!(run_qual_d(mcx, &mut state, &row(Some(0))));
        assert!(!run_qual_d(mcx, &mut state, &row(Some(-7))));
        assert!(!run_qual_d(mcx, &mut state, &row(Some(-8))));
    });
}

// --- censusgaps JIT parity: the new inline stencils vs the interpreter -------
// Mixed int2/int4 comparators + oid unsigned comparators + the int24/int42
// arithmetic family (incl. int24div's division-by-zero and the overflow
// replay paths), byte-compared JIT vs interpreter on (value, isnull) and on
// error (message + sqlstate). Off-aarch64 the JIT never engages and this
// degrades to interpreter self-comparison (same as jit_parity_fuzz).
#[test]
fn jit_parity_censusgaps_inline_ops() {
    const INT2OID: u32 = 21;
    const OIDOID: u32 = 26;
    with_mcx(|mcx| {
        // CASE WHEN <bool expr> THEN 1 ELSE 0 END: forces the Program kernel
        // (jump skeleton) so the copy-and-patch emitter owns the FuncExpr.
        fn wrap_bool<'mcx>(mcx: Mcx<'mcx>, cond: Node<'mcx>) -> Node<'mcx> {
            let when = ::types_nodes::primnodes::CaseWhen {
                expr: Some(cond),
                result: Some(mk_int4_const(mcx, Some(1))),
                location: -1,
            };
            let mut args = NodeList::nil();
            args.lappend(mcx, Node::mk(mcx, when).unwrap()).unwrap();
            Node::mk(
                mcx,
                ::types_nodes::primnodes::CaseExpr {
                    casetype: INT4OID,
                    casecollid: 0,
                    arg: None,
                    args,
                    defresult: Some(mk_int4_const(mcx, Some(0))),
                    location: -1,
                },
            )
            .unwrap()
        }
        // (cmp fn oid, lhs var type, rhs var type)
        let cmp_cases: &[(u32, u32, u32)] = &[
            (158, INT2OID, INT4OID),
            (164, INT2OID, INT4OID),
            (160, INT2OID, INT4OID),
            (166, INT2OID, INT4OID),
            (162, INT2OID, INT4OID),
            (168, INT2OID, INT4OID),
            (159, INT4OID, INT2OID),
            (165, INT4OID, INT2OID),
            (161, INT4OID, INT2OID),
            (167, INT4OID, INT2OID),
            (163, INT4OID, INT2OID),
            (169, INT4OID, INT2OID),
            (184, OIDOID, OIDOID),
            (185, OIDOID, OIDOID),
            (716, OIDOID, OIDOID),
            (717, OIDOID, OIDOID),
            (1638, OIDOID, OIDOID),
            (1639, OIDOID, OIDOID),
        ];
        // (arith fn oid, lhs var type, rhs var type); result compared against
        // a constant so the arith output feeds an inline cmp (still Program
        // under the CASE wrapper).
        let arith_cases: &[(u32, u32, u32)] = &[
            (178, INT2OID, INT4OID),
            (179, INT4OID, INT2OID),
            (182, INT2OID, INT4OID),
            (183, INT4OID, INT2OID),
            (170, INT2OID, INT4OID),
            (171, INT4OID, INT2OID),
            (172, INT2OID, INT4OID),
        ];
        // Row pool: int2-ranged values where the column is int2 (canonical
        // datum invariant), full-range where int4/oid; 0 divisors and
        // overflow-provoking pairs included. As u32 bit patterns, -1 is
        // u32::MAX and i32::MIN is 0x80000000 — the unsigned boundary.
        let a_pool: &[Option<i32>] = &[
            None,
            Some(0),
            Some(1),
            Some(-1),
            Some(7),
            Some(-7),
            Some(i16::MAX as i32),
            Some(i16::MIN as i32),
        ];
        let b_pool: &[Option<i32>] = &[
            None,
            Some(0),
            Some(1),
            Some(-1),
            Some(7),
            Some(-32768),
            Some(32767),
            Some(i32::MAX),
            Some(i32::MIN),
            Some(65537),
        ];
        // Canonical datum for a pool value at the column's declared type.
        fn typed_datum(typ: u32, v: i32) -> Datum {
            match typ {
                21 => Datum::from_i16(v as i16),
                26 => Datum::from_u32(v as u32),
                _ => Datum::from_i32(v),
            }
        }
        fn typed_eval<'mcx>(
            mcx: Mcx<'mcx>,
            state: &mut ExprState<'mcx>,
            cols: &[(u32, i16, Option<Datum>)],
        ) -> FuzzOutcome {
            let mut slot = virtual_slot_typed(mcx, cols);
            let mut slots = EvalSlots { scan: Some(&mut slot), inner: None, outer: None };
            match exec_eval_expr(state, &mut slots) {
                Ok(nd) => Ok((nd.isnull, if nd.isnull { 0 } else { nd.value.as_usize() })),
                Err(e) => Err((e.message.clone(), format!("{:?}", e.sqlstate))),
            }
        }
        let len_of = |typ: u32| if typ == 21 { 2i16 } else { 4 };
        let mut exercised = 0usize;
        let mut run_case = |expr: Node<'_>, lt: u32, rt: u32| {
            let mut interp = exec_init_expr(mcx, Some(expr), ParamBind::NONE).unwrap().unwrap();
            interp.arm_result_mcx(mcx);
            crate::jit::session_begin(crate::jit::PGJIT_PERFORM | crate::jit::PGJIT_EXPR);
            let mut jit = exec_init_expr(mcx, Some(expr), ParamBind::NONE).unwrap().unwrap();
            jit.arm_result_mcx(mcx);
            let _ = crate::jit::session_end();
            if jit.jit.is_some() {
                exercised += 1;
            }
            for &a in a_pool {
                for &b in b_pool {
                    let cols = [
                        (lt, len_of(lt), a.map(|v| typed_datum(lt, v))),
                        (rt, len_of(rt), b.map(|v| typed_datum(rt, v))),
                    ];
                    let want = typed_eval(mcx, &mut interp, &cols);
                    let got = typed_eval(mcx, &mut jit, &cols);
                    assert_eq!(got, want, "row ({a:?}, {b:?})");
                }
            }
        };
        for &(f, lt, rt) in cmp_cases {
            let args =
                NodeList::make2(mcx, mk_scan_var(mcx, 1, lt), mk_scan_var(mcx, 2, rt)).unwrap();
            run_case(wrap_bool(mcx, mk_opexpr(mcx, f, BOOLOID, args)), lt, rt);
        }
        for &(f, lt, rt) in arith_cases {
            let args =
                NodeList::make2(mcx, mk_scan_var(mcx, 1, lt), mk_scan_var(mcx, 2, rt)).unwrap();
            let arith = mk_opexpr(mcx, f, INT4OID, args);
            let cmp_args =
                NodeList::make2(mcx, arith, mk_int4_const(mcx, Some(6))).unwrap();
            run_case(wrap_bool(mcx, mk_opexpr(mcx, 66, BOOLOID, cmp_args)), lt, rt);
        }
        #[cfg(target_arch = "aarch64")]
        assert_eq!(
            exercised,
            cmp_cases.len() + arith_cases.len(),
            "every censusgaps case must engage the JIT emitter on aarch64"
        );
        let _ = exercised;
    });
}

// --- strsearch contains-LIKE qual census + kernel (lane-v2) -----------------

fn text_datum_4b(bytes: &[u8]) -> Datum {
    let mut v = vec![0u8; 4 + bytes.len()];
    let word = ::types_tuple::varatt::set_varsize_4b_word((4 + bytes.len()) as u32);
    v[..4].copy_from_slice(&word.to_ne_bytes());
    v[4..].copy_from_slice(bytes);
    Datum::from_usize(Box::leak(v.into_boxed_slice()).as_ptr() as usize)
}

fn mk_textlike_qual<'mcx>(mcx: Mcx<'mcx>, pattern: &[u8]) -> PgBox<'mcx, ExprState<'mcx>> {
    // texts ~~ '%…%' with a valid input collation (C = 950): OpExpr over
    // textlike (fn 850), scan Var arg0, non-null text Const arg1.
    let args = NodeList::make2(
        mcx,
        mk_scan_var(mcx, 1, TEXTOID_T),
        Node::mk_const(mcx, TEXTOID_T, -1, 950, -1, text_datum_4b(pattern), false, false)
            .unwrap(),
    )
    .unwrap();
    let op = Node::mk(
        mcx,
        OpExpr {
            opno: 1209,
            opfuncid: 850,
            opresulttype: BOOLOID,
            opretset: false,
            opcollid: 0,
            inputcollid: 950,
            args,
            location: -1,
        },
    )
    .unwrap();
    qual_state(mcx, op)
}

#[test]
fn contains_census_admits_contains_class_patterns() {
    with_mcx(|mcx| {
        let state = mk_textlike_qual(mcx, b"%abc%");
        let c = state.scan_contains_clause().expect("contains-class pattern admits");
        assert_eq!(c.attnum, 0);
        assert_eq!(c.collation, 950);
        assert_eq!(c.needle(), b"abc");
        // Multi-% runs collapse; multibyte literals admit.
        let state = mk_textlike_qual(mcx, "%%причал%%%".as_bytes());
        let c = state.scan_contains_clause().expect("wildcard runs + multibyte admit");
        assert_eq!(c.needle(), "причал".as_bytes());
    });
}

#[test]
fn contains_census_refuses_non_contains_patterns() {
    with_mcx(|mcx| {
        for pat in [
            &b"abc%"[..],     // anchored prefix
            b"%abc",          // anchored suffix
            b"abc",           // exact
            b"%a_c%",         // underscore class
            b"%a\\bc%",       // escape class
            b"%a%c%",         // multi-segment
            b"%%",            // empty literal (matches-everything class)
            b"%",             // ditto
        ] {
            let state = mk_textlike_qual(mcx, pat);
            assert!(
                state.scan_contains_clause().is_none(),
                "pattern {:?} must refuse",
                String::from_utf8_lossy(pat)
            );
        }
    });
}

#[test]
fn qual_bitmap_contains_matches_perrow_like_oracle() {
    // Parity fuzz: the batched kernel's bits vs the REAL per-row matcher
    // (match_text::<Utf8Cs>, the kernel a UTF-8 database's textlike runs)
    // over random haystacks (ASCII + multibyte UTF-8 + empties + NULLs) and
    // random needles of length 1..=24. Also proves the undecidable arm: a
    // toast-pointer header must yield an `undecided` bit, never a decision.
    let mut s = 0x243F_6A88_85A3_08D3u64;
    let mut lcg = move || {
        s = s.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
        s
    };
    let alphabet: Vec<char> =
        "abcdefgxyz0123456789 /:.?=&причалЯндексбßé中文".chars().collect();
    for round in 0..200 {
        let needle_len = 1 + (lcg() as usize) % 6;
        let needle: String =
            (0..needle_len).map(|_| alphabet[(lcg() as usize) % alphabet.len()]).collect();
        let pattern = format!("%{needle}%");
        let nrows = 64 + (lcg() as usize) % 130;
        let mut values = vec![Datum::null(); nrows];
        let mut isnull = vec![false; nrows];
        let mut owners: Vec<Box<[u8]>> = Vec::new();
        for i in 0..nrows {
            match lcg() % 8 {
                0 => isnull[i] = true,
                1 if round % 2 == 0 => {
                    // Undecidable: 1B_E toast-pointer header (0x01 tag byte).
                    // Push BEFORE taking the pointer: moving the Box after
                    // the ptr→int cast invalidates the exposed tag under
                    // Stacked Borrows (miri F1).
                    let raw: Box<[u8]> = vec![0x01, 18, 0, 0].into_boxed_slice();
                    owners.push(raw);
                    values[i] =
                        Datum::from_usize(owners.last().unwrap().as_ptr() as usize);
                }
                _ => {
                    let len = (lcg() as usize) % 40;
                    let text: String =
                        (0..len).map(|_| alphabet[(lcg() as usize) % alphabet.len()]).collect();
                    let b = text.as_bytes();
                    let mut v = vec![0u8; 4 + b.len()];
                    let word =
                        ::types_tuple::varatt::set_varsize_4b_word((4 + b.len()) as u32);
                    v[..4].copy_from_slice(&word.to_ne_bytes());
                    v[4..].copy_from_slice(b);
                    // As above: push before exposing the pointer (miri F1).
                    let raw = v.into_boxed_slice();
                    owners.push(raw);
                    values[i] =
                        Datum::from_usize(owners.last().unwrap().as_ptr() as usize);
                }
            }
        }
        let nwords = nrows.div_ceil(64);
        let mut sel = vec![0u64; nwords];
        let mut undecided = vec![0u64; nwords];
        // SAFETY: every non-null value is a live 4B varlena (or a 1B_E
        // header the kernel must classify undecidable without reading past).
        unsafe {
            crate::steps::qual_bitmap_contains(
                needle.as_bytes(),
                &values,
                &isnull,
                &mut sel,
                &mut undecided,
            );
        }
        for i in 0..nrows {
            let bit = sel[i / 64] >> (i % 64) & 1 == 1;
            let und = undecided[i / 64] >> (i % 64) & 1 == 1;
            if isnull[i] {
                assert!(!bit && !und, "NULL row {i} must fail, not defer");
                continue;
            }
            let p = values[i].as_usize() as *const u8;
            let is_toast = unsafe { *p == 0x01 };
            if is_toast {
                assert!(und && !bit, "toast row {i} must defer to the per-row path");
                continue;
            }
            assert!(!und);
            let len = unsafe { ::types_tuple::varatt::varsize_4b(p) } - 4;
            let text = unsafe { core::slice::from_raw_parts(p.add(4), len) };
            let oracle = ::adt_like::utf8_match_text(
                text,
                pattern.as_bytes(),
                Some(&::pg_locale::C_LOCALE),
            )
            .unwrap()
                == 1;
            assert_eq!(bit, oracle, "row {i} needle {needle:?} text {text:?}");
        }
    }
}
