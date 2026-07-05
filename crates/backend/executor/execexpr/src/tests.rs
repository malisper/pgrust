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
use crate::interp::{exec_eval_expr, exec_project, exec_qual, EvalSlots};
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
        assert!(err.message().contains("not compatible"));
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
        let Err(err) = exec_init_expr(mcx, Some(node), ParamBind::NONE) else {
            panic!("unbound PARAM_EXTERN must fail at compile");
        };
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
    #[should_panic(expected = "EEOP_CASE_TESTVAL_EXT")]
    fn ext_case_test_without_permission_stays_loud() {
        with_mcx(|mcx| {
            let ct = Node::mk(
                mcx,
                CaseTestExpr { typeId: INT4OID, typeMod: -1, collation: 0 },
            )
            .unwrap();
            let _ = exec_init_expr(mcx, Some(ct), ParamBind::NONE);
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
                match crate::interp::exec_one_step(state, slots, result_slot.as_deref_mut(), ix)? {
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
