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
            Step::ScanVarFuncStrict2 { attnum: 0, argno: 0, .. }
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
            },
            // sum(int4): int4_sum (1841), non-strict, null init, 1 input.
            AggTransSpec {
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
            },
            // sum(int4): int4_sum (1841), non-strict, null init.
            AggTransSpec {
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
        assert!(matches!(state.steps()[1], Step::ScanVarFuncStrict2 { attnum: 0, argno: 0, .. }));
        assert!(matches!(state.steps()[2], Step::FuncFuncStrict2 { argno: 0, .. }));
        assert!(matches!(state.steps()[4], Step::FuncFuncStrict2 { .. }));
        assert!(matches!(state.steps()[5], Step::FuncExprStrict2 { .. }));
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
            .any(|s| matches!(s, Step::ScanVarFuncStrict2 { .. })));
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
