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
    assert!(core::mem::size_of::<Kernel>() <= 24);
    with_mcx(|mcx| {
        let args = NodeList::make2(mcx, mk_scan_var(mcx, 1, INT4OID), mk_int4_const(mcx, Some(7)))
            .unwrap();
        let state = qual_state(mcx, mk_opexpr(mcx, 65, BOOLOID, args));
        let shapes: alloc::vec::Vec<core::mem::Discriminant<Step>> =
            state.steps().iter().map(core::mem::discriminant).collect();
        assert_eq!(state.steps().len(), 5);
        assert!(matches!(state.steps()[0], Step::ScanFetchSome { last_var: 1 }));
        assert!(matches!(state.steps()[1], Step::ScanVar { attnum: 0, .. }));
        assert!(matches!(state.steps()[2], Step::FuncExprStrict2 { .. }));
        assert!(matches!(state.steps()[3], Step::Qual { jumpdone: 4 }));
        assert!(matches!(state.steps()[4], Step::DoneReturn));
        assert_eq!(shapes.len(), 5);
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
                transfn_oid: 1219,
                inputcollid: 0,
                init_value_is_null: false,
                args: &empty_args,
                pergroup: base,
            },
            // sum(int4): int4_sum (1841), non-strict, null init, 1 input.
            AggTransSpec {
                transfn_oid: 1841,
                inputcollid: 0,
                init_value_is_null: true,
                args: &sum_args,
                // SAFETY: index 1 of the 2-element local array.
                pergroup: unsafe { NonNull::new_unchecked(base.as_ptr().add(1)) },
            },
        ];
        let mut trans = exec_build_agg_trans(mcx, &specs, ParamBind::NONE).unwrap();
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
#[should_panic(expected = "EEOP_AGG_STRICT_INPUT_CHECK_ARGS")]
fn agg_trans_strict_with_args_panics() {
    use core::ptr::NonNull;

    use crate::compile::{exec_build_agg_trans, AggTransSpec};
    use crate::steps::AggPerGroup;
    use ::types_nodes::primnodes::OUTER_VAR;

    with_mcx(|mcx| {
        let mut pg = AggPerGroup {
            trans_value: Datum::null(),
            trans_value_is_null: true,
            no_trans_value: true,
        };
        let var = Node::mk_var(mcx, OUTER_VAR, 1, INT8OID, -1, 0, 0).unwrap();
        let tle = Node::mk_target_entry(mcx, var, 1, None, false).unwrap();
        let args = NodeList::make1(mcx, tle).unwrap();
        // int8larger (1236): strict with one aggregated arg (max()).
        let specs = [AggTransSpec {
            transfn_oid: 1236,
            inputcollid: 0,
            init_value_is_null: false,
            args: &args,
            pergroup: NonNull::from(&mut pg),
        }];
        let _ = exec_build_agg_trans(mcx, &specs, ParamBind::NONE);
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
