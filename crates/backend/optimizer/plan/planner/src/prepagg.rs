//! prepagg.c slice hosted here (unit backend-optimizer-prep-core):
//! preprocess_aggrefs + get_agg_clause_costs for the plain no-GROUP-BY lane.

use types_error::PgResult;
use types_nodes::list::NodeList;
use types_nodes::primnodes::Aggref;
use types_nodes::{Node, NodeEqual, NodeTag};
use types_pathnodes::{AggClauseCosts, AggInfo, AggSplit, AggTransInfo};

use crate::costsize::{cost_qual_eval_node, expr_type_typmod};
use crate::run::PlannerRun;

const INT8OID: u32 = 20;
const INTERNALOID: u32 = 2281;
const RECORDOID: u32 = 2249;
const F_ARRAY_AGG_SERIALIZE: u32 = 6294;
const F_ARRAY_AGG_DESERIALIZE: u32 = 6295;
const AGGMODIFY_READ_WRITE: i8 = b'w' as i8;

// resolve_aggregate_transtype (parse_agg.c): a polymorphic declared
// transtype resolves against the aggregate call's actual input types
// (get_aggregate_argtypes: aggref->aggargtypes, already recorded by the
// parser). Shared with nodewindowagg's copy only in C's own header
// declaration; each Rust caller carries its own port.
fn resolve_aggregate_transtype<'mcx>(
    mcx: mcx::Mcx<'mcx>,
    aggfuncid: u32,
    aggtranstype: u32,
    input_types: &[u32],
) -> PgResult<u32> {
    if !coerce::IsPolymorphicType(aggtranstype) {
        return Ok(aggtranstype);
    }
    let (_rettype, mut declared) = lsyscache::get_func_signature(mcx, aggfuncid)?;
    debug_assert!(declared.len() <= input_types.len());
    let n = declared.len();
    coerce::enforce_generic_type_consistency(
        &input_types[..n],
        &mut declared[..n],
        aggtranstype,
        false,
    )
}

// agg_args_support_sendreceive (parse_agg.c): every non-byval arg type must
// have typsend and typreceive.  RECORD is refused outright: record_recv needs
// the registered typmod of the specific anonymous record type, which
// array_agg_deserialize cannot supply.
fn agg_args_support_sendreceive(aggref: &Aggref<'_>) -> PgResult<bool> {
    for arg in &aggref.args {
        let tle = arg.as_target_entry().expect("agg arg is a TLE");
        let argtype = expr_type_typmod(tle.expr).0;
        if argtype == RECORDOID {
            return Ok(false);
        }
        let ts = syscache_seams::pg_type_io_shape::call(argtype)?
            .unwrap_or_else(|| panic!("cache lookup failed for type {argtype}"));
        if !ts.typbyval && (ts.typsend == 0 || ts.typreceive == 0) {
            return Ok(false);
        }
    }
    Ok(true)
}

pub fn preprocess_aggrefs<'mcx>(
    run: &mut PlannerRun<'mcx>,
    tlist: &NodeList<'mcx>,
) -> PgResult<()> {
    for node in tlist {
        preprocess_aggrefs_walker(run, node)?;
    }
    Ok(())
}

/// The single-node entry (C passes the bare havingQual to the same walker).
pub fn preprocess_aggrefs_node<'mcx>(
    run: &mut PlannerRun<'mcx>,
    node: Node<'mcx>,
) -> PgResult<()> {
    preprocess_aggrefs_walker(run, node)
}

// C returns without descending into a matched Aggref (no same-level nesting).
fn preprocess_aggrefs_walker<'mcx>(
    run: &mut PlannerRun<'mcx>,
    node: Node<'mcx>,
) -> PgResult<()> {
    match node.node_tag() {
        NodeTag::T_Aggref => preprocess_aggref(run, node),
        // C's default expression_tree_walker arm: descend into the args.
        NodeTag::T_GroupingFunc => {
            for a in &node.as_grouping_func().unwrap().args {
                preprocess_aggrefs_walker(run, a)?;
            }
            Ok(())
        }
        NodeTag::T_TargetEntry => {
            preprocess_aggrefs_walker(run, node.as_target_entry().unwrap().expr)
        }
        NodeTag::T_Var | NodeTag::T_Const => Ok(()),
        NodeTag::T_OpExpr => {
            for a in &node.as_op_expr().unwrap().args {
                preprocess_aggrefs_walker(run, a)?;
            }
            Ok(())
        }
        NodeTag::T_FuncExpr => {
            for a in &node.as_func_expr().unwrap().args {
                preprocess_aggrefs_walker(run, a)?;
            }
            Ok(())
        }
        NodeTag::T_BoolExpr => {
            for a in &node.as_bool_expr().unwrap().args {
                preprocess_aggrefs_walker(run, a)?;
            }
            Ok(())
        }
        NodeTag::T_List => {
            for a in node.as_list().unwrap() {
                preprocess_aggrefs_walker(run, a)?;
            }
            Ok(())
        }
        NodeTag::T_WindowFunc => {
            let wf = node.as_window_func().unwrap();
            for a in &wf.args {
                preprocess_aggrefs_walker(run, a)?;
            }
            match wf.aggfilter {
                Some(f) => preprocess_aggrefs_walker(run, f),
                None => Ok(()),
            }
        }
        NodeTag::T_Param
        | NodeTag::T_CaseTestExpr
        | NodeTag::T_SQLValueFunction
        | NodeTag::T_NextValueExpr
        | NodeTag::T_CoerceToDomainValue => Ok(()),
        NodeTag::T_RelabelType => {
            preprocess_aggrefs_walker(run, node.as_relabel_type().unwrap().arg)
        }
        NodeTag::T_FieldSelect => {
            preprocess_aggrefs_walker(run, node.as_field_select().unwrap().arg)
        }
        NodeTag::T_CoerceViaIO => {
            preprocess_aggrefs_walker(run, node.as_coerce_via_io().unwrap().arg)
        }
        NodeTag::T_ArrayCoerceExpr => {
            let a = node.as_array_coerce_expr().unwrap();
            preprocess_aggrefs_walker(run, a.arg)?;
            match a.elemexpr {
                Some(e) => preprocess_aggrefs_walker(run, e),
                None => Ok(()),
            }
        }
        NodeTag::T_ConvertRowtypeExpr => {
            preprocess_aggrefs_walker(run, node.as_convert_rowtype_expr().unwrap().arg)
        }
        NodeTag::T_CoerceToDomain => {
            preprocess_aggrefs_walker(run, node.as_coerce_to_domain().unwrap().arg)
        }
        NodeTag::T_NullTest => match node.as_null_test().unwrap().arg {
            Some(a) => preprocess_aggrefs_walker(run, a),
            None => Ok(()),
        },
        NodeTag::T_BooleanTest => match node.as_boolean_test().unwrap().arg {
            Some(a) => preprocess_aggrefs_walker(run, a),
            None => Ok(()),
        },
        NodeTag::T_DistinctExpr => {
            for a in &node.as_distinct_expr().unwrap().args {
                preprocess_aggrefs_walker(run, a)?;
            }
            Ok(())
        }
        NodeTag::T_NullIfExpr => {
            for a in &node.as_null_if_expr().unwrap().args {
                preprocess_aggrefs_walker(run, a)?;
            }
            Ok(())
        }
        NodeTag::T_FieldStore => {
            let fs = node.as_field_store().unwrap();
            preprocess_aggrefs_walker(run, fs.arg)?;
            for a in &fs.newvals {
                preprocess_aggrefs_walker(run, a)?;
            }
            Ok(())
        }
        NodeTag::T_SubscriptingRef => {
            let sref = node.as_subscripting_ref().unwrap();
            for a in sref.refupperindexpr.iter().flatten() {
                preprocess_aggrefs_walker(run, a)?;
            }
            for a in sref.reflowerindexpr.iter().flatten() {
                preprocess_aggrefs_walker(run, a)?;
            }
            if let Some(e) = sref.refexpr {
                preprocess_aggrefs_walker(run, e)?;
            }
            if let Some(e) = sref.refassgnexpr {
                preprocess_aggrefs_walker(run, e)?;
            }
            Ok(())
        }
        NodeTag::T_ScalarArrayOpExpr => {
            for a in &node.as_scalar_array_op_expr().unwrap().args {
                preprocess_aggrefs_walker(run, a)?;
            }
            Ok(())
        }
        NodeTag::T_ArrayExpr => {
            for e in &node.as_array_expr().unwrap().elements {
                preprocess_aggrefs_walker(run, e)?;
            }
            Ok(())
        }
        NodeTag::T_JsonValueExpr => {
            let j = node.as_json_value_expr().unwrap();
            for e in [j.raw_expr, j.formatted_expr].into_iter().flatten() {
                preprocess_aggrefs_walker(run, e)?;
            }
            Ok(())
        }
        NodeTag::T_RowExpr => {
            for a in &node.as_row_expr().unwrap().args {
                preprocess_aggrefs_walker(run, a)?;
            }
            Ok(())
        }
        NodeTag::T_RowCompareExpr => {
            let rc = node.as_row_compare_expr().unwrap();
            for a in rc.largs.iter().chain(rc.rargs.iter()) {
                preprocess_aggrefs_walker(run, a)?;
            }
            Ok(())
        }
        NodeTag::T_CaseExpr => {
            let c = node.as_case_expr().unwrap();
            if let Some(a) = c.arg {
                preprocess_aggrefs_walker(run, a)?;
            }
            for w in &c.args {
                let cw = w.as_case_when().expect("CaseWhen");
                preprocess_aggrefs_walker(run, cw.expr.expect("CaseWhen.expr"))?;
                preprocess_aggrefs_walker(run, cw.result.expect("CaseWhen.result"))?;
            }
            match c.defresult {
                Some(d) => preprocess_aggrefs_walker(run, d),
                None => Ok(()),
            }
        }
        NodeTag::T_CoalesceExpr => {
            for a in &node.as_coalesce_expr().unwrap().args {
                preprocess_aggrefs_walker(run, a)?;
            }
            Ok(())
        }
        NodeTag::T_MinMaxExpr => {
            for a in &node.as_min_max_expr().unwrap().args {
                preprocess_aggrefs_walker(run, a)?;
            }
            Ok(())
        }
        NodeTag::T_SubPlan => {
            let sp = node.as_sub_plan().unwrap();
            if let Some(te) = sp.testexpr {
                preprocess_aggrefs_walker(run, te)?;
            }
            for a in &sp.args {
                preprocess_aggrefs_walker(run, a)?;
            }
            Ok(())
        }
        NodeTag::T_AlternativeSubPlan => {
            for sp in &node.as_alternative_sub_plan().unwrap().subplans {
                preprocess_aggrefs_walker(run, sp)?;
            }
            Ok(())
        }
        NodeTag::T_JsonConstructorExpr => {
            let c = node.as_json_constructor_expr().unwrap();
            for a in &c.args {
                preprocess_aggrefs_walker(run, a)?;
            }
            for e in [c.func, c.coercion].into_iter().flatten() {
                preprocess_aggrefs_walker(run, e)?;
            }
            Ok(())
        }
        NodeTag::T_JsonIsPredicate => {
            match node.as_json_is_predicate().unwrap().expr {
                Some(e) => preprocess_aggrefs_walker(run, e),
                None => Ok(()),
            }
        }
        NodeTag::T_JsonBehavior => match node.as_json_behavior().unwrap().expr {
            Some(e) => preprocess_aggrefs_walker(run, e),
            None => Ok(()),
        },
        NodeTag::T_JsonExpr => {
            let j = node.as_json_expr().unwrap();
            for e in [j.formatted_expr, j.path_spec, j.on_empty, j.on_error]
                .into_iter()
                .flatten()
            {
                preprocess_aggrefs_walker(run, e)?;
            }
            for v in &j.passing_values {
                preprocess_aggrefs_walker(run, v)?;
            }
            Ok(())
        }
        NodeTag::T_PlaceHolderVar => {
            preprocess_aggrefs_walker(run, node.as_place_holder_var().unwrap().phexpr)
        }
        NodeTag::T_XmlExpr => {
            let x = node.as_xml_expr().unwrap();
            for a in x.named_args.iter().chain(x.args.iter()) {
                preprocess_aggrefs_walker(run, a)?;
            }
            Ok(())
        }
        other => panic!(
            "preprocess_aggrefs_walker (prepagg.c): {other:?}; M3 expression lane"
        ),
    }
}

fn preprocess_aggref<'mcx>(run: &mut PlannerRun<'mcx>, node: Node<'mcx>) -> PgResult<()> {
    let mcx = run.mcx;
    let aggref = node.as_aggref().expect("Aggref");
    debug_assert!(aggref.agglevelsup == 0);

    let shape = syscache_seams::lookup_pg_aggregate_shape::call(aggref.aggfnoid)?
        .unwrap_or_else(|| {
            panic!("cache lookup failed for aggregate {}", aggref.aggfnoid)
        });

    let aggtranstype = resolve_aggregate_transtype(
        mcx,
        aggref.aggfnoid,
        shape.aggtranstype,
        aggref.aggargtypes.as_slice(),
    )?;

    let mut aggtranstypmod = -1;
    if !aggref.args.is_nil() {
        let first = aggref.args.nth(0).as_target_entry().expect("agg arg is a TLE");
        let (argtype, argtypmod) = expr_type_typmod(first.expr);
        if aggtranstype == argtype {
            aggtranstypmod = argtypmod;
        }
    }

    let shareable = shape.aggfinalmodify != AGGMODIFY_READ_WRITE;
    lsyscache::get_typlenbyval(aggref.aggtype)?;

    let (init_value, init_value_is_null) =
        match syscache_seams::pg_aggregate_agginitval::call(mcx, aggref.aggfnoid)? {
            None => panic!("cache lookup failed for aggregate {}", aggref.aggfnoid),
            Some(None) => (datum::Datum::null(), true),
            Some(Some(text)) => (get_agg_init_val(mcx, &text, aggtranstype)?, false),
        };

    let (aggno, transno);
    let mut same_input_transnos: mcx::PgVec<'_, i32> = mcx::PgVec::new_in(mcx);
    if let Some(existing) =
        find_compatible_agg(run, node, aggtranstype, &mut same_input_transnos)?
    {
        let aggref_id = run.root.alloc_expr_node(node);
        run.root.agg_info_mut(existing.1).aggrefs.push(aggref_id);
        aggno = existing.0;
        transno = run.root.agg_info(existing.1).transno;
    } else {
        let aggref_id = run.root.alloc_expr_node(node);
        let mut agginfo = AggInfo::new(mcx);
        agginfo.finalfn_oid = shape.aggfinalfn;
        agginfo.aggrefs.push(aggref_id);
        agginfo.shareable = shareable;

        aggno = run.root.agginfos.len() as i32;

        if !aggref.aggorder.is_nil() || !aggref.aggdistinct.is_nil() {
            run.root.numOrderedAggs += 1;
            run.root.hasNonPartialAggs = true;
        }

        let (transtype_len, transtype_byval) = lsyscache::get_typlenbyval(aggtranstype)?;

        transno = match find_compatible_trans(
            run,
            shareable,
            shape.aggtransfn,
            aggtranstype,
            transtype_len,
            transtype_byval,
            shape.aggcombinefn,
            shape.aggserialfn,
            shape.aggdeserialfn,
            init_value,
            init_value_is_null,
            &same_input_transnos,
        ) {
            Some(t) => t,
            None => {
                let mut ti = AggTransInfo::new(mcx);
                for arg in &aggref.args {
                    let id = run.root.alloc_expr_node(arg);
                    ti.args.push(id);
                }
                ti.aggfilter = aggref.aggfilter.map(|f| run.root.alloc_expr_node(f));
                ti.transfn_oid = shape.aggtransfn;
                ti.combinefn_oid = shape.aggcombinefn;
                ti.serialfn_oid = shape.aggserialfn;
                ti.deserialfn_oid = shape.aggdeserialfn;
                ti.aggtranstype = aggtranstype;
                ti.aggtranstypmod = aggtranstypmod;
                ti.transtypeLen = transtype_len as i32;
                ti.transtypeByVal = transtype_byval;
                ti.aggtransspace = shape.aggtransspace;
                ti.initValue = init_value;
                ti.initValueIsNull = init_value_is_null;

                let t = run.root.aggtransinfos.len() as i32;
                let (serialfn_oid, deserialfn_oid) = (ti.serialfn_oid, ti.deserialfn_oid);
                let has_serde = serialfn_oid != 0 && deserialfn_oid != 0;
                let no_combine = ti.combinefn_oid == 0;
                let internal_transtype = ti.aggtranstype == INTERNALOID;
                let id = run.root.alloc_agg_trans_info(ti);
                run.root.aggtransinfos.push(id);
                if !run.root.hasNonPartialAggs {
                    if no_combine {
                        run.root.hasNonPartialAggs = true;
                    } else if internal_transtype {
                        if !has_serde {
                            run.root.hasNonSerialAggs = true;
                        }
                        // array_agg_serialize/deserialize call the argument
                        // type's send/receive functions, so they only work
                        // when every arg type supports them.
                        if (serialfn_oid == F_ARRAY_AGG_SERIALIZE
                            || deserialfn_oid == F_ARRAY_AGG_DESERIALIZE)
                            && !agg_args_support_sendreceive(aggref)?
                        {
                            run.root.hasNonSerialAggs = true;
                        }
                    }
                }
                t
            }
        };
        let mut agginfo = agginfo;
        agginfo.transno = transno;
        let id = run.root.alloc_agg_info(agginfo);
        run.root.agginfos.push(id);
    }

    // SAFETY: the planner exclusively owns the sealed parse tree during
    // planning (C scribbles these same fields through shared pointers); every
    // reference derived from this node above is dead here.
    unsafe {
        node.with_mut::<Aggref, _>(|a| {
            a.aggtranstype = aggtranstype;
            a.aggno = aggno;
            a.aggtransno = transno;
        })
    }
    .unwrap();
    Ok(())
}

// GetAggInitVal (prepagg.c): initval text through the transtype's typinput.
// In-function by-ref results ride the resolved carrier's scratch (dead once
// flinfo drops); C's palloc'd result is modeled by the datumCopy into mcx.
fn get_agg_init_val(
    mcx: mcx::Mcx<'_>,
    text: &str,
    transtype: u32,
) -> PgResult<datum::Datum> {
    let (typinput, typioparam) = lsyscache::getTypeInputInfo(transtype)?;
    let mut flinfo = fmgr_core::fmgr_info(typinput)?;
    let cstr = std::ffi::CString::new(text)
        .expect("agginitval text contains an interior NUL");
    let d = types_fmgr::input_function_call(&mut flinfo, Some(&cstr), typioparam, -1, mcx)?;
    let (typlen, typbyval) = lsyscache::get_typlenbyval(transtype)?;
    if typbyval {
        Ok(d)
    } else {
        // SAFETY: non-null by-ref in-function result, live until flinfo drops.
        unsafe { execexpr::agg_datum_copy(mcx, d, typlen) }
    }
}

// Returns (aggno, agginfo NodeId) of an identical previous aggregate, and
// collects shareable same-input transnos for find_compatible_trans.
fn find_compatible_agg<'mcx>(
    run: &PlannerRun<'mcx>,
    node: Node<'mcx>,
    aggtranstype: u32,
    same_input_transnos: &mut mcx::PgVec<'_, i32>,
) -> PgResult<Option<(i32, types_pathnodes::NodeId)>> {
    let newagg = node.as_aggref().unwrap();

    if clauses::contain_volatile_functions(node)? {
        return Ok(None);
    }

    for (aggno, &info_id) in run.root.agginfos.iter().enumerate() {
        let agginfo = run.root.agg_info(info_id);
        let existing_node = *run.root.expr_node(agginfo.aggrefs[0]);
        let existing = existing_node.as_aggref().expect("AggInfo holds Aggrefs");

        if newagg.inputcollid != existing.inputcollid
            || aggtranstype != existing.aggtranstype
            || newagg.aggstar != existing.aggstar
            || newagg.aggvariadic != existing.aggvariadic
            || newagg.aggkind != existing.aggkind
            || !newagg.args.node_equal(&existing.args)
            || !newagg.aggorder.node_equal(&existing.aggorder)
            || !newagg.aggdistinct.node_equal(&existing.aggdistinct)
            || !types_nodes::equal_opt(newagg.aggfilter, existing.aggfilter)
        {
            continue;
        }

        if newagg.aggfnoid == existing.aggfnoid
            && newagg.aggtype == existing.aggtype
            && newagg.aggcollid == existing.aggcollid
            && newagg.aggdirectargs.node_equal(&existing.aggdirectargs)
        {
            same_input_transnos.clear();
            return Ok(Some((aggno as i32, info_id)));
        }

        if agginfo.shareable {
            same_input_transnos.push(agginfo.transno);
        }
    }
    Ok(None)
}

#[allow(clippy::too_many_arguments)]
fn find_compatible_trans(
    run: &PlannerRun<'_>,
    shareable: bool,
    aggtransfn: u32,
    aggtranstype: u32,
    transtype_len: i16,
    transtype_byval: bool,
    aggcombinefn: u32,
    aggserialfn: u32,
    aggdeserialfn: u32,
    init_value: datum::Datum,
    init_value_is_null: bool,
    transnos: &[i32],
) -> Option<i32> {
    if !shareable {
        return None;
    }
    for &transno in transnos {
        let id = run.root.aggtransinfos[transno as usize];
        let pertrans = run.root.agg_trans_info(id);
        if aggtransfn != pertrans.transfn_oid || aggtranstype != pertrans.aggtranstype {
            continue;
        }
        if aggserialfn != pertrans.serialfn_oid || aggdeserialfn != pertrans.deserialfn_oid {
            continue;
        }
        if aggcombinefn != pertrans.combinefn_oid {
            continue;
        }
        if init_value_is_null && pertrans.initValueIsNull {
            return Some(transno);
        }
        if !init_value_is_null
            && !pertrans.initValueIsNull
            && datum_is_equal(init_value, pertrans.initValue, transtype_byval, transtype_len)
        {
            return Some(transno);
        }
    }
    None
}

// datumIsEqual (datum.c): by-val full-word compare, by-ref byte-image
// compare, no detoast. By-ref initvals come from GetAggInitVal's input
// function: plain varlena for -1, NUL-terminated cstring for -2.
fn datum_is_equal(a: datum::Datum, b: datum::Datum, byval: bool, typlen: i16) -> bool {
    if byval {
        return a.as_u64() == b.as_u64();
    }
    let p1 = a.as_usize() as *const u8;
    let p2 = b.as_usize() as *const u8;
    // SAFETY: by-ref initval datums are live images of the layout typlen
    // describes for the whole planner run.
    unsafe {
        let size = |p: *const u8| -> usize {
            match typlen {
                -1 => ::types_tuple::varatt::varsize_any(p),
                -2 => {
                    let mut n = 0usize;
                    while *p.add(n) != 0 {
                        n += 1;
                    }
                    n + 1
                }
                l => l as usize,
            }
        };
        let (s1, s2) = (size(p1), size(p2));
        s1 == s2 && core::slice::from_raw_parts(p1, s1) == core::slice::from_raw_parts(p2, s2)
    }
}

pub fn get_agg_clause_costs(
    run: &mut PlannerRun<'_>,
    aggsplit: AggSplit,
    costs: &mut AggClauseCosts,
) -> PgResult<()> {
    let do_combine = aggsplit & types_pathnodes::AGGSPLITOP_COMBINE != 0;
    let do_serialize = aggsplit & types_pathnodes::AGGSPLITOP_SERIALIZE != 0;
    let do_deserialize = aggsplit & types_pathnodes::AGGSPLITOP_DESERIALIZE != 0;
    let skip_final = aggsplit & types_pathnodes::AGGSPLITOP_SKIPFINAL != 0;
    for i in 0..run.root.aggtransinfos.len() {
        let id = run.root.aggtransinfos[i];
        let (transfn_oid, combinefn_oid, serialfn_oid, deserialfn_oid) = {
            let ti = run.root.agg_trans_info(id);
            (ti.transfn_oid, ti.combinefn_oid, ti.serialfn_oid, ti.deserialfn_oid)
        };
        let (byval, transtype, transtypmod, transspace, nargs) = {
            let ti = run.root.agg_trans_info(id);
            (
                ti.transtypeByVal,
                ti.aggtranstype,
                ti.aggtranstypmod,
                ti.aggtransspace,
                ti.args.len(),
            )
        };
        if do_combine {
            crate::plancat::add_function_cost(combinefn_oid, &mut costs.transCost)?;
        } else {
            crate::plancat::add_function_cost(transfn_oid, &mut costs.transCost)?;
        }
        if do_deserialize && deserialfn_oid != 0 {
            crate::plancat::add_function_cost(deserialfn_oid, &mut costs.transCost)?;
        }
        if do_serialize && serialfn_oid != 0 {
            crate::plancat::add_function_cost(serialfn_oid, &mut costs.finalCost)?;
        }

        if !do_combine {
            for a in 0..nargs {
                let arg_id = run.root.agg_trans_info(id).args[a];
                let arg = *run.root.expr_node(arg_id);
                let expr = arg.as_target_entry().map(|t| t.expr).unwrap_or(arg);
                let argcost = cost_qual_eval_node(Some(&mut *run), expr)?;
                costs.transCost.startup += argcost.startup;
                costs.transCost.per_tuple += argcost.per_tuple;
            }

            if let Some(fid) = run.root.agg_trans_info(id).aggfilter {
                let filter = *run.root.expr_node(fid);
                let argcost = cost_qual_eval_node(Some(&mut *run), filter)?;
                costs.transCost.startup += argcost.startup;
                costs.transCost.per_tuple += argcost.per_tuple;
            }
        }

        if !byval {
            let avgwidth = if transspace > 0 {
                transspace
            } else {
                // F_ARRAY_APPEND's expanded-array arm is unreachable while
                // by-ref transtypes stay in this branch's typavgwidth form.
                lsyscache::get_typavgwidth(transtype, transtypmod)?
            };
            let maxaligned = (avgwidth as usize + 7) & !7;
            costs.transitionSpace += maxaligned + 2 * 8;
        } else if transtype == INTERNALOID {
            const ALLOCSET_DEFAULT_INITSIZE: usize = 8 * 1024;
            costs.transitionSpace += if transspace > 0 {
                transspace as usize
            } else {
                ALLOCSET_DEFAULT_INITSIZE
            };
        }
    }

    for i in 0..run.root.agginfos.len() {
        let id = run.root.agginfos[i];
        let (finalfn_oid, aggref_id) = {
            let info = run.root.agg_info(id);
            (info.finalfn_oid, info.aggrefs[0])
        };
        if !skip_final && finalfn_oid != 0 {
            crate::plancat::add_function_cost(finalfn_oid, &mut costs.finalCost)?;
        }
        let aggref_node = *run.root.expr_node(aggref_id);
        let aggref = aggref_node.as_aggref().expect("AggInfo holds Aggrefs");
        for d in aggref.aggdirectargs.iter() {
            let argcost = cost_qual_eval_node(Some(&mut *run), d)?;
            costs.finalCost.startup += argcost.startup;
            costs.finalCost.per_tuple += argcost.per_tuple;
        }
    }
    Ok(())
}
