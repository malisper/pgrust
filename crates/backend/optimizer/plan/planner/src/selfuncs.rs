//! selfuncs.c slice: eqsel/scalarineqsel over Var-op-Const with pg_statistic
//! consumption (MCV + histogram), plus btcostestimate/genericcostestimate.

use datum::Datum;
use syscache_seams::{PgStatisticBundle, PgStatisticSlotData};
use types_core::Oid;
use types_error::PgResult;
use types_fmgr::FmgrInfo;
use types_nodes::parsenodes::RTEKind;
use types_nodes::{Node, NodeTag};
use types_pathnodes::{NodeId, PathNode, RelId, RinfoId, JOIN_INNER};

use crate::gucs;
use crate::run::PlannerRun;

pub const DEFAULT_EQ_SEL: f64 = 0.005;
pub const DEFAULT_INEQ_SEL: f64 = 0.3333333333333333;
pub const DEFAULT_NUM_DISTINCT: f64 = 200.0;
const DEFAULT_PAGE_CPU_MULTIPLIER: f64 = 50.0;
const BOOLOID: u32 = 16;
const SELF_ITEM_POINTER_ATTRIBUTE_NUMBER: i16 = -1;
const TABLE_OID_ATTRIBUTE_NUMBER: i16 = -6;

pub const STATISTIC_KIND_MCV: i16 = 1;
pub const STATISTIC_KIND_HISTOGRAM: i16 = 2;
pub const STATISTIC_KIND_CORRELATION: i16 = 3;

fn clamp_probability(p: f64) -> f64 {
    p.clamp(0.0, 1.0)
}

// VariableStatData (selfuncs.h); `stats` is the decoded statsTuple.
// statistic_proc_security_check (pg_class_aclcheck) reduces to true on this
// single-role substrate, so acl_ok is not modeled.
pub struct VariableStatData<'mcx> {
    pub var: Option<NodeId>,
    pub rel: Option<RelId>,
    pub vartype: u32,
    pub isunique: bool,
    pub stats: Option<PgStatisticBundle<'mcx>>,
}

impl<'mcx> VariableStatData<'mcx> {
    fn nullfrac(&self) -> f64 {
        self.stats.as_ref().map_or(0.0, |s| s.stanullfrac as f64)
    }

    fn slot(&self, kind: i16, reqop: Oid) -> Option<&PgStatisticSlotData<'mcx>> {
        self.stats.as_ref().and_then(|s| {
            s.slots
                .iter()
                .find(|sl| sl.kind == kind && (reqop == 0 || sl.staop == reqop))
        })
    }
}

fn opproc_for(operator: Oid) -> PgResult<FmgrInfo> {
    let opcode = lsyscache::get_opcode(operator)?;
    fmgr_core::fmgr_info(opcode)
}

fn op_test(
    opproc: &mut FmgrInfo,
    collation: Oid,
    slot_value: Datum,
    constval: Datum,
    varonleft: bool,
) -> PgResult<bool> {
    let (a0, a1) = if varonleft { (slot_value, constval) } else { (constval, slot_value) };
    Ok(types_fmgr::function_call2_coll(opproc, collation, a0, a1)?.as_bool())
}

const DEFAULT_UNK_SEL: f64 = 0.005;
const DEFAULT_NOT_UNK_SEL: f64 = 1.0 - DEFAULT_UNK_SEL;

// nulltestsel (selfuncs.c); C's jointype/sjinfo params are unused there too.
pub fn nulltestsel<'mcx>(
    run: &mut PlannerRun<'mcx>,
    is_null: bool,
    arg: Node<'mcx>,
    varrelid: i32,
) -> PgResult<f64> {
    let node_id = run.intern_expr(arg);
    let vardata = examine_variable(run, node_id, arg, varrelid)?;
    let selec = if let Some(stats) = &vardata.stats {
        let freq_null = stats.stanullfrac as f64;
        if is_null {
            freq_null
        } else {
            1.0 - freq_null
        }
    } else if matches!(arg.as_var(), Some(v) if v.varattno < 0) {
        // System attributes are never NULL (C's varattno < 0 arm).
        if is_null {
            0.0
        } else {
            1.0
        }
    } else if is_null {
        DEFAULT_UNK_SEL
    } else {
        DEFAULT_NOT_UNK_SEL
    };
    Ok(clamp_probability(selec))
}

// scalarltsel/scalarlesel/scalargtsel/scalargesel via scalarineqsel_wrapper
// (selfuncs.c).
pub fn scalarineqsel_wrapper<'mcx>(
    run: &mut PlannerRun<'mcx>,
    operator: Oid,
    args: &[NodeId],
    varrelid: i32,
    collation: Oid,
    isgt: bool,
    iseq: bool,
) -> PgResult<f64> {
    let mut operator = operator;
    let mut isgt = isgt;
    let Some((vardata, other, varonleft)) = get_restriction_variable(run, args, varrelid)?
    else {
        return Ok(DEFAULT_INEQ_SEL);
    };
    let Some(c) = other.as_const() else {
        return Ok(DEFAULT_INEQ_SEL);
    };
    if c.constisnull {
        return Ok(0.0);
    }
    if !varonleft {
        operator = lsyscache::get_commutator(operator)?;
        if operator == 0 {
            return Ok(DEFAULT_INEQ_SEL);
        }
        isgt = !isgt;
    }
    scalarineqsel(
        run,
        operator,
        isgt,
        iseq,
        collation,
        &vardata,
        c.constvalue,
        c.consttype,
    )
}

// scalarineqsel (selfuncs.c). The C no-stats CTID arm (block-position
// estimate) keeps this port's pre-existing DEFAULT_INEQ_SEL shape.
fn scalarineqsel<'mcx>(
    run: &PlannerRun<'mcx>,
    operator: Oid,
    isgt: bool,
    iseq: bool,
    collation: Oid,
    vardata: &VariableStatData<'mcx>,
    constval: Datum,
    consttype: Oid,
) -> PgResult<f64> {
    if vardata.stats.is_none() {
        return Ok(DEFAULT_INEQ_SEL);
    }
    let stanullfrac = vardata.nullfrac();
    let mut opproc = opproc_for(operator)?;

    let (mcv_selec, sumcommon) =
        mcv_selectivity(run, vardata, &mut opproc, collation, constval, true)?;
    let hist_selec = ineq_histogram_selectivity(
        run, vardata, operator, &mut opproc, isgt, iseq, collation, constval, consttype,
    )?;

    let mut selec = 1.0 - stanullfrac - sumcommon;
    if hist_selec >= 0.0 {
        selec *= hist_selec;
    } else {
        selec *= 0.5;
    }
    selec += mcv_selec;
    Ok(clamp_probability(selec))
}

// mcv_selectivity (selfuncs.c); returns (mcv_selec, sumcommon).
fn mcv_selectivity<'mcx>(
    _run: &PlannerRun<'mcx>,
    vardata: &VariableStatData<'mcx>,
    opproc: &mut FmgrInfo,
    collation: Oid,
    constval: Datum,
    varonleft: bool,
) -> PgResult<(f64, f64)> {
    let mut mcv_selec = 0.0;
    let mut sumcommon = 0.0;
    if let Some(sslot) = vardata.slot(STATISTIC_KIND_MCV, 0) {
        for (i, &v) in sslot.values.iter().enumerate() {
            if op_test(opproc, collation, v, constval, varonleft)? {
                mcv_selec += sslot.numbers[i] as f64;
            }
            sumcommon += sslot.numbers[i] as f64;
        }
    }
    Ok((mcv_selec, sumcommon))
}

// get_actual_variable_range (selfuncs.c): the index-backed endpoint probe;
// with no indexes on the rel C returns false without probing.
fn get_actual_variable_range(run: &PlannerRun<'_>, vardata: &VariableStatData<'_>) -> bool {
    let Some(rel) = vardata.rel else { return false };
    if run.root.rel(rel).indexlist.is_empty() {
        return false;
    }
    panic!("get_actual_variable_range (selfuncs.c): index-backed range probe; M2 lane");
}

// ineq_histogram_selectivity (selfuncs.c); -1 means no usable histogram.
#[allow(clippy::too_many_arguments)]
fn ineq_histogram_selectivity<'mcx>(
    run: &PlannerRun<'mcx>,
    vardata: &VariableStatData<'mcx>,
    opoid: Oid,
    opproc: &mut FmgrInfo,
    isgt: bool,
    iseq: bool,
    collation: Oid,
    constval: Datum,
    consttype: Oid,
) -> PgResult<f64> {
    let mut hist_selec = -1.0f64;
    let Some(sslot) = vardata.slot(STATISTIC_KIND_HISTOGRAM, 0) else {
        return Ok(hist_selec);
    };
    let nvalues = sslot.values.len() as i32;
    if nvalues > 1
        && sslot.stacoll == collation
        && lsyscache::comparison_ops_are_compatible(sslot.staop, opoid)?
    {
        let have_end = if nvalues == 2 {
            get_actual_variable_range(run, vardata)
        } else {
            false
        };
        let mut lobound = 0i32;
        let mut hibound = nvalues;
        while lobound < hibound {
            let probe = (lobound + hibound) / 2;
            if (probe == 0 || probe == nvalues - 1) && nvalues > 2 {
                // Endpoint replacement rides get_actual_variable_range.
                let _ = get_actual_variable_range(run, vardata);
            }
            let mut ltcmp = op_test(opproc, collation, sslot.values[probe as usize], constval, true)?;
            if isgt {
                ltcmp = !ltcmp;
            }
            if ltcmp {
                lobound = probe + 1;
            } else {
                hibound = probe;
            }
        }

        let histfrac;
        if lobound <= 0 {
            histfrac = 0.0;
        } else if lobound >= nvalues {
            histfrac = 1.0;
        } else {
            let i = lobound;
            let mut eq_selec = 0.0;
            if i == 1 || isgt == iseq {
                let mut otherdistinct = get_variable_numdistinct(run, vardata).0;
                if let Some(mcvslot) = vardata.slot(STATISTIC_KIND_MCV, 0) {
                    otherdistinct -= mcvslot.numbers.len() as f64;
                }
                if otherdistinct > 1.0 {
                    eq_selec = 1.0 / otherdistinct;
                }
            }

            let binfrac = match (
                convert_numeric_to_scalar(constval, consttype),
                convert_numeric_to_scalar(sslot.values[i as usize - 1], vardata.vartype),
                convert_numeric_to_scalar(sslot.values[i as usize], vardata.vartype),
            ) {
                (Some(val), Some(low), Some(high)) => {
                    if high <= low {
                        0.5
                    } else if val <= low {
                        0.0
                    } else if val >= high {
                        1.0
                    } else {
                        let b = (val - low) / (high - low);
                        if b.is_nan() || !(0.0..=1.0).contains(&b) { 0.5 } else { b }
                    }
                }
                _ => 0.5,
            };

            let mut frac = (i - 1) as f64 + binfrac;
            frac /= (nvalues - 1) as f64;
            if i == 1 {
                frac += eq_selec * (1.0 - binfrac);
            }
            if isgt == iseq {
                frac -= eq_selec;
            }
            histfrac = frac;
        }

        hist_selec = if isgt { 1.0 - histfrac } else { histfrac };

        if have_end {
            hist_selec = clamp_probability(hist_selec);
        } else {
            let cutoff = 0.01 / (nvalues - 1) as f64;
            hist_selec = hist_selec.clamp(cutoff, 1.0 - cutoff);
        }
    } else if nvalues > 1 {
        let mut nmatch = 0;
        for &v in sslot.values.iter() {
            if op_test(opproc, collation, v, constval, true)? {
                nmatch += 1;
            }
        }
        hist_selec = nmatch as f64 / nvalues as f64;
        let cutoff = 0.01 / (nvalues - 1) as f64;
        hist_selec = hist_selec.clamp(cutoff, 1.0 - cutoff);
    }
    Ok(hist_selec)
}

// convert_to_scalar (selfuncs.c), numeric-category arm only. Strings/bytea/
// time categories (convert_string_to_scalar etc.) fall back to None, which
// lands on C's binfrac=0.5 failure path — a divergence for those types.
fn convert_numeric_to_scalar(value: Datum, typid: Oid) -> Option<f64> {
    const INT2OID: Oid = 21;
    const INT4OID: Oid = 23;
    const INT8OID: Oid = 20;
    const FLOAT4OID: Oid = 700;
    const FLOAT8OID: Oid = 701;
    const OIDOID: Oid = 26;
    const REGPROCOID: Oid = 24;
    const REGPROCEDUREOID: Oid = 2202;
    const REGOPEROID: Oid = 2203;
    const REGOPERATOROID: Oid = 2204;
    const REGCLASSOID: Oid = 2205;
    const REGTYPEOID: Oid = 2206;
    match typid {
        BOOLOID => Some(value.as_bool() as i32 as f64),
        INT2OID => Some(value.as_i16() as f64),
        INT4OID => Some(value.as_i32() as f64),
        INT8OID => Some(value.as_i64() as f64),
        FLOAT4OID => Some(value.as_f32() as f64),
        FLOAT8OID => Some(value.as_f64()),
        OIDOID | REGPROCOID | REGPROCEDUREOID | REGOPEROID | REGOPERATOROID | REGCLASSOID
        | REGTYPEOID => Some(value.as_u32() as f64),
        _ => None,
    }
}

pub fn eqsel<'mcx>(
    run: &mut PlannerRun<'mcx>,
    operator: u32,
    args: &[NodeId],
    varrelid: i32,
    collation: u32,
) -> PgResult<f64> {
    eqsel_internal(run, operator, args, varrelid, collation, false)
}

pub fn neqsel<'mcx>(
    run: &mut PlannerRun<'mcx>,
    operator: u32,
    args: &[NodeId],
    varrelid: i32,
    collation: u32,
) -> PgResult<f64> {
    eqsel_internal(run, operator, args, varrelid, collation, true)
}

fn eqsel_internal<'mcx>(
    run: &mut PlannerRun<'mcx>,
    mut operator: u32,
    args: &[NodeId],
    varrelid: i32,
    collation: u32,
    negate: bool,
) -> PgResult<f64> {
    if negate {
        // Stats probes run against the negator (the equality operator).
        operator = lsyscache::get_negator(operator)?;
        if operator == 0 {
            return Ok(1.0 - DEFAULT_EQ_SEL);
        }
    }
    let Some((vardata, other, varonleft)) = get_restriction_variable(run, args, varrelid)? else {
        return Ok(if negate { 1.0 - DEFAULT_EQ_SEL } else { DEFAULT_EQ_SEL });
    };
    let selec = match other.as_const() {
        Some(c) => var_eq_const(
            run,
            &vardata,
            operator,
            collation,
            c.constvalue,
            c.constisnull,
            varonleft,
            negate,
        )?,
        None => var_eq_non_const(run, &vardata, negate),
    };
    Ok(selec)
}

// var_eq_non_const (selfuncs.c).
fn var_eq_non_const(run: &PlannerRun<'_>, vardata: &VariableStatData<'_>, negate: bool) -> f64 {
    let nullfrac = vardata.nullfrac();
    let selec = if vardata.isunique
        && vardata.rel.is_some_and(|r| run.root.rel(r).tuples >= 1.0)
    {
        1.0 / run.root.rel(vardata.rel.unwrap()).tuples
    } else if vardata.stats.is_some() {
        let mut selec = 1.0 - nullfrac;
        let nd = get_variable_numdistinct(run, vardata).0;
        if nd > 1.0 {
            selec /= nd;
        }
        if let Some(sslot) = vardata.slot(STATISTIC_KIND_MCV, 0) {
            if let Some(&first) = sslot.numbers.first() {
                if selec > first as f64 {
                    selec = first as f64;
                }
            }
        }
        selec
    } else {
        1.0 / get_variable_numdistinct(run, vardata).0
    };
    let selec = if negate { 1.0 - selec - nullfrac } else { selec };
    clamp_probability(selec)
}
fn get_restriction_variable<'mcx>(
    run: &mut PlannerRun<'mcx>,
    args: &[NodeId],
    varrelid: i32,
) -> PgResult<Option<(VariableStatData<'mcx>, Node<'mcx>, bool)>> {
    if args.len() != 2 {
        return Ok(None);
    }
    let left = *run.root.expr_node(args[0]);
    let right = *run.root.expr_node(args[1]);
    let vardata = examine_variable(run, args[0], left, varrelid)?;
    let rdata = examine_variable(run, args[1], right, varrelid)?;

    // estimate_expression_value: Consts pass through and a PARAM_EXEC stays a
    // Param (no bound value at plan time); other shapes keep the loud arm.
    if vardata.rel.is_some() && rdata.rel.is_none() {
        if !matches!(right.node_tag(), NodeTag::T_Const | NodeTag::T_Param) {
            panic!("estimate_expression_value (clauses.c): M2 expression lane");
        }
        return Ok(Some((vardata, right, true)));
    }
    if vardata.rel.is_none() && rdata.rel.is_some() {
        if !matches!(left.node_tag(), NodeTag::T_Const | NodeTag::T_Param) {
            panic!("estimate_expression_value (clauses.c): M2 expression lane");
        }
        return Ok(Some((rdata, left, false)));
    }
    Ok(None)
}

// examine_variable (selfuncs.c), plain-Var and pseudo-constant arms.
pub fn examine_variable<'mcx>(
    run: &mut PlannerRun<'mcx>,
    node_id: NodeId,
    node: Node<'mcx>,
    varrelid: i32,
) -> PgResult<VariableStatData<'mcx>> {
    let (vartype, _) = crate::costsize::expr_type_typmod(node);
    let mut vardata =
        VariableStatData { var: None, rel: None, vartype, isunique: false, stats: None };

    if let Some(var) = node.as_var() {
        if varrelid == 0 || varrelid == var.varno {
            let rel = crate::relnode::find_base_rel(&run.root, var.varno);
            vardata.var = Some(node_id);
            vardata.rel = Some(rel);
            vardata.isunique = crate::plancat::has_unique_index(run, rel, var.varattno);
            vardata.stats = examine_simple_variable(run, var.varno, var.varattno)?;
            return Ok(vardata);
        }
        panic!("examine_variable (selfuncs.c): foreign-rel Var; M2 join lane");
    }
    match node.node_tag() {
        NodeTag::T_Const => Ok(vardata),
        // Var-free expressions (HAVING Aggrefs, PARAM_EXEC initplan outputs):
        // C's expression leg finds no relids and returns "don't know".
        NodeTag::T_Aggref | NodeTag::T_Param => Ok(vardata),
        other => panic!("examine_variable (selfuncs.c): {other:?}; M2 expression lane"),
    }
}

// examine_simple_variable (selfuncs.c): the STATRELATTINH probe, decoded once.
fn examine_simple_variable<'mcx>(
    run: &PlannerRun<'mcx>,
    varno: i32,
    varattno: i16,
) -> PgResult<Option<PgStatisticBundle<'mcx>>> {
    let rte = run.rte(varno as usize);
    if rte.rtekind != RTEKind::RTE_RELATION {
        panic!("examine_simple_variable (selfuncs.c): {:?}; M2 lane", rte.rtekind);
    }
    syscache_seams::lookup_pg_statistic_bundle::call(run.mcx, rte.relid, varattno, rte.inh)
}

// get_variable_numdistinct (selfuncs.c). Returns (ndistinct, isdefault).
pub fn get_variable_numdistinct(
    run: &PlannerRun<'_>,
    vardata: &VariableStatData<'_>,
) -> (f64, bool) {
    let mut stanullfrac = 0.0f64;
    let mut stadistinct;
    if let Some(stats) = &vardata.stats {
        stadistinct = stats.stadistinct as f64;
        stanullfrac = stats.stanullfrac as f64;
    } else if vardata.vartype == BOOLOID {
        stadistinct = 2.0;
    } else {
        let attno = vardata
            .var
            .and_then(|id| run.root.expr_node(id).as_var().map(|v| v.varattno));
        stadistinct = match attno {
            Some(SELF_ITEM_POINTER_ATTRIBUTE_NUMBER) => -1.0,
            Some(TABLE_OID_ATTRIBUTE_NUMBER) => 1.0,
            _ => 0.0,
        };
    }
    if vardata.isunique {
        stadistinct = -1.0 * (1.0 - stanullfrac);
    }
    if stadistinct > 0.0 {
        return (crate::costsize::clamp_row_est(stadistinct), false);
    }
    let Some(rel) = vardata.rel else {
        return (DEFAULT_NUM_DISTINCT, true);
    };
    let ntuples = run.root.rel(rel).tuples;
    if ntuples <= 0.0 {
        return (DEFAULT_NUM_DISTINCT, true);
    }
    if stadistinct < 0.0 {
        return (crate::costsize::clamp_row_est(-stadistinct * ntuples), false);
    }
    if ntuples < DEFAULT_NUM_DISTINCT {
        return (crate::costsize::clamp_row_est(ntuples), false);
    }
    (DEFAULT_NUM_DISTINCT, true)
}

// var_eq_const (selfuncs.c), negate=false (neqsel is unported).
fn var_eq_const<'mcx>(
    run: &PlannerRun<'mcx>,
    vardata: &VariableStatData<'mcx>,
    oproid: Oid,
    collation: Oid,
    constval: Datum,
    constisnull: bool,
    varonleft: bool,
    negate: bool,
) -> PgResult<f64> {
    // NULL const: strict operator never returns TRUE, even for the negator.
    if constisnull {
        return Ok(0.0);
    }
    let nullfrac = vardata.nullfrac();

    let selec = if vardata.isunique
        && vardata.rel.is_some_and(|r| run.root.rel(r).tuples >= 1.0)
    {
        1.0 / run.root.rel(vardata.rel.unwrap()).tuples
    } else if vardata.stats.is_some() {
        match vardata.slot(STATISTIC_KIND_MCV, 0) {
            Some(sslot) => {
                let mut eqproc = opproc_for(oproid)?;
                let mut matched = None;
                for (i, &v) in sslot.values.iter().enumerate() {
                    if op_test(&mut eqproc, collation, v, constval, varonleft)? {
                        matched = Some(i);
                        break;
                    }
                }
                match matched {
                    Some(i) => sslot.numbers[i] as f64,
                    None => {
                        let sumcommon: f64 =
                            sslot.numbers.iter().map(|&n| n as f64).sum();
                        let mut selec =
                            clamp_probability(1.0 - sumcommon - nullfrac);
                        let otherdistinct = get_variable_numdistinct(run, vardata).0
                            - sslot.numbers.len() as f64;
                        if otherdistinct > 1.0 {
                            selec /= otherdistinct;
                        }
                        let least = sslot.numbers.last().copied().unwrap_or(0.0) as f64;
                        if !sslot.numbers.is_empty() && selec > least {
                            selec = least;
                        }
                        selec
                    }
                }
            }
            None => {
                let mut selec = 1.0 - nullfrac;
                // C treats an absent MCV slot as "no info" and still divides
                // the non-null fraction by ndistinct.
                let nd = get_variable_numdistinct(run, vardata).0;
                if nd > 1.0 {
                    selec /= nd;
                }
                selec
            }
        }
    } else {
        1.0 / get_variable_numdistinct(run, vardata).0
    };
    let selec = if negate { 1.0 - selec - nullfrac } else { selec };
    Ok(clamp_probability(selec))
}

pub struct AmCostEstimate {
    pub index_startup_cost: f64,
    pub index_total_cost: f64,
    pub index_selectivity: f64,
    pub index_correlation: f64,
    pub index_pages: f64,
}

// amcostestimate dispatch: closed set over the committed index AMs (rule 4).
pub fn amcostestimate(
    run: &mut PlannerRun<'_>,
    path_id: types_pathnodes::PathId,
    loop_count: f64,
) -> PgResult<AmCostEstimate> {
    let relam = {
        let PathNode::IndexPath(ip) = run.root.path(path_id) else {
            panic!("amcostestimate: not an IndexPath")
        };
        ip.indexinfo.as_ref().expect("indexinfo set").relam
    };
    match types_relscan::IndexAmKind::from_relam(relam) {
        types_relscan::IndexAmKind::Btree => btcostestimate(run, path_id, loop_count),
        #[allow(unreachable_patterns)]
        other => panic!("amcostestimate (selfuncs.c): {other:?}; M2 index-AM lane"),
    }
}

struct GenericCosts {
    num_index_tuples: f64,
    num_sa_scans: f64,
    index_startup_cost: f64,
    index_total_cost: f64,
    index_selectivity: f64,
    index_correlation: f64,
    num_index_pages: f64,
}

// genericcostestimate (selfuncs.c); num_sa_scans arrives preset (no SAOP).
fn genericcostestimate(
    run: &mut PlannerRun<'_>,
    path_id: types_pathnodes::PathId,
    loop_count: f64,
    costs: &mut GenericCosts,
) -> PgResult<()> {
    let (index_quals, has_orderbys, index_pages, index_tuples, index_rel, reltablespace) = {
        let PathNode::IndexPath(ip) = run.root.path(path_id) else { unreachable!() };
        let index = ip.indexinfo.as_ref().expect("indexinfo set");
        (
            get_quals_from_indexclauses(run, path_id),
            !ip.indexorderbys.is_empty(),
            index.pages,
            index.tuples,
            index.rel.expect("index rel set"),
            index.reltablespace,
        )
    };
    assert!(!has_orderbys, "genericcostestimate (selfuncs.c): indexorderbys; M2 amcanorderbyop lane");
    let index_rel_relid = run.root.rel(index_rel).relid as i32;
    let index_rel_tuples = run.root.rel(index_rel).tuples;

    // add_predicate_to_index_quals: identity for a non-partial index.
    debug_assert!(costs.num_sa_scans >= 1.0);
    let num_sa_scans = costs.num_sa_scans;

    let index_selectivity = crate::clausesel::clauselist_selectivity(
        run,
        &index_quals,
        index_rel_relid,
        JOIN_INNER,
        None,
    )?;

    let mut num_index_tuples = costs.num_index_tuples;
    if num_index_tuples <= 0.0 {
        num_index_tuples = index_selectivity * index_rel_tuples;
        num_index_tuples = (num_index_tuples / num_sa_scans).round_ties_even();
    }
    if num_index_tuples > index_tuples {
        num_index_tuples = index_tuples;
    }
    if num_index_tuples < 1.0 {
        num_index_tuples = 1.0;
    }

    let num_index_pages = if index_pages > 1 && index_tuples > 1.0 {
        (num_index_tuples * index_pages as f64 / index_tuples).ceil()
    } else {
        1.0
    };

    let (spc_random_page_cost, _) = crate::costsize::get_tablespace_page_costs(reltablespace);

    let num_scans = num_sa_scans * loop_count;
    let mut index_total_cost = if num_scans > 1.0 {
        let pages_fetched = crate::costsize::index_pages_fetched(
            run,
            num_index_pages * num_scans,
            index_pages,
            index_pages as f64,
        );
        (pages_fetched * spc_random_page_cost) / loop_count
    } else {
        num_index_pages * spc_random_page_cost
    };

    let qual_arg_cost = index_other_operands_eval_cost(run, &index_quals)?;
    let qual_op_cost = gucs::cpu_operator_cost() * index_quals.len() as f64;

    let index_startup_cost = qual_arg_cost;
    index_total_cost += qual_arg_cost;
    index_total_cost += num_index_tuples * num_sa_scans * (gucs::cpu_index_tuple_cost() + qual_op_cost);

    costs.index_startup_cost = index_startup_cost;
    costs.index_total_cost = index_total_cost;
    costs.index_selectivity = index_selectivity;
    costs.index_correlation = 0.0;
    costs.num_index_pages = num_index_pages;
    costs.num_index_tuples = num_index_tuples;
    costs.num_sa_scans = num_sa_scans;
    Ok(())
}

// get_quals_from_indexclauses (selfuncs.c).
fn get_quals_from_indexclauses<'mcx>(
    run: &PlannerRun<'mcx>,
    path_id: types_pathnodes::PathId,
) -> mcx::PgVec<'mcx, RinfoId> {
    let PathNode::IndexPath(ip) = run.root.path(path_id) else { unreachable!() };
    let mut out = mcx::PgVec::new_in(run.mcx);
    for ic in ip.indexclauses.iter() {
        for &r in ic.indexquals.iter() {
            out.push(r);
        }
    }
    out
}

// index_other_operands_eval_cost (selfuncs.c).
fn index_other_operands_eval_cost(
    run: &mut PlannerRun<'_>,
    index_quals: &[RinfoId],
) -> PgResult<f64> {
    let mut qual_arg_cost = 0.0;
    for &rid in index_quals {
        let clause = *run.root.expr_node(run.root.rinfo(rid).clause);
        let other_operand = match clause.node_tag() {
            // indexkey is always the left operand of a fixed indexqual.
            NodeTag::T_OpExpr => Some(clause.as_op_expr().unwrap().args.nth(1)),
            other => panic!("index_other_operands_eval_cost (selfuncs.c): {other:?}; M2 lane"),
        };
        if let Some(op) = other_operand {
            let cost = crate::costsize::cost_qual_eval_node(op)?;
            qual_arg_cost += cost.startup + cost.per_tuple;
        }
    }
    Ok(qual_arg_cost)
}

// btcostestimate (selfuncs.c); the boundary-qual walk sees only OpExprs.
fn btcostestimate(
    run: &mut PlannerRun<'_>,
    path_id: types_pathnodes::PathId,
    loop_count: f64,
) -> PgResult<AmCostEstimate> {
    let (indexclauses, index_unique, index_nkeycolumns, index_tuples, index_tree_height, index_rel, opfamilies) = {
        let PathNode::IndexPath(ip) = run.root.path(path_id) else {
            panic!("btcostestimate: not an IndexPath")
        };
        let index = ip.indexinfo.as_ref().expect("indexinfo set");
        let mut fams = mcx::PgVec::new_in(run.mcx);
        fams.extend(index.opfamily.iter().copied());
        (
            ip.indexclauses.clone(),
            index.unique,
            index.nkeycolumns,
            index.tuples,
            index.tree_height.get(),
            index.rel.expect("index rel set"),
            fams,
        )
    };
    let index_rel_relid = run.root.rel(index_rel).relid as i32;
    let index_rel_tuples = run.root.rel(index_rel).tuples;
    let index_pages = {
        let PathNode::IndexPath(ip) = run.root.path(path_id) else { unreachable!() };
        ip.indexinfo.as_ref().unwrap().pages
    };

    let mut index_bound_quals: mcx::PgVec<'_, RinfoId> = mcx::PgVec::new_in(run.mcx);
    let mut indexcol: i32 = 0;
    let mut eq_qual_here = false;
    let num_sa_scans = 1.0f64;

    for iclause in indexclauses.iter() {
        if indexcol < iclause.indexcol as i32 {
            // A column gap means nbtree would consider skip arrays.
            if eq_qual_here {
                indexcol += 1;
            }
            eq_qual_here = false;
            if indexcol < iclause.indexcol as i32 {
                panic!("btcostestimate (selfuncs.c): skip-array column gap; M2 skip-scan lane");
            }
        }
        debug_assert!(indexcol == iclause.indexcol as i32);

        for &rid in iclause.indexquals.iter() {
            let clause = *run.root.expr_node(run.root.rinfo(rid).clause);
            let clause_op = match clause.node_tag() {
                NodeTag::T_OpExpr => clause.as_op_expr().unwrap().opno,
                other => panic!("btcostestimate (selfuncs.c): indexqual {other:?}; M2 lane"),
            };
            let op_strategy =
                lsyscache::get_op_opfamily_strategy(clause_op, opfamilies[indexcol as usize])?;
            debug_assert!(op_strategy != 0);
            if op_strategy == lsyscache::BTEqualStrategyNumber as i32 {
                eq_qual_here = true;
            }
            index_bound_quals.push(rid);
        }
    }

    let num_index_tuples = if index_unique
        && indexcol == index_nkeycolumns - 1
        && eq_qual_here
    {
        1.0
    } else {
        let btree_selectivity = crate::clausesel::clauselist_selectivity(
            run,
            &index_bound_quals,
            index_rel_relid,
            JOIN_INNER,
            None,
        )?;
        let nit = btree_selectivity * index_rel_tuples;
        debug_assert!(num_sa_scans == 1.0);
        (nit / num_sa_scans).round_ties_even()
    };

    let mut costs = GenericCosts {
        num_index_tuples,
        num_sa_scans,
        index_startup_cost: 0.0,
        index_total_cost: 0.0,
        index_selectivity: 0.0,
        index_correlation: 0.0,
        num_index_pages: 0.0,
    };
    genericcostestimate(run, path_id, loop_count, &mut costs)?;

    let cpu_operator_cost = gucs::cpu_operator_cost();
    if index_tuples > 1.0 {
        let descent_cost = (index_tuples.ln() / 2.0f64.ln()).ceil() * cpu_operator_cost;
        costs.index_startup_cost += descent_cost;
        costs.index_total_cost += costs.num_sa_scans * descent_cost;
    }
    let descent_cost =
        (index_tree_height as f64 + 1.0) * DEFAULT_PAGE_CPU_MULTIPLIER * cpu_operator_cost;
    costs.index_startup_cost += descent_cost;
    costs.index_total_cost += costs.num_sa_scans * descent_cost;

    // btcost_correlation over the leading simple column.
    {
        let (attno, opfamily0, opcintype0, reverse0, nkeycols) = {
            let PathNode::IndexPath(ip) = run.root.path(path_id) else { unreachable!() };
            let index = ip.indexinfo.as_ref().unwrap();
            (
                index.indexkeys[0] as i16,
                index.opfamily[0],
                index.opcintype[0],
                index.reverse_sort[0],
                index.nkeycolumns,
            )
        };
        if attno == 0 {
            panic!("btcost_correlation (selfuncs.c): expression index column; M2 lane");
        }
        let rte = run.rte(index_rel_relid as usize);
        if let Some(bundle) =
            syscache_seams::lookup_pg_statistic_bundle::call(run.mcx, rte.relid, attno, rte.inh)?
        {
            let sortop = lsyscache::get_opfamily_member(
                opfamily0,
                opcintype0,
                opcintype0,
                lsyscache::BTLessStrategyNumber,
            )?;
            let slot = bundle
                .slots
                .iter()
                .find(|sl| sl.kind == STATISTIC_KIND_CORRELATION && sl.staop == sortop);
            if let (true, Some(slot)) = (sortop != 0, slot) {
                debug_assert!(slot.numbers.len() == 1);
                let mut corr = slot.numbers[0] as f64;
                if reverse0 {
                    corr = -corr;
                }
                costs.index_correlation = if nkeycols > 1 { corr * 0.75 } else { corr };
            }
        }
    }
    let _ = index_pages;

    Ok(AmCostEstimate {
        index_startup_cost: costs.index_startup_cost,
        index_total_cost: costs.index_total_cost,
        index_selectivity: costs.index_selectivity,
        index_correlation: costs.index_correlation,
        index_pages: costs.num_index_pages,
    })
}

// estimate_num_groups (selfuncs.c), no-stats Var-only leg; other families
// and multivariate/extended stats are M3 lanes.
pub fn estimate_num_groups<'mcx>(
    run: &mut PlannerRun<'mcx>,
    group_exprs: &[(NodeId, Node<'mcx>)],
    input_rows: f64,
) -> PgResult<f64> {
    let input_rows = crate::costsize::clamp_row_est(input_rows);
    if group_exprs.is_empty() {
        return Ok(1.0);
    }

    struct GroupVarInfo {
        var: NodeId,
        rel: RelId,
        ndistinct: f64,
    }
    let mcx = run.mcx;
    let mut varinfos: mcx::PgVec<'_, GroupVarInfo> = mcx::PgVec::new_in(mcx);
    for &(id, node) in group_exprs {
        match node.node_tag() {
            NodeTag::T_Const => continue,
            NodeTag::T_Var => {}
            other => panic!(
                "estimate_num_groups (selfuncs.c): grouping expr {other:?}; M3 expression lane"
            ),
        }
        let v = node.as_var().unwrap();
        let dup = varinfos.iter().any(|vi| {
            let u = run.root.expr_node(vi.var).as_var().unwrap();
            u.varno == v.varno && u.varattno == v.varattno
        });
        if dup {
            continue;
        }
        let vardata = examine_variable(run, id, node, 0)?;
        let (ndistinct, _isdefault) = get_variable_numdistinct(run, &vardata);
        varinfos.push(GroupVarInfo {
            var: id,
            rel: vardata.rel.expect("grouping Var has a base rel"),
            ndistinct,
        });
    }
    if varinfos.is_empty() {
        return Ok(1.0);
    }

    let mut numdistinct = 1.0f64;
    let mut remaining = varinfos;
    while !remaining.is_empty() {
        let rel_id = remaining[0].rel;
        let mut reldistinct = 1.0f64;
        let mut relmaxndistinct = 1.0f64;
        let mut relvarcount = 0usize;
        let mut rest: mcx::PgVec<'_, GroupVarInfo> = mcx::PgVec::new_in(mcx);
        for vi in remaining {
            if vi.rel == rel_id {
                reldistinct *= vi.ndistinct;
                if relmaxndistinct < vi.ndistinct {
                    relmaxndistinct = vi.ndistinct;
                }
                relvarcount += 1;
            } else {
                rest.push(vi);
            }
        }
        let (rel_tuples, rel_rows) = {
            let rel = run.root.rel(rel_id);
            (rel.tuples, rel.rows)
        };
        if rel_tuples > 0.0 {
            let mut clamp = rel_tuples;
            if relvarcount > 1 {
                clamp *= 0.1;
                if clamp < relmaxndistinct {
                    clamp = relmaxndistinct.min(rel_tuples);
                }
            }
            if reldistinct > clamp {
                reldistinct = clamp;
            }
            if reldistinct > 0.0 && rel_rows < rel_tuples {
                // Dell'Era approximation of Yao's formula.
                reldistinct *=
                    1.0 - ((rel_tuples - rel_rows) / rel_tuples).powf(rel_tuples / reldistinct);
            }
            numdistinct *= crate::costsize::clamp_row_est(reldistinct);
        }
        remaining = rest;
    }

    let numdistinct = numdistinct.ceil();
    Ok(numdistinct.clamp(1.0, input_rows))
}

// eqjoinsel (selfuncs.c). C's MCV-x-MCV arms fire only when BOTH sides carry
// MCV lists; that lane is unported and panics. eqjoinsel_inner's else arm:
// (1-nullfrac1)*(1-nullfrac2) / max(nd1, nd2).
pub fn eqjoinsel<'mcx>(
    run: &mut PlannerRun<'mcx>,
    _operator: u32,
    args: &[NodeId],
    jointype: types_pathnodes::JoinType,
    sjinfo: Option<&types_pathnodes::SpecialJoinInfo<'mcx>>,
) -> PgResult<f64> {
    assert!(args.len() == 2, "eqjoinsel (selfuncs.c): non-binary clause");
    let sj_jointype = sjinfo.map_or(jointype, |sj| sj.jointype);
    let left = *run.root.expr_node(args[0]);
    let right = *run.root.expr_node(args[1]);
    let vardata1 = examine_variable(run, args[0], left, 0)?;
    let vardata2 = examine_variable(run, args[1], right, 0)?;
    let (nd1, isdefault1) = get_variable_numdistinct(run, &vardata1);
    let (nd2, isdefault2) = get_variable_numdistinct(run, &vardata2);

    if vardata1.slot(STATISTIC_KIND_MCV, 0).is_some()
        && vardata2.slot(STATISTIC_KIND_MCV, 0).is_some()
    {
        panic!("eqjoinsel_inner (selfuncs.c): MCV-join lane");
    }
    let selec_inner =
        (1.0 - vardata1.nullfrac()) * (1.0 - vardata2.nullfrac()) / nd1.max(nd2);
    let selec = match sj_jointype {
        JOIN_INNER | types_pathnodes::JOIN_LEFT | types_pathnodes::JOIN_FULL => selec_inner,
        types_pathnodes::JOIN_SEMI | types_pathnodes::JOIN_ANTI => {
            let sjinfo = sjinfo.expect("SEMI/ANTI eqjoinsel has an sjinfo");
            let inner_rel = find_join_input_rel(run, &sjinfo.min_righthand);
            let inner_rows = run.root.rel(inner_rel).rows;
            // get_join_variables (selfuncs.c) reversal test.
            let rel_subset = |rel: Option<RelId>, side: &types_pathnodes::Relids<'mcx>| {
                rel.is_some_and(|r| {
                    crate::relnode::relids_is_subset(&run.root.rel(r).relids, side)
                })
            };
            let join_is_reversed = rel_subset(vardata1.rel, &sjinfo.syn_righthand)
                || rel_subset(vardata2.rel, &sjinfo.syn_lefthand);
            let semi = if !join_is_reversed {
                eqjoinsel_semi(run, &vardata1, &vardata2, nd1, nd2, isdefault1, isdefault2, inner_rel)
            } else {
                eqjoinsel_semi(run, &vardata2, &vardata1, nd2, nd1, isdefault2, isdefault1, inner_rel)
            };
            semi.min(inner_rows * selec_inner)
        }
        other => panic!("eqjoinsel (selfuncs.c): jointype {other}"),
    };
    Ok(clamp_probability(selec))
}

// eqjoinsel_semi (selfuncs.c), non-MCV arm (the MCV-x-MCV arm panics above).
#[allow(clippy::too_many_arguments)]
fn eqjoinsel_semi(
    run: &PlannerRun<'_>,
    vardata1: &VariableStatData<'_>,
    vardata2: &VariableStatData<'_>,
    _nd1: f64,
    _nd2: f64,
    isdefault1: bool,
    isdefault2: bool,
    inner_rel: RelId,
) -> f64 {
    let nd1 = _nd1;
    let mut nd2 = _nd2;
    let mut isdefault2 = isdefault2;
    if let Some(rel2) = vardata2.rel {
        let rows2 = run.root.rel(rel2).rows;
        if nd2 >= rows2 {
            nd2 = rows2;
            isdefault2 = false;
        }
    }
    let inner_rows = run.root.rel(inner_rel).rows;
    if nd2 >= inner_rows {
        nd2 = inner_rows;
        isdefault2 = false;
    }
    let nullfrac1 = vardata1.nullfrac();
    if !isdefault1 && !isdefault2 {
        if nd1 <= nd2 || nd2 < 0.0 {
            1.0 - nullfrac1
        } else {
            (nd2 / nd1) * (1.0 - nullfrac1)
        }
    } else {
        0.5 * (1.0 - nullfrac1)
    }
}

// find_join_input_rel (selfuncs.c).
fn find_join_input_rel<'mcx>(
    run: &PlannerRun<'mcx>,
    relids: &types_pathnodes::Relids<'mcx>,
) -> RelId {
    if let Some(relid) = crate::relnode::relids_singleton_member(relids) {
        return crate::relnode::find_base_rel(&run.root, relid);
    }
    for &jr in run.root.join_rel_list.iter() {
        if crate::relnode::relids_equal(&run.root.rel(jr).relids, relids) {
            return jr;
        }
    }
    panic!("could not find join input relation");
}

// estimate_hash_bucket_stats (selfuncs.c), no-stats/no-MCV lane. The MCV bucket
// adjustment (mcvfreq/avgfreq) is the extended-stats lane; without an MCV list
// mcvfreq is 0 and the bucketsize is 1/ndistinct clamped to virtualbuckets.
pub fn estimate_hash_bucket_stats<'mcx>(
    run: &mut PlannerRun<'mcx>,
    hashkey: Node<'mcx>,
    virtualbuckets: f64,
) -> PgResult<(f64, f64)> {
    let node_id = run.intern_expr(hashkey);
    let vardata = examine_variable(run, node_id, hashkey, 0)?;
    let (mut ndistinct, isdefault) = get_variable_numdistinct(run, &vardata);
    let mcvfreq = 0.0;
    if isdefault {
        return Ok((mcvfreq, 0.1f64.max(mcvfreq)));
    }
    // stanullfrac is 0 on the no-stats lane; scale ndistinct by the
    // restriction selectivity as C does.
    if let Some(rel) = vardata.rel {
        let (tuples, rows) = (run.root.rel(rel).tuples, run.root.rel(rel).rows);
        if tuples > 0.0 {
            ndistinct *= rows / tuples;
            ndistinct = crate::costsize::clamp_row_est(ndistinct);
        }
    }
    if ndistinct <= 0.0 {
        ndistinct = 1.0;
    }
    let mut estfract = if ndistinct > virtualbuckets {
        1.0 / virtualbuckets
    } else {
        1.0 / ndistinct
    };
    if estfract < 1.0e-6 {
        estfract = 1.0e-6;
    }
    Ok((mcvfreq, estfract))
}

// mergejoinscansel (selfuncs.c) -> (leftstart, leftend, rightstart, rightend).
// Every "insufficient info" leg (missing operators, no histogram/MCV range)
// lands on C's silent-fail defaults 0.0/1.0, which is also the no-stats arm.
pub fn mergejoinscansel<'mcx>(
    run: &mut PlannerRun<'mcx>,
    rinfo: RinfoId,
    opfamily: Oid,
    cmptype: i32,
    nulls_first: bool,
) -> PgResult<(f64, f64, f64, f64)> {
    use types_pathnodes::{COMPARE_GE, COMPARE_GT, COMPARE_LE, COMPARE_LT};

    let mut leftstart = 0.0f64;
    let mut leftend = 1.0f64;
    let mut rightstart = 0.0f64;
    let mut rightend = 1.0f64;
    let fail = Ok((0.0, 1.0, 0.0, 1.0));

    let clause = *run.root.expr_node(run.root.rinfo(rinfo).clause);
    let Some(o) = clause.as_op_expr().filter(|o| o.args.len() == 2) else {
        return fail;
    };
    let (opno, collation) = (o.opno, o.inputcollid);
    let (left, right) = (o.args.nth(0), o.args.nth(1));
    let lid = run.intern_expr(left);
    let rid = run.intern_expr(right);
    let leftvar = examine_variable(run, lid, left, 0)?;
    let rightvar = examine_variable(run, rid, right, 0)?;

    let (op_strategy, op_lefttype, op_righttype) =
        lsyscache::get_op_opfamily_properties(opno, opfamily, false)?;
    debug_assert!(op_strategy == types_pathnodes::COMPARE_EQ);

    let member = |lt: Oid, rt: Oid, cmp: i32| lsyscache::get_opfamily_member_for_cmptype(opfamily, lt, rt, cmp);

    let (isgt, lsortop, rsortop, lstatop, rstatop, ltop, leop, revltop, revleop);
    match cmptype {
        COMPARE_LT => {
            isgt = false;
            ltop = member(op_lefttype, op_righttype, COMPARE_LT)?;
            leop = member(op_lefttype, op_righttype, COMPARE_LE)?;
            if op_lefttype == op_righttype {
                lsortop = ltop;
                rsortop = ltop;
                lstatop = lsortop;
                rstatop = rsortop;
                revltop = ltop;
                revleop = leop;
            } else {
                lsortop = member(op_lefttype, op_lefttype, COMPARE_LT)?;
                rsortop = member(op_righttype, op_righttype, COMPARE_LT)?;
                lstatop = lsortop;
                rstatop = rsortop;
                revltop = member(op_righttype, op_lefttype, COMPARE_LT)?;
                revleop = member(op_righttype, op_lefttype, COMPARE_LE)?;
            }
        }
        COMPARE_GT => {
            isgt = true;
            ltop = member(op_lefttype, op_righttype, COMPARE_GT)?;
            leop = member(op_lefttype, op_righttype, COMPARE_GE)?;
            if op_lefttype == op_righttype {
                lsortop = ltop;
                rsortop = ltop;
                lstatop = member(op_lefttype, op_lefttype, COMPARE_LT)?;
                rstatop = lstatop;
                revltop = ltop;
                revleop = leop;
            } else {
                lsortop = member(op_lefttype, op_lefttype, COMPARE_GT)?;
                rsortop = member(op_righttype, op_righttype, COMPARE_GT)?;
                lstatop = member(op_lefttype, op_lefttype, COMPARE_LT)?;
                rstatop = member(op_righttype, op_righttype, COMPARE_LT)?;
                revltop = member(op_righttype, op_lefttype, COMPARE_GT)?;
                revleop = member(op_righttype, op_lefttype, COMPARE_GE)?;
            }
        }
        _ => return fail,
    }

    if lsortop == 0
        || rsortop == 0
        || lstatop == 0
        || rstatop == 0
        || ltop == 0
        || leop == 0
        || revltop == 0
        || revleop == 0
    {
        return fail;
    }

    let Some((mut leftmin, mut leftmax)) = get_variable_range(run, &leftvar, lstatop, collation)?
    else {
        return fail;
    };
    let Some((mut rightmin, mut rightmax)) =
        get_variable_range(run, &rightvar, rstatop, collation)?
    else {
        return fail;
    };
    if isgt {
        core::mem::swap(&mut leftmin, &mut leftmax);
        core::mem::swap(&mut rightmin, &mut rightmax);
    }

    let selec = scalarineqsel(run, leop, isgt, true, collation, &leftvar, rightmax, op_righttype)?;
    if selec != DEFAULT_INEQ_SEL {
        leftend = selec;
    }
    let selec = scalarineqsel(run, revleop, isgt, true, collation, &rightvar, leftmax, op_lefttype)?;
    if selec != DEFAULT_INEQ_SEL {
        rightend = selec;
    }
    if leftend > rightend {
        leftend = 1.0;
    } else if leftend < rightend {
        rightend = 1.0;
    } else {
        leftend = 1.0;
        rightend = 1.0;
    }

    let selec = scalarineqsel(run, ltop, isgt, false, collation, &leftvar, rightmin, op_righttype)?;
    if selec != DEFAULT_INEQ_SEL {
        leftstart = selec;
    }
    let selec =
        scalarineqsel(run, revltop, isgt, false, collation, &rightvar, leftmin, op_lefttype)?;
    if selec != DEFAULT_INEQ_SEL {
        rightstart = selec;
    }
    if leftstart < rightstart {
        leftstart = 0.0;
    } else if leftstart > rightstart {
        rightstart = 0.0;
    } else {
        leftstart = 0.0;
        rightstart = 0.0;
    }

    if nulls_first {
        if leftvar.stats.is_some() {
            let f = leftvar.nullfrac();
            leftstart = clamp_probability(leftstart + f);
            leftend = clamp_probability(leftend + f);
        }
        if rightvar.stats.is_some() {
            let f = rightvar.nullfrac();
            rightstart = clamp_probability(rightstart + f);
            rightend = clamp_probability(rightend + f);
        }
    }

    if leftstart >= leftend {
        leftstart = 0.0;
        leftend = 1.0;
    }
    if rightstart >= rightend {
        rightstart = 0.0;
        rightend = 1.0;
    }
    Ok((leftstart, leftend, rightstart, rightend))
}

// get_variable_range (selfuncs.c) -> Some((min, max)) or None. The C
// datumCopy is skipped: slot datums live in the planner arena already.
// statistic_proc_security_check reduces to true on this substrate.
fn get_variable_range<'mcx>(
    run: &PlannerRun<'mcx>,
    vardata: &VariableStatData<'mcx>,
    sortop: Oid,
    collation: Oid,
) -> PgResult<Option<(Datum, Datum)>> {
    let Some(stats) = &vardata.stats else {
        return Ok(None);
    };
    let _ = run;
    let opfuncoid = lsyscache::get_opcode(sortop)?;
    let mut opproc: Option<FmgrInfo> = None;
    let mut range: Option<(Datum, Datum)> = None;

    if let Some(sslot) = vardata.slot(STATISTIC_KIND_HISTOGRAM, sortop) {
        if sslot.stacoll == collation && !sslot.values.is_empty() {
            range = Some((sslot.values[0], sslot.values[sslot.values.len() - 1]));
        }
    }
    if range.is_none() {
        if let Some(sslot) = vardata.slot(STATISTIC_KIND_HISTOGRAM, 0) {
            get_stats_slot_range(&sslot.values, opfuncoid, &mut opproc, collation, &mut range)?;
        }
    }
    if let Some(sslot) = vardata.slot(STATISTIC_KIND_MCV, 0) {
        let use_mcvs = if range.is_some() {
            true
        } else {
            let sumcommon: f64 = sslot.numbers.iter().map(|&n| n as f64).sum();
            sumcommon + stats.stanullfrac as f64 > 0.99999
        };
        if use_mcvs {
            get_stats_slot_range(&sslot.values, opfuncoid, &mut opproc, collation, &mut range)?;
        }
    }
    Ok(range)
}

fn get_stats_slot_range(
    values: &[Datum],
    opfuncoid: Oid,
    opproc: &mut Option<FmgrInfo>,
    collation: Oid,
    range: &mut Option<(Datum, Datum)>,
) -> PgResult<()> {
    if values.is_empty() {
        return Ok(());
    }
    if opproc.is_none() {
        *opproc = Some(fmgr_core::fmgr_info(opfuncoid)?);
    }
    let opproc = opproc.as_mut().unwrap();
    for &v in values {
        match range {
            None => *range = Some((v, v)),
            Some((tmin, tmax)) => {
                if types_fmgr::function_call2_coll(opproc, collation, v, *tmin)?.as_bool() {
                    *tmin = v;
                }
                if types_fmgr::function_call2_coll(opproc, collation, *tmax, v)?.as_bool() {
                    *tmax = v;
                }
            }
        }
    }
    Ok(())
}
