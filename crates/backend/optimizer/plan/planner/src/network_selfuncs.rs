//! network_selfuncs.c: networksel/networkjoinsel over MCV + histogram stats.

use adt_network::{bitncmp, bitncommon, InetRef};
use datum::Datum;
use types_core::Oid;
use types_error::PgResult;
use types_pathnodes::{NodeId, SpecialJoinInfo};

use crate::run::PlannerRun;
use crate::selfuncs::{
    clamp_probability, get_join_variables, get_restriction_variable, mcv_selectivity,
    opproc_for, VariableStatData, STATISTIC_KIND_HISTOGRAM, STATISTIC_KIND_MCV,
};

const DEFAULT_OVERLAP_SEL: f64 = 0.01;
const DEFAULT_INCLUSION_SEL: f64 = 0.005;
const MAX_CONSIDERED_ELEMS: i32 = 1024;

const OID_INET_SUB_OP: Oid = 931;
const OID_INET_SUBEQ_OP: Oid = 932;
const OID_INET_SUP_OP: Oid = 933;
const OID_INET_SUPEQ_OP: Oid = 934;
const OID_INET_OVERLAP_OP: Oid = 3552;

fn default_sel(operator: Oid) -> f64 {
    if operator == OID_INET_OVERLAP_OP { DEFAULT_OVERLAP_SEL } else { DEFAULT_INCLUSION_SEL }
}

fn inet_opr_codenum(operator: Oid) -> i32 {
    match operator {
        OID_INET_SUP_OP => -2,
        OID_INET_SUPEQ_OP => -1,
        OID_INET_OVERLAP_OP => 0,
        OID_INET_SUBEQ_OP => 1,
        OID_INET_SUB_OP => 2,
        _ => panic!("unrecognized operator {operator} for inet selectivity"),
    }
}

// Stats/Const inet datums carry short or 4-byte varlena headers.
pub(crate) fn inet_ref<'a>(d: Datum) -> InetRef<'a> {
    let p = d.as_usize() as *const u8;
    // SAFETY: live inet varlena datum (never external: 22 bytes max).
    unsafe {
        let b0 = *p;
        if b0 & 0x01 != 0 {
            assert_ne!(b0, 0x01, "toast pointer in inet selectivity");
            let len = ((b0 as usize) >> 1) & 0x7F;
            InetRef::from_payload(core::slice::from_raw_parts(p.add(1), len - 1))
        } else {
            assert_eq!(b0 & 0x03, 0, "compressed inet in selectivity");
            let len = adt_rangetypes::varsize_4b(p);
            InetRef::from_payload(core::slice::from_raw_parts(p.add(4), len - 4))
        }
    }
}

pub fn networksel<'mcx>(
    run: &mut PlannerRun<'mcx>,
    operator: Oid,
    args: &[NodeId],
    varrelid: i32,
) -> PgResult<f64> {
    let opr_codenum = inet_opr_codenum(operator);
    let Some((vardata, other, varonleft)) = get_restriction_variable(run, args, varrelid)?
    else {
        return Ok(default_sel(operator));
    };
    let Some(c) = other.as_const() else {
        return Ok(default_sel(operator));
    };
    if c.constisnull {
        return Ok(0.0);
    }
    let constvalue = c.constvalue;

    if vardata.stats.is_none() {
        return Ok(default_sel(operator));
    }
    let nullfrac = vardata.nullfrac();

    let mut proc = opproc_for(operator)?;
    let (mcv_selec, sumcommon) =
        mcv_selectivity(run, &vardata, &mut proc, 0, constvalue, varonleft)?;

    let non_mcv_selec = match vardata.slot(STATISTIC_KIND_HISTOGRAM, 0) {
        Some(hslot) => {
            let h_codenum = if varonleft { opr_codenum } else { -opr_codenum };
            inet_hist_value_sel(hslot.values()?, constvalue, h_codenum)
        }
        None => default_sel(operator),
    };

    let selec = mcv_selec + (1.0 - nullfrac - sumcommon) * non_mcv_selec;
    Ok(clamp_probability(selec))
}

pub fn networkjoinsel<'mcx>(
    run: &mut PlannerRun<'mcx>,
    operator: Oid,
    args: &[NodeId],
    sjinfo: Option<&SpecialJoinInfo<'mcx>>,
) -> PgResult<f64> {
    let opr_codenum = inet_opr_codenum(operator);
    let sjinfo = sjinfo.expect("networkjoinsel called with an sjinfo");
    let (vardata1, vardata2, join_is_reversed) = get_join_variables(run, args, sjinfo)?;

    let selec = match sjinfo.jointype {
        types_pathnodes::JOIN_INNER | types_pathnodes::JOIN_LEFT
        | types_pathnodes::JOIN_FULL => {
            networkjoinsel_inner(run, operator, opr_codenum, &vardata1, &vardata2)?
        }
        types_pathnodes::JOIN_SEMI | types_pathnodes::JOIN_ANTI => {
            if !join_is_reversed {
                networkjoinsel_semi(run, operator, opr_codenum, &vardata1, &vardata2)?
            } else {
                networkjoinsel_semi(
                    run,
                    lsyscache::get_commutator(operator)?,
                    -opr_codenum,
                    &vardata2,
                    &vardata1,
                )?
            }
        }
        other => panic!("unrecognized join type: {other}"),
    };
    Ok(clamp_probability(selec))
}

struct SideStats<'a> {
    nullfrac: f64,
    mcv: Option<(&'a [Datum], &'a [f32])>,
    hist: Option<&'a [Datum]>,
    mcv_length: i32,
    sumcommon: f64,
}

fn side_stats<'a, 'mcx>(vardata: &'a VariableStatData<'mcx>) -> PgResult<SideStats<'a>> {
    let mut s = SideStats { nullfrac: 0.0, mcv: None, hist: None, mcv_length: 0, sumcommon: 0.0 };
    if vardata.stats.is_some() {
        s.nullfrac = vardata.nullfrac();
        if let Some(mslot) = vardata.slot(STATISTIC_KIND_MCV, 0) {
            // Torn slot: only the paired (value, frequency) prefix is usable
            // (C's nvalues bound assumes never-torn arrays from the pinned
            // statsTuple). Equal lengths on well-formed slots make this C's
            // Min(nvalues, MAX_CONSIDERED_ELEMS) exactly.
            let values = mslot.values()?;
            let numbers = mslot.numbers()?;
            let paired = values.len().min(numbers.len());
            s.mcv_length = (paired as i32).min(MAX_CONSIDERED_ELEMS);
            s.sumcommon = mcv_population(&numbers[..s.mcv_length as usize]);
            s.mcv = Some((&values[..paired], &numbers[..paired]));
        }
        if let Some(hslot) = vardata.slot(STATISTIC_KIND_HISTOGRAM, 0) {
            s.hist = Some(hslot.values()?);
        }
    }
    Ok(s)
}

fn networkjoinsel_inner<'mcx>(
    run: &PlannerRun<'mcx>,
    operator: Oid,
    opr_codenum: i32,
    vardata1: &VariableStatData<'mcx>,
    vardata2: &VariableStatData<'mcx>,
) -> PgResult<f64> {
    let s1 = side_stats(vardata1)?;
    let s2 = side_stats(vardata2)?;
    let mut selec = 0.0f64;

    if let (Some((v1, n1)), Some((v2, n2))) = (s1.mcv, s2.mcv) {
        selec += inet_mcv_join_sel(
            &v1[..s1.mcv_length as usize],
            n1,
            &v2[..s2.mcv_length as usize],
            n2,
            operator,
        )?;
    }
    if let (Some((v1, n1)), Some(h2)) = (s1.mcv, s2.hist) {
        selec += (1.0 - s2.nullfrac - s2.sumcommon)
            * inet_mcv_hist_sel(&v1[..s1.mcv_length as usize], n1, h2, opr_codenum);
    }
    if let (Some((v2, n2)), Some(h1)) = (s2.mcv, s1.hist) {
        selec += (1.0 - s1.nullfrac - s1.sumcommon)
            * inet_mcv_hist_sel(&v2[..s2.mcv_length as usize], n2, h1, -opr_codenum);
    }
    if let (Some(h1), Some(h2)) = (s1.hist, s2.hist) {
        selec += (1.0 - s1.nullfrac - s1.sumcommon)
            * (1.0 - s2.nullfrac - s2.sumcommon)
            * inet_hist_inclusion_join_sel(h1, h2, opr_codenum);
    }

    if (s1.mcv.is_none() && s1.hist.is_none()) || (s2.mcv.is_none() && s2.hist.is_none()) {
        selec = (1.0 - s1.nullfrac) * (1.0 - s2.nullfrac) * default_sel(operator);
    }
    let _ = run;
    Ok(selec)
}

fn networkjoinsel_semi<'mcx>(
    run: &PlannerRun<'mcx>,
    operator: Oid,
    opr_codenum: i32,
    vardata1: &VariableStatData<'mcx>,
    vardata2: &VariableStatData<'mcx>,
) -> PgResult<f64> {
    let s1 = side_stats(vardata1)?;
    let s2 = side_stats(vardata2)?;
    let mut selec = 0.0f64;

    let mut proc = opproc_for(operator)?;

    let hist2_weight = match (s2.hist, vardata2.rel) {
        (Some(_), Some(rel)) => {
            (1.0 - s2.nullfrac - s2.sumcommon) * run.root.rel(rel).rows
        }
        _ => 0.0,
    };

    if let Some((v1, n1)) = s1.mcv {
        if s2.mcv.is_some() || s2.hist.is_some() {
            for i in 0..s1.mcv_length as usize {
                selec += n1[i] as f64
                    * inet_semi_join_sel(
                        v1[i],
                        s2.mcv.map(|(v, _)| &v[..s2.mcv_length as usize]),
                        s2.hist,
                        hist2_weight,
                        &mut proc,
                        opr_codenum,
                    )?;
            }
        }
    }

    if let Some(h1) = s1.hist {
        if h1.len() > 2 && (s2.mcv.is_some() || s2.hist.is_some()) {
            let mut hist_selec_sum = 0.0f64;
            let k = (h1.len() as i32 - 3) / MAX_CONSIDERED_ELEMS + 1;
            let mut n = 0i32;
            let mut i = 1usize;
            while i < h1.len() - 1 {
                hist_selec_sum += inet_semi_join_sel(
                    h1[i],
                    s2.mcv.map(|(v, _)| &v[..s2.mcv_length as usize]),
                    s2.hist,
                    hist2_weight,
                    &mut proc,
                    opr_codenum,
                )?;
                n += 1;
                i += k as usize;
            }
            selec += (1.0 - s1.nullfrac - s1.sumcommon) * hist_selec_sum / n as f64;
        }
    }

    if (s1.mcv.is_none() && s1.hist.is_none()) || (s2.mcv.is_none() && s2.hist.is_none()) {
        selec = (1.0 - s1.nullfrac) * (1.0 - s2.nullfrac) * default_sel(operator);
    }
    Ok(selec)
}

fn mcv_population(mcv_numbers: &[f32]) -> f64 {
    mcv_numbers.iter().map(|&n| n as f64).sum()
}

fn inet_hist_value_sel(values: &[Datum], constvalue: Datum, opr_codenum: i32) -> f64 {
    let nvalues = values.len() as i32;
    if nvalues <= 1 {
        return 0.0;
    }
    let k = (nvalues - 2) / MAX_CONSIDERED_ELEMS + 1;

    let query = inet_ref(constvalue);
    let mut left = inet_ref(values[0]);
    let mut left_order = inet_inclusion_cmp(&left, &query, opr_codenum);

    let mut match_sum = 0.0f64;
    let mut n = 0i32;
    let mut i = k;
    while i < nvalues {
        let right = inet_ref(values[i as usize]);
        let right_order = inet_inclusion_cmp(&right, &query, opr_codenum);

        if left_order == 0 && right_order == 0 {
            match_sum += 1.0;
        } else if (left_order <= 0 && right_order >= 0)
            || (left_order >= 0 && right_order <= 0)
        {
            let left_divider = inet_hist_match_divider(&left, &query, opr_codenum);
            let right_divider = inet_hist_match_divider(&right, &query, opr_codenum);
            if left_divider >= 0 || right_divider >= 0 {
                match_sum += 1.0 / 2.0f64.powi(left_divider.max(right_divider));
            }
        }

        left = right;
        left_order = right_order;
        n += 1;
        i += k;
    }

    match_sum / n as f64
}

fn inet_mcv_join_sel(
    mcv1_values: &[Datum],
    mcv1_numbers: &[f32],
    mcv2_values: &[Datum],
    mcv2_numbers: &[f32],
    operator: Oid,
) -> PgResult<f64> {
    let mut selec = 0.0f64;
    let mut proc = opproc_for(operator)?;
    // Paired iteration keeps the helper total on torn inputs (values without
    // a frequency contribute nothing); identical to the C index loops when
    // the arrays agree.
    for (&v1, &n1) in mcv1_values.iter().zip(mcv1_numbers.iter()) {
        for (&v2, &n2) in mcv2_values.iter().zip(mcv2_numbers.iter()) {
            if types_fmgr::function_call2_coll(&mut proc, 0, v1, v2)?.as_bool() {
                selec += n1 as f64 * n2 as f64;
            }
        }
    }
    Ok(selec)
}

fn inet_mcv_hist_sel(
    mcv_values: &[Datum],
    mcv_numbers: &[f32],
    hist_values: &[Datum],
    opr_codenum: i32,
) -> f64 {
    let opr_codenum = -opr_codenum;
    let mut selec = 0.0f64;
    // Paired iteration: torn-slot rule, see inet_mcv_join_sel.
    for (&v, &n) in mcv_values.iter().zip(mcv_numbers.iter()) {
        selec += n as f64 * inet_hist_value_sel(hist_values, v, opr_codenum);
    }
    selec
}

fn inet_hist_inclusion_join_sel(
    hist1_values: &[Datum],
    hist2_values: &[Datum],
    opr_codenum: i32,
) -> f64 {
    let hist2_nvalues = hist2_values.len() as i32;
    if hist2_nvalues <= 2 {
        return 0.0;
    }
    let k = (hist2_nvalues - 3) / MAX_CONSIDERED_ELEMS + 1;
    let mut match_sum = 0.0f64;
    let mut n = 0i32;
    let mut i = 1i32;
    while i < hist2_nvalues - 1 {
        match_sum += inet_hist_value_sel(hist1_values, hist2_values[i as usize], opr_codenum);
        n += 1;
        i += k;
    }
    match_sum / n as f64
}

fn inet_semi_join_sel(
    lhs_value: Datum,
    mcv_values: Option<&[Datum]>,
    hist_values: Option<&[Datum]>,
    hist_weight: f64,
    proc: &mut types_fmgr::FmgrInfo,
    opr_codenum: i32,
) -> PgResult<f64> {
    if let Some(mcv) = mcv_values {
        for &v in mcv {
            if types_fmgr::function_call2_coll(proc, 0, lhs_value, v)?.as_bool() {
                return Ok(1.0);
            }
        }
    }
    if let Some(hist) = hist_values {
        if hist_weight > 0.0 {
            let hist_selec = inet_hist_value_sel(hist, lhs_value, -opr_codenum);
            if hist_selec > 0.0 {
                return Ok((hist_weight * hist_selec).min(1.0));
            }
        }
    }
    Ok(0.0)
}

fn inet_inclusion_cmp(left: &InetRef<'_>, right: &InetRef<'_>, opr_codenum: i32) -> i32 {
    if left.family == right.family {
        let order = bitncmp(left.addr, right.addr, left.bits.min(right.bits) as i32);
        if order != 0 {
            return order;
        }
        return inet_masklen_inclusion_cmp(left, right, opr_codenum);
    }
    left.family as i32 - right.family as i32
}

fn inet_masklen_inclusion_cmp(
    left: &InetRef<'_>,
    right: &InetRef<'_>,
    opr_codenum: i32,
) -> i32 {
    let order = left.bits as i32 - right.bits as i32;
    if (order > 0 && opr_codenum >= 0)
        || (order == 0 && (-1..=1).contains(&opr_codenum))
        || (order < 0 && opr_codenum <= 0)
    {
        return 0;
    }
    opr_codenum
}

fn inet_hist_match_divider(
    boundary: &InetRef<'_>,
    query: &InetRef<'_>,
    opr_codenum: i32,
) -> i32 {
    if boundary.family == query.family
        && inet_masklen_inclusion_cmp(boundary, query, opr_codenum) == 0
    {
        let min_bits = boundary.bits.min(query.bits) as i32;
        let decisive_bits = if opr_codenum < 0 {
            boundary.bits as i32
        } else if opr_codenum > 0 {
            query.bits as i32
        } else {
            min_bits
        };
        if min_bits > 0 {
            return decisive_bits - bitncommon(boundary.addr, query.addr, min_bits);
        }
        return decisive_bits;
    }
    -1
}

// GL-STATSLOT-1: a torn MCV slot (values from one pg_statistic row
// generation, numbers image from a later one, so the arrays disagree) must
// degrade softly. C reads a pinned tuple copy and can never see the
// mismatch; the lazy slot re-probe can.
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn side_stats_tolerates_torn_mcv_slot() {
        let cx = mcx::MemoryContext::new_bump("torn-mcv");
        let mcx = cx.mcx();
        let mut mcv_values = mcx::PgVec::new_in(mcx);
        mcv_values.extend([Datum::from_i32(1), Datum::from_i32(2)]);
        let mut slots = mcx::PgVec::new_in(mcx);
        slots.push(syscache_seams::PgStatisticSlotData::from_decoded(
            STATISTIC_KIND_MCV,
            96,
            0,
            23,
            mcv_values,
            mcx::PgVec::new_in(mcx),
            mcx::PgVec::new_in(mcx),
        ));
        let bundle = crate::selfuncs::leak_bundle(
            mcx,
            syscache_seams::PgStatisticBundle {
                stanullfrac: 0.0,
                stawidth: 4,
                stadistinct: 10.0,
                slots,
            },
        )
        .unwrap();
        let vardata = VariableStatData {
            var: None,
            rel: None,
            vartype: 869,
            isunique: false,
            stats: Some(bundle),
            acl_ok: true,
        };
        // Zero (value, frequency) pairs are considerable: the torn tail of
        // the values array has no frequencies to sum or join against.
        let s = side_stats(&vardata).expect("no panic on torn slot");
        assert_eq!(s.mcv_length, 0);
        assert_eq!(s.sumcommon, 0.0);
    }

    #[test]
    fn inet_mcv_join_sel_tolerates_torn_numbers() {
        // int4eq stands in for the inet op: the helper is operator-agnostic
        // and the torn shape is about array pairing, not address semantics.
        crate::tests::install_fixtures();
        let v1 = [Datum::from_i32(1), Datum::from_i32(2)];
        let n1: [f32; 0] = [];
        let v2 = [Datum::from_i32(1)];
        let n2: [f32; 0] = [];
        let selec = inet_mcv_join_sel(&v1, &n1, &v2, &n2, 96).expect("no panic on torn slot");
        assert_eq!(selec, 0.0);
    }
}
