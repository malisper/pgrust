//! ts_selfuncs.c: tsmatchsel/tsmatchjoinsel — MCELEM-based tsquery selectivity.

use adt_tsvector_core::query::{Item, TsQueryRef, OP_AND, OP_NOT, OP_OR, OP_PHRASE};
use datum::Datum;
use types_error::{PgError, PgResult};
use types_pathnodes::NodeId;

use crate::run::PlannerRun;
use crate::selfuncs::{clamp_probability, get_restriction_variable, VariableStatData};

pub(crate) const DEFAULT_TS_MATCH_SEL: f64 = 0.005;
const STATISTIC_KIND_MCELEM: i16 = 4;
const TSVECTOROID: u32 = 3614;
const TSQUERYOID: u32 = 3615;

pub fn tsmatchsel<'mcx>(
    run: &mut PlannerRun<'mcx>,
    args: &[NodeId],
    varrelid: i32,
) -> PgResult<f64> {
    let Some((vardata, other, _varonleft)) = get_restriction_variable(run, args, varrelid)?
    else {
        return Ok(DEFAULT_TS_MATCH_SEL);
    };
    let Some(c) = other.as_const() else {
        return Ok(DEFAULT_TS_MATCH_SEL);
    };
    if c.constisnull {
        return Ok(0.0);
    }
    let selec = if c.consttype == TSQUERYOID && vardata.vartype == TSVECTOROID {
        tsquerysel(run.mcx, &vardata, c.constvalue)?
    } else {
        DEFAULT_TS_MATCH_SEL
    };
    Ok(clamp_probability(selec))
}

fn tsquerysel(mcx: mcx::Mcx<'_>, vardata: &VariableStatData<'_>, constval: Datum) -> PgResult<f64> {
    let img = crate::selfuncs::varlena_image_any(mcx, constval)?;
    let query = TsQueryRef { payload: &img[datum::VARHDRSZ..] };
    if query.size() == 0 {
        return Ok(0.0);
    }
    match &vardata.stats {
        Some(stats) => {
            // The MCELEM stavalues for a tsvector column are TEXT lexemes, and the
            // walk below reads each datum as a text varlena (text_exhdr ->
            // varlena_image dereferences the datum as a pointer and slices a
            // header-derived length). decode_pg_statistic_values honours the
            // element type embedded in the stored stavalues array, not the column
            // type, so a crafted pg_statistic row can declare a by-value elemtype
            // (float8[] -> datum word is an arbitrary address) or a fixed-length
            // by-ref elemtype (uuid[]/name[] -> datum bytes read as a bogus varlena
            // header), turning the decoded Datums into type-confused / wild-pointer
            // reads. Gate on the slot's element type being TEXT before touching the
            // datums, else fall back to the no-stats estimate.
            let selec = match vardata.slot(STATISTIC_KIND_MCELEM, 0) {
                Some(slot) if mcelem_valuetype_ok(slot.valuetype()?) => {
                    mcelem_tsquery_selec(query, slot.values()?, slot.numbers()?)?
                }
                _ => tsquery_opr_selec(query, 0, None, 0.0)?,
            };
            Ok(selec * (1.0 - stats.stanullfrac as f64))
        }
        None => tsquery_opr_selec(query, 0, None, 0.0),
    }
}

/// Gate for interpreting a tsvector MCELEM slot's stavalues datums. The array
/// image's self-declared element type (decode_pg_statistic_values honours the
/// elemtype embedded in the stored array, not the column type) must be TEXT —
/// the type the MCELEM walk treats each datum as. On mismatch the caller falls
/// back to the no-stats estimate rather than dereferencing type-confused Datums.
/// Mirrors rangetypes_selfuncs::hist_elemtype_matches.
#[inline]
fn mcelem_valuetype_ok(valuetype: types_core::Oid) -> bool {
    valuetype == types_core::TEXTOID
}

fn mcelem_tsquery_selec(
    query: TsQueryRef<'_>,
    values: &[Datum],
    numbers: &[f32],
) -> PgResult<f64> {
    // Two extra Numbers cells carry the min and max frequency.
    if numbers.len() != values.len() + 2 {
        return tsquery_opr_selec(query, 0, None, 0.0);
    }
    let minfreq = numbers[numbers.len() - 2];
    tsquery_opr_selec(query, 0, Some((values, numbers)), minfreq)
}

fn text_exhdr<'a>(d: Datum) -> &'a [u8] {
    &crate::rangetypes_selfuncs::varlena_image(d)[datum::VARHDRSZ..]
}

fn tsquery_opr_selec(
    q: TsQueryRef<'_>,
    i: usize,
    lookup: Option<(&[Datum], &[f32])>,
    minfreq: f32,
) -> PgResult<f64> {
    stack_depth::check_stack_depth()?;

    let selec = match q.item(i) {
        Item::Val(oper) => {
            let key = q.operand_str(&oper);
            if oper.prefix {
                // Combine matching-prefix MCELEM frequencies and extrapolate
                // the matched fraction to non-MCELEM rows; needs >=100 MCELEMs.
                let usable = lookup.filter(|(values, _)| values.len() >= 100);
                let Some((values, numbers)) = usable else {
                    return Ok(DEFAULT_TS_MATCH_SEL * 4.0);
                };
                let length = values.len();
                let mut matched = 0.0f64;
                let mut allmces = 0.0f64;
                let mut n_matched = 0i32;
                for (idx, &v) in values.iter().enumerate() {
                    let t = text_exhdr(v);
                    let f = numbers[idx] as f64;
                    if t.len() >= key.len() && &t[..key.len()] == key {
                        matched += f - matched * f;
                        n_matched += 1;
                    }
                    allmces += f - allmces * f;
                }
                let matched = clamp_probability(matched);
                let allmces = clamp_probability(allmces);
                let selec = matched + (1.0 - allmces) * (n_matched as f64 / length as f64);
                // "word:*" must estimate at least as high as "word".
                f64::max(f64::min(DEFAULT_TS_MATCH_SEL, minfreq as f64 / 2.0), selec)
            } else {
                let Some((values, numbers)) = lookup else {
                    return Ok(DEFAULT_TS_MATCH_SEL);
                };
                // MCELEM is sorted by length then bytes (ts_typanalyze.c).
                match values.binary_search_by(|&v| {
                    let t = text_exhdr(v);
                    t.len().cmp(&key.len()).then_with(|| t.cmp(key))
                }) {
                    Ok(idx) => numbers[idx] as f64,
                    Err(_) => f64::min(DEFAULT_TS_MATCH_SEL, minfreq as f64 / 2.0),
                }
            }
        }
        Item::Opr(opr) => match opr.oper {
            OP_NOT => 1.0 - tsquery_opr_selec(q, i + 1, lookup, minfreq)?,
            OP_PHRASE | OP_AND => {
                let s1 = tsquery_opr_selec(q, i + 1, lookup, minfreq)?;
                let s2 = tsquery_opr_selec(q, i + opr.left as usize, lookup, minfreq)?;
                s1 * s2
            }
            OP_OR => {
                let s1 = tsquery_opr_selec(q, i + 1, lookup, minfreq)?;
                let s2 = tsquery_opr_selec(q, i + opr.left as usize, lookup, minfreq)?;
                s1 + s2 - s1 * s2
            }
            other => {
                return Err(PgError::error(format!("unrecognized operator: {other}")).into())
            }
        },
        Item::ValStop => panic!("tsquery_opr_selec: QI_VALSTOP in stored tsquery"),
    };

    Ok(clamp_probability(selec))
}

#[cfg(test)]
mod tests {
    use super::*;

    // A crafted MCELEM slot can declare any stavalues element type; only a TEXT
    // slot may be walked as text lexemes. Anything else (by-value float8, by-ref
    // uuid/name, etc.) must be rejected so the MCELEM datums are never
    // dereferenced as text varlenas (type confusion / wild pointer read).
    #[test]
    fn mcelem_valuetype_gate_accepts_only_text() {
        const TEXTOID: types_core::Oid = 25;
        const FLOAT8OID: types_core::Oid = 701;
        const UUIDOID: types_core::Oid = 2950;
        const NAMEOID: types_core::Oid = 19;

        assert!(mcelem_valuetype_ok(TEXTOID));
        assert!(!mcelem_valuetype_ok(FLOAT8OID));
        assert!(!mcelem_valuetype_ok(UUIDOID));
        assert!(!mcelem_valuetype_ok(NAMEOID));
        assert!(!mcelem_valuetype_ok(0));
    }
}
