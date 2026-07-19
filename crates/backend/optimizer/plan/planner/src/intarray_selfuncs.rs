//! contrib/intarray/_int_selfuncs.c: _int_matchsel — MCELEM-based restriction
//! selectivity for int4[] @@ query_int. The _int_*_sel/_joinsel wrappers land
//! in plancat.rs's dynamic-oprrest dispatch onto arraycontsel/arraycontjoinsel
//! with the built-in operator OID substituted, exactly as the C wrappers do.

use datum::Datum;
use types_core::Oid;
use types_error::{PgError, PgResult};
use types_pathnodes::NodeId;

use crate::run::PlannerRun;
use crate::selfuncs::{clamp_probability, get_restriction_variable, DEFAULT_EQ_SEL};

const INT4ARRAYOID: Oid = 1007;
const STATISTIC_KIND_MCELEM: i16 = 4;

// QUERYTYPE payload (contrib/intarray/_int.h): i32 size then ITEMs of
// (i16 type, i16 left, i32 val); type 2 = VAL, 3 = OPR.
const ITEM_VAL: i16 = 2;
const ITEM_OPR: i16 = 3;

struct QItem {
    typ: i16,
    left: i16,
    val: i32,
}

fn parse_items(image: &[u8]) -> Vec<QItem> {
    if image.len() < 8 {
        return Vec::new();
    }
    let size = (i32::from_ne_bytes(image[4..8].try_into().unwrap()).max(0) as usize)
        .min((image.len() - 8) / 8);
    let mut items = Vec::with_capacity(size);
    for i in 0..size {
        let off = 8 + i * 8;
        items.push(QItem {
            typ: i16::from_ne_bytes(image[off..off + 2].try_into().unwrap()),
            left: i16::from_ne_bytes(image[off + 2..off + 4].try_into().unwrap()),
            val: i32::from_ne_bytes(image[off + 4..off + 8].try_into().unwrap()),
        });
    }
    items
}

pub(crate) fn int_matchsel<'mcx>(
    run: &mut PlannerRun<'mcx>,
    args: &[NodeId],
    varrelid: i32,
    estimator_procid: Oid,
) -> PgResult<f64> {
    let Some((vardata, other, _varonleft)) = get_restriction_variable(run, args, varrelid)?
    else {
        return Ok(DEFAULT_EQ_SEL);
    };
    // Variable must be int[]; query_int-side variables are unsupported.
    if vardata.vartype != INT4ARRAYOID {
        return Ok(DEFAULT_EQ_SEL);
    }
    let Some(c) = other.as_const() else {
        return Ok(DEFAULT_EQ_SEL);
    };
    // "@@" is strict.
    if c.constisnull {
        return Ok(0.0);
    }
    // get_function_sibling_type: consttype must be a type named query_int
    // belonging to the estimator proc's own extension (uncached pg_depend
    // walk; C caches per (funcoid, typname) in a syscache-invalidated list).
    match syscache_seams::pg_type_name_namespace::call(c.consttype)? {
        Some((name, _)) if name.name_str() == b"query_int" => {}
        _ => return Ok(DEFAULT_EQ_SEL),
    }
    {
        const PROCEDURE_RELATION_ID: Oid = 1255;
        const TYPE_RELATION_ID: Oid = 1247;
        let cx = ::mcx::MemoryContext::new("intarray matchsel sibling probe");
        let fext =
            pg_depend::getExtensionOfObject(cx.mcx(), PROCEDURE_RELATION_ID, estimator_procid)?;
        if fext == 0
            || pg_depend::getExtensionOfObject(cx.mcx(), TYPE_RELATION_ID, c.consttype)? != fext
        {
            return Ok(DEFAULT_EQ_SEL);
        }
    }

    let image = crate::selfuncs::varlena_image_any(run.mcx, c.constvalue)?;
    let items = parse_items(image);
    // Empty query matches nothing.
    if items.is_empty() {
        return Ok(0.0);
    }

    let mut nullfrac = 0.0f32;
    let mut lookup: Option<(&[Datum], &[f32], f32)> = None;
    if let Some(stats) = &vardata.stats {
        nullfrac = stats.stanullfrac;
        if let Some(slot) = vardata.slot(STATISTIC_KIND_MCELEM, 0) {
            let values = slot.values()?;
            let numbers = slot.numbers()?;
            // Three extra Numbers cells carry min, max, and null-element
            // frequency; punt on any other shape.
            if numbers.len() == values.len() + 3 {
                lookup = Some((values, numbers, numbers[values.len()]));
            }
        }
    }

    let selec = int_query_opr_selec(&items, items.len() as i32 - 1, lookup)?;
    // MCE stats count only non-null rows.
    Ok(clamp_probability(selec * (1.0 - nullfrac as f64)))
}

fn int_query_opr_selec(
    items: &[QItem],
    pos: i32,
    lookup: Option<(&[Datum], &[f32], f32)>,
) -> PgResult<f64> {
    stack_depth::check_stack_depth()?;
    let item = &items[pos as usize];

    let selec = if item.typ == ITEM_VAL {
        let Some((mcelems, mcefreqs, minfreq)) = lookup else {
            return Ok(DEFAULT_EQ_SEL);
        };
        match mcelems.binary_search_by(|d| d.as_i32().cmp(&item.val)) {
            Ok(i) => mcefreqs[i] as f64,
            // Not in MCELEM: at most minfreq / 2.
            Err(_) => f64::min(DEFAULT_EQ_SEL, (minfreq / 2.0) as f64),
        }
    } else if item.typ == ITEM_OPR {
        let s1 = int_query_opr_selec(items, pos - 1, lookup)?;
        match item.val {
            v if v == '!' as i32 => 1.0 - s1,
            v if v == '&' as i32 => {
                s1 * int_query_opr_selec(items, pos + item.left as i32, lookup)?
            }
            v if v == '|' as i32 => {
                let s2 = int_query_opr_selec(items, pos + item.left as i32, lookup)?;
                s1 + s2 - s1 * s2
            }
            other => {
                return Err(PgError::error(format!("unrecognized operator: {other}")).into())
            }
        }
    } else {
        return Err(PgError::error(format!(
            "unrecognized int query item type: {}",
            item.typ as u16
        ))
        .into());
    };

    // Clamp intermediate results to stay sane despite roundoff error.
    Ok(clamp_probability(selec))
}
