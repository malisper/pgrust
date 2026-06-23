//! The text-search restriction-selectivity estimator of `ts_selfuncs.c` —
//! `tsmatchsel`, the `oprrest` of the `tsvector @@ tsquery` /
//! `tsquery @@ tsvector` match operators.
//!
//! In C this is a `PGFunction` reached via `OidFunctionCall4Coll(oprrest, ...)`;
//! the repo reaches it through the [`crate::dispatch`] `call_oprrest` seam. The
//! decode of the raw `fcinfo` words into the typed planner references happens at
//! that boundary, so the entry point below takes the already-decoded arguments.
//!
//! `ts_selfuncs.c` lives in the (otherwise-unported) `backend-tsearch` subtree;
//! this estimator is carved out here because selfuncs owns the dispatch and the
//! `VariableStatData` / `get_attstatsslot` MCELEM substrate it is built on
//! (matching how selfuncs hosts the cross-cycle `range`/`network`/`array` and
//! `like_support.c` pattern estimators). The MCELEM array decode reuses the
//! `tsquery` ABI codec of `backend-utils-adt-ts-small` (`tsq_size` / `get_query`
//! / `get_operand`, the `GETQUERY`/`GETOPERAND` of `ts_type.h`).

use alloc::vec::Vec;

use mcx::Mcx;
use types_core::primitive::{InvalidOid, Oid};
use types_error::PgResult;
use ::nodes::primnodes::Expr;
use pathnodes::planner_run::PlannerRun;
use pathnodes::{NodeId, PlannerInfo};
use types_selfuncs::{STATISTIC_KIND_MCELEM, VariableStatData};
use tsearch::tsearch::{QueryItem, OP_AND, OP_NOT, OP_OR, OP_PHRASE, QI_VAL};

use ts_small::util::{get_operand, get_query, tsq_size};

use crate::clamp_probability;
use crate::scalar::stats_tuple_stanullfrac;

/// `TSQUERYOID` (pg_type.dat) — the `tsquery` type.
const TSQUERYOID: Oid = 3615;
/// `TSVECTOROID` (pg_type.dat) — the `tsvector` type.
const TSVECTOROID: Oid = 3614;

/// `VARHDRSZ` (c.h) — the 4-byte varlena length header.
const VARHDRSZ: usize = 4;

/// `DEFAULT_TS_MATCH_SEL` (ts_selfuncs.c) — the default text-search selectivity,
/// chosen small enough to encourage indexscans for typical table densities.
const DEFAULT_TS_MATCH_SEL: f64 = 0.005;

/// `TextFreq` (ts_selfuncs.c) — one MCELEM lexeme and its frequency. The C
/// `text *element` is held here as the header-less lexeme bytes (the
/// `VARDATA_ANY`/`VARSIZE_ANY_EXHDR` content the comparator works on).
struct TextFreq<'a> {
    element: &'a [u8],
    frequency: f32,
}

/// `tsmatchsel(fcinfo)` (ts_selfuncs.c) — restriction selectivity of `@@`
/// (`tsvector @@ tsquery` and `tsquery @@ tsvector`). 1:1 with the C body.
pub fn tsmatchsel<'mcx>(
    mcx: Mcx<'mcx>,
    run: &PlannerRun<'mcx>,
    root: &mut PlannerInfo,
    _operator: Oid,
    args: &[NodeId],
    var_relid: i32,
) -> PgResult<f64> {
    // If expression is not variable = something or something = variable, then
    // punt and return a default estimate.
    let (vardata, other, _varonleft) =
        match crate::examine::get_restriction_variable(mcx, run, root, args, var_relid)? {
            Some(t) => t,
            None => return Ok(DEFAULT_TS_MATCH_SEL),
        };

    // Can't do anything useful if the something is not a constant, either.
    let cnst = match &other {
        Expr::Const(c) => c,
        _ => {
            crate::examine::release_variable_stats(vardata);
            return Ok(DEFAULT_TS_MATCH_SEL);
        }
    };

    // The "@@" operator is strict, so we can cope with NULL right away.
    if cnst.constisnull {
        crate::examine::release_variable_stats(vardata);
        return Ok(0.0);
    }

    // OK, there's a Var and a Const we're dealing with here. We need the Const to
    // be a TSQuery, else we can't do anything useful. We have to check this
    // because the Var might be the TSQuery not the TSVector.
    //
    // Also check that the Var really is a TSVector, in case this estimator is
    // mistakenly attached to some other operator.
    let selec = if cnst.consttype == TSQUERYOID && vardata.vartype == TSVECTOROID {
        // tsvector @@ tsquery or the other way around. The TSQuery const value
        // crosses as a by-reference varlena image (DatumGetTSQuery detoasts it;
        // the planner const here is already the flat in-line image).
        let query_image = const_tsquery_image(mcx, &cnst.constvalue)?;
        tsquerysel(mcx, &vardata, &query_image)?
    } else {
        // If we can't see the query structure, must punt.
        DEFAULT_TS_MATCH_SEL
    };

    crate::examine::release_variable_stats(vardata);

    let selec = clamp_probability(selec);
    Ok(selec)
}

/// `DatumGetTSQuery(constval)` — the detoasted flat `tsquery` varlena image (the
/// 4-byte length header + `int32 size` + `QueryItem` array + operand storage).
fn const_tsquery_image<'mcx>(
    mcx: Mcx<'mcx>,
    constvalue: &types_tuple::heaptuple::Datum<'_>,
) -> PgResult<Vec<u8>> {
    let raw = constvalue.as_ref_bytes();
    // PG_DETOAST_DATUM(constval): the const could in principle be a short-header
    // or compressed varlena; canonicalize to the 4-byte-header form the codec
    // (tsq_size at bytes [4..8], HDRSIZETQ=8) expects.
    let image = detoast_seams::detoast_attr::call(mcx, raw)?;
    Ok(image.to_vec())
}

/// `tsquerysel(vardata, constval)` (ts_selfuncs.c) — `@@` selectivity for a
/// tsvector var vs a tsquery constant. 1:1 with the C body.
fn tsquerysel<'mcx>(
    mcx: Mcx<'mcx>,
    vardata: &VariableStatData,
    query: &[u8],
) -> PgResult<f64> {
    // Empty query matches nothing.
    if tsq_size(query) == 0 {
        return Ok(0.0);
    }

    if let Some(stats_tuple) = vardata.stats_tuple {
        // MCELEM will be an array of TEXT elements for a tsvector column. The
        // by-reference (text) slot values are non-dereferenceable bare offsets,
        // so fetch their canonical value-carrying images (the same re-decode the
        // MCV/histogram paths use) to read each lexeme's bytes.
        let slot = lsyscache_seams::get_attstatsslot::call(
            mcx,
            stats_tuple,
            STATISTIC_KIND_MCELEM,
            InvalidOid,
            types_selfuncs::ATTSTATSSLOT_VALUES | types_selfuncs::ATTSTATSSLOT_NUMBERS,
        )?;

        let selec = if let Some(sslot) = slot {
            // There is a most-common-elements slot for the tsvector Var, so use
            // that. The values are text elements; fetch their canonical images.
            let canon = crate::ineq::slot_canon_values(
                mcx,
                stats_tuple,
                STATISTIC_KIND_MCELEM,
                InvalidOid,
                sslot.valuetype,
            )?;
            let element_bytes: Vec<&[u8]> = match canon.as_ref() {
                Some(c) => c.iter().map(|d| varsize_any_exhdr(d.as_ref_bytes())).collect(),
                // A pass-by-value element type can't occur for a tsvector
                // MCELEM (text is by-ref), but stay total: empty payloads.
                None => sslot.values.iter().map(|_| &[][..]).collect(),
            };
            mcelem_tsquery_selec(query, &element_bytes, &sslot.numbers)?
        } else {
            // No most-common-elements info, so do without.
            tsquery_opr_selec_no_stats(query)?
        };

        // MCE stats count only non-null rows, so adjust for null rows.
        let nullfrac = stats_tuple_stanullfrac(stats_tuple) as f64;
        Ok(selec * (1.0 - nullfrac))
    } else {
        // No stats at all, so do without. (We assume no nulls here, so no
        // stanullfrac correction.)
        tsquery_opr_selec_no_stats(query)
    }
}

/// `VARDATA_ANY(b)` / `VARSIZE_ANY_EXHDR` — the header-less payload of a
/// (detoasted, uncompressed 4-byte-header) varlena image, as the MCELEM text
/// values cross here.
#[inline]
fn varsize_any_exhdr(b: &[u8]) -> &[u8] {
    b.get(VARHDRSZ..).unwrap_or(&[])
}

/// `mcelem_tsquery_selec(query, mcelem, nmcelem, numbers, nnumbers)`
/// (ts_selfuncs.c) — extract data from the pg_statistic arrays into useful
/// format and evaluate the query against it. 1:1 with the C body.
fn mcelem_tsquery_selec(
    query: &[u8],
    mcelem: &[&[u8]],
    numbers: &[f32],
) -> PgResult<f64> {
    let nmcelem = mcelem.len();
    let nnumbers = numbers.len();

    // There should be two more Numbers than Values, because the last two cells
    // are taken for minimal and maximal frequency. Punt if not.
    //
    // (Note: the MCELEM statistics slot definition allows for a third extra
    // number containing the frequency of nulls, but we're not expecting that to
    // appear for a tsvector column.)
    if nnumbers != nmcelem + 2 {
        return tsquery_opr_selec_no_stats(query);
    }

    // Transpose the data into a single array so we can binary-search it.
    let lookup: Vec<TextFreq> = (0..nmcelem)
        .map(|i| TextFreq {
            element: mcelem[i],
            frequency: numbers[i],
        })
        .collect();

    // Grab the lowest frequency. compute_tsvector_stats() stored it for us in the
    // one before the last cell of the Numbers array. See ts_typanalyze.c.
    let minfreq = numbers[nnumbers - 2];

    let items = get_query(query)?;
    let operand = get_operand(query);
    tsquery_opr_selec(&items, 0, operand, Some(&lookup), minfreq)
}

/// `tsquery_opr_selec_no_stats(query)` (ts_selfuncs.c) — the
/// `tsquery_opr_selec(GETQUERY(query), GETOPERAND(query), NULL, 0, 0)` macro.
fn tsquery_opr_selec_no_stats(query: &[u8]) -> PgResult<f64> {
    let items = get_query(query)?;
    let operand = get_operand(query);
    tsquery_opr_selec(&items, 0, operand, None, 0.0)
}

/// `tsquery_opr_selec(item, operand, lookup, length, minfreq)` (ts_selfuncs.c) —
/// traverse the tsquery in preorder, computing selectivity. 1:1 with the C body.
///
/// The C `QueryItem *item` pointer arithmetic (`item + 1`, `item +
/// item->qoperator.left`) is expressed as an index into the flat polish-order
/// [`QueryItem`] array (`items`). `lookup == NULL` (no MCELEM stats) is `None`.
fn tsquery_opr_selec(
    items: &[QueryItem],
    idx: usize,
    operand: &[u8],
    lookup: Option<&[TextFreq<'_>]>,
    minfreq: f32,
) -> PgResult<f64> {
    // Since this function recurses, it could be driven to stack overflow.
    stack_depth::check_stack_depth()?;

    let length = lookup.map(|l| l.len()).unwrap_or(0);

    let item = &items[idx];

    let selec = if item.item_type() == QI_VAL {
        let oper = match item {
            QueryItem::Qoperand(o) => *o,
            _ => unreachable!("QI_VAL item is a QueryOperand"),
        };

        // Prepare the key for the search: key.lexeme = operand + oper->distance;
        // key.length = oper->length.
        let key_off = oper.distance() as usize;
        let key_len = oper.length() as usize;
        let key = &operand[key_off..key_off + key_len];

        if oper.prefix {
            // Prefix match, ie the query item is lexeme:*
            //
            // Our strategy is to scan through the MCELEM list and combine the
            // frequencies of the ones that match the prefix. We then extrapolate
            // the fraction of matching MCELEMs to the remaining rows, assuming
            // that the MCELEMs are representative of the whole lexeme population
            // in this respect. (Compare histogram_selectivity().) Note that these
            // are most common elements not most common values, so they're not
            // mutually exclusive. We treat occurrences as independent events.
            //
            // This is only a good plan if we have a pretty fair number of MCELEMs
            // available; we set the threshold at 100. If no stats or insufficient
            // stats, arbitrarily use DEFAULT_TS_MATCH_SEL*4.
            let lookup = match lookup {
                Some(l) if length >= 100 => l,
                _ => return Ok(DEFAULT_TS_MATCH_SEL * 4.0),
            };

            let mut matched = 0.0f64;
            let mut allmces = 0.0f64;
            let mut n_matched = 0usize;
            for t in lookup.iter() {
                let tlen = t.element.len();
                if tlen >= key_len && t.element.starts_with(key) {
                    matched += t.frequency as f64 - matched * t.frequency as f64;
                    n_matched += 1;
                }
                allmces += t.frequency as f64 - allmces * t.frequency as f64;
            }

            // Clamp to ensure sanity in the face of roundoff error.
            let matched = clamp_probability(matched);
            let allmces = clamp_probability(allmces);

            let mut selec = matched + (1.0 - allmces) * (n_matched as f64 / length as f64);

            // In any case, never believe that a prefix match has selectivity less
            // than we would assign for a non-MCELEM lexeme. This preserves the
            // property that "word:*" should be estimated to match at least as
            // many rows as "word" would be.
            selec = selec.max(DEFAULT_TS_MATCH_SEL.min(minfreq as f64 / 2.0));
            selec
        } else {
            // Regular exact lexeme match.
            //
            // If no stats for the variable, use DEFAULT_TS_MATCH_SEL.
            let lookup = match lookup {
                Some(l) => l,
                None => return Ok(DEFAULT_TS_MATCH_SEL),
            };

            // bsearch over the (length-then-byte) ordered MCELEM array.
            match bsearch_lexeme(key, lookup) {
                // The element is in MCELEM. Return precise selectivity (or at
                // least as precise as ANALYZE could find out).
                Some(i) => lookup[i].frequency as f64,
                // The element is not in MCELEM. Punt, but assume that the
                // selectivity cannot be more than minfreq / 2.
                None => DEFAULT_TS_MATCH_SEL.min(minfreq as f64 / 2.0),
            }
        }
    } else {
        // Current TSQuery node is an operator.
        let op = match item {
            QueryItem::Qoperator(o) => *o,
            _ => unreachable!("non-QI_VAL item is a QueryOperator"),
        };
        match op.oper {
            OP_NOT => {
                1.0 - tsquery_opr_selec(items, idx + 1, operand, lookup, minfreq)?
            }
            OP_PHRASE | OP_AND => {
                let s1 = tsquery_opr_selec(items, idx + 1, operand, lookup, minfreq)?;
                let s2 =
                    tsquery_opr_selec(items, idx + op.left as usize, operand, lookup, minfreq)?;
                s1 * s2
            }
            OP_OR => {
                let s1 = tsquery_opr_selec(items, idx + 1, operand, lookup, minfreq)?;
                let s2 =
                    tsquery_opr_selec(items, idx + op.left as usize, operand, lookup, minfreq)?;
                s1 + s2 - s1 * s2
            }
            other => {
                return Err(types_error::PgError::error(alloc::format!(
                    "unrecognized operator: {other}"
                )));
            }
        }
    };

    // Clamp intermediate results to stay sane despite roundoff error.
    Ok(clamp_probability(selec))
}

/// `bsearch(&key, lookup, length, sizeof(TextFreq), compare_lexeme_textfreq)`
/// (ts_selfuncs.c) — binary search the MCELEM array (sorted by length, then
/// byte-for-byte, the order ANALYZE stored). Returns the matching index.
fn bsearch_lexeme(key: &[u8], lookup: &[TextFreq<'_>]) -> Option<usize> {
    let mut lo = 0isize;
    let mut hi = lookup.len() as isize - 1;
    while lo <= hi {
        let mid = (lo + hi) / 2;
        match compare_lexeme_textfreq(key, lookup[mid as usize].element) {
            0 => return Some(mid as usize),
            c if c < 0 => hi = mid - 1,
            _ => lo = mid + 1,
        }
    }
    None
}

/// `compare_lexeme_textfreq(e1, e2)` (ts_selfuncs.c) — compare a search key
/// lexeme (non-NUL-terminated string with length) and a `TextFreq`. Use length,
/// then byte-for-byte comparison, matching how ANALYZE sorted the data. 1:1 with
/// the C body.
fn compare_lexeme_textfreq(key: &[u8], element: &[u8]) -> i32 {
    let len1 = key.len();
    let len2 = element.len();

    // Compare lengths first, possibly avoiding a memcmp call.
    if len1 > len2 {
        return 1;
    } else if len1 < len2 {
        return -1;
    }

    // Fall back on byte-for-byte comparison (strncmp(key, element, len1)).
    match key.cmp(element) {
        core::cmp::Ordering::Less => -1,
        core::cmp::Ordering::Equal => 0,
        core::cmp::Ordering::Greater => 1,
    }
}
