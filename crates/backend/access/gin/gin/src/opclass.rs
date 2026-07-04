//! Closed-set opclass dispatch (rule 4): support procs resolved to a
//! GinOpclass tag at initGinState, called directly here — no fmgr frames on
//! the compare/extract/consistent paths.

use ::datum::Datum;
use ::gin_vocab::*;
use ::mcx::{Mcx, PgVec};
use ::types_error::PgResult;
use ::types_scan::scankey::StrategyNumber;
use ::types_tuple::varatt;

pub(crate) const F_GIN_COMPARE_JSONB: ::types_core::Oid = 3480;
pub(crate) const F_GIN_EXTRACT_JSONB: ::types_core::Oid = 3482;
pub(crate) const F_GIN_EXTRACT_JSONB_PATH: ::types_core::Oid = 3485;
pub(crate) const F_BTINT4CMP: ::types_core::Oid = 351;

/// Detoasted varlena payload of a datum (header stripped). External and
/// compressed images take the detoast path; inline images are borrowed.
pub(crate) fn detoast_payload<'m>(mcx: Mcx<'m>, d: Datum) -> PgResult<&'m [u8]> {
    Ok(&detoast_image(mcx, d)?[4..])
}

/// Detoasted flat 4-byte-header image of a varlena datum.
pub(crate) fn detoast_image<'m>(mcx: Mcx<'m>, d: Datum) -> PgResult<&'m [u8]> {
    let p = d.as_usize() as *const u8;
    // SAFETY: non-null varlena datum, readable through its header.
    unsafe {
        if varatt::varatt_is_1b_e(p) || (!varatt::varatt_is_1b(p) && !varatt::varatt_is_4b_u(p)) {
            let raw = core::slice::from_raw_parts(p, varatt::varsize_any(p));
            let flat = detoast::detoast_attr(mcx, raw)?;
            debug_assert!(flat.len() >= 4);
            let out = core::slice::from_raw_parts(flat.as_ptr(), flat.len());
            core::mem::forget(flat);
            Ok(out)
        } else if varatt::varatt_is_1b(p) {
            // Short-header payloads are odd-aligned; jsonb wants its numeric
            // digits 2-aligned — copy to palloc alignment (C detoast_attr).
            let src = core::slice::from_raw_parts(
                p.add(varatt::VARHDRSZ_SHORT),
                varatt::varsize_1b(p) - varatt::VARHDRSZ_SHORT,
            );
            let total = 4 + src.len();
            let mut buf: ::mcx::PgVec<'m, u8> = mcx::vec_with_capacity_in(mcx, total)?;
            ::mcx::vec_append_bytes(
                &mut buf,
                &varatt::set_varsize_4b_word(total as u32).to_ne_bytes(),
            )?;
            ::mcx::vec_append_bytes(&mut buf, src)?;
            let out = core::slice::from_raw_parts(buf.as_ptr(), buf.len());
            core::mem::forget(buf);
            Ok(out)
        } else {
            Ok(core::slice::from_raw_parts(p, varatt::varsize_4b(p)))
        }
    }
}

#[inline]
fn text_payload<'x>(d: Datum) -> &'x [u8] {
    let p = d.as_usize() as *const u8;
    // SAFETY: gin keys are inline text images built by make_text_key or
    // deformed (possibly short) index-tuple values; pin/scratch keeps them
    // live for the compare.
    unsafe {
        if varatt::varatt_is_1b(p) {
            core::slice::from_raw_parts(
                p.add(varatt::VARHDRSZ_SHORT),
                varatt::varsize_1b(p) - varatt::VARHDRSZ_SHORT,
            )
        } else {
            debug_assert!(varatt::varatt_is_4b_u(p));
            core::slice::from_raw_parts(p.add(4), varatt::varsize_4b(p) - 4)
        }
    }
}

/// compareFn: total order on two non-null key datums.
pub(crate) fn compare(state: &GinState, a: Datum, b: Datum) -> i32 {
    match state.opclass {
        GinOpclass::JsonbOps => {
            ::adt_jsonb::gin::gin_compare_jsonb(text_payload(a), text_payload(b))
        }
        GinOpclass::JsonbPathOps => {
            // btint4cmp over uint32 path hashes stored via UInt32GetDatum.
            let (x, y) = (a.as_usize() as u32 as i32, b.as_usize() as u32 as i32);
            if x < y {
                -1
            } else {
                (x > y) as i32
            }
        }
    }
}

/// extractValueFn.
pub(crate) fn extract_value<'m>(
    mcx: Mcx<'m>,
    state: &GinState,
    value: Datum,
) -> PgResult<PgVec<'m, Datum>> {
    match state.opclass {
        GinOpclass::JsonbOps => {
            let payload = detoast_payload(mcx, value)?;
            ::adt_jsonb::gin::gin_extract_jsonb(mcx, payload)
        }
        GinOpclass::JsonbPathOps => {
            let payload = detoast_payload(mcx, value)?;
            ::adt_jsonb::gin::gin_extract_jsonb_path(mcx, payload)
        }
    }
}

/// extractQueryFn: returns (query key datums, searchMode, jsonpath ops). The
/// closed set yields no null flags and no partial matches; C's extra_data is
/// the flattened jsonpath ops.
pub(crate) fn extract_query<'m>(
    mcx: Mcx<'m>,
    state: &GinState,
    query: Datum,
    strategy: StrategyNumber,
) -> PgResult<(PgVec<'m, Datum>, i32, PgVec<'m, JspGinOp>)> {
    let image = detoast_image(mcx, query)?;
    match state.opclass {
        GinOpclass::JsonbOps => ::adt_jsonb::gin::gin_extract_jsonb_query(mcx, image, strategy),
        GinOpclass::JsonbPathOps => {
            ::adt_jsonb::gin::gin_extract_jsonb_query_path(mcx, image, strategy)
        }
    }
}

/// consistentFn (binary).
pub(crate) fn consistent(
    state: &GinState,
    check: &[GinTernaryValue],
    strategy: StrategyNumber,
    _query: Datum,
    nkeys: usize,
    _query_values: &[Datum],
    _query_categories: &[GinNullCategory],
    jsp_ops: &[JspGinOp],
    recheck: &mut bool,
) -> bool {
    match state.opclass {
        GinOpclass::JsonbOps => {
            ::adt_jsonb::gin::gin_consistent_jsonb(check, strategy, nkeys, recheck, jsp_ops)
        }
        GinOpclass::JsonbPathOps => {
            ::adt_jsonb::gin::gin_consistent_jsonb_path(check, strategy, nkeys, recheck, jsp_ops)
        }
    }
}

/// triConsistentFn.
pub(crate) fn tri_consistent(
    state: &GinState,
    check: &[GinTernaryValue],
    strategy: StrategyNumber,
    _query: Datum,
    nkeys: usize,
    _query_values: &[Datum],
    _query_categories: &[GinNullCategory],
    jsp_ops: &[JspGinOp],
) -> GinTernaryValue {
    match state.opclass {
        GinOpclass::JsonbOps => {
            ::adt_jsonb::gin::gin_triconsistent_jsonb(check, strategy, nkeys, jsp_ops)
        }
        GinOpclass::JsonbPathOps => {
            ::adt_jsonb::gin::gin_triconsistent_jsonb_path(check, strategy, nkeys, jsp_ops)
        }
    }
}

/// gincost_pattern's extractQuery probe (selfuncs.c gincostestimate):
/// resolves the opclass from the opfamily and runs extractQueryFn, returning
/// (nentries, searchMode). The closed set has no partial matches.
pub fn gincost_extract_query(
    opfamily: ::types_core::Oid,
    opcintype: ::types_core::Oid,
    query: Datum,
    strategy: StrategyNumber,
) -> PgResult<(i32, i32)> {
    let extract = lsyscache::get_opfamily_proc(
        opfamily,
        opcintype,
        opcintype,
        GIN_EXTRACTQUERY_PROC as i16,
    )?;
    if extract == ::types_core::InvalidOid {
        crate::unported("missing GIN extractQuery support proc (gincostestimate)");
    }
    let state = GinState {
        opclass: match extract {
            3483 => GinOpclass::JsonbOps,
            3486 => GinOpclass::JsonbPathOps,
            other => crate::unported(&format!("GIN opclass with extractQuery proc {other}")),
        },
        support_collation: ::types_core::catalog::DEFAULT_COLLATION_OID,
        can_partial_match: false,
        key_byval: false,
        key_len: -1,
    };
    let scratch = ::mcx::MemoryContext::new_bump("gincost extract scratch");
    let (entries, mode, _ops) = extract_query(scratch.mcx(), &state, query, strategy)?;
    Ok((entries.len() as i32, mode))
}
