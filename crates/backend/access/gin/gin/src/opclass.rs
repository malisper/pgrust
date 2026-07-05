//! Closed-set opclass dispatch (rule 4): support procs resolved to a
//! GinOpclass tag at initGinState (jsonb_ops / jsonb_path_ops /
//! tsvector_ops), called directly here — no fmgr frames on the
//! compare/extract/consistent paths.

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
pub(crate) const F_GIN_EXTRACT_TSVECTOR: ::types_core::Oid = 3656;
pub(crate) const F_GIN_EXTRACT_TSQUERY: ::types_core::Oid = 3657;
pub(crate) const F_GIN_CMP_TSLEXEME: ::types_core::Oid = 3724;
pub(crate) const F_GIN_CMP_PREFIX: ::types_core::Oid = 2700;

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
        GinOpclass::TsvectorOps => {
            ::adt_tsginidx::gin_cmp_tslexeme(text_payload(a), text_payload(b))
        }
    }
}

/// comparePartialFn (tsvector_ops gin_cmp_prefix; only partial-match opclass).
pub(crate) fn compare_partial(
    state: &GinState,
    partial_key: Datum,
    key: Datum,
    _strategy: StrategyNumber,
) -> i32 {
    match state.opclass {
        GinOpclass::TsvectorOps => {
            ::adt_tsginidx::gin_cmp_prefix(text_payload(partial_key), text_payload(key))
        }
        _ => unreachable!("comparePartialFn on a non-partial-match opclass"),
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
        GinOpclass::TsvectorOps => {
            let payload = detoast_payload(mcx, value)?;
            ::adt_tsginidx::gin_extract_tsvector(
                mcx,
                ::adt_tsvector_core::layout::TsVec { payload },
            )
        }
    }
}

/// extractQueryFn outputs; C's per-opclass out-params and extra_data.
pub struct ExtractedQuery<'m> {
    pub entries: PgVec<'m, Datum>,
    pub search_mode: i32,
    pub jsp_ops: PgVec<'m, JspGinOp>,
    pub partial_match: PgVec<'m, bool>,
    pub map_item_operand: PgVec<'m, i32>,
}

pub(crate) fn extract_query<'m>(
    mcx: Mcx<'m>,
    state: &GinState,
    query: Datum,
    strategy: StrategyNumber,
) -> PgResult<ExtractedQuery<'m>> {
    let image = detoast_image(mcx, query)?;
    match state.opclass {
        GinOpclass::JsonbOps | GinOpclass::JsonbPathOps => {
            let (entries, search_mode, jsp_ops) = match state.opclass {
                GinOpclass::JsonbOps => {
                    ::adt_jsonb::gin::gin_extract_jsonb_query(mcx, image, strategy)?
                }
                _ => ::adt_jsonb::gin::gin_extract_jsonb_query_path(mcx, image, strategy)?,
            };
            Ok(ExtractedQuery {
                entries,
                search_mode,
                jsp_ops,
                partial_match: mcx::vec_new_in(mcx),
                map_item_operand: mcx::vec_new_in(mcx),
            })
        }
        GinOpclass::TsvectorOps => {
            let q = ::adt_tsvector_core::query::TsQueryRef { payload: &image[4..] };
            let out = ::adt_tsginidx::gin_extract_tsquery(mcx, q)?;
            Ok(ExtractedQuery {
                entries: out.entries,
                search_mode: out.search_mode,
                jsp_ops: mcx::vec_new_in(mcx),
                partial_match: out.partial_match,
                map_item_operand: out.map_item_operand,
            })
        }
    }
}

/// consistentFn (binary). `mcx` is the reset-per-call scratch (C tempCtx).
pub(crate) fn consistent(
    mcx: Mcx<'_>,
    state: &GinState,
    check: &[GinTernaryValue],
    strategy: StrategyNumber,
    query: Datum,
    nkeys: usize,
    _query_values: &[Datum],
    _query_categories: &[GinNullCategory],
    jsp_ops: &[JspGinOp],
    map_item_operand: &[i32],
    recheck: &mut bool,
) -> PgResult<bool> {
    match state.opclass {
        GinOpclass::JsonbOps => Ok(::adt_jsonb::gin::gin_consistent_jsonb(
            check, strategy, nkeys, recheck, jsp_ops,
        )),
        GinOpclass::JsonbPathOps => Ok(::adt_jsonb::gin::gin_consistent_jsonb_path(
            check, strategy, nkeys, recheck, jsp_ops,
        )),
        GinOpclass::TsvectorOps => {
            let image = detoast_image(mcx, query)?;
            let q = ::adt_tsvector_core::query::TsQueryRef { payload: &image[4..] };
            let (res, rc) = ::adt_tsginidx::gin_tsquery_consistent(mcx, check, q, map_item_operand)?;
            *recheck = rc;
            Ok(res)
        }
    }
}

/// triConsistentFn. `mcx` is the reset-per-call scratch (C tempCtx).
pub(crate) fn tri_consistent(
    mcx: Mcx<'_>,
    state: &GinState,
    check: &[GinTernaryValue],
    strategy: StrategyNumber,
    query: Datum,
    nkeys: usize,
    _query_values: &[Datum],
    _query_categories: &[GinNullCategory],
    jsp_ops: &[JspGinOp],
    map_item_operand: &[i32],
) -> PgResult<GinTernaryValue> {
    match state.opclass {
        GinOpclass::JsonbOps => Ok(::adt_jsonb::gin::gin_triconsistent_jsonb(
            check, strategy, nkeys, jsp_ops,
        )),
        GinOpclass::JsonbPathOps => Ok(::adt_jsonb::gin::gin_triconsistent_jsonb_path(
            check, strategy, nkeys, jsp_ops,
        )),
        GinOpclass::TsvectorOps => {
            let image = detoast_image(mcx, query)?;
            let q = ::adt_tsvector_core::query::TsQueryRef { payload: &image[4..] };
            ::adt_tsginidx::gin_tsquery_triconsistent(mcx, check, q, map_item_operand)
        }
    }
}

/// gincost_pattern's extractQuery probe (selfuncs.c gincostestimate):
/// resolves the opclass from the opfamily and runs extractQueryFn, returning
/// (nentries, npartial, searchMode).
pub fn gincost_extract_query(
    opfamily: ::types_core::Oid,
    opcintype: ::types_core::Oid,
    query: Datum,
    strategy: StrategyNumber,
) -> PgResult<(i32, i32, i32)> {
    let extract = lsyscache::get_opfamily_proc(
        opfamily,
        opcintype,
        opcintype,
        GIN_EXTRACTQUERY_PROC as i16,
    )?;
    if extract == ::types_core::InvalidOid {
        crate::unported("missing GIN extractQuery support proc (gincostestimate)");
    }
    let (opclass, can_partial) = match extract {
        3483 => (GinOpclass::JsonbOps, false),
        3486 => (GinOpclass::JsonbPathOps, false),
        F_GIN_EXTRACT_TSQUERY => (GinOpclass::TsvectorOps, true),
        other => crate::unported(&format!("GIN opclass with extractQuery proc {other}")),
    };
    let state = GinState {
        opclass,
        support_collation: ::types_core::catalog::DEFAULT_COLLATION_OID,
        can_partial_match: can_partial,
        key_byval: false,
        key_len: -1,
    };
    let scratch = ::mcx::MemoryContext::new_bump("gincost extract scratch");
    let out = extract_query(scratch.mcx(), &state, query, strategy)?;
    let npartial = out.partial_match.iter().filter(|&&p| p).count() as i32;
    Ok((out.entries.len() as i32, npartial, out.search_mode))
}
