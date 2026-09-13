//! Known-set support-proc dispatch (rule 4): each GinColState slot is
//! resolved at initGinState to the body fmgr would dispatch to and called
//! directly here — no fmgr frames on the compare/extract/consistent paths.
//! The compare slot alone keeps an fmgr arm (its procs take no `internal`
//! argument, so a SQL / PL comparator is loadable), exactly C's mechanism.

use ::datum::Datum;
use ::gin_vocab::*;
use ::mcx::{Mcx, PgVec};
use ::types_error::{PgError, PgResult};
use ::types_scan::scankey::StrategyNumber;
use ::types_tuple::varatt;

// pg_proc.dat oids of the core GIN support procs (proname/prosrc).
pub(crate) const F_BTINT2CMP: ::types_core::Oid = 350;
pub(crate) const F_BTINT4CMP: ::types_core::Oid = 351;
pub(crate) const F_BTINT8CMP: ::types_core::Oid = 842;
pub(crate) const F_BTOIDCMP: ::types_core::Oid = 356;
pub(crate) const F_BTTEXTCMP: ::types_core::Oid = 360;
pub(crate) const F_GIN_COMPARE_JSONB: ::types_core::Oid = 3480;
pub(crate) const F_GIN_EXTRACT_JSONB: ::types_core::Oid = 3482;
pub(crate) const F_GIN_EXTRACT_JSONB_QUERY: ::types_core::Oid = 3483;
pub(crate) const F_GIN_CONSISTENT_JSONB: ::types_core::Oid = 3484;
pub(crate) const F_GIN_EXTRACT_JSONB_PATH: ::types_core::Oid = 3485;
pub(crate) const F_GIN_EXTRACT_JSONB_QUERY_PATH: ::types_core::Oid = 3486;
pub(crate) const F_GIN_CONSISTENT_JSONB_PATH: ::types_core::Oid = 3487;
pub(crate) const F_GIN_TRICONSISTENT_JSONB: ::types_core::Oid = 3488;
pub(crate) const F_GIN_TRICONSISTENT_JSONB_PATH: ::types_core::Oid = 3489;
pub(crate) const F_GIN_EXTRACT_TSVECTOR: ::types_core::Oid = 3656;
pub(crate) const F_GIN_EXTRACT_TSQUERY: ::types_core::Oid = 3657;
pub(crate) const F_GIN_TSQUERY_CONSISTENT: ::types_core::Oid = 3658;
pub(crate) const F_GIN_TSQUERY_TRICONSISTENT: ::types_core::Oid = 3921;
pub(crate) const F_GIN_CMP_TSLEXEME: ::types_core::Oid = 3724;
pub(crate) const F_GIN_CMP_PREFIX: ::types_core::Oid = 2700;
pub(crate) const F_GINARRAYEXTRACT: ::types_core::Oid = 2743;
pub(crate) const F_GINARRAYCONSISTENT: ::types_core::Oid = 2744;
pub(crate) const F_GINQUERYARRAYEXTRACT: ::types_core::Oid = 2774;
pub(crate) const F_GINARRAYTRICONSISTENT: ::types_core::Oid = 3920;
// Pre-9.1 signature compatibility rows, forwarding to the current bodies
// (ginarrayproc.c:68 ginarrayextract_2args; tsginidx.c:304-353).
pub(crate) const F_GINARRAYEXTRACT_2ARGS: ::types_core::Oid = 3076;
pub(crate) const F_GIN_EXTRACT_TSVECTOR_2ARGS: ::types_core::Oid = 3077;
pub(crate) const F_GIN_EXTRACT_TSQUERY_5ARGS: ::types_core::Oid = 3087;
pub(crate) const F_GIN_TSQUERY_CONSISTENT_6ARGS: ::types_core::Oid = 3088;
pub(crate) const F_GIN_EXTRACT_TSQUERY_OLDSIG: ::types_core::Oid = 3791;
pub(crate) const F_GIN_TSQUERY_CONSISTENT_OLDSIG: ::types_core::Oid = 3792;

/// contrib/btree_gin's FUNCTION 1 rows are the storage types' core btree
/// comparators (btree_gin--1.0.sql and later; numeric / enum register the
/// module's own gin_numeric_cmp / gin_enum_cmp instead, resolved by
/// symbol). The tag selects gin_btree_seams::btree_compare's arm for that
/// type; the timestamptz / cidr / varbit rows share the collapsed tags.
pub(crate) fn btree_type_of_core_cmp(cmp_proc: ::types_core::Oid) -> Option<GinBtreeType> {
    Some(match cmp_proc {
        354 => GinBtreeType::Float4,      // btfloat4cmp
        355 => GinBtreeType::Float8,      // btfloat8cmp
        377 => GinBtreeType::Money,       // cash_cmp
        2045 | 1314 => GinBtreeType::Timestamp, // timestamp_cmp / timestamptz_cmp
        1107 => GinBtreeType::Time,       // time_cmp
        1358 => GinBtreeType::Timetz,     // timetz_cmp
        1092 => GinBtreeType::Date,       // date_cmp
        1315 => GinBtreeType::Interval,   // interval_cmp
        836 => GinBtreeType::Macaddr,     // macaddr_cmp
        4119 => GinBtreeType::Macaddr8,   // macaddr8_cmp
        926 => GinBtreeType::Inet,        // network_cmp (inet and cidr)
        1078 => GinBtreeType::Bpchar,     // bpcharcmp
        358 => GinBtreeType::Char,        // btcharcmp
        1954 => GinBtreeType::Bytea,      // byteacmp
        1596 | 1672 => GinBtreeType::Bit, // bitcmp / varbitcmp
        2960 => GinBtreeType::Uuid,       // uuid_cmp
        359 => GinBtreeType::Name,        // btnamecmp
        1693 => GinBtreeType::Bool,       // btboolcmp
        _ => return None,
    })
}

// ginarrayproc.c strategy numbers.
const GinOverlapStrategy: StrategyNumber = 1;
const GinContainsStrategy: StrategyNumber = 2;
const GinContainedStrategy: StrategyNumber = 3;
const GinEqualStrategy: StrategyNumber = 4;

#[track_caller]
#[cold]
fn unknown_array_strategy(what: &str, strategy: StrategyNumber) -> Box<PgError> {
    Box::new(PgError::error(format!(
        "{what}: unknown strategy number: {strategy}"
    )))
}

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
    // SAFETY: callers gate on inline_image, so this is an uncompressed
    // non-external image (short or 4-byte header); pin/scratch keeps it
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

/// True for an uncompressed, non-external varlena image — the only shapes
/// text_payload may read directly.
#[inline]
pub(crate) fn inline_image(d: Datum) -> bool {
    let p = d.as_usize() as *const u8;
    // SAFETY: non-null varlena datum readable through its header.
    unsafe {
        (varatt::varatt_is_1b(p) && !varatt::varatt_is_1b_e(p)) || varatt::varatt_is_4b_u(p)
    }
}

// index_form_tuple inline-compresses varlena index keys above the size
// target (TOAST_INDEX_HACK, indextuple.c:104-137), so stored GIN entry keys
// can be compressed images. C detoasts them in every compare support proc
// (PG_GETARG_TEXT_PP: tsginidx.c:26-27/42-43 gin_cmp_tslexeme/gin_cmp_prefix,
// varlena.c:1944-1945 bttextcmp, jsonb_gin.c:205-206 gin_compare_jsonb); a
// raw read would order/match compressed bytes as if they were the value.
// Cold: only compares that see a compressed (or external) key take it.
#[cold]
#[inline(never)]
fn cmp_detoasted(a: Datum, b: Datum, f: &dyn Fn(&[u8], &[u8]) -> i32) -> i32 {
    let cx = ::mcx::MemoryContext::new("gin key detoast");
    let pa = detoast_payload(cx.mcx(), a).expect("gin compare key detoast");
    let pb = detoast_payload(cx.mcx(), b).expect("gin compare key detoast");
    f(pa, pb)
}

/// Compare two text-flavored GIN keys through `f`, detoasting any side that
/// is not an inline image (C's per-compare PG_GETARG_TEXT_PP convention).
#[inline]
fn cmp_text_keys(a: Datum, b: Datum, f: impl Fn(&[u8], &[u8]) -> i32) -> i32 {
    if inline_image(a) && inline_image(b) {
        f(text_payload(a), text_payload(b))
    } else {
        cmp_detoasted(a, b, &f)
    }
}

/// compareFn: total order on two non-null key datums.
pub(crate) fn compare(col: &GinColState, a: Datum, b: Datum) -> PgResult<i32> {
    Ok(match col.compare {
        GinCompareFn::Int2 => {
            let (x, y) = (a.as_u64() as i16, b.as_u64() as i16);
            if x < y {
                -1
            } else {
                (x > y) as i32
            }
        }
        // btint4cmp; jsonb_path_ops / gin_trgm_ops / gin__int_ops keys are
        // int4 datums (path hashes via UInt32GetDatum, trgm2int, elements).
        GinCompareFn::Int4 => {
            let (x, y) = (a.as_i32(), b.as_i32());
            if x < y {
                -1
            } else {
                (x > y) as i32
            }
        }
        GinCompareFn::Int8 => {
            let (x, y) = (a.as_i64(), b.as_i64());
            if x < y {
                -1
            } else {
                (x > y) as i32
            }
        }
        GinCompareFn::Oid => {
            let (x, y) = (a.as_oid(), b.as_oid());
            if x < y {
                -1
            } else {
                (x > y) as i32
            }
        }
        // bttextcmp under the support collation; the collation is resolved
        // by the time an index key is compared, so the PgResult never fires.
        GinCompareFn::Text => cmp_text_keys(a, b, |x, y| {
            varlena::varstr_cmp(x, y, col.support_collation)
                .expect("bttextcmp: varstr_cmp failed")
        }),
        GinCompareFn::Jsonb => cmp_text_keys(a, b, ::adt_jsonb::gin::gin_compare_jsonb),
        GinCompareFn::TsLexeme => cmp_text_keys(a, b, ::adt_tsginidx::gin_cmp_tslexeme),
        // Per-type btree comparators; failures are corruption/lookup-class
        // (collation resolved, enum catalog rows exist by construction).
        GinCompareFn::Btree(ty) => {
            gin_btree_seams::btree_compare::call(ty, a, b, col.support_collation)
                .expect("btree_gin compare failed")
        }
        // Any other comparator through fmgr (C caches the fmgr_info_copy'd
        // compareFn in GinState; GinColState is Copy so we re-resolve the
        // proc oid per compare — cost-only, initGinState already resolved it
        // and raised any lookup error catchably). The callee's own argument
        // fetch detoasts compressed keys, as C's PG_GETARG does; a
        // comparator-raised ERROR propagates as C's FunctionCall2Coll does.
        GinCompareFn::Fmgr(cmp_proc) => {
            let mut finfo = ::fmgr_seams::fmgr_info::call(cmp_proc)
                .expect("GIN compare support function resolved at initGinState");
            let cx = ::mcx::MemoryContext::new_bump("gin fmgr key compare");
            ::types_fmgr::function_call2_coll_in(
                &mut finfo,
                col.support_collation,
                cx.mcx(),
                a,
                b,
            )?
            .as_i32()
        }
    })
}

/// comparePartialFn. `orig` is btree_gin's original query datum (the entry's
/// queryOrig; C passes it via extra_data's QueryInfo).
pub(crate) fn compare_partial(
    col: &GinColState,
    partial_key: Datum,
    key: Datum,
    strategy: StrategyNumber,
    orig: Datum,
) -> i32 {
    match col.compare_partial {
        Some(GinComparePartialFn::TsPrefix) => {
            cmp_text_keys(partial_key, key, ::adt_tsginidx::gin_cmp_prefix)
        }
        Some(GinComparePartialFn::Btree(ty)) => gin_btree_seams::btree_compare_prefix::call(
            ty,
            orig,
            key,
            strategy,
            col.support_collation,
        )
        .expect("btree_gin comparePartial failed"),
        None => unreachable!("comparePartialFn on a column without GIN_COMPARE_PARTIAL_PROC"),
    }
}

/// extractValueFn. The second vec is C's nullFlags out-param; empty means
/// "extractValue left it NULL" (all keys non-null).
pub(crate) fn extract_value<'m>(
    mcx: Mcx<'m>,
    col: &GinColState,
    value: Datum,
) -> PgResult<(PgVec<'m, Datum>, PgVec<'m, bool>)> {
    let no_nulls = mcx::vec_new_in(mcx);
    match col.extract_value {
        GinExtractValueFn::Jsonb => {
            let payload = detoast_payload(mcx, value)?;
            Ok((::adt_jsonb::gin::gin_extract_jsonb(mcx, payload)?, no_nulls))
        }
        GinExtractValueFn::JsonbPath => {
            let payload = detoast_payload(mcx, value)?;
            Ok((::adt_jsonb::gin::gin_extract_jsonb_path(mcx, payload)?, no_nulls))
        }
        GinExtractValueFn::Tsvector => {
            let payload = detoast_payload(mcx, value)?;
            Ok((
                ::adt_tsginidx::gin_extract_tsvector(
                    mcx,
                    ::adt_tsvector_core::layout::TsVec { payload },
                )?,
                no_nulls,
            ))
        }
        GinExtractValueFn::Array => ginarrayextract(mcx, value),
        GinExtractValueFn::Trgm => {
            let payload = detoast_payload(mcx, value)?;
            let keys = gin_trgm_seams::trgm_extract_value::call(payload)?;
            let mut entries: PgVec<'m, Datum> = mcx::vec_with_capacity_in(mcx, keys.len())?;
            for k in keys {
                entries.push(Datum::from_i32(k));
            }
            Ok((entries, no_nulls))
        }
        GinExtractValueFn::Hstore => {
            let image = detoast_image(mcx, value)?;
            let keys = gin_hstore_seams::hstore_extract_value::call(image)?;
            Ok((text_key_datums(mcx, keys)?, no_nulls))
        }
        GinExtractValueFn::Btree(ty) => {
            let d = gin_btree_seams::btree_extract_value::call(mcx, ty, value)?;
            let mut entries: PgVec<'m, Datum> = mcx::vec_with_capacity_in(mcx, 1)?;
            entries.push(d);
            Ok((entries, no_nulls))
        }
    }
}

// hstore keys are freshly built text images; copy each into mcx and hand the
// pointer datum to the GIN core (key_byval=false, key_len=-1 per tupdesc).
fn text_key_datums<'m>(mcx: Mcx<'m>, keys: Vec<Vec<u8>>) -> PgResult<PgVec<'m, Datum>> {
    let mut entries: PgVec<'m, Datum> = mcx::vec_with_capacity_in(mcx, keys.len())?;
    for k in keys {
        let img = mcx::slice_in(mcx, &k)?.leak();
        entries.push(Datum::from_usize(img.as_ptr() as usize));
    }
    Ok(entries)
}

/// ginarrayextract (ginarrayproc.c): element datums + null flags. Elements
/// borrow into the detoasted array image (mcx-lived, C's
/// PG_GETARG_ARRAYTYPE_P_COPY lifetime).
fn ginarrayextract<'m>(
    mcx: Mcx<'m>,
    array: Datum,
) -> PgResult<(PgVec<'m, Datum>, PgVec<'m, bool>)> {
    let image = detoast_image(mcx, array)?;
    let elemtype = ::arrayfuncs::foundation::arr_elemtype(image);
    let (elmlen, elmbyval, elmalign) = lsyscache::get_typlenbyvalalign(elemtype)?;
    ::arrayfuncs::construct::deconstruct_array(
        mcx,
        image,
        elmlen as i32,
        elmbyval,
        elmalign as u8,
        true,
    )
}

/// extractQueryFn outputs; C's per-opclass out-params and extra_data.
/// `null_flags` empty means "extractQuery left nullFlags NULL".
pub struct ExtractedQuery<'m> {
    pub entries: PgVec<'m, Datum>,
    pub search_mode: i32,
    pub jsp_ops: PgVec<'m, JspGinOp>,
    pub partial_match: PgVec<'m, bool>,
    pub map_item_operand: PgVec<'m, i32>,
    pub null_flags: PgVec<'m, bool>,
    /// gin_trgm_ops ~ / ~* only (C's extra_data[0] regexp graph).
    pub trgm_graph: Option<TrgmPackedGraph>,
    /// btree_gin only: the original (detoasted) query datum comparePartial
    /// compares against (C's extra_data[0] QueryInfo.datum).
    pub btree_orig: Datum,
}

/// extractQueryFn. `f` is the column's resolved proc 3 and `collation` its
/// support collation (the planner's gincost probe has no GinColState).
pub(crate) fn extract_query<'m>(
    mcx: Mcx<'m>,
    f: GinExtractQueryFn,
    collation: ::types_core::Oid,
    query: Datum,
    strategy: StrategyNumber,
) -> PgResult<ExtractedQuery<'m>> {
    // btree_gin first: by-value query datums must not hit the varlena
    // detoast below.
    if let GinExtractQueryFn::Btree(ty) = f {
        let (entry, partial, orig) =
            gin_btree_seams::btree_extract_query::call(mcx, ty, query, strategy)?;
        let mut entries: PgVec<'m, Datum> = mcx::vec_with_capacity_in(mcx, 1)?;
        entries.push(entry);
        let mut partial_match: PgVec<'m, bool> = mcx::vec_with_capacity_in(mcx, 1)?;
        partial_match.push(partial);
        return Ok(ExtractedQuery {
            entries,
            search_mode: GIN_SEARCH_MODE_DEFAULT,
            jsp_ops: mcx::vec_new_in(mcx),
            partial_match,
            map_item_operand: mcx::vec_new_in(mcx),
            null_flags: mcx::vec_new_in(mcx),
            trgm_graph: None,
            btree_orig: orig,
        });
    }
    let image = detoast_image(mcx, query)?;
    match f {
        GinExtractQueryFn::Btree(_) => unreachable!("handled above"),
        GinExtractQueryFn::Jsonb | GinExtractQueryFn::JsonbPath => {
            let (entries, search_mode, jsp_ops) = match f {
                GinExtractQueryFn::Jsonb => {
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
                null_flags: mcx::vec_new_in(mcx),
                trgm_graph: None,
                btree_orig: Datum::null(),
            })
        }
        GinExtractQueryFn::Tsquery => {
            let q = ::adt_tsvector_core::query::TsQueryRef { payload: &image[4..] };
            let out = ::adt_tsginidx::gin_extract_tsquery(mcx, q)?;
            Ok(ExtractedQuery {
                entries: out.entries,
                search_mode: out.search_mode,
                jsp_ops: mcx::vec_new_in(mcx),
                partial_match: out.partial_match,
                map_item_operand: out.map_item_operand,
                null_flags: mcx::vec_new_in(mcx),
                trgm_graph: None,
                btree_orig: Datum::null(),
            })
        }
        GinExtractQueryFn::Array => {
            // ginqueryarrayextract: deconstruct + per-strategy search mode.
            let elemtype = ::arrayfuncs::foundation::arr_elemtype(image);
            let (elmlen, elmbyval, elmalign) = lsyscache::get_typlenbyvalalign(elemtype)?;
            let (entries, null_flags) = ::arrayfuncs::construct::deconstruct_array(
                mcx,
                image,
                elmlen as i32,
                elmbyval,
                elmalign as u8,
                true,
            )?;
            let search_mode = match strategy {
                GinOverlapStrategy => GIN_SEARCH_MODE_DEFAULT,
                GinContainsStrategy => {
                    if !entries.is_empty() {
                        GIN_SEARCH_MODE_DEFAULT
                    } else {
                        // everything contains the empty set
                        GIN_SEARCH_MODE_ALL
                    }
                }
                // empty set is contained in everything
                GinContainedStrategy => GIN_SEARCH_MODE_INCLUDE_EMPTY,
                GinEqualStrategy => {
                    if !entries.is_empty() {
                        GIN_SEARCH_MODE_DEFAULT
                    } else {
                        GIN_SEARCH_MODE_INCLUDE_EMPTY
                    }
                }
                other => return Err(unknown_array_strategy("ginqueryarrayextract", other)),
            };
            Ok(ExtractedQuery {
                entries,
                search_mode,
                jsp_ops: mcx::vec_new_in(mcx),
                partial_match: mcx::vec_new_in(mcx),
                map_item_operand: mcx::vec_new_in(mcx),
                null_flags,
                trgm_graph: None,
                btree_orig: Datum::null(),
            })
        }
        GinExtractQueryFn::Trgm => {
            let (keys, search_mode, trgm_graph) = gin_trgm_seams::trgm_extract_query::call(
                &image[4..],
                strategy,
                collation,
            )?;
            let mut entries: PgVec<'m, Datum> = mcx::vec_with_capacity_in(mcx, keys.len())?;
            for k in keys {
                entries.push(Datum::from_i32(k));
            }
            Ok(ExtractedQuery {
                entries,
                search_mode,
                jsp_ops: mcx::vec_new_in(mcx),
                partial_match: mcx::vec_new_in(mcx),
                map_item_operand: mcx::vec_new_in(mcx),
                null_flags: mcx::vec_new_in(mcx),
                trgm_graph,
                btree_orig: Datum::null(),
            })
        }
        GinExtractQueryFn::Hstore => {
            let (keys, search_mode) =
                gin_hstore_seams::hstore_extract_query::call(image, strategy)?;
            Ok(ExtractedQuery {
                entries: text_key_datums(mcx, keys)?,
                search_mode,
                jsp_ops: mcx::vec_new_in(mcx),
                partial_match: mcx::vec_new_in(mcx),
                map_item_operand: mcx::vec_new_in(mcx),
                null_flags: mcx::vec_new_in(mcx),
                trgm_graph: None,
                btree_orig: Datum::null(),
            })
        }
        GinExtractQueryFn::IntArray => {
            let (keys, search_mode) = gin_int4_seams::int4_extract_query::call(image, strategy)?;
            let mut entries: PgVec<'m, Datum> = mcx::vec_with_capacity_in(mcx, keys.len())?;
            for k in keys {
                entries.push(Datum::from_i32(k));
            }
            Ok(ExtractedQuery {
                entries,
                search_mode,
                jsp_ops: mcx::vec_new_in(mcx),
                partial_match: mcx::vec_new_in(mcx),
                map_item_operand: mcx::vec_new_in(mcx),
                null_flags: mcx::vec_new_in(mcx),
                trgm_graph: None,
                // int4 keys never take the btree_gin comparePartial lane.
                btree_orig: Datum::null(),
            })
        }
    }
}

/// pgrust-only guard on a path C dereferences NULL on: a consistent proc
/// paired (by a custom opclass) with an extractQuery proc that does not
/// produce the extra_data it reads.
#[cold]
#[inline(never)]
fn missing_extra_data(proc_name: &str) -> Box<PgError> {
    Box::new(PgError::error(format!(
        "GIN support function {proc_name} called without the extra_data of its extractQuery counterpart"
    )))
}

/// consistentFn (binary), C's directBoolConsistentFn call. `mcx` is the
/// reset-per-call scratch (C tempCtx). The column must carry proc 4
/// (ginlogic.c's shim over the tri-state proc lives in logic.rs).
// pub (was pub(crate)) for proofs/jsonb-gin — visibility-only edit.
pub fn consistent(
    mcx: Mcx<'_>,
    col: &GinColState,
    check: &[GinTernaryValue],
    strategy: StrategyNumber,
    query: Datum,
    nkeys: usize,
    _query_values: &[Datum],
    _query_categories: &[GinNullCategory],
    jsp_ops: &[JspGinOp],
    map_item_operand: &[i32],
    trgm_graph: Option<&mut TrgmPackedGraph>,
    recheck: &mut bool,
) -> PgResult<bool> {
    let f = col
        .consistent
        .expect("consistentFn on a column without GIN_CONSISTENT_PROC");
    match f {
        GinConsistentFn::Jsonb | GinConsistentFn::JsonbPath => {
            if jsp_ops.is_empty() && nkeys > 0 && is_jsonpath_strategy(strategy) {
                return Err(missing_extra_data("gin_consistent_jsonb"));
            }
            if f == GinConsistentFn::Jsonb {
                ::adt_jsonb::gin::gin_consistent_jsonb(check, strategy, nkeys, recheck, jsp_ops)
            } else {
                ::adt_jsonb::gin::gin_consistent_jsonb_path(check, strategy, nkeys, recheck, jsp_ops)
            }
        }
        GinConsistentFn::Tsquery => {
            let image = detoast_image(mcx, query)?;
            let q = ::adt_tsvector_core::query::TsQueryRef { payload: &image[4..] };
            if map_item_operand.len() < q.size() {
                return Err(missing_extra_data("gin_tsquery_consistent"));
            }
            let (res, rc) = ::adt_tsginidx::gin_tsquery_consistent(mcx, check, q, map_item_operand)?;
            *recheck = rc;
            Ok(res)
        }
        // ginarrayconsistent: C reads queryCategories as its bool *nullFlags
        // (GIN_CAT_NULL_KEY == 1).
        GinConsistentFn::Array => {
            let null = |i: usize| _query_categories[i] == GIN_CAT_NULL_KEY;
            let res = match strategy {
                GinOverlapStrategy => {
                    *recheck = false;
                    (0..nkeys).any(|i| check[i] != GIN_FALSE && !null(i))
                }
                GinContainsStrategy => {
                    *recheck = false;
                    (0..nkeys).all(|i| check[i] != GIN_FALSE && !null(i))
                }
                GinContainedStrategy => {
                    *recheck = true;
                    true
                }
                GinEqualStrategy => {
                    *recheck = true;
                    (0..nkeys).all(|i| check[i] != GIN_FALSE)
                }
                other => return Err(unknown_array_strategy("ginarrayconsistent", other)),
            };
            Ok(res)
        }
        GinConsistentFn::Trgm => {
            if trgm_graph.is_none() && nkeys > 0 && is_trgm_regexp_strategy(strategy) {
                return Err(missing_extra_data("gin_trgm_consistent"));
            }
            let (res, rc) =
                gin_trgm_seams::trgm_consistent::call(check, strategy, nkeys, trgm_graph)?;
            *recheck = rc;
            Ok(res)
        }
        GinConsistentFn::Hstore => {
            let (res, rc) = gin_hstore_seams::hstore_consistent::call(check, strategy, nkeys)?;
            *recheck = rc;
            Ok(res)
        }
        // gin_btree_consistent: the single entry's match already decided.
        GinConsistentFn::Btree => {
            *recheck = false;
            Ok(true)
        }
        GinConsistentFn::IntArray => {
            let image = detoast_image(mcx, query)?;
            let (res, rc) = gin_int4_seams::int4_consistent::call(check, strategy, nkeys, image)?;
            *recheck = rc;
            Ok(res)
        }
    }
}

/// triConsistentFn, C's directTriConsistentFn call. `mcx` is the
/// reset-per-call scratch (C tempCtx). The column must carry proc 6
/// (ginlogic.c's shim over the binary proc lives in logic.rs).
// pub (was pub(crate)) for proofs/jsonb-gin — visibility-only edit.
pub fn tri_consistent(
    mcx: Mcx<'_>,
    col: &GinColState,
    check: &[GinTernaryValue],
    strategy: StrategyNumber,
    query: Datum,
    nkeys: usize,
    _query_values: &[Datum],
    _query_categories: &[GinNullCategory],
    jsp_ops: &[JspGinOp],
    map_item_operand: &[i32],
    trgm_graph: Option<&mut TrgmPackedGraph>,
) -> PgResult<GinTernaryValue> {
    let f = col
        .tri_consistent
        .expect("triConsistentFn on a column without GIN_TRICONSISTENT_PROC");
    match f {
        GinTriConsistentFn::Jsonb | GinTriConsistentFn::JsonbPath => {
            if jsp_ops.is_empty() && nkeys > 0 && is_jsonpath_strategy(strategy) {
                return Err(missing_extra_data("gin_triconsistent_jsonb"));
            }
            if f == GinTriConsistentFn::Jsonb {
                ::adt_jsonb::gin::gin_triconsistent_jsonb(check, strategy, nkeys, jsp_ops)
            } else {
                ::adt_jsonb::gin::gin_triconsistent_jsonb_path(check, strategy, nkeys, jsp_ops)
            }
        }
        GinTriConsistentFn::Tsquery => {
            let image = detoast_image(mcx, query)?;
            let q = ::adt_tsvector_core::query::TsQueryRef { payload: &image[4..] };
            if map_item_operand.len() < q.size() {
                return Err(missing_extra_data("gin_tsquery_triconsistent"));
            }
            ::adt_tsginidx::gin_tsquery_triconsistent(mcx, check, q, map_item_operand)
        }
        // ginarraytriconsistent; queryCategories double as C's nullFlags.
        GinTriConsistentFn::Array => {
            let null = |i: usize| _query_categories[i] == GIN_CAT_NULL_KEY;
            let res = match strategy {
                GinOverlapStrategy => {
                    let mut res = GIN_FALSE;
                    for i in 0..nkeys {
                        if !null(i) {
                            if check[i] == GIN_TRUE {
                                res = GIN_TRUE;
                                break;
                            } else if check[i] == GIN_MAYBE && res == GIN_FALSE {
                                res = GIN_MAYBE;
                            }
                        }
                    }
                    res
                }
                GinContainsStrategy => {
                    let mut res = GIN_TRUE;
                    for i in 0..nkeys {
                        if check[i] == GIN_FALSE || null(i) {
                            res = GIN_FALSE;
                            break;
                        }
                        if check[i] == GIN_MAYBE {
                            res = GIN_MAYBE;
                        }
                    }
                    res
                }
                GinContainedStrategy => GIN_MAYBE,
                GinEqualStrategy => {
                    let mut res = GIN_MAYBE;
                    for i in 0..nkeys {
                        if check[i] == GIN_FALSE {
                            res = GIN_FALSE;
                            break;
                        }
                    }
                    res
                }
                other => return Err(unknown_array_strategy("ginarrayconsistent", other)),
            };
            Ok(res)
        }
        GinTriConsistentFn::Trgm => {
            if trgm_graph.is_none() && nkeys > 0 && is_trgm_regexp_strategy(strategy) {
                return Err(missing_extra_data("gin_trgm_triconsistent"));
            }
            gin_trgm_seams::trgm_triconsistent::call(check, strategy, nkeys, trgm_graph)
        }
    }
}

/// jsonb_gin.c strategies whose consistent reads extra_data (the jsonpath
/// GIN expression tree): JsonbJsonpathExistsStrategyNumber (15) and
/// JsonbJsonpathPredicateStrategyNumber (16).
fn is_jsonpath_strategy(strategy: StrategyNumber) -> bool {
    strategy == ::adt_jsonb::gin::JsonbJsonpathExistsStrategyNumber
        || strategy == ::adt_jsonb::gin::JsonbJsonpathPredicateStrategyNumber
}

/// trgm_gin.c strategies whose consistent reads extra_data (the packed
/// regex graph): RegExpStrategyNumber (5) and RegExpICaseStrategyNumber (6).
fn is_trgm_regexp_strategy(strategy: StrategyNumber) -> bool {
    strategy == 5 || strategy == 6
}

/// gincost_pattern's extractQuery probe (selfuncs.c gincostestimate):
/// resolves the index column's extractQueryFn from the opfamily (the
/// planner's index_getprocinfo) and runs it, returning (nentries, npartial,
/// searchMode). `collation` is the caller-resolved index-column collation
/// (already defaulted when the column has none).
pub fn gincost_extract_query(
    opfamily: ::types_core::Oid,
    opcintype: ::types_core::Oid,
    collation: ::types_core::Oid,
    query: Datum,
    strategy: StrategyNumber,
    indexcol: usize,
    index_oid: ::types_core::Oid,
) -> PgResult<(i32, i32, i32)> {
    let extract = lsyscache::get_opfamily_proc(
        opfamily,
        opcintype,
        opcintype,
        GIN_EXTRACTQUERY_PROC as i16,
    )?;
    if extract == ::types_core::InvalidOid {
        // User-reachable: CREATE OPERATOR CLASS ... USING gin without a
        // FUNCTION 3 (extractQuery) entry is accepted at DDL time, and the
        // planner lands here on the first scan over such an index. C throws
        // the same error as index_getprocinfo (selfuncs.c:8018).
        let cx = ::mcx::MemoryContext::new("gincost extract probe");
        let relname = lsyscache::get_rel_name(cx.mcx(), index_oid)?
            .map_or_else(String::new, |n| n.as_str().to_string());
        return Err(Box::new(::types_error::PgError::error(format!(
            "missing support function {GIN_EXTRACTQUERY_PROC} for attribute {} of index \"{relname}\"",
            indexcol + 1
        ))
        .with_sqlstate(::types_error::ERRCODE_INTERNAL_ERROR)));
    }
    let f = crate::util::resolve_extract_query(extract)?;
    let scratch = ::mcx::MemoryContext::new_bump("gincost extract scratch");
    let out = extract_query(scratch.mcx(), f, collation, query, strategy)?;
    let npartial = out.partial_match.iter().filter(|&&p| p).count() as i32;
    Ok((out.entries.len() as i32, npartial, out.search_mode))
}

