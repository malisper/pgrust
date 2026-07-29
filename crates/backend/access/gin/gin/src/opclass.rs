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
pub(crate) const F_BTTEXTCMP: ::types_core::Oid = 360;
pub(crate) const F_GIN_EXTRACT_TSVECTOR: ::types_core::Oid = 3656;
pub(crate) const F_GIN_EXTRACT_TSQUERY: ::types_core::Oid = 3657;
pub(crate) const F_GIN_CMP_TSLEXEME: ::types_core::Oid = 3724;
pub(crate) const F_GIN_CMP_PREFIX: ::types_core::Oid = 2700;
pub(crate) const F_GINARRAYEXTRACT: ::types_core::Oid = 2743;
pub(crate) const F_GINQUERYARRAYEXTRACT: ::types_core::Oid = 2774;

// ginarrayproc.c strategy numbers.
const GinOverlapStrategy: StrategyNumber = 1;
const GinContainsStrategy: StrategyNumber = 2;
const GinContainedStrategy: StrategyNumber = 3;
const GinEqualStrategy: StrategyNumber = 4;

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
pub(crate) fn compare(col: &GinColState, a: Datum, b: Datum) -> i32 {
    match col.opclass {
        GinOpclass::JsonbOps => {
            cmp_text_keys(a, b, ::adt_jsonb::gin::gin_compare_jsonb)
        }
        // Per-type btree comparators; failures are corruption/lookup-class
        // (collation resolved, enum catalog rows exist by construction).
        GinOpclass::BtreeOps(ty) => {
            gin_btree_seams::btree_compare::call(ty, a, b, col.support_collation)
                .expect("btree_gin compare failed")
        }
        // bttextcmp under the support collation (hstore FUNCTION 1).
        GinOpclass::HstoreOps => cmp_text_keys(a, b, |x, y| {
            varlena::varstr_cmp(x, y, col.support_collation)
                .expect("bttextcmp: varstr_cmp failed")
        }),
        GinOpclass::JsonbPathOps | GinOpclass::TrgmOps | GinOpclass::IntArrayOps => {
            // btint4cmp over uint32 path hashes stored via UInt32GetDatum
            // (trgm keys: trgm2int int4, always >= 0, same comparator;
            // intarray keys: signed int4 elements, btint4cmp per its SQL).
            let (x, y) = (a.as_usize() as u32 as i32, b.as_usize() as u32 as i32);
            if x < y {
                -1
            } else {
                (x > y) as i32
            }
        }
        GinOpclass::TsvectorOps => cmp_text_keys(a, b, ::adt_tsginidx::gin_cmp_tslexeme),
        // Element-type default btree comparator (initGinState typcache
        // fallback), closed set resolved to state.elem_cmp.
        GinOpclass::ArrayOps => match col.elem_cmp {
            GinElemCmp::Int2 => {
                let (x, y) = (a.as_u64() as i16, b.as_u64() as i16);
                if x < y {
                    -1
                } else {
                    (x > y) as i32
                }
            }
            GinElemCmp::Int4 => {
                let (x, y) = (a.as_i32(), b.as_i32());
                if x < y {
                    -1
                } else {
                    (x > y) as i32
                }
            }
            GinElemCmp::Int8 => {
                let (x, y) = (a.as_i64(), b.as_i64());
                if x < y {
                    -1
                } else {
                    (x > y) as i32
                }
            }
            GinElemCmp::Oid => {
                let (x, y) = (a.as_oid(), b.as_oid());
                if x < y {
                    -1
                } else {
                    (x > y) as i32
                }
            }
            GinElemCmp::Text => cmp_text_keys(a, b, |x, y| {
                // bttextcmp; the collation is resolved by the time an index
                // key is compared, so the PgResult never fires here.
                ::varlena::varstr_cmp(x, y, col.support_collation)
                    .expect("collation resolved for gin array_ops key compare")
            }),
            GinElemCmp::None => unreachable!("array_ops compare without elem_cmp"),
        },
    }
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
    match col.opclass {
        GinOpclass::TsvectorOps => {
            cmp_text_keys(partial_key, key, ::adt_tsginidx::gin_cmp_prefix)
        }
        GinOpclass::BtreeOps(ty) => gin_btree_seams::btree_compare_prefix::call(
            ty,
            orig,
            key,
            strategy,
            col.support_collation,
        )
        .expect("btree_gin comparePartial failed"),
        _ => unreachable!("comparePartialFn on a non-partial-match opclass"),
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
    match col.opclass {
        GinOpclass::JsonbOps => {
            let payload = detoast_payload(mcx, value)?;
            Ok((::adt_jsonb::gin::gin_extract_jsonb(mcx, payload)?, no_nulls))
        }
        GinOpclass::JsonbPathOps => {
            let payload = detoast_payload(mcx, value)?;
            Ok((::adt_jsonb::gin::gin_extract_jsonb_path(mcx, payload)?, no_nulls))
        }
        GinOpclass::TsvectorOps => {
            let payload = detoast_payload(mcx, value)?;
            Ok((
                ::adt_tsginidx::gin_extract_tsvector(
                    mcx,
                    ::adt_tsvector_core::layout::TsVec { payload },
                )?,
                no_nulls,
            ))
        }
        // gin__int_ops registers core ginarrayextract as its proc 2 too.
        GinOpclass::ArrayOps | GinOpclass::IntArrayOps => ginarrayextract(mcx, value),
        GinOpclass::TrgmOps => {
            let payload = detoast_payload(mcx, value)?;
            let keys = gin_trgm_seams::trgm_extract_value::call(payload)?;
            let mut entries: PgVec<'m, Datum> = mcx::vec_with_capacity_in(mcx, keys.len())?;
            for k in keys {
                entries.push(Datum::from_i32(k));
            }
            Ok((entries, no_nulls))
        }
        GinOpclass::HstoreOps => {
            let image = detoast_image(mcx, value)?;
            let keys = gin_hstore_seams::hstore_extract_value::call(image)?;
            Ok((text_key_datums(mcx, keys)?, no_nulls))
        }
        GinOpclass::BtreeOps(ty) => {
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

pub(crate) fn extract_query<'m>(
    mcx: Mcx<'m>,
    col: &GinColState,
    query: Datum,
    strategy: StrategyNumber,
) -> PgResult<ExtractedQuery<'m>> {
    // btree_gin first: by-value query datums must not hit the varlena
    // detoast below.
    if let GinOpclass::BtreeOps(ty) = col.opclass {
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
    match col.opclass {
        GinOpclass::BtreeOps(_) => unreachable!("handled above"),
        GinOpclass::JsonbOps | GinOpclass::JsonbPathOps => {
            let (entries, search_mode, jsp_ops) = match col.opclass {
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
                null_flags: mcx::vec_new_in(mcx),
                trgm_graph: None,
                btree_orig: Datum::null(),
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
                null_flags: mcx::vec_new_in(mcx),
                trgm_graph: None,
                btree_orig: Datum::null(),
            })
        }
        GinOpclass::ArrayOps => {
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
                other => panic!("ginqueryarrayextract: unknown strategy number: {other}"),
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
        GinOpclass::TrgmOps => {
            let (keys, search_mode, trgm_graph) = gin_trgm_seams::trgm_extract_query::call(
                &image[4..],
                strategy,
                col.support_collation,
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
        GinOpclass::HstoreOps => {
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
        GinOpclass::IntArrayOps => {
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

/// consistentFn (binary). `mcx` is the reset-per-call scratch (C tempCtx).
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
    match col.opclass {
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
        // ginarrayconsistent: C reads queryCategories as its bool *nullFlags
        // (GIN_CAT_NULL_KEY == 1).
        GinOpclass::ArrayOps => {
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
                other => panic!("ginarrayconsistent: unknown strategy number: {other}"),
            };
            Ok(res)
        }
        GinOpclass::TrgmOps => {
            let (res, rc) =
                gin_trgm_seams::trgm_consistent::call(check, strategy, nkeys, trgm_graph)?;
            *recheck = rc;
            Ok(res)
        }
        GinOpclass::HstoreOps => {
            let (res, rc) = gin_hstore_seams::hstore_consistent::call(check, strategy, nkeys)?;
            *recheck = rc;
            Ok(res)
        }
        // gin_btree_consistent: the single entry's match already decided.
        GinOpclass::BtreeOps(_) => {
            *recheck = false;
            Ok(true)
        }
        GinOpclass::IntArrayOps => {
            let image = detoast_image(mcx, query)?;
            let (res, rc) = gin_int4_seams::int4_consistent::call(check, strategy, nkeys, image)?;
            *recheck = rc;
            Ok(res)
        }
    }
}

/// triConsistentFn. `mcx` is the reset-per-call scratch (C tempCtx).
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
    match col.opclass {
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
        // ginarraytriconsistent; queryCategories double as C's nullFlags.
        GinOpclass::ArrayOps => {
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
                other => panic!("ginarrayconsistent: unknown strategy number: {other}"),
            };
            Ok(res)
        }
        GinOpclass::TrgmOps => {
            gin_trgm_seams::trgm_triconsistent::call(check, strategy, nkeys, trgm_graph)
        }
        // hstore and gin__int_ops have no C triconsistent; mirror ginlogic.c
        // shimTriConsistentFn over the bool consistent core, collapsing
        // TRUE+recheck to MAYBE (identical scan outcome to C's
        // recheckCurItem propagation).
        GinOpclass::HstoreOps => shim_tri_consistent(check, nkeys, &|local| {
            gin_hstore_seams::hstore_consistent::call(local, strategy, nkeys)
        }),
        GinOpclass::IntArrayOps => {
            let image = detoast_image(mcx, query)?;
            shim_tri_consistent(check, nkeys, &|local| {
                gin_int4_seams::int4_consistent::call(local, strategy, nkeys, image)
            })
        }
        // ginlogic.c shim over gin_btree_consistent's constant true/no-recheck.
        GinOpclass::BtreeOps(_) => Ok(GIN_TRUE),
    }
}

const MAX_MAYBE_ENTRIES: usize = 4;

fn shim_tri_consistent(
    check: &[GinTernaryValue],
    nkeys: usize,
    call: &dyn Fn(&[i8]) -> PgResult<(bool, bool)>,
) -> PgResult<GinTernaryValue> {
    let mut maybe_entries = [0usize; MAX_MAYBE_ENTRIES];
    let mut nmaybe = 0usize;
    for i in 0..nkeys {
        if check[i] == GIN_MAYBE {
            if nmaybe >= MAX_MAYBE_ENTRIES {
                return Ok(GIN_MAYBE);
            }
            maybe_entries[nmaybe] = i;
            nmaybe += 1;
        }
    }

    if nmaybe == 0 {
        let (res, rc) = call(check)?;
        return Ok(if res && rc { GIN_MAYBE } else if res { GIN_TRUE } else { GIN_FALSE });
    }

    let mut local: Vec<i8> = check[..nkeys].to_vec();
    for &e in &maybe_entries[..nmaybe] {
        local[e] = GIN_FALSE;
    }
    let (first, _rc) = call(&local)?;
    let cur_result = first;
    let mut recheck = false;
    loop {
        let mut i = 0usize;
        while i < nmaybe {
            let e = maybe_entries[i];
            if local[e] == GIN_FALSE {
                local[e] = GIN_TRUE;
                break;
            }
            local[e] = GIN_FALSE;
            i += 1;
        }
        if i == nmaybe {
            break;
        }
        let (res, rc) = call(&local)?;
        recheck |= rc;
        if cur_result != res {
            return Ok(GIN_MAYBE);
        }
    }
    Ok(if cur_result && recheck {
        GIN_MAYBE
    } else if cur_result {
        GIN_TRUE
    } else {
        GIN_FALSE
    })
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
        F_GINQUERYARRAYEXTRACT => (GinOpclass::ArrayOps, false),
        other => {
            let cx = ::mcx::MemoryContext::new("gincost ext opclass probe");
            let name = lsyscache::get_func_name(cx.mcx(), other)?
                .map(|n| n.as_str().to_string());
            let btree_ty = name
                .as_deref()
                .and_then(|n| n.strip_prefix("gin_extract_query_"))
                .and_then(GinBtreeType::from_type_name);
            match (name.as_deref(), btree_ty) {
                (Some("gin_extract_query_trgm"), _) => (GinOpclass::TrgmOps, false),
                (Some("gin_extract_hstore_query"), _) => (GinOpclass::HstoreOps, false),
                (Some("ginint4_queryextract"), _) => (GinOpclass::IntArrayOps, false),
                (_, Some(ty)) => (GinOpclass::BtreeOps(ty), true),
                // unported: opclasses beyond the closed set (user-reachable
                // via planning a scan over a custom-opclass GIN index).
                _ => {
                    return Err(crate::unsupported(format!(
                        "GIN operator class with extractQuery support function {other} is not supported"
                    )))
                }
            }
        }
    };
    let col = GinColState {
        opclass,
        elem_cmp: GinElemCmp::None,
        support_collation: ::types_core::catalog::DEFAULT_COLLATION_OID,
        can_partial_match: can_partial,
        key_byval: false,
        key_len: -1,
    };
    let scratch = ::mcx::MemoryContext::new_bump("gincost extract scratch");
    let out = extract_query(scratch.mcx(), &col, query, strategy)?;
    let npartial = out.partial_match.iter().filter(|&&p| p).count() as i32;
    Ok((out.entries.len() as i32, npartial, out.search_mode))
}
