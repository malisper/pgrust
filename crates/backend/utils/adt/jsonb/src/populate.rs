//! jsonfuncs.c json_populate_type slice: the EEOP_JSONEXPR_COERCION legs
//! (scalar/array/domain live; composite/record and the json-text input leg
//! are loud panics).

extern crate alloc;

use core::ffi::CStr;

use datum::Datum;
use mcx::{alloc_in, Mcx, PgBox, PgVec};
use stack_depth::check_stack_depth;
use stringinfo::StringInfo;
use types_core::catalog::{JSONBOID, JSONOID};
use types_core::{Oid, RECORDOID};
use types_error::{
    ereturn, PgError, PgResult, ERRCODE_INVALID_TEXT_REPRESENTATION,
};
use types_fmgr::{input_function_call_safe, ErrorSaveNode, FmgrInfo};

use crate::builtins::image_result as image_datum;
use crate::container::{container_is_array, container_is_scalar, JsonbItem};
use crate::iter::{JsonbIterator, WjbToken};

struct ScalarIoData {
    typiofunc: FmgrInfo,
    typioparam: Oid,
}

enum ColumnKind<'mcx> {
    Scalar,
    Array {
        element: PgBox<'mcx, ColumnIoData<'mcx>>,
    },
    Composite,
    CompositeDomain,
    Domain {
        base: PgBox<'mcx, ColumnIoData<'mcx>>,
    },
}

/// C ColumnIOData: resolve-once type metadata for one (typid, typmod). The C
/// lazy re-probe on typid/typmod change never fires here — the target type is
/// fixed per step — so the whole tree is built eagerly on first use.
pub struct ColumnIoData<'mcx> {
    typid: Oid,
    typmod: i32,
    // C prepare_column_cache resolves scalar_io at every level
    // (need_scalar=true from populate_record_field): the json-string-to-
    // non-scalar input-function hack reads it for arrays/composites too.
    io: ScalarIoData,
    kind: ColumnKind<'mcx>,
}

impl<'mcx> ColumnIoData<'mcx> {
    // C prepare_column_cache (jsonfuncs.c).
    pub fn new(cache_mcx: Mcx<'mcx>, typid: Oid, typmod: i32) -> PgResult<ColumnIoData<'mcx>> {
        check_stack_depth()?;
        let typtype = lsyscache::get_typtype(typid)?;
        let element_type = lsyscache::get_element_type(typid)?;
        let kind = if typtype == lsyscache::TYPTYPE_DOMAIN {
            let mut base_typmod = typmod;
            let base_typid = lsyscache::getBaseTypeAndTypmod(typid, &mut base_typmod)?;
            if lsyscache::get_typtype(base_typid)? == lsyscache::TYPTYPE_COMPOSITE {
                ColumnKind::CompositeDomain
            } else {
                ColumnKind::Domain {
                    base: alloc_in(
                        cache_mcx,
                        ColumnIoData::new(cache_mcx, base_typid, base_typmod)?,
                    )?,
                }
            }
        } else if typtype == lsyscache::TYPTYPE_COMPOSITE || typid == RECORDOID {
            ColumnKind::Composite
        } else if element_type != types_core::InvalidOid {
            // C: array element typmod is the attribute's typmod.
            ColumnKind::Array {
                element: alloc_in(
                    cache_mcx,
                    ColumnIoData::new(cache_mcx, element_type, typmod)?,
                )?,
            }
        } else {
            ColumnKind::Scalar
        };
        let (typinput, typioparam) = lsyscache::getTypeInputInfo(typid)?;
        let typiofunc = fmgr_seams::fmgr_info::call(typinput)?;
        Ok(ColumnIoData {
            typid,
            typmod,
            io: ScalarIoData { typiofunc, typioparam },
            kind,
        })
    }
}

fn soft_occurred(escontext: &Option<&mut ErrorSaveNode>) -> bool {
    escontext.as_ref().is_some_and(|n| n.ctx.error_occurred())
}

/// C json_populate_type (jsonfuncs.c). `cache_mcx` is C's `mcxt` (per-query,
/// owns the ColumnIOData tree); `mcx` is CurrentMemoryContext (results and
/// per-call scratch). The json (text) input leg is unported — only jsonb
/// reaches EEOP_JSONEXPR_COERCION.
///
/// # Safety
/// When `!*isnull`, `json_val` is a live non-null jsonb varlena datum
/// readable for the duration of the call.
#[allow(clippy::too_many_arguments)]
pub unsafe fn json_populate_type<'mcx>(
    json_val: Datum,
    json_type: Oid,
    typid: Oid,
    typmod: i32,
    cache: &mut Option<ColumnIoData<'mcx>>,
    cache_mcx: Mcx<'mcx>,
    mcx: Mcx<'_>,
    isnull: &mut bool,
    omit_quotes: bool,
    escontext: Option<&mut ErrorSaveNode>,
) -> PgResult<Datum> {
    if json_type != JSONBOID {
        panic!(
            "json_populate_type: json (text) input leg (jsonfuncs.c populate_array_json) \
             unported — jsonfuncs lane (json_type {json_type})"
        );
    }
    let mut payload_holder = None;
    let mut unquoted_holder: Option<PgVec<'_, u8>> = None;
    let jsv: Option<JsonbItem<'_>> = if *isnull {
        None
    } else {
        // SAFETY: caller contract — live non-null jsonb varlena.
        let payload = payload_holder
            .insert(unsafe { crate::builtins::jsonb_payload_from_datum(mcx, json_val)? });
        if omit_quotes {
            let s = unquoted_holder.insert(jsonb_unquote(mcx, payload.as_bytes())?);
            Some(JsonbItem::String(&s[..]))
        } else {
            Some(JsonbItem::Binary(payload.as_bytes()))
        }
    };
    if cache.is_none() {
        *cache = Some(ColumnIoData::new(cache_mcx, typid, typmod)?);
    }
    let col = cache.as_mut().expect("cache just filled");
    populate_record_field(col, None, mcx, jsv, isnull, escontext, omit_quotes)
}

// C populate_record_field, jsonb leg.
fn populate_record_field(
    col: &mut ColumnIoData<'_>,
    colname: Option<&str>,
    mcx: Mcx<'_>,
    jsv: Option<JsonbItem<'_>>,
    isnull: &mut bool,
    escontext: Option<&mut ErrorSaveNode>,
    omit_scalar_quotes: bool,
) -> PgResult<Datum> {
    check_stack_depth()?;
    debug_assert!(col.typid != types_core::InvalidOid);
    // C JsValueIsNull.
    *isnull = matches!(jsv, None | Some(JsonbItem::Null));

    // C: a json string converts to a non-scalar type through its input fn.
    let as_scalar = matches!(jsv, Some(JsonbItem::String(_)))
        && matches!(
            col.kind,
            ColumnKind::Array { .. } | ColumnKind::Composite | ColumnKind::CompositeDomain
        );

    // C: domain checks must run for NULLs; everything else exits now.
    if *isnull && !matches!(col.kind, ColumnKind::Domain { .. } | ColumnKind::CompositeDomain) {
        return Ok(Datum::null());
    }

    if as_scalar {
        return populate_scalar(
            &mut col.io,
            col.typid,
            col.typmod,
            mcx,
            jsv.expect("string jsv"),
            isnull,
            escontext,
            omit_scalar_quotes,
        );
    }
    match &mut col.kind {
        ColumnKind::Scalar => populate_scalar(
            &mut col.io,
            col.typid,
            col.typmod,
            mcx,
            jsv.expect("non-null jsv"),
            isnull,
            escontext,
            omit_scalar_quotes,
        ),
        ColumnKind::Array { element } => populate_array(
            element,
            colname,
            mcx,
            jsv.expect("non-null jsv"),
            isnull,
            escontext,
        ),
        ColumnKind::Composite | ColumnKind::CompositeDomain => panic!(
            "json_populate_type: populate_composite/populate_record (jsonfuncs.c) unported — \
             jsonfuncs lane (composite target type {})",
            col.typid
        ),
        ColumnKind::Domain { base } => populate_domain(
            base,
            col.typid,
            colname,
            mcx,
            jsv,
            isnull,
            escontext,
            omit_scalar_quotes,
        ),
    }
}

// C populate_scalar, jsonb leg (is_json inputs never reach here).
#[allow(clippy::too_many_arguments)]
fn populate_scalar(
    io: &mut ScalarIoData,
    typid: Oid,
    typmod: i32,
    mcx: Mcx<'_>,
    jsv: JsonbItem<'_>,
    isnull: &mut bool,
    escontext: Option<&mut ErrorSaveNode>,
    omit_quotes: bool,
) -> PgResult<Datum> {
    // C branch order: a quote-stripped string wins over the JSONBOID direct
    // JsonbValueToJsonb return.
    if typid == JSONBOID && !(omit_quotes && matches!(jsv, JsonbItem::String(_))) {
        return Ok(image_datum(crate::build::item_to_jsonb_image(mcx, jsv)?));
    }
    let mut buf = StringInfo::new_in(mcx)?;
    match jsv {
        JsonbItem::String(s) if omit_quotes => buf.append_bytes(s)?,
        // C: scalar jsonb to json preserves top-level string quotes
        // (JsonbValueToJsonb + JsonbToCString collapses to a direct render).
        JsonbItem::String(s) if typid == JSONOID => adt_json::escape_json(&mut buf, s)?,
        JsonbItem::String(s) => buf.append_bytes(s)?,
        JsonbItem::Bool(b) => buf.append_bytes(if b { b"true" } else { b"false" })?,
        JsonbItem::Numeric(image) => {
            let mut scratch = alloc::vec::Vec::new();
            adt_numeric::numeric_out_into(adt_numeric::Num::from_payload(&image[4..]), &mut scratch);
            buf.append_bytes(&scratch)?
        }
        JsonbItem::Binary(c) => {
            crate::io::jsonb_to_cstring_into(mcx, &mut buf, c, c.len() + 4)?
        }
        other => panic!("unrecognized jsonb type: {}", other.type_ord()),
    }
    buf.append_bytes(b"\0")?;
    let cstr = CStr::from_bytes_with_nul(buf.as_bytes()).expect("jsonb text has no interior NUL");
    let mut res = Datum::null();
    if !input_function_call_safe(
        &mut io.typiofunc,
        Some(cstr),
        io.typioparam,
        typmod,
        mcx,
        escontext,
        &mut res,
    )? {
        res = Datum::null();
        *isnull = true;
    }
    Ok(res)
}

// C populate_domain; constraint evaluation rides the compiled-check engine
// behind typcache_seams::domain_check_input (C domain_check_safe).
#[allow(clippy::too_many_arguments)]
fn populate_domain(
    base: &mut ColumnIoData<'_>,
    typid: Oid,
    colname: Option<&str>,
    mcx: Mcx<'_>,
    jsv: Option<JsonbItem<'_>>,
    isnull: &mut bool,
    mut escontext: Option<&mut ErrorSaveNode>,
    omit_quotes: bool,
) -> PgResult<Datum> {
    let mut res = Datum::null();
    if !*isnull {
        res = populate_record_field(
            base,
            colname,
            mcx,
            jsv,
            isnull,
            escontext.as_deref_mut(),
            omit_quotes,
        )?;
        debug_assert!(!*isnull || soft_occurred(&escontext));
    }
    typcache_seams::domain_check_input::call(
        res,
        *isnull,
        typid,
        escontext.as_deref_mut().map(|n| &mut n.ctx),
    )?;
    if soft_occurred(&escontext) {
        *isnull = true;
        return Ok(Datum::null());
    }
    Ok(res)
}

// C PopulateArrayContext: `astate` lives in C's ctx.acxt = CurrentMemoryContext.
struct PopulateArrayContext<'e, 'c, 'r> {
    element: &'e mut ColumnIoData<'c>,
    astate: Option<::datum::array_build::ArrayBuildState<'r>>,
    colname: Option<&'e str>,
    mcx: Mcx<'r>,
    ndims: i32,
    dims: PgVec<'r, i32>,
    sizes: PgVec<'r, i32>,
}

#[cold]
fn expected_json_array_error(ctx: &PopulateArrayContext<'_, '_, '_>, ndim: i32) -> PgError {
    let mut e = PgError::error("expected JSON array")
        .with_sqlstate(ERRCODE_INVALID_TEXT_REPRESENTATION);
    if ndim <= 0 {
        if let Some(colname) = ctx.colname {
            e = e.with_hint(alloc::format!("See the value of key \"{colname}\"."));
        }
    } else {
        debug_assert!(ctx.ndims > 0 && ndim < ctx.ndims);
        let mut indices = alloc::string::String::new();
        for i in 0..ndim as usize {
            indices.push_str(&alloc::format!("[{}]", ctx.sizes[i]));
        }
        e = match ctx.colname {
            Some(colname) => e.with_hint(alloc::format!(
                "See the array element {indices} of key \"{colname}\"."
            )),
            None => e.with_hint(alloc::format!("See the array element {indices}.")),
        };
    }
    e
}

// C populate_array_report_expected_array: errsave (soft when escontext armed).
fn populate_array_report_expected_array(
    ctx: &PopulateArrayContext<'_, '_, '_>,
    ndim: i32,
    escontext: Option<&mut ErrorSaveNode>,
) -> PgResult<()> {
    ereturn(
        escontext.map(|n| &mut n.ctx),
        (),
        expected_json_array_error(ctx, ndim),
    )
}

// C populate_array_assign_ndims.
fn populate_array_assign_ndims(
    ctx: &mut PopulateArrayContext<'_, '_, '_>,
    ndims: i32,
    escontext: Option<&mut ErrorSaveNode>,
) -> PgResult<bool> {
    debug_assert!(ctx.ndims <= 0);
    if ndims <= 0 {
        populate_array_report_expected_array(ctx, ndims, escontext)?;
        return Ok(false);
    }
    ctx.ndims = ndims;
    for _ in 0..ndims {
        ctx.dims.push(-1);
        ctx.sizes.push(0);
    }
    Ok(true)
}

// C populate_array_check_dimension.
fn populate_array_check_dimension(
    ctx: &mut PopulateArrayContext<'_, '_, '_>,
    ndim: i32,
    escontext: Option<&mut ErrorSaveNode>,
) -> PgResult<bool> {
    let ndim = ndim as usize;
    let dim = ctx.sizes[ndim];
    if ctx.dims[ndim] == -1 {
        ctx.dims[ndim] = dim;
    } else if ctx.dims[ndim] != dim {
        ereturn(
            escontext.map(|n| &mut n.ctx),
            (),
            PgError::error("malformed JSON array")
                .with_sqlstate(ERRCODE_INVALID_TEXT_REPRESENTATION)
                .with_detail(
                    "Multidimensional arrays must have sub-arrays with matching dimensions.",
                ),
        )?;
        return Ok(false);
    }
    ctx.sizes[ndim] = 0;
    if ndim > 0 {
        ctx.sizes[ndim - 1] += 1;
    }
    Ok(true)
}

// C populate_array_element.
fn populate_array_element(
    ctx: &mut PopulateArrayContext<'_, '_, '_>,
    ndim: i32,
    jsv: JsonbItem<'_>,
    mut escontext: Option<&mut ErrorSaveNode>,
) -> PgResult<bool> {
    let mut element_isnull = false;
    let element = populate_record_field(
        ctx.element,
        None,
        ctx.mcx,
        Some(jsv),
        &mut element_isnull,
        escontext.as_deref_mut(),
        false,
    )?;
    if soft_occurred(&escontext) {
        return Ok(false);
    }
    let element_type = ctx.element.typid;
    ctx.astate = Some(arrayfuncs::accum_array_result(
        ctx.mcx,
        ctx.astate.take(),
        element,
        element_isnull,
        element_type,
    )?);
    debug_assert!(ndim > 0);
    ctx.sizes[ndim as usize - 1] += 1;
    Ok(true)
}

// C populate_array_dim_jsonb.
fn populate_array_dim_jsonb(
    ctx: &mut PopulateArrayContext<'_, '_, '_>,
    jbv: JsonbItem<'_>,
    ndim: i32,
    mut escontext: Option<&mut ErrorSaveNode>,
) -> PgResult<bool> {
    check_stack_depth()?;

    // C: even scalars can end up here thanks to ExecEvalJsonCoercion().
    let jbc = match jbv {
        JsonbItem::Binary(c) if container_is_array(c) && !container_is_scalar(c) => c,
        _ => {
            populate_array_report_expected_array(ctx, ndim - 1, escontext)?;
            return Ok(false);
        }
    };

    let mut it = JsonbIterator::init(ctx.mcx, jbc)?;
    let (tok, _) = it.next(true);
    debug_assert_eq!(tok, WjbToken::BeginArray);

    let (mut tok, mut val) = it.next(true);

    if ctx.ndims <= 0
        && (tok == WjbToken::EndArray
            || (tok == WjbToken::Elem
                && !matches!(val, JsonbItem::Binary(c) if container_is_array(c))))
    {
        if !populate_array_assign_ndims(ctx, ndim, escontext.as_deref_mut())? {
            return Ok(false);
        }
    }

    while tok == WjbToken::Elem {
        if ctx.ndims > 0 && ndim >= ctx.ndims {
            if !populate_array_element(ctx, ndim, val, escontext.as_deref_mut())? {
                return Ok(false);
            }
        } else {
            if !populate_array_dim_jsonb(ctx, val, ndim + 1, escontext.as_deref_mut())? {
                return Ok(false);
            }
            debug_assert!(ctx.ndims > 0);
            if !populate_array_check_dimension(ctx, ndim, escontext.as_deref_mut())? {
                return Ok(false);
            }
        }
        (tok, val) = it.next(true);
    }

    debug_assert_eq!(tok, WjbToken::EndArray);
    let (tok, _) = it.next(true);
    debug_assert_eq!(tok, WjbToken::Done);
    Ok(true)
}

// C populate_array, jsonb leg.
fn populate_array(
    element: &mut ColumnIoData<'_>,
    colname: Option<&str>,
    mcx: Mcx<'_>,
    jsv: JsonbItem<'_>,
    isnull: &mut bool,
    mut escontext: Option<&mut ErrorSaveNode>,
) -> PgResult<Datum> {
    let element_type = element.typid;
    let mut ctx = PopulateArrayContext {
        element,
        astate: Some(arrayfuncs::init_array_result(mcx, element_type, true)?),
        colname,
        mcx,
        ndims: 0,
        dims: mcx::vec_with_capacity_in(mcx, 1)?,
        sizes: mcx::vec_with_capacity_in(mcx, 1)?,
    };

    if !populate_array_dim_jsonb(&mut ctx, jsv, 1, escontext.as_deref_mut())? {
        *isnull = true;
        return Ok(Datum::null());
    }
    ctx.dims[0] = ctx.sizes[0];

    debug_assert!(ctx.ndims > 0);
    let mut lbs: PgVec<'_, i32> = mcx::vec_with_capacity_in(mcx, ctx.ndims as usize)?;
    for _ in 0..ctx.ndims {
        lbs.push(1);
    }
    let image = arrayfuncs::make_md_array_result(
        mcx,
        ctx.astate.as_ref().expect("astate initialized"),
        ctx.ndims,
        &ctx.dims,
        &lbs,
    )?;
    *isnull = false;
    Ok(image_datum(image))
}

// C JsonbUnquote (jsonb.c): quote-stripped text of a scalar, else the
// serialized container. No trailing NUL.
fn jsonb_unquote<'r>(mcx: Mcx<'r>, payload: &[u8]) -> PgResult<PgVec<'r, u8>> {
    match crate::io::extract_scalar(payload) {
        Some(JsonbItem::String(s)) => {
            let mut v = mcx::vec_with_capacity_in(mcx, s.len())?;
            mcx::vec_append_bytes(&mut v, s)?;
            Ok(v)
        }
        Some(JsonbItem::Bool(b)) => {
            let s: &[u8] = if b { b"true" } else { b"false" };
            let mut v = mcx::vec_with_capacity_in(mcx, s.len())?;
            mcx::vec_append_bytes(&mut v, s)?;
            Ok(v)
        }
        Some(JsonbItem::Numeric(image)) => {
            let mut scratch = alloc::vec::Vec::new();
            adt_numeric::numeric_out_into(adt_numeric::Num::from_payload(&image[4..]), &mut scratch);
            let mut v = mcx::vec_with_capacity_in(mcx, scratch.len())?;
            mcx::vec_append_bytes(&mut v, &scratch)?;
            Ok(v)
        }
        Some(JsonbItem::Null) => {
            let mut v = mcx::vec_with_capacity_in(mcx, 4)?;
            mcx::vec_append_bytes(&mut v, b"null")?;
            Ok(v)
        }
        Some(other) => panic!("unrecognized jsonb value type {}", other.type_ord()),
        None => {
            let mut out = StringInfo::new_in(mcx)?;
            crate::io::jsonb_to_cstring_into(mcx, &mut out, payload, payload.len() + 4)?;
            Ok(out.into_vec())
        }
    }
}

