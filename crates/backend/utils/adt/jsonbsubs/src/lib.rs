//! `utils/adt/jsonbsubs.c` — subscripting support functions for `jsonb`.
//!
//! Ports the jsonb `SubscriptRoutines` execution callbacks installed by
//! `jsonb_exec_setup`: the primitive-backed FETCH / ASSIGN / OLD-fetch bodies
//! (`jsonb_subscript_fetch`, `jsonb_subscript_assign`,
//! `jsonb_subscript_fetch_old`). They run jsonb's element get/set
//! (`jsonb_get_element` / `jsonb_set_element`, jsonfuncs.c) over the canonical
//! [`DatumV`] container and a path of text payload byte strings.
//!
//! ## What lives where (mirror of arraysubs)
//!
//! Like the array subscripting bodies (arraysubs.c, homed in
//! `backend-utils-adt-arrayfuncs`), the raw `void (*)(ExprState *,
//! ExprEvalStep *, ExprContext *)` callback shape can not thread the owned
//! `Mcx`/EState/result cell. So:
//!
//!   * `jsonb_subscript_transform` (the parse-analysis method) — its body is the
//!     subscript-coercion logic in `transformContainerSubscripts`'s
//!     handler hook; it depends on the parser (`transformExpr`, `coerce_type`,
//!     `can_coerce_type`) which sits above this `utils/adt` unit, so it is
//!     reached through the `subscripting_transform` parser seam rather than
//!     living here.
//!   * `jsonb_exec_setup` (workspace allocation + method-discriminant
//!     selection) and `jsonb_subscript_check_subscripts` (the INT4→text path
//!     coercion + `expectArray` determination) are executor-side compilation /
//!     interpreter logic; they need no jsonb primitive, so they live in the
//!     executor (`execExpr` / `execExprInterp`) which holds the
//!     `SubscriptingRefState` and the result-cell arena.
//!   * the three primitive-backed bodies ported here are reached through the
//!     `backend-utils-adt-jsonbsubs-seams` per-callback seams.
//!
//! ## `jsonb_subscript_handler` builtin registration
//!
//! `jsonb_subscript_handler(PG_FUNCTION_ARGS)` returns a pointer to a `static
//! const SubscriptRoutines` via `PG_RETURN_POINTER`. Its result type is
//! `internal`, which is unmarshalable at the current fmgr boundary, so the
//! builtin is intentionally NOT registered (mirror of `array_subscript_handler`
//! / `raw_array_subscript_handler`, which are likewise unregistered). The
//! routine is a pure constant-returning function; its `SubscriptRoutines` value
//! is resolved directly in `lsyscache::getSubscriptingRoutines` (the
//! `SubscriptHandler::Jsonb` arm), exactly as PostgreSQL would obtain it through
//! `OidFunctionCall0(F_JSONB_SUBSCRIPT_HANDLER)`.

use mcx::Mcx;
use types_error::{PgError, PgResult};
use types_jsonb::jsonb_util::{JsonbValue, JsonbValueData};
use types_jsonb::jsonb::jbvType;
use types_tuple::heaptuple::Datum as DatumV;

use jsonb_util::{JsonbToJsonbValue, JsonbValueToJsonb};
use adt_jsonfuncs::getfield::jsonb_get_element;
use adt_jsonfuncs::setops::jsonb_set_element;

/// `DatumGetJsonbP(d)` (jsonb.h): `(Jsonb *) PG_DETOAST_DATUM(d)`. Read the
/// by-reference jsonb varlena image off the canonical lane and detoast it via
/// `pg_detoast_datum_packed` (the `PG_DETOAST_DATUM_PACKED` step every
/// `PG_GETARG_JSONB_P` / `DatumGetJsonbP` applies): a value stored
/// compressed-inline or out-of-line external arrives here still toasted (the
/// subscripting executor reads `*op->resvalue` straight off the slot, which the
/// central fmgr dispatch's per-arg detoast never touched). A plain value is
/// returned verbatim.
fn datum_get_jsonb_p<'mcx>(
    mcx: Mcx<'mcx>,
    container: &DatumV<'mcx>,
) -> PgResult<mcx::PgVec<'mcx, u8>> {
    detoast_seams::pg_detoast_datum_packed::call(
        mcx,
        container.as_ref_bytes(),
    )
}

/// `jsonb_subscript_fetch` (jsonbsubs.c): evaluate a SubscriptingRef fetch for a
/// jsonb element.
///
/// ```c
/// jsonbSource = DatumGetJsonbP(*op->resvalue);
/// *op->resvalue = jsonb_get_element(jsonbSource, workspace->index,
///                                   sbsrefstate->numupper, op->resnull, false);
/// ```
///
/// The source container is known not NULL (fetch_strict). `path` is the text
/// payload of each `workspace->index[i]` (the converted subscripts). Returns
/// `(element, isnull)`.
pub fn jsonb_subscript_fetch<'mcx>(
    mcx: Mcx<'mcx>,
    container: DatumV<'mcx>,
    path: &[Vec<u8>],
) -> PgResult<(DatumV<'mcx>, bool)> {
    // Assert(!(*op->resnull)); — guaranteed by fetch_strict; caller has read a
    // non-NULL container.
    // jsonbSource = DatumGetJsonbP(*op->resvalue);
    let jsonb_source = datum_get_jsonb_p(mcx, &container)?;
    let path_refs: Vec<&[u8]> = path.iter().map(|e| e.as_slice()).collect();
    // jsonb_get_element(..., as_text = false): None == *op->resnull = true.
    match jsonb_get_element(mcx, &jsonb_source, &path_refs, false)? {
        Some(bytes) => Ok((DatumV::ByRef(bytes), false)),
        None => Ok((DatumV::null(), true)),
    }
}

/// `jsonb_subscript_assign` (jsonbsubs.c): evaluate a SubscriptingRef assignment
/// for a jsonb element, returning the new whole jsonb value (never NULL).
///
/// ```c
/// if (sbsrefstate->replacenull) replacevalue.type = jbvNull;
/// else JsonbToJsonbValue(DatumGetJsonbP(sbsrefstate->replacevalue), &replacevalue);
/// if (*op->resnull) {
///     /* set up an empty jbvArray (if expectArray) or jbvObject */
///     jsonbSource = JsonbValueToJsonb(&newSource);
///     *op->resnull = false;
/// } else
///     jsonbSource = DatumGetJsonbP(*op->resvalue);
/// *op->resvalue = jsonb_set_element(jsonbSource, workspace->index,
///                                   sbsrefstate->numupper, &replacevalue);
/// ```
pub fn jsonb_subscript_assign<'mcx>(
    mcx: Mcx<'mcx>,
    container: DatumV<'mcx>,
    container_null: bool,
    path: &[Vec<u8>],
    replacevalue: DatumV<'mcx>,
    replacenull: bool,
    expect_array: bool,
) -> PgResult<(DatumV<'mcx>, bool)> {
    // JsonbValue replacevalue;
    let replace_jbv: JsonbValue = if replacenull {
        // replacevalue.type = jbvNull;
        JsonbValue::null()
    } else {
        // JsonbToJsonbValue(DatumGetJsonbP(sbsrefstate->replacevalue), &replacevalue);
        let mut v = JsonbValue::null();
        let replace_bytes = datum_get_jsonb_p(mcx, &replacevalue)?;
        JsonbToJsonbValue(&replace_bytes, &mut v)?;
        v
    };

    // jsonbSource: either an empty source (NULL input) or the input container.
    // jsonb_set_element takes the full on-disk varlena bytes.
    let source_bytes: Vec<u8> = if container_null {
        // To avoid surprising results, set up an empty jsonb array if an array
        // is expected (first subscript is integer), otherwise a jsonb object.
        let new_source = if expect_array {
            // newSource.type = jbvArray; nElems = 0; rawScalar = false;
            JsonbValue {
                typ: jbvType::jbvArray,
                val: JsonbValueData::Array {
                    elems: Vec::new(),
                    raw_scalar: false,
                },
            }
        } else {
            // newSource.type = jbvObject; nPairs = 0;
            JsonbValue {
                typ: jbvType::jbvObject,
                val: JsonbValueData::Object(Vec::new()),
            }
        };
        // jsonbSource = JsonbValueToJsonb(&newSource); *op->resnull = false;
        JsonbValueToJsonb(mcx, &new_source)?.as_slice().to_vec()
    } else {
        // jsonbSource = DatumGetJsonbP(*op->resvalue);
        datum_get_jsonb_p(mcx, &container)?.as_slice().to_vec()
    };

    // *op->resvalue = jsonb_set_element(jsonbSource, workspace->index,
    //                                   sbsrefstate->numupper, &replacevalue);
    let result = jsonb_set_element(mcx, &source_bytes, path, &replace_jbv)?;
    // The result is never NULL, so no need to change *op->resnull.
    Ok((DatumV::ByRef(result), false))
}

/// `jsonb_subscript_fetch_old` (jsonbsubs.c): compute the old jsonb element for
/// a SubscriptingRef assignment whose new-value subexpression contains a nested
/// SubscriptingRef or FieldStore. Same as the regular fetch, but handles a NULL
/// jsonb (yields NULL) and the result is stored into the
/// `SubscriptingRefState`'s prevvalue/prevnull by the caller.
///
/// ```c
/// if (*op->resnull) { prevvalue = (Datum) 0; prevnull = true; }
/// else {
///     Jsonb *jsonbSource = DatumGetJsonbP(*op->resvalue);
///     prevvalue = jsonb_get_element(jsonbSource, sbsrefstate->upperindex,
///                                   sbsrefstate->numupper, &prevnull, false);
/// }
/// ```
pub fn jsonb_subscript_fetch_old<'mcx>(
    mcx: Mcx<'mcx>,
    container: DatumV<'mcx>,
    container_null: bool,
    path: &[Vec<u8>],
) -> PgResult<(DatumV<'mcx>, bool)> {
    if container_null {
        // whole jsonb is null, so any element is too
        return Ok((DatumV::null(), true));
    }
    // Jsonb *jsonbSource = DatumGetJsonbP(*op->resvalue);
    let jsonb_source = datum_get_jsonb_p(mcx, &container)?;
    let path_refs: Vec<&[u8]> = path.iter().map(|e| e.as_slice()).collect();
    match jsonb_get_element(mcx, &jsonb_source, &path_refs, false)? {
        Some(bytes) => Ok((DatumV::ByRef(bytes), false)),
        None => Ok((DatumV::null(), true)),
    }
}

/// Sentinel kept to document the unported / intentionally-unregistered surface:
/// `jsonb_subscript_handler` returns `internal` (a pointer to a `static const
/// SubscriptRoutines`), which can not cross the fmgr marshalling boundary, so
/// the builtin is not registered. The handler's effect (its constant
/// `SubscriptRoutines`) is realized in
/// `backend-utils-cache-lsyscache::getSubscriptingRoutines`.
#[allow(dead_code)]
const JSONB_SUBSCRIPT_HANDLER_UNREGISTERED: () = ();

/// Helper used by the null-subscript-in-assignment error path mirror (kept for
/// symmetry; the NULL-subscript check is performed in the interpreter's
/// `jsonb_subscript_check_subscripts` body before any owner seam is reached).
#[allow(dead_code)]
fn jsonb_subscript_null_assignment_error() -> PgError {
    PgError::error("jsonb subscript in assignment must not be null")
        .with_sqlstate(types_error::ERRCODE_NULL_VALUE_NOT_ALLOWED)
}

/// Install this unit's per-callback execution seams. Invoked from the global
/// seam-init wiring once the crate is part of the build.
pub fn init_seams() {
    jsonbsubs_seams::jsonb_subscript_fetch::set(jsonb_subscript_fetch);
    jsonbsubs_seams::jsonb_subscript_assign::set(jsonb_subscript_assign);
    jsonbsubs_seams::jsonb_subscript_fetch_old::set(jsonb_subscript_fetch_old);
}
