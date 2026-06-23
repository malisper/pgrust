//! `json_categorize_type` (jsonfuncs.c:5991-6098) — type classification for
//! json/jsonb output. json's cycle partner; installed as the
//! `backend-utils-adt-jsonfuncs-seams::categorize_type` inward seam.

use lsyscache::type_::{
    get_base_type, get_element_type, get_type_output_info, type_is_rowtype,
};
use coerce_seams::{find_coercion_pathway_explicit, CoercionPathType};
use types_core::{Oid, OidIsValid};
use types_error::PgResult;
use types_json::JsonTypeCategory;
use types_tuple::heaptuple::{
    ANYARRAYOID, ANYCOMPATIBLEARRAYOID, BOOLOID, DATEOID, FLOAT4OID, FLOAT8OID, INT2OID, INT4OID,
    INT8OID, JSONBOID, JSONOID, NUMERICOID, RECORDARRAYOID, TIMESTAMPOID, TIMESTAMPTZOID,
};

// Well-known output-function OIDs (utils/fmgroids.h, generated from
// pg_proc.dat). Referenced by `json_categorize_type`.
/// `F_BOOLOUT`.
pub const F_BOOLOUT: Oid = 1243;
/// `F_DATE_OUT`.
pub const F_DATE_OUT: Oid = 1085;
/// `F_TIMESTAMP_OUT`.
pub const F_TIMESTAMP_OUT: Oid = 1313;
/// `F_TIMESTAMPTZ_OUT`.
pub const F_TIMESTAMPTZ_OUT: Oid = 1151;
/// `F_ARRAY_OUT`.
pub const F_ARRAY_OUT: Oid = 751;
/// `F_RECORD_OUT`.
pub const F_RECORD_OUT: Oid = 2291;

// Text-output functions selected by the fast escape path in
// `datum_to_json_internal`.
/// `F_TEXTOUT`.
pub const F_TEXTOUT: Oid = 47;
/// `F_VARCHAROUT`.
pub const F_VARCHAROUT: Oid = 1046;
/// `F_BPCHAROUT`.
pub const F_BPCHAROUT: Oid = 1045;

/// `FirstNormalObjectId` (access/transam.h): OIDs below this are built-in.
const FIRST_NORMAL_OBJECT_ID: Oid = 16384;

/// `json_categorize_type` (jsonfuncs.c:5998): determine how to print values of
/// `typoid` in `datum_to_json(b)`. Returns the [`JsonTypeCategory`] and the
/// type's output function OID (or, for [`JsonTypeCategory::JSONTYPE_CAST`], the
/// type→JSON cast function OID).
pub fn json_categorize_type(typoid: Oid, is_jsonb: bool) -> PgResult<(JsonTypeCategory, Oid)> {
    // Look through any domain.
    let typoid = get_base_type(typoid)?;

    let outfuncoid: Oid;
    let tcategory: JsonTypeCategory;

    if typoid == BOOLOID {
        outfuncoid = F_BOOLOUT;
        tcategory = JsonTypeCategory::JSONTYPE_BOOL;
    } else if typoid == INT2OID
        || typoid == INT4OID
        || typoid == INT8OID
        || typoid == FLOAT4OID
        || typoid == FLOAT8OID
        || typoid == NUMERICOID
    {
        // getTypeOutputInfo(typoid, outfuncoid, &typisvarlena)
        outfuncoid = get_type_output_info(typoid)?.0;
        tcategory = JsonTypeCategory::JSONTYPE_NUMERIC;
    } else if typoid == DATEOID {
        outfuncoid = F_DATE_OUT;
        tcategory = JsonTypeCategory::JSONTYPE_DATE;
    } else if typoid == TIMESTAMPOID {
        outfuncoid = F_TIMESTAMP_OUT;
        tcategory = JsonTypeCategory::JSONTYPE_TIMESTAMP;
    } else if typoid == TIMESTAMPTZOID {
        outfuncoid = F_TIMESTAMPTZ_OUT;
        tcategory = JsonTypeCategory::JSONTYPE_TIMESTAMPTZ;
    } else if typoid == JSONOID {
        outfuncoid = get_type_output_info(typoid)?.0;
        tcategory = JsonTypeCategory::JSONTYPE_JSON;
    } else if typoid == JSONBOID {
        outfuncoid = get_type_output_info(typoid)?.0;
        tcategory = if is_jsonb {
            JsonTypeCategory::JSONTYPE_JSONB
        } else {
            JsonTypeCategory::JSONTYPE_JSON
        };
    } else {
        // default: check for arrays and composites.
        if OidIsValid(get_element_type(typoid)?.unwrap_or(0))
            || typoid == ANYARRAYOID
            || typoid == ANYCOMPATIBLEARRAYOID
            || typoid == RECORDARRAYOID
        {
            outfuncoid = F_ARRAY_OUT;
            tcategory = JsonTypeCategory::JSONTYPE_ARRAY;
        } else if type_is_rowtype(typoid)? {
            // includes RECORDOID
            outfuncoid = F_RECORD_OUT;
            tcategory = JsonTypeCategory::JSONTYPE_COMPOSITE;
        } else {
            // It's probably the general case. Look for a cast to json (not to
            // jsonb even if is_jsonb is true), if it's not built-in.
            let mut cat = JsonTypeCategory::JSONTYPE_OTHER;
            if typoid >= FIRST_NORMAL_OBJECT_ID {
                // find_coercion_pathway(JSONOID, typoid, COERCION_EXPLICIT, &castfunc)
                let (ctype, castfunc) = find_coercion_pathway_explicit::call(JSONOID, typoid)?;
                if ctype == CoercionPathType::Func && OidIsValid(castfunc) {
                    outfuncoid = castfunc;
                    cat = JsonTypeCategory::JSONTYPE_CAST;
                } else {
                    // non builtin type with no cast
                    outfuncoid = get_type_output_info(typoid)?.0;
                }
            } else {
                // any other builtin type
                outfuncoid = get_type_output_info(typoid)?.0;
            }
            tcategory = cat;
        }
    }

    Ok((tcategory, outfuncoid))
}

/// True if `outfuncoid` is one of `F_TEXTOUT`/`F_VARCHAROUT`/`F_BPCHAROUT`
/// (the fast text-escape path selector in `datum_to_json_internal`).
pub fn is_text_output_func(outfuncoid: Oid) -> bool {
    outfuncoid == F_TEXTOUT || outfuncoid == F_VARCHAROUT || outfuncoid == F_BPCHAROUT
}
