//! jsonbsubs.c primitive halves: jsonb_subscript_fetch / jsonb_subscript_assign
//! over jsonb_get_element / jsonb_set_element (jsonfuncs.c). The transform
//! lives in parse_expr::subscripts, the exec-state plumbing in execexpr.

use datum::{Datum, NullableDatum};
use mcx::Mcx;
use types_error::PgResult;

use crate::build::{convert_to_jsonb, ArenaVec, JsonbValue};
use crate::builtins::{image_result, root_item};
use crate::container::JsonbItem;
use crate::getfield::{get_element, PathResult};
use crate::mutate::{
    set_path, SetPathArgs, JB_PATH_CONSISTENT_POSITION, JB_PATH_CREATE, JB_PATH_FILL_GAPS,
};

// DatumGetJsonbP: borrow a 4B-uncompressed image in place; short varlenas get
// an aligned 4B copy (embedded numerics need 2-alignment), toast detoasts.
pub fn jsonb_datum_payload<'mcx>(mcx: Mcx<'mcx>, d: Datum) -> PgResult<&'mcx [u8]> {
    let p = d.as_usize() as *const u8;
    // SAFETY: a live varlena readable through its full VARSIZE_ANY; the
    // referent (slot/expression result) outlives the armed 'mcx evaluation.
    let image: &'mcx [u8] =
        unsafe { core::slice::from_raw_parts(p, types_tuple::varatt::varsize_any(p)) };
    if image[0] & 0x01 == 0x01 && image[0] != 0x01 {
        let payload = &image[1..];
        let mut v: mcx::PgVec<'mcx, u8> = mcx::vec_with_capacity_in(mcx, 4 + payload.len())?;
        mcx::vec_append_bytes(&mut v, &(((4 + payload.len()) as u32) << 2).to_ne_bytes())?;
        mcx::vec_append_bytes(&mut v, payload)?;
        let img: &'mcx [u8] = v.leak();
        return Ok(&img[4..]);
    }
    match varlena::open_image(mcx, image)? {
        varlena::VarPayload::Inline(b) => Ok(b),
        varlena::VarPayload::Detoasted(v) => Ok(&v.leak()[4..]),
    }
}

// DatumGetTextPP payload of one path subscript.
fn text_datum_payload<'mcx>(mcx: Mcx<'mcx>, d: Datum) -> PgResult<&'mcx [u8]> {
    let p = d.as_usize() as *const u8;
    // SAFETY: as jsonb_datum_payload.
    let image: &'mcx [u8] =
        unsafe { core::slice::from_raw_parts(p, types_tuple::varatt::varsize_any(p)) };
    match varlena::open_image(mcx, image)? {
        varlena::VarPayload::Inline(b) => Ok(b),
        varlena::VarPayload::Detoasted(v) => Ok(&v.leak()[4..]),
    }
}

// jsonb_get_element under jsonb_subscript_fetch: source and path are non-null.
pub fn subscript_fetch<'mcx>(
    mcx: Mcx<'mcx>,
    source: Datum,
    path: &[Datum],
) -> PgResult<NullableDatum> {
    let payload = jsonb_datum_payload(mcx, source)?;
    let mut p: mcx::PgVec<'mcx, &[u8]> = mcx::vec_with_capacity_in(mcx, path.len())?;
    for d in path {
        p.push(text_datum_payload(mcx, *d)?);
    }
    Ok(match get_element(mcx, payload, &p, false)? {
        PathResult::Null => NullableDatum::null(),
        PathResult::Jsonb(v) => NullableDatum { value: image_result(v), isnull: false },
        PathResult::Input => NullableDatum { value: source, isnull: false },
        PathResult::Text(_) => unreachable!("as_text=false"),
    })
}

// jsonb_subscript_assign: a null source becomes an empty array/object per
// expect_array; jsonb_set_element = setPath CREATE|FILL_GAPS|CONSISTENT_POSITION.
pub fn subscript_assign<'mcx>(
    mcx: Mcx<'mcx>,
    source: NullableDatum,
    expect_array: bool,
    path: &[Datum],
    replace: NullableDatum,
) -> PgResult<Datum> {
    let newval = if replace.isnull {
        JsonbItem::Null
    } else {
        root_item(jsonb_datum_payload(mcx, replace.value)?)
    };
    let payload: &[u8] = if source.isnull {
        let empty = if expect_array {
            JsonbValue::Array { elems: ArenaVec::with_capacity(mcx, 0)?, raw_scalar: false }
        } else {
            JsonbValue::Object { pairs: ArenaVec::with_capacity(mcx, 0)? }
        };
        // convert_to_jsonb returns a full varlena image; the container payload
        // starts past the 4-byte header.
        &convert_to_jsonb(mcx, &empty)?.leak()[4..]
    } else {
        jsonb_datum_payload(mcx, source.value)?
    };
    let mut p: mcx::PgVec<'mcx, Option<&[u8]>> = mcx::vec_with_capacity_in(mcx, path.len())?;
    for d in path {
        p.push(Some(text_datum_payload(mcx, *d)?));
    }
    let args = SetPathArgs {
        path: &p,
        newval: Some(newval),
        op_type: JB_PATH_CREATE | JB_PATH_FILL_GAPS | JB_PATH_CONSISTENT_POSITION,
    };
    Ok(image_result(set_path(mcx, payload, &args)?))
}
