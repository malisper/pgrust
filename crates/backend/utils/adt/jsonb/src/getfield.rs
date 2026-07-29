//! jsonfuncs.c operator slice: -> ->> (object key + array index) and #> #>>
//! path extraction. Argument documents are walked by borrow; only the result
//! is materialized.

extern crate alloc;

use crate::build::item_to_jsonb_image;
use crate::container::*;
use crate::io::jsonb_to_cstring_into;
use adt_numeric::Num;
use mcx::{Mcx, PgVec};
use stringinfo::StringInfo;
use types_error::PgResult;

/// C: JsonbValueAsText. None = SQL NULL (jbvNull).
pub fn value_as_text<'mcx>(
    mcx: Mcx<'mcx>,
    v: &JsonbItem<'_>,
) -> PgResult<Option<datum::Varlena<'mcx>>> {
    let text = |bytes: &[u8]| varlena::cstring_to_text(mcx, bytes).map(Some);
    match v {
        JsonbItem::Null => Ok(None),
        JsonbItem::Bool(true) => text(b"true"),
        JsonbItem::Bool(false) => text(b"false"),
        JsonbItem::String(s) => text(s),
        JsonbItem::Numeric(image) => {
            let mut scratch = alloc::vec::Vec::new();
            adt_numeric::numeric_out_into(Num::from_payload(&image[4..]), &mut scratch);
            text(&scratch)
        }
        JsonbItem::Binary(c) => {
            let mut out = StringInfo::new_in(mcx)?;
            jsonb_to_cstring_into(mcx, &mut out, c, c.len() + 4)?;
            varlena::cstring_to_text(mcx, out.as_bytes()).map(Some)
        }
        _ => panic!("unrecognized jsonb type"),
    }
}

/// C: jsonb_object_field (`->` with a text key). None = SQL NULL.
pub fn object_field<'mcx>(
    mcx: Mcx<'mcx>,
    payload: &[u8],
    key: &[u8],
) -> PgResult<Option<PgVec<'mcx, u8>>> {
    if !container_is_object(payload) {
        return Ok(None);
    }
    match get_key_value(payload, key) {
        Some(v) => Ok(Some(item_to_jsonb_image(mcx, v)?)),
        None => Ok(None),
    }
}

/// C: jsonb_object_field_text (`->>` with a text key).
pub fn object_field_text<'mcx>(
    mcx: Mcx<'mcx>,
    payload: &[u8],
    key: &[u8],
) -> PgResult<Option<datum::Varlena<'mcx>>> {
    if !container_is_object(payload) {
        return Ok(None);
    }
    match get_key_value(payload, key) {
        Some(v) => value_as_text(mcx, &v),
        None => Ok(None),
    }
}

// C: the shared negative-subscript adjustment in jsonb_array_element*.
// pub for proofs/jsonb-probe (visibility-only; behavior unchanged).
pub fn adjust_element_index(payload: &[u8], element: i32) -> Option<u32> {
    if element < 0 {
        let nelements = container_size(payload);
        let abs = element.unsigned_abs();
        if abs > nelements {
            None
        } else {
            Some(nelements - abs)
        }
    } else {
        Some(element as u32)
    }
}

/// C: jsonb_array_element (`->` with an int subscript).
pub fn array_element<'mcx>(
    mcx: Mcx<'mcx>,
    payload: &[u8],
    element: i32,
) -> PgResult<Option<PgVec<'mcx, u8>>> {
    if !container_is_array(payload) {
        return Ok(None);
    }
    let Some(idx) = adjust_element_index(payload, element) else {
        return Ok(None);
    };
    match get_ith_value(payload, idx) {
        Some(v) => Ok(Some(item_to_jsonb_image(mcx, v)?)),
        None => Ok(None),
    }
}

/// C: jsonb_array_element_text (`->>` with an int subscript).
pub fn array_element_text<'mcx>(
    mcx: Mcx<'mcx>,
    payload: &[u8],
    element: i32,
) -> PgResult<Option<datum::Varlena<'mcx>>> {
    if !container_is_array(payload) {
        return Ok(None);
    }
    let Some(idx) = adjust_element_index(payload, element) else {
        return Ok(None);
    };
    match get_ith_value(payload, idx) {
        Some(v) => value_as_text(mcx, &v),
        None => Ok(None),
    }
}

pub enum PathResult<'mcx> {
    Null,
    Jsonb(PgVec<'mcx, u8>),
    Text(datum::Varlena<'mcx>),
    /// Empty path, non-scalar root: hand back the input document (C returns
    /// the input datum unchanged).
    Input,
}

/// C: jsonb_get_element. `path` elements are the text payloads of the #>
/// rhs array (nulls pre-screened by the caller, as in get_jsonb_path_all).
pub fn get_element<'mcx>(
    mcx: Mcx<'mcx>,
    payload: &[u8],
    path: &[&[u8]],
    as_text: bool,
) -> PgResult<PathResult<'mcx>> {
    let root_header = container_header(payload);
    let mut have_object = root_header & JB_FOBJECT != 0;
    let mut have_array = root_header & JB_FARRAY != 0 && root_header & JB_FSCALAR == 0;
    let mut jbvp: Option<JsonbItem<'_>> = None;
    let mut container = payload;

    if !have_object && !have_array && path.is_empty() {
        // Scalar root: extract the value to return below the loop.
        jbvp = get_ith_value(container, 0);
    }

    if path.is_empty() && jbvp.is_none() {
        if as_text {
            let mut out = StringInfo::new_in(mcx)?;
            jsonb_to_cstring_into(mcx, &mut out, container, payload.len() + 4)?;
            return Ok(PathResult::Text(varlena::cstring_to_text(
                mcx,
                out.as_bytes(),
            )?));
        }
        return Ok(PathResult::Input);
    }

    for (i, subscr) in path.iter().enumerate() {
        let v = if have_object {
            get_key_value(container, subscr)
        } else if have_array {
            let Ok(text) = core::str::from_utf8(subscr) else {
                return Ok(PathResult::Null);
            };
            // C: strtoint — leading whitespace allowed, trailing junk is not.
            let Ok(lindex) = text.trim_ascii_start().parse::<i64>() else {
                return Ok(PathResult::Null);
            };
            // C: strtoint + ERANGE check — out-of-int-range indexes are null.
            if lindex > i32::MAX as i64 || lindex < i32::MIN as i64 {
                return Ok(PathResult::Null);
            }
            let lindex = lindex as i32;
            let index = if lindex >= 0 {
                lindex as u32
            } else {
                let nelements = container_size(container);
                debug_assert!(container_is_array(container));
                if lindex == i32::MIN || lindex.unsigned_abs() > nelements {
                    return Ok(PathResult::Null);
                }
                nelements - lindex.unsigned_abs()
            };
            get_ith_value(container, index)
        } else {
            // Scalar mid-path: extraction yields null.
            return Ok(PathResult::Null);
        };

        let Some(v) = v else {
            return Ok(PathResult::Null);
        };
        if i == path.len() - 1 {
            jbvp = Some(v);
            break;
        }
        match v {
            JsonbItem::Binary(c) => {
                container = c;
                have_object = container_is_object(c);
                have_array = container_is_array(c);
                debug_assert!(!container_is_scalar(c));
            }
            scalar => {
                // Scalar mid-path: the next iteration's else arm yields null.
                debug_assert!(scalar.is_scalar());
                have_object = false;
                have_array = false;
            }
        }
    }

    let jbvp = jbvp.expect("path walk ended without a value");
    if as_text {
        match jbvp {
            JsonbItem::Null => Ok(PathResult::Null),
            v => match value_as_text(mcx, &v)? {
                Some(t) => Ok(PathResult::Text(t)),
                None => Ok(PathResult::Null),
            },
        }
    } else {
        Ok(PathResult::Jsonb(item_to_jsonb_image(mcx, jbvp)?))
    }
}
