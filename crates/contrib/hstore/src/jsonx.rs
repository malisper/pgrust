//! hstore_to_json[_loose] / hstore_to_jsonb[_loose] (hstore_io.c tail).

use datum::Datum;
use types_error::PgResult;
use types_fmgr::{FmgrInfo, FunctionCallInfoBaseData as Fcinfo};

use crate::arg_hstore;

// IsValidJsonNumber (common/jsonapi.c): json_lex_number over the whole
// token, leading '-' eaten by the caller.
pub(crate) fn is_valid_json_number(s: &[u8]) -> bool {
    if s.is_empty() {
        return false;
    }
    let s = if s[0] == b'-' { &s[1..] } else { s };
    let n = s.len();
    let mut i = 0usize;
    if i < n && s[i] == b'0' {
        i += 1;
    } else if i < n && (b'1'..=b'9').contains(&s[i]) {
        while i < n && s[i].is_ascii_digit() {
            i += 1;
        }
    } else {
        return false;
    }
    if i < n && s[i] == b'.' {
        i += 1;
        if i == n || !s[i].is_ascii_digit() {
            return false;
        }
        while i < n && s[i].is_ascii_digit() {
            i += 1;
        }
    }
    if i < n && matches!(s[i], b'e' | b'E') {
        i += 1;
        if i < n && matches!(s[i], b'+' | b'-') {
            i += 1;
        }
        if i == n || !s[i].is_ascii_digit() {
            return false;
        }
        while i < n && s[i].is_ascii_digit() {
            i += 1;
        }
    }
    i == n
}

fn to_json_common(fcinfo: &mut Fcinfo, loose: bool) -> PgResult<Datum> {
    // SAFETY: catalog arg is a non-null hstore varlena (strict fn).
    let hs = unsafe { arg_hstore(fcinfo, 0)? };
    let count = hs.count();
    let mcx = fcinfo.result_mcx();
    if count == 0 {
        return Ok(types_fmgr::varlena_result(varlena::cstring_to_text(mcx, b"{}")?));
    }
    let mut dst = stringinfo::StringInfo::new_in(mcx)?;
    dst.append_byte(b'{')?;
    for i in 0..count {
        adt_json::escape_json(&mut dst, hs.key(i))?;
        dst.append_bytes(b": ")?;
        if hs.val_isnull(i) {
            dst.append_bytes(b"null")?;
        } else {
            let val = hs.val(i);
            if loose && val == b"t" {
                dst.append_bytes(b"true")?;
            } else if loose && val == b"f" {
                dst.append_bytes(b"false")?;
            } else if loose && is_valid_json_number(val) {
                dst.append_bytes(val)?;
            } else {
                adt_json::escape_json(&mut dst, val)?;
            }
        }
        if i + 1 != count {
            dst.append_bytes(b", ")?;
        }
    }
    dst.append_byte(b'}')?;
    Ok(types_fmgr::varlena_result(varlena::cstring_to_text(
        mcx,
        dst.as_bytes(),
    )?))
}

pub fn fc_hstore_to_json(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    to_json_common(fcinfo, false)
}

pub fn fc_hstore_to_json_loose(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    to_json_common(fcinfo, true)
}

fn to_jsonb_common(fcinfo: &mut Fcinfo, loose: bool) -> PgResult<Datum> {
    use adt_jsonb::build::{JsonbBuildState, JsonbValue};
    // SAFETY: catalog arg is a non-null hstore varlena (strict fn).
    let hs = unsafe { arg_hstore(fcinfo, 0)? };
    let mcx = fcinfo.result_mcx();
    let mut ps = JsonbBuildState::new(mcx)?;
    ps.begin_object(false)?;
    for i in 0..hs.count() {
        ps.push_key(mcx::slice_in(mcx, hs.key(i))?.leak())?;
        let v: JsonbValue<'_> = if hs.val_isnull(i) {
            JsonbValue::Null
        } else {
            let val = hs.val(i);
            if loose && val == b"t" {
                JsonbValue::Bool(true)
            } else if loose && val == b"f" {
                JsonbValue::Bool(false)
            } else if loose && is_valid_json_number(val) {
                let s = core::str::from_utf8(val).expect("json number is ASCII");
                let img = adt_numeric::numeric_in(s, -1, None)?
                    .expect("numeric_in of a valid json number never soft-fails");
                JsonbValue::Numeric(mcx::slice_in(mcx, img.as_bytes())?.leak())
            } else {
                JsonbValue::String(mcx::slice_in(mcx, val)?.leak())
            }
        };
        ps.push_value(v);
    }
    let root = ps.end_object()?.expect("top-level object closes to a value");
    let img = adt_jsonb::build::convert_to_jsonb(mcx, &root)?;
    Ok(crate::ret_array(fcinfo, img))
}

pub fn fc_hstore_to_jsonb(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    to_jsonb_common(fcinfo, false)
}

pub fn fc_hstore_to_jsonb_loose(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    to_jsonb_common(fcinfo, true)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn json_number_grammar() {
        assert!(is_valid_json_number(b"12345"));
        assert!(is_valid_json_number(b"-1.234"));
        assert!(is_valid_json_number(b"2.345e+4"));
        assert!(is_valid_json_number(b"0.345e-4"));
        assert!(!is_valid_json_number(b"012345"));
        assert!(!is_valid_json_number(b"1."));
        assert!(!is_valid_json_number(b"2016-01-01"));
        assert!(!is_valid_json_number(b""));
        assert!(!is_valid_json_number(b"t"));
    }
}
