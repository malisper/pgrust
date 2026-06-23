//! Shared helpers used across the jsonfuncs modules: `JsonbValueAsText`
//! (jsonfuncs.c:1805) and the internal-context name.

use ::adt_jsonb::JsonbToCString;
use ::adt_numeric::io::numeric_out;
use ::utils_error::ereport;
use ::types_error::error::ERROR;
use mcx::{Mcx, PgVec};
use ::types_error::PgResult;
use types_jsonb::jsonb_util::{JsonbValue, JsonbValueData};
use ::types_jsonb::jsonb::jbvType;

/// The internal context name charged for this crate's one-shot text-output
/// buffers, matching the C source unit name.
pub const MCTX_NAME: &str = "backend-utils-adt-jsonfuncs";

/// `VARDATA_ANY(ptr)` — the payload (`JsonbContainer` root / `json` text body)
/// after the varlena header of an arg-sourced `jsonb`/`json` image: skip ONE
/// header byte for a short (1-byte, low-bit-set) header, else `VARHDRSZ` (4).
/// A small stored value reaches an fmgr arg verbatim (the EEOP_FUNCEXPR boundary
/// does not detoast/unpack), so once `SHORT_VARLENA_PACKING` is on a fixed 4-byte
/// strip would land three bytes into the payload (corrupt container / json body).
/// Behavior-preserving while the flag is off (every stored value is 4-byte).
#[inline]
pub fn vardata_any(image: &[u8]) -> &[u8] {
    const VARHDRSZ: usize = 4;
    match image.first() {
        Some(&h) if h != 0x01 && (h & 0x01) == 0x01 => &image[1..],
        Some(_) if image.len() >= VARHDRSZ => &image[VARHDRSZ..],
        _ => &[],
    }
}

// `enum` (jsonfuncs.h:25) — the GIN/iterate value-kind selector flags shared by
// `parse_jsonb_index_flags` (setops) and `iterate_*`/`transform_*` (iterate).
/// `jtiKey = 0x01`.
pub const jtiKey: u32 = 0x01;
/// `jtiString = 0x02`.
pub const jtiString: u32 = 0x02;
/// `jtiNumeric = 0x04`.
pub const jtiNumeric: u32 = 0x04;
/// `jtiBool = 0x08`.
pub const jtiBool: u32 = 0x08;
/// `jtiAll = jtiKey | jtiString | jtiNumeric | jtiBool`.
pub const jtiAll: u32 = jtiKey | jtiString | jtiNumeric | jtiBool;

/// `JsonbValueAsText(JsonbValue *v)` (jsonfuncs.c:1805): render a scalar (or
/// binary) `JsonbValue` to its `text` byte representation. Returns `None` for
/// the C `NULL` (a `jbvNull`); otherwise the text bytes (no varlena header,
/// as the caller wraps with `cstring_to_text*`).
pub fn JsonbValueAsText<'mcx>(mcx: Mcx<'mcx>, v: &JsonbValue) -> PgResult<Option<PgVec<'mcx, u8>>> {
    match (&v.typ, &v.val) {
        (jbvType::jbvNull, _) => Ok(None),

        (jbvType::jbvBool, JsonbValueData::Bool(b)) => {
            // v->val.boolean ? cstring_to_text_with_len("true", 4)
            //                : cstring_to_text_with_len("false", 5)
            let s: &[u8] = if *b { b"true" } else { b"false" };
            let mut out = ::mcx::vec_with_capacity_in::<u8>(mcx, s.len())?;
            out.extend_from_slice(s);
            Ok(Some(out))
        }

        (jbvType::jbvString, JsonbValueData::String(s)) => {
            // cstring_to_text_with_len(v->val.string.val, v->val.string.len)
            let mut out = ::mcx::vec_with_capacity_in::<u8>(mcx, s.len())?;
            out.extend_from_slice(s);
            Ok(Some(out))
        }

        (jbvType::jbvNumeric, JsonbValueData::Numeric(num)) => {
            // cstr = DirectFunctionCall1(numeric_out, PointerGetDatum(v->val.numeric));
            // return cstring_to_text(DatumGetCString(cstr));
            let s = numeric_out(mcx, num)?;
            let bytes = s.as_bytes();
            let mut out = ::mcx::vec_with_capacity_in::<u8>(mcx, bytes.len())?;
            out.extend_from_slice(bytes);
            Ok(Some(out))
        }

        (jbvType::jbvBinary, JsonbValueData::Binary { len, data, .. }) => {
            // initStringInfo(&jtext);
            // JsonbToCString(&jtext, v->val.binary.data, v->val.binary.len);
            // return cstring_to_text_with_len(jtext.data, jtext.len);
            let text = JsonbToCString(mcx, data, *len)?;
            Ok(Some(text))
        }

        _ => {
            // elog(ERROR, "unrecognized jsonb type: %d", (int) v->type);
            Err(ereport(ERROR)
                .errmsg_internal(alloc::format!("unrecognized jsonb type: {}", v.typ as i32))
                .into_error())
        }
    }
}

extern crate alloc;
