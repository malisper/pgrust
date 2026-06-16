//! Shared helpers used across the jsonfuncs modules: `JsonbValueAsText`
//! (jsonfuncs.c:1805) and the internal-context name.

use backend_utils_adt_jsonb::JsonbToCString;
use backend_utils_adt_numeric::io::numeric_out;
use backend_utils_error::ereport;
use types_error::error::ERROR;
use mcx::{Mcx, PgVec};
use types_error::PgResult;
use types_jsonb::backend_utils_adt_jsonb_util::{JsonbValue, JsonbValueData};
use types_jsonb::jsonb::jbvType;

/// The internal context name charged for this crate's one-shot text-output
/// buffers, matching the C source unit name.
pub const MCTX_NAME: &str = "backend-utils-adt-jsonfuncs";

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
            let mut out = mcx::vec_with_capacity_in::<u8>(mcx, s.len())?;
            out.extend_from_slice(s);
            Ok(Some(out))
        }

        (jbvType::jbvString, JsonbValueData::String(s)) => {
            // cstring_to_text_with_len(v->val.string.val, v->val.string.len)
            let mut out = mcx::vec_with_capacity_in::<u8>(mcx, s.len())?;
            out.extend_from_slice(s);
            Ok(Some(out))
        }

        (jbvType::jbvNumeric, JsonbValueData::Numeric(num)) => {
            // cstr = DirectFunctionCall1(numeric_out, PointerGetDatum(v->val.numeric));
            // return cstring_to_text(DatumGetCString(cstr));
            let s = numeric_out(mcx, num)?;
            let bytes = s.as_bytes();
            let mut out = mcx::vec_with_capacity_in::<u8>(mcx, bytes.len())?;
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
