// String-length lane vocabulary (stringfunc-offsets,
// docs/design/stringfunc-offsets.md): length()/char_length()/octet_length()
// over a text Var answered from the staged lane representation — once per
// dict code on dict windows, per selected row off the inline varlena header
// on raw windows — through the SAME production counting helpers the per-row
// drive calls, so parity is by construction. Non-inline images (never
// published by cbstore lanes) demote the batch to the per-row program.
use datum::Datum;
use types_core::Oid;
use types_error::PgResult;

pub const TEXTLEN_OCTET: u8 = 1;
pub const TEXTLEN_CHAR: u8 = 2;

const F_TEXTLEN: [Oid; 4] = [1257, 1317, 1369, 1381];
const F_TEXTOCTETLEN: Oid = 1374;

/// Admitted length-family fn oid -> mode; None refuses. textlen is
/// CHARACTER count (text_length handles every server encoding), so no
/// encoding gate is needed.
pub fn lane_textlen_mode(fn_oid: Oid) -> Option<u8> {
    if fn_oid == F_TEXTOCTETLEN {
        Some(TEXTLEN_OCTET)
    } else if F_TEXTLEN.contains(&fn_oid) {
        Some(TEXTLEN_CHAR)
    } else {
        None
    }
}

/// The production fc_textlen/fc_textoctetlen result over an inline payload.
pub fn lane_textlen_eval(payload: &[u8], mode: u8) -> PgResult<i32> {
    if mode == TEXTLEN_OCTET {
        Ok(varlena::textoctetlen(payload))
    } else {
        varlena::text_length(payload)
    }
}

/// Payload of an inline text datum; None = compressed/external (demote).
pub fn lane_text_payload<'a>(d: Datum) -> Option<&'a [u8]> {
    crate::dict::inline_varlena_payload(d)
}
