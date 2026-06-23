//! SPI result codes (`spi.h`) and `SPI_result_code_string` (`spi.c`).

use mcx::{Mcx, PgString};
use ::types_error::PgResult;

// --- SPI result/error codes (`include/executor/spi.h`) ---
pub const SPI_ERROR_CONNECT: i32 = -1;
pub const SPI_ERROR_COPY: i32 = -2;
pub const SPI_ERROR_OPUNKNOWN: i32 = -3;
pub const SPI_ERROR_UNCONNECTED: i32 = -4;
pub const SPI_ERROR_CURSOR: i32 = -5; // not used anymore
pub const SPI_ERROR_ARGUMENT: i32 = -6;
pub const SPI_ERROR_PARAM: i32 = -7;
pub const SPI_ERROR_TRANSACTION: i32 = -8;
pub const SPI_ERROR_NOATTRIBUTE: i32 = -9;
pub const SPI_ERROR_NOOUTFUNC: i32 = -10;
pub const SPI_ERROR_TYPUNKNOWN: i32 = -11;
pub const SPI_ERROR_REL_DUPLICATE: i32 = -12;
pub const SPI_ERROR_REL_NOT_FOUND: i32 = -13;

pub const SPI_OK_CONNECT: i32 = 1;
pub const SPI_OK_FINISH: i32 = 2;
pub const SPI_OK_FETCH: i32 = 3;
pub const SPI_OK_UTILITY: i32 = 4;
pub const SPI_OK_SELECT: i32 = 5;
pub const SPI_OK_SELINTO: i32 = 6;
pub const SPI_OK_INSERT: i32 = 7;
pub const SPI_OK_DELETE: i32 = 8;
pub const SPI_OK_UPDATE: i32 = 9;
pub const SPI_OK_CURSOR: i32 = 10;
pub const SPI_OK_INSERT_RETURNING: i32 = 11;
pub const SPI_OK_DELETE_RETURNING: i32 = 12;
pub const SPI_OK_UPDATE_RETURNING: i32 = 13;
pub const SPI_OK_REWRITTEN: i32 = 14;
pub const SPI_OK_REL_REGISTER: i32 = 15;
pub const SPI_OK_REL_UNREGISTER: i32 = 16;
pub const SPI_OK_TD_REGISTER: i32 = 17;
pub const SPI_OK_MERGE: i32 = 18;
pub const SPI_OK_MERGE_RETURNING: i32 = 19;

// `SPI_OPT_NONATOMIC` flag for `SPI_connect_ext` (`spi.h`).
pub const SPI_OPT_NONATOMIC: i32 = 1 << 0;

/// `SPI_result_code_string(int code)` (`spi.c`).
///
/// Returns the human-readable name of an SPI result/error code, or
/// `"Unrecognized SPI code %d"` for an unknown one. C returns a static `char *`
/// / a static 64-byte `buf`; here the (owned, `'static`) string is copied into
/// the caller's `mcx`, mirroring the seam contract.
pub fn SPI_result_code_string(code: i32) -> &'static str {
    match code {
        SPI_ERROR_CONNECT => "SPI_ERROR_CONNECT",
        SPI_ERROR_COPY => "SPI_ERROR_COPY",
        SPI_ERROR_OPUNKNOWN => "SPI_ERROR_OPUNKNOWN",
        SPI_ERROR_UNCONNECTED => "SPI_ERROR_UNCONNECTED",
        SPI_ERROR_ARGUMENT => "SPI_ERROR_ARGUMENT",
        SPI_ERROR_PARAM => "SPI_ERROR_PARAM",
        SPI_ERROR_TRANSACTION => "SPI_ERROR_TRANSACTION",
        SPI_ERROR_NOATTRIBUTE => "SPI_ERROR_NOATTRIBUTE",
        SPI_ERROR_NOOUTFUNC => "SPI_ERROR_NOOUTFUNC",
        SPI_ERROR_TYPUNKNOWN => "SPI_ERROR_TYPUNKNOWN",
        SPI_ERROR_REL_DUPLICATE => "SPI_ERROR_REL_DUPLICATE",
        SPI_ERROR_REL_NOT_FOUND => "SPI_ERROR_REL_NOT_FOUND",
        SPI_OK_CONNECT => "SPI_OK_CONNECT",
        SPI_OK_FINISH => "SPI_OK_FINISH",
        SPI_OK_FETCH => "SPI_OK_FETCH",
        SPI_OK_UTILITY => "SPI_OK_UTILITY",
        SPI_OK_SELECT => "SPI_OK_SELECT",
        SPI_OK_SELINTO => "SPI_OK_SELINTO",
        SPI_OK_INSERT => "SPI_OK_INSERT",
        SPI_OK_DELETE => "SPI_OK_DELETE",
        SPI_OK_UPDATE => "SPI_OK_UPDATE",
        SPI_OK_CURSOR => "SPI_OK_CURSOR",
        SPI_OK_INSERT_RETURNING => "SPI_OK_INSERT_RETURNING",
        SPI_OK_DELETE_RETURNING => "SPI_OK_DELETE_RETURNING",
        SPI_OK_UPDATE_RETURNING => "SPI_OK_UPDATE_RETURNING",
        SPI_OK_REWRITTEN => "SPI_OK_REWRITTEN",
        SPI_OK_REL_REGISTER => "SPI_OK_REL_REGISTER",
        SPI_OK_REL_UNREGISTER => "SPI_OK_REL_UNREGISTER",
        SPI_OK_TD_REGISTER => "SPI_OK_TD_REGISTER",
        SPI_OK_MERGE => "SPI_OK_MERGE",
        SPI_OK_MERGE_RETURNING => "SPI_OK_MERGE_RETURNING",
        // Unrecognized code: C does `sprintf(buf, "Unrecognized SPI code %d")`.
        // We cannot return an owned formatted &'static str, so the seam wrapper
        // below handles the dynamic case directly.
        _ => "Unrecognized SPI code",
    }
}

/// Seam body for `spi_result_code_string`: copy the code name into `mcx`,
/// formatting the unrecognized case exactly as C's `sprintf`.
pub(crate) fn spi_result_code_string_seam<'mcx>(
    mcx: Mcx<'mcx>,
    code: i32,
) -> PgResult<PgString<'mcx>> {
    let known = SPI_result_code_string(code);
    if known == "Unrecognized SPI code" {
        PgString::from_str_in(&alloc::format!("Unrecognized SPI code {code}"), mcx)
    } else {
        PgString::from_str_in(known, mcx)
    }
}

extern crate alloc;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn known_codes() {
        assert_eq!(SPI_result_code_string(SPI_OK_FINISH), "SPI_OK_FINISH");
        assert_eq!(SPI_result_code_string(SPI_OK_SELECT), "SPI_OK_SELECT");
        assert_eq!(
            SPI_result_code_string(SPI_ERROR_UNCONNECTED),
            "SPI_ERROR_UNCONNECTED"
        );
        assert_eq!(
            SPI_result_code_string(SPI_OK_MERGE_RETURNING),
            "SPI_OK_MERGE_RETURNING"
        );
    }

    #[test]
    fn unrecognized_code() {
        assert_eq!(SPI_result_code_string(9999), "Unrecognized SPI code");
    }
}
