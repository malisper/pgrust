//! Port of `src/backend/utils/mb/conversion_procs/utf8_and_big5/utf8_and_big5.c` —
//! the UTF-8 <-> local-encoding conversion procedures.
//!
//! The `PG_FUNCTION_ARGS` entry points become plain Rust functions taking the
//! source/destination encoding ids, the source bytes, and the `no_error` flag.
//! Each validates the encodings with `check_encoding_conversion_args`
//! (`CHECK_ENCODING_CONVERSION_ARGS`) and delegates to the shared radix-tree
//! engine (`UtfToLocal` / `LocalToUtf` in `conv_string_helpers`,
//! a 1:1 port of `conv.c`). The conversion tables are ported byte-for-byte from
//! the generated `Unicode/*.map` files (see `tables`).
//!
//! Like the C module, this crate owns no inward seams: the conversion procedures
//! are reached through the `pg_conversion` catalog, not a seam. `init_seams()` is
//! a no-op.

#![allow(non_snake_case)]
#![allow(clippy::result_large_err)]
#![allow(clippy::too_many_arguments)]

extern crate alloc;

mod tables;

use ::utils_error::PgResult;
use ::conv_string_helpers::{ConversionResult, LocalToUtf, UtfToLocal};
use ::conv_string_helpers::make_conversion_builtin;
use ::mbutils_seams::check_encoding_conversion_args;
use ::types_wchar::encoding::{pg_enc, PG_BIG5, PG_UTF8};

/// `big5_to_utf8` (utf8_and_big5.c): convert PG_BIG5 <-> PG_UTF8.
pub fn big5_to_utf8(
    src_encoding: pg_enc,
    dest_encoding: pg_enc,
    src: &[u8],
    no_error: bool,
) -> PgResult<ConversionResult> {
    check_encoding_conversion_args::call(src_encoding, dest_encoding, src.len() as i32, PG_BIG5, PG_UTF8)?;
    LocalToUtf(
        src,
        Some(&tables::big5_to_unicode_tree()),
        &[],
        None,
        PG_BIG5,
        no_error,
    )
}
/// `utf8_to_big5` (utf8_and_big5.c): convert PG_UTF8 <-> PG_BIG5.
pub fn utf8_to_big5(
    src_encoding: pg_enc,
    dest_encoding: pg_enc,
    src: &[u8],
    no_error: bool,
) -> PgResult<ConversionResult> {
    check_encoding_conversion_args::call(src_encoding, dest_encoding, src.len() as i32, PG_UTF8, PG_BIG5)?;
    UtfToLocal(
        src,
        Some(&tables::big5_from_unicode_tree()),
        &[],
        None,
        PG_BIG5,
        no_error,
    )
}

/// This crate owns no inward seams (conversions are dispatched via the
/// `pg_conversion` catalog, mirroring the C module).
pub fn init_seams() {
    fmgr_core::register_builtins_native([
        make_conversion_builtin(4352, "big5_to_utf8", big5_to_utf8),
        make_conversion_builtin(4353, "utf8_to_big5", utf8_to_big5),
    ]);
}

#[cfg(test)]
mod tests;
