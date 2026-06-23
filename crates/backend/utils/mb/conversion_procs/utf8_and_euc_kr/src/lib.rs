//! Port of `src/backend/utils/mb/conversion_procs/utf8_and_euc_kr/utf8_and_euc_kr.c` —
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

mod fmgr_builtins;
mod tables;

use ::utils_error::PgResult;
use conv_string_helpers::{ConversionResult, LocalToUtf, UtfToLocal};
use ::mbutils_seams::check_encoding_conversion_args;
use ::types_wchar::encoding::{pg_enc, PG_EUC_KR, PG_UTF8};

/// `euc_kr_to_utf8` (utf8_and_euc_kr.c): convert PG_EUC_KR <-> PG_UTF8.
pub fn euc_kr_to_utf8(
    src_encoding: pg_enc,
    dest_encoding: pg_enc,
    src: &[u8],
    no_error: bool,
) -> PgResult<ConversionResult> {
    check_encoding_conversion_args::call(src_encoding, dest_encoding, src.len() as i32, PG_EUC_KR, PG_UTF8)?;
    LocalToUtf(
        src,
        Some(&tables::euc_kr_to_unicode_tree()),
        &[],
        None,
        PG_EUC_KR,
        no_error,
    )
}
/// `utf8_to_euc_kr` (utf8_and_euc_kr.c): convert PG_UTF8 <-> PG_EUC_KR.
pub fn utf8_to_euc_kr(
    src_encoding: pg_enc,
    dest_encoding: pg_enc,
    src: &[u8],
    no_error: bool,
) -> PgResult<ConversionResult> {
    check_encoding_conversion_args::call(src_encoding, dest_encoding, src.len() as i32, PG_UTF8, PG_EUC_KR)?;
    UtfToLocal(
        src,
        Some(&tables::euc_kr_from_unicode_tree()),
        &[],
        None,
        PG_EUC_KR,
        no_error,
    )
}

/// The C module's `euc_kr_to_utf8` / `utf8_to_euc_kr` procedures are reached
/// through the `pg_conversion` catalog by OID; in C they live in a dynamically
/// loaded `$libdir/utf8_and_euc_kr` shared object. This port mocks that loading
/// by registering the two procedures in the fmgr builtin fast-path registry
/// (keyed by their `pg_proc.dat` OIDs), so `convert_via_proc` can dispatch them.
pub fn init_seams() {
    fmgr_builtins::register_utf8_and_euc_kr_builtins();
}

#[cfg(test)]
mod tests;
