//! Port of `src/backend/tsearch/ts_locale.c` — the locale-compatibility layer
//! for tsearch: the `t_isalpha`/`t_isalnum` character-class predicates and the
//! `tsearch_readline` file reader (exposed here as the whole-file `readfile`
//! seam declared in [`backend_tsearch_ts_locale_seams`]).
//!
//! The character predicates mirror the C `GENERATE_T_ISCLASS_DEF` macro
//! (ts_locale.c:34): take the leading multibyte character, and if it is a
//! single byte (or the database ctype is C) classify it with the libc byte
//! `is*` routine, otherwise convert it with `char2wchar` and classify with the
//! libc wide `isw*` routine. The `readfile` seam reproduces
//! `tsearch_readline_begin`/`tsearch_readline`/`tsearch_readline_end`
//! (ts_locale.c:93-178): open the file, read it whole, and recode it from UTF-8
//! to the database encoding (`pg_any_to_server`), returning the `\n`-terminated
//! bytes for the caller to split.

#![no_std]
#![allow(non_snake_case)]

extern crate alloc;

use alloc::string::{String, ToString};
use alloc::vec::Vec;

use backend_tsearch_parse_seams as ctype;
use backend_utils_mb_mbutils_seams::pg_any_to_server;
use backend_utils_mb_mbutils_seams::pg_mblen_range;

/// `PG_UTF8` (`mb/pg_wchar.h`) — tsearch config files are expected to be UTF-8.
const PG_UTF8: i32 = 6;

/// `t_isalpha_with_len(ptr, mblen)` (`ts_locale.c` `GENERATE_T_ISCLASS_DEF`):
/// classify the leading character of `s` as alphabetic. `s` is positioned at
/// the character under test and is never empty at a call site.
fn t_isalpha(s: &[u8]) -> bool {
    classify(s, ctype::isalpha::call, ctype::iswalpha::call)
}

/// `t_isalnum_with_len(ptr, mblen)` (`ts_locale.c` `GENERATE_T_ISCLASS_DEF`):
/// classify the leading character of `s` as alphanumeric.
fn t_isalnum(s: &[u8]) -> bool {
    classify(s, ctype::isalnum::call, ctype::iswalnum::call)
}

/// The shared body of the C `GENERATE_T_ISCLASS_DEF` macro:
///
/// ```c
/// int clen = pg_mblen_with_len(ptr, mblen);
/// wchar_t character[WC_BUF_LEN];
/// pg_locale_t mylocale = 0;
/// if (clen == 1 || database_ctype_is_c)
///     return is##character_class(TOUCHAR(ptr));
/// char2wchar(character, WC_BUF_LEN, ptr, clen, mylocale);
/// return isw##character_class((wint_t) character[0]);
/// ```
///
/// `byte_class` is the libc byte `is*` routine, `wide_class` the wide `isw*`.
fn classify(s: &[u8], byte_class: fn(u32) -> i32, wide_class: fn(u32) -> i32) -> bool {
    // int clen = pg_mblen(ptr): the byte length of the leading character.
    // `pg_mblen_range` now returns PgResult (the panic→Result mb seam
    // migration); tsearch feeds pre-validated text so an error is unreachable
    // here — fall back to the single-byte (`clen == 1`) byte-classification
    // path on the unexpected error, matching this function's existing
    // bad-sequence tolerance (`Err(_) => false` below).
    let clen = pg_mblen_range::call(s).unwrap_or(1);

    // if (clen == 1 || database_ctype_is_c) return is##class(TOUCHAR(ptr));
    //
    // TOUCHAR(ptr) is `*(const unsigned char *) (ptr)` — the leading byte.
    if clen == 1 || ctype::database_ctype_is_c::call() {
        return byte_class(s[0] as u32) != 0;
    }

    // char2wchar(character, WC_BUF_LEN, ptr, clen, mylocale);
    // return isw##class((wint_t) character[0]);
    //
    // We pass exactly the leading `clen` bytes; char2wchar yields the
    // wide-character array, of which only the first element is tested (matching
    // the C "pass just the first code" handling of surrogate pairs / BMP-only
    // classification).
    let head = s[..clen as usize].to_vec();
    match ctype::char2wchar::call(head) {
        Ok(wide) => match wide.first() {
            Some(&wc) => wide_class(wc) != 0,
            // An empty conversion result classifies as false (the leading char
            // produced no wide character).
            None => false,
        },
        // A bad multibyte sequence cannot be classified; treat as false (the C
        // path would have errored earlier in char2wchar, but the tsearch
        // callers feed pre-validated text, so this is unreachable in practice).
        Err(_) => false,
    }
}

/// `tsearch_readline_begin` + `tsearch_readline` loop + `tsearch_readline_end`
/// (`ts_locale.c:93-178`), as the whole-file `readfile` seam: open `filename`
/// with `AllocateFile(filename, "r")`, read the whole file, validate it as
/// UTF-8 and recode it to the database encoding (`pg_any_to_server(buf, len,
/// PG_UTF8)`), and return the recoded bytes. The `\n` line terminators are
/// preserved; the caller splits and parses lines.
///
/// `Err(msg)` carries the C `%m` open-failure text for the caller's
/// `could not open … file "%s": %m` `ereport`.
fn readfile(filename: &[u8]) -> Result<Vec<u8>, String> {
    // The seam's `filename` is a NUL-free path in the database encoding.
    let path = match core::str::from_utf8(filename) {
        Ok(p) => p,
        Err(_) => return Err("invalid file path encoding".to_string()),
    };

    // tsearch_readline_begin: AllocateFile(filename, "r"); on NULL the caller
    // composes the "could not open … file" message, so any open failure
    // (including absence) surfaces as an Err here.
    //
    // `allocate_file_read` performs AllocateFile + whole-file read + FreeFile,
    // returning Ok(None) when the file is absent (errno == ENOENT). A different
    // open/read failure raises ERROR directly, matching the C behaviour of an
    // ereport for an unreadable file.
    let raw = match backend_storage_file_fd::seams::allocate_file_read(path) {
        Ok(Some(bytes)) => bytes,
        Ok(None) => return Err("No such file or directory".to_string()),
        Err(e) => return Err(e.message().to_string()),
    };

    // tsearch_readline: pg_any_to_server(buf, len, PG_UTF8) over the file
    // content. The seam needs an Mcx for the converted bytes; use a private
    // scratch context and copy the result into the returned Vec (the C reader
    // likewise returns palloc'd bytes the caller copies parsed pieces out of).
    let ctx = mcx::MemoryContext::new("readfile");
    let mcx = ctx.mcx();
    // `pg_any_to_server` returns `Ok(None)` when no conversion was needed (the
    // input bytes already stand in the DB encoding, after in-place validation);
    // `Ok(Some(v))` carries the converted bytes. Copy the bytes out of the
    // scratch context before it drops.
    let out: Result<alloc::vec::Vec<u8>, alloc::string::String> =
        match pg_any_to_server::call(mcx, &raw, PG_UTF8) {
            Ok(Some(recoded)) => Ok(recoded.as_slice().to_vec()),
            Ok(None) => Ok(raw.clone()),
            Err(e) => Err(e.message().to_string()),
        };
    out
}

/// Install the three `ts_locale.c` seams.
pub fn init_seams() {
    backend_tsearch_ts_locale_seams::t_isalpha::set(t_isalpha);
    backend_tsearch_ts_locale_seams::t_isalnum::set(t_isalnum);
    backend_tsearch_ts_locale_seams::readfile::set(readfile);
}
