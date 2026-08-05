//! wparserfam_diff: differential fuzz driver for the DEFAULT text-search
//! parser's tokenizer (crates/backend/tsearch/wparser_def) vs verbatim
//! vendored PostgreSQL 18.3 C (csrc/pg_wparserfam_io.c, upstream sha
//! 62d6c7d3df; lane p1-mb-contribc).
//!
//! ONE surface, driven over the whole byte domain: bytes in -> token stream
//! out. Per exec a header byte selects the server encoding from
//! {UTF8, LATIN1, SQL_ASCII} and the database_ctype_is_c posture, both
//! PINNED IDENTICALLY on the two sides (Rust: mbutils::SetDatabaseEncoding
//! + pg_locale::set_database_ctype_is_c; C: wfam_x_set_db_encoding +
//! pg_wpd_set_ctype_is_c). The remaining payload is the text.
//!
//! Compared: the FULL token stream — count, and per token the type id, the
//! byte OFFSET into the input (both sides borrow the same buffer, so
//! token - str is the stable identity) and the byte length. Plus the
//! error verdict + exact sqlstate, and no-panic on both sides.
//!
//! The encoding selector matters: charmaxlen == 1 (LATIN1, SQL_ASCII)
//! takes TParserInit's narrow arm and every p_is* predicate reads the raw
//! byte through libc is*(); charmaxlen > 1 (UTF8) takes the wide arm,
//! which splits again on database_ctype_is_c — the pgwstr path
//! (pg_mb2wchar_with_len + is*() with non-ASCII forced alpha) or the wstr
//! path (char2wchar/mbstowcs + isw*()). All three combinations are driven.
//!
//! Both sides call the ONE in-process libc ctype/wctype table and the ONE
//! libc mbstowcs (the one-libm posture of miscfam's earthdistance arm and
//! tzfam's ts_locale arm): the diff validates the dispatch logic and the
//! state machine, not libc.
//!
//! Also compared: prsd_lextype's full (lexid, alias, descr) table, swept
//! exhaustively in tests::lextype_table (all LASTNUM+1 rows).
//!
//! DOMAIN CARVES (C caller contract, never pgrust behavior):
//!   - under a MULTIBYTE server encoding the input is NUL-free. This is a
//!     genuine C caller contract: TParserInit's callers hand it a `text`
//!     payload, and PostgreSQL text values cannot contain a NUL byte
//!     (pg_verify_mbstr rejects one at input). It is also a C-UB boundary
//!     rather than a behavior: both wide arms stop converting at the NUL
//!     (pg_utf2wchar_with_len's `while (len > 0 && *from)`, mbstowcs's
//!     terminator) while lenstr keeps counting the whole buffer, so C then
//!     indexes UNINITIALIZED palloc'd wchar slots for every position past
//!     the NUL — there is no defined C answer to compare against. The
//!     pgrust port zero-fills those slots (see the resize in
//!     tparser_init/char2wchar_default), which is strictly more defined.
//!     Single-byte encodings read raw bytes and keep NULs in the domain.
//!     WITNESS-LOSS WARNING (recorded 2026-08-01 with the tzparser
//!     interior-NUL ruling): this carve removes the ONLY shape that
//!     witnesses the tparser_init wide-array truncation OOB fix at main
//!     (landed 2622e2955a) — interior-NUL/short-multibyte conversion
//!     shortfall is the only producer reaching that OOB, so a mutation
//!     restoring the pre-fix truncation SURVIVES this driver (observed by
//!     lane p1-mb-text). The fix must be witnessed OUTSIDE the fuzz plane:
//!     a directed crate-level regression test is OWED to
//!     crates/backend/tsearch/wparser_def (p1-mb-text produced
//!     `interior_nul_wide_arrays_inbounds`, input `ab\0cd ef` under UTF8,
//!     both ctype postures; owner to land — see the wparser_def row in
//!     docs/verification/phase1-claims.tsv). Do not treat this driver's
//!     green as covering that fix.
//!   - input length capped at 8 KiB (the token stream is compared in full;
//!     the state machine has no length-dependent arm above a few chars).
//!   - token stream capped at 4096 tokens per exec (same reason; the cap
//!     is applied identically on both sides and a truncated stream is
//!     still compared element-wise up to the cap).
//!
//! CARVED OUT (exception rows): prsd_headline + the whole ts_headline
//! support half of both sides (TSQuery + HeadlineParsedText from the
//! ts_parse/ts_cache layer above this crate — headline.rs is its own
//! route), and the funcapi SRF faces in builtins.rs (ts_parse_by* /
//! ts_token_type_by*), whose kernel is the tokenizer this target drives.

#![allow(dead_code)]

use std::os::raw::{c_char, c_int};

use types_error::PgResult;

extern "C" {
    fn pg_wpd_reset();
    fn pg_wpd_sqlstate() -> c_int;
    fn pg_wpd_set_ctype_is_c(v: c_int);
    fn pg_wpd_tokenize(
        str_: *const c_char,
        len: c_int,
        maxtok: c_int,
        types: *mut c_int,
        offsets: *mut c_int,
        lens: *mut c_int,
    ) -> c_int;
    fn pg_wpd_lextype(
        i: c_int,
        lexid: *mut c_int,
        alias: *mut *const c_char,
        descr: *mut *const c_char,
    ) -> c_int;
    fn wfam_x_set_db_encoding(encoding: c_int);
}

const MAXTOK: usize = 4096;
const MAXLEN: usize = 8192;

/// (encoding, label) triples the selector picks from.
const ENCODINGS: [(i32, &str); 3] = [
    (wchar::PG_UTF8, "UTF8"),
    (wchar::PG_LATIN1, "LATIN1"),
    (wchar::PG_SQL_ASCII, "SQL_ASCII"),
];

fn pin_env(enc: i32, ctype_is_c: bool) {
    mbutils::SetDatabaseEncoding(enc).expect("selector encodings are valid");
    unsafe { wfam_x_set_db_encoding(enc) };
    pg_locale::set_database_ctype_is_c(ctype_is_c);
    unsafe { pg_wpd_set_ctype_is_c(ctype_is_c as c_int) };
}

/// Rust-side token stream: (type, byte offset, byte len).
fn rust_tokens(text: &[u8]) -> PgResult<Vec<(i32, usize, usize)>> {
    let ctx = mcx::MemoryContext::new("wparserfam");
    let mut prs = wparser_def::tparser_init(ctx.mcx(), text.as_ptr(), text.len())?;
    let mut out = Vec::new();
    while wparser_def::tparser_get(&mut prs)? {
        let off = prs.token_ptr() as usize - text.as_ptr() as usize;
        // token_bytes() is the same token by a second route: assert the two
        // accessors agree (keeps the slice face in the measured denominator).
        assert_eq!(
            prs.token_bytes(),
            &text[off..off + prs.lenbytetoken],
            "token_bytes vs (token_ptr, lenbytetoken)"
        );
        out.push((prs.type_, off, prs.lenbytetoken));
        if out.len() >= MAXTOK {
            break;
        }
    }
    Ok(out)
}

fn c_tokens(text: &[u8]) -> Result<Vec<(i32, usize, usize)>, i32> {
    let mut types = vec![0i32; MAXTOK];
    let mut offs = vec![0i32; MAXTOK];
    let mut lens = vec![0i32; MAXTOK];
    let n = unsafe {
        pg_wpd_tokenize(
            text.as_ptr().cast(),
            text.len() as c_int,
            MAXTOK as c_int,
            types.as_mut_ptr(),
            offs.as_mut_ptr(),
            lens.as_mut_ptr(),
        )
    };
    if n < 0 {
        return Err(unsafe { pg_wpd_sqlstate() });
    }
    let n = (n as usize).min(MAXTOK);
    Ok((0..n)
        .map(|i| (types[i], offs[i] as usize, lens[i] as usize))
        .collect())
}

pub fn wparserfam_diff(data: &[u8]) {
    let _oracle = crate::oracle_serial(); // one-thread-at-a-time through the C oracles (process-global statics)
    if data.is_empty() {
        return;
    }
    unsafe { pg_wpd_reset() };
    let sel = data[0];
    let (enc, encname) = ENCODINGS[(sel % 3) as usize];
    // SQL_ASCII / LATIN1 are single-byte encodings: charmaxlen == 1, so the
    // ctype posture is only observable under UTF8 — drive it anyway (it is
    // a live cell on both sides and must not perturb the narrow arm).
    let ctype_is_c = sel & 0x80 != 0;
    pin_env(enc, ctype_is_c);

    let mut text: Vec<u8> = data[1..].iter().copied().take(MAXLEN).collect();
    if enc == wchar::PG_UTF8 {
        // NUL-free under a multibyte encoding (domain carve, header).
        text.retain(|&b| b != 0);
    }

    let r = rust_tokens(&text);
    let c = c_tokens(&text);

    let dbg = || {
        format!(
            "enc={encname} ctype_is_c={ctype_is_c} text={:?}",
            String::from_utf8_lossy(&text[..text.len().min(200)])
        )
    };

    match (&r, &c) {
        (Ok(rt), Ok(ct)) => {
            assert_eq!(rt.len(), ct.len(), "token COUNT ({})", dbg());
            for (i, (rtok, ctok)) in rt.iter().zip(ct.iter()).enumerate() {
                assert_eq!(
                    rtok, ctok,
                    "token {i} (type, offset, len) rust {rtok:?} c {ctok:?} ({})",
                    dbg()
                );
            }
        }
        (Err(e), Err(cs)) => {
            assert_eq!(e.sqlstate().0, *cs, "error sqlstate ({})", dbg());
        }
        (Err(e), Ok(ct)) => panic!(
            "VERDICT DIVERGENCE: rust error {} vs C ok ({} tokens) ({})",
            e.message,
            ct.len(),
            dbg()
        ),
        (Ok(rt), Err(cs)) => panic!(
            "VERDICT DIVERGENCE: rust ok ({} tokens) vs C error sqlstate {cs} ({})",
            rt.len(),
            dbg()
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn run(sel: u8, text: &[u8]) {
        let mut d = vec![sel];
        d.extend_from_slice(text);
        wparserfam_diff(&d);
    }

    /// EXHAUSTIVE-DIFF: prsd_lextype's whole table (LASTNUM+1 rows), lexid
    /// + alias + descr, against the verbatim C table.
    #[test]
    fn lextype_table() {
        let _serial = crate::c_oracle_serial();
        unsafe { pg_wpd_reset() };
        // The Rust face returns the LASTNUM live rows; C's palloc'd array
        // carries a trailing lexid==0 terminator (a C array-length
        // convention, not a value — the callers above both sides stop at
        // lexid 0). Compared: every live row, plus the terminator's
        // existence on the C side.
        let rows = wparser_def::builtins::lextype();
        assert_eq!(rows.len(), wparser_def::LASTNUM as usize, "lextype row count");
        for (i, row) in rows.iter().enumerate() {
            let (mut lexid, mut alias, mut descr): (c_int, *const c_char, *const c_char) =
                (0, std::ptr::null(), std::ptr::null());
            assert_eq!(
                unsafe { pg_wpd_lextype(i as c_int, &mut lexid, &mut alias, &mut descr) },
                0,
                "C lextype errored at {i}"
            );
            assert_eq!(row.lexid, lexid, "lexid {i}");
            let calias = unsafe { std::ffi::CStr::from_ptr(alias) }.to_bytes();
            let cdescr = unsafe { std::ffi::CStr::from_ptr(descr) }.to_bytes();
            assert_eq!(row.alias.as_bytes(), calias, "alias {i}");
            assert_eq!(row.descr.as_bytes(), cdescr, "descr {i}");
        }
        // C terminator row
        let (mut lexid, mut a, mut d): (c_int, *const c_char, *const c_char) =
            (-1, std::ptr::null(), std::ptr::null());
        assert_eq!(
            unsafe { pg_wpd_lextype(wparser_def::LASTNUM as c_int, &mut lexid, &mut a, &mut d) },
            0
        );
        assert_eq!(lexid, 0, "C lextype terminator");
    }

    /// Every single byte as a whole input, under all three encodings and
    /// both ctype postures: the full one-byte domain of the state machine's
    /// entry transitions.
    #[test]
    fn single_byte_domain() {
        for sel_base in [0u8, 1, 2] {
            for ctype in [0u8, 0x80] {
                for b in 0..=255u8 {
                    run(sel_base | ctype, &[b]);
                }
            }
        }
    }

    /// Token-class witnesses lifted from the vendored tsearch regress text
    /// (src/test/regress/sql/tsearch.sql shapes): one input per LEX class.
    #[test]
    fn token_class_witnesses() {
        let inputs: &[&str] = [
            "12",
            "12.34",
            "-1.5e10",
            "+42",
            "1.2.3.4",
            "abc",
            "ABC123",
            "\u{e9}t\u{e9}",
            "\u{4e2d}\u{6587}",
            "foo-bar",
            "foo-bar-baz",
            "123-abc",
            "a1-b2",
            "user@example.com",
            "http://example.com/path?q=1#frag",
            "https://a.b.c:8080/x/y.z",
            "www.example.org",
            "example.com",
            "/usr/local/bin/x",
            "./rel/path",
            "C:\\win\\path",
            "<b>bold</b>",
            "<a href=\"x\">",
            "&amp;",
            "&#123;",
            "&#x1F;",
            "  \t\n  ",
            "one two\tthree\nfour",
            "ftp://host/dir/",
            "mailto:a@b.c",
            "v1.2.3-rc1",
            "1,234.56",
            "a--b",
            "-",
            "--",
            "@",
            "..",
            "a.b@c.d/e?f",
            "\u{e9}-\u{e8}",
            "M\u{fc}ller-Stra\u{df}e",
            "\u{1F600}text",
            "\u{ff}\u{fe}broken",
        ]
        .as_slice();
        for sel_base in [0u8, 1, 2] {
            for ctype in [0u8, 0x80] {
                for s in inputs {
                    run(sel_base | ctype, s.as_bytes());
                }
            }
        }
    }

    /// Regression (ASan-treewide divergence triage, 2026-08-01): valid UTF-8
    /// the PROCESS LOCALE cannot convert (mbstowcs failure in char2wchar,
    /// the UTF8 + !ctype_is_c wstr arm). Rust raised the XX000 internal
    /// default where C raises ERRCODE_CHARACTER_NOT_IN_REPERTOIRE (22021)
    /// with the LC_CTYPE hint — pg_locale.c char2wchar. Whether the error
    /// fires at all depends on the host libc's C-locale mbstowcs (it does on
    /// glibc, which rejects non-ASCII); both sides share the one libc, so
    /// the diff is deterministic either way. Corpus twin:
    /// fuzz/corpus/wparser_diff/seed-char2wchar-sqlstate.
    #[test]
    fn char2wchar_failure_sqlstate() {
        // "80" + U+175C0 (the fleet repro), all encodings x ctype postures.
        let text = b"80\xf0\x97\x97\x80";
        for sel_base in [0u8, 1, 2] {
            for ctype in [0u8, 0x80] {
                run(sel_base | ctype, text);
            }
        }
    }

    /// Invalid / truncated multibyte sequences under UTF8 (the arm where
    /// pg_mblen / pg_mb2wchar / mbstowcs can disagree with a naive walk).
    #[test]
    fn invalid_multibyte() {
        for bytes in [
            &b"\xff"[..],
            &b"\xc3"[..],
            &b"\xc3\x28"[..],
            &b"\xe4\xb8"[..],
            &b"\xf0\x9f\x98"[..],
            &b"a\xffb"[..],
            &b"\xed\xa0\x80"[..], // surrogate
            &b"\xc0\x80"[..],     // overlong NUL
            &b"\xf4\x90\x80\x80"[..], // > U+10FFFF
        ] {
            for sel_base in [0u8, 1, 2] {
                for ctype in [0u8, 0x80] {
                    run(sel_base | ctype, bytes);
                }
            }
        }
    }
}
