use super::*;
use mcx::MemoryContext;
use wchar::{PG_KOI8R, PG_LATIN1, PG_SJIS};

#[test]
fn no_conversion_returns_none_without_allocating() {
    // The pointer-identity contract: C returns the input pointer when no
    // conversion is required; here that is Ok(None) and zero bytes allocated.
    let ctx = MemoryContext::new("test");
    SetDatabaseEncoding(PG_UTF8).unwrap();
    InitializeClientEncoding().unwrap();
    SetClientEncoding(PG_UTF8).unwrap();

    let out = pg_server_to_client(ctx.mcx(), "h\u{00e9}llo".as_bytes()).unwrap();
    assert!(out.is_none());
    let out = pg_client_to_server(ctx.mcx(), "h\u{00e9}llo".as_bytes()).unwrap();
    assert!(out.is_none());
    assert_eq!(ctx.used(), 0, "identity path must not allocate");
}

#[test]
fn sql_ascii_client_is_identity() {
    let ctx = MemoryContext::new("test");
    SetDatabaseEncoding(PG_UTF8).unwrap();
    InitializeClientEncoding().unwrap();
    SetClientEncoding(PG_SQL_ASCII).unwrap();
    assert!(!server_to_client_conversion_needed());
    assert!(pg_server_to_client(ctx.mcx(), b"\xff\xfe raw").unwrap().is_none());
    assert_eq!(ctx.used(), 0);
}

#[test]
fn conversion_needed_truth_table() {
    SetDatabaseEncoding(PG_UTF8).unwrap();
    InitializeClientEncoding().unwrap();
    assert!(!server_to_client_conversion_needed());
    SetClientEncoding(PG_SQL_ASCII).unwrap();
    assert!(!server_to_client_conversion_needed());
    // A real cross-encoding pair needs the (unported) conversion-proc lookup,
    // so flip the state cells directly to check the predicate itself.
    CLIENT_ENCODING.with(|c| c.set(PG_LATIN1));
    assert!(server_to_client_conversion_needed());
}

#[test]
fn pending_client_encoding_applied_at_startup() {
    SetDatabaseEncoding(PG_UTF8).unwrap();
    assert_eq!(SetClientEncoding(PG_UTF8).unwrap(), 0);
    assert_eq!(pg_get_client_encoding(), PG_SQL_ASCII);
    InitializeClientEncoding().unwrap();
    assert_eq!(pg_get_client_encoding(), PG_UTF8);
    assert_eq!(pg_get_client_encoding_name(), "UTF8");
}

#[test]
fn set_client_encoding_rejects_invalid() {
    SetDatabaseEncoding(PG_UTF8).unwrap();
    InitializeClientEncoding().unwrap();
    assert_eq!(SetClientEncoding(999).unwrap(), -1);
    assert_eq!(PrepareClientEncoding(999).unwrap(), -1);
}

#[test]
fn prepare_outside_transaction_needs_cache() {
    SetDatabaseEncoding(PG_UTF8).unwrap();
    InitializeClientEncoding().unwrap();
    xact_seams::is_transaction_state::set(|| false);
    // Not cached and not in a transaction: fails per C.
    assert_eq!(PrepareClientEncoding(PG_LATIN1).unwrap(), -1);
    assert_eq!(SetClientEncoding(PG_LATIN1).unwrap(), -1);
}

#[test]
fn database_encoding_accessors() {
    assert_eq!(GetDatabaseEncoding(), PG_SQL_ASCII);
    assert_eq!(GetDatabaseEncodingName(), "SQL_ASCII");
    SetDatabaseEncoding(PG_UTF8).unwrap();
    assert_eq!(GetDatabaseEncoding(), PG_UTF8);
    assert_eq!(GetDatabaseEncodingName(), "UTF8");
    assert_eq!(pg_database_encoding_max_length(), 4);
    assert!(SetDatabaseEncoding(PG_SJIS).is_err()); // client-only
    assert!(SetDatabaseEncoding(4242).is_err());
    SetMessageEncoding(PG_KOI8R);
    assert_eq!(GetMessageEncoding(), PG_KOI8R);
}

#[test]
fn enc2name_matches_wchar_ids() {
    assert_eq!(PG_ENC2NAME.len(), wchar::_PG_LAST_ENCODING_ as usize);
    assert_eq!(enc_name(PG_LATIN1), "LATIN1");
    assert_eq!(enc_name(PG_KOI8R), "KOI8R");
    assert_eq!(enc_name(wchar::PG_SHIFT_JIS_2004), "SHIFT_JIS_2004");
}

#[test]
fn mbcliplen_utf8_boundaries() {
    SetDatabaseEncoding(PG_UTF8).unwrap();
    let s = "a\u{00e9}b".as_bytes(); // 1 + 2 + 1 bytes
    assert_eq!(pg_mbcliplen(s, s.len() as i32, 4), 4);
    assert_eq!(pg_mbcliplen(s, s.len() as i32, 3), 3);
    assert_eq!(pg_mbcliplen(s, s.len() as i32, 2), 1); // can't split é
    assert_eq!(pg_mbcliplen(s, s.len() as i32, 1), 1);
    assert_eq!(pg_mbcliplen(s, s.len() as i32, 0), 0);
    // len caps the walk independently of limit
    assert_eq!(pg_mbcliplen(s, 1, 10), 1);
}

#[test]
fn mbcliplen_single_byte_stops_at_nul() {
    assert_eq!(GetDatabaseEncoding(), PG_SQL_ASCII);
    assert_eq!(pg_mbcliplen(b"ab\0cd", 5, 10), 2);
    assert_eq!(pg_mbcliplen(b"abcd", 4, 3), 3);
    assert_eq!(pg_encoding_mbcliplen(PG_LATIN1, b"abcd", 4, 2), 2);
}

#[test]
fn mbcharcliplen_counts_characters() {
    SetDatabaseEncoding(PG_UTF8).unwrap();
    let s = "\u{00e9}\u{00e9}\u{00e9}".as_bytes(); // 3 chars, 6 bytes
    assert_eq!(pg_mbcharcliplen(s, 6, 2).unwrap(), 4);
    assert_eq!(pg_mbcharcliplen(s, 6, 5).unwrap(), 6);
    assert_eq!(pg_mbcharcliplen(s, 6, 0).unwrap(), 0);
}

#[test]
fn mbstrlen_variants() {
    SetDatabaseEncoding(PG_UTF8).unwrap();
    let s = "a\u{00e9}\u{4e16}".as_bytes(); // 1 + 2 + 3 bytes, 3 chars
    assert_eq!(pg_mbstrlen_with_len(s).unwrap(), 3);
    assert_eq!(pg_mbstrlen(s).unwrap(), 3);
    assert_eq!(pg_encoding_mbstrlen_with_len(PG_UTF8, &s[..3]).unwrap(), 2);
    assert_eq!(
        pg_encoding_mbstrlen_with_len(PG_LATIN1, s).unwrap(),
        s.len() as i32
    );
    assert_eq!(pg_mbstrlen_with_len(b"ab\0cd").unwrap(), 2);
}

// encoding.sql "truncated" fixture: a 2-byte UTF8 lead byte with the
// continuation byte sliced off. C's pg_mbstrlen_with_len -> pg_mblen_with_len
// ereports "invalid byte sequence for encoding "UTF8": 0xc3".
#[test]
fn mbstrlen_with_len_errors_on_truncated_trailing_char() {
    SetDatabaseEncoding(PG_UTF8).unwrap();
    let s = b"caf\xc3";
    let err = pg_mbstrlen_with_len(s).unwrap_err();
    assert_eq!(
        err.message(),
        "invalid byte sequence for encoding \"UTF8\": 0xc3"
    );
}

// The plain per-char loop pg_encoding_mbstrlen_with_len replaced with the
// ascii_run fast path; the differential oracle.
fn mbstrlen_reference(encoding: pg_enc, mbstr: &[u8]) -> PgResult<i32> {
    let mut len = 0;
    let mut pos = 0usize;
    let mut limit = mbstr.len() as i32;
    while limit > 0 && mbstr[pos] != 0 {
        let l = pg_encoding_mblen_with_len(encoding, &mbstr[pos..], limit)?;
        limit -= l;
        pos += l as usize;
        len += 1;
    }
    Ok(len)
}

#[test]
fn mbstrlen_ascii_run_differential() {
    SetDatabaseEncoding(PG_UTF8).unwrap();
    let atoms: &[&[u8]] = &[
        b"",
        b"a",
        b"abcdefg",
        b"abcdefgh",
        b"abcdefghijklmno",
        b"abcdefghijklmnop",
        b"abcdefghijklmnopqrstuvwxyz0123456789ABCD",
        b"\0",
        b"\x7f",
        b"\x80",             // lone continuation
        b"\xc3\xa9",         // 2-byte
        b"\xe4\xb8\x96",     // 3-byte
        b"\xf0\x9f\x98\x80", // 4-byte
        b"\xc3",             // truncated lead (error case at end)
        b"\xff",
    ];
    for &a in atoms {
        for &b in atoms {
            for &c in atoms {
                let s = [a, b, c].concat();
                let got = pg_encoding_mbstrlen_with_len(PG_UTF8, &s);
                let want = mbstrlen_reference(PG_UTF8, &s);
                match (got, want) {
                    (Ok(g), Ok(w)) => assert_eq!(g, w, "input {s:x?}"),
                    (Err(g), Err(w)) => assert_eq!(g.message(), w.message(), "input {s:x?}"),
                    (g, w) => panic!("divergence on {s:x?}: {g:?} vs {w:?}"),
                }
            }
        }
    }
}

#[test]
fn ascii_run_boundaries() {
    for n in 0..48 {
        let mut s = vec![b'x'; n];
        assert_eq!(ascii_run(&s), n);
        for stopper in [0u8, 0x80, 0xc3] {
            for k in 0..n {
                s[k] = stopper;
                assert_eq!(ascii_run(&s), k, "n={n} k={k} stopper={stopper:#x}");
                s[k] = b'x';
            }
        }
    }
}

#[test]
fn mblen_family() {
    SetDatabaseEncoding(PG_UTF8).unwrap();
    assert_eq!(pg_mblen("\u{4e16}x".as_bytes()), 3);
    assert_eq!(pg_mblen_range("\u{4e16}".as_bytes()).unwrap(), 3);
    assert!(pg_mblen_range(&"\u{4e16}".as_bytes()[..2]).is_err());
    assert_eq!(pg_mblen_with_len("\u{00e9}".as_bytes(), 2).unwrap(), 2);
    assert!(pg_mblen_with_len("\u{00e9}".as_bytes(), 1).is_err());
    assert_eq!(pg_dsplen("\u{4e16}".as_bytes()), 2);
}

#[test]
fn verify_mbstr_reports_c_message() {
    SetDatabaseEncoding(PG_UTF8).unwrap();
    assert!(pg_verifymbstr("ol\u{00e9}".as_bytes(), false).unwrap());
    assert!(!pg_verifymbstr(b"bad\xff", true).unwrap());
    let err = pg_verifymbstr(b"bad\xffx", false).unwrap_err();
    assert_eq!(
        err.message(),
        "invalid byte sequence for encoding \"UTF8\": 0xff"
    );
    assert_eq!(err.sqlstate(), ERRCODE_CHARACTER_NOT_IN_REPERTOIRE);
}

#[test]
fn verify_mbstr_len_counts_and_rejects() {
    assert_eq!(pg_verify_mbstr_len(PG_LATIN1, b"abc", false).unwrap(), 3);
    assert_eq!(pg_verify_mbstr_len(PG_LATIN1, b"a\0c", true).unwrap(), -1);
    assert_eq!(
        pg_verify_mbstr_len(PG_UTF8, "a\u{00e9}\u{4e16}".as_bytes(), false).unwrap(),
        3
    );
    assert_eq!(pg_verify_mbstr_len(PG_UTF8, b"a\xff", true).unwrap(), -1);
    assert!(pg_verify_mbstr_len(PG_UTF8, b"a\xff", false).is_err());
}

#[test]
fn any_to_server_validates_even_without_conversion() {
    SetDatabaseEncoding(PG_UTF8).unwrap();
    InitializeClientEncoding().unwrap();
    let ctx = MemoryContext::new("test");
    assert!(pg_any_to_server(ctx.mcx(), b"ok", PG_UTF8).unwrap().is_none());
    assert!(pg_any_to_server(ctx.mcx(), b"\xff", PG_UTF8).is_err());
    assert!(pg_any_to_server(ctx.mcx(), b"\xff", PG_SQL_ASCII).is_err());
}

#[test]
fn ascii_server_rejects_highbit_from_client_only_encoding() {
    // db SQL_ASCII + ASCII-unsafe client encoding: NUL/high-bit bytes rejected.
    InitializeClientEncoding().unwrap();
    let ctx = MemoryContext::new("test");
    assert!(pg_any_to_server(ctx.mcx(), b"plain", PG_SJIS).unwrap().is_none());
    let err = pg_any_to_server(ctx.mcx(), b"a\x93z", PG_SJIS).unwrap_err();
    assert_eq!(
        err.message(),
        "invalid byte value for encoding \"SQL_ASCII\": 0x93"
    );
    // Server-legal client encoding: validated under that encoding, identity.
    assert!(pg_any_to_server(ctx.mcx(), b"a\xb1z", PG_KOI8R).unwrap().is_none());
}

#[test]
fn server_to_any_identity_cases() {
    SetDatabaseEncoding(PG_UTF8).unwrap();
    InitializeClientEncoding().unwrap();
    let ctx = MemoryContext::new("test");
    assert!(pg_server_to_any(ctx.mcx(), b"x", PG_UTF8).unwrap().is_none());
    assert!(pg_server_to_any(ctx.mcx(), b"x", PG_SQL_ASCII).unwrap().is_none());
    assert!(pg_server_to_any(ctx.mcx(), b"", PG_LATIN1).unwrap().is_none());
    assert_eq!(ctx.used(), 0);
}

#[test]
fn do_encoding_conversion_identity_and_validation() {
    let ctx = MemoryContext::new("test");
    assert!(pg_do_encoding_conversion(ctx.mcx(), b"", PG_LATIN1, PG_UTF8)
        .unwrap()
        .is_none());
    assert!(pg_do_encoding_conversion(ctx.mcx(), b"x", PG_UTF8, PG_UTF8)
        .unwrap()
        .is_none());
    assert!(pg_do_encoding_conversion(ctx.mcx(), b"\xff", PG_UTF8, PG_SQL_ASCII)
        .unwrap()
        .is_none());
    assert!(
        pg_do_encoding_conversion(ctx.mcx(), b"\xff", PG_SQL_ASCII, PG_UTF8).is_err()
    );
    assert!(pg_do_encoding_conversion(ctx.mcx(), b"ok", PG_SQL_ASCII, PG_UTF8)
        .unwrap()
        .is_none());
}

#[test]
fn unicode_to_server_utf8_paths() {
    SetDatabaseEncoding(PG_UTF8).unwrap();
    let ctx = MemoryContext::new("test");
    assert_eq!(&*pg_unicode_to_server(ctx.mcx(), 0x41).unwrap(), b"A");
    assert_eq!(
        &*pg_unicode_to_server(ctx.mcx(), 0xE9).unwrap(),
        "\u{00e9}".as_bytes()
    );
    assert_eq!(
        &*pg_unicode_to_server(ctx.mcx(), 0x4E16).unwrap(),
        "\u{4e16}".as_bytes()
    );
    assert!(pg_unicode_to_server(ctx.mcx(), 0x11_0000).is_err());
    assert!(pg_unicode_to_server_noerror(ctx.mcx(), 0x11_0000)
        .unwrap()
        .is_none());
    assert_eq!(
        &*pg_unicode_to_server_noerror(ctx.mcx(), 0xE9).unwrap().unwrap(),
        "\u{00e9}".as_bytes()
    );
}

#[test]
fn unicode_to_server_without_proc_fails() {
    SetDatabaseEncoding(PG_LATIN1).unwrap();
    let ctx = MemoryContext::new("test");
    let err = pg_unicode_to_server(ctx.mcx(), 0xE9).unwrap_err();
    assert_eq!(
        err.message(),
        "conversion between UTF8 and LATIN1 is not supported"
    );
    assert!(pg_unicode_to_server_noerror(ctx.mcx(), 0xE9).unwrap().is_none());
    // ASCII range needs no conversion proc.
    assert_eq!(&*pg_unicode_to_server(ctx.mcx(), 0x7A).unwrap(), b"z");
}

#[test]
fn mb2wchar_roundtrip_utf8() {
    SetDatabaseEncoding(PG_UTF8).unwrap();
    let ctx = MemoryContext::new("test");
    let w = pg_mb2wchar_with_len(ctx.mcx(), "a\u{00e9}".as_bytes()).unwrap();
    assert_eq!(&*w, &[0x61, 0xE9]);
    let b = pg_wchar2mb_with_len(ctx.mcx(), &w).unwrap();
    assert_eq!(&*b, "a\u{00e9}".as_bytes());
}

// encoding.sql encoding_tests: MULE_INTERNAL LC1 and LC2 roundtrip through
// pg_encoding_mb2wchar_with_len/pg_encoding_wchar2mb_with_len (regress.c's
// test_text_to_wchars/test_wchars_to_text path), independent of database
// encoding since the encoding is threaded explicitly.
#[test]
fn mule_internal_lc1_lc2_roundtrip() {
    let ctx = MemoryContext::new("test");
    // LC1: \x8182 -> {8454274} -> \x8182 = OK
    let lc1 = [0x81u8, 0x82];
    let w = pg_encoding_mb2wchar_with_len(ctx.mcx(), wchar::PG_MULE_INTERNAL, &lc1).unwrap();
    assert_eq!(&*w, &[8454274]);
    let b = pg_encoding_wchar2mb_with_len(ctx.mcx(), wchar::PG_MULE_INTERNAL, &w).unwrap();
    assert_eq!(&*b, &lc1);

    // LC2: \x908283 -> {9470595} -> \x908283 = OK
    let lc2 = [0x90u8, 0x82, 0x83];
    let w = pg_encoding_mb2wchar_with_len(ctx.mcx(), wchar::PG_MULE_INTERNAL, &lc2).unwrap();
    assert_eq!(&*w, &[9470595]);
    let b = pg_encoding_wchar2mb_with_len(ctx.mcx(), wchar::PG_MULE_INTERNAL, &w).unwrap();
    assert_eq!(&*b, &lc2);
}

#[test]
fn utf8_increment_cases() {
    let mut c = *b"a";
    assert!(pg_utf8_increment(&mut c));
    assert_eq!(&c, b"b");
    let mut c = [0x7F];
    assert!(!pg_utf8_increment(&mut c));
    let mut c = [0xC3, 0xBF]; // ÿ: last byte saturated, bump lead byte
    assert!(pg_utf8_increment(&mut c));
    assert_eq!(c, [0xC4, 0xBF]);
    let mut c = [0xED, 0x9F, 0xBF]; // just below surrogates
    assert!(pg_utf8_increment(&mut c));
    assert_eq!(c, [0xEE, 0x9F, 0xBF]);
}

#[test]
fn eucjp_increment_cases() {
    let mut c = [0x41];
    assert!(pg_eucjp_increment(&mut c));
    assert_eq!(c, [0x42]);
    let mut c = [0x8e, 0xdf];
    assert!(pg_eucjp_increment(&mut c));
    assert_eq!(c, [0xa1, 0xa1]);
    let mut c = [0xa1, 0xfe];
    assert!(pg_eucjp_increment(&mut c));
    assert_eq!(c, [0xa2, 0xfe]);
}

#[test]
fn charinc_dispatch_and_generic() {
    SetDatabaseEncoding(PG_UTF8).unwrap();
    assert_eq!(
        pg_database_encoding_character_incrementer() as usize,
        pg_utf8_increment as usize
    );
    let mut c = *b"a";
    assert!(pg_generic_charinc(&mut c));
    assert_eq!(&c, b"b");
}

#[test]
fn check_conversion_args() {
    assert!(check_encoding_conversion_args(PG_UTF8, PG_LATIN1, 4, PG_UTF8, PG_LATIN1).is_ok());
    assert!(check_encoding_conversion_args(PG_UTF8, PG_LATIN1, 4, -1, -1).is_ok());
    let err =
        check_encoding_conversion_args(PG_UTF8, PG_LATIN1, 4, PG_LATIN1, PG_LATIN1).unwrap_err();
    assert_eq!(
        err.message(),
        "expected source encoding \"LATIN1\", but got \"UTF8\""
    );
    assert!(check_encoding_conversion_args(PG_UTF8, PG_LATIN1, -1, -1, -1).is_err());
    assert!(check_encoding_conversion_args(4242, PG_LATIN1, 1, -1, -1).is_err());
}

#[test]
fn reporters_match_c_shapes() {
    let err = report_untranslatable_char(PG_UTF8, PG_LATIN1, "\u{4e16}xy".as_bytes());
    assert_eq!(
        err.message(),
        "character with byte sequence 0xe4 0xb8 0x96 in encoding \"UTF8\" has no equivalent in encoding \"LATIN1\""
    );
    assert_eq!(err.sqlstate(), ERRCODE_UNTRANSLATABLE_CHARACTER);
    // Truncated multibyte char: mblen_or_incomplete clamps the dump to the input.
    let err = report_invalid_encoding(PG_UTF8, &"\u{4e16}".as_bytes()[..2]);
    assert_eq!(
        err.message(),
        "invalid byte sequence for encoding \"UTF8\": 0xe4 0xb8"
    );
}

#[test]
fn seams_installed() {
    init_seams();
    mbutils_seams::set_database_encoding::call(PG_UTF8).unwrap();
    assert_eq!(mbutils_seams::get_database_encoding::call(), PG_UTF8);
    assert_eq!(mbutils_seams::get_database_encoding_name::call(), "UTF8");
    assert_eq!(mbutils_seams::pg_database_encoding_max_length::call(), 4);
    mbutils_seams::initialize_client_encoding::call().unwrap();
    assert!(!mbutils_seams::server_to_client_conversion_needed::call());
    assert_eq!(
        mbutils_seams::pg_mbstrlen_with_len::call("a\u{00e9}".as_bytes()).unwrap(),
        2
    );
    assert_eq!(
        mbutils_seams::pg_mbcliplen::call("a\u{00e9}".as_bytes(), 3, 2),
        1
    );
    let ctx = MemoryContext::new("test");
    assert!(mbutils_seams::pg_server_to_client::call(ctx.mcx(), b"x")
        .unwrap()
        .is_none());
    assert!(pg_client_to_server(ctx.mcx(), b"x").unwrap().is_none());
    assert_eq!(ctx.used(), 0);
}

#[test]
fn encname_lookup_matches_encnames_c() {
    for w in crate::PG_ENCNAME.windows(2) {
        assert!(w[0].0 < w[1].0, "pg_encname_tbl order: {} < {}", w[0].0, w[1].0);
    }
    assert_eq!(pg_char_to_encoding("UTF8"), PG_UTF8);
    assert_eq!(pg_char_to_encoding("utf-8"), PG_UTF8);
    assert_eq!(pg_char_to_encoding("UNICODE"), PG_UTF8);
    assert_eq!(pg_char_to_encoding("SQL_ASCII"), PG_SQL_ASCII);
    assert_eq!(pg_char_to_encoding("Latin-1"), wchar::PG_LATIN1);
    assert_eq!(pg_char_to_encoding("nonsense"), -1);
    assert_eq!(pg_char_to_encoding(""), -1);
    assert_eq!(pg_valid_client_encoding("UTF8"), PG_UTF8);
    assert_eq!(pg_valid_client_encoding("MULE_INTERNAL"), wchar::PG_MULE_INTERNAL);
    assert_eq!(pg_valid_server_encoding("SJIS"), -1);
    assert_eq!(pg_encoding_to_char(PG_UTF8), "UTF8");
    assert_eq!(pg_encoding_to_char(-1), "");
    assert_eq!(pg_encoding_to_char(9999), "");
}
