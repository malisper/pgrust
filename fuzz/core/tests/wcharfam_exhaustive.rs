//! a0 EXHAUSTIVE-DIFF driver for the p1-laneah batch (common/wchar +
//! mb/mbutils pure kernels) against the verbatim vendored 18.3 C oracle
//! (csrc/pg_wcharfam.c). Every `x_*` test enumerates its ENTIRE stated
//! domain; run log banked at proofs/wcharfam/RUNLOG.md.
//!
//! Quick tests run on every `cargo test`; the 2^24/2^32 sweeps are
//! `#[ignore]` and run explicitly in release:
//!   cargo test --release -p decoder_fuzz --test wcharfam_exhaustive -- --ignored --test-threads=8
//!
//! Distinct-kernel representatives (wchar.c pg_wchar_table dedupe):
//!   0=SQL_ASCII 1=EUC_JP 3=EUC_KR(=EUC_CN verifier) 4=EUC_TW
//!   5=EUC_JIS_2004(=EUC_JP fns) 6=UTF8 7=MULE 8=LATIN1(single-byte row)
//!   35=SJIS(=SJIS2004) 36=BIG5 37=GBK 38=UHC 39=GB18030 40=JOHAB
//!
//! ORACLE SERIALIZATION (task #144): the verbatim C oracle is
//! single-threaded process-global state; every test here holds
//! `decoder_fuzz::c_oracle_serial()` for its whole body (the d58db26ba80
//! idiom — reentrant, so guarded drivers like wcharfam_diff still nest).
//! This is an integration-test crate: `crate::c_oracle_serial()` from the
//! panic-message idiom spells `decoder_fuzz::c_oracle_serial()` here.

use decoder_fuzz::wcharfam::*;

const ALL_ENCS: std::ops::Range<i32> = 0..42;
/// one representative per distinct (mblen,dsplen,verifychar) row
const REPS: [i32; 14] = [0, 1, 2, 3, 4, 6, 7, 8, 35, 36, 37, 38, 39, 40];

fn pad8(bytes: &[u8]) -> [u8; 8] {
    let mut p = [0u8; 8];
    p[..bytes.len()].copy_from_slice(bytes);
    p
}

// -------------------------------------------------------------------------
// Quick tier (every cargo test run)
// -------------------------------------------------------------------------

/// mblen + dsplen + bounded + or_incomplete + verifychar, ALL encodings x
/// full 2-byte domain (every mblen and every non-UTF8 dsplen reads at most
/// 2 bytes; UTF8 dsplen's full 4-byte domain is x_utf8_dsplen_full).
#[test]
fn q_mblen_dsplen_verifychar_2byte_full() {
    let _g = decoder_fuzz::c_oracle_serial();
    for enc in ALL_ENCS {
        for w in 0..=0xFFFFu16 {
            let b = w.to_be_bytes();
            cmp_mblen_dsplen(enc, &b);
            if b[0] != 0 {
                cmp_verifychar(enc, &b[..1]);
                cmp_verifychar(enc, &b);
            }
        }
    }
}

/// verifystr, ALL encodings x full 0..2-byte domain.
#[test]
fn q_verifystr_2byte_full() {
    let _g = decoder_fuzz::c_oracle_serial();
    for enc in ALL_ENCS {
        cmp_verifystr(enc, &[]);
        for b0 in 0..=0xFFu8 {
            cmp_verifystr(enc, &[b0]);
        }
        for w in 0..=0xFFFFu16 {
            cmp_verifystr(enc, &w.to_be_bytes());
        }
    }
}

/// utf8/eucjp incrementers, full 1..=3-byte domains (len 4 is x-tier).
#[test]
fn q_increments_len123_full() {
    let _g = decoder_fuzz::c_oracle_serial();
    for b0 in 0..=0xFFu8 {
        cmp_utf8_increment(&[b0]);
        cmp_eucjp_increment(&[b0]);
    }
    for w in 0..=0xFFFFu16 {
        let b = w.to_be_bytes();
        cmp_utf8_increment(&b);
        cmp_eucjp_increment(&b);
    }
    for w in 0..=0xFF_FFFFu32 {
        let b = [(w >> 16) as u8, (w >> 8) as u8, w as u8];
        cmp_utf8_increment(&b);
        cmp_eucjp_increment(&b);
    }
}

/// generic charinc: every BE encoding x full 1..=2-byte domain.
#[test]
fn q_generic_charinc_full_2byte() {
    let _g = decoder_fuzz::c_oracle_serial();
    for enc in 0..35 {
        set_db_encoding_pub(enc);
        for b0 in 0..=0xFFu8 {
            cmp_generic_charinc(&[b0]);
        }
        for w in 0..=0xFFFFu16 {
            cmp_generic_charinc(&w.to_be_bytes());
        }
    }
}

/// encnames: every table name x case/decoration variants, plus every
/// pg_enc2name spelling, plus junk shapes.
#[test]
fn q_encnames_table_full() {
    let _g = decoder_fuzz::c_oracle_serial();
    let names: &[&str] = &[
        "abc", "alt", "big5", "euccn", "eucjis2004", "eucjp", "euckr", "euctw",
        "gb18030", "gbk", "iso88591", "iso885910", "iso885913", "iso885914",
        "iso885915", "iso885916", "iso88592", "iso88593", "iso88594", "iso88595",
        "iso88596", "iso88597", "iso88598", "iso88599", "johab", "koi8", "koi8r",
        "koi8u", "latin1", "latin10", "latin2", "latin3", "latin4", "latin5",
        "latin6", "latin7", "latin8", "latin9", "mskanji", "muleinternal",
        "shiftjis", "shiftjis2004", "sjis", "sqlascii", "tcvn", "tcvn5712", "uhc",
        "unicode", "utf8", "vscii", "win", "win1250", "win1251", "win1252",
        "win1253", "win1254", "win1255", "win1256", "win1257", "win1258",
        "win866", "win874", "win932", "win936", "win949", "win950",
        "windows1250", "windows1251", "windows1252", "windows1253",
        "windows1254", "windows1255", "windows1256", "windows1257",
        "windows1258", "windows866", "windows874", "windows932", "windows936",
        "windows949", "windows950",
        // non-table shapes
        "", "utf", "utf88", "UTF-8-", "latin", "latin11", "sql_ascii!", "x",
    ];
    for n in names {
        cmp_char_to_encoding(n.as_bytes());
        cmp_char_to_encoding(n.to_uppercase().as_bytes());
        // decorated: separators are stripped by clean_encoding_name
        let dec: String = n.chars().flat_map(|c| [c, '-']).collect();
        cmp_char_to_encoding(dec.as_bytes());
        let dec: String = n.chars().flat_map(|c| [c, '_']).collect();
        cmp_char_to_encoding(dec.to_uppercase().as_bytes());
        // non-alnum high bytes interleaved (divergence #10 regression plane)
        let mut hb = Vec::new();
        for &b in n.as_bytes() {
            hb.push(b);
            hb.push(0xF1);
        }
        cmp_char_to_encoding(&hb);
    }
    // NAMEDATALEN boundary
    cmp_char_to_encoding(&[b'a'; 63]);
    cmp_char_to_encoding(&[b'a'; 64]);
    cmp_char_to_encoding(&[b'a'; 65]);
    let mut long_utf8 = vec![b'-'; 60];
    long_utf8.extend_from_slice(b"utf8");
    cmp_char_to_encoding(&long_utf8); // cleans to "utf8" but len>=64 pre-clean? no: 64 total
}

/// pg_encoding_to_char + max_length over the banded selector domain
/// (validity gate collapses everything outside 0..42; band covers both
/// sides of every comparison in the gate plus i32 extremes).
#[test]
fn q_encoding_to_char_and_max_length_band() {
    let _g = decoder_fuzz::c_oracle_serial();
    for enc in -1000..1000 {
        cmp_encoding_to_char(enc);
    }
    for enc in [i32::MIN, i32::MIN + 1, -42, i32::MAX - 1, i32::MAX] {
        cmp_encoding_to_char(enc);
    }
    for enc in 0..42 {
        cmp_max_length(enc);
    }
    for enc in 0..42 {
        if wchar::pg_encoding_max_length(enc) > 1 {
            cmp_set_invalid(enc);
        }
    }
}

/// check_encoding_conversion_args: full verdict grid.
#[test]
fn q_check_args_grid() {
    let _g = decoder_fuzz::c_oracle_serial();
    let encs = [-2, -1, 0, 1, 6, 34, 41, 42, 100];
    let exps = [-1, 0, 6, 34];
    for a in encs {
        for b in encs {
            for l in [-1, 0, 5] {
                for ea in exps {
                    for eb in exps {
                        cmp_check_args(a, b, l, ea, eb);
                    }
                }
            }
        }
    }
}

/// report_untranslatable_char sqlstate plane over encoding pairs x lead bytes.
#[test]
fn q_untranslatable_grid() {
    let _g = decoder_fuzz::c_oracle_serial();
    for src in REPS {
        for dst in [0, 6, 8] {
            for b0 in [0x41u8, 0x8d, 0xc3, 0xf0] {
                cmp_untranslatable(src, dst, &[b0, 0xa1]);
                cmp_untranslatable(src, dst, &[b0]);
            }
        }
    }
}

/// surrogate pair math: full in-contract domain (every first x second
/// surrogate, 2^20) + out-of-contract band grid.
#[test]
fn q_surrogate_pair_full_contract() {
    let _g = decoder_fuzz::c_oracle_serial();
    for first in 0xD800u32..=0xDBFF {
        for second in 0xDC00u32..=0xDFFF {
            cmp_surrogate_pair(first, second);
        }
    }
    for first in (0..0x2_0000u32).step_by(97) {
        for second in (0..0x2_0000u32).step_by(89) {
            cmp_surrogate_pair(first, second);
        }
    }
}

/// db walkers over structured short strings, every BE encoding (the fuzz
/// campaign owns long/adversarial streams; this pins the grid corners).
#[test]
fn q_dbwalk_grid() {
    let _g = decoder_fuzz::c_oracle_serial();
    let pats: &[&[u8]] = &[
        b"", b"a", b"abc", b"\x80", b"\xa1\xa1", b"\x8e\xa1", b"\x8f\xa1\xa1",
        b"\xc3\xa9", b"\xe6\xbc\xa2", b"\xf0\x9f\x8e\x88", b"\xff\xff\xff",
        b"a\x00b", b"\xa1\x00", b"\xa1",
    ];
    for enc in 0..35 {
        set_db_encoding_pub(enc);
        for p in pats {
            let mut data = vec![3u8, enc as u8, p.len() as u8, 3];
            data.extend_from_slice(p);
            wcharfam_diff(&data);
        }
    }
}

// -------------------------------------------------------------------------
// x tier: full 2^24 / 2^32 domains (ignored by default; run explicitly)
// -------------------------------------------------------------------------

/// verifychar: every 3-byte sequence, all 14 distinct kernels (2^24 each).
#[test]
#[ignore = "x-tier exhaustive: ~minutes, run explicitly in release"]
fn x_verifychar_3byte_full() {
    let _g = decoder_fuzz::c_oracle_serial();
    for enc in REPS {
        for w in 0..=0xFF_FFFFu32 {
            let b = [(w >> 16) as u8, (w >> 8) as u8, w as u8];
            if b[0] != 0 {
                cmp_verifychar(enc, &b);
            }
        }
        eprintln!("x_verifychar_3byte_full: enc {enc} done (16777216 seqs)");
    }
}

/// verifychar: every 4-byte sequence for the maxmblen-4 kernels (2^32 each).
#[test]
#[ignore = "x-tier exhaustive: full 2^32 x 4 kernels"]
fn x_verifychar_4byte_full() {
    let _g = decoder_fuzz::c_oracle_serial();
    for enc in [4i32, 6, 7, 39] {
        for w in 0..=u32::MAX {
            let b = w.to_be_bytes();
            if b[0] != 0 {
                cmp_verifychar(enc, &b);
            }
        }
        eprintln!("x_verifychar_4byte_full: enc {enc} done (4294967296 seqs)");
    }
}

/// UTF8 dsplen (utf8_to_unicode + mbbisearch + ucs_wcwidth chain): full
/// 4-byte domain.
#[test]
#[ignore = "x-tier exhaustive: full 2^32"]
fn x_utf8_dsplen_4byte_full() {
    let _g = decoder_fuzz::c_oracle_serial();
    for w in 0..=u32::MAX {
        let mut p = [0u8; 8];
        p[..4].copy_from_slice(&w.to_be_bytes());
        cmp_dsplen_only(6, &p);
    }
    eprintln!("x_utf8_dsplen_4byte_full: done (4294967296 seqs)");
}

/// GB18030 mblen second-byte plane: full 2-byte domain is in the quick
/// tier; this adds the full 4-byte mblen image for the 4-byte kernels.
#[test]
#[ignore = "x-tier exhaustive: full 2^32 x 4 kernels"]
fn x_mblen_4byte_full() {
    let _g = decoder_fuzz::c_oracle_serial();
    for enc in [4i32, 6, 7, 39] {
        for w in 0..=u32::MAX {
            let mut p = [0u8; 8];
            p[..4].copy_from_slice(&w.to_be_bytes());
            cmp_mblen_only(enc, &p);
        }
        eprintln!("x_mblen_4byte_full: enc {enc} done");
    }
}

/// unicode_to_utf8 + unicode_utf8len + codepoint predicates: full u32.
#[test]
#[ignore = "x-tier exhaustive: full 2^32"]
fn x_codepoint_full_u32() {
    let _g = decoder_fuzz::c_oracle_serial();
    for cp in 0..=u32::MAX {
        cmp_codepoint(cp);
    }
    eprintln!("x_codepoint_full_u32: done (4294967296 codepoints)");
}

/// utf8_to_unicode + pg_utf_mblen + pg_utf8_islegal(1..=4): full 4-byte
/// domain.
#[test]
#[ignore = "x-tier exhaustive: full 2^32"]
fn x_utf8_bytes_full() {
    let _g = decoder_fuzz::c_oracle_serial();
    for w in 0..=u32::MAX {
        cmp_utf8_bytes(&w.to_be_bytes());
    }
    eprintln!("x_utf8_bytes_full: done (4294967296 seqs)");
}

/// pg_utf8_increment: full 4-byte domain (whole-image + verdict).
#[test]
#[ignore = "x-tier exhaustive: full 2^32"]
fn x_utf8_increment_4byte_full() {
    let _g = decoder_fuzz::c_oracle_serial();
    for w in 0..=u32::MAX {
        cmp_utf8_increment(&w.to_be_bytes());
    }
    eprintln!("x_utf8_increment_4byte_full: done (4294967296 images)");
}

/// verifystr: every 3-byte stream, all 14 distinct kernels.
#[test]
#[ignore = "x-tier exhaustive: 2^24 x 14 kernels"]
fn x_verifystr_3byte_full() {
    let _g = decoder_fuzz::c_oracle_serial();
    for enc in REPS {
        for w in 0..=0xFF_FFFFu32 {
            let b = [(w >> 16) as u8, (w >> 8) as u8, w as u8];
            cmp_verifystr(enc, &b);
        }
        eprintln!("x_verifystr_3byte_full: enc {enc} done");
    }
}

/// pad8 is used by the quick tier only when buffers are shorter than the
/// dispatch functions' read contract.
#[allow(dead_code)]
fn _keep(b: &[u8]) -> [u8; 8] {
    pad8(b)
}

/// fc wrapper for pg_encoding_max_length (oid 2319): full i32 domain,
/// NULL arm included.
#[test]
#[ignore = "x-tier exhaustive: full 2^32 fcinfo calls (~minutes)"]
fn x_fc_max_length_full_i32() {
    let _g = decoder_fuzz::c_oracle_serial();
    let cx = mcx::MemoryContext::new("wcharfam_x_fc");
    let mut enc = i32::MIN;
    loop {
        cmp_fc_max_length(enc, &cx);
        if enc == i32::MAX {
            break;
        }
        enc += 1;
    }
    eprintln!("x_fc_max_length_full_i32: done (4294967296 calls)");
}

/// straggler planes: validity predicates (full band), db-encoding
/// mb2wchar/wchar2mb wrappers, out-of-contract increment/islegal lengths,
/// SetDatabaseEncoding error arm.
#[test]
fn q_stragglers() {
    let _g = decoder_fuzz::c_oracle_serial();
    for enc in -100..100 {
        check_encoding_predicates(enc);
    }
    check_encoding_predicates(i32::MIN);
    check_encoding_predicates(i32::MAX);
    let pats: &[&[u8]] = &[b"", b"abc", b"\xa1\xa1", b"\x8e\xa1", b"\xc3\xa9\x41", b"\xf0\x9f\x8e\x88"];
    for enc in 0..35 {
        for p in pats {
            cmp_db_mb2wchar_roundtrip(enc, p);
        }
    }
    // out-of-contract lengths: C switch default arms
    for l in [0, 5, 6, -1] {
        cmp_islegal_len(&[0xf0, 0x9f, 0x8e, 0x88], l);
    }
    for n in [5usize, 6, 8] {
        cmp_utf8_increment(&vec![0x41u8; n]);
    }
    assert!(mbutils::SetDatabaseEncoding(99).is_err());
    assert!(mbutils::SetDatabaseEncoding(-1).is_err());
}
