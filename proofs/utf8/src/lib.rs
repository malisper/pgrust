//! Kani C≡Rust equivalence proofs: PostgreSQL UTF-8 validation
//! (pg_utf8_islegal + pg_utf_mblen) vs the shipped pgrust `wchar` crate.

#[cfg(kani)]
mod proofs {
    extern "C" {
        fn c_pg_utf8_islegal(source: *const u8, length: i32) -> i32;
        fn c_pg_utf_mblen(s: *const u8) -> i32;
    }

    fn check_islegal(src: [u8; 4], len: i32) {
        let c = unsafe { c_pg_utf8_islegal(src.as_ptr(), len) } != 0;
        let r = wchar::pg_utf8_islegal(&src, len);
        assert!(c == r, "divergence: C != Rust for pg_utf8_islegal");
    }

    #[kani::proof]
    fn islegal_len1() {
        check_islegal(kani::any(), 1);
    }

    #[kani::proof]
    fn islegal_len2() {
        check_islegal(kani::any(), 2);
    }

    #[kani::proof]
    fn islegal_len3() {
        check_islegal(kani::any(), 3);
    }

    #[kani::proof]
    fn islegal_len4() {
        check_islegal(kani::any(), 4);
    }

    /// Symbolic length within the documented contract (length comes from
    /// pg_utf_mblen, so 1..=4).
    #[kani::proof]
    fn islegal_symlen_contract() {
        let len: i32 = kani::any();
        kani::assume((1..=4).contains(&len));
        check_islegal(kani::any(), len);
    }

    /// Out-of-contract probe: lengths 0..=8. C's `default:` returns false;
    /// the Rust port's `_ => {}` arm falls through to first-byte checks.
    #[kani::proof]
    fn islegal_symlen_out_of_contract() {
        let len: i32 = kani::any();
        kani::assume((0..=8).contains(&len));
        check_islegal(kani::any(), len);
    }

    #[kani::proof]
    fn mblen_all_bytes() {
        let src: [u8; 1] = kani::any();
        let c = unsafe { c_pg_utf_mblen(src.as_ptr()) };
        let r = wchar::pg_utf_mblen(&src);
        assert!(c == r, "divergence: C != Rust for pg_utf_mblen");
    }

    /// Composed check: for every 4-byte sequence, mblen agrees AND
    /// islegal at that mblen agrees (the way callers actually use them).
    #[kani::proof]
    fn mblen_then_islegal() {
        let src: [u8; 4] = kani::any();
        let c_len = unsafe { c_pg_utf_mblen(src.as_ptr()) };
        let r_len = wchar::pg_utf_mblen(&src);
        assert!(c_len == r_len);
        check_islegal(src, c_len);
    }
}

/// Per-encoding length/display-width kernels (src/common/wchar.c) vs the
/// shipped pgrust `wchar` crate, reached through the public dispatchers
/// `pg_encoding_mblen`/`pg_encoding_dsplen` with a concrete encoding — this
/// proves both the kernel body AND the pg_wchar_table row wiring.
/// C side: c/pg_wchar_kernels.c (vendored verbatim).
#[cfg(kani)]
mod kernel_proofs {
    use wchar::*;

    extern "C" {
        fn pg_ascii_mblen(s: *const u8) -> i32;
        fn pg_ascii_dsplen(s: *const u8) -> i32;
        fn pg_eucjp_mblen(s: *const u8) -> i32;
        fn pg_eucjp_dsplen(s: *const u8) -> i32;
        fn pg_euckr_mblen(s: *const u8) -> i32;
        fn pg_euckr_dsplen(s: *const u8) -> i32;
        fn pg_euccn_mblen(s: *const u8) -> i32;
        fn pg_euccn_dsplen(s: *const u8) -> i32;
        fn pg_euctw_mblen(s: *const u8) -> i32;
        fn pg_euctw_dsplen(s: *const u8) -> i32;
        fn pg_johab_mblen(s: *const u8) -> i32;
        fn pg_johab_dsplen(s: *const u8) -> i32;
        fn pg_mule_mblen(s: *const u8) -> i32;
        fn pg_mule_dsplen(s: *const u8) -> i32;
        fn pg_latin1_mblen(s: *const u8) -> i32;
        fn pg_latin1_dsplen(s: *const u8) -> i32;
        fn pg_sjis_mblen(s: *const u8) -> i32;
        fn pg_sjis_dsplen(s: *const u8) -> i32;
        fn pg_big5_mblen(s: *const u8) -> i32;
        fn pg_big5_dsplen(s: *const u8) -> i32;
        fn pg_gbk_mblen(s: *const u8) -> i32;
        fn pg_gbk_dsplen(s: *const u8) -> i32;
        fn pg_uhc_mblen(s: *const u8) -> i32;
        fn pg_uhc_dsplen(s: *const u8) -> i32;
        fn pg_gb18030_mblen(s: *const u8) -> i32;
        fn pg_gb18030_dsplen(s: *const u8) -> i32;
        fn pg_utf_dsplen(s: *const u8) -> i32;
        fn ucs_wcwidth(ucs: u32) -> i32;
        fn c_in_nonspacing(ucs: u32) -> i32;
        fn c_in_east_asian_fw(ucs: u32) -> i32;
    }

    /// One harness per (encoding row, kernel): symbolic N-byte input, C
    /// kernel vs Rust dispatch through the concrete-encoding table row.
    macro_rules! eq_mblen {
        ($name:ident, $cfn:ident, $enc:expr, $n:literal) => {
            #[kani::proof]
            fn $name() {
                let s: [u8; $n] = kani::any();
                let c = unsafe { $cfn(s.as_ptr()) };
                let r = wchar::pg_encoding_mblen($enc, &s);
                assert!(c == r, "divergence: C != Rust mblen");
            }
        };
    }
    macro_rules! eq_dsplen {
        ($name:ident, $cfn:ident, $enc:expr, $n:literal) => {
            #[kani::proof]
            fn $name() {
                let s: [u8; $n] = kani::any();
                let c = unsafe { $cfn(s.as_ptr()) };
                let r = wchar::pg_encoding_dsplen($enc, &s);
                assert!(c == r, "divergence: C != Rust dsplen");
            }
        };
    }

    eq_mblen!(eq_ascii_mblen, pg_ascii_mblen, PG_SQL_ASCII, 1);
    eq_dsplen!(eq_ascii_dsplen, pg_ascii_dsplen, PG_SQL_ASCII, 1);
    eq_mblen!(eq_eucjp_mblen, pg_eucjp_mblen, PG_EUC_JP, 1);
    eq_dsplen!(eq_eucjp_dsplen, pg_eucjp_dsplen, PG_EUC_JP, 1);
    eq_mblen!(eq_eucjis2004_mblen, pg_eucjp_mblen, PG_EUC_JIS_2004, 1);
    eq_dsplen!(eq_eucjis2004_dsplen, pg_eucjp_dsplen, PG_EUC_JIS_2004, 1);
    eq_mblen!(eq_euckr_mblen, pg_euckr_mblen, PG_EUC_KR, 1);
    eq_dsplen!(eq_euckr_dsplen, pg_euckr_dsplen, PG_EUC_KR, 1);
    eq_mblen!(eq_euccn_mblen, pg_euccn_mblen, PG_EUC_CN, 1);
    eq_dsplen!(eq_euccn_dsplen, pg_euccn_dsplen, PG_EUC_CN, 1);
    eq_mblen!(eq_euctw_mblen, pg_euctw_mblen, PG_EUC_TW, 1);
    eq_dsplen!(eq_euctw_dsplen, pg_euctw_dsplen, PG_EUC_TW, 1);
    eq_mblen!(eq_johab_mblen, pg_johab_mblen, PG_JOHAB, 1);
    eq_dsplen!(eq_johab_dsplen, pg_johab_dsplen, PG_JOHAB, 1);
    eq_mblen!(eq_mule_mblen, pg_mule_mblen, PG_MULE_INTERNAL, 1);
    eq_dsplen!(eq_mule_dsplen, pg_mule_dsplen, PG_MULE_INTERNAL, 1);
    eq_mblen!(eq_latin1_mblen, pg_latin1_mblen, PG_LATIN1, 1);
    eq_dsplen!(eq_latin1_dsplen, pg_latin1_dsplen, PG_LATIN1, 1);
    eq_mblen!(eq_sjis_mblen, pg_sjis_mblen, PG_SJIS, 1);
    eq_dsplen!(eq_sjis_dsplen, pg_sjis_dsplen, PG_SJIS, 1);
    eq_mblen!(eq_sjis2004_mblen, pg_sjis_mblen, PG_SHIFT_JIS_2004, 1);
    eq_dsplen!(eq_sjis2004_dsplen, pg_sjis_dsplen, PG_SHIFT_JIS_2004, 1);
    eq_mblen!(eq_big5_mblen, pg_big5_mblen, PG_BIG5, 1);
    eq_dsplen!(eq_big5_dsplen, pg_big5_dsplen, PG_BIG5, 1);
    eq_mblen!(eq_gbk_mblen, pg_gbk_mblen, PG_GBK, 1);
    eq_dsplen!(eq_gbk_dsplen, pg_gbk_dsplen, PG_GBK, 1);
    eq_mblen!(eq_uhc_mblen, pg_uhc_mblen, PG_UHC, 1);
    eq_dsplen!(eq_uhc_dsplen, pg_uhc_dsplen, PG_UHC, 1);
    // GB18030 mblen reads s[1] whenever the high bit of s[0] is set — the
    // documented intentional over-read; the 2-byte input mirrors that C
    // contract (callers guarantee the lookahead byte, cf.
    // pg_encoding_mblen_or_incomplete).
    eq_mblen!(eq_gb18030_mblen, pg_gb18030_mblen, PG_GB18030, 2);
    eq_dsplen!(eq_gb18030_dsplen, pg_gb18030_dsplen, PG_GB18030, 1);
    // UTF-8 dsplen: composed utf8_to_unicode -> ucs_wcwidth -> mbbisearch
    // over both mbinterval tables. Reads up to 4 bytes. Case-split on the
    // lead-byte class (whole-domain harness solves in 18s, over the 10s
    // budget); union coverage harness below keeps the gate complete.
    fn check_utf8_dsplen(s: [u8; 4]) {
        let c = unsafe { pg_utf_dsplen(s.as_ptr()) };
        let r = wchar::pg_encoding_dsplen(PG_UTF8, &s);
        assert!(c == r, "divergence: C != Rust pg_utf_dsplen");
    }

    #[kani::proof]
    #[kani::unwind(12)]
    fn eq_utf8_dsplen_lead1() {
        let s: [u8; 4] = kani::any();
        kani::assume(s[0] & 0x80 == 0);
        check_utf8_dsplen(s);
    }

    #[kani::proof]
    #[kani::unwind(12)]
    fn eq_utf8_dsplen_lead2() {
        let s: [u8; 4] = kani::any();
        kani::assume(s[0] & 0xe0 == 0xc0);
        check_utf8_dsplen(s);
    }

    // lead3/lead4 are halved again on the low lead bits (whole-class
    // harnesses solved in 12.5s/14.6s, over the 10s budget). The halves
    // are complementary assumes on the same bit, so the union is the
    // whole class by construction (see utf8_dsplen_partition_coverage).
    #[kani::proof]
    #[kani::unwind(12)]
    fn eq_utf8_dsplen_lead3_lo() {
        let s: [u8; 4] = kani::any();
        kani::assume(s[0] & 0xf0 == 0xe0 && s[0] & 0x08 == 0);
        check_utf8_dsplen(s);
    }

    #[kani::proof]
    #[kani::unwind(12)]
    fn eq_utf8_dsplen_lead3_hi() {
        let s: [u8; 4] = kani::any();
        kani::assume(s[0] & 0xf0 == 0xe0 && s[0] & 0x08 != 0);
        check_utf8_dsplen(s);
    }

    #[kani::proof]
    #[kani::unwind(12)]
    fn eq_utf8_dsplen_lead4_lo() {
        let s: [u8; 4] = kani::any();
        kani::assume(s[0] & 0xf8 == 0xf0 && s[0] & 0x04 == 0);
        check_utf8_dsplen(s);
    }

    #[kani::proof]
    #[kani::unwind(12)]
    fn eq_utf8_dsplen_lead4_hi() {
        let s: [u8; 4] = kani::any();
        kani::assume(s[0] & 0xf8 == 0xf0 && s[0] & 0x04 != 0);
        check_utf8_dsplen(s);
    }

    #[kani::proof]
    #[kani::unwind(12)]
    fn eq_utf8_dsplen_lead_invalid() {
        let s: [u8; 4] = kani::any();
        kani::assume(
            s[0] & 0x80 != 0
                && s[0] & 0xe0 != 0xc0
                && s[0] & 0xf0 != 0xe0
                && s[0] & 0xf8 != 0xf0,
        );
        check_utf8_dsplen(s);
    }

    /// MANDATORY union-coverage companion for the case-split above: every
    /// lead byte falls in exactly one of the five partitions.
    #[kani::proof]
    fn utf8_dsplen_partition_coverage() {
        let b: u8 = kani::any();
        let p1 = b & 0x80 == 0;
        let p2 = b & 0xe0 == 0xc0;
        let p3 = b & 0xf0 == 0xe0;
        let p4 = b & 0xf8 == 0xf0;
        let p5 = b & 0x80 != 0 && !p2 && !p3 && !p4;
        assert!(p1 || p2 || p3 || p4 || p5);
        assert!(
            (p1 as u8 + p2 as u8 + p3 as u8 + p4 as u8 + p5 as u8) == 1,
            "partitions overlap"
        );
    }

    /// The 27 single-byte server encodings (PG_LATIN1..=PG_KOI8U) all share
    /// the latin1 kernels: symbolic encoding over the whole band proves every
    /// row's wiring at once.
    #[kani::proof]
    fn eq_single_byte_band_mblen() {
        let enc: i32 = kani::any();
        kani::assume((PG_LATIN1..=PG_KOI8U).contains(&enc));
        let s: [u8; 1] = kani::any();
        let c = unsafe { pg_latin1_mblen(s.as_ptr()) };
        let r = wchar::pg_encoding_mblen(enc, &s);
        assert!(c == r, "divergence: single-byte band mblen");
    }

    // unwind: the symbolic-encoding dispatch makes every dsplen row a
    // possible fn-pointer target, including pg_utf_dsplen -> mbbisearch
    // (data-dependent loop; unbounded symex hang without the bound).
    #[kani::proof]
    #[kani::unwind(12)]
    fn eq_single_byte_band_dsplen() {
        let enc: i32 = kani::any();
        kani::assume((PG_LATIN1..=PG_KOI8U).contains(&enc));
        let s: [u8; 1] = kani::any();
        let c = unsafe { pg_latin1_dsplen(s.as_ptr()) };
        let r = wchar::pg_encoding_dsplen(enc, &s);
        assert!(c == r, "divergence: single-byte band dsplen");
    }

    /// Out-of-range encodings fall back to the SQL_ASCII row (C: identical
    /// guard in pg_encoding_mblen; Rust: table_index).
    #[kani::proof]
    #[kani::unwind(12)] // symbolic dispatch can target pg_utf_dsplen, see above
    fn eq_invalid_encoding_fallback() {
        let enc: i32 = kani::any();
        kani::assume(!pg_valid_encoding(enc));
        let s: [u8; 1] = kani::any();
        let cm = unsafe { pg_ascii_mblen(s.as_ptr()) };
        let cd = unsafe { pg_ascii_dsplen(s.as_ptr()) };
        assert!(cm == wchar::pg_encoding_mblen(enc, &s));
        assert!(cd == wchar::pg_encoding_dsplen(enc, &s));
    }

    /// Direct mbbisearch equivalence over the full u32 domain, per table
    /// (C tables = verbatim generated headers; Rust tables = tables.rs).
    #[kani::proof]
    #[kani::unwind(12)]
    fn eq_mbbisearch_nonspacing() {
        let ucs: u32 = kani::any();
        let c = unsafe { c_in_nonspacing(ucs) } != 0;
        let r = wchar::mbbisearch(ucs, &wchar::NONSPACING);
        assert!(c == r, "divergence: mbbisearch nonspacing");
    }

    #[kani::proof]
    #[kani::unwind(12)]
    fn eq_mbbisearch_east_asian_fw() {
        let ucs: u32 = kani::any();
        let c = unsafe { c_in_east_asian_fw(ucs) } != 0;
        let r = wchar::mbbisearch(ucs, &wchar::EAST_ASIAN_FW);
        assert!(c == r, "divergence: mbbisearch east_asian_fw");
    }

    /// ucs_wcwidth over the full u32 domain (superset of what UTF-8 input
    /// can reach; covers both tables and all width branches).
    #[kani::proof]
    #[kani::unwind(12)]
    fn eq_ucs_wcwidth_full_u32() {
        let ucs: u32 = kani::any();
        let c = unsafe { ucs_wcwidth(ucs) };
        let r = wchar::ucs_wcwidth(ucs);
        assert!(c == r, "divergence: ucs_wcwidth");
    }

    /// NEGATIVE CONTROL — must FAIL (proves the rig is non-vacuous):
    /// SJIS mblen deliberately compared against the Big5 C kernel; they
    /// differ on 0xa1..=0xdf (1-byte kana vs 2).
    #[kani::proof]
    fn control_sjis_vs_big5_mblen_must_fail() {
        let s: [u8; 1] = kani::any();
        let c = unsafe { pg_big5_mblen(s.as_ptr()) };
        let r = wchar::pg_encoding_mblen(PG_SJIS, &s);
        assert!(c == r, "expected failure: sjis vs big5");
    }
}
