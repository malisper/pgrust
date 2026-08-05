//! Kani C≡Rust equivalence proofs: the encoding-conversion pg_proc family
//! (oids 4302..=4387, crates/backend/utils/mb/conv) vs verbatim PostgreSQL
//! REL_18_STABLE C (conv.c engines + conversion_procs bodies + Unicode
//! radix-tree maps, vendored in c/ — see each file's provenance header).
//!
//! Harness shape (wrapper-level): each proof builds a real 6-arg
//! `LocalFcinfo` frame (src_encoding, dest_encoding, src ptr, dest ptr,
//! len, noError) and calls the SHIPPED fc_* conversion function directly —
//! so the fcinfo arg unwrap, mbutils::check_encoding_conversion_args, the
//! conversion engine, the committed Rust map tables, and the Datum pack are
//! all inside the theorem. The oid→function wiring (CONV_BUILTINS foid +
//! fn-pointer identity, all 84 entries) is asserted ONCE by the dedicated
//! wiring_conv_builtins harness: referencing CONV_BUILTINS from every
//! harness dragged all conversions + the Rust radix tables into each goto
//! program (measured ~45s fixed CBMC cost per harness vs ~5s pruned; the
//! run recipe likewise passes only the family's own --c-lib files).
//! The C side runs the vendored proc shim on the same symbolic
//! bytes. Asserted, for every input up to the stated bounds:
//!   * success arm: consumed-length parity AND full destination-buffer byte
//!     parity (including the trailing NUL and untouched 0xAA tail);
//!   * error arm: verdict parity and error-CLASS parity (C's
//!     PROOF_EREPORT_FLAG class vs the shipped Rust sqlstate:
//!     1 ↔ 22021 invalid byte sequence, 2 ↔ 22P05 untranslatable,
//!     3 ↔ {22023 invalid encoding number, XX000 unexpected encoding id}).
//!     Dest contents on the error arm are out of the claim (C longjmps and
//!     discards the buffer; consumed count is likewise undefined there).
//!
//! Stubs (all proof_support / documented): PgError::error (Location::caller
//! is Kani-unsupported; field-identical constructor, sqlstate stays shipped),
//! alloc::fmt::format and mbutils::byte_sequence (message-TEXT machinery
//! that walls symex). Ledger wording: "value-space + verdict + sqlstate;
//! message text/location out of proof".
//!
//! The dropped C CHECK_ENCODING_CONVERSION_ARGS macro is covered separately
//! by eq_check_encoding_conversion_args (full symbolic 5-arg domain,
//! verdict parity vs the shipped mbutils::check_encoding_conversion_args).

#[cfg(kani)]
mod proofs {
    use datum::Datum;
    use std::os::raw::c_int;
    use types_error::{
        PgError, ERRCODE_CHARACTER_NOT_IN_REPERTOIRE, ERRCODE_INTERNAL_ERROR,
        ERRCODE_INVALID_PARAMETER_VALUE, ERRCODE_UNTRANSLATABLE_CHARACTER,
    };
    use types_fmgr::{FmgrInfo, FunctionCallInfoBaseData};
    use wchar::*;

    /// Statically-dispatched fcinfo call: generic F keeps the fc_* target a
    /// direct call (a fn POINTER — proof_support::call / CONV_BUILTINS.func —
    /// cannot be devirtualized by CBMC, which then puts every conversion in
    /// the crate into the formula; measured 25s+ fixed cost per harness).
    fn call_fc<F>(fc: F, args: [Datum; 6]) -> types_error::PgResult<Datum>
    where
        F: FnOnce(Option<&mut FmgrInfo>, &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum>,
    {
        let mut f = proof_support::fci(args);
        fc(None, &mut f)
    }

    /// Statement-style vendored proc shim (fmgr unwrap -> scalars).
    type CConv = unsafe extern "C" fn(*const u8, *mut u8, c_int, bool) -> c_int;
    /// Encoding-dispatching proc shim (win/iso8859 families).
    type CConvEnc = unsafe extern "C" fn(c_int, *const u8, *mut u8, c_int, bool) -> c_int;

    extern "C" {
        static mut pg_mbconv_err: c_int;

        fn pg_check_encoding_conversion_args(
            src_encoding: c_int,
            dest_encoding: c_int,
            len: c_int,
            expected_src_encoding: c_int,
            expected_dest_encoding: c_int,
        ) -> c_int;

        // cyrillic_and_mic
        fn pg_koi8r_to_mic(s: *const u8, d: *mut u8, l: c_int, ne: bool) -> c_int;
        fn pg_mic_to_koi8r(s: *const u8, d: *mut u8, l: c_int, ne: bool) -> c_int;
        fn pg_iso_to_mic(s: *const u8, d: *mut u8, l: c_int, ne: bool) -> c_int;
        fn pg_mic_to_iso(s: *const u8, d: *mut u8, l: c_int, ne: bool) -> c_int;
        fn pg_win1251_to_mic(s: *const u8, d: *mut u8, l: c_int, ne: bool) -> c_int;
        fn pg_mic_to_win1251(s: *const u8, d: *mut u8, l: c_int, ne: bool) -> c_int;
        fn pg_win866_to_mic(s: *const u8, d: *mut u8, l: c_int, ne: bool) -> c_int;
        fn pg_mic_to_win866(s: *const u8, d: *mut u8, l: c_int, ne: bool) -> c_int;
        fn pg_koi8r_to_win1251(s: *const u8, d: *mut u8, l: c_int, ne: bool) -> c_int;
        fn pg_win1251_to_koi8r(s: *const u8, d: *mut u8, l: c_int, ne: bool) -> c_int;
        fn pg_koi8r_to_win866(s: *const u8, d: *mut u8, l: c_int, ne: bool) -> c_int;
        fn pg_win866_to_koi8r(s: *const u8, d: *mut u8, l: c_int, ne: bool) -> c_int;
        fn pg_win866_to_win1251(s: *const u8, d: *mut u8, l: c_int, ne: bool) -> c_int;
        fn pg_win1251_to_win866(s: *const u8, d: *mut u8, l: c_int, ne: bool) -> c_int;
        fn pg_iso_to_koi8r(s: *const u8, d: *mut u8, l: c_int, ne: bool) -> c_int;
        fn pg_koi8r_to_iso(s: *const u8, d: *mut u8, l: c_int, ne: bool) -> c_int;
        fn pg_iso_to_win1251(s: *const u8, d: *mut u8, l: c_int, ne: bool) -> c_int;
        fn pg_win1251_to_iso(s: *const u8, d: *mut u8, l: c_int, ne: bool) -> c_int;
        fn pg_iso_to_win866(s: *const u8, d: *mut u8, l: c_int, ne: bool) -> c_int;
        fn pg_win866_to_iso(s: *const u8, d: *mut u8, l: c_int, ne: bool) -> c_int;
        // euc_cn / euc_kr
        fn pg_euc_cn_to_mic(s: *const u8, d: *mut u8, l: c_int, ne: bool) -> c_int;
        fn pg_mic_to_euc_cn(s: *const u8, d: *mut u8, l: c_int, ne: bool) -> c_int;
        fn pg_euc_kr_to_mic(s: *const u8, d: *mut u8, l: c_int, ne: bool) -> c_int;
        fn pg_mic_to_euc_kr(s: *const u8, d: *mut u8, l: c_int, ne: bool) -> c_int;
        // euc_jp / sjis
        fn pg_euc_jp_to_sjis(s: *const u8, d: *mut u8, l: c_int, ne: bool) -> c_int;
        fn pg_sjis_to_euc_jp(s: *const u8, d: *mut u8, l: c_int, ne: bool) -> c_int;
        fn pg_euc_jp_to_mic(s: *const u8, d: *mut u8, l: c_int, ne: bool) -> c_int;
        fn pg_sjis_to_mic(s: *const u8, d: *mut u8, l: c_int, ne: bool) -> c_int;
        fn pg_mic_to_euc_jp(s: *const u8, d: *mut u8, l: c_int, ne: bool) -> c_int;
        fn pg_mic_to_sjis(s: *const u8, d: *mut u8, l: c_int, ne: bool) -> c_int;
        // euc_tw / big5
        fn pg_euc_tw_to_big5(s: *const u8, d: *mut u8, l: c_int, ne: bool) -> c_int;
        fn pg_big5_to_euc_tw(s: *const u8, d: *mut u8, l: c_int, ne: bool) -> c_int;
        fn pg_euc_tw_to_mic(s: *const u8, d: *mut u8, l: c_int, ne: bool) -> c_int;
        fn pg_big5_to_mic(s: *const u8, d: *mut u8, l: c_int, ne: bool) -> c_int;
        fn pg_mic_to_euc_tw(s: *const u8, d: *mut u8, l: c_int, ne: bool) -> c_int;
        fn pg_mic_to_big5(s: *const u8, d: *mut u8, l: c_int, ne: bool) -> c_int;
        // latin2 / win1250
        fn pg_latin2_to_mic(s: *const u8, d: *mut u8, l: c_int, ne: bool) -> c_int;
        fn pg_mic_to_latin2(s: *const u8, d: *mut u8, l: c_int, ne: bool) -> c_int;
        fn pg_win1250_to_mic(s: *const u8, d: *mut u8, l: c_int, ne: bool) -> c_int;
        fn pg_mic_to_win1250(s: *const u8, d: *mut u8, l: c_int, ne: bool) -> c_int;
        fn pg_latin2_to_win1250(s: *const u8, d: *mut u8, l: c_int, ne: bool) -> c_int;
        fn pg_win1250_to_latin2(s: *const u8, d: *mut u8, l: c_int, ne: bool) -> c_int;
        // latin_and_mic
        fn pg_latin1_to_mic(s: *const u8, d: *mut u8, l: c_int, ne: bool) -> c_int;
        fn pg_mic_to_latin1(s: *const u8, d: *mut u8, l: c_int, ne: bool) -> c_int;
        fn pg_latin3_to_mic(s: *const u8, d: *mut u8, l: c_int, ne: bool) -> c_int;
        fn pg_mic_to_latin3(s: *const u8, d: *mut u8, l: c_int, ne: bool) -> c_int;
        fn pg_latin4_to_mic(s: *const u8, d: *mut u8, l: c_int, ne: bool) -> c_int;
        fn pg_mic_to_latin4(s: *const u8, d: *mut u8, l: c_int, ne: bool) -> c_int;
        // euc2004 <-> sjis2004
        fn pg_euc_jis_2004_to_shift_jis_2004(s: *const u8, d: *mut u8, l: c_int, ne: bool) -> c_int;
        fn pg_shift_jis_2004_to_euc_jis_2004(s: *const u8, d: *mut u8, l: c_int, ne: bool) -> c_int;
        // utf8 pairs (statement-style)
        fn pg_iso8859_1_to_utf8(s: *const u8, d: *mut u8, l: c_int, ne: bool) -> c_int;
        fn pg_utf8_to_iso8859_1(s: *const u8, d: *mut u8, l: c_int, ne: bool) -> c_int;
        fn pg_koi8r_to_utf8(s: *const u8, d: *mut u8, l: c_int, ne: bool) -> c_int;
        fn pg_utf8_to_koi8r(s: *const u8, d: *mut u8, l: c_int, ne: bool) -> c_int;
        fn pg_koi8u_to_utf8(s: *const u8, d: *mut u8, l: c_int, ne: bool) -> c_int;
        fn pg_utf8_to_koi8u(s: *const u8, d: *mut u8, l: c_int, ne: bool) -> c_int;
        fn pg_big5_to_utf8(s: *const u8, d: *mut u8, l: c_int, ne: bool) -> c_int;
        fn pg_utf8_to_big5(s: *const u8, d: *mut u8, l: c_int, ne: bool) -> c_int;
        fn pg_euc_cn_to_utf8(s: *const u8, d: *mut u8, l: c_int, ne: bool) -> c_int;
        fn pg_utf8_to_euc_cn(s: *const u8, d: *mut u8, l: c_int, ne: bool) -> c_int;
        fn pg_euc_jp_to_utf8(s: *const u8, d: *mut u8, l: c_int, ne: bool) -> c_int;
        fn pg_utf8_to_euc_jp(s: *const u8, d: *mut u8, l: c_int, ne: bool) -> c_int;
        fn pg_euc_kr_to_utf8(s: *const u8, d: *mut u8, l: c_int, ne: bool) -> c_int;
        fn pg_utf8_to_euc_kr(s: *const u8, d: *mut u8, l: c_int, ne: bool) -> c_int;
        fn pg_euc_tw_to_utf8(s: *const u8, d: *mut u8, l: c_int, ne: bool) -> c_int;
        fn pg_utf8_to_euc_tw(s: *const u8, d: *mut u8, l: c_int, ne: bool) -> c_int;
        fn pg_gb18030_to_utf8(s: *const u8, d: *mut u8, l: c_int, ne: bool) -> c_int;
        fn pg_utf8_to_gb18030(s: *const u8, d: *mut u8, l: c_int, ne: bool) -> c_int;
        fn pg_gbk_to_utf8(s: *const u8, d: *mut u8, l: c_int, ne: bool) -> c_int;
        fn pg_utf8_to_gbk(s: *const u8, d: *mut u8, l: c_int, ne: bool) -> c_int;
        fn pg_johab_to_utf8(s: *const u8, d: *mut u8, l: c_int, ne: bool) -> c_int;
        fn pg_utf8_to_johab(s: *const u8, d: *mut u8, l: c_int, ne: bool) -> c_int;
        fn pg_sjis_to_utf8(s: *const u8, d: *mut u8, l: c_int, ne: bool) -> c_int;
        fn pg_utf8_to_sjis(s: *const u8, d: *mut u8, l: c_int, ne: bool) -> c_int;
        fn pg_uhc_to_utf8(s: *const u8, d: *mut u8, l: c_int, ne: bool) -> c_int;
        fn pg_utf8_to_uhc(s: *const u8, d: *mut u8, l: c_int, ne: bool) -> c_int;
        fn pg_euc_jis_2004_to_utf8(s: *const u8, d: *mut u8, l: c_int, ne: bool) -> c_int;
        fn pg_utf8_to_euc_jis_2004(s: *const u8, d: *mut u8, l: c_int, ne: bool) -> c_int;
        fn pg_shift_jis_2004_to_utf8(s: *const u8, d: *mut u8, l: c_int, ne: bool) -> c_int;
        fn pg_utf8_to_shift_jis_2004(s: *const u8, d: *mut u8, l: c_int, ne: bool) -> c_int;
        // encoding-dispatching pairs
        fn pg_win_to_utf8(e: c_int, s: *const u8, d: *mut u8, l: c_int, ne: bool) -> c_int;
        fn pg_utf8_to_win(e: c_int, s: *const u8, d: *mut u8, l: c_int, ne: bool) -> c_int;
        fn pg_iso8859_to_utf8(e: c_int, s: *const u8, d: *mut u8, l: c_int, ne: bool) -> c_int;
        fn pg_utf8_to_iso8859(e: c_int, s: *const u8, d: *mut u8, l: c_int, ne: bool) -> c_int;
    }

    /// Stub for `mbutils::byte_sequence` (invalid-byte error-message detail:
    /// a symbolic String build that walls symex). Message text leaves the
    /// proof; verdict/sqlstate stay in (text-slice precedent).
    pub fn stub_byte_sequence(_mbstr: &[u8], _mblen: i32, _len: i32) -> String {
        String::new()
    }

    /// Stub for `wchar::pg_encoding_mblen_or_incomplete`: inside the mbutils
    /// error reporters its result feeds ONLY the (stubbed) byte_sequence
    /// message detail, but its fn-pointer table dispatch drags every wchar
    /// kernel into the formula. Message-text plumbing only — same soundness
    /// contract as stub_byte_sequence.
    pub fn stub_mblen_or_incomplete(_encoding: i32, _mbstr: &[u8]) -> i32 {
        1
    }

    /// Map the shipped Rust error to the C-side PROOF_EREPORT_FLAG class.
    fn rust_err_class(e: &PgError) -> i32 {
        if e.sqlstate == ERRCODE_CHARACTER_NOT_IN_REPERTOIRE {
            1
        } else if e.sqlstate == ERRCODE_UNTRANSLATABLE_CHARACTER {
            2
        } else if e.sqlstate == ERRCODE_INVALID_PARAMETER_VALUE
            || e.sqlstate == ERRCODE_INTERNAL_ERROR
        {
            3
        } else {
            -1 // unknown class: fails the parity assert loudly
        }
    }

    /// Shared checker: symbolic src (cap L), symbolic len <= L, symbolic
    /// noError; runs C shim and the shipped Rust builtin (by pg_proc oid,
    /// through a real fcinfo frame) and asserts the parity contract from
    /// the module doc. D must be >= 4*L + 1 (MAX_CONVERSION_GROWTH + NUL).
    fn check_conv<F, const L: usize, const D: usize>(
        _oid: u32,
        fc: F,
        cfn: CConv,
        src_enc: i32,
        dst_enc: i32,
    ) where
        F: FnOnce(Option<&mut FmgrInfo>, &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum>,
    {
        let src: [u8; L] = kani::any();
        let len: i32 = kani::any();
        kani::assume(len >= 0 && (len as usize) <= L);
        let no_error: bool = kani::any();

        let mut cdst = [0xAAu8; D];
        let mut rdst = [0xAAu8; D];

        let c = unsafe { cfn(src.as_ptr(), cdst.as_mut_ptr(), len, no_error) };
        let cerr = unsafe { pg_mbconv_err };

        // pg_proc wiring (foid + fn-pointer identity) is covered by the
        // dedicated wiring_conv_builtins harness, not per-conversion (see
        // conv_proof! comment).
        let r = call_fc(
            fc,
            [
                Datum::from_i32(src_enc),
                Datum::from_i32(dst_enc),
                Datum::from_usize(src.as_ptr() as usize),
                Datum::from_usize(rdst.as_mut_ptr() as usize),
                Datum::from_i32(len),
                Datum::from_bool(no_error),
            ],
        );

        // Vacuity insurance (skill: fallible-op harnesses cover-witness BOTH
        // arms). Both arms are reachable in every check_conv harness: Ok via
        // len==0, Err via a NUL source byte with noError=false (every vendored
        // engine reports invalid encoding on c1==0).
        kani::cover!(r.is_ok(), "Ok arm reachable");
        kani::cover!(r.is_err(), "Err arm reachable");
        match r {
            Ok(d) => {
                assert!(cerr == 0, "C errored where Rust succeeded");
                assert!(c == d.as_i32(), "consumed-length divergence");
                // single-assert byte compare (kissat re-solves per property)
                let mut eq = true;
                let mut i = 0;
                while i < D {
                    eq = eq && (cdst[i] == rdst[i]);
                    i += 1;
                }
                assert!(eq, "dest byte divergence");
            }
            Err(e) => {
                assert!(c == -1, "Rust errored where C succeeded");
                assert!(cerr == rust_err_class(&e), "error-class divergence");
                core::mem::forget(e); // Box<PgError> drop glue walls symex
            }
        }
    }

    /// Same as check_conv for the encoding-dispatching procs (win/iso8859):
    /// `enc` is the symbolic family-band local encoding, passed both to the
    /// C shim and as the fcinfo src/dest encoding arg.
    fn check_conv_enc<F, const L: usize, const D: usize>(
        _oid: u32,
        fc: F,
        cfn: CConvEnc,
        enc: i32,
        to_utf8: bool,
        ok_reachable: bool, // false only for the out-of-family arm (always errors)
    ) where
        F: FnOnce(Option<&mut FmgrInfo>, &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum>,
    {
        let src: [u8; L] = kani::any();
        let len: i32 = kani::any();
        kani::assume(len >= 0 && (len as usize) <= L);
        let no_error: bool = kani::any();

        let mut cdst = [0xAAu8; D];
        let mut rdst = [0xAAu8; D];

        let c = unsafe { cfn(enc, src.as_ptr(), cdst.as_mut_ptr(), len, no_error) };
        let cerr = unsafe { pg_mbconv_err };

        let (src_enc, dst_enc) = if to_utf8 { (enc, PG_UTF8) } else { (PG_UTF8, enc) };
        // wiring covered by wiring_conv_builtins, see check_conv
        let r = call_fc(
            fc,
            [
                Datum::from_i32(src_enc),
                Datum::from_i32(dst_enc),
                Datum::from_usize(src.as_ptr() as usize),
                Datum::from_usize(rdst.as_mut_ptr() as usize),
                Datum::from_i32(len),
                Datum::from_bool(no_error),
            ],
        );

        // Vacuity insurance, see check_conv. The out-of-family arm passes
        // ok_reachable=false (every call errors), which makes the Ok cover
        // trivially satisfiable instead of a false UNSATISFIABLE.
        kani::cover!(r.is_ok() || !ok_reachable, "Ok arm reachable");
        kani::cover!(r.is_err(), "Err arm reachable");
        match r {
            Ok(d) => {
                assert!(cerr == 0, "C errored where Rust succeeded");
                assert!(c == d.as_i32(), "consumed-length divergence");
                // single-assert byte compare (kissat re-solves per property)
                let mut eq = true;
                let mut i = 0;
                while i < D {
                    eq = eq && (cdst[i] == rdst[i]);
                    i += 1;
                }
                assert!(eq, "dest byte divergence");
            }
            Err(e) => {
                assert!(c == -1 || c == 0, "Rust errored where C succeeded");
                assert!(cerr == rust_err_class(&e), "error-class divergence");
                core::mem::forget(e);
            }
        }
    }

    macro_rules! conv_proof {
        ($($h:ident: unwind($u:literal) len($l:literal, $d:literal)
            oid $oid:literal, $fc:path, $cfn:ident, $senc:expr, $denc:expr;)*) => {$(
            #[kani::proof]
            #[kani::unwind($u)]
            #[kani::stub(types_error::PgError::error, proof_support::stub_pg_error_error)]
            #[kani::stub(alloc::fmt::format, proof_support::stub_format)]
            #[kani::stub(mbutils::byte_sequence, stub_byte_sequence)]
            #[kani::stub(wchar::pg_encoding_mblen_or_incomplete, stub_mblen_or_incomplete)]
            fn $h() {
                // oid->function wiring (CONV_BUILTINS foid/func identity) is
                // asserted once in wiring_conv_builtins, NOT here: referencing
                // CONV_BUILTINS drags every conversion fn + all Rust radix
                // tables into each harness's goto program (measured: ~45s
                // fixed CBMC read/instrument cost per harness vs ~5s pruned).
                check_conv::<_, $l, $d>($oid, $fc, $cfn, $senc, $denc);
            }
        )*};
    }

    // ---- single-byte cyrillic family (local2local / latin2mic[_with_table]
    // / mic2latin[_with_table]) — len<=8 covers every source byte value and
    // every multi-char interaction shape these engines have -----------------
    conv_proof! {
        eq_koi8r_to_mic: unwind(18) len(8, 17) oid 4302, conv::cyrillic_and_mic::fc_koi8r_to_mic, pg_koi8r_to_mic,      PG_KOI8R,      PG_MULE_INTERNAL;
        eq_mic_to_koi8r: unwind(10) len(8, 9) oid 4303, conv::cyrillic_and_mic::fc_mic_to_koi8r, pg_mic_to_koi8r,      PG_MULE_INTERNAL, PG_KOI8R;
        eq_iso_to_mic: unwind(18) len(8, 17) oid 4304, conv::cyrillic_and_mic::fc_iso_to_mic, pg_iso_to_mic,        PG_ISO_8859_5, PG_MULE_INTERNAL;
        eq_mic_to_iso: unwind(10) len(8, 9) oid 4305, conv::cyrillic_and_mic::fc_mic_to_iso, pg_mic_to_iso,        PG_MULE_INTERNAL, PG_ISO_8859_5;
        eq_win1251_to_mic: unwind(18) len(8, 17) oid 4306, conv::cyrillic_and_mic::fc_win1251_to_mic, pg_win1251_to_mic,    PG_WIN1251,    PG_MULE_INTERNAL;
        eq_mic_to_win1251: unwind(10) len(8, 9) oid 4307, conv::cyrillic_and_mic::fc_mic_to_win1251, pg_mic_to_win1251,    PG_MULE_INTERNAL, PG_WIN1251;
        eq_win866_to_mic: unwind(18) len(8, 17) oid 4308, conv::cyrillic_and_mic::fc_win866_to_mic, pg_win866_to_mic,     PG_WIN866,     PG_MULE_INTERNAL;
        eq_mic_to_win866: unwind(10) len(8, 9) oid 4309, conv::cyrillic_and_mic::fc_mic_to_win866, pg_mic_to_win866,     PG_MULE_INTERNAL, PG_WIN866;
        eq_koi8r_to_win1251: unwind(10) len(8, 9) oid 4310, conv::cyrillic_and_mic::fc_koi8r_to_win1251, pg_koi8r_to_win1251,  PG_KOI8R,      PG_WIN1251;
        eq_win1251_to_koi8r: unwind(10) len(8, 9) oid 4311, conv::cyrillic_and_mic::fc_win1251_to_koi8r, pg_win1251_to_koi8r,  PG_WIN1251,    PG_KOI8R;
        eq_koi8r_to_win866: unwind(10) len(8, 9) oid 4312, conv::cyrillic_and_mic::fc_koi8r_to_win866, pg_koi8r_to_win866,   PG_KOI8R,      PG_WIN866;
        eq_win866_to_koi8r: unwind(10) len(8, 9) oid 4313, conv::cyrillic_and_mic::fc_win866_to_koi8r, pg_win866_to_koi8r,   PG_WIN866,     PG_KOI8R;
        eq_win866_to_win1251: unwind(10) len(8, 9) oid 4314, conv::cyrillic_and_mic::fc_win866_to_win1251, pg_win866_to_win1251, PG_WIN866,     PG_WIN1251;
        eq_win1251_to_win866: unwind(10) len(8, 9) oid 4315, conv::cyrillic_and_mic::fc_win1251_to_win866, pg_win1251_to_win866, PG_WIN1251,    PG_WIN866;
        eq_iso_to_koi8r: unwind(10) len(8, 9) oid 4316, conv::cyrillic_and_mic::fc_iso_to_koi8r, pg_iso_to_koi8r,      PG_ISO_8859_5, PG_KOI8R;
        eq_koi8r_to_iso: unwind(10) len(8, 9) oid 4317, conv::cyrillic_and_mic::fc_koi8r_to_iso, pg_koi8r_to_iso,      PG_KOI8R,      PG_ISO_8859_5;
        eq_iso_to_win1251: unwind(10) len(8, 9) oid 4318, conv::cyrillic_and_mic::fc_iso_to_win1251, pg_iso_to_win1251,    PG_ISO_8859_5, PG_WIN1251;
        eq_win1251_to_iso: unwind(10) len(8, 9) oid 4319, conv::cyrillic_and_mic::fc_win1251_to_iso, pg_win1251_to_iso,    PG_WIN1251,    PG_ISO_8859_5;
        eq_iso_to_win866: unwind(10) len(8, 9) oid 4320, conv::cyrillic_and_mic::fc_iso_to_win866, pg_iso_to_win866,     PG_ISO_8859_5, PG_WIN866;
        eq_win866_to_iso: unwind(10) len(8, 9) oid 4321, conv::cyrillic_and_mic::fc_win866_to_iso, pg_win866_to_iso,     PG_WIN866,     PG_ISO_8859_5;
    }

    // ---- EUC/MIC + SJIS + BIG5 + 2004 algorithmic families ----------------
    conv_proof! {
        eq_euc_cn_to_mic: unwind(14) len(8, 13) oid 4322, conv::euc_cn_and_mic::fc_euc_cn_to_mic, pg_euc_cn_to_mic,     PG_EUC_CN,     PG_MULE_INTERNAL;
        eq_mic_to_euc_cn: unwind(10) len(8, 9) oid 4323, conv::euc_cn_and_mic::fc_mic_to_euc_cn, pg_mic_to_euc_cn,     PG_MULE_INTERNAL, PG_EUC_CN;
        eq_euc_jp_to_sjis: unwind(392) len(2, 5) oid 4324, conv::euc_jp_and_sjis::fc_euc_jp_to_sjis, pg_euc_jp_to_sjis,    PG_EUC_JP,     PG_SJIS;
        eq_sjis_to_euc_jp: unwind(392) len(2, 5) oid 4325, conv::euc_jp_and_sjis::fc_sjis_to_euc_jp, pg_sjis_to_euc_jp,    PG_SJIS,       PG_EUC_JP;
        eq_euc_jp_to_mic: unwind(14) len(8, 13) oid 4326, conv::euc_jp_and_sjis::fc_euc_jp_to_mic, pg_euc_jp_to_mic,     PG_EUC_JP,     PG_MULE_INTERNAL;
        eq_sjis_to_mic: unwind(392) len(2, 5) oid 4327, conv::euc_jp_and_sjis::fc_sjis_to_mic, pg_sjis_to_mic,       PG_SJIS,       PG_MULE_INTERNAL;
        eq_mic_to_euc_jp: unwind(10) len(8, 9) oid 4328, conv::euc_jp_and_sjis::fc_mic_to_euc_jp, pg_mic_to_euc_jp,     PG_MULE_INTERNAL, PG_EUC_JP;
        eq_mic_to_sjis: unwind(392) len(4, 5) oid 4329, conv::euc_jp_and_sjis::fc_mic_to_sjis, pg_mic_to_sjis,       PG_MULE_INTERNAL, PG_SJIS;
        eq_euc_kr_to_mic: unwind(14) len(8, 13) oid 4330, conv::euc_kr_and_mic::fc_euc_kr_to_mic, pg_euc_kr_to_mic,     PG_EUC_KR,     PG_MULE_INTERNAL;
        eq_mic_to_euc_kr: unwind(10) len(8, 9) oid 4331, conv::euc_kr_and_mic::fc_mic_to_euc_kr, pg_mic_to_euc_kr,     PG_MULE_INTERNAL, PG_EUC_KR;
        eq_euc_tw_to_big5: unwind(12) len(4, 5) oid 4332, conv::euc_tw_and_big5::fc_euc_tw_to_big5, pg_euc_tw_to_big5,    PG_EUC_TW,     PG_BIG5;
        eq_big5_to_euc_tw: unwind(12) len(4, 9) oid 4333, conv::euc_tw_and_big5::fc_big5_to_euc_tw, pg_big5_to_euc_tw,    PG_BIG5,       PG_EUC_TW;
        eq_euc_tw_to_mic: unwind(14) len(8, 13) oid 4334, conv::euc_tw_and_big5::fc_euc_tw_to_mic, pg_euc_tw_to_mic,     PG_EUC_TW,     PG_MULE_INTERNAL;
        eq_big5_to_mic: unwind(12) len(4, 9) oid 4335, conv::euc_tw_and_big5::fc_big5_to_mic, pg_big5_to_mic,       PG_BIG5,       PG_MULE_INTERNAL;
        eq_mic_to_euc_tw: unwind(8) len(4, 7) oid 4336, conv::euc_tw_and_big5::fc_mic_to_euc_tw, pg_mic_to_euc_tw,     PG_MULE_INTERNAL, PG_EUC_TW;
        eq_mic_to_big5: unwind(12) len(4, 5) oid 4337, conv::euc_tw_and_big5::fc_mic_to_big5, pg_mic_to_big5,       PG_MULE_INTERNAL, PG_BIG5;
        eq_latin2_to_mic: unwind(18) len(8, 17) oid 4338, conv::latin2_and_win1250::fc_latin2_to_mic, pg_latin2_to_mic,     PG_LATIN2,     PG_MULE_INTERNAL;
        eq_mic_to_latin2: unwind(10) len(8, 9) oid 4339, conv::latin2_and_win1250::fc_mic_to_latin2, pg_mic_to_latin2,     PG_MULE_INTERNAL, PG_LATIN2;
        eq_win1250_to_mic: unwind(18) len(8, 17) oid 4340, conv::latin2_and_win1250::fc_win1250_to_mic, pg_win1250_to_mic,    PG_WIN1250,    PG_MULE_INTERNAL;
        eq_mic_to_win1250: unwind(10) len(8, 9) oid 4341, conv::latin2_and_win1250::fc_mic_to_win1250, pg_mic_to_win1250,    PG_MULE_INTERNAL, PG_WIN1250;
        eq_latin2_to_win1250: unwind(10) len(8, 9) oid 4342, conv::latin2_and_win1250::fc_latin2_to_win1250, pg_latin2_to_win1250, PG_LATIN2,     PG_WIN1250;
        eq_win1250_to_latin2: unwind(10) len(8, 9) oid 4343, conv::latin2_and_win1250::fc_win1250_to_latin2, pg_win1250_to_latin2, PG_WIN1250,    PG_LATIN2;
        eq_latin1_to_mic: unwind(18) len(8, 17) oid 4344, conv::latin_and_mic::fc_latin1_to_mic, pg_latin1_to_mic,     PG_LATIN1,     PG_MULE_INTERNAL;
        eq_mic_to_latin1: unwind(10) len(8, 9) oid 4345, conv::latin_and_mic::fc_mic_to_latin1, pg_mic_to_latin1,     PG_MULE_INTERNAL, PG_LATIN1;
        eq_latin3_to_mic: unwind(18) len(8, 17) oid 4346, conv::latin_and_mic::fc_latin3_to_mic, pg_latin3_to_mic,     PG_LATIN3,     PG_MULE_INTERNAL;
        eq_mic_to_latin3: unwind(10) len(8, 9) oid 4347, conv::latin_and_mic::fc_mic_to_latin3, pg_mic_to_latin3,     PG_MULE_INTERNAL, PG_LATIN3;
        eq_latin4_to_mic: unwind(18) len(8, 17) oid 4348, conv::latin_and_mic::fc_latin4_to_mic, pg_latin4_to_mic,     PG_LATIN4,     PG_MULE_INTERNAL;
        eq_mic_to_latin4: unwind(10) len(8, 9) oid 4349, conv::latin_and_mic::fc_mic_to_latin4, pg_mic_to_latin4,     PG_MULE_INTERNAL, PG_LATIN4;
        eq_euc_jis_2004_to_shift_jis_2004: unwind(10) len(8, 9) oid 4386, conv::euc2004_sjis2004::fc_euc_jis_2004_to_shift_jis_2004, pg_euc_jis_2004_to_shift_jis_2004, PG_EUC_JIS_2004, PG_SHIFT_JIS_2004;
        eq_shift_jis_2004_to_euc_jis_2004: unwind(18) len(8, 17) oid 4387, conv::euc2004_sjis2004::fc_shift_jis_2004_to_euc_jis_2004, pg_shift_jis_2004_to_euc_jis_2004, PG_SHIFT_JIS_2004, PG_EUC_JIS_2004;
    }

    // ---- UTF8 pairs, algorithmic + single-byte radix maps ------------------
    conv_proof! {
        eq_iso8859_1_to_utf8: unwind(18) len(8, 17) oid 4374, conv::fc_iso8859_1_to_utf8, pg_iso8859_1_to_utf8, PG_LATIN1,     PG_UTF8;
        eq_utf8_to_iso8859_1: unwind(10) len(8, 9) oid 4375, conv::fc_utf8_to_iso8859_1, pg_utf8_to_iso8859_1, PG_UTF8,       PG_LATIN1;
        eq_koi8r_to_utf8: unwind(26) len(8, 25) oid 4355, conv::utf8_procs::fc_koi8r_to_utf8, pg_koi8r_to_utf8,     PG_KOI8R,      PG_UTF8;
        eq_utf8_to_koi8r: unwind(10) len(8, 9) oid 4354, conv::utf8_procs::fc_utf8_to_koi8r, pg_utf8_to_koi8r,     PG_UTF8,       PG_KOI8R;
        eq_koi8u_to_utf8: unwind(26) len(8, 25) oid 4357, conv::utf8_procs::fc_koi8u_to_utf8, pg_koi8u_to_utf8,     PG_KOI8U,      PG_UTF8;
        eq_utf8_to_koi8u: unwind(10) len(8, 9) oid 4356, conv::utf8_procs::fc_utf8_to_koi8u, pg_utf8_to_koi8u,     PG_UTF8,       PG_KOI8U;
    }

    // ---- UTF8 pairs, CJK radix maps (table sizes 10k-60k entries; each
    // harness is its own cost experiment — see ledger for per-pair times) ----
    conv_proof! {
        eq_big5_to_utf8:      unwind(26) len(8, 25) oid 4352, conv::utf8_procs::fc_big5_to_utf8, pg_big5_to_utf8,      PG_BIG5,       PG_UTF8;
        eq_utf8_to_big5:      unwind(18) len(8, 17) oid 4353, conv::utf8_procs::fc_utf8_to_big5, pg_utf8_to_big5,      PG_UTF8,       PG_BIG5;
        eq_euc_cn_to_utf8:    unwind(26) len(8, 25) oid 4360, conv::utf8_procs::fc_euc_cn_to_utf8, pg_euc_cn_to_utf8,    PG_EUC_CN,     PG_UTF8;
        eq_utf8_to_euc_cn:    unwind(18) len(8, 17) oid 4361, conv::utf8_procs::fc_utf8_to_euc_cn, pg_utf8_to_euc_cn,    PG_UTF8,       PG_EUC_CN;
        eq_euc_jp_to_utf8:    unwind(26) len(8, 25) oid 4362, conv::utf8_procs::fc_euc_jp_to_utf8, pg_euc_jp_to_utf8,    PG_EUC_JP,     PG_UTF8;
        eq_utf8_to_euc_jp:    unwind(18) len(8, 17) oid 4363, conv::utf8_procs::fc_utf8_to_euc_jp, pg_utf8_to_euc_jp,    PG_UTF8,       PG_EUC_JP;
        eq_euc_kr_to_utf8:    unwind(26) len(8, 25) oid 4364, conv::utf8_procs::fc_euc_kr_to_utf8, pg_euc_kr_to_utf8,    PG_EUC_KR,     PG_UTF8;
        eq_utf8_to_euc_kr:    unwind(18) len(8, 17) oid 4365, conv::utf8_procs::fc_utf8_to_euc_kr, pg_utf8_to_euc_kr,    PG_UTF8,       PG_EUC_KR;
        eq_euc_tw_to_utf8:    unwind(26) len(8, 25) oid 4366, conv::utf8_procs::fc_euc_tw_to_utf8, pg_euc_tw_to_utf8,    PG_EUC_TW,     PG_UTF8;
        eq_utf8_to_euc_tw:    unwind(18) len(8, 17) oid 4367, conv::utf8_procs::fc_utf8_to_euc_tw, pg_utf8_to_euc_tw,    PG_UTF8,       PG_EUC_TW;
        eq_gb18030_to_utf8:   unwind(26) len(8, 25) oid 4368, conv::utf8_procs::fc_gb18030_to_utf8, pg_gb18030_to_utf8,   PG_GB18030,    PG_UTF8;
        eq_utf8_to_gb18030:   unwind(18) len(8, 17) oid 4369, conv::utf8_procs::fc_utf8_to_gb18030, pg_utf8_to_gb18030,   PG_UTF8,       PG_GB18030;
        eq_gbk_to_utf8:       unwind(26) len(8, 25) oid 4370, conv::utf8_procs::fc_gbk_to_utf8, pg_gbk_to_utf8,       PG_GBK,        PG_UTF8;
        eq_utf8_to_gbk:       unwind(18) len(8, 17) oid 4371, conv::utf8_procs::fc_utf8_to_gbk, pg_utf8_to_gbk,       PG_UTF8,       PG_GBK;
        eq_johab_to_utf8:     unwind(26) len(8, 25) oid 4376, conv::utf8_procs::fc_johab_to_utf8, pg_johab_to_utf8,     PG_JOHAB,      PG_UTF8;
        eq_utf8_to_johab:     unwind(18) len(8, 17) oid 4377, conv::utf8_procs::fc_utf8_to_johab, pg_utf8_to_johab,     PG_UTF8,       PG_JOHAB;
        eq_sjis_to_utf8:      unwind(26) len(8, 25) oid 4378, conv::utf8_procs::fc_sjis_to_utf8, pg_sjis_to_utf8,      PG_SJIS,       PG_UTF8;
        eq_utf8_to_sjis:      unwind(18) len(8, 17) oid 4379, conv::utf8_procs::fc_utf8_to_sjis, pg_utf8_to_sjis,      PG_UTF8,       PG_SJIS;
        eq_uhc_to_utf8:       unwind(26) len(8, 25) oid 4380, conv::utf8_procs::fc_uhc_to_utf8, pg_uhc_to_utf8,       PG_UHC,        PG_UTF8;
        eq_utf8_to_uhc:       unwind(18) len(8, 17) oid 4381, conv::utf8_procs::fc_utf8_to_uhc, pg_utf8_to_uhc,       PG_UTF8,       PG_UHC;
        eq_euc_jis_2004_to_utf8: unwind(26) len(8, 25) oid 4382, conv::utf8_procs::fc_euc_jis_2004_to_utf8, pg_euc_jis_2004_to_utf8, PG_EUC_JIS_2004, PG_UTF8;
        eq_utf8_to_euc_jis_2004: unwind(18) len(8, 17) oid 4383, conv::utf8_procs::fc_utf8_to_euc_jis_2004, pg_utf8_to_euc_jis_2004, PG_UTF8, PG_EUC_JIS_2004;
        eq_shift_jis_2004_to_utf8: unwind(26) len(8, 25) oid 4384, conv::utf8_procs::fc_shift_jis_2004_to_utf8, pg_shift_jis_2004_to_utf8, PG_SHIFT_JIS_2004, PG_UTF8;
        eq_utf8_to_shift_jis_2004: unwind(18) len(8, 17) oid 4385, conv::utf8_procs::fc_utf8_to_shift_jis_2004, pg_utf8_to_shift_jis_2004, PG_UTF8, PG_SHIFT_JIS_2004;
    }

    // ---- encoding-dispatching pairs: one harness per row, symbolic
    // encoding over the WHOLE family band (11 WIN / 13 ISO-8859 rows at
    // once, dispatch loop + Rust match wiring in-theorem) -------------------
    const WIN_BAND: [i32; 11] = [
        PG_WIN866, PG_WIN874, PG_WIN1250, PG_WIN1251, PG_WIN1252, PG_WIN1253, PG_WIN1254,
        PG_WIN1255, PG_WIN1256, PG_WIN1257, PG_WIN1258,
    ];
    const ISO_BAND: [i32; 13] = [
        PG_LATIN2, PG_LATIN3, PG_LATIN4, PG_LATIN5, PG_LATIN6, PG_LATIN7, PG_LATIN8, PG_LATIN9,
        PG_LATIN10, PG_ISO_8859_5, PG_ISO_8859_6, PG_ISO_8859_7, PG_ISO_8859_8,
    ];

    fn in_band(enc: i32, band: &[i32]) -> bool {
        let mut i = 0;
        while i < band.len() {
            if band[i] == enc {
                return true;
            }
            i += 1;
        }
        false
    }

    macro_rules! conv_enc_proof {
        ($($h:ident: unwind($u:literal) len($l:literal, $d:literal)
            oid $oid:literal, $fc:path, $cfn:ident, band $band:ident, to_utf8 $dir:literal;)*) => {$(
            #[kani::proof]
            #[kani::unwind($u)]
            #[kani::stub(types_error::PgError::error, proof_support::stub_pg_error_error)]
            #[kani::stub(alloc::fmt::format, proof_support::stub_format)]
            #[kani::stub(mbutils::byte_sequence, stub_byte_sequence)]
            #[kani::stub(wchar::pg_encoding_mblen_or_incomplete, stub_mblen_or_incomplete)]
            fn $h() {
                let enc: i32 = kani::any();
                kani::assume(in_band(enc, &$band));
                // wiring covered by wiring_conv_builtins, see conv_proof!
                check_conv_enc::<_, $l, $d>($oid, $fc, $cfn, enc, $dir, true);
            }
        )*};
    }

    conv_enc_proof! {
        eq_win_to_utf8: unwind(20) len(6, 19) oid 4359, conv::fc_win_to_utf8, pg_win_to_utf8,     band WIN_BAND, to_utf8 true;
        eq_utf8_to_win: unwind(8) len(6, 7) oid 4358, conv::fc_utf8_to_win, pg_utf8_to_win,     band WIN_BAND, to_utf8 false;
        eq_iso8859_to_utf8: unwind(20) len(6, 19) oid 4373, conv::fc_iso8859_to_utf8, pg_iso8859_to_utf8, band ISO_BAND, to_utf8 true;
        eq_utf8_to_iso8859: unwind(8) len(6, 7) oid 4372, conv::fc_utf8_to_iso8859, pg_utf8_to_iso8859, band ISO_BAND, to_utf8 false;
    }

    // Out-of-family arm: a valid encoding outside the band must yield the
    // "unexpected encoding ID" internal error on both sides (class 3).
    #[kani::proof]
    #[kani::unwind(16)]
    #[kani::stub(types_error::PgError::error, proof_support::stub_pg_error_error)]
    #[kani::stub(alloc::fmt::format, proof_support::stub_format)]
    #[kani::stub(mbutils::byte_sequence, stub_byte_sequence)]
    #[kani::stub(wchar::pg_encoding_mblen_or_incomplete, stub_mblen_or_incomplete)]
    fn eq_win_to_utf8_out_of_family() {
        let enc: i32 = kani::any();
        kani::assume(pg_valid_encoding(enc) && !in_band(enc, &WIN_BAND));
        check_conv_enc::<_, 1, 5>(4359, conv::fc_win_to_utf8, pg_win_to_utf8, enc, true, false);
    }

    // ---- shared fcinfo argument check (covers the dropped
    // CHECK_ENCODING_CONVERSION_ARGS in every shim above): full symbolic
    // 5-arg domain, verdict parity --------------------------------------------
    #[kani::proof]
    #[kani::stub(types_error::PgError::error, proof_support::stub_pg_error_error)]
    #[kani::stub(alloc::fmt::format, proof_support::stub_format)]
    fn eq_check_encoding_conversion_args() {
        let src_enc: i32 = kani::any();
        let dst_enc: i32 = kani::any();
        let len: i32 = kani::any();
        let exp_src: i32 = kani::any();
        let exp_dst: i32 = kani::any();
        // Caller-contract fence (C UB fence): expected encodings are always
        // -1 or a valid pg_enc constant at every call site; outside that, C
        // reads pg_enc2name_tbl out of bounds (UB) and Rust panics on the
        // PG_ENC2NAME index — not a parity question.
        kani::assume(exp_src == -1 || pg_valid_encoding(exp_src));
        kani::assume(exp_dst == -1 || pg_valid_encoding(exp_dst));

        let cerr = unsafe {
            pg_check_encoding_conversion_args(src_enc, dst_enc, len, exp_src, exp_dst)
        };
        let r = mbutils::check_encoding_conversion_args(src_enc, dst_enc, len, exp_src, exp_dst);
        match r {
            Ok(()) => {
                kani::cover!(true, "Ok arm reachable"); // vacuity insurance
                assert!(cerr == 0, "C rejected args Rust accepted");
            }
            Err(e) => {
                kani::cover!(true, "Err arm reachable"); // vacuity insurance
                assert!(cerr != 0, "Rust rejected args C accepted");
                core::mem::forget(e);
            }
        }
    }

    // ---- NEGATIVE CONTROL — must FAIL (rig non-vacuity): the Rust
    // koi8r_to_win1251 builtin compared against the C koi8r_to_win866 shim
    // (mismatched table); they differ wherever koi2win1251/koi2win866
    // disagree. Run with the DEFAULT solver (kissat never terminates on
    // failing harnesses). -----------------------------------------------------
    #[kani::proof]
    #[kani::unwind(18)]
    #[kani::stub(types_error::PgError::error, proof_support::stub_pg_error_error)]
    #[kani::stub(alloc::fmt::format, proof_support::stub_format)]
    #[kani::stub(mbutils::byte_sequence, stub_byte_sequence)]
    #[kani::stub(wchar::pg_encoding_mblen_or_incomplete, stub_mblen_or_incomplete)]
    fn control_mismatched_table_must_fail() {
        check_conv::<_, 8, 17>(4310, conv::cyrillic_and_mic::fc_koi8r_to_win1251, pg_koi8r_to_win866, PG_KOI8R, PG_WIN1251);
    }

    // ---- pg_proc wiring theorem (hoisted out of the per-conversion
    // harnesses; see conv_proof! comment): every CONV_BUILTINS entry's
    // foid and fn-pointer identity, all concrete. This is the ONLY harness
    // that references CONV_BUILTINS.
    #[kani::proof]
    fn wiring_conv_builtins() {
        use conv::{cyrillic_and_mic as cyr, euc_cn_and_mic as ecn, euc_jp_and_sjis as ejp,
                   euc_kr_and_mic as ekr, euc_tw_and_big5 as etw, euc2004_sjis2004 as e2004,
                   latin2_and_win1250 as l2w, latin_and_mic as lam, utf8_procs as u8p};
        let expect: [(u32, types_fmgr::PGFunction); 84] = [
            (4302, cyr::fc_koi8r_to_mic), (4303, cyr::fc_mic_to_koi8r),
            (4304, cyr::fc_iso_to_mic), (4305, cyr::fc_mic_to_iso),
            (4306, cyr::fc_win1251_to_mic), (4307, cyr::fc_mic_to_win1251),
            (4308, cyr::fc_win866_to_mic), (4309, cyr::fc_mic_to_win866),
            (4310, cyr::fc_koi8r_to_win1251), (4311, cyr::fc_win1251_to_koi8r),
            (4312, cyr::fc_koi8r_to_win866), (4313, cyr::fc_win866_to_koi8r),
            (4314, cyr::fc_win866_to_win1251), (4315, cyr::fc_win1251_to_win866),
            (4316, cyr::fc_iso_to_koi8r), (4317, cyr::fc_koi8r_to_iso),
            (4318, cyr::fc_iso_to_win1251), (4319, cyr::fc_win1251_to_iso),
            (4320, cyr::fc_iso_to_win866), (4321, cyr::fc_win866_to_iso),
            (4322, ecn::fc_euc_cn_to_mic), (4323, ecn::fc_mic_to_euc_cn),
            (4324, ejp::fc_euc_jp_to_sjis), (4325, ejp::fc_sjis_to_euc_jp),
            (4326, ejp::fc_euc_jp_to_mic), (4327, ejp::fc_sjis_to_mic),
            (4328, ejp::fc_mic_to_euc_jp), (4329, ejp::fc_mic_to_sjis),
            (4330, ekr::fc_euc_kr_to_mic), (4331, ekr::fc_mic_to_euc_kr),
            (4332, etw::fc_euc_tw_to_big5), (4333, etw::fc_big5_to_euc_tw),
            (4334, etw::fc_euc_tw_to_mic), (4335, etw::fc_big5_to_mic),
            (4336, etw::fc_mic_to_euc_tw), (4337, etw::fc_mic_to_big5),
            (4338, l2w::fc_latin2_to_mic), (4339, l2w::fc_mic_to_latin2),
            (4340, l2w::fc_win1250_to_mic), (4341, l2w::fc_mic_to_win1250),
            (4342, l2w::fc_latin2_to_win1250), (4343, l2w::fc_win1250_to_latin2),
            (4344, lam::fc_latin1_to_mic), (4345, lam::fc_mic_to_latin1),
            (4346, lam::fc_latin3_to_mic), (4347, lam::fc_mic_to_latin3),
            (4348, lam::fc_latin4_to_mic), (4349, lam::fc_mic_to_latin4),
            (4352, u8p::fc_big5_to_utf8), (4353, u8p::fc_utf8_to_big5),
            (4354, u8p::fc_utf8_to_koi8r), (4355, u8p::fc_koi8r_to_utf8),
            (4356, u8p::fc_utf8_to_koi8u), (4357, u8p::fc_koi8u_to_utf8),
            (4358, conv::fc_utf8_to_win), (4359, conv::fc_win_to_utf8),
            (4360, u8p::fc_euc_cn_to_utf8), (4361, u8p::fc_utf8_to_euc_cn),
            (4362, u8p::fc_euc_jp_to_utf8), (4363, u8p::fc_utf8_to_euc_jp),
            (4364, u8p::fc_euc_kr_to_utf8), (4365, u8p::fc_utf8_to_euc_kr),
            (4366, u8p::fc_euc_tw_to_utf8), (4367, u8p::fc_utf8_to_euc_tw),
            (4368, u8p::fc_gb18030_to_utf8), (4369, u8p::fc_utf8_to_gb18030),
            (4370, u8p::fc_gbk_to_utf8), (4371, u8p::fc_utf8_to_gbk),
            (4372, conv::fc_utf8_to_iso8859), (4373, conv::fc_iso8859_to_utf8),
            (4374, conv::fc_iso8859_1_to_utf8), (4375, conv::fc_utf8_to_iso8859_1),
            (4376, u8p::fc_johab_to_utf8), (4377, u8p::fc_utf8_to_johab),
            (4378, u8p::fc_sjis_to_utf8), (4379, u8p::fc_utf8_to_sjis),
            (4380, u8p::fc_uhc_to_utf8), (4381, u8p::fc_utf8_to_uhc),
            (4382, u8p::fc_euc_jis_2004_to_utf8), (4383, u8p::fc_utf8_to_euc_jis_2004),
            (4384, u8p::fc_shift_jis_2004_to_utf8), (4385, u8p::fc_utf8_to_shift_jis_2004),
            (4386, e2004::fc_euc_jis_2004_to_shift_jis_2004),
            (4387, e2004::fc_shift_jis_2004_to_euc_jis_2004),
        ];
        assert!(conv::CONV_BUILTINS.len() == 84, "CONV_BUILTINS length");
        let mut ok = true;
        let mut i = 0;
        while i < 84 {
            ok = ok && conv::CONV_BUILTINS[i].foid == expect[i].0
                && conv::CONV_BUILTINS[i].func as usize == expect[i].1 as usize;
            i += 1;
        }
        assert!(ok, "CONV_BUILTINS foid/func wiring mismatch");
    }
}
