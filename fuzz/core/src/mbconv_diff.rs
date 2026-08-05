//! mbconv_diff — differential fuzz + native EXHAUSTIVE-DIFF driver for the
//! encoding-conversion pg_proc family (mb/conv, oids 4302..=4387; campaign
//! lane p1-lanez).
//!
//! Oracle: the SAME vendored PostgreSQL 18.3 C the proofs/mbconv Kani family
//! solves against (proofs/mbconv/c — conv.c engines + all conversion_procs
//! bodies + Unicode radix maps, provenance headers there), compiled natively
//! by build.rs. Error plane: the PROOF_EREPORT_FLAG convention (pg_mbconv.h)
//! — on error the C engine sets `pg_mbconv_err` to the errcode CLASS and
//! returns -1:
//!   1 = report_invalid_encoding    (22021) <-> ERRCODE_CHARACTER_NOT_IN_REPERTOIRE
//!   2 = report_untranslatable_char (22P05) <-> ERRCODE_UNTRANSLATABLE_CHARACTER
//!   3 = ereport bad-encoding-id    (22023/XX000)
//!   9 = elog defensive arm ("unsupported character length") — must never
//!       fire; treated as a hard finding.
//!
//! Comparison planes (identical to the proofs/mbconv contract):
//!   * success: consumed-length parity + FULL destination-buffer byte parity
//!     (including the trailing NUL and the untouched 0xAA tail);
//!   * error:   verdict parity + errcode-class parity. Dest bytes/consumed
//!     count are out of the claim on the error arm (real C longjmps and
//!     discards the buffer).
//!
//! Fuzz input grammar: [selector, flags, src...]
//!   selector: index into PAIRS (mod len) — one conversion direction;
//!   flags: bit0 = noError; bits 1..=4 = family-band sub-selector for the
//!          four encoding-dispatching rows (win/iso8859);
//!   src: raw source bytes (capped at MAX_FUZZ_SRC).
//!
//! The exhaustive sweeps (see `exhaustive` module) enumerate ENTIRE
//! per-character domains natively — every 1/2/3-byte prefix and the
//! constrained 4-byte-lead singles — the cascade-a0 route for the 36
//! solver-walled map-based pairs. Multi-character stream interaction is the
//! fuzz target's job (the per-char loop advances by consumed length, so
//! per-char totality + stream fuzz compose).

use datum::Datum;
use std::os::raw::c_int;
use types_error::{
    PgError, PgResult, ERRCODE_CHARACTER_NOT_IN_REPERTOIRE, ERRCODE_INTERNAL_ERROR,
    ERRCODE_INVALID_PARAMETER_VALUE, ERRCODE_UNTRANSLATABLE_CHARACTER,
};
use types_fmgr::{FmgrInfo, FunctionCallInfoBaseData, LocalFcinfo};
use wchar::{
    PG_BIG5, PG_EUC_CN, PG_EUC_JIS_2004, PG_EUC_JP, PG_EUC_KR, PG_EUC_TW, PG_GB18030, PG_GBK,
    PG_ISO_8859_5, PG_ISO_8859_6, PG_ISO_8859_7, PG_ISO_8859_8, PG_JOHAB, PG_KOI8R, PG_KOI8U,
    PG_LATIN1, PG_LATIN10, PG_LATIN2, PG_LATIN3, PG_LATIN4, PG_LATIN5, PG_LATIN6, PG_LATIN7,
    PG_LATIN8, PG_LATIN9, PG_MULE_INTERNAL, PG_SHIFT_JIS_2004, PG_SJIS, PG_UHC, PG_UTF8,
    PG_WIN1250, PG_WIN1251, PG_WIN1252, PG_WIN1253, PG_WIN1254, PG_WIN1255, PG_WIN1256,
    PG_WIN1257, PG_WIN1258, PG_WIN866, PG_WIN874,
};

/// Max source length the fuzz entry accepts (stream-interaction plane).
pub const MAX_FUZZ_SRC: usize = 256;

type FcFn = fn(Option<&mut FmgrInfo>, &mut FunctionCallInfoBaseData) -> PgResult<Datum>;
type CConv = unsafe extern "C" fn(*const u8, *mut u8, c_int, bool) -> c_int;
type CConvEnc = unsafe extern "C" fn(c_int, *const u8, *mut u8, c_int, bool) -> c_int;

extern "C" {
    // thread-local error-class flag accessors (csrc/mbconv_glue.c)
    fn pg_mbconv_err_get() -> c_int;
    fn pg_mbconv_err_reset();
    // vendored fcinfo arg check (pg_conv_check.c): returns the error CLASS
    // (0 = args accepted), same convention as pg_mbconv_err
    fn pg_check_encoding_conversion_args(
        src_encoding: c_int,
        dest_encoding: c_int,
        len: c_int,
        expected_src_encoding: c_int,
        expected_dest_encoding: c_int,
    ) -> c_int;
    // verbatim 18.3 appendStringInfoStringQuoted over a flat buffer
    // (csrc/mbconv_glue.c; pg_mbcliplen = pg_name_io.c's UTF8-pinned copy)
    fn pg_diff_append_quoted(s: *const u8, maxlen: c_int, out: *mut u8) -> c_int;
    // vendored conv.c engines (invalid-encoding arm fires before any map
    // deref, so NULL map/cmap is safe for that arm)
    fn UtfToLocal(
        utf: *const u8, len: c_int, iso: *mut u8,
        map: *const core::ffi::c_void, cmap: *const core::ffi::c_void,
        cmapsize: c_int, conv_func: *const core::ffi::c_void,
        encoding: c_int, no_error: bool,
    ) -> c_int;
    fn LocalToUtf(
        iso: *const u8, len: c_int, utf: *mut u8,
        map: *const core::ffi::c_void, cmap: *const core::ffi::c_void,
        cmapsize: c_int, conv_func: *const core::ffi::c_void,
        encoding: c_int, no_error: bool,
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
    // utf8 pairs
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

pub const WIN_BAND: [i32; 11] = [
    PG_WIN866, PG_WIN874, PG_WIN1250, PG_WIN1251, PG_WIN1252, PG_WIN1253, PG_WIN1254, PG_WIN1255,
    PG_WIN1256, PG_WIN1257, PG_WIN1258,
];
pub const ISO_BAND: [i32; 13] = [
    PG_LATIN2, PG_LATIN3, PG_LATIN4, PG_LATIN5, PG_LATIN6, PG_LATIN7, PG_LATIN8, PG_LATIN9,
    PG_LATIN10, PG_ISO_8859_5, PG_ISO_8859_6, PG_ISO_8859_7, PG_ISO_8859_8,
];

#[derive(Clone, Copy)]
pub enum COracle {
    Plain(CConv),
    /// (dispatcher, band, to_utf8)
    Enc(CConvEnc, &'static [i32], bool),
}

pub struct Pair {
    pub oid: u32,
    pub name: &'static str,
    pub fc: FcFn,
    pub c: COracle,
    pub src_enc: i32,
    pub dst_enc: i32,
    /// max bytes one character of the SOURCE encoding can occupy
    pub src_maxlen: u8,
}

macro_rules! plain {
    ($oid:literal, $name:literal, $fc:path, $cfn:ident, $s:expr, $d:expr, $ml:literal) => {
        Pair { oid: $oid, name: $name, fc: $fc, c: COracle::Plain($cfn), src_enc: $s, dst_enc: $d, src_maxlen: $ml }
    };
}

use conv::{
    cyrillic_and_mic as cyr, euc2004_sjis2004 as e2004, euc_cn_and_mic as ecn,
    euc_jp_and_sjis as ejp, euc_kr_and_mic as ekr, euc_tw_and_big5 as etw,
    latin2_and_win1250 as l2w, latin_and_mic as lam, utf8_procs as u8p,
};

/// All 84 conversion directions, CONV_BUILTINS order (oids 4302..=4387).
pub static PAIRS: &[Pair] = &[
    plain!(4302, "koi8r_to_mic", cyr::fc_koi8r_to_mic, pg_koi8r_to_mic, PG_KOI8R, PG_MULE_INTERNAL, 1),
    plain!(4303, "mic_to_koi8r", cyr::fc_mic_to_koi8r, pg_mic_to_koi8r, PG_MULE_INTERNAL, PG_KOI8R, 4),
    plain!(4304, "iso_to_mic", cyr::fc_iso_to_mic, pg_iso_to_mic, PG_ISO_8859_5, PG_MULE_INTERNAL, 1),
    plain!(4305, "mic_to_iso", cyr::fc_mic_to_iso, pg_mic_to_iso, PG_MULE_INTERNAL, PG_ISO_8859_5, 4),
    plain!(4306, "win1251_to_mic", cyr::fc_win1251_to_mic, pg_win1251_to_mic, PG_WIN1251, PG_MULE_INTERNAL, 1),
    plain!(4307, "mic_to_win1251", cyr::fc_mic_to_win1251, pg_mic_to_win1251, PG_MULE_INTERNAL, PG_WIN1251, 4),
    plain!(4308, "win866_to_mic", cyr::fc_win866_to_mic, pg_win866_to_mic, PG_WIN866, PG_MULE_INTERNAL, 1),
    plain!(4309, "mic_to_win866", cyr::fc_mic_to_win866, pg_mic_to_win866, PG_MULE_INTERNAL, PG_WIN866, 4),
    plain!(4310, "koi8r_to_win1251", cyr::fc_koi8r_to_win1251, pg_koi8r_to_win1251, PG_KOI8R, PG_WIN1251, 1),
    plain!(4311, "win1251_to_koi8r", cyr::fc_win1251_to_koi8r, pg_win1251_to_koi8r, PG_WIN1251, PG_KOI8R, 1),
    plain!(4312, "koi8r_to_win866", cyr::fc_koi8r_to_win866, pg_koi8r_to_win866, PG_KOI8R, PG_WIN866, 1),
    plain!(4313, "win866_to_koi8r", cyr::fc_win866_to_koi8r, pg_win866_to_koi8r, PG_WIN866, PG_KOI8R, 1),
    plain!(4314, "win866_to_win1251", cyr::fc_win866_to_win1251, pg_win866_to_win1251, PG_WIN866, PG_WIN1251, 1),
    plain!(4315, "win1251_to_win866", cyr::fc_win1251_to_win866, pg_win1251_to_win866, PG_WIN1251, PG_WIN866, 1),
    plain!(4316, "iso_to_koi8r", cyr::fc_iso_to_koi8r, pg_iso_to_koi8r, PG_ISO_8859_5, PG_KOI8R, 1),
    plain!(4317, "koi8r_to_iso", cyr::fc_koi8r_to_iso, pg_koi8r_to_iso, PG_KOI8R, PG_ISO_8859_5, 1),
    plain!(4318, "iso_to_win1251", cyr::fc_iso_to_win1251, pg_iso_to_win1251, PG_ISO_8859_5, PG_WIN1251, 1),
    plain!(4319, "win1251_to_iso", cyr::fc_win1251_to_iso, pg_win1251_to_iso, PG_WIN1251, PG_ISO_8859_5, 1),
    plain!(4320, "iso_to_win866", cyr::fc_iso_to_win866, pg_iso_to_win866, PG_ISO_8859_5, PG_WIN866, 1),
    plain!(4321, "win866_to_iso", cyr::fc_win866_to_iso, pg_win866_to_iso, PG_WIN866, PG_ISO_8859_5, 1),
    plain!(4322, "euc_cn_to_mic", ecn::fc_euc_cn_to_mic, pg_euc_cn_to_mic, PG_EUC_CN, PG_MULE_INTERNAL, 2),
    plain!(4323, "mic_to_euc_cn", ecn::fc_mic_to_euc_cn, pg_mic_to_euc_cn, PG_MULE_INTERNAL, PG_EUC_CN, 4),
    plain!(4324, "euc_jp_to_sjis", ejp::fc_euc_jp_to_sjis, pg_euc_jp_to_sjis, PG_EUC_JP, PG_SJIS, 3),
    plain!(4325, "sjis_to_euc_jp", ejp::fc_sjis_to_euc_jp, pg_sjis_to_euc_jp, PG_SJIS, PG_EUC_JP, 2),
    plain!(4326, "euc_jp_to_mic", ejp::fc_euc_jp_to_mic, pg_euc_jp_to_mic, PG_EUC_JP, PG_MULE_INTERNAL, 3),
    plain!(4327, "sjis_to_mic", ejp::fc_sjis_to_mic, pg_sjis_to_mic, PG_SJIS, PG_MULE_INTERNAL, 2),
    plain!(4328, "mic_to_euc_jp", ejp::fc_mic_to_euc_jp, pg_mic_to_euc_jp, PG_MULE_INTERNAL, PG_EUC_JP, 4),
    plain!(4329, "mic_to_sjis", ejp::fc_mic_to_sjis, pg_mic_to_sjis, PG_MULE_INTERNAL, PG_SJIS, 4),
    plain!(4330, "euc_kr_to_mic", ekr::fc_euc_kr_to_mic, pg_euc_kr_to_mic, PG_EUC_KR, PG_MULE_INTERNAL, 2),
    plain!(4331, "mic_to_euc_kr", ekr::fc_mic_to_euc_kr, pg_mic_to_euc_kr, PG_MULE_INTERNAL, PG_EUC_KR, 4),
    plain!(4332, "euc_tw_to_big5", etw::fc_euc_tw_to_big5, pg_euc_tw_to_big5, PG_EUC_TW, PG_BIG5, 4),
    plain!(4333, "big5_to_euc_tw", etw::fc_big5_to_euc_tw, pg_big5_to_euc_tw, PG_BIG5, PG_EUC_TW, 2),
    plain!(4334, "euc_tw_to_mic", etw::fc_euc_tw_to_mic, pg_euc_tw_to_mic, PG_EUC_TW, PG_MULE_INTERNAL, 4),
    plain!(4335, "big5_to_mic", etw::fc_big5_to_mic, pg_big5_to_mic, PG_BIG5, PG_MULE_INTERNAL, 2),
    plain!(4336, "mic_to_euc_tw", etw::fc_mic_to_euc_tw, pg_mic_to_euc_tw, PG_MULE_INTERNAL, PG_EUC_TW, 4),
    plain!(4337, "mic_to_big5", etw::fc_mic_to_big5, pg_mic_to_big5, PG_MULE_INTERNAL, PG_BIG5, 4),
    plain!(4338, "latin2_to_mic", l2w::fc_latin2_to_mic, pg_latin2_to_mic, PG_LATIN2, PG_MULE_INTERNAL, 1),
    plain!(4339, "mic_to_latin2", l2w::fc_mic_to_latin2, pg_mic_to_latin2, PG_MULE_INTERNAL, PG_LATIN2, 4),
    plain!(4340, "win1250_to_mic", l2w::fc_win1250_to_mic, pg_win1250_to_mic, PG_WIN1250, PG_MULE_INTERNAL, 1),
    plain!(4341, "mic_to_win1250", l2w::fc_mic_to_win1250, pg_mic_to_win1250, PG_MULE_INTERNAL, PG_WIN1250, 4),
    plain!(4342, "latin2_to_win1250", l2w::fc_latin2_to_win1250, pg_latin2_to_win1250, PG_LATIN2, PG_WIN1250, 1),
    plain!(4343, "win1250_to_latin2", l2w::fc_win1250_to_latin2, pg_win1250_to_latin2, PG_WIN1250, PG_LATIN2, 1),
    plain!(4344, "latin1_to_mic", lam::fc_latin1_to_mic, pg_latin1_to_mic, PG_LATIN1, PG_MULE_INTERNAL, 1),
    plain!(4345, "mic_to_latin1", lam::fc_mic_to_latin1, pg_mic_to_latin1, PG_MULE_INTERNAL, PG_LATIN1, 4),
    plain!(4346, "latin3_to_mic", lam::fc_latin3_to_mic, pg_latin3_to_mic, PG_LATIN3, PG_MULE_INTERNAL, 1),
    plain!(4347, "mic_to_latin3", lam::fc_mic_to_latin3, pg_mic_to_latin3, PG_MULE_INTERNAL, PG_LATIN3, 4),
    plain!(4348, "latin4_to_mic", lam::fc_latin4_to_mic, pg_latin4_to_mic, PG_LATIN4, PG_MULE_INTERNAL, 1),
    plain!(4349, "mic_to_latin4", lam::fc_mic_to_latin4, pg_mic_to_latin4, PG_MULE_INTERNAL, PG_LATIN4, 4),
    plain!(4352, "big5_to_utf8", u8p::fc_big5_to_utf8, pg_big5_to_utf8, PG_BIG5, PG_UTF8, 2),
    plain!(4353, "utf8_to_big5", u8p::fc_utf8_to_big5, pg_utf8_to_big5, PG_UTF8, PG_BIG5, 4),
    plain!(4354, "utf8_to_koi8r", u8p::fc_utf8_to_koi8r, pg_utf8_to_koi8r, PG_UTF8, PG_KOI8R, 4),
    plain!(4355, "koi8r_to_utf8", u8p::fc_koi8r_to_utf8, pg_koi8r_to_utf8, PG_KOI8R, PG_UTF8, 1),
    plain!(4356, "utf8_to_koi8u", u8p::fc_utf8_to_koi8u, pg_utf8_to_koi8u, PG_UTF8, PG_KOI8U, 4),
    plain!(4357, "koi8u_to_utf8", u8p::fc_koi8u_to_utf8, pg_koi8u_to_utf8, PG_KOI8U, PG_UTF8, 1),
    Pair { oid: 4358, name: "utf8_to_win", fc: conv::fc_utf8_to_win, c: COracle::Enc(pg_utf8_to_win, &WIN_BAND, false), src_enc: PG_UTF8, dst_enc: -1, src_maxlen: 4 },
    Pair { oid: 4359, name: "win_to_utf8", fc: conv::fc_win_to_utf8, c: COracle::Enc(pg_win_to_utf8, &WIN_BAND, true), src_enc: -1, dst_enc: PG_UTF8, src_maxlen: 1 },
    plain!(4360, "euc_cn_to_utf8", u8p::fc_euc_cn_to_utf8, pg_euc_cn_to_utf8, PG_EUC_CN, PG_UTF8, 2),
    plain!(4361, "utf8_to_euc_cn", u8p::fc_utf8_to_euc_cn, pg_utf8_to_euc_cn, PG_UTF8, PG_EUC_CN, 4),
    plain!(4362, "euc_jp_to_utf8", u8p::fc_euc_jp_to_utf8, pg_euc_jp_to_utf8, PG_EUC_JP, PG_UTF8, 3),
    plain!(4363, "utf8_to_euc_jp", u8p::fc_utf8_to_euc_jp, pg_utf8_to_euc_jp, PG_UTF8, PG_EUC_JP, 4),
    plain!(4364, "euc_kr_to_utf8", u8p::fc_euc_kr_to_utf8, pg_euc_kr_to_utf8, PG_EUC_KR, PG_UTF8, 2),
    plain!(4365, "utf8_to_euc_kr", u8p::fc_utf8_to_euc_kr, pg_utf8_to_euc_kr, PG_UTF8, PG_EUC_KR, 4),
    plain!(4366, "euc_tw_to_utf8", u8p::fc_euc_tw_to_utf8, pg_euc_tw_to_utf8, PG_EUC_TW, PG_UTF8, 4),
    plain!(4367, "utf8_to_euc_tw", u8p::fc_utf8_to_euc_tw, pg_utf8_to_euc_tw, PG_UTF8, PG_EUC_TW, 4),
    plain!(4368, "gb18030_to_utf8", u8p::fc_gb18030_to_utf8, pg_gb18030_to_utf8, PG_GB18030, PG_UTF8, 4),
    plain!(4369, "utf8_to_gb18030", u8p::fc_utf8_to_gb18030, pg_utf8_to_gb18030, PG_UTF8, PG_GB18030, 4),
    plain!(4370, "gbk_to_utf8", u8p::fc_gbk_to_utf8, pg_gbk_to_utf8, PG_GBK, PG_UTF8, 2),
    plain!(4371, "utf8_to_gbk", u8p::fc_utf8_to_gbk, pg_utf8_to_gbk, PG_UTF8, PG_GBK, 4),
    Pair { oid: 4372, name: "utf8_to_iso8859", fc: conv::fc_utf8_to_iso8859, c: COracle::Enc(pg_utf8_to_iso8859, &ISO_BAND, false), src_enc: PG_UTF8, dst_enc: -1, src_maxlen: 4 },
    Pair { oid: 4373, name: "iso8859_to_utf8", fc: conv::fc_iso8859_to_utf8, c: COracle::Enc(pg_iso8859_to_utf8, &ISO_BAND, true), src_enc: -1, dst_enc: PG_UTF8, src_maxlen: 1 },
    plain!(4374, "iso8859_1_to_utf8", conv::fc_iso8859_1_to_utf8, pg_iso8859_1_to_utf8, PG_LATIN1, PG_UTF8, 1),
    plain!(4375, "utf8_to_iso8859_1", conv::fc_utf8_to_iso8859_1, pg_utf8_to_iso8859_1, PG_UTF8, PG_LATIN1, 4),
    plain!(4376, "johab_to_utf8", u8p::fc_johab_to_utf8, pg_johab_to_utf8, PG_JOHAB, PG_UTF8, 2),
    plain!(4377, "utf8_to_johab", u8p::fc_utf8_to_johab, pg_utf8_to_johab, PG_UTF8, PG_JOHAB, 4),
    plain!(4378, "sjis_to_utf8", u8p::fc_sjis_to_utf8, pg_sjis_to_utf8, PG_SJIS, PG_UTF8, 2),
    plain!(4379, "utf8_to_sjis", u8p::fc_utf8_to_sjis, pg_utf8_to_sjis, PG_UTF8, PG_SJIS, 4),
    plain!(4380, "uhc_to_utf8", u8p::fc_uhc_to_utf8, pg_uhc_to_utf8, PG_UHC, PG_UTF8, 2),
    plain!(4381, "utf8_to_uhc", u8p::fc_utf8_to_uhc, pg_utf8_to_uhc, PG_UTF8, PG_UHC, 4),
    plain!(4382, "euc_jis_2004_to_utf8", u8p::fc_euc_jis_2004_to_utf8, pg_euc_jis_2004_to_utf8, PG_EUC_JIS_2004, PG_UTF8, 3),
    plain!(4383, "utf8_to_euc_jis_2004", u8p::fc_utf8_to_euc_jis_2004, pg_utf8_to_euc_jis_2004, PG_UTF8, PG_EUC_JIS_2004, 4),
    plain!(4384, "shift_jis_2004_to_utf8", u8p::fc_shift_jis_2004_to_utf8, pg_shift_jis_2004_to_utf8, PG_SHIFT_JIS_2004, PG_UTF8, 2),
    plain!(4385, "utf8_to_shift_jis_2004", u8p::fc_utf8_to_shift_jis_2004, pg_utf8_to_shift_jis_2004, PG_UTF8, PG_SHIFT_JIS_2004, 4),
    plain!(4386, "euc_jis_2004_to_shift_jis_2004", e2004::fc_euc_jis_2004_to_shift_jis_2004, pg_euc_jis_2004_to_shift_jis_2004, PG_EUC_JIS_2004, PG_SHIFT_JIS_2004, 3),
    plain!(4387, "shift_jis_2004_to_euc_jis_2004", e2004::fc_shift_jis_2004_to_euc_jis_2004, pg_shift_jis_2004_to_euc_jis_2004, PG_SHIFT_JIS_2004, PG_EUC_JIS_2004, 2),
];

/// Map the shipped Rust error to the C-side PROOF_EREPORT_FLAG class
/// (same mapping as proofs/mbconv rust_err_class).
fn rust_err_class(e: &PgError) -> i32 {
    if e.sqlstate == ERRCODE_CHARACTER_NOT_IN_REPERTOIRE {
        1
    } else if e.sqlstate == ERRCODE_UNTRANSLATABLE_CHARACTER {
        2
    } else if e.sqlstate == ERRCODE_INVALID_PARAMETER_VALUE || e.sqlstate == ERRCODE_INTERNAL_ERROR
    {
        3
    } else {
        -1 // unknown class: fails the parity assert loudly
    }
}

/// Run ONE conversion on both sides and assert the parity contract.
/// `enc_sub` picks the family-band member for the 4 dispatching rows
/// (ignored elsewhere). Panics (with full context) on any divergence.
pub fn diff_one(pair: &Pair, enc_sub: u8, no_error: bool, src: &[u8]) {
    let len = src.len();
    let dcap = 4 * len + 8; // MAX_CONVERSION_GROWTH=4, + NUL + tail slack
    let mut cdst = vec![0xAAu8; dcap];
    let mut rdst = vec![0xAAu8; dcap];

    let (c_ret, c_err, src_enc, dst_enc) = match pair.c {
        COracle::Plain(cfn) => {
            let (r, e) = unsafe {
                pg_mbconv_err_reset();
                let r = cfn(src.as_ptr(), cdst.as_mut_ptr(), len as c_int, no_error);
                (r, pg_mbconv_err_get())
            };
            (r, e, pair.src_enc, pair.dst_enc)
        }
        COracle::Enc(cfn, band, to_utf8) => {
            // index == band.len() selects the OUT-OF-FAMILY arm: a valid
            // encoding outside the band (PG_SQL_ASCII) must yield the
            // "unexpected encoding ID" internal error (class 3) on both sides
            let idx = enc_sub as usize % (band.len() + 1);
            let enc = if idx == band.len() { 0 } else { band[idx] };
            let (r, e) = unsafe {
                pg_mbconv_err_reset();
                let r = cfn(enc, src.as_ptr(), cdst.as_mut_ptr(), len as c_int, no_error);
                (r, pg_mbconv_err_get())
            };
            let (s, d) = if to_utf8 { (enc, PG_UTF8) } else { (PG_UTF8, enc) };
            (r, e, s, d)
        }
    };
    assert!(
        c_err != 9,
        "[{}] C elog defensive arm fired (unsupported character length): src={src:02x?}",
        pair.name
    );

    let mut fcinfo = LocalFcinfo::<6>::new(0);
    fcinfo.set_arg(0, Datum::from_i32(src_enc));
    fcinfo.set_arg(1, Datum::from_i32(dst_enc));
    fcinfo.set_arg(2, Datum::from_usize(src.as_ptr() as usize));
    fcinfo.set_arg(3, Datum::from_usize(rdst.as_mut_ptr() as usize));
    fcinfo.set_arg(4, Datum::from_i32(len as i32));
    fcinfo.set_arg(5, Datum::from_bool(no_error));
    // Route through the SHIPPED pg_proc lookup so conv_builtin's
    // binary-search wrapper is inside the differential every exec (the
    // CONV_BUILTINS wiring content itself is Kani-proved by
    // wiring_conv_builtins).
    let builtin = conv::conv_builtin(pair.oid).expect("conv_builtin lookup");
    let r = (builtin.func)(None, &mut fcinfo);

    match r {
        Ok(d) => {
            assert!(
                c_err == 0,
                "[{}] C errored (class {c_err}) where Rust succeeded: noError={no_error} src={src:02x?}",
                pair.name
            );
            assert!(
                c_ret == d.as_i32(),
                "[{}] consumed-length divergence: C={c_ret} Rust={} noError={no_error} src={src:02x?}",
                pair.name,
                d.as_i32()
            );
            assert!(
                cdst == rdst,
                "[{}] dest byte divergence: noError={no_error} src={src:02x?}\n  C   ={cdst:02x?}\n  Rust={rdst:02x?}",
                pair.name
            );
        }
        Err(e) => {
            assert!(
                c_ret == -1 || c_ret == 0,
                "[{}] Rust errored ({}) where C succeeded (ret {c_ret}): noError={no_error} src={src:02x?}",
                pair.name,
                format!("{:?}", e.sqlstate)
            );
            assert!(
                c_err == rust_err_class(&e),
                "[{}] error-class divergence: C class={c_err} Rust sqlstate={} noError={no_error} src={src:02x?}",
                pair.name,
                format!("{:?}", e.sqlstate)
            );
        }
    }
}

/// Wrong-argument frames: run the shipped fc with a MISMATCHED fcinfo
/// (wrong src/dst encoding or negative len) and diff the rejection against
/// the vendored C check (pg_check_encoding_conversion_args, the macro every
/// C conversion proc opens with). Only rejecting frames are asserted here —
/// accepted frames are diff_one's ordinary domain.
pub fn diff_bad_args(pair: &Pair, src_enc: i32, dst_enc: i32, len: i32) {
    let (exp_src, exp_dst) = match pair.c {
        COracle::Plain(_) => (pair.src_enc, pair.dst_enc),
        COracle::Enc(_, _, to_utf8) => {
            if to_utf8 {
                (-1, PG_UTF8)
            } else {
                (PG_UTF8, -1)
            }
        }
    };
    let cerr =
        unsafe { pg_check_encoding_conversion_args(src_enc, dst_enc, len, exp_src, exp_dst) };
    if cerr == 0 {
        return; // accepted frame — ordinary conversion domain (diff_one)
    }
    let src = [0u8; 4];
    let mut rdst = [0xAAu8; 32];
    let mut fcinfo = LocalFcinfo::<6>::new(0);
    fcinfo.set_arg(0, Datum::from_i32(src_enc));
    fcinfo.set_arg(1, Datum::from_i32(dst_enc));
    fcinfo.set_arg(2, Datum::from_usize(src.as_ptr() as usize));
    fcinfo.set_arg(3, Datum::from_usize(rdst.as_mut_ptr() as usize));
    fcinfo.set_arg(4, Datum::from_i32(len));
    fcinfo.set_arg(5, Datum::from_bool(false));
    let builtin = conv::conv_builtin(pair.oid).expect("conv_builtin lookup");
    match (builtin.func)(None, &mut fcinfo) {
        Ok(_) => panic!(
            "[{}] Rust ACCEPTED args C rejected (class {cerr}): src_enc={src_enc} dst_enc={dst_enc} len={len}",
            pair.name
        ),
        Err(e) => {
            // C class 9 = elog(ERROR, "invalid source/destination encoding
            // ID") — a real ereport(XX000)-equivalent in C (not the
            // conversion-loop defensive arm), so it maps to the Rust
            // internal-error class 3.
            let expect = if cerr == 9 { 3 } else { cerr };
            assert!(
                expect == rust_err_class(&e),
                "[{}] arg-rejection class divergence: C={cerr} Rust sqlstate={:?} src_enc={src_enc} dst_enc={dst_enc} len={len}",
                pair.name,
                e.sqlstate
            );
        }
    }
}

/// Pin the thread's database encoding to UTF8 (the quoted-append oracle's
/// pg_mbcliplen is UTF8-pinned on the C side too — name_diff convention).
fn pin_utf8() {
    std::thread_local! {
        static PINNED: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
    }
    PINNED.with(|c| {
        if !c.get() {
            mbutils::SetDatabaseEncoding(PG_UTF8).expect("UTF8 is a valid backend encoding");
            c.set(true);
        }
    });
}

/// Differential for conv::append_string_info_string_quoted (verbatim 18.3
/// stringinfo_mb.c oracle in csrc/mbconv_glue.c). Domain: valid UTF-8,
/// NUL-free (the C side is NUL-terminated — a stream-representation
/// non-surface, not a behavior difference), len < 2000.
pub fn quoted_diff(s: &str, maxlen: i32) {
    let _oracle = crate::oracle_serial(); // one-thread-at-a-time through the C oracles (process-global statics)
    if s.len() >= 2000 || s.as_bytes().contains(&0) {
        return;
    }
    pin_utf8();
    let mut cs = Vec::with_capacity(s.len() + 1);
    cs.extend_from_slice(s.as_bytes());
    cs.push(0);
    let mut cout = vec![0xAAu8; 2 * s.len() + 16];
    let clen = unsafe { pg_diff_append_quoted(cs.as_ptr(), maxlen, cout.as_mut_ptr()) } as usize;

    let mcx = mcx::MemoryContext::new("mbconv_quoted_diff");
    let mut buf = mcx::PgString::new_in(mcx.mcx());
    conv::append_string_info_string_quoted(&mut buf, s, maxlen)
        .expect("append_string_info_string_quoted: oom");
    assert!(
        buf.as_bytes() == &cout[..clen],
        "append_quoted divergence: s={s:?} maxlen={maxlen}\n  C   ={:?}\n  Rust={:?}",
        String::from_utf8_lossy(&cout[..clen]),
        buf.as_str()
    );
}

/// Fuzz entry: [selector, flags, src...] (see module doc).
/// selector 84 (mod 85) = quoted-append mode: flags = maxlen, src = utf8.
pub fn mbconv_diff(data: &[u8]) {
    let _oracle = crate::oracle_serial(); // one-thread-at-a-time through the C oracles (process-global statics)
    if data.len() < 2 {
        return;
    }
    let sel = data[0] as usize % (PAIRS.len() + 1);
    let src = &data[2..data.len().min(2 + MAX_FUZZ_SRC)];
    if sel == PAIRS.len() {
        if let Ok(s) = std::str::from_utf8(src) {
            quoted_diff(s, data[1] as i32 - 2); // -2, -1, 0.. band incl. negatives
        }
        return;
    }
    let pair = &PAIRS[sel];
    let no_error = data[1] & 1 != 0;
    let enc_sub = data[1] >> 1;
    diff_one(pair, enc_sub, no_error, src);
}

/// EXHAUSTIVE-DIFF sweeps (cascade a0). Every function here enumerates a
/// TOTAL domain; run logs banked in proofs/coverage/lanez/.
pub mod exhaustive {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    pub static EXECS: AtomicU64 = AtomicU64::new(0);

    fn enc_subs(pair: &Pair) -> u8 {
        match pair.c {
            COracle::Plain(_) => 1,
            COracle::Enc(_, band, _) => band.len() as u8,
        }
    }

    /// Sweep ALL k-byte inputs (full 2^(8k) domain) for one pair, both
    /// noError values, every band member for dispatching rows.
    /// Parallelized over the leading byte.
    pub fn sweep_full(pair: &'static Pair, k: u32) {
        assert!(k >= 1 && k <= 3, "full sweeps are k<=3; k=4 is lead-constrained");
        let nsub = enc_subs(pair);
        let next = AtomicU64::new(0);
        let next = &next;
        std::thread::scope(|sc| {
            let nthreads = std::thread::available_parallelism().map(|n| n.get()).unwrap_or(8);
            for _ in 0..nthreads.min(256) {
                sc.spawn(move || loop {
                    let b0 = next.fetch_add(1, Ordering::Relaxed);
                    if b0 > 255 {
                        break;
                    }
                    let mut buf = [0u8; 3];
                    buf[0] = b0 as u8;
                    let tail_space = 1u64 << (8 * (k - 1));
                    let mut n = 0u64;
                    for tail in 0..tail_space {
                        if k >= 2 {
                            buf[1] = (tail & 0xff) as u8;
                        }
                        if k >= 3 {
                            buf[2] = ((tail >> 8) & 0xff) as u8;
                        }
                        for sub in 0..nsub {
                            diff_one(pair, sub, false, &buf[..k as usize]);
                            diff_one(pair, sub, true, &buf[..k as usize]);
                            n += 2;
                        }
                    }
                    EXECS.fetch_add(n, Ordering::Relaxed);
                });
            }
        });
    }

    /// Sweep all 4-byte single-character candidates whose LEAD byte is in
    /// `leads`, with byte2 restricted to `b2` (full tail 2^16). Covers every
    /// 4-byte character of the source encoding totally; 4-byte strings whose
    /// prefix decodes as a shorter character are compositions of the k<=3
    /// full sweeps (the conversion loop advances per character).
    pub fn sweep_lead4(pair: &'static Pair, leads: &[u8], b2: std::ops::RangeInclusive<u8>) {
        let nsub = enc_subs(pair);
        let work: Vec<(u8, u8)> = leads
            .iter()
            .flat_map(|&l| b2.clone().map(move |b| (l, b)))
            .collect();
        let next = AtomicU64::new(0);
        let (workref, next) = (&work, &next);
        std::thread::scope(|sc| {
            let nthreads = std::thread::available_parallelism().map(|n| n.get()).unwrap_or(8);
            for _ in 0..nthreads {
                sc.spawn(move || loop {
                    let i = next.fetch_add(1, Ordering::Relaxed) as usize;
                    if i >= workref.len() {
                        break;
                    }
                    let (b0, b1) = workref[i];
                    let mut buf = [b0, b1, 0, 0];
                    let mut n = 0u64;
                    for tail in 0..=0xffffu32 {
                        buf[2] = (tail & 0xff) as u8;
                        buf[3] = (tail >> 8) as u8;
                        for sub in 0..nsub {
                            diff_one(pair, sub, false, &buf);
                            diff_one(pair, sub, true, &buf);
                            n += 2;
                        }
                    }
                    EXECS.fetch_add(n, Ordering::Relaxed);
                });
            }
        });
    }

    pub fn pair_by_name(name: &str) -> &'static Pair {
        PAIRS.iter().find(|p| p.name == name).unwrap()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Smoke: every pair, a small deterministic input set (valid ASCII,
    /// high-bit singles, truncations) — quick contract check on stable.
    #[test]
    fn smoke_all_pairs() {
        let _serial = crate::c_oracle_serial();
        for pair in PAIRS {
            let nsub = match pair.c {
                COracle::Plain(_) => 1u8,
                COracle::Enc(_, band, _) => band.len() as u8,
            };
            for sub in 0..nsub {
                for ne in [false, true] {
                    diff_one(pair, sub, ne, b"");
                    diff_one(pair, sub, ne, b"hello");
                    diff_one(pair, sub, ne, &[0x80]);
                    diff_one(pair, sub, ne, &[0xa1, 0xa1]);
                    diff_one(pair, sub, ne, &[0x8e, 0xa1]);
                    diff_one(pair, sub, ne, &[0xe4, 0xb8, 0xad]);
                    diff_one(pair, sub, ne, &[0xf0, 0x9f, 0x98, 0x80]);
                    diff_one(pair, sub, ne, &[0xff, 0xfe, 0x00, 0x41]);
                }
            }
        }
    }

    /// Replay the COMMITTED corpus (seeds + fleet coverage-guided growth)
    /// through the fuzz entry — the mutation-audit kill rail: every input
    /// libFuzzer retained for new C/Rust edges becomes a standing witness,
    /// no libfuzzer build needed.
    #[test]
    fn corpus_replay() {
        let _serial = crate::c_oracle_serial();
        let dir = concat!(env!("CARGO_MANIFEST_DIR"), "/../corpus/mbconv_diff");
        let mut n = 0u32;
        for e in std::fs::read_dir(dir).expect("committed corpus dir") {
            let p = e.unwrap().path();
            if p.is_file() {
                mbconv_diff(&std::fs::read(&p).unwrap());
                n += 1;
            }
        }
        assert!(n > 5000, "corpus unexpectedly small: {n}");
    }

    /// Full 1-byte and 2-byte domains for every pair (fast: 84 x 65792 x 2).
    #[test]
    fn exhaustive_k1_k2_all_pairs() {
        let _serial = crate::c_oracle_serial();
        for pair in PAIRS {
            exhaustive::sweep_full(pair, 1);
            exhaustive::sweep_full(pair, 2);
        }
    }

    use std::sync::atomic::Ordering;

    /// Wrong-argument frames for every pair: every (src,dst) in the full
    /// valid-encoding square plus invalid ids and negative len, diffed
    /// against the vendored C arg check. EXHAUSTIVE over the rejecting
    /// domain at encoding granularity (len witnesses: -1, i32::MIN, 0).
    #[test]
    fn bad_args_all_pairs() {
        let _serial = crate::c_oracle_serial();
        let encs: Vec<i32> = (-2..=42).collect(); // valid band 0..=41 + invalid edges
        for pair in PAIRS {
            for &s in &encs {
                for &d in &encs {
                    diff_bad_args(pair, s, d, 4);
                }
            }
            diff_bad_args(pair, pair.src_enc.max(0), pair.dst_enc.max(0), -1);
            diff_bad_args(pair, pair.src_enc.max(0), pair.dst_enc.max(0), i32::MIN);
            diff_bad_args(pair, i32::MAX, i32::MIN, 4);
        }
    }

    /// Invalid-encoding-number arm of the pub UtfToLocal/LocalToUtf engines
    /// (unreachable through every shipped wrapper, which pass compile-time
    /// constants; the pub API surface still has it) — diffed against the
    /// vendored C engines over every invalid encoding id near the valid band.
    #[test]
    fn utf_engines_invalid_encoding() {
        let _serial = crate::c_oracle_serial();
        let src = [0x41u8, 0x42];
        for enc in [-1000, -2, -1, 42, 43, 100, i32::MAX] {
            let mut cdst = [0xAAu8; 16];
            unsafe { pg_mbconv_err_reset() };
            let c = unsafe {
                UtfToLocal(
                    src.as_ptr(), 2, cdst.as_mut_ptr(),
                    core::ptr::null(), core::ptr::null(), 0, core::ptr::null(),
                    enc, false,
                )
            };
            let cerr = unsafe { pg_mbconv_err_get() };
            assert!(c == -1 && cerr == 3, "C UtfToLocal accepted invalid encoding {enc}");
            let mut rdst = [0xAAu8; 16];
            let map = &conv::maps::euc2004::EUC_JIS_2004_FROM_UNICODE_TREE;
            let r = unsafe { conv::UtfToLocal(&src, rdst.as_mut_ptr(), map, &[], None, enc, false) };
            match r {
                Ok(_) => panic!("Rust UtfToLocal accepted invalid encoding {enc}"),
                Err(e) => assert!(rust_err_class(&e) == 3, "class divergence enc={enc}"),
            }

            unsafe { pg_mbconv_err_reset() };
            let c = unsafe {
                LocalToUtf(
                    src.as_ptr(), 2, cdst.as_mut_ptr(),
                    core::ptr::null(), core::ptr::null(), 0, core::ptr::null(),
                    enc, false,
                )
            };
            let cerr = unsafe { pg_mbconv_err_get() };
            assert!(c == -1 && cerr == 3, "C LocalToUtf accepted invalid encoding {enc}");
            let lmap = &conv::maps::euc2004::EUC_JIS_2004_TO_UNICODE_TREE;
            let r = unsafe { conv::LocalToUtf(&src, rdst.as_mut_ptr(), lmap, &[], None, enc, false) };
            match r {
                Ok(_) => panic!("Rust LocalToUtf accepted invalid encoding {enc}"),
                Err(e) => assert!(rust_err_class(&e) == 3, "class divergence enc={enc}"),
            }
        }
    }

    /// Combined-map sweep (EUC_JIS_2004 / SHIFT_JIS_2004 two-codepoint
    /// characters): for EVERY combined-map first codepoint utf1, pair it
    /// with EVERY Unicode scalar as the following character (plus a bare
    /// tail truncation) — total over the combined-lookup second-codepoint
    /// domain. Also runs each map's local combined code through the reverse
    /// direction (covered by k2 too; kept for the same-run witness).
    #[test]
    #[ignore = "exhaustive evidence run (minutes); run explicitly, bank the log"]
    fn exhaustive_combined_second_codepoint() {
        let _serial = crate::c_oracle_serial();
        fn utf8_of(cp: u32) -> Option<Vec<u8>> {
            char::from_u32(cp).map(|c| c.to_string().into_bytes())
        }
        fn bytes_of_packed(p: u32) -> Vec<u8> {
            // maps store utf8 sequences packed big-endian into u32
            p.to_be_bytes().iter().copied().skip_while(|&b| b == 0).collect()
        }
        for (pname, cmap) in [
            ("utf8_to_euc_jis_2004", &conv::maps::euc2004::ULMAPEUC_JIS_2004_COMBINED[..]),
            ("utf8_to_shift_jis_2004", &conv::maps::sjis2004::ULMAPSHIFT_JIS_2004_COMBINED[..]),
        ] {
            let pair = exhaustive::pair_by_name(pname);
            let t = std::time::Instant::now();
            let mut n = 0u64;
            for e in cmap {
                let u1 = bytes_of_packed(e.utf1);
                // bare utf1 (combined lookup falls through to plain map)
                for ne in [false, true] {
                    diff_one(pair, 0, ne, &u1);
                }
                // utf1 followed by EVERY Unicode scalar
                for cp in 0..=0x10FFFFu32 {
                    if let Some(u2) = utf8_of(cp) {
                        let mut buf = u1.clone();
                        buf.extend_from_slice(&u2);
                        diff_one(pair, 0, false, &buf);
                        diff_one(pair, 0, true, &buf);
                        n += 2;
                    }
                }
            }
            println!("combined {}: {} execs in {:.1}s", pname, n, t.elapsed().as_secs_f64());
        }
    }

    /// Quoted-append differential: quote positions x multibyte boundaries x
    /// the full maxlen lattice per string (maxlen in -2..=len+2 — total over
    /// the clip-decision domain for each witness string).
    #[test]
    fn quoted_append_lattice() {
        let _serial = crate::c_oracle_serial();
        let cases: [&str; 12] = [
            "",
            "'",
            "''",
            "a'b''c'''d",
            "hello world",
            "'leading",
            "trailing'",
            "日本語のテキスト",
            "mix日'本ed'語",
            "\u{10348}\u{1F600}'x",
            "é'è''ê",
            "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx'yyyyyyyyyyyyyyyyyyyyyyyyyyyyyyy",
        ];
        for s in cases {
            for maxlen in -2..=(s.len() as i32 + 2) {
                quoted_diff(s, maxlen);
            }
        }
    }

    /// Full 3-byte domain (2^24 x {noError} x band members) for EVERY pair —
    /// total per-character coverage for all source encodings with maxlen<=3
    /// and every <=3-byte prefix/truncation shape of the 4-byte encodings.
    /// EXHAUSTIVE-DIFF evidence run; log the exec count.
    #[test]
    #[ignore = "exhaustive evidence run (minutes); run explicitly, bank the log"]
    fn exhaustive_k3_all_pairs() {
        let _serial = crate::c_oracle_serial();
        for pair in PAIRS {
            let t = std::time::Instant::now();
            exhaustive::EXECS.store(0, Ordering::Relaxed);
            exhaustive::sweep_full(pair, 3);
            println!(
                "k3 {}: {} execs in {:.1}s",
                pair.name,
                exhaustive::EXECS.load(Ordering::Relaxed),
                t.elapsed().as_secs_f64()
            );
        }
    }

    /// Sampled k3+k4 pass for COVERAGE CAPTURE runs (strided tails, prime
    /// step): touches every per-char code path class the full sweeps prove
    /// totally, at ~1/512 the volume, so an instrumented build can measure
    /// the lines in minutes. NOT evidence — the full k3/k4 logs are.
    #[test]
    #[ignore = "coverage-capture helper; run under -Cinstrument-coverage"]
    fn exhaustive_sampled_for_coverage() {
        let _serial = crate::c_oracle_serial();
        const STRIDE: u32 = 509; // prime
        for pair in PAIRS {
            let nsub = match pair.c {
                COracle::Plain(_) => 1u8,
                COracle::Enc(_, band, _) => band.len() as u8,
            };
            // strided 3-byte
            let mut t = 0u32;
            while t < 1 << 24 {
                let buf = [(t & 0xff) as u8, ((t >> 8) & 0xff) as u8, ((t >> 16) & 0xff) as u8];
                for sub in 0..nsub {
                    diff_one(pair, sub, false, &buf);
                    diff_one(pair, sub, true, &buf);
                }
                t += STRIDE;
            }
            // strided 4-byte over all leads
            let mut t = 0u32;
            while t < 1 << 24 {
                for b0 in [0x8e, 0x9c, 0x9d, 0xf0, 0xf4, 0x81, 0xfe] {
                    let buf = [b0, (t & 0xff) as u8, ((t >> 8) & 0xff) as u8, ((t >> 16) & 0xff) as u8];
                    for sub in 0..nsub {
                        diff_one(pair, sub, false, &buf);
                        diff_one(pair, sub, true, &buf);
                    }
                }
                t += STRIDE * 16;
            }
        }
    }

    /// 4-byte single-character candidates, lead-constrained (see sweep_lead4
    /// doc): every 4-byte character of each 4-byte-capable SOURCE encoding.
    /// b1 outside these leads (or b2 outside the gb18030 digit range) makes
    /// the first character <=3 bytes long — those strings are compositions
    /// of the k<=3 full sweeps.
    #[test]
    #[ignore = "exhaustive evidence run (minutes-hours); run explicitly, bank the log"]
    fn exhaustive_k4_lead_constrained() {
        let _serial = crate::c_oracle_serial();
        // (pair name, 4-byte lead bytes, b2 range)
        const UTF8_LEADS: &[u8] = &[0xf0, 0xf1, 0xf2, 0xf3, 0xf4, 0xf5, 0xf6, 0xf7];
        const MIC_LEADS: &[u8] = &[0x9c, 0x9d]; // IS_LCPRV2 -> pg_mule_mblen 4
        const GB_LEADS: &[u8] = &[
            // 0x81..=0xfe: 4-byte iff second byte is 0x30..=0x39
        ];
        let _ = GB_LEADS;
        let mut jobs: Vec<(&str, Vec<u8>, std::ops::RangeInclusive<u8>)> = Vec::new();
        for p in PAIRS {
            if p.src_enc == PG_UTF8 {
                jobs.push((p.name, UTF8_LEADS.to_vec(), 0..=255));
            } else if p.src_enc == PG_MULE_INTERNAL {
                jobs.push((p.name, MIC_LEADS.to_vec(), 0..=255));
            } else if p.src_enc == PG_EUC_TW {
                jobs.push((p.name, vec![0x8e], 0..=255)); // SS2 4-byte
            } else if p.src_enc == PG_GB18030 {
                jobs.push((p.name, (0x81..=0xfe).collect(), 0x30..=0x39));
            }
        }
        for (name, leads, b2) in jobs {
            let pair = exhaustive::pair_by_name(name);
            let t = std::time::Instant::now();
            exhaustive::EXECS.store(0, Ordering::Relaxed);
            exhaustive::sweep_lead4(pair, &leads, b2);
            println!(
                "k4 {}: {} execs in {:.1}s",
                name,
                exhaustive::EXECS.load(Ordering::Relaxed),
                t.elapsed().as_secs_f64()
            );
        }
    }
}
