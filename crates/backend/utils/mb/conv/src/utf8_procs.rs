// The thirteen table-driven utf8_and_*.c modules (big5, cyrillic, euc_cn,
// euc_jp, euc_kr, euc_tw, gb18030, gbk, johab, sjis, uhc, euc2004, sjis2004).

use crate::maps;
use crate::{ConvArgs, LocalToUtf, UtfToLocal};
use datum::Datum;
use mbutils::check_encoding_conversion_args;
use types_error::PgResult;
use types_fmgr::{FmgrInfo, FunctionCallInfoBaseData as Fcinfo};
use wchar::{
    PG_BIG5, PG_EUC_CN, PG_EUC_JIS_2004, PG_EUC_JP, PG_EUC_KR, PG_EUC_TW, PG_GB18030, PG_GBK,
    PG_JOHAB, PG_KOI8R, PG_KOI8U, PG_SHIFT_JIS_2004, PG_SJIS, PG_UHC, PG_UTF8,
};

macro_rules! conv_pair {
    ($to_utf8:ident, $from_utf8:ident, $enc:expr, $map_to:expr, $map_from:expr,
     lu_cmap: $lu_cmap:expr, ul_cmap: $ul_cmap:expr,
     lu_conv: $lu_conv:expr, ul_conv: $ul_conv:expr) => {
        pub fn $to_utf8(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
            let a = unsafe { ConvArgs::from(fcinfo) };
            check_encoding_conversion_args(a.src_encoding, a.dest_encoding, a.len, $enc, PG_UTF8)?;
            let n =
                unsafe { LocalToUtf(a.src(), a.dest, $map_to, $lu_cmap, $lu_conv, $enc, a.no_error)? };
            Ok(Datum::from_i32(n))
        }

        pub fn $from_utf8(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
            let a = unsafe { ConvArgs::from(fcinfo) };
            check_encoding_conversion_args(a.src_encoding, a.dest_encoding, a.len, PG_UTF8, $enc)?;
            let n = unsafe {
                UtfToLocal(a.src(), a.dest, $map_from, $ul_cmap, $ul_conv, $enc, a.no_error)?
            };
            Ok(Datum::from_i32(n))
        }
    };
    ($to_utf8:ident, $from_utf8:ident, $enc:expr, $map_to:expr, $map_from:expr) => {
        conv_pair!($to_utf8, $from_utf8, $enc, $map_to, $map_from,
            lu_cmap: &[], ul_cmap: &[], lu_conv: None, ul_conv: None);
    };
}

conv_pair!(
    fc_big5_to_utf8,
    fc_utf8_to_big5,
    PG_BIG5,
    &maps::big5::BIG5_TO_UNICODE_TREE,
    &maps::big5::BIG5_FROM_UNICODE_TREE
);

conv_pair!(
    fc_koi8r_to_utf8,
    fc_utf8_to_koi8r,
    PG_KOI8R,
    &maps::cyrillic::KOI8R_TO_UNICODE_TREE,
    &maps::cyrillic::KOI8R_FROM_UNICODE_TREE
);

conv_pair!(
    fc_koi8u_to_utf8,
    fc_utf8_to_koi8u,
    PG_KOI8U,
    &maps::cyrillic::KOI8U_TO_UNICODE_TREE,
    &maps::cyrillic::KOI8U_FROM_UNICODE_TREE
);

conv_pair!(
    fc_euc_cn_to_utf8,
    fc_utf8_to_euc_cn,
    PG_EUC_CN,
    &maps::euc_cn::EUC_CN_TO_UNICODE_TREE,
    &maps::euc_cn::EUC_CN_FROM_UNICODE_TREE
);

conv_pair!(
    fc_euc_jp_to_utf8,
    fc_utf8_to_euc_jp,
    PG_EUC_JP,
    &maps::euc_jp::EUC_JP_TO_UNICODE_TREE,
    &maps::euc_jp::EUC_JP_FROM_UNICODE_TREE
);

conv_pair!(
    fc_euc_kr_to_utf8,
    fc_utf8_to_euc_kr,
    PG_EUC_KR,
    &maps::euc_kr::EUC_KR_TO_UNICODE_TREE,
    &maps::euc_kr::EUC_KR_FROM_UNICODE_TREE
);

conv_pair!(
    fc_euc_tw_to_utf8,
    fc_utf8_to_euc_tw,
    PG_EUC_TW,
    &maps::euc_tw::EUC_TW_TO_UNICODE_TREE,
    &maps::euc_tw::EUC_TW_FROM_UNICODE_TREE
);

conv_pair!(
    fc_gbk_to_utf8,
    fc_utf8_to_gbk,
    PG_GBK,
    &maps::gbk::GBK_TO_UNICODE_TREE,
    &maps::gbk::GBK_FROM_UNICODE_TREE
);

conv_pair!(
    fc_johab_to_utf8,
    fc_utf8_to_johab,
    PG_JOHAB,
    &maps::johab::JOHAB_TO_UNICODE_TREE,
    &maps::johab::JOHAB_FROM_UNICODE_TREE
);

conv_pair!(
    fc_sjis_to_utf8,
    fc_utf8_to_sjis,
    PG_SJIS,
    &maps::sjis::SJIS_TO_UNICODE_TREE,
    &maps::sjis::SJIS_FROM_UNICODE_TREE
);

conv_pair!(
    fc_uhc_to_utf8,
    fc_utf8_to_uhc,
    PG_UHC,
    &maps::uhc::UHC_TO_UNICODE_TREE,
    &maps::uhc::UHC_FROM_UNICODE_TREE
);

conv_pair!(
    fc_euc_jis_2004_to_utf8,
    fc_utf8_to_euc_jis_2004,
    PG_EUC_JIS_2004,
    &maps::euc2004::EUC_JIS_2004_TO_UNICODE_TREE,
    &maps::euc2004::EUC_JIS_2004_FROM_UNICODE_TREE,
    lu_cmap: &maps::euc2004::LUMAPEUC_JIS_2004_COMBINED,
    ul_cmap: &maps::euc2004::ULMAPEUC_JIS_2004_COMBINED,
    lu_conv: None,
    ul_conv: None
);

conv_pair!(
    fc_shift_jis_2004_to_utf8,
    fc_utf8_to_shift_jis_2004,
    PG_SHIFT_JIS_2004,
    &maps::sjis2004::SHIFT_JIS_2004_TO_UNICODE_TREE,
    &maps::sjis2004::SHIFT_JIS_2004_FROM_UNICODE_TREE,
    lu_cmap: &maps::sjis2004::LUMAPSHIFT_JIS_2004_COMBINED,
    ul_cmap: &maps::sjis2004::ULMAPSHIFT_JIS_2004_COMBINED,
    lu_conv: None,
    ul_conv: None
);

conv_pair!(
    fc_gb18030_to_utf8,
    fc_utf8_to_gb18030,
    PG_GB18030,
    &maps::gb18030::GB18030_TO_UNICODE_TREE,
    &maps::gb18030::GB18030_FROM_UNICODE_TREE,
    lu_cmap: &[],
    ul_cmap: &[],
    lu_conv: Some(conv_18030_to_utf8),
    ul_conv: Some(conv_utf8_to_18030)
);

// GB18030 4-byte codes: first/third bytes 0x81..=0xfe, second/fourth 0x30..=0x39.
fn gb_linear(gb: u32) -> u32 {
    let b0 = (gb & 0xff00_0000) >> 24;
    let b1 = (gb & 0x00ff_0000) >> 16;
    let b2 = (gb & 0x0000_ff00) >> 8;
    let b3 = gb & 0x0000_00ff;
    (b0 * 12600 + b1 * 1260 + b2 * 10 + b3)
        .wrapping_sub(0x81 * 12600 + 0x30 * 1260 + 0x81 * 10 + 0x30)
}

fn gb_unlinear(lin: u32) -> u32 {
    let r0 = 0x81 + lin / 12600;
    let r1 = 0x30 + (lin / 1260) % 10;
    let r2 = 0x81 + (lin / 10) % 126;
    let r3 = 0x30 + lin % 10;
    (r0 << 24) | (r1 << 16) | (r2 << 8) | r3
}

fn unicode_to_utf8word(c: u32) -> u32 {
    if c <= 0x7f {
        c
    } else if c <= 0x7ff {
        ((0xc0 | ((c >> 6) & 0x1f)) << 8) | (0x80 | (c & 0x3f))
    } else if c <= 0xffff {
        ((0xe0 | ((c >> 12) & 0x0f)) << 16) | ((0x80 | ((c >> 6) & 0x3f)) << 8) | (0x80 | (c & 0x3f))
    } else {
        ((0xf0 | ((c >> 18) & 0x07)) << 24)
            | ((0x80 | ((c >> 12) & 0x3f)) << 16)
            | ((0x80 | ((c >> 6) & 0x3f)) << 8)
            | (0x80 | (c & 0x3f))
    }
}

fn utf8word_to_unicode(c: u32) -> u32 {
    if c <= 0x7f {
        c
    } else if c <= 0xffff {
        (((c >> 8) & 0x1f) << 6) | (c & 0x3f)
    } else if c <= 0xff_ffff {
        (((c >> 16) & 0x0f) << 12) | (((c >> 8) & 0x3f) << 6) | (c & 0x3f)
    } else {
        (((c >> 24) & 0x07) << 18) | (((c >> 16) & 0x3f) << 12) | (((c >> 8) & 0x3f) << 6) | (c & 0x3f)
    }
}

const GB18030_RANGES: [(u32, u32, u32, u32); 13] = [
    // (min_unicode, max_unicode, min_gb, max_gb) per gb-18030-2000.xml
    (0x0452, 0x200f, 0x8130_d330, 0x8136_a531),
    (0x2643, 0x2e80, 0x8137_a839, 0x8138_fd38),
    (0x361b, 0x3917, 0x8230_a633, 0x8230_f237),
    (0x3ce1, 0x4055, 0x8231_d438, 0x8232_af32),
    (0x4160, 0x4336, 0x8232_c937, 0x8232_f837),
    (0x44d7, 0x464b, 0x8233_a339, 0x8233_c931),
    (0x478e, 0x4946, 0x8233_e838, 0x8234_9638),
    (0x49b8, 0x4c76, 0x8234_a131, 0x8234_e733),
    (0x9fa6, 0xd7ff, 0x8235_8f33, 0x8336_c738),
    (0xe865, 0xf92b, 0x8336_d030, 0x8430_8534),
    (0xfa2a, 0xfe2f, 0x8430_9c38, 0x8431_8537),
    (0xffe6, 0xffff, 0x8431_a234, 0x8431_a439),
    (0x10000, 0x10ffff, 0x9030_8130, 0xe332_9a35),
];

fn conv_18030_to_utf8(code: u32) -> u32 {
    for &(minunicode, _, mincode, maxcode) in &GB18030_RANGES {
        if code >= mincode && code <= maxcode {
            return unicode_to_utf8word(gb_linear(code) - gb_linear(mincode) + minunicode);
        }
    }
    0
}

fn conv_utf8_to_18030(code: u32) -> u32 {
    let ucs = utf8word_to_unicode(code);
    for &(minunicode, maxunicode, mincode, _) in &GB18030_RANGES {
        if ucs >= minunicode && ucs <= maxunicode {
            return gb_unlinear(ucs - minunicode + gb_linear(mincode));
        }
    }
    0
}
