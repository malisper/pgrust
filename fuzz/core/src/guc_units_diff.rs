//! guc_units_diff: differential fuzz driver — shipped Rust `guc` units/cnum
//! carve vs vendored PostgreSQL 18.3 (Stamp-18.3, upstream sha 62d6c7d3df) C
//! (csrc/pg_guc_units_io.c: guc.c unit tables + convert_* + parse_int/
//! parse_real verbatim; strtol/strtod/isspace = libc, exactly as upstream —
//! glibc on the fleet floor is the oracle of record).
//! Crate under test: crates/backend/utils/misc/guc (carve: src/units.rs +
//! src/cnum.rs; parse_bool lives in scalar_seams — OUT; registry/store/
//! layers/autotune = session state — OUT).
//!
//! Semantic planes:
//!   arm 0 parse_int(str, flags): verdict + i32 value + hint identity class
//!     (0 none / 1 memory / 2 time / 3 int-range). flags = full i32 domain
//!     (no elog path in parse).
//!   arm 1 parse_real: verdict + f64 bits + hint class.
//!   arm 2 convert_to_base_unit(value, unit-bytes, base_unit): verdict +
//!     base_value f64 bits. base_unit full domain (table miss = false).
//!   arm 3 convert_int_from_base_unit(i64, base_unit): unit-presence +
//!     unit string + converted value (C leaves outputs unwritten when no
//!     table row matches — presence plane only on that path; the shipped
//!     Rust returns (base_value, "")).
//!   arm 4 convert_real_from_base_unit(f64, base_unit): same shape.
//!   arm 5 get_config_unit_name(flags): presence + name over the 9 valid
//!     units values; invalid values are the elog/panic C-parity arm
//!     (driver-fenced; witnessed by elog_parity_units_value below).
//!
//! Domain notes:
//!   - The shipped parse entry points take &str: the fuzz arm truncates at
//!     the first NUL (C is NUL-terminated) and skips non-UTF-8 payloads
//!     (unrepresentable in the shipped API).
//!   - i64::MAX-scale convert_int values exercise C's (int64) rint(...)
//!     cast, UB-saturating on aarch64 exactly like Rust `as` — the fleet
//!     floor (aarch64) is the platform of record.

use guc::cnum::{c_strtod, c_strtol_base0};
use guc::units::{
    convert_int_from_base_unit, convert_real_from_base_unit, convert_to_base_unit, fmt_e, fmt_g,
    fmt_g_prec, get_config_unit_name, parse_int, parse_real, ParseNum, MEMORY_UNITS_HINT,
    TIME_UNITS_HINT,
};

extern "C" {
    fn pg_diff_guc_parse_int(
        value: *const core::ffi::c_char,
        flags: i32,
        result: *mut i32,
        hint_class: *mut i32,
    ) -> i32;
    fn pg_diff_guc_parse_real(
        value: *const core::ffi::c_char,
        flags: i32,
        result: *mut f64,
        hint_class: *mut i32,
    ) -> i32;
    fn pg_diff_guc_convert_to_base_unit(
        value: f64,
        unit: *const core::ffi::c_char,
        base_unit: i32,
        base_value: *mut f64,
    ) -> i32;
    fn pg_diff_guc_convert_int_from_base_unit(
        base_value: i64,
        base_unit: i32,
        value: *mut i64,
        unit8: *mut u8,
        has_unit: *mut i32,
    ) -> i32;
    fn pg_diff_guc_convert_real_from_base_unit(
        base_value: f64,
        base_unit: i32,
        value: *mut f64,
        unit8: *mut u8,
        has_unit: *mut i32,
    ) -> i32;
    fn pg_diff_guc_get_config_unit_name(
        flags: i32,
        name8: *mut u8,
        has_name: *mut i32,
    ) -> i32;
    fn pg_diff_guc_fmt(
        value: f64,
        prec: i32,
        style_e: i32,
        out: *mut u8,
        outlen: i32,
    ) -> i32;
}

const GUC_UNIT_KB: i32 = 0x0100_0000;
const GUC_UNIT_BLOCKS: i32 = 0x0200_0000;
const GUC_UNIT_XBLOCKS: i32 = 0x0300_0000;
const GUC_UNIT_MB: i32 = 0x0400_0000;
const GUC_UNIT_BYTE: i32 = 0x0500_0000;
const GUC_UNIT_MS: i32 = 0x1000_0000;
const GUC_UNIT_S: i32 = 0x2000_0000;
const GUC_UNIT_MIN: i32 = 0x3000_0000;
const VALID_UNITS: [i32; 9] = [
    0,
    GUC_UNIT_BYTE,
    GUC_UNIT_KB,
    GUC_UNIT_MB,
    GUC_UNIT_BLOCKS,
    GUC_UNIT_XBLOCKS,
    GUC_UNIT_MS,
    GUC_UNIT_S,
    GUC_UNIT_MIN,
];

struct Rd<'a> {
    b: &'a [u8],
}

impl<'a> Rd<'a> {
    fn u8(&mut self) -> u8 {
        let v = self.b.first().copied().unwrap_or(0);
        self.b = self.b.get(1..).unwrap_or(&[]);
        v
    }
    fn i32(&mut self) -> i32 {
        i32::from_le_bytes([self.u8(), self.u8(), self.u8(), self.u8()])
    }
    fn i64(&mut self) -> i64 {
        let mut a = [0u8; 8];
        for x in &mut a {
            *x = self.u8();
        }
        i64::from_le_bytes(a)
    }
    fn f64(&mut self) -> f64 {
        let mut a = [0u8; 8];
        for x in &mut a {
            *x = self.u8();
        }
        f64::from_le_bytes(a)
    }
    /// Remaining bytes truncated at the first NUL, as UTF-8 (None = skip:
    /// unrepresentable in the shipped &str API).
    fn rest_str(&mut self) -> Option<&'a str> {
        let b = self.b;
        self.b = &[];
        let end = b.iter().position(|&c| c == 0).unwrap_or(b.len());
        core::str::from_utf8(&b[..end]).ok()
    }
}

fn hint_class(h: Option<&'static str>) -> i32 {
    match h {
        None => 0,
        Some(s) if s == MEMORY_UNITS_HINT => 1,
        Some(s) if s == TIME_UNITS_HINT => 2,
        Some(_) => 3,
    }
}

fn cstr(s: &str) -> Vec<u8> {
    let mut v = s.as_bytes().to_vec();
    v.push(0);
    v
}

pub fn guc_units_diff(data: &[u8]) {
    let _oracle = crate::oracle_serial(); // one-thread-at-a-time through the C oracles (process-global statics)
    let Some((&sel, payload)) = data.split_first() else {
        return;
    };
    let mut rd = Rd { b: payload };
    match sel % 7 {
        0 => arm_parse_int(&mut rd),
        1 => arm_parse_real(&mut rd),
        2 => arm_to_base(&mut rd),
        3 => arm_int_from_base(&mut rd),
        4 => arm_real_from_base(&mut rd),
        5 => arm_unit_name(&mut rd),
        _ => arm_fmt(&mut rd),
    }
}

fn arm_parse_int(rd: &mut Rd<'_>) {
    let flags = rd.i32();
    let Some(s) = rd.rest_str() else { return };
    let c = cstr(s);
    let (mut c_res, mut c_hint) = (0i32, 0i32);
    let c_ok = unsafe {
        pg_diff_guc_parse_int(c.as_ptr() as *const _, flags, &mut c_res, &mut c_hint)
    };
    match parse_int(s, flags) {
        ParseNum::Ok(v) => {
            assert_eq!(c_ok, 1, "parse_int verdict DIVERGENCE on {s:?} flags={flags:#x}: C rejects, Rust Ok({v})");
            assert_eq!(c_res, v, "parse_int value DIVERGENCE on {s:?} flags={flags:#x}");
        }
        ParseNum::Err { hint } => {
            assert_eq!(c_ok, 0, "parse_int verdict DIVERGENCE on {s:?} flags={flags:#x}: C Ok({c_res}), Rust rejects");
            assert_eq!(c_hint, hint_class(hint), "parse_int hint DIVERGENCE on {s:?} flags={flags:#x}");
        }
    }
}

fn arm_parse_real(rd: &mut Rd<'_>) {
    let flags = rd.i32();
    let Some(s) = rd.rest_str() else { return };
    let c = cstr(s);
    let (mut c_res, mut c_hint) = (0f64, 0i32);
    let c_ok = unsafe {
        pg_diff_guc_parse_real(c.as_ptr() as *const _, flags, &mut c_res, &mut c_hint)
    };
    match parse_real(s, flags) {
        ParseNum::Ok(v) => {
            assert_eq!(c_ok, 1, "parse_real verdict DIVERGENCE on {s:?} flags={flags:#x}: C rejects, Rust Ok({v})");
            assert_eq!(
                c_res.to_bits(),
                v.to_bits(),
                "parse_real value DIVERGENCE on {s:?} flags={flags:#x}: C={c_res} Rust={v}"
            );
        }
        ParseNum::Err { hint } => {
            assert_eq!(c_ok, 0, "parse_real verdict DIVERGENCE on {s:?} flags={flags:#x}: C Ok({c_res}), Rust rejects");
            assert_eq!(c_hint, hint_class(hint), "parse_real hint DIVERGENCE on {s:?} flags={flags:#x}");
        }
    }
}

fn arm_to_base(rd: &mut Rd<'_>) {
    let base_unit = rd.i32();
    let value = rd.f64();
    let Some(unit) = rd.rest_str() else { return };
    let c = cstr(unit);
    let mut c_bv = 0f64;
    let c_ok = unsafe {
        pg_diff_guc_convert_to_base_unit(value, c.as_ptr() as *const _, base_unit, &mut c_bv)
    };
    match convert_to_base_unit(value, unit.as_bytes(), base_unit) {
        Some(bv) => {
            assert_eq!(c_ok, 1, "convert_to_base_unit verdict DIVERGENCE on {unit:?} base={base_unit:#x}");
            assert_eq!(
                c_bv.to_bits(),
                bv.to_bits(),
                "convert_to_base_unit value DIVERGENCE on {unit:?} base={base_unit:#x} v={value}: C={c_bv} Rust={bv}"
            );
        }
        None => {
            assert_eq!(c_ok, 0, "convert_to_base_unit verdict DIVERGENCE on {unit:?} base={base_unit:#x}: C Ok({c_bv}), Rust None");
        }
    }
}

fn arm_int_from_base(rd: &mut Rd<'_>) {
    let base_unit = rd.i32();
    let base_value = rd.i64();
    let mut c_v = 0i64;
    let mut c_unit = [0u8; 8];
    let mut c_has = 0i32;
    unsafe {
        pg_diff_guc_convert_int_from_base_unit(
            base_value,
            base_unit,
            &mut c_v,
            c_unit.as_mut_ptr(),
            &mut c_has,
        )
    };
    let (r_v, r_unit) = convert_int_from_base_unit(base_value, base_unit);
    if c_has == 0 {
        // C leaves *value/*unit unwritten (Assert-only arm); the shipped
        // fallback is (base_value, "") — presence plane only.
        assert_eq!(r_unit, "", "convert_int_from_base_unit presence DIVERGENCE base={base_unit:#x}");
        return;
    }
    let c_unit_str = core::str::from_utf8(&c_unit[..c_unit.iter().position(|&b| b == 0).unwrap()])
        .expect("C unit is ASCII");
    assert_eq!(
        (c_v, c_unit_str),
        (r_v, r_unit),
        "convert_int_from_base_unit DIVERGENCE v={base_value} base={base_unit:#x}"
    );
}

fn arm_real_from_base(rd: &mut Rd<'_>) {
    let base_unit = rd.i32();
    let base_value = rd.f64();
    let mut c_v = 0f64;
    let mut c_unit = [0u8; 8];
    let mut c_has = 0i32;
    unsafe {
        pg_diff_guc_convert_real_from_base_unit(
            base_value,
            base_unit,
            &mut c_v,
            c_unit.as_mut_ptr(),
            &mut c_has,
        )
    };
    let (r_v, r_unit) = convert_real_from_base_unit(base_value, base_unit);
    if c_has == 0 {
        assert_eq!(r_unit, "", "convert_real_from_base_unit presence DIVERGENCE base={base_unit:#x}");
        return;
    }
    let c_unit_str = core::str::from_utf8(&c_unit[..c_unit.iter().position(|&b| b == 0).unwrap()])
        .expect("C unit is ASCII");
    assert_eq!(c_unit_str, r_unit, "convert_real_from_base_unit unit DIVERGENCE v={base_value} base={base_unit:#x}");
    assert_eq!(
        c_v.to_bits(),
        r_v.to_bits(),
        "convert_real_from_base_unit value DIVERGENCE v={base_value} base={base_unit:#x}: C={c_v} Rust={r_v}"
    );
}

fn arm_unit_name(rd: &mut Rd<'_>) {
    // Valid units values only: the invalid arm is elog/panic C-parity
    // (fenced; witnessed by elog_parity_units_value).
    let flags = VALID_UNITS[(rd.u8() % 9) as usize] | (rd.i32() & !(0x7F00_0000));
    let mut c_name = [0u8; 8];
    let mut c_has = 0i32;
    let cst = unsafe {
        pg_diff_guc_get_config_unit_name(flags, c_name.as_mut_ptr(), &mut c_has)
    };
    assert_eq!(cst, 0, "get_config_unit_name errored on a valid units value {flags:#x}");
    let r = get_config_unit_name(flags);
    assert_eq!(c_has != 0, r.is_some(), "get_config_unit_name presence DIVERGENCE flags={flags:#x}");
    if let Some(name) = r {
        let c_str =
            core::str::from_utf8(&c_name[..c_name.iter().position(|&b| b == 0).unwrap()]).unwrap();
        assert_eq!(c_str, name, "get_config_unit_name DIVERGENCE flags={flags:#x}");
    }
}

/// arm 6: fmt_g_prec / fmt_e vs PG-snprintf semantics (NaN/Infinity arms
/// verbatim-replicated in the oracle; finite values delegate to the system
/// snprintf %.*g / %.*e — glibc on the floor is the oracle of record).
/// Precision domain 0..=30 covers guc's uses (%g default 6) with headroom.
fn arm_fmt(rd: &mut Rd<'_>) {
    let sel = rd.u8();
    let style_e = sel & 1 != 0;
    let prec = (sel >> 1) % 31;
    let v = rd.f64();
    let mut out = [0u8; 1200];
    let cst = unsafe {
        pg_diff_guc_fmt(v, prec as i32, style_e as i32, out.as_mut_ptr(), 1200)
    };
    assert_eq!(cst, 0);
    let c_str = core::str::from_utf8(&out[..out.iter().position(|&b| b == 0).unwrap()])
        .expect("printf output is ASCII");
    let r = if style_e {
        fmt_e(v, prec as usize)
    } else {
        fmt_g_prec(v, (prec as usize).max(1))
    };
    // C %.*g with prec 0 is treated as 1 (printf spec); fmt_g_prec applies
    // the same floor internally — feed the floored value for the compare.
    assert_eq!(c_str, r, "fmt DIVERGENCE v={v:e} prec={prec} style_e={style_e}");
    if !style_e && prec == 6 {
        // fmt_g is the shipped default-precision wrapper (%g): same plane.
        assert_eq!(c_str, fmt_g(v), "fmt_g DIVERGENCE v={v:e}");
    }
}

/// Auxiliary exhaustive check used by tests: c_strtol_base0/c_strtod endptr
/// parity is already exercised through parse_int/parse_real (every call
/// routes through them); no separate plane needed.
pub fn _cnum_probe(s: &str) -> (i64, usize, bool, f64, usize, bool) {
    let a = c_strtol_base0(s.as_bytes());
    let b = c_strtod(s.as_bytes());
    (a.value, a.consumed, a.erange, b.value, b.consumed, b.erange)
}

// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn seed_corpus_replays_clean() {
        let dir = concat!(env!("CARGO_MANIFEST_DIR"), "/../corpus/guc_units_diff");
        let mut n = 0;
        for e in std::fs::read_dir(dir).expect("corpus/guc_units_diff missing") {
            let p = e.unwrap().path();
            if p.is_file() {
                guc_units_diff(&std::fs::read(&p).unwrap());
                n += 1;
            }
        }
        assert!(n >= 30, "expected >=30 seeds, found {n}");
    }

    /// C-parity witness for the unrecognized-units elog in
    /// get_config_unit_name: C status 190 and the shipped Rust panics on the
    /// same invalid units value (0x06000000 is not a defined unit).
    #[test]
    fn elog_parity_units_value() {
        let _g = crate::c_oracle_serial();
        let mut name = [0u8; 8];
        let mut has = 0i32;
        let cst = unsafe {
            pg_diff_guc_get_config_unit_name(0x0600_0000, name.as_mut_ptr(), &mut has)
        };
        assert_eq!(cst, 190, "C must elog on unrecognized units value");
        let r = std::panic::catch_unwind(|| get_config_unit_name(0x0600_0000));
        assert!(r.is_err(), "Rust must panic on unrecognized units value");
    }

    #[test]
    fn arms_smoke() {
        let strs = [
            "42", "-42", "  42  ", "0x1f", "0X1F", "010", "08", "0x", "0", "",
            "1.5", "-1.5", ".5", "5.", "1e3", "1E3", "1e-3", "1e+3", "1.5e2",
            "2147483647", "2147483648", "-2147483648", "-2147483649",
            "9223372036854775807", "9223372036854775808", "-9223372036854775808",
            "1e400", "-1e400", "1e-400", "1e-320",
            "inf", "INF", "infinity", "nan", "NaN", "-inf",
            "0x1.8p1", "0x10p2",
            "1kB", "1 kB", "1\tkB", "1kB ", "1 kB x", "30.1GB", "1TB", "512MB",
            "100ms", "1 s", "5min", "2h", "1d", "10us", "1.5s", "0.5min",
            "1KB", "1Mb", "1gb", "1kBs", "1kBB", "1xyz", "1 xyzw",
            "999999999999TB", "-1kB", "-30.1GB",
        ];
        let units: [i32; 12] = [
            0,
            GUC_UNIT_BYTE,
            GUC_UNIT_KB,
            GUC_UNIT_MB,
            GUC_UNIT_BLOCKS,
            GUC_UNIT_XBLOCKS,
            GUC_UNIT_MS,
            GUC_UNIT_S,
            GUC_UNIT_MIN,
            0x0600_0000, // invalid memory-class units value (no elog in parse path)
            0x4000_0000, // invalid time-class
            -1,
        ];
        for s in strs {
            for u in units {
                for arm in [0u8, 1] {
                    let mut v = vec![arm];
                    v.extend_from_slice(&u.to_le_bytes());
                    v.extend_from_slice(s.as_bytes());
                    guc_units_diff(&v);
                }
                // convert_to_base_unit arm over the unit-suffix corpus
                let mut v = vec![2];
                v.extend_from_slice(&u.to_le_bytes());
                v.extend_from_slice(&1.5f64.to_le_bytes());
                v.extend_from_slice(s.as_bytes());
                guc_units_diff(&v);
            }
        }
        // int/real from_base over boundary values
        for u in units {
            for x in [
                0i64, 1, -1, 1024, 1025, 1024 * 1024, 8, 86400000, 60, 1000,
                i64::MAX, i64::MIN, 999999999,
            ] {
                let mut v = vec![3];
                v.extend_from_slice(&u.to_le_bytes());
                v.extend_from_slice(&x.to_le_bytes());
                guc_units_diff(&v);
            }
            for x in [0.0f64, 1.0, -1.0, 0.5, 1024.0, 1e-8, 1e300, -0.0, 123.456] {
                let mut v = vec![4];
                v.extend_from_slice(&u.to_le_bytes());
                v.extend_from_slice(&x.to_le_bytes());
                guc_units_diff(&v);
            }
        }
        // unit_name over all valid values
        for i in 0u8..9 {
            let mut v = vec![5, i];
            v.extend_from_slice(&0i32.to_le_bytes());
            guc_units_diff(&v);
        }
        // fmt plane: styles x precisions x value classes
        for sel in [0u8, 1, 12, 13, 34, 35, 60, 61] {
            for v in [
                0.0f64, -0.0, 1.5, -1.5, 100.0, 1.23456789, 1234567.0, 0.0001,
                0.00001, 9.9999999, 1e300, -1e300, 5e-324, 1e-308, f64::NAN,
                -f64::NAN, f64::INFINITY, f64::NEG_INFINITY, 123456789.123456789,
            ] {
                let mut b = vec![6, sel];
                b.extend_from_slice(&v.to_le_bytes());
                guc_units_diff(&b);
            }
        }
        // truncated
        guc_units_diff(&[]);
        guc_units_diff(&[0]);
        guc_units_diff(&[2, 1]);
        guc_units_diff(&[5]);
    }
}

#[cfg(test)]
mod hexfloat_underflow_witness {
    /// Fleet floor-2 crash-83e8055b5c: a hex-float literal with a >2^32
    /// binary-exponent digit string must underflow to 0 with ERANGE (glibc
    /// parity). Pre-fix, round_to_float's `drop as u32` truncated (2^40 ->
    /// 0) and produced a bogus 2.99e-300. The banked fleet input replays
    /// through the parse_int hint plane below.
    #[test]
    fn huge_negative_hex_exponent_underflows_erange() {
        let (v, consumed, erange) =
            ::adt_float::io::strtod_c(b"0x1p-10995116277760").expect("token");
        assert_eq!((v.to_bits(), consumed, erange), (0, 19, true));
        let (v2, _, er2) = ::adt_float::io::strtod_c(b"0x1p+10995116277760").expect("token");
        assert!(v2.is_infinite() && er2, "overflow side must stay inf+ERANGE");
    }

    #[test]
    fn fleet_divergence_input_replays_clean() {
        let dir = concat!(env!("CARGO_MANIFEST_DIR"), "/../corpus/guc_units_diff");
        let f = format!("{dir}/seed-fleet-crash-83e8055b5c");
        super::guc_units_diff(&std::fs::read(f).unwrap());
    }
}
