//! Tests for the idiomatic `snprintf.c` port.  Expected outputs are what C
//! `printf` produces for the same format/args.

use super::*;

fn fmt(format: &str, args: &[PrintfArg<'_>]) -> String {
    String::from_utf8(pg_vsnprintf(format, args, 0).expect("format")).expect("utf8")
}

#[test]
fn formats_strings_and_integers() {
    let out = fmt(
        "hello %s %+05d %u %x %X",
        &[
            "pg".into(),
            42.into(),
            PrintfArg::Int(-1),
            255u32.into(),
            255u32.into(),
        ],
    );
    assert_eq!(out, "hello pg +0042 4294967295 ff FF");
}

#[test]
fn literal_percent() {
    assert_eq!(fmt("100%% done", &[]), "100% done");
}

#[test]
fn signed_negative_and_forcesign() {
    assert_eq!(fmt("%d", &[PrintfArg::Int(-5)]), "-5");
    assert_eq!(fmt("%+d", &[7.into()]), "+7");
    assert_eq!(fmt("%5d", &[42.into()]), "   42");
    assert_eq!(fmt("%-5d|", &[42.into()]), "42   |");
    assert_eq!(fmt("%05d", &[42.into()]), "00042");
    assert_eq!(fmt("%+05d", &[42.into()]), "+0042");
    assert_eq!(fmt("%05d", &[PrintfArg::Int(-42)]), "-0042");
}

#[test]
fn precision_on_integers() {
    assert_eq!(fmt("%.5d", &[42.into()]), "00042");
    assert_eq!(fmt("%8.5d", &[42.into()]), "   00042");
    // SUS: 0 with explicit precision 0 is no characters.
    assert_eq!(fmt("[%.0d]", &[0.into()]), "[]");
    assert_eq!(fmt("[%.0d]", &[5.into()]), "[5]");
}

#[test]
fn unsigned_bases() {
    assert_eq!(fmt("%o", &[64u32.into()]), "100");
    assert_eq!(fmt("%x", &[255u32.into()]), "ff");
    assert_eq!(fmt("%X", &[255u32.into()]), "FF");
    assert_eq!(fmt("%u", &[100u32.into()]), "100");
}

#[test]
fn long_and_longlong_widths() {
    // -1 as unsigned: int -> 32-bit, long/ll -> 64-bit.
    assert_eq!(fmt("%u", &[PrintfArg::Int(-1)]), "4294967295");
    assert_eq!(fmt("%lu", &[PrintfArg::Int(-1)]), "18446744073709551615");
    assert_eq!(fmt("%llu", &[PrintfArg::Int(-1)]), "18446744073709551615");
    assert_eq!(fmt("%lld", &[PrintfArg::Int(-1)]), "-1");
    // z maps to long-width.
    assert_eq!(fmt("%zu", &[123usize.into()]), "123");
}

#[test]
fn char_conversion() {
    assert_eq!(fmt("%c", &['A'.into()]), "A");
    assert_eq!(fmt("%3c|", &['A'.into()]), "  A|");
    assert_eq!(fmt("%-3c|", &['A'.into()]), "A  |");
}

#[test]
fn string_precision_and_width() {
    assert_eq!(fmt("%.3s", &["abcdef".into()]), "abc");
    assert_eq!(fmt("%6s", &["abc".into()]), "   abc");
    assert_eq!(fmt("%-6s|", &["abc".into()]), "abc   |");
    // NULL silently becomes "(null)".
    assert_eq!(fmt("%s", &[PrintfArg::Null]), "(null)");
}

#[test]
fn fast_path_percent_s() {
    assert_eq!(fmt("%s%s", &["a".into(), "b".into()]), "ab");
}

#[test]
fn pointer_format() {
    assert_eq!(fmt("%p", &[PrintfArg::Null]), "(nil)");
    assert_eq!(fmt("%p", &[PrintfArg::Ptr(0x1234)]), "0x1234");
    assert_eq!(fmt("%p", &[PrintfArg::Ptr(0xdeadbeef)]), "0xdeadbeef");
}

#[test]
fn float_f_conversion() {
    assert_eq!(fmt("%f", &[1.5f64.into()]), "1.500000");
    assert_eq!(fmt("%.2f", &[1.5f64.into()]), "1.50");
    assert_eq!(fmt("%+.2f", &[1.5f64.into()]), "+1.50");
    assert_eq!(fmt("%.0f", &[2.5f64.into()]), "2"); // round-half-to-even
    assert_eq!(fmt("%.0f", &[3.5f64.into()]), "4");
    assert_eq!(fmt("%8.2f", &[1.5f64.into()]), "    1.50");
    assert_eq!(fmt("%-8.2f|", &[1.5f64.into()]), "1.50    |");
    assert_eq!(fmt("%08.2f", &[1.5f64.into()]), "00001.50");
    assert_eq!(fmt("%f", &[(-1.5f64).into()]), "-1.500000");
}

#[test]
fn float_e_conversion() {
    assert_eq!(fmt("%e", &[1234.5f64.into()]), "1.234500e+03");
    assert_eq!(fmt("%.2e", &[1234.5f64.into()]), "1.23e+03");
    assert_eq!(fmt("%E", &[1234.5f64.into()]), "1.234500E+03");
    assert_eq!(fmt("%.1e", &[0.001f64.into()]), "1.0e-03");
}

#[test]
fn float_g_conversion() {
    assert_eq!(fmt("%g", &[1234.5f64.into()]), "1234.5");
    assert_eq!(fmt("%g", &[0.0001f64.into()]), "0.0001");
    assert_eq!(fmt("%g", &[0.00001f64.into()]), "1e-05");
    assert_eq!(fmt("%g", &[100000.0f64.into()]), "100000");
    assert_eq!(fmt("%g", &[1000000.0f64.into()]), "1e+06");
    assert_eq!(fmt("%.3g", &[3.14159f64.into()]), "3.14");
    assert_eq!(fmt("%G", &[0.00001f64.into()]), "1E-05");
}

#[test]
fn float_specials() {
    assert_eq!(fmt("%f", &[f64::NAN.into()]), "NaN");
    assert_eq!(fmt("%f", &[f64::INFINITY.into()]), "Infinity");
    assert_eq!(fmt("%f", &[f64::NEG_INFINITY.into()]), "-Infinity");
    assert_eq!(fmt("%g", &[f64::INFINITY.into()]), "Infinity");
    // Specials never zero-pad.
    assert_eq!(fmt("%08f", &[f64::INFINITY.into()]), "Infinity");
    // -0.0 keeps its sign.
    assert_eq!(fmt("%f", &[(-0.0f64).into()]), "-0.000000");
}

#[test]
fn float_precision_over_350_zero_pads() {
    // precision beyond 350 is filled with literal zeroes after the digits.
    let s = fmt("%.355f", &[1.0f64.into()]);
    // "1." + 355 fractional digits.
    assert_eq!(s.len(), "1.".len() + 355);
    assert!(s.starts_with("1."));
    assert!(s.ends_with("00000")); // trailing extra zero pad
}

#[test]
fn truncating_buffer_reports_would_be_length() {
    let mut buf = [0u8; 6];
    let len = pg_snprintf_into(&mut buf, "abcdef%s", &["ghi".into()], 0).expect("format");
    assert_eq!(len, 9);
    assert_eq!(&buf, b"abcde\0");
}

#[test]
fn zero_length_buffer_reports_length_without_writing() {
    let len = pg_snprintf_into(&mut [], "abc", &[], 0).expect("format");
    assert_eq!(len, 3);
}

#[test]
fn supports_positional_arguments_and_star_width() {
    // %2$*1$s : second arg as string, width from first arg.
    let out = fmt("%2$*1$s:%3$d", &[5.into(), "x".into(), 7.into()]);
    assert_eq!(out, "    x:7");
}

#[test]
fn positional_reuse() {
    let out = fmt("%1$d %1$d %2$s", &[9.into(), "z".into()]);
    assert_eq!(out, "9 9 z");
}

#[test]
fn positional_type_conflict_is_error() {
    // %1$d and %1$s disagree on type -> invalid.
    assert_eq!(
        pg_vsnprintf("%1$d %1$s", &[1.into(), "x".into()], 0),
        Err(PrintfError::InvalidFormat)
    );
}

#[test]
fn positional_out_of_range_is_error() {
    assert_eq!(
        pg_vsnprintf("%99$d", &[1.into()], 0),
        Err(PrintfError::InvalidFormat)
    );
}

#[test]
fn percent_m_uses_strerror() {
    // ENOENT == 2 on every unix.
    let out = String::from_utf8(pg_vsnprintf("failed: %m", &[], 2).expect("format")).unwrap();
    assert!(out.starts_with("failed: "));
    assert!(out.len() > "failed: ".len());
}

#[test]
fn invalid_format_is_an_error() {
    assert_eq!(pg_vsnprintf("%q", &[], 0), Err(PrintfError::InvalidFormat));
    // A bare '%' at end of string is bogus.
    assert_eq!(pg_vsnprintf("abc%", &[], 0), Err(PrintfError::InvalidFormat));
}

#[test]
fn missing_argument_is_an_error() {
    assert_eq!(
        pg_vsnprintf("%d %d", &[1.into()], 0),
        Err(PrintfError::MissingArgument(2))
    );
}

#[test]
fn wrong_argument_type_is_an_error() {
    assert_eq!(
        pg_vsnprintf("%d", &["nope".into()], 0),
        Err(PrintfError::WrongArgumentType)
    );
    assert_eq!(
        pg_vsnprintf("%f", &[1.into()], 0),
        Err(PrintfError::WrongArgumentType)
    );
}

#[test]
fn fprintf_writes_to_rust_writer() {
    let mut out = Vec::new();
    let len = pg_fprintf(&mut out, "%s %d", &["ok".into(), 3.into()], 0).expect("write");
    assert_eq!(len, 4);
    assert_eq!(out, b"ok 3");
}

#[test]
fn fprintf_large_run_chunks() {
    let mut out = Vec::new();
    // width 200 forces an outchmulti run longer than the stream staging chunk.
    let len = pg_fprintf(&mut out, "%200d", &[1.into()], 0).expect("write");
    assert_eq!(len, 200);
    assert_eq!(out.len(), 200);
    assert!(out.starts_with(&[b' '; 100]));
    assert_eq!(out[199], b'1');
}

#[test]
fn strfromd_basic() {
    let mut buf = [0u8; 64];
    let len = pg_strfromd(&mut buf, 6, 1234.5);
    assert_eq!(len, "1234.5".len());
    assert_eq!(&buf[..len], b"1234.5");

    let len = pg_strfromd(&mut buf, 2, 0.0001234);
    assert_eq!(&buf[..len], b"0.00012");
}

#[test]
fn strfromd_specials_and_sign() {
    let mut buf = [0u8; 64];
    let len = pg_strfromd(&mut buf, 6, f64::NAN);
    assert_eq!(&buf[..len], b"NaN");

    let len = pg_strfromd(&mut buf, 6, f64::NEG_INFINITY);
    assert_eq!(&buf[..len], b"-Infinity");

    let len = pg_strfromd(&mut buf, 6, -1.5);
    assert_eq!(&buf[..len], b"-1.5");

    let len = pg_strfromd(&mut buf, 6, -0.0);
    assert_eq!(&buf[..len], b"-0");
}

#[test]
fn strfromd_precision_clamped() {
    let mut buf = [0u8; 64];
    // precision < 1 clamps to 1.
    let len = pg_strfromd(&mut buf, 0, 1234.5);
    assert_eq!(&buf[..len], b"1e+03");
}

#[test]
fn literal_runs_and_specs_interleave() {
    let out = fmt("a%db%sc%dd", &[1.into(), "X".into(), 2.into()]);
    assert_eq!(out, "a1bXc2d");
}

#[test]
fn ignored_flags_and_lengths() {
    // 'h' length and "'" group flag are ignored.
    assert_eq!(fmt("%hd", &[42.into()]), "42");
    assert_eq!(fmt("%'d", &[42.into()]), "42");
}
