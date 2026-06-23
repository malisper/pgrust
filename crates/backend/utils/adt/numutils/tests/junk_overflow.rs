use ::numutils_fgram::pg_strtoint16;
use ::types_error::{ERRCODE_INVALID_TEXT_REPRESENTATION, ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE};

// C numutils.c gives invalid_syntax for these (trailing junk seen after the
// per-digit guard tmp > -(MIN/base) admits tmp up to 32769).
#[test]
fn overflow_boundary_with_trailing_junk_is_invalid_syntax() {
    for s in ["32768x", "32769x", "-32769x", "32768_", "0x8001z"] {
        let e = pg_strtoint16(s).unwrap_err();
        assert_eq!(e.sqlstate(), ERRCODE_INVALID_TEXT_REPRESENTATION, "{s}");
    }
    // but with no trailing junk it is out of range
    let e = pg_strtoint16("32768").unwrap_err();
    assert_eq!(e.sqlstate(), ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE);
}
