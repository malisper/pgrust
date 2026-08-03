// strtol(value, &endptr, 0) / strtod(value, &endptr) with exact endptr and
// ERANGE semantics; both are load-bearing for parse_int/parse_real.

#[derive(Clone, Copy, Debug)]
pub struct ScanInt {
    pub value: i64,
    pub consumed: usize,
    pub erange: bool,
}

#[derive(Clone, Copy, Debug)]
pub struct ScanReal {
    pub value: f64,
    pub consumed: usize,
    pub erange: bool,
}

#[inline]
pub(crate) fn is_c_space(c: u8) -> bool {
    matches!(c, b' ' | b'\t' | b'\n' | 0x0b | 0x0c | b'\r')
}

pub fn c_strtol_base0(s: &[u8]) -> ScanInt {
    let mut i = 0usize;
    while i < s.len() && is_c_space(s[i]) {
        i += 1;
    }

    let mut negative = false;
    if i < s.len() && (s[i] == b'+' || s[i] == b'-') {
        negative = s[i] == b'-';
        i += 1;
    }

    let mut base: u32 = 10;
    if i < s.len() && s[i] == b'0' {
        if i + 1 < s.len() && (s[i + 1] == b'x' || s[i + 1] == b'X') {
            if i + 2 < s.len() && (s[i + 2] as char).is_ascii_hexdigit() {
                base = 16;
                i += 2;
            } else {
                // "0" alone; the 'x' is left for endptr.
                return ScanInt { value: 0, consumed: i + 1, erange: false };
            }
        } else {
            base = 8;
        }
    }

    let digits_start = i;
    // i128 magnitude accumulator: i64 cannot hold |i64::MIN| (2^63), and a
    // wrapped magnitude made `-acc` overflow on "-9223372036854775808"
    // (debug-panic crash class, fuzz-caught).
    let mut acc: i128 = 0;
    let mut overflow = false;
    let mut any = false;

    while i < s.len() {
        let c = s[i];
        let digit = match c {
            b'0'..=b'9' => (c - b'0') as u32,
            b'a'..=b'f' => (c - b'a' + 10) as u32,
            b'A'..=b'F' => (c - b'A' + 10) as u32,
            _ => break,
        };
        if digit >= base {
            break;
        }
        any = true;
        if !overflow {
            let next = acc * (base as i128) + (digit as i128);
            let signed = if negative { -next } else { next };
            if signed > i64::MAX as i128 || signed < i64::MIN as i128 {
                overflow = true;
            } else {
                acc = next;
            }
        }
        i += 1;
    }

    if !any && digits_start == i {
        if base == 8 && digits_start > 0 && s.get(digits_start - 1) == Some(&b'0') {
            return ScanInt { value: 0, consumed: digits_start, erange: false };
        }
        return ScanInt { value: 0, consumed: 0, erange: false };
    }

    if overflow {
        return ScanInt {
            value: if negative { i64::MIN } else { i64::MAX },
            consumed: i,
            erange: true,
        };
    }

    let value = (if negative { -acc } else { acc }) as i64;
    ScanInt { value, consumed: i, erange: false }
}

pub fn c_strtod(s: &[u8]) -> ScanReal {
    // strtod(3) parity is owned by adt_float::io::strtod_c (decimal + hex
    // floats + inf/nan words, glibc ERANGE semantics including the
    // subnormal-inexact underflow probe). This shim only adapts the shape.
    match ::adt_float::io::strtod_c(s) {
        Some((value, consumed, erange)) => ScanReal { value, consumed, erange },
        None => ScanReal { value: 0.0, consumed: 0, erange: false },
    }
}
