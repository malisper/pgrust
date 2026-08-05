//! Kani C≡Rust equivalence PoC: pg_strtoint32_safe.
//!
//! C side: c/pg_numutils_vendored.c — verbatim postgres-master parsing logic
//! with ereturn shimmed to an err_kind out-param (see SHIM MANIFEST in that
//! file). Rust side: the SHIPPED numutils crate from pgrust-fast (path dep),
//! including its SWAR fast path and deferred overflow guard.
//!
//! Equivalence claim, per symbolic input of bounded length:
//!   verdict(C) == verdict(Rust)  where verdict ∈ {OK, OUT_OF_RANGE, INVALID_SYNTAX}
//!   and, when both are OK, the parsed i32 values are equal.
//!
//! Domain restriction (documented, deliberate):
//!   - bytes are 1..=127 (no interior NUL: C is NUL-terminated, Rust is
//!     length-delimited — interior NUL is a representational mismatch, not a
//!     parser question; no >=128 bytes: keeps the &str UTF-8-valid without a
//!     multi-byte-UTF-8 side condition, and pg parses in the C locale where
//!     high bytes are never digits/space anyway).

use types_error::{ERRCODE_INVALID_TEXT_REPRESENTATION, ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE};

extern "C" {
    fn pgc_strtoint32_safe(s: *const core::ffi::c_char, err_kind: *mut i32) -> i32;
}

const OK: i32 = 0;
const OUT_OF_RANGE: i32 = 1;
const INVALID_SYNTAX: i32 = 2;

fn rust_verdict(s: &str) -> (i32, i32) {
    match numutils::pg_strtoint32_safe(s, None) {
        Ok(v) => (OK, v),
        Err(e) => {
            let ss = e.sqlstate();
            if ss == ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE {
                (OUT_OF_RANGE, 0)
            } else if ss == ERRCODE_INVALID_TEXT_REPRESENTATION {
                (INVALID_SYNTAX, 0)
            } else {
                (99, 0) // unreachable if the port is faithful
            }
        }
    }
}

fn c_verdict(buf: &[core::ffi::c_char]) -> (i32, i32) {
    let mut kind: i32 = -1;
    let v = unsafe { pgc_strtoint32_safe(buf.as_ptr(), &mut kind) };
    (kind, v)
}

/// Core check for symbolic strings of length exactly-up-to N (a symbolic
/// length `len <= N` is modeled by making bytes beyond `len` the terminator).
#[cfg(kani)]
fn check<const N: usize>() {
    let len: usize = kani::any();
    kani::assume(len <= N);

    let mut bytes = [0u8; N];
    // C buffer is deliberately OVERSIZED (32 >> N + unwind bound): CBMC
    // unwinds loops past the feasible trip count, and if those infeasible
    // iterations can push `ptr` out of the object, the pointer-UB modeling
    // blows up symex (measured: N=4 with a 5-byte buffer walls >400s; with
    // this 32-byte buffer it verifies in ~85s). Bytes beyond `len` stay NUL.
    let mut cbuf = [0 as core::ffi::c_char; 32];
    for i in 0..N {
        if i < len {
            let b: u8 = kani::any();
            kani::assume(b >= 1 && b <= 127); // no NUL, ASCII-only (see module doc)
            bytes[i] = b;
            cbuf[i] = b as core::ffi::c_char;
        }
    }

    let s = unsafe { core::str::from_utf8_unchecked(&bytes[..len]) }; // ASCII ⇒ valid UTF-8

    let (ck, cv) = c_verdict(&cbuf);
    let (rk, rv) = rust_verdict(s);

    kani::assert(ck == rk, "error-verdict divergence");
    kani::assert(ck != OK || cv == rv, "parsed-value divergence");
}

/// Stub for `alloc::fmt::format`: the port's error paths build the human
/// message via `format!`, whose fmt machinery walls CBMC. The message text
/// is NOT part of the equivalence claim (only the error KIND, read off the
/// sqlstate, which is set independently of the message), so replacing the
/// formatter with an empty string preserves everything we assert.
#[cfg(kani)]
fn stub_format(_args: core::fmt::Arguments<'_>) -> String {
    String::new()
}

/// Stub for `types_error::PgError::new`: the shipped constructor is
/// #[track_caller] and reads core::panic::Location::caller() at runtime to
/// fill the F/L wire fields; Kani does not support the caller_location
/// intrinsic (kani#374). This stub builds the same struct with location=None.
/// Harmless to the claim: numutils' error constructors immediately overwrite
/// location via .with_location("numutils.c", ...), and neither the location
/// nor the message text is asserted — only the sqlstate-derived error KIND,
/// which this stub sets identically (default_sqlstate_for_level, then
/// .with_sqlstate at the numutils call sites).
#[cfg(kani)]
fn stub_pg_error_new(
    level: types_error::ErrorLevel,
    message: impl Into<String>,
) -> types_error::PgError {
    types_error::PgError {
        level,
        sqlstate: types_error::default_sqlstate_for_level(level),
        message: message.into(),
        message_raw: None,
        detail: None,
        detail_log: None,
        hint: None,
        context: None,
        backtrace: None,
        message_id: None,
        domain: None,
        context_domain: None,
        hide_statement: false,
        hide_context: false,
        location: None,
        saved_errno: None,
        cursor_position: None,
        internal_position: None,
        internal_query: None,
        schema_name: None,
        table_name: None,
        column_name: None,
        datatype_name: None,
        constraint_name: None,
        plpgsql_context_attached: false,
    }
}

/// Like `check`, but restricted to the sign+digits shape ^-?[0-9]+$ with
/// 1..=N chars. This is the domain where the port's novel code lives (SWAR
/// blocks + deferred overflow guard vs C's per-digit guard); the general
/// harness covers the rest of the grammar at smaller lengths.
#[cfg(kani)]
fn check_digits<const N: usize>() {
    let len: usize = kani::any();
    kani::assume(len >= 1 && len <= N);

    let mut bytes = [0u8; N];
    let mut cbuf = [0 as core::ffi::c_char; 32]; // oversized on purpose, see check()
    for i in 0..N {
        if i < len {
            let b: u8 = kani::any();
            if i == 0 {
                kani::assume(b == b'-' || (b'0'..=b'9').contains(&b));
            } else {
                kani::assume((b'0'..=b'9').contains(&b));
            }
            bytes[i] = b;
            cbuf[i] = b as core::ffi::c_char;
        }
    }

    let s = unsafe { core::str::from_utf8_unchecked(&bytes[..len]) };

    let (ck, cv) = c_verdict(&cbuf);
    let (rk, rv) = rust_verdict(s);

    kani::assert(ck == rk, "error-verdict divergence (digits domain)");
    kani::assert(ck != OK || cv == rv, "parsed-value divergence (digits domain)");
}

/// Exact-length digits harness: len is CONCRETE (= N), first byte is '-' or a
/// digit, the rest are digits. Removes the symbolic-length dimension so the
/// i32-boundary length (sign + 10 digits = 11) fits the solver budget.
#[cfg(kani)]
fn check_digits_exact<const N: usize>() {
    let mut bytes = [0u8; N];
    let mut cbuf = [0 as core::ffi::c_char; 32];
    for i in 0..N {
        let b: u8 = kani::any();
        if i == 0 {
            kani::assume(b == b'-' || (b'0'..=b'9').contains(&b));
        } else {
            kani::assume((b'0'..=b'9').contains(&b));
        }
        bytes[i] = b;
        cbuf[i] = b as core::ffi::c_char;
    }

    let s = unsafe { core::str::from_utf8_unchecked(&bytes[..]) };

    let (ck, cv) = c_verdict(&cbuf);
    let (rk, rv) = rust_verdict(s);

    kani::assert(ck == rk, "error-verdict divergence (exact digits)");
    kani::assert(ck != OK || cv == rv, "parsed-value divergence (exact digits)");
}

#[cfg(kani)]
mod harnesses {
    use super::*;

    #[kani::proof]
    #[kani::unwind(14)]
    #[kani::stub(alloc::fmt::format, stub_format)]
    #[kani::stub(types_error::PgError::new, stub_pg_error_new)]
    fn equiv_digits_exact11() {
        check_digits_exact::<11>();
    }

    #[kani::proof]
    #[kani::unwind(8)]
    #[kani::stub(alloc::fmt::format, stub_format)]
    #[kani::stub(types_error::PgError::new, stub_pg_error_new)]
    fn equiv_len4() {
        check::<4>();
    }

    #[kani::proof]
    #[kani::unwind(10)]
    #[kani::stub(alloc::fmt::format, stub_format)]
    #[kani::stub(types_error::PgError::new, stub_pg_error_new)]
    fn equiv_len8() {
        check::<8>();
    }

    #[kani::proof]
    #[kani::unwind(16)]
    #[kani::stub(alloc::fmt::format, stub_format)]
    #[kani::stub(types_error::PgError::new, stub_pg_error_new)]
    fn equiv_len12() {
        check::<12>();
    }

    #[kani::proof]
    #[kani::unwind(12)]
    #[kani::stub(alloc::fmt::format, stub_format)]
    #[kani::stub(types_error::PgError::new, stub_pg_error_new)]
    fn equiv_digits_len8() {
        check_digits::<8>();
    }

    #[kani::proof]
    #[kani::unwind(9)]
    #[kani::stub(alloc::fmt::format, stub_format)]
    #[kani::stub(types_error::PgError::new, stub_pg_error_new)]
    fn equiv_digits_len6() {
        check_digits::<6>();
    }

    #[kani::proof]
    #[kani::unwind(14)]
    #[kani::stub(alloc::fmt::format, stub_format)]
    #[kani::stub(types_error::PgError::new, stub_pg_error_new)]
    fn equiv_digits_len11() {
        check_digits::<11>();
    }

    #[kani::proof]
    #[kani::unwind(8)]
    #[kani::stub(alloc::fmt::format, stub_format)]
    #[kani::stub(types_error::PgError::new, stub_pg_error_new)]
    fn equiv_len5() {
        check::<5>();
    }
}
