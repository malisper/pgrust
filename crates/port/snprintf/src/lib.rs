//! Idiomatic port of PostgreSQL's `src/port/snprintf.c` — the portable
//! `*printf` family (`pg_snprintf`, `pg_vsnprintf`, `pg_sprintf`, `pg_fprintf`,
//! `pg_printf`, and the nonstandard `pg_strfromd`).
//!
//! This is a 1:1 port of `dopr()` and its subroutines, but written in owned,
//! safe Rust:
//!
//! * No raw `*mut`/`c_void` pointers and no `extern "C"`.  The C original keeps
//!   a `PrintfTarget` that walks raw `char *` cursors into either a fixed buffer
//!   (snprintf) or a 1 KiB staging buffer flushed to a `FILE *` (fprintf).  We
//!   model the same two behaviours with a [`Target`] enum that either grows an
//!   owned `Vec<u8>` (the "infinite buffer" sprintf/snprintf compute the result
//!   length in, exactly as C does via `bufptr - bufstart + nchars`) or writes
//!   through a `std::io::Write` (fprintf).
//!
//! * The float and pointer conversions, which the C source delegates to the
//!   platform libc `snprintf("%e"/"%f"/"%g"/"%p", …)`, are reimplemented in pure
//!   Rust here (Rust's own float formatter + a `%g` style-selector that mirrors
//!   C99 `%g`).  No FFI.
//!
//! * `%m` expands to `strerror(errno)` for the errno captured at call entry.
//!   The idiomatic API takes that errno as an explicit parameter (the caller
//!   reads `errno` — we never touch the global), and renders the message via
//!   `std::io::Error::from_raw_os_error`, which is the platform `strerror`
//!   without any FFI of our own. Pure formatting otherwise needs no external
//!   dependency.
//!
//! ## Result semantics (C99, matching the C source's header comment)
//!
//! * The functions return the number of bytes that *would* have been written to
//!   an infinite buffer (excluding any trailing NUL), or `-1` on a format error.
//! * [`pg_snprintf_into`] additionally truncates into the caller's buffer and
//!   writes a trailing NUL (unless the buffer is empty), exactly like the C
//!   `pg_snprintf(str, count, …)`.
//!
//! Unimplemented features (same list as the C source): no locale radix, no `%n`,
//! no wide chars, no `long double`, and space/`#` flags are ignored.

#![allow(clippy::too_many_arguments)]

use std::ffi::CStr;
use std::fmt;
use std::io::{self, Write};

/// `#define PG_NL_ARGMAX 31` — the cap on `%n$` positional argument indices.
///
/// PostgreSQL deliberately uses a fixed small value rather than the platform's
/// `NL_ARGMAX`, to bound stack consumption and remove platform dependence.
pub const PG_NL_ARGMAX: usize = 31;

/// A single formatting argument.
///
/// The C variadic interface fetches each argument with `va_arg` at the type
/// demanded by the conversion specifier; here the caller supplies a typed value
/// up front and the conversion checks that the type is compatible (returning a
/// [`PrintfError::WrongArgumentType`] if not, which has no C analog — C has UB
/// instead, but a typed Rust API can report the mismatch).
///
/// `Ptr` carries the pointer *address* as a `usize` rather than a raw pointer,
/// so the idiomatic crate never holds `*mut`/`*const`.  `%p` renders it the way
/// glibc does (`0x` + lowercase hex, or `(nil)` for a null address).
#[derive(Clone, Copy, Debug)]
pub enum PrintfArg<'a> {
    Int(i64),
    UInt(u64),
    Float(f64),
    Str(&'a str),
    Bytes(&'a [u8]),
    CStr(&'a CStr),
    Char(u8),
    Ptr(usize),
    Null,
}

impl From<i32> for PrintfArg<'_> {
    fn from(value: i32) -> Self {
        Self::Int(value as i64)
    }
}

impl From<i64> for PrintfArg<'_> {
    fn from(value: i64) -> Self {
        Self::Int(value)
    }
}

impl From<u32> for PrintfArg<'_> {
    fn from(value: u32) -> Self {
        Self::UInt(value as u64)
    }
}

impl From<u64> for PrintfArg<'_> {
    fn from(value: u64) -> Self {
        Self::UInt(value)
    }
}

impl From<usize> for PrintfArg<'_> {
    fn from(value: usize) -> Self {
        Self::UInt(value as u64)
    }
}

impl From<f64> for PrintfArg<'_> {
    fn from(value: f64) -> Self {
        Self::Float(value)
    }
}

impl<'a> From<&'a str> for PrintfArg<'a> {
    fn from(value: &'a str) -> Self {
        Self::Str(value)
    }
}

impl<'a> From<&'a [u8]> for PrintfArg<'a> {
    fn from(value: &'a [u8]) -> Self {
        Self::Bytes(value)
    }
}

impl<'a> From<&'a CStr> for PrintfArg<'a> {
    fn from(value: &'a CStr) -> Self {
        Self::CStr(value)
    }
}

impl From<char> for PrintfArg<'_> {
    fn from(value: char) -> Self {
        Self::Char(value as u8)
    }
}

/// The type tags from `PrintfArgType` in the C source.  Used by
/// [`find_arguments`] to verify consistent `%n$` use and to know what each
/// physical argument is when collecting them in order.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ArgType {
    None,
    Int,
    Long,
    LongLong,
    Double,
    CharPtr,
}

/// Error result.  The C source signals failure by returning `-1` and setting
/// `errno = EINVAL` (a bad format string, or a stream write failure).  A typed
/// API surfaces the cause; the public functions map every variant onto the C
/// `-1` where they return an `int`.
#[derive(Debug, Clone, Eq, PartialEq)]
pub enum PrintfError {
    /// Bad format string (the C `bad_format:` / `EINVAL` path), or an
    /// inconsistent / out-of-range `%n$` use (`find_arguments` returning false).
    InvalidFormat,
    /// The caller supplied too few arguments for the format (C reads past the
    /// `va_list`, which is UB; we report it instead).
    MissingArgument(usize),
    /// The supplied argument's type does not match the conversion specifier.
    WrongArgumentType,
    /// A positional index `%n$` exceeded [`PG_NL_ARGMAX`].
    TooManyPositionalArguments,
    /// Growing the owned output buffer failed (out of memory).
    ///
    /// The C source has no analog: sprintf/snprintf write into a fixed caller
    /// buffer and never allocate.  The idiomatic port grows an owned `Vec`, so
    /// per the project allocation HARD RULE every data-derived growth reserves
    /// fallibly first and surfaces this recoverable error instead of aborting.
    OutOfMemory,
}

impl fmt::Display for PrintfError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidFormat => f.write_str("invalid printf format"),
            Self::MissingArgument(index) => write!(f, "missing printf argument {index}"),
            Self::WrongArgumentType => f.write_str("printf argument has the wrong type"),
            Self::TooManyPositionalArguments => {
                write!(f, "printf positional argument exceeds {PG_NL_ARGMAX}")
            }
            Self::OutOfMemory => f.write_str("out of memory formatting printf output"),
        }
    }
}

impl std::error::Error for PrintfError {}

// ---------------------------------------------------------------------------
// PrintfTarget: where the formatted output goes.
// ---------------------------------------------------------------------------

/// Output sink, mirroring the C `PrintfTarget`.
///
/// * [`Target::Buffer`] is the sprintf/snprintf case.  C writes into a caller
///   buffer and counts dropped overrun bytes in `nchars`; the *result* it
///   returns is `(bufptr - bufstart) + nchars`, i.e. the would-be length in an
///   infinite buffer.  We therefore just grow an owned `Vec<u8>` (the infinite
///   buffer); truncation into a fixed caller buffer is applied afterward by
///   [`pg_vsnprintf_into`].
///
/// * [`Target::Stream`] is the fprintf case: bytes go straight to a
///   `std::io::Write`.  `nchars` counts the bytes actually written; `failed`
///   records the first write error (preserving its errno, as C does by skipping
///   later writes once failed).
enum Target<'w> {
    Buffer {
        out: Vec<u8>,
        /// Set when a `try_reserve` on `out` failed; checked alongside the
        /// stream `failed` flag so dopr stops, the same way C stops on a stream
        /// write failure.
        oom: bool,
    },
    Stream {
        writer: &'w mut dyn Write,
        nchars: usize,
        failed: Option<io::Error>,
    },
}

impl Target<'_> {
    /// Append a run of `len` copies of byte `c` (`dopr_outchmulti`) or a slice
    /// (`dostr`); both reduce to writing a byte slice through this one path.
    ///
    /// Per the allocation HARD RULE the owned-buffer path reserves fallibly
    /// before growing on a data-derived size; a reservation failure sets `oom`
    /// and drops the bytes (the call ultimately returns
    /// [`PrintfError::OutOfMemory`]) rather than aborting.
    fn dostr(&mut self, bytes: &[u8]) {
        match self {
            Target::Buffer { out, oom } => {
                if *oom {
                    return;
                }
                if out.try_reserve(bytes.len()).is_err() {
                    *oom = true;
                    return;
                }
                out.extend_from_slice(bytes);
            }
            Target::Stream {
                writer,
                nchars,
                failed,
            } => {
                if failed.is_some() {
                    return;
                }
                match writer.write_all(bytes) {
                    Ok(()) => *nchars += bytes.len(),
                    Err(e) => *failed = Some(e),
                }
            }
        }
    }

    /// `dopr_outch`: emit a single byte.
    fn outch(&mut self, c: u8) {
        self.dostr(&[c]);
    }

    /// `dopr_outchmulti`: emit `len` copies of byte `c`.  `len <= 0` emits
    /// nothing, matching the C loop guard.
    fn outchmulti(&mut self, c: u8, len: i32) {
        if len <= 0 {
            return;
        }
        match self {
            Target::Buffer { out, oom } => {
                if *oom {
                    return;
                }
                let n = len as usize;
                // Reserve the whole run fallibly before growing (HARD RULE):
                // `len` is data-derived (a field width can be huge).
                if out.try_reserve(n).is_err() {
                    *oom = true;
                    return;
                }
                out.extend(std::iter::repeat(c).take(n));
            }
            Target::Stream { .. } => {
                // Small fixed staging chunk; the byte run is rarely large.
                let chunk = [c; 64];
                let mut remaining = len as usize;
                while remaining > 0 {
                    let n = remaining.min(chunk.len());
                    self.dostr(&chunk[..n]);
                    remaining -= n;
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Public entry points.
// ---------------------------------------------------------------------------

/// `pg_vsnprintf` / `pg_snprintf` core producing the full would-be output as an
/// owned byte vector.  Equivalent to formatting into an infinite buffer; the
/// returned length is `output.len()`.
///
/// `errno` is taken explicitly (the C source captures the global `errno` at
/// entry for `%m`); pass `0` when the format has no `%m`.
pub fn pg_vsnprintf(fmt: &str, args: &[PrintfArg<'_>], errno: i32) -> Result<Vec<u8>, PrintfError> {
    let mut target = Target::Buffer {
        out: Vec::new(),
        oom: false,
    };
    dopr(&mut target, fmt.as_bytes(), args, errno)?;
    match target {
        Target::Buffer { oom: true, .. } => Err(PrintfError::OutOfMemory),
        Target::Buffer { out, oom: false } => Ok(out),
        Target::Stream { .. } => unreachable!(),
    }
}

/// `pg_snprintf(str, count, fmt, …)`: format into a fixed caller buffer of size
/// `buf.len()`, NUL-terminate (unless empty), and return the would-be length
/// excluding the NUL — exactly the C contract (a result `>= buf.len()` means it
/// was truncated).
pub fn pg_snprintf_into(
    buf: &mut [u8],
    fmt: &str,
    args: &[PrintfArg<'_>],
    errno: i32,
) -> Result<usize, PrintfError> {
    let output = pg_vsnprintf(fmt, args, errno)?;
    write_truncated_c_string(buf, &output);
    Ok(output.len())
}

/// `pg_vsprintf` / `pg_sprintf`: the C source assumes the buffer is big enough
/// (`bufend == NULL`).  With an owned `Vec` there is no overrun to worry about;
/// this is the same as [`pg_vsnprintf`].
pub fn pg_sprintf(fmt: &str, args: &[PrintfArg<'_>], errno: i32) -> Result<Vec<u8>, PrintfError> {
    pg_vsnprintf(fmt, args, errno)
}

/// `pg_vfprintf` / `pg_fprintf`: format directly to a `std::io::Write` sink.
///
/// Returns the number of bytes written on success, or surfaces the first write
/// error (the C function returns `-1` and leaves the stream's errno).  A format
/// error maps to [`io::ErrorKind::InvalidInput`] (the C `EINVAL`).
pub fn pg_fprintf<W: Write>(
    writer: &mut W,
    fmt: &str,
    args: &[PrintfArg<'_>],
    errno: i32,
) -> io::Result<usize> {
    let mut target = Target::Stream {
        writer,
        nchars: 0,
        failed: None,
    };
    match dopr(&mut target, fmt.as_bytes(), args, errno) {
        Ok(()) => match target {
            Target::Stream {
                nchars,
                failed: None,
                ..
            } => Ok(nchars),
            Target::Stream {
                failed: Some(e), ..
            } => Err(e),
            Target::Buffer { .. } => unreachable!(),
        },
        Err(e) => Err(io::Error::new(io::ErrorKind::InvalidInput, e)),
    }
}

/// `pg_vprintf` / `pg_printf`: format to the process stdout.
pub fn pg_printf(fmt: &str, args: &[PrintfArg<'_>], errno: i32) -> io::Result<usize> {
    let mut stdout = io::stdout();
    pg_fprintf(&mut stdout, fmt, args, errno)
}

/// `pg_strfromd(str, count, precision, value)` (snprintf.c:1317): the nonstandard
/// fast path that float8out wants.  Behaves like `snprintf("%.*g", precision)`
/// but bounds `precision` to `1..=32` and requires a nonempty buffer.
///
/// Writes into `buf` (NUL-terminated like snprintf) and returns the would-be
/// length excluding the NUL.
pub fn pg_strfromd(buf: &mut [u8], precision: i32, value: f64) -> usize {
    debug_assert!(!buf.is_empty(), "pg_strfromd requires count > 0");

    // Inlined fmtfloat() logic, simplified knowing no padding is wanted.
    let precision = precision.clamp(1, 32);

    let mut out: Vec<u8> = Vec::new();
    let mut signvalue: Option<u8> = None;

    let convert: Vec<u8> = if value.is_nan() {
        b"NaN".to_vec()
    } else {
        let mut v = value;
        // "value < 0.0" is false for IEEE -0.0, so detect -0.0 via sign bit.
        if v < 0.0 || (v == 0.0 && v.is_sign_negative()) {
            signvalue = Some(b'-');
            v = -v;
        }
        if v.is_infinite() {
            b"Infinity".to_vec()
        } else {
            format_g_bytes(v, precision)
        }
    };

    if let Some(s) = signvalue {
        out.push(s);
    }
    out.extend_from_slice(&convert);

    write_truncated_c_string(buf, &out);
    out.len()
}

// ---------------------------------------------------------------------------
// dopr(): the guts of *printf for all cases.
// ---------------------------------------------------------------------------

/// Mirrors C `dopr()`.  Walks the format byte string, emitting literal runs and
/// dispatching each conversion spec.  `save_errno` is the value `errno` had at
/// the start of the call (used for `%m`).
fn dopr(
    target: &mut Target<'_>,
    format: &[u8],
    args: &[PrintfArg<'_>],
    save_errno: i32,
) -> Result<(), PrintfError> {
    // argvalues[i] (1-based) once a %n$ spec triggers find_arguments().
    let mut argvalues: Option<Vec<PrintfArg<'_>>> = None;
    let mut have_dollar = false;
    let mut next_arg = 0usize; // index into `args` for non-positional fetches
    let mut first_pct: Option<usize> = None;

    let mut i = 0usize;
    while i < format.len() {
        // Locate next conversion specifier.
        if format[i] != b'%' {
            // Scan to next '%' or end (strchrnul(format + 1, '%')).
            let start = i;
            i += 1;
            while i < format.len() && format[i] != b'%' {
                i += 1;
            }
            target.dostr(&format[start..i]);
            if i >= format.len() {
                break;
            }
            // fall through with format[i] == '%'
        }

        if first_pct.is_none() {
            first_pct = Some(i);
        }

        // Process conversion spec starting at format[i] == '%'.
        i += 1;

        // Fast path for exactly "%s".
        if format.get(i) == Some(&b's') && !have_dollar_lookahead_blocks_fast(format, i) {
            // The C fast path is unconditional `if (*format == 's')`; it fetches
            // the next non-positional arg.  But a "%s" with no flags can never be
            // positional, so this is safe even with %n$ elsewhere.
            i += 1;
            let arg = fetch_next(args, &mut next_arg)?;
            let s = string_bytes(arg)?;
            target.dostr(s);
            if target_failed(target) {
                break;
            }
            continue;
        }

        // Spec state, reset per conversion (matches the C locals).
        let mut fieldwidth: i32 = 0;
        let mut precision: i32 = 0;
        let mut zpad: u8 = 0;
        let mut leftjust = false;
        let mut forcesign = false;
        let mut longflag = false;
        let mut longlongflag = false;
        let mut pointflag = false;
        let mut fmtpos: usize = 0;
        let mut accum: i32 = 0;
        let mut have_star = false;
        let mut afterstar = false;

        // nextch2 loop.
        loop {
            let ch = match format.get(i) {
                Some(&c) => {
                    i += 1;
                    c
                }
                None => {
                    // '\0' / end-of-string in the middle of a spec is bogus.
                    return bad_format();
                }
            };

            match ch {
                b'-' => {
                    leftjust = true;
                    continue;
                }
                b'+' => {
                    forcesign = true;
                    continue;
                }
                b'0' => {
                    // set zero padding if no nonzero digits yet
                    if accum == 0 && !pointflag {
                        zpad = b'0';
                    }
                    accum = accum.wrapping_mul(10).wrapping_add((ch - b'0') as i32);
                    continue;
                }
                b'1'..=b'9' => {
                    accum = accum.wrapping_mul(10).wrapping_add((ch - b'0') as i32);
                    continue;
                }
                b'.' => {
                    if have_star {
                        have_star = false;
                    } else {
                        fieldwidth = accum;
                    }
                    pointflag = true;
                    accum = 0;
                    continue;
                }
                b'*' => {
                    if have_dollar {
                        // process value after reading n$
                        afterstar = true;
                    } else {
                        // fetch and process value now
                        let starval = star_value(fetch_next(args, &mut next_arg)?)?;
                        if pointflag {
                            precision = starval;
                            if precision < 0 {
                                precision = 0;
                                pointflag = false;
                            }
                        } else {
                            fieldwidth = starval;
                            if fieldwidth < 0 {
                                leftjust = true;
                                fieldwidth = -fieldwidth;
                            }
                        }
                    }
                    have_star = true;
                    accum = 0;
                    continue;
                }
                b'$' => {
                    // First dollar sign?
                    if !have_dollar {
                        let collected = find_arguments(
                            &format[first_pct.ok_or(PrintfError::InvalidFormat)?..],
                            args,
                        )?;
                        argvalues = Some(collected);
                        have_dollar = true;
                    }
                    if afterstar {
                        // fetch and process star value from argvalues[accum]
                        let av = argvalues.as_ref().ok_or(PrintfError::InvalidFormat)?;
                        let starval = star_value(arg_at(av, accum)?)?;
                        if pointflag {
                            precision = starval;
                            if precision < 0 {
                                precision = 0;
                                pointflag = false;
                            }
                        } else {
                            fieldwidth = starval;
                            if fieldwidth < 0 {
                                leftjust = true;
                                fieldwidth = -fieldwidth;
                            }
                        }
                        afterstar = false;
                    } else {
                        fmtpos = position_from_accum(accum)?;
                    }
                    accum = 0;
                    continue;
                }
                b'l' => {
                    if longflag {
                        longlongflag = true;
                    } else {
                        longflag = true;
                    }
                    continue;
                }
                b'z' => {
                    // SIZEOF_SIZE_T == SIZEOF_LONG on LP64/ILP32 targets PG runs.
                    longflag = true;
                    continue;
                }
                b'h' | b'\'' => {
                    // ignore these
                    continue;
                }
                b'd' | b'i' => {
                    if !have_star {
                        if pointflag {
                            precision = accum;
                        } else {
                            fieldwidth = accum;
                        }
                    }
                    let arg = resolve_value(
                        have_dollar,
                        argvalues.as_deref(),
                        fmtpos,
                        args,
                        &mut next_arg,
                    )?;
                    let numvalue = signed_value(arg, longflag, longlongflag)?;
                    fmtint(
                        target, numvalue, ch, forcesign, leftjust, fieldwidth, zpad, precision,
                        pointflag,
                    );
                    break;
                }
                b'o' | b'u' | b'x' | b'X' => {
                    if !have_star {
                        if pointflag {
                            precision = accum;
                        } else {
                            fieldwidth = accum;
                        }
                    }
                    let arg = resolve_value(
                        have_dollar,
                        argvalues.as_deref(),
                        fmtpos,
                        args,
                        &mut next_arg,
                    )?;
                    let uvalue = unsigned_value(arg, longflag, longlongflag)?;
                    fmtuint(
                        target, uvalue, ch, forcesign, leftjust, fieldwidth, zpad, precision,
                        pointflag,
                    );
                    break;
                }
                b'c' => {
                    // C sets precision/fieldwidth from accum here too, but
                    // fmtchar() ignores precision; the assignment is dead in
                    // both the C source and here (kept for structural parity).
                    #[allow(unused_assignments)]
                    if !have_star {
                        if pointflag {
                            precision = accum;
                        } else {
                            fieldwidth = accum;
                        }
                    }
                    let arg = resolve_value(
                        have_dollar,
                        argvalues.as_deref(),
                        fmtpos,
                        args,
                        &mut next_arg,
                    )?;
                    let cvalue = char_value(arg)?;
                    fmtchar(target, cvalue, leftjust, fieldwidth);
                    break;
                }
                b's' => {
                    if !have_star {
                        if pointflag {
                            precision = accum;
                        } else {
                            fieldwidth = accum;
                        }
                    }
                    let arg = resolve_value(
                        have_dollar,
                        argvalues.as_deref(),
                        fmtpos,
                        args,
                        &mut next_arg,
                    )?;
                    let value = string_bytes(arg)?;
                    fmtstr(target, value, leftjust, fieldwidth, precision, pointflag);
                    break;
                }
                b'p' => {
                    // fieldwidth/leftjust are ignored
                    let arg = resolve_value(
                        have_dollar,
                        argvalues.as_deref(),
                        fmtpos,
                        args,
                        &mut next_arg,
                    )?;
                    let value = pointer_value(arg)?;
                    fmtptr(target, value);
                    break;
                }
                b'e' | b'E' | b'f' | b'g' | b'G' => {
                    if !have_star {
                        if pointflag {
                            precision = accum;
                        } else {
                            fieldwidth = accum;
                        }
                    }
                    let arg = resolve_value(
                        have_dollar,
                        argvalues.as_deref(),
                        fmtpos,
                        args,
                        &mut next_arg,
                    )?;
                    let fvalue = float_value(arg)?;
                    fmtfloat(
                        target, fvalue, ch, forcesign, leftjust, fieldwidth, zpad, precision,
                        pointflag,
                    );
                    break;
                }
                b'm' => {
                    let errm = pg_strerror(save_errno);
                    target.dostr(errm.as_bytes());
                    break;
                }
                b'%' => {
                    target.outch(b'%');
                    break;
                }
                _ => {
                    // Anything else --- including '\0' --- is bogus.
                    return bad_format();
                }
            }
        }

        // Check for failure after each conversion spec.
        if target_failed(target) {
            break;
        }
    }

    Ok(())
}

/// The C "%s" fast path is taken unconditionally; this helper exists only to
/// keep the fast path from firing for `%s` written as part of a positional spec
/// like `%2$s` (where `format[i]` would be a digit, not `s`).  In practice
/// `format[i] == 's'` already excludes that; this is always `false`.
#[inline]
fn have_dollar_lookahead_blocks_fast(_format: &[u8], _i: usize) -> bool {
    false
}

/// Returns true once output has failed: a stream write error, or an owned
/// buffer reservation failure (OOM).  Mirrors C's `target->failed` check, which
/// dopr consults after every conversion spec.
fn target_failed(target: &Target<'_>) -> bool {
    matches!(
        target,
        Target::Stream {
            failed: Some(_),
            ..
        } | Target::Buffer { oom: true, .. }
    )
}

fn bad_format() -> Result<(), PrintfError> {
    Err(PrintfError::InvalidFormat)
}

/// Fetch the next non-positional argument (`va_arg`), advancing the cursor.
fn fetch_next<'a>(
    args: &'a [PrintfArg<'a>],
    next_arg: &mut usize,
) -> Result<PrintfArg<'a>, PrintfError> {
    let idx = *next_arg;
    *next_arg += 1;
    args.get(idx)
        .copied()
        .ok_or(PrintfError::MissingArgument(idx + 1))
}

/// Resolve a conversion's value argument: from `argvalues[fmtpos]` when we are
/// in `%n$` mode, else the next positional `va_arg`.
fn resolve_value<'a>(
    have_dollar: bool,
    argvalues: Option<&'a [PrintfArg<'a>]>,
    fmtpos: usize,
    args: &'a [PrintfArg<'a>],
    next_arg: &mut usize,
) -> Result<PrintfArg<'a>, PrintfError> {
    if have_dollar {
        let av = argvalues.ok_or(PrintfError::InvalidFormat)?;
        arg_at(av, fmtpos as i32)
    } else {
        fetch_next(args, next_arg)
    }
}

/// `argvalues[accum]` access with 1-based index `accum` (C indexes from 1; the
/// `Vec` we build is also 1-based, slot 0 unused).
fn arg_at<'a>(argvalues: &'a [PrintfArg<'a>], accum: i32) -> Result<PrintfArg<'a>, PrintfError> {
    if accum <= 0 {
        return Err(PrintfError::InvalidFormat);
    }
    argvalues
        .get(accum as usize)
        .copied()
        .ok_or(PrintfError::MissingArgument(accum as usize))
}

/// Validate and convert an `%n$` accum into a 1-based `fmtpos`.
fn position_from_accum(accum: i32) -> Result<usize, PrintfError> {
    if accum <= 0 {
        return Err(PrintfError::InvalidFormat);
    }
    if accum as usize > PG_NL_ARGMAX {
        return Err(PrintfError::TooManyPositionalArguments);
    }
    Ok(accum as usize)
}

/// Extract an `int`-typed star (width/precision) value.
fn star_value(arg: PrintfArg<'_>) -> Result<i32, PrintfError> {
    match arg {
        PrintfArg::Int(v) => Ok(v as i32),
        PrintfArg::UInt(v) => Ok(v as i32),
        PrintfArg::Char(v) => Ok(v as i32),
        _ => Err(PrintfError::WrongArgumentType),
    }
}

// ---------------------------------------------------------------------------
// find_arguments(): sort out the arguments for a format spec with %n$.
// ---------------------------------------------------------------------------

/// Mirrors C `find_arguments()`.  Scans the format (starting at the first `%`)
/// to learn the type of every `%n$` / `*n$` argument, verifies consistency, then
/// collects the physical arguments in index order into a 1-based vector.
///
/// In C the physical arguments come from the `va_list`; here they come from the
/// caller's flat `args` slice in order (since we rejected any non-dollar specs
/// that would have consumed arguments first, `dopr` has not advanced its cursor
/// yet — the physical order is just `args[0], args[1], …]`).
fn find_arguments<'a>(
    format: &[u8],
    args: &'a [PrintfArg<'a>],
) -> Result<Vec<PrintfArg<'a>>, PrintfError> {
    let mut argtypes = [ArgType::None; PG_NL_ARGMAX + 1];
    let mut last_dollar: usize = 0;

    let mut i = 0usize;
    while i < format.len() {
        // Locate next conversion specifier.
        if format[i] != b'%' {
            match memchr(&format[i + 1..], b'%') {
                Some(off) => i = i + 1 + off,
                None => break,
            }
        }

        // Process conversion spec starting at format[i] == '%'.
        i += 1;
        let mut longflag = false;
        let mut longlongflag = false;
        let mut fmtpos: usize = 0;
        let mut accum: i32 = 0;
        let mut afterstar = false;

        // nextch1 loop.
        loop {
            let ch = match format.get(i) {
                Some(&c) => {
                    i += 1;
                    c
                }
                None => return Err(PrintfError::InvalidFormat),
            };

            match ch {
                b'-' | b'+' => continue,
                b'0'..=b'9' => {
                    accum = accum.wrapping_mul(10).wrapping_add((ch - b'0') as i32);
                    continue;
                }
                b'.' => {
                    accum = 0;
                    continue;
                }
                b'*' => {
                    if afterstar {
                        return Err(PrintfError::InvalidFormat); // previous star missing dollar
                    }
                    afterstar = true;
                    accum = 0;
                    continue;
                }
                b'$' => {
                    if accum <= 0 || accum as usize > PG_NL_ARGMAX {
                        return Err(PrintfError::InvalidFormat);
                    }
                    let idx = accum as usize;
                    if afterstar {
                        if argtypes[idx] != ArgType::None && argtypes[idx] != ArgType::Int {
                            return Err(PrintfError::InvalidFormat);
                        }
                        argtypes[idx] = ArgType::Int;
                        last_dollar = last_dollar.max(idx);
                        afterstar = false;
                    } else {
                        fmtpos = idx;
                    }
                    accum = 0;
                    continue;
                }
                b'l' => {
                    if longflag {
                        longlongflag = true;
                    } else {
                        longflag = true;
                    }
                    continue;
                }
                b'z' => {
                    longflag = true;
                    continue;
                }
                b'h' | b'\'' => continue,
                b'd' | b'i' | b'o' | b'u' | b'x' | b'X' => {
                    if fmtpos != 0 {
                        let atype = if longlongflag {
                            ArgType::LongLong
                        } else if longflag {
                            ArgType::Long
                        } else {
                            ArgType::Int
                        };
                        if argtypes[fmtpos] != ArgType::None && argtypes[fmtpos] != atype {
                            return Err(PrintfError::InvalidFormat);
                        }
                        argtypes[fmtpos] = atype;
                        last_dollar = last_dollar.max(fmtpos);
                    } else {
                        return Err(PrintfError::InvalidFormat); // non-dollar conversion spec
                    }
                    break;
                }
                b'c' => {
                    if fmtpos != 0 {
                        if argtypes[fmtpos] != ArgType::None && argtypes[fmtpos] != ArgType::Int {
                            return Err(PrintfError::InvalidFormat);
                        }
                        argtypes[fmtpos] = ArgType::Int;
                        last_dollar = last_dollar.max(fmtpos);
                    } else {
                        return Err(PrintfError::InvalidFormat);
                    }
                    break;
                }
                b's' | b'p' => {
                    if fmtpos != 0 {
                        if argtypes[fmtpos] != ArgType::None && argtypes[fmtpos] != ArgType::CharPtr
                        {
                            return Err(PrintfError::InvalidFormat);
                        }
                        argtypes[fmtpos] = ArgType::CharPtr;
                        last_dollar = last_dollar.max(fmtpos);
                    } else {
                        return Err(PrintfError::InvalidFormat);
                    }
                    break;
                }
                b'e' | b'E' | b'f' | b'g' | b'G' => {
                    if fmtpos != 0 {
                        if argtypes[fmtpos] != ArgType::None && argtypes[fmtpos] != ArgType::Double {
                            return Err(PrintfError::InvalidFormat);
                        }
                        argtypes[fmtpos] = ArgType::Double;
                        last_dollar = last_dollar.max(fmtpos);
                    } else {
                        return Err(PrintfError::InvalidFormat);
                    }
                    break;
                }
                b'm' | b'%' => break,
                _ => return Err(PrintfError::InvalidFormat), // bogus format string
            }
        }

        // A leftover afterstar means a non-dollar star (no n$ followed it).
        if afterstar {
            return Err(PrintfError::InvalidFormat);
        }
    }

    // Collect the arguments in physical (index) order.  The C source reads them
    // from the va_list with the right `va_arg(type)`; here the value is already
    // typed, so we just validate that the supplied type matches what the format
    // demanded and place it at its 1-based slot.
    let mut argvalues = vec![PrintfArg::Null; last_dollar + 1];
    for (i, slot) in argvalues.iter_mut().enumerate().take(last_dollar + 1).skip(1) {
        let provided = args
            .get(i - 1)
            .copied()
            .ok_or(PrintfError::MissingArgument(i))?;
        match argtypes[i] {
            ArgType::None => return Err(PrintfError::InvalidFormat),
            ArgType::Int | ArgType::Long | ArgType::LongLong => match provided {
                PrintfArg::Int(_) | PrintfArg::UInt(_) | PrintfArg::Char(_) => {}
                _ => return Err(PrintfError::WrongArgumentType),
            },
            ArgType::Double => {
                if !matches!(provided, PrintfArg::Float(_)) {
                    return Err(PrintfError::WrongArgumentType);
                }
            }
            ArgType::CharPtr => match provided {
                PrintfArg::Str(_)
                | PrintfArg::Bytes(_)
                | PrintfArg::CStr(_)
                | PrintfArg::Ptr(_)
                | PrintfArg::Null => {}
                _ => return Err(PrintfError::WrongArgumentType),
            },
        }
        *slot = provided;
    }

    Ok(argvalues)
}

/// `memchr` equivalent for the `strchr(format + 1, '%')` scan.
fn memchr(haystack: &[u8], needle: u8) -> Option<usize> {
    haystack.iter().position(|&b| b == needle)
}

// ---------------------------------------------------------------------------
// Argument value coercions (the va_arg type fetches in dopr()).
// ---------------------------------------------------------------------------

/// Signed integer fetch for `%d`/`%i`, applying the C width rules: a `default`
/// argument is read as `int` (32-bit, sign extended), `l` as `long`, `ll` as
/// `long long`.  `z` maps to `long` (see dopr).
fn signed_value(
    arg: PrintfArg<'_>,
    longflag: bool,
    longlongflag: bool,
) -> Result<i64, PrintfError> {
    let value = match arg {
        PrintfArg::Int(v) => v,
        PrintfArg::UInt(v) => v as i64,
        PrintfArg::Char(v) => v as i64,
        _ => return Err(PrintfError::WrongArgumentType),
    };
    Ok(if longlongflag {
        value
    } else if longflag {
        value as i64 // long == i64 on LP64
    } else {
        value as i32 as i64
    })
}

/// Unsigned integer fetch for `%o`/`%u`/`%x`/`%X`, applying the same width rules
/// as `signed_value` but truncating to the unsigned width (the C casts
/// `(unsigned int)`, `(unsigned long)`, `(unsigned long long)`).  Returned as
/// the wider `u64` (then fmtuint divides it down with the right base).
fn unsigned_value(
    arg: PrintfArg<'_>,
    longflag: bool,
    longlongflag: bool,
) -> Result<u64, PrintfError> {
    let value = match arg {
        PrintfArg::Int(v) => v as u64,
        PrintfArg::UInt(v) => v,
        PrintfArg::Char(v) => v as u64,
        _ => return Err(PrintfError::WrongArgumentType),
    };
    Ok(if longlongflag {
        value
    } else if longflag {
        value // unsigned long == u64 on LP64
    } else {
        value as u32 as u64
    })
}

fn char_value(arg: PrintfArg<'_>) -> Result<u8, PrintfError> {
    match arg {
        PrintfArg::Char(v) => Ok(v),
        // C casts the int va_arg to `(unsigned char)`.
        PrintfArg::Int(v) => Ok(v as u8),
        PrintfArg::UInt(v) => Ok(v as u8),
        _ => Err(PrintfError::WrongArgumentType),
    }
}

fn string_bytes(arg: PrintfArg<'_>) -> Result<&[u8], PrintfError> {
    match arg {
        PrintfArg::Str(v) => Ok(v.as_bytes()),
        PrintfArg::Bytes(v) => Ok(v),
        PrintfArg::CStr(v) => Ok(v.to_bytes()),
        // C silently substitutes "(null)" for a NULL char *.
        PrintfArg::Null => Ok(b"(null)"),
        _ => Err(PrintfError::WrongArgumentType),
    }
}

fn pointer_value(arg: PrintfArg<'_>) -> Result<usize, PrintfError> {
    match arg {
        PrintfArg::Ptr(v) => Ok(v),
        PrintfArg::Null => Ok(0),
        _ => Err(PrintfError::WrongArgumentType),
    }
}

fn float_value(arg: PrintfArg<'_>) -> Result<f64, PrintfError> {
    match arg {
        PrintfArg::Float(v) => Ok(v),
        _ => Err(PrintfError::WrongArgumentType),
    }
}

// ---------------------------------------------------------------------------
// Conversion formatters (fmtstr/fmtptr/fmtint/fmtchar/fmtfloat).
// ---------------------------------------------------------------------------

/// `fmtstr()`: emit a string with optional precision (max length) and width pad.
fn fmtstr(
    target: &mut Target<'_>,
    value: &[u8],
    leftjust: bool,
    minlen: i32,
    maxwidth: i32,
    pointflag: bool,
) {
    // strnlen(value, maxwidth) when a precision was given.
    let vallen = if pointflag {
        strnlen(value, maxwidth.max(0) as usize)
    } else {
        value.len()
    };

    let mut padlen = compute_padlen(minlen, vallen as i32, leftjust);

    if padlen > 0 {
        target.outchmulti(b' ', padlen);
        padlen = 0;
    }

    target.dostr(&value[..vallen]);

    trailing_pad(target, padlen);
}

/// `fmtptr()`: render a pointer the way glibc's `%p` does — `0x` + lowercase hex
/// of the address, or `(nil)` when the address is null.  The C source delegates
/// this to the platform libc; we reproduce the common (glibc/macOS) spelling.
fn fmtptr(target: &mut Target<'_>, value: usize) {
    if value == 0 {
        target.dostr(b"(nil)");
        return;
    }
    let mut buf = [0u8; 2 + 16]; // "0x" + up to 16 hex digits for a 64-bit addr
    buf[0] = b'0';
    buf[1] = b'x';
    // Write hex digits into a temporary, then copy the significant run.
    let digits = {
        let mut tmp = [0u8; 16];
        let mut n = value;
        let mut idx = tmp.len();
        loop {
            idx -= 1;
            tmp[idx] = b"0123456789abcdef"[(n & 0xf) as usize];
            n >>= 4;
            if n == 0 {
                break;
            }
        }
        let len = tmp.len() - idx;
        buf[2..2 + len].copy_from_slice(&tmp[idx..]);
        2 + len
    };
    target.dostr(&buf[..digits]);
}

/// `fmtint()`: signed integer conversion (`%d`/`%i`).  The unsigned conversions
/// route through [`fmtuint`]; both share the digit emission + padding logic.
fn fmtint(
    target: &mut Target<'_>,
    value: i64,
    type_: u8,
    forcesign: bool,
    leftjust: bool,
    minlen: i32,
    zpad: u8,
    precision: i32,
    pointflag: bool,
) {
    // dosign && (value < 0) => signvalue '-', magnitude = -(unsigned)value.
    let (signvalue, uvalue) = if value < 0 {
        (Some(b'-'), (value as u64).wrapping_neg())
    } else if forcesign {
        (Some(b'+'), value as u64)
    } else {
        (None, value as u64)
    };
    fmt_integer(
        target, uvalue, 10, false, signvalue, value == 0, leftjust, minlen, zpad, precision,
        pointflag,
    );
    let _ = type_;
}

/// `fmtint()` for the unsigned types (`%o`/`%u`/`%x`/`%X`): no sign handling.
fn fmtuint(
    target: &mut Target<'_>,
    value: u64,
    type_: u8,
    _forcesign: bool,
    leftjust: bool,
    minlen: i32,
    zpad: u8,
    precision: i32,
    pointflag: bool,
) {
    let (base, uppercase) = match type_ {
        b'o' => (8u64, false),
        b'u' => (10, false),
        b'x' => (16, false),
        b'X' => (16, true),
        _ => return,
    };
    fmt_integer(
        target,
        value,
        base,
        uppercase,
        None,
        value == 0,
        leftjust,
        minlen,
        zpad,
        precision,
        pointflag,
    );
}

/// Shared integer body: emit `uvalue` in `base`, with the precision/zero-pad/
/// width/sign rules of the C `fmtint()`.
fn fmt_integer(
    target: &mut Target<'_>,
    uvalue: u64,
    base: u64,
    uppercase: bool,
    signvalue: Option<u8>,
    is_zero: bool,
    leftjust: bool,
    minlen: i32,
    zpad: u8,
    precision: i32,
    pointflag: bool,
) {
    let cvt: &[u8; 16] = if uppercase {
        b"0123456789ABCDEF"
    } else {
        b"0123456789abcdef"
    };

    // SUS: converting 0 with explicit precision 0 yields no characters.
    let digits: Vec<u8> = if is_zero && pointflag && precision == 0 {
        Vec::new()
    } else {
        let mut tmp = Vec::new();
        let mut v = uvalue;
        loop {
            tmp.push(cvt[(v % base) as usize]);
            v /= base;
            if v == 0 {
                break;
            }
        }
        tmp.reverse();
        tmp
    };

    let vallen = digits.len() as i32;
    let zeropad = (precision - vallen).max(0);
    let mut padlen = compute_padlen(minlen, vallen + zeropad, leftjust);

    leading_pad(target, zpad, signvalue, &mut padlen);

    if zeropad > 0 {
        target.outchmulti(b'0', zeropad);
    }

    target.dostr(&digits);

    trailing_pad(target, padlen);
}

/// `fmtchar()`: emit one byte with width padding.
fn fmtchar(target: &mut Target<'_>, value: u8, leftjust: bool, minlen: i32) {
    let mut padlen = compute_padlen(minlen, 1, leftjust);
    if padlen > 0 {
        target.outchmulti(b' ', padlen);
        padlen = 0;
    }
    target.outch(value);
    trailing_pad(target, padlen);
}

/// `fmtfloat()`: `%e`/`%E`/`%f`/`%g`/`%G` conversion with width/precision/sign
/// and the over-350-digit zero-pad trick.  Infinity/NaN are spelled
/// platform-independently ("Infinity"/"NaN"), matching the C source.
fn fmtfloat(
    target: &mut Target<'_>,
    mut value: f64,
    type_: u8,
    forcesign: bool,
    leftjust: bool,
    minlen: i32,
    zpad: u8,
    precision: i32,
    pointflag: bool,
) {
    // cover possible overflow of "accum"
    let precision = precision.max(0);
    let prec = precision.min(350);

    let mut signvalue: Option<u8> = None;
    let mut zeropadlen = 0i32;
    let mut is_special = false;

    let convert: Vec<u8> = if value.is_nan() {
        is_special = true;
        b"NaN".to_vec()
    } else {
        // Sign (NaNs excluded above).  "< 0.0" is false for IEEE -0.0, so we
        // detect -0.0 via its sign bit (the C memcmp-against-dzero trick).
        if value < 0.0 || (value == 0.0 && value.is_sign_negative()) {
            signvalue = Some(b'-');
            value = -value;
        } else if forcesign {
            signvalue = Some(b'+');
        }

        if value.is_infinite() {
            is_special = true;
            b"Infinity".to_vec()
        } else if pointflag {
            zeropadlen = precision - prec;
            float_convert(type_, value, Some(prec))
        } else {
            // Default precision: C passes no precision, libc defaults to 6.
            float_convert(type_, value, None)
        }
    };

    let vallen = convert.len() as i32;
    let mut padlen = compute_padlen(minlen, vallen + zeropadlen, leftjust);

    // Specials never get zero padding regardless of the zpad flag.
    let effective_zpad = if is_special { 0 } else { zpad };

    leading_pad(target, effective_zpad, signvalue, &mut padlen);

    if zeropadlen > 0 {
        // If 'e'/'E' format, inject zeroes before the exponent.
        let epos = convert
            .iter()
            .rposition(|&b| b == b'e')
            .or_else(|| convert.iter().rposition(|&b| b == b'E'));
        if let Some(epos) = epos {
            target.dostr(&convert[..epos]);
            target.outchmulti(b'0', zeropadlen);
            target.dostr(&convert[epos..]);
        } else {
            target.dostr(&convert);
            target.outchmulti(b'0', zeropadlen);
        }
    } else {
        target.dostr(&convert);
    }

    trailing_pad(target, padlen);
}

// ---------------------------------------------------------------------------
// Pure-Rust float rendering (the libc snprintf("%e"/"%f"/"%g") delegation).
// ---------------------------------------------------------------------------

/// Render a finite, non-negative `value` for conversion `type_` with the given
/// precision (None => the C default of 6).  This reproduces C `printf`'s
/// `%e`/`%f`/`%g` (and uppercase variants) in pure Rust.
fn float_convert(type_: u8, value: f64, precision: Option<i32>) -> Vec<u8> {
    let prec = precision.unwrap_or(6).max(0);
    match type_ {
        b'f' => format_f_bytes(value, prec),
        b'e' => format_e_bytes(value, prec, false),
        b'E' => format_e_bytes(value, prec, true),
        b'g' => format_g_bytes(value, if prec == 0 { 1 } else { prec }),
        b'G' => to_upper_e(format_g_bytes(value, if prec == 0 { 1 } else { prec })),
        _ => Vec::new(),
    }
}

/// Uppercase the exponent marker `e` -> `E` for `%G` output of `format_g_bytes`.
fn to_upper_e(mut bytes: Vec<u8>) -> Vec<u8> {
    for b in bytes.iter_mut() {
        if *b == b'e' {
            *b = b'E';
        }
    }
    bytes
}

/// C `%f`: fixed notation with exactly `prec` fractional digits, round-half-to
/// -even on the true binary value (Rust's `{:.*}` does exactly this).
fn format_f_bytes(value: f64, prec: i32) -> Vec<u8> {
    format!("{:.*}", prec as usize, value).into_bytes()
}

/// C `%e`/`%E`: scientific notation with `prec` fractional digits and an
/// exponent of at least two digits, sign always present.  Rust's `{:e}` differs
/// (no leading zero / sign on the exponent), so we reformat the exponent.
fn format_e_bytes(value: f64, prec: i32, upper: bool) -> Vec<u8> {
    let s = format!("{:.*e}", prec as usize, value);
    canonicalize_exponent(&s, upper)
}

/// Rewrite a Rust `{:e}` rendering (`<mant>e<exp>`) into C `%e` form: marker
/// `e`/`E`, an explicit sign, and at least two exponent digits.
fn canonicalize_exponent(s: &str, upper: bool) -> Vec<u8> {
    let (mant, exp_str) = match s.split_once('e') {
        Some(parts) => parts,
        None => return s.as_bytes().to_vec(),
    };
    let exp: i32 = exp_str.parse().unwrap_or(0);
    let mut out = Vec::with_capacity(mant.len() + 5);
    out.extend_from_slice(mant.as_bytes());
    out.push(if upper { b'E' } else { b'e' });
    out.push(if exp < 0 { b'-' } else { b'+' });
    let mag = exp.unsigned_abs();
    let mag_str = mag.to_string();
    if mag_str.len() < 2 {
        out.push(b'0');
    }
    out.extend_from_slice(mag_str.as_bytes());
    out
}

/// C `%g`: `prec` significant digits (prec==0 treated as 1), trailing zeros
/// stripped, `%e` style chosen iff the decimal exponent of the leading digit is
/// `< -4 || >= prec`, the `%e` exponent rendered with at least two digits.
///
/// This mirrors the proven idiomatic `%.*g` renderer in
/// `backend-utils-adt-float`, extended to handle negative inputs and a
/// nonzero-but-fractional value (snprintf is also called for the `pg_strfromd`
/// path).  Input is assumed finite (specials handled by the caller).
fn format_g_bytes(val: f64, prec: i32) -> Vec<u8> {
    let prec = if prec <= 0 { 1 } else { prec };

    if val == 0.0 {
        return if val.is_sign_negative() {
            b"-0".to_vec()
        } else {
            b"0".to_vec()
        };
    }

    let neg = val < 0.0;
    let a = val.abs();

    // {:e} with prec-1 fractional digits gives `prec` significant digits.
    let sci = format!("{:.*e}", (prec - 1) as usize, a);
    let (mant, exp_str) = sci.split_once('e').expect("scientific format has 'e'");
    let exp: i32 = exp_str.parse().expect("exponent is an integer");
    let digits: String = mant.chars().filter(|c| *c != '.').collect();

    let mut out: Vec<u8> = Vec::new();
    if neg {
        out.push(b'-');
    }

    if exp < -4 || exp >= prec {
        let frac = trim_trailing_zeros(&digits[1..]);
        out.extend_from_slice(digits[..1].as_bytes());
        if !frac.is_empty() {
            out.push(b'.');
            out.extend_from_slice(frac.as_bytes());
        }
        out.push(b'e');
        out.push(if exp < 0 { b'-' } else { b'+' });
        let mag = exp.unsigned_abs();
        let mag_str = mag.to_string();
        if mag_str.len() < 2 {
            out.push(b'0');
        }
        out.extend_from_slice(mag_str.as_bytes());
    } else if exp >= 0 {
        let intlen = (exp + 1) as usize;
        if intlen >= digits.len() {
            out.extend_from_slice(digits.as_bytes());
            for _ in 0..(intlen - digits.len()) {
                out.push(b'0');
            }
        } else {
            let (int_part, frac_part) = digits.split_at(intlen);
            let frac = trim_trailing_zeros(frac_part);
            out.extend_from_slice(int_part.as_bytes());
            if !frac.is_empty() {
                out.push(b'.');
                out.extend_from_slice(frac.as_bytes());
            }
        }
    } else {
        out.extend_from_slice(b"0.");
        for _ in 0..(-exp - 1) {
            out.push(b'0');
        }
        out.extend_from_slice(trim_trailing_zeros(&digits).as_bytes());
    }

    out
}

fn trim_trailing_zeros(s: &str) -> &str {
    s.trim_end_matches('0')
}

// ---------------------------------------------------------------------------
// Padding helpers (adjust_sign/compute_padlen/leading_pad/trailing_pad).
// ---------------------------------------------------------------------------

/// `compute_padlen()`: amount to pad to reach `minlen`; negative result means
/// left-justified (pad goes on the trailing side).
fn compute_padlen(minlen: i32, vallen: i32, leftjust: bool) -> i32 {
    let mut padlen = minlen - vallen;
    if padlen < 0 {
        padlen = 0;
    }
    if leftjust {
        padlen = -padlen;
    }
    padlen
}

/// `leading_pad()`: emit the leading sign and field padding.  Mirrors the exact
/// sequence in the C source: when zero-padding, the sign comes first and the
/// zeros fill the rest; otherwise spaces lead, then the sign.
fn leading_pad(target: &mut Target<'_>, zpad: u8, signvalue: Option<u8>, padlen: &mut i32) {
    let mut signvalue = signvalue;

    if *padlen > 0 && zpad != 0 {
        if let Some(sv) = signvalue {
            target.outch(sv);
            *padlen -= 1;
            signvalue = None;
        }
        if *padlen > 0 {
            target.outchmulti(zpad, *padlen);
            *padlen = 0;
        }
    }

    let maxpad: i32 = i32::from(signvalue.is_some());
    if *padlen > maxpad {
        target.outchmulti(b' ', *padlen - maxpad);
        *padlen = maxpad;
    }
    if let Some(sv) = signvalue {
        target.outch(sv);
        if *padlen > 0 {
            *padlen -= 1;
        } else if *padlen < 0 {
            *padlen += 1;
        }
    }
}

/// `trailing_pad()`: emit `-padlen` spaces when left-justified.
fn trailing_pad(target: &mut Target<'_>, padlen: i32) {
    if padlen < 0 {
        target.outchmulti(b' ', -padlen);
    }
}

// ---------------------------------------------------------------------------
// Misc helpers.
// ---------------------------------------------------------------------------

/// `strnlen(s, maxlen)`.
fn strnlen(s: &[u8], maxlen: usize) -> usize {
    s.len().min(maxlen)
}

/// snprintf truncation + NUL-termination into a fixed caller buffer.
fn write_truncated_c_string(buf: &mut [u8], output: &[u8]) {
    if buf.is_empty() {
        return;
    }
    let copy_len = output.len().min(buf.len() - 1);
    buf[..copy_len].copy_from_slice(&output[..copy_len]);
    buf[copy_len] = 0;
}

/// `%m` expansion: the platform message for `errno`.
///
/// The C source calls `strerror_r(save_errno, …)`.  We use the platform
/// `strerror` via `std::io::Error::from_raw_os_error`, which has no FFI of our
/// own and is the idiomatic strerror wrapper.  An unknown errno yields the
/// generic "Unknown error N" / "Operation not permitted"-style text the OS
/// returns, matching `strerror`'s behavior.
fn pg_strerror(errnum: i32) -> String {
    io::Error::from_raw_os_error(errnum).to_string()
}

#[cfg(test)]
mod tests;
