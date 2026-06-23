//! Text and binary I/O for the geometric types (geo_ops.c:193-402, plus each
//! type's `*_in`/`*_out`/`*_recv`/`*_send`).
//!
//! The varlena `path` / `polygon` values are represented by the owned safe-Rust
//! [`Path`] / [`Polygon`] structs (the actual palloc / `SET_VARSIZE` varlena
//! serialization is part of the deferred Datum fmgr layer). The `*_recv` /
//! `*_send` cores work on a byte cursor / byte vector in the same big-endian
//! wire form PostgreSQL uses, with the same validation.
//!
//! The text decoder is driven by a [`Cursor`] over the input bytes, mirroring
//! the C pointer-advancing `single_decode`/`pair_decode`/`path_decode` exactly
//! (including the `endptr_p` "report stopping point" behavior and the syntax
//! errors).

use ::types_core::geo::{Point, BOX, CIRCLE, LINE, LSEG};
use types_error::{ereturn, PgError, PgResult, SoftErrorContext};

use float_seams::{float8in_internal_endptr, float8out_internal};

use crate::boxes::box_construct;
use crate::f8::float8_lt;
use crate::point::point_eq_point;
use crate::poly::make_bound_box;
use crate::{errcode_invalid_binary, invalid_input, lseg, Path, Polygon};

// Delimiters (geo_ops.c:157-165).
const LDELIM: u8 = b'(';
const RDELIM: u8 = b')';
const DELIM: u8 = b',';
const LDELIM_EP: u8 = b'[';
const RDELIM_EP: u8 = b']';
const LDELIM_C: u8 = b'<';
const RDELIM_C: u8 = b'>';
const RDELIM_L: u8 = b'}';
const LDELIM_L: u8 = b'{';

// ===========================================================================
// Cursor-based text decoding (geo_ops.c:193-337).
// ===========================================================================

/// A pointer-advancing cursor over the input bytes, mirroring the C `char *str`
/// walking done by the decoders.
struct Cursor<'a> {
    bytes: &'a [u8],
    pos: usize,
}

impl<'a> Cursor<'a> {
    fn new(s: &'a str) -> Self {
        Cursor {
            bytes: s.as_bytes(),
            pos: 0,
        }
    }

    /// The current byte (`*str`), or NUL (`'\0'`) at/after the end -- matching
    /// C's NUL-terminated string semantics.
    #[inline]
    fn cur(&self) -> u8 {
        if self.pos < self.bytes.len() {
            self.bytes[self.pos]
        } else {
            0
        }
    }

    /// `*str++`: return the current byte and advance.
    #[inline]
    fn next(&mut self) -> u8 {
        let c = self.cur();
        if self.pos < self.bytes.len() {
            self.pos += 1;
        }
        c
    }

    #[inline]
    fn advance(&mut self) {
        if self.pos < self.bytes.len() {
            self.pos += 1;
        }
    }

    /// Skip leading whitespace (`while (isspace(*str)) str++`).
    #[inline]
    fn skip_ws(&mut self) {
        while self.cur().is_ascii_whitespace() {
            self.advance();
        }
    }

    /// `*str == '\0'`: at end of string.
    #[inline]
    fn at_end(&self) -> bool {
        self.cur() == 0
    }

    /// The remaining tail as a `&str` (for the `float8in_internal` seam).
    #[inline]
    fn tail(&self) -> &'a str {
        // The `bytes` came from a `&str`; `pos` is on a char boundary because
        // every byte we consume in the decoders is ASCII.
        core::str::from_utf8(&self.bytes[self.pos.min(self.bytes.len())..]).unwrap_or("")
    }

    /// `strrchr(str, c) == str`: is `c` present only at the current position
    /// (i.e. the last occurrence of `c` in the tail is the current byte)?
    #[inline]
    fn last_occurrence_is_here(&self, c: u8) -> bool {
        // C `strrchr` searches the whole NUL-terminated string from `str`.
        match self.bytes[self.pos.min(self.bytes.len())..]
            .iter()
            .rposition(|&b| b == c)
        {
            Some(0) => self.cur() == c,
            _ => false,
        }
    }
}

/// `single_decode(num, x, endptr_p, type_name, orig_string)` (geo_ops.c:193):
/// parse one float8 off the cursor via the `float8in_internal` seam, advancing
/// the cursor past the consumed token (and any trailing whitespace).
fn single_decode(
    cur: &mut Cursor,
    type_name: &str,
    orig_string: &str,
    escontext: Option<&mut SoftErrorContext>,
) -> PgResult<f64> {
    let tail = cur.tail();
    // C: `*x = float8in_internal(num, endptr_p, type_name, orig_string,
    // escontext)` (geo_ops.c:198) — a recoverable float syntax/range error is
    // routed into the soft sink rather than thrown. The endptr seam surfaces
    // such errors as `Err`; under a soft request we `ereturn` them (returning a
    // 0 sentinel, exactly as C leaves `*x` undefined after a soft error).
    match float8in_internal_endptr::call(
        tail.to_string(),
        type_name.to_string(),
        orig_string.to_string(),
    ) {
        Ok((value, consumed)) => {
            cur.pos += consumed;
            Ok(value)
        }
        Err(err) => ereturn(escontext, 0.0, err),
    }
}

/// `pair_decode(str, x, y, endptr_p, type_name, orig_string)` (geo_ops.c:211).
/// Parses an `(x,y)` pair (optionally parenthesized). When `report_endptr` is
/// false, requires end-of-string after the pair.
fn pair_decode(
    cur: &mut Cursor,
    report_endptr: bool,
    type_name: &str,
    orig_string: &str,
    mut escontext: Option<&mut SoftErrorContext>,
) -> PgResult<(f64, f64)> {
    cur.skip_ws();
    let has_delim = cur.cur() == LDELIM;
    if has_delim {
        cur.advance();
    }

    // C: `if (!single_decode(...)) return false;` — a soft error from the
    // float parse stops the decode (the saved error is already in escontext).
    let x = single_decode(cur, type_name, orig_string, escontext.as_deref_mut())?;
    if soft_occurred(&escontext) {
        return Ok((0.0, 0.0));
    }

    if cur.next() != DELIM {
        return ereturn(escontext, (0.0, 0.0), invalid_input(type_name, orig_string));
    }

    let y = single_decode(cur, type_name, orig_string, escontext.as_deref_mut())?;
    if soft_occurred(&escontext) {
        return Ok((0.0, 0.0));
    }

    if has_delim {
        if cur.next() != RDELIM {
            return ereturn(escontext, (0.0, 0.0), invalid_input(type_name, orig_string));
        }
        cur.skip_ws();
    }

    // Report stopping point if wanted, else complain if not end of string.
    if !report_endptr && !cur.at_end() {
        return ereturn(escontext, (0.0, 0.0), invalid_input(type_name, orig_string));
    }

    Ok((x, y))
}

/// `SOFT_ERROR_OCCURRED(escontext)`: true iff a soft-error sink is installed
/// and has already recorded a (recoverable) error.
#[inline]
fn soft_occurred(escontext: &Option<&mut SoftErrorContext>) -> bool {
    escontext.as_ref().is_some_and(|c| c.error_occurred())
}

/// `path_decode(str, opentype, npts, p, isopen, endptr_p, type_name,
/// orig_string)` (geo_ops.c:265). Decodes `npts` points into `out`, returning
/// the `isopen` flag. When `report_endptr` is false, requires end-of-string.
fn path_decode(
    cur: &mut Cursor,
    opentype: bool,
    npts: usize,
    out: &mut [Point],
    report_endptr: bool,
    type_name: &str,
    orig_string: &str,
    mut escontext: Option<&mut SoftErrorContext>,
) -> PgResult<bool> {
    let mut depth = 0i32;

    cur.skip_ws();
    let isopen = cur.cur() == LDELIM_EP;
    if isopen {
        // no open delimiter allowed?
        if !opentype {
            return ereturn(escontext, false, invalid_input(type_name, orig_string));
        }
        depth += 1;
        cur.advance();
    } else if cur.cur() == LDELIM {
        // peek past whitespace following the '('
        let mut peek = cur.pos + 1;
        while peek < cur.bytes.len() && cur.bytes[peek].is_ascii_whitespace() {
            peek += 1;
        }
        // C checks `*cp == LDELIM` first, then `strrchr(str, LDELIM) == str`;
        // both lead to the same `depth++; str = cp;`, so `||` (short-circuit,
        // same evaluation order) is faithful.
        let cp_is_ldelim = peek < cur.bytes.len() && cur.bytes[peek] == LDELIM;
        if cp_is_ldelim || cur.last_occurrence_is_here(LDELIM) {
            depth += 1;
            cur.pos = peek;
        }
    }

    for slot in out.iter_mut().take(npts) {
        // C: `if (!pair_decode(...)) return false;` — propagate a soft stop.
        let (x, y) = pair_decode(cur, true, type_name, orig_string, escontext.as_deref_mut())?;
        if soft_occurred(&escontext) {
            return Ok(false);
        }
        slot.x = x;
        slot.y = y;
        if cur.cur() == DELIM {
            cur.advance();
        }
    }

    while depth > 0 {
        if cur.cur() == RDELIM || (cur.cur() == RDELIM_EP && isopen && depth == 1) {
            depth -= 1;
            cur.advance();
            cur.skip_ws();
        } else {
            return ereturn(escontext, false, invalid_input(type_name, orig_string));
        }
    }

    // Report stopping point if wanted, else complain if not end of string.
    if !report_endptr && !cur.at_end() {
        return ereturn(escontext, false, invalid_input(type_name, orig_string));
    }

    Ok(isopen)
}

/// `pair_count(s, delim)` (geo_ops.c:391): count points given an odd number of
/// `delim` chars (`(ndelim + 1) / 2`), or -1 if even.
fn pair_count(s: &str, delim: u8) -> i32 {
    let ndelim = s.bytes().filter(|&b| b == delim).count() as i32;
    if ndelim % 2 != 0 {
        (ndelim + 1) / 2
    } else {
        -1
    }
}

// ===========================================================================
// Text encoding (geo_ops.c:202-382).
// ===========================================================================

/// `single_encode(x, str)` (geo_ops.c:202): append `float8out_internal(x)`.
fn single_encode(out: &mut String, x: f64) {
    out.push_str(&float8out_internal::call(x));
}

/// `pair_encode(x, y, str)` (geo_ops.c:254): append `"<x>,<y>"`.
fn pair_encode(out: &mut String, x: f64, y: f64) {
    out.push_str(&float8out_internal::call(x));
    out.push(DELIM as char);
    out.push_str(&float8out_internal::call(y));
}

/// The three path-encoding bracket styles (geo_ops.c:73-76).
#[derive(Copy, Clone)]
enum PathDelim {
    None,
    Open,
    Closed,
}

/// `path_encode(path_delim, npts, pt)` (geo_ops.c:339): render a point list into
/// a text buffer.
fn path_encode(delim: PathDelim, pts: &[Point]) -> String {
    let mut out = String::new();
    match delim {
        PathDelim::Closed => out.push(LDELIM as char),
        PathDelim::Open => out.push(LDELIM_EP as char),
        PathDelim::None => {}
    }

    for (i, pt) in pts.iter().enumerate() {
        if i > 0 {
            out.push(DELIM as char);
        }
        out.push(LDELIM as char);
        pair_encode(&mut out, pt.x, pt.y);
        out.push(RDELIM as char);
    }

    match delim {
        PathDelim::Closed => out.push(RDELIM as char),
        PathDelim::Open => out.push(RDELIM_EP as char),
        PathDelim::None => {}
    }

    out
}

// ===========================================================================
// point I/O (geo_ops.c:1830-1877).
// ===========================================================================

/// `point_in(str)` (geo_ops.c:1830). The decode forwards `fcinfo->context` so a
/// recoverable syntax error is routed into the soft sink (`pg_input_is_valid`).
pub fn point_in(str: &str, escontext: Option<&mut SoftErrorContext>) -> PgResult<Point> {
    let mut cur = Cursor::new(str);
    // C ignores `pair_decode`'s return (the result won't matter on error), but
    // the soft sink still records the error and the caller discards the value.
    let (x, y) = pair_decode(&mut cur, false, "point", str, escontext)?;
    Ok(Point { x, y })
}

/// `point_out(pt)` (geo_ops.c:1841).
pub fn point_out(pt: &Point) -> String {
    path_encode(PathDelim::None, core::slice::from_ref(pt))
}

/// `point_recv(buf)` (geo_ops.c:1852): read x, y as big-endian f64.
pub fn point_recv(buf: &mut &[u8]) -> PgResult<Point> {
    Ok(Point {
        x: getmsgfloat8(buf)?,
        y: getmsgfloat8(buf)?,
    })
}

/// `point_send(pt)` (geo_ops.c:1867): x, y as big-endian f64.
pub fn point_send(pt: &Point) -> Vec<u8> {
    let mut out = Vec::new();
    sendfloat8(&mut out, pt.x);
    sendfloat8(&mut out, pt.y);
    out
}

// ===========================================================================
// box I/O (geo_ops.c:415-512).
// ===========================================================================

/// `box_in(str)` (geo_ops.c:421): parse `(f8,f8),(f8,f8)` (or the old
/// `(f8,f8,f8,f8)`), reordering corners so `high` >= `low`.
pub fn box_in(str: &str, mut escontext: Option<&mut SoftErrorContext>) -> PgResult<BOX> {
    let mut cur = Cursor::new(str);
    let mut corners = [Point::default(); 2];
    path_decode(
        &mut cur,
        false,
        2,
        &mut corners,
        false,
        "box",
        str,
        escontext.as_deref_mut(),
    )?;
    if soft_occurred(&escontext) {
        return Ok(BOX::default());
    }
    let mut b = BOX {
        high: corners[0],
        low: corners[1],
    };

    if float8_lt(b.high.x, b.low.x) {
        core::mem::swap(&mut b.high.x, &mut b.low.x);
    }
    if float8_lt(b.high.y, b.low.y) {
        core::mem::swap(&mut b.high.y, &mut b.low.y);
    }

    Ok(b)
}

/// `box_out(box)` (geo_ops.c:454).
pub fn box_out(b: &BOX) -> String {
    path_encode(PathDelim::None, &[b.high, b.low])
}

/// `box_recv(buf)` (geo_ops.c:465): read high.x/y, low.x/y; reorder corners.
pub fn box_recv(buf: &mut &[u8]) -> PgResult<BOX> {
    let mut b = BOX {
        high: Point {
            x: getmsgfloat8(buf)?,
            y: getmsgfloat8(buf)?,
        },
        low: Point {
            x: getmsgfloat8(buf)?,
            y: getmsgfloat8(buf)?,
        },
    };
    if float8_lt(b.high.x, b.low.x) {
        core::mem::swap(&mut b.high.x, &mut b.low.x);
    }
    if float8_lt(b.high.y, b.low.y) {
        core::mem::swap(&mut b.high.y, &mut b.low.y);
    }
    Ok(b)
}

/// `box_send(box)` (geo_ops.c:500).
pub fn box_send(b: &BOX) -> Vec<u8> {
    let mut out = Vec::new();
    sendfloat8(&mut out, b.high.x);
    sendfloat8(&mut out, b.high.y);
    sendfloat8(&mut out, b.low.x);
    sendfloat8(&mut out, b.low.y);
    out
}

// ===========================================================================
// line I/O (geo_ops.c:949-1071).
// ===========================================================================

/// `line_decode(s, str, line)` (geo_ops.c:949): decode `A,B,C}` (the leading
/// '{' has already been consumed by `line_in`).
fn line_decode(
    cur: &mut Cursor,
    str: &str,
    mut escontext: Option<&mut SoftErrorContext>,
) -> PgResult<LINE> {
    let a = single_decode(cur, "line", str, escontext.as_deref_mut())?;
    if soft_occurred(&escontext) {
        return Ok(LINE::default());
    }
    if cur.next() != DELIM {
        return ereturn(escontext, LINE::default(), invalid_input("line", str));
    }
    let b = single_decode(cur, "line", str, escontext.as_deref_mut())?;
    if soft_occurred(&escontext) {
        return Ok(LINE::default());
    }
    if cur.next() != DELIM {
        return ereturn(escontext, LINE::default(), invalid_input("line", str));
    }
    let c = single_decode(cur, "line", str, escontext.as_deref_mut())?;
    if soft_occurred(&escontext) {
        return Ok(LINE::default());
    }
    if cur.next() != RDELIM_L {
        return ereturn(escontext, LINE::default(), invalid_input("line", str));
    }
    cur.skip_ws();
    if !cur.at_end() {
        return ereturn(escontext, LINE::default(), invalid_input("line", str));
    }
    Ok(LINE { A: a, B: b, C: c })
}

/// `line_in(str)` (geo_ops.c:978): parse `{A,B,C}` or two points.
pub fn line_in(str: &str, mut escontext: Option<&mut SoftErrorContext>) -> PgResult<LINE> {
    let mut cur = Cursor::new(str);
    cur.skip_ws();
    if cur.cur() == LDELIM_L {
        cur.advance();
        let line = line_decode(&mut cur, str, escontext.as_deref_mut())?;
        if soft_occurred(&escontext) {
            return Ok(LINE::default());
        }
        if crate::FPzero(line.A) && crate::FPzero(line.B) {
            return ereturn(
                escontext,
                LINE::default(),
                invalid_input_msg("invalid line specification: A and B cannot both be zero"),
            );
        }
        Ok(line)
    } else {
        let mut pts = [Point::default(); 2];
        path_decode(
            &mut cur,
            true,
            2,
            &mut pts,
            false,
            "line",
            str,
            escontext.as_deref_mut(),
        )?;
        if soft_occurred(&escontext) {
            return Ok(LINE::default());
        }
        if point_eq_point(&pts[0], &pts[1]) {
            return ereturn(
                escontext,
                LINE::default(),
                invalid_input_msg("invalid line specification: must be two distinct points"),
            );
        }
        // lseg_sl() / line_construct() can throw overflow/underflow errors
        // (kept hard, matching C's XXX comment).
        let lseg = LSEG { p: pts };
        crate::line::line_construct(&pts[0], lseg::lseg_sl(&lseg)?)
    }
}

/// Build the `invalid line specification: ...` syntax error (SQLSTATE 22P02,
/// `line_in`).
fn invalid_input_msg(msg: &str) -> PgError {
    PgError::error(msg).with_sqlstate(crate::errcode_invalid_text())
}

/// `line_out(line)` (geo_ops.c:1022): `{A,B,C}`.
pub fn line_out(line: &LINE) -> String {
    format!(
        "{}{}{}{}{}{}{}",
        LDELIM_L as char,
        float8out_internal::call(line.A),
        DELIM as char,
        float8out_internal::call(line.B),
        DELIM as char,
        float8out_internal::call(line.C),
        RDELIM_L as char,
    )
}

/// `line_recv(buf)` (geo_ops.c:1037): A, B, C as big-endian f64. Errors with
/// SQLSTATE 22P03 if A and B are both zero.
pub fn line_recv(buf: &mut &[u8]) -> PgResult<LINE> {
    let line = LINE {
        A: getmsgfloat8(buf)?,
        B: getmsgfloat8(buf)?,
        C: getmsgfloat8(buf)?,
    };
    if crate::FPzero(line.A) && crate::FPzero(line.B) {
        return Err(
            PgError::error("invalid line specification: A and B cannot both be zero")
                .with_sqlstate(errcode_invalid_binary()),
        );
    }
    Ok(line)
}

/// `line_send(line)` (geo_ops.c:1060).
pub fn line_send(line: &LINE) -> Vec<u8> {
    let mut out = Vec::new();
    sendfloat8(&mut out, line.A);
    sendfloat8(&mut out, line.B);
    sendfloat8(&mut out, line.C);
    out
}

// ===========================================================================
// lseg I/O (geo_ops.c:2064-2122).
// ===========================================================================

/// `lseg_in(str)` (geo_ops.c:2064).
pub fn lseg_in(str: &str, mut escontext: Option<&mut SoftErrorContext>) -> PgResult<LSEG> {
    let mut cur = Cursor::new(str);
    let mut pts = [Point::default(); 2];
    path_decode(
        &mut cur,
        true,
        2,
        &mut pts,
        false,
        "lseg",
        str,
        escontext.as_deref_mut(),
    )?;
    if soft_occurred(&escontext) {
        return Ok(LSEG::default());
    }
    Ok(LSEG { p: pts })
}

/// `lseg_out(ls)` (geo_ops.c:2080).
pub fn lseg_out(ls: &LSEG) -> String {
    path_encode(PathDelim::Open, &ls.p)
}

/// `lseg_recv(buf)` (geo_ops.c:2091).
pub fn lseg_recv(buf: &mut &[u8]) -> PgResult<LSEG> {
    Ok(LSEG {
        p: [
            Point {
                x: getmsgfloat8(buf)?,
                y: getmsgfloat8(buf)?,
            },
            Point {
                x: getmsgfloat8(buf)?,
                y: getmsgfloat8(buf)?,
            },
        ],
    })
}

/// `lseg_send(ls)` (geo_ops.c:2110).
pub fn lseg_send(ls: &LSEG) -> Vec<u8> {
    let mut out = Vec::new();
    for p in &ls.p {
        sendfloat8(&mut out, p.x);
        sendfloat8(&mut out, p.y);
    }
    out
}

// ===========================================================================
// path I/O (geo_ops.c:1401-1541).
// ===========================================================================

/// Size of one `Point` in the varlena payload (for the overflow checks).
const POINT_SIZE: usize = core::mem::size_of::<Point>();

/// `path_in(str)` (geo_ops.c:1401).
pub fn path_in(str: &str, mut escontext: Option<&mut SoftErrorContext>) -> PgResult<Path> {
    let empty = || Path {
        closed: false,
        points: Vec::new(),
    };
    let npts = pair_count(str, b',');
    if npts <= 0 {
        return ereturn(escontext, empty(), invalid_input("path", str));
    }
    let npts = npts as usize;
    if let Err(err) = check_points_overflow(npts, ::types_core::geo::PATH_HEADER_SIZE) {
        return ereturn(escontext, empty(), err);
    }

    let mut cur = Cursor::new(str);
    cur.skip_ws();

    // skip single leading paren
    let mut depth = 0i32;
    if cur.cur() == LDELIM && cur.last_occurrence_is_here(LDELIM) {
        cur.advance();
        depth += 1;
    }

    let mut points = vec![Point::default(); npts];
    let isopen = path_decode(
        &mut cur,
        true,
        npts,
        &mut points,
        true,
        "path",
        str,
        escontext.as_deref_mut(),
    )?;
    if soft_occurred(&escontext) {
        return Ok(empty());
    }

    if depth >= 1 {
        if cur.next() != RDELIM {
            return ereturn(escontext, empty(), invalid_input("path", str));
        }
        cur.skip_ws();
    }
    if !cur.at_end() {
        return ereturn(escontext, empty(), invalid_input("path", str));
    }

    Ok(Path {
        closed: !isopen,
        points,
    })
}

/// `path_out(path)` (geo_ops.c:1473).
pub fn path_out(path: &Path) -> String {
    path_encode(
        if path.closed {
            PathDelim::Closed
        } else {
            PathDelim::Open
        },
        &path.points,
    )
}

/// `path_recv(buf)` (geo_ops.c:1487): closed byte, int32 npts, then the points.
pub fn path_recv(buf: &mut &[u8]) -> PgResult<Path> {
    let closed = getmsgbyte(buf)?;
    let npts = getmsgint32(buf)?;
    let max = ((i32::MAX as usize - ::types_core::geo::PATH_HEADER_SIZE) / POINT_SIZE) as i64;
    if npts <= 0 || (npts as i64) >= max {
        return Err(
            PgError::error("invalid number of points in external \"path\" value")
                .with_sqlstate(errcode_invalid_binary()),
        );
    }
    let npts = npts as usize;

    let mut points: Vec<Point> = Vec::with_capacity(npts);
    for _ in 0..npts {
        let x = getmsgfloat8(buf)?;
        let y = getmsgfloat8(buf)?;
        points.push(Point { x, y });
    }
    Ok(Path {
        closed: closed != 0,
        points,
    })
}

/// `path_send(path)` (geo_ops.c:1525).
pub fn path_send(path: &Path) -> Vec<u8> {
    let mut out = Vec::new();
    sendbyte(&mut out, if path.closed { 1 } else { 0 });
    sendint32(&mut out, path.npts());
    for p in &path.points {
        sendfloat8(&mut out, p.x);
        sendfloat8(&mut out, p.y);
    }
    out
}

// ===========================================================================
// polygon I/O (geo_ops.c:3414-3524).
// ===========================================================================

/// `poly_in(str)` (geo_ops.c:3414).
pub fn poly_in(str: &str, mut escontext: Option<&mut SoftErrorContext>) -> PgResult<Polygon> {
    let empty = || Polygon {
        boundbox: BOX::default(),
        points: Vec::new(),
    };
    let npts = pair_count(str, b',');
    if npts <= 0 {
        return ereturn(escontext, empty(), invalid_input("polygon", str));
    }
    let npts = npts as usize;
    if let Err(err) = check_points_overflow(npts, ::types_core::geo::POLYGON_HEADER_SIZE) {
        return ereturn(escontext, empty(), err);
    }

    let mut points = vec![Point::default(); npts];
    let mut cur = Cursor::new(str);
    path_decode(
        &mut cur,
        false,
        npts,
        &mut points,
        false,
        "polygon",
        str,
        escontext.as_deref_mut(),
    )?;
    if soft_occurred(&escontext) {
        return Ok(empty());
    }

    let mut poly = Polygon {
        boundbox: BOX::default(),
        points,
    };
    make_bound_box(&mut poly);
    Ok(poly)
}

/// `poly_out(poly)` (geo_ops.c:3458).
pub fn poly_out(poly: &Polygon) -> String {
    path_encode(PathDelim::Closed, &poly.points)
}

/// `poly_recv(buf)` (geo_ops.c:3474): int32 npts then the points; the bounding
/// box is recomputed.
pub fn poly_recv(buf: &mut &[u8]) -> PgResult<Polygon> {
    let npts = getmsgint32(buf)?;
    let max = ((i32::MAX as usize - ::types_core::geo::POLYGON_HEADER_SIZE) / POINT_SIZE) as i64;
    if npts <= 0 || (npts as i64) >= max {
        return Err(
            PgError::error("invalid number of points in external \"polygon\" value")
                .with_sqlstate(errcode_invalid_binary()),
        );
    }
    let npts = npts as usize;

    let mut points: Vec<Point> = Vec::with_capacity(npts);
    for _ in 0..npts {
        let x = getmsgfloat8(buf)?;
        let y = getmsgfloat8(buf)?;
        points.push(Point { x, y });
    }
    let mut poly = Polygon {
        boundbox: BOX::default(),
        points,
    };
    make_bound_box(&mut poly);
    Ok(poly)
}

/// `poly_send(poly)` (geo_ops.c:3509).
pub fn poly_send(poly: &Polygon) -> Vec<u8> {
    let mut out = Vec::new();
    sendint32(&mut out, poly.npts());
    for p in &poly.points {
        sendfloat8(&mut out, p.x);
        sendfloat8(&mut out, p.y);
    }
    out
}

// ===========================================================================
// circle I/O (geo_ops.c:4610-4737).
// ===========================================================================

/// `circle_in(str)` (geo_ops.c:4610): parse `<(f8,f8),f8>` or `f8,f8,f8`.
pub fn circle_in(str: &str, mut escontext: Option<&mut SoftErrorContext>) -> PgResult<CIRCLE> {
    let mut cur = Cursor::new(str);
    let mut depth = 0i32;

    cur.skip_ws();
    if cur.cur() == LDELIM_C {
        depth += 1;
        cur.advance();
    } else if cur.cur() == LDELIM {
        // If there are two left parens, consume the first one.
        let mut peek = cur.pos + 1;
        while peek < cur.bytes.len() && cur.bytes[peek].is_ascii_whitespace() {
            peek += 1;
        }
        if peek < cur.bytes.len() && cur.bytes[peek] == LDELIM {
            depth += 1;
            cur.pos = peek;
        }
    }

    // pair_decode will consume parens around the pair, if any.
    let (cx, cy) = pair_decode(&mut cur, true, "circle", str, escontext.as_deref_mut())?;
    if soft_occurred(&escontext) {
        return Ok(CIRCLE::default());
    }

    if cur.cur() == DELIM {
        cur.advance();
    }

    let radius = single_decode(&mut cur, "circle", str, escontext.as_deref_mut())?;
    if soft_occurred(&escontext) {
        return Ok(CIRCLE::default());
    }

    // We have to accept NaN.
    if radius < 0.0 {
        return ereturn(escontext, CIRCLE::default(), invalid_input("circle", str));
    }

    while depth > 0 {
        if cur.cur() == RDELIM || (cur.cur() == RDELIM_C && depth == 1) {
            depth -= 1;
            cur.advance();
            cur.skip_ws();
        } else {
            return ereturn(escontext, CIRCLE::default(), invalid_input("circle", str));
        }
    }

    if !cur.at_end() {
        return ereturn(escontext, CIRCLE::default(), invalid_input("circle", str));
    }

    Ok(CIRCLE {
        center: Point { x: cx, y: cy },
        radius,
    })
}

/// `circle_out(circle)` (geo_ops.c:4680): `<(f8,f8),f8>`.
pub fn circle_out(circle: &CIRCLE) -> String {
    let mut out = String::new();
    out.push(LDELIM_C as char);
    out.push(LDELIM as char);
    pair_encode(&mut out, circle.center.x, circle.center.y);
    out.push(RDELIM as char);
    out.push(DELIM as char);
    single_encode(&mut out, circle.radius);
    out.push(RDELIM_C as char);
    out
}

/// `circle_recv(buf)` (geo_ops.c:4702): center.x/y, radius as big-endian f64.
/// Errors with SQLSTATE 22P03 if the radius is negative.
pub fn circle_recv(buf: &mut &[u8]) -> PgResult<CIRCLE> {
    let circle = CIRCLE {
        center: Point {
            x: getmsgfloat8(buf)?,
            y: getmsgfloat8(buf)?,
        },
        radius: getmsgfloat8(buf)?,
    };
    if circle.radius < 0.0 {
        return Err(
            PgError::error("invalid radius in external \"circle\" value")
                .with_sqlstate(errcode_invalid_binary()),
        );
    }
    Ok(circle)
}

/// `circle_send(circle)` (geo_ops.c:4726).
pub fn circle_send(circle: &CIRCLE) -> Vec<u8> {
    let mut out = Vec::new();
    sendfloat8(&mut out, circle.center.x);
    sendfloat8(&mut out, circle.center.y);
    sendfloat8(&mut out, circle.radius);
    out
}

// ===========================================================================
// Conversions that produce a polygon (geo_ops.c:4534): box_poly.
// ===========================================================================

/// `box_poly()` (geo_ops.c:4534): convert a box to a 4-vertex polygon.
pub fn box_poly(b: &BOX) -> Polygon {
    let points = vec![
        Point {
            x: b.low.x,
            y: b.low.y,
        },
        Point {
            x: b.low.x,
            y: b.high.y,
        },
        Point {
            x: b.high.x,
            y: b.high.y,
        },
        Point {
            x: b.high.x,
            y: b.low.y,
        },
    ];
    Polygon {
        boundbox: box_construct(&b.high, &b.low),
        points,
    }
}

// ===========================================================================
// Overflow check + minimal big-endian wire helpers.
// ===========================================================================

/// The integer-overflow guard from `path_in`/`poly_in`/`circle_poly`
/// (geo_ops.c:1434, 3434, 5249): `base_size / npts != POINT_SIZE || size <=
/// base_size` raises "too many points requested" (SQLSTATE 54000).
///
/// C computes `base_size` and `size` as 32-bit signed `int`; the overflow guard
/// relies on that 32-bit wraparound to reject `npts` values that would produce a
/// header+points allocation past `INT_MAX`. A `usize`-based check would never
/// trip on a 64-bit platform, so we faithfully reproduce the C `int` arithmetic
/// here (multiply/add in 64-bit, truncate to `i32`, compare with C's signed
/// integer division).
pub(crate) fn check_points_overflow(npts: usize, header: usize) -> PgResult<()> {
    // `npts` here is always a positive value that originated from an `i32`
    // (`pair_count` returns `i32`; `circle_poly` passes an `i32`), so the cast
    // back to `i32` is lossless for every reachable input.
    debug_assert!(npts <= i32::MAX as usize);
    let npts_i32 = npts as i32;

    // base_size = (size_t) POINT_SIZE * npts, then truncated to `int`.
    let base_size = (POINT_SIZE as i64 * npts as i64) as i32;
    // size = (size_t) header + base_size, then truncated to `int`. `base_size`
    // (an `int`) is sign-extended to 64-bit before the add, matching C.
    let size = (header as i64 + base_size as i64) as i32;

    // C's `base_size / npts` is signed integer division truncating toward zero.
    if base_size / npts_i32 != POINT_SIZE as i32 || size <= base_size {
        return Err(PgError::error("too many points requested")
            .with_sqlstate(crate::errcode_program_limit()));
    }
    Ok(())
}

/// `pq_getmsgfloat8`: read a big-endian IEEE f64 off the cursor.
fn getmsgfloat8(buf: &mut &[u8]) -> PgResult<f64> {
    let bytes = take(buf, 8)?;
    let arr: [u8; 8] = bytes.try_into().expect("take(8) yields 8 bytes");
    Ok(f64::from_bits(u64::from_be_bytes(arr)))
}

/// `pq_getmsgint(buf, 4)`: read a big-endian int32.
fn getmsgint32(buf: &mut &[u8]) -> PgResult<i32> {
    let bytes = take(buf, 4)?;
    let arr: [u8; 4] = bytes.try_into().expect("take(4) yields 4 bytes");
    Ok(i32::from_be_bytes(arr))
}

/// `pq_getmsgbyte(buf)`: read a single byte.
fn getmsgbyte(buf: &mut &[u8]) -> PgResult<u8> {
    let bytes = take(buf, 1)?;
    Ok(bytes[0])
}

/// Consume `n` bytes off the cursor, erroring on a short buffer (the
/// `pq_getmsg*` "insufficient data left in message" path, SQLSTATE 22P03).
fn take<'a>(buf: &mut &'a [u8], n: usize) -> PgResult<&'a [u8]> {
    if buf.len() < n {
        return Err(
            PgError::error("insufficient data left in message")
                .with_sqlstate(errcode_invalid_binary()),
        );
    }
    let (head, tail) = buf.split_at(n);
    *buf = tail;
    Ok(head)
}

/// `pq_sendfloat8`: append a big-endian IEEE f64.
fn sendfloat8(out: &mut Vec<u8>, v: f64) {
    out.extend_from_slice(&v.to_bits().to_be_bytes());
}

/// `pq_sendint32`: append a big-endian int32.
fn sendint32(out: &mut Vec<u8>, v: i32) {
    out.extend_from_slice(&v.to_be_bytes());
}

/// `pq_sendbyte`: append a single byte.
fn sendbyte(out: &mut Vec<u8>, b: u8) {
    out.push(b);
}
