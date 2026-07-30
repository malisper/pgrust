use ::adt_float::{float8_lt, float8in_internal, float8out_internal};
use ::datum::Varlena;
use ::mcx::{Mcx, PgVec};
use ::stringinfo::StringInfo;
use ::types_core::geo::{Point, BOX, CIRCLE, LINE, LSEG, PATH_HEADER_SIZE, POLYGON_HEADER_SIZE};
use ::types_error::{
    ereturn, PgError, PgResult, SoftErrorContext, ERRCODE_INVALID_BINARY_REPRESENTATION,
    ERRCODE_INVALID_TEXT_REPRESENTATION, ERRCODE_PROGRAM_LIMIT_EXCEEDED,
};

use crate::lseg::lseg_sl;
use crate::{bound_box, invalid_input, point_eq_point, PathRef, PolyRef, Pts, POINT_SIZE};

const LDELIM: u8 = b'(';
const RDELIM: u8 = b')';
const DELIM: u8 = b',';
const LDELIM_EP: u8 = b'[';
const RDELIM_EP: u8 = b']';
const LDELIM_C: u8 = b'<';
const RDELIM_C: u8 = b'>';
const RDELIM_L: u8 = b'}';
const LDELIM_L: u8 = b'{';

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

    #[inline]
    fn cur(&self) -> u8 {
        if self.pos < self.bytes.len() {
            self.bytes[self.pos]
        } else {
            0
        }
    }

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

    #[inline]
    fn skip_ws(&mut self) {
        while self.cur().is_ascii_whitespace() {
            self.advance();
        }
    }

    #[inline]
    fn at_end(&self) -> bool {
        self.cur() == 0
    }

    #[inline]
    fn tail(&self) -> &'a str {
        // ASCII-only consumption keeps pos on a char boundary.
        core::str::from_utf8(&self.bytes[self.pos.min(self.bytes.len())..]).unwrap_or("")
    }

    // C `strrchr(str, c) == str`: the last occurrence of c is the current byte.
    #[inline]
    fn last_occurrence_is_here(&self, c: u8) -> bool {
        match self.bytes[self.pos.min(self.bytes.len())..]
            .iter()
            .rposition(|&b| b == c)
        {
            Some(0) => self.cur() == c,
            _ => false,
        }
    }
}

fn single_decode(
    cur: &mut Cursor,
    type_name: &str,
    orig_string: &str,
    escontext: Option<&mut SoftErrorContext>,
) -> PgResult<f64> {
    let tail = cur.tail();
    let mut consumed = 0usize;
    let value = float8in_internal(tail, Some(&mut consumed), type_name, orig_string, escontext)?;
    cur.pos += consumed;
    Ok(value)
}

#[inline]
fn soft_occurred(escontext: &Option<&mut SoftErrorContext>) -> bool {
    escontext.as_ref().is_some_and(|c| c.error_occurred())
}

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

    if !report_endptr && !cur.at_end() {
        return ereturn(escontext, (0.0, 0.0), invalid_input(type_name, orig_string));
    }

    Ok((x, y))
}

#[allow(clippy::too_many_arguments)]
fn path_decode(
    cur: &mut Cursor,
    opentype: bool,
    npts: usize,
    mut put: impl FnMut(usize, Point),
    report_endptr: bool,
    type_name: &str,
    orig_string: &str,
    mut escontext: Option<&mut SoftErrorContext>,
) -> PgResult<bool> {
    let mut depth = 0i32;

    cur.skip_ws();
    let isopen = cur.cur() == LDELIM_EP;
    if isopen {
        if !opentype {
            return ereturn(escontext, false, invalid_input(type_name, orig_string));
        }
        depth += 1;
        cur.advance();
    } else if cur.cur() == LDELIM {
        let mut peek = cur.pos + 1;
        while peek < cur.bytes.len() && cur.bytes[peek].is_ascii_whitespace() {
            peek += 1;
        }
        let cp_is_ldelim = peek < cur.bytes.len() && cur.bytes[peek] == LDELIM;
        if cp_is_ldelim || cur.last_occurrence_is_here(LDELIM) {
            depth += 1;
            cur.pos = peek;
        }
    }

    for i in 0..npts {
        let (x, y) = pair_decode(cur, true, type_name, orig_string, escontext.as_deref_mut())?;
        if soft_occurred(&escontext) {
            return Ok(false);
        }
        put(i, Point { x, y });
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

    if !report_endptr && !cur.at_end() {
        return ereturn(escontext, false, invalid_input(type_name, orig_string));
    }

    Ok(isopen)
}

fn pair_count(s: &str, delim: u8) -> i32 {
    let ndelim = s.bytes().filter(|&b| b == delim).count() as i32;
    if ndelim % 2 != 0 {
        (ndelim + 1) / 2
    } else {
        -1
    }
}

fn single_encode(x: f64, out: &mut Vec<u8>) {
    let mut buf = [0u8; 64];
    let n = float8out_internal(x, &mut buf);
    out.extend_from_slice(&buf[..n]);
}

#[derive(Copy, Clone)]
pub(crate) enum PathDelim {
    None,
    Open,
    Closed,
}

pub(crate) fn path_encode(delim: PathDelim, pts: &impl Pts, out: &mut Vec<u8>) {
    match delim {
        PathDelim::Closed => out.push(LDELIM),
        PathDelim::Open => out.push(LDELIM_EP),
        PathDelim::None => {}
    }

    for i in 0..pts.n() {
        if i > 0 {
            out.push(DELIM);
        }
        out.push(LDELIM);
        let p = pts.pt(i);
        crate::pair_encode(p.x, p.y, out);
        out.push(RDELIM);
    }

    match delim {
        PathDelim::Closed => out.push(RDELIM),
        PathDelim::Open => out.push(RDELIM_EP),
        PathDelim::None => {}
    }
}

// C computes base_size/size as 32-bit int; the guard relies on that wraparound.
pub fn check_points_overflow(npts: i32, header: usize) -> PgResult<()> {
    let base_size = (POINT_SIZE as i64 * npts as i64) as i32;
    let size = (header as i64 + base_size as i64) as i32;
    if base_size / npts != POINT_SIZE as i32 || size <= base_size {
        return Err(Box::new(
            PgError::error("too many points requested")
                .with_sqlstate(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
        ));
    }
    Ok(())
}

pub fn point_in(str: &str, escontext: Option<&mut SoftErrorContext>) -> PgResult<Point> {
    let mut cur = Cursor::new(str);
    let (x, y) = pair_decode(&mut cur, false, "point", str, escontext)?;
    Ok(Point { x, y })
}

pub fn point_out(pt: &Point, out: &mut Vec<u8>) {
    let one: &[Point] = core::slice::from_ref(pt);
    path_encode(PathDelim::None, &one, out);
}

pub fn box_in(str: &str, mut escontext: Option<&mut SoftErrorContext>) -> PgResult<BOX> {
    let mut cur = Cursor::new(str);
    let mut corners = [Point::default(); 2];
    path_decode(
        &mut cur,
        false,
        2,
        |i, p| corners[i] = p,
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

pub fn box_out(b: &BOX, out: &mut Vec<u8>) {
    let pts: &[Point] = &[b.high, b.low];
    path_encode(PathDelim::None, &pts, out);
}

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

#[cold]
fn invalid_line_spec(msg: &str) -> PgError {
    PgError::error(msg).with_sqlstate(ERRCODE_INVALID_TEXT_REPRESENTATION)
}

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
                invalid_line_spec("invalid line specification: A and B cannot both be zero"),
            );
        }
        Ok(line)
    } else {
        let mut pts = [Point::default(); 2];
        path_decode(
            &mut cur,
            true,
            2,
            |i, p| pts[i] = p,
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
                invalid_line_spec("invalid line specification: must be two distinct points"),
            );
        }
        // lseg_sl/line_construct overflow errors stay hard (C's XXX comment).
        let lseg = LSEG { p: pts };
        crate::line::line_construct(&pts[0], lseg_sl(&lseg)?)
    }
}

pub fn line_out(line: &LINE, out: &mut Vec<u8>) {
    out.push(LDELIM_L);
    single_encode(line.A, out);
    out.push(DELIM);
    single_encode(line.B, out);
    out.push(DELIM);
    single_encode(line.C, out);
    out.push(RDELIM_L);
}

pub fn lseg_in(str: &str, mut escontext: Option<&mut SoftErrorContext>) -> PgResult<LSEG> {
    let mut cur = Cursor::new(str);
    let mut pts = [Point::default(); 2];
    path_decode(
        &mut cur,
        true,
        2,
        |i, p| pts[i] = p,
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

pub fn lseg_out(ls: &LSEG, out: &mut Vec<u8>) {
    let pts: &[Point] = &ls.p;
    path_encode(PathDelim::Open, &pts, out);
}

fn empty_path_image<'m>(mcx: Mcx<'m>) -> PgResult<Varlena<'m>> {
    path_image(mcx, false, 0, |_| unreachable!())
}

fn empty_poly_image<'m>(mcx: Mcx<'m>) -> PgResult<Varlena<'m>> {
    poly_image(mcx, 0, |_| unreachable!())
}

/// Build a PATH varlena image (header stamped by Varlena::from_image).
pub fn path_image<'m>(
    mcx: Mcx<'m>,
    closed: bool,
    npts: usize,
    mut get: impl FnMut(usize) -> PgResult<Point>,
) -> PgResult<Varlena<'m>> {
    let total = PATH_HEADER_SIZE + npts * POINT_SIZE;
    let mut img: PgVec<'m, u8> = ::mcx::vec_with_capacity_in(mcx, total)?;
    img.resize(total, 0);
    img[4..8].copy_from_slice(&(npts as i32).to_ne_bytes());
    img[8..12].copy_from_slice(&(closed as i32).to_ne_bytes());
    for i in 0..npts {
        let p = get(i)?;
        let off = PATH_HEADER_SIZE + i * POINT_SIZE;
        img[off..off + POINT_SIZE].copy_from_slice(&p.to_datum_bytes());
    }
    Ok(Varlena::from_image(img))
}

/// Build a POLYGON varlena image; the boundbox is computed from the points.
pub fn poly_image<'m>(
    mcx: Mcx<'m>,
    npts: usize,
    mut get: impl FnMut(usize) -> PgResult<Point>,
) -> PgResult<Varlena<'m>> {
    let total = POLYGON_HEADER_SIZE + npts * POINT_SIZE;
    let mut img: PgVec<'m, u8> = ::mcx::vec_with_capacity_in(mcx, total)?;
    img.resize(total, 0);
    img[4..8].copy_from_slice(&(npts as i32).to_ne_bytes());
    for i in 0..npts {
        let p = get(i)?;
        let off = POLYGON_HEADER_SIZE + i * POINT_SIZE;
        img[off..off + POINT_SIZE].copy_from_slice(&p.to_datum_bytes());
    }
    if npts > 0 {
        let bb = bound_box(&PolyRef::from_payload(&img[4..]));
        img[8..40].copy_from_slice(&bb.to_datum_bytes());
    }
    Ok(Varlena::from_image(img))
}

pub fn path_in<'m>(
    mcx: Mcx<'m>,
    str: &str,
    mut escontext: Option<&mut SoftErrorContext>,
) -> PgResult<Varlena<'m>> {
    let npts = pair_count(str, DELIM);
    if npts <= 0 {
        let e = ereturn(escontext, (), invalid_input("path", str));
        e?;
        return empty_path_image(mcx);
    }
    check_points_overflow(npts, PATH_HEADER_SIZE)?;
    let npts = npts as usize;

    let mut cur = Cursor::new(str);
    cur.skip_ws();

    let mut depth = 0i32;
    if cur.cur() == LDELIM && cur.last_occurrence_is_here(LDELIM) {
        cur.advance();
        depth += 1;
    }

    let mut points: PgVec<'m, Point> = ::mcx::vec_with_capacity_in(mcx, npts)?;
    points.resize(npts, Point::default());
    let isopen = path_decode(
        &mut cur,
        true,
        npts,
        |i, p| points[i] = p,
        true,
        "path",
        str,
        escontext.as_deref_mut(),
    )?;
    if soft_occurred(&escontext) {
        return empty_path_image(mcx);
    }

    if depth >= 1 {
        if cur.next() != RDELIM {
            let e = ereturn(escontext, (), invalid_input("path", str));
            e?;
            return empty_path_image(mcx);
        }
        cur.skip_ws();
    }
    if !cur.at_end() {
        let e = ereturn(escontext, (), invalid_input("path", str));
        e?;
        return empty_path_image(mcx);
    }

    path_image(mcx, !isopen, npts, |i| Ok(points[i]))
}

pub fn path_out(path: &PathRef<'_>, out: &mut Vec<u8>) {
    path_encode(
        if path.closed {
            PathDelim::Closed
        } else {
            PathDelim::Open
        },
        path,
        out,
    );
}

pub fn poly_in<'m>(
    mcx: Mcx<'m>,
    str: &str,
    mut escontext: Option<&mut SoftErrorContext>,
) -> PgResult<Varlena<'m>> {
    let npts = pair_count(str, DELIM);
    if npts <= 0 {
        let e = ereturn(escontext, (), invalid_input("polygon", str));
        e?;
        return empty_poly_image(mcx);
    }
    check_points_overflow(npts, POLYGON_HEADER_SIZE)?;
    let npts = npts as usize;

    let mut points: PgVec<'m, Point> = ::mcx::vec_with_capacity_in(mcx, npts)?;
    points.resize(npts, Point::default());
    let mut cur = Cursor::new(str);
    path_decode(
        &mut cur,
        false,
        npts,
        |i, p| points[i] = p,
        false,
        "polygon",
        str,
        escontext.as_deref_mut(),
    )?;
    if soft_occurred(&escontext) {
        return empty_poly_image(mcx);
    }

    poly_image(mcx, npts, |i| Ok(points[i]))
}

pub fn poly_out(poly: &PolyRef<'_>, out: &mut Vec<u8>) {
    path_encode(PathDelim::Closed, poly, out);
}

pub fn circle_in(str: &str, mut escontext: Option<&mut SoftErrorContext>) -> PgResult<CIRCLE> {
    let mut cur = Cursor::new(str);
    let mut depth = 0i32;

    cur.skip_ws();
    if cur.cur() == LDELIM_C {
        depth += 1;
        cur.advance();
    } else if cur.cur() == LDELIM {
        let mut peek = cur.pos + 1;
        while peek < cur.bytes.len() && cur.bytes[peek].is_ascii_whitespace() {
            peek += 1;
        }
        if peek < cur.bytes.len() && cur.bytes[peek] == LDELIM {
            depth += 1;
            cur.pos = peek;
        }
    }

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

    // NaN must be accepted; only a definitely-negative radius is rejected.
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

pub fn circle_out(circle: &CIRCLE, out: &mut Vec<u8>) {
    out.push(LDELIM_C);
    out.push(LDELIM);
    crate::pair_encode(circle.center.x, circle.center.y, out);
    out.push(RDELIM);
    out.push(DELIM);
    single_encode(circle.radius, out);
    out.push(RDELIM_C);
}

#[track_caller]
#[cold]
fn invalid_binary(msg: &str) -> Box<PgError> {
    Box::new(PgError::error(msg).with_sqlstate(ERRCODE_INVALID_BINARY_REPRESENTATION))
}

pub fn point_recv(buf: &mut StringInfo<'_>) -> PgResult<Point> {
    Ok(Point {
        x: ::pqformat::pq_getmsgfloat8(buf)?,
        y: ::pqformat::pq_getmsgfloat8(buf)?,
    })
}

pub fn box_recv(buf: &mut StringInfo<'_>) -> PgResult<BOX> {
    let mut b = BOX {
        high: Point {
            x: ::pqformat::pq_getmsgfloat8(buf)?,
            y: ::pqformat::pq_getmsgfloat8(buf)?,
        },
        low: Point {
            x: ::pqformat::pq_getmsgfloat8(buf)?,
            y: ::pqformat::pq_getmsgfloat8(buf)?,
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

pub fn lseg_recv(buf: &mut StringInfo<'_>) -> PgResult<LSEG> {
    Ok(LSEG {
        p: [
            Point {
                x: ::pqformat::pq_getmsgfloat8(buf)?,
                y: ::pqformat::pq_getmsgfloat8(buf)?,
            },
            Point {
                x: ::pqformat::pq_getmsgfloat8(buf)?,
                y: ::pqformat::pq_getmsgfloat8(buf)?,
            },
        ],
    })
}

pub fn line_recv(buf: &mut StringInfo<'_>) -> PgResult<LINE> {
    let line = LINE {
        A: ::pqformat::pq_getmsgfloat8(buf)?,
        B: ::pqformat::pq_getmsgfloat8(buf)?,
        C: ::pqformat::pq_getmsgfloat8(buf)?,
    };
    if crate::FPzero(line.A) && crate::FPzero(line.B) {
        return Err(invalid_binary(
            "invalid line specification: A and B cannot both be zero",
        ));
    }
    Ok(line)
}

pub fn circle_recv(buf: &mut StringInfo<'_>) -> PgResult<CIRCLE> {
    let circle = CIRCLE {
        center: Point {
            x: ::pqformat::pq_getmsgfloat8(buf)?,
            y: ::pqformat::pq_getmsgfloat8(buf)?,
        },
        radius: ::pqformat::pq_getmsgfloat8(buf)?,
    };
    if circle.radius < 0.0 {
        return Err(invalid_binary("invalid radius in external \"circle\" value"));
    }
    Ok(circle)
}

pub fn path_recv<'m>(mcx: Mcx<'m>, buf: &mut StringInfo<'_>) -> PgResult<Varlena<'m>> {
    let closed = ::pqformat::pq_getmsgbyte(buf)?;
    let npts = ::pqformat::pq_getmsgint(buf, 4)? as i32;
    let max = ((i32::MAX as usize - PATH_HEADER_SIZE) / POINT_SIZE) as i64;
    if npts <= 0 || (npts as i64) >= max {
        return Err(invalid_binary(
            "invalid number of points in external \"path\" value",
        ));
    }
    path_image(mcx, closed != 0, npts as usize, |_| {
        Ok(Point {
            x: ::pqformat::pq_getmsgfloat8(buf)?,
            y: ::pqformat::pq_getmsgfloat8(buf)?,
        })
    })
}

pub fn poly_recv<'m>(mcx: Mcx<'m>, buf: &mut StringInfo<'_>) -> PgResult<Varlena<'m>> {
    let npts = ::pqformat::pq_getmsgint(buf, 4)? as i32;
    let max = ((i32::MAX as usize - POLYGON_HEADER_SIZE) / POINT_SIZE) as i64;
    if npts <= 0 || (npts as i64) >= max {
        return Err(invalid_binary(
            "invalid number of points in external \"polygon\" value",
        ));
    }
    poly_image(mcx, npts as usize, |_| {
        Ok(Point {
            x: ::pqformat::pq_getmsgfloat8(buf)?,
            y: ::pqformat::pq_getmsgfloat8(buf)?,
        })
    })
}

pub fn point_send<'m>(mcx: Mcx<'m>, pt: &Point) -> PgResult<Varlena<'m>> {
    let mut buf = ::pqformat::pq_begintypsend(mcx)?;
    ::pqformat::pq_sendfloat8(&mut buf, pt.x)?;
    ::pqformat::pq_sendfloat8(&mut buf, pt.y)?;
    Ok(::pqformat::pq_endtypsend(buf))
}

pub fn box_send<'m>(mcx: Mcx<'m>, b: &BOX) -> PgResult<Varlena<'m>> {
    let mut buf = ::pqformat::pq_begintypsend(mcx)?;
    ::pqformat::pq_sendfloat8(&mut buf, b.high.x)?;
    ::pqformat::pq_sendfloat8(&mut buf, b.high.y)?;
    ::pqformat::pq_sendfloat8(&mut buf, b.low.x)?;
    ::pqformat::pq_sendfloat8(&mut buf, b.low.y)?;
    Ok(::pqformat::pq_endtypsend(buf))
}

pub fn lseg_send<'m>(mcx: Mcx<'m>, ls: &LSEG) -> PgResult<Varlena<'m>> {
    let mut buf = ::pqformat::pq_begintypsend(mcx)?;
    for p in &ls.p {
        ::pqformat::pq_sendfloat8(&mut buf, p.x)?;
        ::pqformat::pq_sendfloat8(&mut buf, p.y)?;
    }
    Ok(::pqformat::pq_endtypsend(buf))
}

pub fn line_send<'m>(mcx: Mcx<'m>, line: &LINE) -> PgResult<Varlena<'m>> {
    let mut buf = ::pqformat::pq_begintypsend(mcx)?;
    ::pqformat::pq_sendfloat8(&mut buf, line.A)?;
    ::pqformat::pq_sendfloat8(&mut buf, line.B)?;
    ::pqformat::pq_sendfloat8(&mut buf, line.C)?;
    Ok(::pqformat::pq_endtypsend(buf))
}

pub fn circle_send<'m>(mcx: Mcx<'m>, circle: &CIRCLE) -> PgResult<Varlena<'m>> {
    let mut buf = ::pqformat::pq_begintypsend(mcx)?;
    ::pqformat::pq_sendfloat8(&mut buf, circle.center.x)?;
    ::pqformat::pq_sendfloat8(&mut buf, circle.center.y)?;
    ::pqformat::pq_sendfloat8(&mut buf, circle.radius)?;
    Ok(::pqformat::pq_endtypsend(buf))
}

pub fn path_send<'m>(mcx: Mcx<'m>, path: &PathRef<'_>) -> PgResult<Varlena<'m>> {
    let mut buf = ::pqformat::pq_begintypsend(mcx)?;
    ::pqformat::pq_sendbyte(&mut buf, path.closed as u8)?;
    ::pqformat::pq_sendint32(&mut buf, path.n() as u32)?;
    for i in 0..path.n() {
        let p = path.pt(i);
        ::pqformat::pq_sendfloat8(&mut buf, p.x)?;
        ::pqformat::pq_sendfloat8(&mut buf, p.y)?;
    }
    Ok(::pqformat::pq_endtypsend(buf))
}

pub fn poly_send<'m>(mcx: Mcx<'m>, poly: &PolyRef<'_>) -> PgResult<Varlena<'m>> {
    let mut buf = ::pqformat::pq_begintypsend(mcx)?;
    ::pqformat::pq_sendint32(&mut buf, poly.n() as u32)?;
    for i in 0..poly.n() {
        let p = poly.pt(i);
        ::pqformat::pq_sendfloat8(&mut buf, p.x)?;
        ::pqformat::pq_sendfloat8(&mut buf, p.y)?;
    }
    Ok(::pqformat::pq_endtypsend(buf))
}
