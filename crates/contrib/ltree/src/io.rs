//! `contrib/ltree/ltree_io.c` + `ltxtquery_io.c` — the in/out parsers for
//! `ltree`, `lquery`, and `ltxtquery`. Each parser is a faithful port of the
//! C state machine (the regression suite compares exact parse results AND the
//! exact syntax-error messages / character positions).

use ::types_error::error::{
    ERRCODE_NAME_TOO_LONG, ERRCODE_PROGRAM_LIMIT_EXCEEDED, ERRCODE_SYNTAX_ERROR,
};
use ::types_error::PgError;

use crate::crc::ltree_crc32_sz;
use crate::repr::*;

/// `pg_mblen_cstr(ptr)` over the remaining bytes (the input is a real cstring,
/// so there is always at least the current byte).
#[inline]
fn mblen(s: &[u8]) -> usize {
    ::mbutils::pg_mblen(s).max(1) as usize
}

/// `ISLABEL(x)` — alphanumeric, '_' or '-'.
#[inline]
fn is_label(s: &[u8]) -> bool {
    let c = s[0];
    if c == b'_' || c == b'-' {
        return true;
    }
    ::ts_locale_seams::t_isalnum::call(&s[..mblen(s)])
}

// The regression .out compares sqlstate + message + (sometimes) detail, so we
// attach all three exactly as the C `ereport` does.
fn syntax_at(kind: &str, pos: i32) -> PgError {
    PgError::error(format!("{kind} syntax error at character {pos}")).with_sqlstate(ERRCODE_SYNTAX_ERROR)
}

fn syntax_detail(kind_msg: &str, detail: &str) -> PgError {
    PgError::error(kind_msg.to_string())
        .with_sqlstate(ERRCODE_SYNTAX_ERROR)
        .with_detail(detail.to_string())
}

fn prog_limit(msg: impl Into<String>) -> PgError {
    PgError::error(msg).with_sqlstate(ERRCODE_PROGRAM_LIMIT_EXCEEDED)
}

fn prog_limit_detail(msg: &str, detail: &str) -> PgError {
    PgError::error(msg.to_string())
        .with_sqlstate(ERRCODE_PROGRAM_LIMIT_EXCEEDED)
        .with_detail(detail.to_string())
}

fn name_too_long(detail: &str) -> PgError {
    PgError::error("label string is too long".to_string())
        .with_sqlstate(ERRCODE_NAME_TOO_LONG)
        .with_detail(detail.to_string())
}

// ---------------------------------------------------------------------------
// ltree
// ---------------------------------------------------------------------------

#[derive(Clone, Copy)]
struct NodeItem {
    start: usize, // byte offset into buf
    len: usize,   // byte length
    flag: u8,
    wlen: i32, // length in characters
}

const LTPRS_WAITNAME: i32 = 0;
const LTPRS_WAITDELIM: i32 = 1;

/// `finish_nodeitem` — compute the byte length and validate (empty/too-long).
fn finish_nodeitem(
    buf: &[u8],
    lptr: &mut NodeItem,
    ptr: usize,
    is_lquery: bool,
    pos: i32,
) -> Result<(), PgError> {
    let mut ptr = ptr;
    let mut pos = pos;
    if is_lquery {
        // Back up over trailing flag characters @ * %, discounting length/pos.
        while ptr > lptr.start && matches!(buf[ptr - 1], b'@' | b'*' | b'%') {
            ptr -= 1;
            lptr.wlen -= 1;
            pos -= 1;
        }
    }
    lptr.len = ptr - lptr.start;
    if lptr.len == 0 {
        let kind = if is_lquery { "lquery" } else { "ltree" };
        return Err(syntax_detail(
            &format!("{kind} syntax error at character {pos}"),
            "Empty labels are not allowed.",
        ));
    }
    if lptr.wlen > LTREE_LABEL_MAX_CHARS {
        return Err(name_too_long(&format!(
            "Label length is {}, must be at most {}, at character {}.",
            lptr.wlen, LTREE_LABEL_MAX_CHARS, pos
        )));
    }
    Ok(())
}

/// `parse_ltree` → an `ltree` varlena image.
pub fn parse_ltree(buf: &[u8]) -> Result<Vec<u8>, PgError> {
    // buf is the cstring payload (no trailing NUL needed; we treat end-of-slice
    // as the C '\0').
    let n = buf.len();

    // Count '.' to size the node list.
    let mut num = 0i32;
    {
        let mut i = 0;
        while i < n {
            let cl = mblen(&buf[i..]);
            if buf[i] == b'.' {
                num += 1;
            }
            i += cl;
        }
    }
    if num + 1 > LTREE_MAX_LEVELS {
        return Err(prog_limit(format!(
            "number of ltree labels ({}) exceeds the maximum allowed ({})",
            num + 1,
            LTREE_MAX_LEVELS
        )));
    }

    let mut list: Vec<NodeItem> = vec![
        NodeItem {
            start: 0,
            len: 0,
            flag: 0,
            wlen: 0
        };
        (num + 1) as usize
    ];
    let mut lptr_idx = 0usize;
    let mut state = LTPRS_WAITNAME;
    let mut pos = 1i32;

    let mut i = 0usize;
    while i < n {
        let cl = mblen(&buf[i..]);
        match state {
            LTPRS_WAITNAME => {
                if is_label(&buf[i..]) {
                    list[lptr_idx].start = i;
                    list[lptr_idx].wlen = 0;
                    state = LTPRS_WAITDELIM;
                } else {
                    return Err(syntax_at("ltree", pos));
                }
            }
            LTPRS_WAITDELIM => {
                if buf[i] == b'.' {
                    finish_nodeitem(buf, &mut list[lptr_idx], i, false, pos)?;
                    lptr_idx += 1;
                    state = LTPRS_WAITNAME;
                } else if !is_label(&buf[i..]) {
                    return Err(syntax_at("ltree", pos));
                }
            }
            _ => unreachable!("internal error in ltree parser"),
        }
        i += cl;
        list[lptr_idx].wlen += 1;
        pos += 1;
    }

    if state == LTPRS_WAITDELIM {
        finish_nodeitem(buf, &mut list[lptr_idx], n, false, pos)?;
        lptr_idx += 1;
    } else if !(state == LTPRS_WAITNAME && lptr_idx == 0) {
        return Err(syntax_detail("ltree syntax error", "Unexpected end of input."));
    }

    // Build the ltree image.
    let labels: Vec<&[u8]> = list[..lptr_idx]
        .iter()
        .map(|it| &buf[it.start..it.start + it.len])
        .collect();
    Ok(build_ltree(&labels))
}

/// `deparse_ltree` → the textual cstring payload (no trailing NUL).
pub fn deparse_ltree(image: &[u8]) -> Vec<u8> {
    let t = Ltree::new(image);
    let mut out = Vec::new();
    for (i, lvl) in t.levels().enumerate() {
        if i != 0 {
            out.push(b'.');
        }
        out.extend_from_slice(lvl.name);
    }
    out
}

// ---------------------------------------------------------------------------
// lquery
// ---------------------------------------------------------------------------

const LQPRS_WAITLEVEL: i32 = 0;
const LQPRS_WAITDELIM: i32 = 1;
const LQPRS_WAITOPEN: i32 = 2;
const LQPRS_WAITFNUM: i32 = 3;
const LQPRS_WAITSNUM: i32 = 4;
const LQPRS_WAITND: i32 = 5;
const LQPRS_WAITCLOSE: i32 = 6;
const LQPRS_WAITEND: i32 = 7;
const LQPRS_WAITVAR: i32 = 8;

/// A parsed lquery level, in owned (pre-serialization) form.
struct PLevel {
    flag: u16,
    low: u16,
    high: u16,
    variants: Vec<NodeItem>,
}

/// `atoi(ptr)` C-style: parse the leading run of digits.
fn atoi(buf: &[u8], i: usize) -> i32 {
    let mut v: i64 = 0;
    let mut j = i;
    while j < buf.len() && buf[j].is_ascii_digit() {
        v = v * 10 + (buf[j] - b'0') as i64;
        if v > i32::MAX as i64 {
            v = i32::MAX as i64;
        }
        j += 1;
    }
    v as i32
}

/// `parse_lquery` → an `lquery` varlena image.
pub fn parse_lquery(buf: &[u8]) -> Result<Vec<u8>, PgError> {
    let n = buf.len();

    // count '.' (level separators); the C also counts '|' to size the variant
    // array up-front, but we grow the variant Vec dynamically so we don't need
    // it.
    let mut num = 0i32;
    {
        let mut i = 0;
        while i < n {
            let cl = mblen(&buf[i..]);
            if buf[i] == b'.' {
                num += 1;
            }
            i += cl;
        }
    }
    num += 1;
    if num > LQUERY_MAX_LEVELS {
        return Err(prog_limit(format!(
            "number of lquery items ({}) exceeds the maximum allowed ({})",
            num, LQUERY_MAX_LEVELS
        )));
    }

    // Owned levels; we push one per '.'-separated level. Start with one level.
    let mut levels: Vec<PLevel> = Vec::with_capacity(num as usize);
    levels.push(PLevel {
        flag: 0,
        low: 0,
        high: 0,
        variants: Vec::new(),
    });
    let mut cur = 0usize; // current level index
    let mut lvar = 0usize; // current variant index within cur
    let mut hasnot = false;
    let mut state = LQPRS_WAITLEVEL;
    let mut pos = 1i32;

    // helper to push a new empty level (NEXTLEV)
    macro_rules! nextlev {
        () => {{
            levels.push(PLevel {
                flag: 0,
                low: 0,
                high: 0,
                variants: Vec::new(),
            });
            cur += 1;
        }};
    }

    let mut i = 0usize;
    while i < n {
        let cl = mblen(&buf[i..]);
        let c = buf[i];
        match state {
            LQPRS_WAITLEVEL => {
                if is_label(&buf[i..]) {
                    levels[cur].variants.push(NodeItem {
                        start: i,
                        len: 0,
                        flag: 0,
                        wlen: 0,
                    });
                    lvar = 0;
                    state = LQPRS_WAITDELIM;
                } else if c == b'!' {
                    levels[cur].variants.push(NodeItem {
                        start: i + 1,
                        len: 0,
                        flag: 0,
                        wlen: -1, // compensate for counting ! below
                    });
                    lvar = 0;
                    state = LQPRS_WAITDELIM;
                    levels[cur].flag |= LQL_NOT;
                    hasnot = true;
                } else if c == b'*' {
                    state = LQPRS_WAITOPEN;
                } else {
                    return Err(syntax_at("lquery", pos));
                }
            }
            LQPRS_WAITVAR => {
                if is_label(&buf[i..]) {
                    levels[cur].variants.push(NodeItem {
                        start: i,
                        len: 0,
                        flag: 0,
                        wlen: 0,
                    });
                    lvar += 1;
                    state = LQPRS_WAITDELIM;
                } else {
                    return Err(syntax_at("lquery", pos));
                }
            }
            LQPRS_WAITDELIM => {
                if c == b'@' {
                    levels[cur].variants[lvar].flag |= LVAR_INCASE;
                    levels[cur].flag |= LVAR_INCASE as u16;
                } else if c == b'*' {
                    levels[cur].variants[lvar].flag |= LVAR_ANYEND;
                    levels[cur].flag |= LVAR_ANYEND as u16;
                } else if c == b'%' {
                    levels[cur].variants[lvar].flag |= LVAR_SUBLEXEME;
                    levels[cur].flag |= LVAR_SUBLEXEME as u16;
                } else if c == b'|' {
                    finish_variant(buf, &mut levels[cur].variants[lvar], i, pos)?;
                    state = LQPRS_WAITVAR;
                } else if c == b'{' {
                    finish_variant(buf, &mut levels[cur].variants[lvar], i, pos)?;
                    levels[cur].flag |= LQL_COUNT;
                    state = LQPRS_WAITFNUM;
                } else if c == b'.' {
                    finish_variant(buf, &mut levels[cur].variants[lvar], i, pos)?;
                    state = LQPRS_WAITLEVEL;
                    nextlev!();
                } else if is_label(&buf[i..]) {
                    // disallow more chars after a flag
                    if levels[cur].variants[lvar].flag != 0 {
                        return Err(syntax_at("lquery", pos));
                    }
                } else {
                    return Err(syntax_at("lquery", pos));
                }
            }
            LQPRS_WAITOPEN => {
                if c == b'{' {
                    state = LQPRS_WAITFNUM;
                } else if c == b'.' {
                    levels[cur].low = 0;
                    levels[cur].high = LTREE_MAX_LEVELS as u16;
                    nextlev!();
                    state = LQPRS_WAITLEVEL;
                } else {
                    return Err(syntax_at("lquery", pos));
                }
            }
            LQPRS_WAITFNUM => {
                if c == b',' {
                    state = LQPRS_WAITSNUM;
                } else if c.is_ascii_digit() {
                    let low = atoi(buf, i);
                    if low < 0 || low > LTREE_MAX_LEVELS {
                        return Err(prog_limit_detail(
                            "lquery syntax error",
                            &format!(
                                "Low limit ({}) exceeds the maximum allowed ({}), at character {}.",
                                low, LTREE_MAX_LEVELS, pos
                            ),
                        ));
                    }
                    levels[cur].low = low as u16;
                    state = LQPRS_WAITND;
                } else {
                    return Err(syntax_at("lquery", pos));
                }
            }
            LQPRS_WAITSNUM => {
                if c.is_ascii_digit() {
                    let high = atoi(buf, i);
                    if high < 0 || high > LTREE_MAX_LEVELS {
                        return Err(prog_limit_detail(
                            "lquery syntax error",
                            &format!(
                                "High limit ({}) exceeds the maximum allowed ({}), at character {}.",
                                high, LTREE_MAX_LEVELS, pos
                            ),
                        ));
                    } else if levels[cur].low as i32 > high {
                        return Err(syntax_detail(
                            "lquery syntax error",
                            &format!(
                                "Low limit ({}) is greater than high limit ({}), at character {}.",
                                levels[cur].low, high, pos
                            ),
                        ));
                    }
                    levels[cur].high = high as u16;
                    state = LQPRS_WAITCLOSE;
                } else if c == b'}' {
                    levels[cur].high = LTREE_MAX_LEVELS as u16;
                    state = LQPRS_WAITEND;
                } else {
                    return Err(syntax_at("lquery", pos));
                }
            }
            LQPRS_WAITCLOSE => {
                if c == b'}' {
                    state = LQPRS_WAITEND;
                } else if !c.is_ascii_digit() {
                    return Err(syntax_at("lquery", pos));
                }
            }
            LQPRS_WAITND => {
                if c == b'}' {
                    levels[cur].high = levels[cur].low;
                    state = LQPRS_WAITEND;
                } else if c == b',' {
                    state = LQPRS_WAITSNUM;
                } else if !c.is_ascii_digit() {
                    return Err(syntax_at("lquery", pos));
                }
            }
            LQPRS_WAITEND => {
                if c == b'.' {
                    state = LQPRS_WAITLEVEL;
                    nextlev!();
                } else {
                    return Err(syntax_at("lquery", pos));
                }
            }
            _ => unreachable!("internal error in lquery parser"),
        }

        i += cl;
        if state == LQPRS_WAITDELIM {
            // wlen counts characters of the current variant
            levels[cur].variants[lvar].wlen += 1;
        }
        pos += 1;
    }

    if state == LQPRS_WAITDELIM {
        finish_variant(buf, &mut levels[cur].variants[lvar], n, pos)?;
    } else if state == LQPRS_WAITOPEN {
        levels[cur].high = LTREE_MAX_LEVELS as u16;
    } else if state != LQPRS_WAITEND {
        return Err(syntax_detail("lquery syntax error", "Unexpected end of input."));
    }

    // Number of levels actually allocated equals `num`.
    debug_assert_eq!(levels.len() as i32, num);

    serialize_lquery(buf, &levels, num as u16, hasnot)
}

/// `finish_nodeitem(lptr, ptr, true, pos)` for lquery variants.
fn finish_variant(buf: &[u8], v: &mut NodeItem, ptr: usize, pos: i32) -> Result<(), PgError> {
    finish_nodeitem(buf, v, ptr, true, pos)
}

/// Build the `lquery` varlena image from the parsed levels.
fn serialize_lquery(
    buf: &[u8],
    levels: &[PLevel],
    num: u16,
    hasnot: bool,
) -> Result<Vec<u8>, PgError> {
    // Compute total length.
    let mut totallen = LQUERY_HDRSIZE;
    for lvl in levels {
        totallen += LQL_HDRSIZE;
        for v in &lvl.variants {
            totallen += maxalign(LVAR_HDRSIZE + v.len);
        }
    }

    let mut out = vec![0u8; totallen];
    set_varsize(&mut out, totallen);
    write_u16(&mut out, 4, num); // numlevel
    write_u16(&mut out, 6, 0); // firstgood (filled below)
    write_u16(&mut out, 8, if hasnot { LQUERY_HASNOT } else { 0 }); // flag

    let mut firstgood: u16 = 0;
    let mut wasbad = false;

    let mut off = LQUERY_HDRSIZE;
    for lvl in levels {
        let numvar = lvl.variants.len() as u16;
        // Write LQL header.
        // cur->totallen computed as we go.
        let lql_off = off;
        write_u16(&mut out, lql_off + 2, lvl.flag); // flag
        write_u16(&mut out, lql_off + 4, numvar); // numvar
        write_u16(&mut out, lql_off + 6, lvl.low); // low
        write_u16(&mut out, lql_off + 8, lvl.high); // high
        let mut cur_totallen = LQL_HDRSIZE;
        let mut voff = off + LQL_HDRSIZE;
        for v in &lvl.variants {
            cur_totallen += maxalign(LVAR_HDRSIZE + v.len);
            let val = ltree_crc32_sz(&buf[v.start..v.start + v.len]) as i32;
            write_i32(&mut out, voff, val); // val
            write_u16(&mut out, voff + 4, v.len as u16); // len
            out[voff + 6] = v.flag; // flag
            out[voff + LVAR_HDRSIZE..voff + LVAR_HDRSIZE + v.len]
                .copy_from_slice(&buf[v.start..v.start + v.len]);
            voff += maxalign(LVAR_HDRSIZE + v.len);
        }
        write_u16(&mut out, lql_off, cur_totallen as u16); // totallen

        if numvar > 0 {
            if numvar > 1 || lvl.flag != 0 {
                wasbad = true;
            } else if !wasbad {
                firstgood += 1;
            }
        } else {
            wasbad = true;
        }

        off += maxalign(cur_totallen);
    }

    write_u16(&mut out, 6, firstgood);
    Ok(out)
}

/// `deparse_lquery` → textual cstring payload (no trailing NUL).
pub fn deparse_lquery(image: &[u8]) -> Vec<u8> {
    let q = Lquery::new(image);
    let mut out = Vec::new();
    for (i, lvl) in q.levels().enumerate() {
        if i != 0 {
            out.push(b'.');
        }
        let numvar = lvl.numvar();
        if numvar > 0 {
            if lvl.flag() & LQL_NOT != 0 {
                out.push(b'!');
            }
            for (j, v) in lvl.variants().enumerate() {
                if j != 0 {
                    out.push(b'|');
                }
                out.extend_from_slice(v.name);
                if v.flag & LVAR_SUBLEXEME != 0 {
                    out.push(b'%');
                }
                if v.flag & LVAR_INCASE != 0 {
                    out.push(b'@');
                }
                if v.flag & LVAR_ANYEND != 0 {
                    out.push(b'*');
                }
            }
        } else {
            out.push(b'*');
        }

        if (lvl.flag() & LQL_COUNT != 0) || numvar == 0 {
            let low = lvl.low();
            let high = lvl.high();
            if low == high {
                out.extend_from_slice(format!("{{{}}}", low).as_bytes());
            } else if low == 0 {
                if high == LTREE_MAX_LEVELS as u16 {
                    if numvar == 0 {
                        // default for '*', print nothing
                    } else {
                        out.extend_from_slice(b"{,}");
                    }
                } else {
                    out.extend_from_slice(format!("{{,{}}}", high).as_bytes());
                }
            } else if high == LTREE_MAX_LEVELS as u16 {
                out.extend_from_slice(format!("{{{},}}", low).as_bytes());
            } else {
                out.extend_from_slice(format!("{{{},{}}}", low, high).as_bytes());
            }
        }
    }
    out
}

// ---------------------------------------------------------------------------
// ltxtquery
// ---------------------------------------------------------------------------

const WAITOPERAND: i32 = 1;
const INOPERAND: i32 = 2;
const WAITOPERATOR: i32 = 3;

/// Owned NODE for the polish-notation list (reverse order, like C).
#[derive(Clone, Copy)]
struct QNode {
    typ: i32,
    val: i32,
    distance: i32,
    length: i32,
    flag: u16,
}

struct QprsState<'a> {
    buf: &'a [u8],
    i: usize, // cursor into buf
    state: i32,
    count: i32,
    /// reverse-polish list (newest first, matching C's prepend `tmp->next = str`)
    str: Vec<QNode>,
    num: i32,
    // operand buffer
    op: Vec<u8>,
    sumlen: i32,
}

/// gettoken_query token kinds returned alongside the polish parse.
struct Tok {
    kind: i32,
    val: i32,
    lenval: i32,    // byte length of operand
    strval: usize,  // byte offset of operand start
    flag: u16,
}

impl<'a> QprsState<'a> {
    fn at_end(&self) -> bool {
        self.i >= self.buf.len()
    }
    fn cur(&self) -> u8 {
        // C reads *buf; at the NUL terminator this is 0.
        if self.i < self.buf.len() {
            self.buf[self.i]
        } else {
            0
        }
    }
    fn mblen(&self) -> usize {
        if self.i >= self.buf.len() {
            1
        } else {
            mblen(&self.buf[self.i..])
        }
    }
}

/// `gettoken_query` — pull the next token. Returns Err on hard syntax error.
fn gettoken_query(st: &mut QprsState) -> Result<Tok, PgError> {
    let mut flag: u16 = 0;
    let mut strval: usize = 0;
    let mut lenval: i32 = 0;
    loop {
        let charlen = st.mblen();
        match st.state {
            WAITOPERAND => {
                let c = st.cur();
                if c == b'!' {
                    st.i += 1;
                    return Ok(Tok { kind: OPR, val: b'!' as i32, lenval: 0, strval: 0, flag: 0 });
                } else if c == b'(' {
                    st.count += 1;
                    st.i += 1;
                    return Ok(Tok { kind: OPEN, val: 0, lenval: 0, strval: 0, flag: 0 });
                } else if !st.at_end() && is_label(&st.buf[st.i..]) {
                    st.state = INOPERAND;
                    strval = st.i;
                    lenval = charlen as i32;
                    flag = 0;
                } else if st.at_end() {
                    // *buf == '\0' in WAITOPERAND is not handled by C here; it
                    // falls through to the isspace check (NUL is not space) →
                    // operand syntax error. But makepol's loop only calls
                    // gettoken when not at END; reaching here means empty input
                    // → the `!state.num` empty-query check fires. Return END so
                    // makepol terminates and the caller reports "Empty query".
                    return Ok(Tok { kind: END, val: 0, lenval: 0, strval: 0, flag: 0 });
                } else if !(c as char).is_whitespace() {
                    return Err(PgError::error("operand syntax error")
                        .with_sqlstate(ERRCODE_SYNTAX_ERROR));
                }
            }
            INOPERAND => {
                if !st.at_end() && is_label(&st.buf[st.i..]) {
                    if flag != 0 {
                        return Err(PgError::error("modifiers syntax error")
                            .with_sqlstate(ERRCODE_SYNTAX_ERROR));
                    }
                    lenval += charlen as i32;
                } else {
                    let c = st.cur();
                    if c == b'%' {
                        flag |= LVAR_SUBLEXEME as u16;
                    } else if c == b'@' {
                        flag |= LVAR_INCASE as u16;
                    } else if c == b'*' {
                        flag |= LVAR_ANYEND as u16;
                    } else {
                        st.state = WAITOPERATOR;
                        return Ok(Tok { kind: VAL, val: 0, lenval, strval, flag });
                    }
                }
            }
            WAITOPERATOR => {
                let c = st.cur();
                if c == b'&' || c == b'|' {
                    st.state = WAITOPERAND;
                    let v = c as i32;
                    st.i += 1;
                    return Ok(Tok { kind: OPR, val: v, lenval: 0, strval: 0, flag: 0 });
                } else if c == b')' {
                    st.i += 1;
                    st.count -= 1;
                    return Ok(Tok {
                        kind: if st.count < 0 { ERR } else { CLOSE },
                        val: 0, lenval: 0, strval: 0, flag: 0,
                    });
                } else if st.at_end() {
                    return Ok(Tok {
                        kind: if st.count != 0 { ERR } else { END },
                        val: 0, lenval: 0, strval: 0, flag: 0,
                    });
                } else if c != b' ' {
                    return Ok(Tok { kind: ERR, val: 0, lenval: 0, strval: 0, flag: 0 });
                }
            }
            _ => return Ok(Tok { kind: ERR, val: 0, lenval: 0, strval: 0, flag: 0 }),
        }
        st.i += charlen;
    }
}

/// `pushquery` — prepend a NODE.
fn pushquery(
    st: &mut QprsState,
    typ: i32,
    val: i32,
    distance: i32,
    lenval: i32,
    flag: u16,
) -> Result<(), PgError> {
    if distance > 0xffff {
        return Err(PgError::error("value is too big")
            .with_sqlstate(ERRCODE_SYNTAX_ERROR));
    }
    if lenval > 0xff {
        return Err(PgError::error("operand is too long")
            .with_sqlstate(ERRCODE_SYNTAX_ERROR));
    }
    st.str.push(QNode { typ, val, distance, length: lenval, flag });
    st.num += 1;
    Ok(())
}

/// `pushval_asis` — push a VAL node and append its operand bytes.
fn pushval_asis(
    st: &mut QprsState,
    typ: i32,
    strval: usize,
    lenval: i32,
    flag: u16,
) -> Result<(), PgError> {
    if lenval > 0xffff {
        return Err(PgError::error("word is too long")
            .with_sqlstate(ERRCODE_SYNTAX_ERROR));
    }
    let distance = st.op.len() as i32;
    let crc = ltree_crc32_sz(&st.buf[strval..strval + lenval as usize]) as i32;
    pushquery(st, typ, crc, distance, lenval, flag)?;
    st.op.extend_from_slice(&st.buf[strval..strval + lenval as usize]);
    st.op.push(0u8); // NUL terminator
    st.sumlen += lenval + 1;
    Ok(())
}

const STACKDEPTH: usize = 32;

/// `makepol` — produce the reverse-polish list. Recursive.
fn makepol(st: &mut QprsState, depth: u32) -> Result<i32, PgError> {
    if depth > 10_000 {
        return Err(PgError::error("stack depth limit exceeded")
            .with_sqlstate(ERRCODE_PROGRAM_LIMIT_EXCEEDED));
    }
    let mut stack = [0i32; STACKDEPTH];
    let mut lenstack = 0usize;

    loop {
        let tok = gettoken_query(st)?;
        let typ = tok.kind;
        if typ == END {
            break;
        }
        match typ {
            x if x == VAL => {
                pushval_asis(st, VAL, tok.strval, tok.lenval, tok.flag)?;
                while lenstack > 0
                    && (stack[lenstack - 1] == b'&' as i32 || stack[lenstack - 1] == b'!' as i32)
                {
                    lenstack -= 1;
                    pushquery(st, OPR, stack[lenstack], 0, 0, 0)?;
                }
            }
            x if x == OPR => {
                if lenstack > 0 && tok.val == b'|' as i32 {
                    pushquery(st, OPR, tok.val, 0, 0, 0)?;
                } else {
                    if lenstack == STACKDEPTH {
                        return Err(PgError::error("stack too short"));
                    }
                    stack[lenstack] = tok.val;
                    lenstack += 1;
                }
            }
            x if x == OPEN => {
                if makepol(st, depth + 1)? == ERR {
                    return Ok(ERR);
                }
                while lenstack > 0
                    && (stack[lenstack - 1] == b'&' as i32 || stack[lenstack - 1] == b'!' as i32)
                {
                    lenstack -= 1;
                    pushquery(st, OPR, stack[lenstack], 0, 0, 0)?;
                }
            }
            x if x == CLOSE => {
                while lenstack > 0 {
                    lenstack -= 1;
                    pushquery(st, OPR, stack[lenstack], 0, 0, 0)?;
                }
                return Ok(END);
            }
            x if x == ERR => {
                return Err(PgError::error("syntax error").with_sqlstate(ERRCODE_SYNTAX_ERROR));
            }
            _ => {
                return Err(PgError::error("syntax error").with_sqlstate(ERRCODE_SYNTAX_ERROR));
            }
        }
    }
    while lenstack > 0 {
        lenstack -= 1;
        pushquery(st, OPR, stack[lenstack], 0, 0, 0)?;
    }
    Ok(END)
}

/// `findoprnd` — set each operator's `left` back-link over the ITEM array.
fn findoprnd(items: &mut [Item], pos: &mut usize, depth: u32) -> Result<(), PgError> {
    if depth > 10_000 {
        return Err(PgError::error("stack depth limit exceeded")
            .with_sqlstate(ERRCODE_PROGRAM_LIMIT_EXCEEDED));
    }
    let p = *pos;
    if items[p].typ as i32 == VAL || items[p].typ as i32 == VALTRUE {
        items[p].left = 0;
        *pos += 1;
    } else if items[p].val == b'!' as i32 {
        items[p].left = 1;
        *pos += 1;
        findoprnd(items, pos, depth + 1)?;
    } else {
        let tmp = *pos;
        *pos += 1;
        findoprnd(items, pos, depth + 1)?;
        items[tmp].left = (*pos - tmp) as i16;
        findoprnd(items, pos, depth + 1)?;
    }
    Ok(())
}

/// `queryin` → an `ltxtquery` varlena image.
pub fn parse_ltxtquery(buf: &[u8]) -> Result<Vec<u8>, PgError> {
    let mut st = QprsState {
        buf,
        i: 0,
        state: WAITOPERAND,
        count: 0,
        str: Vec::new(),
        num: 0,
        op: Vec::new(),
        sumlen: 0,
    };

    if makepol(&mut st, 0)? == ERR {
        // soft error path; queryin returns NULL → but we raise via the Err above
        return Err(PgError::error("syntax error").with_sqlstate(ERRCODE_SYNTAX_ERROR));
    }
    if st.num == 0 {
        return Err(syntax_detail("syntax error", "Empty query."));
    }

    let size = st.num as usize;
    let sumlen = st.sumlen as usize;
    let commonlen = computesize(size, sumlen);
    let mut out = vec![0u8; commonlen];
    set_varsize(&mut out, commonlen);
    write_i32(&mut out, 4, size as i32);

    // The reverse-polish list `st.str` was prepended (newest first); C reads
    // `state.str` (head) into ptr[0..num] in order, consuming the list from the
    // head. Since we pushed in arrival order to a Vec, the C "head" corresponds
    // to our LAST element. So iterate the Vec in reverse.
    let mut items: Vec<Item> = Vec::with_capacity(size);
    for node in st.str.iter().rev() {
        items.push(Item {
            typ: node.typ as i16,
            left: 0,
            val: node.val,
            flag: node.flag as u8,
            length: node.length as u8,
            distance: node.distance as u16,
        });
    }
    // Write items.
    for (i, it) in items.iter().enumerate() {
        write_item(&mut out, i, it);
    }

    // Operand region.
    let op_off = HDRSIZEQT + size * ITEM_SIZE;
    out[op_off..op_off + sumlen].copy_from_slice(&st.op[..sumlen]);

    // Set left links.
    let mut pos = 0usize;
    findoprnd(&mut items, &mut pos, 0)?;
    for (i, it) in items.iter().enumerate() {
        write_item(&mut out, i, it);
    }

    Ok(out)
}

/// INFIX printer for ltxtquery (`infix` in C). Recursive over the ITEM array.
struct Infix<'a> {
    items: &'a [Item],
    cur: usize, // index into items
    op: &'a [u8],
    out: Vec<u8>,
}

impl<'a> Infix<'a> {
    fn run(&mut self, first: bool, depth: u32) -> Result<(), PgError> {
        if depth > 10_000 {
            return Err(PgError::error("stack depth limit exceeded")
                .with_sqlstate(ERRCODE_PROGRAM_LIMIT_EXCEEDED));
        }
        let it = self.items[self.cur];
        if it.typ as i32 == VAL {
            // operand bytes at op + distance, up to NUL
            let start = it.distance as usize;
            let mut k = start;
            while k < self.op.len() && self.op[k] != 0 {
                self.out.push(self.op[k]);
                k += 1;
            }
            if it.flag & LVAR_SUBLEXEME != 0 {
                self.out.push(b'%');
            }
            if it.flag & LVAR_INCASE != 0 {
                self.out.push(b'@');
            }
            if it.flag & LVAR_ANYEND != 0 {
                self.out.push(b'*');
            }
            self.cur += 1;
        } else if it.val == b'!' as i32 {
            self.out.push(b'!');
            self.cur += 1;
            let isopr = self.items[self.cur].typ as i32 == OPR;
            if isopr {
                self.out.extend_from_slice(b"( ");
            }
            self.run(isopr, depth + 1)?;
            if isopr {
                self.out.extend_from_slice(b" )");
            }
        } else {
            let op = it.val;
            self.cur += 1;
            if op == b'|' as i32 && !first {
                self.out.extend_from_slice(b"( ");
            }

            // right operand into a sub-buffer
            let mut nrm = Infix {
                items: self.items,
                cur: self.cur,
                op: self.op,
                out: Vec::new(),
            };
            nrm.run(false, depth + 1)?;

            // left operand into self
            self.cur = nrm.cur;
            self.run(false, depth + 1)?;

            // operator + right operand
            self.out.push(b' ');
            self.out.push(op as u8);
            self.out.push(b' ');
            self.out.extend_from_slice(&nrm.out);

            if op == b'|' as i32 && !first {
                self.out.extend_from_slice(b" )");
            }
        }
        Ok(())
    }
}

/// `deparse_ltxtquery` (the `ltxtq_out`/`ltxtq_send` infix body).
pub fn deparse_ltxtquery(image: &[u8]) -> Result<Vec<u8>, PgError> {
    let q = Ltxtquery::new(image);
    if q.size() == 0 {
        return Err(syntax_detail("syntax error", "Empty query."));
    }
    let items: Vec<Item> = (0..q.size()).map(|i| q.item(i)).collect();
    let mut inf = Infix {
        items: &items,
        cur: 0,
        op: q.operand(),
        out: Vec::new(),
    };
    inf.run(true, 0)?;
    Ok(inf.out)
}
