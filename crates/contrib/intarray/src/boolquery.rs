//! _int_bool.c: the query_int (QUERYTYPE) parser, deparser, and boolean
//! evaluator shared by the operators and the GiST/GIN consistent paths.

use types_error::{
    PgError, PgResult, ERRCODE_INVALID_PARAMETER_VALUE, ERRCODE_PROGRAM_LIMIT_EXCEEDED,
    ERRCODE_STATEMENT_TOO_COMPLEX, ERRCODE_SYNTAX_ERROR,
};
use types_tuple::varatt;

use crate::tool::{getbit, hashval, IntArray};

pub const VAL: i32 = 2;
pub const OPR: i32 = 3;
const OPEN: i32 = 4;
const CLOSE: i32 = 5;
const END: i32 = 0;
const ERR: i32 = 1;

const WAITOPERAND: i32 = 1;
const WAITENDOPERAND: i32 = 2;
const WAITOPERATOR: i32 = 3;

const STACKDEPTH: usize = 16;
const HDRSIZEQT: usize = 8;
const QUERYTYPEMAXITEMS: usize = (0x3fffffff - HDRSIZEQT) / 8;

#[derive(Clone, Copy)]
pub struct Item {
    pub typ: i16,
    pub left: i16,
    pub val: i32,
}

/// bqarr_in error: `soft` marks C's four ereturn sites (reportable through an
/// ErrorSaveContext); stack-depth exhaustion stays hard.
#[derive(Debug)]
pub struct ParseError {
    pub err: Box<PgError>,
    pub soft: bool,
}

impl ParseError {
    fn soft(err: PgError) -> ParseError {
        ParseError { err: Box::new(err), soft: true }
    }

    fn hard(err: Box<PgError>) -> ParseError {
        ParseError { err, soft: false }
    }
}

/// ITEMs of a QUERYTYPE image (on-disk: i16 type, i16 left, i32 val).
pub fn parse_items(image: &[u8]) -> Vec<Item> {
    let size = (i32::from_ne_bytes(image[4..8].try_into().unwrap()).max(0) as usize)
        .min(image.len().saturating_sub(HDRSIZEQT) / 8);
    let mut items = Vec::with_capacity(size);
    for i in 0..size {
        let off = HDRSIZEQT + i * 8;
        items.push(Item {
            typ: i16::from_ne_bytes(image[off..off + 2].try_into().unwrap()),
            left: i16::from_ne_bytes(image[off + 2..off + 4].try_into().unwrap()),
            val: i32::from_ne_bytes(image[off + 4..off + 8].try_into().unwrap()),
        });
    }
    items
}

fn build_image(items: &[Item]) -> Vec<u8> {
    let nbytes = HDRSIZEQT + items.len() * 8;
    let mut img = Vec::with_capacity(nbytes);
    img.extend_from_slice(&varatt::set_varsize_4b_word(nbytes as u32).to_ne_bytes());
    img.extend_from_slice(&(items.len() as i32).to_ne_bytes());
    for it in items {
        img.extend_from_slice(&it.typ.to_ne_bytes());
        img.extend_from_slice(&it.left.to_ne_bytes());
        img.extend_from_slice(&it.val.to_ne_bytes());
    }
    img
}

struct WorkState<'a> {
    buf: &'a [u8],
    pos: usize,
    state: i32,
    count: i32,
    // reverse polish notation, in push order (C fills items[] from its
    // prepended list so items[i] is the i-th push).
    items: Vec<(i32, i32)>,
}

impl WorkState<'_> {
    fn cur(&self) -> u8 {
        if self.pos < self.buf.len() {
            self.buf[self.pos]
        } else {
            0
        }
    }

    // strtol(nnn, NULL, 0) over a token of optional '-' plus digits: a "0"
    // prefix selects octal and the parse stops at the first invalid digit
    // (so e.g. "08" is 0), endptr is ignored, and only an out-of-int32
    // result is an error.
    fn parse_number(token: &[u8]) -> Result<i32, ()> {
        let (neg, digits) = match token.split_first() {
            Some((b'-', rest)) => (true, rest),
            _ => (false, token),
        };
        let (base, digits) = match digits.split_first() {
            Some((b'0', rest)) => (8i64, rest),
            _ => (10i64, digits),
        };
        let mut v: i64 = 0;
        for &c in digits {
            let d = (c - b'0') as i64;
            if base == 8 && d > 7 {
                break;
            }
            v = v * base + d;
        }
        if neg {
            v = -v;
        }
        i32::try_from(v).map_err(|_| ())
    }

    fn gettoken(&mut self) -> (i32, i32) {
        let mut nnn = [0u8; 16];
        let mut innn = 0usize;
        loop {
            if innn >= nnn.len() {
                return (ERR, 0);
            }
            match self.state {
                WAITOPERAND => {
                    innn = 0;
                    let c = self.cur();
                    if c.is_ascii_digit() || c == b'-' {
                        self.state = WAITENDOPERAND;
                        nnn[innn] = c;
                        innn += 1;
                    } else if c == b'!' {
                        self.pos += 1;
                        return (OPR, '!' as i32);
                    } else if c == b'(' {
                        self.count += 1;
                        self.pos += 1;
                        return (OPEN, 0);
                    } else if c != b' ' {
                        return (ERR, 0);
                    }
                }
                WAITENDOPERAND => {
                    let c = self.cur();
                    if c.is_ascii_digit() {
                        nnn[innn] = c;
                        innn += 1;
                    } else {
                        let val = match Self::parse_number(&nnn[..innn]) {
                            Ok(v) => v,
                            Err(()) => return (ERR, 0),
                        };
                        self.state = WAITOPERATOR;
                        return if self.count != 0 && self.cur() == 0 {
                            (ERR, 0)
                        } else {
                            (VAL, val)
                        };
                    }
                }
                WAITOPERATOR => {
                    let c = self.cur();
                    if c == b'&' || c == b'|' {
                        self.state = WAITOPERAND;
                        self.pos += 1;
                        return (OPR, c as i32);
                    } else if c == b')' {
                        self.pos += 1;
                        self.count -= 1;
                        return if self.count < 0 { (ERR, 0) } else { (CLOSE, 0) };
                    } else if c == 0 {
                        return if self.count != 0 { (ERR, 0) } else { (END, 0) };
                    } else if c != b' ' {
                        return (ERR, 0);
                    }
                }
                _ => return (ERR, 0),
            }
            self.pos += 1;
        }
    }

    fn push(&mut self, typ: i32, val: i32) {
        self.items.push((typ, val));
    }

    fn makepol(&mut self) -> Result<(), ParseError> {
        stack_depth::check_stack_depth().map_err(ParseError::hard)?;
        let mut stack = [0i32; STACKDEPTH];
        let mut lenstack = 0usize;
        loop {
            let (typ, val) = self.gettoken();
            match typ {
                END => break,
                VAL => {
                    self.push(VAL, val);
                    while lenstack > 0
                        && (stack[lenstack - 1] == '&' as i32 || stack[lenstack - 1] == '!' as i32)
                    {
                        lenstack -= 1;
                        self.push(OPR, stack[lenstack]);
                    }
                }
                OPR => {
                    if lenstack > 0 && val == '|' as i32 {
                        self.push(OPR, val);
                    } else {
                        if lenstack == STACKDEPTH {
                            return Err(ParseError::soft(
                                PgError::error("statement too complex")
                                    .with_sqlstate(ERRCODE_STATEMENT_TOO_COMPLEX),
                            ));
                        }
                        stack[lenstack] = val;
                        lenstack += 1;
                    }
                }
                OPEN => {
                    self.makepol()?;
                    while lenstack > 0
                        && (stack[lenstack - 1] == '&' as i32 || stack[lenstack - 1] == '!' as i32)
                    {
                        lenstack -= 1;
                        self.push(OPR, stack[lenstack]);
                    }
                }
                CLOSE => {
                    while lenstack > 0 {
                        lenstack -= 1;
                        self.push(OPR, stack[lenstack]);
                    }
                    return Ok(());
                }
                _ => {
                    return Err(ParseError::soft(
                        PgError::error("syntax error").with_sqlstate(ERRCODE_SYNTAX_ERROR),
                    ))
                }
            }
        }
        while lenstack > 0 {
            lenstack -= 1;
            self.push(OPR, stack[lenstack]);
        }
        Ok(())
    }
}

fn findoprnd(items: &mut [Item], pos: &mut i32) -> Result<(), ParseError> {
    stack_depth::check_stack_depth().map_err(ParseError::hard)?;
    if *pos < 0 {
        // Unreachable from the parser (operand/operator states pair up);
        // C reads out of bounds here.
        return Err(ParseError::hard(
            PgError::error("syntax error").with_sqlstate(ERRCODE_SYNTAX_ERROR).into(),
        ));
    }
    let p = *pos as usize;
    if items[p].typ == VAL as i16 {
        items[p].left = 0;
        *pos -= 1;
    } else if items[p].val == '!' as i32 {
        items[p].left = -1;
        *pos -= 1;
        findoprnd(items, pos)?;
    } else {
        let tmp = *pos;
        *pos -= 1;
        findoprnd(items, pos)?;
        items[p].left = (*pos - tmp) as i16;
        findoprnd(items, pos)?;
    }
    Ok(())
}

/// bqarr_in core: cstring -> QUERYTYPE image.
pub fn parse_query(buf: &[u8]) -> Result<Vec<u8>, ParseError> {
    let mut ws = WorkState { buf, pos: 0, state: WAITOPERAND, count: 0, items: Vec::new() };
    ws.makepol()?;
    if ws.items.is_empty() {
        return Err(ParseError::soft(
            PgError::error("empty query").with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE),
        ));
    }
    if ws.items.len() > QUERYTYPEMAXITEMS {
        return Err(ParseError::soft(
            PgError::error(format!(
                "number of query items ({}) exceeds the maximum allowed ({})",
                ws.items.len(),
                QUERYTYPEMAXITEMS
            ))
            .with_sqlstate(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
        ));
    }
    let mut items: Vec<Item> = ws
        .items
        .iter()
        .map(|&(t, v)| Item { typ: t as i16, left: 0, val: v })
        .collect();
    let mut pos = items.len() as i32 - 1;
    findoprnd(&mut items, &mut pos)?;
    Ok(build_image(&items))
}

/// The boolean evaluator over reverse polish notation; chkcond receives the
/// item index (gin_bool_consistent maps check[] positionally).
pub fn execute(
    items: &[Item],
    pos: i32,
    calcnot: bool,
    chkcond: &mut dyn FnMut(usize, &Item) -> bool,
) -> PgResult<bool> {
    stack_depth::check_stack_depth()?;
    let p = pos as usize;
    let cur = &items[p];
    if cur.typ == VAL as i16 {
        Ok(chkcond(p, cur))
    } else if cur.val == '!' as i32 {
        Ok(if calcnot { !execute(items, pos - 1, calcnot, chkcond)? } else { true })
    } else if cur.val == '&' as i32 {
        if execute(items, pos + cur.left as i32, calcnot, chkcond)? {
            execute(items, pos - 1, calcnot, chkcond)
        } else {
            Ok(false)
        }
    } else {
        if execute(items, pos + cur.left as i32, calcnot, chkcond)? {
            Ok(true)
        } else {
            execute(items, pos - 1, calcnot, chkcond)
        }
    }
}

/// signconsistent: evaluate against a signature bitmap.
pub fn signconsistent(items: &[Item], sign: &[u8], siglen: usize, calcnot: bool) -> PgResult<bool> {
    execute(items, items.len() as i32 - 1, calcnot, &mut |_, it| {
        getbit(sign, hashval(it.val, siglen))
    })
}

/// execconsistent: evaluate against a sorted array.
pub fn execconsistent(items: &[Item], array: &IntArray, calcnot: bool) -> PgResult<bool> {
    array.check_valid()?;
    let elems = array.elems();
    execute(items, items.len() as i32 - 1, calcnot, &mut |_, it| {
        checkcondition_arr(elems, it.val)
    })
}

pub fn checkcondition_arr(sorted: &[i32], val: i32) -> bool {
    let mut lo = 0usize;
    let mut hi = sorted.len();
    while lo < hi {
        let mid = lo + (hi - lo) / 2;
        if sorted[mid] == val {
            return true;
        } else if sorted[mid] < val {
            lo = mid + 1;
        } else {
            hi = mid;
        }
    }
    false
}

/// gin_bool_consistent: check[] maps positionally onto the VAL items, in the
/// same order ginint4_queryextract emitted them.
pub fn gin_bool_consistent(items: &[Item], check: &[bool]) -> PgResult<bool> {
    if items.is_empty() {
        return Ok(false);
    }
    let mut mapped = vec![false; items.len()];
    let mut j = 0usize;
    for (i, it) in items.iter().enumerate() {
        if it.typ == VAL as i16 {
            mapped[i] = check[j];
            j += 1;
        }
    }
    execute(items, items.len() as i32 - 1, true, &mut |i, _| mapped[i])
}

fn contains_required_value(items: &[Item], pos: i32) -> PgResult<bool> {
    stack_depth::check_stack_depth()?;
    let cur = &items[pos as usize];
    if cur.typ == VAL as i16 {
        Ok(true)
    } else if cur.val == '!' as i32 {
        // Anything under a NOT is assumed non-required.
        Ok(false)
    } else if cur.val == '&' as i32 {
        if contains_required_value(items, pos + cur.left as i32)? {
            Ok(true)
        } else {
            contains_required_value(items, pos - 1)
        }
    } else {
        if contains_required_value(items, pos + cur.left as i32)? {
            contains_required_value(items, pos - 1)
        } else {
            Ok(false)
        }
    }
}

pub fn query_has_required_values(items: &[Item]) -> PgResult<bool> {
    if items.is_empty() {
        return Ok(false);
    }
    contains_required_value(items, items.len() as i32 - 1)
}

fn infix(items: &[Item], pos: i32, first: bool) -> PgResult<(String, i32)> {
    stack_depth::check_stack_depth()?;
    let cur = &items[pos as usize];
    if cur.typ == VAL as i16 {
        Ok((format!("{}", cur.val), pos - 1))
    } else if cur.val == '!' as i32 {
        let operand = pos - 1;
        let isopr = items[operand as usize].typ == OPR as i16;
        let (s, next) = infix(items, operand, isopr)?;
        Ok((if isopr { format!("!( {s} )") } else { format!("!{s}") }, next))
    } else {
        let op = cur.val as u8 as char;
        let (right, rnext) = infix(items, pos - 1, false)?;
        let (left, lnext) = infix(items, rnext, false)?;
        let s = if op == '|' && !first {
            format!("( {left} {op} {right} )")
        } else {
            format!("{left} {op} {right}")
        };
        Ok((s, lnext))
    }
}

/// bqarr_out core: QUERYTYPE items -> infix text.
pub fn deparse(items: &[Item]) -> PgResult<Vec<u8>> {
    if items.is_empty() {
        return Err(PgError::error("empty query")
            .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE)
            .into());
    }
    let (s, _) = infix(items, items.len() as i32 - 1, true)?;
    Ok(s.into_bytes())
}
