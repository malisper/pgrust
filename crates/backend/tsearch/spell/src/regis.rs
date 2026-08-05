use ::mcx::{Mcx, PgVec};
use ::types_error::{PgError, PgResult, ERRCODE_INTERNAL_ERROR};

use crate::{pg_mblen, t_isalpha, t_iseq};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RegisNodeKind {
    OneOf,
    NoneOf,
}

pub struct RegisNode<'mcx> {
    pub kind: RegisNodeKind,
    pub data: PgVec<'mcx, u8>,
}

pub struct Regis<'mcx> {
    pub issuffix: bool,
    pub nchar: u32,
    pub nodes: PgVec<'mcx, RegisNode<'mcx>>,
}

const RS_IN_ONEOF: i32 = 1;
const RS_IN_ONEOF_IN: i32 = 2;
const RS_IN_NONEOF: i32 = 3;
const RS_IN_WAIT: i32 = 4;

pub fn rs_is_regis(str: &[u8]) -> PgResult<bool> {
    let mut state: i32 = RS_IN_WAIT;
    let mut off = 0usize;

    while off < str.len() {
        let c = &str[off..];
        if state == RS_IN_WAIT {
            if t_isalpha(c) {
            } else if t_iseq(c, b'[') {
                state = RS_IN_ONEOF;
            } else {
                return Ok(false);
            }
        } else if state == RS_IN_ONEOF {
            if t_iseq(c, b'^') {
                state = RS_IN_NONEOF;
            } else if t_isalpha(c) {
                state = RS_IN_ONEOF_IN;
            } else {
                return Ok(false);
            }
        } else if state == RS_IN_ONEOF_IN || state == RS_IN_NONEOF {
            if t_isalpha(c) {
            } else if t_iseq(c, b']') {
                state = RS_IN_WAIT;
            } else {
                return Ok(false);
            }
        } else {
            return Err(PgError::error(format!("internal error in RS_isRegis: state {state}"))
                .with_sqlstate(ERRCODE_INTERNAL_ERROR)
                .into());
        }
        off += pg_mblen(c);
    }

    Ok(state == RS_IN_WAIT)
}

pub fn rs_compile<'mcx>(mcx: Mcx<'mcx>, issuffix: bool, str: &[u8]) -> PgResult<Regis<'mcx>> {
    let mut r = Regis { issuffix, nchar: 0, nodes: PgVec::new_in(mcx) };

    let mut state: i32 = RS_IN_WAIT;
    let mut off = 0usize;
    let mut cur: Option<usize> = None;

    while off < str.len() {
        let c = &str[off..];
        let clen = pg_mblen(c).min(c.len());
        let ch = &c[..clen];

        if state == RS_IN_WAIT {
            if t_isalpha(c) {
                push_node(mcx, &mut r.nodes, RegisNodeKind::OneOf, &mut cur)?;
                let idx = cur.expect("node started");
                copy_char_into(mcx, &mut r.nodes[idx].data, ch)?;
            } else if t_iseq(c, b'[') {
                push_node(mcx, &mut r.nodes, RegisNodeKind::OneOf, &mut cur)?;
                state = RS_IN_ONEOF;
            } else {
                return Err(invalid_regis_pattern(str).into());
            }
        } else if state == RS_IN_ONEOF {
            if t_iseq(c, b'^') {
                r.nodes[cur.expect("node started")].kind = RegisNodeKind::NoneOf;
                state = RS_IN_NONEOF;
            } else if t_isalpha(c) {
                let idx = cur.expect("node started");
                copy_char_into(mcx, &mut r.nodes[idx].data, ch)?;
                state = RS_IN_ONEOF_IN;
            } else {
                return Err(invalid_regis_pattern(str).into());
            }
        } else if state == RS_IN_ONEOF_IN || state == RS_IN_NONEOF {
            if t_isalpha(c) {
                let idx = cur.expect("node started");
                copy_char_into(mcx, &mut r.nodes[idx].data, ch)?;
            } else if t_iseq(c, b']') {
                state = RS_IN_WAIT;
            } else {
                return Err(invalid_regis_pattern(str).into());
            }
        } else {
            return Err(PgError::error(format!("internal error in RS_compile: state {state}"))
                .with_sqlstate(ERRCODE_INTERNAL_ERROR)
                .into());
        }
        off += clen;
    }

    if state != RS_IN_WAIT {
        return Err(invalid_regis_pattern(str).into());
    }

    r.nchar = r.nodes.len() as u32;
    Ok(r)
}

#[inline]
fn push_node<'mcx>(
    mcx: Mcx<'mcx>,
    nodes: &mut PgVec<'mcx, RegisNode<'mcx>>,
    kind: RegisNodeKind,
    cur: &mut Option<usize>,
) -> PgResult<()> {
    nodes
        .try_reserve(1)
        .map_err(|_| mcx.oom(core::mem::size_of::<RegisNode>()))?;
    nodes.push(RegisNode { kind, data: PgVec::new_in(mcx) });
    *cur = Some(nodes.len() - 1);
    Ok(())
}

#[inline]
fn copy_char_into<'mcx>(mcx: Mcx<'mcx>, data: &mut PgVec<'mcx, u8>, ch: &[u8]) -> PgResult<()> {
    data.try_reserve(ch.len()).map_err(|_| mcx.oom(ch.len()))?;
    data.extend_from_slice(ch);
    Ok(())
}

fn mb_strchr(str: &[u8], c: &[u8]) -> bool {
    let clen = pg_mblen(c).min(c.len());
    let mut pos = 0usize;
    while pos < str.len() {
        let plen = pg_mblen(&str[pos..]).min(str.len() - pos);
        if plen == clen && str[pos..pos + plen] == c[..clen] {
            return true;
        }
        pos += plen;
    }
    false
}

pub fn rs_execute(r: &Regis<'_>, str: &[u8]) -> PgResult<bool> {
    let mut len: i64 = 0;
    let mut off = 0usize;
    while off < str.len() {
        len += 1;
        off += pg_mblen(&str[off..]);
    }

    if len < r.nchar as i64 {
        return Ok(false);
    }

    off = 0;
    if r.issuffix {
        len -= r.nchar as i64;
        while len > 0 {
            len -= 1;
            off += pg_mblen(&str[off..]);
        }
    }

    for node in r.nodes.iter() {
        if off >= str.len() {
            return Ok(false);
        }
        let c = &str[off..];
        match node.kind {
            RegisNodeKind::OneOf => {
                if !mb_strchr(&node.data, c) {
                    return Ok(false);
                }
            }
            RegisNodeKind::NoneOf => {
                if mb_strchr(&node.data, c) {
                    return Ok(false);
                }
            }
        }
        off += pg_mblen(c);
    }

    Ok(true)
}

fn invalid_regis_pattern(bytes: &[u8]) -> PgError {
    PgError::error(format!(
        "invalid regis pattern: \"{}\"",
        String::from_utf8_lossy(bytes)
    ))
    .with_sqlstate(ERRCODE_INTERNAL_ERROR)
}
