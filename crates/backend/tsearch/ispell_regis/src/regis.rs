//! Port of `src/backend/tsearch/regis.c` — a fast regex subset used by the
//! ISpell dictionary (`spell.c`) to match affix-rule word endings.
//!
//! A pattern compiles into a chain of single-character class nodes (the C
//! `RegisNode` linked list), each `RSF_ONEOF` / `RSF_NONEOF`. The four public
//! entry points mirror the C file: [`rs_is_regis`] (`RS_isRegis`),
//! [`rs_compile`] (`RS_compile`), [`rs_execute`] (`RS_execute`), and
//! [`Regis::free`] (`RS_free`).
//!
//! The C `RegisNode` is a `palloc0`'d flexible-array struct with packed
//! `uint32` bitfields walked through raw pointers. The owned model is a
//! [`PgVec`] of nodes (the linked list), the `type` bitfield a two-variant
//! enum ([`RegisNodeKind`]), and the flexible-array `data` a context-allocated
//! byte vector. Inputs are `&[u8]` views (the lowercased mask / word the spell
//! dictionary hands regis), so all walking is bounds-checked slice work.
//!
//! The two locale/encoding helpers regis calls cross seams to their unported
//! owners: `t_isalpha_cstr` (`ts_locale.c`) and `pg_mblen_cstr`
//! (`mbutils.c`). `t_iseq` is a pure ASCII byte compare, ported in-crate.

use ::ts_locale_seams::t_isalpha as t_isalpha_seam;
use ::utils_error::ereport;
use ::mbutils_seams::pg_mblen_range;
use ::mcx::{Mcx, PgVec};
use ::types_error::{PgResult, ERRCODE_INTERNAL_ERROR, ERROR};

/// `RSF_ONEOF` — a character class the input character must be a member of.
pub const RSF_ONEOF: u32 = 1;
/// `RSF_NONEOF` — a character class the input character must NOT be a member of.
pub const RSF_NONEOF: u32 = 2;

/// The C `RegisNode.type` two-bit field, as a typed enum.
///
/// `[abc]` and bare letters compile to [`OneOf`](RegisNodeKind::OneOf); `[^abc]`
/// compiles to [`NoneOf`](RegisNodeKind::NoneOf).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RegisNodeKind {
    /// `RSF_ONEOF`: the input character must be one of the class.
    OneOf,
    /// `RSF_NONEOF`: the input character must not be one of the class.
    NoneOf,
}

impl RegisNodeKind {
    /// The C numeric `type` value (`RSF_ONEOF` / `RSF_NONEOF`).
    pub fn as_u32(self) -> u32 {
        match self {
            RegisNodeKind::OneOf => RSF_ONEOF,
            RegisNodeKind::NoneOf => RSF_NONEOF,
        }
    }
}

/// One node of a compiled [`Regis`]: a single-character class (the C linked-list
/// `RegisNode`). `data` is the C flexible-array class bytes (a concatenation of
/// whole multibyte characters; its length is the C `len`).
#[derive(Debug)]
pub struct RegisNode<'mcx> {
    /// The C `type` bitfield: `RSF_ONEOF` or `RSF_NONEOF`.
    pub kind: RegisNodeKind,
    /// The C flexible-array `data`: the stored character class bytes.
    pub data: PgVec<'mcx, u8>,
}

/// A compiled regis pattern (the C `Regis` struct).
///
/// `issuffix` selects suffix anchoring in [`rs_execute`]; `nchar` is the node
/// count (the minimum input length); `nodes` is the C `node` linked list.
#[derive(Debug)]
pub struct Regis<'mcx> {
    /// The C `issuffix` bitfield: anchor to the word's trailing `nchar` chars.
    pub issuffix: bool,
    /// The C `nchar` bitfield: the number of nodes == `nodes.len()`.
    pub nchar: u32,
    /// The C `node` linked list: the chain of single-character class nodes.
    pub nodes: PgVec<'mcx, RegisNode<'mcx>>,
}

impl Regis<'_> {
    /// The number of compiled nodes (the C `r->nchar`).
    #[inline]
    pub fn nchar(&self) -> u32 {
        self.nchar
    }

    /// Whether this is a suffix pattern (the C `r->issuffix`).
    #[inline]
    pub fn is_suffix(&self) -> bool {
        self.issuffix
    }

    /// `RS_free`: release the node chain. C `pfree`s each node and sets
    /// `r->node = NULL`; the owned chain is cleared (dropping the nodes uncharges
    /// their context allocations).
    pub fn free(&mut self) {
        self.nodes.clear();
        self.nchar = 0;
    }
}

// Internal compile states (the C `RS_IN_*` defines).
const RS_IN_ONEOF: i32 = 1;
const RS_IN_ONEOF_IN: i32 = 2;
const RS_IN_NONEOF: i32 = 3;
const RS_IN_WAIT: i32 = 4;

/// `t_isalpha_cstr(c)` for a byte slice positioned at the character under test.
#[inline]
fn t_isalpha(s: &[u8]) -> bool {
    t_isalpha_seam::call(s)
}

/// `pg_mblen_cstr(c)` for a byte slice positioned at a character boundary.
/// C returns the byte length of the leading character (`1..=s.len()`).
#[inline]
fn pg_mblen(s: &[u8]) -> PgResult<usize> {
    Ok(pg_mblen_range::call(s)? as usize)
}

/// `t_iseq(c, x)`: the leading byte of `s` equals the (ASCII) byte `x`. Pure.
/// `s` is non-empty at every call site (the loop guard ensures we are not at
/// the end of input).
#[inline]
fn t_iseq(s: &[u8], x: u8) -> bool {
    s[0] == x
}

/// `RS_isRegis`: true iff `str` is a valid regis pattern (only the `[...]` /
/// `[^...]` character-class subset plus bare letters). Keep in sync with
/// [`rs_compile`]! `str` is the byte view of the (already-lowercased) mask.
pub fn rs_is_regis(str: &[u8]) -> PgResult<bool> {
    let mut state: i32 = RS_IN_WAIT;
    let mut off = 0usize;

    while off < str.len() {
        let c = &str[off..];
        if state == RS_IN_WAIT {
            if t_isalpha(c) {
                /* okay */
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
                /* okay */
            } else if t_iseq(c, b']') {
                state = RS_IN_WAIT;
            } else {
                return Ok(false);
            }
        } else {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_INTERNAL_ERROR)
                .errmsg(alloc::format!("internal error in RS_isRegis: state {state}"))
                .into_error());
        }
        off += pg_mblen(c)?;
    }

    Ok(state == RS_IN_WAIT)
}

/// `RS_compile`: compile `str` into an owned [`Regis`]. `issuffix` marks a
/// suffix pattern. Raises `invalid regis pattern: "%s"` (an `XX000` internal
/// error, like the C `elog(ERROR)`) on malformed input — exactly at the C
/// `shouldn't get here` sites. Allocates the node chain in `mcx`.
pub fn rs_compile<'mcx>(mcx: Mcx<'mcx>, issuffix: bool, str: &[u8]) -> PgResult<Regis<'mcx>> {
    // C `memset(r, 0, sizeof(Regis))` then `r->issuffix = issuffix`.
    let mut r = Regis {
        issuffix,
        nchar: 0,
        nodes: PgVec::new_in(mcx),
    };

    let mut state: i32 = RS_IN_WAIT;
    let mut off = 0usize;
    // Index of the node currently being built (the C `ptr`); `None` is the C
    // `ptr == NULL`. We index into `r.nodes` rather than chase a raw `next`.
    let mut cur: Option<usize> = None;

    while off < str.len() {
        let c = &str[off..];
        let clen = pg_mblen(c)?;
        let ch = &c[..clen];

        if state == RS_IN_WAIT {
            if t_isalpha(c) {
                // New node, type ONEOF, data = this character (C lines 99-107).
                push_node(mcx, &mut r.nodes, RegisNodeKind::OneOf, &mut cur)?;
                let idx = cur.expect("node started");
                copy_char_into(mcx, &mut r.nodes[idx].data, ch)?;
            } else if t_iseq(c, b'[') {
                // New node, type ONEOF, empty data so far (C lines 108-115).
                push_node(mcx, &mut r.nodes, RegisNodeKind::OneOf, &mut cur)?;
                state = RS_IN_ONEOF;
            } else {
                /* shouldn't get here */
                return Err(invalid_regis_pattern(str));
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
                /* shouldn't get here */
                return Err(invalid_regis_pattern(str));
            }
        } else if state == RS_IN_ONEOF_IN || state == RS_IN_NONEOF {
            if t_isalpha(c) {
                let idx = cur.expect("node started");
                copy_char_into(mcx, &mut r.nodes[idx].data, ch)?;
            } else if t_iseq(c, b']') {
                state = RS_IN_WAIT;
            } else {
                /* shouldn't get here */
                return Err(invalid_regis_pattern(str));
            }
        } else {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_INTERNAL_ERROR)
                .errmsg(alloc::format!("internal error in RS_compile: state {state}"))
                .into_error());
        }
        off += clen;
    }

    if state != RS_IN_WAIT {
        /* shouldn't get here */
        return Err(invalid_regis_pattern(str));
    }

    // C: `ptr = r->node; while (ptr) { r->nchar++; ptr = ptr->next; }`.
    r.nchar = r.nodes.len() as u32;

    Ok(r)
}

/// `newRegisNode(prev, len)` + chain: append a node of the given `kind` to
/// `nodes` and set `cur` to its index (the C `ptr = r->node = newRegisNode(...)`
/// / `ptr = newRegisNode(ptr, len)`).
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
    nodes.push(RegisNode {
        kind,
        data: PgVec::new_in(mcx),
    });
    *cur = Some(nodes.len() - 1);
    Ok(())
}

/// Append one whole multibyte character `ch` to a node's class `data` (the C
/// `ts_copychar_cstr(data + len, c)` plus the `len += ...` update; `data.len()`
/// is the running C `len`).
#[inline]
fn copy_char_into<'mcx>(mcx: Mcx<'mcx>, data: &mut PgVec<'mcx, u8>, ch: &[u8]) -> PgResult<()> {
    data.try_reserve(ch.len()).map_err(|_| mcx.oom(ch.len()))?;
    data.extend_from_slice(ch);
    Ok(())
}

/// `mb_strchr(str, c)`: true iff the multibyte character at the front of `c`
/// occurs as a whole character within the class bytes `str`. Mirrors the C
/// `while (*ptr && !res)` walk: each class character of the same byte length as
/// `c`'s leading character is compared byte-for-byte.
fn mb_strchr(str: &[u8], c: &[u8]) -> PgResult<bool> {
    let clen = pg_mblen(c)?;
    let mut pos = 0usize;
    let mut res = false;

    while pos < str.len() && !res {
        let plen = pg_mblen(&str[pos..])?;
        if plen == clen {
            // Compare the `clen` bytes (the C `while (i--)` byte loop).
            res = str[pos..pos + plen] == c[..clen];
        }
        pos += plen;
    }

    Ok(res)
}

/// `RS_execute`: true iff `str` matches the compiled pattern `r`. Mirrors C
/// 1:1: the leading length count, the `len < r->nchar` early-out, the
/// `issuffix` alignment loop, then the per-node `RSF_ONEOF`/`RSF_NONEOF`
/// membership test, advancing one node and one character per step.
pub fn rs_execute(r: &Regis<'_>, str: &[u8]) -> PgResult<bool> {
    // C: count characters in the word (`while (*c) { len++; c += mblen; }`).
    let mut len: i64 = 0;
    let mut off = 0usize;
    while off < str.len() {
        len += 1;
        off += pg_mblen(&str[off..])?;
    }

    if len < r.nchar as i64 {
        return Ok(false);
    }

    // C: `c = str;` then, if issuffix, skip the first `len - nchar` characters.
    off = 0;
    if r.issuffix {
        len -= r.nchar as i64;
        while len > 0 {
            len -= 1;
            off += pg_mblen(&str[off..])?;
        }
    }

    for node in r.nodes.iter() {
        let c = &str[off..];
        match node.kind {
            RegisNodeKind::OneOf => {
                if !mb_strchr(&node.data, c)? {
                    return Ok(false);
                }
            }
            RegisNodeKind::NoneOf => {
                if mb_strchr(&node.data, c)? {
                    return Ok(false);
                }
            }
        }
        off += pg_mblen(c)?;
    }

    Ok(true)
}

/// The `invalid regis pattern: "%s"` `elog(ERROR)` (an `XX000` internal error),
/// with `bytes` rendered as the C `%s` would.
fn invalid_regis_pattern(bytes: &[u8]) -> ::types_error::PgError {
    ereport(ERROR)
        .errcode(ERRCODE_INTERNAL_ERROR)
        .errmsg(alloc::format!(
            "invalid regis pattern: \"{}\"",
            alloc::string::String::from_utf8_lossy(bytes)
        ))
        .into_error()
}
