//! Family: **read** — `nodes/read.c`, the node-tree de-serializer entry
//! (`stringToNode`).
//!
//! `stringToNode` / `stringToNodeInternal` / `stringToNodeWithLocations` plus
//! the tokenizer (`pg_strtok`, `debackslash`, `nodeTokenType`) and the
//! dispatch entry `nodeRead`. The per-tag field readers live in the
//! `readfuncs` unit (`backend-nodes-readfuncs`, separate catalog row); this
//! family is the tokenizer + driver only.
//!
//! Owns the canonical `backend-nodes-read-seams` (`string_to_node`) — installed
//! in `init_seams()` now that this family is filled. The reconstructed node
//! tree is `mcx`-allocated.
//!
//! ## The shared `pg_strtok` cursor
//!
//! C keeps the scan position in the file-static `pg_strtok_ptr`; `stringToNode`
//! save/restores it for re-entrancy, and both `read.c` and `readfuncs.c`
//! advance it through `pg_strtok`. We mirror that exactly with a thread-local
//! cursor that [`string_to_node`] sets up (and restores) around the read, so
//! the unported `parseNodeString` (readfuncs) can recurse back into our
//! [`pg_strtok`]/[`node_read`] over the same cursor when it lands. The cursor
//! holds a borrowed pointer into the caller's `&str`; it is only ever read
//! while that borrow is live (within [`string_to_node`]), matching the C
//! invariant that `pg_strtok_ptr` points into the live argument string.
//!
//! ## Split-model boundary
//!
//! `nodeRead`'s `LEFT_BRACE` case yields a concrete plan/expr node, which is
//! the [`types_nodes::nodes::Node`] the `string_to_node` seam hands back; that
//! body is read by `parseNodeString` (readfuncs). The integer/OID/XID/bitmapset
//! `(...)` lists and bare value tokens that `nodeRead` also recognises are
//! reached only as *sub-fields* during that readfuncs recursion — they carry
//! `List`/`Bitmapset`/value-node types that are deliberately not part of the
//! split plan-node enum, so the readfuncs unit drives them through the shared
//! cursor (calling back into [`pg_strtok`]). At the `stringToNode` top level a
//! `nodeToString` rendering of a real node is always a `{...}`, so the driver
//! here resolves that case and rejects a bare value/list as the top-level
//! result, exactly as those paths are unreachable from `stringToNode`'s
//! callers.

use std::cell::Cell;

use mcx::{Mcx, PgBox, PgString, PgVec};
use types_error::{PgError, PgResult, ERRCODE_INTERNAL_ERROR};
use types_nodes::nodes::Node;

/// `elog(ERROR, msg)` — an internal-error `PgError` carrying `msg`
/// (`ERRCODE_INTERNAL_ERROR`), the shape the C reader's `elog(ERROR, ...)`
/// raises for a malformed node string.
fn elog_error(message: impl Into<String>) -> PgError {
    PgError::error(message).with_sqlstate(ERRCODE_INTERNAL_ERROR)
}

// ---------------------------------------------------------------------------
// pg_strtok cursor (C: file-static `pg_strtok_ptr`)
// ---------------------------------------------------------------------------

thread_local! {
    /// Base pointer of the string being scanned (C: the value `pg_strtok_ptr`
    /// was last initialized to / advances within). Null when no scan is active.
    static PG_STRTOK_BASE: Cell<*const u8> = const { Cell::new(std::ptr::null()) };
    /// Total length of the scanned string in bytes.
    static PG_STRTOK_LEN: Cell<usize> = const { Cell::new(0) };
    /// Current scan offset into the string (C: `pg_strtok_ptr - base`).
    static PG_STRTOK_POS: Cell<usize> = const { Cell::new(0) };
}

/// A token returned by [`pg_strtok`]: a borrowed slice of the scanned string
/// (including any embedded backslashes) plus its byte length. The C
/// `pg_strtok` returns the start pointer and writes `*length`; this bundles
/// both. `length` may be 0 only for the special `<>` token.
#[derive(Clone, Copy)]
pub struct Token<'a> {
    /// The token bytes (length matches the C `*length`).
    pub bytes: &'a [u8],
}

impl<'a> Token<'a> {
    /// The token's byte length (C `*length`).
    #[inline]
    pub fn len(&self) -> usize {
        self.bytes.len()
    }

    /// Whether the token is the zero-length `<>` token.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.bytes.is_empty()
    }
}

/// `pg_strtok(int *length)` (read.c) — retrieve the next token from the active
/// scan string, advancing the shared cursor.
///
/// Works like `strtok` but never modifies the source: whitespace separates
/// tokens; `(`, `)`, `{`, `}` are individual single-byte tokens; otherwise a
/// token runs to the next whitespace/special char, with `\` quoting the next
/// byte (the backslashes are left in — [`debackslash`] removes them). The
/// special `<>` token is returned non-`None` with length 0. Returns `None`
/// (C: NULL / `*length = 0`) when no tokens remain.
///
/// # Safety / invariants
///
/// Reads the thread-local cursor set up by [`string_to_node`]; the returned
/// slice borrows the string that cursor points into, which the caller
/// guarantees outlives the borrow (mirrors C's `pg_strtok_ptr` pointing into
/// the live argument string).
pub fn pg_strtok<'a>() -> Option<Token<'a>> {
    let base = PG_STRTOK_BASE.with(|c| c.get());
    if base.is_null() {
        return None;
    }
    let len = PG_STRTOK_LEN.with(|c| c.get());
    // SAFETY: base..base+len is the active scan string, kept live by the
    // string_to_node scope that installed the cursor.
    let s: &[u8] = unsafe { std::slice::from_raw_parts(base, len) };

    let mut local = PG_STRTOK_POS.with(|c| c.get());

    // Skip leading whitespace.
    while local < len && (s[local] == b' ' || s[local] == b'\n' || s[local] == b'\t') {
        local += 1;
    }

    // End of input (C: *local_str == '\0').
    if local >= len {
        PG_STRTOK_POS.with(|c| c.set(local));
        return None;
    }

    // Now pointing at the start of the next token.
    let ret_start = local;

    let c0 = s[local];
    if c0 == b'(' || c0 == b')' || c0 == b'{' || c0 == b'}' {
        // Special 1-character token.
        local += 1;
    } else {
        // Normal token, possibly containing backslashes.
        while local < len {
            let c = s[local];
            if c == b' ' || c == b'\n' || c == b'\t' || c == b'(' || c == b')' || c == b'{'
                || c == b'}'
            {
                break;
            }
            if c == b'\\' && local + 1 < len {
                local += 2;
            } else {
                local += 1;
            }
        }
    }

    let mut tok_len = local - ret_start;

    // Recognize special case for the "empty" token "<>".
    if tok_len == 2 && s[ret_start] == b'<' && s[ret_start + 1] == b'>' {
        tok_len = 0;
    }

    PG_STRTOK_POS.with(|c| c.set(local));

    // For the "<>" case, length is 0 but the pointer is non-NULL; we keep the
    // (now zero-length) slice starting at ret_start.
    Some(Token {
        bytes: &s[ret_start..ret_start + tok_len],
    })
}

/// `debackslash(const char *token, int length)` (read.c) — produce an owned
/// string holding the token with any protective backslashes removed.
///
/// C palloc's `length + 1` bytes (NUL-terminated); the owned-string model
/// drops the trailing NUL and returns the de-escaped contents.
pub fn debackslash(token: &[u8]) -> String {
    let mut result = Vec::with_capacity(token.len());
    let mut i = 0;
    let len = token.len();
    while i < len {
        // C: if (*token == '\\' && length > 1) { token++; length--; }
        if token[i] == b'\\' && (len - i) > 1 {
            i += 1;
        }
        result.push(token[i]);
        i += 1;
    }
    // The reader only ever produces tokens over a valid UTF-8 (ASCII-superset)
    // source string; debackslash never splits a multibyte sequence because the
    // escaped byte is copied verbatim. Preserve bytes faithfully.
    String::from_utf8_lossy(&result).into_owned()
}

// ---------------------------------------------------------------------------
// nodeTokenType (C: returns NodeTag or one of the four reader-private codes)
// ---------------------------------------------------------------------------

/// The token classification produced by [`node_token_type`], mirroring the C
/// `nodeTokenType` return: either a value `NodeTag` or one of read.c's four
/// private parenthesis/brace codes.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum TokenType {
    /// C `T_Integer`.
    Integer,
    /// C `T_Float`.
    Float,
    /// C `T_Boolean`.
    Boolean,
    /// C `T_String`.
    String,
    /// C `T_BitString`.
    BitString,
    /// C `RIGHT_PAREN` (`(1000000 + 1)`).
    RightParen,
    /// C `LEFT_PAREN` (`(1000000 + 2)`).
    LeftParen,
    /// C `LEFT_BRACE` (`(1000000 + 3)`).
    LeftBrace,
    /// C `OTHER_TOKEN` (`(1000000 + 4)`).
    Other,
}

/// `strtoint`-style parse used by the number test: parse a base-10 `i32`,
/// returning the value and the number of bytes consumed. Mirrors the C
/// `strtoint(numptr, &endptr, 10)` used purely to decide integral-vs-float.
fn strtoint_prefix(s: &[u8]) -> (Result<i32, ()>, usize) {
    // Leading sign already stripped by the caller's numptr handling for the
    // digit test; here we parse the full numeric prefix as C strtoint would.
    let mut i = 0;
    let n = s.len();
    let mut sign_len = 0;
    if i < n && (s[i] == b'+' || s[i] == b'-') {
        sign_len = 1;
        i += 1;
    }
    let digits_start = i;
    while i < n && s[i].is_ascii_digit() {
        i += 1;
    }
    if i == digits_start {
        // No digits.
        return (Err(()), 0);
    }
    let end = i;
    let text = std::str::from_utf8(&s[..end]).unwrap_or("");
    // strtoint returns the long value with range checking; mirror its ERANGE
    // by reporting overflow as Err so the caller treats it as Float.
    let parsed = text.parse::<i32>();
    let _ = sign_len;
    match parsed {
        Ok(v) => (Ok(v), end),
        Err(_) => (Err(()), end),
    }
}

/// `nodeTokenType(const char *token, int length)` (read.c) — classify a token.
///
/// Assumes the ASCII representation is legal. Numbers are split into
/// `Integer`/`Float` by a strtoint syntax+range check; the four
/// parenthesis/brace bytes and the `true`/`false`/quoted-string/bitstring
/// shapes map to their tags; everything else is `Other`.
pub fn node_token_type(token: &[u8]) -> TokenType {
    let length = token.len();

    // Check if the token is a number.
    let mut numoff = 0usize;
    if length > 0 && (token[0] == b'+' || token[0] == b'-') {
        numoff = 1;
    }
    let numptr = &token[numoff..];
    let numlen = length - numoff;

    let is_number = (numlen > 0 && numptr[0].is_ascii_digit())
        || (numlen > 1 && numptr[0] == b'.' && numptr[1].is_ascii_digit());

    if is_number {
        // Figure out whether it is integral or float: strtoint over numptr,
        // requiring it to consume exactly through token+length (no trailing
        // junk) and not overflow, else it is a Float.
        let (res, consumed) = strtoint_prefix(numptr);
        // endptr != token + length  <=>  consumed != numlen
        if consumed != numlen || res.is_err() {
            return TokenType::Float;
        }
        return TokenType::Integer;
    }

    // These three single-byte cases need no length check (pg_strtok always
    // produces them as one-byte tokens).
    if token[0] == b'(' {
        TokenType::LeftParen
    } else if token[0] == b')' {
        TokenType::RightParen
    } else if token[0] == b'{' {
        TokenType::LeftBrace
    } else if (length == 4 && &token[..4] == b"true") || (length == 5 && &token[..5] == b"false") {
        TokenType::Boolean
    } else if token[0] == b'"' && length > 1 && token[length - 1] == b'"' {
        TokenType::String
    } else if token[0] == b'b' || token[0] == b'x' {
        TokenType::BitString
    } else {
        TokenType::Other
    }
}

// ---------------------------------------------------------------------------
// nodeRead (C: the slightly higher-level polymorphic reader)
// ---------------------------------------------------------------------------

/// `nodeRead(const char *token, int tok_len)` (read.c) — the higher-level
/// reader that applies semantic knowledge on top of [`pg_strtok`].
///
/// The `LEFT_BRACE` case yields a `{LABEL ...}`-framed node via `parseNodeString`
/// (readfuncs). The bare value-node tokens (`Integer`/`Float`/`Boolean`/`String`/
/// `BitString`) and a `(node node ...)` node list ARE arms of the unified
/// `types_nodes::nodes::Node` enum, so they are reconstructed here directly,
/// faithfully mirroring C's `makeInteger`/`makeFloat`/.../`lappend` cases. The
/// `(i ...)`/`(o ...)`/`(x ...)` scalar lists and the `(b ...)` Bitmapset carry
/// `IntList`/`OidList`/`XidList`/`Bitmapset` types that the split enum does not
/// model as top-level variants (reached only as typed sub-fields by the still-
/// unported readfuncs recursion), so those discriminator arms error as C would
/// treat an unreconstructable list at this level.
///
/// `token`/`pre_read` mirror C's optional pre-scanned first token: pass `None`
/// to read one (the external/top-level contract). `Ok(None)` is C's `NULL`
/// result: either end of input, or the explicit `<>` null-pointer token.
pub fn node_read<'mcx>(mcx: Mcx<'mcx>, pre_read: Option<Token<'_>>) -> PgResult<Option<PgBox<'mcx, Node<'mcx>>>> {
    // C: if (token == NULL) { token = pg_strtok(&tok_len); if (token == NULL) return NULL; }
    let tok = match pre_read {
        Some(t) => t,
        None => match pg_strtok() {
            None => return Ok(None), // end of input
            Some(tok) => tok,
        },
    };

    // C: nodeTokenType(token, tok_len). The "<>" empty token (tok_len == 0)
    // classifies as OTHER_TOKEN in C and is handled below as the null pointer;
    // node_token_type must not index an empty slice, so detect it up front.
    if tok.is_empty() {
        // C OTHER_TOKEN + tok_len == 0: "must be \"<>\" --- represents a null
        // pointer".
        return Ok(None);
    }

    let typ = node_token_type(tok.bytes);

    match typ {
        TokenType::LeftBrace => {
            // C: result = parseNodeString(); then expect a closing '}'.
            // parseNodeString (readfuncs) reads the node body off the shared
            // cursor and recurses back into pg_strtok/nodeRead for sub-fields.
            let result = backend_nodes_readfuncs_seams::parse_node_string::call(mcx)?;
            let close = pg_strtok();
            match close {
                Some(tok) if tok.len() >= 1 && tok.bytes[0] == b'}' => {}
                _ => return Err(elog_error("did not find '}' at end of input node")),
            }
            Ok(Some(result))
        }
        TokenType::RightParen => {
            // C: elog(ERROR, "unexpected right parenthesis");
            Err(elog_error("unexpected right parenthesis"))
        }
        TokenType::LeftParen => {
            // C (read.c): a `(`-opened list. The discriminator is the next
            // token: `i`/`o`/`x` are scalar Int/OID/XID lists, `b` is a
            // Bitmapset, anything else opens a list of nodes.
            //
            // The scalar `(i ...)`/`(o ...)`/`(x ...)` lists and the `(b ...)`
            // Bitmapset carry the `IntList`/`OidList`/`XidList`/`Bitmapset`
            // types, which the split `types_nodes::nodes::Node` enum does not
            // model as variants (they are reached only as typed sub-fields read
            // by the readfuncs recursion, still unported) — so they cannot be
            // reconstructed as a top-level `Node` here. A `(node node ...)` list,
            // however, IS a `Node::List`, so it is reconstructed faithfully.
            let disc = match pg_strtok() {
                None => return Err(elog_error("unterminated List structure")),
                Some(t) => t,
            };
            if disc.len() == 1
                && (disc.bytes[0] == b'i' || disc.bytes[0] == b'o' || disc.bytes[0] == b'x')
            {
                // C: the scalar Int/OID/XID list arms (lappend_int/_oid/_xid).
                Err(elog_error(
                    "scalar list (i/o/x) is a typed sub-field read by readfuncs, \
                     not modeled as a top-level Node",
                ))
            } else if disc.len() == 1 && disc.bytes[0] == b'b' {
                // C: the `(b ...)` Bitmapset arm (see also _readBitmapset).
                Err(elog_error(
                    "bitmapset is a typed sub-field read by readfuncs, not modeled as a top-level Node",
                ))
            } else {
                // C: "List of other node types". `disc` is already the first
                // element's first token (C has "already scanned next token").
                // Loop: while the current token is not ')', nodeRead it and
                // append, then scan the next token.
                let mut elements: PgVec<'mcx, PgBox<'mcx, Node<'mcx>>> =
                    mcx::vec_with_capacity_in(mcx, 0)?;
                let mut cur = disc;
                loop {
                    // C: if (token[0] == ')') break;  (the closing paren)
                    if cur.len() == 1 && cur.bytes[0] == b')' {
                        break;
                    }
                    // C: l = lappend(l, nodeRead(token, tok_len));
                    let child = node_read(mcx, Some(cur))?;
                    let boxed = match child {
                        // A `<>` element is the C NULL list element. The owned
                        // `List` cell type is a non-null `PgBox<Node>`; a NULL
                        // element is not representable, so reject it (no live
                        // `nodeToString` of a `Node::List` emits a NULL cell).
                        None => {
                            return Err(elog_error(
                                "null element in node list is not representable",
                            ))
                        }
                        Some(node) => node,
                    };
                    elements.push(boxed);
                    // C: token = pg_strtok(&tok_len); if (token == NULL) error.
                    cur = match pg_strtok() {
                        None => return Err(elog_error("unterminated List structure")),
                        Some(t) => t,
                    };
                }
                let node = mcx::alloc_in(mcx, Node::mk_list(mcx, elements)?)?;
                Ok(Some(node))
            }
        }
        TokenType::Other => {
            // C: in the OTHER_TOKEN arm, tok_len == 0 is the "<>" null pointer
            // (handled above before classifying); a non-empty Other token is
            // "unrecognized token: \"%.*s\"".
            let text = String::from_utf8_lossy(tok.bytes);
            Err(elog_error(format!("unrecognized token: \"{}\"", text)))
        }
        TokenType::Integer => {
            // C: result = makeInteger(atoi(token));
            // atoi stops at the first non-digit; the token is a clean integer
            // literal here (nodeTokenType classified it as T_Integer). Match
            // atoi's saturating/prefix behaviour by parsing the leading int.
            let s = String::from_utf8_lossy(tok.bytes);
            let ival = atoi_i32(&s);
            let node =
                mcx::alloc_in(mcx, Node::mk_integer(mcx, types_nodes::value::Integer { ival })?)?;
            Ok(Some(node))
        }
        TokenType::Float => {
            // C: fval = palloc(tok_len + 1); memcpy(token); makeFloat(fval).
            // The numeric literal is kept verbatim as its source string.
            let s = String::from_utf8_lossy(tok.bytes);
            let fval = PgString::from_str_in(&s, mcx)?;
            let node = mcx::alloc_in(mcx, Node::mk_float(mcx, types_nodes::value::Float { fval })?)?;
            Ok(Some(node))
        }
        TokenType::Boolean => {
            // C: result = makeBoolean(token[0] == 't');
            let boolval = tok.bytes[0] == b't';
            let node =
                mcx::alloc_in(mcx, Node::mk_boolean(mcx, types_nodes::value::Boolean { boolval })?)?;
            Ok(Some(node))
        }
        TokenType::String => {
            // C: makeString(debackslash(token + 1, tok_len - 2)) — strip the
            // surrounding quotes, then de-escape the inner content. The token is
            // `"..."` with len >= 2 (nodeTokenType requires a leading + trailing
            // quote with length > 1; the minimal `""` has len 2 -> empty inner).
            let inner = &tok.bytes[1..tok.len() - 1];
            let sval_s = debackslash(inner);
            let sval = PgString::from_str_in(&sval_s, mcx)?;
            let node =
                mcx::alloc_in(mcx, Node::mk_string(mcx, types_nodes::value::StringNode { sval })?)?;
            Ok(Some(node))
        }
        TokenType::BitString => {
            // C: makeBitString(debackslash(token, tok_len)) — no quotes to strip
            // (the leading 'b'/'x' is part of the value), just de-escape.
            let bsval_s = debackslash(tok.bytes);
            let bsval = PgString::from_str_in(&bsval_s, mcx)?;
            let node =
                mcx::alloc_in(mcx, Node::mk_bit_string(mcx, types_nodes::value::BitString { bsval })?)?;
            Ok(Some(node))
        }
    }
}

/// `atoi` over a `&str` (C `<stdlib.h>` `atoi`): parse the leading optional
/// sign + digit run as an `i32`, stopping at the first non-digit, and clamp on
/// overflow (C's `atoi` is undefined on overflow; a clean `nodeTokenType`
/// T_Integer token is in range, so the saturating fallback is never hit on
/// well-formed input). Returns 0 when there is no leading integer (atoi).
fn atoi_i32(s: &str) -> i32 {
    let bytes = s.as_bytes();
    let mut i = 0;
    let n = bytes.len();
    let neg = if i < n && (bytes[i] == b'+' || bytes[i] == b'-') {
        let neg = bytes[i] == b'-';
        i += 1;
        neg
    } else {
        false
    };
    let start = i;
    while i < n && bytes[i].is_ascii_digit() {
        i += 1;
    }
    if i == start {
        return 0;
    }
    let digits = &s[..i];
    match digits.parse::<i32>() {
        Ok(v) => v,
        Err(_) => {
            if neg {
                i32::MIN
            } else {
                i32::MAX
            }
        }
    }
}

// ---------------------------------------------------------------------------
// stringToNode entry points
// ---------------------------------------------------------------------------

/// RAII guard that save/restores the thread-local `pg_strtok` cursor across a
/// scan, mirroring C's `save_strtok = pg_strtok_ptr; ...; pg_strtok_ptr =
/// save_strtok;` re-entrancy dance.
struct StrtokGuard {
    saved_base: *const u8,
    saved_len: usize,
    saved_pos: usize,
}

impl StrtokGuard {
    /// Point the cursor at `s` (C: `pg_strtok_ptr = str`).
    fn install(s: &str) -> Self {
        let saved_base = PG_STRTOK_BASE.with(|c| c.get());
        let saved_len = PG_STRTOK_LEN.with(|c| c.get());
        let saved_pos = PG_STRTOK_POS.with(|c| c.get());
        PG_STRTOK_BASE.with(|c| c.set(s.as_ptr()));
        PG_STRTOK_LEN.with(|c| c.set(s.len()));
        PG_STRTOK_POS.with(|c| c.set(0));
        StrtokGuard {
            saved_base,
            saved_len,
            saved_pos,
        }
    }
}

impl Drop for StrtokGuard {
    fn drop(&mut self) {
        PG_STRTOK_BASE.with(|c| c.set(self.saved_base));
        PG_STRTOK_LEN.with(|c| c.set(self.saved_len));
        PG_STRTOK_POS.with(|c| c.set(self.saved_pos));
    }
}

/// `stringToNodeInternal(const char *str, bool restore_loc_fields)` (read.c).
///
/// Save/restore the `pg_strtok` cursor around `nodeRead(NULL, 0)`, so the read
/// is re-entrant. `restore_loc_fields` corresponds to the
/// `DEBUG_NODE_TESTS_ENABLED` location-field flag; it is plumbed to the
/// readfuncs recursion in debug builds (a no-op here, as that build flag is
/// off, matching the production C path).
fn string_to_node_internal<'mcx>(
    mcx: Mcx<'mcx>,
    s: &str,
    _restore_loc_fields: bool,
) -> PgResult<PgBox<'mcx, Node<'mcx>>> {
    let _guard = StrtokGuard::install(s);
    let retval = node_read(mcx, None)?;
    // C returns the void* as-is (possibly NULL for empty input or a top-level
    // "<>"). The `string_to_node` seam contract is a non-null `PgBox<Node>`,
    // and its callers only ever pass a real node rendering (a stored default
    // expression is never empty / "<>"), so a NULL top-level result is a
    // malformed input here.
    match retval {
        Some(node) => Ok(node),
        None => Err(elog_error("unexpected null/empty node string")),
    }
}

/// `stringToNode(const char *str)` (read.c) — the externally visible entry:
/// build a node tree from its `nodeToString` representation (assumed valid),
/// allocated in `mcx`.
pub fn string_to_node<'mcx>(mcx: Mcx<'mcx>, s: &str) -> PgResult<PgBox<'mcx, Node<'mcx>>> {
    string_to_node_internal(mcx, s, false)
}

/// `stringToNode(str)` (read.c) faithful to C's *nullable* `void *` return: a
/// top-level `<>` / empty input renders C's NULL pointer (`nodeRead` returns
/// NULL), here `Ok(None)`. This is the shape the catalog read paths need when a
/// stored `pg_node_tree` can legitimately be a null pointer — e.g. an
/// unconditional rule's `pg_rewrite.ev_qual` (`<>`), a policy with no qual, or a
/// dropped/empty default — where C does `node = stringToNode(text)` and keeps
/// the resulting NULL. The non-`Option` [`string_to_node`] entry is for callers
/// whose input is guaranteed to be a real node rendering.
pub fn string_to_node_opt<'mcx>(
    mcx: Mcx<'mcx>,
    s: &str,
) -> PgResult<Option<PgBox<'mcx, Node<'mcx>>>> {
    let _guard = StrtokGuard::install(s);
    node_read(mcx, None)
}

/// `stringToNodeWithLocations(const char *str)` (read.c, under
/// `DEBUG_NODE_TESTS_ENABLED`) — like [`string_to_node`] but instructing the
/// readfuncs recursion to restore location fields rather than reset them to
/// -1. In a non-debug build this is identical to [`string_to_node`]; provided
/// for API completeness.
pub fn string_to_node_with_locations<'mcx>(
    mcx: Mcx<'mcx>,
    s: &str,
) -> PgResult<PgBox<'mcx, Node<'mcx>>> {
    string_to_node_internal(mcx, s, true)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Drive `pg_strtok` over a string by installing the cursor directly,
    /// collecting (token-as-utf8, len) pairs until exhaustion.
    fn tokenize(s: &str) -> Vec<(String, usize)> {
        let _g = StrtokGuard::install(s);
        let mut out = Vec::new();
        while let Some(tok) = pg_strtok() {
            out.push((String::from_utf8_lossy(tok.bytes).into_owned(), tok.len()));
        }
        out
    }

    #[test]
    fn strtok_braces_and_parens_are_single_tokens() {
        let toks = tokenize("{CONST :consttype 23}");
        let names: Vec<&str> = toks.iter().map(|(t, _)| t.as_str()).collect();
        assert_eq!(names, vec!["{", "CONST", ":consttype", "23", "}"]);
    }

    #[test]
    fn strtok_whitespace_separates() {
        let toks = tokenize("  a\tb\nc  ");
        let names: Vec<&str> = toks.iter().map(|(t, _)| t.as_str()).collect();
        assert_eq!(names, vec!["a", "b", "c"]);
    }

    #[test]
    fn strtok_empty_token_is_zero_length() {
        let toks = tokenize("<>");
        assert_eq!(toks.len(), 1);
        assert_eq!(toks[0].1, 0); // length == 0 for "<>"
        assert_eq!(toks[0].0, ""); // zero-length slice
    }

    #[test]
    fn strtok_backslash_quotes_special_chars() {
        // "\(" and "\ " and "\\" are part of one token (backslashes retained).
        let toks = tokenize(r"a\(b\ c\\d");
        assert_eq!(toks.len(), 1);
        assert_eq!(toks[0].0, r"a\(b\ c\\d");
    }

    #[test]
    fn strtok_returns_none_at_end() {
        let _g = StrtokGuard::install("");
        assert!(pg_strtok().is_none());
    }

    #[test]
    fn debackslash_removes_protective_backslashes() {
        assert_eq!(debackslash(br"a\(b\ c"), "a(b c");
        assert_eq!(debackslash(br"\\"), r"\");
        // Trailing lone backslash (length == 1 after the last char): C keeps it
        // because the "length > 1" guard fails on the final backslash.
        assert_eq!(debackslash(br"x\"), "x\\");
        assert_eq!(debackslash(b"plain"), "plain");
    }

    #[test]
    fn token_type_integer_vs_float() {
        assert_eq!(node_token_type(b"123"), TokenType::Integer);
        assert_eq!(node_token_type(b"-123"), TokenType::Integer);
        assert_eq!(node_token_type(b"+5"), TokenType::Integer);
        assert_eq!(node_token_type(b"1.5"), TokenType::Float);
        assert_eq!(node_token_type(b".5"), TokenType::Float);
        assert_eq!(node_token_type(b"1e10"), TokenType::Float);
        // Out-of-range integer is a Float (ERANGE path).
        assert_eq!(node_token_type(b"99999999999999999999"), TokenType::Float);
    }

    #[test]
    fn token_type_specials_and_keywords() {
        assert_eq!(node_token_type(b"("), TokenType::LeftParen);
        assert_eq!(node_token_type(b")"), TokenType::RightParen);
        assert_eq!(node_token_type(b"{"), TokenType::LeftBrace);
        assert_eq!(node_token_type(b"true"), TokenType::Boolean);
        assert_eq!(node_token_type(b"false"), TokenType::Boolean);
        assert_eq!(node_token_type(b"\"hi\""), TokenType::String);
        assert_eq!(node_token_type(b"b101"), TokenType::BitString);
        assert_eq!(node_token_type(b"xFF"), TokenType::BitString);
        assert_eq!(node_token_type(b"CONST"), TokenType::Other);
        // 'truest' is not the keyword 'true' (length check).
        assert_eq!(node_token_type(b"truest"), TokenType::Other);
    }
}
