#![no_std]
//! `backend-nodes-node-support` — runtime support traits for the
//! `#[derive(PgNode)]` macro (see the `backend-nodes-macros` crate) and the
//! central node-tree generator.
//!
//! A `proc-macro = true` crate may only export the macros themselves, so the
//! traits and the container/leaf impls that the *generated* code calls into must
//! live in an ordinary library crate. This is that crate. The generated code
//! refers to these items as `::backend_nodes_node_support::PgNodeCopy` etc.
//!
//! This crate is `#![no_std]` (it only needs `alloc` for the few owned-`Vec`
//! helpers, but otherwise allocates exclusively through the charged `mcx`
//! containers) so that the `#![no_std]` `types-nodes` crate can depend on it
//! without dragging in `std`. It is a LEAF crate: it depends only on `mcx`
//! (memory contexts + charged containers) and `types-error` (the `PgResult`
//! error lane), NOT on `types-nodes`, which is what lets `types-nodes` depend on
//! it without forming a cycle (the central `Node` enum is generated INTO
//! `types-nodes` itself).
//!
//! # Uniform dispatch (mirrors the C COPY_*/COMPARE_* macro families)
//!
//! Every node field — scalar or child — is copied with
//! `field.copy_node_in(dst)?` and compared with `a.equal_node(&b)`. The two
//! trait families are populated by:
//!
//! * **Container impls** (`PgBox<T>`, `Option<T>`, `PgVec<T>`) that forward
//!   through their element so child links recurse automatically, re-homing the
//!   allocation onto the TARGET context. This is `COPY_NODE_FIELD` /
//!   `COMPARE_NODE_FIELD` over a `Node *` / `List *`.
//! * **Node-struct impls**, generated per struct by `#[derive(PgNode)]`, which
//!   recurse field-by-field.
//! * **Scalar-leaf impls**, supplied by the owning crate (here for the Rust
//!   primitives + `Oid` and friends, and in `types-nodes` via the
//!   [`pg_scalar_eq!`] / [`pg_scalar_ignore!`] helpers for the many node-local
//!   enums / aliases / opaque handles). These collapse to `clone()` / a per-type
//!   scalar equality, exactly like `COPY_SCALAR_FIELD` / `COMPARE_SCALAR_FIELD`.
//!
//! # Why copy is FALLIBLE and threads a target context
//!
//! C's `copyObject` deep-copies an arbitrary node tree into
//! `CurrentMemoryContext`; the copy can fail (OOM → `ereport(ERROR)`). The
//! owned-tree port re-homes ALL allocation onto `mcx`: a copy allocates a fresh
//! `PgVec`/`PgBox`/`PgString` charged to the explicit destination context
//! `dst`, and the call returns `PgResult` so an allocation failure surfaces as
//! the destination context's OOM error — exactly the contract of the existing
//! ~42 hand-written `clone_in` methods. The associated type `Bound<'dst>` is the
//! node value re-parameterized to live in `dst` (`Foo<'mcx>` copies to
//! `Foo<'dst>`; a lifetime-free leaf copies to itself).

extern crate alloc;

use alloc::string::String;
use mcx::{PgBox, PgString, PgVec};

// `Mcx` (the target context handle) and `PgResult` (the fallible-copy error
// lane) are re-exported under this crate's path so the `#[derive(PgNode)]`-
// generated code (expanded in the downstream `types-nodes` crate) and the
// exported `pg_scalar_*!` macros can name them as
// `::backend_nodes_node_support::Mcx` / `::PgResult` without bringing `mcx` /
// `types-error` into scope themselves.
pub use mcx::Mcx;
pub use types_error::PgResult;

/// Deep-copy a node value INTO a target memory context. Fallible owned-tree
/// analogue of `copyObject` (`copyfuncs.c`), which deep-copies into
/// `CurrentMemoryContext`; here the destination context is threaded explicitly
/// as `dst` and the copy allocates against it.
///
/// `Bound<'dst>` is `Self` re-parameterized to live in `dst`: a node value
/// `Foo<'mcx>` has `Bound<'dst> = Foo<'dst>`, and a lifetime-free leaf has
/// `Bound<'dst> = Self`. The matching `#[derive(PgNode)]` impl sets `Bound`
/// accordingly.
pub trait PgNodeCopy {
    /// The copied value, re-homed to the destination context's lifetime.
    type Bound<'dst>;
    /// Deep-copy `self` into `dst`, allocating the copy there. Fallible: a
    /// charged allocation can hit the context's limit (the C `ereport(ERROR)`
    /// on OOM).
    fn copy_node_in<'dst>(&self, dst: Mcx<'dst>) -> PgResult<Self::Bound<'dst>>;
}

/// Structural equality of two node values. Owned-tree analogue of `equal()`
/// (`equalfuncs.c`). Infallible and lifetime-agnostic (equality never
/// allocates and is invisible to the memory context the values live in).
pub trait PgNodeEqual {
    fn equal_node(&self, other: &Self) -> bool;
}

/// Opt the Rust primitive leaf types into `PgNodeCopy`/`PgNodeEqual` with a flat
/// `clone()` / `==`. `COPY_SCALAR_FIELD`/`COPY_STRING_FIELD` and
/// `COMPARE_SCALAR_FIELD`/`COMPARE_STRING_FIELD` collapse here. A scalar leaf is
/// lifetime-free, so `Bound<'dst> = Self` and the copy never touches `dst` (a
/// flat value lives wherever its owning node does). These are per-type
/// *concrete* impls (NOT a blanket over a marker) — a blanket over a
/// downstream-implementable marker trait would coherence-conflict with the
/// generic container impls below (`PgBox<T>`/`PgVec<T>`/…), since the compiler
/// cannot prove a downstream type won't be both. Per-type impls sidestep that.
macro_rules! impl_scalar_leaf {
    ($($t:ty),* $(,)?) => {
        $(
            impl PgNodeCopy for $t {
                type Bound<'dst> = Self;
                #[inline]
                fn copy_node_in<'dst>(&self, _dst: Mcx<'dst>) -> PgResult<Self> {
                    Ok(::core::clone::Clone::clone(self))
                }
            }
            impl PgNodeEqual for $t {
                #[inline]
                fn equal_node(&self, other: &Self) -> bool { self == other }
            }
        )*
    };
}

// The Rust primitive scalars. `Oid` and its friends (`Index`, `AttrNumber`,
// `SubTransactionId`, `RepOriginId`, …) are integer *aliases* in `types-core`
// (`Oid = u32`, `Index = u32`, `AttrNumber = i16`, …), so these primitive impls
// ARE their impls — a separate `impl PgNodeCopy for Oid` would coherence-collide
// with the `u32` impl. (Node-local *enums* / newtypes opt in from `types-nodes`
// via `pg_scalar_eq!` / `pg_scalar_ignore!`.)
impl_scalar_leaf!(
    i8, i16, i32, i64, i128, isize, u8, u16, u32, u64, u128, usize, bool, char, f32, f64
);

/// Opt a list of *leaf* field types into `PgNodeCopy`/`PgNodeEqual` directly,
/// with `clone()` copy and **`==`** equality — ordinary value leaves
/// (`COMPARE_SCALAR_FIELD`). Use this from the owning crate (`types-nodes`) for
/// every node-struct field type that is a plain value (node-local enums, small
/// `Copy`/`PartialEq` structs/newtypes).
///
/// These emit *concrete* `impl PgNodeCopy for T` / `impl PgNodeEqual for T` (not
/// a blanket), so they never collide with the generic container impls. The
/// listed type must be `Clone + PartialEq` and lifetime-free (`Bound<'dst> =
/// Self`).
///
/// ```ignore
/// pg_scalar_eq!(crate::AggStrategy, crate::ItemPointerData);
/// ```
#[macro_export]
macro_rules! pg_scalar_eq {
    ($($t:ty),* $(,)?) => {
        $(
            impl $crate::PgNodeCopy for $t {
                type Bound<'dst> = Self;
                #[inline]
                fn copy_node_in<'dst>(
                    &self,
                    _dst: $crate::Mcx<'dst>,
                ) -> $crate::PgResult<Self> {
                    ::core::result::Result::Ok(::core::clone::Clone::clone(self))
                }
            }
            impl $crate::PgNodeEqual for $t {
                #[inline]
                fn equal_node(&self, other: &Self) -> bool { self == other }
            }
        )*
    };
}

/// Opt a list of *leaf* field types into `PgNodeCopy`/`PgNodeEqual` with a real
/// `clone()` copy but **always-equal** (`true`) equality — the owned-tree
/// analogue of an `equal_ignore` field. Use this for opaque handles /
/// function-pointer aliases that `equalfuncs.c` skips and that cannot derive a
/// meaningful `PartialEq`. The listed type must be `Clone` and lifetime-free
/// (`Bound<'dst> = Self`).
///
/// ```ignore
/// pg_scalar_ignore!(crate::ExprStateEvalFunc);
/// ```
#[macro_export]
macro_rules! pg_scalar_ignore {
    ($($t:ty),* $(,)?) => {
        $(
            impl $crate::PgNodeCopy for $t {
                type Bound<'dst> = Self;
                #[inline]
                fn copy_node_in<'dst>(
                    &self,
                    _dst: $crate::Mcx<'dst>,
                ) -> $crate::PgResult<Self> {
                    ::core::result::Result::Ok(::core::clone::Clone::clone(self))
                }
            }
            impl $crate::PgNodeEqual for $t {
                #[inline]
                fn equal_node(&self, _other: &Self) -> bool { true }
            }
        )*
    };
}

// Re-export the leaf macros' helper types under a stable path so the exported
// macros above (expanded in the downstream `types-nodes` crate) can name `Mcx`
// and `PgResult` without the downstream crate having to bring them into scope.
/// `palloc`-shaped fallible `PgVec` constructor, re-exported under a stable path
/// so the `#[derive(PgNode)]`-generated `array_size` copy code (expanded in the
/// downstream `types-nodes` crate) can build a destination-charged `PgVec`
/// without naming `mcx` directly. Forwards to [`mcx::vec_with_capacity_in`].
#[doc(hidden)]
#[inline]
pub fn mcx_vec_with_capacity_in<'dst, T>(
    dst: Mcx<'dst>,
    cap: usize,
) -> PgResult<PgVec<'dst, T>> {
    mcx::vec_with_capacity_in(dst, cap)
}

// ---------------------------------------------------------------------------
// Container impls — these RECURSE through their element via `copy_node_in` /
// `equal_node`, re-homing the allocation onto the TARGET context, so a
// `PgBox<Node>` / `PgVec<Node>` / `Option<PgBox<Node>>` child link deep-copies
// and deep-compares into `dst`.
// ---------------------------------------------------------------------------

/// `Option<T>` field: copy/compare element-wise. Used for nullable scalars and
/// for `Option<PgBox<Node>>` node children (a possibly-NULL `Node *` in C). The
/// `None` arm is the C NULL pointer (copies to NULL without allocating).
impl<T: PgNodeCopy> PgNodeCopy for Option<T> {
    type Bound<'dst> = Option<T::Bound<'dst>>;
    fn copy_node_in<'dst>(&self, dst: Mcx<'dst>) -> PgResult<Self::Bound<'dst>> {
        match self {
            Some(v) => Ok(Some(v.copy_node_in(dst)?)),
            None => Ok(None),
        }
    }
}
impl<T: PgNodeEqual> PgNodeEqual for Option<T> {
    fn equal_node(&self, other: &Self) -> bool {
        match (self, other) {
            (Some(a), Some(b)) => a.equal_node(b),
            (None, None) => true,
            _ => false,
        }
    }
}

/// `PgBox<T>` field — a charged `Node *` child link. Deep-copy the payload into
/// `dst` and box it in a fresh `PgBox` charged to `dst` (the owned-tree analogue
/// of `copyObject`'s `palloc` + recurse on a `Node *`). Compare through the box.
impl<'a, T: PgNodeCopy> PgNodeCopy for PgBox<'a, T> {
    type Bound<'dst> = PgBox<'dst, T::Bound<'dst>>;
    fn copy_node_in<'dst>(&self, dst: Mcx<'dst>) -> PgResult<Self::Bound<'dst>> {
        let payload = (**self).copy_node_in(dst)?;
        mcx::alloc_in(dst, payload)
    }
}
impl<'a, T: PgNodeEqual> PgNodeEqual for PgBox<'a, T> {
    fn equal_node(&self, other: &Self) -> bool {
        (**self).equal_node(&**other)
    }
}

/// `PgVec<T>` field — a charged node `List *`. Deep-copy each element into `dst`
/// (recursing through the element's `copy_node_in`), pushing into a fresh
/// `PgVec` charged to `dst` (the owned-tree analogue of `COPY_NODE_FIELD` over a
/// `List *`, which recurses into every element). Compares length then
/// element-wise (the charge is invisible to equality).
impl<'a, T: PgNodeCopy> PgNodeCopy for PgVec<'a, T> {
    type Bound<'dst> = PgVec<'dst, T::Bound<'dst>>;
    fn copy_node_in<'dst>(&self, dst: Mcx<'dst>) -> PgResult<Self::Bound<'dst>> {
        let mut out = mcx::vec_with_capacity_in(dst, self.len())?;
        for elem in self.iter() {
            out.push(elem.copy_node_in(dst)?);
        }
        Ok(out)
    }
}
impl<'a, T: PgNodeEqual> PgNodeEqual for PgVec<'a, T> {
    fn equal_node(&self, other: &Self) -> bool {
        self.len() == other.len() && self.iter().zip(other.iter()).all(|(a, b)| a.equal_node(b))
    }
}

/// `PgString` field — a charged `char *` (`COPY_STRING_FIELD`). Deep-copy the
/// string bytes into `dst` via `PgString::clone_in`; compare by contents.
impl<'a> PgNodeCopy for PgString<'a> {
    type Bound<'dst> = PgString<'dst>;
    fn copy_node_in<'dst>(&self, dst: Mcx<'dst>) -> PgResult<Self::Bound<'dst>> {
        self.clone_in(dst)
    }
}
impl<'a> PgNodeEqual for PgString<'a> {
    fn equal_node(&self, other: &Self) -> bool {
        self.as_str() == other.as_str()
    }
}

// ===========================================================================
// Serialization traits (nodeToString / stringToNode) — OUT/READ families.
//
// The owned-tree analogues of `outfuncs.c` (`_outNode` and the `WRITE_*_FIELD`
// macro family) and `readfuncs.c` / `read.c` (`nodeRead`, `pg_strtok`, and the
// `READ_*_FIELD` macro family). The emitted bytes MUST be byte-for-byte
// identical to what PostgreSQL's `outfuncs.c` writes so that a PostgreSQL
// `stringToNode` can round-trip them and vice-versa.
//
// At the field level a `WRITE_*_FIELD` macro emits `" :fldname VALUE"` — a
// leading space, a colon-prefixed field name, a space, then the per-type token.
// The trait surface here only deals with the *per-type token* (the `VALUE`
// part); the `#[derive(PgNode)]`-generated code is responsible for the
// `" :fldname "` framing around each `out_node`/`read_node` call, exactly as the
// macros split the per-field framing from the per-type writer.
//
// # The mcx / fallible asymmetry vs the C model (and vs copy)
//
// OUT only appends ASCII token bytes into a scratch `String` (the
// `appendStringInfo`/`StringInfo` analogue); it never allocates a node, never
// charges `mcx`, and never fails — so `out_node(&self, &mut String)` mirrors
// outfuncs.c verbatim.
//
// READ, like `copyObject`/`copy_node_in`, REBUILDS a node tree, so it re-homes
// ALL allocation onto a TARGET context `dst` and is fallible (a charged alloc
// can OOM → the C `ereport(ERROR)` that `stringToNode` raises). It therefore
// threads `dst: Mcx<'dst>` and returns `PgResult<Self::Bound<'dst>>` with the
// SAME `Bound<'dst>` associated type as [`PgNodeCopy`] (a node `Foo<'mcx>` reads
// to `Foo<'dst>`; a lifetime-free leaf reads to `Self`). C's `stringToNode`
// allocates against `CurrentMemoryContext`; here the destination is explicit.
// ===========================================================================

/// Serialize a node value to `outfuncs.c`'s textual token form. Owned-tree
/// analogue of `_outNode` / the `WRITE_*` writers in `outfuncs.c`.
///
/// Implementors append *only* the per-type token bytes to `buf` (no leading
/// space, no `:fldname`). The generated per-struct code supplies the field
/// framing, mirroring how `WRITE_INT_FIELD(x)` expands to
/// `appendStringInfo(str, " :x %d", node->x)` — the `" :x "` is framing, the
/// `%d` is what this trait produces. `buf` is a plain `alloc::String` scratch
/// buffer (the `StringInfo` analogue): OUT never allocates a node, never charges
/// `mcx`, and never fails.
pub trait PgNodeOut {
    fn out_node(&self, buf: &mut String);
}

/// Parse a node value back from the `pg_strtok` token stream. Owned-tree
/// analogue of `nodeRead` / the `READ_*` readers in `readfuncs.c`.
///
/// Like [`PgNodeCopy::copy_node_in`], reading REBUILDS a node tree, so it
/// re-homes all allocation onto the target context `dst` and is fallible (a
/// charged allocation can OOM, the C `ereport(ERROR)` `stringToNode` raises).
/// `Bound<'dst>` is `Self` re-parameterized to live in `dst` (a node value
/// `Foo<'mcx>` reads to `Foo<'dst>`; a lifetime-free leaf reads to `Self`),
/// matching the `PgNodeCopy` convention.
///
/// Implementors consume exactly the tokens that the matching [`PgNodeOut`]
/// produced. The generated per-struct code skips the `:fldname` token (as
/// `READ_INT_FIELD` does with its first `pg_strtok` call) before invoking
/// `read_node` for the value.
pub trait PgNodeRead {
    /// The read value, re-homed to the destination context's lifetime.
    type Bound<'dst>;
    /// Parse `self`'s value from `cur`, allocating any rebuilt node storage in
    /// `dst`. Fallible: a charged allocation can hit the context's limit.
    fn read_node<'dst>(
        cur: &mut ReadCursor<'_>,
        dst: Mcx<'dst>,
    ) -> PgResult<Self::Bound<'dst>>;
}

/// A cursor over a `pg_strtok`-style token stream — the owned-tree analogue of
/// `read.c`'s `pg_strtok_ptr` static plus `pg_strtok()`.
///
/// Tokenization rules are transcribed exactly from `pg_strtok` (read.c:152-206):
///
/// * whitespace (space, `\n`, `\t`) separates tokens and is skipped,
/// * `(`, `)`, `{`, `}` are each a single-character token even with no
///   surrounding whitespace,
/// * otherwise a token runs to the next whitespace / special char, with `\`
///   quoting the following character (advance by 2),
/// * a `<>` token is reported as a *non-NULL, length-0* token (the NULL-string
///   sentinel), distinct from end-of-input which is `None`.
pub struct ReadCursor<'a> {
    s: &'a [u8],
    pos: usize,
}

/// One token returned by [`ReadCursor::next_token`]. `text` still contains any
/// embedded backslashes (matching `pg_strtok`, which "doesn't remove
/// backslashes; the caller must do so"). `is_empty_sentinel` is set for the
/// `<>` token, which `pg_strtok` reports with length 0.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Token<'a> {
    pub text: &'a str,
    pub is_empty_sentinel: bool,
}

impl<'a> ReadCursor<'a> {
    /// Wrap a token stream produced by the OUT side (or by PostgreSQL's
    /// `nodeToString`). Analogue of pointing `pg_strtok_ptr` at a string.
    #[inline]
    pub fn new(s: &'a str) -> Self {
        ReadCursor {
            s: s.as_bytes(),
            pos: 0,
        }
    }

    /// Retrieve the next token, or `None` at end of input. Exact transcription
    /// of `pg_strtok` (read.c:152-206).
    pub fn next_token(&mut self) -> Option<Token<'a>> {
        let b = self.s;
        let mut i = self.pos;

        // Skip leading whitespace: space, '\n', '\t'.
        while i < b.len() && (b[i] == b' ' || b[i] == b'\n' || b[i] == b'\t') {
            i += 1;
        }

        if i >= b.len() {
            self.pos = i;
            return None; // no more tokens
        }

        let start = i;
        let c = b[i];
        if c == b'(' || c == b')' || c == b'{' || c == b'}' {
            // special 1-character token
            i += 1;
        } else {
            // Normal token, possibly containing backslashes.
            while i < b.len() {
                let d = b[i];
                if d == b' ' || d == b'\n' || d == b'\t' || d == b'(' || d == b')' || d == b'{'
                    || d == b'}'
                {
                    break;
                }
                if d == b'\\' && i + 1 < b.len() {
                    i += 2;
                } else {
                    i += 1;
                }
            }
        }

        let mut len = i - start;
        self.pos = i;

        // Recognize special case for the "empty"/NULL token "<>".
        let is_empty_sentinel = len == 2 && b[start] == b'<' && b[start + 1] == b'>';
        if is_empty_sentinel {
            len = 0;
        }

        // SAFETY: the byte slice came from a `&str`, and our token boundaries
        // never split a UTF-8 sequence: the separators we cut on (whitespace,
        // the four brace chars, '\\') are all single-byte ASCII, and a '\\'
        // escape advances past a whole following byte. So `start..start+len`
        // (and the sentinel's ASCII "<>" range) is a valid UTF-8 boundary.
        let text_bytes = &b[start..start + len];
        let text = unsafe { ::core::str::from_utf8_unchecked(text_bytes) };
        Some(Token {
            text,
            is_empty_sentinel,
        })
    }

    /// Read the next token, panicking at end of input. Analogue of the many
    /// `if (token == NULL) elog(ERROR, ...)` checks in readfuncs.
    #[inline]
    pub fn expect_token(&mut self) -> Token<'a> {
        self.next_token()
            .expect("unexpected end of node token stream")
    }

    /// Peek the next token *without* advancing the cursor. The owned-tree
    /// analogue of the dispatch in `parseNodeString` / a central `Node`
    /// `read_node`, which must look at the node LABEL (e.g. `OPEXPR`) to choose
    /// which per-struct reader to call, while leaving the `{ LABEL ...` tokens in
    /// place for that reader to consume itself.
    #[inline]
    pub fn peek_token(&mut self) -> Option<Token<'a>> {
        let save = self.pos;
        let tok = self.next_token();
        self.pos = save;
        tok
    }

    /// Capture the current position, for a manual peek-then-rewind. Pairs with
    /// [`ReadCursor::restore`]. Cross-crate analogue of saving `pg_strtok_ptr`.
    #[inline]
    pub fn save(&self) -> usize {
        self.pos
    }

    /// Restore a position captured by [`ReadCursor::save`].
    #[inline]
    pub fn restore(&mut self, pos: usize) {
        self.pos = pos;
    }

    /// Skip one token unconditionally — the owned-tree analogue of the
    /// `token = pg_strtok(&length); /* skip :fldname */` first line of every
    /// `READ_*_FIELD` macro.
    #[inline]
    pub fn skip_token(&mut self) {
        let _ = self.next_token();
    }

    /// Peek the *kind* of the next token without advancing, the owned-tree
    /// analogue of `nodeTokenType` (read.c:246-301). This is what `nodeRead`
    /// uses to dispatch a bare value-node token (an `Integer`/`Float`/
    /// `Boolean`/`String`/`BitString` literal that is NOT wrapped in `{LABEL
    /// ...}`) onto the right value-node reader. The `{`-braced node case is
    /// reported as [`ValueTokenKind::Brace`] (LEFT_BRACE); a `(`/`)` paren as
    /// [`ValueTokenKind::Paren`]; anything else as [`ValueTokenKind::Other`].
    ///
    /// Transcribed from `nodeTokenType`: a leading `+`/`-` then a digit, or a
    /// `.`+digit, is a number — `Integer` if it parses cleanly as an i32, else
    /// `Float`; `true`/`false` is `Boolean`; a `"`-quoted token (length > 1,
    /// trailing `"`) is `String`; a leading `b`/`x` is `BitString`.
    pub fn peek_value_token_kind(&mut self) -> Option<ValueTokenKind> {
        let save = self.pos;
        let tok = self.next_token();
        self.pos = save;
        let tok = tok?;
        // The `<>` empty sentinel is reported by its (empty) text below; the
        // sentinel flag itself doesn't change the kind classification.
        let s = tok.text;
        let bytes = s.as_bytes();
        if bytes.is_empty() {
            return Some(ValueTokenKind::Other);
        }
        // Number? (nodeTokenType:255-275)
        let (numptr, numlen) = if bytes[0] == b'+' || bytes[0] == b'-' {
            (&bytes[1..], bytes.len() - 1)
        } else {
            (bytes, bytes.len())
        };
        let is_number = (numlen > 0 && numptr[0].is_ascii_digit())
            || (numlen > 1 && numptr[0] == b'.' && numptr[1].is_ascii_digit());
        if is_number {
            // strtoint both syntax- and range-checks: an i32 parse that
            // consumes the entire numeric portion (no trailing chars, in range)
            // is T_Integer, else T_Float.
            return Some(if s.parse::<i32>().is_ok() {
                ValueTokenKind::Integer
            } else {
                ValueTokenKind::Float
            });
        }
        // Single-char structural tokens.
        if bytes[0] == b'(' || bytes[0] == b')' {
            return Some(ValueTokenKind::Paren);
        }
        if bytes[0] == b'{' {
            return Some(ValueTokenKind::Brace);
        }
        // true / false (nodeTokenType:287-289).
        if s == "true" || s == "false" {
            return Some(ValueTokenKind::Boolean);
        }
        // "..." quoted string (length > 1, leading + trailing '"').
        if bytes[0] == b'"' && bytes.len() > 1 && bytes[bytes.len() - 1] == b'"' {
            return Some(ValueTokenKind::String);
        }
        // b... / x... bit string.
        if bytes[0] == b'b' || bytes[0] == b'x' {
            return Some(ValueTokenKind::BitString);
        }
        Some(ValueTokenKind::Other)
    }
}

/// The classification of a `pg_strtok` token, the owned-tree analogue of the
/// `NodeTag` returned by `nodeTokenType` (read.c). Used by a `custom_read_write`
/// reader (e.g. `A_Const`'s `val`) to dispatch a *bare* value-node token onto
/// the matching value-node reader, exactly as `nodeRead` does.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ValueTokenKind {
    /// `T_Integer` — an integral numeric literal.
    Integer,
    /// `T_Float` — a non-integral / out-of-range numeric literal.
    Float,
    /// `T_Boolean` — `true` / `false`.
    Boolean,
    /// `T_String` — a `"`-quoted token.
    String,
    /// `T_BitString` — a `b`/`x`-prefixed token.
    BitString,
    /// `LEFT_BRACE` — a `{`-opened braced node.
    Brace,
    /// `LEFT_PAREN` / `RIGHT_PAREN`.
    Paren,
    /// `OTHER_TOKEN` — anything else.
    Other,
}

// ---------------------------------------------------------------------------
// outToken / debackslash — string escaping shared by STRING and CHAR fields.
// ---------------------------------------------------------------------------

/// Append a plain (already-escaped) string fragment to the OUT buffer. The
/// `#[derive(PgNode)]`-generated `out_node` uses this to emit the `{LABEL`
/// opener, each field's `" :fldname "` framing, and the closing `}` — the
/// framing bytes that `appendStringInfoString` writes in outfuncs.c. It exists
/// so the generated code (which lives in a downstream crate) need not name
/// `alloc::string::String`'s push methods directly.
#[inline]
pub fn out_str(buf: &mut String, s: &str) {
    buf.push_str(s);
}

/// `outToken` (outfuncs.c:154-189). Encode an ordinary (non-NULL) string token
/// with the protective backslashes that `read.c`'s `pg_strtok` needs.
///
/// * empty string -> `""`
/// * a leading `<`, `"`, digit, or sign-before-digit/dot gets a protective
///   leading `\`
/// * any ` `, `\n`, `\t`, `(`, `)`, `{`, `}`, `\` anywhere is backslashed.
///
/// NULL is *not* handled here; the caller emits `<>` for a NULL `Option`.
fn out_token(buf: &mut String, s: &str) {
    if s.is_empty() {
        buf.push_str("\"\"");
        return;
    }
    let bytes = s.as_bytes();
    let first = bytes[0];
    // Leading-only protective backslash (outfuncs.c:174-179).
    let needs_lead = first == b'<'
        || first == b'"'
        || first.is_ascii_digit()
        || ((first == b'+' || first == b'-')
            && bytes.len() > 1
            && (bytes[1].is_ascii_digit() || bytes[1] == b'.'));
    if needs_lead {
        buf.push('\\');
    }
    for &c in bytes {
        // Chars that must be backslashed anywhere (outfuncs.c:183-185).
        if c == b' '
            || c == b'\n'
            || c == b'\t'
            || c == b'('
            || c == b')'
            || c == b'{'
            || c == b'}'
            || c == b'\\'
        {
            buf.push('\\');
        }
        buf.push(c as char);
    }
}

/// `debackslash` (read.c:213-228). Remove protective backslashes from a token,
/// returning the plain string.
fn debackslash(token: &str) -> String {
    let bytes = token.as_bytes();
    let mut out = String::new();
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'\\' && i + 1 < bytes.len() {
            i += 1;
        }
        out.push(bytes[i] as char);
        i += 1;
    }
    out
}

/// `outChar` (outfuncs.c:195-211). A `\0` becomes `<>`; any other char goes
/// through `out_token` (so it is escaped) as a one-character string.
fn out_char(buf: &mut String, c: char) {
    if c == '\0' {
        buf.push_str("<>");
        return;
    }
    let mut tmp = [0u8; 4];
    out_token(buf, c.encode_utf8(&mut tmp));
}

/// OUT side of a `WRITE_CHAR_FIELD` whose in-memory storage is an integer
/// (`u8`/`i8`) rather than a Rust `char`. PostgreSQL declares fields like
/// `RangeVar.relpersistence` as C `char` and serializes them with
/// `WRITE_CHAR_FIELD` (`outChar` — a one-character, possibly-escaped token, or
/// `<>` for `\0`), NOT as a `%u`/`%d` integer. The idiomatic node structs model
/// such fields as `u8`/`i8` (an ASCII code shared with non-node call sites), so
/// the `#[pg_node(char_as)]` field attribute routes them HERE — preserving the
/// integer storage while emitting the byte-identical `outChar` token. The
/// argument is the raw byte, rendered through the same `out_char` path the
/// `char` leaf uses.
#[inline]
pub fn out_char_field_byte(buf: &mut String, c: u8) {
    out_char(buf, c as char);
}

/// READ side of a `char_as` field (see [`out_char_field_byte`]). Reads the
/// one-character `outChar` token (or `<>`) the OUT side wrote and returns the
/// raw byte, the analogue of `READ_CHAR_FIELD` storing into an integer field.
#[inline]
pub fn read_char_field_byte(cur: &mut ReadCursor) -> u8 {
    read_char_token(cur.expect_token()) as u8
}

/// READ side of a CHAR field (readfuncs.c:96-100). Mirrors the inline logic
/// `(length == 0) ? '\0' : (token[0] == '\\' ? token[1] : token[0])`.
fn read_char_token(tok: Token<'_>) -> char {
    if tok.is_empty_sentinel || tok.text.is_empty() {
        return '\0';
    }
    let bytes = tok.text.as_bytes();
    if bytes[0] == b'\\' && bytes.len() > 1 {
        bytes[1] as char
    } else {
        bytes[0] as char
    }
}

// ---------------------------------------------------------------------------
// Scalar-leaf OUT/READ impls for the Rust primitives.
//
// Each primitive matches a specific WRITE_*_FIELD/READ_*_FIELD pair from
// outfuncs.c / readfuncs.c:
//   i16, i32      -> WRITE_INT_FIELD  "%d"          / READ_INT_FIELD  atoi
//   i64           -> WRITE_INT64_FIELD INT64_FORMAT / READ_INT64_FIELD strtoi64
//                    (also covers WRITE_LONG_FIELD "%ld" / READ_LONG_FIELD atol)
//   u32           -> WRITE_UINT_FIELD/WRITE_OID_FIELD "%u" / READ_UINT_FIELD atoui
//   u64           -> WRITE_UINT64_FIELD UINT64_FORMAT / READ_UINT64_FIELD strtou64
//   bool          -> WRITE_BOOL_FIELD "true"/"false" / READ_BOOL_FIELD strtobool
//   char          -> WRITE_CHAR_FIELD outChar       / READ_CHAR_FIELD
//
// A scalar leaf is lifetime-free (`Bound<'dst> = Self`) and READ never charges
// `dst` (a flat value lives wherever its owning node does), but the signature
// stays uniform with the node case so the central dispatch is one shape.
// ---------------------------------------------------------------------------

/// Signed integer leaf written as `%d` (WRITE_INT_FIELD) and read with `atoi`.
macro_rules! impl_int_leaf {
    ($($t:ty),* $(,)?) => {
        $(
            impl PgNodeOut for $t {
                #[inline]
                fn out_node(&self, buf: &mut String) {
                    // %d — plain signed decimal.
                    let _ = ::core::fmt::Write::write_fmt(buf, format_args!("{}", self));
                }
            }
            impl PgNodeRead for $t {
                type Bound<'dst> = Self;
                #[inline]
                fn read_node<'dst>(
                    cur: &mut ReadCursor<'_>,
                    _dst: Mcx<'dst>,
                ) -> PgResult<Self> {
                    // atoi/strtoi64/atol — base-10 parse of the value token.
                    Ok(cur.expect_token().text.parse::<$t>().expect("invalid integer token"))
                }
            }
        )*
    };
}
// i16: AttrNumber-ish; i32: INT; i64: INT64/LONG. Same %d / decimal text.
// i8: a `char`-width signed integer field written as a plain decimal `%d`.
impl_int_leaf!(i8, i16, i32, i64);

/// Unsigned integer leaf written as `%u` (WRITE_UINT_FIELD / WRITE_OID_FIELD,
/// also UINT64_FIELD) and read with `atoui`/`strtou64`. `Oid` is `u32` in the
/// types crates, so this `u32` impl IS the OID impl (a separate one would
/// coherence-conflict with `u32`).
macro_rules! impl_uint_leaf {
    ($($t:ty),* $(,)?) => {
        $(
            impl PgNodeOut for $t {
                #[inline]
                fn out_node(&self, buf: &mut String) {
                    // %u — plain unsigned decimal.
                    let _ = ::core::fmt::Write::write_fmt(buf, format_args!("{}", self));
                }
            }
            impl PgNodeRead for $t {
                type Bound<'dst> = Self;
                #[inline]
                fn read_node<'dst>(
                    cur: &mut ReadCursor<'_>,
                    _dst: Mcx<'dst>,
                ) -> PgResult<Self> {
                    Ok(cur.expect_token().text.parse::<$t>()
                        .expect("invalid unsigned integer token"))
                }
            }
        )*
    };
}
// u8: uint8-width; u16: uint16/RepOriginId-width; u32: UINT/OID/Index; u64:
// UINT64. All widen to the varargs unsigned type in `appendStringInfo`, so the
// textual form is a plain unsigned decimal.
impl_uint_leaf!(u8, u16, u32, u64);

// NOTE on FLOAT fields: `WRITE_FLOAT_FIELD` (outfuncs.c) renders a C `double`
// via `outDouble` -> `double_to_shortest_decimal_buf` (src/common/d2s.c, the
// Ryū shortest-round-trip algorithm). A byte-identical OUT requires that d2s
// port, which this workspace does not yet have (no `common-ryu` crate). The
// executor-reachable node subset this serialization layer currently serves (the
// value.h leaves: Integer/Float/Boolean/String/BitString, where the `Float`
// node stores its value as a STRING, not a `double`) has NO `f64`/`f32` field,
// so no float `PgNodeOut`/`PgNodeRead` impl is reachable today. Rather than emit
// a `core::fmt`-based float token that would silently DIVERGE from PostgreSQL's
// byte stream (breaking round-trip with a real `nodeToString`), the float leaf
// impls are intentionally omitted until the Ryū d2s port lands — the
// prerequisite for the F1 wave that adds `Cost`/`Selectivity` (f64) fields to
// `Plan`/`Path`. See the K1 phase-3 notes.

impl PgNodeOut for bool {
    #[inline]
    fn out_node(&self, buf: &mut String) {
        // booltostr (outfuncs.c:142) — "true"/"false".
        buf.push_str(if *self { "true" } else { "false" });
    }
}
impl PgNodeRead for bool {
    type Bound<'dst> = Self;
    #[inline]
    fn read_node<'dst>(cur: &mut ReadCursor<'_>, _dst: Mcx<'dst>) -> PgResult<Self> {
        // strtobool (readfuncs.c:185) — leading 't' => true, else false.
        Ok(cur.expect_token().text.as_bytes().first() == Some(&b't'))
    }
}

impl PgNodeOut for char {
    #[inline]
    fn out_node(&self, buf: &mut String) {
        out_char(buf, *self);
    }
}
impl PgNodeRead for char {
    type Bound<'dst> = Self;
    #[inline]
    fn read_node<'dst>(cur: &mut ReadCursor<'_>, _dst: Mcx<'dst>) -> PgResult<Self> {
        Ok(read_char_token(cur.expect_token()))
    }
}

// ---------------------------------------------------------------------------
// PgString leaf OUT/READ — WRITE_STRING_FIELD (outfuncs.c) via outToken and
// READ_STRING_FIELD (readfuncs.c) via nullable_string. A `PgString` (vs an
// `Option<PgString>`) is the non-NULL `char *` case, so OUT never emits `<>`.
// READ re-homes the rebuilt string onto the target context (stringToNode
// allocates against CurrentMemoryContext).
// ---------------------------------------------------------------------------

impl<'a> PgNodeOut for PgString<'a> {
    #[inline]
    fn out_node(&self, buf: &mut String) {
        out_token(buf, self.as_str());
    }
}
impl<'a> PgNodeRead for PgString<'a> {
    type Bound<'dst> = PgString<'dst>;
    fn read_node<'dst>(
        cur: &mut ReadCursor<'_>,
        dst: Mcx<'dst>,
    ) -> PgResult<PgString<'dst>> {
        // nullable_string (readfuncs.c:187-198) for the non-NULL cases: `""` is
        // the empty string, otherwise debackslash. A NULL (`<>`) maps to the
        // empty string here; use `Option<PgString>` to distinguish NULL.
        let tok = cur.expect_token();
        if tok.is_empty_sentinel {
            return PgString::from_str_in("", dst);
        }
        if tok.text == "\"\"" {
            return PgString::from_str_in("", dst);
        }
        // A `List*` of String NODES (e.g. `Alias.colnames`, flattened to a vec
        // of strings in the owned tree) is emitted by C `_outString` as a QUOTED
        // token (`("x")`), where a bare WRITE_STRING_FIELD leaf is not; nodeRead's
        // T_String arm strips the quotes (read.c:496-499). A leaf token can never
        // START with an unescaped `"` (outToken escapes it to `\"`), so a leading
        // raw quote unambiguously marks the String-node form: strip the
        // surrounding quotes, then debackslash the interior.
        let bytes = tok.text.as_bytes();
        let plain = if bytes.len() >= 2 && bytes[0] == b'"' && bytes[bytes.len() - 1] == b'"' {
            debackslash(&tok.text[1..tok.text.len() - 1])
        } else {
            debackslash(tok.text)
        };
        PgString::from_str_in(&plain, dst)
    }
}

/// Opt a list of *leaf* field types into `PgNodeOut`/`PgNodeRead` as **integer**
/// tokens (`%d` write, `atoi`-style read), via `as`-casts to/from `i64`. The
/// OUT/READ analogue of [`pg_scalar_eq!`]: use it from the owning crate
/// (`types-nodes`) for every node *enum*/integer-alias (which `WRITE_ENUM_FIELD`
/// serializes as `(int) value` — outfuncs.c:79-81 — and `READ_ENUM_FIELD` reads
/// back as `(enumtype) atoi(token)` — readfuncs.c:103-106).
///
/// The listed type must be `Copy`, lifetime-free, and convertible to/from `i64`
/// via `as` (a `#[repr(...)]` C-like enum or an integer alias).
///
/// ```ignore
/// pg_scalar_enum_out_read!(crate::AggStrategy, crate::JoinType, crate::CmdType);
/// ```
#[macro_export]
macro_rules! pg_scalar_enum_out_read {
    ($($t:ty),* $(,)?) => {
        $(
            impl $crate::PgNodeOut for $t {
                #[inline]
                fn out_node(&self, buf: &mut $crate::alloc_reexport::string::String) {
                    // WRITE_ENUM_FIELD: " :fld %d", (int) node->fld
                    let v: i64 = *self as i64;
                    let _ = ::core::fmt::Write::write_fmt(
                        buf, ::core::format_args!("{}", v));
                }
            }
            impl $crate::PgNodeRead for $t {
                type Bound<'dst> = Self;
                #[inline]
                fn read_node<'dst>(
                    cur: &mut $crate::ReadCursor<'_>,
                    _dst: $crate::Mcx<'dst>,
                ) -> $crate::PgResult<Self> {
                    // READ_ENUM_FIELD: (enumtype) atoi(token)
                    let v: i64 = cur.expect_token().text.parse::<i64>()
                        .expect("invalid enum token");
                    ::core::result::Result::Ok(v as $t)
                }
            }
        )*
    };
}

/// Implement `PgNodeOut`/`PgNodeRead` for a node *enum* given its variant ->
/// discriminant table, faithfully matching `WRITE_ENUM_FIELD` (`(int) value`,
/// `%d`) / `READ_ENUM_FIELD` (`(enumtype) atoi(token)`).
///
/// This is the enum analogue of [`pg_scalar_enum_out_read!`], but where that one
/// reconstructs the enum from the integer with a plain `as`-cast (which only
/// compiles for an integer *alias*, not a real Rust `enum`), this one rebuilds
/// the enum by matching the parsed discriminant against the supplied table — the
/// safe owned-tree analogue of C's `(enumtype) atoi(token)` reinterpret cast. An
/// out-of-range discriminant panics loudly (an invalid node string), rather than
/// fabricating a bogus variant.
///
/// ```ignore
/// pg_node_enum_out_read!(crate::CmdType { CMD_UNKNOWN => 0, CMD_SELECT => 1 });
/// ```
#[macro_export]
macro_rules! pg_node_enum_out_read {
    ($( $t:ty { $( $variant:path => $disc:expr ),+ $(,)? } )*) => {
        $(
            impl $crate::PgNodeOut for $t {
                #[inline]
                fn out_node(&self, buf: &mut $crate::alloc_reexport::string::String) {
                    // WRITE_ENUM_FIELD: " :fld %d", (int) node->fld.
                    let v: i64 = *self as i64;
                    let _ = ::core::fmt::Write::write_fmt(
                        buf, ::core::format_args!("{}", v));
                }
            }
            impl $crate::PgNodeRead for $t {
                type Bound<'dst> = Self;
                fn read_node<'dst>(
                    cur: &mut $crate::ReadCursor<'_>,
                    _dst: $crate::Mcx<'dst>,
                ) -> $crate::PgResult<Self> {
                    // READ_ENUM_FIELD: (enumtype) atoi(token). Match the parsed
                    // discriminant back to its variant (the safe analogue of C's
                    // reinterpret cast); an unknown value is an invalid node
                    // string, so panic loudly.
                    let v: i64 = cur.expect_token().text.parse::<i64>()
                        .expect("invalid enum token");
                    ::core::result::Result::Ok(match v {
                        $( x if x == ($disc as i64) => $variant, )+
                        other => ::core::panic!(
                            "enum discriminant {} out of range for {}",
                            other, ::core::stringify!($t)),
                    })
                }
            }
        )*
    };
}

/// Opt a list of *leaf* field types into `PgNodeOut`/`PgNodeRead` as a single
/// unsigned token (`%u` write, `atoui`-style read), via `as`-casts to/from `u64`
/// — for OID-like and `Index`/`uint` aliases that `outfuncs.c` writes with
/// `WRITE_OID_FIELD`/`WRITE_UINT_FIELD`. Use this instead of
/// [`pg_scalar_enum_out_read!`] when the C field is `Oid`/`Index`/unsigned.
///
/// ```ignore
/// pg_scalar_uint_out_read!(crate::SomeOidAlias);
/// ```
#[macro_export]
macro_rules! pg_scalar_uint_out_read {
    ($($t:ty),* $(,)?) => {
        $(
            impl $crate::PgNodeOut for $t {
                #[inline]
                fn out_node(&self, buf: &mut $crate::alloc_reexport::string::String) {
                    let v: u64 = *self as u64;
                    let _ = ::core::fmt::Write::write_fmt(
                        buf, ::core::format_args!("{}", v));
                }
            }
            impl $crate::PgNodeRead for $t {
                type Bound<'dst> = Self;
                #[inline]
                fn read_node<'dst>(
                    cur: &mut $crate::ReadCursor<'_>,
                    _dst: $crate::Mcx<'dst>,
                ) -> $crate::PgResult<Self> {
                    let v: u64 = cur.expect_token().text.parse::<u64>()
                        .expect("invalid unsigned token");
                    ::core::result::Result::Ok(v as $t)
                }
            }
        )*
    };
}

/// Opt a list of *leaf* field types into `PgNodeOut`/`PgNodeRead` as a NO-OP:
/// the OUT side writes **nothing** and the READ side reconstructs a
/// `Default::default()`. The OUT/READ analogue of [`pg_scalar_ignore!`], for the
/// field types `outfuncs.c` never serializes as a standalone token: opaque
/// runtime handles / routine tables and the `nodetag_only` runtime-state nodes
/// (`EState`/`ExprState`/`TupleTableSlot`/`FdwRoutine`/…), and the embedded
/// inheritance-base sub-structs (`Expr`/`Plan`/`Path`/`QualCost`) whose fields
/// `gen_node_support.pl` flattens into the child. They reach a node tree only
/// through a `read_write_ignore` field or a `nodetag_only` parent (neither of
/// which calls the per-type writer on the wire), but the central dispatch must
/// stay total, so we supply a do-nothing impl.
///
/// READ returns `Default::default()`, so the listed type must be `Default` and
/// lifetime-free. If a type is not `Default`, register it with a hand-written
/// impl instead.
///
/// ```ignore
/// pg_scalar_ignore_out_read!(crate::executor::TupleTableSlot, crate::executor::Expr);
/// ```
#[macro_export]
macro_rules! pg_scalar_ignore_out_read {
    ($($t:ty),* $(,)?) => {
        $(
            impl $crate::PgNodeOut for $t {
                #[inline]
                fn out_node(&self, _buf: &mut $crate::alloc_reexport::string::String) {
                    // Never serialized as a standalone token: write nothing.
                }
            }
            impl $crate::PgNodeRead for $t {
                type Bound<'dst> = Self;
                #[inline]
                fn read_node<'dst>(
                    _cur: &mut $crate::ReadCursor<'_>,
                    _dst: $crate::Mcx<'dst>,
                ) -> $crate::PgResult<Self> {
                    // The OUT side wrote nothing, so consume nothing and
                    // reconstruct the zero value (makeNode's palloc0 analogue).
                    ::core::result::Result::Ok(::core::default::Default::default())
                }
            }
        )*
    };
}

/// Opt opaque-handle / runtime-state leaf field types into `PgNodeOut`/
/// `PgNodeRead` where the type is **never** part of a serialized node and is
/// **not** `Default` (so [`pg_scalar_ignore_out_read!`] cannot be used). OUT
/// writes nothing; READ **panics loudly**. A reached `read_node`/written token
/// would mean a node struct opted a non-`read_write_ignore` field of this type
/// into the OUT/READ stage — a faithful-port bug — so surfacing it as a panic
/// (rather than silently fabricating a value) matches `gen_node_support.pl`,
/// which would likewise refuse to emit support for such a field. The listed type
/// need only be lifetime-free (no `Default`/`Clone` bound).
///
/// ```ignore
/// pg_scalar_never_out_read!(crate::executor::TupleTableSlotOps, crate::fmgr::FmgrInfo);
/// ```
#[macro_export]
macro_rules! pg_scalar_never_out_read {
    ($($t:ty),* $(,)?) => {
        $(
            impl $crate::PgNodeOut for $t {
                #[inline]
                fn out_node(&self, _buf: &mut $crate::alloc_reexport::string::String) {
                    // Never serialized as a standalone token: write nothing. (If
                    // this type ever reaches a *serialized* field the READ side
                    // below will panic on the way back, surfacing the bug.)
                }
            }
            impl $crate::PgNodeRead for $t {
                type Bound<'dst> = Self;
                fn read_node<'dst>(
                    _cur: &mut $crate::ReadCursor<'_>,
                    _dst: $crate::Mcx<'dst>,
                ) -> $crate::PgResult<Self> {
                    // Unreachable on a faithful node string: an opaque runtime
                    // handle / state node is never written, so it is never read.
                    // Panic loudly rather than fabricate a value (no meaningful
                    // `Default`).
                    ::core::panic!(
                        "PgNodeRead::read_node reached for an opaque/never-serialized \
                         leaf type ({}); such a field must be `read_write_ignore` or \
                         live under a `nodetag_only` parent",
                        ::core::stringify!($t),
                    )
                }
            }
        )*
    };
}

// Re-export `alloc` under a stable path so the exported macros above can name
// `String` without the downstream crate having to bring `alloc` into scope.
#[doc(hidden)]
pub mod alloc_reexport {
    pub use alloc::string;
}

// ---------------------------------------------------------------------------
// Container OUT/READ impls. RECURSE through their element, re-homing rebuilt
// node storage onto the TARGET context `dst` (READ), exactly like the copy
// container impls above.
//
// Option<T>     -> a possibly-NULL field. NULL renders as `<>` (outToken NULL);
//                  Some(v) renders as v. This is how a nullable `Node *` /
//                  `char *` field round-trips.
// PgBox<T>      -> a charged `Node *` child link: transparent OUT, READ rebuilds
//                  into a fresh PgBox charged to `dst`.
// PgVec<T>      -> a charged `List *`: a NIL (empty) list is `<>` (the NULL
//                  pointer), a non-empty one is `(elem elem ...)`. (PostgreSQL
//                  has no non-NULL empty `List *` — an empty list IS NIL.)
// PgString<'a>  -> the leaf above (a non-NULL `char *`).
// ---------------------------------------------------------------------------

impl<T: PgNodeOut> PgNodeOut for Option<T> {
    fn out_node(&self, buf: &mut String) {
        match self {
            // NULL pointer encodes as "<>" (outToken, outfuncs.c:157-160).
            None => buf.push_str("<>"),
            Some(v) => v.out_node(buf),
        }
    }
}
impl<T: PgNodeRead> PgNodeRead for Option<T> {
    type Bound<'dst> = Option<T::Bound<'dst>>;
    fn read_node<'dst>(
        cur: &mut ReadCursor<'_>,
        dst: Mcx<'dst>,
    ) -> PgResult<Self::Bound<'dst>> {
        // Peek the next token: a bare "<>" sentinel means NULL. Otherwise let
        // the element parser consume the token(s), so rewind first.
        let save = cur.save();
        let tok = cur.expect_token();
        if tok.is_empty_sentinel {
            return Ok(None);
        }
        cur.restore(save); // rewind; element reader re-reads from here
        Ok(Some(T::read_node(cur, dst)?))
    }
}

impl<'a, T: PgNodeOut> PgNodeOut for PgBox<'a, T> {
    #[inline]
    fn out_node(&self, buf: &mut String) {
        (**self).out_node(buf);
    }
}
impl<'a, T: PgNodeRead> PgNodeRead for PgBox<'a, T> {
    type Bound<'dst> = PgBox<'dst, T::Bound<'dst>>;
    fn read_node<'dst>(
        cur: &mut ReadCursor<'_>,
        dst: Mcx<'dst>,
    ) -> PgResult<Self::Bound<'dst>> {
        let payload = T::read_node(cur, dst)?;
        mcx::alloc_in(dst, payload)
    }
}

impl<'a, T: PgNodeOut> PgNodeOut for PgVec<'a, T> {
    fn out_node(&self, buf: &mut String) {
        // _outList / outNode: a NIL (empty) list is `<>` (the NULL-pointer
        // token), a non-empty one is `( item item ... )` with a space BETWEEN
        // elements (outfuncs.c:280-319). An empty `List *` IS NIL in PostgreSQL,
        // which `outNode` writes as `<>` (outfuncs.c:866-868) — NOT `()` — so an
        // empty PgVec must render as `<>` to round-trip the faithful nodeToString.
        if self.is_empty() {
            buf.push_str("<>");
            return;
        }
        buf.push('(');
        for (i, elem) in self.iter().enumerate() {
            if i != 0 {
                buf.push(' ');
            }
            elem.out_node(buf);
        }
        buf.push(')');
    }
}
impl<'a, T: PgNodeRead> PgNodeRead for PgVec<'a, T> {
    type Bound<'dst> = PgVec<'dst, T::Bound<'dst>>;
    fn read_node<'dst>(
        cur: &mut ReadCursor<'_>,
        dst: Mcx<'dst>,
    ) -> PgResult<Self::Bound<'dst>> {
        // Accept `<>` (NIL -> empty) or `( ... )`, matching the OUT writer (a NIL
        // list is the NULL-pointer token). The rebuilt list is charged to `dst`.
        let open = cur.expect_token();
        if open.is_empty_sentinel || open.text == "<>" {
            return mcx::vec_with_capacity_in(dst, 0);
        }
        debug_assert_eq!(open.text, "(", "expected '(' or '<>' at start of list");
        let mut out: PgVec<'dst, T::Bound<'dst>> = PgVec::new_in(dst);
        loop {
            let save = cur.save();
            let tok = cur.expect_token();
            if tok.text == ")" {
                break;
            }
            cur.restore(save);
            out.push(T::read_node(cur, dst)?);
        }
        Ok(out)
    }
}

#[cfg(test)]
mod serialization_tests {
    extern crate std;

    use super::*;
    use mcx::MemoryContext;

    fn out_to_string<T: PgNodeOut>(v: &T) -> String {
        let mut s = String::new();
        v.out_node(&mut s);
        s
    }

    // --- Byte-for-byte fidelity vs outfuncs.c (the per-type token) ----------

    #[test]
    fn scalar_token_bytes_match_outfuncs() {
        // WRITE_INT_FIELD "%d", WRITE_UINT_FIELD "%u", WRITE_BOOL_FIELD, outChar.
        assert_eq!(out_to_string(&0i32), "0");
        assert_eq!(out_to_string(&(-7i32)), "-7");
        assert_eq!(out_to_string(&i32::MIN), "-2147483648");
        assert_eq!(out_to_string(&i64::MIN), "-9223372036854775808");
        assert_eq!(out_to_string(&u32::MAX), "4294967295");
        assert_eq!(out_to_string(&u64::MAX), "18446744073709551615");
        assert_eq!(out_to_string(&true), "true");
        assert_eq!(out_to_string(&false), "false");
        assert_eq!(out_to_string(&'a'), "a");
        assert_eq!(out_to_string(&'\0'), "<>"); // outChar \0 -> <>
        assert_eq!(out_to_string(&'('), "\\("); // escaped special char
        assert_eq!(out_to_string(&'"'), "\\\""); // leading protective backslash
    }

    #[test]
    fn pgstring_token_bytes_match_outfuncs() {
        let ctx = MemoryContext::new("t");
        let mcx = ctx.mcx();
        let mk = |s: &str| PgString::from_str_in(s, mcx).unwrap();
        assert_eq!(out_to_string(&mk("foo")), "foo"); // plain identifier
        assert_eq!(out_to_string(&mk("")), "\"\""); // empty -> ""
        assert_eq!(out_to_string(&mk("a b")), "a\\ b"); // space backslashed
        assert_eq!(out_to_string(&mk("a(b)c")), "a\\(b\\)c");
        assert_eq!(out_to_string(&mk("<tag>")), "\\<tag>"); // leading protective \
        assert_eq!(out_to_string(&mk("123")), "\\123"); // leading digit
    }

    #[test]
    fn option_and_list_token_bytes_match_outfuncs() {
        let ctx = MemoryContext::new("t");
        let mcx = ctx.mcx();
        // None (NULL) -> <>; Some(v) -> v.
        let none: Option<i32> = None;
        assert_eq!(out_to_string(&none), "<>");
        assert_eq!(out_to_string(&Some(42i32)), "42");
        // A non-empty PgVec list -> "(e e e)"; an empty (NIL) list -> "<>".
        let mut v: PgVec<i32> = PgVec::new_in(mcx);
        v.push(1);
        v.push(2);
        v.push(3);
        assert_eq!(out_to_string(&v), "(1 2 3)");
        let empty: PgVec<i32> = PgVec::new_in(mcx);
        assert_eq!(out_to_string(&empty), "<>");
    }

    // --- Round-trip (out -> read) through the charged/fallible READ side ----

    fn round_trip_scalar<T>(v: T) -> T
    where
        T: PgNodeOut + for<'d> PgNodeRead<Bound<'d> = T>,
    {
        let ctx = MemoryContext::new("rt");
        let mcx = ctx.mcx();
        let mut s = String::new();
        v.out_node(&mut s);
        let mut cur = ReadCursor::new(&s);
        T::read_node(&mut cur, mcx).unwrap()
    }

    #[test]
    fn round_trip_scalars() {
        for v in [0i32, 1, -1, 42, -42, i32::MIN, i32::MAX] {
            assert_eq!(round_trip_scalar(v), v);
        }
        for v in [0i64, -1, i64::MIN, i64::MAX, 9_000_000_000] {
            assert_eq!(round_trip_scalar(v), v);
        }
        for v in [0u32, 42, u32::MAX] {
            assert_eq!(round_trip_scalar(v), v);
        }
        for v in [0u64, 42, u64::MAX] {
            assert_eq!(round_trip_scalar(v), v);
        }
        assert!(round_trip_scalar(true));
        assert!(!round_trip_scalar(false));
        for c in ['a', 'Z', '0', '!', ' ', '(', ')', '{', '}', '\\', '"', '<', '\0'] {
            assert_eq!(round_trip_scalar(c), c, "char round-trip failed for {c:?}");
        }
    }

    #[test]
    fn round_trip_pgstring() {
        let ctx = MemoryContext::new("rt");
        let mcx = ctx.mcx();
        for s in ["", "foo", "a b c", "a(b)c", "x{y}z", "back\\slash", "<tag>", "123", "-3.5"] {
            let orig = PgString::from_str_in(s, mcx).unwrap();
            let mut buf = String::new();
            orig.out_node(&mut buf);
            let mut cur = ReadCursor::new(&buf);
            let back = PgString::read_node(&mut cur, mcx).unwrap();
            assert_eq!(back.as_str(), s, "PgString round-trip failed for {s:?}");
        }
    }

    #[test]
    fn round_trip_option_and_list() {
        let ctx = MemoryContext::new("rt");
        let mcx = ctx.mcx();
        // Option<i32>: None and Some distinguished by the <> sentinel.
        for v in [None, Some(7i32), Some(-3)] {
            let mut buf = String::new();
            v.out_node(&mut buf);
            let mut cur = ReadCursor::new(&buf);
            let back: Option<i32> = Option::<i32>::read_node(&mut cur, mcx).unwrap();
            assert_eq!(back, v);
        }
        // PgVec<i32>: empty (NIL) and non-empty.
        for elems in [std::vec![], std::vec![1i32, -2, 3, i32::MIN, i32::MAX]] {
            let mut v: PgVec<i32> = PgVec::new_in(mcx);
            for &e in &elems {
                v.push(e);
            }
            let mut buf = String::new();
            v.out_node(&mut buf);
            let mut cur = ReadCursor::new(&buf);
            let back: PgVec<i32> = PgVec::<i32>::read_node(&mut cur, mcx).unwrap();
            assert_eq!(back.as_slice(), elems.as_slice());
        }
        // PgBox<i32>: transparent.
        let boxed = mcx::alloc_in(mcx, 99i32).unwrap();
        let mut buf = String::new();
        boxed.out_node(&mut buf);
        let mut cur = ReadCursor::new(&buf);
        let back = PgBox::<i32>::read_node(&mut cur, mcx).unwrap();
        assert_eq!(*back, 99);
    }

    // --- ReadCursor tokenizer fidelity vs pg_strtok (read.c) ---------------

    #[test]
    fn read_cursor_matches_pg_strtok_rules() {
        // Braces/parens are single-char tokens even with no whitespace.
        let mut cur = ReadCursor::new("{FOO :x 3}");
        assert_eq!(cur.next_token().unwrap().text, "{");
        assert_eq!(cur.next_token().unwrap().text, "FOO");
        assert_eq!(cur.next_token().unwrap().text, ":x");
        assert_eq!(cur.next_token().unwrap().text, "3");
        assert_eq!(cur.next_token().unwrap().text, "}");
        assert!(cur.next_token().is_none());

        // "<>" is a non-NULL, length-0 sentinel token (read.c:199-201).
        let mut cur = ReadCursor::new("<>");
        let t = cur.next_token().unwrap();
        assert!(t.is_empty_sentinel);
        assert_eq!(t.text, "");
        assert!(cur.next_token().is_none());

        // Backslash quotes the following special char: the escaped space does
        // NOT split the token.
        let mut cur = ReadCursor::new("a\\ b");
        assert_eq!(cur.next_token().unwrap().text, "a\\ b");
        assert!(cur.next_token().is_none());

        // Whitespace (space/tab/newline) separates tokens.
        let mut cur = ReadCursor::new("one\ttwo\nthree four");
        assert_eq!(cur.next_token().unwrap().text, "one");
        assert_eq!(cur.next_token().unwrap().text, "two");
        assert_eq!(cur.next_token().unwrap().text, "three");
        assert_eq!(cur.next_token().unwrap().text, "four");
        assert!(cur.next_token().is_none());
    }

    #[test]
    fn value_token_kind_matches_node_token_type() {
        let mut cur = ReadCursor::new("42");
        assert_eq!(cur.peek_value_token_kind(), Some(ValueTokenKind::Integer));
        let mut cur = ReadCursor::new("-7");
        assert_eq!(cur.peek_value_token_kind(), Some(ValueTokenKind::Integer));
        let mut cur = ReadCursor::new("3.14");
        assert_eq!(cur.peek_value_token_kind(), Some(ValueTokenKind::Float));
        let mut cur = ReadCursor::new("99999999999999999999");
        assert_eq!(cur.peek_value_token_kind(), Some(ValueTokenKind::Float));
        let mut cur = ReadCursor::new("true");
        assert_eq!(cur.peek_value_token_kind(), Some(ValueTokenKind::Boolean));
        let mut cur = ReadCursor::new("\"hi\"");
        assert_eq!(cur.peek_value_token_kind(), Some(ValueTokenKind::String));
        let mut cur = ReadCursor::new("b101");
        assert_eq!(cur.peek_value_token_kind(), Some(ValueTokenKind::BitString));
        let mut cur = ReadCursor::new("{FOO");
        assert_eq!(cur.peek_value_token_kind(), Some(ValueTokenKind::Brace));
        let mut cur = ReadCursor::new("(");
        assert_eq!(cur.peek_value_token_kind(), Some(ValueTokenKind::Paren));
    }
}
