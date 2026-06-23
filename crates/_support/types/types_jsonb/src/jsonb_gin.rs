//! Cross-crate vocabulary for the jsonb GIN opclass support (`jsonb_gin.c`).
//!
//! These are the data types that travel across the GIN opclass-proc dispatch
//! boundary between the GIN core and the `backend-utils-adt-jsonb-gin` owner:
//!
//!  * [`GinEntries`] — the buffer of extracted GIN key payloads (C: the
//!    returned `Datum *entries`).
//!  * [`JsonPathGinNode`] — the jsonpath query expression tree the
//!    `gin_extract_jsonb_query[_path]` procs build and stash in the opaque GIN
//!    `extra_data[0]`, and the `gin_*consistent_jsonb[_path]` procs evaluate
//!    (C: the opaque `Pointer` round-tripped through the index scan).
//!  * [`JspQuery`] / [`GinQueryExtraction`] — the de-pointered outputs of the
//!    extract-query procs (the returned entries plus the `*searchMode` /
//!    `extra_data[0]` out-params).
//!  * [`GinJsonbQuery`] — the strategy-tagged query argument the extract-query
//!    procs receive.
//!
//! The opclass-internal extraction state (`JsonPathGinPath`,
//! `JsonPathGinPathItem`, `JsonPathGinContext`) never crosses this boundary and
//! lives in the owner crate.

use alloc::vec::Vec;

/// C: `struct GinEntries` — a growable buffer of GIN key payloads.
///
/// In C the entries are `Datum`s pointing at on-disk `text` varlenas (built by
/// `make_text_key`) or bare `uint32` hash Datums (`jsonb_path_ops`); here we
/// hold the assembled GIN key bytes directly.
#[derive(Clone, Debug, Default)]
pub struct GinEntries {
    /// C: `Datum *buf` — one assembled GIN key per entry (the varlena `text`
    /// bytes for `jsonb_ops`, the 4 native-endian hash bytes for
    /// `jsonb_path_ops`).
    pub buf: Vec<Vec<u8>>,
}

/// C: `enum JsonPathGinNodeType`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum JsonPathGinNodeType {
    JSP_GIN_OR,
    JSP_GIN_AND,
    JSP_GIN_ENTRY,
}

/// C: `struct JsonPathGinNode`. The C union over `nargs` / `entryIndex` /
/// `entryDatum` is modeled as a Rust enum payload; the flexible `args[]` member
/// is a `Vec` of child nodes.
#[derive(Clone, Debug)]
pub enum JsonPathGinNode {
    /// C: `JSP_GIN_OR` / `JSP_GIN_AND` with `val.nargs` children.
    Logic {
        /// `true` for `JSP_GIN_AND`, `false` for `JSP_GIN_OR`.
        and: bool,
        args: Vec<JsonPathGinNode>,
    },
    /// C: `JSP_GIN_ENTRY` holding `val.entryDatum` (the GIN key bytes) before
    /// entries are emitted.
    EntryDatum(Vec<u8>),
    /// C: `JSP_GIN_ENTRY` holding `val.entryIndex` after `emit_jsp_gin_entries`
    /// has replaced the datum with its index in the [`GinEntries`] array.
    EntryIndex(usize),
}

impl JsonPathGinNode {
    /// Serialize the (already-emitted) node tree to a compact byte blob so it can
    /// round-trip through the GIN scan-key `extra_data[0]` `Pointer` channel
    /// (which the owned GIN model carries as opaque `Vec<u8>`). This is purely a
    /// transport encoding for the in-memory C `Pointer` the C code stashes in
    /// `(*extra_data)[0]`; it is not an on-disk format.
    ///
    /// Layout (all little-endian): tag byte, then
    ///   * `0` `Logic`: `and` byte, `u32` arg count, each arg recursively.
    ///   * `1` `EntryIndex`: `u32` index.
    ///   * `2` `EntryDatum`: `u32` length + payload bytes (only appears before
    ///     `emit_jsp_gin_entries`; encoded for completeness).
    pub fn encode_extra_data(&self) -> Vec<u8> {
        let mut out = Vec::new();
        self.encode_into(&mut out);
        out
    }

    fn encode_into(&self, out: &mut Vec<u8>) {
        match self {
            JsonPathGinNode::Logic { and, args } => {
                out.push(0);
                out.push(*and as u8);
                out.extend_from_slice(&(args.len() as u32).to_le_bytes());
                for arg in args {
                    arg.encode_into(out);
                }
            }
            JsonPathGinNode::EntryIndex(idx) => {
                out.push(1);
                out.extend_from_slice(&(*idx as u32).to_le_bytes());
            }
            JsonPathGinNode::EntryDatum(bytes) => {
                out.push(2);
                out.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
                out.extend_from_slice(bytes);
            }
        }
    }

    /// Inverse of [`Self::encode_extra_data`]. Returns `None` on a malformed
    /// blob (never expected — the encoder is the only writer).
    pub fn decode_extra_data(bytes: &[u8]) -> Option<Self> {
        let mut pos = 0usize;
        Self::decode_from(bytes, &mut pos)
    }

    fn decode_from(bytes: &[u8], pos: &mut usize) -> Option<Self> {
        let tag = *bytes.get(*pos)?;
        *pos += 1;
        match tag {
            0 => {
                let and = *bytes.get(*pos)? != 0;
                *pos += 1;
                let n = read_u32(bytes, pos)? as usize;
                let mut args = Vec::with_capacity(n);
                for _ in 0..n {
                    args.push(Self::decode_from(bytes, pos)?);
                }
                Some(JsonPathGinNode::Logic { and, args })
            }
            1 => {
                let idx = read_u32(bytes, pos)? as usize;
                Some(JsonPathGinNode::EntryIndex(idx))
            }
            2 => {
                let len = read_u32(bytes, pos)? as usize;
                let end = pos.checked_add(len)?;
                let payload = bytes.get(*pos..end)?.to_vec();
                *pos = end;
                Some(JsonPathGinNode::EntryDatum(payload))
            }
            _ => None,
        }
    }
}

#[inline]
fn read_u32(bytes: &[u8], pos: &mut usize) -> Option<u32> {
    let end = pos.checked_add(4)?;
    let slice = bytes.get(*pos..end)?;
    let v = u32::from_le_bytes([slice[0], slice[1], slice[2], slice[3]]);
    *pos = end;
    Some(v)
}

/// The output of `extract_jsp_query`: the assembled GIN entries plus the root
/// expression node (C: the value stored in `(*extra_data)[0]`).
#[derive(Clone, Debug)]
pub struct JspQuery {
    /// C: the returned `entries.buf` (length is `*nentries`).
    pub entries: Vec<Vec<u8>>,
    /// C: `(*extra_data)[0] = (Pointer) node` — the root expression node.
    pub node: JsonPathGinNode,
}

/// The result of a `gin_extract_jsonb_query[_path]` call (C: the returned
/// `Datum *entries` plus the `*nentries`, `*searchMode`, and
/// `(*extra_data)[0]` out-parameters).
#[derive(Clone, Debug, Default)]
pub struct GinQueryExtraction {
    /// C: the returned `entries` array (length is `*nentries`).
    pub entries: Vec<Vec<u8>>,
    /// C: `*searchMode` was set to `GIN_SEARCH_MODE_ALL`.
    pub search_mode_all: bool,
    /// C: `(*extra_data)[0] = (Pointer) node` (jsonpath strategies only).
    pub node: Option<JsonPathGinNode>,
}

/// The query argument for the `gin_extract_jsonb_query[_path]` procs, selected
/// by the GIN strategy number. The fmgr GIN dispatch boundary detoasts the
/// scan-key argument into the variant the strategy expects.
pub enum GinJsonbQuery<'a> {
    /// `JsonbContainsStrategyNumber` — the query is itself a jsonb value (its
    /// root container bytes).
    Contains(&'a [u8]),
    /// `JsonbExistsStrategyNumber` — the query is a single text key (detoasted
    /// payload bytes).
    Exists(&'a [u8]),
    /// `JsonbExistsAnyStrategyNumber` / `JsonbExistsAllStrategyNumber` — the
    /// query is a `text[]`; each element is passed as `Option<&[u8]>` (`None`
    /// is a SQL NULL, ignored), in array order.
    ExistsArray(&'a [Option<&'a [u8]>]),
    /// `JsonbJsonpath{Predicate,Exists}StrategyNumber` — the query is the
    /// on-disk jsonpath varlena bytes.
    Jsonpath(&'a [u8]),
}
