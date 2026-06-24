//! Signature types for `backend-utils-adt-jsonb-util`.
//!
//! The on-disk `jsonb` ABI surface (`Jsonb`, `JsonbContainer`, `JEntry`, the
//! `jbvType`/`JsonbIteratorToken`/`JsonbIterState` enums and every flag/accessor)
//! lives in [`crate::jsonb`].  This module holds the *in-memory* working types
//! `jsonb_util.c` operates on (C: `JsonbValue`, `JsonbPair`, `JsonbParseState`,
//! `JsonbIterator`, the `jbvDatetime` payload).  They are never stored on disk
//! and never cross a C ABI boundary, so they are idiomatic owned-tree Rust types:
//! the C unions become Rust enums, the `numeric`/`string` byte runs become owned
//! `Vec<u8>`, and the raw `char *` cursors into the document buffer become byte
//! offsets into an owned container `Vec<u8>`.
//!
//! These live here (not in the owning crate) because the genuine externals the
//! crate seams over -- notably the `jbvDatetime` rendering seam -- name
//! [`JsonbDatetime`] in their signatures, and centralized seams may only
//! reference vocabulary from the `types` crate.

#![allow(non_camel_case_types)]
#![allow(non_snake_case)]

use alloc::boxed::Box;
use alloc::vec::Vec;

use crate::jsonb::{is_a_jsonb_scalar, jbvType, JsonbIterState};

/// A `numeric` value carried through jsonb (C: `Numeric`, an on-disk varlena).
/// Stored as the owned on-disk varlena bytes; the `numeric` crate provides the
/// operations.
pub type JsonbNumeric = Vec<u8>;

/// In-memory representation of a jsonb scalar/container value
/// (C: `struct JsonbValue`).  The `type`-tagged union is modeled as a Rust enum
/// payload alongside the explicit `jbvType` tag the C code branches on.
#[derive(Clone, Debug)]
pub struct JsonbValue {
    /// Influences sort order (C: `enum jbvType type`).
    pub typ: jbvType,
    /// The tagged payload (C: the `val` union).
    pub val: JsonbValueData,
}

/// The payload union of [`JsonbValue`] (C: `JsonbValue.val`).
#[derive(Clone, Debug)]
pub enum JsonbValueData {
    /// `jbvNull`: no payload.
    Null,
    /// `jbvNumeric`: a `numeric` value (on-disk varlena bytes).
    Numeric(JsonbNumeric),
    /// `jbvBool`: a boolean.
    Bool(bool),
    /// `jbvString`: a string primitive (not necessarily NUL-terminated).
    String(Vec<u8>),
    /// `jbvArray`: an array container (`nElems`, `elems`, `rawScalar`).
    Array {
        elems: Vec<JsonbValue>,
        raw_scalar: bool,
    },
    /// `jbvObject`: an associative container of key/value pairs.
    Object(Vec<JsonbPair>),
    /// `jbvBinary`: an array/object already in on-disk container form.  `data`
    /// holds the container bytes starting at the `JsonbContainer` header (C:
    /// `binary.data`); `len` is `binary.len`.
    ///
    /// `offset` records this container's byte position **within the root
    /// container of its origin document**.  In C, `binary.data` is a raw
    /// pointer into the document buffer, so the document-relative position is
    /// implicit in pointer arithmetic (`(char*)a - (char*)b`).  Because the
    /// safe port carries owned slices instead of pointers, that relationship is
    /// preserved explicitly here: a document root has `offset == 0`, and every
    /// nested container extracted by `fillJsonbValue` / the iterator inherits
    /// its parent's offset plus the in-parent byte position of the child.  This
    /// is exactly what `.keyvalue()`'s `id` field consumes
    /// (jsonpath_exec.c:2862-2864).
    Binary {
        len: i32,
        data: Vec<u8>,
        offset: i32,
    },
    /// `jbvDatetime`: a virtual datetime value used during processing.
    Datetime(JsonbDatetime),
}

impl JsonbValue {
    /// Construct a `jbvNull` value.
    pub fn null() -> Self {
        JsonbValue {
            typ: jbvType::jbvNull,
            val: JsonbValueData::Null,
        }
    }

    /// `IsAJsonbScalar(val)` -- scalar primitives plus the virtual datetime.
    #[inline]
    pub fn is_scalar(&self) -> bool {
        is_a_jsonb_scalar(self.typ)
    }
}

/// The `jbvDatetime` payload (C: `JsonbValue.val.datetime`).
#[derive(Clone, Debug)]
pub struct JsonbDatetime {
    /// C: `Datum value`.
    pub value: usize,
    /// C: `Oid typid`.
    pub typid: u32,
    /// C: `int32 typmod`.
    pub typmod: i32,
    /// Numeric time zone, in seconds, for `TimestampTz`.
    pub tz: i32,
}

/// Key/value pair within an object (C: `struct JsonbPair`).
#[derive(Clone, Debug)]
pub struct JsonbPair {
    /// Must be a `jbvString` (C: `JsonbValue key`).
    pub key: JsonbValue,
    /// May be of any type (C: `JsonbValue value`).
    pub value: JsonbValue,
    /// Pair's index in the original sequence, for last-observed-wins dedup.
    pub order: u32,
}

/// Conversion state used when parsing Jsonb from text or coercing types
/// (C: `struct JsonbParseState`).  Modeled as a stack of frames threaded by
/// `next`, mirroring the C singly linked list.
#[derive(Clone, Debug)]
pub struct JsonbParseState {
    pub cont_val: JsonbValue,
    pub size: usize,
    /// Check object key uniqueness.
    pub unique_keys: bool,
    /// Skip null object fields.
    pub skip_nulls: bool,
    /// Parent frame (C: `JsonbParseState *next`).
    pub next: Option<Box<JsonbParseState>>,
}

/// Iterator over an on-disk `JsonbContainer` (C: `struct JsonbIterator`).
///
/// `dataProper` is replaced by `data_proper` (a byte offset within the
/// container window).
///
/// The backing bytes live in a shared [`Rc<[u8]>`] document buffer (`buf`) plus
/// a `cont_start` window offset.  When the iterator recurses into a nested
/// container, the child iterator **shares the same `Rc`** and only records the
/// nested container's start offset in `cont_start`, instead of copying the
/// nested sub-slice into a fresh owned `Vec`.  This mirrors C, where every
/// nesting level's `JsonbIterator` holds a raw `JsonbContainer *` into the same
/// document buffer, and it removes the per-recursion `malloc`/`free` of each
/// nested container.  Use [`JsonbIterator::container`] to get the windowed
/// `&[u8]` the call sites operate on.
#[derive(Clone, Debug)]
pub struct JsonbIterator {
    /// Shared document buffer the container bytes live in.  Shared across the
    /// parent/child iterator chain via [`Rc`] so recursion never re-copies a
    /// nested container (C: the document the `JsonbContainer *` points into).
    pub buf: alloc::rc::Rc<[u8]>,
    /// Byte offset of this iterator's `JsonbContainer` header within `buf`
    /// (`0` for the document root).
    pub cont_start: usize,
    /// Number of elements (`nPairs` for objects) (C: `nElems`).
    pub n_elems: u32,
    /// Pseudo-array scalar value? (C: `isScalar`).
    pub is_scalar: bool,
    /// Byte offset within `container` where the children JEntry array begins
    /// (always 4, just past the header) (C: `children`).
    pub children_off: usize,
    /// Byte offset within `container` to the variable-length data
    /// (C: `dataProper`).
    pub data_proper: usize,
    /// Current item index (C: `curIndex`).
    pub cur_index: u32,
    /// Data offset of the current item (C: `curDataOffset`).
    pub cur_data_offset: u32,
    /// Data offset of the current value, for objects (C: `curValueOffset`).
    pub cur_value_offset: u32,
    /// Iterator phase (C: `JsonbIterState state`).
    pub state: JsonbIterState,
    /// Parent iterator (C: `struct JsonbIterator *parent`).
    pub parent: Option<Box<JsonbIterator>>,
    /// Byte position of `container` within the root container of its origin
    /// document (0 for the document root).  Threaded into the `offset` field of
    /// any `jbvBinary` children so `.keyvalue()` ids stay document-relative.
    /// This is bookkeeping unique to the safe port (C reconstructs it from raw
    /// container pointers).
    pub doc_offset: i32,
}

impl JsonbIterator {
    /// The container bytes this iterator operates on, as a windowed view into
    /// the shared document buffer (C: `JsonbContainer *container`).  All the
    /// container-relative indexing (`data_proper`, `children_off`, `JEntry`
    /// reads) is taken against this slice.
    #[inline]
    pub fn container(&self) -> &[u8] {
        &self.buf[self.cont_start..]
    }
}
