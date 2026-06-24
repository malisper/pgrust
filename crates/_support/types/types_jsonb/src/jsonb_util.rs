//! Signature types for `backend-utils-adt-jsonb-util`.
//!
//! The on-disk `jsonb` ABI surface (`Jsonb`, `JsonbContainer`, `JEntry`, the
//! `jbvType`/`JsonbIteratorToken`/`JsonbIterState` enums and every flag/accessor)
//! lives in [`crate::jsonb`].  This module holds the *in-memory* working types
//! `jsonb_util.c` operates on (C: `JsonbValue`, `JsonbPair`, `JsonbParseState`,
//! `JsonbIterator`, the `jbvDatetime` payload).  They are never stored on disk
//! and never cross a C ABI boundary.
//!
//! ## Memory-context lifetime (`'mcx`)
//!
//! The working tree carries a memory-context lifetime `'mcx` (mirroring the
//! `Expr<'mcx>` campaign).  Leaf byte runs (`String`/`Numeric`/`Binary.data`) are
//! `&'mcx [u8]` borrows: a **read** sub-slices the source document buffer with
//! **zero copy**, and **construction** bump-allocates fresh bytes into the same
//! `mcx` arena and stores the resulting borrow.  The `Array`/`Object` spines and
//! the build-stack are arena-backed [`PgVec`].  This is exactly C/Postgres's
//! `palloc` / `MemoryContextReset` model: the lifetime is the arena, not
//! ownership, and the borrow checker enforces that a working tree never outlives
//! the document/arena it points into.
//!
//! These live here (not in the owning crate) because the genuine externals the
//! crate seams over -- notably the `jbvDatetime` rendering seam -- name
//! [`JsonbDatetime`] in their signatures, and centralized seams may only
//! reference vocabulary from the `types` crate.

#![allow(non_camel_case_types)]
#![allow(non_snake_case)]

use crate::jsonb::{is_a_jsonb_scalar, jbvType, JsonbIterState};
use ::mcx::PgVec;

/// A `numeric` value carried through jsonb (C: `Numeric`, an on-disk varlena).
/// The on-disk varlena bytes, borrowed from the source document or from a fresh
/// arena allocation; the `numeric` crate provides the operations.
pub type JsonbNumeric<'mcx> = &'mcx [u8];

/// In-memory representation of a jsonb scalar/container value
/// (C: `struct JsonbValue`).  The `type`-tagged union is modeled as a Rust enum
/// payload alongside the explicit `jbvType` tag the C code branches on.
#[derive(Clone, Debug)]
pub struct JsonbValue<'mcx> {
    /// Influences sort order (C: `enum jbvType type`).
    pub typ: jbvType,
    /// The tagged payload (C: the `val` union).
    pub val: JsonbValueData<'mcx>,
}

/// The payload union of [`JsonbValue`] (C: `JsonbValue.val`).
#[derive(Clone, Debug)]
pub enum JsonbValueData<'mcx> {
    /// `jbvNull`: no payload.
    Null,
    /// `jbvNumeric`: a `numeric` value (on-disk varlena bytes).
    Numeric(JsonbNumeric<'mcx>),
    /// `jbvBool`: a boolean.
    Bool(bool),
    /// `jbvString`: a string primitive (not necessarily NUL-terminated).
    ///
    /// A `&'mcx [u8]` sub-slice of the source document (zero-copy read) or of a
    /// fresh arena allocation (construction).
    String(&'mcx [u8]),
    /// `jbvArray`: an array container (`nElems`, `elems`, `rawScalar`).
    Array {
        elems: PgVec<'mcx, JsonbValue<'mcx>>,
        raw_scalar: bool,
    },
    /// `jbvObject`: an associative container of key/value pairs.
    Object(PgVec<'mcx, JsonbPair<'mcx>>),
    /// `jbvBinary`: an array/object already in on-disk container form.  `data`
    /// holds the container bytes starting at the `JsonbContainer` header (C:
    /// `binary.data`); `len` is `binary.len`.
    ///
    /// `data` is a `&'mcx [u8]` sub-slice of the source document buffer
    /// (zero-copy) -- replacing C's raw `char *` into the document.
    ///
    /// `offset` records this container's byte position **within the root
    /// container of its origin document**.  In C, `binary.data` is a raw
    /// pointer into the document buffer, so the document-relative position is
    /// implicit in pointer arithmetic (`(char*)a - (char*)b`).  Because the
    /// safe port carries borrowed slices instead of pointers, that relationship
    /// is preserved explicitly here: a document root has `offset == 0`, and every
    /// nested container extracted by `fillJsonbValue` / the iterator inherits
    /// its parent's offset plus the in-parent byte position of the child.  This
    /// is exactly what `.keyvalue()`'s `id` field consumes
    /// (jsonpath_exec.c:2862-2864).
    Binary {
        len: i32,
        data: &'mcx [u8],
        offset: i32,
    },
    /// `jbvDatetime`: a virtual datetime value used during processing.
    Datetime(JsonbDatetime),
}

impl JsonbValue<'_> {
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
pub struct JsonbPair<'mcx> {
    /// Must be a `jbvString` (C: `JsonbValue key`).
    pub key: JsonbValue<'mcx>,
    /// May be of any type (C: `JsonbValue value`).
    pub value: JsonbValue<'mcx>,
    /// Pair's index in the original sequence, for last-observed-wins dedup.
    pub order: u32,
}

/// Conversion state used when parsing Jsonb from text or coercing types
/// (C: `struct JsonbParseState`).  Modeled as a stack of frames threaded by
/// `next`, mirroring the C singly linked list.  The frame chain is heap-boxed on
/// the global allocator (a small, bounded stack depth, not a hot byte-run
/// payload); only the contained `JsonbValue` tree is arena-backed.
#[derive(Clone, Debug)]
pub struct JsonbParseState<'mcx> {
    pub cont_val: JsonbValue<'mcx>,
    pub size: usize,
    /// Check object key uniqueness.
    pub unique_keys: bool,
    /// Skip null object fields.
    pub skip_nulls: bool,
    /// Parent frame (C: `JsonbParseState *next`).
    pub next: Option<alloc::boxed::Box<JsonbParseState<'mcx>>>,
}

extern crate alloc;

/// Iterator over an on-disk `JsonbContainer` (C: `struct JsonbIterator`).
///
/// `dataProper` is replaced by `data_proper` (a byte offset within the
/// container window).
///
/// The backing bytes are a `&'mcx [u8]` borrow of the source document buffer
/// (`buf`) plus a `cont_start` window offset.  When the iterator recurses into a
/// nested container, the child iterator **shares the same borrow** and only
/// records the nested container's start offset in `cont_start`, instead of
/// copying the nested sub-slice.  This mirrors C, where every nesting level's
/// `JsonbIterator` holds a raw `JsonbContainer *` into the same document buffer.
/// The arena (not a refcount) owns the buffer; nothing is copied per recursion.
/// Use [`JsonbIterator::container`] to get the windowed `&[u8]` the call sites
/// operate on.
#[derive(Clone, Debug)]
pub struct JsonbIterator<'mcx> {
    /// The memory context the iterator works in (C: `CurrentMemoryContext`).
    /// The element-count placeholder spines the iterator produces for
    /// `WJB_BEGIN_ARRAY`/`WJB_BEGIN_OBJECT` are arena-allocated here.
    pub mcx: ::mcx::Mcx<'mcx>,
    /// The document buffer the container bytes live in, borrowed for `'mcx`
    /// (C: the document the `JsonbContainer *` points into).  Shared across the
    /// parent/child iterator chain by copying the borrow so recursion never
    /// re-copies a nested container.
    pub buf: &'mcx [u8],
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
    pub parent: Option<alloc::boxed::Box<JsonbIterator<'mcx>>>,
    /// Byte position of `container` within the root container of its origin
    /// document (0 for the document root).  Threaded into the `offset` field of
    /// any `jbvBinary` children so `.keyvalue()` ids stay document-relative.
    /// This is bookkeeping unique to the safe port (C reconstructs it from raw
    /// container pointers).
    pub doc_offset: i32,
}

impl<'mcx> JsonbIterator<'mcx> {
    /// The container bytes this iterator operates on, as a windowed view into
    /// the document buffer (C: `JsonbContainer *container`).  All the
    /// container-relative indexing (`data_proper`, `children_off`, `JEntry`
    /// reads) is taken against this slice.
    #[inline]
    pub fn container(&self) -> &'mcx [u8] {
        &self.buf[self.cont_start..]
    }
}
