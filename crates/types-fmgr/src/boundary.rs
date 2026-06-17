//! Fmgr **boundary** value vocabulary — the "Option 4" convention.
//!
//! PostgreSQL's `Datum` is a single machine word. A pass-by-value type lives
//! inside that word; a pass-by-reference type's word is a pointer into palloc'd
//! memory. The idiomatic rewrite forbids raw pointers, so a by-reference value
//! cannot be a bare `Datum` word — its owned referent is carried by value at the
//! fmgr call boundary as an [`RefPayload`]: `String` for a `cstring`, `Vec<u8>`
//! for a varlena, or a live [`ExpandedObject`]. The bare `Datum` stays a word for
//! every by-value type.
//!
//! | C boundary fn        | C arg / return            | shape                |
//! |----------------------|---------------------------|----------------------|
//! | `InputFunctionCall`  | `char *str` → `Datum`     | `&str` → [`FmgrOut`] |
//! | `OutputFunctionCall` | `Datum` → `char *`        | [`FmgrArg`] → String |
//! | `ReceiveFunctionCall`| `StringInfo buf` → `Datum`| `&[u8]` → [`FmgrOut`]|
//! | `SendFunctionCall`   | `Datum` → `bytea *`       | [`FmgrArg`] → Vec<u8>|

// Datum-unification migration: the by-value boundary arms carry the canonical
// value type `types_tuple::Datum<'mcx>` (its `ByVal` arm is the bare machine
// word; `from_*`/`as_*` are the conversion methods). The deprecated shim newtype
// `types_datum::Datum` is no longer used by this crate's own code.
use types_tuple::Datum;

// The `ExpandedObject` trait now lives in the lower `types-datum` crate so that
// both `types-tuple` (the canonical `Datum::Expanded` arm) and this crate
// (`RefPayload::Expanded`) can name the one trait without a layering cycle.
// Re-exported here so existing `types_fmgr::ExpandedObject` paths keep working.
pub use types_datum::ExpandedObject;

/// The owned referent of a pass-by-reference `Datum`, carried by value at the
/// fmgr boundary instead of behind a raw pointer.
///
/// * [`RefPayload::Cstring`] is C's `char *` (`cstring`) — owned text, never a
///   `*const c_char` / `CStr`.
/// * [`RefPayload::Varlena`] is C's `struct varlena *` (`text`/`bytea`/numeric/
///   array/fixed-by-ref) — its byte image, owned `Vec<u8>`.
/// * [`RefPayload::Expanded`] is C's `VARATT_IS_EXPANDED` value — a live,
///   possibly-mutable in-memory object.
///
/// Invariant (2): the bare `Datum` word is meaningless for a by-reference value;
/// only the caller's `typbyval` decides which lane to read.
pub enum RefPayload {
    /// A C `cstring` (`char *`): owned text, no terminating NUL stored.
    Cstring(String),
    /// A C varlena: the owned byte image (also covers fixed-length-by-ref types).
    Varlena(Vec<u8>),
    /// A composite / record value (C: a `Datum` that is a pointer to a
    /// `HeapTupleHeader`, itself a varlena-tagged contiguous block). The owned
    /// byte image is the flat `HeapTupleHeader` Datum image — the first four
    /// bytes are the `datum_len_` varlena length word, exactly the block C would
    /// `DatumGetHeapTupleHeader` from. (Serialized via
    /// `FormedTuple::to_datum_image`; reconstructed via
    /// `FormedTuple::from_datum_image`.)
    Composite(Vec<u8>),
    /// A live expanded object (PG `VARATT_IS_EXPANDED`).
    Expanded(Box<dyn ExpandedObject>),
}

impl RefPayload {
    /// Borrow the payload as a `cstring`, if it is one.
    pub fn as_cstring(&self) -> Option<&str> {
        match self {
            RefPayload::Cstring(s) => Some(s.as_str()),
            _ => None,
        }
    }

    /// Borrow the payload as a varlena byte image, if it is one.
    pub fn as_varlena(&self) -> Option<&[u8]> {
        match self {
            RefPayload::Varlena(b) => Some(b.as_slice()),
            _ => None,
        }
    }

    /// Borrow the payload as a composite `HeapTupleHeader` Datum image, if it
    /// is one.
    pub fn as_composite(&self) -> Option<&[u8]> {
        match self {
            RefPayload::Composite(b) => Some(b.as_slice()),
            _ => None,
        }
    }

    /// PG `VARATT_IS_EXPANDED`: true iff this is a live expanded object.
    pub fn is_expanded(&self) -> bool {
        matches!(self, RefPayload::Expanded(_))
    }

    /// Borrow the expanded object read-only (PG `VARTAG_EXPANDED_RO`).
    pub fn as_expanded(&self) -> Option<&dyn ExpandedObject> {
        match self {
            RefPayload::Expanded(eo) => Some(eo.as_ref()),
            _ => None,
        }
    }

    /// Borrow the expanded object read/write (PG `VARTAG_EXPANDED_RW`).
    pub fn as_expanded_mut(&mut self) -> Option<&mut dyn ExpandedObject> {
        match self {
            RefPayload::Expanded(eo) => Some(eo.as_mut()),
            _ => None,
        }
    }

    /// Flatten-on-store (the disk/wire path): consume and produce flat bytes.
    pub fn flatten(self) -> Vec<u8> {
        match self {
            RefPayload::Varlena(b) => b,
            RefPayload::Composite(b) => b,
            RefPayload::Cstring(s) => s.into_bytes(),
            RefPayload::Expanded(eo) => {
                let n = eo.get_flat_size();
                let mut dst = vec![0u8; n];
                eo.flatten_into(&mut dst);
                dst
            }
        }
    }

    /// Value-only clone for the by-ref byte arms (`Cstring`/`Varlena`); an
    /// `Expanded` value flattens into a `Varlena` (no `Clone` on the trait obj).
    pub fn clone_flat(&self) -> RefPayload {
        match self {
            RefPayload::Cstring(s) => RefPayload::Cstring(s.clone()),
            RefPayload::Varlena(b) => RefPayload::Varlena(b.clone()),
            RefPayload::Composite(b) => RefPayload::Composite(b.clone()),
            RefPayload::Expanded(eo) => {
                let n = eo.get_flat_size();
                let mut dst = vec![0u8; n];
                eo.flatten_into(&mut dst);
                RefPayload::Varlena(dst)
            }
        }
    }
}

impl Clone for RefPayload {
    fn clone(&self) -> Self {
        self.clone_flat()
    }
}

impl core::fmt::Debug for RefPayload {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            RefPayload::Cstring(s) => f.debug_tuple("Cstring").field(s).finish(),
            RefPayload::Varlena(b) => f.debug_tuple("Varlena").field(b).finish(),
            RefPayload::Composite(b) => f.debug_tuple("Composite").field(b).finish(),
            RefPayload::Expanded(eo) => f
                .debug_struct("Expanded")
                .field("flat_size", &eo.get_flat_size())
                .finish(),
        }
    }
}

impl PartialEq for RefPayload {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (RefPayload::Cstring(a), RefPayload::Cstring(b)) => a == b,
            (RefPayload::Varlena(a), RefPayload::Varlena(b)) => a == b,
            (RefPayload::Composite(a), RefPayload::Composite(b)) => a == b,
            (RefPayload::Cstring(_), RefPayload::Varlena(_))
            | (RefPayload::Varlena(_), RefPayload::Cstring(_)) => false,
            // A composite image only equals another composite image (never a
            // bare cstring/varlena, even byte-for-byte).
            (RefPayload::Composite(_), _) | (_, RefPayload::Composite(_)) => false,
            (a, b) => a.clone_flat().flatten() == b.clone_flat().flatten(),
        }
    }
}

impl Eq for RefPayload {}

/// What an fmgr function **returns** at the call boundary.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum FmgrOut<'mcx> {
    /// A pass-by-value result: the canonical `Datum` (its `ByVal` arm is the
    /// machine word the value lives in).
    ByVal(Datum<'mcx>),
    /// A pass-by-reference result: the owned referent.
    Ref(RefPayload),
}

impl<'mcx> FmgrOut<'mcx> {
    /// The by-value `Datum`, if this is a by-value result.
    pub fn by_val(&self) -> Option<Datum<'mcx>> {
        match self {
            FmgrOut::ByVal(d) => Some(d.clone()),
            FmgrOut::Ref(_) => None,
        }
    }

    /// The by-reference payload, if this is a by-reference result.
    pub fn as_ref_payload(&self) -> Option<&RefPayload> {
        match self {
            FmgrOut::Ref(p) => Some(p),
            FmgrOut::ByVal(_) => None,
        }
    }
}

/// A single fmgr **argument** at the call boundary.
///
/// `'a` borrows the by-reference referent; `'mcx` is the canonical value's
/// allocation lifetime. (No longer `Copy`: the canonical `Datum<'mcx>` owns its
/// by-reference bytes and so is not `Copy`.)
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum FmgrArg<'a, 'mcx> {
    /// A pass-by-value argument: the canonical `Datum` (its `ByVal` arm is the
    /// machine word the value lives in).
    ByVal(Datum<'mcx>),
    /// A pass-by-reference argument: a borrow of the caller-owned referent.
    Ref(&'a RefPayload),
}

impl<'a, 'mcx> FmgrArg<'a, 'mcx> {
    /// The by-value `Datum`, if this argument is by value.
    pub fn by_val(&self) -> Option<Datum<'mcx>> {
        match self {
            FmgrArg::ByVal(d) => Some(d.clone()),
            FmgrArg::Ref(_) => None,
        }
    }

    /// The borrowed by-reference payload, if this argument is by reference.
    pub fn as_ref_payload(&self) -> Option<&'a RefPayload> {
        match self {
            FmgrArg::Ref(p) => Some(p),
            FmgrArg::ByVal(_) => None,
        }
    }
}
