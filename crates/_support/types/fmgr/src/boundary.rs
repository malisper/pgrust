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
// value type `::types_tuple::Datum<'mcx>` (its `ByVal` arm is the bare machine
// word; `from_*`/`as_*` are the conversion methods). The deprecated shim newtype
// `::datum::Datum` is no longer used by this crate's own code.
use ::types_tuple::Datum;

// The `ExpandedObject` trait now lives in the lower `types-datum` crate so that
// both `types-tuple` (the canonical `Datum::Expanded` arm) and this crate
// (`RefPayload::Expanded`) can name the one trait without a layering cycle.
// Re-exported here so existing `fmgr::ExpandedObject` paths keep working.
pub use ::datum::ExpandedObject;

/// The owned referent of a pass-by-reference `Datum`, carried by value at the
/// fmgr boundary instead of behind a raw pointer.
///
/// * [`RefPayload::Cstring`] is C's `char *` (`cstring`) — owned text, never a
///   `*const c_char` / `CStr`.
/// * [`RefPayload::Varlena`] is C's `struct varlena *` (`text`/`bytea`/numeric/
///   array/fixed-by-ref) — its HEADER-FUL byte image, owned `Vec<u8>`: the
///   complete `struct varlena` memory C would see, the 4-byte length word
///   (`VARHDRSZ`/`set_varsize_4b`) followed by the payload. This is the single,
///   self-describing representation everywhere (no header-LESS variant): an adt
///   core reads the payload via `&image[VARHDRSZ..]` and writes by prepending
///   `set_varsize_4b(4 + payload.len())`. Carried VERBATIM across the fmgr-core
///   boundary — no strip/restamp.
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
    /// A C `internal` pseudo-type value — a `void *` to live, caller-owned
    /// mutable state (e.g. an aggregate's `NumericAggState`/`ArrayBuildState`).
    /// The owned referent is an erased `Box<dyn Any>`; the callee downcasts it
    /// to its private state type and mutates it in place, returning the same
    /// box. C never `datumCopy`s, flattens, or compares an `internal` Datum, so
    /// the byte-oriented operations panic on this arm (a wiring bug, not a data
    /// path).
    Internal(Box<dyn core::any::Any>),
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

    /// Borrow the payload as a self-describing by-reference varlena image,
    /// accepting either the generic `Varlena` lane or the `Composite`
    /// (`HeapTupleHeader`) lane. In C both are the same physical image: a
    /// pointer to a varlena-tagged block. The `Varlena`/`Composite` split is a
    /// port-side tag with no C analogue, so a consumer that only needs the
    /// raw varlena bytes (e.g. a generic by-reference *element* whose subtype
    /// happens to be composite) must accept both.
    pub fn as_byref_image(&self) -> Option<&[u8]> {
        match self {
            RefPayload::Varlena(b) | RefPayload::Composite(b) => Some(b.as_slice()),
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

    /// True iff this is an `internal` pseudo-type value.
    pub fn is_internal(&self) -> bool {
        matches!(self, RefPayload::Internal(_))
    }

    /// CHECKED `&T` downcast of an `internal` payload (C: the unchecked
    /// `(StateType *) PG_GETARG_POINTER(0)`). `None` if this is not an
    /// `Internal` arm; panics on a type mismatch (a wiring bug).
    pub fn as_internal<T: core::any::Any>(&self) -> Option<&T> {
        match self {
            RefPayload::Internal(a) => Some(a.downcast_ref::<T>().unwrap_or_else(|| {
                panic!(
                    "RefPayload::Internal: downcast_ref to {} failed",
                    core::any::type_name::<T>()
                )
            })),
            _ => None,
        }
    }

    /// CHECKED `&mut T` downcast of an `internal` payload.
    pub fn as_internal_mut<T: core::any::Any>(&mut self) -> Option<&mut T> {
        match self {
            RefPayload::Internal(a) => Some(a.downcast_mut::<T>().unwrap_or_else(|| {
                panic!(
                    "RefPayload::Internal: downcast_mut to {} failed",
                    core::any::type_name::<T>()
                )
            })),
            _ => None,
        }
    }

    /// Take the erased `internal` box out, consuming the payload.
    pub fn into_internal(self) -> Option<Box<dyn core::any::Any>> {
        match self {
            RefPayload::Internal(a) => Some(a),
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
            RefPayload::Internal(_) => {
                panic!("RefPayload::Internal cannot be flattened (C: an internal Datum is never serialized; use the aggregate's serialfn)")
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
            RefPayload::Internal(_) => {
                panic!("RefPayload::Internal cannot be cloned (C: an internal Datum is never datumCopy'd)")
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
            RefPayload::Internal(_) => f.debug_tuple("Internal").field(&"<opaque>").finish(),
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
            // An `internal` value has no value-equality (C compares the void*
            // pointers; two distinct boxes are never equal). Identity is tested
            // out-of-band by the caller, never through this impl.
            (RefPayload::Internal(_), _) | (_, RefPayload::Internal(_)) => false,
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
