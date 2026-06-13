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

use types_datum::Datum;

/// Mirror of PG's `ExpandedObjectMethods` vtable (`utils/expandeddatum.h`): the
/// two method pointers every expanded (`VARATT_IS_EXPANDED`) object exposes.
/// `: Any` enables the checked downcast (PG's `EA_MAGIC` identity check before a
/// C cast). The flattened image `flatten_into` writes must be exactly
/// `get_flat_size()` bytes (PG `allocated_size` cross-check).
pub trait ExpandedObject: std::any::Any {
    /// C: `EOM_get_flat_size_method` — bytes the flat varlena image needs.
    fn get_flat_size(&self) -> usize;
    /// C: `EOM_flatten_into_method` — serialize the flat varlena image into
    /// `dst`, whose length is exactly a preceding `get_flat_size()`.
    fn flatten_into(&self, dst: &mut [u8]);
}

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
            (RefPayload::Cstring(_), RefPayload::Varlena(_))
            | (RefPayload::Varlena(_), RefPayload::Cstring(_)) => false,
            (a, b) => a.clone_flat().flatten() == b.clone_flat().flatten(),
        }
    }
}

impl Eq for RefPayload {}

/// What an fmgr function **returns** at the call boundary.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum FmgrOut {
    /// A pass-by-value result: the `Datum` word is the value.
    ByVal(Datum),
    /// A pass-by-reference result: the owned referent.
    Ref(RefPayload),
}

impl FmgrOut {
    /// The by-value `Datum`, if this is a by-value result.
    pub fn by_val(&self) -> Option<Datum> {
        match self {
            FmgrOut::ByVal(d) => Some(*d),
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
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FmgrArg<'a> {
    /// A pass-by-value argument: the `Datum` word is the value.
    ByVal(Datum),
    /// A pass-by-reference argument: a borrow of the caller-owned referent.
    Ref(&'a RefPayload),
}

impl<'a> FmgrArg<'a> {
    /// The by-value `Datum`, if this argument is by value.
    pub fn by_val(&self) -> Option<Datum> {
        match self {
            FmgrArg::ByVal(d) => Some(*d),
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
