//! The typed expanded-object handle (`utils/expandeddatum.h`).
//!
//! In C an expanded TOAST datum is a `varattrib_1b_e` whose tag is
//! `VARTAG_EXPANDED_RO`/`_RW`; `DatumGetEOHP` reaches through it to the
//! `ExpandedObjectHeader *`. The owned model keeps the verbatim datum bytes,
//! but crosses seams (e.g. `EOH_get_flat_size`/`EOH_flatten_into`) as this
//! typed reference rather than an untyped `&[u8]` — construction asserts the
//! `VARTAG_EXPANDED` shape, so a non-expanded datum stops loud at the
//! boundary instead of being silently misread by the owner.
//!
//! Also home to the [`ExpandedObject`] *trait* — the live, possibly-mutable
//! in-memory expanded object (PG `VARATT_IS_EXPANDED`) with its
//! `ExpandedObjectMethods` vtable. It lives here (the bottom of the
//! `types-datum` layer) so that both `types-tuple` (the canonical `Datum`
//! value enum) and `types-fmgr` (the fmgr boundary `RefPayload`) can name the
//! one trait without a layering cycle.

extern crate alloc;
use alloc::vec;

/// Mirror of PG's `ExpandedObjectMethods` vtable (`utils/expandeddatum.h`): the
/// two method pointers every expanded (`VARATT_IS_EXPANDED`) object exposes.
/// `: Any` enables the checked downcast (PG's `EA_MAGIC` identity check before a
/// C cast). The flattened image `flatten_into` writes must be exactly
/// `get_flat_size()` bytes (PG `allocated_size` cross-check).
pub trait ExpandedObject: core::any::Any {
    /// C: `EOM_get_flat_size_method` — bytes the flat varlena image needs.
    fn get_flat_size(&self) -> usize;
    /// C: `EOM_flatten_into_method` — serialize the flat varlena image into
    /// `dst`, whose length is exactly a preceding `get_flat_size()`.
    fn flatten_into(&self, dst: &mut [u8]);
}

/// Value-only clone of a live expanded object: flatten it into its varlena
/// byte image (there is no `Clone` on the trait object). Shared by both the
/// canonical `Datum::Expanded` arm and `RefPayload::Expanded`.
pub fn flatten_expanded(eo: &dyn ExpandedObject) -> alloc::vec::Vec<u8> {
    let n = eo.get_flat_size();
    let mut dst = vec![0u8; n];
    eo.flatten_into(&mut dst);
    dst
}

/// `VARTAG_EXPANDED_RO` (`varatt.h`, `enum vartag_external`).
pub const VARTAG_EXPANDED_RO: u8 = 2;
/// `VARTAG_EXPANDED_RW` (`varatt.h`, `enum vartag_external`).
pub const VARTAG_EXPANDED_RW: u8 = 3;

/// A typed reference to the verbatim datum bytes of an expanded external
/// varlena (`VARATT_IS_EXTERNAL_EXPANDED`) — the owned stand-in for C's
/// `ExpandedObjectHeader *` as produced by `DatumGetEOHP`.
#[derive(Clone, Copy, Debug)]
pub struct ExpandedObjectRef<'a> {
    bytes: &'a [u8],
}

impl<'a> ExpandedObjectRef<'a> {
    /// `DatumGetEOHP(datum)` over the datum's verbatim varlena bytes. Panics
    /// (C: the `Assert(VARATT_IS_EXTERNAL_EXPANDED(...))` in `DatumGetEOHP`)
    /// if the bytes are not a `VARTAG_EXPANDED_RO`/`_RW` TOAST pointer.
    pub fn from_expanded_datum_bytes(bytes: &'a [u8]) -> Self {
        // VARATT_IS_1B_E (va_header == 0x01) with an expanded tag.
        assert!(
            bytes.len() >= 2 && bytes[0] == 0x01 && (bytes[1] & !1) == VARTAG_EXPANDED_RO,
            "ExpandedObjectRef: datum is not a VARTAG_EXPANDED external varlena"
        );
        ExpandedObjectRef { bytes }
    }

    /// `VARTAG_IS_EXPANDED(tag) && (tag & 1) != 0` — is this the read-write
    /// flavor (`VARTAG_EXPANDED_RW`)?
    pub fn is_read_write(&self) -> bool {
        self.bytes[1] == VARTAG_EXPANDED_RW
    }

    /// The verbatim datum bytes (the full external TOAST pointer).
    pub fn as_datum_bytes(&self) -> &'a [u8] {
        self.bytes
    }
}
