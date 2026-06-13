//! The typed expanded-object handle (`utils/expandeddatum.h`).
//!
//! In C an expanded TOAST datum is a `varattrib_1b_e` whose tag is
//! `VARTAG_EXPANDED_RO`/`_RW`; `DatumGetEOHP` reaches through it to the
//! `ExpandedObjectHeader *`. The owned model keeps the verbatim datum bytes,
//! but crosses seams (e.g. `EOH_get_flat_size`/`EOH_flatten_into`) as this
//! typed reference rather than an untyped `&[u8]` — construction asserts the
//! `VARTAG_EXPANDED` shape, so a non-expanded datum stops loud at the
//! boundary instead of being silently misread by the owner.

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
