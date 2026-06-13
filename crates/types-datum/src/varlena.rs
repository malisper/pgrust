//! Owned varlena images (`varatt.h`, `c.h`).
//!
//! C builds a variable-length datum in palloc'd memory and stamps the 4-byte
//! length word with `SET_VARSIZE` before handing the pointer around as
//! `bytea *` / `text *` / `struct varlena *`. The owned Rust analog is
//! [`Varlena`]: the complete in-memory image (header + payload) in
//! context-allocated storage. The header encoding lives here and only here —
//! producers hand over the raw image and [`Varlena::from_image`] stamps it.

use mcx::PgVec;

/// `VARHDRSZ` (`c.h`): `sizeof(int32)` — the 4-byte varlena length word.
pub const VARHDRSZ: usize = 4;

/// `VARSIZE_4B` mask (`varatt.h`): the length occupies 30 bits of the header.
const VARLENA_SIZE_MASK: u32 = 0x3FFF_FFFF;

/// `SET_VARSIZE_4B(PTR, len)` (`varatt.h`) — the 4-byte-header, uncompressed,
/// inline encoding of a total length (header included), as the native-order
/// `uint32` C stores in `va_header`. Big-endian builds store the length with
/// the two tag bits on top (`(len) & 0x3FFFFFFF`); little-endian builds keep
/// the tag bits in the low byte (`((uint32) (len)) << 2`).
fn set_varsize_4b(len: usize) -> [u8; VARHDRSZ] {
    debug_assert!(len as u64 <= VARLENA_SIZE_MASK as u64);
    #[cfg(target_endian = "big")]
    let header = (len as u32) & VARLENA_SIZE_MASK;
    #[cfg(target_endian = "little")]
    let header = (len as u32) << 2;
    header.to_ne_bytes()
}

/// `VARSIZE_4B(PTR)` (`varatt.h`) — total length (header included) read back
/// from the native-order header word.
fn varsize_4b(header: [u8; VARHDRSZ]) -> usize {
    let word = u32::from_ne_bytes(header);
    #[cfg(target_endian = "big")]
    let len = word & VARLENA_SIZE_MASK;
    #[cfg(target_endian = "little")]
    let len = (word >> 2) & VARLENA_SIZE_MASK;
    len as usize
}

/// An owned `struct varlena` image (`c.h`): the 4-byte length word followed by
/// the payload, in context-allocated storage — the trimmed analog of a
/// palloc'd `struct varlena *` in the 4-byte-header uncompressed inline
/// format (`VARATT_IS_4B_U`). Toasted/compressed/short-header forms are not
/// representable; they stay with the units that own detoasting.
#[derive(Debug)]
pub struct Varlena<'mcx> {
    image: PgVec<'mcx, u8>,
}

/// `typedef struct varlena bytea` (`c.h`).
pub type Bytea<'mcx> = Varlena<'mcx>;

impl<'mcx> Varlena<'mcx> {
    /// Stamp a fully built image with its length word — C's
    /// `SET_VARSIZE(result, len)` over a buffer whose first `VARHDRSZ` bytes
    /// were reserved for the header. Panics (C: `Assert`) if the image is
    /// shorter than the header it must contain.
    pub fn from_image(mut image: PgVec<'mcx, u8>) -> Self {
        let len = image.len();
        assert!(len >= VARHDRSZ);
        image[..VARHDRSZ].copy_from_slice(&set_varsize_4b(len));
        Varlena { image }
    }

    /// `VARSIZE(PTR)` — total length, header included.
    pub fn varsize(&self) -> usize {
        let mut header = [0u8; VARHDRSZ];
        header.copy_from_slice(&self.image[..VARHDRSZ]);
        varsize_4b(header)
    }

    /// `VARDATA(PTR)` / `VARSIZE - VARHDRSZ` — the payload bytes.
    pub fn data(&self) -> &[u8] {
        &self.image[VARHDRSZ..]
    }

    /// The complete image (header + payload), as C would see the `bytea *`
    /// memory.
    pub fn as_bytes(&self) -> &[u8] {
        &self.image
    }

    /// Take back the raw image storage.
    pub fn into_image(self) -> PgVec<'mcx, u8> {
        self.image
    }
}
