use ::mcx::PgVec;

pub const VARHDRSZ: usize = 4;

const VARLENA_SIZE_MASK: u32 = 0x3FFF_FFFF;

// SET_VARSIZE_4B: LE keeps the two tag bits in the low byte, BE on top.
pub fn set_varsize_4b(len: usize) -> [u8; VARHDRSZ] {
    debug_assert!(len as u64 <= VARLENA_SIZE_MASK as u64);
    #[cfg(target_endian = "big")]
    let header = (len as u32) & VARLENA_SIZE_MASK;
    #[cfg(target_endian = "little")]
    let header = (len as u32) << 2;
    header.to_ne_bytes()
}

fn varsize_4b(header: [u8; VARHDRSZ]) -> usize {
    let word = u32::from_ne_bytes(header);
    #[cfg(target_endian = "big")]
    let len = word & VARLENA_SIZE_MASK;
    #[cfg(target_endian = "little")]
    let len = (word >> 2) & VARLENA_SIZE_MASK;
    len as usize
}

// Borrowed raw-image read lane (C's VARSIZE/VARDATA off a thin datum pointer); 4B-U form only.
#[derive(Clone, Copy, Debug)]
pub struct VarlenaRef<'a> {
    ptr: core::ptr::NonNull<u8>,
    _image: core::marker::PhantomData<&'a [u8]>,
}

impl<'a> VarlenaRef<'a> {
    /// # Safety
    /// `p`: live 4B-header (uncompressed, untoasted) varlena image, readable for its full VARSIZE and unwritten for `'a`.
    #[inline]
    pub unsafe fn from_ptr(p: *const u8) -> VarlenaRef<'a> {
        VarlenaRef {
            ptr: core::ptr::NonNull::new_unchecked(p.cast_mut()),
            _image: core::marker::PhantomData,
        }
    }

    pub fn from_varlena(v: &'a Varlena<'_>) -> VarlenaRef<'a> {
        // SAFETY: Varlena owns a full 4B-header image, borrowed for 'a.
        unsafe { Self::from_ptr(v.as_bytes().as_ptr()) }
    }

    #[inline]
    pub fn as_ptr(self) -> *const u8 {
        self.ptr.as_ptr()
    }

    #[inline]
    pub fn varsize(self) -> usize {
        // SAFETY: from_ptr contract — header readable.
        let word = unsafe { self.ptr.as_ptr().cast::<u32>().read_unaligned() };
        // 1B/1B_E headers misread as a u32 length; short-form datums (tuple,
        // array-element, param sources) must go through a VARSIZE_ANY lane.
        #[cfg(target_endian = "little")]
        debug_assert!(word & 0x01 == 0, "VarlenaRef::varsize on a 1-byte-header varlena");
        #[cfg(target_endian = "big")]
        debug_assert!(word & 0x8000_0000 == 0, "VarlenaRef::varsize on a 1-byte-header varlena");
        #[cfg(target_endian = "big")]
        let len = word & VARLENA_SIZE_MASK;
        #[cfg(target_endian = "little")]
        let len = word >> 2;
        len as usize
    }

    #[inline]
    pub fn data(self) -> &'a [u8] {
        // SAFETY: from_ptr contract — header readable.
        #[cfg(target_endian = "little")]
        debug_assert!(
            unsafe { *self.ptr.as_ptr() } & 0x03 == 0,
            "VarlenaRef::data on a non-4B-U varlena"
        );
        let len = self.varsize() - VARHDRSZ;
        // SAFETY: from_ptr contract — image readable for VARSIZE bytes.
        unsafe { core::slice::from_raw_parts(self.ptr.as_ptr().add(VARHDRSZ), len) }
    }

    #[inline]
    pub fn as_bytes(self) -> &'a [u8] {
        // SAFETY: from_ptr contract — image readable for VARSIZE bytes.
        unsafe { core::slice::from_raw_parts(self.ptr.as_ptr(), self.varsize()) }
    }
}

// Owned varlena image (header + payload), 4B-U only; toast/short forms stay with the detoasting units.
#[derive(Debug)]
pub struct Varlena<'mcx> {
    image: PgVec<'mcx, u8>,
}

pub type Bytea<'mcx> = Varlena<'mcx>;

impl<'mcx> Varlena<'mcx> {
    pub fn from_image(mut image: PgVec<'mcx, u8>) -> Self {
        let len = image.len();
        assert!(len >= VARHDRSZ);
        image[..VARHDRSZ].copy_from_slice(&set_varsize_4b(len));
        Varlena { image }
    }

    pub fn varsize(&self) -> usize {
        let mut header = [0u8; VARHDRSZ];
        header.copy_from_slice(&self.image[..VARHDRSZ]);
        varsize_4b(header)
    }

    pub fn data(&self) -> &[u8] {
        &self.image[VARHDRSZ..]
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.image
    }

    pub fn into_image(self) -> PgVec<'mcx, u8> {
        self.image
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ::mcx::MemoryContext;

    #[test]
    fn varlena_image_round_trip() {
        let ctx = MemoryContext::new("varlena-test");
        let mcx = ctx.mcx();
        let payload = b"hello varlena";
        let mut image: PgVec<u8> = PgVec::new_in(mcx);
        image.resize(VARHDRSZ, 0);
        image.extend_from_slice(payload);
        let v = Varlena::from_image(image);
        assert_eq!(v.varsize(), VARHDRSZ + payload.len());
        assert_eq!(v.data(), payload);
        assert_eq!(&v.as_bytes()[VARHDRSZ..], payload);
        assert_eq!(v.as_bytes().len(), VARHDRSZ + payload.len());
    }

    #[test]
    fn borrowed_ref_reads_match_owning_reads() {
        let ctx = MemoryContext::new("varlena-test");
        let mut image: PgVec<u8> = PgVec::new_in(ctx.mcx());
        image.resize(VARHDRSZ, 0);
        image.extend_from_slice(b"borrowed lane");
        let v = Varlena::from_image(image);
        let r = VarlenaRef::from_varlena(&v);
        assert_eq!(r.varsize(), v.varsize());
        assert_eq!(r.data(), v.data());
        assert_eq!(r.as_bytes(), v.as_bytes());
        // SAFETY: live image owned by `v` above.
        let raw = unsafe { VarlenaRef::from_ptr(v.as_bytes().as_ptr()) };
        assert_eq!(raw.varsize(), VARHDRSZ + 13);
        assert_eq!(raw.data(), b"borrowed lane");
    }

    #[test]
    fn header_encoding_round_trips() {
        for len in [VARHDRSZ, 5, 100, 0x3FFF_FFFF] {
            assert_eq!(varsize_4b(set_varsize_4b(len)), len);
        }
    }

    #[test]
    #[should_panic]
    #[cfg(debug_assertions)]
    fn varsize_on_short_header_panics() {
        let short: [u8; 4] = [(4u8 << 1) | 0x01, b'a', b'b', b'c'];
        // SAFETY: violates the 4B contract on purpose; the debug_assert fires
        // before any out-of-image read.
        let _ = unsafe { VarlenaRef::from_ptr(short.as_ptr()) }.varsize();
    }

    #[test]
    #[should_panic]
    fn image_shorter_than_header_panics() {
        let ctx = MemoryContext::new("varlena-test");
        let image: PgVec<u8> = PgVec::new_in(ctx.mcx());
        let _ = Varlena::from_image(image);
    }
}
