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

// Owned varlena image (header + payload), VARATT_IS_4B_U form only;
// toasted/compressed/short forms stay with the units that own detoasting.
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
    fn header_encoding_round_trips() {
        for len in [VARHDRSZ, 5, 100, 0x3FFF_FFFF] {
            assert_eq!(varsize_4b(set_varsize_4b(len)), len);
        }
    }

    #[test]
    #[should_panic]
    fn image_shorter_than_header_panics() {
        let ctx = MemoryContext::new("varlena-test");
        let image: PgVec<u8> = PgVec::new_in(ctx.mcx());
        let _ = Varlena::from_image(image);
    }
}
