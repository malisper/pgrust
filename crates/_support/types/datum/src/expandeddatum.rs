extern crate alloc;
use alloc::vec;

// `: Any` enables the checked downcast (C's EA_MAGIC identity check);
// flatten_into must write exactly get_flat_size() bytes.
pub trait ExpandedObject: core::any::Any {
    fn get_flat_size(&self) -> usize;
    fn flatten_into(&self, dst: &mut [u8]);
}

pub fn flatten_expanded(eo: &dyn ExpandedObject) -> alloc::vec::Vec<u8> {
    let n = eo.get_flat_size();
    let mut dst = vec![0u8; n];
    eo.flatten_into(&mut dst);
    dst
}

pub const VARTAG_EXPANDED_RO: u8 = 2;
pub const VARTAG_EXPANDED_RW: u8 = 3;

#[derive(Clone, Copy, Debug)]
pub struct ExpandedObjectRef<'a> {
    bytes: &'a [u8],
}

impl<'a> ExpandedObjectRef<'a> {
    // Panics unless VARATT_IS_1B_E with an expanded tag (C: DatumGetEOHP Assert).
    pub fn from_expanded_datum_bytes(bytes: &'a [u8]) -> Self {
        assert!(
            bytes.len() >= 2 && bytes[0] == 0x01 && (bytes[1] & !1) == VARTAG_EXPANDED_RO,
            "ExpandedObjectRef: datum is not a VARTAG_EXPANDED external varlena"
        );
        ExpandedObjectRef { bytes }
    }

    pub fn is_read_write(&self) -> bool {
        self.bytes[1] == VARTAG_EXPANDED_RW
    }

    pub fn as_datum_bytes(&self) -> &'a [u8] {
        self.bytes
    }
}
