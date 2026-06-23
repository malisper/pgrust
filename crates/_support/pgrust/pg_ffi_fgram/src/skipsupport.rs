use core::ffi::c_void;

use crate::Datum;

pub const BTSKIPSUPPORT_PROC: i16 = 6;

pub type SkipSupportIncDec = Option<unsafe extern "C" fn(*mut c_void, Datum, *mut bool) -> Datum>;

#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct SkipSupportData {
    pub low_elem: Datum,
    pub high_elem: Datum,
    pub decrement: SkipSupportIncDec,
    pub increment: SkipSupportIncDec,
}

impl SkipSupportData {
    pub const fn empty() -> Self {
        Self {
            low_elem: 0,
            high_elem: 0,
            decrement: None,
            increment: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn skip_support_data_matches_generated_layout() {
        assert_eq!(core::mem::size_of::<SkipSupportData>(), 32);
        assert_eq!(core::mem::align_of::<SkipSupportData>(), 8);
        assert_eq!(core::mem::offset_of!(SkipSupportData, low_elem), 0);
        assert_eq!(core::mem::offset_of!(SkipSupportData, high_elem), 8);
        assert_eq!(core::mem::offset_of!(SkipSupportData, decrement), 16);
        assert_eq!(core::mem::offset_of!(SkipSupportData, increment), 24);
    }
}
