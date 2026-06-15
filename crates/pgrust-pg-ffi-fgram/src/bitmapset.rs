use core::ffi::c_int;
use core::mem::offset_of;
use core::slice;

#[cfg(target_pointer_width = "32")]
use crate::types::uint32;
#[cfg(target_pointer_width = "64")]
use crate::types::uint64;
use crate::types::NodeTag;

pub const T_Bitmapset: NodeTag = 445;

#[cfg(target_pointer_width = "64")]
pub type bitmapword = uint64;
#[cfg(target_pointer_width = "64")]
pub type signedbitmapword = i64;
#[cfg(target_pointer_width = "64")]
pub const BITS_PER_BITMAPWORD: usize = 64;

#[cfg(target_pointer_width = "32")]
pub type bitmapword = uint32;
#[cfg(target_pointer_width = "32")]
pub type signedbitmapword = i32;
#[cfg(target_pointer_width = "32")]
pub const BITS_PER_BITMAPWORD: usize = 32;

#[repr(C)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BMS_Comparison {
    BMS_EQUAL = 0,
    BMS_SUBSET1 = 1,
    BMS_SUBSET2 = 2,
    BMS_DIFFERENT = 3,
}

#[repr(C)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BMS_Membership {
    BMS_EMPTY_SET = 0,
    BMS_SINGLETON = 1,
    BMS_MULTIPLE = 2,
}

#[repr(C)]
pub struct Bitmapset {
    type_: NodeTag,
    nwords: c_int,
    words: [bitmapword; 0],
}

impl Bitmapset {
    pub fn header_size() -> usize {
        offset_of!(Self, words)
    }

    pub fn allocation_size(nwords: usize) -> Option<usize> {
        Self::header_size().checked_add(nwords.checked_mul(core::mem::size_of::<bitmapword>())?)
    }

    /// # Safety
    ///
    /// `raw` must point at writable storage large enough for a `Bitmapset`
    /// header plus `nwords` bitmap words.
    pub unsafe fn initialize(raw: *mut Self, nwords: c_int) {
        unsafe {
            core::ptr::addr_of_mut!((*raw).type_).write(T_Bitmapset);
            core::ptr::addr_of_mut!((*raw).nwords).write(nwords);
        }
    }

    pub fn node_tag(&self) -> NodeTag {
        self.type_
    }

    pub fn nwords(&self) -> c_int {
        self.nwords
    }

    pub fn set_nwords(&mut self, nwords: c_int) {
        self.nwords = nwords;
    }

    pub fn words(&self) -> &[bitmapword] {
        unsafe { slice::from_raw_parts(self.words_ptr(), self.nwords as usize) }
    }

    pub fn words_mut(&mut self) -> &mut [bitmapword] {
        unsafe { slice::from_raw_parts_mut(self.words_mut_ptr(), self.nwords as usize) }
    }

    pub fn words_ptr(&self) -> *const bitmapword {
        core::ptr::addr_of!(self.words).cast::<bitmapword>()
    }

    pub fn words_mut_ptr(&mut self) -> *mut bitmapword {
        core::ptr::addr_of_mut!(self.words).cast::<bitmapword>()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use core::mem::{align_of, offset_of, size_of};

    #[test]
    fn bitmapset_layout_matches_postgres_abi() {
        assert_eq!(offset_of!(Bitmapset, type_), 0);
        assert_eq!(offset_of!(Bitmapset, nwords), 4);
        assert_eq!(offset_of!(Bitmapset, words), 8);
        assert_eq!(Bitmapset::header_size(), 8);
        assert_eq!(size_of::<Bitmapset>(), 8);
        assert_eq!(align_of::<Bitmapset>(), align_of::<bitmapword>());
    }
}
