use core::ffi::{c_char, c_int, c_void, CStr};

pub use types_core::KeywordCategory;

pub type ScanKeywordHashFunc = Option<unsafe extern "C" fn(*const c_void, usize) -> c_int>;

#[derive(Debug, Clone, Copy)]
#[repr(C)]
pub struct ScanKeywordList {
    kw_string: *const c_char,
    kw_offsets: *const u16,
    hash: ScanKeywordHashFunc,
    num_keywords: c_int,
    max_kw_len: c_int,
}

unsafe impl Sync for ScanKeywordList {}

impl ScanKeywordList {
    /// # Safety
    ///
    /// `kw_string` must point at `num_keywords` NUL-terminated strings, and
    /// `kw_offsets` must contain `num_keywords` valid offsets into that storage.
    pub const unsafe fn from_static_parts(
        kw_string: *const c_char,
        kw_offsets: *const u16,
        hash: ScanKeywordHashFunc,
        num_keywords: c_int,
        max_kw_len: c_int,
    ) -> Self {
        Self {
            kw_string,
            kw_offsets,
            hash,
            num_keywords,
            max_kw_len,
        }
    }

    pub const fn hash(&self) -> ScanKeywordHashFunc {
        self.hash
    }

    pub const fn num_keywords(&self) -> usize {
        self.num_keywords as usize
    }

    pub const fn max_kw_len(&self) -> usize {
        self.max_kw_len as usize
    }

    pub fn keyword(&self, index: usize) -> Option<&str> {
        let cstr = self.keyword_cstr(index)?;
        cstr.to_str().ok()
    }

    pub fn keyword_cstr(&self, index: usize) -> Option<&CStr> {
        if index >= self.num_keywords() {
            return None;
        }

        let offset = unsafe { *self.kw_offsets.add(index) };
        let ptr = unsafe { self.kw_string.add(offset as usize) };
        Some(unsafe { CStr::from_ptr(ptr) })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scan_keyword_list_layout_matches_kwlookup_abi() {
        assert_eq!(core::mem::size_of::<ScanKeywordList>(), 32);
        assert_eq!(core::mem::align_of::<ScanKeywordList>(), 8);
    }

    #[test]
    fn keyword_lookup_over_static_parts() {
        static KW_STRING: &[u8] = b"abort\0all\0and\0";
        static KW_OFFSETS: [u16; 3] = [0, 6, 10];
        let list = unsafe {
            ScanKeywordList::from_static_parts(
                KW_STRING.as_ptr().cast(),
                KW_OFFSETS.as_ptr(),
                None,
                3,
                5,
            )
        };
        assert_eq!(list.num_keywords(), 3);
        assert_eq!(list.max_kw_len(), 5);
        assert_eq!(list.keyword(0), Some("abort"));
        assert_eq!(list.keyword(2), Some("and"));
        assert_eq!(list.keyword(3), None);
    }
}
