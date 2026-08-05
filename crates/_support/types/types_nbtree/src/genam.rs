// IndexUniqueCheck (access/genam.h); carried here until the genam unit lands.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u32)]
pub enum IndexUniqueCheck {
    UNIQUE_CHECK_NO = 0,
    UNIQUE_CHECK_YES,
    UNIQUE_CHECK_PARTIAL,
    UNIQUE_CHECK_EXISTING,
}

pub use IndexUniqueCheck::*;

// IndexBulkDeleteResult (access/genam.h).
#[derive(Clone, Copy, Debug, Default)]
pub struct IndexBulkDeleteResult {
    pub num_pages: types_core::BlockNumber,
    pub estimated_count: bool,
    pub num_index_tuples: f64,
    pub tuples_removed: f64,
    pub pages_newly_deleted: types_core::BlockNumber,
    pub pages_deleted: types_core::BlockNumber,
    pub pages_free: types_core::BlockNumber,
}

#[cfg(test)]
mod genam_tests {
    use super::*;

    #[test]
    fn unique_check_codes_match_genam_h() {
        assert_eq!(UNIQUE_CHECK_NO as u32, 0);
        assert_eq!(UNIQUE_CHECK_YES as u32, 1);
        assert_eq!(UNIQUE_CHECK_PARTIAL as u32, 2);
        assert_eq!(UNIQUE_CHECK_EXISTING as u32, 3);
    }
}
