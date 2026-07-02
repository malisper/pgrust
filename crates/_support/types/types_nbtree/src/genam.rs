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
