use core::ffi::{c_int, c_void};

use crate::{AttrNumber, Datum, MemoryContext, Oid};

pub const BTORDER_PROC: i16 = 1;
pub const BTSORTSUPPORT_PROC: i16 = 2;
pub const GIST_SORTSUPPORT_PROC: i16 = 11;
pub const GIST_AM_OID: Oid = 783;

/// A pointer to `SortSupportData`, as passed to sort-support comparators.
pub type SortSupport = *mut SortSupportData;

pub type SortSupportComparator =
    Option<unsafe extern "C" fn(Datum, Datum, *mut SortSupportData) -> c_int>;
pub type SortSupportAbbrevConverter =
    Option<unsafe extern "C" fn(Datum, *mut SortSupportData) -> Datum>;
pub type SortSupportAbbrevAbort = Option<unsafe extern "C" fn(c_int, *mut SortSupportData) -> bool>;

#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct SortSupportData {
    pub ssup_cxt: MemoryContext,
    pub ssup_collation: Oid,
    pub ssup_reverse: bool,
    pub ssup_nulls_first: bool,
    pub ssup_attno: AttrNumber,
    pub ssup_extra: *mut c_void,
    pub comparator: SortSupportComparator,
    pub abbreviate: bool,
    pub abbrev_converter: SortSupportAbbrevConverter,
    pub abbrev_abort: SortSupportAbbrevAbort,
    pub abbrev_full_comparator: SortSupportComparator,
}

impl SortSupportData {
    pub const fn empty() -> Self {
        Self {
            ssup_cxt: core::ptr::null_mut(),
            ssup_collation: 0,
            ssup_reverse: false,
            ssup_nulls_first: false,
            ssup_attno: 0,
            ssup_extra: core::ptr::null_mut(),
            comparator: None,
            abbreviate: false,
            abbrev_converter: None,
            abbrev_abort: None,
            abbrev_full_comparator: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sort_support_data_matches_generated_layout() {
        assert_eq!(core::mem::size_of::<SortSupportData>(), 64);
        assert_eq!(core::mem::align_of::<SortSupportData>(), 8);
        assert_eq!(core::mem::offset_of!(SortSupportData, ssup_cxt), 0);
        assert_eq!(core::mem::offset_of!(SortSupportData, ssup_collation), 8);
        assert_eq!(core::mem::offset_of!(SortSupportData, ssup_reverse), 12);
        assert_eq!(core::mem::offset_of!(SortSupportData, ssup_nulls_first), 13);
        assert_eq!(core::mem::offset_of!(SortSupportData, ssup_attno), 14);
        assert_eq!(core::mem::offset_of!(SortSupportData, ssup_extra), 16);
        assert_eq!(core::mem::offset_of!(SortSupportData, comparator), 24);
        assert_eq!(core::mem::offset_of!(SortSupportData, abbreviate), 32);
        assert_eq!(core::mem::offset_of!(SortSupportData, abbrev_converter), 40);
        assert_eq!(core::mem::offset_of!(SortSupportData, abbrev_abort), 48);
        assert_eq!(
            core::mem::offset_of!(SortSupportData, abbrev_full_comparator),
            56
        );
    }
}
