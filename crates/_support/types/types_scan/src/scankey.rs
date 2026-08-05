use ::datum::Datum;
use ::types_core::{AttrNumber, Oid};
use ::types_fmgr::FmgrInfo;

pub type StrategyNumber = u16;

pub const InvalidStrategy: StrategyNumber = 0;

pub const BTLessStrategyNumber: StrategyNumber = 1;
pub const BTLessEqualStrategyNumber: StrategyNumber = 2;
pub const BTEqualStrategyNumber: StrategyNumber = 3;
pub const BTGreaterEqualStrategyNumber: StrategyNumber = 4;
pub const BTGreaterStrategyNumber: StrategyNumber = 5;
pub const BTMaxStrategyNumber: StrategyNumber = 5;

pub const HTEqualStrategyNumber: StrategyNumber = 1;
pub const HTMaxStrategyNumber: StrategyNumber = 1;

pub const RTLeftStrategyNumber: StrategyNumber = 1;
pub const RTOverLeftStrategyNumber: StrategyNumber = 2;
pub const RTOverlapStrategyNumber: StrategyNumber = 3;
pub const RTOverRightStrategyNumber: StrategyNumber = 4;
pub const RTRightStrategyNumber: StrategyNumber = 5;
pub const RTSameStrategyNumber: StrategyNumber = 6;
pub const RTContainsStrategyNumber: StrategyNumber = 7;
pub const RTContainedByStrategyNumber: StrategyNumber = 8;
pub const RTOverBelowStrategyNumber: StrategyNumber = 9;
pub const RTBelowStrategyNumber: StrategyNumber = 10;
pub const RTAboveStrategyNumber: StrategyNumber = 11;
pub const RTOverAboveStrategyNumber: StrategyNumber = 12;
pub const RTOldContainsStrategyNumber: StrategyNumber = 13;
pub const RTOldContainedByStrategyNumber: StrategyNumber = 14;
pub const RTKNNSearchStrategyNumber: StrategyNumber = 15;
pub const RTContainsElemStrategyNumber: StrategyNumber = 16;
pub const RTAdjacentStrategyNumber: StrategyNumber = 17;
pub const RTEqualStrategyNumber: StrategyNumber = 18;
pub const RTNotEqualStrategyNumber: StrategyNumber = 19;
pub const RTLessStrategyNumber: StrategyNumber = 20;
pub const RTLessEqualStrategyNumber: StrategyNumber = 21;
pub const RTGreaterStrategyNumber: StrategyNumber = 22;
pub const RTGreaterEqualStrategyNumber: StrategyNumber = 23;
pub const RTSubStrategyNumber: StrategyNumber = 24;
pub const RTSubEqualStrategyNumber: StrategyNumber = 25;
pub const RTSuperStrategyNumber: StrategyNumber = 26;
pub const RTSuperEqualStrategyNumber: StrategyNumber = 27;
pub const RTPrefixStrategyNumber: StrategyNumber = 28;
pub const RTOldBelowStrategyNumber: StrategyNumber = 29;
pub const RTOldAboveStrategyNumber: StrategyNumber = 30;
pub const RTMaxStrategyNumber: StrategyNumber = 30;

pub const SK_ISNULL: i32 = 0x0001;
pub const SK_UNARY: i32 = 0x0002;
pub const SK_ROW_HEADER: i32 = 0x0004;
pub const SK_ROW_MEMBER: i32 = 0x0008;
pub const SK_ROW_END: i32 = 0x0010;
pub const SK_SEARCHARRAY: i32 = 0x0020;
pub const SK_SEARCHNULL: i32 = 0x0040;
pub const SK_SEARCHNOTNULL: i32 = 0x0080;
pub const SK_ORDER_BY: i32 = 0x0100;

// nbtree-private sk_flags bits (access/nbtree.h).
pub const SK_BT_REQFWD: i32 = 0x00010000;
pub const SK_BT_REQBKWD: i32 = 0x00020000;
pub const SK_BT_SKIP: i32 = 0x00040000;
pub const SK_BT_MINVAL: i32 = 0x00080000;
pub const SK_BT_MAXVAL: i32 = 0x00100000;
pub const SK_BT_NEXT: i32 = 0x00200000;
pub const SK_BT_PRIOR: i32 = 0x00400000;
pub const SK_BT_INDOPTION_SHIFT: i32 = 24;
pub const SK_BT_DESC: i32 = 0x0001 << SK_BT_INDOPTION_SHIFT;
pub const SK_BT_NULLS_FIRST: i32 = 0x0002 << SK_BT_INDOPTION_SHIFT;

// C-shaped: for SK_ROW_HEADER keys sk_argument is the pointer word of the
// subsidiary ScanKeyData array (owned by the index scan state's RowSubkeys
// buffer), SK_ROW_END-terminated (skey.h); copies share it verbatim, as C's
// struct assignment does, and nbtree preprocessing scribbles flags/strategy
// on it through the shared pointer.
#[derive(Clone)]
pub struct ScanKeyData {
    pub sk_flags: i32,
    pub sk_attno: AttrNumber,
    pub sk_strategy: StrategyNumber,
    pub sk_subtype: Oid,
    pub sk_collation: Oid,
    pub sk_func: FmgrInfo,
    pub sk_argument: Datum,
}

// C sizeof(ScanKeyData) is 72 on LP64; rule-9 cap 128.
const _: () = assert!(core::mem::size_of::<ScanKeyData>() <= 128);

impl ScanKeyData {
    pub fn empty() -> Self {
        Self {
            sk_flags: 0,
            sk_attno: 0,
            sk_strategy: InvalidStrategy,
            sk_subtype: 0,
            sk_collation: 0,
            sk_func: FmgrInfo::unresolved(),
            sk_argument: Datum::null(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn skey_flag_values_match_c() {
        assert_eq!(SK_ISNULL, 0x0001);
        assert_eq!(SK_UNARY, 0x0002);
        assert_eq!(SK_ROW_HEADER, 0x0004);
        assert_eq!(SK_ROW_MEMBER, 0x0008);
        assert_eq!(SK_ROW_END, 0x0010);
        assert_eq!(SK_SEARCHARRAY, 0x0020);
        assert_eq!(SK_SEARCHNULL, 0x0040);
        assert_eq!(SK_SEARCHNOTNULL, 0x0080);
        assert_eq!(SK_ORDER_BY, 0x0100);
        assert_eq!(SK_BT_DESC, 0x0100_0000);
        assert_eq!(SK_BT_NULLS_FIRST, 0x0200_0000);
    }

    #[test]
    fn clone_copies_argument_word_verbatim() {
        let mut key = ScanKeyData::empty();
        key.sk_flags = SK_SEARCHNOTNULL;
        key.sk_attno = 2;
        key.sk_strategy = BTEqualStrategyNumber;
        key.sk_argument = Datum::from_usize(0x1a);
        let copy = key.clone();
        assert_eq!(copy.sk_flags, SK_SEARCHNOTNULL);
        assert_eq!(copy.sk_attno, 2);
        assert_eq!(copy.sk_strategy, BTEqualStrategyNumber);
        assert_eq!(copy.sk_argument.as_usize(), 0x1a);
    }

    #[test]
    fn empty_key_is_invalid_strategy_and_null() {
        let key = ScanKeyData::empty();
        assert_eq!(key.sk_strategy, InvalidStrategy);
        assert_eq!(key.sk_argument, Datum::null());
        assert_eq!(key.sk_flags, 0);
    }
}
