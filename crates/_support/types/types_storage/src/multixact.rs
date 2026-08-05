// DoesMultiXactIdConflict's bool return + *current_is_member out param.
#[derive(Clone, Copy, Debug)]
pub struct MultiXactConflict {
    pub conflict: bool,
    pub current_is_member: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, PartialOrd, Ord)]
#[repr(i32)]
pub enum MultiXactStatus {
    MultiXactStatusForKeyShare = 0,
    MultiXactStatusForShare,
    MultiXactStatusForNoKeyUpdate,
    MultiXactStatusForUpdate,
    MultiXactStatusNoKeyUpdate,
    MultiXactStatusUpdate,
}

pub const MaxMultiXactStatus: MultiXactStatus = MultiXactStatus::MultiXactStatusUpdate;

#[inline]
pub fn ISUPDATE_from_mxstatus(status: MultiXactStatus) -> bool {
    status > MultiXactStatus::MultiXactStatusForUpdate
}

#[derive(Clone, Copy, Debug)]
pub struct MultiXactMember {
    pub xid: ::types_core::TransactionId,
    pub status: MultiXactStatus,
}
