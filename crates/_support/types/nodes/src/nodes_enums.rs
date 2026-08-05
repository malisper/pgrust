// Values verified against vendor/nodes.h (test: enum_values_match_c_headers).
#![allow(non_camel_case_types)]

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
#[repr(u32)]
pub enum CmdType {
    #[default]
    CMD_UNKNOWN = 0,
    CMD_SELECT = 1,
    CMD_UPDATE = 2,
    CMD_INSERT = 3,
    CMD_DELETE = 4,
    CMD_MERGE = 5,
    CMD_UTILITY = 6,
    CMD_NOTHING = 7,
}

mcx::forget_safe_nodrop!(CmdType);

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
#[repr(u32)]
pub enum LimitOption {
    #[default]
    LIMIT_OPTION_COUNT = 0,
    LIMIT_OPTION_WITH_TIES = 1,
}

// Ordering carries applyLockingClause's Max() precedence (lockoptions.h).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(u32)]
pub enum LockClauseStrength {
    #[default]
    LCS_NONE = 0,
    LCS_FORKEYSHARE = 1,
    LCS_FORSHARE = 2,
    LCS_FORNOKEYUPDATE = 3,
    LCS_FORUPDATE = 4,
}

// Ordering carries applyLockingClause's Max() precedence (lockoptions.h).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(u32)]
pub enum LockWaitPolicy {
    #[default]
    LockWaitBlock = 0,
    LockWaitSkip = 1,
    LockWaitError = 2,
}
