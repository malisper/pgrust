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

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
#[repr(u32)]
pub enum LimitOption {
    #[default]
    LIMIT_OPTION_COUNT = 0,
    LIMIT_OPTION_WITH_TIES = 1,
}
