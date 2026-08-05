use core::ffi::c_int;

pub type RawParseMode = c_int;

pub const RAW_PARSE_DEFAULT: RawParseMode = 0;
pub const RAW_PARSE_TYPE_NAME: RawParseMode = 1;
pub const RAW_PARSE_PLPGSQL_EXPR: RawParseMode = 2;
pub const RAW_PARSE_PLPGSQL_ASSIGN1: RawParseMode = 3;
pub const RAW_PARSE_PLPGSQL_ASSIGN2: RawParseMode = 4;
pub const RAW_PARSE_PLPGSQL_ASSIGN3: RawParseMode = 5;
