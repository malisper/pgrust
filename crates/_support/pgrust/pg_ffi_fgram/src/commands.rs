use core::ffi::c_char;

use crate::{fmgr::Node, NodeTag, ParseLoc};

// Progress parameters for PROGRESS_COPY (src/include/commands/progress.h).
// Additive: needed by the copyto.c / copyfrom.c command ports.
/// `PROGRESS_COPY_BYTES_PROCESSED` — bytes processed so far.
pub const PROGRESS_COPY_BYTES_PROCESSED: i32 = 0;
/// `PROGRESS_COPY_BYTES_TOTAL`.
pub const PROGRESS_COPY_BYTES_TOTAL: i32 = 1;
/// `PROGRESS_COPY_TUPLES_PROCESSED`.
pub const PROGRESS_COPY_TUPLES_PROCESSED: i32 = 2;
/// `PROGRESS_COPY_TUPLES_EXCLUDED`.
pub const PROGRESS_COPY_TUPLES_EXCLUDED: i32 = 3;
/// `PROGRESS_COPY_COMMAND`.
pub const PROGRESS_COPY_COMMAND: i32 = 4;
/// `PROGRESS_COPY_TYPE`.
pub const PROGRESS_COPY_TYPE: i32 = 5;
/// `PROGRESS_COPY_TUPLES_SKIPPED`.
pub const PROGRESS_COPY_TUPLES_SKIPPED: i32 = 6;

// Commands of COPY (as advertised via PROGRESS_COPY_COMMAND).
/// `PROGRESS_COPY_COMMAND_FROM`.
pub const PROGRESS_COPY_COMMAND_FROM: i32 = 1;
/// `PROGRESS_COPY_COMMAND_TO`.
pub const PROGRESS_COPY_COMMAND_TO: i32 = 2;

// Types of COPY commands (as advertised via PROGRESS_COPY_TYPE).
/// `PROGRESS_COPY_TYPE_FILE`.
pub const PROGRESS_COPY_TYPE_FILE: i32 = 1;
/// `PROGRESS_COPY_TYPE_PROGRAM`.
pub const PROGRESS_COPY_TYPE_PROGRAM: i32 = 2;
/// `PROGRESS_COPY_TYPE_PIPE`.
pub const PROGRESS_COPY_TYPE_PIPE: i32 = 3;
/// `PROGRESS_COPY_TYPE_CALLBACK`.
pub const PROGRESS_COPY_TYPE_CALLBACK: i32 = 4;

/// `PROGRESS_COMMAND_COPY` — the command tag passed to
/// `pgstat_progress_start_command` (`src/include/utils/backend_progress.h`).
pub const PROGRESS_COMMAND_COPY: i32 = 6;

pub type DefElemAction = u32;

pub const DEFELEM_UNSPEC: DefElemAction = 0;
pub const DEFELEM_SET: DefElemAction = 1;
pub const DEFELEM_ADD: DefElemAction = 2;
pub const DEFELEM_DROP: DefElemAction = 3;

#[derive(Clone, Copy)]
#[repr(C)]
pub struct DefElem {
    pub type_: NodeTag,
    pub defnamespace: *mut c_char,
    pub defname: *mut c_char,
    pub arg: *mut Node,
    pub defaction: DefElemAction,
    pub location: ParseLoc,
}

/// `AlterTSDictionaryStmt` (nodes/parsenodes.h) — `ALTER TEXT SEARCH DICTIONARY`.
#[repr(C)]
pub struct AlterTSDictionaryStmt {
    pub type_: NodeTag,
    /// `List *dictname` — qualified name (list of `String`).
    pub dictname: *mut crate::List,
    /// `List *options` — list of `DefElem` nodes.
    pub options: *mut crate::List,
}

/// `AlterTSConfigType` (nodes/parsenodes.h) — the `kind` of an
/// `AlterTSConfigurationStmt`.
pub type AlterTSConfigType = u32;
/// `ALTER_TSCONFIG_ADD_MAPPING`.
pub const ALTER_TSCONFIG_ADD_MAPPING: AlterTSConfigType = 0;
/// `ALTER_TSCONFIG_ALTER_MAPPING_FOR_TOKEN`.
pub const ALTER_TSCONFIG_ALTER_MAPPING_FOR_TOKEN: AlterTSConfigType = 1;
/// `ALTER_TSCONFIG_REPLACE_DICT`.
pub const ALTER_TSCONFIG_REPLACE_DICT: AlterTSConfigType = 2;
/// `ALTER_TSCONFIG_REPLACE_DICT_FOR_TOKEN`.
pub const ALTER_TSCONFIG_REPLACE_DICT_FOR_TOKEN: AlterTSConfigType = 3;
/// `ALTER_TSCONFIG_DROP_MAPPING`.
pub const ALTER_TSCONFIG_DROP_MAPPING: AlterTSConfigType = 4;

/// `AlterTSConfigurationStmt` (nodes/parsenodes.h) — `ALTER TEXT SEARCH
/// CONFIGURATION … ADD/ALTER/DROP MAPPING` / `REPLACE`.
#[repr(C)]
pub struct AlterTSConfigurationStmt {
    pub type_: NodeTag,
    /// `AlterTSConfigType kind`.
    pub kind: AlterTSConfigType,
    /// `List *cfgname` — qualified name (list of `String`).
    pub cfgname: *mut crate::List,
    /// `List *tokentype` — list of `String`.
    pub tokentype: *mut crate::List,
    /// `List *dicts` — list of list of `String`.
    pub dicts: *mut crate::List,
    /// `bool override` — if true, remove old variant.
    pub r#override: bool,
    /// `bool replace` — if true, replace dictionary by another.
    pub replace: bool,
    /// `bool missing_ok` — for DROP, skip error if missing?
    pub missing_ok: bool,
}

#[cfg(test)]
mod tests {
    use super::*;
    use core::mem::{align_of, offset_of, size_of};

    #[test]
    fn alter_ts_dictionary_stmt_layout() {
        assert_eq!(align_of::<AlterTSDictionaryStmt>(), 8);
        assert_eq!(offset_of!(AlterTSDictionaryStmt, type_), 0);
        assert_eq!(offset_of!(AlterTSDictionaryStmt, dictname), 8);
        assert_eq!(offset_of!(AlterTSDictionaryStmt, options), 16);
    }

    #[test]
    fn alter_ts_configuration_stmt_layout() {
        assert_eq!(align_of::<AlterTSConfigurationStmt>(), 8);
        assert_eq!(offset_of!(AlterTSConfigurationStmt, type_), 0);
        assert_eq!(offset_of!(AlterTSConfigurationStmt, kind), 4);
        assert_eq!(offset_of!(AlterTSConfigurationStmt, cfgname), 8);
        assert_eq!(offset_of!(AlterTSConfigurationStmt, tokentype), 16);
        assert_eq!(offset_of!(AlterTSConfigurationStmt, dicts), 24);
        assert_eq!(offset_of!(AlterTSConfigurationStmt, r#override), 32);
        assert_eq!(offset_of!(AlterTSConfigurationStmt, replace), 33);
        assert_eq!(offset_of!(AlterTSConfigurationStmt, missing_ok), 34);
    }

    #[test]
    fn defelem_layout_matches_postgres() {
        assert_eq!(size_of::<DefElem>(), 40);
        assert_eq!(align_of::<DefElem>(), 8);
        assert_eq!(offset_of!(DefElem, type_), 0);
        assert_eq!(offset_of!(DefElem, defnamespace), 8);
        assert_eq!(offset_of!(DefElem, defname), 16);
        assert_eq!(offset_of!(DefElem, arg), 24);
        assert_eq!(offset_of!(DefElem, defaction), 32);
        assert_eq!(offset_of!(DefElem, location), 36);
    }
}
