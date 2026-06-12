//! Port of the small `src/backend/access/rmgrdesc/` descriptor units:
//! `clogdesc.c`, `committsdesc.c`, `dbasedesc.c`, `genericdesc.c`,
//! `logicalmsgdesc.c`, `relmapdesc.c`, `rmgrdesc_utils.c`, `seqdesc.c`,
//! `tblspcdesc.c`.
//!
//! Each `*_desc` appends a human-readable rendering of one WAL record to the
//! caller's `StringInfo`; each `*_identify` names the record subtype. The C
//! signature `void f(StringInfo buf, XLogReaderState *record)` becomes
//! `fn f(buf: &mut PgString<'_>, record: &DecodedXLogRecord<'_>) -> PgResult<()>`:
//!
//! - `buf` is the caller's context-allocated string (the `StringInfo` living
//!   in `CurrentMemoryContext`); appends are fallible because C's
//!   `appendStringInfo` can `ereport(ERROR)` on OOM.
//! - `record` is `types-wal`'s `DecodedXLogRecord` view: `record.info()` is
//!   `XLogRecGetInfo(record)` (masked inside each function exactly where the
//!   C masks it) and `record.main_data()` is `XLogRecGetData(record)` (with
//!   `XLogRecGetDataLen` == `.len()`).
//! - Where the C casts the payload to an `xl_*` struct, the typed structs in
//!   `types_wal::rmgrdesc` parse it with bounds-checked `from_bytes`. A
//!   payload too short for its record is impossible for well-formed WAL (C
//!   would read garbage); here it raises `ERRCODE_DATA_CORRUPTED`.
//!
//! `*_identify` returns `Option<&'static str>` (C `const char *` / NULL).

#![allow(non_upper_case_globals)]

mod util;

pub mod clogdesc;
pub mod committsdesc;
pub mod dbasedesc;
pub mod genericdesc;
pub mod logicalmsgdesc;
pub mod relmapdesc;
pub mod rmgrdesc_utils;
pub mod seqdesc;
pub mod tblspcdesc;

pub use clogdesc::{clog_desc, clog_identify, CLOG_TRUNCATE, CLOG_ZEROPAGE};
pub use committsdesc::{
    commit_ts_desc, commit_ts_identify, COMMIT_TS_TRUNCATE, COMMIT_TS_ZEROPAGE,
};
pub use dbasedesc::{
    dbase_desc, dbase_identify, XLOG_DBASE_CREATE_FILE_COPY, XLOG_DBASE_CREATE_WAL_LOG,
    XLOG_DBASE_DROP,
};
pub use genericdesc::{generic_desc, generic_identify};
pub use logicalmsgdesc::{logicalmsg_desc, logicalmsg_identify, XLOG_LOGICAL_MESSAGE};
pub use relmapdesc::{relmap_desc, relmap_identify, XLOG_RELMAP_UPDATE};
pub use rmgrdesc_utils::{array_desc, offset_elem_desc, oid_elem_desc, redirect_elem_desc};
pub use seqdesc::{seq_desc, seq_identify, XLOG_SEQ_LOG};
pub use tblspcdesc::{tblspc_desc, tblspc_identify, XLOG_TBLSPC_CREATE, XLOG_TBLSPC_DROP};

/// No seams to install: every function here is a leaf consumers can depend on
/// directly.
pub fn init_seams() {}

#[cfg(test)]
mod tests;
