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
//!   `wal::rmgrdesc` parse it with bounds-checked `from_bytes`. A
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

/// Install all seam slots owned by this crate.
pub fn init_seams() {
    use rmgrdesc_small_seams as seams;
    seams::array_desc::set(rmgrdesc_utils::array_desc_seam);
    seams::offset_elem_desc::set(rmgrdesc_utils::offset_elem_desc_seam);
    seams::redirect_elem_desc::set(rmgrdesc_utils::redirect_elem_desc_seam);
    seams::oid_elem_desc::set(rmgrdesc_utils::oid_elem_desc_seam);

    clogdesc_seams::clog_desc::set(clogdesc::clog_desc_seam);
    clogdesc_seams::clog_identify::set(clogdesc::clog_identify);
    committsdesc_seams::commit_ts_desc::set(committsdesc::commit_ts_desc_seam);
    committsdesc_seams::commit_ts_identify::set(committsdesc::commit_ts_identify);
    dbasedesc_seams::dbase_desc::set(dbasedesc::dbase_desc_seam);
    dbasedesc_seams::dbase_identify::set(dbasedesc::dbase_identify);
    genericdesc_seams::generic_desc::set(genericdesc::generic_desc_seam);
    genericdesc_seams::generic_identify::set(genericdesc::generic_identify);
    logicalmsgdesc_seams::logicalmsg_desc::set(logicalmsgdesc::logicalmsg_desc_seam);
    logicalmsgdesc_seams::logicalmsg_identify::set(logicalmsgdesc::logicalmsg_identify);
    relmapdesc_seams::relmap_desc::set(relmapdesc::relmap_desc_seam);
    relmapdesc_seams::relmap_identify::set(relmapdesc::relmap_identify);
    seqdesc_seams::seq_desc::set(seqdesc::seq_desc_seam);
    seqdesc_seams::seq_identify::set(seqdesc::seq_identify);
    tblspcdesc_seams::tblspc_desc::set(tblspcdesc::tblspc_desc_seam);
    tblspcdesc_seams::tblspc_identify::set(tblspcdesc::tblspc_identify);
}

#[cfg(test)]
mod tests;
