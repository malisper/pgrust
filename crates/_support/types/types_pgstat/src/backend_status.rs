//! The shared-memory backend status entry (`utils/backend_status.h`),
//! trimmed to the fields current ports consume (`backend_progress.c`'s
//! `st_progress_*` writes and the `st_changecount` write-activity protocol).

use core::sync::atomic::AtomicU32;

use ::types_core::{int64, Oid};

use crate::backend_progress::{ProgressCommandType, PGSTAT_NUM_PROGRESS_PARAM};

/// `PgBackendStatus` (`utils/backend_status.h`), trimmed. Field order matches
/// the C struct's relative order.
///
/// `st_changecount` is `int` in C, manipulated only through the
/// `PGSTAT_BEGIN_WRITE_ACTIVITY` / `PGSTAT_END_WRITE_ACTIVITY` /
/// `pgstat_begin_read_activity` / `pgstat_end_read_activity` barrier macros;
/// it is a real atomic here because concurrent readers race the writer by
/// design.
#[derive(Debug)]
pub struct PgBackendStatus {
    /// `int st_changecount` — protects all the non-changecount fields.
    pub st_changecount: AtomicU32,
    /// `ProgressCommandType st_progress_command`.
    pub st_progress_command: ProgressCommandType,
    /// `Oid st_progress_command_target`.
    pub st_progress_command_target: Oid,
    /// `int64 st_progress_param[PGSTAT_NUM_PROGRESS_PARAM]`.
    pub st_progress_param: [int64; PGSTAT_NUM_PROGRESS_PARAM],
}

impl Default for PgBackendStatus {
    fn default() -> Self {
        PgBackendStatus {
            st_changecount: AtomicU32::new(0),
            st_progress_command: ProgressCommandType::Invalid,
            st_progress_command_target: 0,
            st_progress_param: [0; PGSTAT_NUM_PROGRESS_PARAM],
        }
    }
}
