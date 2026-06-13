//! Timeline-history vocabulary (`access/timeline.h`, `access/xlog_internal.h`).

use crate::primitive::{TimeLineID, XLogRecPtr};

/// `XLOGDIR` (`access/xlog_internal.h`) — the WAL directory name.
pub const XLOGDIR: &str = "pg_wal";

/// `TimeLineHistoryEntry` (`access/timeline.h`) — one piece of WAL belonging to
/// the timeline history. All WAL locations between `begin` (inclusive) and `end`
/// (exclusive; `InvalidXLogRecPtr` means infinity) belong to the timeline `tli`.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct TimeLineHistoryEntry {
    pub tli: TimeLineID,
    /// inclusive
    pub begin: XLogRecPtr,
    /// exclusive, `InvalidXLogRecPtr` means infinity
    pub end: XLogRecPtr,
}
