//! Bgwriter statistics types (`pgstat.h`, `utils/pgstat_internal.h`).

use crate::activity_pgstat::PgStat_Counter;
use ::types_core::primitive::TimestampTz;
use ::types_storage::LWLock;

/// `PgStat_BgWriterStats` (`pgstat.h`). Field order matches C exactly.
///
/// This struct contains only actual event counters (plus the reset timestamp),
/// because `pgstat_report_bgwriter` uses an all-zeros test to detect whether
/// there are any stats updates to apply.
#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub struct PgStat_BgWriterStats {
    pub buf_written_clean: PgStat_Counter,
    pub maxwritten_clean: PgStat_Counter,
    pub buf_alloc: PgStat_Counter,
    pub stat_reset_timestamp: TimestampTz,
}

impl PgStat_BgWriterStats {
    /// Equivalent of C's `pg_memory_is_all_zeros(&PendingBgWriterStats,
    /// sizeof(struct PgStat_BgWriterStats))` (`utils/memutils.h`): true iff
    /// every byte of the struct is zero. The struct is a plain record of four
    /// integer fields, so an all-zero byte image is exactly all fields == 0.
    pub fn is_all_zeros(&self) -> bool {
        *self == PgStat_BgWriterStats::default()
    }
}

/// `PgStatShared_BgWriter` (`utils/pgstat_internal.h`). Field order matches C.
/// See `PgStatShared_Archiver` (`activity_pgstat`) on `changecount`.
#[derive(Debug, Default)]
pub struct PgStatShared_BgWriter {
    /// lock protects `reset_offset` as well as `stats.stat_reset_timestamp`
    pub lock: LWLock,
    pub changecount: core::sync::atomic::AtomicU32,
    pub stats: PgStat_BgWriterStats,
    pub reset_offset: PgStat_BgWriterStats,
}
