//! VACUUM/ANALYZE command option vocabulary (`commands/vacuum.h`).
//!
//! The owned-tree definitions of `VacuumParams` and `VacOptValue`, plus the
//! `VACOPT_*` option flag bits, consumed by autovacuum's per-table scheduling
//! before the `backend-commands-vacuum` driver itself is ported.

#![no_std]
#![allow(non_camel_case_types)]
#![allow(non_upper_case_globals)]

use types_core::{bits32, Oid};

/// `BufferAccessStrategy` (`storage/bufmgr.h`) — the vacuum buffer-replacement
/// ring object created once per command and threaded down to the table-AM
/// vacuum. A "null" strategy (full use of shared buffers) is
/// [`BufferStrategyHandle::none`]. The interior id is resolved to the real
/// strategy object by the installed runtime; this crate never inspects it.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub struct BufferStrategyHandle {
    /// Backend-local id, or 0 for the "no strategy" (NULL) case.
    pub id: u64,
}

impl BufferStrategyHandle {
    /// The NULL strategy: full use of shared buffers.
    pub fn none() -> Self {
        Self { id: 0 }
    }
    /// Was a real strategy object created? (`bstrategy != NULL`.)
    pub fn is_some(self) -> bool {
        self.id != 0
    }
}

/// `VacOptValue` (`commands/vacuum.h`) — a tri-state vacuum option.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[repr(i32)]
pub enum VacOptValue {
    #[default]
    VACOPTVALUE_UNSPECIFIED = 0,
    VACOPTVALUE_AUTO = 1,
    VACOPTVALUE_DISABLED = 2,
    VACOPTVALUE_ENABLED = 3,
}

/// `VacuumParams` (`commands/vacuum.h`) — parameters customizing a single
/// VACUUM/ANALYZE invocation. Field order mirrors the C struct.
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct VacuumParams {
    /// `bits32 options` — bitmask of `VACOPT_*`.
    pub options: bits32,
    /// `int freeze_min_age` — min freeze age, -1 to use default.
    pub freeze_min_age: i32,
    /// `int freeze_table_age` — age at which to scan whole table.
    pub freeze_table_age: i32,
    /// `int multixact_freeze_min_age` — min multixact freeze age, -1 default.
    pub multixact_freeze_min_age: i32,
    /// `int multixact_freeze_table_age` — multixact age to scan whole table.
    pub multixact_freeze_table_age: i32,
    /// `bool is_wraparound` — force a for-wraparound vacuum.
    pub is_wraparound: bool,
    /// `int log_min_duration` — min execution threshold (ms) for logging.
    pub log_min_duration: i32,
    /// `VacOptValue index_cleanup` — do index vacuum and cleanup.
    pub index_cleanup: VacOptValue,
    /// `VacOptValue truncate` — truncate empty pages at the end.
    pub truncate: VacOptValue,
    /// `Oid toast_parent` — for privilege checks when recursing.
    pub toast_parent: Oid,
    /// `double max_eager_freeze_failure_rate` — eager-scan fail fraction (0 off).
    pub max_eager_freeze_failure_rate: f64,
    /// `int nworkers` — number of parallel vacuum workers (0 auto, -1 disabled).
    pub nworkers: i32,
}

/* flag bits for VacuumParams->options (commands/vacuum.h) */
pub const VACOPT_VACUUM: bits32 = 0x01;
pub const VACOPT_ANALYZE: bits32 = 0x02;
pub const VACOPT_VERBOSE: bits32 = 0x04;
pub const VACOPT_FREEZE: bits32 = 0x08;
pub const VACOPT_FULL: bits32 = 0x10;
pub const VACOPT_SKIP_LOCKED: bits32 = 0x20;
pub const VACOPT_PROCESS_MAIN: bits32 = 0x40;
pub const VACOPT_PROCESS_TOAST: bits32 = 0x80;
pub const VACOPT_DISABLE_PAGE_SKIPPING: bits32 = 0x100;
pub const VACOPT_SKIP_DATABASE_STATS: bits32 = 0x200;
pub const VACOPT_ONLY_DATABASE_STATS: bits32 = 0x400;
