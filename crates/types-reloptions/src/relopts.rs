//! Parsed result-option structs and their enum vocabulary (`utils/rel.h`).
//!
//! These are the typed, owned-tree stand-ins for the `bytea` blobs that
//! `default_reloptions` / `view_reloptions` build. C begins each with a
//! varlena header (`int32 vl_len_`); the owned model passes them by value, so
//! the header is dropped. The fields below are filled by the reloptions parser
//! keyed by option name, the owned-tree equivalent of C's `offsetof`-based
//! parse table.

/// `StdRdOptIndexCleanup` (`utils/rel.h`) — `StdRdOptions->vacuum_index_cleanup`
/// values.
pub type StdRdOptIndexCleanup = i32;
pub const STDRD_OPTION_VACUUM_INDEX_CLEANUP_AUTO: StdRdOptIndexCleanup = 0;
pub const STDRD_OPTION_VACUUM_INDEX_CLEANUP_OFF: StdRdOptIndexCleanup = 1;
pub const STDRD_OPTION_VACUUM_INDEX_CLEANUP_ON: StdRdOptIndexCleanup = 2;

/// `ViewOptCheckOption` (`utils/rel.h`) — `ViewOptions->check_option` values.
pub type ViewOptCheckOption = i32;
pub const VIEW_OPTION_CHECK_OPTION_NOT_SET: ViewOptCheckOption = 0;
pub const VIEW_OPTION_CHECK_OPTION_LOCAL: ViewOptCheckOption = 1;
pub const VIEW_OPTION_CHECK_OPTION_CASCADED: ViewOptCheckOption = 2;

/// `AutoVacOpts` — autovacuum-related reloptions (`utils/rel.h`).
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct AutoVacOpts {
    pub enabled: bool,
    pub vacuum_threshold: i32,
    pub vacuum_max_threshold: i32,
    pub vacuum_ins_threshold: i32,
    pub analyze_threshold: i32,
    pub vacuum_cost_limit: i32,
    pub freeze_min_age: i32,
    pub freeze_max_age: i32,
    pub freeze_table_age: i32,
    pub multixact_freeze_min_age: i32,
    pub multixact_freeze_max_age: i32,
    pub multixact_freeze_table_age: i32,
    pub log_min_duration: i32,
    pub vacuum_cost_delay: f64,
    pub vacuum_scale_factor: f64,
    pub vacuum_ins_scale_factor: f64,
    pub analyze_scale_factor: f64,
}

/// `StdRdOptions` — standard contents of `rd_options` for heaps (`utils/rel.h`).
///
/// `RelationData::rd_options` carries this by value; `None` is the C NULL
/// `rd_options`. When present, the parse filled every field (defaults
/// included), as in C.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct StdRdOptions {
    pub fillfactor: i32,
    pub toast_tuple_target: i32,
    pub autovacuum: AutoVacOpts,
    pub user_catalog_table: bool,
    pub parallel_workers: i32,
    pub vacuum_index_cleanup: StdRdOptIndexCleanup,
    pub vacuum_truncate: bool,
    /// Whether `vacuum_truncate` was set (its parse-table `isset_offset`).
    pub vacuum_truncate_set: bool,
    /// Fraction of pages vacuum can eagerly scan and fail to freeze. 0 if
    /// disabled, -1 if unspecified.
    pub vacuum_max_eager_freeze_failure_rate: f64,
}

impl Default for StdRdOptions {
    fn default() -> Self {
        // C `allocateReloptStruct` palloc0's the struct, so every field starts
        // zero; `fillRelOptions` then writes the resolved value (the parsed
        // value if set, else the option's definition default) for every option
        // the kind admits. Zero is the correct unwritten baseline.
        StdRdOptions {
            fillfactor: 0,
            toast_tuple_target: 0,
            autovacuum: AutoVacOpts::default(),
            user_catalog_table: false,
            parallel_workers: 0,
            vacuum_index_cleanup: 0,
            vacuum_truncate: false,
            vacuum_truncate_set: false,
            vacuum_max_eager_freeze_failure_rate: 0.0,
        }
    }
}

/// `ViewOptions` — contents of `rd_options` for views (`utils/rel.h`).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ViewOptions {
    pub security_barrier: bool,
    pub security_invoker: bool,
    pub check_option: ViewOptCheckOption,
}
