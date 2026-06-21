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

/// `bytea *rd_options` — the relcache's opaque parsed-reloptions slot.
///
/// In C `rd_options` is a single `bytea *` that each access method casts to its
/// own option struct (`StdRdOptions` for heaps/toast/matviews/partitioned
/// tables, `ViewOptions` for views, an AM-private struct such as `BrinOptions`
/// for indexes). The owned model carries the parsed `StdRdOptions`/`ViewOptions`
/// by value where it has a typed stand-in, and keeps the raw serialized varlena
/// bytes for an AM-defined index-option blob the relcache cannot interpret
/// generically — exactly the AMs (BRIN, GiST, …) whose `amoptions` produces a
/// custom struct. The owning AM reinterprets [`RdOptions::Bytea`] on demand
/// (e.g. `BrinGetPagesPerRange`).
#[derive(Clone, Debug, PartialEq)]
pub enum RdOptions {
    /// Parsed `StdRdOptions` (heap / toast / matview / partitioned table).
    Std(StdRdOptions),
    /// Parsed `ViewOptions` (views / materialized-view view side).
    View(ViewOptions),
    /// Opaque AM-defined option struct, kept as its serialized varlena bytes
    /// (the C `bytea`). Includes the 4-byte `vl_len_` varlena header, so byte
    /// offsets match the C struct's `offsetof` (e.g. `BrinOptions.pagesPerRange`
    /// at offset 4).
    Bytea(alloc::vec::Vec<u8>),
}

impl RdOptions {
    /// Return the parsed `StdRdOptions` if this slot holds one, else `None`.
    /// Used by `RelationGetFillFactor`/`RelationGetToastTupleTarget` and the
    /// autovacuum/parallel-workers readers, which only ever look at a table's
    /// `StdRdOptions` slot (an index/view never reaches those code paths).
    pub fn std(&self) -> Option<&StdRdOptions> {
        match self {
            RdOptions::Std(s) => Some(s),
            _ => None,
        }
    }

    /// Return the parsed `ViewOptions` if this slot holds one, else `None`.
    /// Used by the `RelationHasSecurityInvoker`/`RelationIsSecurityView`/
    /// `RelationHasCheckOption` view-option predicates (`utils/rel.h`).
    pub fn view(&self) -> Option<&ViewOptions> {
        match self {
            RdOptions::View(v) => Some(v),
            _ => None,
        }
    }

    /// Return the opaque AM-defined option bytes if this slot holds them.
    pub fn bytea(&self) -> Option<&[u8]> {
        match self {
            RdOptions::Bytea(b) => Some(b),
            _ => None,
        }
    }
}
