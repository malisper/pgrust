pub const HEAP_DEFAULT_FILLFACTOR: i32 = 100;
pub const HEAP_MIN_FILLFACTOR: i32 = 10;

#[derive(Clone, Copy, Debug, PartialEq)]
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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(i32)]
pub enum StdRdOptIndexCleanup {
    STDRD_OPTION_VACUUM_INDEX_CLEANUP_AUTO = 0,
    STDRD_OPTION_VACUUM_INDEX_CLEANUP_OFF = 1,
    STDRD_OPTION_VACUUM_INDEX_CLEANUP_ON = 2,
}

pub use StdRdOptIndexCleanup::*;

// StdRdOptions (utils/rel.h) minus the vl_len_ varlena header: the parse
// result is an owned struct, not a bytea image.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct StdRdOptions {
    pub fillfactor: i32,
    pub toast_tuple_target: i32,
    pub autovacuum: AutoVacOpts,
    pub user_catalog_table: bool,
    pub parallel_workers: i32,
    pub vacuum_index_cleanup: StdRdOptIndexCleanup,
    pub vacuum_truncate: bool,
    pub vacuum_truncate_set: bool,
    pub vacuum_max_eager_freeze_failure_rate: f64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(i32)]
pub enum ViewOptCheckOption {
    VIEW_OPTION_CHECK_OPTION_NOT_SET = 0,
    VIEW_OPTION_CHECK_OPTION_LOCAL = 1,
    VIEW_OPTION_CHECK_OPTION_CASCADED = 2,
}

pub use ViewOptCheckOption::*;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ViewOptions {
    pub security_barrier: bool,
    pub security_invoker: bool,
    pub check_option: ViewOptCheckOption,
}

// rd_options payload: the heap/view parses land with the reloptions unit; the
// AM-opaque index blobs are a later widening.
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum RdOptions {
    Std(StdRdOptions),
    View(ViewOptions),
}

impl RdOptions {
    #[inline]
    pub fn std(&self) -> Option<&StdRdOptions> {
        match self {
            RdOptions::Std(o) => Some(o),
            RdOptions::View(_) => None,
        }
    }

    #[inline]
    pub fn view(&self) -> Option<&ViewOptions> {
        match self {
            RdOptions::Std(_) => None,
            RdOptions::View(o) => Some(o),
        }
    }
}
