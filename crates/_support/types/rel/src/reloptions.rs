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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(i32)]
pub enum GistOptBufferingMode {
    GIST_OPTION_BUFFERING_AUTO = 0,
    GIST_OPTION_BUFFERING_ON = 1,
    GIST_OPTION_BUFFERING_OFF = 2,
}

pub use GistOptBufferingMode::*;

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct BTOptions {
    pub fillfactor: i32,
    pub vacuum_cleanup_index_scale_factor: f64,
    pub deduplicate_items: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct HashOptions {
    pub fillfactor: i32,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct GinOptions {
    pub use_fast_update: bool,
    pub pending_list_cleanup_size: i32,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct GistOptions {
    pub fillfactor: i32,
    pub buffering_mode: GistOptBufferingMode,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SpGistOptions {
    pub fillfactor: i32,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct HnswOptions {
    pub m: i32,
    pub ef_construction: i32,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct BrinOptions {
    pub pages_per_range: i32,
    pub autosummarize: bool,
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum RdOptions {
    Std(StdRdOptions),
    View(ViewOptions),
    BTree(BTOptions),
    Hash(HashOptions),
    Gin(GinOptions),
    Gist(GistOptions),
    SpGist(SpGistOptions),
    Brin(BrinOptions),
    Hnsw(HnswOptions),
}

impl RdOptions {
    #[inline]
    pub fn std(&self) -> Option<&StdRdOptions> {
        match self {
            RdOptions::Std(o) => Some(o),
            _ => None,
        }
    }

    #[inline]
    pub fn view(&self) -> Option<&ViewOptions> {
        match self {
            RdOptions::View(o) => Some(o),
            _ => None,
        }
    }

    // RelationGetFillFactor is used by C against whichever option struct the
    // relkind/AM parsed; variants without a fillfactor member fall to default.
    #[inline]
    pub fn fillfactor(&self) -> Option<i32> {
        match self {
            RdOptions::Std(o) => Some(o.fillfactor),
            RdOptions::BTree(o) => Some(o.fillfactor),
            RdOptions::Hash(o) => Some(o.fillfactor),
            RdOptions::Gist(o) => Some(o.fillfactor),
            RdOptions::SpGist(o) => Some(o.fillfactor),
            RdOptions::View(_) | RdOptions::Gin(_) | RdOptions::Brin(_) | RdOptions::Hnsw(_) => None,
        }
    }

    #[inline]
    pub fn btree(&self) -> Option<&BTOptions> {
        match self {
            RdOptions::BTree(o) => Some(o),
            _ => None,
        }
    }

    #[inline]
    pub fn hash(&self) -> Option<&HashOptions> {
        match self {
            RdOptions::Hash(o) => Some(o),
            _ => None,
        }
    }

    #[inline]
    pub fn gin(&self) -> Option<&GinOptions> {
        match self {
            RdOptions::Gin(o) => Some(o),
            _ => None,
        }
    }

    #[inline]
    pub fn gist(&self) -> Option<&GistOptions> {
        match self {
            RdOptions::Gist(o) => Some(o),
            _ => None,
        }
    }

    #[inline]
    pub fn spgist(&self) -> Option<&SpGistOptions> {
        match self {
            RdOptions::SpGist(o) => Some(o),
            _ => None,
        }
    }

    #[inline]
    pub fn brin(&self) -> Option<&BrinOptions> {
        match self {
            RdOptions::Brin(o) => Some(o),
            _ => None,
        }
    }

    #[inline]
    pub fn hnsw(&self) -> Option<&HnswOptions> {
        match self {
            RdOptions::Hnsw(o) => Some(o),
            _ => None,
        }
    }
}
