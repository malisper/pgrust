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

/// contrib/bloom's parsed reloptions: types_bloom::BloomOptions (length already
/// converted bits -> words, C bloptions contract).
pub use types_bloom::BloomOptions;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct BrinOptions {
    pub pages_per_range: i32,
    pub autosummarize: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PgrcolumnarCodec {
    Auto,
    Lz4,
    Zstd,
    Plain,
}

pub const PGRCOLUMNAR_CLUSTER_KEY_MAX: usize = 512;
pub const PGRCOLUMNAR_CODEC_COLS_MAX: usize = 2048;

// pgrcolumnar AM storage options (CREATE TABLE ... USING cbstore WITH (...)).
// Strings live in fixed inline buffers so RdOptions stays Copy; lengths are
// validated at option-parse time.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct PgrcolumnarOptions {
    pub codec: PgrcolumnarCodec,
    pub zstd_level: i32,
    // Same contract as StdRdOptions.parallel_workers (-1 = unset): pins the
    // planner's worker count for scans of this relation, overriding the
    // row-group-based default sizing (C's compute_parallel_worker honors
    // the reloption first; pgrcolumnar does too).
    pub parallel_workers: i32,
    cluster_key_len: u16,
    cluster_key_buf: [u8; PGRCOLUMNAR_CLUSTER_KEY_MAX],
    codec_cols_len: u16,
    codec_cols_buf: [u8; PGRCOLUMNAR_CODEC_COLS_MAX],
}

impl Default for PgrcolumnarOptions {
    fn default() -> PgrcolumnarOptions {
        PgrcolumnarOptions {
            // Ingest default flipped Auto -> Lz4 (train #8): decode-hot
            // scans want LZ4's decompression speed; ZSTD stays opt-in via
            // WITH (codec = 'zstd'). New ingests only — existing chunks
            // carry their codec in the chunk header.
            codec: PgrcolumnarCodec::Lz4,
            zstd_level: 3,
            parallel_workers: -1,
            cluster_key_len: 0,
            cluster_key_buf: [0; PGRCOLUMNAR_CLUSTER_KEY_MAX],
            codec_cols_len: 0,
            codec_cols_buf: [0; PGRCOLUMNAR_CODEC_COLS_MAX],
        }
    }
}

impl PgrcolumnarOptions {
    /// false = value too long (caller reports the option error).
    pub fn set_cluster_key(&mut self, v: &str) -> bool {
        if v.len() > PGRCOLUMNAR_CLUSTER_KEY_MAX {
            return false;
        }
        self.cluster_key_buf[..v.len()].copy_from_slice(v.as_bytes());
        self.cluster_key_len = v.len() as u16;
        true
    }

    pub fn set_codec_cols(&mut self, v: &str) -> bool {
        if v.len() > PGRCOLUMNAR_CODEC_COLS_MAX {
            return false;
        }
        self.codec_cols_buf[..v.len()].copy_from_slice(v.as_bytes());
        self.codec_cols_len = v.len() as u16;
        true
    }

    pub fn cluster_key(&self) -> &str {
        // Buffers only ever hold set_* validated UTF-8.
        core::str::from_utf8(&self.cluster_key_buf[..self.cluster_key_len as usize]).unwrap()
    }

    pub fn codec_cols(&self) -> &str {
        core::str::from_utf8(&self.codec_cols_buf[..self.codec_cols_len as usize]).unwrap()
    }
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
    Pgrcolumnar(PgrcolumnarOptions),
    Hnsw(HnswOptions),
    Bloom(BloomOptions),
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

    #[inline]
    pub fn pgrcolumnar(&self) -> Option<&PgrcolumnarOptions> {
        match self {
            RdOptions::Pgrcolumnar(o) => Some(o),
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
            RdOptions::View(_) | RdOptions::Gin(_) | RdOptions::Brin(_)
            | RdOptions::Pgrcolumnar(_) | RdOptions::Hnsw(_) | RdOptions::Bloom(_) => None,
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
    pub fn bloom(&self) -> Option<BloomOptions> {
        match self {
            RdOptions::Bloom(o) => Some(*o),
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
