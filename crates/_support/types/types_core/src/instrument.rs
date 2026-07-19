pub const NS_PER_S: i64 = 1_000_000_000;

pub const NS_PER_MS: i64 = 1_000_000;

// A monotonic-clock reading or interval, in nanosecond ticks.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct instr_time {
    pub ticks: i64,
}

impl instr_time {
    pub fn set_zero(&mut self) {
        self.ticks = 0;
    }

    pub fn is_zero(self) -> bool {
        self.ticks == 0
    }

    pub fn add(&mut self, y: instr_time) {
        self.ticks += y.ticks;
    }

    pub fn subtract(&mut self, y: instr_time) {
        self.ticks -= y.ticks;
    }

    pub fn accum_diff(&mut self, y: instr_time, z: instr_time) {
        self.ticks += y.ticks - z.ticks;
    }

    pub fn get_double(self) -> f64 {
        self.ticks as f64 / NS_PER_S as f64
    }

    pub fn get_millisec(self) -> f64 {
        self.ticks as f64 / NS_PER_MS as f64
    }

    pub fn get_microsec(self) -> u64 {
        (self.ticks / NS_PER_US) as u64
    }
}

pub const NS_PER_US: i64 = 1_000;

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct BufferUsage {
    pub shared_blks_hit: i64,
    pub shared_blks_read: i64,
    pub shared_blks_dirtied: i64,
    pub shared_blks_written: i64,
    pub local_blks_hit: i64,
    pub local_blks_read: i64,
    pub local_blks_dirtied: i64,
    pub local_blks_written: i64,
    pub temp_blks_read: i64,
    pub temp_blks_written: i64,
    pub shared_blk_read_time: instr_time,
    pub shared_blk_write_time: instr_time,
    pub local_blk_read_time: instr_time,
    pub local_blk_write_time: instr_time,
    pub temp_blk_read_time: instr_time,
    pub temp_blk_write_time: instr_time,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct SerializeMetrics {
    pub timeSpent: instr_time,
    pub bytesSent: u64,
    pub bufferUsage: BufferUsage,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct WalUsage {
    pub wal_records: i64,
    pub wal_fpi: i64,
    /// `uint64` in C; arithmetic on it is unsigned (modular).
    pub wal_bytes: u64,
    pub wal_buffers_full: i64,
}

pub type InstrumentOption = i32;

pub const INSTRUMENT_TIMER: InstrumentOption = 1 << 0;
pub const INSTRUMENT_BUFFERS: InstrumentOption = 1 << 1;
pub const INSTRUMENT_ROWS: InstrumentOption = 1 << 2;
pub const INSTRUMENT_WAL: InstrumentOption = 1 << 3;
pub const INSTRUMENT_ALL: InstrumentOption = i32::MAX;

#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct Instrumentation {
    pub need_timer: bool,
    pub need_bufusage: bool,
    pub need_walusage: bool,
    pub async_mode: bool,
    pub running: bool,
    pub starttime: instr_time,
    pub counter: instr_time,
    pub firsttuple: f64,
    pub tuplecount: f64,
    pub bufusage_start: BufferUsage,
    pub walusage_start: WalUsage,
    pub startup: f64,
    pub total: f64,
    pub ntuples: f64,
    pub ntuples2: f64,
    pub nloops: f64,
    pub nfiltered1: f64,
    pub nfiltered2: f64,
    pub bufusage: BufferUsage,
    pub walusage: WalUsage,
}

/// EA-on-morsels pipeline report (pgrust-fast docs/design/ea-morsels.md §4;
/// no C counterpart). One record per engaged runtime pipeline phase,
/// constructed by the arm's leader merge on a clean Completed outcome and
/// read by EXPLAIN through the query_desc_runtime_ea_pipeline seam. All
/// counters are EXACT merges of per-worker instrument partials; timing
/// fields are zero under TIMING OFF and MUST NOT be printed as times then
/// (never fake a time).
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct RuntimeEaPipeline {
    /// The engaged pipeline's breaker (root) node — the block prints here.
    pub root_node_id: i32,
    /// Bypassed member nodes marked in EXPLAIN (-1 = unused slot). The m2
    /// arms have one (SeqScan) or two (SeqScan + skipped Sort).
    pub member_node_id: i32,
    pub member2_node_id: i32,
    /// Arm vocabulary: "scan" | "agg" | "distinct" (extensible).
    pub arm: &'static str,
    /// Phase role: "accept" ("combine"/"probe"/"fill" reserved for later
    /// arms and the m3 extension points).
    pub role: &'static str,
    pub taskset_index: u32,
    pub taskset_count: u32,
    /// Workers that executed at least one claim.
    pub workers: u32,
    pub claims: u64,
    /// Claim epoch segments (= claims until the dop1-tax coalescing split
    /// is threaded through).
    pub epoch_segments: u64,
    pub granules: u64,
    pub rows_scanned: u64,
    pub rows_survived: u64,
    /// Exported partials merged by the leader (scan: non-empty result
    /// partials; sinks: participating workers).
    pub partials: u64,
    /// Scan-descriptor fold: [granules_scanned, granules_pruned,
    /// granules_bloom_pruned, granules_meta, granules_bound_skipped,
    /// blocks_pruned, windows_staged].
    pub prune: [u64; 7],
    /// TIMING ON only (inc-3); zero under TIMING OFF.
    pub busy_ns: u64,
    pub first_start_ns: u64,
    pub last_end_ns: u64,
    /// min over workers' last_end (finish spread = last_end - this).
    pub min_last_end_ns: u64,
    /// min/max per-claim wall ns.
    pub morsel_min_ns: u64,
    pub morsel_max_ns: u64,
}

/// EXPLAIN (ENGINE) wire vocabulary (pgrust-fast single-executor Phase 0.2;
/// no C counterpart): which engine owned a plan node's execution. Mirror of
/// `executils::EngineKind` — the seam crate cannot depend on executils, so
/// this repr(u8) twin rides the seam (`query_desc_engine_events`), the same
/// home as `RuntimeEaPipeline`.
#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EngineKindWire {
    /// Serial lane-v2 push pipeline owns the node (production verdict).
    Lane = 0,
    /// Volcano row spine runs it (detail carries the refusal reason; "" if
    /// the node was never offered to a lane).
    Spine = 1,
    /// Spine refusal whose reason is admission-economics-fused-drive — the
    /// legacy fused batch arm owns the shape (displayed "spine/fused-arm").
    FusedArm = 2,
    /// Morsel-runtime arm engaged (pipeline identity via RuntimeEaPipeline).
    Runtime = 3,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum TuplesortMethod {
    #[default]
    StillInProgress,
    TopNHeapsort,
    Quicksort,
    ExternalSort,
    ExternalMerge,
}

impl TuplesortMethod {
    /// C TuplesortMethod bit value (tuplesort.h); StillInProgress is 0.
    pub fn bit(self) -> u32 {
        match self {
            TuplesortMethod::StillInProgress => 0,
            TuplesortMethod::TopNHeapsort => 1 << 0,
            TuplesortMethod::Quicksort => 1 << 1,
            TuplesortMethod::ExternalSort => 1 << 2,
            TuplesortMethod::ExternalMerge => 1 << 3,
        }
    }

    pub fn name(self) -> &'static str {
        match self {
            TuplesortMethod::StillInProgress => "still in progress",
            TuplesortMethod::TopNHeapsort => "top-N heapsort",
            TuplesortMethod::Quicksort => "quicksort",
            TuplesortMethod::ExternalSort => "external sort",
            TuplesortMethod::ExternalMerge => "external merge",
        }
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum TuplesortSpaceType {
    Disk,
    #[default]
    Memory,
}

impl TuplesortSpaceType {
    pub fn name(self) -> &'static str {
        match self {
            TuplesortSpaceType::Disk => "Disk",
            TuplesortSpaceType::Memory => "Memory",
        }
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct TuplesortInstrumentation {
    pub sortMethod: TuplesortMethod,
    pub spaceType: TuplesortSpaceType,
    pub spaceUsed: i64,
}

/// C IncrementalSortGroupInfo (execnodes.h); sortMethods is the C bitmask of
/// TuplesortMethod bits.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct IncrementalSortGroupInfo {
    pub groupCount: i64,
    pub maxDiskSpaceUsed: i64,
    pub totalDiskSpaceUsed: i64,
    pub maxMemorySpaceUsed: i64,
    pub totalMemorySpaceUsed: i64,
    pub sortMethods: u32,
}

impl IncrementalSortGroupInfo {
    /// C instrumentSortedGroup (nodeIncrementalSort.c).
    pub fn record(&mut self, stats: &TuplesortInstrumentation) {
        self.groupCount += 1;
        match stats.spaceType {
            TuplesortSpaceType::Disk => {
                self.totalDiskSpaceUsed += stats.spaceUsed;
                self.maxDiskSpaceUsed = self.maxDiskSpaceUsed.max(stats.spaceUsed);
            }
            TuplesortSpaceType::Memory => {
                self.totalMemorySpaceUsed += stats.spaceUsed;
                self.maxMemorySpaceUsed = self.maxMemorySpaceUsed.max(stats.spaceUsed);
            }
        }
        self.sortMethods |= stats.sortMethod.bit();
    }
}

/// C IncrementalSortInfo (execnodes.h).
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct IncrementalSortInfo {
    pub fullsortGroupInfo: IncrementalSortGroupInfo,
    pub prefixsortGroupInfo: IncrementalSortGroupInfo,
}

// C AggregateInstrumentation (nodeAgg.h) + hash_planned_partitions (an
// AggState field in C; rides this carrier so EXPLAIN reads one struct).
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct AggregateInstrumentation {
    pub hash_mem_peak: u64,
    pub hash_disk_used: u64,
    pub hash_batches_used: i32,
    pub hash_planned_partitions: i32,
}

// tuplestore_get_stats output (tuplestore.c); space_type reuses the
// Memory/Disk vocabulary tuplesort displays share.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct TuplestoreInstrumentation {
    pub space_type: TuplesortSpaceType,
    pub max_space: i64,
}

// C MemoizeInstrumentation (execnodes.h).
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct MemoizeInstrumentation {
    pub cache_hits: u64,
    pub cache_misses: u64,
    pub cache_evictions: u64,
    pub cache_overflows: u64,
    pub mem_peak: u64,
}

// C BitmapHeapScanInstrumentation (execnodes.h).
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct BitmapHeapScanInstrumentation {
    pub exact_pages: u64,
    pub lossy_pages: u64,
}

// C HashInstrumentation (execnodes.h).
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct HashInstrumentation {
    pub nbuckets: i32,
    pub nbuckets_original: i32,
    pub nbatch: i32,
    pub nbatch_original: i32,
    pub space_peak: u64,
}

impl HashInstrumentation {
    /// `ExecHashAccumInstrumentation`: per-field maxima across instances.
    pub fn accum(&mut self, other: &HashInstrumentation) {
        self.nbuckets = self.nbuckets.max(other.nbuckets);
        self.nbuckets_original = self.nbuckets_original.max(other.nbuckets_original);
        self.nbatch = self.nbatch.max(other.nbatch);
        self.nbatch_original = self.nbatch_original.max(other.nbatch_original);
        self.space_peak = self.space_peak.max(other.space_peak);
    }
}
