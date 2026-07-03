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
