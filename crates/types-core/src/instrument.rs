//! Instrumentation counter vocabulary (`executor/instrument.h`,
//! `portability/instr_time.h`), shared by the executor, EXPLAIN, and the
//! statistics views.

/// Nanoseconds per second; the tick unit of [`instr_time`].
pub const NS_PER_S: i64 = 1_000_000_000;

/// `NS_PER_MS` (`portability/instr_time.h`) — nanoseconds per millisecond.
pub const NS_PER_MS: i64 = 1_000_000;

/// `instr_time` — a monotonic-clock reading or interval, in nanosecond ticks.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct instr_time {
    pub ticks: i64,
}

/// The pure-arithmetic `INSTR_TIME_*` macros from `portability/instr_time.h`.
/// The clock read (`INSTR_TIME_SET_CURRENT[_LAZY]`) needs libc and lives in
/// the `portability-instr-time` crate; this crate stays dependency-free.
impl instr_time {
    /// `INSTR_TIME_SET_ZERO(t)`.
    pub fn set_zero(&mut self) {
        self.ticks = 0;
    }

    /// `INSTR_TIME_IS_ZERO(t)`.
    pub fn is_zero(self) -> bool {
        self.ticks == 0
    }

    /// `INSTR_TIME_ADD(x, y)` — `x += y`.
    pub fn add(&mut self, y: instr_time) {
        self.ticks += y.ticks;
    }

    /// `INSTR_TIME_SUBTRACT(x, y)` — `x -= y`.
    pub fn subtract(&mut self, y: instr_time) {
        self.ticks -= y.ticks;
    }

    /// `INSTR_TIME_ACCUM_DIFF(x, y, z)` — `x += (y - z)`.
    pub fn accum_diff(&mut self, y: instr_time, z: instr_time) {
        self.ticks += y.ticks - z.ticks;
    }

    /// `INSTR_TIME_GET_DOUBLE(t)` — ticks (nanoseconds) to seconds.
    pub fn get_double(self) -> f64 {
        self.ticks as f64 / NS_PER_S as f64
    }

    /// `INSTR_TIME_GET_MILLISEC(t)` — ticks (nanoseconds) to milliseconds.
    pub fn get_millisec(self) -> f64 {
        self.ticks as f64 / NS_PER_MS as f64
    }

    /// `INSTR_TIME_GET_MICROSEC(t)` — ticks (nanoseconds) to whole
    /// microseconds (`t.ticks / NS_PER_US`, integer division as in C's
    /// `uint64` macro).
    pub fn get_microsec(self) -> u64 {
        (self.ticks / NS_PER_US) as u64
    }
}

/// `NS_PER_US` (`portability/instr_time.h`) — nanoseconds per microsecond.
pub const NS_PER_US: i64 = 1_000;

/// `BufferUsage` (`executor/instrument.h`).
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

/// `SerializeMetrics` (`commands/explain.h`) — metrics collected by the
/// EXPLAIN (SERIALIZE) `DestReceiver`: the time spent serializing the result,
/// the total serialized output volume in bytes, and the buffer usage incurred.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct SerializeMetrics {
    /// `instr_time timeSpent` — time spent serializing.
    pub timeSpent: instr_time,
    /// `uint64 bytesSent` — number of bytes that would have been sent.
    pub bytesSent: u64,
    /// `BufferUsage bufferUsage` — buffers accessed during serialization.
    pub bufferUsage: BufferUsage,
}

/// `WalUsage` (`executor/instrument.h`).
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct WalUsage {
    pub wal_records: i64,
    pub wal_fpi: i64,
    /// `uint64` in C; arithmetic on it is unsigned (modular).
    pub wal_bytes: u64,
    pub wal_buffers_full: i64,
}

/// `InstrumentOption` flag bits (`executor/instrument.h`).
pub type InstrumentOption = i32;

pub const INSTRUMENT_TIMER: InstrumentOption = 1 << 0;
pub const INSTRUMENT_BUFFERS: InstrumentOption = 1 << 1;
pub const INSTRUMENT_ROWS: InstrumentOption = 1 << 2;
pub const INSTRUMENT_WAL: InstrumentOption = 1 << 3;
pub const INSTRUMENT_ALL: InstrumentOption = i32::MAX;

/// `Instrumentation` (`executor/instrument.h`).
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct Instrumentation {
    // Parameters set at node creation:
    pub need_timer: bool,
    pub need_bufusage: bool,
    pub need_walusage: bool,
    pub async_mode: bool,
    // Info about current plan cycle:
    pub running: bool,
    pub starttime: instr_time,
    pub counter: instr_time,
    pub firsttuple: f64,
    pub tuplecount: f64,
    pub bufusage_start: BufferUsage,
    pub walusage_start: WalUsage,
    // Accumulated statistics across all completed cycles:
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
