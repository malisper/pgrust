use core::ffi::{c_double, c_int};

pub const NS_PER_S: i64 = 1_000_000_000;

#[repr(C)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct instr_time {
    pub ticks: i64,
}

#[repr(C)]
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

#[repr(C)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct WalUsage {
    pub wal_records: i64,
    pub wal_fpi: i64,
    pub wal_bytes: u64,
    pub wal_buffers_full: i64,
}

pub type InstrumentOption = c_int;

pub const INSTRUMENT_TIMER: InstrumentOption = 1 << 0;
pub const INSTRUMENT_BUFFERS: InstrumentOption = 1 << 1;
pub const INSTRUMENT_ROWS: InstrumentOption = 1 << 2;
pub const INSTRUMENT_WAL: InstrumentOption = 1 << 3;
pub const INSTRUMENT_ALL: InstrumentOption = c_int::MAX;

#[repr(C)]
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct Instrumentation {
    pub need_timer: bool,
    pub need_bufusage: bool,
    pub need_walusage: bool,
    pub async_mode: bool,
    pub running: bool,
    pub starttime: instr_time,
    pub counter: instr_time,
    pub firsttuple: c_double,
    pub tuplecount: c_double,
    pub bufusage_start: BufferUsage,
    pub walusage_start: WalUsage,
    pub startup: c_double,
    pub total: c_double,
    pub ntuples: c_double,
    pub ntuples2: c_double,
    pub nloops: c_double,
    pub nfiltered1: c_double,
    pub nfiltered2: c_double,
    pub bufusage: BufferUsage,
    pub walusage: WalUsage,
}

#[repr(C)]
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct WorkerInstrumentation {
    pub num_workers: c_int,
    /// `Instrumentation instrument[FLEXIBLE_ARRAY_MEMBER]` — per-worker
    /// instrumentation, modeled as a zero-length array (the C flexible array
    /// member). The objects that follow are reached through
    /// [`worker_instrumentation_array`] at `offsetof(WorkerInstrumentation,
    /// instrument)` bytes from the start.
    pub instrument: [Instrumentation; 0],
}

/// Byte offset of the flexible array member `instrument` within
/// `WorkerInstrumentation`, i.e. `offsetof(WorkerInstrumentation, instrument)`.
#[inline]
pub fn worker_instrumentation_array_offset() -> usize {
    core::mem::offset_of!(WorkerInstrumentation, instrument)
}

/// `&worker_instrument->instrument` — the `Instrumentation` array that follows
/// the `WorkerInstrumentation` header.
///
/// # Safety
/// `wi` must point at a live `WorkerInstrumentation` allocated with at least
/// `offsetof(WorkerInstrumentation, instrument) + num_workers *
/// sizeof(Instrumentation)` bytes.
#[inline]
pub unsafe fn worker_instrumentation_array(wi: *mut WorkerInstrumentation) -> *mut Instrumentation {
    let offset = worker_instrumentation_array_offset();
    (wi as *mut u8).add(offset).cast::<Instrumentation>()
}
