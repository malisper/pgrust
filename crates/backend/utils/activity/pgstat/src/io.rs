// pgstat_io.c — per-backend pending IO matrix, flush into the per-BackendType
// shared table, tracked-combination predicates.

use core::cell::RefCell;
use std::sync::Mutex;

use types_core::{BackendType, TimestampTz, BACKEND_NUM_TYPES};
pub use types_storage::buf::IOContext;

use crate::pending;

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum IOObject {
    Relation = 0,
    TempRelation = 1,
    Wal = 2,
}

pub const IOOBJECT_NUM_TYPES: usize = 3;
pub const IOCONTEXT_NUM_TYPES: usize = 5;
pub const IOOP_NUM_TYPES: usize = 8;

// Order matters: ops tracked in bytes are at the end (pgstat.h:298).
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Debug)]
pub enum IOOp {
    Evict = 0,
    Fsync = 1,
    Hit = 2,
    Reuse = 3,
    Writeback = 4,
    Extend = 5,
    Read = 6,
    Write = 7,
}

pub fn pgstat_is_ioop_tracked_in_bytes(io_op: IOOp) -> bool {
    io_op as usize >= IOOp::Extend as usize
}

type IoMatrix = [[[i64; IOOP_NUM_TYPES]; IOCONTEXT_NUM_TYPES]; IOOBJECT_NUM_TYPES];
type IoMatrixU = [[[u64; IOOP_NUM_TYPES]; IOCONTEXT_NUM_TYPES]; IOOBJECT_NUM_TYPES];

const IO_ZERO: IoMatrix = [[[0; IOOP_NUM_TYPES]; IOCONTEXT_NUM_TYPES]; IOOBJECT_NUM_TYPES];
const IO_ZERO_U: IoMatrixU = [[[0; IOOP_NUM_TYPES]; IOCONTEXT_NUM_TYPES]; IOOBJECT_NUM_TYPES];

// repr(C), fixed-size scalar arrays: statsfile serialization copies as bytes.
// times in microseconds (shared/snapshot form).
#[derive(Clone, Copy, PartialEq, Debug)]
#[repr(C)]
pub struct PgStat_BktypeIO {
    pub bytes: IoMatrixU,
    pub counts: IoMatrix,
    pub times: IoMatrix,
}

pub const BKTYPE_IO_ZERO: PgStat_BktypeIO = PgStat_BktypeIO {
    bytes: IO_ZERO_U,
    counts: IO_ZERO,
    times: IO_ZERO,
};

impl Default for PgStat_BktypeIO {
    fn default() -> Self {
        BKTYPE_IO_ZERO
    }
}

#[derive(Clone, Copy, PartialEq, Debug)]
#[repr(C)]
pub struct PgStat_IO {
    pub stat_reset_timestamp: TimestampTz,
    pub stats: [PgStat_BktypeIO; BACKEND_NUM_TYPES],
}

const IO_STATS_ZERO: PgStat_IO = PgStat_IO {
    stat_reset_timestamp: 0,
    stats: [BKTYPE_IO_ZERO; BACKEND_NUM_TYPES],
};

impl Default for PgStat_IO {
    fn default() -> Self {
        IO_STATS_ZERO
    }
}

// Pending half: times kept in ns ticks, converted to microseconds at flush.
#[derive(Clone, Copy, PartialEq, Debug)]
pub struct PgStat_PendingIO {
    pub bytes: IoMatrixU,
    pub counts: IoMatrix,
    pub pending_times_ns: IoMatrix,
}

pub const PENDING_IO_ZERO: PgStat_PendingIO = PgStat_PendingIO {
    bytes: IO_ZERO_U,
    counts: IO_ZERO,
    pending_times_ns: IO_ZERO,
};

static SHARED_IO: Mutex<PgStat_IO> = Mutex::new(IO_STATS_ZERO);

pub(crate) struct PendingIoBlock {
    pub(crate) io: PgStat_PendingIO,
    pub(crate) backend: PgStat_PendingIO,
    pub(crate) have_iostats: bool,
    pub(crate) backend_has_iostats: bool,
}

thread_local! {
    // One UnsafeCell block: the per-buffer-hit count sits on the M2/M4 pin
    // path where C pays bare adds on plain globals; a single TLS access
    // covers both matrices and both flags. Every access is a leaf (no
    // callback escapes with_pending_block).
    static PENDING_IO_BLOCK: core::cell::UnsafeCell<PendingIoBlock> =
        const { core::cell::UnsafeCell::new(PendingIoBlock {
            io: PENDING_IO_ZERO,
            backend: PENDING_IO_ZERO,
            have_iostats: false,
            backend_has_iostats: false,
        }) };
    static SNAPSHOT_IO: RefCell<Option<PgStat_IO>> = const { RefCell::new(None) };
}

#[inline(always)]
pub(crate) fn with_pending_block<R>(f: impl FnOnce(&mut PendingIoBlock) -> R) -> R {
    // SAFETY: thread-local; every caller passes a closure that neither
    // re-enters this module nor stores the reference (single-entry leaf).
    PENDING_IO_BLOCK.with(|s| f(unsafe { &mut *s.get() }))
}

pub fn pgstat_bktype_io_stats_valid(backend_io: &PgStat_BktypeIO, bktype: BackendType) -> bool {
    for (o, obj) in [IOObject::Relation, IOObject::TempRelation, IOObject::Wal]
        .into_iter()
        .enumerate()
    {
        for c in 0..IOCONTEXT_NUM_TYPES {
            let ctx = io_context_from_index(c);
            for (p, op) in IOOP_ALL.into_iter().enumerate() {
                let tracked = pgstat_tracks_io_op(bktype, obj, ctx, op);
                if !tracked
                    && (backend_io.counts[o][c][p] != 0
                        || backend_io.bytes[o][c][p] != 0
                        || backend_io.times[o][c][p] != 0)
                {
                    return false;
                }
                if backend_io.times[o][c][p] != 0 && backend_io.counts[o][c][p] == 0 {
                    return false;
                }
            }
        }
    }
    true
}

pub const IOOP_ALL: [IOOp; IOOP_NUM_TYPES] = [
    IOOp::Evict,
    IOOp::Fsync,
    IOOp::Hit,
    IOOp::Reuse,
    IOOp::Writeback,
    IOOp::Extend,
    IOOp::Read,
    IOOp::Write,
];

pub fn io_context_from_index(i: usize) -> IOContext {
    match i {
        0 => IOContext::IOCONTEXT_BULKREAD,
        1 => IOContext::IOCONTEXT_BULKWRITE,
        2 => IOContext::IOCONTEXT_INIT,
        3 => IOContext::IOCONTEXT_NORMAL,
        4 => IOContext::IOCONTEXT_VACUUM,
        _ => unreachable!("bad IOContext index"),
    }
}

pub fn pgstat_count_io_op(
    io_object: IOObject,
    io_context: IOContext,
    io_op: IOOp,
    cnt: u32,
    bytes: u64,
) {
    let (o, c, p) = (io_object as usize, io_context as usize, io_op as usize);
    debug_assert!(pgstat_is_ioop_tracked_in_bytes(io_op) || bytes == 0);
    // C asserts unconditionally; Invalid is exempt here because in-process
    // unit tests exercise buffer paths without a backend type.
    debug_assert!(
        miscinit::GetMyBackendType() == BackendType::Invalid
            || pgstat_tracks_io_op(miscinit::GetMyBackendType(), io_object, io_context, io_op)
    );

    let track_backend = crate::backend::pgstat_tracks_backend_bktype(miscinit::GetMyBackendType());
    with_pending_block(|blk| {
        blk.io.counts[o][c][p] += cnt as i64;
        blk.io.bytes[o][c][p] += bytes;
        blk.have_iostats = true;
        if track_backend {
            blk.backend.counts[o][c][p] += cnt as i64;
            blk.backend.bytes[o][c][p] += bytes;
            blk.backend_has_iostats = true;
        }
    });
    pending::pgstat_report_fixed_set();
}

// Zero start means timing disabled: pgstat_count_io_op_time skips the diff.
pub fn pgstat_prepare_io_time(track_io_guc: bool) -> i64 {
    if track_io_guc {
        crate::now_ns()
    } else {
        0
    }
}

pub fn pgstat_count_io_op_time(
    io_object: IOObject,
    io_context: IOContext,
    io_op: IOOp,
    start_ns: i64,
    cnt: u32,
    bytes: u64,
) {
    if start_ns != 0 {
        let elapsed_ns = crate::now_ns() - start_ns;
        if io_object != IOObject::Wal {
            // pgBufferUsage blk time additions happen at the bufmgr call site
            // (it owns those counters); the dbstats half lives here as in C.
            match io_op {
                IOOp::Write | IOOp::Extend => {
                    crate::database::pgstat_count_buffer_write_time(elapsed_ns / 1000);
                }
                IOOp::Read => {
                    crate::database::pgstat_count_buffer_read_time(elapsed_ns / 1000);
                }
                _ => {}
            }
        }
        let (o, c, p) = (io_object as usize, io_context as usize, io_op as usize);
        let track_backend =
            crate::backend::pgstat_tracks_backend_bktype(miscinit::GetMyBackendType());
        with_pending_block(|blk| {
            blk.io.pending_times_ns[o][c][p] += elapsed_ns;
            if track_backend {
                blk.backend.pending_times_ns[o][c][p] += elapsed_ns;
                blk.backend_has_iostats = true;
            }
        });
    }
    pgstat_count_io_op(io_object, io_context, io_op, cnt, bytes);
}

pub fn pgstat_fetch_stat_io() -> PgStat_IO {
    pgstat_io_snapshot_build();
    SNAPSHOT_IO.with(|s| s.borrow().expect("io snapshot built above"))
}

pub(crate) fn pgstat_io_snapshot_build() {
    crate::shmem::consume_forced_snapshot_clear();
    if crate::pgstat_fetch_consistency() == crate::PGSTAT_FETCH_CONSISTENCY_SNAPSHOT {
        crate::shmem::build_snapshot();
        return;
    }
    let refresh = crate::pgstat_fetch_consistency() == crate::PGSTAT_FETCH_CONSISTENCY_NONE
        || SNAPSHOT_IO.with(|s| s.borrow().is_none());
    if refresh {
        pgstat_io_snapshot_cb();
    }
}

pub(crate) fn pgstat_io_snapshot_cb() {
    let shared = *SHARED_IO.lock().unwrap();
    SNAPSHOT_IO.with(|s| *s.borrow_mut() = Some(shared));
}

pub(crate) fn pgstat_io_snapshot_clear() {
    SNAPSHOT_IO.with(|s| *s.borrow_mut() = None);
}

pub fn pgstat_flush_io(nowait: bool) {
    pgstat_io_flush_cb(nowait);
}

pub(crate) fn pgstat_io_flush_cb(_nowait: bool) -> bool {
    if !with_pending_block(|blk| blk.have_iostats) {
        return false;
    }
    let bktype = miscinit::GetMyBackendType() as usize;
    with_pending_block(|blk| {
        let mut shared = SHARED_IO.lock().unwrap();
        let dst = &mut shared.stats[bktype];
        let pending = &mut blk.io;
        for o in 0..IOOBJECT_NUM_TYPES {
            for c in 0..IOCONTEXT_NUM_TYPES {
                for p in 0..IOOP_NUM_TYPES {
                    dst.counts[o][c][p] += pending.counts[o][c][p];
                    dst.bytes[o][c][p] += pending.bytes[o][c][p];
                    dst.times[o][c][p] += pending.pending_times_ns[o][c][p] / 1000;
                }
            }
        }
        *pending = PENDING_IO_ZERO;
        blk.have_iostats = false;
    });
    false
}

pub(crate) fn import_io_stats(v: PgStat_IO) {
    *SHARED_IO.lock().unwrap() = v;
}

pub(crate) fn export_io_stats() -> PgStat_IO {
    *SHARED_IO.lock().unwrap()
}

pub(crate) fn pgstat_io_reset_all_cb(ts: TimestampTz) {
    let mut shared = SHARED_IO.lock().unwrap();
    *shared = IO_STATS_ZERO;
    shared.stat_reset_timestamp = ts;
}

pub fn pgstat_get_io_context_name(io_context: IOContext) -> &'static str {
    match io_context {
        IOContext::IOCONTEXT_BULKREAD => "bulkread",
        IOContext::IOCONTEXT_BULKWRITE => "bulkwrite",
        IOContext::IOCONTEXT_INIT => "init",
        IOContext::IOCONTEXT_NORMAL => "normal",
        IOContext::IOCONTEXT_VACUUM => "vacuum",
    }
}

pub fn pgstat_get_io_object_name(io_object: IOObject) -> &'static str {
    match io_object {
        IOObject::Relation => "relation",
        IOObject::TempRelation => "temp relation",
        IOObject::Wal => "wal",
    }
}

pub fn pgstat_tracks_io_bktype(bktype: BackendType) -> bool {
    !matches!(
        bktype,
        BackendType::Invalid
            | BackendType::DeadEndBackend
            | BackendType::Archiver
            | BackendType::Logger
    )
}

pub fn pgstat_tracks_io_object(
    bktype: BackendType,
    io_object: IOObject,
    io_context: IOContext,
) -> bool {
    use BackendType as B;
    use IOContext as C;
    use IOObject as O;

    if !pgstat_tracks_io_bktype(bktype) {
        return false;
    }
    if io_object == O::Wal && !matches!(io_context, C::IOCONTEXT_NORMAL | C::IOCONTEXT_INIT) {
        return false;
    }
    if io_object == O::TempRelation && io_context != C::IOCONTEXT_NORMAL {
        return false;
    }

    let no_temp_rel = matches!(
        bktype,
        B::AutovacLauncher
            | B::BgWriter
            | B::Checkpointer
            | B::AutovacWorker
            | B::StandaloneBackend
            | B::Startup
            | B::WalSummarizer
            | B::WalWriter
            | B::WalReceiver
    );
    if no_temp_rel && io_context == C::IOCONTEXT_NORMAL && io_object == O::TempRelation {
        return false;
    }
    if matches!(bktype, B::WalSummarizer | B::WalReceiver | B::WalWriter) && io_object != O::Wal {
        return false;
    }
    if matches!(bktype, B::Checkpointer | B::BgWriter)
        && matches!(io_context, C::IOCONTEXT_BULKREAD | C::IOCONTEXT_BULKWRITE | C::IOCONTEXT_VACUUM)
    {
        return false;
    }
    if bktype == B::AutovacLauncher && io_context == C::IOCONTEXT_VACUUM {
        return false;
    }
    if matches!(bktype, B::AutovacWorker | B::AutovacLauncher)
        && io_context == C::IOCONTEXT_BULKWRITE
    {
        return false;
    }
    true
}

pub fn pgstat_tracks_io_op(
    bktype: BackendType,
    io_object: IOObject,
    io_context: IOContext,
    io_op: IOOp,
) -> bool {
    use BackendType as B;
    use IOContext as C;
    use IOObject as O;
    use IOOp as P;

    if !pgstat_tracks_io_object(bktype, io_object, io_context) {
        return false;
    }
    if bktype == B::BgWriter && matches!(io_op, P::Read | P::Evict | P::Hit) {
        return false;
    }
    if bktype == B::Checkpointer
        && ((io_object != O::Wal && io_op == P::Read) || matches!(io_op, P::Evict | P::Hit))
    {
        return false;
    }
    if matches!(bktype, B::AutovacLauncher | B::BgWriter | B::Checkpointer) && io_op == P::Extend {
        return false;
    }
    if io_object == O::Wal
        && io_op == P::Read
        && matches!(
            bktype,
            B::WalReceiver | B::BgWriter | B::AutovacLauncher | B::AutovacWorker | B::WalWriter
        )
    {
        return false;
    }
    if io_object == O::TempRelation && matches!(io_op, P::Fsync | P::Writeback) {
        return false;
    }
    if io_context == C::IOCONTEXT_BULKREAD && io_op == P::Extend {
        return false;
    }
    let strategy_ctx = matches!(
        io_context,
        C::IOCONTEXT_BULKREAD | C::IOCONTEXT_BULKWRITE | C::IOCONTEXT_VACUUM
    );
    if io_op == P::Reuse && !strategy_ctx {
        return false;
    }
    if io_object == O::Wal {
        if io_context == C::IOCONTEXT_INIT && !matches!(io_op, P::Write | P::Fsync) {
            return false;
        }
        if io_context == C::IOCONTEXT_NORMAL && !matches!(io_op, P::Write | P::Read | P::Fsync) {
            return false;
        }
    }
    if strategy_ctx && io_op == P::Fsync {
        return false;
    }
    true
}

pub fn io_object_from_u32(v: u32) -> IOObject {
    match v {
        0 => IOObject::Relation,
        1 => IOObject::TempRelation,
        2 => IOObject::Wal,
        _ => unreachable!("bad IOObject value"),
    }
}

pub fn io_op_from_u32(v: u32) -> IOOp {
    match v {
        0 => IOOp::Evict,
        1 => IOOp::Fsync,
        2 => IOOp::Hit,
        3 => IOOp::Reuse,
        4 => IOOp::Writeback,
        5 => IOOp::Extend,
        6 => IOOp::Read,
        7 => IOOp::Write,
        _ => unreachable!("bad IOOp value"),
    }
}

pub fn pgstat_have_pending_io() -> bool {
    with_pending_block(|blk| blk.have_iostats)
}

pub fn pgstat_pending_io() -> PgStat_PendingIO {
    with_pending_block(|blk| blk.io)
}
