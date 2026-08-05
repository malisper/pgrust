// pgstat_backend.c — per-backend cumulative IO + WAL, a variable-numbered
// kind keyed by ProcNumber (dboid = InvalidOid). Pending IO is a private
// matrix; pending WAL is the pgWalUsage delta against a backend-local mark.

use core::cell::Cell;

use types_core::instrument::WalUsage;
use types_core::{BackendType, InvalidOid, TimestampTz};

use crate::io::{PgStat_BktypeIO, PENDING_IO_ZERO};
use crate::pending::{PgStat_HashKey, PGSTAT_KIND_BACKEND};
use crate::wal::PgStat_WalCounters;

#[derive(Clone, Copy, PartialEq, Debug)]
#[repr(C)]
pub struct PgStat_Backend {
    pub stat_reset_timestamp: TimestampTz,
    pub io_stats: PgStat_BktypeIO,
    pub wal_counters: PgStat_WalCounters,
}

impl Default for PgStat_Backend {
    fn default() -> Self {
        PgStat_Backend {
            stat_reset_timestamp: 0,
            io_stats: crate::io::BKTYPE_IO_ZERO,
            wal_counters: PgStat_WalCounters::default(),
        }
    }
}

pub const PGSTAT_BACKEND_FLUSH_IO: u32 = 1 << 0;
pub const PGSTAT_BACKEND_FLUSH_WAL: u32 = 1 << 1;
pub const PGSTAT_BACKEND_FLUSH_ALL: u32 = PGSTAT_BACKEND_FLUSH_IO | PGSTAT_BACKEND_FLUSH_WAL;

thread_local! {
    static PREV_BACKEND_WAL_USAGE: Cell<WalUsage> = const { Cell::new(WalUsage {
        wal_records: 0,
        wal_fpi: 0,
        wal_bytes: 0,
        wal_buffers_full: 0,
    }) };
}

fn backend_key(proc_number: i32) -> PgStat_HashKey {
    PgStat_HashKey {
        kind: PGSTAT_KIND_BACKEND,
        dboid: InvalidOid,
        objid: proc_number as u32 as u64,
    }
}

fn current_wal_usage() -> Option<WalUsage> {
    transam_xlog_seams::wal_usage::is_installed()
        .then(transam_xlog_seams::wal_usage::call)
}

pub fn pgstat_tracks_backend_bktype(bktype: BackendType) -> bool {
    use BackendType as B;
    !matches!(
        bktype,
        B::Invalid
            | B::AutovacLauncher
            | B::DeadEndBackend
            | B::Archiver
            | B::Logger
            | B::BgWriter
            | B::Checkpointer
            | B::IoWorker
            | B::Startup
    )
}

fn backend_wal_have_pending() -> bool {
    match current_wal_usage() {
        Some(usage) => {
            usage.wal_records != PREV_BACKEND_WAL_USAGE.with(|c| c.get()).wal_records
        }
        None => false,
    }
}

pub fn pgstat_flush_backend(nowait: bool, flags: u32) -> bool {
    let _ = nowait;
    if !pgstat_tracks_backend_bktype(miscinit::GetMyBackendType()) {
        return false;
    }
    let has_io = crate::io::with_pending_block(|blk| blk.backend_has_iostats);
    let has_wal = backend_wal_have_pending();
    if !has_io && !has_wal {
        return false;
    }
    let key = backend_key(init_small::globals::MyProcNumber());
    crate::shmem::update_backend_entry(key, |entry| {
        if flags & PGSTAT_BACKEND_FLUSH_IO != 0 && has_io {
            crate::io::with_pending_block(|blk| {
                let pending = &mut blk.backend;
                for o in 0..crate::io::IOOBJECT_NUM_TYPES {
                    for c in 0..crate::io::IOCONTEXT_NUM_TYPES {
                        for p in 0..crate::io::IOOP_NUM_TYPES {
                            entry.io_stats.counts[o][c][p] += pending.counts[o][c][p];
                            entry.io_stats.bytes[o][c][p] += pending.bytes[o][c][p];
                            entry.io_stats.times[o][c][p] +=
                                pending.pending_times_ns[o][c][p] / 1000;
                        }
                    }
                }
                *pending = PENDING_IO_ZERO;
                blk.backend_has_iostats = false;
            });
        }
        if flags & PGSTAT_BACKEND_FLUSH_WAL != 0 && has_wal {
            let usage = current_wal_usage().expect("has_wal implies installed");
            let prev = PREV_BACKEND_WAL_USAGE.with(|c| c.get());
            let w = &mut entry.wal_counters;
            w.wal_records += usage.wal_records - prev.wal_records;
            w.wal_fpi += usage.wal_fpi - prev.wal_fpi;
            w.wal_bytes =
                w.wal_bytes.wrapping_add(usage.wal_bytes.wrapping_sub(prev.wal_bytes));
            w.wal_buffers_full += usage.wal_buffers_full - prev.wal_buffers_full;
            PREV_BACKEND_WAL_USAGE.with(|c| c.set(usage));
        }
    });
    false
}

pub(crate) fn pgstat_backend_flush_cb(nowait: bool) -> bool {
    pgstat_flush_backend(nowait, PGSTAT_BACKEND_FLUSH_ALL)
}

// pgstat_bestart_initial's create: the slot may carry a previous holder of
// this ProcNumber — reset shared and local halves.
pub fn pgstat_create_backend(proc_number: i32) {
    let key = backend_key(proc_number);
    crate::shmem::update_backend_entry(key, |entry| {
        *entry = PgStat_Backend::default();
    });
    crate::io::with_pending_block(|blk| {
        blk.backend = PENDING_IO_ZERO;
        blk.backend_has_iostats = false;
    });
    if let Some(usage) = current_wal_usage() {
        PREV_BACKEND_WAL_USAGE.with(|c| c.set(usage));
    }
}

pub fn pgstat_fetch_stat_backend(proc_number: i32) -> Option<PgStat_Backend> {
    match crate::shmem::fetch_entry(backend_key(proc_number)) {
        Some(crate::shmem::SharedEntry::Backend(b)) => Some(b),
        Some(_) => unreachable!("backend key holds non-backend shared entry"),
        None => None,
    }
}

pub fn pgstat_reset_backend(proc_number: i32) {
    crate::shmem::pgstat_reset(PGSTAT_KIND_BACKEND, InvalidOid, proc_number as u32 as u64);
}

pub(crate) fn pgstat_backend_shutdown() {
    let proc_number = init_small::globals::MyProcNumber();
    if proc_number >= 0 && pgstat_tracks_backend_bktype(miscinit::GetMyBackendType()) {
        crate::shmem::drop_entry(backend_key(proc_number));
    }
}
