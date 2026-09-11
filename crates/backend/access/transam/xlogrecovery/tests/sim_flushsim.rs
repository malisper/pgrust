// GL-FLUSHSIM-1 — deterministic multi-session COMMIT-PATH trajectory sim.
//
// The sim_crash_sweep enumeration pattern (writer child + recover child over
// SimVfs, product WAL/commit/recovery paths) generalized to the commit path
// under CONCURRENCY: N committing sessions (mixed synchronous_commit), M
// read-only sessions, K listener sessions (modeled at the product's own
// async_seams notify hook points) and a walwriter actor, all REAL threads
// over one process-shared sim universe, interleaved by the pgsync permit
// scheduler (PGRUST_SIM_SCHED=1, seeded by PGRUST_SIM_SEED).
//
// TRAJECTORY = (schedule seed x flush-timing profile x crash point).
// Crash points come in two granularities:
//   * vfs-op cuts (FaultRule::crash_at_op k, the sweep template's contract) —
//     the disk image can only change at vfs ops, so an op sweep covers every
//     durability-relevant window (commit-record write, WAL fdatasync, clog
//     page write ...);
//   * EXTERNAL-EVENT-triggered cuts (cut when the i-th ack / read-completion
//     / notify-delivery is appended to the ledger) — exact positioning
//     around the externally-visible steps the durability ledger governs.
//
// THE LEDGER: every externally-visible event (writer ack, read
// CommandComplete with its observed transaction set, delivered notification)
// is appended, under the permit total order, with the commit LSN and the
// engine's claimed flush position at that moment. Post-recovery invariants
// (recover child):
//   I1a durability of acks: every pre-cut SYNC ack's txn survives recovery;
//   I1b read visibility: every txn observed by a pre-cut completed read
//       survives;
//   I1c notify: every pre-cut delivered notification's notifier survives;
//   I2  LSN-prefix closure over the acked set (no holes);
//   I3  atomicity: every value's visible row count is all-or-nothing, and
//       the MVCC-visible fold equals the clog-committed set exactly.
// Live (no-crash) invariants (writer side):
//   L1 at-ack durability: a sync-claiming commit returns only when the
//      claimed flush position covers its commit record;
//   L2 flush-position monotonicity across the ledger;
//   L3 liveness: every committer finishes every txn, within a per-commit
//      virtual-time budget; pgsync virtual ceiling is the net;
//   L4 flushpipe tripwires: backstop_self_served == 0 and retry_rearm == 0
//      in healthy profiles; registered == completed + self_served +
//      backstop_self_served + retry_rearm (conservation); knob-OFF runs
//      keep every counter at zero (structural inertness).
//
// SENSITIVITY (the mutation battery — deliberately broken variants, all
// harness-side, ZERO product deltas):
//   (a) ack-early     — committer 0 runs the async posture while claiming
//                       sync acks (the server lies to the client); caught
//                       by L1 immediately + I1a under cuts.
//   (b) wake-drop     — committer 0 parks its pipelined wait on its LOCAL
//                       latch while the queue node names its PROC latch
//                       (SwitchBackToLocalLatch): every directed completion
//                       wake is misdirected; caught by the backstop
//                       tripwire L4 (+ the per-commit budget L3). The
//                       registration-DROP variant proper needs a sim-only
//                       hook inside wait_for_flush — the one negotiated
//                       seam ask, ledgered in the letter.
//   (c) notify-early  — notifications delivered at the pre_commit hook
//                       (before the commit record exists) instead of
//                       at_commit; caught by I1c under cuts.
//   (d) wrong-lsn     — a one-shot overclaim of the shared flush-result
//                       word (completion published for an LSN the disk
//                       never saw); INVISIBLE to L1 by construction (the
//                       in-band probe reads the same lying word — the
//                       reason the crash tier exists); caught by I1a under
//                       cuts, and opportunistically by L2 when the next
//                       real flush publishes a lower truth.
//
// Run: RUSTFLAGS='--cfg pgrust_sim' cargo test -p xlogrecovery --test sim_flushsim
#![cfg(pgrust_sim)]
#![allow(clippy::too_many_arguments)]

use std::cell::Cell;
use std::rc::Rc;
use std::sync::atomic::Ordering::Relaxed;
use std::sync::atomic::{AtomicBool, AtomicI32, AtomicU32, AtomicU64};

use mcx::{Mcx, MemoryContext, PgVec};
use transam_xlog::control_file::{
    FirstNormalUnloggedLSN, FLOATFORMAT_VALUE, PG_CONTROL_FILE_SIZE, PG_CONTROL_VERSION,
    TOAST_MAX_CHUNK_SIZE,
};
use transam_xlog::{
    SizeOfXLogRecord, XLogRecPtrToBytePos, DB_IN_PRODUCTION, MAXALIGN, RM_XLOG_ID,
    WAL_LEVEL_REPLICA, XLOG_CHECKPOINT_SHUTDOWN, XLP_LONG_HEADER,
};
use types_core::{
    BackendType, ForkNumber, Oid, TransactionId, XLogRecPtr, BLCKSZ, INVALID_PROC_NUMBER,
    RELPERSISTENCE_PERMANENT,
};
use types_error::PgResult;
use types_rel::{FormData_pg_class, LockInfoData, LockRelId, RelationData, LOCKMODE};
use types_snapshot::{SnapshotData, SnapshotType};
use types_storage::RelFileLocator;
use types_tuple::itemptr::ItemPointerData;
use types_tuple::{
    CompactAttribute, FormData_pg_attribute, HeapTupleData, NameData, TupleDescData,
};

use vfs::sim::{FaultRule, SeededFaultPlan, SimVfs};

// ---------------------------------------------------------------------------
// Rig constants (the sweep template's minted world, commit-path sized).
// ---------------------------------------------------------------------------

const SEG: i32 = 1024 * 1024;
const SYS_ID: u64 = 0x5EED_F7A5_0001;
const REL_OID: Oid = 61000;
const RLOC: RelFileLocator = RelFileLocator::new(1663, 5, REL_OID);
const CKPT_LOC: XLogRecPtr = SEG as u64 + 40;
const CKPT_TOT_LEN: usize = SizeOfXLogRecord + 2 + controldata_utils::SIZEOF_CHECKPOINT;
const BASE_XID: u32 = 3;

/// Rows per committed transaction (small: commit-path density over volume).
const ROWS_PER_TXN: u32 = 4;

const ROLE_ENV: &str = "PGRUST_FLUSHSIM_ROLE";
const PACK_ENV: &str = "PGRUST_FLUSHSIM_PACK";
const K_ENV: &str = "PGRUST_FLUSHSIM_K";
const TRIGGER_ENV: &str = "PGRUST_FLUSHSIM_TRIGGER";
const PROFILE_ENV: &str = "PGRUST_FLUSHSIM_PROFILE";
const MUTATION_ENV: &str = "PGRUST_FLUSHSIM_MUTATION";
const NC_ENV: &str = "PGRUST_FLUSHSIM_NC";
const NR_ENV: &str = "PGRUST_FLUSHSIM_NR";
const NL_ENV: &str = "PGRUST_FLUSHSIM_NL";
const TXNS_ENV: &str = "PGRUST_FLUSHSIM_TXNS";
const MIX_ENV: &str = "PGRUST_FLUSHSIM_MIX";
/// "<hold_ms>:<gap_ms>" arms the flush-hold disturbor: a harness actor that
/// takes WALWriteLock and sleeps (virtual time) with it held — "a long
/// flush in flight". Under the permit scheduler the product's own lock
/// windows contain no yield points (raw memcpy/vfs ops are not scheduler
/// touches), so WITHOUT this actor a conditional WALWriteLock acquire never
/// fails and the pending-flush queue never engages; the disturbor opens the
/// contended-arm trajectory space deterministically. (The product-shaped
/// alternative — commit_delay — sleeps RAW std::thread::sleep inside the
/// lock window, invisible to the scheduler: ledgered as a LANES ask.)
const HOLD_ENV: &str = "PGRUST_FLUSHSIM_HOLD";

fn env_u32(name: &str, default: u32) -> u32 {
    std::env::var(name).ok().and_then(|v| v.parse().ok()).unwrap_or(default)
}

// ---------------------------------------------------------------------------
// sim-side fs helpers (sim_crash_sweep verbatim; all through the active vfs)
// ---------------------------------------------------------------------------

fn cpath(path: &str) -> std::ffi::CString {
    std::ffi::CString::new(path).unwrap()
}

fn vfs_mkdir_p(path: &str) {
    let mut prefix = String::new();
    for comp in path.split('/') {
        if comp.is_empty() {
            continue;
        }
        prefix.push('/');
        prefix.push_str(comp);
        let rc = vfs::mkdir(&cpath(&prefix), 0o700);
        assert!(
            rc == 0 || vfs::get_errno() == libc::EEXIST,
            "vfs_mkdir_p({prefix}): errno {}",
            vfs::get_errno()
        );
    }
}

fn vfs_write_file(path: &str, data: &[u8]) {
    let fd = vfs::open(&cpath(path), libc::O_RDWR | libc::O_CREAT | libc::O_TRUNC, 0o600);
    assert!(fd >= 0, "vfs_write_file open({path}): errno {}", vfs::get_errno());
    if !data.is_empty() {
        assert_eq!(vfs::pwrite(fd, data, 0), data.len() as isize, "{path}");
    }
    assert_eq!(vfs::close(fd), 0);
}

fn vfs_read_range(path: &str, off: i64, len: usize) -> Vec<u8> {
    let fd = vfs::open(&cpath(path), libc::O_RDONLY, 0);
    assert!(fd >= 0, "vfs_read_range open({path}): errno {}", vfs::get_errno());
    let mut buf = vec![0u8; len];
    let n = vfs::pread(fd, &mut buf, off);
    assert!(n >= 0, "{path}");
    buf.truncate(n as usize);
    assert_eq!(vfs::close(fd), 0);
    buf
}

fn sim_fsync_tree() {
    for (path, entry) in SimVfs::new().image_dump() {
        let p = path.to_str().unwrap().to_string();
        let fd = vfs::open(&cpath(&p), libc::O_RDONLY, 0);
        assert!(fd >= 0, "fsync_tree open({p}): errno {}", vfs::get_errno());
        assert_eq!(vfs::fsync(fd), 0, "fsync_tree fsync({p})");
        assert_eq!(vfs::close(fd), 0);
        let _ = entry;
    }
}

fn export_sim_tree(dst: &std::path::Path) {
    for (path, entry) in SimVfs::new().image_dump() {
        let rel = path.strip_prefix("/").unwrap();
        let out = dst.join(rel);
        match entry {
            None => std::fs::create_dir_all(&out).unwrap(),
            Some((_volatile, durable)) => {
                if let Some(parent) = out.parent() {
                    std::fs::create_dir_all(parent).unwrap();
                }
                std::fs::write(&out, &durable).unwrap();
            }
        }
    }
}

/// crc32c digest of the DURABLE image (sorted walk) — the determinism gate's
/// disk-identity witness.
fn durable_tree_digest() -> u32 {
    let mut crc = crc32c::CRC32C_INIT;
    for (path, entry) in SimVfs::new().image_dump() {
        crc = crc32c::pg_comp_crc32c(crc, path.to_str().unwrap().as_bytes());
        if let Some((_volatile, durable)) = entry {
            crc = crc32c::pg_comp_crc32c(crc, &durable);
        }
    }
    crc32c::fin_crc32c(crc)
}

fn import_tree_into_sim(src: &std::path::Path, sim_base: &str) {
    let mut entries: Vec<_> = std::fs::read_dir(src).unwrap().map(|e| e.unwrap()).collect();
    entries.sort_by_key(|e| e.file_name());
    for e in entries {
        let name = e.file_name().into_string().unwrap();
        let sim_path = if sim_base == "/" {
            format!("/{name}")
        } else {
            format!("{sim_base}/{name}")
        };
        let meta = std::fs::metadata(e.path()).unwrap();
        if meta.is_dir() {
            vfs_mkdir_p(&sim_path);
            import_tree_into_sim(&e.path(), &sim_path);
        } else {
            vfs_write_file(&sim_path, &std::fs::read(e.path()).unwrap());
        }
    }
}

// ---------------------------------------------------------------------------
// The external-event LEDGER (permit-total-ordered; the sim's client's-eye
// record of everything the "outside world" was told before the crash).
// ---------------------------------------------------------------------------

#[derive(Clone, Debug)]
enum EvKind {
    /// A committer's first WAL insert bound (actor, j) to `xid` — not an
    /// external event; the value->xid map the recovery checks need.
    Attempt,
    /// Commit returned to the "client". `sync` = the actor CLAIMS sync-ack
    /// semantics (mutation (a) claims while running async).
    Ack,
    /// A read-only transaction completed (its CommandComplete moment);
    /// `observed` lists the (actor, j) values its snapshot saw.
    ReadDone,
    /// A listener received a notification from txn `xid`.
    NotifyDelivered,
}

#[derive(Clone, Debug)]
struct Event {
    kind: EvKind,
    /// SimVfs cut count at append: 0 = delivered before any crash.
    cut: u64,
    actor: u32,
    j: u32,
    xid: TransactionId,
    /// Commit-record end LSN (Ack) or 0.
    lsn: XLogRecPtr,
    /// The engine's CLAIMED flush position at append (GetFlushRecPtr).
    flushed: XLogRecPtr,
    sync: bool,
    observed: Vec<(u32, u32)>,
}

pgsync::process_global! {
    static LEDGER_G: pgsync::Mutex<Vec<Event>> = pgsync::Mutex::new(Vec::new());
    static VIOLATIONS_G: pgsync::Mutex<Vec<String>> = pgsync::Mutex::new(Vec::new());
    static NOTIFY_BOX_G: pgsync::Mutex<Vec<(u32, u32, TransactionId, bool)>> =
        pgsync::Mutex::new(Vec::new());
}

/// Cut trigger: fire SimVfs::cut() when the n-th event of `kind` is
/// appended ("ack:3"), or at quiescence ("end"), or never ("none"; vfs-op
/// cuts come from the fault plan instead).
struct Trigger {
    kind: &'static str,
    n: u32,
    seen: AtomicU32,
}

static TRIGGER: Trigger = Trigger { kind: "", n: 0, seen: AtomicU32::new(0) };
static TRIGGER_KIND: pgsync::OnceLock<String> = pgsync::OnceLock::new();
static TRIGGER_N: AtomicU32 = AtomicU32::new(0);

fn trigger_kind() -> &'static str {
    let _ = &TRIGGER;
    TRIGGER_KIND.get().map(|s| s.as_str()).unwrap_or("none")
}

fn ev_kind_name(k: &EvKind) -> &'static str {
    match k {
        EvKind::Attempt => "attempt",
        EvKind::Ack => "ack",
        EvKind::ReadDone => "read",
        EvKind::NotifyDelivered => "notify",
    }
}

fn violation(msg: String) {
    eprintln!("FLUSHSIM-VIOLATION: {msg}");
    VIOLATIONS_G.lock().unwrap_or_else(|e| e.into_inner()).push(msg);
}

/// Append an event (permit-ordered), run the live invariants L1/L2, and
/// fire the event cut trigger if armed.
fn ledger_append(mut ev: Event) {
    ev.cut = SimVfs::cut_count();
    let claimed = transam_xlog::GetFlushRecPtr(None);
    ev.flushed = claimed;

    // L1: at-ack durability (sync claims only; pre-cut events only — after
    // a whole-node kill nothing is "delivered" any more).
    if ev.cut == 0 {
        if let EvKind::Ack = ev.kind {
            if ev.sync && ev.flushed < ev.lsn {
                violation(format!(
                    "L1 at-ack: actor {} j {} xid {} acked at lsn {:#x} with claimed flush {:#x}",
                    ev.actor, ev.j, ev.xid, ev.lsn, ev.flushed
                ));
            }
        }
    }

    let fire = {
        let mut g = LEDGER_G.lock().unwrap_or_else(|e| e.into_inner());
        // L2: claimed-flush monotonicity across the ledger order.
        if let Some(prev) = g.last() {
            if ev.cut == 0 && prev.cut == 0 && ev.flushed < prev.flushed {
                violation(format!(
                    "L2 monotonicity: claimed flush regressed {:#x} -> {:#x} at {} actor {}",
                    prev.flushed,
                    ev.flushed,
                    ev_kind_name(&ev.kind),
                    ev.actor
                ));
            }
        }
        let kind = ev_kind_name(&ev.kind);
        g.push(ev);
        let armed = trigger_kind();
        if armed == kind {
            let seen = TRIGGER.seen.fetch_add(1, Relaxed) + 1;
            seen == TRIGGER_N.load(Relaxed)
        } else {
            false
        }
    };
    if fire && SimVfs::cut_count() == 0 {
        SimVfs::cut();
    }
}

fn ledger_dump() -> String {
    let g = LEDGER_G.lock().unwrap_or_else(|e| e.into_inner());
    let mut out = String::new();
    for (i, ev) in g.iter().enumerate() {
        let obs = ev
            .observed
            .iter()
            .map(|(a, j)| format!("{a}.{j}"))
            .collect::<Vec<_>>()
            .join(",");
        out.push_str(&format!(
            "EV|seq={i}|cut={}|kind={}|actor={}|j={}|xid={}|lsn={}|flushed={}|sync={}|obs={obs}\n",
            ev.cut,
            ev_kind_name(&ev.kind),
            ev.actor,
            ev.j,
            ev.xid,
            ev.lsn,
            ev.flushed,
            ev.sync as u32
        ));
    }
    out
}

// ---------------------------------------------------------------------------
// Mutation config (sensitivity battery).
// ---------------------------------------------------------------------------

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Mutation {
    None,
    /// (a) committer 0 claims sync acks while running the async posture.
    AckEarly,
    /// (b) committer 0 parks its pipelined wait on the WRONG latch.
    WakeDrop,
    /// (c) notify delivery moved to the pre-commit hook (pre-durability).
    NotifyEarly,
    /// (d) one-shot overclaim of the shared flush-result word.
    WrongLsn,
}

fn mutation() -> Mutation {
    static M: pgsync::OnceLock<Mutation> = pgsync::OnceLock::new();
    *M.get_or_init(|| match std::env::var(MUTATION_ENV).as_deref() {
        Ok("ack-early") => Mutation::AckEarly,
        Ok("wake-drop") => Mutation::WakeDrop,
        Ok("notify-early") => Mutation::NotifyEarly,
        Ok("wrong-lsn") => Mutation::WrongLsn,
        _ => Mutation::None,
    })
}

// The committer currently inside CommitTransactionCommand on this thread,
// so the product-called notify hooks (async_seams) can attribute delivery.
thread_local! {
    /// (actor, j, xid, claims_sync) for the commit in flight on this thread.
    static CURRENT_COMMIT: Cell<Option<(u32, u32, TransactionId, bool)>> =
        const { Cell::new(None) };
}

fn deliver_pending_notify() -> PgResult<()> {
    if let Some((actor, j, xid, sync)) = CURRENT_COMMIT.with(|c| c.get()) {
        NOTIFY_BOX_G.lock().unwrap_or_else(|e| e.into_inner()).push((actor, j, xid, sync));
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// The rig: stub seams + real units. Cribbed from sim_crash_sweep's
// install_stub_seams/install_real with ONE structural difference: the
// park/wake stack (latch, pg_sema, s_lock spin backoff, the latch-switch
// miscinit rows) is REAL — this world has concurrent committers and the
// whole point is the wait mechanics. The notify hooks route to the harness
// mailbox at the product's own call points.
// ---------------------------------------------------------------------------

fn install_stub_seams() {
    use init_small::globals as g;
    g::SetMaxConnections(16);
    g::set_max_worker_processes(2);
    g::SetMaxBackends(16 + 3 + 2 + 2 + 2);
    g::SetMyProcPid(779);
    g::SetMyDatabaseId(5);
    g::SetNBuffers(128);
    g::set_transaction_buffers(64);
    g::set_subtransaction_buffers(64);

    // REAL park/wake stack (the delta vs the sweep template).
    pg_sema::init_seams();
    s_lock::init_seams();
    latch::init_seams();
    miscinit_seams::switch_to_shared_latch::set(miscinit::SwitchToSharedLatch);
    miscinit_seams::switch_back_to_local_latch::set(miscinit::SwitchBackToLocalLatch);

    miscinit_seams::get_user_id::set(|| 10);
    miscinit_seams::is_bootstrap_processing_mode::set(|| false);
    waitevent_seams::pgstat_set_wait_event_storage::set(|_| {});
    waitevent_seams::pgstat_report_wait_start::set(|_| {});
    waitevent_seams::pgstat_report_wait_end::set(|| {});
    waitevent_seams::pgstat_reset_wait_event_storage::set(|| {});
    ipc_seams::on_shmem_exit::set(|_, _| {});
    deadlock_seams::init_dead_lock_checking::set(|| Ok(()));
    pmsignal_seams::register_postmaster_child_active::set(|| {});
    syncrep_seams::sync_rep_cleanup_at_proc_exit::set(|| {});
    condition_variable_seams::condition_variable_cancel_sleep::set(|| false);
    autovacuum_seams::wake_autovacuum_launcher::set(|| {});
    lock_seams::abort_strong_lock_acquire::set(|| {});
    lock_seams::get_awaited_lock_hashcode::set(|| None);
    lock_seams::lock_release_all::set(|_, _| lock::VirtualXactLockTableCleanup());
    lock_seams::lock_release::set(|_, _, _| Ok(true));
    timeout_seams::disable_timeouts::set(|_| {});
    // Real aio (the M4 crash_recovery.rs pattern): cold buffer reads —
    // the recover child's verifier walk on images where redo replayed
    // little — run the pgaio pipeline; stubbing pgaio_io_start_readv
    // leaves handles handed out and the read wait errors out.
    ipc_seams::before_shmem_exit::set(|_, _| Ok(()));
    aio_core::init_seams();
    guc_tables::vars::io_max_combine_limit.install_if_absent(guc_tables::GucVarAccessors {
        get: || 16,
        set: |_| {},
    });
    lock_seams::lock_acquire_extended::set(|_, _, _, _, _, _| {
        Ok(types_storage::lock::LOCKACQUIRE_OK)
    });

    timestamp_seams::get_current_timestamp::set(|| 777_000_000);
    trigger_seams::after_trigger_begin_xact::set(|| Ok(()));
    trigger_seams::after_trigger_end_xact::set(|_| Ok(()));
    trigger_seams::after_trigger_fire_deferred::set(|| Ok(()));
    // Notify hooks: the C-exact placements. PreCommit_Notify runs BEFORE the
    // commit record exists; AtCommit_Notify after the txn is durably
    // committed and visible. Mutation (c) swaps the delivery to the
    // pre-commit hook — the "signal at logical commit" bug class.
    if mutation() == Mutation::NotifyEarly {
        async_seams::pre_commit_notify::set(deliver_pending_notify);
        async_seams::at_commit_notify::set(|| Ok(()));
    } else {
        async_seams::pre_commit_notify::set(|| Ok(()));
        async_seams::at_commit_notify::set(deliver_pending_notify);
    }
    async_seams::at_abort_notify::set(|| {});
    tablecmds_seams::pre_commit_on_commit_actions::set(|| Ok(()));
    tablecmds_seams::at_eoxact_on_commit_actions::set(|_| {});
    spi_seams::at_eoxact_spi::set(|_| Ok(()));
    sinval_seams::receive_shared_invalid_messages::set(|_, _| Ok(()));
    spi_seams::spi_inside_nonatomic_context::set(|| false);
    be_fsstubs_seams::at_eoxact_large_object::set(|_| Ok(()));
    namespace_seams::at_eoxact_namespace::set(|_, _| {});
    catalog_index_seams::reset_reindex_state::set(|_| {});
    catalog_storage_seams::smgr_get_pending_deletes::set(|mcx, _for_commit| {
        Ok(PgVec::new_in(mcx))
    });
    catalog_storage_seams::smgr_do_pending_deletes::set(|_| Ok(()));
    catalog_storage_seams::smgr_do_pending_syncs::set(|_, _| Ok(()));
    catalog_storage_seams::rel_file_locator_skipping_wal::set(|_| false);
    combocid_seams::at_eoxact_combocid::set(|| {});
    combocid_seams::heap_tuple_header_adjust_cmax::set(|_hdr, cid| Ok((cid, false)));
    combocid_seams::heap_tuple_header_get_cmax::set(|hdr| hdr.raw_command_id());
    combocid_seams::heap_tuple_header_get_cmin::set(|hdr| hdr.raw_command_id());
    multixact_seams::at_eoxact_multixact::set(|| {});
    multixact_seams::multi_xact_id_set_oldest_member::set(|| Ok(()));
    multixact_seams::multi_xact_id_is_running::set(|_, _| Ok(false));
    pg_enum_seams::at_eoxact_enum::set(|| {});
    relcache_seams::at_eoxact_relation_cache::set(|_| Ok(()));
    relcache_seams::relation_cache_init_file_remove::set(|| {});
    typcache_seams::at_eoxact_type_cache::set(|| {});
    logical_seams::reset_logical_streaming_state::set(|| {});
    logical_worker_seams::at_eoxact_logical_rep_workers::set(|_| {});
    snapbuild_seams::snap_build_reset_exported_snapshot_state::set(|| {});
    parallel_seams::is_parallel_worker::set(|| false);
    parallel_seams::at_eoxact_parallel::set(|_| Ok(()));
    origin_seams::replorigin_session_origin::set(|| types_core::InvalidRepOriginId);
    origin_seams::replorigin_session_origin_lsn::set(|| 0);
    origin_seams::replorigin_session_origin_timestamp::set(|| 0);
    origin_seams::set_replorigin_session_origin_timestamp::set(|_| {});
    commit_ts_seams::transaction_tree_set_commit_ts_data::set(|_, _, _, _| Ok(()));
    commit_ts_seams::extend_commit_ts::set(|_| Ok(()));
    syncrep_seams::sync_rep_wait_for_lsn::set(|_, _| Ok(()));
    backend_status_seams::pgstat_report_xact_timestamp::set(|_| {});
    backend_status_seams::pgstat_report_query_id::set(|_, _| {});
    backend_status_seams::pgstat_report_plan_id::set(|_, _| {});
    backend_status_seams::pgstat_clear_backend_status_snapshot::set(|| {});
    backend_progress_seams::pgstat_progress_end_command::set(|| {});
    predicate_seams::pre_commit_check_for_serialization_failure::set(|| Ok(()));
    predicate_seams::release_predicate_locks::set(|_, _| Ok(()));
    predicate_seams::check_for_serializable_conflict_in::set(|_rel, _tid, _blk| Ok(()));
    predicate_seams::check_table_for_serializable_conflict_in::set(|_rel| Ok(()));
    predicate_seams::transfer_predicate_locks_to_heap_relation::set(|_rel| Ok(()));
    predicate_seams::predicate_lock_page_split::set(|_rel, _o, _n| Ok(()));
    predicate_seams::predicate_lock_page_combine::set(|_rel, _o, _n| Ok(()));
    predicate_seams::check_for_serializable_conflict_out_needed::set(|_r, _s| Ok(false));
    predicate_seams::register_predicate_locking_xid::set(|_| Ok(()));
    pruneheap_seams::heap_page_prune_opt::set(|_r, _b| Ok(()));
    freespace_seams::get_page_with_free_space::set(|_rel, _need| {
        Ok(types_core::InvalidBlockNumber)
    });
    freespace_seams::record_and_get_page_with_free_space::set(|_rel, _old, _avail, _need| {
        Ok(types_core::InvalidBlockNumber)
    });
    catalog_seams::is_catalog_relation::set(|_rel| false);
    aclchk_seams::object_aclcheck::set(|_classid, _objid, _roleid, _mode| Ok(0));
    lmgr_seams::check_relation_locked_by_me::set(|_, _, _| true);
    tablespace_seams::tablespace_create_dbspace::set(|_, _, _| Ok(()));
    dbcommands_seams::get_database_name::set(|_| Ok(Some("testdb".to_string())));
    syscache_seams::search_syscache_exists_databaseoid::set(|_| Ok(true));

    startup_seams::begin_startup_progress_phase::set(|| {});
    postgres_seams::check_for_interrupts::set(|| Ok(()));
    startup_seams::process_startup_proc_interrupts::set(|| Ok(()));

    walsummarizer_seams::wakeup_wal_summarizer::set(|| {});
    walsummarizer_seams::get_oldest_unsummarized_lsn::set(|| Ok(0));
    standby_seams::log_standby_snapshot::set(|| Ok(0));
    // Actor threads have no per-thread pendingOps table (InitSync is the
    // checkpointer's); data-file sync requests forward to a checkpointer
    // that never runs before the cut — absorbed, exactly the
    // crash-before-checkpoint world. WAL durability never rides these.
    checkpointer_seams::forward_sync_request::set(|_, _| Ok(true));
}

fn install_real() {
    shmem::init_seams();
    guc_tables::init_seams();
    guc::init_seams();
    adt_bool::init_seams();
    adt_float::init_seams();
    transam_xlog::init_seams();
    heapam_visibility::init_seams();
    if !pruneheap_seams::heap_page_prune_execute::is_installed() {
        pruneheap_seams::heap_page_prune_execute::set(pruneheap::heap_page_prune_execute);
    }
    clog::init_seams();
    subtrans::init_seams();
    transam::init_seams();
    varsup::init_seams();
    xact::init_seams();
    walsender_config::init_seams();
    twophase_config::init_seams();
    guc_tables::vars::max_locks_per_xact.install(guc_tables::GucVarAccessors {
        get: || 64,
        set: |_| {},
    });
    guc_tables::vars::WalWriterFlushAfter.install(guc_tables::GucVarAccessors {
        get: || 128,
        set: |_| {},
    });
    // The walwriter actor's XLogBackgroundFlush pacing input (the sweep
    // template has no walwriter; this world does).
    guc_tables::vars::WalWriterDelay.install(guc_tables::GucVarAccessors {
        get: || 200,
        set: |_| {},
    });
    snapmgr::init_seams();
    resowner::init_seams();
    procarray::init_seams();
    inval::init_seams();
    pgstat::init_seams();
    relpath::init_seams();
    smgr::init_seams();
    sync::init_seams();
    xloginsert::init_seams();
    xlogreader::init_seams();
    xlogutils::init_seams();
    xlogprefetcher::init_seams();
    xlogprefetcher::XLogPrefetchShmemInit();
    guc_tables::vars::maintenance_io_concurrency.install(guc_tables::GucVarAccessors {
        get: || 10,
        set: |_| {},
    });
    xlogrecovery::init_seams();
    timeline::init_seams();
    guc::store::initialize_guc_options().unwrap();
    // SIM HARNESS LAW (wasm-lane wal_sync_method trap): pin fdatasync so
    // every commit's durability is an explicit vfs op the fault model sees.
    transam_xlog::stamp_wal_sync_method(transam_xlog::WAL_SYNC_METHOD_FDATASYNC);
    // GUC bring-up stamped the C default io_method (worker) through the
    // aio_core accessor; this world has no IO workers — pin the sync
    // method back (the aio_core boot default) or every COLD buffer read
    // (the recover child's verifier on a redo-poor image) dies EBADF in a
    // worker-less pipeline.
    guc_tables::vars::io_method.write(guc_tables::consts::IOMETHOD_SYNC);

    fd::init_seams();
    fd::InitFileAccess();
    lwlock::CreateLWLocks(false).unwrap();
    lmgr_proc::init_seams();
    lmgr_proc::InitProcGlobal(&lmgr_proc::ProcGlobalConfig {
        autovacuum_worker_slots: 3,
        max_wal_senders: 2,
        max_prepared_xacts: 2,
        fastpath_lock_groups_per_backend: 1,
    });
    varsup::VarsupShmemInit();
    procarray::ProcArrayShmemInit();
    clog::CLOGShmemInit().unwrap();
    subtrans::SUBTRANSShmemInit().unwrap();
    bufmgr::BufferManagerShmemInit().unwrap();
    bufmgr::init_seams();
    aio_core::AioShmemSize().unwrap();
    aio_core::AioShmemInit().unwrap();
    sync::InitSync().unwrap();
    // NOTE: unlike the sweep template, the MAIN thread does NOT InitProcess
    // here — every actor (and only actors) claims a PGPROC on its own
    // thread; the main thread only orchestrates.

    if resowner::CurrentResourceOwner().is_null() {
        let owner =
            resowner::ResourceOwnerCreate(types_resowner::ResourceOwner::NULL, "flushsim-main")
                .unwrap();
        resowner::SetCurrentResourceOwner(owner);
    }
}

// ---------------------------------------------------------------------------
// World minting (sweep template, minted arm).
// ---------------------------------------------------------------------------

fn make_checkpoint() -> controldata_utils::CheckPoint {
    let mut ckpt = controldata_utils::CheckPoint::ZEROED;
    ckpt.redo = CKPT_LOC;
    ckpt.ThisTimeLineID = 1;
    ckpt.PrevTimeLineID = 1;
    ckpt.fullPageWrites = true;
    ckpt.wal_level = WAL_LEVEL_REPLICA;
    ckpt.nextXid = types_core::FullTransactionId::from_epoch_and_xid(0, BASE_XID);
    ckpt.oldestXid = BASE_XID;
    ckpt.oldestXidDB = 5;
    ckpt
}

fn mint_control_file(ckpt: &controldata_utils::CheckPoint) {
    let mut cf = controldata_utils::ControlFileData::ZEROED;
    cf.system_identifier = SYS_ID;
    cf.pg_control_version = PG_CONTROL_VERSION;
    cf.catalog_version_no = controldata_utils::CATALOG_VERSION_NO;
    cf.state = DB_IN_PRODUCTION;
    cf.checkPoint = CKPT_LOC;
    cf.checkPointCopy = *ckpt;
    cf.unloggedLSN = FirstNormalUnloggedLSN;
    cf.maxAlign = 8;
    cf.floatFormat = FLOATFORMAT_VALUE;
    cf.blcksz = 8192;
    cf.relseg_size = 131072;
    cf.xlog_blcksz = 8192;
    cf.xlog_seg_size = SEG as u32;
    cf.nameDataLen = 64;
    cf.indexMaxKeys = 32;
    cf.toast_max_chunk_size = TOAST_MAX_CHUNK_SIZE;
    cf.loblksize = 2048;
    cf.float8ByVal = true;
    cf.crc = controldata_utils::crc_of_image(&cf.to_disk_bytes());
    let mut image = vec![0u8; PG_CONTROL_FILE_SIZE];
    image[..controldata_utils::SIZEOF_CONTROL_FILE_DATA].copy_from_slice(&cf.to_disk_bytes());
    vfs_write_file("/global/pg_control", &image);
}

fn mint_wal_segment(ckpt: &controldata_utils::CheckPoint) {
    let segno = CKPT_LOC / SEG as u64;
    let page_addr = CKPT_LOC - CKPT_LOC % 8192;
    let mut seg = vec![0u8; SEG as usize];
    seg[0..2].copy_from_slice(&0xD118u16.to_ne_bytes());
    seg[2..4].copy_from_slice(&XLP_LONG_HEADER.to_ne_bytes());
    seg[4..8].copy_from_slice(&1u32.to_ne_bytes());
    seg[8..16].copy_from_slice(&page_addr.to_ne_bytes());
    seg[24..32].copy_from_slice(&SYS_ID.to_ne_bytes());
    seg[32..36].copy_from_slice(&(SEG as u32).to_ne_bytes());
    seg[36..40].copy_from_slice(&8192u32.to_ne_bytes());

    let mut rec = vec![0u8; CKPT_TOT_LEN];
    rec[0..4].copy_from_slice(&(CKPT_TOT_LEN as u32).to_ne_bytes());
    rec[8..16].copy_from_slice(&(CKPT_LOC - 0x28).to_ne_bytes());
    rec[16] = XLOG_CHECKPOINT_SHUTDOWN;
    rec[17] = RM_XLOG_ID;
    rec[24] = 255; // XLR_BLOCK_ID_DATA_SHORT
    rec[25] = controldata_utils::SIZEOF_CHECKPOINT as u8;
    rec[26..26 + controldata_utils::SIZEOF_CHECKPOINT].copy_from_slice(&ckpt.to_bytes());
    let crc = crc32c::fin_crc32c(crc32c::pg_comp_crc32c(
        crc32c::pg_comp_crc32c(crc32c::CRC32C_INIT, &rec[SizeOfXLogRecord..]),
        &rec[..20],
    ));
    rec[20..24].copy_from_slice(&crc.to_ne_bytes());

    let off = (CKPT_LOC % SEG as u64) as usize;
    seg[off..off + rec.len()].copy_from_slice(&rec);
    let name = transam_xlog::XLogFileName(1, segno, SEG);
    vfs_write_file(&format!("/pg_wal/{name}"), &seg);
}

fn mint_and_boot_writer() {
    for d in [
        "/global",
        "/pg_wal",
        "/pg_wal/archive_status",
        "/pg_wal/summaries",
        "/pg_xact",
        "/pg_subtrans",
        "/base/5",
        "/pg_tblspc",
    ] {
        vfs_mkdir_p(d);
    }
    init_small::globals::SetDataDir("/");
    init_small::globals::set_enableFsync(true);

    install_stub_seams();
    install_real();

    let ckpt = make_checkpoint();
    mint_control_file(&ckpt);
    mint_wal_segment(&ckpt);
    clog::BootStrapCLOG().unwrap();
    subtrans::BootStrapSUBTRANS().unwrap();

    transam_xlog::ReadControlFile().unwrap();
    transam_xlog::XLOGShmemInit();

    let end_of_log: XLogRecPtr = CKPT_LOC + MAXALIGN(CKPT_TOT_LEN) as u64;
    let ctl = transam_xlog::ctl::XLogCtl();
    ctl.InsertTimeLineID.store(1, Relaxed);
    ctl.PrevTimeLineID.store(1, Relaxed);
    ctl.Insert.CurrBytePos.store(XLogRecPtrToBytePos(end_of_log), Relaxed);
    ctl.Insert.PrevBytePos.store(XLogRecPtrToBytePos(CKPT_LOC), Relaxed);
    ctl.Insert.fullPageWrites.store(true, Relaxed);
    ctl.Insert.RedoRecPtr.store(CKPT_LOC, Relaxed);
    ctl.RedoRecPtr.store(CKPT_LOC, Relaxed);
    ctl.InitializedUpTo.store(end_of_log, Relaxed);
    ctl.logInsertResult.store(end_of_log, Relaxed);
    ctl.logWriteResult.store(end_of_log, Relaxed);
    ctl.logFlushResult.store(end_of_log, Relaxed);
    ctl.LogwrtRqstWrite.store(end_of_log, Relaxed);
    ctl.LogwrtRqstFlush.store(end_of_log, Relaxed);
    ctl.SharedRecoveryState.store(transam_xlog::RECOVERY_STATE_DONE, Relaxed);
    ctl.InstallXLogFileSegmentActive.store(true, Relaxed);
    {
        let page_begin = end_of_log - end_of_log % 8192;
        let idx = transam_xlog::ctl::XLogRecPtrToBufIdx(end_of_log) as usize;
        let name = transam_xlog::XLogFileName(1, CKPT_LOC / SEG as u64, SEG);
        let off = (page_begin % SEG as u64) as i64;
        let len = (end_of_log - page_begin) as usize;
        let tail = vfs_read_range(&format!("/pg_wal/{name}"), off, len);
        assert_eq!(tail.len(), len);
        let dst = ctl.page_ptr(idx);
        // SAFETY: pre-actor single-threaded boot; ctl page buffers are
        // XLOG_BLCKSZ.
        unsafe {
            core::ptr::copy_nonoverlapping(tail.as_ptr(), dst, len);
            core::ptr::write_bytes(dst.add(len), 0, 8192 - len);
        }
        ctl.xlblocks[idx].store(page_begin + 8192, std::sync::atomic::Ordering::Release);
        ctl.InitializedUpTo.store(page_begin + 8192, Relaxed);
    }
    xlogutils::set_in_recovery(false);
    procarray::TransamVariables()
        .nextXid
        .store(types_core::FullTransactionId::from_epoch_and_xid(0, BASE_XID).value, Relaxed);
    varsup::SetTransactionIdLimit(BASE_XID, 5).unwrap();
    subtrans::StartupSUBTRANS(BASE_XID).unwrap();
    assert!(transam_xlog::XLogInsertAllowed());

    smgr::smgropen(RLOC, INVALID_PROC_NUMBER).unwrap();
    smgr::smgrcreate(
        types_storage::RelFileLocatorBackend { locator: RLOC, backend: INVALID_PROC_NUMBER },
        ForkNumber::MAIN_FORKNUM,
        false,
    )
    .unwrap();

    sim_fsync_tree();
}

// ---------------------------------------------------------------------------
// Relation / tupdesc scaffolding (sweep template verbatim).
// ---------------------------------------------------------------------------

fn int4_tupdesc<'mcx>(mcx: Mcx<'mcx>) -> Rc<TupleDescData<'mcx>> {
    let att = FormData_pg_attribute {
        attnum: 1,
        atttypid: types_core::INT4OID,
        attlen: 4,
        attbyval: true,
        attalign: types_tuple::TYPALIGN_INT,
        attstorage: types_tuple::TYPSTORAGE_PLAIN,
        ..Default::default()
    };
    let mut attrs = PgVec::new_in(mcx);
    let mut compact = PgVec::new_in(mcx);
    compact.push(CompactAttribute::populate_from(&att));
    attrs.push(att);
    Rc::new(TupleDescData {
        natts: 1,
        tdtypeid: 0,
        tdtypmod: -1,
        tdrefcount: -1,
        constr: None,
        compact_attrs: compact,
        attrs,
    })
}

fn test_relation<'mcx>(mcx: Mcx<'mcx>, oid: Oid) -> RelationData<'mcx> {
    let mut relname = NameData::default();
    relname.namestrcpy("t");
    let rd_rel = FormData_pg_class {
        relname,
        relnamespace: 2200,
        reltype: 0,
        relowner: 10,
        relam: tableam_vocab::HEAP_TABLE_AM_OID,
        relfilenode: oid,
        reltablespace: 0,
        relpages: 0,
        reltuples: -1.0,
        relallvisible: 0,
        reltoastrelid: 0,
        relhasindex: false,
        relisshared: false,
        relpersistence: RELPERSISTENCE_PERMANENT,
        relkind: types_rel::RELKIND_RELATION,
        relhassubclass: false,
        relrowsecurity: false,
        relispopulated: true,
        relreplident: b'd',
        relispartition: false,
        relfrozenxid: BASE_XID,
        relminmxid: 1,
    };
    RelationData {
        rd_locator: Default::default(),
        rd_smgr: Default::default(),
        rd_id: oid,
        rd_backend: INVALID_PROC_NUMBER,
        rd_islocaltemp: false,
        rd_isvalid: Cell::new(true),
        rd_createSubid: Cell::new(0),
        rd_newRelfilelocatorSubid: Cell::new(0),
        rd_firstRelfilelocatorSubid: Cell::new(0),
        rd_droppedSubid: Cell::new(0),
        rd_lockInfo: LockInfoData { lockRelId: LockRelId { relId: oid, dbId: 5 } },
        rd_rel,
        rd_att: int4_tupdesc(mcx),
        rd_index: None,
        rd_opcintype: PgVec::new_in(mcx),
        rd_opfamily: PgVec::new_in(mcx),
        rd_indoption: PgVec::new_in(mcx),
        rd_indcollation: PgVec::new_in(mcx),
        rd_options: None,
        pgstat_enabled: Cell::new(false),
        pgstat_link: core::cell::Cell::new((0, core::ptr::null_mut())),
        rd_amcache: Default::default(),
        rd_amcache_hash: Default::default(),
        rd_amcache_gin: Default::default(),
        rd_amcache_spgist: Default::default(),
        rd_support: PgVec::new_in(mcx),
        rd_supportinfo: Default::default(),
        rd_opcoptions: Default::default(),
        rd_indexlist: Default::default(),
        rd_trigdesc: Default::default(),
        rd_hastriggers: false,
        rd_hasrules: false,
    }
}

fn noop_close(_oid: Oid, _mode: LOCKMODE) -> PgResult<()> {
    Ok(())
}

// ---------------------------------------------------------------------------
// Actor scaffolding.
// ---------------------------------------------------------------------------

static COMMITTERS_DONE: AtomicU32 = AtomicU32::new(0);
static WW_PROCNO: AtomicI32 = AtomicI32::new(-1);
static WW_READY: AtomicBool = AtomicBool::new(false);
static STOP_WW: AtomicBool = AtomicBool::new(false);
/// Max per-commit virtual duration observed (ms) — the L3 witness.
static MAX_COMMIT_MS: AtomicU64 = AtomicU64::new(0);

/// Per-actor-thread backend world entry: local latch -> PGPROC (InitProcess
/// owns the proc latch and, through the REAL miscinit seam, points MyLatch
/// at it) -> proc array membership -> resowner.
fn enter_backend_world(actor: u32) {
    use init_small::globals as g;
    g::SetMyProcPid(1000 + actor as i32);
    g::SetMyDatabaseId(5);
    // The VFD cache is per-thread (each backend owns its file handles).
    fd::InitFileAccess();
    // SIM HARNESS LAW, per-THREAD form (the sweep template stamps this on
    // its single thread; the GUC backing cells are per-thread): every actor
    // must pin wal_sync_method=fdatasync itself, or it inherits the
    // platform default — open_datasync folds durability into an O_DSYNC
    // open flag SimVfs does not model, and every "flush" silently syncs
    // nothing (found the hard way: 18/18 sync acks lost at the first
    // quiescence cut).
    transam_xlog::stamp_wal_sync_method(transam_xlog::WAL_SYNC_METHOD_FDATASYNC);
    miscinit::InitProcessLocalLatch().expect("local latch");
    lmgr_proc::InitProcess(BackendType::Backend).unwrap();
    aio_core::pgaio_init_backend().expect("pgaio_init_backend");
    procarray::ProcArrayAdd(lmgr_proc::MyProc().unwrap()).unwrap();
    let owner = resowner::ResourceOwnerCreate(
        types_resowner::ResourceOwner::NULL,
        "flushsim-actor",
    )
    .unwrap();
    resowner::SetCurrentResourceOwner(owner);
    // Consume the deliberate SwitchToSharedLatch startup set (the real
    // session loop's WaitLatch would have). Without this every actor's
    // FIRST pipelined wait wakes instantly on the stale set and — per
    // FINDING-1 (wait_for_flush discards the WaitLatch result, so ANY
    // uncovered non-timeout wake re-arms as a leader) — degrades to the
    // incumbent path at zero cadence.
    if let Some(l) = init_small::globals::MyLatch() {
        latch::ResetLatch(l);
    }
}

fn value_of(actor: u32, j: u32) -> i32 {
    (actor as i32) * 100_000 + j as i32
}

/// One committed transaction on the CALLING actor thread, through the full
/// product path (StartTransactionCommand -> heap_insert xN ->
/// CommitTransactionCommand, which is where the commit record, the flush
/// wait — pipelined or incumbent —, clog, visibility and the notify hook
/// all live).
fn run_one_commit<'m>(
    mcx: Mcx<'m>,
    rel: &RelationData<'m>,
    tupdesc: &Rc<TupleDescData<'m>>,
    actor: u32,
    j: u32,
    claims_sync: bool,
) -> PgResult<()> {
    xact::StartTransactionCommand()?;
    let mut xid: TransactionId = 0;
    for r in 0..ROWS_PER_TXN {
        let mut tup = heaptuple::heap_form_tuple(
            mcx,
            tupdesc,
            &[datum::Datum::from_i32(value_of(actor, j))],
            &[false],
        )?;
        heapam::heap_insert(rel, tup.as_tuple_mut(), 0, 0, None)?;
        if r == 0 {
            xid = xact::GetTopTransactionId()?;
            ledger_append(Event {
                kind: EvKind::Attempt,
                cut: 0,
                actor,
                j,
                xid,
                lsn: 0,
                flushed: 0,
                // The CLAIM: async txns' visibility/notify anomalies are
                // C-sanctioned (synchronous_commit=off), so the recovery
                // invariants bind on sync claims only.
                sync: claims_sync,
                observed: Vec::new(),
            });
        }
    }
    CURRENT_COMMIT.with(|c| c.set(Some((actor, j, xid, claims_sync))));
    let commit_started = waiter::now_ms();
    let res = xact::CommitTransactionCommand();
    let commit_ms = (waiter::now_ms() - commit_started).max(0) as u64;
    MAX_COMMIT_MS.fetch_max(commit_ms, Relaxed);
    CURRENT_COMMIT.with(|c| c.set(None));
    res?;

    // XactLastRecEnd resets inside the commit tail (C-exact); the PRESERVED
    // commit-record end is XactLastCommitEnd (xact_last_commit_end seam).
    let lsn = transam_xlog_seams::xact_last_commit_end::call();
    ledger_append(Event {
        kind: EvKind::Ack,
        cut: 0,
        actor,
        j,
        xid,
        lsn,
        flushed: 0,
        sync: claims_sync,
        observed: Vec::new(),
    });
    Ok(())
}

fn committer_body(actor: u32, txns: u32, profile: &str, claims_sync_posture: bool) {
    enter_backend_world(actor);
    let claims_sync = claims_sync_posture || (mutation() == Mutation::AckEarly && actor == 0);
    if !claims_sync_posture {
        // Honest async committer (mixed posture axis): acks carry sync=false
        // and are exempt from I1a/L1 — C async-commit semantics.
        xact::SetSynchronousCommit(0);
    }
    if mutation() == Mutation::AckEarly && actor == 0 {
        // (a): the server-side lie — async execution, sync-claiming acks.
        xact::SetSynchronousCommit(0);
    }
    if mutation() == Mutation::WakeDrop && actor == 0 {
        // (b): park every wait on the LOCAL latch while queue registrations
        // (and their directed completion wakes) name the PROC latch.
        miscinit::SwitchBackToLocalLatch();
    }
    // Wait for the walwriter to be registered (profile != absent), so the
    // pipeline arming guard sees a live flusher from the first commit.
    if profile != "absent" {
        while !WW_READY.load(Relaxed) && SimVfs::cut_count() == 0 {
            waiter::sleep(core::time::Duration::from_millis(1));
        }
    }

    let ctx = MemoryContext::new("flushsim-committer");
    let mcx = ctx.mcx();
    let rel = test_relation(mcx, REL_OID);
    let tupdesc = int4_tupdesc(mcx);

    for j in 1..=txns {
        if SimVfs::cut_count() > 0 {
            break;
        }
        // (d): one-shot overclaim of the shared flush-result word before
        // txn 3 — the flush publication lies upward; every in-band reader
        // of "flushed" (including L1) sees the same lie.
        if mutation() == Mutation::WrongLsn && actor == 0 && j == 3 {
            let ctl = transam_xlog::ctl::XLogCtl();
            let cur = ctl.logFlushResult.load(Relaxed);
            ctl.logWriteResult.fetch_max(cur + 4096, Relaxed);
            ctl.logFlushResult.fetch_max(cur + 4096, Relaxed);
        }
        let r = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            run_one_commit(mcx, &rel, &tupdesc, actor, j, claims_sync)
        }));
        match r {
            Ok(Ok(())) => {}
            Ok(Err(_)) | Err(_) => {
                actor_abort_cleanup();
                break; // cut/engine stop: power is gone
            }
        }
        if SimVfs::cut_count() > 0 {
            break;
        }
    }
    COMMITTERS_DONE.fetch_add(1, Relaxed);
}

/// Read-only session: repeated product-snapshot scans of the committers'
/// table; each completed read appends its observed transaction set (the
/// CommandComplete moment) and live-checks snapshot atomicity.
fn reader_body(actor: u32, n_committers: u32) {
    enter_backend_world(actor);
    let ctx = MemoryContext::new("flushsim-reader");
    let mcx = ctx.mcx();
    let rel = test_relation(mcx, REL_OID);

    loop {
        if SimVfs::cut_count() > 0 {
            break;
        }
        let done = COMMITTERS_DONE.load(Relaxed) >= n_committers;
        let r = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| -> PgResult<()> {
            xact::StartTransactionCommand()?;
            let snap = snapmgr::GetTransactionSnapshot()?;
            let reg = snapmgr::RegisterSnapshot(Some(&snap))?.expect("registered snapshot");
            let observed = scan_observed(&rel, &reg)?;
            snapmgr::UnregisterSnapshot(Some(&reg));
            xact::CommitTransactionCommand()?;
            ledger_append(Event {
                kind: EvKind::ReadDone,
                cut: 0,
                actor,
                j: 0,
                xid: 0,
                lsn: 0,
                flushed: 0,
                sync: true,
                observed,
            });
            Ok(())
        }));
        match r {
            Ok(Ok(())) => {}
            Ok(Err(_)) | Err(_) => {
                actor_abort_cleanup();
                break;
            }
        }
        if done {
            break;
        }
        waiter::sleep(core::time::Duration::from_millis(2));
    }
}

/// Snapshot fold: every visible row's value, grouped; asserts per-txn
/// all-or-nothing (I3's live form) and returns the observed (actor, j) set.
fn scan_observed<'m>(
    rel: &RelationData<'m>,
    snap: &SnapshotData<'m>,
) -> PgResult<Vec<(u32, u32)>> {
    let key = types_storage::RelFileLocatorBackend { locator: RLOC, backend: INVALID_PROC_NUMBER };
    smgr::smgropen(RLOC, INVALID_PROC_NUMBER)?;
    let nblocks = smgr::smgrnblocks(key, ForkNumber::MAIN_FORKNUM)?;
    let mut counts: std::collections::BTreeMap<i32, u32> = std::collections::BTreeMap::new();
    for b in 0..nblocks {
        let buf = bufmgr::ReadBuffer(rel, b)?;
        let page_addr = bufmgr::BufferGetPagePtr(buf).as_ptr();
        // SAFETY: pinned page image.
        let page = unsafe {
            types_storage::bufpage::PageRef::from_raw(core::ptr::NonNull::new(page_addr).unwrap())
        };
        for off in 1..=page.max_offset_number() {
            let id = page.item_id(off);
            if !id.is_normal() {
                continue;
            }
            let mut t = page_tuple(page_addr, off);
            let vis =
                heapam_visibility_seams::heap_tuple_satisfies_visibility::call(&mut t, snap, buf)?;
            if vis {
                let (ptr, _len) = page.item_raw(id);
                // SAFETY: heap tuple in-page: t_hoff byte at offset 22, int4
                // datum right after the (aligned) header.
                let val = unsafe {
                    let hoff = *ptr.add(22) as usize;
                    ptr.add(hoff).cast::<i32>().read_unaligned()
                };
                *counts.entry(val).or_insert(0) += 1;
            }
        }
        bufmgr::ReleaseBuffer(buf)?;
    }
    let mut observed = Vec::new();
    for (val, n) in counts {
        if n != ROWS_PER_TXN {
            violation(format!(
                "I3-live snapshot atomicity: value {val} visible {n} times (want {ROWS_PER_TXN})"
            ));
        }
        observed.push(((val / 100_000) as u32, (val % 100_000) as u32));
    }
    Ok(observed)
}

fn page_tuple(page_addr: *mut u8, off: u16) -> HeapTupleData<'static> {
    // SAFETY: pinned buffer page, held across the visibility check.
    let page = unsafe {
        types_storage::bufpage::PageRef::from_raw(core::ptr::NonNull::new(page_addr).unwrap())
    };
    let id = page.item_id(off);
    let (ptr, len) = page.item_raw(id);
    // SAFETY: in-page image under the caller's pin.
    unsafe { HeapTupleData::from_raw_parts(ptr, len, ItemPointerData::new(0, off), REL_OID) }
}

/// Listener session: drains the notify mailbox (fed by the product's
/// at_commit_notify / pre_commit_notify hook, per mutation) and appends a
/// NotifyDelivered ledger event per message — the "listener connection got
/// signaled" moment.
fn listener_body(actor: u32, n_committers: u32) {
    enter_backend_world(actor);
    loop {
        if SimVfs::cut_count() > 0 {
            break;
        }
        let msgs: Vec<(u32, u32, TransactionId, bool)> =
            NOTIFY_BOX_G.lock().unwrap_or_else(|e| e.into_inner()).drain(..).collect();
        for (from_actor, j, xid, sync) in msgs {
            ledger_append(Event {
                kind: EvKind::NotifyDelivered,
                cut: 0,
                actor,
                j,
                xid,
                lsn: 0,
                flushed: 0,
                sync,
                observed: vec![(from_actor, j)],
            });
        }
        if COMMITTERS_DONE.load(Relaxed) >= n_committers
            && NOTIFY_BOX_G.lock().unwrap_or_else(|e| e.into_inner()).is_empty()
        {
            break;
        }
        waiter::sleep(core::time::Duration::from_millis(1));
    }
}

/// Walwriter actor: claims a PGPROC, publishes itself as walwriterProc (the
/// arming guard's liveness requirement) and drives the PRODUCT's
/// XLogBackgroundFlush on a profile-controlled cadence. Profiles:
///   normal   5 ms cadence
///   hyper    1 ms cadence
///   stalled  400 ms cadence (long flush-formation windows)
///   dead50   normal cadence, silently exits after 50 cycles WITHOUT
///            clearing walwriterProc (the flusher-death window — the
///            liveness-fix backstop/Retry surface)
///   absent   never started (arming guard refuses; incumbent-path control)
fn walwriter_body(actor: u32, profile: String) {
    enter_backend_world(actor);
    lmgr_proc::ProcGlobal()
        .walwriterProc
        .store(init_small::globals::MyProcNumber(), Relaxed);
    WW_PROCNO.store(init_small::globals::MyProcNumber(), Relaxed);
    WW_READY.store(true, Relaxed);

    let cadence_ms: i64 = match profile.as_str() {
        "hyper" => 1,
        "stalled" => 400,
        _ => 5,
    };
    let dead_after: Option<u32> = match profile.as_str() {
        "dead50" => Some(50),
        _ => None,
    };

    let mut pacing = transam_xlog::WalFlushPacing::default();
    let mut cycles = 0u32;
    loop {
        if STOP_WW.load(Relaxed) || SimVfs::cut_count() > 0 {
            break;
        }
        if let Some(d) = dead_after {
            if cycles >= d {
                // Silent flusher death: walwriterProc stays stale.
                break;
            }
        }
        cycles += 1;
        let my_latch = init_small::globals::MyLatch();
        let _ = latch::WaitLatch(
            my_latch,
            types_storage::waiteventset::WL_LATCH_SET | types_storage::waiteventset::WL_TIMEOUT,
            cadence_ms,
            0,
        );
        if let Some(l) = my_latch {
            latch::ResetLatch(l);
        }
        let r = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            transam_xlog::XLogBackgroundFlush(&mut pacing)
        }));
        match r {
            Ok(Ok(_)) => {}
            Ok(Err(_)) | Err(_) => {
                // Cut (or engine stop) mid-flush: the product would PANIC
                // the whole process; here only this actor dies — release
                // like the product's error recovery so peers can reach
                // their own cut checks.
                actor_abort_cleanup();
                break;
            }
        }
    }
}

fn actor_abort_cleanup() {
    // The product's per-thread error-recovery floor (walwriter
    // abort_cleanup / backend AbortTransaction do this before anything
    // else): a dead-by-unwind actor must not take its LWLocks or buffer
    // pins to the grave — in the real server a PANIC kills the PROCESS, so
    // post-crash lock residue cannot wedge peers; the sim writer keeps
    // running to pack the image, so it must clean up like the product's
    // error path would.
    init_small::globals::SetInterruptHoldoffCount(0);
    init_small::globals::SetCritSectionCount(0);
    let _ = lwlock::LWLockReleaseAll();
    bufmgr::UnlockBuffers();
}

/// Flush-hold disturbor (see HOLD_ENV): pins WALWriteLock across a virtual
/// sleep so concurrent committers lose the conditional acquire and REGISTER
/// (pipelined arm) or join the convoy (incumbent arm).
fn disturbor_body(actor: u32, hold_ms: u64, gap_ms: u64, n_committers: u32) {
    enter_backend_world(actor);
    let lock = transam_xlog::ctl::WALWriteLock();
    loop {
        if SimVfs::cut_count() > 0 || COMMITTERS_DONE.load(Relaxed) >= n_committers {
            break;
        }
        let r = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| -> PgResult<()> {
            lwlock::LWLockAcquire(
                lock,
                lwlock::LW_EXCLUSIVE,
                init_small::globals::MyProcNumber(),
            )?;
            waiter::sleep(core::time::Duration::from_millis(hold_ms));
            lwlock::LWLockRelease(lock)?;
            Ok(())
        }));
        if r.is_err() || matches!(r, Ok(Err(_))) {
            actor_abort_cleanup();
            break;
        }
        waiter::sleep(core::time::Duration::from_millis(gap_ms));
    }
}

// ---------------------------------------------------------------------------
// WRITER CHILD
// ---------------------------------------------------------------------------

#[test]
#[ignore]
fn flushsim_writer_child() {
    if std::env::var(ROLE_ENV).as_deref() != Ok("writer") {
        return;
    }
    let pack = std::path::PathBuf::from(std::env::var(PACK_ENV).unwrap());
    let k: u64 = env_u32(K_ENV, 0) as u64;
    let profile = std::env::var(PROFILE_ENV).unwrap_or_else(|_| "normal".into());
    let trigger = std::env::var(TRIGGER_ENV).unwrap_or_else(|_| "none".into());
    let nc = env_u32(NC_ENV, 3);
    let nr = env_u32(NR_ENV, 1);
    let nl = env_u32(NL_ENV, 1);
    let txns = env_u32(TXNS_ENV, 6);
    let mix = std::env::var(MIX_ENV).unwrap_or_else(|_| "sync".into());
    // FLIP (Michael-ruled): parse mirrors production's inverted default
    // (unset => armed); the harness always spells the posture explicitly
    // anyway (writer_envs).
    let pipe_on = !matches!(
        std::env::var("PGRUST_FLUSH_PIPELINE").ok().as_deref(),
        Some("0") | Some("off")
    );

    // Parse the trigger ("ack:3" / "read:2" / "notify:1" / "end" / "none").
    {
        let (kind, n) = match trigger.split_once(':') {
            Some((kk, nn)) => (kk.to_string(), nn.parse::<u32>().unwrap_or(1)),
            None => (trigger.clone(), 0),
        };
        let _ = TRIGGER_KIND.set(kind);
        TRIGGER_N.store(n, Relaxed);
    }

    // One process-shared sim universe: every actor thread sees one disk.
    SimVfs::install_process_universe();
    // The permit scheduler is the schedule authority; register the main
    // thread at the door (no-op when PGRUST_SIM_SCHED is off).
    let _door = pgsync::sim::spawn_door::register_self("flushsim-main");

    mint_and_boot_writer();

    // Whole-node kill on cut: the packed image is the pure at-cut image.
    SimVfs::set_kill_on_cut(true);
    if k > 0 {
        SeededFaultPlan::install(
            SYS_ID ^ k.wrapping_mul(0x9E37_79B9_7F4A_7C15),
            vec![FaultRule::crash_at_op(k)],
        );
    }
    let ops_at_start = SimVfs::op_seq();

    // Spawn the actor set. Actor ids double as proc-pid salt.
    let mut handles = Vec::new();
    if profile != "absent" {
        let p = profile.clone();
        handles.push(
            pgsync::thread::Builder::new()
                .name("flushsim-walwriter".into())
                .spawn(move || walwriter_body(100, p))
                .unwrap(),
        );
    }
    if let Ok(hold) = std::env::var(HOLD_ENV) {
        if let Some((h, g)) = hold.split_once(':') {
            let (h, g): (u64, u64) = (h.parse().unwrap(), g.parse().unwrap());
            handles.push(
                pgsync::thread::Builder::new()
                    .name("flushsim-disturbor".into())
                    .spawn(move || disturbor_body(400, h, g, nc))
                    .unwrap(),
            );
        }
    }
    for c in 0..nc {
        let prof = profile.clone();
        let claims_sync = match mix.as_str() {
            "mixed" => c % 2 == 0,
            _ => true,
        };
        handles.push(
            pgsync::thread::Builder::new()
                .name(format!("flushsim-committer-{c}"))
                .spawn(move || committer_body(c, txns, &prof, claims_sync))
                .unwrap(),
        );
    }
    for r in 0..nr {
        handles.push(
            pgsync::thread::Builder::new()
                .name(format!("flushsim-reader-{r}"))
                .spawn(move || reader_body(200 + r, nc))
                .unwrap(),
        );
    }
    for l in 0..nl {
        handles.push(
            pgsync::thread::Builder::new()
                .name(format!("flushsim-listener-{l}"))
                .spawn(move || listener_body(300 + l, nc))
                .unwrap(),
        );
    }

    // Stop the walwriter once the committers are done (or a cut fired).
    loop {
        if COMMITTERS_DONE.load(Relaxed) >= nc || SimVfs::cut_count() > 0 {
            break;
        }
        waiter::sleep(core::time::Duration::from_millis(5));
    }
    STOP_WW.store(true, Relaxed);
    let ww = WW_PROCNO.load(Relaxed);
    if ww >= 0 {
        latch::SetLatch(types_storage::latch::LatchHandle::proc(ww));
    }
    for h in handles {
        let _ = h.join();
    }

    // Quiescence cut ("end" trigger): power loss at a fully-acked state —
    // every sync-acked txn must survive, exactly.
    if trigger_kind() == "end" && SimVfs::cut_count() == 0 {
        SimVfs::cut();
    }

    let stats = &transam_xlog::flushpipe::STATS;
    let (reg, selfs, comp, leader, backstop, retry) = (
        stats.registered.load(Relaxed),
        stats.self_served.load(Relaxed),
        stats.completed.load(Relaxed),
        stats.leader_flushes.load(Relaxed),
        stats.backstop_self_served.load(Relaxed),
        stats.retry_rearm.load(Relaxed),
    );
    // L4 conservation: every registration resolves exactly once.
    if reg != comp + selfs + backstop + retry {
        violation(format!(
            "L4 conservation: registered {reg} != completed {comp} + self {selfs} + backstop {backstop} + retry {retry}"
        ));
    }
    // Knob-OFF structural inertness.
    if !pipe_on && (reg + selfs + comp + leader + backstop + retry) != 0 {
        violation(format!(
            "L4 knob-off: pipeline counters nonzero (reg={reg} self={selfs} comp={comp} leader={leader} backstop={backstop} retry={retry})"
        ));
    }

    let acked_pre_cut = LEDGER_G
        .lock()
        .unwrap()
        .iter()
        .filter(|e| matches!(e.kind, EvKind::Ack) && e.cut == 0)
        .count();

    // Pack image + meta + ledger.
    let _ = std::fs::remove_dir_all(&pack);
    std::fs::create_dir_all(pack.join("root")).unwrap();
    export_sim_tree(&pack.join("root"));
    let violations = VIOLATIONS_G.lock().unwrap_or_else(|e| e.into_inner()).clone();
    let mut meta = String::new();
    meta.push_str(&format!("k={k}\n"));
    meta.push_str(&format!("profile={profile}\n"));
    meta.push_str(&format!("trigger={trigger}\n"));
    meta.push_str(&format!("mutation={:?}\n", mutation()));
    meta.push_str(&format!("pipe={}\n", pipe_on as u32));
    meta.push_str(&format!("mix={mix}\n"));
    meta.push_str(&format!("nc={nc}\nnr={nr}\nnl={nl}\ntxns={txns}\n"));
    meta.push_str(&format!("base_xid={BASE_XID}\n"));
    meta.push_str(&format!("acked={acked_pre_cut}\n"));
    meta.push_str(&format!("ops={}\n", SimVfs::op_seq() - ops_at_start));
    meta.push_str(&format!("cuts={}\n", SimVfs::cut_count()));
    meta.push_str(&format!("killed={}\n", SimVfs::killed()));
    meta.push_str(&format!(
        "stats=reg:{reg},self:{selfs},comp:{comp},leader:{leader},backstop:{backstop},retry:{retry}\n"
    ));
    meta.push_str(&format!("max_commit_ms={}\n", MAX_COMMIT_MS.load(Relaxed)));
    meta.push_str(&format!("tree_digest={:#010x}\n", durable_tree_digest()));
    for v in &violations {
        meta.push_str(&format!("violation={v}\n"));
    }
    std::fs::write(pack.join("meta.txt"), &meta).unwrap();
    std::fs::write(pack.join("ledger.txt"), ledger_dump()).unwrap();
    println!(
        "FLUSHSIM_WRITER_DONE acked={acked_pre_cut} cuts={} reg={reg} comp={comp} self={selfs} leader={leader} backstop={backstop} retry={retry} violations={}",
        SimVfs::cut_count(),
        violations.len()
    );
}

// ---------------------------------------------------------------------------
// RECOVER CHILD: import the crash image, boot PRODUCT recovery, verify the
// ledger invariants I1a/I1b/I1c/I2/I3.
// ---------------------------------------------------------------------------

fn mvcc_snapshot<'m>(mcx: Mcx<'m>) -> SnapshotData<'m> {
    let mut s = SnapshotData::sentinel(mcx, SnapshotType::SNAPSHOT_MVCC);
    s.xmin = BASE_XID + 100_000;
    s.xmax = BASE_XID + 100_000;
    s.regd_count.set(1);
    s
}

#[derive(Debug, Clone)]
struct LEvent {
    cut: u64,
    kind: String,
    actor: u32,
    j: u32,
    xid: TransactionId,
    lsn: XLogRecPtr,
    sync: bool,
    observed: Vec<(u32, u32)>,
}

fn parse_ledger(text: &str) -> Vec<LEvent> {
    let mut out = Vec::new();
    for line in text.lines() {
        if !line.starts_with("EV|") {
            continue;
        }
        let mut ev = LEvent {
            cut: 0,
            kind: String::new(),
            actor: 0,
            j: 0,
            xid: 0,
            lsn: 0,
            sync: false,
            observed: Vec::new(),
        };
        for f in line.split('|').skip(1) {
            let Some((key, val)) = f.split_once('=') else { continue };
            match key {
                "cut" => ev.cut = val.parse().unwrap(),
                "kind" => ev.kind = val.to_string(),
                "actor" => ev.actor = val.parse().unwrap(),
                "j" => ev.j = val.parse().unwrap(),
                "xid" => ev.xid = val.parse().unwrap(),
                "lsn" => ev.lsn = val.parse().unwrap(),
                "sync" => ev.sync = val == "1",
                "obs" => {
                    for pair in val.split(',').filter(|p| !p.is_empty()) {
                        let (a, j) = pair.split_once('.').unwrap();
                        ev.observed.push((a.parse().unwrap(), j.parse().unwrap()));
                    }
                }
                _ => {}
            }
        }
        out.push(ev);
    }
    out
}

#[test]
#[ignore]
fn flushsim_recover_child() {
    if std::env::var(ROLE_ENV).as_deref() != Ok("recover") {
        return;
    }
    let pack = std::path::PathBuf::from(std::env::var(PACK_ENV).unwrap());
    let meta = std::fs::read_to_string(pack.join("meta.txt")).unwrap();
    let ledger = parse_ledger(&std::fs::read_to_string(pack.join("ledger.txt")).unwrap());
    let get = |key: &str| -> String {
        meta.lines()
            .find(|l| l.starts_with(&format!("{key}=")))
            .map(|l| l.splitn(2, '=').nth(1).unwrap().to_string())
            .unwrap_or_default()
    };
    let tag = format!("k={} trigger={} mutation={}", get("k"), get("trigger"), get("mutation"));
    let mut violations: Vec<String> = Vec::new();

    SimVfs::reset();
    import_tree_into_sim(&pack.join("root"), "/");
    init_small::globals::SetDataDir("/");
    init_small::globals::set_enableFsync(true);
    install_stub_seams();
    install_real();
    // Recovery + the verifier walk run on this thread: give it a PGPROC
    // (buffer pins are proc-numbered).
    miscinit::InitProcessLocalLatch().expect("local latch");
    lmgr_proc::InitProcess(BackendType::Backend).unwrap();
    aio_core::pgaio_init_backend().expect("pgaio_init_backend");
    procarray::ProcArrayAdd(lmgr_proc::MyProc().unwrap()).unwrap();

    let boot = std::panic::catch_unwind(|| -> PgResult<()> {
        transam_xlog::ReadControlFile()?;
        transam_xlog::XLOGShmemInit();
        transam_xlog::StartupXLOG()?;
        Ok(())
    });
    match boot {
        Ok(Ok(())) => {}
        Ok(Err(e)) => violations.push(format!("{tag}: RECOVERY FAILED: {e:?}")),
        Err(_) => violations.push(format!("{tag}: RECOVERY PANICKED")),
    }

    if violations.is_empty() {
        let panicked = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let cf = *transam_xlog::control_file::control_file();
            if cf.state != DB_IN_PRODUCTION {
                violations.push(format!("{tag}: post-recovery control state {}", cf.state));
            }

            // xid -> committed, per the recovered clog.
            let committed = |xid: TransactionId| -> bool {
                transam::TransactionIdDidCommit(xid).unwrap_or(false)
            };

            // The visible fold (recovered MVCC state).
            let ctx = MemoryContext::new("flushsim-verify");
            let mcx = ctx.mcx();
            let rel = test_relation(mcx, REL_OID);
            smgr::smgropen(RLOC, INVALID_PROC_NUMBER).unwrap();
            let key = types_storage::RelFileLocatorBackend {
                locator: RLOC,
                backend: INVALID_PROC_NUMBER,
            };
            let nblocks = smgr::smgrnblocks(key, ForkNumber::MAIN_FORKNUM).unwrap();
            // Page fold through the product's smgr/md read path (bufmgr's
            // cold-read path is aio-native post-GL-AIO-1; this rig has no
            // aio bring-up and needs none). Visibility for this INSERT-ONLY
            // workload is exactly "xmin clog-committed" — the same
            // semantics the MVCC walk yields with every xid in the past.
            let mut counts: std::collections::BTreeMap<i32, u32> =
                std::collections::BTreeMap::new();
            #[repr(align(8))]
            struct P([u8; BLCKSZ]);
            let mut pg = P([0u8; BLCKSZ]);
            for b in 0..nblocks {
                smgr::smgrreadv(key, ForkNumber::MAIN_FORKNUM, b, &mut [&mut pg.0[..]])
                    .unwrap();
                // Never-written (all-zero) page image: valid, empty.
                let pd_lower = u16::from_ne_bytes([pg.0[12], pg.0[13]]);
                if pd_lower == 0 {
                    continue;
                }
                let page_addr = pg.0.as_mut_ptr();
                // SAFETY: local aligned page image.
                let page = unsafe {
                    types_storage::bufpage::PageRef::from_raw(
                        core::ptr::NonNull::new(page_addr).unwrap(),
                    )
                };
                for off in 1..=page.max_offset_number() {
                    let id = page.item_id(off);
                    if !id.is_normal() {
                        continue;
                    }
                    let (ptr, _len) = page.item_raw(id);
                    // SAFETY: heap tuple in-page: t_xmin at offset 0,
                    // t_hoff byte at offset 22, int4 datum after the header.
                    let (xmin, val) = unsafe {
                        let xmin = ptr.cast::<u32>().read_unaligned();
                        let hoff = *ptr.add(22) as usize;
                        (xmin, ptr.add(hoff).cast::<i32>().read_unaligned())
                    };
                    if committed(xmin) {
                        *counts.entry(val).or_insert(0) += 1;
                    }
                }
            }

            // Ledger indexes (pre-cut events only: post-cut appends were
            // never delivered to anyone).
            let attempts: std::collections::BTreeMap<(u32, u32), (TransactionId, bool)> =
                ledger
                    .iter()
                    .filter(|e| e.kind == "attempt")
                    .map(|e| ((e.actor, e.j), (e.xid, e.sync)))
                    .collect();
            let acks: Vec<&LEvent> =
                ledger.iter().filter(|e| e.kind == "ack" && e.cut == 0).collect();

            // I1a: durability of sync acks.
            for a in acks.iter().filter(|a| a.sync) {
                if !committed(a.xid) {
                    violations.push(format!(
                        "{tag}: I1a ACKED-LOST actor {} j {} xid {} lsn {:#x}",
                        a.actor, a.j, a.xid, a.lsn
                    ));
                } else {
                    let n = counts.get(&value_of(a.actor, a.j)).copied().unwrap_or(0);
                    if n != ROWS_PER_TXN {
                        violations.push(format!(
                            "{tag}: I1a acked txn actor {} j {} committed but {} rows visible",
                            a.actor, a.j, n
                        ));
                    }
                }
            }

            // I1b: every txn observed by a completed pre-cut read survives.
            // I1b binds on SYNC claims: a reader observing an ASYNC txn that
            // a crash then loses is the C-sanctioned synchronous_commit=off
            // anomaly, not a violation (xact.c's async arm runs the whole
            // visibility tail pre-durability by design).
            for r in ledger.iter().filter(|e| e.kind == "read" && e.cut == 0) {
                for (actor, j) in &r.observed {
                    match attempts.get(&(*actor, *j)) {
                        Some((xid, sync)) if !*sync || committed(*xid) => {}
                        Some((xid, _)) => violations.push(format!(
                            "{tag}: I1b READ-OBSERVED-LOST actor {actor} j {j} xid {xid} (reader {} saw it pre-cut)",
                            r.actor
                        )),
                        None => violations.push(format!(
                            "{tag}: I1b observed value without attempt: actor {actor} j {j}"
                        )),
                    }
                }
            }

            // I1c: every pre-cut delivered notification's notifier survives.
            // I1c likewise binds on sync claims (C delivers notifications
            // post-commit regardless of sync_commit; async notifiers may
            // legally vanish).
            for nev in ledger.iter().filter(|e| e.kind == "notify" && e.cut == 0 && e.sync) {
                if !committed(nev.xid) {
                    violations.push(format!(
                        "{tag}: I1c NOTIFY-DELIVERED-LOST from xid {} (j {})",
                        nev.xid, nev.j
                    ));
                }
            }

            // I2: LSN-prefix closure over the acked set — if any acked txn
            // is lost, every acked txn with a HIGHER commit LSN is lost too
            // (no holes in the surviving WAL prefix).
            let mut surviving_max: Option<(XLogRecPtr, &LEvent)> = None;
            for a in acks.iter().filter(|a| a.sync) {
                if committed(a.xid) && a.lsn > surviving_max.map(|(l, _)| l).unwrap_or(0) {
                    surviving_max = Some((a.lsn, a));
                }
            }
            if let Some((maxlsn, _)) = surviving_max {
                for a in acks.iter().filter(|a| a.sync) {
                    if a.lsn <= maxlsn && !committed(a.xid) {
                        violations.push(format!(
                            "{tag}: I2 prefix hole — acked xid {} at lsn {:#x} lost while lsn {:#x} survived",
                            a.xid, a.lsn, maxlsn
                        ));
                    }
                }
            }

            // I3: atomicity + fold consistency: every visible value is
            // all-or-nothing and belongs to a clog-committed txn.
            for (val, n) in &counts {
                if *n != ROWS_PER_TXN {
                    violations.push(format!(
                        "{tag}: I3 torn txn — value {val} visible {n} times (want {ROWS_PER_TXN})"
                    ));
                }
                let (actor, j) = ((val / 100_000) as u32, (val % 100_000) as u32);
                if let Some((xid, _)) = attempts.get(&(actor, j)) {
                    if !committed(*xid) {
                        violations.push(format!(
                            "{tag}: I3 uncommitted rows visible — actor {actor} j {j} xid {xid}"
                        ));
                    }
                }
            }
            // Committed ⇒ visible (the fold covers exactly the clog set).
            for ((actor, j), (xid, _sync)) in &attempts {
                if committed(*xid) {
                    let n = counts.get(&value_of(*actor, *j)).copied().unwrap_or(0);
                    if n != ROWS_PER_TXN {
                        violations.push(format!(
                            "{tag}: I3 committed-but-missing — actor {actor} j {j} xid {xid} rows {n}"
                        ));
                    }
                }
            }
        }));
        if panicked.is_err() {
            violations.push(format!("{tag}: VERIFIER PANICKED on the recovered image"));
        }
    }

    for v in &violations {
        println!("FLUSHSIM_RECOVER_VIOLATION {v}");
    }
    println!("FLUSHSIM_RECOVER_DONE violations={}", violations.len());
}

// ---------------------------------------------------------------------------
// Orchestrator plumbing.
// ---------------------------------------------------------------------------

fn spawn_child(test_name: &str, envs: &[(&str, String)]) -> (bool, String) {
    let mut cmd = std::process::Command::new(std::env::current_exe().unwrap());
    cmd.args([test_name, "--exact", "--ignored", "--test-threads=1", "--nocapture"]);
    for (key, v) in envs {
        cmd.env(key, v);
    }
    let out = cmd.output().unwrap();
    let text = format!(
        "{}{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    (out.status.success(), text)
}

struct RunSpec {
    seed: u64,
    k: u64,
    trigger: String,
    profile: String,
    mutation: String,
    pipe: bool,
    mix: String,
    nc: u32,
    txns: u32,
    hold: String,
}

impl Default for RunSpec {
    fn default() -> Self {
        RunSpec {
            seed: 1,
            k: 0,
            trigger: "none".into(),
            profile: "normal".into(),
            mutation: "".into(),
            pipe: true,
            mix: "sync".into(),
            nc: 3,
            txns: 6,
            hold: String::new(),
        }
    }
}

fn spec_clone(s: &RunSpec) -> RunSpec {
    RunSpec {
        seed: s.seed,
        k: s.k,
        trigger: s.trigger.clone(),
        profile: s.profile.clone(),
        mutation: s.mutation.clone(),
        pipe: s.pipe,
        mix: s.mix.clone(),
        nc: s.nc,
        txns: s.txns,
        hold: s.hold.clone(),
    }
}

fn writer_envs(spec: &RunSpec, pack: &std::path::Path) -> Vec<(&'static str, String)> {
    let mut envs = vec![
        (ROLE_ENV, "writer".to_string()),
        (PACK_ENV, pack.to_str().unwrap().to_string()),
        (K_ENV, spec.k.to_string()),
        (TRIGGER_ENV, spec.trigger.clone()),
        (PROFILE_ENV, spec.profile.clone()),
        (MUTATION_ENV, spec.mutation.clone()),
        (MIX_ENV, spec.mix.clone()),
        (NC_ENV, spec.nc.to_string()),
        (TXNS_ENV, spec.txns.to_string()),
        ("PGRUST_SIM_SEED", spec.seed.to_string()),
        ("PGRUST_SIM_SCHED", "1".to_string()),
        // Virtual ceiling: the liveness net — a wedged trajectory dies
        // deterministically instead of hanging the sweep.
        ("PGRUST_SIM_VCEIL_S", "60".to_string()),
    ];
    // FLIP (Michael-ruled): the default is ON, so OFF trajectories must
    // SPELL the control posture — an omitted var now means armed.
    envs.push((
        "PGRUST_FLUSH_PIPELINE",
        if spec.pipe { "1" } else { "0" }.to_string(),
    ));
    if !spec.hold.is_empty() {
        envs.push((HOLD_ENV, spec.hold.clone()));
    }
    envs
}

/// Run one writer(+recover) trajectory; returns (writer text, meta text,
/// ledger text, recover text or empty when the writer never cut).
fn run_traj(base: &std::path::Path, name: &str, spec: &RunSpec) -> (String, String, String, String) {
    let pack = base.join(name);
    let (ok, wtext) = spawn_child("flushsim_writer_child", &writer_envs(spec, &pack));
    assert!(
        ok && wtext.contains("FLUSHSIM_WRITER_DONE"),
        "writer child failed ({name}):\n{wtext}"
    );
    let meta = std::fs::read_to_string(pack.join("meta.txt")).unwrap();
    let ledger = std::fs::read_to_string(pack.join("ledger.txt")).unwrap();
    let cuts: u64 = meta
        .lines()
        .find(|l| l.starts_with("cuts="))
        .and_then(|l| l.split('=').nth(1))
        .and_then(|v| v.parse().ok())
        .unwrap_or(0);
    let rtext = if cuts > 0 {
        let (ok, rtext) = spawn_child(
            "flushsim_recover_child",
            &[(ROLE_ENV, "recover".to_string()), (PACK_ENV, pack.to_str().unwrap().to_string())],
        );
        assert!(
            ok && rtext.contains("FLUSHSIM_RECOVER_DONE"),
            "recover child failed ({name}):\n{rtext}"
        );
        rtext
    } else {
        String::new()
    };
    let _ = std::fs::remove_dir_all(&pack);
    (wtext, meta, ledger, rtext)
}

fn meta_field(meta: &str, key: &str) -> String {
    meta.lines()
        .find(|l| l.starts_with(&format!("{key}=")))
        .map(|l| l.splitn(2, '=').nth(1).unwrap().to_string())
        .unwrap_or_default()
}

fn writer_violations(meta: &str) -> Vec<String> {
    meta.lines()
        .filter(|l| l.starts_with("violation="))
        .map(|l| l.splitn(2, '=').nth(1).unwrap().to_string())
        .collect()
}

fn recover_violations(rtext: &str) -> usize {
    rtext
        .lines()
        .filter_map(|l| {
            l.find("FLUSHSIM_RECOVER_DONE violations=")
                .map(|i| l[i + "FLUSHSIM_RECOVER_DONE violations=".len()..].trim().to_string())
        })
        .next()
        .and_then(|v| v.parse().ok())
        .unwrap_or(usize::MAX)
}

fn stats_field(meta: &str, name: &str) -> u64 {
    meta_field(meta, "stats")
        .split(',')
        .find_map(|kv| kv.strip_prefix(&format!("{name}:")))
        .and_then(|v| v.parse().ok())
        .unwrap_or(u64::MAX)
}

fn scratch_base(tag: &str) -> std::path::PathBuf {
    let base = std::env::temp_dir().join(format!("flushsim-{tag}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&base);
    std::fs::create_dir_all(&base).unwrap();
    base
}

// ---------------------------------------------------------------------------
// CI TIER
// ---------------------------------------------------------------------------

/// Determinism gate: same (workload, schedule seed) ⇒ byte-identical ledger,
/// identical durable-tree digest, identical stats line. x2 seeds.
#[test]
fn flushsim_determinism() {
    let base = scratch_base("det");
    for seed in [11u64, 47u64] {
        let spec = RunSpec {
            seed,
            trigger: "end".into(),
            hold: "8:6".into(),
            ..Default::default()
        };
        let (_, meta1, ledger1, _) = run_traj(&base, &format!("det-{seed}-a"), &spec);
        let (_, meta2, ledger2, _) = run_traj(&base, &format!("det-{seed}-b"), &spec);
        assert_eq!(ledger1, ledger2, "seed {seed}: ledger not deterministic");
        assert_eq!(
            meta_field(&meta1, "tree_digest"),
            meta_field(&meta2, "tree_digest"),
            "seed {seed}: durable image not deterministic"
        );
        assert_eq!(
            meta_field(&meta1, "stats"),
            meta_field(&meta2, "stats"),
            "seed {seed}: flushpipe stats not deterministic"
        );
        assert_eq!(
            meta_field(&meta1, "ops"),
            meta_field(&meta2, "ops"),
            "seed {seed}: vfs op count not deterministic"
        );
    }
    let _ = std::fs::remove_dir_all(&base);
}

/// No-crash liveness + tripwires, knob ON and OFF, across seeds/profiles.
/// Every trajectory must run every committer to completion with zero
/// violations, zero backstop/retry (healthy profiles), conservation exact —
/// and knob-OFF must keep every pipeline counter at zero.
#[test]
fn flushsim_liveness_and_control() {
    let base = scratch_base("live");
    // (seed, profile, pipe, hold): hold="4:40" = brief contention windows
    // (the pipelined arm ENGAGES: conditional-acquire losses -> register ->
    // directed completion) with a long gap so a live flusher covers every
    // registrant well inside the 100 ms backstop — healthy runs must show
    // retry == 0 AND backstop == 0.
    for &(seed, profile, pipe, hold) in &[
        (3u64, "normal", true, "4:40"),
        (5, "hyper", true, "4:40"),
        (7, "stalled", true, ""),
        (3, "normal", false, "4:40"),
        (9, "dead50", true, "4:40"),
        (13, "normal", true, ""),
    ] {
        let spec = RunSpec {
            seed,
            profile: profile.into(),
            pipe,
            trigger: "end".into(),
            mix: "mixed".into(),
            hold: hold.into(),
            ..Default::default()
        };
        let name = format!(
            "live-{seed}-{profile}-{}{}",
            if pipe { "on" } else { "off" },
            if hold.is_empty() { "" } else { "-hold" }
        );
        let (wtext, meta, _ledger, rtext) = run_traj(&base, &name, &spec);
        let wv = writer_violations(&meta);
        assert!(wv.is_empty(), "{name}: writer violations: {wv:?}\n{wtext}");
        // Missed-wake tripwire: NEVER fires with the real latch routing.
        if pipe {
            assert_eq!(stats_field(&meta, "backstop"), 0, "{name}: missed-wake tripwire");
        }
        // Liveness re-arm witness: recorded, NOT asserted zero — FINDING-1:
        // wait_for_flush re-arms on ANY uncovered latch wake (the WaitLatch
        // result is discarded), so a spurious set (startup pre-set;
        // completion observed at loop-head without a park, leaving the set
        // unconsumed) retries at zero cadence even with a healthy flusher.
        // Durability-safe (Retry = C follower self-sufficiency) but it
        // defeats directed completion and denies the letter's
        // "retry_rearm never fires in a healthy pipeline" witness.
        let _healthy_retry = stats_field(&meta, "retry");
        // Engagement witness (harness sanity): the contended cell must
        // actually exercise the queue — a sweep that never registers a
        // waiter tests nothing.
        if pipe && hold == "4:40" && profile != "dead50" {
            assert!(
                stats_field(&meta, "reg") > 0,
                "{name}: pipeline never engaged (reg=0) — contention rig broken"
            );
        }
        // Quiescent-cut recovery must be perfectly clean.
        assert_eq!(recover_violations(&rtext), 0, "{name}: recovery violations\n{rtext}");
    }
    let _ = std::fs::remove_dir_all(&base);
}

/// Crash-point smoke: vfs-op cuts across the commit window + event-trigger
/// cuts at ack/notify/read positions, knob ON. Every recovered trajectory
/// must satisfy I1-I3.
#[test]
fn flushsim_crash_smoke() {
    let base = scratch_base("crash");
    // Baseline: measure the op budget of this workload shape.
    let spec0 = RunSpec {
        seed: 21,
        trigger: "end".into(),
        hold: "4:40".into(),
        ..Default::default()
    };
    let (_, meta0, _, _) = run_traj(&base, "crash-baseline", &spec0);
    let ops: u64 = meta_field(&meta0, "ops").parse().unwrap();
    assert!(ops > 20, "workload too small to sweep (ops={ops})");

    // Stratified op sweep (CI-sized): 8 points spread over the run.
    for i in 1..=8u64 {
        let k = (ops * i) / 9;
        let spec = RunSpec { seed: 21, k: k.max(1), hold: "4:40".into(), ..Default::default() };
        let name = format!("crash-k{k}");
        let (_, meta, _, rtext) = run_traj(&base, &name, &spec);
        if meta_field(&meta, "cuts") == "0" {
            continue; // op count ran short of k on this schedule: no cut
        }
        assert_eq!(recover_violations(&rtext), 0, "{name}: violations\n{rtext}");
        let wv = writer_violations(&meta);
        assert!(wv.is_empty(), "{name}: writer violations: {wv:?}");
    }

    // Event-trigger cuts: right after the i-th ack / first notify / second
    // read completion.
    for (trigger, seed) in
        [("ack:1", 31u64), ("ack:5", 31), ("ack:9", 33), ("notify:1", 35), ("read:2", 37)]
    {
        let spec = RunSpec { seed, trigger: trigger.into(), ..Default::default() };
        let name = format!("crash-{}", trigger.replace(':', "-"));
        let (_, meta, _, rtext) = run_traj(&base, &name, &spec);
        if meta_field(&meta, "cuts") == "0" {
            continue; // trigger never reached on this schedule (e.g. few notifies)
        }
        assert_eq!(recover_violations(&rtext), 0, "{name}: violations\n{rtext}");
    }
    let _ = std::fs::remove_dir_all(&base);
}

/// SENSITIVITY BAR: each deliberately-broken variant must be flagged within
/// the CI seed budget, by the named detector tier. Prints the seed at which
/// each mutation was first caught (the catch-time record for the letter).
#[test]
fn flushsim_mutation_battery() {
    let base = scratch_base("mut");

    // (a) ack-early: L1 must fire live (no crash needed), and a cut right
    // after an ack must show ACKED-LOST.
    {
        let mut caught_live = false;
        for seed in 101..=103u64 {
            let spec = RunSpec {
                seed,
                mutation: "ack-early".into(),
                trigger: "end".into(),
                ..Default::default()
            };
            let (_, meta, _, _) = run_traj(&base, &format!("mut-a-{seed}"), &spec);
            if writer_violations(&meta).iter().any(|v| v.contains("L1 at-ack")) {
                println!("MUTATION-CAUGHT a=ack-early tier=L1 seed={seed}");
                caught_live = true;
                break;
            }
        }
        assert!(caught_live, "mutation (a) ack-early NOT caught by L1 within seed budget");

        let mut caught_crash = false;
        for seed in 101..=108u64 {
            for ack_n in [2u32, 4, 6] {
                let spec = RunSpec {
                    seed,
                    mutation: "ack-early".into(),
                    trigger: format!("ack:{ack_n}"),
                    ..Default::default()
                };
                let (_, meta, _, rtext) =
                    run_traj(&base, &format!("mut-a-crash-{seed}-{ack_n}"), &spec);
                if meta_field(&meta, "cuts") != "0"
                    && rtext.contains("I1a ACKED-LOST")
                {
                    println!(
                        "MUTATION-CAUGHT a=ack-early tier=I1a seed={seed} trigger=ack:{ack_n}"
                    );
                    caught_crash = true;
                    break;
                }
            }
            if caught_crash {
                break;
            }
        }
        assert!(caught_crash, "mutation (a) ack-early NOT caught by the crash ledger");
    }

    // (b) wake-drop: every directed completion wake aimed at committer 0
    // lands on the WRONG latch. The victim still exits via the 100 ms
    // timeout lap seeing `completed` — a path that bumps NO counter — so
    // the catch signal is the LATENCY SIGNATURE: a registered wait that
    // should complete in <10 ms virtual takes a full backstop lap. The
    // battery first proves the control (same spec, no mutation) stays
    // under the budget, then requires the mutated run to blow it (or to
    // trip backstop/retry when the walk itself is late).
    {
        const BUDGET_MS: u64 = 90;
        let mut caught = false;
        for seed in 201..=212u64 {
            let spec = RunSpec {
                seed,
                trigger: "end".into(),
                nc: 4,
                txns: 8,
                hold: "4:40".into(),
                ..Default::default()
            };
            let control = RunSpec { mutation: String::new(), ..spec_clone(&spec) };
            let (_, cmeta, _, _) = run_traj(&base, &format!("mut-b-ctl-{seed}"), &control);
            let ctl_ms: u64 = meta_field(&cmeta, "max_commit_ms").parse().unwrap();
            if stats_field(&cmeta, "reg") == 0 || ctl_ms >= BUDGET_MS {
                continue; // this schedule never engages cleanly: not a probe cell
            }
            let mutated = RunSpec { mutation: "wake-drop".into(), ..spec_clone(&spec) };
            let (_, meta, _, _) = run_traj(&base, &format!("mut-b-{seed}"), &mutated);
            let mut_ms: u64 = meta_field(&meta, "max_commit_ms").parse().unwrap();
            if mut_ms >= BUDGET_MS
                || stats_field(&meta, "backstop") > 0
                || stats_field(&meta, "retry") > 0
            {
                println!(
                    "MUTATION-CAUGHT b=wake-drop tier=L3-latency seed={seed} control_ms={ctl_ms} mutated_ms={mut_ms}"
                );
                caught = true;
                break;
            }
        }
        assert!(caught, "mutation (b) wake-drop NOT caught by the latency budget");
    }

    // (c) notify-early: a cut at the first delivered notification must show
    // a notifier that recovery cannot produce.
    {
        let mut caught = false;
        for seed in 301..=312u64 {
            // HOLD armed: the pre-commit-delivery -> durability window
            // needs virtual EXTENT (a parked pipelined wait) for the
            // listener's drain cadence to land inside it; an uncontended
            // commit completes in zero virtual time and the window is
            // unhittable by construction.
            let spec = RunSpec {
                seed,
                mutation: "notify-early".into(),
                trigger: "notify:1".into(),
                hold: "4:40".into(),
                ..Default::default()
            };
            let (_, meta, _, rtext) = run_traj(&base, &format!("mut-c-{seed}"), &spec);
            if meta_field(&meta, "cuts") != "0" && rtext.contains("I1c NOTIFY-DELIVERED-LOST") {
                println!("MUTATION-CAUGHT c=notify-early tier=I1c seed={seed}");
                caught = true;
                break;
            }
        }
        assert!(caught, "mutation (c) notify-early NOT caught by the notify ledger");
    }

    // (d) wrong-lsn: the overclaimed flush word blinds L1 by construction;
    // the crash tier (cuts positioned after post-mutation acks) must catch
    // the acked-but-never-durable txn, or L2 must catch the truth
    // regression when a later real flush publishes below the overclaim.
    {
        let mut caught = false;
        for seed in 401..=416u64 {
            for ack_n in [7u32, 9, 11] {
                let spec = RunSpec {
                    seed,
                    mutation: "wrong-lsn".into(),
                    trigger: format!("ack:{ack_n}"),
                    nc: 3,
                    txns: 6,
                    hold: "4:40".into(),
                    ..Default::default()
                };
                let (_, meta, _, rtext) =
                    run_traj(&base, &format!("mut-d-{seed}-{ack_n}"), &spec);
                let live_l2 =
                    writer_violations(&meta).iter().any(|v| v.contains("L2 monotonicity"));
                let crash_i1a =
                    meta_field(&meta, "cuts") != "0" && rtext.contains("I1a ACKED-LOST");
                if live_l2 || crash_i1a {
                    println!(
                        "MUTATION-CAUGHT d=wrong-lsn tier={} seed={seed} trigger=ack:{ack_n}",
                        if crash_i1a { "I1a" } else { "L2" }
                    );
                    caught = true;
                    break;
                }
            }
            if caught {
                break;
            }
        }
        assert!(caught, "mutation (d) wrong-lsn NOT caught within seed budget");
    }

    let _ = std::fs::remove_dir_all(&base);
}

// ---------------------------------------------------------------------------
// DEEP TIER (env-gated; hours-scale locally, CI-shardable by seed range).
//   PGRUST_FLUSHSIM_DEEP="<seed_base>:<n_seeds>" sweeps: for each seed, the
//   full vfs-op cut range (stride via PGRUST_FLUSHSIM_STRIDE, default 3) x
//   profiles x knob postures. Every red is reproducible from the printed
//   (seed, k, profile, pipe) tuple.
// ---------------------------------------------------------------------------

#[test]
#[ignore]
fn flushsim_deep_sweep() {
    let Ok(spec) = std::env::var("PGRUST_FLUSHSIM_DEEP") else {
        eprintln!("set PGRUST_FLUSHSIM_DEEP=<seed_base>:<n_seeds> to run");
        return;
    };
    let (sbase, nseeds) = spec.split_once(':').expect("PGRUST_FLUSHSIM_DEEP=<base>:<n>");
    let sbase: u64 = sbase.parse().unwrap();
    let nseeds: u64 = nseeds.parse().unwrap();
    let stride: u64 = std::env::var("PGRUST_FLUSHSIM_STRIDE")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(3);
    let base = scratch_base("deep");
    let mut trajectories = 0u64;
    let mut reds = 0u64;
    for seed in sbase..sbase + nseeds {
        for &(profile, pipe) in
            &[("normal", true), ("stalled", true), ("hyper", true), ("normal", false)]
        {
            // Baseline for the op budget of this (seed, profile) cell.
            let spec0 = RunSpec {
                seed,
                profile: profile.into(),
                pipe,
                trigger: "end".into(),
                mix: "mixed".into(),
                ..Default::default()
            };
            let (_, meta0, _, rtext0) = run_traj(&base, "deep-baseline", &spec0);
            trajectories += 1;
            if recover_violations(&rtext0) != 0 || !writer_violations(&meta0).is_empty() {
                reds += 1;
                println!("DEEP-RED seed={seed} profile={profile} pipe={pipe} k=end");
            }
            let ops: u64 = meta_field(&meta0, "ops").parse().unwrap();
            let mut k = 1;
            while k < ops {
                let spec = RunSpec {
                    seed,
                    k,
                    profile: profile.into(),
                    pipe,
                    mix: "mixed".into(),
                    ..Default::default()
                };
                let (_, meta, _, rtext) = run_traj(&base, "deep-point", &spec);
                trajectories += 1;
                if meta_field(&meta, "cuts") != "0"
                    && (recover_violations(&rtext) != 0
                        || !writer_violations(&meta).is_empty())
                {
                    reds += 1;
                    println!(
                        "DEEP-RED seed={seed} profile={profile} pipe={pipe} k={k} — repro: \
                         PGRUST_SIM_SEED={seed} PGRUST_SIM_SCHED=1 {K_ENV}={k} \
                         {PROFILE_ENV}={profile} {}",
                        if pipe { "PGRUST_FLUSH_PIPELINE=1" } else { "PGRUST_FLUSH_PIPELINE=0" }
                    );
                }
                k += stride;
            }
        }
        println!("DEEP-PROGRESS seed={seed} trajectories={trajectories} reds={reds}");
    }
    println!("DEEP-DONE trajectories={trajectories} reds={reds}");
    assert_eq!(reds, 0, "deep sweep found {reds} red trajectories (see DEEP-RED lines)");
    let _ = std::fs::remove_dir_all(&base);
}
