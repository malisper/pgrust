// DST P4 inc-2 — the PRODUCT-SHAPED crash-recovery property sweep, sim-cfg
// only. inc-1's own verdict named its harness-transcribed recovery step the
// biggest fidelity gap; this file closes it:
//
//   * the datadir is minted INSIDE the SimVfs namespace (product
//     controldata/WAL-segment builders + the product's own BootStrapCLOG/
//     BootStrapSUBTRANS writing through fd -> vfs -> SimVfs);
//   * the workload drives WAL through the PRODUCT paths — heap_insert /
//     CommitTransactionCommand (RecordTransactionCommit -> XLogFlush ->
//     issue_xlog_fsync) + a mid-run product CreateCheckPoint — over the
//     same fd/vfs data plane the server uses;
//   * the CUT is the P4 fault model's crash-image primitive
//     (FaultRule::crash_at_op swept over every workload op, seeded-subset
//     survivor images per point);
//   * recovery is THE PRODUCT'S StartupXLOG (InitWalRecovery /
//     PerformWalRecovery / end-of-recovery checkpoint) booted over the
//     post-crash image inside a fresh sim universe.
//
// C provenance for "committed" at each cut class: xact.c's
// RecordTransactionCommit — a transaction is durably committed iff its
// commit record is flushed to WAL (XLogFlush returned; wal_sync_method
// fdatasync under sim — the O_DSYNC open_datasync arm is not modeled by
// SimVfs, same law as the wasm lane's harness). An ACK to the client
// happens only after that flush, so at every cut: every acked txn must be
// clog-committed and MVCC-visible after StartupXLOG; unacked txns may be
// either (their record may or may not have made the flush) but the visible
// set must equal EXACTLY the clog-committed prefix (internal consistency;
// no torn/garbage tuple applies).
//
// Process shape (fresh statics per boot, the M4 crash_recovery.rs pattern):
// each sweep point spawns a WRITER child (mint + workload + cut + pack the
// post-crash sim image to a real-fs dir — the cp -RL/pack shape from the
// wasm lanes) and a RECOVER child (import the pack into a fresh sim
// universe, boot the product recovery, verify the properties). The pack
// import helper doubles as the real-initdb-datadir importer (see
// initdb_datadir_boots_under_sim below).
//
// Run with: RUSTFLAGS='--cfg pgrust_sim' cargo test -p xlogrecovery --test sim_crash_sweep
#![cfg(pgrust_sim)]

use std::cell::Cell;
use std::rc::Rc;
use std::sync::atomic::Ordering::Relaxed;

use mcx::{Mcx, MemoryContext, PgVec};
use transam_xlog::control_file::{
    FirstNormalUnloggedLSN, FLOATFORMAT_VALUE, PG_CONTROL_FILE_SIZE, PG_CONTROL_VERSION,
    TOAST_MAX_CHUNK_SIZE,
};
use transam_xlog::{
    SizeOfXLogRecord, XLogRecPtrToBytePos, CHECKPOINT_FORCE, CHECKPOINT_IMMEDIATE,
    CHECKPOINT_WAIT, DB_IN_PRODUCTION, MAXALIGN, RM_XLOG_ID, WAL_LEVEL_REPLICA,
    XLOG_CHECKPOINT_SHUTDOWN, XLP_LONG_HEADER,
};
use types_core::{
    BackendType, ForkNumber, InvalidBlockNumber, Oid, XLogRecPtr, BLCKSZ, INVALID_PROC_NUMBER,
    RELPERSISTENCE_PERMANENT,
};
use types_error::PgResult;
use types_fmgr::{FmgrInfo, FunctionCallInfoBaseData};
use types_nbtree::{
    BTPageOpaqueData, BTP_DELETED, BTP_HALF_DEAD, BTP_LEAF, P_NONE as BT_P_NONE,
};
use types_rel::{
    FormData_pg_class, FormData_pg_index, LockInfoData, LockRelId, Relation, RelationData,
    LOCKMODE, RELKIND_INDEX, RELKIND_RELATION,
};
use types_snapshot::{SnapshotData, SnapshotType};
use types_storage::bufpage::PageMut;
use types_storage::RelFileLocator;
use types_tuple::itemptr::ItemPointerCompare;
use types_tuple::{
    CompactAttribute, FormData_pg_attribute, HeapTupleData, ItemPointerData, NameData,
    TupleDescData,
};

use vfs::sim::{
    CrashImage, FaultDecision, FaultRule, OpKind, OpMatch, PathClass, SeededFaultPlan, SimVfs,
};

/// inc-3: 1 MB WAL segments (a valid wal_segment_size) so the scaled
/// workload CROSSES segment boundaries — XLogFileInit (zero-fill + install
/// rename) and checkpoint-time RemoveOldXlogFiles recycling become cut
/// classes (the N4 path-at-op work classifies the recycled renames).
const SEG: i32 = 1024 * 1024;
const SYS_ID: u64 = 0x5EED_FA17_0002;
const REL_OID: Oid = 61000;
const RLOC: RelFileLocator = RelFileLocator::new(1663, 5, REL_OID);
const CKPT_LOC: XLogRecPtr = SEG as u64 + 40;
const CKPT_TOT_LEN: usize = SizeOfXLogRecord + 2 + controldata_utils::SIZEOF_CHECKPOINT;

/// Transactions per workload run; txn t inserts ROWS_PER_TXN int4 rows of
/// value t and commits (inc-3 scale-up: the heap spans MANY pages — buffer
/// eviction writes heap pages mid-run — and the WAL volume crosses 1 MB
/// segments). First real xid is 3 (the minted checkpoint's nextXid), so
/// txn t <-> xid t+2. A product CreateCheckPoint runs after each txn in
/// CKPT_AFTER — the second one sits past the first segment crossing so
/// RemoveOldXlogFiles gets recyclable segments.
///
/// inc-4 (SEGMENT-RECYCLE CUT CLASSES): 13 txns push the WAL tail INTO the
/// recycled segment — the txn-8 checkpoint recycles segment 1 to the future
/// segment 3 name WITH ITS STALE CONTENT (recycled segments are not
/// re-zeroed; XLogFileInitInternal's open of an existing segment is the
/// reuse), so the last txns write live WAL over stale segment-1 residue.
/// Cuts there exercise the stale-residue guards (xlp_pageaddr + CRC) that
/// make recycling safe. inc-3 trap re-checked: CKPT_AFTER[1]=8 still sits
/// past the first crossing (txn 6), and NBuffers=128 stays below the ~150
/// heap pages so mid-run eviction writes remain cut points.
const TXNS: u32 = 13;
const ROWS_PER_TXN: u32 = 2600;
const CKPT_AFTER: [u32; 2] = [3, 8];

const SEED: u64 = 0x5EED_FA17_0002;

const ROLE_ENV: &str = "PGRUST_SIM_SWEEP_ROLE";
const K_ENV: &str = "PGRUST_SIM_SWEEP_K";
const PACK_ENV: &str = "PGRUST_SIM_SWEEP_PACK";
/// Red-arm selector: "" (none), "fsync" (product fsync layer disabled),
/// "recycle-needed" (over-eager recycle of a segment the redo horizon still
/// needs), "stale-residue" (validating stale bytes at the WAL tail).
const RED_ENV: &str = "PGRUST_SIM_SWEEP_RED";
const INITDB_ENV: &str = "PGRUST_SIM_SWEEP_INITDB_DD";
/// Writer/recover rig selector: "" (the minted 1 MB-segment rig) or
/// "initdb" (the real-initdb datadir composed through the t28 provider seam
/// — vfs::sim_boot::compose_boot_namespace).
const ARM_ENV: &str = "PGRUST_SIM_SWEEP_ARM";

/// Transactions for the initdb-composition arm (16 MB real segments — this
/// arm widens the cut distribution over the REAL datadir composition; the
/// segment-crossing/recycle classes stay on the minted 1 MB rig).
const INITDB_TXNS: u32 = 6;

// ---------------------------------------------------------------------------
// inc-5 INDEX LANE (btree arm): a second relation — a real nbtree index over
// the int4 column — driven through the PRODUCT's btinsert/btbulkdelete on the
// live buffer manager, in the minted 1 MB-segment rig. New cut classes:
// btree leaf inserts + page splits + NEWROOT in the WAL stream, index-file
// page flushes at checkpoints, and the VACUUM (_bt_delitems_vacuum) window.
// ---------------------------------------------------------------------------

const IDX_OID: Oid = 61001;
const IDX_RLOC: RelFileLocator = RelFileLocator::new(1663, 5, IDX_OID);
/// btree-arm workload: BT_TXNS txns; every txn but the delete txn inserts
/// BT_ROWS (heap row + index entry per row). Txn BT_DELETE_TXN heap-deletes
/// txn 1's rows and commits; btbulkdelete (the product vacuum entry) then
/// removes their index entries — vacuum runs only after the deleting commit
/// was ACKED, so its durability precedes every later cut (the C invariant:
/// vacuum only removes tuples whose deleting xid is durably committed).
const BT_TXNS: u32 = 8;
const BT_ROWS: u32 = 900;
const BT_DELETE_TXN: u32 = 6;
const BT_CKPT_AFTER: [u32; 2] = [3, 6];

/// Index-arm keys. `hash`: a bijective odd-multiplier scramble — unique keys
/// in pseudorandom order, so splits land mid-tree (the general split path).
/// `asc`: dense ascending — rightmost-split fastpath; the idx-stale red uses
/// it so txns 4-5 never touch the leftmost leaf again after the checkpoint.
fn bt_key(kind: &str, t: u32, i: u32) -> i32 {
    let ord = (t - 1) * BT_ROWS + i;
    match kind {
        "asc" => (ord + 1) as i32,
        _ => ord.wrapping_add(1).wrapping_mul(2_654_435_761) as i32,
    }
}

/// Torn-write arm selector: "<heap|wal>:<j>:<p>" — the j-th PWriteV of that
/// path class crashes MID-WRITE keeping a p-byte prefix (floored to the
/// 512 B sector atomicity floor by the engine).
const TORN_ENV: &str = "PGRUST_SIM_SWEEP_TORN";
/// EMFILE arm selector: "<once|sticky>:<j>" — the j-th Open fails with
/// EMFILE (descriptor exhaustion); sticky = that open and every later one.
const EMFILE_ENV: &str = "PGRUST_SIM_SWEEP_EMFILE";
/// inc-6 recover-side WEAKENED-REDO red selector (read by the RECOVER child;
/// the writer stays green — the deliberate bug lives entirely in replay):
/// "gin-listpage" (gin_xlog::sim_red: pending-list content restore skipped),
/// "brin-narrow" (brin_xlog::sim_red: samepage summary update kept stale),
/// "btvac-keep" (nbtree_xlog::sim_red: vacuum item deletions skipped),
/// "heap-prune-keep" (harness seam wrap: the prune record's LP_UNUSED
/// transitions dropped — no product hook needed).
const REDO_RED_ENV: &str = "PGRUST_SIM_SWEEP_REDO_RED";

// ---------------------------------------------------------------------------
// inc-6 GIN LANE: a real gin index (array_ops-over-int4[] shape) driven
// through the PRODUCT's gininsert (fastupdate default ON -> every insert
// lands in the PENDING LIST) and ginInsertCleanup (the gin_clean_pending_list
// merge) on the live buffer manager. New cut classes: pending-list page
// writes (INSERT_LISTPAGE / UPDATE_META_PAGE WAL), the two cleanup windows
// (DELETE_LISTPAGE + entry-tree INSERT/SPLIT records — the first merge grows
// the tree from an empty root through leaf splits, the second inserts into
// the EXISTING tree), and gin-index-page flushes at checkpoints.
// ---------------------------------------------------------------------------

const GIN_IDX_OID: Oid = 61002;
const GIN_IDX_RLOC: RelFileLocator = RelFileLocator::new(1663, 5, GIN_IDX_OID);
/// gin-arm workload: GIN_TXNS txns x GIN_ROWS rows; each row inserts the heap
/// int4 value v plus gininsert of the 1-element array [v] (extractValue =
/// ginarrayextract yields exactly v back, so index keys == heap values and
/// the inc-5 coverage property generalizes unchanged). Values REPEAT
/// (GIN_DISTINCT distinct keys) so entry tuples carry multi-item posting
/// lists; ~1.5k distinct keys split several entry leaves at cleanup.
const GIN_TXNS: u32 = 7;
const GIN_ROWS: u32 = 700;
const GIN_DISTINCT: u32 = 1531;
const GIN_CKPT_AFTER: [u32; 2] = [2, 5];
/// ginInsertCleanup (the product gin_clean_pending_list path) runs after
/// these txns; both spans are recorded as dense cut windows.
const GIN_CLEAN_AFTER: [u32; 2] = [4, 7];

fn gin_val(t: u32, i: u32) -> i32 {
    let ord = (t - 1) * GIN_ROWS + i;
    (1 + ord.wrapping_mul(2_654_435_761) % GIN_DISTINCT) as i32
}

// ---------------------------------------------------------------------------
// inc-6 BRIN LANE: a real brin index (int4 minmax, pages_per_range=2) under
// the summarization workload. txns 1-2 insert ascending values heap-only
// (ranges unsummarized = must-scan = legally WIDER); a product brinsummarize
// pass (the brin_summarize_new_values shape) summarizes every range incl.
// the partial boundary range; a product checkpoint follows; txn 3 inserts
// DESCENDING NEGATIVE values — each row landing on the summarized boundary
// page WIDENS its range's min (one SAMEPAGE_UPDATE per row: the first
// post-checkpoint update carries the FPI, the rest replay as needs-redo —
// exactly the chain the narrowing red leans on); txns 4-5 extend into new
// ranges (brininsert no-ops there), a second summarize covers them, range 0
// is then DESUMMARIZED (the invalidation class), and txn 6 keeps inserting.
// Property: a lossy index may over-include, never exclude — every VISIBLE
// heap row's range must be unsummarized, a placeholder, or carry a summary
// with min <= value <= max.
// ---------------------------------------------------------------------------

const BRIN_IDX_OID: Oid = 61003;
const BRIN_IDX_RLOC: RelFileLocator = RelFileLocator::new(1663, 5, BRIN_IDX_OID);
const BR_TXNS: u32 = 6;
const BR_ROWS: u32 = 700;
const BR_PPR: u32 = 2; // pages_per_range
const BR_NEG_TXN: u32 = 3;
/// The txn-2 checkpoint runs AFTER the first summarize pass (writer-loop
/// ordering), so txn 3's widening updates are inside the replay range.
const BR_CKPT_AFTER: [u32; 1] = [2];
const BR_SUM_AFTER: [u32; 2] = [2, 5];

fn brin_val(t: u32, i: u32) -> i32 {
    let ord = ((t - 1) * BR_ROWS + i) as i32;
    if t == BR_NEG_TXN {
        -(ord + 1)
    } else {
        ord + 1
    }
}

// ---------------------------------------------------------------------------
// inc-6 VACUUM-CONTENT LANE ("lpreuse", the inc-5 V5-O2/O3 ledger): an
// ascending-key btree-arm variant whose delete txn removes txns 3-4
// ENTIRELY — contiguous key ranges empty whole leaves, so the product
// btbulkdelete reaches _bt_pagedel (MARK_PAGE_HALFDEAD / UNLINK_PAGE cut
// classes) — followed by a product heap_page_prune_and_freeze(MARK_UNUSED_NOW)
// pass over the freed heap pages (LP_DEAD -> LP_UNUSED via the unified
// prune record; run only AFTER btbulkdelete removed the index entries, the
// C ordering invariant) and reinsert txns whose NEW keys land in the REUSED
// line pointers (the freespace stubs route inserts back to the freed
// blocks; PageAddItem's PD_HAS_FREE_LINES scan does the LP reuse). Old and
// new key ranges are DISJOINT so a recovery divergence CLASSIFIES: an
// old-range value visible after the delete committed = OLD TUPLE
// RESURRECTED; a missing new-range value = NEW TUPLE LOST. With LP reuse a
// LOST vacuum-content redo finally becomes VISIBLE: a stale index entry
// resolves to a reused slot holding a different row (key != heap value) —
// the silent-loss class the inc-5 properties could not see.
// ---------------------------------------------------------------------------

const LP_TXNS: u32 = 7;
const LP_ROWS: u32 = 600;
const LP_DELETE_TXN: u32 = 5; // deletes txns 3-4 (tail heap pages, whole leaves)
/// One checkpoint after txn 2: txns 3-4 spend the post-checkpoint FPIs, so
/// the vacuum/prune records replay as needs-redo at end-of-workload cuts.
const LP_CKPT_AFTER: [u32; 1] = [2];
/// Reinsert-generation keys are NEGATIVE: their btinserts land on the
/// LEFTMOST leaves, far from the stale entries the btvac-keep red leaves on
/// the (undeletable) rightmost leaf — so that red's catch stays SILENT
/// (key-vs-heap-value / missing-item), never an insert-redo collision. The
/// heap rows still land in the REUSED line pointers (the freespace queue
/// routes by block, not key).
fn lp_key(t: u32, i: u32) -> i32 {
    if t >= 6 {
        -(((t - 6) * LP_ROWS + i) as i32 + 1)
    } else {
        ((t - 1) * LP_ROWS + i + 1) as i32
    }
}

std::thread_local! {
    /// lpreuse arm: heap blocks freed by the vacuum pass, consumed by the
    /// freespace stubs so reinserts land on reused line pointers.
    static REUSE_BLOCKS: std::cell::RefCell<std::collections::VecDeque<u32>> =
        const { std::cell::RefCell::new(std::collections::VecDeque::new()) };
}

fn pop_reuse_block() -> u32 {
    REUSE_BLOCKS.with(|q| q.borrow_mut().pop_front().unwrap_or(InvalidBlockNumber))
}

fn per_point_seed(k: u64) -> u64 {
    SEED ^ k.wrapping_mul(0x9E37_79B9_7F4A_7C15)
}

// ---------------------------------------------------------------------------
// sim-side fs helpers (all through the active vfs = SimVfs under this cfg)
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

/// Make the WHOLE current sim tree durable (files fold their journals, dirs
/// promote their entry images) — the mint is the pre-history the sweep never
/// cuts into, exactly like inc-1's bootstrap() discipline. Raw vfs fsyncs:
/// deliberately NOT pg_fsync, so the red arm's enableFsync=false cannot
/// weaken the mint.
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

/// Export the sim tree's DURABLE images to a real-fs directory (the pack).
/// Harness-side plumbing: the pack crosses the process boundary between the
/// writer's post-cut universe and the recover child's fresh one.
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

/// Import a real-fs directory tree into the sim namespace at `sim_base`
/// ("/" = the sim root). The cp -RL shape from the wasm lanes: symlinks are
/// dereferenced (std::fs::metadata follows), entries imported in sorted
/// order (determinism). This is ALSO the real-initdb-datadir importer.
fn import_tree_into_sim(src: &std::path::Path, sim_base: &str) {
    let mut entries: Vec<_> = std::fs::read_dir(src)
        .unwrap()
        .map(|e| e.unwrap())
        .collect();
    entries.sort_by_key(|e| e.file_name());
    for e in entries {
        let name = e.file_name().into_string().unwrap();
        let sim_path = if sim_base == "/" {
            format!("/{name}")
        } else {
            format!("{sim_base}/{name}")
        };
        let meta = std::fs::metadata(e.path()).unwrap(); // follows symlinks
        if meta.is_dir() {
            vfs_mkdir_p(&sim_path);
            import_tree_into_sim(&e.path(), &sim_path);
        } else {
            vfs_write_file(&sim_path, &std::fs::read(e.path()).unwrap());
        }
    }
}

// ---------------------------------------------------------------------------
// the rig (M4 crash_recovery.rs shape, vfs-ified): stub seams + real units
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

    pg_sema_seams::pg_semaphore_create::set(|_| {});
    pg_sema_seams::pg_semaphore_reset::set(|_| {});
    pg_sema_seams::pg_semaphore_lock::set(|_| {});
    pg_sema_seams::pg_semaphore_unlock::set(|_| {});
    s_lock_seams::perform_spin_delay::set(|_| std::thread::yield_now());
    s_lock_seams::finish_spin_delay::set(|_| {});
    s_lock_seams::set_spins_per_delay::set(|_| {});
    s_lock_seams::update_spins_per_delay::set(|v| v);
    latch_seams::own_latch::set(|_| {});
    latch_seams::disown_latch::set(|_| {});
    latch_seams::set_latch::set(|_| {});
    latch_seams::set_latch_my_latch::set(|| {});
    latch_seams::wait_latch_my_latch::set(|_, _, _| 0);
    latch_seams::reset_latch_my_latch::set(|| {});
    miscinit_seams::switch_to_shared_latch::set(|| {});
    miscinit_seams::switch_back_to_local_latch::set(|| {});
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
    aio_seams::pgaio_closing_fd::set(|_| {});
    aio_seams::pgaio_io_start_readv::set(|_, _, _| Ok(()));
    aio_seams::at_eoxact_aio::set(|_| {});
    aio_seams::pgaio_error_cleanup::set(|| {});
    lock_seams::lock_acquire_extended::set(|_, _, _, _, _, _| {
        Ok(types_storage::lock::LOCKACQUIRE_OK)
    });

    timestamp_seams::get_current_timestamp::set(|| 777_000_000);
    trigger_seams::after_trigger_begin_xact::set(|| Ok(()));
    trigger_seams::after_trigger_end_xact::set(|_| Ok(()));
    trigger_seams::after_trigger_fire_deferred::set(|| Ok(()));
    async_seams::pre_commit_notify::set(|| Ok(()));
    async_seams::at_commit_notify::set(|| Ok(()));
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
    // inc-4 initdb arm: real initdb datadirs carry DATA CHECKSUMS (the PG 18
    // initdb default), so MarkBufferDirtyHint consults the wal-skip route
    // (XLogHintBitIsNeeded); the rig has no pending-sync state — nothing
    // skips WAL (matches the smgr pending stubs above).
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
    // inc-6: the FSM stubs drain the lpreuse arm's freed-block queue (empty
    // on every other arm — then exactly the old InvalidBlockNumber shape),
    // so reinserts land on REUSED line pointers through the product's
    // RelationGetBufferForTuple/PageAddItem path.
    freespace_seams::get_page_with_free_space::set(|_rel, _need| Ok(pop_reuse_block()));
    freespace_seams::record_and_get_page_with_free_space::set(|_rel, _old, _avail, _need| {
        Ok(pop_reuse_block())
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

    // Mid-run CreateCheckPoint (wal_level=replica) logs a running-xacts
    // snapshot for hot standby; the standby unit is absent in this rig and
    // crash recovery does not need the record. CreateCheckPoint ignores the
    // returned LSN.
    standby_seams::log_standby_snapshot::set(|| Ok(0));
}

fn install_real() {
    shmem::init_seams();
    guc_tables::init_seams();
    guc::init_seams();
    adt_bool::init_seams();
    adt_float::init_seams();
    transam_xlog::init_seams();
    heapam_visibility::init_seams();
    // inc-6 gin lane: gin's GUC storage (gin_pending_list_limit default 4MB
    // — far above this workload, so cleanup runs only where the rig calls
    // it). Inert for every other arm.
    gin::init_seams();
    // inc-6 lpreuse lane: the prune-record redo mutates pages through this
    // seam (heapam_xlog::heap_xlog_prune_freeze). The "heap-prune-keep" red
    // pre-installs a weakened wrapper (seams are set-once), so only install
    // the real one when the slot is still free. Inert without prune WAL.
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
    snapmgr::init_seams();
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
    // SIM HARNESS LAW (same as the wasm lane's wal_sync_method trap): an
    // O_DSYNC-style open_datasync arm would fold the WAL durability point
    // into an open flag SimVfs does not model — pin fdatasync (the port's
    // default, stamped explicitly through the product's own assign path)
    // so every commit's durability is an explicit vfs fdatasync the fault
    // model can see, gate and journal.
    transam_xlog::stamp_wal_sync_method(transam_xlog::WAL_SYNC_METHOD_FDATASYNC);

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
    sync::InitSync().unwrap();
    lmgr_proc::InitProcess(BackendType::Backend).unwrap();
    procarray::ProcArrayAdd(lmgr_proc::MyProc().unwrap()).unwrap();

    if resowner::CurrentResourceOwner().is_null() {
        let owner = resowner::ResourceOwnerCreate(
            types_resowner::ResourceOwner::NULL,
            "sim-crash-sweep",
        )
        .unwrap();
        resowner::SetCurrentResourceOwner(owner);
    }
}

fn int4_tupdesc<'mcx>(mcx: Mcx<'mcx>) -> Rc<TupleDescData<'mcx>> {
    let att = FormData_pg_attribute {
        attnum: 1,
        // inc-6: the AM lanes dispatch on the attr type (gin's array_ops
        // element comparator, brin minmax's strategy lookups); the heap
        // paths never consult it.
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
        relkind: RELKIND_RELATION,
        relhassubclass: false,
        relrowsecurity: false,
        relispopulated: true,
        relreplident: b'd',
        relispartition: false,
        relfrozenxid: 3,
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
        rd_lockInfo: LockInfoData {
            lockRelId: LockRelId { relId: oid, dbId: 5 },
        },
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

/// btint4cmp's C shape (pg_proc 351) — the FmgrInfo the rig installs into
/// rd_supportinfo is exactly what index_getprocinfo would build from the
/// catalog (the opclass/fmgr support wiring the sim harness lacked; the
/// M4 crash_recovery.rs precedent).
fn rig_btint4cmp(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut FunctionCallInfoBaseData,
) -> PgResult<datum::Datum> {
    let a = fcinfo.arg(0).as_i32();
    let b = fcinfo.arg(1).as_i32();
    Ok(datum::Datum::from_i32((a > b) as i32 - (a < b) as i32))
}

/// Shared index-relation shape over the rig's int4 heap column (the M4
/// crash_recovery.rs index_rel, parameterized for the inc-6 AM lanes).
fn rig_index_data<'mcx>(
    mcx: Mcx<'mcx>,
    oid: Oid,
    relam: Oid,
    opcintype: Oid,
    opfamily: Oid,
    options: Option<types_rel::RdOptions>,
) -> RelationData<'mcx> {
    let mut relname = NameData::default();
    relname.namestrcpy("t_idx");
    let one = |v: Oid| {
        let mut vec = PgVec::new_in(mcx);
        vec.push(v);
        vec
    };
    let mut indkey = PgVec::new_in(mcx);
    indkey.push(1i16);
    let mut indoption = PgVec::new_in(mcx);
    indoption.push(0i16);
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
        rd_lockInfo: LockInfoData {
            lockRelId: LockRelId { relId: oid, dbId: 5 },
        },
        rd_rel: FormData_pg_class {
            relname,
            relnamespace: 2200,
            reltype: 0,
            relowner: 10,
            relam,
            relfilenode: oid,
            reltablespace: 0,
            relpages: 0,
            reltuples: -1.0,
            relallvisible: 0,
            reltoastrelid: 0,
            relhasindex: false,
            relisshared: false,
            relpersistence: RELPERSISTENCE_PERMANENT,
            relkind: RELKIND_INDEX,
            relhassubclass: false,
            relrowsecurity: false,
            relispopulated: true,
            relreplident: b'd',
            relispartition: false,
            relfrozenxid: 3,
            relminmxid: 1,
        },
        rd_att: int4_tupdesc(mcx),
        rd_index: Some(FormData_pg_index {
            indexrelid: oid,
            indrelid: REL_OID,
            indnatts: 1,
            indnkeyatts: 1,
            indisunique: false,
            indnullsnotdistinct: false,
            indisprimary: false,
            indisexclusion: false,
            indimmediate: true,
            indisvalid: true,
            indisready: true,
            indkey,
            has_indpred: false,
            indexprs_src: None,
            indpred_src: None,
        }),
        rd_opcintype: one(opcintype),
        rd_opfamily: one(opfamily),
        rd_indoption: indoption,
        rd_indcollation: one(0),
        rd_options: options,
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

/// The btree index relation (int4_ops shape: opfamily 1976, opcintype 23,
/// support proc 1 = btint4cmp/351), M4 crash_recovery.rs's index_rel.
fn index_rel<'mcx>(mcx: Mcx<'mcx>) -> Relation<'mcx> {
    let data = rig_index_data(mcx, IDX_OID, types_core::BTREE_AM_OID, 23, 1976, None);
    let rel = Relation::open(data, Some(noop_close));
    rel.rd_supportinfo
        .borrow_mut()
        .push(Some(FmgrInfo::new(rig_btint4cmp, 351, 2, true, false)));
    rel
}

/// The gin index relation (array_ops-over-int4[] shape: opfamily 2745 /
/// opcintype anyarray 2277; extractValue resolves to ginarrayextract through
/// the lookup_pg_amproc stub, and the element comparator comes from the
/// index tupdesc's int4 attr — init_gin_col's closed set). rd_options None
/// keeps the C default fastupdate=ON: every gininsert rides the pending
/// list until ginInsertCleanup merges it.
fn gin_index_rel<'mcx>(mcx: Mcx<'mcx>) -> Relation<'mcx> {
    let data = rig_index_data(mcx, GIN_IDX_OID, types_core::GIN_AM_OID, 2277, 2745, None);
    Relation::open(data, Some(noop_close))
}

/// The brin index relation (int4 minmax shape: opfamily 4054 / opcintype 23;
/// opcinfo resolves to brin_minmax_opcinfo through the lookup_pg_amproc
/// stub and the strategy ladder through the amop/operator stubs).
/// pages_per_range=BR_PPR via the decoded reloptions enum.
fn brin_index_rel<'mcx>(mcx: Mcx<'mcx>) -> Relation<'mcx> {
    let data = rig_index_data(
        mcx,
        BRIN_IDX_OID,
        types_core::BRIN_AM_OID,
        23,
        4054,
        Some(types_rel::RdOptions::Brin(types_rel::BrinOptions {
            pages_per_range: BR_PPR as i32,
            autosummarize: false,
        })),
    );
    Relation::open(data, Some(noop_close))
}

/// Per-arm catalog-projection stubs (the M4 / executor-test shapes): gin
/// resolves extractValue through lookup_pg_amproc plus the int4 element
/// shape through lookup_pg_type_shape; brin resolves opcinfo
/// (F_BRIN_MINMAX_OPCINFO) plus the int4 btree operator ladder through the
/// pg_amop/pg_operator seams; lpreuse adds the single-backend prune horizon
/// (a deleting xid is removable iff it committed — no other snapshots
/// exist) and the freed-block routing that drives LP reuse.
fn install_arm_catalog_stubs(arm: &str) {
    match arm {
        "gin" => {
            syscache_seams::lookup_pg_amproc::set(|_of, _lt, _rt, procnum| {
                Ok(match procnum {
                    2 => 2743, // GIN_EXTRACTVALUE_PROC -> ginarrayextract (array_ops)
                    _ => types_core::InvalidOid, // array_ops: no compare/comparePartial
                })
            });
            syscache_seams::lookup_pg_type_shape::set(|typid| {
                assert_eq!(typid, types_core::INT4OID, "gin arm extracts int4 elements only");
                Ok(Some(types_tuple::PgTypeShape {
                    typlen: 4,
                    typbyval: true,
                    typalign: b'i' as i8,
                    typstorage: b'p' as i8,
                    typcollation: 0,
                }))
            });
        }
        "brin" => {
            syscache_seams::lookup_pg_amproc::set(|_of, _lt, _rt, procnum| {
                assert_eq!(procnum, 1, "brin resolves only BRIN_PROCNUM_OPCINFO here");
                Ok(types_brin::F_BRIN_MINMAX_OPCINFO)
            });
            // summarize_range's build scan probes system-relation-ness.
            namespace_seams::is_temp_toast_namespace::set(|_| false);
            // brin_build_desc's disk tupdesc consults the stored type shape.
            syscache_seams::lookup_pg_type_shape::set(|typid| {
                assert_eq!(typid, types_core::INT4OID, "brin arm stores int4 summaries only");
                Ok(Some(types_tuple::PgTypeShape {
                    typlen: 4,
                    typbyval: true,
                    typalign: b'i' as i8,
                    typstorage: b'p' as i8,
                    typcollation: 0,
                }))
            });
            // minmax_get_strategy_procinfo: strategy 1..5 -> the int4 btree
            // operators < <= = >= > -> procs int4lt/le/eq/ge/gt.
            syscache_seams::lookup_pg_amop_by_strategy::set(|_of, lt, rt, strategy| {
                assert_eq!((lt, rt), (types_core::INT4OID, types_core::INT4OID));
                Ok(match strategy {
                    1 => 97,  // <
                    2 => 523, // <=
                    3 => 96,  // =
                    4 => 525, // >=
                    5 => 521, // >
                    other => panic!("unexpected brin minmax strategy {other}"),
                })
            });
            syscache_seams::lookup_pg_operator_shape::set(|opno| {
                let oprcode = match opno {
                    97 => 66,   // int4lt
                    523 => 149, // int4le
                    96 => 65,   // int4eq
                    525 => 150, // int4ge
                    521 => 147, // int4gt
                    other => panic!("unexpected operator lookup {other}"),
                };
                Ok(Some(syscache_seams::PgOperatorShape {
                    oprnamespace: 11,
                    oprleft: types_core::INT4OID,
                    oprright: types_core::INT4OID,
                    oprresult: 16,
                    oprcom: 0,
                    oprnegate: 0,
                    oprcode,
                    oprrest: 0,
                    oprjoin: 0,
                    oprcanmerge: false,
                    oprcanhash: false,
                }))
            });
        }
        // lpreuse: no catalog stubs — the REAL procarray horizon machinery
        // (GlobalVisTestFor/IsRemovableXid, installed by procarray::
        // init_seams) serves the prune pass in this single-backend rig.
        _ => {}
    }
}

/// Parse a btree page opaque from a raw 8 KB page image.
/// PageHeaderData: pd_lsn 0..8, pd_checksum 8..10, pd_flags 10..12,
/// pd_lower 12..14, pd_upper 14..16, pd_special 16..18.
fn bt_opaque_of(page: &[u8]) -> BTPageOpaqueData {
    let special = u16::from_ne_bytes([page[16], page[17]]) as usize;
    let off = if special == 0 || special + 16 > page.len() {
        page.len() - 16
    } else {
        special
    };
    BTPageOpaqueData {
        btpo_prev: u32::from_ne_bytes(page[off..off + 4].try_into().unwrap()),
        btpo_next: u32::from_ne_bytes(page[off + 4..off + 8].try_into().unwrap()),
        btpo_level: u32::from_ne_bytes(page[off + 8..off + 12].try_into().unwrap()),
        btpo_flags: u16::from_ne_bytes(page[off + 12..off + 14].try_into().unwrap()),
        btpo_cycleid: u16::from_ne_bytes(page[off + 14..off + 16].try_into().unwrap()),
    }
}

/// (line-pointer count, item offset+len for a 1-based offset#) of a raw page.
fn raw_item(page: &[u8], off: usize) -> Option<(usize, usize)> {
    let pd_lower = u16::from_ne_bytes([page[12], page[13]]) as usize;
    let nitems = pd_lower.saturating_sub(24) / 4;
    if off == 0 || off > nitems {
        return None;
    }
    let raw = u32::from_ne_bytes(page[24 + (off - 1) * 4..24 + off * 4].try_into().unwrap());
    let lp_off = (raw & 0x7FFF) as usize;
    let lp_flags = (raw >> 15) & 3;
    let lp_len = (raw >> 17) as usize;
    if lp_flags != 1 {
        return None; // LP_NORMAL only
    }
    Some((lp_off, lp_len))
}

fn raw_max_offset(page: &[u8]) -> usize {
    let pd_lower = u16::from_ne_bytes([page[12], page[13]]) as usize;
    pd_lower.saturating_sub(24) / 4
}

/// Gin page opaque (rightlink, maxoff, flags) from a raw 8 KB page image —
/// GinPageOpaqueData lives in the 8-byte special area.
fn gin_opaque_of(page: &[u8]) -> (u32, u16, u16) {
    let off = BLCKSZ - 8;
    (
        u32::from_ne_bytes(page[off..off + 4].try_into().unwrap()),
        u16::from_ne_bytes(page[off + 4..off + 6].try_into().unwrap()),
        u16::from_ne_bytes(page[off + 6..off + 8].try_into().unwrap()),
    )
}

/// A gin IndexTuple's raw header + int4 key: (t_tid block word, t_tid posid,
/// t_info, key). Pending tuples carry the heap TID in t_tid; entry-tree
/// tuples overload it (block word: GIN_ITUP_COMPRESSED bit | posting byte
/// offset; posid: posting count, 0xffff = posting tree).
fn gin_raw_tuple(page: &[u8], lp_off: usize) -> (u32, u16, u16, i32) {
    let hi = u16::from_ne_bytes(page[lp_off..lp_off + 2].try_into().unwrap());
    let lo = u16::from_ne_bytes(page[lp_off + 2..lp_off + 4].try_into().unwrap());
    let posid = u16::from_ne_bytes(page[lp_off + 4..lp_off + 6].try_into().unwrap());
    let t_info = u16::from_ne_bytes(page[lp_off + 6..lp_off + 8].try_into().unwrap());
    let key = i32::from_ne_bytes(page[lp_off + 8..lp_off + 12].try_into().unwrap());
    (((hi as u32) << 16) | lo as u32, posid, t_info, key)
}

/// Independent decoder for gin compressed posting lists (ginpostinglist.c:
/// consecutive segments of {first TID (6B), nbytes (u16), varbyte-encoded
/// deltas of (block << 11 | posid) 43-bit words}, SHORTALIGNed) — raw-parse
/// on purpose, like the btree walker: the verifier must not lean on the
/// reader of the unit under test. `nitems`-driven like C's ginReadTuple
/// (the tuple tail may carry MAXALIGN padding past the last segment).
fn gin_decode_posting(data: &[u8], nitems: usize) -> Result<Vec<(u32, u16)>, String> {
    let mut out = Vec::new();
    let mut segoff = 0usize;
    while segoff < data.len() && out.len() < nitems {
        let seg = &data[segoff..];
        if seg.len() < 8 {
            return Err(format!("trailing segment header ({} bytes)", seg.len()));
        }
        let hi = u16::from_ne_bytes(seg[0..2].try_into().unwrap());
        let lo = u16::from_ne_bytes(seg[2..4].try_into().unwrap());
        let pos = u16::from_ne_bytes(seg[4..6].try_into().unwrap());
        let nbytes = u16::from_ne_bytes(seg[6..8].try_into().unwrap()) as usize;
        if 8 + nbytes > seg.len() {
            return Err(format!("segment nbytes {nbytes} overruns tuple"));
        }
        let first_blk = ((hi as u32) << 16) | lo as u32;
        let mut val = ((first_blk as u64) << 11) | pos as u64;
        out.push(((val >> 11) as u32, (val & 0x7FF) as u16));
        let payload = &seg[8..8 + nbytes];
        let mut p = 0usize;
        while p < nbytes {
            let mut delta = 0u64;
            let mut shift = 0u32;
            loop {
                if p >= nbytes {
                    return Err("truncated varbyte item".to_string());
                }
                let c = payload[p] as u64;
                p += 1;
                if shift == 42 {
                    delta |= c << 42;
                    break;
                }
                delta |= (c & 0x7F) << shift;
                if c & 0x80 == 0 {
                    break;
                }
                shift += 7;
            }
            val += delta;
            out.push(((val >> 11) as u32, (val & 0x7FF) as u16));
        }
        segoff += (8 + nbytes + 1) & !1usize; // SHORTALIGN'd segment size
    }
    if out.len() != nitems {
        return Err(format!("decoded {} items, tuple claims {nitems}", out.len()));
    }
    Ok(out)
}

/// An index tuple's (heap block, heap posid, int4 key) from a raw item.
fn raw_index_tuple(page: &[u8], lp_off: usize) -> (u32, u16, i32) {
    let hi = u16::from_ne_bytes(page[lp_off..lp_off + 2].try_into().unwrap());
    let lo = u16::from_ne_bytes(page[lp_off + 2..lp_off + 4].try_into().unwrap());
    let posid = u16::from_ne_bytes(page[lp_off + 4..lp_off + 6].try_into().unwrap());
    let key = i32::from_ne_bytes(page[lp_off + 8..lp_off + 12].try_into().unwrap());
    (((hi as u32) << 16) | lo as u32, posid, key)
}

fn make_checkpoint() -> controldata_utils::CheckPoint {
    let mut ckpt = controldata_utils::CheckPoint::ZEROED;
    ckpt.redo = CKPT_LOC;
    ckpt.ThisTimeLineID = 1;
    ckpt.PrevTimeLineID = 1;
    ckpt.fullPageWrites = true;
    ckpt.wal_level = WAL_LEVEL_REPLICA;
    ckpt.nextXid = types_core::FullTransactionId::from_epoch_and_xid(0, 3);
    ckpt.oldestXid = 3;
    // inc-6: carry the workload database's oid so the recover child's
    // StartupXLOG SetTransactionIdLimit call sees the same world the writer
    // stamps by hand (V5-O5 wraparound-noise fix).
    ckpt.oldestXidDB = 5;
    ckpt
}

/// The control-file image, built by the PRODUCT's controldata layout code,
/// written into the sim namespace.
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

/// Mint the datadir inside the sim namespace and boot the WRITER rig
/// (insert-capable XLogCtl, StartupXLOG skipped — the M4 writer shape).
/// Everything here happens BEFORE the fault plan installs: it is the
/// durable pre-history at every sweep point. Index arms also mint their
/// empty index file (the AM's C-shape build image — initdb-side artifact,
/// durable pre-history like the heap file): btree/lpreuse a bt metapage,
/// gin its metapage + empty root entry leaf, brin its metapage.
fn mint_and_boot_writer(arm: &str) {
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
    ctl.SharedRecoveryState
        .store(transam_xlog::RECOVERY_STATE_DONE, Relaxed);
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
        // SAFETY: single-threaded rig; ctl page buffers are XLOG_BLCKSZ.
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
        .store(types_core::FullTransactionId::from_epoch_and_xid(0, 3).value, Relaxed);
    // inc-6 (V5-O5 fix): the real boot path stamps the wraparound limits from
    // the checkpoint (StartupXLOG -> SetTransactionIdLimit); this hand-poked
    // writer boot skipped that since inc-2, leaving xidVacLimit/xidWarnLimit
    // at 0 so EVERY GetNewTransactionId printed the cosmetic "database must
    // be vacuumed within N transactions" WARNING. Same call, same inputs
    // (oldestXid=3, oldestXidDB=5 per the minted checkpoint's world); no
    // disk I/O, so packs and op traces are unaffected.
    varsup::SetTransactionIdLimit(3, 5).unwrap();
    subtrans::StartupSUBTRANS(3).unwrap();
    assert!(transam_xlog::XLogInsertAllowed());

    // The heap relation file (initdb-side artifact: exists before the sweep).
    smgr::smgropen(RLOC, INVALID_PROC_NUMBER).unwrap();
    smgr::smgrcreate(
        types_storage::RelFileLocatorBackend { locator: RLOC, backend: INVALID_PROC_NUMBER },
        ForkNumber::MAIN_FORKNUM,
        false,
    )
    .unwrap();

    #[repr(align(8))]
    struct P([u8; BLCKSZ]);
    let mint_index = |rloc: RelFileLocator, pages: &mut [P]| {
        let idx_key = types_storage::RelFileLocatorBackend {
            locator: rloc,
            backend: INVALID_PROC_NUMBER,
        };
        smgr::smgropen(rloc, INVALID_PROC_NUMBER).unwrap();
        smgr::smgrcreate(idx_key, ForkNumber::MAIN_FORKNUM, false).unwrap();
        for (b, p) in pages.iter().enumerate() {
            smgr::smgrextend(idx_key, ForkNumber::MAIN_FORKNUM, b as u32, &p.0, false)
                .unwrap();
        }
    };
    match arm {
        "btree" | "lpreuse" => {
            let mut p = P([0u8; BLCKSZ]);
            // SAFETY: aligned, exclusively owned stack page.
            let mut pm = unsafe {
                PageMut::from_raw(core::ptr::NonNull::new(p.0.as_mut_ptr()).unwrap())
            };
            nbtree::bt_initmetapage(&mut pm, BT_P_NONE, 0, false);
            mint_index(IDX_RLOC, &mut [p]);
        }
        "gin" => {
            // ginbuild's empty C-shape image: metapage + empty root entry
            // leaf (blocks 0 and 1).
            let mut meta = P([0u8; BLCKSZ]);
            gin::sim_rig::init_metapage_bytes(&mut meta.0);
            let mut root = P([0u8; BLCKSZ]);
            gin::sim_rig::init_page_bytes(&mut root.0, gin_vocab::GIN_LEAF);
            mint_index(GIN_IDX_RLOC, &mut [meta, root]);
        }
        "brin" => {
            // brinbuild's metapage image; revmap page 1 is created by the
            // PRODUCT's brinRevmapExtend inside the swept span (the
            // REVMAP_EXTEND cut class), not minted here.
            let mut meta = P([0u8; BLCKSZ]);
            // SAFETY: aligned, exclusively owned stack page.
            let mut pm = unsafe {
                PageMut::from_raw(core::ptr::NonNull::new(meta.0.as_mut_ptr()).unwrap())
            };
            brin_pageops::brin_metapage_init(&mut pm, BR_PPR, types_brin::BRIN_CURRENT_VERSION);
            mint_index(BRIN_IDX_RLOC, &mut [meta]);
        }
        _ => {}
    }

    // The mint is durable pre-history: fold every journal, promote every dir.
    sim_fsync_tree();
}

/// inc-4 arm (b): boot the WRITER over a REAL C-initdb datadir composed
/// into the sim namespace through the t28 provider seam —
/// `vfs::sim_boot::compose_boot_namespace` (process-shared universe,
/// durable-from-birth ingest, boot cwd = the datadir, SIM-ASSETS content
/// identity). Unlike the minted rig there is no XLogCtl hand-poke: the
/// PRODUCT's own StartupXLOG boots the composed image into insert mode.
/// Returns (datadir_abs, first workload xid).
fn boot_initdb_writer() -> (String, u32) {
    let dd = std::env::var(INITDB_ENV).expect("initdb arm needs the datadir env");
    // PGRUST_PGSHAREDIR: the orchestrator points it at an EMPTY dir — the
    // recovery entry point never consults share/timezone (the inc-2 COMPOSE
    // FINDING 1 scope fact); what this arm exercises is the seam's datadir
    // composition, and lean packs keep the sweep a local gate.
    assert!(
        std::env::var("PGRUST_PGSHAREDIR").is_ok(),
        "orchestrator must set PGRUST_PGSHAREDIR for the initdb arm"
    );
    let assets =
        vfs::sim_boot::compose_boot_namespace(&dd).expect("compose_boot_namespace failed");
    println!("{assets}");
    init_small::globals::SetDataDir(&dd);
    init_small::globals::set_enableFsync(true);
    install_stub_seams();
    install_real();

    transam_xlog::ReadControlFile().unwrap();
    transam_xlog::XLOGShmemInit();
    transam_xlog::StartupXLOG().unwrap();

    // The workload heap rel file (the rig's initdb-side artifact, as in the
    // minted arm; base/5 is the real postgres database directory).
    smgr::smgropen(RLOC, INVALID_PROC_NUMBER).unwrap();
    smgr::smgrcreate(
        types_storage::RelFileLocatorBackend { locator: RLOC, backend: INVALID_PROC_NUMBER },
        ForkNumber::MAIN_FORKNUM,
        false,
    )
    .unwrap();

    // Boot mutations (control update, end-of-recovery WAL) become durable
    // pre-history: the fault plan installs after this, so the sweep never
    // cuts into the boot itself.
    sim_fsync_tree();

    let next = procarray::TransamVariables().nextXid.load(Relaxed);
    (dd, (next & 0xFFFF_FFFF) as u32)
}

/// RED (recycle-window class): an OVER-EAGER RECYCLE — durably rename a
/// segment the last checkpoint's redo horizon still NEEDS to a future
/// segno (fd::durable_rename, parent-dir-fsync'd: no cut policy can undo
/// it), then cut. RemoveOldXlogFiles' horizon compare (`fname <= lastoff`)
/// is the product guard this bypasses from beneath; the sweep must flag
/// the unrecoverable image (property 1). If this red ever comes back
/// green, cuts in the recycle window have lost their teeth.
fn red_overeager_recycle() {
    let cf = *transam_xlog::control_file::control_file();
    let segno = cf.checkPointCopy.redo / SEG as u64;
    let name = transam_xlog::XLogFileName(1, segno, SEG);
    let future = transam_xlog::XLogFileName(1, segno + 7, SEG);
    let rc = fd::durable_rename(
        &format!("pg_wal/{name}"),
        &format!("pg_wal/{future}"),
        types_error::LOG,
    )
    .expect("red recycle durable_rename");
    assert_eq!(rc, 0, "red recycle durable_rename rc");
}

/// RED (recycled-reuse class): VALIDATING STALE RESIDUE. A correct recycle
/// leaves old-segment pages whose xlp_pageaddr/CRC guards reject them —
/// the green sweep's reuse window proves exactly that. The bug class this
/// red pins is residue that PASSES the guards: bytes at the exact
/// end-of-WAL whose header chain and CRC are valid. Plant a fully valid
/// commit record for a FUTURE txn's xid (a clog-prefix gap), durably,
/// beneath the product; cut. Recovery cannot distinguish it from real WAL
/// (WAL is self-describing) and replays it — the sweep's clog-prefix
/// property is the tooth that must catch the stale replay.
fn red_plant_validating_stale_record(gap_xid: u32) {
    const REC_LEN: usize = SizeOfXLogRecord + 2 + 8;
    let ctl = transam_xlog::ctl::XLogCtl();
    let insert_lsn =
        transam_xlog::XLogBytePosToRecPtr(ctl.Insert.CurrBytePos.load(Relaxed));
    let prev_lsn = transam_xlog::XLogBytePosToRecPtr(ctl.Insert.PrevBytePos.load(Relaxed));
    // Deterministic workload: the plant must sit mid-page (no page-header
    // interleaving) and within one segment.
    assert!(
        insert_lsn % 8192 != 0 && insert_lsn % 8192 + (REC_LEN as u64) <= 8192,
        "stale-residue plant would straddle a page boundary (insert_lsn={insert_lsn:#x})"
    );

    let mut rec = vec![0u8; REC_LEN];
    rec[0..4].copy_from_slice(&(REC_LEN as u32).to_ne_bytes());
    rec[4..8].copy_from_slice(&gap_xid.to_ne_bytes());
    rec[8..16].copy_from_slice(&prev_lsn.to_ne_bytes());
    rec[16] = xact::XLOG_XACT_COMMIT; // xl_info
    rec[17] = types_core::RmgrIds::RM_XACT_ID as u8; // xl_rmid
    rec[24] = 255; // XLR_BLOCK_ID_DATA_SHORT
    rec[25] = 8; // main data: xl_xact_commit { TimestampTz xact_time }
    rec[26..34].copy_from_slice(&777_000_000i64.to_ne_bytes());
    let crc = crc32c::fin_crc32c(crc32c::pg_comp_crc32c(
        crc32c::pg_comp_crc32c(crc32c::CRC32C_INIT, &rec[SizeOfXLogRecord..]),
        &rec[..20],
    ));
    rec[20..24].copy_from_slice(&crc.to_ne_bytes());

    let segno = insert_lsn / SEG as u64;
    let name = transam_xlog::XLogFileName(1, segno, SEG);
    let off = (insert_lsn % SEG as u64) as i64;
    // Raw vfs write + fsync: durable stale bytes planted beneath the
    // product's WAL layer (deliberately NOT pg_write/pg_fsync — the plant
    // models disk-borne residue, not engine activity).
    let fdno = vfs::open(&cpath(&format!("/pg_wal/{name}")), libc::O_RDWR, 0);
    assert!(fdno >= 0, "plant open: errno {}", vfs::get_errno());
    assert_eq!(vfs::pwrite(fdno, &rec, off), REC_LEN as isize);
    assert_eq!(vfs::fsync(fdno), 0);
    assert_eq!(vfs::close(fdno), 0);
}

/// RED (index lane): a LOST INDEX PAGE. The idx-stale writer uses ASCENDING
/// keys, so after the txn-3 checkpoint flushed the tree, txns 4-5 only touch
/// the RIGHT edge — the leftmost leaf's last modification predates the
/// checkpoint redo, so no post-checkpoint WAL (and no FPI) covers it. Zero
/// that leaf durably beneath the product and cut: recovery has nothing to
/// restore it from, and the sweep's index walk/coverage properties must flag
/// the loss. If this red ever comes back green, the index-lane cut points
/// stopped proving anything.
fn red_zero_leftmost_leaf() {
    let idx_path = format!("/base/5/{IDX_OID}");
    let read_blk =
        |b: u32| vfs_read_range(&idx_path, b as i64 * BLCKSZ as i64, BLCKSZ);
    // On-disk state = the txn-3 checkpoint image (no eviction at this scale):
    // a consistent tree whose left spine is final for this workload.
    let meta = read_blk(0);
    let hdr = 24usize; // SizeOfPageHeaderData: BTMetaPageData starts here
    let mut blk = u32::from_ne_bytes(meta[hdr + 8..hdr + 12].try_into().unwrap());
    let mut level = u32::from_ne_bytes(meta[hdr + 12..hdr + 16].try_into().unwrap());
    assert!(blk != BT_P_NONE, "idx-stale red needs a rooted on-disk tree");
    while level > 0 {
        let page = read_blk(blk);
        let opaque = bt_opaque_of(&page);
        assert_eq!(opaque.btpo_level, level, "on-disk left spine consistent");
        let first = if opaque.btpo_next == BT_P_NONE { 1 } else { 2 };
        let (off, _len) = raw_item(&page, first).expect("internal downlink item");
        let (child, _pos, _key) = raw_index_tuple(&page, off);
        blk = child;
        level -= 1;
    }
    let leaf = read_blk(blk);
    assert!(bt_opaque_of(&leaf).btpo_flags & BTP_LEAF != 0, "descend ends on a leaf");
    // Raw vfs write + fsync: durable page loss planted beneath the product's
    // WAL/buffer layers (disk-borne damage, not engine activity).
    let fdno = vfs::open(&cpath(&idx_path), libc::O_RDWR, 0);
    assert!(fdno >= 0, "idx-stale plant open: errno {}", vfs::get_errno());
    let zeros = vec![0u8; BLCKSZ];
    assert_eq!(
        vfs::pwrite(fdno, &zeros, blk as i64 * BLCKSZ as i64),
        BLCKSZ as isize
    );
    assert_eq!(vfs::fsync(fdno), 0);
    assert_eq!(vfs::close(fdno), 0);
    eprintln!("IDX_STALE_RED zeroed leftmost leaf block {blk}");
}

// ---------------------------------------------------------------------------
// WRITER child: workload through product paths, cut, pack the crash image
// ---------------------------------------------------------------------------

/// One committed transaction through the product commit protocol: inc-3
/// scale-up inserts ROWS_PER_TXN rows (multi-page heap; the WAL crosses
/// segments). Any Err is the engine stopping (the PANIC-equivalent for a
/// correct engine).
fn run_one_txn<'m>(
    mcx: Mcx<'m>,
    rel: &RelationData<'m>,
    tupdesc: &Rc<TupleDescData<'m>>,
    t: u32,
) -> PgResult<()> {
    xact::StartTransactionCommand()?;
    for _ in 0..ROWS_PER_TXN {
        let mut tup = heaptuple::heap_form_tuple(
            mcx,
            tupdesc,
            &[datum::Datum::from_i32(t as i32)],
            &[false],
        )?;
        heapam::heap_insert(rel, tup.as_tuple_mut(), 0, 0, None)?;
    }
    xact::CommitTransactionCommand()?;
    Ok(())
}

/// One committed btree-arm transaction: heap row + product btinsert per row
/// (insert txns), or the heap_delete of txn 1's rows (the delete txn).
fn run_one_btree_txn<'m>(
    mcx: Mcx<'m>,
    rel: &RelationData<'m>,
    idx: &Relation<'m>,
    tupdesc: &Rc<TupleDescData<'m>>,
    t: u32,
    keys: &str,
    txn1_tids: &mut Vec<ItemPointerData>,
) -> PgResult<()> {
    xact::StartTransactionCommand()?;
    if t == BT_DELETE_TXN {
        for tid in txn1_tids.iter() {
            let mut tmfd = tableam_vocab::TM_FailureData::default();
            let r = heapam::heap_delete(rel, tid, 0, None, true, &mut tmfd, false)?;
            assert!(
                matches!(r, tableam_vocab::TM_Result::TM_Ok),
                "single-backend heap_delete must succeed: {r:?}"
            );
        }
    } else {
        for i in 0..BT_ROWS {
            let key = bt_key(keys, t, i);
            let mut tup = heaptuple::heap_form_tuple(
                mcx,
                tupdesc,
                &[datum::Datum::from_i32(key)],
                &[false],
            )?;
            heapam::heap_insert(rel, tup.as_tuple_mut(), 0, 0, None)?;
            let tid = tup.as_tuple().t_self;
            if t == 1 {
                txn1_tids.push(tid);
            }
            let icx = MemoryContext::new("sim-sweep-btins");
            nbtree::btinsert(
                icx.mcx(),
                idx,
                &[datum::Datum::from_i32(key)],
                &[false],
                &tid,
                idx,
                types_nbtree::genam::IndexUniqueCheck::UNIQUE_CHECK_NO,
                false,
            )?;
        }
    }
    xact::CommitTransactionCommand()?;
    Ok(())
}

/// One committed gin-arm transaction: heap row + product gininsert of the
/// 1-element int4 array [value] per row. fastupdate is ON (rd_options None),
/// so every insert rides the PENDING LIST (ginHeapTupleFastInsert:
/// INSERT_LISTPAGE / UPDATE_META_PAGE WAL).
fn run_one_gin_txn<'m>(
    mcx: Mcx<'m>,
    rel: &RelationData<'m>,
    idx: &Relation<'m>,
    tupdesc: &Rc<TupleDescData<'m>>,
    t: u32,
) -> PgResult<()> {
    xact::StartTransactionCommand()?;
    for i in 0..GIN_ROWS {
        let v = gin_val(t, i);
        let mut tup = heap_form_tuple_i32(mcx, tupdesc, v)?;
        heapam::heap_insert(rel, tup.as_tuple_mut(), 0, 0, None)?;
        let tid = tup.as_tuple().t_self;
        let icx = MemoryContext::new("sim-sweep-ginins");
        let arr = arrayfuncs::construct::construct_array(
            icx.mcx(),
            &[datum::Datum::from_i32(v)],
            types_core::INT4OID,
            4,
            true,
            b'i',
        )?;
        let ad = datum::Datum::from_usize(arr.as_ptr() as usize);
        gin::gininsert(icx.mcx(), idx, &[ad], &[false], &tid, idx)?;
    }
    xact::CommitTransactionCommand()?;
    Ok(())
}

/// One committed brin-arm transaction: heap row + product brininsert per
/// row. Rows in unsummarized ranges no-op (C semantics); rows landing on a
/// summarized boundary page WIDEN its summary (SAMEPAGE_UPDATE per row).
/// The amcache (C ii_AmCache) lives per txn and is released through the
/// product brininsertcleanup — its revmap holds a pinned metapage buffer.
fn run_one_brin_txn<'m>(
    mcx: Mcx<'m>,
    rel: &RelationData<'m>,
    idx: &Relation<'m>,
    tupdesc: &Rc<TupleDescData<'m>>,
    t: u32,
) -> PgResult<()> {
    xact::StartTransactionCommand()?;
    {
        let mut amcache: Option<types_brin::BrinInsertState<'m>> = None;
        for i in 0..BR_ROWS {
            let v = brin_val(t, i);
            let mut tup = heap_form_tuple_i32(mcx, tupdesc, v)?;
            heapam::heap_insert(rel, tup.as_tuple_mut(), 0, 0, None)?;
            let tid = tup.as_tuple().t_self;
            brin::brininsert(
                mcx,
                idx,
                &[datum::Datum::from_i32(v)],
                &[false],
                &tid,
                &mut amcache,
            )?;
        }
        brin::brininsertcleanup(&mut amcache)?;
    }
    xact::CommitTransactionCommand()?;
    Ok(())
}

/// One committed lpreuse-arm transaction: ascending-key insert txns (heap +
/// btinsert), the range-delete txn (txns 3-4's rows — contiguous keys =
/// whole leaves), or the reinsert txns (disjoint new-key range; the heap
/// rows land in REUSED line pointers via targblock + the freespace stubs).
fn run_one_lpreuse_txn<'m>(
    mcx: Mcx<'m>,
    rel: &RelationData<'m>,
    idx: &Relation<'m>,
    tupdesc: &Rc<TupleDescData<'m>>,
    t: u32,
    del_tids: &mut Vec<ItemPointerData>,
) -> PgResult<()> {
    xact::StartTransactionCommand()?;
    if t == LP_DELETE_TXN {
        for tid in del_tids.iter() {
            let mut tmfd = tableam_vocab::TM_FailureData::default();
            let r = heapam::heap_delete(rel, tid, 0, None, true, &mut tmfd, false)?;
            assert!(
                matches!(r, tableam_vocab::TM_Result::TM_Ok),
                "single-backend heap_delete must succeed: {r:?}"
            );
        }
    } else {
        for i in 0..LP_ROWS {
            let key = lp_key(t, i);
            let mut tup = heap_form_tuple_i32(mcx, tupdesc, key)?;
            heapam::heap_insert(rel, tup.as_tuple_mut(), 0, 0, None)?;
            let tid = tup.as_tuple().t_self;
            if t == 3 || t == 4 {
                del_tids.push(tid);
            }
            let icx = MemoryContext::new("sim-sweep-lpins");
            nbtree::btinsert(
                icx.mcx(),
                idx,
                &[datum::Datum::from_i32(key)],
                &[false],
                &tid,
                idx,
                types_nbtree::genam::IndexUniqueCheck::UNIQUE_CHECK_NO,
                false,
            )?;
        }
    }
    xact::CommitTransactionCommand()?;
    Ok(())
}

/// Flush the WAL tail through the product XLogFlush — the walwriter's job in
/// C. The inc-6 wrapper steps (summarize/desummarize/vacuum/cleanup) run in
/// xid-less txns whose commits flush nothing, so without this their records
/// would only persist under later txns' flushes and the dense step windows
/// would carry no durability ops to cut on.
fn flush_wal_tail() -> PgResult<()> {
    let lsn = transam_xlog::XLogBytePosToRecPtr(
        transam_xlog::ctl::XLogCtl().Insert.CurrBytePos.load(Relaxed),
    );
    transam_xlog::XLogFlush(lsn)
}

fn heap_form_tuple_i32<'m>(
    mcx: Mcx<'m>,
    tupdesc: &Rc<TupleDescData<'m>>,
    v: i32,
) -> PgResult<heaptuple::HeapTuple<'m>> {
    heaptuple::heap_form_tuple(mcx, tupdesc, &[datum::Datum::from_i32(v)], &[false])
}

#[test]
#[ignore]
fn sim_sweep_writer_child() {
    if std::env::var(ROLE_ENV).as_deref() != Ok("writer") {
        return;
    }
    let k: u64 = std::env::var(K_ENV).unwrap().parse().unwrap();
    let pack = std::path::PathBuf::from(std::env::var(PACK_ENV).unwrap());
    let red = match std::env::var(RED_ENV).as_deref() {
        Ok("0") | Err(_) => String::new(),
        Ok(v) => v.to_string(),
    };
    let arm = std::env::var(ARM_ENV).unwrap_or_default();

    SimVfs::reset();
    let (datadir, base_xid) = if arm == "initdb" {
        boot_initdb_writer()
    } else {
        mint_and_boot_writer(&arm);
        ("/".to_string(), 3)
    };
    install_arm_catalog_stubs(&arm);

    // inc-5 arms: torn-write and EMFILE specs ride their own envs so the
    // sweep's k naming stays the crash-at-op contract.
    let torn_spec = std::env::var(TORN_ENV).ok().filter(|s| !s.is_empty());
    let emfile_spec = std::env::var(EMFILE_ENV).ok().filter(|s| !s.is_empty());
    // Index-arm key pattern: the idx-stale red uses ascending keys (see
    // red_zero_leftmost_leaf); everything else the bijective hash scramble.
    let keys = if red == "idx-stale" { "asc" } else { "hash" };

    // inc-3 WHOLE-NODE KILL: the cut kills the node — every post-cut vfs op
    // the engine's error/unwind paths issue is refused without mutation, so
    // the packed image is the PURE at-cut image (no unwind residue).
    SimVfs::set_kill_on_cut(true);
    if red == "fsync" {
        // PRODUCT-SHAPED RED ARM: disable the product's fsync layer through
        // its own knob (fd::pg_fsync and issue_xlog_fsync both consult
        // enableFsync). Every commit will claim durability it never bought.
        init_small::globals::set_enableFsync(false);
        SimVfs::set_crash_image(CrashImage::DropAll);
    } else if red == "fpw-torn" {
        // inc-5 RED (torn-write class): the PRODUCT's full-page-writes knob
        // OFF (XLogInsert's doPageWrites consults Insert.fullPageWrites), so
        // no FPI protects torn data pages — then a heap-page flush inside
        // the first checkpoint's write wave (the spec is computed by the
        // orchestrator from the green baseline: heap-class write counts are
        // FPW-independent) tears mid-page. Recovery has no base image to
        // restore; the fold property must catch the damage.
        transam_xlog::ctl::XLogCtl()
            .Insert
            .fullPageWrites
            .store(false, Relaxed);
        let spec = torn_spec.clone().expect("fpw-torn red needs a TORN spec");
        let mut it = spec.split(':');
        assert_eq!(it.next(), Some("heap"), "fpw-torn tears heap pages");
        let j: u64 = it.next().unwrap().parse().unwrap();
        let p: usize = it.next().unwrap().parse().unwrap();
        SeededFaultPlan::install(
            per_point_seed(0xF937_0000 ^ j),
            vec![FaultRule::nth_matching(
                OpMatch {
                    kinds: Some(vec![OpKind::PWriteV]),
                    class: Some(PathClass::Heap),
                    path_contains: None,
                },
                j,
                FaultDecision::TornWrite { persist_prefix: p },
            )],
        );
    } else if red == "emfile-ack" {
        // inc-5 RED (EMFILE class): descriptor exhaustion from the 8th open
        // on; the buggy-server model below ACKS the failing transaction
        // anyway — the sweep must flag the acked loss.
        SeededFaultPlan::install(
            per_point_seed(0xEACC),
            vec![FaultRule {
                matcher: OpMatch {
                    kinds: Some(vec![OpKind::Open]),
                    class: None,
                    path_contains: None,
                },
                nth: 8,
                action: FaultDecision::Errno(libc::EMFILE),
                sticky: true,
            }],
        );
    } else if red.is_empty() && torn_spec.is_some() {
        // "<heap|wal>:<j>:<p>": the j-th data write of that class crashes
        // MID-WRITE keeping a p-byte prefix (sector-floored by the engine).
        let spec = torn_spec.clone().unwrap();
        let mut it = spec.split(':');
        let class = match it.next().unwrap() {
            "wal" => PathClass::Wal,
            _ => PathClass::Heap,
        };
        let j: u64 = it.next().unwrap().parse().unwrap();
        let p: usize = it.next().unwrap().parse().unwrap();
        SeededFaultPlan::install(
            per_point_seed(0x7093_0000 ^ j),
            vec![FaultRule::nth_matching(
                OpMatch {
                    kinds: Some(vec![OpKind::PWriteV]),
                    class: Some(class),
                    path_contains: None,
                },
                j,
                FaultDecision::TornWrite { persist_prefix: p },
            )],
        );
    } else if red.is_empty() && emfile_spec.is_some() {
        // "<once|sticky>:<j>": the j-th Open fails with EMFILE (sticky: that
        // one and every later open — the exhaustion regime).
        let spec = emfile_spec.clone().unwrap();
        let mut it = spec.split(':');
        let sticky = it.next().unwrap() == "sticky";
        let j: u64 = it.next().unwrap().parse().unwrap();
        SeededFaultPlan::install(
            per_point_seed(0xEF11_0000 ^ j),
            vec![FaultRule {
                matcher: OpMatch {
                    kinds: Some(vec![OpKind::Open]),
                    class: None,
                    path_contains: None,
                },
                nth: j,
                action: FaultDecision::Errno(libc::EMFILE),
                sticky,
            }],
        );
    } else if red.is_empty() && k > 0 {
        SeededFaultPlan::install(per_point_seed(k), vec![FaultRule::crash_at_op(k)]);
    } else if red.is_empty() {
        // Baseline: record the op trace — the sweep stratifier reads it.
        SimVfs::set_op_trace(true);
    }

    let ops_at_start = SimVfs::op_seq();

    let ctx = MemoryContext::new("sim_sweep_writer");
    let mcx = ctx.mcx();
    let rel = test_relation(mcx, REL_OID);
    let tupdesc = int4_tupdesc(mcx);
    let idx = if arm == "btree" || arm == "lpreuse" {
        Some(index_rel(mcx))
    } else {
        None
    };
    let gin_idx = if arm == "gin" { Some(gin_index_rel(mcx)) } else { None };
    let brin_idx = if arm == "brin" { Some(brin_index_rel(mcx)) } else { None };
    let mut txn1_tids: Vec<ItemPointerData> = Vec::new();
    let mut lp_del_tids: Vec<ItemPointerData> = Vec::new();
    let mut vac_span: Option<(u64, u64)> = None;
    let mut vac_removed: i64 = -1;
    // inc-6 arm windows for the stratifiers: (name, lo, hi) op spans.
    let mut windows: Vec<(&'static str, u64, u64)> = Vec::new();
    let mut lp_pages_deleted: i64 = -1;

    // The recycle-class reds stop the workload at a deterministic horizon:
    // "recycle-needed" right after the recycling checkpoint (txn 8), the
    // stale plant with acked txns 1..7 so the planted txn-10 commit is a
    // clog-prefix GAP the checker must flag. The idx-stale red stops after
    // txn 5 — the leftmost leaf's content is then pre-checkpoint history.
    let txn_limit = match red.as_str() {
        "recycle-needed" => 8,
        "stale-residue" => 7,
        "idx-stale" => 5,
        // inc-6 weakened-redo red positioning (the writers stay GREEN; the
        // weakening lives in the recover child): gin stops pre-cleanup so
        // the pending list is the only index state; brin stops after the
        // widening txn; lpreuse runs the whole lifecycle.
        "gin-pending" => 3,
        "brin-stale" => BR_NEG_TXN,
        "lpr-stale" => LP_TXNS,
        _ if arm == "initdb" => INITDB_TXNS,
        _ if arm == "btree" => BT_TXNS,
        _ if arm == "gin" => GIN_TXNS,
        _ if arm == "brin" => BR_TXNS,
        _ if arm == "lpreuse" => LP_TXNS,
        _ => TXNS,
    };
    let ckpt_after: &[u32] = match arm.as_str() {
        "btree" => &BT_CKPT_AFTER,
        "gin" => &GIN_CKPT_AFTER,
        "brin" => &BR_CKPT_AFTER,
        "lpreuse" => &LP_CKPT_AFTER,
        _ => &CKPT_AFTER,
    };

    let mut acked: u32 = 0;
    let mut stopped: Option<String> = None;
    for t in 1..=txn_limit {
        let r = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| match arm.as_str() {
            "btree" => run_one_btree_txn(
                mcx,
                &rel,
                idx.as_ref().unwrap(),
                &tupdesc,
                t,
                keys,
                &mut txn1_tids,
            ),
            "gin" => run_one_gin_txn(mcx, &rel, gin_idx.as_ref().unwrap(), &tupdesc, t),
            "brin" => run_one_brin_txn(mcx, &rel, brin_idx.as_ref().unwrap(), &tupdesc, t),
            "lpreuse" => run_one_lpreuse_txn(
                mcx,
                &rel,
                idx.as_ref().unwrap(),
                &tupdesc,
                t,
                &mut lp_del_tids,
            ),
            _ => run_one_txn(mcx, &rel, &tupdesc, t),
        }));
        match r {
            Ok(Ok(())) => {
                // Commit returned success to the "client": ACKED — even if a
                // cut fired inside the call. If the record is not durable the
                // property harness flags it (that would be a real finding).
                acked += 1;
            }
            Ok(Err(e)) => {
                if red == "emfile-ack" {
                    // The buggy-server model: reports success to the client
                    // although the transaction failed under fd pressure.
                    acked += 1;
                    stopped = Some(format!("txn {t} error acked anyway: {e:?}"));
                } else {
                    stopped = Some(format!("txn {t} error: {e:?}"));
                }
                break;
            }
            Err(_) => {
                if red == "emfile-ack" {
                    // The buggy-server model acks even an engine-stop txn.
                    acked += 1;
                    stopped = Some(format!("txn {t} panicked, acked anyway"));
                } else {
                    stopped = Some(format!("txn {t} panicked (engine stop)"));
                }
                break;
            }
        }
        if SimVfs::cut_count() > 0 {
            break; // power is gone; do not touch the image any further
        }
        // inc-6 GIN LANE: the product pending-list merge
        // (gin_clean_pending_list's exact call shape) — DELETE_LISTPAGE +
        // entry-tree INSERT/SPLIT records; its op span is a dense cut window.
        if arm == "gin" && GIN_CLEAN_AFTER.contains(&t) {
            let lo = SimVfs::op_seq() - ops_at_start + 1;
            let gidx = gin_idx.as_ref().unwrap();
            let r = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| -> PgResult<()> {
                xact::StartTransactionCommand()?;
                let vcx = MemoryContext::new("sim-sweep-ginclean");
                let state = gin::build::initGinState(gidx)?;
                gin::ginInsertCleanup(vcx.mcx(), gidx, &state, true, false, true, None)?;
                flush_wal_tail()?;
                xact::CommitTransactionCommand()?;
                Ok(())
            }));
            let name: &'static str = if t == GIN_CLEAN_AFTER[0] { "clean1" } else { "clean2" };
            windows.push((name, lo, SimVfs::op_seq() - ops_at_start));
            match r {
                Ok(Ok(())) => {}
                Ok(Err(e)) => {
                    stopped = Some(format!("gin cleanup after txn {t} error: {e:?}"));
                    break;
                }
                Err(_) => {
                    stopped = Some(format!("gin cleanup after txn {t} panicked (engine stop)"));
                    break;
                }
            }
            if SimVfs::cut_count() > 0 {
                break;
            }
        }
        // inc-6 BRIN LANE: the product summarize pass (the
        // brin_summarize_new_values shape; include_partial so the boundary
        // range is summarized and txn 3 can WIDEN it), and after the second
        // pass the range-0 DESUMMARIZE (the invalidation class). Runs
        // BEFORE the checkpoint block: the txn-2 checkpoint must land
        // after sum1 so txn 3's samepage updates are inside replay.
        if arm == "brin" && BR_SUM_AFTER.contains(&t) {
            let bidx = brin_idx.as_ref().unwrap();
            let lo = SimVfs::op_seq() - ops_at_start + 1;
            let r = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| -> PgResult<()> {
                xact::StartTransactionCommand()?;
                let scx = MemoryContext::new("sim-sweep-brinsum");
                let heap = Relation::open(test_relation(scx.mcx(), REL_OID), Some(noop_close));
                let mut nsum = 0f64;
                brin_build::brinsummarize(
                    scx.mcx(),
                    bidx,
                    &heap,
                    InvalidBlockNumber, // BRIN_ALL_BLOCKRANGES
                    true,
                    Some(&mut nsum),
                    None,
                )?;
                flush_wal_tail()?;
                xact::CommitTransactionCommand()?;
                Ok(())
            }));
            let name: &'static str = if t == BR_SUM_AFTER[0] { "sum1" } else { "sum2" };
            windows.push((name, lo, SimVfs::op_seq() - ops_at_start));
            match r {
                Ok(Ok(())) => {}
                Ok(Err(e)) => {
                    stopped = Some(format!("brin summarize after txn {t} error: {e:?}"));
                    break;
                }
                Err(_) => {
                    stopped =
                        Some(format!("brin summarize after txn {t} panicked (engine stop)"));
                    break;
                }
            }
            if SimVfs::cut_count() > 0 {
                break;
            }
            if t == BR_SUM_AFTER[1] {
                let lo = SimVfs::op_seq() - ops_at_start + 1;
                let r =
                    std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| -> PgResult<()> {
                        xact::StartTransactionCommand()?;
                        let desummarized = brin_pageops::brinRevmapDesummarizeRange(bidx, 0)?;
                        assert!(desummarized, "range 0 was summarized by sum1");
                        flush_wal_tail()?;
                        xact::CommitTransactionCommand()?;
                        Ok(())
                    }));
                windows.push(("desum", lo, SimVfs::op_seq() - ops_at_start));
                match r {
                    Ok(Ok(())) => {}
                    Ok(Err(e)) => {
                        stopped = Some(format!("brin desummarize error: {e:?}"));
                        break;
                    }
                    Err(_) => {
                        stopped = Some("brin desummarize panicked (engine stop)".to_string());
                        break;
                    }
                }
                if SimVfs::cut_count() > 0 {
                    break;
                }
            }
        }
        // inc-6 VACUUM-CONTENT LANE: after the acked range-delete commit, the
        // product btbulkdelete (item removal + _bt_pagedel on the emptied
        // leaves) then heap_page_prune_and_freeze(MARK_UNUSED_NOW) over the
        // freed heap pages — index entries go FIRST (the C invariant that
        // makes LP_UNUSED safe), then line pointers, then the freed blocks
        // feed the reuse queue. One dense "vac" window spans it all.
        if arm == "lpreuse" && t == LP_DELETE_TXN {
            let lo = SimVfs::op_seq() - ops_at_start + 1;
            let idx_ref = idx.as_ref().unwrap();
            let r = std::panic::catch_unwind(std::panic::AssertUnwindSafe(
                || -> PgResult<(i64, i64)> {
                    xact::StartTransactionCommand()?;
                    let mut dead = lp_del_tids.clone();
                    dead.sort_by(|a, b| ItemPointerCompare(a, b).cmp(&0));
                    let info = nbtree::IndexVacuumInfo {
                        index: idx_ref,
                        heaprel: &rel,
                        analyze_only: false,
                        estimated_count: false,
                        num_heap_tuples: -1.0,
                        strategy: None,
                    };
                    let vcx = MemoryContext::new("sim-sweep-lpvac");
                    let stats = nbtree::btbulkdelete(vcx.mcx(), &info, None, &dead)?;
                    let mut blocks: Vec<u32> = dead
                        .iter()
                        .map(types_tuple::itemptr::ItemPointerGetBlockNumber)
                        .collect();
                    blocks.sort_unstable();
                    blocks.dedup();
                    for b in &blocks {
                        let buf = bufmgr::ReadBuffer(&rel, *b)?;
                        bufmgr_seams::lock_buffer::call(
                            buf,
                            bufmgr_seams::BUFFER_LOCK_EXCLUSIVE,
                        )?;
                        let mut presult = pruneheap::PruneFreezeResult::default();
                        let mut off_loc: types_core::OffsetNumber = 0;
                        // The REAL product horizon (procarray GlobalVisTestFor):
                        // single backend, every delete already committed —
                        // honestly removable.
                        let vistest = procarray_seams::global_vis_test_for::call(&rel);
                        pruneheap::heap_page_prune_and_freeze(
                            &rel,
                            buf,
                            vistest,
                            pruneheap::HEAP_PAGE_PRUNE_MARK_UNUSED_NOW,
                            None,
                            &mut presult,
                            pruneheap::PruneReason::PruneVacuumScan,
                            &mut off_loc,
                            None,
                            None,
                        )?;
                        bufmgr_seams::lock_buffer::call(
                            buf,
                            bufmgr_seams::BUFFER_LOCK_UNLOCK,
                        )?;
                        bufmgr::ReleaseBuffer(buf)?;
                        REUSE_BLOCKS.with(|q| q.borrow_mut().push_back(*b));
                    }
                    flush_wal_tail()?;
                    xact::CommitTransactionCommand()?;
                    Ok((stats.tuples_removed as i64, stats.pages_deleted as i64))
                },
            ));
            windows.push(("vac", lo, SimVfs::op_seq() - ops_at_start));
            match r {
                Ok(Ok((removed, pagedel))) => {
                    vac_removed = removed;
                    lp_pages_deleted = pagedel;
                }
                Ok(Err(e)) => {
                    stopped = Some(format!("lpreuse vacuum error: {e:?}"));
                    break;
                }
                Err(_) => {
                    stopped = Some("lpreuse vacuum panicked (engine stop)".to_string());
                    break;
                }
            }
            if SimVfs::cut_count() > 0 {
                break;
            }
        }
        if ckpt_after.contains(&t) {
            // The PRODUCT's checkpoint: CheckPointGuts (clog/subtrans/buffer
            // flushes + sync requests) and the in-place control update. The
            // checkpointer's buffer pins need a live owner (C: the aux
            // process resource owner); commits null the current one out.
            if resowner::CurrentResourceOwner().is_null() {
                let owner = resowner::ResourceOwnerCreate(
                    types_resowner::ResourceOwner::NULL,
                    "sim-sweep-ckpt",
                )
                .unwrap();
                resowner::SetCurrentResourceOwner(owner);
            }
            let r = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                transam_xlog::CreateCheckPoint(
                    CHECKPOINT_IMMEDIATE | CHECKPOINT_FORCE | CHECKPOINT_WAIT,
                )
            }));
            match r {
                Ok(Ok(_)) => {}
                Ok(Err(e)) => {
                    stopped = Some(format!("checkpoint after txn {t} error: {e:?}"));
                    break;
                }
                Err(_) => {
                    stopped = Some(format!("checkpoint after txn {t} panicked (engine stop)"));
                    break;
                }
            }
            if SimVfs::cut_count() > 0 {
                break;
            }
        }
        // inc-5 INDEX LANE: the product vacuum entry (btbulkdelete) right
        // after the deleting txn's ACKED commit + checkpoint — the C
        // invariant (vacuum only removes tuples whose deleting xid is
        // durably committed) holds at every later cut by construction. Its
        // op span is the VACUUM cut-class window (_bt_delitems_vacuum WAL +
        // page writes), recorded in the meta for the stratifier.
        if arm == "btree" && t == BT_DELETE_TXN && red.is_empty() {
            let vac_lo = SimVfs::op_seq() - ops_at_start + 1;
            let idx_ref = idx.as_ref().unwrap();
            let r = std::panic::catch_unwind(std::panic::AssertUnwindSafe(
                || -> PgResult<types_nbtree::genam::IndexBulkDeleteResult> {
                    xact::StartTransactionCommand()?;
                    let mut dead = txn1_tids.clone();
                    dead.sort_by(|a, b| ItemPointerCompare(a, b).cmp(&0));
                    let info = nbtree::IndexVacuumInfo {
                        index: idx_ref,
                        heaprel: &rel,
                        analyze_only: false,
                        estimated_count: false,
                        num_heap_tuples: -1.0,
                        strategy: None,
                    };
                    let vcx = MemoryContext::new("sim-sweep-btvac");
                    let stats = nbtree::btbulkdelete(vcx.mcx(), &info, None, &dead)?;
                    xact::CommitTransactionCommand()?;
                    Ok(stats)
                },
            ));
            match r {
                Ok(Ok(stats)) => {
                    vac_removed = stats.tuples_removed as i64;
                    vac_span = Some((vac_lo, SimVfs::op_seq() - ops_at_start));
                }
                Ok(Err(e)) => {
                    vac_span = Some((vac_lo, SimVfs::op_seq() - ops_at_start));
                    stopped = Some(format!("btree vacuum error: {e:?}"));
                    break;
                }
                Err(_) => {
                    vac_span = Some((vac_lo, SimVfs::op_seq() - ops_at_start));
                    stopped = Some(format!("btree vacuum panicked (engine stop)"));
                    break;
                }
            }
            if SimVfs::cut_count() > 0 {
                break;
            }
        }
    }

    let ops_used = SimVfs::op_seq() - ops_at_start;
    match red.as_str() {
        "recycle-needed" if SimVfs::cut_count() == 0 => {
            red_overeager_recycle();
            SimVfs::cut();
        }
        "stale-residue" if SimVfs::cut_count() == 0 => {
            // Plant a commit for txn 10's xid: acked ends at txn 7, so the
            // replayed plant is a clog-prefix gap (txns 8 and 9 missing).
            red_plant_validating_stale_record(base_xid + 9);
            SimVfs::cut();
        }
        "fsync" if SimVfs::cut_count() == 0 => {
            // Power loss after the "successful" run: the red arm's cut.
            SimVfs::cut();
        }
        "idx-stale" if SimVfs::cut_count() == 0 => {
            red_zero_leftmost_leaf();
            SimVfs::cut();
        }
        "emfile-ack" if SimVfs::cut_count() == 0 => {
            SimVfs::cut();
        }
        // inc-6 weakened-redo red positioning: a GREEN writer, power loss at
        // the end — the deliberate bug is armed in the RECOVER child.
        "gin-pending" | "brin-stale" | "lpr-stale" if SimVfs::cut_count() == 0 => {
            SimVfs::cut();
        }
        _ => {}
    }
    // The EMFILE battery's power loss at end-of-regime: the recover child
    // must find every acked txn intact — degraded-but-never-corrupt.
    if emfile_spec.is_some() && red.is_empty() && SimVfs::cut_count() == 0 {
        SimVfs::cut();
    }

    // Pack the post-crash image (durable == volatile after a cut) plus meta.
    let _ = std::fs::remove_dir_all(&pack);
    std::fs::create_dir_all(pack.join("root")).unwrap();
    export_sim_tree(&pack.join("root"));
    let mut meta = String::new();
    meta.push_str(&format!("k={k}\n"));
    meta.push_str(&format!("seed={:#x}\n", per_point_seed(k)));
    meta.push_str(&format!("arm={}\n", if arm.is_empty() { "minted" } else { &arm }));
    meta.push_str(&format!("datadir={datadir}\n"));
    meta.push_str(&format!("base_xid={base_xid}\n"));
    meta.push_str(&format!("acked={acked}\n"));
    meta.push_str(&format!("ops={ops_used}\n"));
    meta.push_str(&format!("cuts={}\n", SimVfs::cut_count()));
    meta.push_str(&format!("killed={}\n", SimVfs::killed()));
    meta.push_str(&format!("frozen={}\n", SimVfs::frozen_op_count()));
    meta.push_str(&format!("stopped={}\n", stopped.as_deref().unwrap_or("-")));
    meta.push_str(&format!("keys={keys}\n"));
    if let Some(spec) = &torn_spec {
        meta.push_str(&format!("torn={spec}\n"));
    }
    if let Some(spec) = &emfile_spec {
        meta.push_str(&format!("emfile={spec}\n"));
    }
    if let Some((lo, hi)) = vac_span {
        meta.push_str(&format!("vac_lo={lo}\n"));
        meta.push_str(&format!("vac_hi={hi}\n"));
        meta.push_str(&format!("vac_removed={vac_removed}\n"));
    }
    for (name, lo, hi) in &windows {
        meta.push_str(&format!("win={name}:{lo}:{hi}\n"));
    }
    if arm == "lpreuse" && vac_removed >= 0 {
        meta.push_str(&format!("lp_vac_removed={vac_removed}\n"));
        meta.push_str(&format!("lp_pages_deleted={lp_pages_deleted}\n"));
        // Freed blocks the reinserts did NOT consume (0 = full LP reuse).
        let left = REUSE_BLOCKS.with(|q| q.borrow().len());
        meta.push_str(&format!("lp_reuse_left={left}\n"));
    }
    for l in SimVfs::fault_log() {
        meta.push_str(&format!("faultlog: {l}\n"));
    }
    // Baseline op trace (k=0 only), rebased to plan-relative op numbers so
    // the orchestrator's stratifier maps lines straight to sweep ks.
    for l in SimVfs::op_trace() {
        let seq: u64 = l
            .split_whitespace()
            .find_map(|w| w.strip_prefix("seq="))
            .and_then(|s| s.parse().ok())
            .unwrap_or(0);
        if seq > ops_at_start {
            meta.push_str(&format!("optrace: k={} {l}\n", seq - ops_at_start));
        }
    }
    std::fs::write(pack.join("meta.txt"), meta).unwrap();
    println!(
        "SIM_WRITER_DONE k={k} acked={acked} ops={ops_used} cuts={} frozen={}",
        SimVfs::cut_count(),
        SimVfs::frozen_op_count()
    );
}

// ---------------------------------------------------------------------------
// RECOVER child: import the crash image, boot the PRODUCT recovery, verify
// ---------------------------------------------------------------------------

fn mvcc_snapshot<'m>(mcx: Mcx<'m>, base_xid: u32) -> SnapshotData<'m> {
    let mut s = SnapshotData::sentinel(mcx, SnapshotType::SNAPSHOT_MVCC);
    // Every workload xid (base_xid..) is in the past: visibility = did it
    // commit. base_xid-relative so the initdb arm's real xid range (the
    // datadir's post-boot nextXid) is covered too.
    s.xmin = base_xid + 1000;
    s.xmax = base_xid + 1000;
    s.regd_count.set(1);
    s
}

fn page_tuple(page_addr: *mut u8, off: u16) -> HeapTupleData<'static> {
    // SAFETY: pinned buffer page, held across the visibility check.
    let page = unsafe {
        types_storage::bufpage::PageRef::from_raw(
            core::ptr::NonNull::new(page_addr).unwrap(),
        )
    };
    let id = page.item_id(off);
    let (ptr, len) = page.item_raw(id);
    // SAFETY: in-page image under the caller's pin.
    unsafe { HeapTupleData::from_raw_parts(ptr, len, ItemPointerData::new(0, off), REL_OID) }
}

#[test]
#[ignore]
fn sim_sweep_recover_child() {
    if std::env::var(ROLE_ENV).as_deref() != Ok("recover") {
        return;
    }
    let pack = std::path::PathBuf::from(std::env::var(PACK_ENV).unwrap());
    let meta = std::fs::read_to_string(pack.join("meta.txt")).unwrap();
    let get = |key: &str| -> String {
        meta.lines()
            .find(|l| l.starts_with(&format!("{key}=")))
            .map(|l| l.split('=').nth(1).unwrap().to_string())
            .unwrap()
    };
    let acked: u32 = get("acked").parse().unwrap();
    let base_xid: u32 = get("base_xid").parse().unwrap();
    let arm = get("arm");
    let writer_dd = get("datadir");
    let k = get("k");
    let seed = get("seed");
    // Index-arm key pattern (absent in pre-inc-5 metas: default hash).
    let keys = meta
        .lines()
        .find(|l| l.starts_with("keys="))
        .map(|l| l.split('=').nth(1).unwrap().to_string())
        .unwrap_or_else(|| "hash".to_string());
    let tag = format!("k={k} seed={seed}");
    let mut violations: Vec<String> = Vec::new();

    SimVfs::reset();
    if arm == "initdb" {
        // The post-crash datadir sits inside the pack at the writer's host
        // absolute path; compose it through the SAME provider seam the
        // writer used (fresh process-shared universe, boot cwd = the pack
        // datadir, SIM-ASSETS = the post-crash composition identity).
        let dd = format!("{}{}", pack.join("root").to_str().unwrap(), writer_dd);
        let assets =
            vfs::sim_boot::compose_boot_namespace(&dd).expect("recover compose failed");
        println!("{assets}");
        init_small::globals::SetDataDir(&dd);
    } else {
        import_tree_into_sim(&pack.join("root"), "/");
        init_small::globals::SetDataDir("/");
    }
    init_small::globals::set_enableFsync(true);
    // inc-6 weakened-REDO reds: the deliberate bug lives HERE, in replay —
    // the writer ran green. Three arms flip the sim-cfg hooks inside the
    // rmgr redo crates (the table itself is a static — no seam); the heap
    // arm claims the prune-execute seam FIRST (seams are set-once;
    // install_real skips its default when the slot is taken) with a wrapper
    // that DROPS the record's LP_UNUSED transitions — a lost vacuum-content
    // redo with no product hook at all.
    match std::env::var(REDO_RED_ENV).as_deref() {
        Ok("gin-listpage") => {
            gin_xlog::sim_red::SKIP_LISTPAGE_CONTENT.store(true, Relaxed);
        }
        Ok("brin-narrow") => {
            brin_xlog::sim_red::KEEP_STALE_SUMMARY.store(true, Relaxed);
        }
        Ok("btvac-keep") => {
            nbtree_xlog::sim_red::KEEP_VACUUMED_ITEMS.store(true, Relaxed);
        }
        Ok("heap-prune-keep") => {
            pruneheap_seams::heap_page_prune_execute::set(
                |buffer, lp_truncate_only, redirected, nowdead, _nowunused| {
                    pruneheap::heap_page_prune_execute(
                        buffer,
                        lp_truncate_only,
                        redirected,
                        nowdead,
                        &[],
                    )
                },
            );
        }
        _ => {}
    }
    install_stub_seams();
    install_real();
    install_arm_catalog_stubs(&arm);

    // The PRODUCT boot path over the crash image. A failure anywhere here is
    // a recovery-completeness violation (property 1) — a FINDING.
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
        // A corrupt recovered image may make the verifiers themselves panic
        // (garbage tuple headers under the visibility check, torn pages
        // under the walks): that is a property VIOLATION, not a harness
        // crash — catch it and say so.
        let panicked = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let cf = *transam_xlog::control_file::control_file();
        if cf.state != DB_IN_PRODUCTION {
            violations.push(format!("{tag}: post-recovery control state {}", cf.state));
        }

        // Property 3 scaffold: clog-committed txns must be a PREFIX 1..=h
        // (commits are strictly sequential in this workload). Txn t's xid is
        // base_xid + t - 1 (3 + t - 1 on the minted rig; the real datadir's
        // post-boot nextXid on the initdb arm).
        let mut h: u32 = 0;
        let mut prefix_ok = true;
        let arm_txns = match arm.as_str() {
            "initdb" => INITDB_TXNS,
            "btree" => BT_TXNS,
            "gin" => GIN_TXNS,
            "brin" => BR_TXNS,
            "lpreuse" => LP_TXNS,
            _ => TXNS,
        };
        for t in 1..=arm_txns {
            let xid = base_xid + t - 1;
            let committed = transam::TransactionIdDidCommit(xid).unwrap_or(false);
            if committed {
                if t != h + 1 {
                    prefix_ok = false;
                    violations.push(format!(
                        "{tag}: clog commit gap — txn {t} committed but txn {} is not",
                        h + 1
                    ));
                }
                h = t;
            }
        }

        // Property 2: every ACKED txn survives (commit ack = WAL flush OK).
        if prefix_ok && h < acked {
            for t in (h + 1)..=acked {
                violations.push(format!(
                    "{tag}: ACKED txn {t} lost (recovered horizon {h})"
                ));
            }
        }

        // Property 3: the MVCC-visible heap content equals EXACTLY the fold
        // of the clog-committed prefix — uncommitted absent, nothing torn.
        let ctx = MemoryContext::new("sim_sweep_verify");
        let mcx = ctx.mcx();
        let rel = test_relation(mcx, REL_OID);
        smgr::smgropen(RLOC, INVALID_PROC_NUMBER).unwrap();
        let key = types_storage::RelFileLocatorBackend {
            locator: RLOC,
            backend: INVALID_PROC_NUMBER,
        };
        let nblocks = smgr::smgrnblocks(key, ForkNumber::MAIN_FORKNUM).unwrap();
        let snap = mvcc_snapshot(mcx, base_xid);
        let mut visible: Vec<i32> = Vec::new();
        // brin arm: (heap block, value) pairs — the consistent-or-wider
        // coverage check needs each row's RANGE.
        let mut visible_pos: Vec<(u32, i32)> = Vec::new();
        for b in 0..nblocks {
            let buf = bufmgr::ReadBuffer(&rel, b).unwrap();
            let page_addr = bufmgr::BufferGetPagePtr(buf).as_ptr();
            // SAFETY: pinned page image.
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
                let mut t = page_tuple(page_addr, off);
                let vis = heapam_visibility_seams::heap_tuple_satisfies_visibility::call(
                    &mut t, &snap, buf,
                )
                .unwrap();
                if vis {
                    let (ptr, _len) = page.item_raw(id);
                    // SAFETY: heap tuple in-page: t_hoff byte at offset 22,
                    // int4 datum right after the (aligned) header.
                    let val = unsafe {
                        let hoff = *ptr.add(22) as usize;
                        ptr.add(hoff).cast::<i32>().read_unaligned()
                    };
                    visible.push(val);
                    if arm == "brin" {
                        visible_pos.push((b, val));
                    }
                }
            }
            bufmgr::ReleaseBuffer(buf).unwrap();
        }
        visible.sort_unstable();
        let expected: Vec<i32> = match arm.as_str() {
            "btree" => {
                // btree-arm fold: keys of every committed insert txn; the
                // delete txn inserts nothing, and once it is committed txn
                // 1's rows are deleted (invisible).
                let mut v = Vec::new();
                for t in 1..=h {
                    if t == BT_DELETE_TXN || (t == 1 && h >= BT_DELETE_TXN) {
                        continue;
                    }
                    for i in 0..BT_ROWS {
                        v.push(bt_key(&keys, t, i));
                    }
                }
                v.sort_unstable();
                v
            }
            "gin" => {
                let mut v = Vec::new();
                for t in 1..=h {
                    for i in 0..GIN_ROWS {
                        v.push(gin_val(t, i));
                    }
                }
                v.sort_unstable();
                v
            }
            "brin" => {
                let mut v = Vec::new();
                for t in 1..=h {
                    for i in 0..BR_ROWS {
                        v.push(brin_val(t, i));
                    }
                }
                v.sort_unstable();
                v
            }
            "lpreuse" => {
                // The delete txn (5) inserts nothing; once it commits, txns
                // 3-4 are gone; txns 6-7 are the reinsert generation.
                let mut v = Vec::new();
                for t in 1..=h {
                    if t == LP_DELETE_TXN || ((t == 3 || t == 4) && h >= LP_DELETE_TXN) {
                        continue;
                    }
                    for i in 0..LP_ROWS {
                        v.push(lp_key(t, i));
                    }
                }
                v.sort_unstable();
                v
            }
            _ => (1..=h as i32)
                .flat_map(|t| std::iter::repeat(t).take(ROWS_PER_TXN as usize))
                .collect(),
        };
        if visible != expected {
            let mut msg = format!(
                "{tag}: visible heap fold diverges — {} visible rows vs {} expected \
                 (h={h}, {ROWS_PER_TXN}/txn)",
                visible.len(),
                expected.len()
            );
            if let Some((i, (g, w))) = visible
                .iter()
                .zip(expected.iter())
                .enumerate()
                .find(|(_, (g, w))| g != w)
            {
                msg.push_str(&format!("; first divergence at row {i}: got {g} want {w}"));
            }
            violations.push(msg);
            // inc-6 vacuum-content CLASSIFICATION (lpreuse arm): the two
            // silent-loss classes get NAMED. Old and new key ranges are
            // disjoint by construction, so a multiset diff attributes every
            // divergent value.
            if arm == "lpreuse" {
                let mut count: std::collections::BTreeMap<i32, i64> =
                    std::collections::BTreeMap::new();
                for v in &visible {
                    *count.entry(*v).or_insert(0) += 1;
                }
                for v in &expected {
                    *count.entry(*v).or_insert(0) -= 1;
                }
                let deleted_lo = lp_key(3, 0);
                let deleted_hi = lp_key(4, LP_ROWS - 1);
                for (v, c) in count {
                    if c > 0 && h >= LP_DELETE_TXN && v >= deleted_lo && v <= deleted_hi {
                        violations.push(format!(
                            "{tag}: OLD TUPLE RESURRECTED — deleted-range value {v} visible \
                             after the delete txn committed ({c} extra)"
                        ));
                    } else if c < 0 && v < 0 {
                        violations.push(format!(
                            "{tag}: NEW TUPLE LOST — reinserted value {v} missing from the \
                             visible fold ({} lost)",
                            -c
                        ));
                    }
                }
            }
        }

        // inc-5 INDEX LANE properties (btree arm): walk the recovered tree
        // (descend the left spine, then the leaf right-link chain) and prove
        //  (a) the walk is structurally sound (no zeroed/non-leaf pages, no
        //      loops),
        //  (b) leaf keys are nondecreasing along the chain,
        //  (c) every index entry's TID resolves to a real heap item, visible
        //      entries carry the heap value as their key, and the multiset
        //      of visible-index keys equals EXACTLY the visible heap fold —
        //      the index covers every committed row and nothing else.
        // inc-6: the lpreuse arm shares the btree index and inherits the
        // walk/coverage properties unchanged; with reused line pointers the
        // key-vs-heap-value check is the vacuum-content tooth (a stale entry
        // resolves to a reused slot holding a different row).
        let mut idx_visible: Vec<i32> = Vec::new();
        if arm == "btree" || arm == "lpreuse" {
            let idx = index_rel(mcx);
            let idx_key = types_storage::RelFileLocatorBackend {
                locator: IDX_RLOC,
                backend: INVALID_PROC_NUMBER,
            };
            smgr::smgropen(IDX_RLOC, INVALID_PROC_NUMBER).unwrap();
            let idx_nblocks = smgr::smgrnblocks(idx_key, ForkNumber::MAIN_FORKNUM).unwrap();
            let read_page = |b: u32| -> Vec<u8> {
                let buf = bufmgr::ReadBuffer(&idx, b).unwrap();
                let ptr = bufmgr::BufferGetPagePtr(buf).as_ptr();
                // SAFETY: pinned BLCKSZ page image, copied under the pin.
                let v = unsafe { core::slice::from_raw_parts(ptr as *const u8, BLCKSZ) }
                    .to_vec();
                bufmgr::ReleaseBuffer(buf).unwrap();
                v
            };
            let meta_pg = read_page(0);
            let hdr = 24usize;
            let root = u32::from_ne_bytes(meta_pg[hdr + 8..hdr + 12].try_into().unwrap());
            let mut walk_ok = true;
            let mut entries: Vec<(i32, u32, u16)> = Vec::new();
            if root != BT_P_NONE {
                // Descend the left spine to the leftmost leaf.
                let mut blk = root;
                let mut hops = 0u32;
                loop {
                    hops += 1;
                    if hops > idx_nblocks + 2 {
                        violations.push(format!("{tag}: index descend loops (block {blk})"));
                        walk_ok = false;
                        break;
                    }
                    let page = read_page(blk);
                    let op = bt_opaque_of(&page);
                    if op.btpo_level == 0 {
                        break;
                    }
                    let first = if op.btpo_next == BT_P_NONE { 1 } else { 2 };
                    match raw_item(&page, first) {
                        Some((off, _len)) => {
                            let (child, _pos, _key) = raw_index_tuple(&page, off);
                            blk = child;
                        }
                        None => {
                            violations.push(format!(
                                "{tag}: index descend broken at block {blk} (no downlink)"
                            ));
                            walk_ok = false;
                            break;
                        }
                    }
                }
                // Leaf chain walk.
                if walk_ok {
                    let mut steps = 0u32;
                    let mut last_key = i64::MIN;
                    let mut cur = blk;
                    while cur != BT_P_NONE {
                        steps += 1;
                        if steps > idx_nblocks * 2 {
                            violations
                                .push(format!("{tag}: index leaf chain loops at block {cur}"));
                            walk_ok = false;
                            break;
                        }
                        let page = read_page(cur);
                        let op = bt_opaque_of(&page);
                        if op.btpo_flags & (BTP_DELETED | BTP_HALF_DEAD) == 0 {
                            if op.btpo_flags & BTP_LEAF == 0 {
                                violations.push(format!(
                                    "{tag}: index leaf chain hit non-leaf block {cur} \
                                     (flags {:#x})",
                                    op.btpo_flags
                                ));
                                walk_ok = false;
                                break;
                            }
                            let first = if op.btpo_next == BT_P_NONE { 1 } else { 2 };
                            for off in first..=raw_max_offset(&page) {
                                let Some((lp, _len)) = raw_item(&page, off) else {
                                    continue;
                                };
                                let (hblk, hpos, key) = raw_index_tuple(&page, lp);
                                if (key as i64) < last_key {
                                    violations.push(format!(
                                        "{tag}: index key order broken at block {cur} \
                                         off {off} ({key} after {last_key})"
                                    ));
                                }
                                last_key = key as i64;
                                entries.push((key, hblk, hpos));
                            }
                        }
                        cur = op.btpo_next;
                    }
                }
            }
            // Project the entries through heap visibility.
            if walk_ok {
                for (key, hblk, hpos) in entries {
                    if hblk >= nblocks {
                        violations.push(format!(
                            "{tag}: index entry {key} points past heap end \
                             (block {hblk} of {nblocks})"
                        ));
                        continue;
                    }
                    let buf = bufmgr::ReadBuffer(&rel, hblk).unwrap();
                    let page_addr = bufmgr::BufferGetPagePtr(buf).as_ptr();
                    // SAFETY: pinned page image.
                    let page = unsafe {
                        types_storage::bufpage::PageRef::from_raw(
                            core::ptr::NonNull::new(page_addr).unwrap(),
                        )
                    };
                    let id = page.item_id(hpos);
                    if !id.is_normal() {
                        violations.push(format!(
                            "{tag}: index entry {key} points at missing heap item \
                             ({hblk},{hpos})"
                        ));
                        bufmgr::ReleaseBuffer(buf).unwrap();
                        continue;
                    }
                    let mut t = page_tuple(page_addr, hpos);
                    let vis = heapam_visibility_seams::heap_tuple_satisfies_visibility::call(
                        &mut t, &snap, buf,
                    )
                    .unwrap();
                    if vis {
                        let (ptr, _len) = page.item_raw(id);
                        // SAFETY: heap tuple in-page (see the fold walk).
                        let val = unsafe {
                            let hoff = *ptr.add(22) as usize;
                            ptr.add(hoff).cast::<i32>().read_unaligned()
                        };
                        if val != key {
                            violations.push(format!(
                                "{tag}: index key {key} != heap value {val} at \
                                 ({hblk},{hpos})"
                            ));
                        }
                        idx_visible.push(key);
                    }
                    bufmgr::ReleaseBuffer(buf).unwrap();
                }
                idx_visible.sort_unstable();
                if idx_visible != expected {
                    violations.push(format!(
                        "{tag}: index coverage diverges — {} visible-index keys vs {} \
                         expected (h={h})",
                        idx_visible.len(),
                        expected.len()
                    ));
                }
            }
        }

        // inc-6 GIN LANE properties: walk the PENDING LIST (metapage
        // head/tail chain of GIN_LIST pages; tuples carry the heap TID in
        // t_tid) and the ENTRY TREE (left-spine descend from the fixed root
        // block 1, then the leaf right-link chain; leaf tuples carry
        // compressed posting lists), prove structural soundness + key order,
        // and project every (key, TID) through heap visibility: the SET of
        // visible index items must fold to EXACTLY the visible heap multiset.
        // SET semantics on purpose — a cut inside ginInsertCleanup legally
        // leaves an item in BOTH the pending list and the tree (the list
        // delete follows the tree inserts; C dedups at scan level).
        if arm == "gin" {
            let idx = gin_index_rel(mcx);
            let idx_key = types_storage::RelFileLocatorBackend {
                locator: GIN_IDX_RLOC,
                backend: INVALID_PROC_NUMBER,
            };
            smgr::smgropen(GIN_IDX_RLOC, INVALID_PROC_NUMBER).unwrap();
            let idx_nblocks = smgr::smgrnblocks(idx_key, ForkNumber::MAIN_FORKNUM).unwrap();
            let read_page = |b: u32| -> Vec<u8> {
                let buf = bufmgr::ReadBuffer(&idx, b).unwrap();
                let ptr = bufmgr::BufferGetPagePtr(buf).as_ptr();
                // SAFETY: pinned BLCKSZ page image, copied under the pin.
                let v = unsafe { core::slice::from_raw_parts(ptr as *const u8, BLCKSZ) }
                    .to_vec();
                bufmgr::ReleaseBuffer(buf).unwrap();
                v
            };
            let mut walk_ok = true;
            let mut entries: std::collections::BTreeSet<(i32, u32, u16)> =
                std::collections::BTreeSet::new();

            let meta_pg = read_page(0);
            if gin_opaque_of(&meta_pg).2 & gin_vocab::GIN_META == 0 {
                violations.push(format!("{tag}: gin metapage lost its GIN_META flag"));
                walk_ok = false;
            }
            let head = u32::from_ne_bytes(meta_pg[24..28].try_into().unwrap());
            let tail = u32::from_ne_bytes(meta_pg[28..32].try_into().unwrap());
            if (head == InvalidBlockNumber) != (tail == InvalidBlockNumber) {
                violations.push(format!(
                    "{tag}: gin pending head/tail disagree ({head:#x}/{tail:#x})"
                ));
                walk_ok = false;
            }
            // Pending-list chain walk.
            let mut cur = if walk_ok { head } else { InvalidBlockNumber };
            let mut steps = 0u32;
            while cur != InvalidBlockNumber {
                steps += 1;
                if steps > idx_nblocks * 2 {
                    violations.push(format!("{tag}: gin pending chain loops at block {cur}"));
                    walk_ok = false;
                    break;
                }
                let page = read_page(cur);
                let (rl, _mo, flags) = gin_opaque_of(&page);
                if flags & gin_vocab::GIN_LIST == 0 {
                    violations.push(format!(
                        "{tag}: gin pending chain hit non-list block {cur} (flags {flags:#x})"
                    ));
                    walk_ok = false;
                    break;
                }
                for off in 1..=raw_max_offset(&page) {
                    let Some((lp, _len)) = raw_item(&page, off) else {
                        continue;
                    };
                    let (blkword, posid, t_info, key) = gin_raw_tuple(&page, lp);
                    if t_info & 0x8000 != 0 {
                        violations.push(format!(
                            "{tag}: NULL-marked gin pending tuple at ({cur},{off})"
                        ));
                        continue;
                    }
                    entries.insert((key, blkword, posid));
                }
                if rl == InvalidBlockNumber && cur != tail {
                    violations.push(format!(
                        "{tag}: gin pending chain ends at {cur} but meta tail is {tail}"
                    ));
                }
                cur = rl;
            }
            // Entry-tree walk: left-spine descend from the fixed root.
            if walk_ok {
                let mut blk = 1u32; // GIN_ROOT_BLKNO
                let mut hops = 0u32;
                loop {
                    hops += 1;
                    if hops > idx_nblocks + 2 {
                        violations
                            .push(format!("{tag}: gin entry descend loops (block {blk})"));
                        walk_ok = false;
                        break;
                    }
                    let page = read_page(blk);
                    let (_rl, _mo, flags) = gin_opaque_of(&page);
                    if flags & gin_vocab::GIN_LEAF != 0 {
                        break;
                    }
                    if flags
                        & (gin_vocab::GIN_DATA | gin_vocab::GIN_LIST | gin_vocab::GIN_DELETED)
                        != 0
                    {
                        violations.push(format!(
                            "{tag}: gin entry descend hit wrong page kind at {blk} \
                             (flags {flags:#x})"
                        ));
                        walk_ok = false;
                        break;
                    }
                    match raw_item(&page, 1) {
                        Some((lp, _len)) => {
                            let (child, _posid, _ti, _k) = gin_raw_tuple(&page, lp);
                            blk = child;
                        }
                        None => {
                            violations.push(format!(
                                "{tag}: gin entry descend broken at block {blk} (no downlink)"
                            ));
                            walk_ok = false;
                            break;
                        }
                    }
                }
                // Entry leaf chain walk.
                if walk_ok {
                    let mut steps = 0u32;
                    let mut last_key = i64::MIN;
                    let mut cur = blk;
                    while cur != InvalidBlockNumber {
                        steps += 1;
                        if steps > idx_nblocks * 2 {
                            violations.push(format!(
                                "{tag}: gin entry leaf chain loops at block {cur}"
                            ));
                            walk_ok = false;
                            break;
                        }
                        let page = read_page(cur);
                        let (rl, _mo, flags) = gin_opaque_of(&page);
                        if flags & gin_vocab::GIN_DELETED != 0 {
                            cur = rl;
                            continue;
                        }
                        if flags & gin_vocab::GIN_LEAF == 0
                            || flags & (gin_vocab::GIN_DATA | gin_vocab::GIN_LIST) != 0
                        {
                            violations.push(format!(
                                "{tag}: gin entry leaf chain hit wrong page kind at {cur} \
                                 (flags {flags:#x})"
                            ));
                            walk_ok = false;
                            break;
                        }
                        for off in 1..=raw_max_offset(&page) {
                            let Some((lp, len)) = raw_item(&page, off) else {
                                continue;
                            };
                            let (blkword, nposting, t_info, key) = gin_raw_tuple(&page, lp);
                            if t_info & 0x8000 != 0 {
                                violations.push(format!(
                                    "{tag}: NULL-marked gin entry tuple at ({cur},{off})"
                                ));
                                continue;
                            }
                            if nposting == 0xffff {
                                violations.push(format!(
                                    "{tag}: unexpected posting TREE at ({cur},{off}) — this \
                                     workload never grows one"
                                ));
                                continue;
                            }
                            if (key as i64) < last_key {
                                violations.push(format!(
                                    "{tag}: gin entry key order broken at block {cur} off \
                                     {off} ({key} after {last_key})"
                                ));
                            }
                            last_key = key as i64;
                            if blkword & 0x8000_0000 == 0 {
                                violations.push(format!(
                                    "{tag}: uncompressed gin posting at ({cur},{off})"
                                ));
                                continue;
                            }
                            let postoff = (blkword & 0x7FFF_FFFF) as usize;
                            let size = (t_info & 0x1FFF) as usize;
                            if postoff > size || size > len {
                                violations.push(format!(
                                    "{tag}: gin posting offsets out of range at ({cur},{off})"
                                ));
                                continue;
                            }
                            match gin_decode_posting(
                                &page[lp + postoff..lp + size],
                                nposting as usize,
                            ) {
                                Ok(items) => {
                                    for (hblk, hpos) in items {
                                        entries.insert((key, hblk, hpos));
                                    }
                                }
                                Err(e) => violations.push(format!(
                                    "{tag}: gin posting decode failed at ({cur},{off}): {e}"
                                )),
                            }
                        }
                        cur = rl;
                    }
                }
            }
            // Project the deduped items through heap visibility (the inc-5
            // coverage property, generalized).
            if walk_ok {
                for (key, hblk, hpos) in entries {
                    if hblk >= nblocks {
                        violations.push(format!(
                            "{tag}: gin item {key} points past heap end \
                             (block {hblk} of {nblocks})"
                        ));
                        continue;
                    }
                    let buf = bufmgr::ReadBuffer(&rel, hblk).unwrap();
                    let page_addr = bufmgr::BufferGetPagePtr(buf).as_ptr();
                    // SAFETY: pinned page image.
                    let page = unsafe {
                        types_storage::bufpage::PageRef::from_raw(
                            core::ptr::NonNull::new(page_addr).unwrap(),
                        )
                    };
                    let id = page.item_id(hpos);
                    if !id.is_normal() {
                        violations.push(format!(
                            "{tag}: gin item {key} points at missing heap item ({hblk},{hpos})"
                        ));
                        bufmgr::ReleaseBuffer(buf).unwrap();
                        continue;
                    }
                    let mut t = page_tuple(page_addr, hpos);
                    let vis = heapam_visibility_seams::heap_tuple_satisfies_visibility::call(
                        &mut t, &snap, buf,
                    )
                    .unwrap();
                    if vis {
                        let (ptr, _len) = page.item_raw(id);
                        // SAFETY: heap tuple in-page (see the fold walk).
                        let val = unsafe {
                            let hoff = *ptr.add(22) as usize;
                            ptr.add(hoff).cast::<i32>().read_unaligned()
                        };
                        if val != key {
                            violations.push(format!(
                                "{tag}: gin key {key} != heap value {val} at ({hblk},{hpos})"
                            ));
                        }
                        idx_visible.push(key);
                    }
                    bufmgr::ReleaseBuffer(buf).unwrap();
                }
                idx_visible.sort_unstable();
                if idx_visible != expected {
                    violations.push(format!(
                        "{tag}: gin index coverage diverges — {} visible-index keys vs {} \
                         expected (h={h})",
                        idx_visible.len(),
                        expected.len()
                    ));
                }
            }
        }

        // inc-6 BRIN LANE property — CONSISTENT-OR-WIDER: a lossy index may
        // over-include but never exclude. For every VISIBLE heap row, its
        // block's range must be unsummarized (revmap TID unset / revmap page
        // never created / desummarized), a PLACEHOLDER (must-scan), or carry
        // a summary whose [min,max] includes the value. Structure checks are
        // raw (metapage magic/version/ppr, page types, revmap bounds); the
        // summary decode rides the product brin_deform_tuple over the
        // stub-resolved BrinDesc.
        if arm == "brin" {
            let idx = brin_index_rel(mcx);
            let idx_key = types_storage::RelFileLocatorBackend {
                locator: BRIN_IDX_RLOC,
                backend: INVALID_PROC_NUMBER,
            };
            smgr::smgropen(BRIN_IDX_RLOC, INVALID_PROC_NUMBER).unwrap();
            let idx_nblocks = smgr::smgrnblocks(idx_key, ForkNumber::MAIN_FORKNUM).unwrap();
            let read_page = |b: u32| -> Vec<u8> {
                let buf = bufmgr::ReadBuffer(&idx, b).unwrap();
                let ptr = bufmgr::BufferGetPagePtr(buf).as_ptr();
                // SAFETY: pinned BLCKSZ page image, copied under the pin.
                let v = unsafe { core::slice::from_raw_parts(ptr as *const u8, BLCKSZ) }
                    .to_vec();
                bufmgr::ReleaseBuffer(buf).unwrap();
                v
            };
            let meta_pg = read_page(0);
            let magic = u32::from_ne_bytes(meta_pg[24..28].try_into().unwrap());
            let version = u32::from_ne_bytes(meta_pg[28..32].try_into().unwrap());
            let ppr = u32::from_ne_bytes(meta_pg[32..36].try_into().unwrap());
            let last_revmap = u32::from_ne_bytes(meta_pg[36..40].try_into().unwrap());
            if magic != 0xA810_9CFA || version != 1 || ppr != BR_PPR {
                violations.push(format!(
                    "{tag}: brin metapage corrupt (magic {magic:#x} version {version} \
                     ppr {ppr})"
                ));
            } else {
                match brin::brin_build_desc(mcx, &idx) {
                    Err(e) => violations
                        .push(format!("{tag}: brin_build_desc failed on recovery: {e:?}")),
                    Ok(bdesc) => {
                        let mut dtup = brin_tuple::brin_new_memtuple(&bdesc);
                        let revmap_items = ((BLCKSZ - 24 - 8) / 6) as u32;
                        let mut ranges: Vec<u32> =
                            visible_pos.iter().map(|(b, _)| *b / BR_PPR).collect();
                        ranges.sort_unstable();
                        ranges.dedup();
                        // None = must-scan (unsummarized/placeholder): WIDER, ok.
                        let mut summaries: std::collections::BTreeMap<u32, Option<(i32, i32)>> =
                            std::collections::BTreeMap::new();
                        for range in ranges {
                            let rblk = 1 + range / revmap_items;
                            if rblk > last_revmap || rblk >= idx_nblocks {
                                summaries.insert(range, None);
                                continue;
                            }
                            let rpage = read_page(rblk);
                            let ptype = u16::from_ne_bytes(
                                rpage[BLCKSZ - 2..BLCKSZ].try_into().unwrap(),
                            );
                            if ptype != 0xF092 {
                                violations.push(format!(
                                    "{tag}: brin revmap block {rblk} wrong page type \
                                     {ptype:#x}"
                                ));
                                summaries.insert(range, None);
                                continue;
                            }
                            let off = 24 + (range % revmap_items) as usize * 6;
                            let hi =
                                u16::from_ne_bytes(rpage[off..off + 2].try_into().unwrap());
                            let lo = u16::from_ne_bytes(
                                rpage[off + 2..off + 4].try_into().unwrap(),
                            );
                            let pos = u16::from_ne_bytes(
                                rpage[off + 4..off + 6].try_into().unwrap(),
                            );
                            if pos == 0 {
                                summaries.insert(range, None); // unsummarized
                                continue;
                            }
                            let tblk = ((hi as u32) << 16) | lo as u32;
                            if tblk >= idx_nblocks {
                                violations.push(format!(
                                    "{tag}: brin revmap tid for range {range} points past \
                                     index end (block {tblk})"
                                ));
                                summaries.insert(range, None);
                                continue;
                            }
                            let tpage = read_page(tblk);
                            let ptype = u16::from_ne_bytes(
                                tpage[BLCKSZ - 2..BLCKSZ].try_into().unwrap(),
                            );
                            if ptype != 0xF093 {
                                violations.push(format!(
                                    "{tag}: brin regular block {tblk} wrong page type \
                                     {ptype:#x}"
                                ));
                                summaries.insert(range, None);
                                continue;
                            }
                            let Some((lp, len)) = raw_item(&tpage, pos as usize) else {
                                violations.push(format!(
                                    "{tag}: brin revmap tid for range {range} points at \
                                     missing item ({tblk},{pos})"
                                ));
                                summaries.insert(range, None);
                                continue;
                            };
                            let tup = &tpage[lp..lp + len];
                            let bt_blkno = u32::from_ne_bytes(tup[0..4].try_into().unwrap());
                            if bt_blkno != range * BR_PPR {
                                violations.push(format!(
                                    "{tag}: brin tuple range start {bt_blkno} != {} for \
                                     range {range}",
                                    range * BR_PPR
                                ));
                            }
                            let state = match brin_tuple::brin_deform_tuple(
                                &bdesc, tup, &mut dtup,
                            ) {
                                Err(e) => {
                                    violations.push(format!(
                                        "{tag}: brin_deform_tuple failed for range \
                                         {range}: {e:?}"
                                    ));
                                    None
                                }
                                Ok(()) => {
                                    if dtup.bt_placeholder {
                                        None // must-scan: legally wider
                                    } else if dtup.bt_empty_range
                                        || dtup.bt_columns[0].bv_allnulls
                                    {
                                        // Any visible non-null row violates.
                                        Some((i32::MAX, i32::MIN))
                                    } else {
                                        Some((
                                            dtup.bt_columns[0].bv_values[0].as_i32(),
                                            dtup.bt_columns[0].bv_values[1].as_i32(),
                                        ))
                                    }
                                }
                            };
                            summaries.insert(range, state);
                        }
                        for (b, v) in &visible_pos {
                            let range = *b / BR_PPR;
                            if let Some(Some((min, max))) = summaries.get(&range) {
                                if v < min || v > max {
                                    violations.push(format!(
                                        "{tag}: BRIN range {range} NARROWER than heap — \
                                         visible value {v} at block {b} outside summary \
                                         [{min},{max}]"
                                    ));
                                }
                            }
                        }
                    }
                }
            }
        }

        // Determinism digest (FNV over the visible fold + horizons).
        let mut digest: u64 = 0xcbf2_9ce4_8422_2325;
        let mut mix = |b: u64| {
            digest ^= b;
            digest = digest.wrapping_mul(0x1000_0000_01b3);
        };
        mix(h as u64);
        mix(acked as u64);
        for v in &visible {
            mix(*v as u64);
        }
        for v in &idx_visible {
            mix(*v as u64);
        }
        mix(cf.checkPoint);
        println!(
            "SIM_RECOVER_STATE h={h} acked={acked} visible_rows={} nblocks={nblocks} digest={digest:016x}",
            visible.len()
        );
        }))
        .is_err();
        if panicked {
            violations
                .push(format!("{tag}: property verification panicked on the recovered image"));
        }
    }

    for v in &violations {
        println!("VIOLATION: {v}");
    }
    println!("SIM_RECOVER_DONE violations={}", violations.len());
}

// ---------------------------------------------------------------------------
// the orchestrating tests
// ---------------------------------------------------------------------------

fn spawn_child(test_name: &str, envs: &[(&str, String)]) -> (bool, String) {
    let mut cmd = std::process::Command::new(std::env::current_exe().unwrap());
    cmd.args([test_name, "--exact", "--ignored", "--test-threads=1", "--nocapture"]);
    for (k, v) in envs {
        cmd.env(k, v);
    }
    let out = cmd.output().unwrap();
    let text = format!(
        "{}{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    (out.status.success(), text)
}

/// Run one writer+recover point pair. `red` is the red-arm selector (""
/// for the property sweep); `extra` env pairs select the rig arm (the
/// initdb-composition arm passes ARM/INITDB/PGRUST_PGSHAREDIR through to
/// both children).
fn run_point(
    base: &std::path::Path,
    k: u64,
    red: &str,
    extra: &[(&str, String)],
) -> (String, String) {
    run_point_tagged(base, k, red, extra, "")
}

/// `tag` disambiguates pack directories for arms that all run at k=0 with
/// their own fault envs (torn/EMFILE points run concurrently).
fn run_point_tagged(
    base: &std::path::Path,
    k: u64,
    red: &str,
    extra: &[(&str, String)],
    tag: &str,
) -> (String, String) {
    let pack = base.join(format!(
        "pack_k{k}{tag}{}",
        if red.is_empty() { String::new() } else { format!("_red_{red}") }
    ));
    let mut wenvs: Vec<(&str, String)> = vec![
        (ROLE_ENV, "writer".into()),
        (K_ENV, k.to_string()),
        (PACK_ENV, pack.to_str().unwrap().into()),
        (RED_ENV, red.to_string()),
    ];
    wenvs.extend(extra.iter().cloned());
    let (ok, wtext) = spawn_child("sim_sweep_writer_child", &wenvs);
    assert!(
        ok && wtext.contains("SIM_WRITER_DONE"),
        "writer child failed at k={k}:\n{wtext}"
    );
    let mut renvs: Vec<(&str, String)> = vec![
        (ROLE_ENV, "recover".into()),
        (PACK_ENV, pack.to_str().unwrap().into()),
    ];
    renvs.extend(extra.iter().cloned());
    let (ok, rtext) = spawn_child("sim_sweep_recover_child", &renvs);
    assert!(
        ok && rtext.contains("SIM_RECOVER_DONE"),
        "recover child failed at k={k}:\n{rtext}"
    );
    let meta = std::fs::read_to_string(pack.join("meta.txt")).unwrap();
    let _ = std::fs::remove_dir_all(&pack);
    (rtext, meta)
}

fn meta_field(meta: &str, key: &str) -> String {
    meta.lines()
        .find(|l| l.starts_with(&format!("{key}=")))
        .map(|l| l.split('=').nth(1).unwrap().to_string())
        .unwrap()
}

/// Find a marker line in captured child output. GLUE TRAP: the child's
/// libtest harness prints `test <name> ... ` WITHOUT a trailing newline, so
/// the FIRST stdout line a child test prints arrives glued to it —
/// `starts_with` misses exactly the first marker. Match the marker anywhere
/// in the line and slice from it.
fn marker_line(text: &str, marker: &str) -> Option<String> {
    text.lines()
        .find_map(|l| l.find(marker).map(|i| l[i..].to_string()))
}

/// Parse the baseline meta's op-trace lines into (k, kind, class, path).
fn parse_trace(meta: &str) -> Vec<(u64, String, String, String)> {
    meta.lines()
        .filter_map(|l| l.strip_prefix("optrace: "))
        .filter_map(|l| {
            let mut k = None;
            let mut kind = None;
            let mut class = None;
            let mut path = None;
            for w in l.split_whitespace() {
                if let Some(v) = w.strip_prefix("k=") {
                    k = v.parse().ok();
                } else if let Some(v) = w.strip_prefix("kind=") {
                    kind = Some(v.to_string());
                } else if let Some(v) = w.strip_prefix("class=") {
                    class = Some(v.to_string());
                } else if let Some(v) = w.strip_prefix("path=") {
                    path = Some(v.to_string());
                }
            }
            Some((k?, kind?, class?, path.unwrap_or_default()))
        })
        .collect()
}

/// inc-4: locate the SEGMENT-RECYCLE cut classes in the baseline trace.
///
/// * `k_recycle` — the RECYCLE rename: a Wal-class Rename whose FROM path
///   (the path the sim op consult logs) is a real segment name, NOT the
///   `xlogtemp.<pid>` install temp file. This is checkpoint-time
///   RemoveOldXlogFiles renaming an obsolete segment to a future segno.
/// * `k_reuse` — the first write INTO the recycled segment: the first
///   Wal-class PWriteV on a segment path that was never written before the
///   recycle rename (the recycled node's primary name is the future segment
///   name from the rename on — the N4 path-at-op work exists for exactly
///   this shape).
fn find_recycle_and_reuse(trace: &[(u64, String, String, String)]) -> (Option<u64>, Option<u64>) {
    let k_recycle = trace
        .iter()
        .find(|(_, kind, class, path)| {
            kind == "Rename" && class == "Wal" && !path.contains("xlogtemp")
        })
        .map(|(k, _, _, _)| *k);
    let Some(krec) = k_recycle else {
        return (None, None);
    };
    let pre_recycle_wal_writes: std::collections::BTreeSet<&str> = trace
        .iter()
        .filter(|(k, kind, class, _)| *k < krec && kind == "PWriteV" && class == "Wal")
        .map(|(_, _, _, path)| path.as_str())
        .collect();
    let k_reuse = trace
        .iter()
        .find(|(k, kind, class, path)| {
            *k > krec
                && kind == "PWriteV"
                && class == "Wal"
                && !path.contains("xlogtemp")
                && !pre_recycle_wal_writes.contains(path.as_str())
        })
        .map(|(k, _, _, _)| *k);
    (k_recycle, k_reuse)
}

/// inc-3 stratified cut-point selection over the scaled workload's op span
/// (the span is too wide to sweep every op as a routine gate). Picks, in
/// order: (0) EVERY op inside the caller's dense windows (inc-4: the
/// segment-recycle rename window and the recycled-segment reuse window);
/// (1) EVERY durability/namespace/metadata op — fsyncs, renames (segment
/// install + recycle), opens, unlinks, truncates, mkdirs; (2) ±1 neighbors
/// of every rename (the install/recycle windows); (3) evenly thinned
/// class-boundary writes (first write after the active path class changed —
/// WAL<->heap<->clog transitions); (4) a uniform stride over the remaining
/// span. Deterministic in the trace alone.
///
/// `target` is a soft size goal, not a hard cap (the inc-3 review's O2
/// observation, resolved by renaming per its disposition): mandatory picks
/// — window ops, durability ops, rename neighbors, the boundary-fill floor
/// — may exceed it; only the optional stride fill respects it.
fn stratify(
    trace: &[(u64, String, String, String)],
    n: u64,
    target: usize,
    windows: &[(u64, u64)],
) -> Vec<u64> {
    use std::collections::BTreeSet;
    let mut pick: BTreeSet<u64> = BTreeSet::new();
    for &(lo, hi) in windows {
        for k in lo.max(1)..=hi.min(n) {
            pick.insert(k);
        }
    }
    for (k, kind, _, _) in trace {
        if matches!(
            kind.as_str(),
            "Fsync" | "Fdatasync" | "Rename" | "Unlink" | "Open" | "Ftruncate"
                | "TruncatePath" | "Mkdir" | "Rmdir" | "Fallocate"
        ) {
            pick.insert(*k);
        }
    }
    for (k, kind, _, _) in trace {
        if kind == "Rename" {
            if *k > 1 {
                pick.insert(k - 1);
            }
            if *k < n {
                pick.insert(k + 1);
            }
        }
    }
    let mut boundaries: Vec<u64> = Vec::new();
    let mut prev_class = String::new();
    for (k, kind, class, _) in trace {
        if kind == "PWriteV" && *class != prev_class {
            boundaries.push(*k);
        }
        prev_class = class.clone();
    }
    let room = target.saturating_sub(pick.len()).max(20) / 2;
    let step = (boundaries.len() / room.max(1)).max(1);
    for k in boundaries.iter().step_by(step) {
        pick.insert(*k);
    }
    if pick.len() < target {
        let stride = (n.max(1) / (target - pick.len()).max(1) as u64).max(1);
        let mut k = 1;
        while k <= n && pick.len() < target {
            pick.insert(k);
            k += stride;
        }
    }
    pick.into_iter().filter(|&k| k >= 1 && k <= n).collect()
}

/// THE PAYOFF (inc-2, scaled up in inc-3): cut across a stratified set of
/// product-workload ops — every durability/namespace op, the segment
/// install/recycle windows, class-boundary writes, plus a uniform stride —
/// under the WHOLE-NODE KILL (pure crash images). The PRODUCT's StartupXLOG
/// must recover every image and the committed/uncommitted/consistency
/// properties must hold at every point.
#[test]
fn product_shaped_crash_recovery_sweep() {
    if std::env::var(ROLE_ENV).is_ok() {
        return; // never recurse
    }
    let base = std::env::temp_dir().join(format!("pgrust_simsweep_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&base);
    std::fs::create_dir_all(&base).unwrap();

    // Fault-free baseline defines the op span and proves the writer rig.
    let (rtext, meta) = run_point(&base, 0, "", &[]);
    assert_eq!(meta_field(&meta, "acked"), TXNS.to_string(), "baseline acks all: {meta}");
    assert_eq!(meta_field(&meta, "cuts"), "0", "baseline must not cut: {meta}");
    assert!(
        rtext.contains("SIM_RECOVER_DONE violations=0"),
        "baseline recovery must be clean:\n{rtext}"
    );
    let n: u64 = meta_field(&meta, "ops").parse().unwrap();
    assert!(n > 200, "scaled workload too small to stratify ({n} ops)");
    let trace = parse_trace(&meta);
    assert_eq!(trace.len() as u64, n, "trace must cover the whole workload span");
    // The inc-3 scale-up must actually reach its new cut classes.
    assert!(
        trace.iter().any(|(_, kind, class, _)| kind == "Rename" && class == "Wal"),
        "workload must cross a WAL segment (install/recycle renames)"
    );
    assert!(
        trace.iter().any(|(_, kind, class, _)| kind == "PWriteV" && class == "Heap"),
        "workload must write heap pages mid-run (multi-page + eviction)"
    );
    // inc-4 SEGMENT-RECYCLE CUT CLASSES: the workload must both RECYCLE a
    // segment (checkpoint-time RemoveOldXlogFiles rename to a future segno)
    // and REUSE it (live WAL written over the stale residue) — an absent
    // class would make the dense windows below vacuous.
    let (k_recycle, k_reuse) = find_recycle_and_reuse(&trace);
    let k_recycle = k_recycle.expect("workload must RECYCLE a segment (RemoveOldXlogFiles)");
    let k_reuse =
        k_reuse.expect("workload must REUSE the recycled segment (write over stale residue)");
    assert!(k_reuse > k_recycle);
    let baseline_rows = marker_line(&rtext, "SIM_RECOVER_STATE").unwrap_or_default();
    assert!(
        baseline_rows.contains(&format!("visible_rows={}", TXNS * ROWS_PER_TXN)),
        "baseline fold must carry all rows: {baseline_rows}"
    );

    // Dense every-op windows around the two recycle-class anchors: the
    // recycle window covers RemoveXlogFile's whole op neighborhood (lstat,
    // rename, durable_rename's parent-dir fsync open/fsync/close, archive
    // cleanup unlinks); the reuse window covers the recycled segment's open
    // and its first stale-overwriting WAL writes.
    let windows = [
        (k_recycle.saturating_sub(8), k_recycle + 12),
        (k_reuse.saturating_sub(2), k_reuse + 18),
    ];
    let points = stratify(&trace, n, 180, &windows);
    eprintln!(
        "PRODUCT-SHAPED SWEEP: stratified {} cut points over {n} product-workload ops \
         (recycle rename k={k_recycle}, recycled-reuse first write k={k_reuse})",
        points.len()
    );

    let failures = std::sync::Mutex::new(Vec::<String>::new());
    let cut_points = std::sync::atomic::AtomicU64::new(0);
    let next = std::sync::atomic::AtomicUsize::new(0);
    std::thread::scope(|s| {
        for _ in 0..6 {
            s.spawn(|| loop {
                let i = next.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                if i >= points.len() {
                    break;
                }
                let k = points[i];
                let (rtext, meta) = run_point(&base, k, "", &[]);
                if meta_field(&meta, "cuts") == "0" {
                    failures
                        .lock()
                        .unwrap()
                        .push(format!("k={k}: planned cut never fired"));
                    continue;
                }
                cut_points.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                for line in rtext.lines() {
                    // marker_line glue trap: match anywhere in the line.
                    if let Some(i) = line.find("VIOLATION: ") {
                        failures.lock().unwrap().push(line[i + 11..].to_string());
                    }
                }
            });
        }
    });
    let _ = std::fs::remove_dir_all(&base);
    let failures = failures.into_inner().unwrap();
    let cut_points = cut_points.into_inner();

    eprintln!(
        "PRODUCT-SHAPED SWEEP: {cut_points} cut points over {n} product-workload ops \
         ({TXNS} txns x {ROWS_PER_TXN} rows + {} product checkpoints, whole-node kill), \
         {} violations",
        CKPT_AFTER.len(),
        failures.len()
    );
    assert_eq!(cut_points as usize, points.len(), "every sweep point must cut");
    assert!(
        failures.is_empty(),
        "PRODUCT-SHAPED CRASH-RECOVERY VIOLATIONS ({}):\n{}",
        failures.len(),
        failures.join("\n")
    );
}

/// `xlogtemp.<pid>` carries the writer process's ambient pid in its NAME
/// (C's XLogFileInitInternal shape) — harness ambience, not model entropy:
/// the x3 gate runs three FRESH OS processes. Normalize just that token for
/// the cross-process byte-compare; everything else must be byte-identical.
fn normalize_pid(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let mut rest = s;
    while let Some(pos) = rest.find("xlogtemp.") {
        let split = pos + "xlogtemp.".len();
        out.push_str(&rest[..split]);
        rest = &rest[split..];
        let nd = rest.chars().take_while(|c| c.is_ascii_digit()).count();
        if nd > 0 {
            out.push_str("PID");
        }
        rest = &rest[nd..];
    }
    out.push_str(rest);
    out
}

/// Flatten a pack tree to (pid-normalized file name, bytes) pairs in sorted
/// walk order — the byte-compare unit of the determinism gates.
fn tree_digest(dir: &std::path::Path, acc: &mut Vec<(String, Vec<u8>)>) {
    let mut entries: Vec<_> = std::fs::read_dir(dir).unwrap().map(|e| e.unwrap()).collect();
    entries.sort_by_key(|e| e.file_name());
    for e in entries {
        if e.file_type().unwrap().is_dir() {
            tree_digest(&e.path(), acc);
        } else {
            acc.push((
                normalize_pid(e.path().to_str().unwrap().rsplit('/').next().unwrap()),
                std::fs::read(e.path()).unwrap(),
            ));
        }
    }
}

/// Same point, three fresh universes: byte-identical packs, metas (incl. the
/// fault log) and recovered state (the replay-determinism gate, x3).
/// inc-3 (review observation 2): the pinned points are ROTATED/WIDENED —
/// three span fractions plus the first WAL-segment install/recycle rename,
/// instead of inc-2's single k=N/2.
#[test]
fn sweep_point_replay_determinism_x3() {
    if std::env::var(ROLE_ENV).is_ok() {
        return;
    }
    let base = std::env::temp_dir().join(format!("pgrust_simdet_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&base);
    std::fs::create_dir_all(&base).unwrap();

    // Re-derive the pins from the baseline op count/trace so they survive
    // workload evolution.
    let (_, meta) = run_point(&base, 0, "", &[]);
    let n: u64 = meta_field(&meta, "ops").parse().unwrap();
    let trace = parse_trace(&meta);
    let mut pins = vec![n / 4, n / 2, 3 * n / 4];
    if let Some((k, _, _, _)) = trace
        .iter()
        .find(|(_, kind, class, _)| kind == "Rename" && class == "Wal")
    {
        pins.push(*k); // cut ON the segment install rename
    }
    // inc-4 rotation: cut ON the recycle rename and ON the first write into
    // the recycled segment (the new cut classes get their own replay pins).
    let (k_recycle, k_reuse) = find_recycle_and_reuse(&trace);
    pins.extend(k_recycle);
    pins.extend(k_reuse);
    pins.sort_unstable();
    pins.dedup();

    for &k in &pins {
        let mut runs: Vec<(Vec<(String, Vec<u8>)>, String, String)> = Vec::new();
        for rep in 0..3 {
            let pack = base.join(format!("det_k{k}_rep{rep}"));
            let (ok, _) = spawn_child(
                "sim_sweep_writer_child",
                &[
                    (ROLE_ENV, "writer".into()),
                    (K_ENV, k.to_string()),
                    (PACK_ENV, pack.to_str().unwrap().into()),
                    (RED_ENV, "0".into()),
                ],
            );
            assert!(ok);
            let (ok, rtext) = spawn_child(
                "sim_sweep_recover_child",
                &[
                    (ROLE_ENV, "recover".into()),
                    (PACK_ENV, pack.to_str().unwrap().into()),
                ],
            );
            assert!(ok, "recover k={k} rep {rep} failed:\n{rtext}");
            let mut tree = Vec::new();
            tree_digest(&pack.join("root"), &mut tree);
            let meta =
                normalize_pid(&std::fs::read_to_string(pack.join("meta.txt")).unwrap());
            let state = marker_line(&rtext, "SIM_RECOVER_STATE")
                .unwrap_or_else(|| "MISSING".to_string());
            assert_ne!(state, "MISSING", "k={k} rep {rep}: recover state line absent");
            runs.push((tree, meta, state));
            let _ = std::fs::remove_dir_all(&pack);
        }
        assert!(!runs[0].1.is_empty());
        assert!(
            runs[0].1.contains("cuts=1"),
            "determinism point k={k} must cut: {}",
            runs[0].1
        );
        for rep in 1..3 {
            assert_eq!(
                runs[0].0, runs[rep].0,
                "k={k}: post-crash image trees must be byte-identical"
            );
            assert_eq!(
                runs[0].1, runs[rep].1,
                "k={k}: meta (incl. fault log) must be byte-identical"
            );
            assert_eq!(runs[0].2, runs[rep].2, "k={k}: recovered state must be identical");
        }
        eprintln!("DETERMINISM x3 OK at k={k}: {}", runs[0].2);
    }
    let _ = std::fs::remove_dir_all(&base);
}

/// PRODUCT-SHAPED RED: disable the product's fsync layer through its own
/// knob (enableFsync gates fd::pg_fsync and issue_xlog_fsync — the exact
/// surface a lost fsync bug would occupy) and prove the sweep CATCHES the
/// resulting acked-data loss. If this arm ever comes back clean, the
/// product-shaped harness lost its teeth.
#[test]
fn red_product_fsync_disabled_is_caught() {
    if std::env::var(ROLE_ENV).is_ok() {
        return;
    }
    let base = std::env::temp_dir().join(format!("pgrust_simred_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&base);
    std::fs::create_dir_all(&base).unwrap();
    let (rtext, meta) = run_point(&base, 0, "fsync", &[]);
    let _ = std::fs::remove_dir_all(&base);

    assert_eq!(
        meta_field(&meta, "acked"),
        TXNS.to_string(),
        "the fsync-less engine happily acks everything: {meta}"
    );
    assert!(
        rtext.contains("VIOLATION:") && rtext.contains("ACKED txn"),
        "the sweep must flag the acked-data loss of the fsync-disabled arm:\n{rtext}"
    );
}

/// inc-4 RED (recycle-window class): an over-eager recycle durably renames
/// a segment the redo horizon still NEEDS to a future segno (bypassing
/// RemoveOldXlogFiles' `fname <= lastoff` horizon guard from beneath), then
/// the node dies. The sweep must flag the unrecoverable image — property 1
/// (recovery completes) is the tooth. If this ever comes back clean, cuts
/// in the recycle window stopped proving anything.
#[test]
fn red_overeager_recycle_of_needed_segment_is_caught() {
    if std::env::var(ROLE_ENV).is_ok() {
        return;
    }
    let base = std::env::temp_dir().join(format!("pgrust_simredrec_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&base);
    std::fs::create_dir_all(&base).unwrap();
    let (rtext, meta) = run_point(&base, 0, "recycle-needed", &[]);
    let _ = std::fs::remove_dir_all(&base);

    assert_eq!(meta_field(&meta, "acked"), "8", "red arm acks through txn 8: {meta}");
    assert_eq!(meta_field(&meta, "cuts"), "1", "red arm must cut: {meta}");
    assert!(
        rtext.contains("VIOLATION:")
            && (rtext.contains("RECOVERY FAILED") || rtext.contains("RECOVERY PANICKED")),
        "the sweep must flag the over-eager recycle as a recovery-completeness \
         violation:\n{rtext}"
    );
}

/// inc-4 RED (recycled-reuse class): VALIDATING stale residue at the WAL
/// tail — a fully valid commit record for a future txn's xid, planted
/// durably beneath the product. Recovery cannot tell it from real WAL and
/// replays it; the clog-prefix property must catch the stale replay. The
/// green reuse-window sweep + this red together prove the reuse cut class
/// has teeth: honest recycled residue is rejected (xlp_pageaddr/CRC), and
/// if residue ever validated, the sweep would see it.
#[test]
fn red_stale_recycled_residue_replay_is_caught() {
    if std::env::var(ROLE_ENV).is_ok() {
        return;
    }
    let base = std::env::temp_dir().join(format!("pgrust_simredstale_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&base);
    std::fs::create_dir_all(&base).unwrap();
    let (rtext, meta) = run_point(&base, 0, "stale-residue", &[]);
    let _ = std::fs::remove_dir_all(&base);

    assert_eq!(meta_field(&meta, "acked"), "7", "red arm acks through txn 7: {meta}");
    assert_eq!(meta_field(&meta, "cuts"), "1", "red arm must cut: {meta}");
    assert!(
        rtext.contains("VIOLATION:") && rtext.contains("clog commit gap"),
        "the sweep must flag the replayed stale commit as a clog-prefix gap:\n{rtext}"
    );
}

/// inc-4 arm (b): the CUT-SWEEP OVER THE REAL-INITDB IMAGE. The writer
/// boots a real C-initdb datadir through the t28 provider seam
/// (compose_boot_namespace: process-shared universe, durable-from-birth
/// ingest, boot cwd), runs the product workload over the REAL datadir
/// composition (real control file, real 16 MB segment, real clog/xid range,
/// real catalog namespace), and the stratified cut distribution runs over
/// those ops; recovery is the product StartupXLOG over the packed post-cut
/// composition, re-composed through the same seam. Plus the arm's own red
/// (product fsync knob) and an x3 replay-determinism pin.
#[test]
fn initdb_image_cut_sweep() {
    if std::env::var(ROLE_ENV).is_ok() {
        return;
    }
    let Some(dd) = find_or_mint_initdb_dd("sweep") else {
        eprintln!(
            "SKIP initdb_image_cut_sweep: no C PostgreSQL 18 initdb found \
             (set {INITDB_ENV} or install to /opt/homebrew/bin) — flagged, not silent"
        );
        return;
    };
    let base = std::env::temp_dir().join(format!("pgrust_simsweep_dd_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&base);
    std::fs::create_dir_all(&base).unwrap();
    // Empty share dir: the recovery entry point never consults share assets
    // (inc-2 COMPOSE FINDING 1 scope fact); manifest roots that do not exist
    // under it are skipped as part of the world identity.
    let share = base.join("empty_share");
    std::fs::create_dir_all(&share).unwrap();
    let extra: Vec<(&str, String)> = vec![
        (ARM_ENV, "initdb".into()),
        (INITDB_ENV, dd.to_str().unwrap().into()),
        ("PGRUST_PGSHAREDIR", share.to_str().unwrap().into()),
    ];

    // Fault-free baseline: proves the composed writer rig and defines the
    // op span for the stratifier.
    let (rtext, meta) = run_point(&base, 0, "", &extra);
    assert_eq!(
        meta_field(&meta, "acked"),
        INITDB_TXNS.to_string(),
        "initdb baseline acks all: {meta}"
    );
    assert_eq!(meta_field(&meta, "cuts"), "0", "initdb baseline must not cut: {meta}");
    assert_eq!(meta_field(&meta, "arm"), "initdb", "{meta}");
    assert!(
        rtext.contains("SIM_RECOVER_DONE violations=0"),
        "initdb baseline recovery must be clean:\n{rtext}"
    );
    let base_xid: u32 = meta_field(&meta, "base_xid").parse().unwrap();
    assert!(base_xid > 3, "initdb arm must run in the REAL xid range, got {base_xid}");
    let n: u64 = meta_field(&meta, "ops").parse().unwrap();
    assert!(n > 100, "initdb workload too small to stratify ({n} ops)");
    let trace = parse_trace(&meta);
    assert_eq!(trace.len() as u64, n, "trace must cover the whole workload span");
    let baseline_state = marker_line(&rtext, "SIM_RECOVER_STATE").unwrap_or_default();
    assert!(
        baseline_state.contains(&format!("visible_rows={}", INITDB_TXNS * ROWS_PER_TXN)),
        "initdb baseline fold must carry all rows: {baseline_state}"
    );

    let points = stratify(&trace, n, 60, &[]);
    eprintln!(
        "INITDB-IMAGE SWEEP: stratified {} cut points over {n} ops on the composed \
         real-initdb datadir (base_xid={base_xid})",
        points.len()
    );

    let failures = std::sync::Mutex::new(Vec::<String>::new());
    let cut_points = std::sync::atomic::AtomicU64::new(0);
    let next = std::sync::atomic::AtomicUsize::new(0);
    std::thread::scope(|s| {
        for _ in 0..6 {
            s.spawn(|| loop {
                let i = next.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                if i >= points.len() {
                    break;
                }
                let k = points[i];
                let (rtext, meta) = run_point(&base, k, "", &extra);
                if meta_field(&meta, "cuts") == "0" {
                    failures
                        .lock()
                        .unwrap()
                        .push(format!("initdb k={k}: planned cut never fired"));
                    continue;
                }
                cut_points.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                for line in rtext.lines() {
                    if let Some(i) = line.find("VIOLATION: ") {
                        failures.lock().unwrap().push(line[i + 11..].to_string());
                    }
                }
            });
        }
    });
    let cut_points = cut_points.into_inner();
    let failures_v = failures.into_inner().unwrap();
    eprintln!(
        "INITDB-IMAGE SWEEP: {cut_points} cut points over {n} ops \
         ({INITDB_TXNS} txns x {ROWS_PER_TXN} rows + 1 product checkpoint, \
         whole-node kill, provider-seam composition), {} violations",
        failures_v.len()
    );
    assert_eq!(cut_points as usize, points.len(), "every initdb sweep point must cut");
    assert!(
        failures_v.is_empty(),
        "INITDB-IMAGE CRASH-RECOVERY VIOLATIONS ({}):\n{}",
        failures_v.len(),
        failures_v.join("\n")
    );

    // The arm's RED: the product fsync knob off — acked-loss must be caught
    // on the composed image too (teeth end-to-end for this arm).
    let (rtext, meta) = run_point(&base, 0, "fsync", &extra);
    assert_eq!(meta_field(&meta, "acked"), INITDB_TXNS.to_string(), "{meta}");
    assert!(
        rtext.contains("VIOLATION:") && rtext.contains("ACKED txn"),
        "the initdb arm must flag acked-data loss when the product fsync layer \
         is disabled:\n{rtext}"
    );

    // Replay determinism x3 at the mid-workload pin: byte-identical packs
    // (the whole composed post-cut datadir), metas (incl. the fault log and
    // the SIM-ASSETS content identity) and recovered state.
    let pin = n / 2;
    let mut runs: Vec<(Vec<(String, Vec<u8>)>, String, String)> = Vec::new();
    for rep in 0..3 {
        let pack = base.join(format!("dddet_k{pin}_rep{rep}"));
        let mut wenvs: Vec<(&str, String)> = vec![
            (ROLE_ENV, "writer".into()),
            (K_ENV, pin.to_string()),
            (PACK_ENV, pack.to_str().unwrap().into()),
            (RED_ENV, String::new()),
        ];
        wenvs.extend(extra.iter().cloned());
        let (ok, _) = spawn_child("sim_sweep_writer_child", &wenvs);
        assert!(ok);
        let mut renvs: Vec<(&str, String)> = vec![
            (ROLE_ENV, "recover".into()),
            (PACK_ENV, pack.to_str().unwrap().into()),
        ];
        renvs.extend(extra.iter().cloned());
        let (ok, rtext) = spawn_child("sim_sweep_recover_child", &renvs);
        assert!(ok, "initdb det recover k={pin} rep {rep} failed:\n{rtext}");
        let mut tree = Vec::new();
        tree_digest(&pack.join("root"), &mut tree);
        let meta = normalize_pid(&std::fs::read_to_string(pack.join("meta.txt")).unwrap());
        let state = marker_line(&rtext, "SIM_RECOVER_STATE")
            .unwrap_or_else(|| "MISSING".to_string());
        assert_ne!(state, "MISSING", "initdb det k={pin} rep {rep}: state line absent");
        runs.push((tree, meta, state));
        let _ = std::fs::remove_dir_all(&pack);
    }
    assert!(runs[0].1.contains("cuts=1"), "initdb det pin must cut: {}", runs[0].1);
    for rep in 1..3 {
        assert_eq!(runs[0].0, runs[rep].0, "initdb det k={pin}: pack trees differ");
        assert_eq!(runs[0].1, runs[rep].1, "initdb det k={pin}: metas differ");
        assert_eq!(runs[0].2, runs[rep].2, "initdb det k={pin}: recovered state differs");
    }
    eprintln!("INITDB DETERMINISM x3 OK at k={pin}: {}", runs[0].2);

    let _ = std::fs::remove_dir_all(&base);
}

/// Item 1 of the inc-2 charter: a REAL (C PostgreSQL 18) initdb'd datadir
/// imported into the SimVfs namespace via the cp -RL/pack importer, booted
/// through the PRODUCT's ReadControlFile + StartupXLOG entirely inside sim.
///
/// COMPOSE FINDING 1 scope note: the finding's tzdata/share blocker lives in
/// whole-server boots (postgresql.conf -> timezone GUCs -> pgtz scan). The
/// recovery entry point never consults share/timezone, so NO provider
/// slice is needed for THIS lane; the full-server sim boot stays blocked on
/// the finding (dst-p3-scheduler REQUIRED section), untouched here.
///
/// Needs a native C initdb (the wasm-boot prereq): set PGRUST_SIM_SWEEP_INITDB_DD
/// to a pre-minted datadir, or have initdb on the wasm-boot probe paths.
/// Skips LOUDLY when absent.
/// Find (env override) or mint (native C initdb) a real datadir; None when
/// no initdb exists on this box — callers must SKIP LOUDLY, never silently.
fn find_or_mint_initdb_dd(tag: &str) -> Option<std::path::PathBuf> {
    match std::env::var(INITDB_ENV) {
        Ok(p) => Some(std::path::PathBuf::from(p)),
        Err(_) => {
            let mut found = None;
            for cand in ["/tmp/pgrust_pginstall/bin/initdb", "/opt/homebrew/bin/initdb"] {
                if std::path::Path::new(cand).exists() {
                    found = Some(cand);
                    break;
                }
            }
            found.map(|initdb| {
                let dd = std::env::temp_dir()
                    .join(format!("pgrust_sim_initdb_{tag}_{}", std::process::id()));
                let _ = std::fs::remove_dir_all(&dd);
                let out = std::process::Command::new(initdb)
                    .args([
                        "-D",
                        dd.to_str().unwrap(),
                        "--no-locale",
                        "--encoding=UTF8",
                        "-U",
                        "postgres",
                        "-A",
                        "trust",
                    ])
                    .output()
                    .expect("initdb spawn");
                assert!(
                    out.status.success(),
                    "initdb failed: {}",
                    String::from_utf8_lossy(&out.stderr)
                );
                dd
            })
        }
    }
}

#[test]
fn initdb_datadir_boots_under_sim() {
    if std::env::var(ROLE_ENV).is_ok() {
        return;
    }
    let Some(dd) = find_or_mint_initdb_dd("boot") else {
        eprintln!(
            "SKIP initdb_datadir_boots_under_sim: no C PostgreSQL 18 initdb found \
             (set {INITDB_ENV} or install to /opt/homebrew/bin) — flagged, not silent"
        );
        return;
    };

    // Import + boot in a CHILD (fresh statics), reusing the recover child
    // over a pack whose root IS the initdb datadir.
    let base = std::env::temp_dir().join(format!("pgrust_sim_initdbpack_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&base);
    std::fs::create_dir_all(&base).unwrap();
    let (ok, text) = spawn_child(
        "sim_initdb_boot_child",
        &[
            (ROLE_ENV, "initdb-boot".into()),
            (PACK_ENV, dd.to_str().unwrap().into()),
        ],
    );
    let _ = std::fs::remove_dir_all(&base);
    assert!(
        ok && text.contains("SIM_INITDB_BOOT_OK"),
        "real-initdb datadir failed to boot the product recovery under sim:\n{text}"
    );
}

#[test]
#[ignore]
fn sim_initdb_boot_child() {
    if std::env::var(ROLE_ENV).as_deref() != Ok("initdb-boot") {
        return;
    }
    let dd = std::path::PathBuf::from(std::env::var(PACK_ENV).unwrap());

    SimVfs::reset();
    // The cp -RL/pack import: the whole real initdb'd datadir into the sim
    // namespace (symlinks dereferenced by the importer).
    import_tree_into_sim(&dd, "/");
    init_small::globals::SetDataDir("/");
    init_small::globals::set_enableFsync(true);
    install_stub_seams();
    install_real();

    transam_xlog::ReadControlFile().unwrap();
    transam_xlog::XLOGShmemInit();
    transam_xlog::StartupXLOG().unwrap();

    let cf = *transam_xlog::control_file::control_file();
    assert_eq!(cf.state, DB_IN_PRODUCTION, "post-boot control state");
    println!(
        "SIM_INITDB_BOOT_OK sysid={:#x} ckpt={:#x} tli={}",
        cf.system_identifier, cf.checkPoint, cf.checkPointCopy.ThisTimeLineID
    );
}

// ---------------------------------------------------------------------------
// inc-5 arms: INDEX LANE (btree), TORN-WRITE (FPW repair), EMFILE battery
// ---------------------------------------------------------------------------

/// inc-5 INDEX LANE: the btree-arm crash-recovery sweep. The workload drives
/// the PRODUCT's btinsert per heap row (leaf inserts, page splits, NEWROOT in
/// WAL), checkpoints flush index pages (index-file write/fsync cut class),
/// txn 6 heap-deletes txn 1's rows and the product btbulkdelete removes their
/// index entries (the VACUUM cut-class window). Cuts land densely around the
/// first index-file write and inside the vacuum window, plus the standard
/// stratification; recovery must satisfy the heap properties AND the index
/// walk/order/coverage properties at every point. Ends with the arm's
/// replay-determinism x3 at two rotated pins.
#[test]
fn btree_index_crash_recovery_sweep() {
    if std::env::var(ROLE_ENV).is_ok() {
        return;
    }
    let base = std::env::temp_dir().join(format!("pgrust_simsweep_bt_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&base);
    std::fs::create_dir_all(&base).unwrap();
    let extra: Vec<(&str, String)> = vec![(ARM_ENV, "btree".into())];

    let (rtext, meta) = run_point(&base, 0, "", &extra);
    assert_eq!(meta_field(&meta, "acked"), BT_TXNS.to_string(), "btree baseline acks all: {meta}");
    assert_eq!(meta_field(&meta, "cuts"), "0", "btree baseline must not cut: {meta}");
    assert!(
        rtext.contains("SIM_RECOVER_DONE violations=0"),
        "btree baseline recovery must be clean:\n{rtext}"
    );
    let baseline_state = marker_line(&rtext, "SIM_RECOVER_STATE").unwrap_or_default();
    assert!(
        baseline_state.contains(&format!("visible_rows={}", 6 * BT_ROWS)),
        "btree baseline fold: 6 insert txns survive the delete txn: {baseline_state}"
    );
    let n: u64 = meta_field(&meta, "ops").parse().unwrap();
    let trace = parse_trace(&meta);
    assert_eq!(trace.len() as u64, n, "trace must cover the whole workload span");
    // The index cut classes must EXIST: index-file page writes + the vacuum
    // window (btbulkdelete removed exactly txn 1's entries).
    let idx_path_frag = format!("/base/5/{IDX_OID}");
    let k_idx = trace
        .iter()
        .find(|(_, kind, _, path)| kind == "PWriteV" && path.contains(&idx_path_frag))
        .map(|(k, _, _, _)| *k)
        .expect("workload must flush btree index pages (checkpoint wave)");
    let vac_lo: u64 = meta_field(&meta, "vac_lo").parse().unwrap();
    let vac_hi: u64 = meta_field(&meta, "vac_hi").parse().unwrap();
    assert!(vac_lo < vac_hi && vac_hi <= n);
    assert_eq!(
        meta_field(&meta, "vac_removed"),
        BT_ROWS.to_string(),
        "btbulkdelete must remove exactly txn 1's index entries: {meta}"
    );

    let windows = [
        (k_idx.saturating_sub(2), k_idx + 10),
        (vac_lo, vac_hi.min(vac_lo + 18)),
    ];
    let points = stratify(&trace, n, 90, &windows);
    eprintln!(
        "BTREE-INDEX SWEEP: stratified {} cut points over {n} ops \
         (first index-page write k={k_idx}, vacuum window [{vac_lo},{vac_hi}])",
        points.len()
    );

    let failures = std::sync::Mutex::new(Vec::<String>::new());
    let cut_points = std::sync::atomic::AtomicU64::new(0);
    let next = std::sync::atomic::AtomicUsize::new(0);
    std::thread::scope(|s| {
        for _ in 0..6 {
            s.spawn(|| loop {
                let i = next.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                if i >= points.len() {
                    break;
                }
                let k = points[i];
                let (rtext, meta) = run_point(&base, k, "", &extra);
                if meta_field(&meta, "cuts") == "0" {
                    failures
                        .lock()
                        .unwrap()
                        .push(format!("btree k={k}: planned cut never fired"));
                    continue;
                }
                cut_points.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                for line in rtext.lines() {
                    if let Some(i) = line.find("VIOLATION: ") {
                        failures.lock().unwrap().push(line[i + 11..].to_string());
                    }
                }
            });
        }
    });
    let cut_points = cut_points.into_inner();
    let failures_v = failures.into_inner().unwrap();
    eprintln!(
        "BTREE-INDEX SWEEP: {cut_points} cut points over {n} ops \
         ({BT_TXNS} txns x {BT_ROWS} rows, delete txn {BT_DELETE_TXN} + product \
         btbulkdelete, whole-node kill), {} violations",
        failures_v.len()
    );
    assert_eq!(cut_points as usize, points.len(), "every btree sweep point must cut");
    assert!(
        failures_v.is_empty(),
        "BTREE-INDEX CRASH-RECOVERY VIOLATIONS ({}):\n{}",
        failures_v.len(),
        failures_v.join("\n")
    );

    // Replay determinism x3 at two rotated pins: mid-span and inside the
    // vacuum window (the new cut class gets its own pin).
    for pin in [n / 2, vac_lo + 2] {
        let mut runs: Vec<(Vec<(String, Vec<u8>)>, String, String)> = Vec::new();
        for rep in 0..3 {
            let pack = base.join(format!("btdet_k{pin}_rep{rep}"));
            let mut wenvs: Vec<(&str, String)> = vec![
                (ROLE_ENV, "writer".into()),
                (K_ENV, pin.to_string()),
                (PACK_ENV, pack.to_str().unwrap().into()),
                (RED_ENV, String::new()),
            ];
            wenvs.extend(extra.iter().cloned());
            let (ok, _) = spawn_child("sim_sweep_writer_child", &wenvs);
            assert!(ok);
            let mut renvs: Vec<(&str, String)> = vec![
                (ROLE_ENV, "recover".into()),
                (PACK_ENV, pack.to_str().unwrap().into()),
            ];
            renvs.extend(extra.iter().cloned());
            let (ok, rtext) = spawn_child("sim_sweep_recover_child", &renvs);
            assert!(ok, "btree det recover k={pin} rep {rep} failed:\n{rtext}");
            let mut tree = Vec::new();
            tree_digest(&pack.join("root"), &mut tree);
            let m = normalize_pid(&std::fs::read_to_string(pack.join("meta.txt")).unwrap());
            let state = marker_line(&rtext, "SIM_RECOVER_STATE")
                .unwrap_or_else(|| "MISSING".to_string());
            assert_ne!(state, "MISSING", "btree det k={pin} rep {rep}: state line absent");
            runs.push((tree, m, state));
            let _ = std::fs::remove_dir_all(&pack);
        }
        assert!(runs[0].1.contains("cuts=1"), "btree det pin k={pin} must cut");
        for rep in 1..3 {
            assert_eq!(runs[0].0, runs[rep].0, "btree det k={pin}: pack trees differ");
            assert_eq!(runs[0].1, runs[rep].1, "btree det k={pin}: metas differ");
            assert_eq!(runs[0].2, runs[rep].2, "btree det k={pin}: recovered state differs");
        }
        eprintln!("BTREE DETERMINISM x3 OK at k={pin}: {}", runs[0].2);
    }
    let _ = std::fs::remove_dir_all(&base);
}

/// inc-5 RED (index lane): a lost index page. Ascending keys leave the
/// leftmost leaf untouched after the txn-3 checkpoint; the red zeroes it
/// durably beneath the product and cuts. No post-checkpoint WAL (hence no
/// FPI) covers that page, so recovery cannot restore it — the index
/// walk/coverage properties must flag the loss. If this comes back green,
/// the index-lane sweep lost its teeth.
#[test]
fn red_stale_btree_page_is_caught() {
    if std::env::var(ROLE_ENV).is_ok() {
        return;
    }
    let base = std::env::temp_dir().join(format!("pgrust_simredbt_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&base);
    std::fs::create_dir_all(&base).unwrap();
    let extra: Vec<(&str, String)> = vec![(ARM_ENV, "btree".into())];
    let (rtext, meta) = run_point(&base, 0, "idx-stale", &extra);
    let _ = std::fs::remove_dir_all(&base);

    assert_eq!(meta_field(&meta, "acked"), "5", "idx-stale red acks txns 1-5: {meta}");
    assert_eq!(meta_field(&meta, "cuts"), "1", "idx-stale red must cut: {meta}");
    assert!(
        rtext.contains("VIOLATION:") && rtext.contains("index"),
        "the sweep must flag the zeroed pre-checkpoint index leaf via the \
         index properties:\n{rtext}"
    );
}

/// inc-5 TORN-WRITE arm (product-shaped FPW proof): cut points that crash
/// MID-WRITE on data-plane writes, the surviving prefix floored to the 512 B
/// sector atomicity floor. Torn HEAP-page flushes must be repaired by
/// recovery from full-page images (WAL-before-data: the FPI is durable
/// before any page write); torn WAL-tail writes must be truncated at the
/// guards (CRC/pageaddr) with every acked commit before them intact.
#[test]
fn torn_write_fpw_repair_sweep() {
    if std::env::var(ROLE_ENV).is_ok() {
        return;
    }
    let base = std::env::temp_dir().join(format!("pgrust_simtorn_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&base);
    std::fs::create_dir_all(&base).unwrap();

    let (_, meta) = run_point(&base, 0, "", &[]);
    let trace = parse_trace(&meta);
    let class_writes = |class: &str| -> Vec<u64> {
        trace
            .iter()
            .filter(|(_, kind, c, _)| kind == "PWriteV" && c == class)
            .map(|(k, _, _, _)| *k)
            .collect()
    };
    let heap_ws = class_writes("Heap");
    let wal_ws = class_writes("Wal");
    assert!(heap_ws.len() >= 20, "workload must flush heap pages ({})", heap_ws.len());
    assert!(wal_ws.len() >= 20, "workload must write WAL ({})", wal_ws.len());
    // Evenly spread torn points: the j-th class write, torn at a seeded
    // sector prefix (1..15 sectors of an 8 KB page).
    let mut specs: Vec<(String, u64)> = Vec::new();
    for i in 0..10u64 {
        let j = 1 + i * (heap_ws.len() as u64 - 1) / 9;
        let p = 512 * (1 + per_point_seed(heap_ws[(j - 1) as usize]) % 15);
        specs.push((format!("heap:{j}:{p}"), heap_ws[(j - 1) as usize]));
    }
    for i in 0..6u64 {
        let j = 1 + i * (wal_ws.len() as u64 - 1) / 5;
        let p = 512 * (1 + per_point_seed(wal_ws[(j - 1) as usize]) % 15);
        specs.push((format!("wal:{j}:{p}"), wal_ws[(j - 1) as usize]));
    }
    eprintln!("TORN-WRITE SWEEP: {} torn points (heap + wal classes)", specs.len());

    let failures = std::sync::Mutex::new(Vec::<String>::new());
    let next = std::sync::atomic::AtomicUsize::new(0);
    std::thread::scope(|s| {
        for _ in 0..6 {
            s.spawn(|| loop {
                let i = next.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                if i >= specs.len() {
                    break;
                }
                let (spec, k) = &specs[i];
                let tag = format!("_torn_{}", spec.replace(':', "_"));
                let extra: Vec<(&str, String)> = vec![(TORN_ENV, spec.clone())];
                let (rtext, meta) = run_point_tagged(&base, 0, "", &extra, &tag);
                if meta_field(&meta, "cuts") != "1" {
                    failures
                        .lock()
                        .unwrap()
                        .push(format!("torn {spec} (trace k={k}): cut never fired"));
                    continue;
                }
                if !meta.contains("faultlog: TORN") {
                    failures
                        .lock()
                        .unwrap()
                        .push(format!("torn {spec}: no TORN fault-log entry"));
                }
                for line in rtext.lines() {
                    if let Some(i) = line.find("VIOLATION: ") {
                        failures
                            .lock()
                            .unwrap()
                            .push(format!("torn {spec}: {}", &line[i + 11..]));
                    }
                }
            });
        }
    });
    let failures_v = failures.into_inner().unwrap();
    eprintln!(
        "TORN-WRITE SWEEP: {} torn points, {} violations",
        specs.len(),
        failures_v.len()
    );
    assert!(
        failures_v.is_empty(),
        "TORN-WRITE VIOLATIONS ({}):\n{}",
        failures_v.len(),
        failures_v.join("\n")
    );

    // Replay determinism x3 at one torn heap point (seeded tear prefix +
    // seeded crash-image subset must reproduce byte-identically).
    let (spec, _) = &specs[4];
    let mut runs: Vec<(Vec<(String, Vec<u8>)>, String, String)> = Vec::new();
    for rep in 0..3 {
        let pack = base.join(format!("torndet_rep{rep}"));
        let (ok, _) = spawn_child(
            "sim_sweep_writer_child",
            &[
                (ROLE_ENV, "writer".into()),
                (K_ENV, "0".into()),
                (PACK_ENV, pack.to_str().unwrap().into()),
                (RED_ENV, String::new()),
                (TORN_ENV, spec.clone()),
            ],
        );
        assert!(ok);
        let (ok, rtext) = spawn_child(
            "sim_sweep_recover_child",
            &[
                (ROLE_ENV, "recover".into()),
                (PACK_ENV, pack.to_str().unwrap().into()),
            ],
        );
        assert!(ok, "torn det rep {rep} failed:\n{rtext}");
        let mut tree = Vec::new();
        tree_digest(&pack.join("root"), &mut tree);
        let m = normalize_pid(&std::fs::read_to_string(pack.join("meta.txt")).unwrap());
        let state =
            marker_line(&rtext, "SIM_RECOVER_STATE").unwrap_or_else(|| "MISSING".to_string());
        assert_ne!(state, "MISSING");
        runs.push((tree, m, state));
        let _ = std::fs::remove_dir_all(&pack);
    }
    for rep in 1..3 {
        assert_eq!(runs[0].0, runs[rep].0, "torn det: pack trees differ");
        assert_eq!(runs[0].1, runs[rep].1, "torn det: metas differ");
        assert_eq!(runs[0].2, runs[rep].2, "torn det: recovered state differs");
    }
    eprintln!("TORN DETERMINISM x3 OK ({spec}): {}", runs[0].2);
    let _ = std::fs::remove_dir_all(&base);
}

/// inc-5 RED (torn-write class): the product's full-page-writes knob OFF,
/// then a torn heap-page flush. Without an FPI the torn page cannot be
/// rebuilt (the record-level redo is LSN-skipped by the surviving new page
/// header) — the fold/recovery properties must flag the damage. Proves the
/// green sweep's cleanliness IS the FPW guarantee, not an accident.
#[test]
fn red_torn_write_without_fpw_is_caught() {
    if std::env::var(ROLE_ENV).is_ok() {
        return;
    }
    let base = std::env::temp_dir().join(format!("pgrust_simredfpw_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&base);
    std::fs::create_dir_all(&base).unwrap();
    // FPW only matters for pages whose history STRADDLES a completed
    // checkpoint: replay rebuilds any younger page from self-contained
    // insert records alone. So the tear must hit the REWRITE of a page that
    // the first checkpoint already flushed durably (its pre-redo-point rows
    // exist only on disk): pick the first heap write AFTER the first
    // heap-file fsync whose page offset was also written BEFORE it. Heap
    // write ranks are FPW-independent (same rows, same pages), so the green
    // baseline's rank carries to the FPW-off run.
    let (_, meta) = run_point(&base, 0, "", &[]);
    let heap_ops: Vec<(u64, String, i64)> = meta
        .lines()
        .filter_map(|l| l.strip_prefix("optrace: "))
        .filter(|l| l.contains("class=Heap"))
        .filter_map(|l| {
            let mut k = None;
            let mut kind = None;
            let mut off = None;
            for w in l.split_whitespace() {
                if let Some(v) = w.strip_prefix("k=") {
                    k = v.parse().ok();
                } else if let Some(v) = w.strip_prefix("kind=") {
                    kind = Some(v.to_string());
                } else if let Some(v) = w.strip_prefix("off=") {
                    off = v.parse().ok();
                }
            }
            Some((k?, kind?, off.unwrap_or(-1)))
        })
        .collect();
    let k_fsync1 = heap_ops
        .iter()
        .find(|(_, kind, _)| kind == "Fsync" || kind == "Fdatasync")
        .map(|(k, _, _)| *k)
        .expect("checkpoint must fsync heap files");
    let pre: std::collections::BTreeSet<i64> = heap_ops
        .iter()
        .filter(|(k, kind, _)| *k < k_fsync1 && kind == "PWriteV")
        .map(|(_, _, off)| *off)
        .collect();
    let mut j: u64 = 0;
    let mut rewrite_rank: Option<u64> = None;
    for (k, kind, off) in &heap_ops {
        if kind == "PWriteV" {
            j += 1;
            if *k > k_fsync1 && pre.contains(off) {
                rewrite_rank = Some(j);
                break;
            }
        }
    }
    let j = rewrite_rank
        .expect("a checkpointed heap page must be rewritten later (partial-page refill)");
    let extra: Vec<(&str, String)> = vec![(TORN_ENV, format!("heap:{j}:1024"))];
    let (rtext, meta) = run_point(&base, 0, "fpw-torn", &extra);
    let _ = std::fs::remove_dir_all(&base);

    assert_eq!(meta_field(&meta, "cuts"), "1", "fpw-torn red must cut mid-write: {meta}");
    assert!(meta.contains("faultlog: TORN"), "fpw-torn red must tear: {meta}");
    assert!(
        rtext.contains("VIOLATION:"),
        "with full-page writes disabled the torn heap page must be caught:\n{rtext}"
    );
}

/// inc-5 EMFILE battery: file-descriptor exhaustion injected at swept Open
/// points. `once` legs (a single EMFILE) must be absorbed by the fd layer's
/// LRU-release retry (degrade cleanly) or fail loudly; `sticky` legs (every
/// open fails from the j-th on) must stop the engine loudly. In EVERY case
/// the post-regime crash image must recover with zero property violations —
/// degraded, loud, but never corrupt.
#[test]
fn emfile_exhaustion_battery() {
    if std::env::var(ROLE_ENV).is_ok() {
        return;
    }
    let base = std::env::temp_dir().join(format!("pgrust_simemf_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&base);
    std::fs::create_dir_all(&base).unwrap();

    let (_, meta) = run_point(&base, 0, "", &[]);
    let trace = parse_trace(&meta);
    let n_open = trace.iter().filter(|(_, kind, _, _)| kind == "Open").count() as u64;
    assert!(n_open >= 8, "workload must open files ({n_open} opens)");

    let mut specs: Vec<String> = Vec::new();
    for i in 0..8u64 {
        specs.push(format!("once:{}", 1 + i * (n_open - 1) / 7));
    }
    for i in 0..5u64 {
        specs.push(format!("sticky:{}", 1 + i * (n_open - 1) / 4));
    }
    eprintln!("EMFILE BATTERY: {} injection points over {n_open} opens", specs.len());

    let results = std::sync::Mutex::new(Vec::<(String, u32, String, usize)>::new());
    let failures = std::sync::Mutex::new(Vec::<String>::new());
    let next = std::sync::atomic::AtomicUsize::new(0);
    std::thread::scope(|s| {
        for _ in 0..6 {
            s.spawn(|| loop {
                let i = next.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                if i >= specs.len() {
                    break;
                }
                let spec = &specs[i];
                let tag = format!("_emf_{}", spec.replace(':', "_"));
                let extra: Vec<(&str, String)> = vec![(EMFILE_ENV, spec.clone())];
                let (rtext, meta) = run_point_tagged(&base, 0, "", &extra, &tag);
                let acked: u32 = meta_field(&meta, "acked").parse().unwrap();
                let stopped = meta_field(&meta, "stopped");
                let nviol = rtext.lines().filter(|l| l.contains("VIOLATION: ")).count();
                if meta_field(&meta, "cuts") != "1" {
                    failures
                        .lock()
                        .unwrap()
                        .push(format!("emfile {spec}: end-of-regime cut missing"));
                }
                // Fail LOUDLY or complete: a partial run without a loud stop
                // would be a silent-degradation bug.
                if acked < TXNS && stopped == "-" {
                    failures.lock().unwrap().push(format!(
                        "emfile {spec}: silent degradation (acked {acked}/{TXNS}, no stop)"
                    ));
                }
                for line in rtext.lines() {
                    if let Some(p) = line.find("VIOLATION: ") {
                        failures
                            .lock()
                            .unwrap()
                            .push(format!("emfile {spec}: {}", &line[p + 11..]));
                    }
                }
                results.lock().unwrap().push((spec.clone(), acked, stopped, nviol));
            });
        }
    });
    let mut results = results.into_inner().unwrap();
    results.sort();
    let mut absorbed = 0;
    for (spec, acked, stopped, nviol) in &results {
        eprintln!("EMFILE {spec}: acked={acked}/{TXNS} stopped={stopped} violations={nviol}");
        if spec.starts_with("once:") && *acked == TXNS && stopped == "-" {
            absorbed += 1;
        }
    }
    let failures_v = failures.into_inner().unwrap();
    assert!(
        failures_v.is_empty(),
        "EMFILE BATTERY FAILURES ({}):\n{}",
        failures_v.len(),
        failures_v.join("\n")
    );
    // The fd layer's EMFILE machinery (ReleaseLruFile retry) must absorb at
    // least one single-shot injection transparently, or the battery is not
    // actually exercising the native degrade path.
    assert!(
        absorbed >= 1,
        "no once-EMFILE point was absorbed by the fd LRU-release retry:\n{results:?}"
    );

    // Replay determinism x3 at one sticky exhaustion point.
    let spec = specs.last().unwrap().clone();
    let mut runs: Vec<(Vec<(String, Vec<u8>)>, String, String)> = Vec::new();
    for rep in 0..3 {
        let pack = base.join(format!("emfdet_rep{rep}"));
        let (ok, _) = spawn_child(
            "sim_sweep_writer_child",
            &[
                (ROLE_ENV, "writer".into()),
                (K_ENV, "0".into()),
                (PACK_ENV, pack.to_str().unwrap().into()),
                (RED_ENV, String::new()),
                (EMFILE_ENV, spec.clone()),
            ],
        );
        assert!(ok);
        let (ok, rtext) = spawn_child(
            "sim_sweep_recover_child",
            &[
                (ROLE_ENV, "recover".into()),
                (PACK_ENV, pack.to_str().unwrap().into()),
            ],
        );
        assert!(ok, "emfile det rep {rep} failed:\n{rtext}");
        let mut tree = Vec::new();
        tree_digest(&pack.join("root"), &mut tree);
        let m = normalize_pid(&std::fs::read_to_string(pack.join("meta.txt")).unwrap());
        let state =
            marker_line(&rtext, "SIM_RECOVER_STATE").unwrap_or_else(|| "MISSING".to_string());
        assert_ne!(state, "MISSING");
        runs.push((tree, m, state));
        let _ = std::fs::remove_dir_all(&pack);
    }
    for rep in 1..3 {
        assert_eq!(runs[0].0, runs[rep].0, "emfile det: pack trees differ");
        assert_eq!(runs[0].1, runs[rep].1, "emfile det: metas differ");
        assert_eq!(runs[0].2, runs[rep].2, "emfile det: recovered state differs");
    }
    eprintln!("EMFILE DETERMINISM x3 OK ({spec}): {}", runs[0].2);
    let _ = std::fs::remove_dir_all(&base);
}

/// inc-5 RED (EMFILE class): a buggy server that ACKS a transaction which
/// failed under descriptor exhaustion. The sweep's acked-txn-survives
/// property must flag the loss — proving the battery's "never corrupt"
/// verdict has teeth against dirty degradation.
#[test]
fn red_emfile_acked_loss_is_caught() {
    if std::env::var(ROLE_ENV).is_ok() {
        return;
    }
    let base = std::env::temp_dir().join(format!("pgrust_simredemf_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&base);
    std::fs::create_dir_all(&base).unwrap();
    let (rtext, meta) = run_point(&base, 0, "emfile-ack", &[]);
    let _ = std::fs::remove_dir_all(&base);

    assert_eq!(meta_field(&meta, "cuts"), "1", "emfile-ack red must cut: {meta}");
    assert!(
        meta_field(&meta, "stopped").contains("acked anyway"),
        "emfile-ack red must hit the buggy-ack path: {meta}"
    );
    assert!(
        rtext.contains("VIOLATION:") && rtext.contains("ACKED txn"),
        "the sweep must flag the acked-but-lost transaction:\n{rtext}"
    );
}

// ---------------------------------------------------------------------------
// inc-6 arms: GIN pending-list lane, BRIN summarization lane, LP-reuse
// vacuum-content lane — sweeps, weakened-redo reds, determinism.
// ---------------------------------------------------------------------------

/// Parse the writer meta's `win=<name>:<lo>:<hi>` window lines.
fn meta_windows(meta: &str) -> std::collections::BTreeMap<String, (u64, u64)> {
    meta.lines()
        .filter_map(|l| l.strip_prefix("win="))
        .filter_map(|l| {
            let mut it = l.split(':');
            let name = it.next()?.to_string();
            let lo: u64 = it.next()?.parse().ok()?;
            let hi: u64 = it.next()?.parse().ok()?;
            Some((name, (lo, hi)))
        })
        .collect()
}

/// The shared 6-worker cut-point pool (the product/btree/initdb sweep shape):
/// returns (points that cut, collected VIOLATION lines).
fn sweep_points(
    base: &std::path::Path,
    points: &[u64],
    extra: &[(&str, String)],
    label: &str,
) -> (u64, Vec<String>) {
    let failures = std::sync::Mutex::new(Vec::<String>::new());
    let cut_points = std::sync::atomic::AtomicU64::new(0);
    let next = std::sync::atomic::AtomicUsize::new(0);
    std::thread::scope(|s| {
        for _ in 0..6 {
            s.spawn(|| loop {
                let i = next.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                if i >= points.len() {
                    break;
                }
                let k = points[i];
                let (rtext, meta) = run_point(base, k, "", extra);
                if meta_field(&meta, "cuts") == "0" {
                    failures
                        .lock()
                        .unwrap()
                        .push(format!("{label} k={k}: planned cut never fired"));
                    continue;
                }
                cut_points.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                for line in rtext.lines() {
                    if let Some(i) = line.find("VIOLATION: ") {
                        failures.lock().unwrap().push(line[i + 11..].to_string());
                    }
                }
            });
        }
    });
    (cut_points.into_inner(), failures.into_inner().unwrap())
}

/// Replay determinism x3 at `pins`: byte-identical packs, metas (incl. fault
/// log + windows) and recovered state — the standing gate, shared by the
/// inc-6 arms.
fn determinism_x3(base: &std::path::Path, pins: &[u64], extra: &[(&str, String)], label: &str) {
    for &pin in pins {
        let mut runs: Vec<(Vec<(String, Vec<u8>)>, String, String)> = Vec::new();
        for rep in 0..3 {
            let pack = base.join(format!("{label}det_k{pin}_rep{rep}"));
            let mut wenvs: Vec<(&str, String)> = vec![
                (ROLE_ENV, "writer".into()),
                (K_ENV, pin.to_string()),
                (PACK_ENV, pack.to_str().unwrap().into()),
                (RED_ENV, String::new()),
            ];
            wenvs.extend(extra.iter().cloned());
            let (ok, _) = spawn_child("sim_sweep_writer_child", &wenvs);
            assert!(ok);
            let mut renvs: Vec<(&str, String)> = vec![
                (ROLE_ENV, "recover".into()),
                (PACK_ENV, pack.to_str().unwrap().into()),
            ];
            renvs.extend(extra.iter().cloned());
            let (ok, rtext) = spawn_child("sim_sweep_recover_child", &renvs);
            assert!(ok, "{label} det recover k={pin} rep {rep} failed:\n{rtext}");
            let mut tree = Vec::new();
            tree_digest(&pack.join("root"), &mut tree);
            let m = normalize_pid(&std::fs::read_to_string(pack.join("meta.txt")).unwrap());
            let state = marker_line(&rtext, "SIM_RECOVER_STATE")
                .unwrap_or_else(|| "MISSING".to_string());
            assert_ne!(state, "MISSING", "{label} det k={pin} rep {rep}: state line absent");
            runs.push((tree, m, state));
            let _ = std::fs::remove_dir_all(&pack);
        }
        assert!(runs[0].1.contains("cuts=1"), "{label} det pin k={pin} must cut");
        for rep in 1..3 {
            assert_eq!(runs[0].0, runs[rep].0, "{label} det k={pin}: pack trees differ");
            assert_eq!(runs[0].1, runs[rep].1, "{label} det k={pin}: metas differ");
            assert_eq!(runs[0].2, runs[rep].2, "{label} det k={pin}: recovered state differs");
        }
        eprintln!("{} DETERMINISM x3 OK at k={pin}: {}", label.to_uppercase(), runs[0].2);
    }
}

/// inc-6 GIN LANE sweep: the pending-list workload (every gininsert buffered
/// in the pending list) with two product ginInsertCleanup merges, under the
/// stratified crash sweep — dense windows on the first gin-index write and
/// BOTH cleanup spans. Heap properties + the gin walk/coverage properties
/// must hold at every point. Ends with determinism x3 at two rotated pins.
#[test]
fn gin_pending_list_crash_recovery_sweep() {
    if std::env::var(ROLE_ENV).is_ok() {
        return;
    }
    let base = std::env::temp_dir().join(format!("pgrust_simsweep_gin_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&base);
    std::fs::create_dir_all(&base).unwrap();
    let extra: Vec<(&str, String)> = vec![(ARM_ENV, "gin".into())];

    let (rtext, meta) = run_point(&base, 0, "", &extra);
    assert_eq!(meta_field(&meta, "acked"), GIN_TXNS.to_string(), "gin baseline acks all: {meta}");
    assert_eq!(meta_field(&meta, "cuts"), "0", "gin baseline must not cut: {meta}");
    assert!(
        rtext.contains("SIM_RECOVER_DONE violations=0"),
        "gin baseline recovery must be clean:\n{rtext}"
    );
    let baseline_state = marker_line(&rtext, "SIM_RECOVER_STATE").unwrap_or_default();
    assert!(
        baseline_state.contains(&format!("visible_rows={}", GIN_TXNS * GIN_ROWS)),
        "gin baseline fold must carry all rows: {baseline_state}"
    );
    let n: u64 = meta_field(&meta, "ops").parse().unwrap();
    let trace = parse_trace(&meta);
    assert_eq!(trace.len() as u64, n, "trace must cover the whole workload span");
    // The gin cut classes must EXIST: gin-index-file page writes (pending
    // list + entry tree + checkpoint flushes) and both cleanup windows.
    let gin_path_frag = format!("/base/5/{GIN_IDX_OID}");
    let k_gidx = trace
        .iter()
        .find(|(_, kind, _, path)| kind == "PWriteV" && path.contains(&gin_path_frag))
        .map(|(k, _, _, _)| *k)
        .expect("workload must write gin index pages");
    let wins = meta_windows(&meta);
    let (c1_lo, c1_hi) = wins["clean1"];
    let (c2_lo, c2_hi) = wins["clean2"];
    assert!(c1_lo < c1_hi && c2_lo < c2_hi && c2_hi <= n, "cleanup windows sane: {wins:?}");

    let windows = [
        (k_gidx.saturating_sub(2), k_gidx + 10),
        (c1_lo, c1_hi.min(c1_lo + 18)),
        (c2_lo, c2_hi.min(c2_lo + 18)),
    ];
    let points = stratify(&trace, n, 80, &windows);
    eprintln!(
        "GIN-PENDING SWEEP: stratified {} cut points over {n} ops (first gin-index write \
         k={k_gidx}, cleanup windows [{c1_lo},{c1_hi}] [{c2_lo},{c2_hi}])",
        points.len()
    );
    let (cut_points, failures) = sweep_points(&base, &points, &extra, "gin");
    eprintln!(
        "GIN-PENDING SWEEP: {cut_points} cut points over {n} ops ({GIN_TXNS} txns x \
         {GIN_ROWS} rows, {GIN_DISTINCT} distinct keys, 2 cleanups, whole-node kill), \
         {} violations",
        failures.len()
    );
    assert_eq!(cut_points as usize, points.len(), "every gin sweep point must cut");
    assert!(
        failures.is_empty(),
        "GIN PENDING-LIST CRASH-RECOVERY VIOLATIONS ({}):\n{}",
        failures.len(),
        failures.join("\n")
    );

    determinism_x3(&base, &[n / 2, c1_lo + 2], &extra, "gin");
    let _ = std::fs::remove_dir_all(&base);
}

/// inc-6 BRIN LANE sweep: the summarization workload (two product
/// brinsummarize passes, per-row samepage widening, a desummarize) under the
/// stratified crash sweep — dense windows on all three spans. Heap
/// properties + the consistent-or-wider coverage property at every point.
#[test]
fn brin_summarize_crash_recovery_sweep() {
    if std::env::var(ROLE_ENV).is_ok() {
        return;
    }
    let base = std::env::temp_dir().join(format!("pgrust_simsweep_brin_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&base);
    std::fs::create_dir_all(&base).unwrap();
    let extra: Vec<(&str, String)> = vec![(ARM_ENV, "brin".into())];

    let (rtext, meta) = run_point(&base, 0, "", &extra);
    assert_eq!(meta_field(&meta, "acked"), BR_TXNS.to_string(), "brin baseline acks all: {meta}");
    assert_eq!(meta_field(&meta, "cuts"), "0", "brin baseline must not cut: {meta}");
    assert!(
        rtext.contains("SIM_RECOVER_DONE violations=0"),
        "brin baseline recovery must be clean:\n{rtext}"
    );
    let baseline_state = marker_line(&rtext, "SIM_RECOVER_STATE").unwrap_or_default();
    assert!(
        baseline_state.contains(&format!("visible_rows={}", BR_TXNS * BR_ROWS)),
        "brin baseline fold must carry all rows: {baseline_state}"
    );
    let n: u64 = meta_field(&meta, "ops").parse().unwrap();
    let trace = parse_trace(&meta);
    assert_eq!(trace.len() as u64, n, "trace must cover the whole workload span");
    let brin_path_frag = format!("/base/5/{BRIN_IDX_OID}");
    let k_bidx = trace
        .iter()
        .find(|(_, kind, _, path)| kind == "PWriteV" && path.contains(&brin_path_frag))
        .map(|(k, _, _, _)| *k)
        .expect("workload must write brin index pages");
    let wins = meta_windows(&meta);
    let (s1_lo, s1_hi) = wins["sum1"];
    let (s2_lo, s2_hi) = wins["sum2"];
    let (d_lo, d_hi) = wins["desum"];
    assert!(s1_lo < s1_hi && s2_lo < s2_hi && d_lo <= d_hi && d_hi <= n);

    let windows = [
        (k_bidx.saturating_sub(2), k_bidx + 10),
        (s1_lo, s1_hi.min(s1_lo + 18)),
        (s2_lo, s2_hi.min(s2_lo + 18)),
        (d_lo, d_hi.min(d_lo + 10)),
    ];
    let points = stratify(&trace, n, 80, &windows);
    eprintln!(
        "BRIN-SUMMARIZE SWEEP: stratified {} cut points over {n} ops (first brin-index \
         write k={k_bidx}, summarize windows [{s1_lo},{s1_hi}] [{s2_lo},{s2_hi}], \
         desummarize [{d_lo},{d_hi}])",
        points.len()
    );
    let (cut_points, failures) = sweep_points(&base, &points, &extra, "brin");
    eprintln!(
        "BRIN-SUMMARIZE SWEEP: {cut_points} cut points over {n} ops ({BR_TXNS} txns x \
         {BR_ROWS} rows, ppr={BR_PPR}, 2 summarize passes + desummarize, whole-node \
         kill), {} violations",
        failures.len()
    );
    assert_eq!(cut_points as usize, points.len(), "every brin sweep point must cut");
    assert!(
        failures.is_empty(),
        "BRIN SUMMARIZATION CRASH-RECOVERY VIOLATIONS ({}):\n{}",
        failures.len(),
        failures.join("\n")
    );

    determinism_x3(&base, &[n / 2, s1_lo + 2], &extra, "brin");
    let _ = std::fs::remove_dir_all(&base);
}

/// inc-6 VACUUM-CONTENT LANE sweep: range delete -> btbulkdelete (item
/// removal + _bt_pagedel on the emptied leaves) -> product prune to
/// LP_UNUSED -> reinserts into the REUSED line pointers, swept across the
/// whole lifecycle with a dense window on the vacuum span. Heap fold (with
/// resurrected/lost classification) + btree walk/coverage at every point.
#[test]
fn lpreuse_vacuum_content_crash_recovery_sweep() {
    if std::env::var(ROLE_ENV).is_ok() {
        return;
    }
    let base = std::env::temp_dir().join(format!("pgrust_simsweep_lpr_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&base);
    std::fs::create_dir_all(&base).unwrap();
    let extra: Vec<(&str, String)> = vec![(ARM_ENV, "lpreuse".into())];

    let (rtext, meta) = run_point(&base, 0, "", &extra);
    assert_eq!(
        meta_field(&meta, "acked"),
        LP_TXNS.to_string(),
        "lpreuse baseline acks all: {meta}"
    );
    assert_eq!(meta_field(&meta, "cuts"), "0", "lpreuse baseline must not cut: {meta}");
    assert!(
        rtext.contains("SIM_RECOVER_DONE violations=0"),
        "lpreuse baseline recovery must be clean:\n{rtext}"
    );
    let baseline_state = marker_line(&rtext, "SIM_RECOVER_STATE").unwrap_or_default();
    assert!(
        baseline_state.contains(&format!("visible_rows={}", 4 * LP_ROWS)),
        "lpreuse baseline fold: txns 1-2 + reinserts 6-7 survive: {baseline_state}"
    );
    // Vacuum-content non-vacuity: btbulkdelete removed exactly txns 3-4's
    // entries AND _bt_pagedel actually deleted emptied leaves (V5-O3).
    assert_eq!(
        meta_field(&meta, "lp_vac_removed"),
        (2 * LP_ROWS).to_string(),
        "btbulkdelete must remove exactly the deleted txns' entries: {meta}"
    );
    let pagedel: i64 = meta_field(&meta, "lp_pages_deleted").parse().unwrap();
    assert!(
        pagedel >= 1,
        "the range delete must empty whole leaves into _bt_pagedel (got {pagedel}): {meta}"
    );
    // LP-reuse non-vacuity: the reinsert generation must consume the freed
    // blocks (land in reused line pointers), or the lane name is a lie. One
    // leftover queue entry is the targblock duplicate: the LAST freed block
    // is also the relation's cached insertion target, so reinserts fill it
    // through targblock and its queue entry never pops.
    let reuse_left: u32 = meta_field(&meta, "lp_reuse_left").parse().unwrap();
    assert!(reuse_left <= 1, "reinserts must drain the freed-block queue: {meta}");
    let n: u64 = meta_field(&meta, "ops").parse().unwrap();
    let trace = parse_trace(&meta);
    assert_eq!(trace.len() as u64, n, "trace must cover the whole workload span");
    let idx_path_frag = format!("/base/5/{IDX_OID}");
    let k_idx = trace
        .iter()
        .find(|(_, kind, _, path)| kind == "PWriteV" && path.contains(&idx_path_frag))
        .map(|(k, _, _, _)| *k)
        .expect("workload must write btree index pages");
    let wins = meta_windows(&meta);
    let (v_lo, v_hi) = wins["vac"];
    assert!(v_lo < v_hi && v_hi <= n);

    let windows = [(k_idx.saturating_sub(2), k_idx + 10), (v_lo, v_hi.min(v_lo + 24))];
    let points = stratify(&trace, n, 90, &windows);
    eprintln!(
        "LP-REUSE SWEEP: stratified {} cut points over {n} ops (first index write \
         k={k_idx}, vacuum window [{v_lo},{v_hi}], {pagedel} pages deleted)",
        points.len()
    );
    let (cut_points, failures) = sweep_points(&base, &points, &extra, "lpreuse");
    eprintln!(
        "LP-REUSE SWEEP: {cut_points} cut points over {n} ops ({LP_TXNS} txns x {LP_ROWS} \
         rows, delete txn {LP_DELETE_TXN} + btbulkdelete/_bt_pagedel + prune-to-unused + \
         reinserts, whole-node kill), {} violations",
        failures.len()
    );
    assert_eq!(cut_points as usize, points.len(), "every lpreuse sweep point must cut");
    assert!(
        failures.is_empty(),
        "LP-REUSE VACUUM-CONTENT CRASH-RECOVERY VIOLATIONS ({}):\n{}",
        failures.len(),
        failures.join("\n")
    );

    determinism_x3(&base, &[n / 2, v_lo + 2], &extra, "lpreuse");
    let _ = std::fs::remove_dir_all(&base);
}

/// Shared shape of the inc-6 weakened-redo reds: run the GREEN writer once
/// (the red selector only positions the workload stop + end-of-run cut),
/// prove the pack recovers CLEAN without the weakening (attribution), then
/// recover it again with the weakened redo armed and return that output.
fn run_weakened_redo_red(
    base: &std::path::Path,
    arm: &str,
    writer_red: &str,
    redo_red: &str,
) -> (String, String) {
    let pack = base.join(format!("pack_{writer_red}"));
    let (ok, wtext) = spawn_child(
        "sim_sweep_writer_child",
        &[
            (ROLE_ENV, "writer".into()),
            (K_ENV, "0".into()),
            (PACK_ENV, pack.to_str().unwrap().into()),
            (RED_ENV, writer_red.into()),
            (ARM_ENV, arm.into()),
        ],
    );
    assert!(ok && wtext.contains("SIM_WRITER_DONE"), "writer failed:\n{wtext}");
    let meta = std::fs::read_to_string(pack.join("meta.txt")).unwrap();
    assert_eq!(meta_field(&meta, "cuts"), "1", "red writer must cut: {meta}");
    let (ok, clean) = spawn_child(
        "sim_sweep_recover_child",
        &[
            (ROLE_ENV, "recover".into()),
            (PACK_ENV, pack.to_str().unwrap().into()),
        ],
    );
    assert!(
        ok && clean.contains("SIM_RECOVER_DONE violations=0"),
        "ATTRIBUTION: the same pack must recover CLEAN without the weakened redo \
         ({redo_red}):\n{clean}"
    );
    let (ok, red) = spawn_child(
        "sim_sweep_recover_child",
        &[
            (ROLE_ENV, "recover".into()),
            (PACK_ENV, pack.to_str().unwrap().into()),
            (REDO_RED_ENV, redo_red.into()),
        ],
    );
    assert!(ok, "weakened recover ({redo_red}) child failed:\n{red}");
    let _ = std::fs::remove_dir_all(&pack);
    (meta, red)
}

/// inc-6 RED (gin lane): a WEAKENED gin redo — redo_insert_listpage restores
/// the pending page's structure but SKIPS its tuple content
/// (gin_xlog::sim_red). The gin coverage property must catch the loss
/// SILENTLY (structure stays walkable; no replay failure). If this comes
/// back green, the pending-list cut class has no teeth.
#[test]
fn red_gin_lost_listpage_redo_is_caught() {
    if std::env::var(ROLE_ENV).is_ok() {
        return;
    }
    let base = std::env::temp_dir().join(format!("pgrust_simredgin_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&base);
    std::fs::create_dir_all(&base).unwrap();
    let (meta, red) = run_weakened_redo_red(&base, "gin", "gin-pending", "gin-listpage");
    let _ = std::fs::remove_dir_all(&base);

    assert_eq!(meta_field(&meta, "acked"), "3", "gin red stops pre-cleanup: {meta}");
    assert!(
        red.contains("VIOLATION:") && red.contains("gin index coverage diverges"),
        "the weakened listpage redo must be caught by the gin coverage property:\n{red}"
    );
    assert!(
        !red.contains("RECOVERY FAILED") && !red.contains("RECOVERY PANICKED"),
        "the gin red must be a SILENT-loss catch, not a replay failure:\n{red}"
    );
}

/// inc-6 RED (brin lane): a WEAKENED brin redo — samepage summary updates
/// keep the OLD (narrower) tuple (brin_xlog::sim_red), so the recovered
/// range EXCLUDES rows the heap holds. The query-level consistent-or-wider
/// coverage check must catch it SILENTLY.
#[test]
fn red_brin_narrowing_redo_is_caught() {
    if std::env::var(ROLE_ENV).is_ok() {
        return;
    }
    let base = std::env::temp_dir().join(format!("pgrust_simredbrin_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&base);
    std::fs::create_dir_all(&base).unwrap();
    let (meta, red) = run_weakened_redo_red(&base, "brin", "brin-stale", "brin-narrow");
    let _ = std::fs::remove_dir_all(&base);

    assert_eq!(
        meta_field(&meta, "acked"),
        BR_NEG_TXN.to_string(),
        "brin red stops after the widening txn: {meta}"
    );
    assert!(
        red.contains("VIOLATION:") && red.contains("NARROWER than heap"),
        "the narrowing redo must be caught by the coverage check:\n{red}"
    );
    assert!(
        !red.contains("RECOVERY FAILED") && !red.contains("RECOVERY PANICKED"),
        "the brin red must be a SILENT catch, not a replay failure:\n{red}"
    );
}

/// inc-6 RED (vacuum-content, the V-O1 SILENT-ONLY leg): a WEAKENED btree
/// vacuum redo — XLOG_BTREE_VACUUM keeps the deleted items
/// (nbtree_xlog::sim_red). With the heap line pointers REUSED by the
/// reinsert generation, the stale entries resolve to live rows with
/// DIFFERENT keys: the walk/coverage properties must carry the catch ALONE
/// (structure stays walkable — any replay failure fails this test).
#[test]
fn red_btree_lost_vacuum_content_redo_is_caught_silently() {
    if std::env::var(ROLE_ENV).is_ok() {
        return;
    }
    let base = std::env::temp_dir().join(format!("pgrust_simredbtv_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&base);
    std::fs::create_dir_all(&base).unwrap();
    let (meta, red) = run_weakened_redo_red(&base, "lpreuse", "lpr-stale", "btvac-keep");
    let _ = std::fs::remove_dir_all(&base);

    assert_eq!(meta_field(&meta, "acked"), LP_TXNS.to_string(), "full lifecycle ran: {meta}");
    assert!(
        red.contains("VIOLATION:")
            && (red.contains("!= heap value")
                || red.contains("points at missing heap item")
                || red.contains("index coverage diverges")),
        "the kept-stale-entries redo must be caught by the index properties:\n{red}"
    );
    assert!(
        !red.contains("RECOVERY FAILED") && !red.contains("RECOVERY PANICKED"),
        "V-O1: the vacuum-content red must be caught SILENTLY by the walk/coverage \
         properties alone:\n{red}"
    );
}

/// inc-6 RED (vacuum-content, heap side): a WEAKENED heap prune redo — the
/// prune record's LP_UNUSED transitions are DROPPED (a harness seam wrap;
/// no product hook). Replay then collides the reinsert redo with the
/// still-occupied line pointers, or resurrects/loses tuples — either way
/// the sweep must flag it (loud replay failure or the classified fold
/// divergence both count; the SILENT-only requirement lives on the btree
/// leg above).
#[test]
fn red_heap_lost_prune_redo_is_caught() {
    if std::env::var(ROLE_ENV).is_ok() {
        return;
    }
    let base = std::env::temp_dir().join(format!("pgrust_simredhpr_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&base);
    std::fs::create_dir_all(&base).unwrap();
    let (meta, red) = run_weakened_redo_red(&base, "lpreuse", "lpr-stale", "heap-prune-keep");
    let _ = std::fs::remove_dir_all(&base);

    assert_eq!(meta_field(&meta, "acked"), LP_TXNS.to_string(), "full lifecycle ran: {meta}");
    assert!(
        red.contains("VIOLATION:"),
        "the dropped-LP_UNUSED redo must be caught (replay failure, resurrected/lost \
         classification, or fold divergence):\n{red}"
    );
}
