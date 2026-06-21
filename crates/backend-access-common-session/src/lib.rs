//! `backend/access/common/session.c` — encapsulation of a user session.
//!
//! Owned-value rewrite of PostgreSQL 18.3 `session.c`. A `Session` holds the
//! state shared between the leader and worker backends of a parallel query —
//! currently the per-session DSM segment + DSA area and the shared record
//! typmod registry (whose *storage* lives in the session, even though its
//! *logic* is owned by `typcache.c`). `CurrentSession` is the backend-global
//! handle, modeled here as a `thread_local!` exactly mirroring the C global.
//!
//! # Seam boundary
//!
//! `session.c` proper is just `InitializeSession` / `GetSessionDsmHandle` /
//! `AttachSession` / `DetachSession`. The seam crate
//! `backend-access-common-session-seams` additionally declares the six
//! `SharedRecordTypmodRegistry*` entry points that `typcache.c` *calls* but
//! whose storage hangs off `CurrentSession`; this crate owns them because the
//! shared registry/tables are `Session` fields.
//!
//! ## What this crate installs
//!
//! - `initialize_session` — `InitializeSession()`. Fully ported; this is the
//!   single-user boot-path entry (`InitPostgres`).
//! - `shared_registry_estimate` — `SharedRecordTypmodRegistryEstimate()`:
//!   `sizeof(SharedRecordTypmodRegistry)`. Fully ported.
//! - `shared_registry_attached` — reads `CurrentSession->shared_typmod_registry
//!   != NULL`. Fully ported (pure field read; always false in a single backend).
//! - `find_or_make_matching_shared_tupledesc` — the typcache shared path. Its C
//!   body returns NULL when no registry is attached, which is the only reachable
//!   case in a single backend; that early-return (`Ok(None)`) is ported so the
//!   caller falls back to the local `RecordCacheArray`. The attached (dshash)
//!   leg remains keystone-blocked and panics loudly, never silently stubs.
//!
//! ## What this crate does NOT install (keystone-blocked, NOT stubbed)
//!
//! The other three registry seams (`shared_registry_init`,
//! `shared_registry_attach`, `shared_typmod_table_find`) are the parallel-worker
//! path. Their faithful bodies create/attach the
//! registry's **record table** via `dshash_create(area,
//! &srtr_record_table_params, area)` — a dshash whose `compare`/`hash` are the
//! custom `shared_record_table_compare` / `shared_record_table_hash` callbacks
//! (they resolve a `dsa_pointer` via `dsa_get_address(area, ...)` and run
//! `equalRowTypes` / `hashRowType` over the addressed `TupleDesc`).
//!
//! The ported dshash (`backend-lib-dshash` over `types_storage::DshashParameters`
//! / `DshashKeyKind`) deliberately supports **only** the two built-in key sets
//! (`String`, `Binary`) — "function pointers can't be shared between backends"
//! — and has no variant for a caller-supplied compare/hash taking the
//! `dsa_area *arg`. So the record table is not expressible over the current
//! substrate. Installing those four would require an out-of-lane keystone:
//! widen `DshashKeyKind` with a `Custom { compare, hash, copy }` variant +
//! thread the `arg` through `backend-lib-dshash`, then move `equalRowTypes` /
//! `hashRowType` / `share_tupledesc` (all private to `typcache.c`) across the
//! seam. Until that lands these seams keep their loud default-panic — never a
//! silent stub. The single-user boot path never reaches them.

#![allow(non_snake_case)]

use std::cell::RefCell;
use std::sync::atomic::AtomicU32;

use mcx::{Mcx, MemoryContext, PgBox};
use types_error::{PgResult, ERROR};
use backend_utils_error::ereport;
use types_storage::storage::{dsm_handle, shm_toc_estimator, DSM_HANDLE_INVALID};
use types_storage::{
    dshash_table_handle, DsaArea, DshashKeyKind, DshashParameters, DshashTable,
    LWTRANCHE_PER_SESSION_DSA, LWTRANCHE_PER_SESSION_RECORD_TYPE,
    LWTRANCHE_PER_SESSION_RECORD_TYPMOD,
};
use types_execparallel::{DsmSegmentHandle, SerializeCursor};
use types_tuple::heaptuple::TupleDescData;

use backend_storage_ipc_dsm_core::dsm::{
    self, dsm_segment_address, dsm_segment_handle, DsmSegment, DsmSegmentId,
    DSM_CREATE_NULL_IF_MAXSEGMENTS,
};
use backend_storage_ipc_shm_toc::{
    shm_toc_estimate, shm_toc_estimate_chunk, shm_toc_estimate_keys,
    shm_toc_initialize_estimator, ShmToc,
};
use backend_utils_mmgr_dsa_seams as dsa;
use backend_utils_mmgr_mcxt_seams::top_memory_context;
use backend_lib_dshash_seams as dshash;

/// `SESSION_MAGIC` — magic number for the per-session DSM TOC (session.c:29).
const SESSION_MAGIC: u64 = 0xabb0fbc9;
/// `SESSION_DSA_SIZE` — enough to hold a very small registry (session.c:38).
const SESSION_DSA_SIZE: usize = 0x30000;
/// `SESSION_KEY_DSA` (session.c:43).
const SESSION_KEY_DSA: u64 = 0xFFFF_FFFF_FFFF_0001;
/// `SESSION_KEY_RECORD_TYPMOD_REGISTRY` (session.c:44).
const SESSION_KEY_RECORD_TYPMOD_REGISTRY: u64 = 0xFFFF_FFFF_FFFF_0002;

/// `struct SharedRecordTypmodRegistry` (typcache.c:178). Lives in the per-session
/// DSM segment (a `shm_toc` chunk), shared across leader and workers, so it is
/// `#[repr(C)]` with a shared atomic `next_typmod`.
#[repr(C)]
struct SharedRecordTypmodRegistry {
    /// `dshash_table_handle record_table_handle`.
    record_table_handle: dshash_table_handle,
    /// `dshash_table_handle typmod_table_handle`.
    typmod_table_handle: dshash_table_handle,
    /// `pg_atomic_uint32 next_typmod`.
    next_typmod: AtomicU32,
}

/// `struct SharedRecordTableKey` (typcache.c:196) — a record-table key that
/// holds either a backend-local `TupleDesc *` or a shared `dsa_pointer`. The
/// custom `DshashKeyKind::Record` callbacks resolve it to a `TupleDesc`. The
/// layout mirrors the C union+bool exactly: a pointer-sized union followed by a
/// `bool`, padded to pointer alignment.
#[repr(C)]
#[derive(Clone, Copy)]
struct SharedRecordTableKey {
    /// `union { TupleDesc local_tupdesc; dsa_pointer shared_tupdesc; }` — both
    /// pointer-sized; stored as the raw `u64` bit pattern.
    u: u64,
    /// `bool shared`.
    shared: bool,
}

/// `struct SharedRecordTableEntry` (typcache.c:211).
#[repr(C)]
#[derive(Clone, Copy)]
struct SharedRecordTableEntry {
    key: SharedRecordTableKey,
}

/// `struct SharedTypmodTableEntry` (typcache.c:220).
#[repr(C)]
#[derive(Clone, Copy)]
struct SharedTypmodTableEntry {
    typmod: u32,
    shared_tupdesc: u64,
}

/// `srtr_record_table_params` (typcache.c:275): the registry's TupleDesc table,
/// keyed by a `SharedRecordTableKey` via the custom `Record` callbacks.
fn srtr_record_table_params() -> DshashParameters {
    DshashParameters {
        key_size: core::mem::size_of::<SharedRecordTableKey>(),
        entry_size: core::mem::size_of::<SharedRecordTableEntry>(),
        key_kind: DshashKeyKind::Record,
        tranche_id: LWTRANCHE_PER_SESSION_RECORD_TYPE,
    }
}

/// `srtr_typmod_table_params` (typcache.c:285): the registry's typmod table,
/// keyed by a `uint32` typmod via the built-in binary callbacks.
fn srtr_typmod_table_params() -> DshashParameters {
    DshashParameters {
        key_size: core::mem::size_of::<u32>(),
        entry_size: core::mem::size_of::<SharedTypmodTableEntry>(),
        key_kind: DshashKeyKind::Binary,
        tranche_id: LWTRANCHE_PER_SESSION_RECORD_TYPMOD,
    }
}

/// Raw pointer to the registry's `next_typmod` atomic, for the typcache import
/// seam to seed from `NextRecordTypmod`.
fn registry_next_typmod_ptr(registry: *mut SharedRecordTypmodRegistry) -> *mut AtomicU32 {
    // SAFETY: `registry` addresses a live `SharedRecordTypmodRegistry`.
    unsafe { core::ptr::addr_of_mut!((*registry).next_typmod) }
}

/// `typedef struct Session` (`access/session.h`).
///
/// `segment`/`area` are the session-scoped DSM segment and DSA area;
/// `shared_typmod_registry`/`shared_record_table`/`shared_typmod_table` are the
/// shared record-typmod registry state managed by `typcache.c`. All start NULL
/// (`InitializeSession` zero-initializes the whole struct).
// Fields mirror the C `Session` struct 1:1. The DSM/DSA/registry fields are
// only written/read on the parallel-worker registry paths whose seams are
// keystone-blocked (not installed); they are present so the struct stays
// faithful and the install lands without re-layout when dshash gains a custom
// key kind.
struct Session {
    /// `dsm_segment *segment` — the session-scoped DSM segment (its id, the
    /// owned-value substitute for the `dsm_segment *`). `None` until created.
    segment: Option<DsmSegmentId>,
    /// `dsa_area *area` — the session-scoped DSA area handle. `None` until
    /// created. Stored as the `*mut DsaArea` the dshash/dsa seams consume.
    area: *mut DsaArea,
    /// `struct SharedRecordTypmodRegistry *shared_typmod_registry` — points into
    /// the per-session DSM segment.
    shared_typmod_registry: *mut SharedRecordTypmodRegistry,
    /// `dshash_table *shared_record_table`.
    shared_record_table: *mut DshashTable,
    /// `dshash_table *shared_typmod_table`.
    shared_typmod_table: *mut DshashTable,
}

impl Session {
    /// `MemoryContextAllocZero(TopMemoryContext, sizeof(Session))` — an empty
    /// (all-NULL) Session.
    const fn zeroed() -> Self {
        Session {
            segment: None,
            area: core::ptr::null_mut(),
            shared_typmod_registry: core::ptr::null_mut(),
            shared_record_table: core::ptr::null_mut(),
            shared_typmod_table: core::ptr::null_mut(),
        }
    }
}

thread_local! {
    /// `Session *CurrentSession = NULL;` — this backend's current session.
    /// `None` until `InitializeSession` runs.
    static CURRENT_SESSION: RefCell<Option<Session>> = const { RefCell::new(None) };

    /// The per-session memory context ("Session"), created lazily alongside
    /// `CurrentSession`. In C the `Session` struct is allocated directly in
    /// `TopMemoryContext`; here the owned-value `Session` lives in the
    /// `thread_local`, and we materialize the matching context so the lifetime
    /// correspondence (per-backend, freed at backend exit) is explicit.
    static SESSION_CONTEXT: RefCell<Option<MemoryContext>> = const { RefCell::new(None) };
}

/// `InitializeSession(void)` (session.c:54).
///
/// `CurrentSession = MemoryContextAllocZero(TopMemoryContext, sizeof(Session));`
///
/// Sets up `CurrentSession` to point to an empty `Session` object. The owned
/// rewrite materializes the per-session context and installs a zeroed `Session`
/// in the backend-global `thread_local`.
fn initialize_session() -> PgResult<()> {
    SESSION_CONTEXT.with(|c| {
        let mut slot = c.borrow_mut();
        if slot.is_none() {
            *slot = Some(MemoryContext::new("Session"));
        }
    });
    CURRENT_SESSION.with(|s| {
        *s.borrow_mut() = Some(Session::zeroed());
    });
    Ok(())
}

/// `GetSessionDsmHandle(void)` (session.c:70).
///
/// Initialize the per-session DSM segment if it isn't already initialized, and
/// return its handle so that worker processes can attach to it. The segment is
/// reused for the rest of this backend's lifetime.
///
/// C's contract (session.c:66-67): "Return `DSM_HANDLE_INVALID` if a segment
/// can't be allocated due to lack of resources." When INVALID is returned, the
/// parallel leader (`InitializeParallelDSM`) sets `nworkers = 0` and runs the
/// whole operation itself in backend-private memory — the leader-only path.
///
/// The segment holds a `shm_toc` with two chunks: the per-session DSA area
/// (`SESSION_KEY_DSA`) and the shared record-typmod registry
/// (`SESSION_KEY_RECORD_TYPMOD_REGISTRY`). `SharedRecordTypmodRegistryInit`
/// builds the registry (its record/typmod dshash tables) over that DSA area and
/// imports this backend's local `RecordCacheArray`.
#[allow(unreachable_code)]
fn get_session_dsm_handle() -> PgResult<dsm_handle> {
    // TEMPORARY: force the leader-only path by returning INVALID.
    //
    // Several fork-COW-shared-memory blockers are now fixed, so a parallel worker
    // gets MUCH further than before: it forks, joins the leader's lock group
    // (PGPROC.lockGroupLeader in real shmem), attaches as the error-queue sender,
    // restores the leader's transaction snapshot (PGPROC.xmin / databaseId /
    // statusFlags now in genuine shmem, so ProcArrayInstallRestoredXmin sees the
    // live leader's advertised xmin), restores the leader's GUC state (reset path
    // now passes reset_extra to the assign hooks), and resolves chunks in the
    // worker-attached DSM TOC (shm_toc_* dispatches worker-base vs leader-slot).
    //
    // The deep plan-shipping blocker is now CLEARED: `create_parallel_query_desc`
    // is installed (execMain), so the worker reconstructs its owned `QueryDesc`
    // from the serialized `PlannedStmt` text — `string_to_planned_stmt`
    // (backend-nodes-readfuncs::read_plannedstmt) reverses `ExecSerializePlan`'s
    // `_outPlannedStmt` field-for-field — then `CreateQueryDesc` + `ExecutorStart`
    // run in the worker. Verified empirically: with this early return removed the
    // worker forks, joins, restores the snapshot, deserializes the plan, and
    // reaches the executor (the prior `unrecognized token`/`stringToNode`
    // deserialization gap is gone).
    //
    // The tqueue DEST-BRIDGE leg (formerly blocker 1) is now CLEARED: tqueue's
    // `CreateTupleQueueDestReceiver` registers its three callbacks into the single
    // `backend-tcop-dest` router via `register_dest_receiver` (mirroring copyto's
    // `CreateCopyDestReceiver`), and `tqueueReceiveSlot` materializes the slot's
    // `MinimalTuple` through the new `&mut SlotData`-only
    // `exec_fetch_slot_minimal_tuple_copy_standalone` seam (the dest vtable's
    // `receiveSlot(mcx, state, slot)` boundary carries no `EState`). Verified
    // empirically: with this early return removed, the worker forks, joins,
    // restores the snapshot, deserializes the plan, reaches the executor, and now
    // gets PAST the dest layer — it crashes later, in `ProcArrayAdd` (below).
    //
    // The prior two crash walls are now CLEARED:
    //   1. `ProcArrayAdd` `pgxactoff == index-1` debug_assert (procarray
    //      membership.rs:113) — RESOLVED: PGPROC.pgxactoff + the dense ProcGlobal
    //      xids/subxidStates/statusFlags arrays are promoted to genuine shmem
    //      (proc_shmem.rs SHARED_PROC_PGXACTOFF / SHARED_PROC_XIDS / ...), so the
    //      worker reads the leader's renumbered offsets, not a fork-COW copy.
    //   2. `relation_open` `CheckRelationLockedByMe` assert — RESOLVED:
    //      `ExecGetRangeTableRelation` now branches on `IsParallelWorker()` (the
    //      `is_parallel_worker` execUtils-seam, installed from this parallel
    //      crate), so a worker takes its OWN AccessShareLock on the scan rel
    //      (`table_open(relid, rellockmode)`) instead of the leader's `NoLock`
    //      path + assert. Verified: the worker forks, joins the lock group,
    //      restores the snapshot, reconstructs the QueryDesc, passes ProcArrayAdd,
    //      and opens the scan relation without crashing.
    //
    // BUT the worker still cannot FINISH the query — the NEXT wall is the
    // worker-finish handshake (NOT a crash; a HANG): the parallel worker exits
    // cleanly (bgworker exit code 0) without the leader ever observing it as
    // finished, so `WaitForParallelWorkersToFinish` (parallel/lib.rs:1531, reached
    // via ExecGather → gather_readnext → exec_shutdown_gather_workers →
    // ExecParallelFinish) loops forever in `WaitLatch`. The worker exits too fast
    // to have executed the 10k-row scan, so it is short-circuiting before running
    // ParallelQueryMain's plan and/or its `parallel_worker_shutdown`
    // (PROCSIG_PARALLEL_MESSAGE 'X' + leader-latch set) is not nulling the
    // leader's `error_mqh`. This is the next keystone to port; until it lands,
    // returning INVALID keeps the known-good leader-only behavior (correct
    // results, no hang). (The tqueue dest-bridge it formerly also waited on has
    // LANDED.)
    //
    // UPDATE (procsignal shmem keystone landed): the worker-finish handshake
    // HANG is now resolved — `ProcSignalSlot`s were promoted into genuine
    // cross-process MAP_SHARED memory (was a fork-COW `OnceLock<Box<[...]>>`
    // copy, so a worker's `pss_signalFlags[PROCSIG_PARALLEL_MESSAGE]` write was
    // never observed by the leader; the leader's WaitLatch woke on the SIGUSR1
    // EINTR but `CheckProcSignal` read its own stale copy → no
    // `process_parallel_messages` → `error_mqh` never nulled → infinite loop).
    // Verified empirically: with this early return removed, both workers run
    // the plan, send the 'X' terminate, signal the leader, and the leader now
    // enters `process_parallel_messages` (the hang is gone).
    //
    // The NEXT wall (now a CRASH, not a hang) is the LWLock fork-COW keystone:
    // `MainLWLockArray` is a process-local `OnceLock<LWLockTable{ locks: Vec<…> }>`
    // (lwlock.rs:430) built on the "threaded backends share process memory"
    // assumption — but parallel workers are genuine `fork(2)` children, so each
    // gets a COW *copy* of every LWLock word. The leader + both workers therefore
    // do NOT mutually exclude on `ProcArrayLock`, so concurrent `ProcArrayAdd`
    // races corrupt the shared procarray's dense `pgxactoff` and trips the
    // `proc_pgxactoff == index-1` debug_assert (procarray membership.rs:113,
    // observed left:0 right:2). Fix = promote `MainLWLockArray` into the real
    // MAP_SHARED segment via `ShmemInitStruct` (same idiom as the PGPROC arrays
    // and this commit's ProcSignal slots) — a large, separate keystone. Until it
    // lands, returning INVALID keeps the known-good leader-only behavior.
    //
    // UPDATE (LWLock shmem keystone landed): `MainLWLockArray` is now in genuine
    // MAP_SHARED memory (CreateLWLocks allocates via the new `shmem_alloc` seam,
    // NOT `ShmemInitStruct` — the latter takes ShmemIndexLock, a MainLWLockArray
    // lock, which cannot exist while that very array is being built). VERIFIED
    // empirically with this early return REMOVED: real parallel `count(*) FROM
    // tenk1` now passes ProcArrayAdd (the `pgxactoff == index-1` debug_assert is
    // GONE — leader + workers mutually exclude on ProcArrayLock) and returns the
    // correct 10000 with two real fork(2) workers, promptly. The plan shape is
    // the exact PG18.3 Finalize/Gather/Partial/ParallelSeqScan.
    //
    // Three downstream walls remain (so the revert stays for now — they are NOT
    // the LWLock keystone, which is cleared):
    //   1. Worker procarray-slot cleanup leak: after a few parallel queries the
    //      workers' PGPROC slots are not released, so new workers hit
    //      `FATAL: sorry, too many clients already` → `parallel worker failed
    //      to initialize`. (worker shmem-exit / ProcArrayRemove detach gap.)
    //   2. EXPLAIN ANALYZE only: the worker-side per-PlanState instrumentation
    //      accumulation into the DSM `SharedExecutorInstrumentation` is not yet
    //      modeled (execParallel.rs:1283 honest panic — pre-existing residual,
    //      not on the plain count(*) path).
    //   3. An intermittent `RefCell already borrowed` in the parallel-context
    //      teardown (parallel/lib.rs:321 `with_globals`) under some teardown
    //      timing (re-entrant `with_globals`).
    return Ok(DSM_HANDLE_INVALID);

    // If we already created a session-scope segment, return its handle.
    if let Some(seg_id) = CURRENT_SESSION.with(|s| s.borrow().as_ref().and_then(|x| x.segment)) {
        return Ok(dsm_segment_handle(seg_id));
    }

    // dsm/dsa/toc descriptors are allocated in TopMemoryContext (C global), so
    // they outlive this (possibly short-lived) caller context.
    let top = top_memory_context::call();

    // Estimate space for the DSA area and the registry header.
    let mut estimator: shm_toc_estimator = shm_toc_estimator::default();
    shm_toc_initialize_estimator(&mut estimator);
    shm_toc_estimate_keys(&mut estimator, 1)?;
    shm_toc_estimate_chunk(&mut estimator, SESSION_DSA_SIZE)?;
    let typmod_registry_size = shared_registry_estimate();
    shm_toc_estimate_keys(&mut estimator, 1)?;
    shm_toc_estimate_chunk(&mut estimator, typmod_registry_size)?;
    let size = shm_toc_estimate(&estimator)?;

    // Set up the segment. On max-segments, return INVALID (leader-only fallback).
    let seg: DsmSegment = match dsm::dsm_create(size, DSM_CREATE_NULL_IF_MAXSEGMENTS, top)? {
        Some(seg) => seg,
        None => return Ok(DSM_HANDLE_INVALID),
    };
    let seg_id = seg.id();
    let base = dsm_segment_address(seg_id);
    let base_nn = core::ptr::NonNull::new(base).expect("dsm segment base is non-null");
    // SAFETY: `base` addresses `>= size` writable bytes of the fresh segment.
    let toc = unsafe { ShmToc::create(SESSION_MAGIC, base_nn, size) };

    // Create the per-session DSA area in place.
    let dsa_space = toc.allocate(SESSION_DSA_SIZE)?;
    let dsa_cursor = SerializeCursor(dsa_space.as_ptr() as usize);
    let seg_handle = DsmSegmentHandle(seg_id.as_u64() as usize);
    let area_handle =
        dsa::dsa_create_in_place::call(dsa_cursor, SESSION_DSA_SIZE, LWTRANCHE_PER_SESSION_DSA, seg_handle);
    let area: *mut DsaArea = area_handle.0 as *mut DsaArea;
    // SAFETY: as above; the DSA chunk lives in the segment.
    unsafe { toc.insert(SESSION_KEY_DSA, dsa_space)? };

    // Make the segment/area available to the registry init (it reads
    // CurrentSession->area).
    CURRENT_SESSION.with(|s| {
        let mut slot = s.borrow_mut();
        let sess = slot.as_mut().expect("CurrentSession");
        sess.segment = Some(seg_id);
        sess.area = area;
    });

    // Create the session-scoped shared record typmod registry in place.
    let registry_space = toc.allocate(typmod_registry_size)?;
    let registry = registry_space.as_ptr() as *mut SharedRecordTypmodRegistry;
    // SAFETY: `registry` addresses a fresh `typmod_registry_size`-byte chunk in
    // the segment; init writes the header and imports the local cache.
    init_shared_record_typmod_registry(registry, area)?;
    // SAFETY: as above.
    unsafe { toc.insert(SESSION_KEY_RECORD_TYPMOD_REGISTRY, registry_space)? };

    // Pin the mapping for the rest of this backend's life.
    dsa::dsa_pin_mapping::call(area)?;
    // dsm_pin_mapping consumes the segment value (resowner = NULL).
    let pinned_id = dsm::dsm_pin_mapping(seg);
    debug_assert_eq!(pinned_id.as_u64(), seg_id.as_u64());

    Ok(dsm_segment_handle(seg_id))
}

/// `SharedRecordTypmodRegistryInit(registry, segment, area)` (typcache.c:2197).
///
/// Creates the registry's two dshash tables over the session DSA area and
/// imports this backend's local `RecordCacheArray` (via the typcache-owned
/// `shared_registry_init` seam, which calls back into `share_tupledesc` /
/// the record+typmod table inserts). The record table uses the custom
/// `DshashKeyKind::Record` callbacks (typcache `shared_record_key_*`).
fn init_shared_record_typmod_registry(
    registry: *mut SharedRecordTypmodRegistry,
    area: *mut DsaArea,
) -> PgResult<()> {
    // Create the record/typmod tables (empty). The record table's `arg` is the
    // area (its Record callbacks resolve dsa_pointers); we pass it as the
    // table's area.
    let record_table = dshash::dshash_create::call(area, srtr_record_table_params())?;
    let typmod_table = dshash::dshash_create::call(area, srtr_typmod_table_params())?;

    // SAFETY: `registry` addresses a fresh registry-sized chunk in the segment.
    unsafe {
        (*registry).record_table_handle = dshash::dshash_get_hash_table_handle::call(record_table);
        (*registry).typmod_table_handle = dshash::dshash_get_hash_table_handle::call(typmod_table);
        core::ptr::addr_of_mut!((*registry).next_typmod).write(AtomicU32::new(0));
    }

    // Store the tables on the current session BEFORE importing, so the record
    // table's Record callbacks (which resolve via CurrentSession->area) work.
    CURRENT_SESSION.with(|s| {
        let mut slot = s.borrow_mut();
        let sess = slot.as_mut().expect("CurrentSession");
        sess.shared_record_table = record_table;
        sess.shared_typmod_table = typmod_table;
        sess.shared_typmod_registry = registry;
    });

    // Import the local RecordCacheArray into the shared tables. The typcache
    // owner copies each (typmod, tupdesc) into the DSA area (share_tupledesc)
    // and inserts into both tables, and stores `NextRecordTypmod` into
    // `registry->next_typmod`. For the common case (no blessed record types)
    // this is a no-op and the tables stay empty.
    backend_utils_cache_typcache_seams::shared_registry_import::call(
        record_table as usize,
        typmod_table as usize,
        area as usize,
        registry_next_typmod_ptr(registry) as usize,
    )?;

    Ok(())
}

/// `AttachSession(dsm_handle handle)` (session.c:154) — worker-side: attach to
/// the leader's per-session DSM segment, its DSA area, and the shared record
/// typmod registry.
fn attach_session(handle: dsm_handle) -> PgResult<()> {
    let top = top_memory_context::call();

    // Attach to the DSM segment.
    let seg: DsmSegment = match dsm::dsm_attach(handle, top)? {
        Some(seg) => seg,
        None => {
            return Err(ereport(ERROR)
                .errmsg_internal("could not attach to per-session DSM segment")
                .into_error())
        }
    };
    let seg_id = seg.id();
    let base = dsm_segment_address(seg_id);
    let base_nn = core::ptr::NonNull::new(base).expect("dsm segment base is non-null");
    // SAFETY: `base` addresses the attached segment's mapped bytes.
    let toc = match unsafe { ShmToc::attach(SESSION_MAGIC, base_nn) } {
        Some(toc) => toc,
        None => {
            return Err(ereport(ERROR)
                .errmsg_internal("bad magic number in per-session DSM segment")
                .into_error())
        }
    };

    // Attach to the DSA area.
    let dsa_space = toc
        .lookup(SESSION_KEY_DSA, false)?
        .expect("SESSION_KEY_DSA present");
    let dsa_cursor = SerializeCursor(dsa_space.as_ptr() as usize);
    let seg_handle = DsmSegmentHandle(seg_id.as_u64() as usize);
    let area_handle = dsa::dsa_attach_in_place::call(dsa_cursor, seg_handle);
    let area: *mut DsaArea = area_handle.0 as *mut DsaArea;

    CURRENT_SESSION.with(|s| {
        let mut slot = s.borrow_mut();
        let sess = slot.as_mut().expect("CurrentSession");
        sess.segment = Some(seg_id);
        sess.area = area;
    });

    // Attach to the shared record typmod registry.
    let registry_space = toc
        .lookup(SESSION_KEY_RECORD_TYPMOD_REGISTRY, false)?
        .expect("SESSION_KEY_RECORD_TYPMOD_REGISTRY present");
    let registry = registry_space.as_ptr() as *mut SharedRecordTypmodRegistry;
    attach_shared_record_typmod_registry(registry, area)?;

    // Remain attached until end of backend or DetachSession().
    dsa::dsa_pin_mapping::call(area)?;
    let _ = dsm::dsm_pin_mapping(seg);

    Ok(())
}

/// `SharedRecordTypmodRegistryAttach(registry)` (typcache.c:2300) — worker-side:
/// attach to the registry's two dshash tables (this backend's local cache must
/// be empty, asserted in the typcache owner).
fn attach_shared_record_typmod_registry(
    registry: *mut SharedRecordTypmodRegistry,
    area: *mut DsaArea,
) -> PgResult<()> {
    // SAFETY: `registry` addresses the shared registry header in the segment.
    let (record_handle, typmod_handle) =
        unsafe { ((*registry).record_table_handle, (*registry).typmod_table_handle) };

    let record_table =
        dshash::dshash_attach::call(area, srtr_record_table_params(), record_handle)?;
    let typmod_table =
        dshash::dshash_attach::call(area, srtr_typmod_table_params(), typmod_handle)?;

    // Let the typcache owner verify NextRecordTypmod == 0 (its precondition).
    backend_utils_cache_typcache_seams::shared_registry_attach_check::call()?;

    CURRENT_SESSION.with(|s| {
        let mut slot = s.borrow_mut();
        let sess = slot.as_mut().expect("CurrentSession");
        sess.shared_typmod_registry = registry;
        sess.shared_record_table = record_table;
        sess.shared_typmod_table = typmod_table;
    });

    Ok(())
}

/// `SharedRecordTypmodRegistryEstimate(void)` (typcache.c:2174).
///
/// `return sizeof(SharedRecordTypmodRegistry);`
///
/// Exists only to avoid exposing the private innards of
/// `SharedRecordTypmodRegistry` in a header; the result sizes the shmem chunk
/// reserved for the registry header in `GetSessionDsmHandle`.
fn shared_registry_estimate() -> usize {
    core::mem::size_of::<SharedRecordTypmodRegistry>()
}

/// Whether a `SharedRecordTypmodRegistry` is attached to the current session
/// (`CurrentSession->shared_typmod_registry != NULL`). Pure read of the
/// `Session` field. False whenever no parallel registry has been attached —
/// always the case in a single (non-parallel) backend.
fn shared_registry_attached() -> bool {
    CURRENT_SESSION.with(|s| {
        s.borrow()
            .as_ref()
            .is_some_and(|sess| !sess.shared_typmod_registry.is_null())
    })
}

/// `find_or_make_matching_shared_tupledesc(tupdesc)` (typcache.c:2943).
///
/// The shared path of `assign_record_type_typmod`. The C body returns NULL
/// immediately when `CurrentSession->shared_typmod_registry == NULL` (the only
/// case in a single backend), which maps to `None` here, telling the caller to
/// use the local `RecordCacheArray`/`RecordCacheHash`.
///
/// The attached path (dshash record/typmod tables over the session DSA area) is
/// keystone-blocked on dshash custom-callback support (see module docs); it
/// keeps a loud panic rather than a silent stub. It is unreachable in a single
/// backend.
fn find_or_make_matching_shared_tupledesc<'mcx>(
    mcx: Mcx<'mcx>,
    tupdesc: &TupleDescData<'_>,
) -> PgResult<Option<PgBox<'mcx, TupleDescData<'mcx>>>> {
    // If not even attached, nothing to do (the only case for non-record-type
    // queries — including the parallel count(*) common case).
    if !shared_registry_attached() {
        return Ok(None);
    }

    // Attached (parallel) path: the record/typmod dshash tables are owned by
    // the session but the share_tupledesc / find logic is typcache's; delegate.
    let (record_table, typmod_table, area, registry) = CURRENT_SESSION.with(|s| {
        let b = s.borrow();
        let sess = b.as_ref().expect("CurrentSession");
        (
            sess.shared_record_table as usize,
            sess.shared_typmod_table as usize,
            sess.area as usize,
            sess.shared_typmod_registry as usize,
        )
    });
    backend_utils_cache_typcache_seams::find_or_make_matching_shared_tupledesc::call(
        mcx,
        tupdesc,
        record_table,
        typmod_table,
        area,
        registry_next_typmod_ptr(registry as *mut SharedRecordTypmodRegistry) as usize,
    )
}

/// `shared_typmod_table_find(typmod)` — the shared path of
/// `lookup_rowtype_tupdesc_internal`. Returns a copy of the shared descriptor
/// for `typmod` in `mcx`, or `None` when not attached / miss. Delegates the
/// attached-table read to the typcache owner.
fn shared_typmod_table_find<'mcx>(
    mcx: Mcx<'mcx>,
    typmod: i32,
) -> PgResult<Option<PgBox<'mcx, TupleDescData<'mcx>>>> {
    if !shared_registry_attached() {
        return Ok(None);
    }
    let (typmod_table, area) = CURRENT_SESSION.with(|s| {
        let b = s.borrow();
        let sess = b.as_ref().expect("CurrentSession");
        (sess.shared_typmod_table as usize, sess.area as usize)
    });
    backend_utils_cache_typcache_seams::shared_typmod_table_find::call(
        mcx,
        typmod,
        typmod_table,
        area,
    )
}

/// `DetachSession(void)` (session.c:200) — detach this backend from the session
/// DSM segment (runs detach hooks) and DSA area.
fn detach_session() -> PgResult<()> {
    let (seg, area) = CURRENT_SESSION.with(|s| {
        let b = s.borrow();
        let sess = b.as_ref().expect("CurrentSession");
        (sess.segment, sess.area)
    });
    if let Some(seg_id) = seg {
        // dsm_detach(CurrentSession->segment) — runs detach hooks.
        dsm::dsm_detach(seg_id)?;
    }
    if !area.is_null() {
        dsa::dsa_detach::call(types_execparallel::DsaAreaHandle(area as usize));
    }
    CURRENT_SESSION.with(|s| {
        let mut b = s.borrow_mut();
        let sess = b.as_mut().expect("CurrentSession");
        sess.segment = None;
        sess.area = core::ptr::null_mut();
    });
    Ok(())
}

/// Install the session seams this crate owns.
pub fn init_seams() {
    backend_access_common_session_seams::initialize_session::set(initialize_session);
    backend_access_common_session_seams::get_session_dsm_handle::set(get_session_dsm_handle);
    backend_access_common_session_seams::shared_registry_estimate::set(shared_registry_estimate);
    backend_access_common_session_seams::shared_registry_attached::set(shared_registry_attached);
    backend_access_common_session_seams::find_or_make_matching_shared_tupledesc::set(
        find_or_make_matching_shared_tupledesc,
    );
    backend_access_common_session_seams::shared_typmod_table_find::set(shared_typmod_table_find);
    // AttachSession / DetachSession (worker-side session DSM attach), owned by
    // session.c; installed into the parallel runtime seam surface.
    backend_access_transam_parallel_rt_seams::attach_session::set(attach_session);
    backend_access_transam_parallel_rt_seams::detach_session::set(detach_session);
}
