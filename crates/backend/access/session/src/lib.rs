#![allow(non_snake_case)]

use core::cell::Cell;
use std::marker::PhantomData;

use types_core::{InvalidOid, Oid};
use types_error::{
    PgError, PgResult, ERRCODE_FEATURE_NOT_SUPPORTED, ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE,
    ERROR,
};

thread_local! {
    static CURRENT_SESSION: Cell<bool> = const { Cell::new(false) };
    static ENVELOPE_DEPTH: Cell<u32> = const { Cell::new(0) };
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum EnvelopeMemberId {
    DatabaseIdentity,
    DatabasePaths,
    ProcessIdentity,
    SessionLifecycle,
    UserIdentity,
    TempNamespace,
    SearchPath,
    SnapshotState,
    TransactionState,
    GucStore,
    GucFlatBackings,
    GucNesting,
    ResourceOwnerCells,
    ResourceOwnerArena,
    ErrorStack,
    ErrorCallbacks,
    InterruptPending,
    InterruptHoldoffs,
    Catcache,
    Relcache,
    Typcache,
    Plancache,
    InvalidationCallbacks,
    InvalidationMessages,
    PendingInvalidations,
    SyscacheArrays,
    Relmapper,
    Partcache,
    TsCache,
    EventCache,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EnvelopeBindKind {
    SwapRoot,
    ScalarRestore,
    DrainSameDatabase,
    MustBeEmpty,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Phase0Action {
    CaptureApply,
    RestoreScalar,
    RequireSameDatabase,
    Drain,
    CheckEmpty,
    Refuse,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct EnvelopeMember {
    pub id: EnvelopeMemberId,
    pub name: &'static str,
    pub declaration: &'static str,
    pub kind: EnvelopeBindKind,
    pub phase0: Phase0Action,
    pub blocker: Option<&'static str>,
}

pub const SESSION_ENVELOPE_MANIFEST: &[EnvelopeMember] = &[
    EnvelopeMember { id: EnvelopeMemberId::DatabaseIdentity, name: "database identity", declaration: "init_small/globals.rs: MY_DATABASE_ID, MY_DATABASE_TABLE_SPACE", kind: EnvelopeBindKind::ScalarRestore, phase0: Phase0Action::RequireSameDatabase, blocker: Some("cross-database cache roots have no portable key or cold-switch protocol") },
    EnvelopeMember { id: EnvelopeMemberId::DatabasePaths, name: "database and data paths", declaration: "init_small/globals.rs: DATA_DIR, DATABASE_PATH", kind: EnvelopeBindKind::ScalarRestore, phase0: Phase0Action::RequireSameDatabase, blocker: Some("cross-database path switching belongs with the cache cold-switch protocol") },
    EnvelopeMember { id: EnvelopeMemberId::ProcessIdentity, name: "MyProc/MyProcNumber", declaration: "init_small/globals.rs: MY_PROC_NUMBER; lmgr_proc/lib.rs: MY_PROC", kind: EnvelopeBindKind::ScalarRestore, phase0: Phase0Action::Refuse, blocker: Some("worker PGPROC ownership cannot alias the target backend") },
    EnvelopeMember { id: EnvelopeMemberId::SessionLifecycle, name: "session lifecycle marker", declaration: "session/lib.rs: CURRENT_SESSION", kind: EnvelopeBindKind::ScalarRestore, phase0: Phase0Action::RestoreScalar, blocker: None },
    EnvelopeMember { id: EnvelopeMemberId::UserIdentity, name: "role, RLS, authenticated and system user", declaration: "miscinit/userid.rs: AUTHENTICATED_USER_ID..SET_ROLE_IS_ACTIVE", kind: EnvelopeBindKind::ScalarRestore, phase0: Phase0Action::RestoreScalar, blocker: None },
    EnvelopeMember { id: EnvelopeMemberId::TempNamespace, name: "temp namespace state", declaration: "catalog_namespace/lib.rs: MY_TEMP_NAMESPACE..BASE_TEMP_CREATION_PENDING", kind: EnvelopeBindKind::ScalarRestore, phase0: Phase0Action::RestoreScalar, blocker: Some("foreign temp-relation execution remains refused") },
    EnvelopeMember { id: EnvelopeMemberId::SearchPath, name: "search_path and path cache", declaration: "catalog_namespace/lib.rs,path.rs: PATH, SPCACHE and validity scalars", kind: EnvelopeBindKind::SwapRoot, phase0: Phase0Action::CaptureApply, blocker: Some("derived path caches are invalidated and lazily rebuilt in Phase 0") },
    EnvelopeMember { id: EnvelopeMemberId::SnapshotState, name: "snapmgr root", declaration: "snapmgr/lib.rs: STATE and debug-only STATE_BUSY/STATIC_REPLACED", kind: EnvelopeBindKind::SwapRoot, phase0: Phase0Action::Refuse, blocker: Some("query-owned snapshot state belongs to Envelope Phase 5") },
    EnvelopeMember { id: EnvelopeMemberId::TransactionState, name: "xact root and mirrors", declaration: "xact/state.rs,engine.rs: STATE and CUR_* mirrors", kind: EnvelopeBindKind::SwapRoot, phase0: Phase0Action::Refuse, blocker: Some("query-owned xact state belongs to Envelope Phase 5") },
    EnvelopeMember { id: EnvelopeMemberId::GucStore, name: "GUC registry root", declaration: "guc/store.rs: GUC_STORE", kind: EnvelopeBindKind::SwapRoot, phase0: Phase0Action::CaptureApply, blocker: None },
    EnvelopeMember { id: EnvelopeMemberId::GucFlatBackings, name: "GUC flat backings and store hints", declaration: "guc_tables/session.rs; guc/store.rs: PG_RELOAD_TIME, hints", kind: EnvelopeBindKind::ScalarRestore, phase0: Phase0Action::RestoreScalar, blocker: None },
    EnvelopeMember { id: EnvelopeMemberId::GucNesting, name: "GUC nesting level", declaration: "guc/lib.rs: GUC_NEST_LEVEL", kind: EnvelopeBindKind::MustBeEmpty, phase0: Phase0Action::CheckEmpty, blocker: Some("SET LOCAL and nested GUC state require the transaction root") },
    EnvelopeMember { id: EnvelopeMemberId::ResourceOwnerCells, name: "resource-owner cells", declaration: "resowner/lib.rs: CURRENT_OWNER..AUX_PROCESS_OWNER", kind: EnvelopeBindKind::MustBeEmpty, phase0: Phase0Action::CheckEmpty, blocker: None },
    EnvelopeMember { id: EnvelopeMemberId::ResourceOwnerArena, name: "resource-owner arena", declaration: "resowner/lib.rs: ARENA", kind: EnvelopeBindKind::MustBeEmpty, phase0: Phase0Action::CheckEmpty, blocker: Some("the per-thread arena is shared-sequential, never transferred") },
    EnvelopeMember { id: EnvelopeMemberId::ErrorStack, name: "error stack", declaration: "elog/stack.rs: STACK", kind: EnvelopeBindKind::MustBeEmpty, phase0: Phase0Action::CheckEmpty, blocker: None },
    EnvelopeMember { id: EnvelopeMemberId::ErrorCallbacks, name: "error context callbacks", declaration: "elog/stack.rs: EMIT_CONTEXT_CALLBACKS", kind: EnvelopeBindKind::MustBeEmpty, phase0: Phase0Action::CheckEmpty, blocker: None },
    EnvelopeMember { id: EnvelopeMemberId::InterruptPending, name: "interrupt and cancellation pending flags", declaration: "init_small/globals.rs: INTERRUPT_PENDING, QUERY_CANCEL_PENDING, PROC_DIE_PENDING", kind: EnvelopeBindKind::MustBeEmpty, phase0: Phase0Action::CheckEmpty, blocker: None },
    EnvelopeMember { id: EnvelopeMemberId::InterruptHoldoffs, name: "interrupt and critical-section holdoffs", declaration: "init_small/globals.rs: INTERRUPT_HOLDOFF_COUNT, QUERY_CANCEL_HOLDOFF_COUNT, CRIT_SECTION_COUNT", kind: EnvelopeBindKind::MustBeEmpty, phase0: Phase0Action::CheckEmpty, blocker: None },
    EnvelopeMember { id: EnvelopeMemberId::Catcache, name: "catcache", declaration: "catcache/lib.rs,graph.rs: STATE, INVAL_EPOCH", kind: EnvelopeBindKind::DrainSameDatabase, phase0: Phase0Action::Drain, blocker: None },
    EnvelopeMember { id: EnvelopeMemberId::Relcache, name: "relcache", declaration: "relcache/lib.rs: STATE", kind: EnvelopeBindKind::DrainSameDatabase, phase0: Phase0Action::Drain, blocker: None },
    EnvelopeMember { id: EnvelopeMemberId::Typcache, name: "typcache", declaration: "typcache/lib.rs: STATE", kind: EnvelopeBindKind::DrainSameDatabase, phase0: Phase0Action::Drain, blocker: None },
    EnvelopeMember { id: EnvelopeMemberId::Plancache, name: "plancache", declaration: "plancache/lib.rs: STATE and invalidation counters", kind: EnvelopeBindKind::DrainSameDatabase, phase0: Phase0Action::Drain, blocker: None },
    EnvelopeMember { id: EnvelopeMemberId::InvalidationCallbacks, name: "invalidation callbacks", declaration: "inval/lib.rs: CALLBACKS, SYSCACHE_LINKS", kind: EnvelopeBindKind::DrainSameDatabase, phase0: Phase0Action::Drain, blocker: None },
    EnvelopeMember { id: EnvelopeMemberId::InvalidationMessages, name: "invalidation message state", declaration: "inval/lib.rs: STATE, DEBUG_DISCARD_CACHES, ACCEPT_RECURSION_DEPTH", kind: EnvelopeBindKind::DrainSameDatabase, phase0: Phase0Action::Drain, blocker: None },
    EnvelopeMember { id: EnvelopeMemberId::PendingInvalidations, name: "uncommitted invalidations", declaration: "inval/lib.rs: STATE trans_stack and inplace_info", kind: EnvelopeBindKind::MustBeEmpty, phase0: Phase0Action::CheckEmpty, blocker: Some("uncommitted invalidations are transaction-owned and cannot be drained") },
    EnvelopeMember { id: EnvelopeMemberId::SyscacheArrays, name: "syscache oid arrays", declaration: "cache_syscache/lib.rs: caches", kind: EnvelopeBindKind::DrainSameDatabase, phase0: Phase0Action::Drain, blocker: None },
    EnvelopeMember { id: EnvelopeMemberId::Relmapper, name: "relation mapper", declaration: "relmapper/lib.rs: STATE", kind: EnvelopeBindKind::DrainSameDatabase, phase0: Phase0Action::Drain, blocker: None },
    EnvelopeMember { id: EnvelopeMemberId::Partcache, name: "partition cache", declaration: "partcache/lib.rs: STATE", kind: EnvelopeBindKind::DrainSameDatabase, phase0: Phase0Action::Drain, blocker: None },
    EnvelopeMember { id: EnvelopeMemberId::TsCache, name: "text-search cache", declaration: "ts_cache/lib.rs: STATE", kind: EnvelopeBindKind::DrainSameDatabase, phase0: Phase0Action::Drain, blocker: None },
    EnvelopeMember { id: EnvelopeMemberId::EventCache, name: "event-trigger cache", declaration: "cache_evtcache/lib.rs: STATE", kind: EnvelopeBindKind::DrainSameDatabase, phase0: Phase0Action::Drain, blocker: None },
];

pub struct SessionContext {
    database_id: Oid,
    database_tablespace: Oid,
    data_dir: Option<&'static str>,
    database_path: Option<&'static str>,
    session_exists: bool,
    identity: miscinit::SessionIdentityState,
    namespace: catalog_namespace::SessionNamespaceState,
    gucs: guc::store::ExactGucState,
    guc_nest_level: i32,
    xact_nest_level: i32,
    transaction_active: bool,
    snapshot_clean: bool,
    pending_invalidations: bool,
}

const _: fn() = || {
    fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<SessionContext>();
};

impl SessionContext {
    pub fn capture() -> Self {
        Self {
            database_id: init_small::globals::MyDatabaseId(),
            database_tablespace: init_small::globals::MyDatabaseTableSpace(),
            data_dir: init_small::globals::DataDir(),
            database_path: init_small::globals::DatabasePath(),
            session_exists: CurrentSessionExists(),
            identity: miscinit::CaptureSessionIdentityState(),
            namespace: catalog_namespace::CaptureSessionNamespaceState(),
            gucs: guc::store::capture_exact_guc_state(),
            guc_nest_level: guc::guc_nest_level(),
            xact_nest_level: xact::GetCurrentTransactionNestLevel(),
            transaction_active: xact::IsTransactionOrTransactionBlock(),
            snapshot_clean: snapmgr::SnapshotStateClean(),
            pending_invalidations: inval::TransactionHasPendingInvalidationMessages(),
        }
    }

    pub fn database_id(&self) -> Oid {
        self.database_id
    }
}

struct SavedEnvelope {
    session_exists: bool,
    identity: miscinit::SessionIdentityState,
    namespace: catalog_namespace::SessionNamespaceState,
    gucs: guc::store::ExactGucState,
}

pub struct SessionEnvelopeBinding {
    saved: Option<SavedEnvelope>,
    depth: u32,
    _not_send: PhantomData<*const ()>,
}

impl SessionEnvelopeBinding {
    pub fn finish(mut self) -> PgResult<()> {
        let issue = bound_state_issue();
        self.restore();
        match issue {
            Some(message) => Err(prerequisite_error(message)),
            None => Ok(()),
        }
    }

    fn restore(&mut self) {
        let Some(saved) = self.saved.take() else {
            return;
        };
        ENVELOPE_DEPTH.with(|depth| {
            assert_eq!(
                depth.get(),
                self.depth,
                "SessionEnvelopeBinding dropped out of nesting order"
            );
        });
        install_saved(saved);
        ENVELOPE_DEPTH.with(|depth| depth.set(self.depth - 1));
    }
}

impl Drop for SessionEnvelopeBinding {
    fn drop(&mut self) {
        self.restore();
    }
}

pub fn bind_session_envelope(target: &SessionContext) -> PgResult<SessionEnvelopeBinding> {
    bind_session_envelope_with(target, inval::local::AcceptInvalidationMessages)
}

pub fn with_session_envelope<T>(
    target: &SessionContext,
    body: impl FnOnce() -> PgResult<T>,
) -> PgResult<T> {
    let binding = bind_session_envelope(target)?;
    let result = body();
    binding.finish()?;
    result
}

fn bind_session_envelope_with(
    target: &SessionContext,
    drain_same_database: impl FnOnce() -> PgResult<()>,
) -> PgResult<SessionEnvelopeBinding> {
    validate_target(target)?;
    validate_entry_boundary()?;
    drain_same_database()?;

    let saved = SavedEnvelope {
        session_exists: CurrentSessionExists(),
        identity: miscinit::CaptureSessionIdentityState(),
        namespace: catalog_namespace::CaptureSessionNamespaceState(),
        gucs: guc::store::capture_exact_guc_state(),
    };
    let depth = ENVELOPE_DEPTH.with(|cell| {
        let next = cell
            .get()
            .checked_add(1)
            .expect("session envelope nesting overflow");
        cell.set(next);
        next
    });
    let binding = SessionEnvelopeBinding {
        saved: Some(saved),
        depth,
        _not_send: PhantomData,
    };

    guc::store::replace_exact_guc_state_for_envelope(&target.gucs);
    catalog_namespace::ReplaceSessionNamespaceState(&target.namespace);
    miscinit::ReplaceSessionIdentityState(target.identity);
    CURRENT_SESSION.set(target.session_exists);

    Ok(binding)
}

fn install_saved(saved: SavedEnvelope) {
    guc::store::replace_exact_guc_state(&saved.gucs);
    catalog_namespace::ReplaceSessionNamespaceState(&saved.namespace);
    miscinit::ReplaceSessionIdentityState(saved.identity);
    CURRENT_SESSION.set(saved.session_exists);
}

fn validate_target(target: &SessionContext) -> PgResult<()> {
    let current_db = init_small::globals::MyDatabaseId();
    if !target.session_exists {
        return Err(unsupported(
            "SessionEnvelope Phase 0 requires an initialized target session",
        ));
    }
    if current_db == InvalidOid || target.database_id == InvalidOid {
        return Err(unsupported(
            "SessionEnvelope Phase 0 requires an attached database",
        ));
    }
    if current_db != target.database_id {
        return Err(unsupported(
            "SessionEnvelope Phase 0 refuses cross-database binding",
        ));
    }
    if init_small::globals::MyDatabaseTableSpace() != target.database_tablespace {
        return Err(prerequisite_error(
            "same database has mismatched tablespace identity",
        ));
    }
    if init_small::globals::DataDir() != target.data_dir
        || init_small::globals::DatabasePath() != target.database_path
    {
        return Err(prerequisite_error(
            "same database has mismatched database path identity",
        ));
    }
    if target.xact_nest_level != 0 || target.transaction_active || !target.snapshot_clean {
        return Err(unsupported(
            "SessionEnvelope Phase 0 has no query-owned transaction/snapshot root",
        ));
    }
    if target.guc_nest_level != 0 {
        return Err(unsupported(
            "SessionEnvelope Phase 0 refuses SET LOCAL/nested GUC state until GUC root swap",
        ));
    }
    if target.pending_invalidations {
        return Err(unsupported(
            "SessionEnvelope Phase 0 cannot expose target-uncommitted invalidations",
        ));
    }
    Ok(())
}

pub fn SessionEnvelopeBoundaryClean() -> bool {
    SessionEnvelopeBoundaryIssue().is_none()
}

pub fn SessionEnvelopeBoundaryIssue() -> Option<&'static str> {
    if ENVELOPE_DEPTH.get() != 0 {
        return Some("session envelope binding is still live");
    }
    if guc::store::session_bound() {
        return Some("legacy SessionGucBinding is still live");
    }
    if init_small::globals::InterruptPending()
        || init_small::globals::QueryCancelPending()
        || init_small::globals::ProcDiePending()
    {
        return Some("interrupt or cancellation is pending");
    }
    bound_state_issue()
}

/// Ceremony-v2 sticky task binding (parallel::query_task_guard): a standing
/// runtime executor parked BETWEEN engagements of the SAME session
/// deliberately retains its `SessionGucBinding` (the applied query pin), so
/// the live-binding condition above is not a leak there — it is the keyed
/// retention itself. Every OTHER boundary condition must still hold at
/// sticky park and sticky resume: envelope depth, pending interrupts, and
/// the full bound-state sweep (transaction/snapshot/GUC-nesting/error-stack
/// cleanliness). A dirty boundary demotes the park to the full unbind and
/// refuses the resume. The retention is keyed (leader identity + GUC pin
/// Arc) and evicted by the binder before any FOREIGN session's engagement
/// can bind — SESSION_BOUND accounting stays exact: exactly one live
/// binding, always the keyed session's.
pub fn SessionEnvelopeBoundaryIssueForRetainedBind() -> Option<&'static str> {
    if ENVELOPE_DEPTH.get() != 0 {
        return Some("session envelope binding is still live");
    }
    if init_small::globals::InterruptPending()
        || init_small::globals::QueryCancelPending()
        || init_small::globals::ProcDiePending()
    {
        return Some("interrupt or cancellation is pending");
    }
    bound_state_issue()
}

fn validate_entry_boundary() -> PgResult<()> {
    if guc::store::session_bound() {
        return Err(prerequisite_error("legacy SessionGucBinding is still live"));
    }
    if init_small::globals::InterruptPending()
        || init_small::globals::QueryCancelPending()
        || init_small::globals::ProcDiePending()
    {
        return Err(prerequisite_error("interrupt or cancellation is pending"));
    }
    if let Some(issue) = bound_state_issue() {
        return Err(prerequisite_error(issue));
    }
    Ok(())
}

fn bound_state_issue() -> Option<&'static str> {
    if !elog::error_stack_clean() {
        return Some("error or callback stack is not empty");
    }
    if let Some(issue) = xact::ResourceOwnerBoundaryIssue() {
        return Some(issue);
    }
    if !snapmgr::SnapshotStateClean() {
        return Some("snapshot state is not empty");
    }
    if xact::GetCurrentTransactionNestLevel() != 0 || xact::IsTransactionOrTransactionBlock() {
        return Some("transaction state is not empty");
    }
    if guc::guc_nest_level() != 0 {
        return Some("GUC nesting state is not empty");
    }
    if inval::TransactionHasPendingInvalidationMessages() {
        return Some("uncommitted invalidation state is not empty");
    }
    if init_small::globals::InterruptHoldoffCount() != 0
        || init_small::globals::QueryCancelHoldoffCount() != 0
        || init_small::globals::CritSectionCount() != 0
    {
        return Some("interrupt, cancellation, or critical-section holdoff is live");
    }
    None
}

fn unsupported(message: &'static str) -> Box<PgError> {
    PgError::new(ERROR, message)
        .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED)
        .into()
}

fn prerequisite_error(message: &'static str) -> Box<PgError> {
    PgError::new(ERROR, message)
        .with_sqlstate(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
        .into()
}

pub fn InitializeSession() -> PgResult<()> {
    CURRENT_SESSION.set(true);
    Ok(())
}

pub fn CurrentSessionExists() -> bool {
    CURRENT_SESSION.get()
}

pub fn GetSessionDsmHandle() -> ! {
    panic!("GetSessionDsmHandle: parallel-worker session DSM unported (backend-access-common session.c)");
}

pub fn init_seams() {
    session_seams::initialize_session::set(InitializeSession);
}

#[cfg(test)]
mod tests;
