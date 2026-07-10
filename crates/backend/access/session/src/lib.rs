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
    ProcessIdentity,
    UserIdentity,
    TempNamespace,
    SearchPath,
    SnapshotState,
    TransactionState,
    GucStore,
    ResourceOwners,
    ErrorStack,
    InterruptHoldoffs,
    Catcache,
    Relcache,
    Typcache,
    Plancache,
    InvalidationState,
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
    pub kind: EnvelopeBindKind,
    pub phase0: Phase0Action,
    pub blocker: Option<&'static str>,
}

pub const SESSION_ENVELOPE_MANIFEST: &[EnvelopeMember] = &[
    EnvelopeMember { id: EnvelopeMemberId::DatabaseIdentity, name: "database identity", kind: EnvelopeBindKind::ScalarRestore, phase0: Phase0Action::RequireSameDatabase, blocker: Some("cross-database cache roots have no portable key or cold-switch protocol") },
    EnvelopeMember { id: EnvelopeMemberId::ProcessIdentity, name: "MyProc/MyProcNumber", kind: EnvelopeBindKind::ScalarRestore, phase0: Phase0Action::Refuse, blocker: Some("worker PGPROC ownership cannot alias the target backend") },
    EnvelopeMember { id: EnvelopeMemberId::UserIdentity, name: "role, RLS, authenticated and system user", kind: EnvelopeBindKind::ScalarRestore, phase0: Phase0Action::RestoreScalar, blocker: None },
    EnvelopeMember { id: EnvelopeMemberId::TempNamespace, name: "temp namespace ids", kind: EnvelopeBindKind::ScalarRestore, phase0: Phase0Action::RestoreScalar, blocker: Some("foreign temp-relation execution remains refused") },
    EnvelopeMember { id: EnvelopeMemberId::SearchPath, name: "search_path and path cache", kind: EnvelopeBindKind::SwapRoot, phase0: Phase0Action::CaptureApply, blocker: Some("GUC assign hooks invalidate the worker-local path cache; root swap remains pending") },
    EnvelopeMember { id: EnvelopeMemberId::SnapshotState, name: "snapmgr root", kind: EnvelopeBindKind::SwapRoot, phase0: Phase0Action::Refuse, blocker: Some("query-owned snapshot state belongs to Envelope Phase 5") },
    EnvelopeMember { id: EnvelopeMemberId::TransactionState, name: "xact root", kind: EnvelopeBindKind::SwapRoot, phase0: Phase0Action::Refuse, blocker: Some("query-owned xact state belongs to Envelope Phase 5") },
    EnvelopeMember { id: EnvelopeMemberId::GucStore, name: "GUC store", kind: EnvelopeBindKind::SwapRoot, phase0: Phase0Action::CaptureApply, blocker: Some("Phase 0 uses validated capture/apply; O(1) root swap is blocked by flat assign-hook backings") },
    EnvelopeMember { id: EnvelopeMemberId::ResourceOwners, name: "resource-owner cells and arena", kind: EnvelopeBindKind::MustBeEmpty, phase0: Phase0Action::CheckEmpty, blocker: Some("the per-thread arena is shared-sequential, never transferred") },
    EnvelopeMember { id: EnvelopeMemberId::ErrorStack, name: "error and callback stacks", kind: EnvelopeBindKind::MustBeEmpty, phase0: Phase0Action::CheckEmpty, blocker: None },
    EnvelopeMember { id: EnvelopeMemberId::InterruptHoldoffs, name: "interrupt and critical-section holdoffs", kind: EnvelopeBindKind::MustBeEmpty, phase0: Phase0Action::CheckEmpty, blocker: None },
    EnvelopeMember { id: EnvelopeMemberId::Catcache, name: "catcache", kind: EnvelopeBindKind::DrainSameDatabase, phase0: Phase0Action::Drain, blocker: None },
    EnvelopeMember { id: EnvelopeMemberId::Relcache, name: "relcache", kind: EnvelopeBindKind::DrainSameDatabase, phase0: Phase0Action::Drain, blocker: None },
    EnvelopeMember { id: EnvelopeMemberId::Typcache, name: "typcache", kind: EnvelopeBindKind::DrainSameDatabase, phase0: Phase0Action::Drain, blocker: None },
    EnvelopeMember { id: EnvelopeMemberId::Plancache, name: "plancache", kind: EnvelopeBindKind::DrainSameDatabase, phase0: Phase0Action::Drain, blocker: None },
    EnvelopeMember { id: EnvelopeMemberId::InvalidationState, name: "invalidation callbacks and message state", kind: EnvelopeBindKind::DrainSameDatabase, phase0: Phase0Action::Drain, blocker: None },
    EnvelopeMember { id: EnvelopeMemberId::SyscacheArrays, name: "syscache oid arrays", kind: EnvelopeBindKind::DrainSameDatabase, phase0: Phase0Action::Drain, blocker: None },
    EnvelopeMember { id: EnvelopeMemberId::Relmapper, name: "relation mapper", kind: EnvelopeBindKind::DrainSameDatabase, phase0: Phase0Action::Drain, blocker: None },
    EnvelopeMember { id: EnvelopeMemberId::Partcache, name: "partition cache", kind: EnvelopeBindKind::DrainSameDatabase, phase0: Phase0Action::Drain, blocker: None },
    EnvelopeMember { id: EnvelopeMemberId::TsCache, name: "text-search cache", kind: EnvelopeBindKind::DrainSameDatabase, phase0: Phase0Action::Drain, blocker: None },
    EnvelopeMember { id: EnvelopeMemberId::EventCache, name: "event-trigger cache", kind: EnvelopeBindKind::DrainSameDatabase, phase0: Phase0Action::Drain, blocker: None },
];

pub struct SessionContext {
    database_id: Oid,
    database_tablespace: Oid,
    identity: miscinit::SessionIdentityState,
    temp_namespace: (Oid, Oid),
    gucs: Vec<guc::store::CapturedGuc>,
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
            identity: miscinit::CaptureSessionIdentityState(),
            temp_namespace: catalog_namespace::GetTempNamespaceState(),
            gucs: guc::store::capture_session_gucs(),
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
    identity: miscinit::SessionIdentityState,
    temp_namespace: (Oid, Oid),
    gucs: Vec<guc::store::CapturedGuc>,
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
        identity: miscinit::CaptureSessionIdentityState(),
        temp_namespace: catalog_namespace::GetTempNamespaceState(),
        gucs: guc::store::capture_session_gucs(),
    };
    let depth = ENVELOPE_DEPTH.with(|cell| {
        let next = cell
            .get()
            .checked_add(1)
            .expect("session envelope nesting overflow");
        cell.set(next);
        next
    });

    miscinit::ReplaceSessionIdentityState(target.identity);
    catalog_namespace::ReplaceTempNamespaceState(target.temp_namespace.0, target.temp_namespace.1);
    guc::ResetAllOptions();
    if let Err(error) = guc::store::apply_captured_session_gucs(&target.gucs) {
        install_saved(saved);
        ENVELOPE_DEPTH.with(|cell| cell.set(depth - 1));
        return Err(error);
    }
    miscinit::ReplaceSessionIdentityState(target.identity);

    Ok(SessionEnvelopeBinding {
        saved: Some(saved),
        depth,
        _not_send: PhantomData,
    })
}

fn install_saved(saved: SavedEnvelope) {
    miscinit::ReplaceSessionIdentityState(saved.identity);
    catalog_namespace::ReplaceTempNamespaceState(saved.temp_namespace.0, saved.temp_namespace.1);
    guc::ResetAllOptions();
    guc::store::apply_captured_session_gucs(&saved.gucs)
        .expect("SessionEnvelopeBinding: validated GUC restore failed");
    miscinit::ReplaceSessionIdentityState(saved.identity);
}

fn validate_target(target: &SessionContext) -> PgResult<()> {
    let current_db = init_small::globals::MyDatabaseId();
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
    !guc::store::session_bound() && bound_state_issue().is_none()
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
    if !resowner::ResourceOwnerStateClean() {
        return Some("resource-owner state is not empty");
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
