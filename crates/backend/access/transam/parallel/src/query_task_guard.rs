use std::cell::RefCell;
use std::panic::{catch_unwind, resume_unwind, AssertUnwindSafe};
use std::sync::Arc;

use types_core::{InvalidOid, Oid, SavedTransactionCharacteristics, TimestampTz};
use types_error::{
    PgError, PgResult, ERRCODE_FEATURE_NOT_SUPPORTED, ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE,
    ERROR, WARNING,
};

use super::ParallelShared;

#[cfg(debug_assertions)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum QueryTaskFaultPoint {
    BindIdentity,
    BindTransaction,
    BindRelationMap,
    BindTransactionSnapshot,
    BindActiveSnapshot,
    BindInvalidations,
    BindGucs,
    BindClient,
    BindParallelMode,
    FinishParallelMode,
    FinishSnapshot,
    FinishTransaction,
    FinishSessionState,
    FinishBoundary,
}

#[cfg(debug_assertions)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum QueryTaskFaultAction {
    Error,
    Panic,
}

#[cfg(debug_assertions)]
thread_local! {
    static QUERY_TASK_FAULT: std::cell::Cell<Option<(QueryTaskFaultPoint, QueryTaskFaultAction)>> =
        const { std::cell::Cell::new(None) };
}

#[cfg(debug_assertions)]
pub fn set_query_task_fault(point: QueryTaskFaultPoint, action: QueryTaskFaultAction) {
    QUERY_TASK_FAULT.with(|fault| fault.set(Some((point, action))));
}

#[cfg(debug_assertions)]
fn inject(point: QueryTaskFaultPoint) -> PgResult<()> {
    let action = QUERY_TASK_FAULT.with(|fault| {
        let configured = fault.get();
        if configured.map(|v| v.0) == Some(point) {
            fault.set(None);
            configured.map(|v| v.1)
        } else {
            None
        }
    });
    match action {
        Some(QueryTaskFaultAction::Error) => {
            Err(PgError::new(ERROR, format!("query-task injected fault at {point:?}")).into())
        }
        Some(QueryTaskFaultAction::Panic) => panic!("query-task injected panic at {point:?}"),
        None => Ok(()),
    }
}

/// Claim-side invalidation for a parked helper adopting a leader transaction:
/// the cheap drain is only sound while this thread's caches carry no
/// uncommitted-catalog poison from an earlier full-worker task
/// (`wretain::caches_tainted`, set when that task bound a transaction with
/// unbroadcast invalidation messages that may since have aborted — aborts
/// broadcast nothing). The binding itself refuses pending-invals targets
/// (`validate`), so no NEW taint can arise on this path.
fn accept_invals_or_flush_taint() -> PgResult<()> {
    if init_small::wretain::caches_tainted() {
        inval::local::InvalidateSystemCaches()?;
        init_small::wretain::clear_caches_taint();
    } else {
        inval::local::AcceptInvalidationMessages()?;
    }
    Ok(())
}

/// The bind-time invalidation step, target-aware (M4.2): an
/// uncommitted-DDL target (installed with `invals_flush`, or a
/// `leader_pending_invals` capture) gets the launched-substrate fallback —
/// blanket `InvalidateSystemCaches` so every cache entry consulted during
/// the task is rebuilt under the bound snapshot/xid (the leader's
/// uncommitted catalog rows become visible), then an EAGER taint: entries
/// built during the task hold uncommitted catalog state, and if that
/// transaction ABORTS no sinval traffic ever corrects them, so the next
/// adoption on this thread must re-blanket instead of trusting the cheap
/// drain (the launched path's `note_caches_tainted` law, parallel/lib.rs).
/// Eager (at bind, not unbind) so every exit path — error, panic, retry —
/// is covered without touching the finish choreography.
fn bind_invalidations(shared: &Arc<ParallelShared>) -> PgResult<()> {
    let policy = shared
        .query_task_binding
        .load(std::sync::atomic::Ordering::Acquire);
    if policy & super::QUERY_TASK_INVALS_FLUSH != 0 || shared.leader_pending_invals {
        inval::local::InvalidateSystemCaches()?;
        init_small::wretain::note_caches_tainted();
        Ok(())
    } else {
        accept_invals_or_flush_taint()
    }
}

pub(super) fn with_query_task_binding<T>(
    shared: &Arc<ParallelShared>,
    body: impl FnOnce() -> PgResult<T>,
) -> PgResult<T> {
    validate(shared)?;
    super::gtrace("w.qtb.bind.begin");
    let mut guard = QueryTaskBindingGuard::bind(shared)?;
    super::gtrace("w.qtb.bind.end");
    let outcome = catch_unwind(AssertUnwindSafe(body));
    super::gtrace("w.qtb.body.end");
    match outcome {
        Ok(Ok(value)) => {
            guard.finish(true)?;
            super::gtrace("w.qtb.finish.end");
            Ok(value)
        }
        Ok(Err(error)) => {
            if catch_unwind(AssertUnwindSafe(|| guard.finish(false))).is_err() {
                guard.retry_cleanup_after_panic();
            }
            Err(error)
        }
        Err(payload) => {
            if catch_unwind(AssertUnwindSafe(|| guard.finish(false))).is_err() {
                guard.retry_cleanup_after_panic();
            }
            resume_unwind(payload)
        }
    }
}

fn validate(shared: &ParallelShared) -> PgResult<()> {
    if !super::IsParallelWorker() {
        return Err(prerequisite(
            "query-task binding requires a parked parallel helper",
        ));
    }
    if super::MY_WORKER_SHARED.with(|slot| slot.borrow().is_some()) {
        return Err(prerequisite("nested query-task binding is not allowed"));
    }
    if let Some(issue) = session::SessionEnvelopeBoundaryIssue() {
        return Err(prerequisite(issue));
    }
    if shared.database_id == InvalidOid
        || init_small::globals::MyDatabaseId() == InvalidOid
        || shared.database_id != init_small::globals::MyDatabaseId()
    {
        return Err(unsupported(
            "query-task binding refuses cross-database helpers",
        ));
    }
    let proc_number = init_small::globals::MyProcNumber();
    if proc_number == types_core::INVALID_PROC_NUMBER
        || lmgr_proc::GetPGProcByNumber(proc_number)
            .lockGroupLeader
            .load(std::sync::atomic::Ordering::Relaxed)
            != shared.parallel_leader_proc_number
    {
        return Err(unsupported(
            "query-task binding refuses cross-leader helpers",
        ));
    }
    let policy = shared
        .query_task_binding
        .load(std::sync::atomic::Ordering::Acquire);
    if policy & super::QUERY_TASK_INSTALLED == 0 {
        return Err(prerequisite("query-task binding target was not installed"));
    }
    if policy & super::QUERY_TASK_PARAMS != 0 {
        return Err(unsupported("query-task binding refuses Params"));
    }
    if policy & super::QUERY_TASK_SERIALIZABLE != 0 || shared.serializable_xact_handle != 0 {
        return Err(unsupported(
            "query-task binding refuses serializable transactions",
        ));
    }
    if policy & super::QUERY_TASK_TEMP != 0
        || shared.temp_namespace_id != InvalidOid
        || shared.temp_toast_namespace_id != InvalidOid
    {
        return Err(unsupported("query-task binding refuses temporary state"));
    }
    if (policy & super::QUERY_TASK_PENDING_INVALS != 0 || shared.leader_pending_invals)
        && policy & super::QUERY_TASK_INVALS_FLUSH == 0
    {
        // M4.2: an installer that DECLARED the uncommitted-DDL target
        // (invals_flush) opts into the launched-substrate fallback
        // semantics instead (blanket flush + eager taint at bind).
        return Err(unsupported(
            "query-task binding refuses target-uncommitted invalidations",
        ));
    }
    Ok(())
}

/// RAII binder for one query task executing on a foreign (parked helper)
/// thread: binds identity, xact, snapshot, GUCs, temp-namespace, client and
/// record-registry state, and restores the helper's exact pre-bind state on
/// every exit path (success, error, cancellation, panic; cleanup-panic gets
/// one retry). Opaque outside this crate: construction and completion are
/// only reachable through `with_query_task_binding`, which owns the
/// catch-unwind and cleanup-retry choreography this type's safety depends on.
/// Re-exported as `runtime::QueryTaskGuard` (pinned M0 interface).
pub struct QueryTaskBindingGuard {
    saved_worker_shared: Option<Arc<ParallelShared>>,
    saved_identity: miscinit::SessionIdentityState,
    saved_xact_characteristics: SavedTransactionCharacteristics,
    saved_xact_timestamp: TimestampTz,
    saved_statement_timestamp: TimestampTz,
    saved_namespace: catalog_namespace::SessionNamespaceState,
    saved_gucs: Option<guc::store::ExactGucState>,
    saved_client: (Option<&'static str>, types_core::init::UserAuth),
    saved_record_registry: Option<typcache_seams::RecordRegistryHandle>,
    guc_binding: Option<guc::store::SessionGucBinding>,
    transaction_started: bool,
    snapshot_pushed: bool,
    parallel_mode: bool,
    armed: bool,
}

impl QueryTaskBindingGuard {
    fn bind(shared: &Arc<ParallelShared>) -> PgResult<Self> {
        let saved_client = miscinit::client_connection_info();
        let saved_record_registry = typcache_seams::record_registry_handle::is_installed()
            .then(typcache_seams::record_registry_handle::call);
        let mut guard = Self {
            saved_worker_shared: super::MY_WORKER_SHARED
                .with(|slot| slot.borrow_mut().replace(Arc::clone(shared))),
            saved_identity: miscinit::CaptureSessionIdentityState(),
            saved_xact_characteristics: xact::SaveTransactionCharacteristics(),
            saved_xact_timestamp: xact::GetCurrentTransactionStartTimestamp(),
            saved_statement_timestamp: xact::GetCurrentStatementStartTimestamp(),
            saved_namespace: catalog_namespace::CaptureSessionNamespaceState(),
            saved_gucs: Some(guc::store::capture_exact_guc_state()),
            saved_client,
            saved_record_registry,
            guc_binding: None,
            transaction_started: false,
            snapshot_pushed: false,
            parallel_mode: false,
            armed: true,
        };

        let setup = (|| {
            miscinit::ReplaceSessionIdentityState(miscinit::SessionIdentityState {
                authenticated_user_id: InvalidOid,
                session_user_id: InvalidOid,
                outer_user_id: InvalidOid,
                current_user_id: InvalidOid,
                system_user: None,
                session_user_is_superuser: false,
                security_restriction_context: 0,
                set_role_is_active: false,
            });
            miscinit::SetAuthenticatedUserId(shared.authenticated_user_id);
            miscinit::SetSessionAuthorization(
                shared.session_user_id,
                shared.session_user_is_superuser,
            )?;
            miscinit::SetCurrentRoleId(shared.outer_user_id, shared.role_is_superuser)?;
            #[cfg(debug_assertions)]
            inject(QueryTaskFaultPoint::BindIdentity)?;

            xact::SetParallelStartTimestamps(shared.xact_ts, shared.stmt_ts);
            guard.transaction_started = true;
            xact::StartParallelWorkerTransaction(&shared.tstate)?;
            #[cfg(debug_assertions)]
            inject(QueryTaskFaultPoint::BindTransaction)?;

            catalog_storage::RestorePendingSyncs(&shared.pending_syncs);
            relmapper::RestoreRelationMap(&shared.relmap)?;
            types_rel::reindex::restore_reindex_state(
                &shared.reindex,
                xact::GetCurrentTransactionNestLevel(),
            );
            combocid::RestoreComboCIDState(&shared.combocid);
            if typcache_seams::install_record_registry::is_installed() {
                typcache_seams::install_record_registry::call(std::sync::Arc::clone(
                    &shared.record_registry,
                ));
            }
            #[cfg(debug_assertions)]
            inject(QueryTaskFaultPoint::BindRelationMap)?;

            let active = snapmgr::RestoreSnapshot(&shared.active_snapshot);
            let transaction = shared
                .transaction_snapshot
                .as_ref()
                .unwrap_or(&shared.active_snapshot);
            snapmgr::RestoreTransactionSnapshot(transaction, shared.parallel_leader_proc_number)?;
            #[cfg(debug_assertions)]
            inject(QueryTaskFaultPoint::BindTransactionSnapshot)?;
            snapmgr::PushActiveSnapshot(&active)?;
            guard.snapshot_pushed = true;
            #[cfg(debug_assertions)]
            inject(QueryTaskFaultPoint::BindActiveSnapshot)?;
            bind_invalidations(shared)?;
            #[cfg(debug_assertions)]
            inject(QueryTaskFaultPoint::BindInvalidations)?;

            guc::ResetAllOptions();
            // Composition (train-11): guc-snapshots replaced ParallelShared's
            // captured-session share (guc_bind) with the typed query pin
            // (guc_pin, populated iff session_guc_bind_enabled() at capture);
            // parked query tasks adopt it exactly like the worker path in
            // lib.rs — leader-validated values + base adoption, no check-hook
            // rerun. bind_query_pin returns the same SessionGucBinding guard.
            if let Some(pin) = shared.guc_pin.as_ref() {
                guard.guc_binding = Some(guc::layers::bind_query_pin(pin)?);
            } else {
                guc::store::restore_nondefault_variables(&shared.guc_state)?;
            }
            #[cfg(debug_assertions)]
            inject(QueryTaskFaultPoint::BindGucs)?;
            miscinit::SetUserIdAndSecContext(shared.current_user_id, shared.sec_context);
            // A parked helper carries its own session temp-namespace state; a C
            // parallel worker is a fresh process whose temp namespace is unset.
            // SetTempNamespaceState asserts that fresh-process precondition, so
            // reset the pooled helper to the fresh baseline first. The helper's
            // pre-bind namespace was captured in `saved_namespace` above and is
            // restored on every exit path (finish/Drop/retry) below, so this
            // reset never leaks: it is undone byte-for-byte at task boundary.
            catalog_namespace::ResetTempNamespaceStateForRetainedPark();
            catalog_namespace::SetTempNamespaceState(
                shared.temp_namespace_id,
                shared.temp_toast_namespace_id,
            );
            miscinit::RestoreClientConnectionInfo(&shared.clientconninfo)?;
            #[cfg(debug_assertions)]
            inject(QueryTaskFaultPoint::BindClient)?;
            let (authn_id, auth_method) = miscinit::client_connection_info();
            if let Some(authn_id) = authn_id {
                miscinit::InitializeSystemUser(
                    authn_id,
                    hba_seams::hba_authname::call(auth_method),
                );
            }
            xact::EnterParallelMode();
            guard.parallel_mode = true;
            #[cfg(debug_assertions)]
            inject(QueryTaskFaultPoint::BindParallelMode)?;
            Ok(())
        })();

        if let Err(error) = setup {
            if catch_unwind(AssertUnwindSafe(|| guard.finish(false))).is_err() {
                guard.retry_cleanup_after_panic();
            }
            return Err(error);
        }
        Ok(guard)
    }

    /// The STATEMENT half of the unbind: parallel mode, active snapshot,
    /// the worker transaction, and the transaction characteristics /
    /// timestamps — everything the binder re-establishes PER STATEMENT.
    /// Split out for the ceremony-v2 sticky park (`finish_for_sticky_park`),
    /// which runs this half and RETAINS the session half.
    fn finish_statement_into(&mut self, commit: bool, first: &mut Option<Box<PgError>>) {
        if self.parallel_mode {
            xact::ExitParallelMode();
            self.parallel_mode = false;
        }
        #[cfg(debug_assertions)]
        retain_first(first, inject(QueryTaskFaultPoint::FinishParallelMode));
        if self.snapshot_pushed {
            retain_first(first, snapmgr::PopActiveSnapshot());
            self.snapshot_pushed = false;
        }
        #[cfg(debug_assertions)]
        retain_first(first, inject(QueryTaskFaultPoint::FinishSnapshot));
        if self.transaction_started {
            let end = if commit && first.is_none() {
                xact::EndParallelWorkerTransaction()
            } else {
                xact::AbortOutOfAnyTransaction()
            };
            if end.is_err() && commit {
                retain_first(first, end);
                retain_first(first, xact::AbortOutOfAnyTransaction());
            } else {
                retain_first(first, end);
            }
            self.transaction_started = false;
        }
        #[cfg(debug_assertions)]
        retain_first(first, inject(QueryTaskFaultPoint::FinishTransaction));

        xact::RestoreTransactionCharacteristics(self.saved_xact_characteristics);
        xact::SetParallelStartTimestamps(self.saved_xact_timestamp, self.saved_statement_timestamp);
    }

    /// The SESSION half of the unbind: GUC binding + exact store restore,
    /// record registry, client connection, namespace, identity. This is the
    /// half the ceremony-v2 sticky park RETAINS between same-session
    /// engagements.
    fn finish_session_into(&mut self, first: &mut Option<Box<PgError>>) {
        self.guc_binding.take();
        if let Some(gucs) = self.saved_gucs.take() {
            guc::store::replace_exact_guc_state(&gucs);
        }
        if let Some(registry) = self.saved_record_registry.take() {
            if typcache_seams::install_record_registry::is_installed() {
                typcache_seams::install_record_registry::call(registry);
            }
        }
        miscinit::set_client_connection_info(self.saved_client.0, self.saved_client.1);
        catalog_namespace::ReplaceSessionNamespaceState(&self.saved_namespace);
        miscinit::ReplaceSessionIdentityState(self.saved_identity);
        #[cfg(debug_assertions)]
        retain_first(first, inject(QueryTaskFaultPoint::FinishSessionState));
    }

    fn finish(&mut self, commit: bool) -> PgResult<()> {
        let mut first = None;
        self.finish_statement_into(commit, &mut first);
        self.finish_session_into(&mut first);
        super::MY_WORKER_SHARED.with(|slot| {
            *slot.borrow_mut() = self.saved_worker_shared.take();
        });
        #[cfg(debug_assertions)]
        retain_first(&mut first, inject(QueryTaskFaultPoint::FinishBoundary));
        self.armed = false;

        if let Some(issue) = session::SessionEnvelopeBoundaryIssue() {
            retain_first(&mut first, Err(prerequisite(issue)));
        }
        match first {
            Some(error) => {
                init_small::wretain::refuse_park();
                Err(error)
            }
            None => Ok(()),
        }
    }

    /// Ceremony-v2 sticky park: finish only the STATEMENT half and verify
    /// the retained-bind boundary (everything the envelope demands of a
    /// task boundary EXCEPT the deliberately-live SessionGucBinding). On
    /// success the guard is DISARMED and parked in the sticky slot — its
    /// session half (identity, applied GUC pin, namespace, client, record
    /// registry, and the saved pre-bind session state) stays live for the
    /// next same-session engagement. Any statement-half error or boundary
    /// issue DEMOTES to the full unbind and returns the first error.
    fn finish_for_sticky_park(&mut self) -> PgResult<()> {
        let mut first = None;
        self.finish_statement_into(true, &mut first);
        super::MY_WORKER_SHARED.with(|slot| {
            *slot.borrow_mut() = self.saved_worker_shared.take();
        });
        if first.is_none() {
            if let Some(issue) = session::SessionEnvelopeBoundaryIssueForRetainedBind() {
                retain_first(&mut first, Err(prerequisite(issue)));
            }
        }
        match first {
            None => {
                // Disarmed while parked: an abandoned sticky guard (thread
                // death) must be a plain drop — restoration of a dying
                // thread's TLS is pointless and TLS-destructor order makes
                // it hazardous. Resume re-arms.
                self.armed = false;
                Ok(())
            }
            Some(error) => {
                let mut rest = None;
                self.finish_session_into(&mut rest);
                self.armed = false;
                init_small::wretain::refuse_park();
                if let Some(issue) = session::SessionEnvelopeBoundaryIssue() {
                    retain_first(&mut rest, Err(prerequisite(issue)));
                }
                // The park-refusing error wins; the demotion's own errors
                // were best-effort cleanup.
                let _ = rest;
                Err(error)
            }
        }
    }

    /// Ceremony-v2 sticky resume: re-establish ONLY the per-statement state
    /// over the retained session half — parallel start timestamps, the
    /// worker transaction, pending-syncs/relmap/reindex/combocid (kept
    /// per-statement for exactness; they are cheap copies), the snapshots,
    /// the sinval drain, current-user/sec-context, and parallel mode. The
    /// expensive session half (identity ceremony, GUC capture + reset +
    /// pin apply, namespace, client, record registry) is skipped — the
    /// sticky key proved it byte-identical. On error the guard completes
    /// the FULL unbind (retention over) and returns the error.
    fn resume_statement(&mut self, shared: &Arc<ParallelShared>) -> PgResult<()> {
        debug_assert!(!self.armed, "sticky resume over an armed guard");
        self.saved_worker_shared = super::MY_WORKER_SHARED
            .with(|slot| slot.borrow_mut().replace(Arc::clone(shared)));
        self.armed = true;
        let setup = (|| {
            xact::SetParallelStartTimestamps(shared.xact_ts, shared.stmt_ts);
            self.transaction_started = true;
            xact::StartParallelWorkerTransaction(&shared.tstate)?;
            #[cfg(debug_assertions)]
            inject(QueryTaskFaultPoint::BindTransaction)?;

            catalog_storage::RestorePendingSyncs(&shared.pending_syncs);
            relmapper::RestoreRelationMap(&shared.relmap)?;
            types_rel::reindex::restore_reindex_state(
                &shared.reindex,
                xact::GetCurrentTransactionNestLevel(),
            );
            combocid::RestoreComboCIDState(&shared.combocid);
            #[cfg(debug_assertions)]
            inject(QueryTaskFaultPoint::BindRelationMap)?;

            let active = snapmgr::RestoreSnapshot(&shared.active_snapshot);
            let transaction = shared
                .transaction_snapshot
                .as_ref()
                .unwrap_or(&shared.active_snapshot);
            snapmgr::RestoreTransactionSnapshot(transaction, shared.parallel_leader_proc_number)?;
            #[cfg(debug_assertions)]
            inject(QueryTaskFaultPoint::BindTransactionSnapshot)?;
            snapmgr::PushActiveSnapshot(&active)?;
            self.snapshot_pushed = true;
            #[cfg(debug_assertions)]
            inject(QueryTaskFaultPoint::BindActiveSnapshot)?;
            bind_invalidations(shared)?;
            #[cfg(debug_assertions)]
            inject(QueryTaskFaultPoint::BindInvalidations)?;

            miscinit::SetUserIdAndSecContext(shared.current_user_id, shared.sec_context);
            xact::EnterParallelMode();
            self.parallel_mode = true;
            #[cfg(debug_assertions)]
            inject(QueryTaskFaultPoint::BindParallelMode)?;
            Ok(())
        })();
        if let Err(error) = setup {
            if catch_unwind(AssertUnwindSafe(|| self.finish(false))).is_err() {
                self.retry_cleanup_after_panic();
            }
            return Err(error);
        }
        Ok(())
    }

    fn retry_cleanup_after_panic(&mut self) {
        init_small::wretain::refuse_park();
        let _ = catch_unwind(AssertUnwindSafe(|| self.finish(false)));
        self.armed = false;
    }
}

impl Drop for QueryTaskBindingGuard {
    fn drop(&mut self) {
        if self.armed {
            self.retry_cleanup_after_panic();
        }
    }
}

fn retain_first(first: &mut Option<Box<PgError>>, result: PgResult<()>) {
    if let Err(error) = result {
        if first.is_none() {
            *first = Some(error);
        }
    }
}

// ---------------------------------------------------------------------------
// CEREMONY-V2 (notes/runtime-ceremony2.md): lazy (first-claim) binding and
// sticky session-affine retention for standing runtime executors.
//
// LAZY: the drive-side caller constructs a `DeferredQueryTaskBinding` and
// enters the pinned drive UNBOUND; the bind happens at the worker's FIRST
// morsel claim (the sink layer's fork-on-first-touch precedent), so a
// participant that never claims work never pays the bind. `validate()`
// runs pre-drive so refusals keep today's fail-closed non-participation
// surface; a bind ERROR after a claim is a real query error (the claimed
// morsel range cannot be returned).
//
// STICKY: a gang worker finishing an engagement for session S parks with
// its session half BOUND (finish_for_sticky_park). The next engagement
// from S with an unchanged GUC pin (Arc identity — the leader's
// current_query_pin is cached on the store mutation counter, so unchanged
// state ⇒ the same Arc) resumes with only the statement half
// (resume_statement). A different session's engagement, a changed pin, or
// any boundary issue evicts the retention with the full unbind BEFORE the
// new bind — SESSION_BOUND accounting stays exact and no session state can
// cross sessions. Kill switches: PGRUST_RUNTIME_LAZYBIND=0 (restores the
// eager wrap, which also disables sticky), PGRUST_RUNTIME_STICKY=0.
// ---------------------------------------------------------------------------

/// PGRUST_RUNTIME_LAZYBIND=0 restores the eager per-engagement bind.
pub fn lazy_bind_enabled() -> bool {
    static ON: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ON.get_or_init(|| {
        std::env::var("PGRUST_RUNTIME_LAZYBIND").map_or(true, |v| v.trim() != "0")
    })
}

/// PGRUST_RUNTIME_STICKY=0 disables session-affine retention (lazy bind
/// stays). Sticky is implemented only on the deferred path, so LAZYBIND=0
/// disables both.
pub fn sticky_bind_enabled() -> bool {
    static ON: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    lazy_bind_enabled()
        && *ON.get_or_init(|| {
            std::env::var("PGRUST_RUNTIME_STICKY").map_or(true, |v| v.trim() != "0")
        })
}

/// A parked worker's retained session bind, keyed to exactly one session's
/// engagement identity. Holds NO references into any leader arena or shared
/// memory: the guard's saved state and the key Arcs (GUC pin, record
/// registry) are plain heap — a sticky-parked worker can be abandoned at
/// thread death with a plain drop (the guard is disarmed while parked).
struct StickySession {
    guard: QueryTaskBindingGuard,
    leader_proc_number: types_core::ProcNumber,
    leader_pid: i32,
    database_id: Oid,
    authenticated_user_id: Oid,
    session_user_id: Oid,
    outer_user_id: Oid,
    guc_pin: Arc<guc::layers::GucQuerySnapshot>,
    record_registry: typcache_seams::RecordRegistryHandle,
}

impl StickySession {
    fn matches(&self, shared: &ParallelShared) -> bool {
        shared.parallel_leader_proc_number == self.leader_proc_number
            && shared.parallel_leader_pid == self.leader_pid
            && shared.database_id == self.database_id
            && shared.authenticated_user_id == self.authenticated_user_id
            && shared.session_user_id == self.session_user_id
            && shared.outer_user_id == self.outer_user_id
            // The pin compare IS the unchanged-GUC test: the leader's
            // statement-window cache returns the same Arc iff its store
            // mutation counter is unchanged, and the worker holds a strong
            // ref, so pointer identity cannot alias across captures.
            && shared
                .guc_pin
                .as_ref()
                .is_some_and(|pin| Arc::ptr_eq(pin, &self.guc_pin))
            && Arc::ptr_eq(&shared.record_registry, &self.record_registry)
    }
}

/// TLS wrapper for the mid-drive bound guard: if the thread dies with a
/// bound guard still installed (exit-committed unwind that bypassed the
/// structured cleanup), FORGET it — running session restores from a TLS
/// destructor races other TLS destruction and the proc-exit callbacks.
struct ActiveDeferredSlot(Option<QueryTaskBindingGuard>);

impl Drop for ActiveDeferredSlot {
    fn drop(&mut self) {
        if let Some(guard) = self.0.take() {
            std::mem::forget(guard);
        }
    }
}

thread_local! {
    static STICKY: RefCell<Option<StickySession>> = const { RefCell::new(None) };
    static ACTIVE_DEFERRED: RefCell<ActiveDeferredSlot> =
        const { RefCell::new(ActiveDeferredSlot(None)) };
}

/// Standing-worker exit paths: drop any sticky retention with a PLAIN drop
/// (the parked guard is disarmed — no restores run, no shared memory is
/// touched; only heap Arcs release). Safe on both the clean and the
/// crash-fence (raw) exits.
pub(super) fn sticky_clear() {
    STICKY.with(|slot| {
        let _ = slot.borrow_mut().take();
    });
}

/// True when this thread currently parks a sticky retention (tranche/unit
/// observability).
pub fn sticky_parked() -> bool {
    STICKY.with(|slot| slot.borrow().is_some())
}

/// Evict one taken sticky retention with the full session-half unbind —
/// finish(true): the parked guard holds no statement state, so this is the
/// session restore only. The eviction choreography shared by
/// `DeferredQueryTaskBinding::new` and `sticky_evict_parked`.
fn finish_evicted_sticky(mut sticky: StickySession) -> PgResult<()> {
    super::gtrace("w.qtb.unstick.begin");
    let r = catch_unwind(AssertUnwindSafe(|| sticky.guard.finish(true)));
    super::gtrace("w.qtb.unstick.end");
    match r {
        Ok(Ok(())) => Ok(()),
        Ok(Err(e)) => Err(e),
        Err(payload) => {
            if is_exit_unwind_payload(&*payload) {
                resume_unwind(payload);
            }
            sticky.guard.retry_cleanup_after_panic();
            Err(prerequisite(
                "sticky session eviction panicked; engagement refused",
            ))
        }
    }
}

/// Evict ANY parked sticky retention (M2 inc-1): the EAGER binder's
/// pre-bind discipline on a standing worker — the sink arms bind through
/// `with_query_task_binding`, whose validate() envelope gate refuses over
/// a live retained session bind, so a retention parked by a (scan-arm)
/// deferred engagement must be restored away first. No-op when nothing is
/// parked; an eviction error refuses the engagement (fail-closed,
/// pre-claim).
pub(crate) fn sticky_evict_parked() -> PgResult<()> {
    let Some(sticky) = STICKY.with(|slot| slot.borrow_mut().take()) else {
        return Ok(());
    };
    finish_evicted_sticky(sticky)
}

/// validate() for the sticky resume: the same gates, except the envelope's
/// live-SessionGucBinding condition — the retention IS a deliberately-live
/// binding (see SessionEnvelopeBoundaryIssueForRetainedBind). Only reachable
/// when the sticky key matched this engagement.
fn validate_for_sticky_resume(shared: &ParallelShared) -> PgResult<()> {
    if !super::IsParallelWorker() {
        return Err(prerequisite(
            "query-task binding requires a parked parallel helper",
        ));
    }
    if super::MY_WORKER_SHARED.with(|slot| slot.borrow().is_some()) {
        return Err(prerequisite("nested query-task binding is not allowed"));
    }
    if let Some(issue) = session::SessionEnvelopeBoundaryIssueForRetainedBind() {
        return Err(prerequisite(issue));
    }
    if shared.database_id == InvalidOid
        || init_small::globals::MyDatabaseId() == InvalidOid
        || shared.database_id != init_small::globals::MyDatabaseId()
    {
        return Err(unsupported(
            "query-task binding refuses cross-database helpers",
        ));
    }
    let proc_number = init_small::globals::MyProcNumber();
    if proc_number == types_core::INVALID_PROC_NUMBER
        || lmgr_proc::GetPGProcByNumber(proc_number)
            .lockGroupLeader
            .load(std::sync::atomic::Ordering::Relaxed)
            != shared.parallel_leader_proc_number
    {
        return Err(unsupported(
            "query-task binding refuses cross-leader helpers",
        ));
    }
    let policy = shared
        .query_task_binding
        .load(std::sync::atomic::Ordering::Acquire);
    if policy & super::QUERY_TASK_INSTALLED == 0 {
        return Err(prerequisite("query-task binding target was not installed"));
    }
    if policy & super::QUERY_TASK_PARAMS != 0 {
        return Err(unsupported("query-task binding refuses Params"));
    }
    if policy & super::QUERY_TASK_SERIALIZABLE != 0 || shared.serializable_xact_handle != 0 {
        return Err(unsupported(
            "query-task binding refuses serializable transactions",
        ));
    }
    if policy & super::QUERY_TASK_TEMP != 0
        || shared.temp_namespace_id != InvalidOid
        || shared.temp_toast_namespace_id != InvalidOid
    {
        return Err(unsupported("query-task binding refuses temporary state"));
    }
    if (policy & super::QUERY_TASK_PENDING_INVALS != 0 || shared.leader_pending_invals)
        && policy & super::QUERY_TASK_INVALS_FLUSH == 0
    {
        // M4.2: an installer that DECLARED the uncommitted-DDL target
        // (invals_flush) opts into the launched-substrate fallback
        // semantics instead (blanket flush + eager taint at bind).
        return Err(unsupported(
            "query-task binding refuses target-uncommitted invalidations",
        ));
    }
    Ok(())
}

/// One engagement's deferred (first-touch) query-task binding. Construct
/// pre-drive (evicts a mismatched sticky retention), `validate()` pre-drive
/// (fail-closed refusal), `bind_now()` at the first morsel claim, and
/// `finish(commit)` post-drive on every structured path.
pub struct DeferredQueryTaskBinding {
    shared: Arc<ParallelShared>,
    sticky_allowed: bool,
    bound: std::cell::Cell<bool>,
    resumed: std::cell::Cell<bool>,
}

impl DeferredQueryTaskBinding {
    /// Pre-drive construction. Evicts (full unbind) a sticky retention that
    /// does not key to THIS engagement — before validate(), so the envelope
    /// gate sees a clean boundary for the foreign-session full bind. An
    /// eviction error refuses the engagement (pre-claim, fail-closed).
    pub fn new(shared: &Arc<ParallelShared>, sticky_allowed: bool) -> PgResult<Self> {
        // Containment: a previous engagement that lost its structured
        // cleanup (a panic between install and finish) must not wedge this
        // worker into permanent nested-binding refusals.
        let stale = ACTIVE_DEFERRED.with(|slot| slot.borrow_mut().0.take());
        if let Some(mut guard) = stale {
            let _ = elog::elog(
                WARNING,
                "query-task deferred binding: cleaning a stale bound guard".to_string(),
            );
            if catch_unwind(AssertUnwindSafe(|| guard.finish(false))).is_err() {
                guard.retry_cleanup_after_panic();
            }
        }
        let evict = STICKY.with(|slot| {
            let mut slot = slot.borrow_mut();
            match slot.as_ref() {
                Some(sticky)
                    if !(sticky_allowed
                        && sticky_bind_enabled()
                        && sticky.matches(shared)) =>
                {
                    slot.take()
                }
                _ => None,
            }
        });
        if let Some(sticky) = evict {
            finish_evicted_sticky(sticky)?;
        }
        Ok(DeferredQueryTaskBinding {
            shared: Arc::clone(shared),
            sticky_allowed,
            bound: std::cell::Cell::new(false),
            resumed: std::cell::Cell::new(false),
        })
    }

    fn sticky_resume_ready(&self) -> bool {
        self.sticky_allowed
            && sticky_bind_enabled()
            && STICKY.with(|slot| {
                slot.borrow()
                    .as_ref()
                    .is_some_and(|sticky| sticky.matches(&self.shared))
            })
    }

    /// Pre-drive refusal gate: today's fail-closed validate surface (the
    /// sticky-aware variant when a matching retention will resume).
    pub fn validate(&self) -> PgResult<()> {
        if self.sticky_resume_ready() {
            validate_for_sticky_resume(&self.shared)
        } else {
            validate(&self.shared)
        }
    }

    pub fn is_bound(&self) -> bool {
        self.bound.get()
    }

    /// True when the bind was a sticky resume (attribution/tranche
    /// observability).
    pub fn resumed_sticky(&self) -> bool {
        self.resumed.get()
    }

    /// First-touch bind: sticky resume when the retention keys to this
    /// engagement, the full bind otherwise. Consumes the standing channel's
    /// deferred visibility arming (procarray + ProcSignal) FIRST — the
    /// snapshot restore publishes xmin, which only counts while visible.
    pub fn bind_now(&self) -> PgResult<()> {
        assert!(!self.bound.get(), "deferred query-task binding bound twice");
        super::standing::engage_deferred_visibility()?;
        let sticky = STICKY.with(|slot| {
            let mut slot = slot.borrow_mut();
            match slot.as_ref() {
                Some(sticky)
                    if self.sticky_allowed
                        && sticky_bind_enabled()
                        && sticky.matches(&self.shared) =>
                {
                    slot.take()
                }
                _ => None,
            }
        });
        let guard = if let Some(mut sticky) = sticky {
            super::gtrace("w.qtb.stickybind.begin");
            if let Err(e) = validate_for_sticky_resume(&self.shared) {
                // Conservative: a dirty resume boundary ends the retention.
                let _ = catch_unwind(AssertUnwindSafe(|| sticky.guard.finish(true)));
                return Err(e);
            }
            sticky.guard.resume_statement(&self.shared)?;
            super::gtrace("w.qtb.stickybind.end");
            self.resumed.set(true);
            sticky.guard
        } else {
            validate(&self.shared)?;
            super::gtrace("w.qtb.bind.begin");
            let guard = QueryTaskBindingGuard::bind(&self.shared)?;
            super::gtrace("w.qtb.bind.end");
            guard
        };
        ACTIVE_DEFERRED.with(|slot| {
            let prev = slot.borrow_mut().0.replace(guard);
            debug_assert!(prev.is_none(), "deferred binding installed twice");
            if let Some(prev) = prev {
                std::mem::forget(prev);
            }
        });
        self.bound.set(true);
        Ok(())
    }

    /// Post-drive completion, on every structured path. Never bound: a
    /// matching sticky retention (if any) simply stays parked. Bound +
    /// commit + sticky allowed: statement-half finish and park the session
    /// half keyed to this engagement. Otherwise: the full unbind with the
    /// eager wrap's exact catch/retry choreography.
    pub fn finish(self, commit: bool) -> PgResult<()> {
        if !self.bound.get() {
            return Ok(());
        }
        let Some(mut guard) = ACTIVE_DEFERRED.with(|slot| slot.borrow_mut().0.take()) else {
            debug_assert!(false, "deferred binding finish without a bound guard");
            return Ok(());
        };
        if commit
            && self.sticky_allowed
            && sticky_bind_enabled()
            // No pin (PGRUST_NO_GUC_BIND string-restore path) = no
            // unchanged-pin test = no retention.
            && self.shared.guc_pin.is_some()
        {
            match catch_unwind(AssertUnwindSafe(|| guard.finish_for_sticky_park())) {
                Ok(Ok(())) => {
                    super::gtrace("w.qtb.stickypark");
                    let shared = &self.shared;
                    let pin = shared
                        .guc_pin
                        .as_ref()
                        .expect("sticky park requires a guc pin (gated above)")
                        .clone();
                    STICKY.with(|slot| {
                        *slot.borrow_mut() = Some(StickySession {
                            guard,
                            leader_proc_number: shared.parallel_leader_proc_number,
                            leader_pid: shared.parallel_leader_pid,
                            database_id: shared.database_id,
                            authenticated_user_id: shared.authenticated_user_id,
                            session_user_id: shared.session_user_id,
                            outer_user_id: shared.outer_user_id,
                            guc_pin: pin,
                            record_registry: Arc::clone(&shared.record_registry),
                        });
                    });
                    return Ok(());
                }
                Ok(Err(e)) => return Err(e),
                Err(payload) => {
                    guard.retry_cleanup_after_panic();
                    resume_unwind(payload);
                }
            }
        }
        if commit {
            guard.finish(true)
        } else {
            if catch_unwind(AssertUnwindSafe(|| guard.finish(false))).is_err() {
                guard.retry_cleanup_after_panic();
            }
            Ok(())
        }
    }
}

/// Exit-committed unwinds must keep unwinding (mirror of
/// standing::is_exit_unwind, local to avoid a module cycle).
fn is_exit_unwind_payload(payload: &(dyn std::any::Any + Send)) -> bool {
    payload.is::<ipc::ProcExitThread>() || payload.is::<types_error::PanicExitThread>()
}

fn unsupported(message: &'static str) -> Box<PgError> {
    PgError::new(ERROR, message)
        .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED)
        .into()
}

fn prerequisite(message: &'static str) -> Box<PgError> {
    PgError::new(ERROR, message)
        .with_sqlstate(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
        .into()
}
