use std::panic::{catch_unwind, resume_unwind, AssertUnwindSafe};
use std::sync::Arc;

use types_core::{InvalidOid, SavedTransactionCharacteristics, TimestampTz};
use types_error::{
    PgError, PgResult, ERRCODE_FEATURE_NOT_SUPPORTED, ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE,
    ERROR,
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
static QUERY_TASK_FAULT: std::sync::Mutex<Option<(QueryTaskFaultPoint, QueryTaskFaultAction)>> =
    std::sync::Mutex::new(None);

#[cfg(debug_assertions)]
pub fn set_query_task_fault(point: QueryTaskFaultPoint, action: QueryTaskFaultAction) {
    *QUERY_TASK_FAULT.lock().unwrap_or_else(|error| error.into_inner()) = Some((point, action));
}

#[cfg(debug_assertions)]
fn inject(point: QueryTaskFaultPoint) -> PgResult<()> {
    let action = {
        let mut fault = QUERY_TASK_FAULT.lock().unwrap_or_else(|error| error.into_inner());
        if fault.as_ref().map(|value| value.0) == Some(point) {
            fault.take().map(|value| value.1)
        } else {
            None
        }
    };
    match action {
        Some(QueryTaskFaultAction::Error) => {
            Err(PgError::new(ERROR, format!("query-task injected fault at {point:?}")).into())
        }
        Some(QueryTaskFaultAction::Panic) => panic!("query-task injected panic at {point:?}"),
        None => Ok(()),
    }
}

pub(super) fn with_query_task_binding<T>(
    shared: &Arc<ParallelShared>,
    body: impl FnOnce() -> PgResult<T>,
) -> PgResult<T> {
    validate(shared)?;
    let mut guard = QueryTaskBindingGuard::bind(shared)?;
    let outcome = catch_unwind(AssertUnwindSafe(body));
    match outcome {
        Ok(Ok(value)) => {
            guard.finish(true)?;
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
    if policy & super::QUERY_TASK_PENDING_INVALS != 0 || shared.leader_pending_invals {
        return Err(unsupported(
            "query-task binding refuses target-uncommitted invalidations",
        ));
    }
    Ok(())
}

struct QueryTaskBindingGuard {
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
            inval::local::AcceptInvalidationMessages()?;
            #[cfg(debug_assertions)]
            inject(QueryTaskFaultPoint::BindInvalidations)?;

            guc::ResetAllOptions();
            if guc::store::session_guc_bind_enabled() {
                guard.guc_binding = Some(guc::store::bind_session_gucs(&shared.guc_bind)?);
            } else {
                guc::store::restore_nondefault_variables(&shared.guc_state)?;
            }
            #[cfg(debug_assertions)]
            inject(QueryTaskFaultPoint::BindGucs)?;
            miscinit::SetUserIdAndSecContext(shared.current_user_id, shared.sec_context);
            catalog_namespace::ReplaceTempNamespaceState(
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

    fn finish(&mut self, commit: bool) -> PgResult<()> {
        let mut first = None;
        if self.parallel_mode {
            xact::ExitParallelMode();
            self.parallel_mode = false;
        }
        #[cfg(debug_assertions)]
        retain_first(&mut first, inject(QueryTaskFaultPoint::FinishParallelMode));
        if self.snapshot_pushed {
            retain_first(&mut first, snapmgr::PopActiveSnapshot());
            self.snapshot_pushed = false;
        }
        #[cfg(debug_assertions)]
        retain_first(&mut first, inject(QueryTaskFaultPoint::FinishSnapshot));
        if self.transaction_started {
            let end = if commit && first.is_none() {
                xact::EndParallelWorkerTransaction()
            } else {
                xact::AbortOutOfAnyTransaction()
            };
            if end.is_err() && commit {
                retain_first(&mut first, end);
                retain_first(&mut first, xact::AbortOutOfAnyTransaction());
            } else {
                retain_first(&mut first, end);
            }
            self.transaction_started = false;
        }
        #[cfg(debug_assertions)]
        retain_first(&mut first, inject(QueryTaskFaultPoint::FinishTransaction));

        xact::RestoreTransactionCharacteristics(self.saved_xact_characteristics);
        xact::SetParallelStartTimestamps(self.saved_xact_timestamp, self.saved_statement_timestamp);
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
        retain_first(&mut first, inject(QueryTaskFaultPoint::FinishSessionState));
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
