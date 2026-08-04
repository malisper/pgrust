//! D2 mint-on-connect (docs/design/test-views.md D2): both halves of the
//! mint protocol live in this module —
//!
//! - **Backend side** (`mint_on_connect`, installed as
//!   `janitor_seams::ephemeral_db_mint_on_connect` and called from
//!   InitPostgres's database-lookup miss, ON THE CONNECTING BACKEND'S
//!   THREAD): grammar + security-posture gating, the per-role cap count,
//!   posting the idempotent Ensure, and parking on the backend's own latch
//!   until the janitor resolves it.
//! - **Janitor side** (`service_pass`, called from the janitor tick):
//!   serialized minting via an internal `CREATE DATABASE ... TEMPLATE t
//!   STRATEGY file_copy` (a direct `dbcommands::createdb` call in its own
//!   transaction — the dropdb_this_victim shape; `PreventInTransactionBlock`
//!   lives only in utility dispatch, which is exactly why this is legal),
//!   then waking every waiter by ProcNumber. Since the batched-mint
//!   addendum, >= 2 pending Ensures in one tick share ONE checkpoint pair
//!   through a single batch transaction of
//!   `dbcommands::createdb_skip_checkpoints` calls (preflight-validated
//!   per entry; whole-batch abort falls back to the serial path) — the
//!   service_pass doc carries the choreography.
//!
//! Refusal discipline (the security posture's teeth): every disqualification
//! that must look stock — feature off, non-matching grammar, unlisted role,
//! bare token with no default template — returns `Ok(false)` so InitPostgres
//! falls through to its byte-identical does-not-exist FATAL. Conditions with
//! their own story — paused/absent janitor, cap, timeout, a failed CREATE —
//! return `Err` (a clean FATAL), because hiding them behind "does not exist"
//! would turn operational states into gaslighting.

use elog::{elog as log_report, ereport};
use init_small::globals as g;
use mcx::Mcx;
use types_core::{Oid, ProcNumber};
use types_error::{
    PgError, PgResult, ERRCODE_CONFIGURATION_LIMIT_EXCEEDED, ERRCODE_INTERNAL_ERROR,
    ERRCODE_UNDEFINED_DATABASE, ERRCODE_WRONG_OBJECT_TYPE, ERROR, FATAL, LOG,
};
use types_guc::GucSource;
use types_nodes::parsenodes::{CreatedbStmt, DefElem, DefElemAction};
use types_nodes::{Node, NodeList};
use types_storage::waiteventset::{WL_EXIT_ON_PM_DEATH, WL_LATCH_SET, WL_TIMEOUT};

use crate::grammar::{self, MintShape};
use crate::registry::{self, EnsureStatus, PostEnsure};

/// 57P03 cannot_connect_now: the mint path's own refusals (paused/absent
/// janitor, timeout). Deliberately NOT 3D000 undefined_database — these are
/// operational states, and libpq-side retry loops key on 57P03 as
/// "try again later".
use types_error::ERRCODE_CANNOT_CONNECT_NOW;

/// Hard cap on how long a connecting backend parks waiting for its mint.
/// FILE_COPY mints are checkpoint-bound, but the janitor batches them: a
/// mint cycle of up to MINT_BATCH_MAX entries shares ONE synchronous
/// checkpoint pair (the batched-mint addendum), so a cold-start storm of K
/// distinct tokens costs O(K/32) checkpoint pairs before the last waiter
/// wakes — with the serial fallback's O(K) pairs as the degraded worst
/// case; 60s covers hundreds of cheap-preset mints while still bounding a
/// wedged janitor to one minute of connect latency (spec: waiters must
/// NEVER hang).
const MINT_WAIT_TIMEOUT_NS: u64 = 60 * 1_000_000_000;

/// Waiter tick: WL_LATCH_SET is advisory (the proc latch is shared with
/// every procsignal sender), so a lost SetLatch must never strand the
/// waiter — the timeout re-polls the entry state. Short ticks also keep the
/// claimed-but-parked sinval slot draining via CHECK_FOR_INTERRUPTS.
const WAIT_TICK_MS: i64 = 250;

/// Same wait tag as the janitor tick (main_loop.rs rationale).
const PG_WAIT_EXTENSION: u32 = 0x0700_0000;

// ---------------------------------------------------------------------------
// Backend side.
// ---------------------------------------------------------------------------

/// The `janitor_seams::ephemeral_db_mint_on_connect` impl. Runs inside
/// InitPostgres's open startup transaction, post-authentication (GetUserId
/// et al. are live), BEFORE any database lock is taken (a parked waiter
/// blocks nothing and counts in no CountDBBackends).
pub fn mint_on_connect(dbname: &str) -> PgResult<bool> {
    // Disarm checks first, cheapest first: with the feature off this path
    // costs two thread-local GUC reads after the (already-taken) lookup
    // miss — the postinit parity budget.
    let prefix = crate::ephemeral_db_prefix();
    if prefix.is_empty() {
        return Ok(false);
    }
    let roles_guc = crate::ephemeral_db_mint_roles();
    if roles_guc.is_empty() {
        return Ok(false);
    }
    // Client backends only: bgworkers (the janitor itself connects by name)
    // and walsenders ride the same in_dbname path and must never mint.
    if miscinit::GetMyBackendType() != types_core::init::BackendType::Backend {
        return Ok(false);
    }
    let Some(shape) = grammar::parse_mint_name(&prefix, dbname) else {
        return Ok(false);
    };
    let template = match shape {
        MintShape::Template { template, .. } => template.to_string(),
        MintShape::Bare { .. } => {
            let t = crate::ephemeral_db_default_template();
            if t.is_empty() {
                // Spec: '' = bare tokens refuse to mint (stock FATAL).
                return Ok(false);
            }
            t
        }
    };

    let role_oid = miscinit::GetUserId();
    let cx = mcx::MemoryContext::new("pgrust mint-on-connect");
    let mcx = cx.mcx();
    // The authenticated session's role must resolve; a concurrent DROP ROLE
    // is a legitimate miss — treat as unlisted (stock FATAL).
    let Some(role_name) = miscinit::GetUserNameFromId(mcx, role_oid, true)? else {
        return Ok(false);
    };
    if !role_qualifies(mcx, &roles_guc, role_name.as_str())? {
        return Ok(false);
    }

    // Per-role cap. Counting semantics (documented here, referenced by the
    // GUC's long_desc): "live" = pg_database rows matching the prefix with
    // datistemplate = false and datdba = the connecting role — regardless
    // of provenance (a manually created prefix-owned database counts;
    // catalog rows are the self-healing ground truth) — plus, inside
    // post_ensure under the registry lock, the role's other in-flight
    // mints: pending entries AND resolved-Done lingering entries whose
    // names this scan did not see (a same-role mint committing between
    // this scan and the post is otherwise in neither term — the
    // scan-to-post TOCTOU). The scan hands post_ensure the NAMES so it can
    // tell the two Done cases apart. Residual, bounded and accepted: a
    // post delayed past ENSURE_LINGER_NS after such a commit misses both
    // terms (M3 addendum item 8).
    let max_per_role = crate::ephemeral_db_max_per_role();
    let live_owned = if max_per_role > 0 {
        live_owned_names(&prefix, role_oid)?
    } else {
        Vec::new()
    };

    let my_procno: ProcNumber = lmgr_proc::MyProc().expect("mint_on_connect before InitProcess");
    match registry::post_ensure(
        dbname,
        &template,
        role_name.as_str(),
        role_oid,
        my_procno,
        &live_owned,
        max_per_role,
    ) {
        // The hint must cover BOTH absent states: the boot window (clients
        // can authenticate before the janitor bgworker's
        // BackgroundWorkerInitializeConnection reaches set_janitor_proc —
        // nothing is wrong, retry succeeds) and disabled-after-error
        // (BGW_NEVER_RESTART). 57P03 is the retryable class either way;
        // claiming "disabled until restart" during normal startup would
        // tell a retry-capable harness to abort.
        PostEnsure::JanitorAbsent => Err(ereport(FATAL)
            .errcode(ERRCODE_CANNOT_CONNECT_NOW)
            .errmsg(format!(
                "cannot mint ephemeral database \"{dbname}\": the pgrust ephemeral-db janitor \
                 is not running"
            ))
            .errhint(
                "The janitor may still be starting up, or it has been disabled after an \
                 unrecoverable error (see the server log); retrying shortly is safe."
                    .to_string(),
            )
            .into_error()
            .into()),
        PostEnsure::JanitorPaused => Err(ereport(FATAL)
            .errcode(ERRCODE_CANNOT_CONNECT_NOW)
            .errmsg(format!(
                "cannot mint ephemeral database \"{dbname}\": the pgrust ephemeral-db janitor \
                 is paused by the adoption guard"
            ))
            .errhint(
                "Run SELECT pgrust_janitor_unpause(); (superuser) to acknowledge the \
                 configured prefix."
                    .to_string(),
            )
            .into_error()
            .into()),
        PostEnsure::PerRoleCap { counted, max } => Err(ereport(FATAL)
            .errcode(ERRCODE_CONFIGURATION_LIMIT_EXCEEDED)
            .errmsg(format!(
                "cannot mint ephemeral database \"{dbname}\": role \"{}\" already holds \
                 {counted} live ephemeral database(s) (pgrust.ephemeral_db_max_per_role = {max})",
                role_name.as_str()
            ))
            .errhint(
                "Disconnect from (and let the janitor reap) existing ephemeral databases, or \
                 raise pgrust.ephemeral_db_max_per_role."
                    .to_string(),
            )
            .into_error()
            .into()),
        // Invariant violation, not load: the table is sized from
        // max_connections (registry::ensure_capacity — every concurrent
        // connecting backend fits, plus slack for waiter-less stragglers),
        // so filling it means the janitor leaked entries. Kept as a clean
        // FATAL rather than a panic because a connect attempt is the wrong
        // process to crash for a janitor-side accounting bug; the errcode
        // says "server defect", not "raise a knob".
        PostEnsure::TableFull { cap } => Err(ereport(FATAL)
            .errcode(ERRCODE_INTERNAL_ERROR)
            .errmsg(format!(
                "cannot mint ephemeral database \"{dbname}\": the mint request table is full \
                 ({cap} entries)"
            ))
            .errdetail(
                "The table is sized from max_connections so every connecting backend fits; \
                 overflowing it indicates a janitor defect (leaked mint entries), not load."
                    .to_string(),
            )
            .into_error()
            .into()),
        PostEnsure::Posted(gen) | PostEnsure::Joined(gen) => {
            // Mint latency must not be tick-bound.
            registry::wake_janitor();
            wait_for_mint(dbname, gen, my_procno)
        }
    }
}

/// Match the connecting role against the mint_roles list.
/// SplitIdentifierString conventions (the identifier-list law shared with
/// the check hook): unquoted entries downcase, quoted entries stay exact.
/// The `$createdb` sentinel admits any role with the CREATEDB attribute
/// (`have_createdb_privilege`: superuser or rolcreatedb — callable here,
/// the startup transaction is open and the syscache is live). A malformed
/// list matches nothing (the check hook refuses new ones; a pre-existing
/// bad value must fail closed).
///
/// `$createdb` is RESERVED: split_identifier_string strips quotes before
/// returning entries, so a quoted `"$createdb"` is indistinguishable here
/// and still selects the sentinel — a role literally named `$createdb`
/// (creatable only via quoting) can never be allowlisted by name. This
/// deliberately diverges from pg_hba's quoting-demotes-keywords
/// convention; documented on the GUC's long_desc.
fn role_qualifies(mcx: Mcx<'_>, roles_guc: &str, role_name: &str) -> PgResult<bool> {
    let Some(entries) =
        varlena::split_identifier_string(mcx, roles_guc, b',', mbutils::GetDatabaseEncoding())?
    else {
        return Ok(false);
    };
    for entry in &entries {
        if entry == "$createdb" {
            if dbcommands::have_createdb_privilege()? {
                return Ok(true);
            }
        } else if entry == role_name {
            return Ok(true);
        }
    }
    Ok(false)
}

/// The live-databases half of the cap count (semantics documented at the
/// call site above). Returns the owned NAMES, not a count: post_ensure
/// needs them to decide which resolved-Done entries this scan already saw
/// (the scan-to-post window closure). Runs inside the CURRENT (startup)
/// transaction.
fn live_owned_names(prefix: &str, role: Oid) -> PgResult<Vec<String>> {
    let rows = crate::dbscan::scan_prefix_rows(prefix)?;
    Ok(rows
        .iter()
        .filter(|r| !r.istemplate && r.datdba == role)
        .map(|r| r.name.clone())
        .collect())
}

/// Park on our own (shared proc) latch until the janitor resolves the
/// Ensure. Every exit path — success, janitor failure, timeout, and a
/// CHECK_FOR_INTERRUPTS die (clean FATAL 57P01) — deregisters the waiter
/// via the drop guard. Deregistration bounds waiter-list staleness to
/// FUTURE snapshots only: complete_ensure snapshots the waiters under the
/// registry lock and the janitor SetLatches them OUTSIDE it, so a waiter
/// departing after the snapshot can still receive one wake on its old
/// ProcNumber — possibly landing on a backend that reclaimed the slot.
/// That cross-wake is benign BY CONTRACT: proc-latch wakes are
/// spurious-set tolerant (the WAIT_TICK rationale above) and every waiter
/// re-polls entry state after waking. Do not replace the latch with a
/// non-spurious-tolerant wake channel without also re-reading waiters
/// under the lock at wake time.
fn wait_for_mint(dbname: &str, gen: u64, procno: ProcNumber) -> PgResult<bool> {
    struct WaiterGuard {
        gen: u64,
        procno: ProcNumber,
    }
    impl Drop for WaiterGuard {
        fn drop(&mut self) {
            registry::remove_ensure_waiter(self.gen, self.procno);
        }
    }
    let _guard = WaiterGuard { gen, procno };

    let deadline = pg_clock::mono_ns() + MINT_WAIT_TIMEOUT_NS;
    loop {
        match registry::ensure_status(gen) {
            EnsureStatus::Done => return Ok(true),
            EnsureStatus::Failed(e) => {
                // The janitor's saved errdata, fanned out: same sqlstate and
                // message on every waiter, re-leveled to FATAL (it kills
                // this connection attempt, not the janitor).
                let mut b = ereport(FATAL).errcode(e.sqlstate()).errmsg(format!(
                    "ephemeral database \"{dbname}\" could not be minted: {}",
                    e.message()
                ));
                if let Some(d) = e.detail() {
                    b = b.errdetail(d.to_string());
                }
                if let Some(h) = e.hint() {
                    b = b.errhint(h.to_string());
                }
                return Err(b.into_error().into());
            }
            // Gone while we hold a waiter registration = the entry was
            // retired out from under us (a bug, or a gc racing a stale gen).
            // Fail closed rather than park forever.
            EnsureStatus::Gone => {
                return Err(ereport(FATAL)
                    .errcode(ERRCODE_CANNOT_CONNECT_NOW)
                    .errmsg(format!(
                        "mint request for ephemeral database \"{dbname}\" was retired \
                         before completion"
                    ))
                    .into_error()
                    .into())
            }
            EnsureStatus::Pending => {}
        }

        // Belt-and-suspenders for a janitor that died between our post and
        // its exit drain (the drain fails every pending entry, but a wedge
        // in between must not cost us the full deadline), and for a pause
        // landing mid-wait.
        if !registry::janitor_present() || registry::is_paused() {
            return Err(ereport(FATAL)
                .errcode(ERRCODE_CANNOT_CONNECT_NOW)
                .errmsg(format!(
                    "cannot mint ephemeral database \"{dbname}\": the pgrust ephemeral-db \
                     janitor went away while the request was pending"
                ))
                .into_error()
                .into());
        }

        if pg_clock::mono_ns() >= deadline {
            return Err(ereport(FATAL)
                .errcode(ERRCODE_CANNOT_CONNECT_NOW)
                .errmsg(format!(
                    "timed out waiting for ephemeral database \"{dbname}\" to be minted \
                     ({}s)",
                    MINT_WAIT_TIMEOUT_NS / 1_000_000_000
                ))
                .errhint(
                    "The janitor serializes mints; a long queue or a slow checkpoint can \
                     exceed the wait budget. Check the server log."
                        .to_string(),
                )
                .into_error()
                .into());
        }

        let rc = latch::WaitLatch(
            g::MyLatch(),
            WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
            WAIT_TICK_MS,
            PG_WAIT_EXTENSION,
        )?;
        if rc & WL_LATCH_SET != 0 {
            if let Some(l) = g::MyLatch() {
                latch::ResetLatch(l);
            }
        }
        // Every wake, latch or timeout: ProcDiePending becomes the clean
        // FATAL 57P01 ("terminating connection due to administrator
        // command") out of InitPostgres — the wanted abort discipline; the
        // drop guard deregisters us on the way out.
        postgres_seams::check_for_interrupts::call()?;
    }
}

/// GUC check hook for pgrust.ephemeral_db_mint_roles: list SYNTAX only
/// (the createrole_self_grant shape). Role existence is deliberately not
/// checked — roles are resolved at mint time, and a listed-but-missing role
/// simply never matches (the pg_hba convention), so a reload can never fail
/// on a dropped role.
pub fn check_ephemeral_db_mint_roles(
    newval: &mut Option<String>,
    _extra: &mut Option<guc_tables::GucHookExtra>,
    _source: GucSource,
) -> PgResult<bool> {
    let value = newval.clone().unwrap_or_default();
    let ctx = mcx::MemoryContext::new("check_pgrust_ephemeral_db_mint_roles");
    if varlena::split_identifier_string(ctx.mcx(), &value, b',', mbutils::GetDatabaseEncoding())?
        .is_none()
    {
        guc::GUC_check_errdetail("List syntax is invalid.");
        return Ok(false);
    }
    Ok(true)
}

// ---------------------------------------------------------------------------
// Janitor side.
// ---------------------------------------------------------------------------

/// Batch ceiling per janitor cycle (docs/design/test-views.md batched-mint
/// addendum): bounds the single batch transaction's lock footprint and the
/// crash-orphan window. Excess entries simply stay Pending for the next
/// tick — the pass re-arms the janitor's own latch so the follow-up tick is
/// immediate, and waiters carry the 60s deadline regardless.
pub(crate) const MINT_BATCH_MAX: usize = 32;

/// One mint service pass, run from the janitor tick BEFORE the reap pass.
/// Ordering rationale (recorded decision): mint-before-reap makes waiter
/// latency one tick at worst and lets a mint racing a same-name reap
/// resolve in mint's favor within the tick; the same tick's reap pass
/// cannot victimize the fresh mint because (a) the Ensure entry shields the
/// name (`registry::ensure_shields`, consulted next to pins) while pending
/// and through the post-completion linger, and (b) reaping additionally
/// requires a full observed-idle grace streak.
///
/// Batched minting (the dropdb_skip_checkpoint precedent applied to
/// creates): N pending Ensures in one cycle share ONE checkpoint pair
/// instead of N pairs — a preflight pass fails misconfigured entries
/// individually and routes entries whose template is CONNECTABLE
/// (datallowconn = true) to the serial path (the batch-eligibility law on
/// `PreflightVerdict::Mint`: only templates ordinary connections cannot
/// reach may share the batch's widened torn-copy window), then every
/// batch-eligible entry runs `dbcommands::createdb_skip_checkpoints`
/// inside a SINGLE janitor transaction bracketed by one FLUSH_ALL
/// pre-checkpoint and one post-checkpoint (inside the transaction, after
/// the last copy), THEN the one commit, THEN the waiters wake. Any error
/// inside the batch transaction aborts the WHOLE batch, and the pass falls
/// back to the serial per-mint path for the same entries this same tick —
/// one bad entry degrades throughput, never the others' minting; an error
/// out of the preflight PROBE itself is contained and falls back to the
/// serial path the same way, so a persistent infrastructure failure
/// resolves every entry per-entry instead of wedging the queue Pending.
/// A single-entry tick takes the serial path directly: identical
/// checkpoint cost (one pair either way), zero new machinery on the
/// low-rate path.
///
/// Per-entry errors are contained (main_loop::contain choreography) and the
/// SAVED error is fanned out to every waiter — a failed CREATE must fail
/// the waiters, never the janitor. FATAL-class errors propagate; the
/// janitor's exit drain (main_loop's ClearProc) then fails whatever is
/// still pending.
pub(crate) fn service_pass() -> PgResult<()> {
    let pending = registry::pending_ensures();
    if pending.is_empty() {
        return Ok(());
    }
    let (batch, deferred) = split_batch(pending, MINT_BATCH_MAX);
    if deferred > 0 {
        let _ = log_report(
            LOG,
            format!(
                "pgrust ephemeral-db janitor: deferring {deferred} pending mint request(s) to \
                 the next tick (batch cap {MINT_BATCH_MAX})"
            ),
        );
        // Deferral must cost one loop turn, not a full 500ms tick: the
        // janitor sets its own latch (wake_janitor targets janitor_proc,
        // which is us).
        registry::wake_janitor();
    }
    if batch.len() == 1 {
        return service_serial(&batch);
    }

    // Preflight (its own transaction, probes only): fail each misconfigured
    // entry ALONE — template missing/unsealed, or the name already exists
    // (idempotent success) — so per-entry misconfiguration never poisons
    // the batch transaction below. A probe error is infrastructure, not
    // per-entry state — but it must NOT strand the batch: mirroring the
    // batch-abort fallback below, the error is contained (FATAL-class
    // still propagates to the exit drain) and the SAME entries run through
    // the serial path, whose per-entry containment resolves every one. A
    // TRANSIENT error costs one degraded (per-entry-checkpointed) cycle; a
    // PERSISTENT one fans the saved error out to the waiters instead of
    // wedging the entries Pending forever — where waiters would degrade
    // from a prompt saved-error to 60s-deadline FATALs and the retrying
    // entries would accumulate toward ensure_capacity() TableFull.
    let verdicts = match preflight_probe(&batch) {
        Ok(v) => v,
        Err(e) => {
            crate::main_loop::contain(e, "mint preflight probe")?;
            return service_serial(&batch);
        }
    };
    let now = pg_clock::mono_ns();
    let mut to_batch: Vec<registry::PendingEnsure> = Vec::new();
    let mut to_serial: Vec<registry::PendingEnsure> = Vec::new();
    for (p, v) in batch.into_iter().zip(verdicts) {
        match v {
            PreflightVerdict::Mint => to_batch.push(p),
            PreflightVerdict::MintSerial => to_serial.push(p),
            PreflightVerdict::AlreadyExists => {
                let waiters = registry::complete_ensure(p.gen, Ok(()), now);
                log_mint_success(false, &p, waiters.len());
                wake_waiters(&waiters);
            }
            PreflightVerdict::TemplateMissing | PreflightVerdict::TemplateUnsealed => {
                let e = match v {
                    PreflightVerdict::TemplateMissing => template_missing_error(&p.template),
                    _ => template_unsealed_error(&p.template, &p.name),
                };
                report_contained_refusal(&e, &p.name);
                let waiters = registry::complete_ensure(p.gen, Err(e), now);
                wake_waiters(&waiters);
            }
        }
    }

    // A lone batch-eligible entry gains nothing from the batch machinery
    // (identical checkpoint cost serially — the single-entry rationale).
    if to_batch.len() < 2 {
        to_serial.append(&mut to_batch);
    }
    if to_batch.len() >= 2 {
        service_batch(&to_batch)?;
    }
    if to_serial.is_empty() {
        return Ok(());
    }
    service_serial(&to_serial)
}

/// The serial per-mint path: each entry in its own transaction with its own
/// checkpoint pair (`dbcommands::createdb`, C-shaped). This is both the
/// single-entry fast path and the whole-batch-abort fallback.
fn service_serial(entries: &[registry::PendingEnsure]) -> PgResult<()> {
    for p in entries {
        let outcome = match mint_one(p) {
            Ok(minted) => {
                let waiters = registry::complete_ensure(p.gen, Ok(()), pg_clock::mono_ns());
                log_mint_success(minted, p, waiters.len());
                waiters
            }
            Err(e) => {
                let saved: Box<PgError> = Box::new((*e).clone());
                // contain() aborts the failed transaction, reports, and
                // propagates FATAL-class untouched (in which case the entry
                // stays Pending for the exit drain).
                crate::main_loop::contain(
                    e,
                    &format!("minting ephemeral database \"{}\"", p.name),
                )?;
                registry::complete_ensure(p.gen, Err(saved), pg_clock::mono_ns())
            }
        };
        wake_waiters(&outcome);
    }
    Ok(())
}

/// The batch fast path over >= 2 validated entries. Completion and waking
/// run strictly AFTER the batch commit (as the serial path wakes after its
/// per-entry commit); on batch failure the transaction is aborted, orphaned
/// batch datadirs are removed (pre-commit failures only — see
/// `cleanup_orphaned_datadirs`), and the SAME entries retry serially this
/// same tick.
fn service_batch(to_mint: &[registry::PendingEnsure]) -> PgResult<()> {
    let mut created: Vec<Oid> = Vec::new();
    match mint_batch(to_mint, &mut created) {
        Ok(outcomes) => {
            let now = pg_clock::mono_ns();
            // The batch witness line (race-suite storm phase), BEFORE any
            // completion/wake: a woken waiter finishes its connect fast
            // enough that the gate may snapshot the log the moment the last
            // client returns — every line the gate accounts must already be
            // written by then. The commit above already happened, so the
            // line is truthful at this point.
            let n_minted = outcomes
                .iter()
                .filter(|o| matches!(o, BatchOutcome::Minted { .. }))
                .count();
            if n_minted > 0 {
                let _ = log_report(
                    LOG,
                    format!(
                        "pgrust ephemeral-db janitor: batch-minted {n_minted} ephemeral \
                         database(s) in one transaction with one checkpoint pair"
                    ),
                );
            }
            let mut deferred_serial: Vec<registry::PendingEnsure> = Vec::new();
            for (p, o) in to_mint.iter().zip(outcomes) {
                match o {
                    BatchOutcome::Minted { copied } => {
                        if copied > 0 {
                            let _ = log_report(
                                LOG,
                                format!(
                                    "pgrust ephemeral-db janitor: copied {copied} \
                                     database-setting row(s) from template \"{}\" to ephemeral \
                                     database \"{}\"",
                                    p.template, p.name
                                ),
                            );
                        }
                        let waiters = registry::complete_ensure(p.gen, Ok(()), now);
                        log_mint_success(true, p, waiters.len());
                        wake_waiters(&waiters);
                    }
                    BatchOutcome::FoundExisting => {
                        let waiters = registry::complete_ensure(p.gen, Ok(()), now);
                        log_mint_success(false, p, waiters.len());
                        wake_waiters(&waiters);
                    }
                    BatchOutcome::Refused(e) => {
                        report_contained_refusal(&e, &p.name);
                        let waiters = registry::complete_ensure(p.gen, Err(e), now);
                        wake_waiters(&waiters);
                    }
                    // Not completed: the entry stayed Pending through the
                    // batch (it did no work in it) and mints serially now.
                    BatchOutcome::DeferSerial => deferred_serial.push(registry::PendingEnsure {
                        gen: p.gen,
                        name: p.name.clone(),
                        template: p.template.clone(),
                        owner_name: p.owner_name.clone(),
                    }),
                }
            }
            if deferred_serial.is_empty() {
                Ok(())
            } else {
                service_serial(&deferred_serial)
            }
        }
        Err(failure) => {
            let (e, pre_commit) = match failure {
                BatchFailure::BeforeCommit(e) => (e, true),
                BatchFailure::AtCommit(e) => (e, false),
            };
            // contain() aborts the batch transaction, reports, and
            // propagates FATAL-class untouched (entries stay Pending for
            // the exit drain).
            crate::main_loop::contain(e, "batch mint transaction")?;
            if pre_commit {
                cleanup_orphaned_datadirs(&created);
            }
            let _ = log_report(
                LOG,
                format!(
                    "pgrust ephemeral-db janitor: mint batch aborted; retrying {} request(s) \
                     serially this tick",
                    to_mint.len()
                ),
            );
            service_serial(to_mint)
        }
    }
}

/// Reject every pending Ensure against a paused janitor (the loop's paused
/// branch): entries can be queued between `set_janitor_proc` and the
/// adoption guard's pause decision — or a whole tick can pass before the
/// drain — and a queue nothing will service must fail loudly, never hang
/// (spec item 4). The backend post path refuses NEW Ensures while paused;
/// this drains the window's stragglers. The drain is CONDITIONAL —
/// paused-ness is re-checked under the registry lock — because
/// `pgrust_janitor_unpause()` can land between the tick's is_paused()
/// observation and this call, and a fresh Ensure admitted after the flip
/// must not be failed with a wrong-cause "paused" FATAL
/// (registry::fail_pending_ensures_if_paused's rationale). Skipping never
/// hangs anyone: once unpaused, the next tick's service pass runs.
pub(crate) fn reject_pending_paused() {
    let cause = ereport(ERROR)
        .errcode(ERRCODE_CANNOT_CONNECT_NOW)
        .errmsg("the pgrust ephemeral-db janitor is paused by the adoption guard".to_string())
        .errhint(
            "Run SELECT pgrust_janitor_unpause(); (superuser) to acknowledge the configured \
             prefix."
                .to_string(),
        )
        .into_error();
    let waiters = registry::fail_pending_ensures_if_paused(&cause, pg_clock::mono_ns());
    for w in waiters {
        latch::SetLatch(types_storage::latch::LatchHandle::proc(w));
    }
}

/// Fail every pending Ensure with `cause` and wake the waiters —
/// UNCONDITIONALLY (main_loop's janitor-exit drain: nothing will ever
/// service the queue again, paused or not). The paused drain above uses
/// the paused-rechecking variant instead.
pub(crate) fn fail_pending_and_wake(cause: &PgError) {
    let waiters = registry::fail_pending_ensures(cause, pg_clock::mono_ns());
    for w in waiters {
        latch::SetLatch(types_storage::latch::LatchHandle::proc(w));
    }
}

// ---------------------------------------------------------------------------
// Batch assembly + preflight (pure halves unit-tested below).
// ---------------------------------------------------------------------------

/// Take at most `cap` entries off the front of the pending snapshot (oldest
/// first — registry insertion order); the rest wait for the next tick.
/// Returns (batch, deferred_count). Pure: unit-tested.
fn split_batch(
    mut pending: Vec<registry::PendingEnsure>,
    cap: usize,
) -> (Vec<registry::PendingEnsure>, usize) {
    let deferred = pending.len().saturating_sub(cap);
    pending.truncate(cap);
    (pending, deferred)
}

/// Per-entry preflight classification. Precedence mirrors `mint_one`'s
/// check order: name-exists wins (idempotent success even when the template
/// has meanwhile vanished — the database IS there, which is all the waiter
/// asked for), then template resolution, then sealing. Pure: unit-tested.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum PreflightVerdict {
    /// The name already exists: instant success to all waiters.
    AlreadyExists,
    /// The template does not resolve: fail this entry alone.
    TemplateMissing,
    /// The template is not sealed (datistemplate = false): fail this entry
    /// alone (the superuser-janitor clone guard, M3 addendum item 7).
    TemplateUnsealed,
    /// Validated AND batch-eligible: sealed with datallowconn = false. The
    /// batch widens FILE_COPY's connect-dirty-disconnect torn-copy window
    /// from one copy to one batch, and a writer that DISCONNECTS before a
    /// member's CountOtherDBBackends check is invisible to it — so only
    /// templates ordinary connections cannot reach at all may share a
    /// batch (createdb_skip_checkpoints rationale item 1).
    Mint,
    /// Validated but the template is CONNECTABLE (datallowconn = true, the
    /// template1 shape): mint on the SERIAL path, whose window is one copy
    /// wide and immediately adjacent — stock C's own residual, no wider.
    MintSerial,
}

fn preflight_verdict(
    name_exists: bool,
    // (datistemplate, datallowconn); None = template row missing.
    template: Option<(bool, bool)>,
) -> PreflightVerdict {
    if name_exists {
        return PreflightVerdict::AlreadyExists;
    }
    match template {
        None => PreflightVerdict::TemplateMissing,
        Some((false, _)) => PreflightVerdict::TemplateUnsealed,
        Some((true, true)) => PreflightVerdict::MintSerial,
        Some((true, false)) => PreflightVerdict::Mint,
    }
}

/// The catalog-probe half of the preflight: one read-only transaction, two
/// name lookups per entry, classification via the pure verdict above.
fn preflight_probe(batch: &[registry::PendingEnsure]) -> PgResult<Vec<PreflightVerdict>> {
    let cx = mcx::MemoryContext::new("pgrust janitor mint preflight");
    xact::StartTransactionCommand()?;
    let mcx = cx.mcx();
    let mut verdicts = Vec::with_capacity(batch.len());
    for p in batch {
        let name_exists = pg_database::get_database_tuple_by_name(mcx, &p.name)?.is_some();
        let template_state = pg_database::get_database_tuple_by_name(mcx, &p.template)?
            .map(|t| (t.datistemplate, t.datallowconn));
        verdicts.push(preflight_verdict(name_exists, template_state));
    }
    xact::CommitTransactionCommand()?;
    Ok(verdicts)
}

// ---------------------------------------------------------------------------
// The batch transaction.
// ---------------------------------------------------------------------------

/// Per-entry result inside a successful batch transaction.
enum BatchOutcome {
    /// createdb ran for this entry (`copied` = pg_db_role_setting rows).
    Minted { copied: usize },
    /// The name appeared between preflight and the batch transaction (the
    /// manual-CREATE residual, M3 addendum item 10): idempotent success.
    FoundExisting,
    /// The template vanished/unsealed between preflight and the batch
    /// transaction. Recorded BEFORE any transactional work for this entry,
    /// so it fails alone without poisoning the batch.
    Refused(Box<PgError>),
    /// The template turned CONNECTABLE (datallowconn = true) between
    /// preflight and the batch re-check: this entry did no transactional
    /// work in the batch and stays Pending; the caller mints it on the
    /// serial path after the batch commits (the batch-eligibility law on
    /// `PreflightVerdict::Mint`, enforced at the batch's own snapshot too).
    DeferSerial,
}

/// Batch failure, split on the commit boundary: pre-commit failures abort
/// every member and their copied datadirs are safe to remove (no catalog
/// row ever became visible); an error out of the commit itself is ambiguous
/// (the record may have made it durable), so the caller must NOT remove
/// files — the serial retry's exists-check resolves the ambiguity.
enum BatchFailure {
    BeforeCommit(Box<PgError>),
    AtCommit(Box<PgError>),
}

/// Run the whole batch in ONE transaction: one FLUSH_ALL pre-checkpoint,
/// N x `createdb_skip_checkpoints` (+ the M4 setting copy, same as the
/// serial path), one post-checkpoint after the last copy, then the commit.
/// This preserves C's FILE_COPY crash invariant for every member —
/// committed create implies a checkpoint ran after its copy — because the
/// single commit is preceded by the single post-checkpoint (the safety
/// analysis lives on `dbcommands::createdb_skip_checkpoints`).
///
/// `created` collects the dst oids of every completed createdb as we go, so
/// the caller can remove orphaned datadirs when a later member (or the
/// post-checkpoint) fails and the whole transaction aborts.
fn mint_batch(
    batch: &[registry::PendingEnsure],
    created: &mut Vec<Oid>,
) -> Result<Vec<BatchOutcome>, BatchFailure> {
    let cx = mcx::MemoryContext::new("pgrust janitor batch mint");
    if let Err(e) = xact::StartTransactionCommand() {
        return Err(BatchFailure::BeforeCommit(e));
    }
    match mint_batch_body(cx.mcx(), batch, created) {
        Ok(outcomes) => match xact::CommitTransactionCommand() {
            Ok(()) => Ok(outcomes),
            Err(e) => Err(BatchFailure::AtCommit(e)),
        },
        Err(e) => Err(BatchFailure::BeforeCommit(e)),
    }
}

fn mint_batch_body(
    mcx: Mcx<'_>,
    batch: &[registry::PendingEnsure],
    created: &mut Vec<Oid>,
) -> PgResult<Vec<BatchOutcome>> {
    use transam_xlog::{
        CHECKPOINT_FLUSH_ALL, CHECKPOINT_FORCE, CHECKPOINT_IMMEDIATE, CHECKPOINT_WAIT,
    };

    // ONE pre-checkpoint for the whole batch — the exact flags createdb's
    // own FILE_COPY pre-checkpoint uses (the caller contract on
    // `createdb_skip_checkpoints`) — requested LAZILY, before the FIRST
    // member that actually copies: a batch whose every member resolves
    // without a copy (all FoundExisting/Refused/DeferSerial) then requests
    // no checkpoints at all, so the storm gate's flush-all bookkeeping
    // stays exact (flush-all lines == cycles that created something) and
    // no synchronous checkpoint stall is paid for nothing.
    let mut pre_checkpointed = false;

    let mut outcomes = Vec::with_capacity(batch.len());
    for p in batch {
        // Re-checks under the batch transaction (preflight ran in an
        // earlier transaction; the windows are the manual-CREATE /
        // concurrent-ALTER residuals). A refusal here has done no
        // transactional work for this entry yet, so it is a per-entry
        // outcome, not a batch abort.
        if pg_database::get_database_tuple_by_name(mcx, &p.name)?.is_some() {
            outcomes.push(BatchOutcome::FoundExisting);
            continue;
        }
        let tpl = match pg_database::get_database_tuple_by_name(mcx, &p.template)? {
            None => {
                outcomes.push(BatchOutcome::Refused(template_missing_error(&p.template)));
                continue;
            }
            Some(t) if !t.datistemplate => {
                outcomes.push(BatchOutcome::Refused(template_unsealed_error(
                    &p.template,
                    &p.name,
                )));
                continue;
            }
            // Batch-eligibility re-check: preflight admitted only
            // datallowconn=false templates, but an ALTER DATABASE ...
            // ALLOW_CONNECTIONS true can land between the two
            // transactions. Deferring to the serial path keeps the batch's
            // widened torn-copy window UNREACHABLE by ordinary connections
            // (provably, at the batch's own snapshot) instead of
            // convention-guarded.
            Some(t) if t.datallowconn => {
                outcomes.push(BatchOutcome::DeferSerial);
                continue;
            }
            Some(t) => t,
        };
        if !pre_checkpointed {
            checkpointer::RequestCheckpoint(
                CHECKPOINT_IMMEDIATE | CHECKPOINT_FORCE | CHECKPOINT_WAIT | CHECKPOINT_FLUSH_ALL,
            )?;
            pre_checkpointed = true;
        }
        let stmt = build_createdb_stmt(mcx, &p.name, &p.template, &p.owner_name)?;
        let db_oid = dbcommands::createdb_skip_checkpoints(mcx, &stmt)?;
        created.push(db_oid);
        let copied = pg_db_role_setting::copy_database_settings(mcx, tpl.oid, db_oid)?;
        outcomes.push(BatchOutcome::Minted { copied });
        // Multi-statement-transaction shape: make this member's catalog
        // rows command-visible before the next member's scans (not strictly
        // required — member names are registry-unique and templates are
        // pre-committed; GetNewOidWithIndex scans SnapshotAny — but it is
        // the conservative utility-statement convention).
        xact::CommandCounterIncrement()?;
    }

    if !created.is_empty() {
        // ONE post-checkpoint INSIDE the transaction, after the last copy
        // and before the single commit (skipped when nothing was copied —
        // no member owes the invariant then).
        checkpointer::RequestCheckpoint(CHECKPOINT_IMMEDIATE | CHECKPOINT_FORCE | CHECKPOINT_WAIT)?;
    }
    Ok(outcomes)
}

/// Best-effort removal of datadirs copied by an ABORTED batch transaction:
/// the catalog rows never became visible, so the files are unreferencable
/// orphans — the same shape `createdb_failure_cleanup` handles for a
/// single failed copy (`remove_dbtablespaces`: rmtree + one XLOG_DBASE_DROP
/// record), run in a fresh transaction because the batch transaction is
/// already gone. A failure here (or a crash before/mid-cleanup) leaves the
/// orphans on disk — the documented residual of the batch design (same
/// class as C createdb's own abort-after-copy window; the boot sweep is
/// catalog-driven and will not see them). A crash AFTER a successful
/// cleanup cannot resurrect them: the DROP records are XLogFlush'd before
/// the commit (the flush comment in the body), so recovery never replays
/// the aborted batch's durable CREATE_FILE_COPY records without the DROPs
/// that follow them. Never fails the pass.
fn cleanup_orphaned_datadirs(created: &[Oid]) {
    if created.is_empty() {
        return;
    }
    let run = || -> PgResult<()> {
        let cx = mcx::MemoryContext::new("pgrust janitor batch-abort cleanup");
        xact::StartTransactionCommand()?;
        for &oid in created {
            dbcommands::remove_dbtablespaces(cx.mcx(), oid)?;
        }
        // The XLOG_DBASE_DROP records must be DURABLE before cleanup counts
        // as done: this transaction changes no catalog, so its commit
        // assigns no xid, writes no commit record, and flushes nothing on
        // its own — while the aborted batch's XLOG_DBASE_CREATE_FILE_COPY
        // records may already be flushed (walwriter, or the serial retry's
        // ForceSyncCommit). A crash after an apparently successful cleanup
        // would then replay CREATE without DROP and resurrect the just-
        // removed orphans. Flush the tail explicitly (dropdb's
        // set_database_invalid shape); a no-op when nothing was inserted.
        transam_xlog::write::XLogFlush(transam_xlog::XactLastRecEnd())?;
        xact::CommitTransactionCommand()?;
        Ok(())
    };
    if let Err(e) = run() {
        let _ = xact::AbortOutOfAnyTransaction();
        let _ = log_report(
            LOG,
            format!(
                "pgrust ephemeral-db janitor: could not remove {} orphaned datadir(s) after a \
                 mint-batch abort (harmless but wasteful; remove base/<oid> manually): {}",
                created.len(),
                e.message()
            ),
        );
    }
}

// ---------------------------------------------------------------------------
// Shared helpers (serial + batch paths).
// ---------------------------------------------------------------------------

fn wake_waiters(waiters: &[ProcNumber]) {
    for &w in waiters {
        latch::SetLatch(types_storage::latch::LatchHandle::proc(w));
    }
}

/// The per-entry success line, IDENTICAL between the serial and batch paths
/// (the race suite's `minted_count` greps it per database name).
fn log_mint_success(minted: bool, p: &registry::PendingEnsure, n_waiters: usize) {
    let _ = log_report(
        LOG,
        format!(
            "pgrust ephemeral-db janitor: {} ephemeral database \"{}\" from \
             template \"{}\" for role \"{}\" ({} waiter(s))",
            if minted { "minted" } else { "found existing" },
            p.name,
            p.template,
            p.owner_name,
            n_waiters
        ),
    );
}

/// Report a per-entry refusal with the containment choreography's exact log
/// convention (main_loop::contain minus the transaction abort — refusals
/// are decided outside, or before any work inside, a live transaction), so
/// the gates' contained-failure audits count batch-path refusals the same
/// way they count serial ones.
fn report_contained_refusal(e: &PgError, name: &str) {
    g::HoldInterrupts();
    elog::emit_error_report_for(e);
    let _ = log_report(
        LOG,
        format!(
            "pgrust ephemeral-db janitor: minting ephemeral database \"{name}\" failed \
             (see above); continuing"
        ),
    );
    elog::FlushErrorState();
    g::ResumeInterrupts();
}

/// The two per-entry refusal shapes, shared verbatim by `mint_one` and the
/// preflight/batch paths so waiters see byte-identical errors on either
/// path.
fn template_missing_error(template: &str) -> Box<PgError> {
    ereport(ERROR)
        .errcode(ERRCODE_UNDEFINED_DATABASE)
        .errmsg(format!("template database \"{template}\" does not exist"))
        .into_error()
        .into()
}

fn template_unsealed_error(template: &str, name: &str) -> Box<PgError> {
    ereport(ERROR)
        .errcode(ERRCODE_WRONG_OBJECT_TYPE)
        .errmsg(format!(
            "database \"{template}\" is not a template; refusing to mint \"{name}\" from it"
        ))
        .errhint(
            "Seal it first: ALTER DATABASE ... WITH IS_TEMPLATE true ALLOW_CONNECTIONS \
             false."
                .to_string(),
        )
        .into_error()
        .into()
}

/// Mint one Ensure: idempotency pre-check, sealed-template enforcement,
/// then the internal CREATE DATABASE — all in one private transaction (the
/// dropdb_this_victim template). Returns Ok(true) = created, Ok(false) =
/// already existed (idempotent success). On Err the transaction is left
/// for the caller's contain() to abort.
fn mint_one(p: &registry::PendingEnsure) -> PgResult<bool> {
    let cx = mcx::MemoryContext::new("pgrust janitor mint");
    xact::StartTransactionCommand()?;
    let mcx = cx.mcx();

    // Idempotent fast path (spec: name exists -> instant success to all
    // waiters). Also what resolves "mint racing grace-expiry drop": drop
    // and mint serialize in this loop, so by the time this entry is
    // serviced the name either still exists (attach) or was dropped (fresh
    // mint) — never half of each.
    if pg_database::get_database_tuple_by_name(mcx, &p.name)?.is_some() {
        xact::CommitTransactionCommand()?;
        return Ok(false);
    }

    // Sealed-template enforcement, HERE and not in the grammar: the janitor
    // runs superuser, and createdb lets superusers clone ANY database —
    // without this check `<prefix>postgres__x` would clone the janitor's
    // home database for any listed role. datistemplate is the sealing bit
    // (D1 item 1); ALLOW_CONNECTIONS false is convention on top of it.
    let Some(tpl) = pg_database::get_database_tuple_by_name(mcx, &p.template)? else {
        return Err(template_missing_error(&p.template));
    };
    if !tpl.datistemplate {
        return Err(template_unsealed_error(&p.template, &p.name));
    }

    // Owner = the connecting role (spec security posture). The utility path
    // reaches the same effect via createdb's "owner" DefElem (datdba
    // resolution + member_can_set_role, which the janitor's superuser
    // session passes) — no post-CREATE ALTER OWNER needed.
    let stmt = build_createdb_stmt(mcx, &p.name, &p.template, &p.owner_name)?;
    let db_oid = dbcommands::createdb(mcx, &stmt)?;

    // M4 clone fidelity, MINT-TIME ONLY: inherit the template's
    // pg_db_role_setting state (both `ALTER DATABASE ... SET` and
    // `ALTER ROLE ... IN DATABASE ... SET` rows), which a vanilla template
    // clone deliberately drops — stock CREATE DATABASE stays C-exact, only
    // janitor-minted databases get the copy. Same transaction as the
    // createdb: a copy failure aborts the whole mint (the pg_database row
    // never becomes visible, waiters get the saved error, a retry re-mints
    // cleanly); the copied datadir is orphaned in that window — the same
    // class as C createdb's own abort-after-copy window (M4 addendum).
    let copied = pg_db_role_setting::copy_database_settings(mcx, tpl.oid, db_oid)?;
    if copied > 0 {
        let _ = log_report(
            LOG,
            format!(
                "pgrust ephemeral-db janitor: copied {copied} database-setting row(s) from \
                 template \"{}\" to ephemeral database \"{}\"",
                p.template, p.name
            ),
        );
    }
    xact::CommitTransactionCommand()?;
    Ok(true)
}

/// Programmatic `CREATE DATABASE <name> TEMPLATE <t> STRATEGY file_copy
/// OWNER <role>` (the gram_core actions.rs DefElem construction precedent).
fn build_createdb_stmt<'mcx>(
    mcx: Mcx<'mcx>,
    name: &str,
    template: &str,
    owner: &str,
) -> PgResult<CreatedbStmt<'mcx>> {
    fn def<'mcx>(mcx: Mcx<'mcx>, defname: &'static str, value: &str) -> PgResult<Node<'mcx>> {
        Node::mk(
            mcx,
            DefElem {
                defnamespace: None,
                defname: Some(defname),
                arg: Some(Node::mk_string(mcx, str_in(mcx, value)?)?),
                defaction: DefElemAction::DEFELEM_UNSPEC,
                location: -1,
            },
        )
    }
    let mut options = NodeList::make1(mcx, def(mcx, "template", template)?)?;
    options.lappend(mcx, def(mcx, "strategy", "file_copy")?)?;
    options.lappend(mcx, def(mcx, "owner", owner)?)?;
    Ok(CreatedbStmt {
        dbname: Some(str_in(mcx, name)?),
        options,
    })
}

/// Copy a str into the context (the parse_utilcmd::like str_in shape).
fn str_in<'mcx>(mcx: Mcx<'mcx>, s: &str) -> PgResult<&'mcx str> {
    let mut v: mcx::PgVec<'mcx, u8> = mcx::vec_with_capacity_in(mcx, s.len())?;
    mcx::vec_append_bytes(&mut v, s.as_bytes())?;
    Ok(core::str::from_utf8(v.leak()).expect("was UTF-8"))
}

// pgrust_set_template_grace's privilege boundary lives in builtins.rs with
// its siblings; the storage is registry::set_template_grace.

#[cfg(test)]
mod tests {
    use super::*;

    /// Allowlist matching semantics (pure half: the list parse + match; the
    /// $createdb sentinel's privilege probe needs a live syscache and is
    /// covered by the race suite's security phase). Under the crate-wide
    /// registry test lock only because it shares the process-global GUC
    /// encoding default — not the registry — keep it serialized anyway for
    /// uniformity.
    #[test]
    fn allowlist_matching_semantics() {
        let _table = crate::registry::test_pin_table_lock();
        let cx = mcx::MemoryContext::new("mint allowlist test");
        let mcx = cx.mcx();

        let q = |list: &str, role: &str| -> bool {
            // $createdb is unreachable for these fixtures (no syscache in
            // unit tests): lists here never rely on it.
            role_qualifies(mcx, list, role).unwrap()
        };

        // Unquoted entries downcase (identifier convention).
        assert!(q("alice, bob", "alice"));
        assert!(q("Alice", "alice"));
        assert!(!q("alice", "Alice"));
        // Quoted entries stay exact.
        assert!(q("\"Alice\"", "Alice"));
        assert!(!q("\"Alice\"", "alice"));
        // Non-members refuse.
        assert!(!q("alice, bob", "mallory"));
        // Empty and malformed lists match nothing (fail closed).
        assert!(!q("", "alice"));
        assert!(!q("alice,,bob", "alice"));
        assert!(!q("\"unterminated", "alice"));
    }

    fn pe(gen: u64, name: &str) -> registry::PendingEnsure {
        registry::PendingEnsure {
            gen,
            name: name.to_string(),
            template: "tpl_x".to_string(),
            owner_name: "minter".to_string(),
        }
    }

    /// Batch assembly (split_batch) + validation split (preflight_verdict):
    /// the pure halves of the batched-mint fast path. RELEASE-effective —
    /// plain asserts. No registry state is touched (PendingEnsure values
    /// are built directly), so no crate-wide lock is needed.
    #[test]
    fn batch_assembly_and_preflight_split() {
        // Under the cap: everything batches, nothing deferred.
        let (b, d) = split_batch(vec![pe(1, "a"), pe(2, "b")], MINT_BATCH_MAX);
        assert_eq!(d, 0);
        assert_eq!(b.len(), 2);

        // Exactly at the cap: still nothing deferred.
        let all: Vec<_> = (0..MINT_BATCH_MAX as u64)
            .map(|i| pe(i, &format!("n{i}")))
            .collect();
        let (b, d) = split_batch(all, MINT_BATCH_MAX);
        assert_eq!((b.len(), d), (MINT_BATCH_MAX, 0));

        // Over the cap: the OLDEST cap entries batch IN ORDER (oldest-first
        // is the fairness contract — a deferred waiter must not be passed
        // by a younger one forever), the excess count is reported.
        let all: Vec<_> = (0..(MINT_BATCH_MAX as u64 + 5))
            .map(|i| pe(i, &format!("n{i}")))
            .collect();
        let (b, d) = split_batch(all, MINT_BATCH_MAX);
        assert_eq!((b.len(), d), (MINT_BATCH_MAX, 5));
        assert_eq!(b.first().unwrap().gen, 0, "oldest entry leads the batch");
        assert_eq!(
            b.last().unwrap().gen,
            MINT_BATCH_MAX as u64 - 1,
            "batch is a prefix, never a sample"
        );

        // Validation split. Existing name = idempotent success, and it WINS
        // over any template state (the database is there — that is all the
        // waiter asked for; mint_one's check order).
        assert_eq!(
            preflight_verdict(true, Some((true, false))),
            PreflightVerdict::AlreadyExists
        );
        assert_eq!(preflight_verdict(true, None), PreflightVerdict::AlreadyExists);
        assert_eq!(
            preflight_verdict(true, Some((false, true))),
            PreflightVerdict::AlreadyExists
        );
        // Fresh name: template must resolve AND be sealed to mint at all;
        // each refusal classifies distinctly (distinct errors). Sealing is
        // decided on datistemplate alone — datallowconn never turns a
        // refusal into a mint or vice versa.
        assert_eq!(preflight_verdict(false, None), PreflightVerdict::TemplateMissing);
        assert_eq!(
            preflight_verdict(false, Some((false, false))),
            PreflightVerdict::TemplateUnsealed
        );
        assert_eq!(
            preflight_verdict(false, Some((false, true))),
            PreflightVerdict::TemplateUnsealed
        );
        // Batch-eligibility law: only a sealed AND unconnectable
        // (datallowconn = false) template may share the batch's widened
        // torn-copy window; a sealed-but-connectable template (the
        // template1 shape) still mints, on the SERIAL path.
        assert_eq!(
            preflight_verdict(false, Some((true, false))),
            PreflightVerdict::Mint
        );
        assert_eq!(
            preflight_verdict(false, Some((true, true))),
            PreflightVerdict::MintSerial
        );
    }
}
