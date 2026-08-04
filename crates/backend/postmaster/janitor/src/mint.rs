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
//!   then waking every waiter by ProcNumber.
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
/// FILE_COPY mints are checkpoint-bound (two synchronous checkpoints each)
/// and the janitor serializes them, so a cold-start storm of K distinct
/// tokens costs O(K) checkpoints before the last waiter wakes; 60s covers
/// hundreds of cheap-preset mints while still bounding a wedged janitor to
/// one minute of connect latency (spec: waiters must NEVER hang).
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

/// One mint service pass, run from the janitor tick BEFORE the reap pass.
/// Ordering rationale (recorded decision): mint-before-reap makes waiter
/// latency one tick at worst and lets a mint racing a same-name reap
/// resolve in mint's favor within the tick; the same tick's reap pass
/// cannot victimize the fresh mint because (a) the Ensure entry shields the
/// name (`registry::ensure_shields`, consulted next to pins) while pending
/// and through the post-completion linger, and (b) reaping additionally
/// requires a full observed-idle grace streak.
///
/// Per-entry errors are contained (main_loop::contain) and the SAVED error
/// is fanned out to every waiter — a failed CREATE must fail the waiters,
/// never the janitor. FATAL-class errors propagate; the janitor's exit
/// drain (main_loop's ClearProc) then fails whatever is still pending.
pub(crate) fn service_pass() -> PgResult<()> {
    for p in registry::pending_ensures() {
        let outcome = match mint_one(&p) {
            Ok(minted) => {
                let waiters = registry::complete_ensure(p.gen, Ok(()), pg_clock::mono_ns());
                let _ = log_report(
                    LOG,
                    format!(
                        "pgrust ephemeral-db janitor: {} ephemeral database \"{}\" from \
                         template \"{}\" for role \"{}\" ({} waiter(s))",
                        if minted { "minted" } else { "found existing" },
                        p.name,
                        p.template,
                        p.owner_name,
                        waiters.len()
                    ),
                );
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
        for w in outcome {
            latch::SetLatch(types_storage::latch::LatchHandle::proc(w));
        }
    }
    Ok(())
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
        return Err(ereport(ERROR)
            .errcode(ERRCODE_UNDEFINED_DATABASE)
            .errmsg(format!(
                "template database \"{}\" does not exist",
                p.template
            ))
            .into_error()
            .into());
    };
    if !tpl.datistemplate {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_WRONG_OBJECT_TYPE)
            .errmsg(format!(
                "database \"{}\" is not a template; refusing to mint \"{}\" from it",
                p.template, p.name
            ))
            .errhint(
                "Seal it first: ALTER DATABASE ... WITH IS_TEMPLATE true ALLOW_CONNECTIONS \
                 false."
                    .to_string(),
            )
            .into_error()
            .into());
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
}
