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
//! that must look stock — feature off, non-matching prefix, unlisted role —
//! returns `Ok(false)` so InitPostgres falls through to its byte-identical
//! does-not-exist FATAL. Conditions with their own story — a malformed
//! prefix-matching name from an AUTHORIZED role (the template-form FATAL),
//! an absent janitor, cap, timeout, a failed CREATE — return `Err` (a clean
//! FATAL), because hiding them behind "does not exist" would turn
//! operational states into gaslighting.

use elog::{elog as log_report, ereport};
use init_small::globals as g;
use mcx::Mcx;
use types_core::{Oid, ProcNumber};
use types_error::{
    PgError, PgResult, ERRCODE_CONFIGURATION_LIMIT_EXCEEDED, ERRCODE_INTERNAL_ERROR,
    ERRCODE_OBJECT_IN_USE, ERRCODE_UNDEFINED_DATABASE, ERRCODE_WRONG_OBJECT_TYPE, ERROR, FATAL,
    LOG,
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

    let role_oid = miscinit::GetUserId();
    let cx = mcx::MemoryContext::new("pgrust mint-on-connect");
    let mcx = cx.mcx();
    // The authenticated session's role must resolve; a concurrent DROP ROLE
    // is a legitimate miss — treat as unlisted (stock FATAL).
    let Some(role_name) = miscinit::GetUserNameFromId(mcx, role_oid, true)? else {
        return Ok(false);
    };
    if !role_qualifies(mcx, &roles_guc, role_name.as_str())? {
        // Authorization opacity: unauthorized roles get the stock
        // does-not-exist FATAL for EVERY prefix-matching shape, malformed
        // included — the namespace does not exist for them.
        return Ok(false);
    }
    let template = match shape {
        MintShape::Template { template, .. } => template.to_string(),
        MintShape::Malformed => {
            // Template-form-only grammar (ruling 2026-08-05): an authorized
            // role connecting to a prefix-matching name without the
            // `__` separator gets a clear FATAL naming the required form —
            // never a silent stock error it would misread as a typo, and
            // never a mint from some implicit template.
            return Err(ereport(FATAL)
                .errcode(ERRCODE_UNDEFINED_DATABASE)
                .errmsg(format!(
                    "database \"{dbname}\" does not exist and does not name a template to \
                     mint it from"
                ))
                .errhint(format!(
                    "Ephemeral database names embed their template: connect to \
                     \"{prefix}<template>__<token>\" (e.g. \"{prefix}myapp_tpl__w1\")."
                ))
                .into_error()
                .into());
        }
    };

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
/// `$createdb` and `*` are RESERVED: split_identifier_string strips quotes
/// before returning entries, so a quoted `"$createdb"` / `"*"` is
/// indistinguishable here and still selects the sentinel — roles literally
/// named `$createdb` or `*` (creatable only via quoting) can never be
/// allowlisted by name. This deliberately diverges from pg_hba's
/// quoting-demotes-keywords convention; documented on the GUC's long_desc.
///
/// `*` = ANY role may mint (user ruling 2026-08-05: `--profile test`
/// declares a disposable test server, where mint gating is friction
/// without protection — the profile sets this). The fail-closed default
/// (`''` = minting off) is unchanged for servers that arm the janitor
/// WITHOUT the profile (durable dev/preview boxes), where the allowlist
/// keeps its purpose.
fn role_qualifies(mcx: Mcx<'_>, roles_guc: &str, role_name: &str) -> PgResult<bool> {
    let Some(entries) =
        varlena::split_identifier_string(mcx, roles_guc, b',', mbutils::GetDatabaseEncoding())?
    else {
        return Ok(false);
    };
    for entry in &entries {
        if entry == "*" {
            return Ok(true);
        }
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
        // in between must not cost us the full deadline).
        if !registry::janitor_present() {
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
/// crash-orphan window. COLD entries only — the cap amortizes checkpoint
/// pairs across batch clones, so warm-pool handouts (catalog renames, no
/// checkpoints) are served uncapped BEFORE it applies (gather_batch).
/// Excess cold entries simply stay Pending for the next tick — the pass
/// re-arms the janitor's own latch so the follow-up tick is immediate, and
/// waiters carry the 60s deadline regardless.
pub(crate) const MINT_BATCH_MAX: usize = 32;

/// Worker-pool ceiling for the batch copy phase (F1): workers =
/// min(this, batch members with a deferred copy). Small on purpose — the
/// copies are IO-bound and the janitor must not commandeer the box.
#[cfg(not(pgrust_sim))]
const MINT_COPY_WORKERS_MAX: usize = 8;

/// Per-mint CREATE DATABASE strategy (F2, the mint-strategy addendum):
/// picked per template from its cached swept-relation count against
/// `pgrust.ephemeral_db_wal_log_threshold`. file_copy keeps the batched
/// checkpoint machinery (flush marks, one pair per batch); wal_log has no
/// checkpoint sites at all — `createdb_skip_checkpoints`' flag is a
/// documented no-op there — so wal_log members consult and record NO flush
/// mark and trigger NEITHER batch checkpoint. Mixed batches are legal: the
/// pre-checkpoint fires only for a file-copying member lacking its mark,
/// the post-checkpoint only when >= 1 file_copy member actually minted.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) enum MintStrategy {
    FileCopy,
    WalLog,
}

/// The pure pick (unit-tested): threshold < 0 = wal_log disabled; an
/// unknown count (probe unavailable) stays on the C-shaped file_copy path.
fn pick_strategy(relcount: Option<usize>, threshold: i32) -> MintStrategy {
    match relcount {
        _ if threshold < 0 => MintStrategy::FileCopy,
        None => MintStrategy::FileCopy,
        Some(n) if n >= threshold as usize => MintStrategy::WalLog,
        Some(_) => MintStrategy::FileCopy,
    }
}

/// Does this member's mint owe the batch a FLUSH_ALL pre-checkpoint (absent
/// one already requested this batch)? Pure half of the batch checkpoint
/// decision table (unit-tested); `marked` = the template carries a live
/// flush mark.
fn member_pays_pre(strategy: MintStrategy, marked: bool) -> bool {
    strategy == MintStrategy::FileCopy && !marked
}

/// Does this member's completed mint oblige the batch's post-checkpoint?
/// Pure half of the decision table (unit-tested): only file_copy members
/// carry the commit-implies-checkpointed crash invariant.
fn member_owes_post(strategy: MintStrategy) -> bool {
    strategy == MintStrategy::FileCopy
}

/// The FULL per-member pre-checkpoint decision (pure, unit-tested):
/// `member_pays_pre` gated by the batch's lazy single-checkpoint term —
/// EXCEPT when this member's own strategy pick ran the pg_class probe
/// (`probed`), which may have dirtied the template's buffers with hint
/// bits AFTER any checkpoint an earlier member requested: a checkpoint
/// that completed before the dirtying covers nothing, so a probed
/// file_copy member re-pays the FLUSH_ALL unconditionally. (`probed`
/// implies `!marked`: the probe cleared the mark.) Steady state — counts
/// cached, no probes — keeps the one-checkpoint-per-batch shape; only a
/// template's FIRST-ever pick can add a re-fire.
fn member_fires_pre(
    strategy: MintStrategy,
    marked: bool,
    probed: bool,
    pre_checkpointed: bool,
) -> bool {
    member_pays_pre(strategy, marked) && (!pre_checkpointed || probed)
}

/// The strategy pick for one template, cache-through: a cached swept-
/// relation count is reused; a miss runs the pg_class sweep probe
/// (`dbcommands::count_swept_relations`, inside the caller's transaction)
/// and caches it. The cache is invalidated at every observed-unseal site
/// (registry::clear_template_flushed) and pruned with the flush marks.
/// With the threshold at -1 (disabled) the probe never runs at all.
///
/// PROBE DIRTIES THE TEMPLATE (the flush-mark hazard): the probe READS the
/// template's pg_class through shared buffers and can DIRTY those buffers
/// via hint bits (heap_tuple_satisfies_visibility -> SetHintBits ->
/// MarkBufferDirtyHint), which falsifies the sealed-template flush mark's
/// premise ("no dirty buffer of the template can exist"). Therefore a
/// probe FIRST clears the template's mark — before the scan, so an error
/// mid-probe cannot leave a stale mark either (clear_template_flushed
/// also drops any cached count; the fresh count is re-cached after) — and
/// the returned `probed` flag tells the batch checkpoint decision to
/// (re)pay the FLUSH_ALL for this member even when the batch already
/// checkpointed for an earlier member (`member_fires_pre`). The SERIAL
/// path needs no flag: its C-shaped createdb runs its own FLUSH_ALL after
/// the probe (file_copy), or needs none (wal_log) — the cleared mark
/// alone keeps future batches honest there.
fn strategy_for_template(
    mcx: Mcx<'_>,
    oid: Oid,
    dattablespace: Oid,
) -> PgResult<(MintStrategy, bool)> {
    let threshold = crate::ephemeral_db_wal_log_threshold();
    if threshold < 0 {
        return Ok((MintStrategy::FileCopy, false));
    }
    match registry::template_relcount(oid) {
        Some(n) => Ok((pick_strategy(Some(n), threshold), false)),
        None => {
            registry::clear_template_flushed(oid);
            let n = dbcommands::count_swept_relations(mcx, oid, dattablespace)?;
            registry::set_template_relcount(oid, n);
            Ok((pick_strategy(Some(n), threshold), true))
        }
    }
}

/// One committed-or-pending createdb of the CURRENT batch transaction:
/// oid + the strategy it ran under, so the batch-abort cleanup can give
/// wal_log members their buffer-drop arm (file_copy members only need the
/// rmtree + DROP-record path).
#[derive(Clone, Copy)]
pub(crate) struct CreatedDb {
    pub oid: Oid,
    pub strategy: MintStrategy,
}

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
    // D3 warm-pool handout, FIRST and UNCAPPED (the tick-quantized-dispatch
    // fix): every handout-eligible pending Ensure — default/pooled-template
    // request with a live spare — is satisfied THIS pass by a catalog-only
    // RENAME (pool.rs choreography, ~0.03ms each). MINT_BATCH_MAX exists to
    // amortize checkpoint pairs across COLD batch clones and is meaningless
    // for renames, so it applies only to the cold remainder below; handout
    // failures fall through to the mint paths — a waiter is never failed on
    // a pool problem. With the pool off (no spares) this is one registry
    // probe. gather_batch also re-snapshots after each handout round, so
    // Ensures arriving while a round's renames run are served this same
    // pass instead of aging a loop turn.
    let (batch, deferred) =
        gather_batch(registry::pending_ensures, crate::pool::service_handouts, MINT_BATCH_MAX)?;
    log_deferral(deferred);
    if batch.is_empty() {
        return Ok(());
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
        let (outcome, created) = match mint_one(p) {
            Ok(created) => {
                let waiters = registry::complete_ensure(p.gen, Ok(()), pg_clock::mono_ns());
                log_mint_success(created.is_some(), p, waiters.len());
                (waiters, created)
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
                (registry::complete_ensure(p.gen, Err(saved), pg_clock::mono_ns()), None)
            }
        };
        wake_waiters(&outcome);
        // AFTER the wakes (prewarm is strictly off the waiter path): churn
        // accounting + the post-mint touch enqueue (prewarm.rs dispatches
        // in a later tick step) + the usage-keyed pool registration.
        if let Some(oid) = created {
            registry::note_catalog_churn(1);
            if !p.spare {
                if crate::ephemeral_db_prewarm() {
                    let _ = registry::enqueue_touch(&p.name, oid);
                }
                note_template_pooled(&p.template);
            }
        }
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
    let mut created: Vec<CreatedDb> = Vec::new();
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
                // "shared checkpoint cycle", not "pair": the sealed-template
                // skip (mint_batch_body) may have elided the pre-checkpoint,
                // in which case the adjacent skip line says so. An
                // all-wal_log batch (F2) requested NO checkpoints at all —
                // its suffix says that instead (the race suite's
                // zero-checkpoint witness).
                let n_filecopy =
                    created.iter().filter(|c| c.strategy == MintStrategy::FileCopy).count();
                let suffix = if n_filecopy > 0 {
                    "one shared checkpoint cycle"
                } else {
                    "zero checkpoints (all wal_log)"
                };
                let _ = log_report(
                    LOG,
                    format!(
                        "pgrust ephemeral-db janitor: batch-minted {n_minted} ephemeral \
                         database(s) in one transaction with {suffix}"
                    ),
                );
            }
            let mut deferred_serial: Vec<registry::PendingEnsure> = Vec::new();
            // Minted outcomes and `created` oids track in lockstep (the
            // replenish_batch zip precedent) — the prewarm enqueue needs
            // each minted database's oid.
            let mut created_oids = created.iter();
            for (p, o) in to_mint.iter().zip(outcomes) {
                match o {
                    BatchOutcome::Minted { copied } => {
                        let db_oid =
                            created_oids.next().expect("created oids track Minted outcomes").oid;
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
                        // AFTER the wakes (the service_serial discipline).
                        registry::note_catalog_churn(1);
                        if !p.spare {
                            if crate::ephemeral_db_prewarm() {
                                let _ = registry::enqueue_touch(&p.name, db_oid);
                            }
                            note_template_pooled(&p.template);
                        }
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
                        spare: p.spare,
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

/// Fail every pending Ensure with `cause` and wake the waiters —
/// UNCONDITIONALLY (main_loop's janitor-exit drain: nothing will ever
/// service the queue again).
pub(crate) fn fail_pending_and_wake(cause: &PgError) {
    let waiters = registry::fail_pending_ensures(cause, pg_clock::mono_ns());
    for w in waiters {
        latch::SetLatch(types_storage::latch::LatchHandle::proc(w));
    }
}

// ---------------------------------------------------------------------------
// Batch assembly + preflight (pure halves unit-tested below).
// ---------------------------------------------------------------------------

/// The pass's dispatch choreography, generic over the two I/O actions
/// (`snapshot` = registry::pending_ensures, `handout` =
/// pool::service_handouts) so the no-cap-on-warm-handouts law is
/// unit-testable without a catalog: drain warm handouts UNCAPPED — looping
/// on a fresh snapshot after every round that served >= 1 entry, so
/// arrivals during a round are picked up this same pass — then apply the
/// cold batch cap to what the pool could not serve. Returns
/// (cold batch, deferred_count).
///
/// TERMINATION BOUND: an entry leaves `handout`'s return only via a
/// committed rename, which consumes one listed spare, and spares are added
/// only by replenish_pass — a LATER step of this same single janitor
/// thread — so the loop runs at most (listed spares + 1) rounds, each
/// round's handouts costing ~0.03ms catalog transactions. Starvation of
/// reap/replenish is not a real risk at these costs; the hard ceiling is
/// the finite pool (MAX_SPARES).
fn gather_batch(
    mut snapshot: impl FnMut() -> Vec<registry::PendingEnsure>,
    mut handout: impl FnMut(Vec<registry::PendingEnsure>) -> PgResult<Vec<registry::PendingEnsure>>,
    cap: usize,
) -> PgResult<(Vec<registry::PendingEnsure>, usize)> {
    loop {
        let pending = snapshot();
        if pending.is_empty() {
            return Ok((Vec::new(), 0));
        }
        let n = pending.len();
        let rest = handout(pending)?;
        if rest.len() == n {
            // Nothing was served warm this round: the remainder is cold
            // (no matching spare / stale pool / pool off) and flows into
            // the capped batch path.
            return Ok(split_batch(rest, cap));
        }
    }
}

/// End-of-tick warm-handout re-check (main_loop, after replenish/prewarm/
/// reap/maint and before the loop returns to WaitLatch): Ensures posted
/// while those passes ran must not age a full loop turn when a spare can
/// serve them NOW. Cold arrivals are left Pending on purpose — their post
/// already set our latch (mint_on_connect's wake_janitor), so the next
/// WaitLatch returns immediately and the next full service pass takes
/// them; running the cold mint machinery twice per turn would double
/// checkpoint work for nothing. Same termination bound as gather_batch.
/// A non-zero deferral IS logged here (the shared log_deferral): warm
/// handouts must never be silently capped at ANY drain site — the race
/// suite's dispatch phase pins the zero-deferral invariant against this
/// pass exactly as against service_pass.
pub(crate) fn late_handout_pass() -> PgResult<()> {
    let (_cold, deferred) =
        gather_batch(registry::pending_ensures, crate::pool::service_handouts, MINT_BATCH_MAX)?;
    log_deferral(deferred);
    Ok(())
}

/// The over-cap deferral witness line + self-wake, shared by every
/// gather_batch call site (the line is load-bearing: the race suite's
/// dispatch phase asserts ZERO of these fire for warm handouts, and the
/// storm bookkeeping tolerates them for cold overflow). Deferral must
/// cost one loop turn, not a full 500ms tick: the janitor sets its own
/// latch (wake_janitor targets janitor_proc, which is us).
fn log_deferral(deferred: usize) {
    if deferred == 0 {
        return;
    }
    let _ = log_report(
        LOG,
        format!(
            "pgrust ephemeral-db janitor: deferring {deferred} pending mint request(s) to \
             the next tick (batch cap {MINT_BATCH_MAX})"
        ),
    );
    registry::wake_janitor();
}

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
        let template_state = match pg_database::get_database_tuple_by_name(mcx, &p.template)? {
            Some(t) => {
                if !t.datistemplate || t.datallowconn {
                    // Observed unsealed/connectable: invalidate the
                    // sealed-template flush mark (the skip rationale on
                    // mint_batch_body — the mark must not survive an
                    // observed unseal).
                    registry::clear_template_flushed(t.oid);
                }
                Some((t.datistemplate, t.datallowconn))
            }
            None => None,
        };
        verdicts.push(preflight_verdict(name_exists, template_state));
    }
    xact::CommitTransactionCommand()?;
    Ok(verdicts)
}

// ---------------------------------------------------------------------------
// The batch transaction.
// ---------------------------------------------------------------------------

/// Per-entry result inside a successful batch transaction.
pub(crate) enum BatchOutcome {
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
pub(crate) enum BatchFailure {
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
pub(crate) fn mint_batch(
    batch: &[registry::PendingEnsure],
    created: &mut Vec<CreatedDb>,
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
    created: &mut Vec<CreatedDb>,
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
    //
    // SEALED-TEMPLATE PRE-CHECKPOINT SKIP (D3 warm-pool addendum): the
    // pre-checkpoint is additionally skipped for a member whose template
    // carries a registry flush mark. Rationale, discharged term by term:
    // C's FLUSH_ALL pre-checkpoint exists solely to push the SOURCE
    // database's dirty buffers (unlogged relations included — hence
    // FLUSH_ALL) to disk before the file-level copy. Every batch member's
    // template is datistemplate = true AND datallowconn = false at THIS
    // transaction's snapshot (the re-checks below), so ordinary connections
    // cannot dirty its buffers; once ONE flush-all checkpoint has completed
    // while it is in that state, no dirty buffer of it can exist and the
    // pre-checkpoint is a no-op — skipped. The mark is restart-lossy BY
    // DESIGN: the first batch touching a template after janitor start pays
    // the checkpoint once and marks it (self-healing, no marker file).
    //
    // Caveats, carried honestly:
    // - Anti-wraparound autovacuum is the one writer datallowconn = false
    //   does NOT stop. A COMPLETED wraparound vacuum advances
    //   datfrozenxid/datminmxid (vac_update_datfrozenxid), which the mark
    //   records — the mark self-invalidates and the next batch re-pays. A
    //   vacuum still IN PROGRESS at batch time has dirtied buffers without
    //   advancing either — the undetectable residual; the template recipe
    //   therefore recommends freeze-at-seal (VACUUM FREEZE before sealing),
    //   which parks the wraparound trigger ~2^31 xids out.
    // - An unseal-write-reseal cycle: any janitor probe that OBSERVES the
    //   template unsealed/connectable clears the mark
    //   (registry::clear_template_flushed at the preflight and batch
    //   re-check sites); a cycle entirely between observations is invisible
    //   — documented residual, and the recipe never unseals (rebuilds get a
    //   NEW template name).
    // The POST-checkpoint is NOT touched (option 4 stays parked).
    //
    // STRATEGY (F2): both checkpoint terms above apply ONLY to file_copy
    // members. A wal_log member's copy is WAL-logged page by page through
    // shared buffers — it needs no source-flush before and no crash-replay
    // exemption after — so it consults no flush mark, records none, and
    // an all-wal_log batch requests ZERO checkpoints. The strategy PICK
    // itself is a batch-transaction writer the original discharge missed:
    // its pg_class probe can dirty the TEMPLATE's buffers via hint bits —
    // so a probing pick clears the template's flush mark and its member
    // (re)pays the FLUSH_ALL even when the batch already checkpointed
    // (strategy_for_template + member_fires_pre carry the analysis).
    //
    // BATCH ORDER (F1, live builds): catalog-all (this loop, file_copy
    // members deferring their directory copies) -> copy-all (the parallel
    // worker pool, run_deferred_copies) -> WAL-record-all (this thread) ->
    // post-checkpoint -> the caller's commit. Replay-equivalent to the old
    // per-member interleaving: every FILE_COPY record still precedes the
    // single post-checkpoint and the single commit, every record still
    // strictly follows its member's completed (and fsynced) copy, and
    // member-to-member record order carries no constraint (independent
    // src/dst pairs). Under pgrust_sim the copies run inline (serial
    // fd::copydir through the sim vfs), preserving the pre-F1 shape.
    let mut pre_checkpointed = false;

    let mut outcomes = Vec::with_capacity(batch.len());
    #[cfg(not(pgrust_sim))]
    let mut deferred: Vec<dbcommands::DeferredFileCopy> = Vec::new();
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
                // Observed unsealed: the flush mark must not survive a
                // later re-seal (the skip rationale above).
                registry::clear_template_flushed(t.oid);
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
                // Observed connectable: same mark invalidation as above.
                registry::clear_template_flushed(t.oid);
                outcomes.push(BatchOutcome::DeferSerial);
                continue;
            }
            Some(t) => t,
        };
        let (strategy, probed) = strategy_for_template(mcx, tpl.oid, tpl.dattablespace)?;
        if strategy == MintStrategy::FileCopy {
            let marked =
                registry::template_flushed_matches(tpl.oid, tpl.datfrozenxid, tpl.datminmxid);
            // `probed` forces a (re)fire even when the batch already
            // checkpointed: the pick's pg_class scan may have dirtied THIS
            // template's buffers (hint bits) after that earlier checkpoint,
            // which would otherwise be file-copied dirty AND wrongly marked
            // flushed (member_fires_pre's rationale). Probes touch only
            // their own template, and a same-template later member cache-
            // hits (probed=false), so at most one re-fire per NEW template.
            if member_fires_pre(strategy, marked, probed, pre_checkpointed) {
                checkpointer::RequestCheckpoint(
                    CHECKPOINT_IMMEDIATE
                        | CHECKPOINT_FORCE
                        | CHECKPOINT_WAIT
                        | CHECKPOINT_FLUSH_ALL,
                )?;
                pre_checkpointed = true;
            }
            // Reaching here implies marked-or-just-checkpointed — the
            // probed case cleared the mark and fired above, so nothing in
            // this batch dirtied the template after the checkpoint that
            // covers it — and the mark is sound to (re)record NOW, even if
            // the batch later aborts: the checkpoint itself is synchronous
            // and non-transactional, and an abort dirties no template
            // buffers.
            registry::mark_template_flushed(tpl.oid, tpl.datfrozenxid, tpl.datminmxid);
        }
        let stmt = build_createdb_stmt(mcx, p, strategy)?;
        let db_oid = match strategy {
            MintStrategy::WalLog => dbcommands::createdb_skip_checkpoints(mcx, &stmt)?,
            #[cfg(not(pgrust_sim))]
            MintStrategy::FileCopy => {
                // F1: catalog work now, the directory copies (and their
                // WAL records) after the loop.
                let d = dbcommands::createdb_deferred_file_copy(mcx, &stmt)?;
                let oid = d.db_oid;
                deferred.push(d);
                oid
            }
            #[cfg(pgrust_sim)]
            MintStrategy::FileCopy => dbcommands::createdb_skip_checkpoints(mcx, &stmt)?,
        };
        created.push(CreatedDb { oid: db_oid, strategy });
        let copied = pg_db_role_setting::copy_database_settings(mcx, tpl.oid, db_oid)?;
        outcomes.push(BatchOutcome::Minted { copied });
        // Multi-statement-transaction shape: make this member's catalog
        // rows command-visible before the next member's scans (not strictly
        // required — member names are registry-unique and templates are
        // pre-committed; GetNewOidWithIndex scans SnapshotAny — but it is
        // the conservative utility-statement convention).
        xact::CommandCounterIncrement()?;
    }

    // F1 copy-all + WAL-record-all phases (live builds; a sim batch copied
    // inline above). Any error here aborts the whole batch onto the serial
    // fallback; partially copied datadirs are the caller's
    // cleanup_orphaned_datadirs orphans, exactly like an inline copy
    // failure's.
    #[cfg(not(pgrust_sim))]
    if !deferred.is_empty() {
        run_deferred_copies(&deferred)?;
    }

    let n_filecopy = created.iter().filter(|c| member_owes_post(c.strategy)).count();
    if n_filecopy > 0 {
        if !pre_checkpointed {
            // The race suite's skip witness: copies happened and the
            // pre-checkpoint was skipped outright (every copying member's
            // template carried a live flush mark).
            let _ = log_report(
                LOG,
                "pgrust ephemeral-db janitor: batch pre-checkpoint skipped: every member \
                 template already sealed-and-flushed"
                    .to_string(),
            );
        }
        // ONE post-checkpoint INSIDE the transaction, after the last copy
        // and before the single commit (skipped when no FILE_COPY member
        // copied — wal_log members never owe the invariant, so an
        // all-wal_log batch requests zero checkpoints).
        checkpointer::RequestCheckpoint(CHECKPOINT_IMMEDIATE | CHECKPOINT_FORCE | CHECKPOINT_WAIT)?;
    }
    Ok(outcomes)
}

/// F1's copy-all + WAL-record-all phases, live builds only. Re-asserts
/// source occupancy first (createdb_deferred_file_copy's contract: the
/// catalog-time CountOtherDBBackends no longer sits adjacent to the copy),
/// then copies fan out across min(8, members) scoped worker threads
/// issuing raw path-based syscalls (parcopy.rs carries the thread-safety
/// analysis); this thread babysits interrupts meanwhile, then runs the
/// per-directory fsyncs, then inserts every member's
/// XLOG_DBASE_CREATE_FILE_COPY records — WAL insertion state is
/// backend-thread TLS, so the records CANNOT move to the workers. The
/// witness line is the race suite's parallel-path proof.
#[cfg(not(pgrust_sim))]
fn run_deferred_copies(deferred: &[dbcommands::DeferredFileCopy]) -> PgResult<()> {
    // Occupancy re-check, restoring the guard deferral widened: C's
    // CountOtherDBBackends runs "immediately before its copy", but each
    // member's catalog-time check (createdb_guts) now sits a whole batch of
    // catalog work — and any checkpoint stall — before the copies. An
    // anti-wraparound autovacuum worker attaching to a template in that gap
    // (ordinary connections cannot: batch templates are datallowconn=false
    // at the batch snapshot) would otherwise write-race the parallel copy
    // where the pre-F1 inline path would have terminated it. Re-run the
    // check per unique source HERE, adjacent to the copies; occupied
    // aborts the whole batch onto the serial fallback, whose per-member
    // C-shaped check (with its autovacuum-kill retry loop) resolves it.
    let mut checked: Vec<Oid> = Vec::with_capacity(deferred.len());
    for d in deferred {
        if checked.contains(&d.src_dboid) {
            continue;
        }
        checked.push(d.src_dboid);
        if let Some((notherbackends, npreparedxacts)) =
            procarray::CountOtherDBBackends(d.src_dboid)?
        {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_OBJECT_IN_USE)
                .errmsg(format!(
                    "source database with OID {} is being accessed by other users",
                    d.src_dboid
                ))
                .errdetail(dbcommands::errdetail_busy_db(notherbackends, npreparedxacts))
                .into_error()
                .into());
        }
    }
    let mut jobs = Vec::new();
    for d in deferred {
        for dir in &d.dirs {
            jobs.push(crate::parcopy::enumerate_dir_job(&dir.srcpath, &dir.dstpath)?);
        }
    }
    let members = deferred.len();
    let workers = members.min(MINT_COPY_WORKERS_MAX);
    let knobs = crate::parcopy::snapshot_knobs();
    let stats = crate::parcopy::copy_dirs_parallel(jobs, workers, knobs)?;

    // Destination-directory fsyncs (copydir's tail), on THIS thread:
    // per-file fsyncs already ran on the workers' own fds.
    if init_small::globals::enableFsync() {
        for d in deferred {
            for dir in &d.dirs {
                fd::fsync_fname(&dir.dstpath, true)?;
            }
        }
    }
    for d in deferred {
        for dir in &d.dirs {
            dbcommands::log_file_copy_record(
                d.db_oid,
                dir.dst_tablespace,
                d.src_dboid,
                dir.src_tablespace,
            )?;
        }
    }
    let _ = log_report(
        LOG,
        format!(
            "pgrust ephemeral-db janitor: batch copy: {members} member(s) across {} thread(s): \
             {} file(s), wall {:.1} ms, summed worker time {:.1} ms",
            stats.threads,
            stats.files,
            stats.wall_ns as f64 / 1e6,
            stats.busy_ns as f64 / 1e6,
        ),
    );
    Ok(())
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
/// wal_log members (F2) additionally get createdb_failure_cleanup's
/// buffer-drop arm FIRST (`dbcommands::forget_walog_database`): their copy
/// ran through shared buffers, and a dirty dst-database buffer surviving
/// past the rmtree would have the checkpointer writing into a removed
/// directory. The same flushed-DROP discipline covers their durable
/// CREATE_WAL_LOG records.
pub(crate) fn cleanup_orphaned_datadirs(created: &[CreatedDb]) {
    if created.is_empty() {
        return;
    }
    let run = || -> PgResult<()> {
        let cx = mcx::MemoryContext::new("pgrust janitor batch-abort cleanup");
        xact::StartTransactionCommand()?;
        for c in created {
            if c.strategy == MintStrategy::WalLog {
                dbcommands::forget_walog_database(c.oid)?;
            }
            dbcommands::remove_dbtablespaces(cx.mcx(), c.oid)?;
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

/// Usage-keyed pool registration (item A): the FIRST cold mint of a
/// template registers it as pooled — that mint was served cold by design;
/// the replenisher keeps up to pool_size spares of it warm thereafter. The
/// witness line fires once per template per boot (the races-suite pool
/// phase greps it).
fn note_template_pooled(template: &str) {
    if registry::note_pooled_template(template) {
        let _ = log_report(
            LOG,
            format!(
                "pgrust ephemeral-db janitor: template \"{template}\" registered for warm \
                 pooling (first mint served cold; replenish maintains spares)"
            ),
        );
    }
}

pub(crate) fn wake_waiters(waiters: &[ProcNumber]) {
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
pub(crate) fn report_contained_refusal(e: &PgError, name: &str) {
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
/// dropdb_this_victim template). Returns Ok(Some(oid)) = created (the new
/// database's oid — the warm-pool replenisher registers it), Ok(None) =
/// already existed (idempotent success). On Err the transaction is left
/// for the caller's contain() to abort.
pub(crate) fn mint_one(p: &registry::PendingEnsure) -> PgResult<Option<Oid>> {
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
        return Ok(None);
    }

    // Sealed-template enforcement, HERE and not in the grammar: the janitor
    // runs superuser, and createdb lets superusers clone ANY database —
    // without this check `<prefix>postgres__x` would clone the janitor's
    // home database for any listed role. datistemplate is the sealing bit
    // (D1 item 1); ALLOW_CONNECTIONS false is convention on top of it.
    let Some(tpl) = pg_database::get_database_tuple_by_name(mcx, &p.template)? else {
        return Err(template_missing_error(&p.template));
    };
    if !tpl.datistemplate || tpl.datallowconn {
        // Observed unsealed/connectable: invalidate the sealed-template
        // flush mark (the mint_batch_body skip rationale). The serial path
        // is an observation site EXACTLY like preflight and the batch
        // re-check — a single-entry tick bypasses preflight entirely
        // (service_pass routes batch.len()==1 straight here), so skipping
        // the clear here would let a mark survive an observed unseal and a
        // later batch skip the FLUSH_ALL pre-checkpoint over a template
        // whose recent writes are still dirty in shared buffers.
        registry::clear_template_flushed(tpl.oid);
    }
    if !tpl.datistemplate {
        return Err(template_unsealed_error(&p.template, &p.name));
    }

    // Owner = the connecting role (spec security posture). The utility path
    // reaches the same effect via createdb's "owner" DefElem (datdba
    // resolution + member_can_set_role, which the janitor's superuser
    // session passes) — no post-CREATE ALTER OWNER needed.
    //
    // Strategy (F2) applies on the serial path too: a wal_log pick makes
    // this C-shaped createdb checkpoint-free (that strategy has no
    // checkpoint sites), a file_copy pick keeps the stock pair. Stock
    // failure cleanup handles both arms. The probe flag is moot here: a
    // file_copy createdb below runs its OWN FLUSH_ALL after the probe
    // (covering any hint-bit dirtying), and the probe already cleared the
    // template's flush mark so later BATCHES re-pay theirs.
    let (strategy, _probed) = strategy_for_template(mcx, tpl.oid, tpl.dattablespace)?;
    let stmt = build_createdb_stmt(mcx, p, strategy)?;
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
    Ok(Some(db_oid))
}

/// One string-valued DefElem (the gram_core actions.rs construction
/// precedent), shared by the createdb and ALTER DATABASE stmt builders
/// (seal.rs builds its IS_TEMPLATE/ALLOW_CONNECTIONS flip from it too).
pub(crate) fn def<'mcx>(mcx: Mcx<'mcx>, defname: &'static str, value: &str) -> PgResult<Node<'mcx>> {
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

/// Programmatic `CREATE DATABASE <name> TEMPLATE <t> STRATEGY <picked>
/// OWNER <role>`, plus `ALLOW_CONNECTIONS false` for warm-pool spare specs
/// (`p.spare`): a listed spare must not be enterable — any role with
/// CONNECT could otherwise connect-write-disconnect between ticks and the
/// next handout would serve the dirtied database as a fresh template clone
/// (the handout's occupancy check only sees clients still connected AT
/// handout time). The handout transaction flips connectability back on
/// (pool::handout_one). Client Ensures keep the stock createdb default.
/// The strategy word is the F2 per-template pick (file_copy unless the
/// template's swept-relation count crosses the wal_log threshold).
fn build_createdb_stmt<'mcx>(
    mcx: Mcx<'mcx>,
    p: &registry::PendingEnsure,
    strategy: MintStrategy,
) -> PgResult<CreatedbStmt<'mcx>> {
    let strategy_word = match strategy {
        MintStrategy::FileCopy => "file_copy",
        MintStrategy::WalLog => "wal_log",
    };
    let mut options = NodeList::make1(mcx, def(mcx, "template", &p.template)?)?;
    options.lappend(mcx, def(mcx, "strategy", strategy_word)?)?;
    options.lappend(mcx, def(mcx, "owner", &p.owner_name)?)?;
    if p.spare {
        options.lappend(mcx, def(mcx, "allow_connections", "false")?)?;
    }
    Ok(CreatedbStmt {
        dbname: Some(str_in(mcx, &p.name)?),
        options,
    })
}

/// Programmatic `ALTER DATABASE <name> WITH ALLOW_CONNECTIONS <bool>`
/// (pool::handout_one's connectability flip). Internally callable like the
/// rename/chown entries: AlterDatabase's only PreventInTransactionBlock
/// arm is SET TABLESPACE's, which this stmt never selects.
pub(crate) fn build_alterdb_allowconn_stmt<'mcx>(
    mcx: Mcx<'mcx>,
    name: &str,
    allow: bool,
) -> PgResult<types_nodes::parsenodes::AlterDatabaseStmt<'mcx>> {
    let options = NodeList::make1(
        mcx,
        def(mcx, "allow_connections", if allow { "true" } else { "false" })?,
    )?;
    Ok(types_nodes::parsenodes::AlterDatabaseStmt {
        dbname: Some(str_in(mcx, name)?),
        options,
    })
}

/// Copy a str into the context (the parse_utilcmd::like str_in shape).
pub(crate) fn str_in<'mcx>(mcx: Mcx<'mcx>, s: &str) -> PgResult<&'mcx str> {
    let mut v: mcx::PgVec<'mcx, u8> = mcx::vec_with_capacity_in(mcx, s.len())?;
    mcx::vec_append_bytes(&mut v, s.as_bytes())?;
    Ok(core::str::from_utf8(v.leak()).expect("was UTF-8"))
}

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
        // The allow-all sentinel admits ANY role (the --profile test
        // posture), alone or among names; quoting does not demote it
        // (reserved-by-grammar, the $createdb convention).
        assert!(q("*", "mallory"));
        assert!(q("alice, *", "mallory"));
        assert!(q("\"*\"", "mallory"));
        // A literal asterisk is not a wildcard pattern: only the exact
        // one-char entry is the sentinel.
        assert!(!q("a*", "mallory"));
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
            spare: false,
        }
    }

    /// F2 strategy pick (pure half): the threshold sign disables, an
    /// unknown count stays file_copy, the boundary is inclusive
    /// (count >= threshold -> wal_log). RELEASE-effective plain asserts.
    #[test]
    fn strategy_pick_semantics() {
        use MintStrategy::*;
        // Disabled: -1 beats everything, even a huge count.
        assert_eq!(pick_strategy(None, -1), FileCopy);
        assert_eq!(pick_strategy(Some(0), -1), FileCopy);
        assert_eq!(pick_strategy(Some(1_000_000), -1), FileCopy);
        // Unknown count: conservative file_copy at any enabled threshold.
        assert_eq!(pick_strategy(None, 0), FileCopy);
        assert_eq!(pick_strategy(None, 50), FileCopy);
        // Inclusive boundary.
        assert_eq!(pick_strategy(Some(49), 50), FileCopy);
        assert_eq!(pick_strategy(Some(50), 50), WalLog);
        assert_eq!(pick_strategy(Some(51), 50), WalLog);
        // Threshold 0 = everything (with a known count) goes wal_log.
        assert_eq!(pick_strategy(Some(0), 0), WalLog);
        assert_eq!(pick_strategy(Some(231), 0), WalLog);
    }

    /// F2 mixed-batch checkpoint decision table, driven through the SAME
    /// per-member predicates the batch body folds over
    /// (member_fires_pre/member_owes_post): pre fires iff some file_copy
    /// member lacks its flush mark — RE-firing when a member's own probe
    /// ran after an earlier member's checkpoint (the probe-dirties-template
    /// hazard) — post fires iff any file_copy member minted, and an
    /// all-wal_log batch requests NEITHER — mark state irrelevant on
    /// wal_log members.
    #[test]
    fn mixed_batch_checkpoint_decision_table() {
        use MintStrategy::*;
        // (members as (strategy, marked, probed)) -> (n_pres, post): the
        // body's fold. probed => !marked (the probe clears the mark).
        let plan = |members: &[(MintStrategy, bool, bool)]| -> (usize, bool) {
            let mut pres = 0usize;
            let mut pre_checkpointed = false;
            let mut post = false;
            for &(s, marked, probed) in members {
                assert!(!(probed && marked), "probe clears the mark by construction");
                if member_fires_pre(s, marked, probed, pre_checkpointed) {
                    pres += 1;
                    pre_checkpointed = true;
                }
                if member_owes_post(s) {
                    post = true;
                }
            }
            (pres, post)
        };
        // All file_copy, nothing marked, no probes: ONE shared pre.
        assert_eq!(plan(&[(FileCopy, false, false), (FileCopy, false, false)]), (1, true));
        // All file_copy, all marked: pre skipped (the D3 skip), post owed.
        assert_eq!(plan(&[(FileCopy, true, false), (FileCopy, true, false)]), (0, true));
        // Marked + unmarked file_copy: one pre (the unmarked member), post.
        assert_eq!(plan(&[(FileCopy, true, false), (FileCopy, false, false)]), (1, true));
        // All wal_log: ZERO checkpoints, regardless of mark state.
        assert_eq!(plan(&[(WalLog, false, false), (WalLog, true, false)]), (0, false));
        assert_eq!(plan(&[]), (0, false));
        // Mixed: wal_log members never contribute a term; the file_copy
        // member alone decides.
        assert_eq!(plan(&[(WalLog, false, false), (FileCopy, true, false)]), (0, true));
        assert_eq!(plan(&[(WalLog, true, false), (FileCopy, false, false)]), (1, true));
        // THE HAZARD CELL (probe-dirties-template): a first-touch batch
        // over two distinct new templates probes both; member 2's probe
        // lands AFTER member 1's checkpoint, so it must RE-fire — two
        // pres, never one.
        assert_eq!(plan(&[(FileCopy, false, true), (FileCopy, false, true)]), (2, true));
        // Same template twice: the second member cache-hits (no probe) and
        // is marked by member 1 — steady one-pre shape.
        assert_eq!(plan(&[(FileCopy, false, true), (FileCopy, true, false)]), (1, true));
        // A probing WAL_LOG pick never fires (no file copy to protect).
        assert_eq!(plan(&[(WalLog, false, true), (WalLog, false, true)]), (0, false));
        // SIGHUP-flip shape: mark exists from an earlier era but the first
        // post-flip pick probes (mark cleared, probed) — re-pays even as
        // the batch's first member.
        assert_eq!(plan(&[(FileCopy, false, true)]), (1, true));
        // Wal_log-only mark state can never turn the pre on (the guard the
        // charter pins: the flush-mark machinery is file_copy-only).
        assert!(!member_pays_pre(WalLog, false));
        assert!(!member_owes_post(WalLog));
        // The probed flag can only ADD fires, never suppress one.
        assert!(member_fires_pre(FileCopy, false, true, true));
        assert!(member_fires_pre(FileCopy, false, false, false));
        assert!(!member_fires_pre(FileCopy, true, false, true));
        assert!(!member_fires_pre(FileCopy, false, false, true));
    }

    /// The no-cap-on-warm-handouts law (gather_batch, the tick-quantized-
    /// dispatch fix): N handout-eligible pending entries + M cold ones in
    /// ONE pass — every one of the N is served warm (no MINT_BATCH_MAX
    /// term), at most 32 of the M reach the cold batch, and an entry
    /// arriving DURING a handout round is served the same pass. Driven
    /// through injected snapshot/handout fakes over a pending-list model
    /// (the drop_one_gated convention: choreography separated from
    /// catalog I/O). RELEASE-effective plain asserts; no registry state.
    ///
    /// MUTATION SENSITIVITY: re-imposing the cap before the handout filter
    /// (the pre-fix service_pass shape: split_batch THEN service_handouts)
    /// caps warm service at 32 of the 96 and fails the served==64 assert.
    #[test]
    fn warm_handouts_uncapped_cold_capped() {
        use core::cell::RefCell;

        let warm = |i: u64| -> registry::PendingEnsure {
            registry::PendingEnsure {
                gen: i,
                name: format!("w{i}"),
                template: "tpl_default".to_string(),
                owner_name: "minter".to_string(),
                spare: false,
            }
        };
        // Cold = a template the pool holds no spare of.
        let cold = |i: u64| -> registry::PendingEnsure {
            registry::PendingEnsure {
                gen: 1000 + i,
                name: format!("c{i}"),
                template: "tpl_cold".to_string(),
                owner_name: "minter".to_string(),
                spare: false,
            }
        };

        // Pending-list model: 64 warm + 40 cold posted before the pass,
        // plus 2 warm stragglers that arrive DURING the first handout
        // round (pushed by the handout fake below, modeling posts landing
        // while the renames run).
        let pending: RefCell<Vec<registry::PendingEnsure>> = RefCell::new(
            (0..64).map(warm).chain((0..40).map(cold)).collect(),
        );
        let served: RefCell<Vec<String>> = RefCell::new(Vec::new());
        let rounds: RefCell<usize> = RefCell::new(0);

        let (batch, deferred) = gather_batch(
            || pending.borrow().clone(),
            |entries| {
                let round = {
                    let mut r = rounds.borrow_mut();
                    *r += 1;
                    *r
                };
                let mut rest = Vec::new();
                for p in entries {
                    if p.template == "tpl_default" {
                        served.borrow_mut().push(p.name.clone());
                        pending.borrow_mut().retain(|e| e.gen != p.gen);
                    } else {
                        rest.push(p);
                    }
                }
                if round == 1 {
                    // Mid-round arrivals (posted while round 1's renames
                    // committed): must be served THIS pass, not next tick.
                    pending.borrow_mut().push(warm(500));
                    pending.borrow_mut().push(warm(501));
                }
                Ok(rest)
            },
            MINT_BATCH_MAX,
        )
        .unwrap();

        // Every warm entry — including the two mid-pass arrivals — was
        // served, uncapped: 66 > 2 x MINT_BATCH_MAX.
        assert_eq!(served.borrow().len(), 66, "all warm entries served in ONE pass");
        assert!(
            served.borrow().iter().any(|n| n == "w500") && served.borrow().iter().any(|n| n == "w501"),
            "mid-pass arrivals served the same pass"
        );
        // The cold remainder alone feeds the capped batch path.
        assert_eq!(batch.len(), MINT_BATCH_MAX, "cold batch capped at MINT_BATCH_MAX");
        assert_eq!(deferred, 40 - MINT_BATCH_MAX, "only cold overflow defers");
        assert!(batch.iter().all(|p| p.template == "tpl_cold"), "no warm entry in the cold batch");
        assert_eq!(batch.first().unwrap().gen, 1000, "cold batch keeps oldest-first order");

        // Degenerate shapes: empty queue, and an all-cold queue (handout
        // serves nothing -> exactly one round, straight to the cap).
        let (b, d) = gather_batch(Vec::new, |e| Ok(e), MINT_BATCH_MAX).unwrap();
        assert!(b.is_empty() && d == 0);
        let all_cold: Vec<_> = (0..5).map(cold).collect();
        let calls = RefCell::new(0usize);
        let (b, d) = gather_batch(
            || all_cold.clone(),
            |e| {
                *calls.borrow_mut() += 1;
                Ok(e)
            },
            MINT_BATCH_MAX,
        )
        .unwrap();
        assert_eq!((b.len(), d), (5, 0));
        assert_eq!(*calls.borrow(), 1, "an all-cold round terminates the drain immediately");
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
