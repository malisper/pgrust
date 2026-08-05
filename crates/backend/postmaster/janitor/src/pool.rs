//! D3 warm pool, usage-keyed per-template redesign (item A ruling
//! 2026-08-05): the janitor keeps up to `pgrust.ephemeral_db_pool_size`
//! pre-minted spare clones warm PER TEMPLATE MINTED-FROM SINCE BOOT, and a
//! mint Ensure is satisfied by `ALTER DATABASE ... RENAME` of a spare of
//! the RIGHT template — catalog-only (the oid and the datadir path are
//! untouched; RenameDatabase heap-updates only datname, and its own
//! CountOtherDBBackends check discharges the zero-connection requirement
//! under its AccessExclusiveLock) — plus an owner assignment, taking the
//! connect-path mint cost from a FILE_COPY-plus-checkpoint-pair to a
//! sub-millisecond catalog transaction.
//!
//! USAGE KEYING: there is no default template and no per-template
//! configuration. The FIRST mint of a template registers it as pooled
//! (registry::note_pooled_template, called from the cold mint paths) and is
//! served cold; the replenisher maintains warmth thereafter. The global
//! MAX_SPARES cap binds when pooled-templates x pool_size exceeds it: fair
//! per-template targets share the cap, the remainder rotating with a
//! round-robin cursor, and the per-tick mint quota is allocated one spare
//! per template per rotation (the pure planners below, unit-tested).
//! Per-template drain: a pooled template that is dropped, rebuilt,
//! unsealed, or crosses a connectable edge — including via
//! `pgrust_seal_template` on a rebuilt successor — drains ITS spares only;
//! every other template's pool is untouched.
//!
//! Invariants inherited from the loop design:
//! - ALL pool mutations run inside the janitor loop (main_loop's
//!   serialization invariant): handout inside mint::service_pass, replenish
//!   as its own tick step between mint servicing and the reap pass. A
//!   backend-side rename would reintroduce every mint-racing-reap window
//!   the serialization closed.
//! - Spares are shielded from sweep/reap while listed
//!   (registry::spare_shields at both enumeration predicates and the
//!   pre-drop re-check); unlisted spares — post-restart leftovers (the
//!   registry is restart-lossy: the pool cold-starts empty), or spares
//!   dropped from the pool after a failed handout — are ordinary ephemeral
//!   candidates, which is the wanted self-heal.
//! - Handout failures NEVER fail the waiter: the entry falls through to the
//!   normal mint paths in the same service pass.
//!
//! Spare naming: `<prefix>spare_<seq>`, seq monotonic per postmaster
//! lifetime — a name that ever failed a handout (a squatter created it
//! first) is burned, never reused. The spare namespace
//! (`<prefix>spare_<digits>`) is RESERVED in the mint grammar
//! (grammar::parse_mint_name), so no client Ensure can ever collide with a
//! spare name. Spares are also minted ALLOW_CONNECTIONS false
//! (mint::build_createdb_stmt) and flipped connectable only inside the
//! handout transaction: a connectable spare could be entered, written, and
//! left between ticks, and the handout's occupancy check
//! (CountOtherDBBackends, point-in-time) would never see the visit — the
//! next waiter would receive a dirtied database as a fresh template clone.
//! A squatter still connected at handout time fails the rename's occupancy
//! check as before: the spare is dropped from the pool and the ordinary
//! reap path collects the database once idle.

use elog::elog as log_report;
use types_core::{InvalidOid, Oid};
use types_error::{PgResult, ERRCODE_DUPLICATE_DATABASE, LOG};

use crate::registry::{self, PendingEnsure, SpareEntry};

/// Per-tick replenish cap: bounds the janitor tick's latency contribution
/// (a replenish batch is one transaction around one checkpoint pair) and
/// stays under mint::MINT_BATCH_MAX. Deficits beyond it fill over
/// subsequent ~500ms ticks — replenish is background QoS, waiters never
/// depend on it.
pub(crate) const POOL_REPLENISH_MAX: usize = 8;

// ---------------------------------------------------------------------------
// Handout (called from mint::service_pass, entries with pending waiters).
// ---------------------------------------------------------------------------

enum HandoutVerdict {
    /// Renamed + chowned + connectable + committed: the entry is complete.
    Renamed,
    /// The spare's recorded template identity no longer matches the
    /// catalog (template repointed/rebuilt/unsealed, or its datallowconn
    /// changed, since the spare was minted): no handout for THIS entry —
    /// replenish (later this tick) drains that template's stale spares;
    /// other templates' handouts proceed.
    SpareStale,
    /// The requesting role vanished between post and service: fall through
    /// to the mint path, whose createdb surfaces the clean role error to
    /// the waiters.
    OwnerMissing,
    /// The REQUESTED name already exists (posted in the narrow
    /// lookup-miss-to-service window — mint_on_connect only fires on a
    /// lookup miss, so this needs the name to appear in between): the
    /// spare is untouched and the entry falls through to the mint path's
    /// idempotent success, instead of burning a doomed rename transaction
    /// into a contained duplicate_database report.
    NameExists,
}

/// Try to satisfy each entry from the warm pool; return the entries the
/// pool could not serve (they continue through the normal mint paths).
/// Handouts match by the ENTRY's template (registry::peek_spare keys on
/// template name): a request is only ever served from the RIGHT template's
/// spares. With no spares listed this is one registry probe.
pub(crate) fn service_handouts(batch: Vec<PendingEnsure>) -> PgResult<Vec<PendingEnsure>> {
    if !registry::any_spares() {
        return Ok(batch);
    }
    let mut rest: Vec<PendingEnsure> = Vec::with_capacity(batch.len());
    for p in batch {
        let Some(spare) = registry::peek_spare(&p.template) else {
            rest.push(p);
            continue;
        };
        let t0 = pg_clock::mono_ns();
        match handout_one(&spare, &p) {
            Ok(HandoutVerdict::Renamed) => {
                let ms = pg_clock::mono_ns().saturating_sub(t0) as f64 / 1e6;
                // Retire the spare entry AFTER the rename committed: its
                // old name no longer exists (shielding it is vacuous) and
                // the requested name is shielded by the Ensure entry
                // through Done + linger — no window where either name is
                // reap-exposed (and reap cannot interleave anyway: it runs
                // later in this same single-threaded tick).
                registry::remove_spare(&spare.name);
                let waiters = registry::complete_ensure(p.gen, Ok(()), pg_clock::mono_ns());
                // The handout witness line (race-suite pool phase greps the
                // spare->name pair and the ms figure), written BEFORE the
                // wakes like every other completion line.
                let _ = log_report(
                    LOG,
                    format!(
                        "pgrust ephemeral-db janitor: handed out warm spare \"{}\" as ephemeral \
                         database \"{}\" from template \"{}\" for role \"{}\" ({} waiter(s), \
                         rename+chown in {ms:.2} ms)",
                        spare.name,
                        p.name,
                        p.template,
                        p.owner_name,
                        waiters.len()
                    ),
                );
                crate::mint::wake_waiters(&waiters);
                // Shared-catalog churn (maint.rs): the handout transaction
                // heap-updated the spare's pg_database row (rename + chown
                // + allow_connections) — one lifecycle op. No prewarm
                // enqueue: the spare was touched at replenish and the
                // RENAME preserves oid and datadir, so its init file and
                // warm pages survive the handout — that is the point.
                registry::note_catalog_churn(1);
            }
            Ok(HandoutVerdict::SpareStale) => {
                // Per-template staleness: this template's requests go cold
                // this pass (replenish drains its spares later this tick);
                // OTHER templates' spares stay usable in this same loop.
                rest.push(p);
            }
            Ok(HandoutVerdict::OwnerMissing) => {
                rest.push(p);
            }
            Ok(HandoutVerdict::NameExists) => {
                // Silent by design: the mint path resolves the entry
                // idempotently this same pass; the spare stays listed.
                rest.push(p);
            }
            Err(e) => {
                // A failed rename/chown transaction. Keep the spare only
                // when the failure implicates the REQUESTED name
                // (duplicate_database: someone created it concurrently —
                // the spare itself is intact and the mint path resolves
                // the entry idempotently); every other cause (spare
                // vanished, spare occupied by a squatter) poisons the
                // spare: drop it from the pool and let the ordinary reap
                // path collect whatever is left once idle.
                let keep_spare = e.sqlstate() == ERRCODE_DUPLICATE_DATABASE;
                crate::main_loop::contain(
                    e,
                    &format!(
                        "handing out warm spare \"{}\" as ephemeral database \"{}\"",
                        spare.name, p.name
                    ),
                )?;
                if !keep_spare {
                    registry::remove_spare(&spare.name);
                }
                rest.push(p);
            }
        }
    }
    Ok(rest)
}

/// The handout transaction: template-identity re-check, then RENAME +
/// owner assignment through the PUBLIC dbcommands entries — both are
/// internally callable (no PreventInTransactionBlock in either entry nor
/// in their utility-dispatch arms; the only ALTER DATABASE guard is SET
/// TABLESPACE's), so no C-shaped entry changes are needed. On Err the
/// transaction is left open for the caller's contain() to abort — the
/// service_serial convention.
fn handout_one(spare: &SpareEntry, p: &PendingEnsure) -> PgResult<HandoutVerdict> {
    let cx = mcx::MemoryContext::new("pgrust janitor warm-pool handout");
    xact::StartTransactionCommand()?;
    let mcx = cx.mcx();

    // Requested-name existence, FIRST (mint_one's idempotency-precedence
    // order): a name that already exists must resolve on the idempotent
    // mint path — attempting the rename would only burn a doomed
    // transaction into a contained duplicate_database report. Reachable
    // via the narrow lookup-miss-to-service window only (mint_on_connect
    // fires solely on the backend's lookup miss).
    if pg_database::get_database_tuple_by_name(mcx, &p.name)?.is_some() {
        xact::CommitTransactionCommand()?;
        return Ok(HandoutVerdict::NameExists);
    }

    // Template identity AT HANDOUT (the scout's template-hash validation):
    // the entry names a template; the spare records the identity it was
    // minted from. Replenish runs AFTER mint servicing in the tick, so a
    // rebuild landing this same tick has not been drained yet — without
    // this check a stale spare (old template's content) could satisfy a
    // new-template request. datistemplate is required too: an unsealed
    // template refuses on the mint path, and a handout must not let a
    // request "succeed" where the mint path refuses. The datallowconn term
    // is the connectable-EDGE staleness gate (registry::SpareEntry's
    // template_connectable rationale): a spare copied while the template
    // was sealed must not serve after a window in which ordinary
    // connections could write the template (and vice versa) — replenish
    // drains such spares later this tick.
    let fresh = match pg_database::get_database_tuple_by_name(mcx, &p.template)? {
        Some(t) => {
            if !t.datistemplate || t.datallowconn {
                // Observed unsealed/connectable: the sealed-template flush
                // mark must not survive (the mint_batch_body skip
                // rationale) — this handout is an observation site like
                // preflight/mint_one/the replenish probe.
                registry::clear_template_flushed(t.oid);
            }
            t.oid == spare.template_oid
                && t.datistemplate
                && t.datallowconn == spare.template_connectable
        }
        None => false,
    };
    if !fresh {
        xact::CommitTransactionCommand()?;
        return Ok(HandoutVerdict::SpareStale);
    }

    // Resolve the requesting role BY NAME (C's ALTER DATABASE OWNER
    // resolves names via get_rolespec_oid): a role dropped between post
    // and service is a clean miss, not a dangling-oid catalog write.
    let owner = adt_acl::get_role_oid(&p.owner_name, true)?;
    if owner == InvalidOid {
        xact::CommitTransactionCommand()?;
        return Ok(HandoutVerdict::OwnerMissing);
    }

    // Catalog-only rename: preserves the oid (and with it the datadir path
    // and the spare's copied pg_db_role_setting rows, which are keyed by
    // oid — no re-copy needed); its own checks enforce the zero-connection
    // requirement (CountOtherDBBackends) under the database object's
    // AccessExclusiveLock, held to commit.
    dbcommands::RenameDatabase(mcx, &spare.name, &p.name)?;
    // AlterDatabaseOwner re-scans pg_database by the NEW name: without a
    // CommandCounterIncrement the scan misses the just-renamed tuple
    // (the mint_batch_body CCI precedent).
    xact::CommandCounterIncrement()?;
    // Owner assignment: datdba + datacl (aclnewowner; spares are
    // janitor-owned with NULL datacl, so the acl branch is a no-op) +
    // pg_shdepend. The janitor's superuser session passes every check.
    dbcommands::AlterDatabaseOwner(mcx, &p.name, owner)?;
    // Spares are minted ALLOW_CONNECTIONS false (content-poisoning
    // defense, mint::build_createdb_stmt): flip connectability ON in this
    // same transaction — committed before the waiters wake, so the
    // waiter's CheckMyDatabase sees datallowconn = true. CCI first:
    // AlterDatabase re-scans pg_database by name (the same
    // multi-statement-transaction convention as the rename->chown CCI).
    xact::CommandCounterIncrement()?;
    let alter = crate::mint::build_alterdb_allowconn_stmt(mcx, &p.name, true)?;
    dbcommands::AlterDatabase(mcx, &alter, false)?;
    xact::CommitTransactionCommand()?;
    Ok(HandoutVerdict::Renamed)
}

// ---------------------------------------------------------------------------
// Replenish (its own tick step: after mint servicing, before the reap pass
// — the ordering rationale lives on the main_loop call site).
// ---------------------------------------------------------------------------

/// Identity/staleness probe of one pooled template.
struct TplProbe {
    oid: Oid,
    datistemplate: bool,
    datallowconn: bool,
}

/// Top each pooled template's pool up toward its fair target (capped per
/// tick), dropping stale spares first. Errors propagate to the tick's
/// contain().
pub(crate) fn replenish_pass(prefix: &str) -> PgResult<()> {
    let pool_size = crate::ephemeral_db_pool_size().max(0) as usize;
    let pooled = registry::pooled_templates();
    // Feature-off / nothing-pooled fast path: no catalog probe.
    if (pool_size == 0 || pooled.is_empty()) && !registry::any_spares() {
        return Ok(());
    }

    let (probes, janitor_role) = probe_templates_and_self(&pooled)?;

    // De-list pooled templates whose catalog row is GONE (dropped; a
    // rebuild under the same name keeps the listing — the new oid simply
    // drains the old spares below and replenish re-fills from the new
    // row). An unsealed-but-present template stays listed with no valid
    // identity: its spares drain, nothing mints, and a re-seal re-warms
    // without waiting for a fresh cold mint.
    for (name, probe) in pooled.iter().zip(&probes) {
        if probe.is_none() && registry::remove_pooled_template(name) {
            let _ = log_report(
                LOG,
                format!(
                    "pgrust ephemeral-db janitor: template \"{name}\" is gone; no longer \
                     pooling spares of it"
                ),
            );
        }
    }

    // The identities the pool may hold spares of (one per pooled template
    // with a live SEALED row and a non-zero pool): everything else is
    // stale. The datallowconn term drains spares across a connectable
    // EDGE (either direction) while keeping a pool whose template's
    // datallowconn is STABLE — a permanently connectable (template1-shape)
    // template must not mint-and-drain its whole pool every tick; its
    // accepted content-staleness residual is documented on
    // SpareEntry::template_connectable.
    let mut identities: Vec<(String, Oid, bool)> = Vec::new();
    let mut live: Vec<(&str, &TplProbe)> = Vec::new();
    if pool_size > 0 {
        for (name, probe) in pooled.iter().zip(&probes) {
            if let Some(t) = probe {
                if t.datistemplate {
                    identities.push((name.clone(), t.oid, t.datallowconn));
                    live.push((name.as_str(), t));
                }
            }
        }
    }

    // Invalidation: drain-and-drop spares that no longer match THEIR
    // template's live identity — per-template by construction (a drained
    // template's siblings keep matching their own identities). Drained
    // entries stop shielding (drain first, then drop — the gate re-checks
    // shields), and the drop is immediate via the batch drop path (one
    // checkpoint for the lot) rather than waiting out a reap grace.
    let stale = registry::drain_stale_spares(&identities);
    if !stale.is_empty() {
        let names: Vec<&str> = stale.iter().map(|s| s.name.as_str()).collect();
        let _ = log_report(
            LOG,
            format!(
                "pgrust ephemeral-db janitor: dropping {} stale warm spare(s): {}",
                stale.len(),
                names.join(", ")
            ),
        );
        let victims: Vec<(Oid, &str)> = stale.iter().map(|s| (s.oid, s.name.as_str())).collect();
        crate::main_loop::drop_batch(&victims, None)?;
    }
    if live.is_empty() {
        return Ok(());
    }
    let Some(janitor_role) = janitor_role else {
        // The janitor's own role must resolve; skipping a tick is the safe
        // containment (next tick retries).
        return Ok(());
    };

    // Fair per-template targets under the global cap, remainder rotated by
    // the round-robin cursor (advanced once per pass so the extra slots —
    // and the mint order below — visit every template over successive
    // ticks when the cap binds).
    let rr = registry::advance_pool_rr(live.len());
    let targets = per_template_targets(pool_size, registry::max_spares_cap(), live.len(), rr);
    let haves: Vec<usize> = live
        .iter()
        .map(|(name, t)| registry::spare_count(name, t.oid))
        .collect();

    // Excess drain, per template: pool_size shrank under the live pool
    // (SIGHUP), or a newly pooled template shrank this one's fair share
    // under a binding cap — surplus spares would otherwise sit shielded
    // forever.
    let mut drained_excess = false;
    for (i, (name, t)) in live.iter().enumerate() {
        if haves[i] > targets[i] {
            let excess = registry::take_excess_spares(name, t.oid, targets[i]);
            if excess.is_empty() {
                continue;
            }
            drained_excess = true;
            let names: Vec<&str> = excess.iter().map(|s| s.name.as_str()).collect();
            let _ = log_report(
                LOG,
                format!(
                    "pgrust ephemeral-db janitor: dropping {} excess warm spare(s) of template \
                     \"{name}\" (target {}): {}",
                    excess.len(),
                    targets[i],
                    names.join(", ")
                ),
            );
            let victims: Vec<(Oid, &str)> =
                excess.iter().map(|s| (s.oid, s.name.as_str())).collect();
            crate::main_loop::drop_batch(&victims, None)?;
        }
    }
    if drained_excess {
        return Ok(());
    }

    let quotas = mint_quotas(&targets, &haves, POOL_REPLENISH_MAX);
    if quotas.iter().all(|&q| q == 0) {
        return Ok(());
    }
    // Waiters outrank refill, EXTENDED to the dispatch shadow (the
    // tick-quantized-dispatch fix's replenish lever): a replenish cycle
    // blocks this single-threaded loop for its whole copy + checkpoint
    // wall (~100ms+ per 8-member batch), so an Ensure arriving mid-cycle
    // pays that wall in connect latency — for zero pool-health gain when
    // the pool is deep. While mint traffic landed within the last tick AND
    // the pool sits at/above HALF its total target, defer the top-up (the
    // deficit simply waits; refill is background QoS by charter). Below
    // half the refill proceeds regardless — sustained load must never
    // starve the pool — and that residual shadow under drain pressure is
    // accepted and documented here.
    if should_defer_refill(
        targets.iter().sum(),
        haves.iter().sum(),
        pg_clock::mono_ns(),
        registry::last_ensure_post_ns(),
    ) {
        return Ok(());
    }

    // Mint specs under burned-forever monotonic names. The PendingEnsure
    // shape is reused so the batch/serial mint bodies are shared verbatim
    // with the Ensure path (gen 0: these have no registry entry and no
    // waiters). Batch-eligibility law per template (the Ensure servicing
    // law): only datallowconn = false templates may share the batch's
    // widened torn-copy window; connectable (template1-shape) templates
    // replenish serially, one checkpoint pair per spare.
    let mut batch_specs: Vec<(PendingEnsure, &str, &TplProbe)> = Vec::new();
    let mut serial_specs: Vec<(PendingEnsure, &str, &TplProbe)> = Vec::new();
    for (i, (name, t)) in live.iter().enumerate() {
        for _ in 0..quotas[i] {
            let spare_name = format!("{prefix}spare_{}", registry::next_spare_seq());
            if spare_name.len() > crate::grammar::MAX_NAME_BYTES {
                // A prefix long enough to overflow spare names would mint
                // truncated datnames the registry could never match
                // (shields and handouts would silently miss). Refuse
                // loudly, ONCE per postmaster lifetime (the registry
                // latch): the condition is permanent — the prefix is
                // PGC_POSTMASTER and the seq only grows.
                if registry::pool_name_overflow_log_once() {
                    let _ = log_report(
                        LOG,
                        format!(
                            "pgrust ephemeral-db janitor: pgrust.ephemeral_db_prefix is too \
                             long for warm-pool spare names ({} > {} bytes); the pool stays \
                             empty",
                            spare_name.len(),
                            crate::grammar::MAX_NAME_BYTES
                        ),
                    );
                }
                return Ok(());
            }
            let spec = PendingEnsure {
                gen: 0,
                name: spare_name,
                template: name.to_string(),
                owner_name: janitor_role.clone(),
                spare: true,
            };
            if t.datallowconn {
                serial_specs.push((spec, name, t));
            } else {
                batch_specs.push((spec, name, t));
            }
        }
    }

    let mut minted: Vec<(String, Oid, &str, &TplProbe)> = Vec::new();
    if batch_specs.len() >= 2 {
        let specs: Vec<PendingEnsure> = batch_specs.iter().map(|(s, ..)| s.clone()).collect();
        for (name, oid) in replenish_batch(&specs)? {
            let &(_, tpl_name, probe) = batch_specs
                .iter()
                .find(|(s, ..)| s.name == name)
                .expect("minted spare tracks its spec");
            minted.push((name, oid, tpl_name, probe));
        }
    } else {
        serial_specs.append(&mut batch_specs);
    }
    if !serial_specs.is_empty() {
        let specs: Vec<PendingEnsure> = serial_specs.iter().map(|(s, ..)| s.clone()).collect();
        for (name, oid) in replenish_serial(&specs)? {
            let &(_, tpl_name, probe) = serial_specs
                .iter()
                .find(|(s, ..)| s.name == name)
                .expect("minted spare tracks its spec");
            minted.push((name, oid, tpl_name, probe));
        }
    }

    let n = minted.len();
    // Shared-catalog churn (maint.rs): one pg_database insert (plus
    // shdepend/setting rows) per minted spare.
    registry::note_catalog_churn(n as u64);
    let prewarm = crate::ephemeral_db_prewarm();
    for (name, oid, tpl_name, probe) in minted {
        // Post-mint touch (prewarm.rs): spares are THE prewarm payoff —
        // ALLOW_CONNECTIONS false means no client session ever warms them,
        // so without the touch every handout's first client session pays
        // the fresh-database catalog bootstrap. Enqueued before add_spare
        // is irrelevant to ordering (dispatch runs as a later tick step);
        // the touch worker enters via BGWORKER_BYPASS_ALLOWCONN.
        if prewarm {
            let _ = registry::enqueue_touch(&name, oid);
        }
        // Registered (and thereby shielded) BEFORE this tick's reap pass
        // enumerates: the tick order makes a fresh zero-connection spare
        // never reap-visible unshielded.
        registry::add_spare(SpareEntry {
            name,
            oid,
            template_name: tpl_name.to_string(),
            template_oid: probe.oid,
            template_connectable: probe.datallowconn,
        });
    }
    if n > 0 {
        let names: Vec<&str> = live.iter().map(|(name, _)| *name).collect();
        let _ = log_report(
            LOG,
            format!(
                "pgrust ephemeral-db janitor: replenished warm pool with {n} spare(s) across \
                 {} pooled template(s) ({}) — {}/{} total",
                live.len(),
                names.join(", "),
                registry::total_spares(),
                targets.iter().sum::<usize>()
            ),
        );
    }
    Ok(())
}

/// Fair per-template spare targets (pure, unit-tested): each of `n` pooled
/// templates gets `pool_size`, unless the global `cap` binds — then the cap
/// is shared as floor(cap/n) each with the remainder's extra slot rotated
/// by `rr` (so cap-bound pools even out over successive passes rather than
/// permanently favoring registration order).
fn per_template_targets(pool_size: usize, cap: usize, n: usize, rr: usize) -> Vec<usize> {
    if n == 0 {
        return Vec::new();
    }
    if pool_size.saturating_mul(n) <= cap {
        return vec![pool_size; n];
    }
    let base = cap / n;
    let extra = cap % n;
    (0..n)
        .map(|i| {
            // The `extra` slots go to the `extra` templates at rotated
            // positions rr, rr+1, ... (mod n).
            let rotated = (i + n - rr % n) % n;
            let t = base + usize::from(rotated < extra);
            t.min(pool_size)
        })
        .collect()
}

/// Per-tick mint quotas (pure, unit-tested): allocate `per_tick_cap` mints
/// across the templates' deficits ONE SPARE PER TEMPLATE PER ROTATION —
/// the round-robin the item-A ruling names — so a cap-bound tick advances
/// every deficit instead of filling template 0 first.
fn mint_quotas(targets: &[usize], haves: &[usize], per_tick_cap: usize) -> Vec<usize> {
    let n = targets.len();
    let mut quotas = vec![0usize; n];
    let mut deficits: Vec<usize> = targets
        .iter()
        .zip(haves)
        .map(|(&t, &h)| t.saturating_sub(h))
        .collect();
    let mut budget = per_tick_cap;
    while budget > 0 && deficits.iter().any(|&d| d > 0) {
        for i in 0..n {
            if budget == 0 {
                break;
            }
            if deficits[i] > 0 {
                deficits[i] -= 1;
                quotas[i] += 1;
                budget -= 1;
            }
        }
    }
    quotas
}

/// Recent-traffic window for the refill deferral: one tick — traffic older
/// than a full tick means at least one whole quiet turn passed, and the
/// refill resumes at POOL_REPLENISH_MAX per tick.
const REFILL_DEFER_TRAFFIC_NS: u64 = 500 * 1_000_000;

/// Pure half of the refill deferral (unit-tested): defer iff the pool is
/// at/above HALF its total target (integer arithmetic: have*2 >= target)
/// and an Ensure post landed within the last tick. `last_post_ns == 0` =
/// never.
fn should_defer_refill(total_target: usize, have: usize, now_ns: u64, last_post_ns: u64) -> bool {
    have * 2 >= total_target
        && last_post_ns != 0
        && now_ns.saturating_sub(last_post_ns) < REFILL_DEFER_TRAFFIC_NS
}

/// One read-only transaction: probe every pooled template's identity plus
/// the janitor's own role name (spare owner).
fn probe_templates_and_self(
    pooled: &[String],
) -> PgResult<(Vec<Option<TplProbe>>, Option<String>)> {
    let cx = mcx::MemoryContext::new("pgrust janitor warm-pool probe");
    xact::StartTransactionCommand()?;
    let mcx = cx.mcx();
    let mut probes = Vec::with_capacity(pooled.len());
    for name in pooled {
        let probe = match pg_database::get_database_tuple_by_name(mcx, name)? {
            Some(t) => {
                if !t.datistemplate || t.datallowconn {
                    // Observed unsealed/connectable: invalidate the
                    // sealed-template flush mark (mint_batch_body's skip
                    // rationale) — with the pool ON this probe runs every
                    // tick, tightening the observation mesh.
                    registry::clear_template_flushed(t.oid);
                }
                Some(TplProbe {
                    oid: t.oid,
                    datistemplate: t.datistemplate,
                    datallowconn: t.datallowconn,
                })
            }
            None => None,
        };
        probes.push(probe);
    }
    let role = miscinit::GetUserNameFromId(mcx, miscinit::GetUserId(), true)?
        .map(|s| s.as_str().to_string());
    xact::CommitTransactionCommand()?;
    Ok((probes, role))
}

/// Replenish through the shared batch-mint transaction (one checkpoint
/// pair for the lot). Spares have no waiters, so failure handling is
/// simpler than service_batch's: contain, clean pre-commit orphans, and
/// let the NEXT tick retry from a fresh deficit count (under fresh burned
/// names) — no serial fallback needed for background refill.
fn replenish_batch(specs: &[PendingEnsure]) -> PgResult<Vec<(String, Oid)>> {
    use crate::mint::{BatchFailure, BatchOutcome, CreatedDb};
    let mut created: Vec<CreatedDb> = Vec::new();
    match crate::mint::mint_batch(specs, &mut created) {
        Ok(outcomes) => {
            let mut minted = Vec::with_capacity(specs.len());
            let mut oids = created.into_iter();
            for (spec, o) in specs.iter().zip(outcomes) {
                match o {
                    BatchOutcome::Minted { .. } => {
                        let oid =
                            oids.next().expect("created oids track Minted outcomes").oid;
                        minted.push((spec.name.clone(), oid));
                    }
                    // A foreign database squatting a spare name is NEVER
                    // adopted (unknown content/owner); the burned seq means
                    // the name is simply skipped forever.
                    BatchOutcome::FoundExisting => {
                        let _ = log_report(
                            LOG,
                            format!(
                                "pgrust ephemeral-db janitor: warm-spare name \"{}\" already \
                                 exists (not pool-minted); skipping it permanently",
                                spec.name
                            ),
                        );
                    }
                    // Template vanished/unsealed inside the batch window:
                    // the entry failed alone; the next tick's probe
                    // re-decides that template's fate.
                    BatchOutcome::Refused(e) => {
                        crate::mint::report_contained_refusal(&e, &spec.name);
                    }
                    // Template turned connectable inside the batch window:
                    // no work was done; the next tick's probe routes that
                    // template's refill serially.
                    BatchOutcome::DeferSerial => {}
                }
            }
            Ok(minted)
        }
        Err(failure) => {
            let (e, pre_commit) = match failure {
                BatchFailure::BeforeCommit(e) => (e, true),
                BatchFailure::AtCommit(e) => (e, false),
            };
            crate::main_loop::contain(e, "warm-pool batch mint")?;
            if pre_commit {
                crate::mint::cleanup_orphaned_datadirs(&created);
            }
            Ok(Vec::new())
        }
    }
}

/// Serial replenish (connectable template, or a lone batch spec): the
/// C-shaped createdb per spare, own checkpoint pair each.
fn replenish_serial(specs: &[PendingEnsure]) -> PgResult<Vec<(String, Oid)>> {
    let mut minted = Vec::with_capacity(specs.len());
    for spec in specs {
        match crate::mint::mint_one(spec) {
            Ok(Some(oid)) => minted.push((spec.name.clone(), oid)),
            Ok(None) => {
                let _ = log_report(
                    LOG,
                    format!(
                        "pgrust ephemeral-db janitor: warm-spare name \"{}\" already exists \
                         (not pool-minted); skipping it permanently",
                        spec.name
                    ),
                );
            }
            Err(e) => {
                crate::main_loop::contain(
                    e,
                    &format!("minting warm spare \"{}\"", spec.name),
                )?;
                // Stop this tick's refill; the next tick retries the
                // remaining deficit under fresh names.
                break;
            }
        }
    }
    Ok(minted)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Fair per-template targets under the global cap (RELEASE-effective
    /// plain asserts; no registry state touched).
    #[test]
    fn per_template_target_math() {
        // Cap slack: everyone gets pool_size.
        assert_eq!(per_template_targets(8, 4096, 3, 0), [8, 8, 8]);
        assert_eq!(per_template_targets(0, 4096, 2, 0), [0, 0]);
        assert!(per_template_targets(8, 4096, 0, 0).is_empty());
        // Cap binds evenly: 2 x 8 > 10 -> 5 each.
        assert_eq!(per_template_targets(8, 10, 2, 0), [5, 5]);
        // Cap binds with remainder: 3 x 8 > 10 -> base 3, one extra —
        // rotated by rr so the favored template changes pass to pass.
        assert_eq!(per_template_targets(8, 10, 3, 0), [4, 3, 3]);
        assert_eq!(per_template_targets(8, 10, 3, 1), [3, 4, 3]);
        assert_eq!(per_template_targets(8, 10, 3, 2), [3, 3, 4]);
        assert_eq!(per_template_targets(8, 10, 3, 3), [4, 3, 3], "rr wraps");
        // The per-template pool_size still ceilings a fair share.
        assert_eq!(per_template_targets(2, 10, 3, 0), [2, 2, 2]);
        // Total never exceeds the cap when it binds.
        for rr in 0..5 {
            let t = per_template_targets(8, 10, 3, rr);
            assert_eq!(t.iter().sum::<usize>(), 10);
        }
    }

    /// The per-tick round-robin mint allocation (the item-A ruling's
    /// "replenish round-robins under the cap").
    #[test]
    fn mint_quota_round_robin() {
        // One template: plain deficit under the tick cap.
        assert_eq!(mint_quotas(&[8], &[8], POOL_REPLENISH_MAX), [0]);
        assert_eq!(mint_quotas(&[8], &[5], POOL_REPLENISH_MAX), [3]);
        assert_eq!(mint_quotas(&[64], &[0], POOL_REPLENISH_MAX), [POOL_REPLENISH_MAX]);
        // Over-full (target shrank): nothing to MINT (the excess-drain
        // branch drops the surplus separately).
        assert_eq!(mint_quotas(&[4], &[8], POOL_REPLENISH_MAX), [0]);
        // TWO cold templates share the tick budget one-per-rotation:
        // never 8 to the first and 0 to the second.
        assert_eq!(mint_quotas(&[8, 8], &[0, 0], 8), [4, 4]);
        assert_eq!(mint_quotas(&[8, 8, 8], &[0, 0, 0], 8), [3, 3, 2]);
        // Uneven deficits: the rotation skips full templates.
        assert_eq!(mint_quotas(&[8, 8], &[7, 0], 8), [1, 7]);
        // Budget exceeds deficits: exact top-up.
        assert_eq!(mint_quotas(&[2, 2], &[1, 1], 8), [1, 1]);
        assert!(mint_quotas(&[], &[], 8).is_empty());
    }

    /// The refill deferral (pure half): defer iff pool at/above half its
    /// total target AND traffic within the window.
    #[test]
    fn refill_deferral_semantics() {
        let now: u64 = 10_000_000_000;
        let fresh = now - 1; // just-landed post
        let stale = now - REFILL_DEFER_TRAFFIC_NS; // exactly aged out
        // Deep pool + fresh traffic: defer (the dispatch-shadow case).
        assert!(should_defer_refill(128, 128, now, fresh));
        assert!(should_defer_refill(128, 64, now, fresh), "half target is inclusive");
        // Below half: refill regardless of traffic (pool health outranks).
        assert!(!should_defer_refill(128, 63, now, fresh));
        assert!(!should_defer_refill(8, 0, now, fresh), "cold fill never defers");
        // Quiet (or never-posted) traffic: refill proceeds.
        assert!(!should_defer_refill(128, 128, now, stale));
        assert!(!should_defer_refill(128, 128, now, 0));
        // Odd target boundary: have*2 >= target (integer half-up).
        assert!(should_defer_refill(7, 4, now, fresh));
        assert!(!should_defer_refill(7, 3, now, fresh));
    }
}
