//! D3 warm pool (docs/design/test-views.md warm-pool addendum): the janitor
//! keeps `pgrust.ephemeral_db_pool_size` pre-minted spare clones of the
//! DEFAULT template warm, and a default-template mint Ensure is satisfied by
//! `ALTER DATABASE ... RENAME` of a spare — catalog-only (the oid and the
//! datadir path are untouched; RenameDatabase heap-updates only datname, and
//! its own CountOtherDBBackends check discharges the zero-connection
//! requirement under its AccessExclusiveLock) — plus an owner assignment,
//! taking the connect-path mint cost from a FILE_COPY-plus-checkpoint-pair
//! to a sub-millisecond catalog transaction.
//!
//! v1 SCOPE (recorded decision): the pool exists for the DEFAULT template
//! only — `pgrust.ephemeral_db_default_template` names the one identity the
//! replenisher mints. Named-template pools are out of scope. (A
//! template-form request that happens to NAME the default template still
//! takes a spare: the handout matches by template identity, and a spare of
//! that identity is exactly what such a request asked for.)
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
//! Adoption-guard interaction, documented deliberately: with the pool ON,
//! prefix-matching survivors (the spares) always exist across restarts, so
//! a lost/changed marker now deterministically starts the janitor PAUSED —
//! not a wedge (unpause runs the deferred sweep, which drops the
//! unregistered leftovers, and replenish re-mints), but a behavior change
//! an operator will see.
//!
//! Spare naming: `<prefix>spare_<seq>`, seq monotonic per postmaster
//! lifetime — a name that ever failed a handout (a squatter created it
//! first) is burned, never reused. The spare namespace
//! (`<prefix>spare_<digits>`) is RESERVED in the mint grammar
//! (grammar::parse_mint_name), so no client Ensure can ever collide with a
//! spare name — without the reservation, an Ensure racing a replenish
//! could complete idempotently ON a listed spare (double-booked: the pool
//! would later rename the client's database out from under it). Spares are
//! also minted ALLOW_CONNECTIONS false (mint::build_createdb_stmt) and
//! flipped connectable only inside the handout transaction: a connectable
//! spare could be entered, written, and left between ticks, and the
//! handout's occupancy check (CountOtherDBBackends, point-in-time) would
//! never see the visit — the next waiter would receive a dirtied database
//! as a fresh template clone. A squatter still connected at handout time
//! fails the rename's occupancy check as before: the spare is dropped from
//! the pool and the ordinary reap path collects the database once idle.

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
    /// changed, since the spare was minted): no handout, and the pass
    /// stops consulting the pool — replenish (later this tick) drains the
    /// stale spares.
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
/// With no spares listed this is one registry probe.
pub(crate) fn service_handouts(batch: Vec<PendingEnsure>) -> PgResult<Vec<PendingEnsure>> {
    if !registry::any_spares() {
        return Ok(batch);
    }
    let mut rest: Vec<PendingEnsure> = Vec::with_capacity(batch.len());
    let mut pool_usable = true;
    for p in batch {
        if !pool_usable {
            rest.push(p);
            continue;
        }
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
            }
            Ok(HandoutVerdict::SpareStale) => {
                pool_usable = false;
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

/// Identity/staleness probe of the default template plus the janitor's own
/// role name (spare owner), in one read-only transaction.
struct TplProbe {
    oid: Oid,
    datistemplate: bool,
    datallowconn: bool,
}

/// Top the pool up to `pgrust.ephemeral_db_pool_size` (capped per tick),
/// dropping stale spares first. Errors propagate to the tick's contain().
pub(crate) fn replenish_pass(prefix: &str) -> PgResult<()> {
    let pool_size = crate::ephemeral_db_pool_size().max(0) as usize;
    let tpl_name = crate::ephemeral_db_default_template();
    // Feature-off fast path: no catalog probe, one registry lock.
    if pool_size == 0 && !registry::any_spares() {
        return Ok(());
    }

    let (probe, janitor_role) = probe_template_and_self(&tpl_name)?;

    // The one identity the pool may hold spares of. None = no valid pool
    // (feature off, template unset/missing/unsealed): every spare is
    // stale. The datallowconn term drains spares across a connectable
    // EDGE (either direction) while keeping a pool whose template's
    // datallowconn is STABLE — a permanently connectable (template1-shape)
    // template must not mint-and-drain its whole pool every tick, and its
    // accepted content-staleness residual is documented on
    // SpareEntry::template_connectable and addendum item 6.
    let identity: Option<(&str, Oid, bool)> = match &probe {
        Some(t) if pool_size > 0 && t.datistemplate => {
            Some((tpl_name.as_str(), t.oid, t.datallowconn))
        }
        _ => None,
    };

    // Invalidation: drain-and-drop spares that no longer match. Drained
    // entries stop shielding (drain first, then drop — the gate re-checks
    // shields), and the drop is immediate via the batch drop path (one
    // checkpoint for the lot) rather than waiting out a reap grace.
    let stale = registry::drain_stale_spares(identity);
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

    let Some((_, tpl_oid, _)) = identity else {
        return Ok(());
    };
    let probe = probe.expect("identity implies a probed template");
    let Some(janitor_role) = janitor_role else {
        // The janitor's own role must resolve; skipping a tick is the safe
        // containment (next tick retries).
        return Ok(());
    };

    let have = registry::spare_count(&tpl_name, tpl_oid);
    if have > pool_size {
        // pool_size shrank under the live pool (SIGHUP): drain the excess —
        // the GUC contract says shrinking drains, and surplus spares would
        // otherwise sit shielded forever.
        let excess = registry::take_excess_spares(&tpl_name, tpl_oid, pool_size);
        let names: Vec<&str> = excess.iter().map(|s| s.name.as_str()).collect();
        let _ = log_report(
            LOG,
            format!(
                "pgrust ephemeral-db janitor: dropping {} excess warm spare(s) \
                 (pool_size {pool_size}): {}",
                excess.len(),
                names.join(", ")
            ),
        );
        let victims: Vec<(Oid, &str)> = excess.iter().map(|s| (s.oid, s.name.as_str())).collect();
        crate::main_loop::drop_batch(&victims, None)?;
        return Ok(());
    }
    let want = replenish_quota(pool_size, have, POOL_REPLENISH_MAX);
    if want == 0 {
        return Ok(());
    }

    // Mint specs under burned-forever monotonic names. The PendingEnsure
    // shape is reused so the batch/serial mint bodies are shared verbatim
    // with the Ensure path (gen 0: these have no registry entry and no
    // waiters).
    let mut specs: Vec<PendingEnsure> = Vec::with_capacity(want);
    for _ in 0..want {
        let name = format!("{prefix}spare_{}", registry::next_spare_seq());
        if name.len() > crate::grammar::MAX_NAME_BYTES {
            // A prefix long enough to overflow spare names would mint
            // truncated datnames the registry could never match (shields
            // and handouts would silently miss). Refuse loudly, ONCE per
            // postmaster lifetime (the registry latch): the condition is
            // permanent — the prefix is PGC_POSTMASTER and the seq only
            // grows — so an unlatched line would repeat every deficit
            // tick, ~2 lines/s forever.
            if registry::pool_name_overflow_log_once() {
                let _ = log_report(
                    LOG,
                    format!(
                        "pgrust ephemeral-db janitor: pgrust.ephemeral_db_prefix is too long for \
                         warm-pool spare names ({} > {} bytes); the pool stays empty",
                        name.len(),
                        crate::grammar::MAX_NAME_BYTES
                    ),
                );
            }
            return Ok(());
        }
        specs.push(PendingEnsure {
            gen: 0,
            name,
            template: tpl_name.clone(),
            owner_name: janitor_role.clone(),
            spare: true,
        });
    }

    // Batch when the batch-eligibility law admits it (>= 2 members and a
    // datallowconn = false template — the same law as Ensure servicing);
    // a connectable default template (the template1 shape) replenishes
    // serially, one checkpoint pair per spare — worth knowing when sizing
    // the pool, recorded in the addendum.
    let minted: Vec<(String, Oid)> = if specs.len() >= 2 && !probe.datallowconn {
        replenish_batch(&specs)?
    } else {
        replenish_serial(&specs)?
    };

    let n = minted.len();
    for (name, oid) in minted {
        // Registered (and thereby shielded) BEFORE this tick's reap pass
        // enumerates: the tick order makes a fresh zero-connection spare
        // never reap-visible unshielded.
        registry::add_spare(SpareEntry {
            name,
            oid,
            template_name: tpl_name.clone(),
            template_oid: tpl_oid,
            template_connectable: probe.datallowconn,
        });
    }
    if n > 0 {
        let _ = log_report(
            LOG,
            format!(
                "pgrust ephemeral-db janitor: replenished warm pool with {n} spare(s) from \
                 template \"{tpl_name}\" ({}/{pool_size})",
                have + n
            ),
        );
    }
    Ok(())
}

/// Pure deficit math (unit-tested): how many spares to mint this tick.
fn replenish_quota(pool_size: usize, have: usize, per_tick_cap: usize) -> usize {
    pool_size.saturating_sub(have).min(per_tick_cap)
}

fn probe_template_and_self(tpl_name: &str) -> PgResult<(Option<TplProbe>, Option<String>)> {
    let cx = mcx::MemoryContext::new("pgrust janitor warm-pool probe");
    xact::StartTransactionCommand()?;
    let mcx = cx.mcx();
    let tpl = if tpl_name.is_empty() {
        None
    } else {
        match pg_database::get_database_tuple_by_name(mcx, tpl_name)? {
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
        }
    };
    let role = miscinit::GetUserNameFromId(mcx, miscinit::GetUserId(), true)?
        .map(|s| s.as_str().to_string());
    xact::CommitTransactionCommand()?;
    Ok((tpl, role))
}

/// Replenish through the shared batch-mint transaction (one checkpoint
/// pair for the lot). Spares have no waiters, so failure handling is
/// simpler than service_batch's: contain, clean pre-commit orphans, and
/// let the NEXT tick retry from a fresh deficit count (under fresh burned
/// names) — no serial fallback needed for background refill.
fn replenish_batch(specs: &[PendingEnsure]) -> PgResult<Vec<(String, Oid)>> {
    use crate::mint::{BatchFailure, BatchOutcome};
    let mut created: Vec<Oid> = Vec::new();
    match crate::mint::mint_batch(specs, &mut created) {
        Ok(outcomes) => {
            let mut minted = Vec::with_capacity(specs.len());
            let mut oids = created.into_iter();
            for (spec, o) in specs.iter().zip(outcomes) {
                match o {
                    BatchOutcome::Minted { .. } => {
                        let oid = oids.next().expect("created oids track Minted outcomes");
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
                    // re-decides the pool's fate.
                    BatchOutcome::Refused(e) => {
                        crate::mint::report_contained_refusal(&e, &spec.name);
                    }
                    // Template turned connectable inside the batch window:
                    // no work was done; the next tick's probe routes the
                    // refill serially.
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

/// Serial replenish (connectable template, or a deficit of one): the
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

    /// The pure deficit math (RELEASE-effective plain asserts; no registry
    /// state touched).
    #[test]
    fn replenish_quota_math() {
        // Full pool: nothing to do.
        assert_eq!(replenish_quota(8, 8, POOL_REPLENISH_MAX), 0);
        // Over-full (pool_size shrank): nothing to MINT (the excess-drain
        // branch in replenish_pass drops the surplus separately).
        assert_eq!(replenish_quota(4, 8, POOL_REPLENISH_MAX), 0);
        // Deficit under the cap: exact top-up.
        assert_eq!(replenish_quota(8, 5, POOL_REPLENISH_MAX), 3);
        // Cold start at the cap: one tick's worth.
        assert_eq!(replenish_quota(64, 0, POOL_REPLENISH_MAX), POOL_REPLENISH_MAX);
        // Feature off.
        assert_eq!(replenish_quota(0, 0, POOL_REPLENISH_MAX), 0);
    }
}
