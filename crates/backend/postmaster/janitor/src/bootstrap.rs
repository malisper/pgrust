//! Bootstrap-catalog backfill for the three janitor builtins (item B,
//! ruling 2026-08-05): `pgrust_pin_database`, `pgrust_unpin_database` and
//! `pgrust_seal_template` are TRUE builtins — present in every database a
//! session can reach, with no install step.
//!
//! WHY BACKFILL AND NOT A .BKI DELTA: pgrust ships no initdb — datadirs
//! come from stock C `initdb`, whose postgres.bki this port neither
//! generates nor patches (the Dockerfile harvests the C tools verbatim).
//! The pg_proc rows therefore cannot exist "at initdb"; instead, the FIRST
//! client connection to a database inserts them (one small transaction,
//! `PostgresMain` calls through `janitor_seams::builtin_backfill` right
//! after InitPostgres), and every later connection is a process-global
//! set probe. Databases created from a backfilled template inherit the
//! rows the ordinary way, so steady state is indistinguishable from a
//! bootstrap catalog that always carried them.
//!
//! ROW SHAPE (the C-parity argument, additive-only): oids 9001/9002/9005 —
//! the same reserved-range oids the fmgr EXTRA_BUILTINS table dispatches
//! (PostgreSQL documents 9000-9999 as reserved for forks, bki.sgml "OID
//! Assignment"; the execmain lanev2 coverage test pins the range) — in the
//! `pg_catalog` namespace, owned by the bootstrap superuser, `LANGUAGE
//! internal` with `prosrc` = the builtin name. Everything about the rows
//! mirrors a genuine initdb builtin: below FirstUnpinnedObjectId they are
//! PINNED (DROP FUNCTION refuses, no pg_depend rows — exactly how .dat
//! rows are pinned), pg_dump never emits pg_catalog objects, and fmgr
//! resolves the oid straight to the Rust implementation without reading
//! the row (fmgr_isbuiltin precedes pg_proc lookup, as in C). This is the
//! same sanctioned-additive class as the dbcommands extensions: template0
//! and template1 contents gain exactly three rows in the reserved range
//! and nothing else changes shape.
//!
//! Residuals, documented deliberately:
//! - A database NEVER client-connected keeps a bare pg_proc (invisible: no
//!   session exists to observe the absence). Clones of it backfill on
//!   their own first connection.
//! - On a hot standby the backfill skips (no WAL writes in recovery); the
//!   functions appear on first connect after promotion.
//! - Two SIMULTANEOUS first-ever connections: one claims the backfill, the
//!   other proceeds without waiting — a query racing the claimant's commit
//!   can still see "function does not exist" once, that session's next
//!   connect (or statement-level retry) resolves it. Bounded to the first
//!   connection ever per database.
//! - Legacy databases that ran the deleted janitor-functions.sql keep
//!   their `public.pgrust_*` wrappers; `pg_catalog` precedes `public` in
//!   every search_path, so the builtins win and the leftovers are inert
//!   (DROP FUNCTION removes them).

use datum::Datum;
use mcx::Mcx;
use types_core::catalog::{BOOLOID, BOOTSTRAP_SUPERUSERID, TEXTOID, VOIDOID};
use types_core::{Oid, OidIsValid, PG_CATALOG_NAMESPACE};
use types_error::{PgResult, LOG};
use types_rel::RowExclusiveLock;
use types_tuple::NameData;

use crate::builtins::{
    PGRUST_PIN_DATABASE_FOID, PGRUST_SEAL_TEMPLATE_FOID, PGRUST_UNPIN_DATABASE_FOID,
};

/// One builtin's pg_proc row description.
struct BuiltinRow {
    foid: Oid,
    name: &'static str,
    rettype: Oid,
    argtypes: &'static [Oid],
    argnames: &'static [&'static str],
}

/// The three janitor builtins (builtins.rs owns the implementations and
/// the EXTRA_BUILTINS dispatch rows; this table must stay in lockstep —
/// the unit test below pins it against JANITOR_BUILTINS).
const BUILTIN_ROWS: &[BuiltinRow] = &[
    BuiltinRow {
        foid: PGRUST_PIN_DATABASE_FOID,
        name: "pgrust_pin_database",
        rettype: BOOLOID,
        argtypes: &[TEXTOID],
        argnames: &["dbname"],
    },
    BuiltinRow {
        foid: PGRUST_UNPIN_DATABASE_FOID,
        name: "pgrust_unpin_database",
        rettype: BOOLOID,
        argtypes: &[TEXTOID],
        argnames: &["dbname"],
    },
    BuiltinRow {
        foid: PGRUST_SEAL_TEMPLATE_FOID,
        name: "pgrust_seal_template",
        rettype: VOIDOID,
        argtypes: &[TEXTOID],
        argnames: &["dbname"],
    },
];

/// Per-database backfill progress, process-global (the registry "shmem"
/// convention — one mutex IS the lock). `done` = a probe or an insert
/// COMMITTED all three rows this boot; `inflight` = a first-connector is
/// inserting right now (concurrent first-connectors skip instead of racing
/// the unique indexes — the module-doc residual). Restart-lossy on
/// purpose: the first connect per database per boot re-probes (one
/// syscache hit x3 when the rows exist).
struct BackfillState {
    done: Vec<Oid>,
    inflight: Vec<Oid>,
}

pgsync::process_global! {
    static BACKFILL: pgsync::Mutex<BackfillState> = pgsync::Mutex::new(BackfillState {
        done: Vec::new(),
        inflight: Vec::new(),
    });
}

enum Claim {
    /// This session owns the backfill for its database.
    Claimed,
    /// Already done, or another session is mid-backfill: nothing to do.
    Skip,
}

fn claim(db: Oid) -> Claim {
    let mut g = BACKFILL.lock().unwrap_or_else(|e| e.into_inner());
    if g.done.contains(&db) || g.inflight.contains(&db) {
        return Claim::Skip;
    }
    g.inflight.push(db);
    Claim::Claimed
}

fn settle(db: Oid, done: bool) {
    let mut g = BACKFILL.lock().unwrap_or_else(|e| e.into_inner());
    g.inflight.retain(|&o| o != db);
    if done && !g.done.contains(&db) {
        g.done.push(db);
    }
}

/// The `janitor_seams::builtin_backfill` impl: ensure the three builtin
/// pg_proc rows exist in the connected database. Called by PostgresMain
/// (client backends only) between InitPostgres and the query loop, OUTSIDE
/// any transaction; runs its own. Errors propagate for the caller to
/// contain (a failed backfill must never kill the session) and release the
/// claim so the next connection retries.
pub fn backfill_builtin_rows() -> PgResult<()> {
    let db = init_small::globals::MyDatabaseId();
    if !OidIsValid(db) {
        return Ok(());
    }
    // No WAL writes in recovery: a hot standby serves whatever the primary
    // has; the first connect after promotion backfills.
    if transam_xlog::RecoveryInProgress() {
        return Ok(());
    }
    match claim(db) {
        Claim::Skip => return Ok(()),
        Claim::Claimed => {}
    }
    struct Release {
        db: Oid,
        done: bool,
    }
    impl Drop for Release {
        fn drop(&mut self) {
            settle(self.db, self.done);
        }
    }
    let mut release = Release { db, done: false };

    let inserted = probe_and_insert()?;
    // Only after the COMMIT below returned: an aborted insert must not
    // latch `done` (the rows rolled back; the next connection retries).
    release.done = true;
    if inserted > 0 {
        let _ = elog::elog(
            LOG,
            format!(
                "pgrust: backfilled {inserted} builtin function row(s) into database oid {db} \
                 (pgrust_pin_database / pgrust_unpin_database / pgrust_seal_template)"
            ),
        );
    }
    Ok(())
}

/// One transaction: probe each reserved oid via syscache, insert the
/// missing rows, commit. Returns how many rows were inserted.
fn probe_and_insert() -> PgResult<usize> {
    let cx = mcx::MemoryContext::new("pgrust builtin backfill");
    xact::StartTransactionCommand()?;
    let mcx = cx.mcx();

    let mut missing: Vec<&BuiltinRow> = Vec::new();
    for b in BUILTIN_ROWS {
        match cache_syscache::SearchSysCache1(
            cache_syscache::cacheinfo::PROCOID,
            cache_syscache::SysCacheKey::Value(Datum::from_oid(b.foid)),
        )? {
            Some(t) => cache_syscache::ReleaseSysCache(t),
            None => missing.push(b),
        }
    }
    if missing.is_empty() {
        xact::CommitTransactionCommand()?;
        return Ok(0);
    }

    let rel = table::table_open(mcx, types_core::catalog::PROCEDURE_RELATION_ID, RowExclusiveLock)?;
    for b in &missing {
        insert_row(mcx, &rel, b)?;
        // Multi-statement-transaction convention (the mint_batch_body CCI
        // precedent): make each row command-visible before the next.
        xact::CommandCounterIncrement()?;
    }
    rel.close(RowExclusiveLock)?;
    let n = missing.len();
    xact::CommitTransactionCommand()?;
    Ok(n)
}

/// Form and insert one pg_proc row with an EXPLICIT reserved oid (the
/// ProcedureCreate insert-branch shape minus GetNewOidWithIndex and minus
/// pg_depend records — oids below FirstUnpinnedObjectId are implicitly
/// pinned, exactly like initdb's own .dat rows).
fn insert_row(mcx: Mcx<'_>, rel: &types_rel::Relation<'_>, b: &BuiltinRow) -> PgResult<()> {
    use pg_proc::*;

    let mut proname = NameData::default();
    proname.namestrcpy(b.name);
    let prosrc_text = varlena::cstring_to_text(mcx, b.name.as_bytes())?;
    let argtypes_image = build_oidvector_image(mcx, b.argtypes)?;
    // proargnames (text[]): the ProcedureCreate construction.
    let mut name_texts = Vec::with_capacity(b.argnames.len());
    for &n in b.argnames {
        name_texts.push(varlena::cstring_to_text(mcx, n.as_bytes())?);
    }
    let mut name_elems: mcx::PgVec<'_, Datum> = mcx::vec_with_capacity_in(mcx, name_texts.len())?;
    for t in name_texts.iter() {
        name_elems.push(Datum::from_usize(t.as_bytes().as_ptr() as usize));
    }
    let argnames_image =
        datum::array_build::construct_array_image(mcx, &name_elems, TEXTOID, -1, false, b'i')?;

    let mut values = [Datum::null(); Natts_pg_proc];
    let mut nulls = [false; Natts_pg_proc];
    let set = |values: &mut [Datum], attnum: usize, d: Datum| values[attnum - 1] = d;
    values[Anum_pg_proc_oid as usize - 1] = Datum::from_oid(b.foid);
    set(&mut values, Anum_pg_proc_proname, Datum::from_usize(proname.data.as_ptr() as usize));
    set(&mut values, Anum_pg_proc_pronamespace, Datum::from_oid(PG_CATALOG_NAMESPACE));
    set(&mut values, Anum_pg_proc_proowner, Datum::from_oid(BOOTSTRAP_SUPERUSERID));
    set(&mut values, Anum_pg_proc_prolang, Datum::from_oid(INTERNALlanguageId));
    set(&mut values, Anum_pg_proc_procost, Datum::from_f32(1.0));
    set(&mut values, Anum_pg_proc_prorows, Datum::from_f32(0.0));
    set(&mut values, Anum_pg_proc_provariadic, Datum::from_oid(types_core::InvalidOid));
    set(&mut values, Anum_pg_proc_prosupport, Datum::from_oid(types_core::InvalidOid));
    set(&mut values, Anum_pg_proc_prokind, Datum::from_char(PROKIND_FUNCTION));
    set(&mut values, Anum_pg_proc_prosecdef, Datum::from_bool(false));
    set(&mut values, Anum_pg_proc_proleakproof, Datum::from_bool(false));
    set(&mut values, Anum_pg_proc_proisstrict, Datum::from_bool(true));
    set(&mut values, Anum_pg_proc_proretset, Datum::from_bool(false));
    set(&mut values, Anum_pg_proc_provolatile, Datum::from_char(PROVOLATILE_VOLATILE));
    set(&mut values, Anum_pg_proc_proparallel, Datum::from_char(PROPARALLEL_UNSAFE));
    set(&mut values, Anum_pg_proc_pronargs, Datum::from_i16(b.argtypes.len() as i16));
    set(&mut values, Anum_pg_proc_pronargdefaults, Datum::from_i16(0));
    set(&mut values, Anum_pg_proc_prorettype, Datum::from_oid(b.rettype));
    set(
        &mut values,
        Anum_pg_proc_proargtypes,
        Datum::from_usize(argtypes_image.as_ptr() as usize),
    );
    nulls[Anum_pg_proc_proallargtypes - 1] = true;
    nulls[Anum_pg_proc_proargmodes - 1] = true;
    set(
        &mut values,
        Anum_pg_proc_proargnames,
        Datum::from_usize(argnames_image.as_ptr() as usize),
    );
    nulls[Anum_pg_proc_proargdefaults - 1] = true;
    nulls[Anum_pg_proc_protrftypes - 1] = true;
    set(
        &mut values,
        Anum_pg_proc_prosrc,
        Datum::from_usize(prosrc_text.as_bytes().as_ptr() as usize),
    );
    nulls[Anum_pg_proc_probin - 1] = true;
    nulls[Anum_pg_proc_prosqlbody - 1] = true;
    nulls[Anum_pg_proc_proconfig - 1] = true;
    // NULL proacl = the stock function default (EXECUTE granted to PUBLIC),
    // matching every initdb builtin.
    nulls[Anum_pg_proc_proacl - 1] = true;

    let mut tup = heaptuple::heap_form_tuple(mcx, rel.descr(), &values, &nulls)?;
    catalog_indexing::CatalogTupleInsert(mcx, rel, &mut tup)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The backfill table and the fmgr dispatch table (builtins.rs) must
    /// describe the SAME surface: same oids, same names, same arities and
    /// strictness/retset shape — a drift would make the catalog row and
    /// the dispatched implementation disagree.
    #[test]
    fn backfill_rows_match_dispatch_table() {
        assert_eq!(BUILTIN_ROWS.len(), crate::builtins::JANITOR_BUILTINS.len());
        for b in BUILTIN_ROWS {
            let d = crate::builtins::JANITOR_BUILTINS
                .iter()
                .find(|d| d.foid == b.foid)
                .expect("every backfill row has a dispatch row");
            assert_eq!(d.name, b.name);
            assert_eq!(d.nargs as usize, b.argtypes.len());
            assert_eq!(b.argnames.len(), b.argtypes.len());
            assert!(d.strict, "rows are formed proisstrict = true");
            assert!(!d.retset, "rows are formed proretset = false");
            // The pinned-object law: every backfilled oid sits below
            // FirstUnpinnedObjectId, so DROP FUNCTION refuses without any
            // pg_depend row — the initdb-builtin posture.
            assert!(b.foid < 12000, "{} must be implicitly pinned", b.foid);
        }
    }

    /// Claim/settle semantics: one claimant per database; failure releases
    /// for retry; success latches done.
    #[test]
    fn claim_settle_semantics() {
        let db: Oid = 90701;
        assert!(matches!(claim(db), Claim::Claimed));
        assert!(matches!(claim(db), Claim::Skip), "in-flight databases skip");
        settle(db, false);
        assert!(matches!(claim(db), Claim::Claimed), "failure releases the claim");
        settle(db, true);
        assert!(matches!(claim(db), Claim::Skip), "done databases skip forever");
    }
}
