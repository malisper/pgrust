//! The shared pg_database prefix scan (the autovacuum get_database_list
//! template), factored out of main_loop so both sides of D2 use ONE reader:
//! the janitor loop (its own transaction, via
//! `main_loop::list_prefix_databases`) and the connecting backend's
//! per-role cap scan (`mint::live_owned_names`, inside the already-open
//! InitPostgres startup transaction).
//!
//! Contract: the caller supplies an OPEN transaction; this function only
//! opens/closes the relation and a private memory context.

use types_core::{InvalidOid, Oid};
use types_error::PgResult;

/// One enumerated pg_database row the janitor/mint paths care about.
pub(crate) struct DbRow {
    pub oid: Oid,
    pub name: String,
    pub istemplate: bool,
    /// datdba: per-role mint-cap accounting (D2 security posture).
    pub datdba: Oid,
}

/// Seqscan pg_database and keep only prefix-matching rows. Non-UTF-8
/// datnames are skipped: the prefix is UTF-8 configuration and `dropdb`
/// takes &str — a harness minting non-UTF-8 ephemeral names is out of
/// contract.
pub(crate) fn scan_prefix_rows(prefix: &str) -> PgResult<Vec<DbRow>> {
    scan_prefix_rows_collect(prefix, None)
}

/// `scan_prefix_rows` plus an every-row oid collector (`all_oids` — the
/// UNFILTERED database population, prefix-matching or not, templates
/// included). The reap pass feeds it to
/// `registry::retain_template_flush_marks`: templates conventionally live
/// OUTSIDE the prefix, so the filtered row set cannot prune dead flush
/// marks — and this way the tick's ONE catalog scan serves both needs.
pub(crate) fn scan_prefix_rows_collect(
    prefix: &str,
    mut all_oids: Option<&mut Vec<Oid>>,
) -> PgResult<Vec<DbRow>> {
    let mut rows = Vec::new();

    let cx = mcx::MemoryContext::new("pgrust janitor pg_database scan");
    let mcx = cx.mcx();
    let rd = table::table_open(
        mcx,
        types_core::catalog::DATABASE_RELATION_ID,
        types_rel::lock::AccessShareLock,
    )?;
    let desc = rd.descr();
    let mut scan = genam::systable_beginscan(mcx, &rd, InvalidOid, false, None, &[])?;
    while let Some(tup) = genam::systable_getnext(mcx, &mut scan)? {
        let att = |attnum: i32| -> datum::Datum {
            let mut isnull = false;
            // SAFETY: pg_database row under pg_database's descriptor;
            // oid/datname/datdba/datistemplate are fixed never-null columns.
            let d = unsafe { types_tuple::heap_getattr(tup, attnum, desc, &mut isnull) };
            debug_assert!(!isnull);
            d
        };
        if let Some(v) = all_oids.as_deref_mut() {
            v.push(att(pg_database::Anum_pg_database_oid).as_oid());
        }
        let name_d = att(pg_database::Anum_pg_database_datname);
        // SAFETY: a NameData column datum: NAMEDATALEN readable bytes,
        // NUL-terminated (the pg_database crate's own decode contract).
        let bytes = unsafe {
            core::slice::from_raw_parts(
                name_d.as_usize() as *const u8,
                types_core::fmgr::NAMEDATALEN as usize,
            )
        };
        let end = bytes.iter().position(|&b| b == 0).unwrap_or(bytes.len());
        if !bytes[..end].starts_with(prefix.as_bytes()) {
            continue;
        }
        let Ok(name) = core::str::from_utf8(&bytes[..end]) else {
            continue;
        };
        rows.push(DbRow {
            oid: att(pg_database::Anum_pg_database_oid).as_oid(),
            name: name.to_string(),
            istemplate: att(pg_database::Anum_pg_database_datistemplate).as_bool(),
            datdba: att(pg_database::Anum_pg_database_datdba).as_oid(),
        });
    }
    genam::systable_endscan(mcx, scan)?;
    rd.close(types_rel::lock::AccessShareLock)?;

    Ok(rows)
}
