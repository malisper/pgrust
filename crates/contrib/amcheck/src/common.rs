use ::mcx::Mcx;
use ::types_core::{
    init::SECURITY_RESTRICTED_OPERATION, InvalidOid, Oid, OidIsValid, RELPERSISTENCE_TEMP,
    RELPERSISTENCE_UNLOGGED,
};
use ::types_error::{
    PgError, PgResult, ERRCODE_FEATURE_NOT_SUPPORTED, ERRCODE_READ_ONLY_SQL_TRANSACTION,
    ERRCODE_UNDEFINED_TABLE,
};
use ::types_rel::pg_class::RELKIND_INDEX;
use ::types_rel::Relation;
use ::types_storage::lock::{ShareLock, LOCKMODE};

fn amcheck_index_mainfork_expected(rel: &Relation<'_>) -> PgResult<bool> {
    if rel.rd_rel.relpersistence != RELPERSISTENCE_UNLOGGED || !transam_xlog::RecoveryInProgress()
    {
        return Ok(true);
    }

    elog_seams::ereport::call(
        PgError::notice(format!(
            "cannot verify unlogged index \"{}\" during recovery, skipping",
            rel.name()
        ))
        .with_sqlstate(ERRCODE_READ_ONLY_SQL_TRANSACTION),
    )?;
    Ok(false)
}

fn am_name(am_id: Oid) -> String {
    syscache_seams::pg_am_amname::call(am_id)
        .ok()
        .flatten()
        .unwrap_or_default()
}

pub(crate) fn index_checkable(rel: &Relation<'_>, am_id: Oid) -> PgResult<bool> {
    if rel.rd_rel.relkind != RELKIND_INDEX || rel.rd_rel.relam != am_id {
        let want = am_name(am_id);
        let have = am_name(rel.rd_rel.relam);
        return Err(Box::new(
            PgError::error(format!(
                "expected \"{want}\" index as targets for verification"
            ))
            .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED)
            .with_detail(format!("Relation \"{}\" is a {have} index.", rel.name())),
        ));
    }

    if rel.rd_rel.relpersistence == RELPERSISTENCE_TEMP && !rel.rd_islocaltemp {
        return Err(Box::new(
            PgError::error("cannot access temporary tables of other sessions")
                .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED)
                .with_detail(format!(
                    "Index \"{}\" is associated with temporary relation.",
                    rel.name()
                )),
        ));
    }

    if !rel.rd_index.as_ref().is_some_and(|ix| ix.indisvalid) {
        return Err(Box::new(
            PgError::error(format!("cannot check index \"{}\"", rel.name()))
                .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED)
                .with_detail("Index is not valid."),
        ));
    }

    amcheck_index_mainfork_expected(rel)
}

// Error paths unwind through xact abort, which releases the GUC nest level,
// userid/sec-context, and locks — C's ereport(ERROR) contract.
pub(crate) fn amcheck_lock_relation_and_check<'mcx>(
    mcx: Mcx<'mcx>,
    indrelid: Oid,
    am_id: Oid,
    lockmode: LOCKMODE,
    check: impl FnOnce(&Relation<'mcx>, &Relation<'mcx>, bool) -> PgResult<()>,
) -> PgResult<()> {
    let heapid = catalog_index::IndexGetRelation(mcx, indrelid, true)?;

    let mut heaprel: Option<Relation<'mcx>> = None;
    let mut save_userid = InvalidOid;
    let mut save_sec_context = -1;
    let mut save_nestlevel = -1;

    if OidIsValid(heapid) {
        let hr = table::table_open(mcx, heapid, lockmode)?;
        let (uid, sec) = miscinit::GetUserIdAndSecContext();
        save_userid = uid;
        save_sec_context = sec;
        miscinit::SetUserIdAndSecContext(
            hr.rd_rel.relowner,
            save_sec_context | SECURITY_RESTRICTED_OPERATION,
        );
        save_nestlevel = guc::NewGUCNestLevel();
        heaprel = Some(hr);
    }

    let indrel = indexam::index_open(mcx, indrelid, lockmode)?;

    if heaprel.is_none() || heapid != catalog_index::IndexGetRelation(mcx, indrelid, false)? {
        return Err(Box::new(
            PgError::error(format!(
                "could not open parent table of index \"{}\"",
                indrel.name()
            ))
            .with_sqlstate(ERRCODE_UNDEFINED_TABLE),
        ));
    }
    let heaprel = heaprel.expect("checked is_none above");

    if index_checkable(&indrel, am_id)? {
        check(&indrel, &heaprel, lockmode == ShareLock)?;
    }

    guc::AtEOXact_GUC(false, save_nestlevel);
    miscinit::SetUserIdAndSecContext(save_userid, save_sec_context);

    indexam::index_close(indrel, lockmode)?;
    table::table_close(heaprel, lockmode)?;
    Ok(())
}
