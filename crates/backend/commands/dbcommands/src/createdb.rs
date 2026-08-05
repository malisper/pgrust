use datum::Datum;
use elog::ereport;
use mcx::Mcx;
use pg_database::{
    Anum_pg_database_oid, DatabaseOidIndexId, Natts_pg_database, DATCONNLIMIT_UNLIMITED,
};
use pg_database_seams::{COLLPROVIDER_BUILTIN, COLLPROVIDER_ICU, COLLPROVIDER_LIBC};
use types_core::catalog::{FirstNormalObjectId, DATABASE_RELATION_ID};
use types_core::{InvalidOid, Oid};
use types_error::{
    PgResult, ERRCODE_DUPLICATE_DATABASE, ERRCODE_INSUFFICIENT_PRIVILEGE,
    ERRCODE_INVALID_PARAMETER_VALUE, ERRCODE_OBJECT_IN_USE,
    ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE, ERRCODE_SYNTAX_ERROR, ERRCODE_UNDEFINED_DATABASE,
    ERRCODE_UNDEFINED_OBJECT, ERRCODE_WRONG_OBJECT_TYPE, ERROR, NOTICE, WARNING,
};
use types_nodes::parsenodes::{CreatedbStmt, DefElem};
use types_nodes::NodeTag;
use types_storage::lock::{AccessShareLock, RowExclusiveLock, ShareLock};
use types_tuple::NameData;
use wchar::{PG_SQL_ASCII, PG_UTF8};

// wasm32: no LC_* names in the wasi libc crate; musl numbering (the
// pg_locale wasm arm's convention).
#[cfg(not(target_family = "wasm"))]
use libc::{LC_COLLATE, LC_CTYPE};
#[cfg(target_family = "wasm")]
const LC_CTYPE: i32 = 0;
#[cfg(target_family = "wasm")]
const LC_COLLATE: i32 = 3;

use crate::{
    check_db_file_conflict, database_is_invalid_form, get_db_info, have_createdb_privilege, loc,
    GLOBALTABLESPACE_OID, TableSpaceRelationId, XLOG_DBASE_CREATE_FILE_COPY,
};

// collprovider_name (pg_collation.h).
fn collprovider_name(c: u8) -> &'static str {
    match c {
        COLLPROVIDER_BUILTIN => "builtin",
        COLLPROVIDER_ICU => "icu",
        COLLPROVIDER_LIBC => "libc",
        _ => "???",
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum CreateDBStrategy {
    WalLog,
    FileCopy,
}

fn conflicting(defel: &DefElem<'_>) -> Box<types_error::PgError> {
    let _ = defel;
    ereport(ERROR)
        .errcode(ERRCODE_SYNTAX_ERROR)
        .errmsg("conflicting or redundant options".to_string())
        .into_error()
        .into()
}

fn defGetInt32(def: &DefElem<'_>) -> PgResult<i32> {
    match def.arg.and_then(|n| n.as_integer()) {
        Some(i) => Ok(i.ival),
        None => Err(ereport(ERROR)
            .errcode(ERRCODE_SYNTAX_ERROR)
            .errmsg(format!("{} requires an integer value", def.defname.unwrap_or("")))
            .into_error()
            .into()),
    }
}

fn defGetObjectId(def: &DefElem<'_>) -> PgResult<Oid> {
    if let Some(i) = def.arg.and_then(|n| n.as_integer()) {
        return Ok(i.ival as u32);
    }
    // OIDs above i32::MAX lex as Float (pg_upgrade's preserved OIDs).
    if let Some(f) = def.arg.and_then(|n| n.as_float()) {
        return Ok(numutils::uint32in_subr(f.fval, false, "oid", None)?.0);
    }
    Err(ereport(ERROR)
        .errcode(ERRCODE_SYNTAX_ERROR)
        .errmsg(format!("{} requires a numeric value", def.defname.unwrap_or("")))
        .into_error()
        .into())
}


pub fn check_encoding_locale_matches(
    encoding: i32,
    collate: &str,
    ctype: &str,
) -> PgResult<()> {
    let ctype_encoding = pg_locale::pg_get_encoding_from_locale(Some(ctype), true)?;
    let collate_encoding = pg_locale::pg_get_encoding_from_locale(Some(collate), true)?;

    let allowed = |loc_encoding: i32| -> PgResult<bool> {
        Ok(loc_encoding == encoding
            || loc_encoding == PG_SQL_ASCII
            || loc_encoding == -1
            || (encoding == PG_SQL_ASCII && superuser::superuser()?))
    };

    if !allowed(ctype_encoding)? {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
            .errmsg(format!(
                "encoding \"{}\" does not match locale \"{ctype}\"",
                mbutils::pg_encoding_to_char(encoding)
            ))
            .errdetail(format!(
                "The chosen LC_CTYPE setting requires encoding \"{}\".",
                mbutils::pg_encoding_to_char(ctype_encoding)
            ))
            .into_error()
            .into());
    }
    if !allowed(collate_encoding)? {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
            .errmsg(format!(
                "encoding \"{}\" does not match locale \"{collate}\"",
                mbutils::pg_encoding_to_char(encoding)
            ))
            .errdetail(format!(
                "The chosen LC_COLLATE setting requires encoding \"{}\".",
                mbutils::pg_encoding_to_char(collate_encoding)
            ))
            .into_error()
            .into());
    }
    Ok(())
}

/// One per-tablespace directory copy a FILE_COPY create owes: the exact
/// (srcpath, dstpath, srctablespace, dsttablespace) tuple the
/// CreateDatabaseUsingFileCopy loop derives per pg_tablespace row with
/// template content. Owned Strings on purpose: the janitor's deferred-copy
/// batch carries these across per-member memory contexts.
pub struct FileCopyDir {
    pub srcpath: String,
    pub dstpath: String,
    pub src_tablespace: Oid,
    pub dst_tablespace: Oid,
}

/// The deferred half of `createdb_deferred_file_copy`: the created
/// database's oid plus every directory copy (and matching
/// XLOG_DBASE_CREATE_FILE_COPY record) the caller now owes before commit.
pub struct DeferredFileCopy {
    pub db_oid: Oid,
    pub src_dboid: Oid,
    pub dirs: Vec<FileCopyDir>,
}

/// The XLOG_DBASE_CREATE_FILE_COPY record for ONE copied per-tablespace
/// directory (the record half of the CreateDatabaseUsingFileCopy loop,
/// shared with the janitor's deferred-copy WAL phase). MUST be called on
/// the backend thread that owns the transaction, strictly AFTER the
/// directory copy it describes completed (record-implies-copy-complete).
pub fn log_file_copy_record(
    dst_dboid: Oid,
    dst_tablespace: Oid,
    src_dboid: Oid,
    src_tablespace: Oid,
) -> PgResult<()> {
    let mut xlrec = [0u8; 16];
    xlrec[0..4].copy_from_slice(&dst_dboid.to_ne_bytes());
    xlrec[4..8].copy_from_slice(&dst_tablespace.to_ne_bytes());
    xlrec[8..12].copy_from_slice(&src_dboid.to_ne_bytes());
    xlrec[12..16].copy_from_slice(&src_tablespace.to_ne_bytes());
    xloginsert::insert_record(
        types_core::primitive::RmgrIds::RM_DBASE_ID as u8,
        XLOG_DBASE_CREATE_FILE_COPY | xloginsert::XLR_SPECIAL_REL_UPDATE,
        0,
        &[&xlrec],
        &[],
    )?;
    Ok(())
}

/// The enumeration half of the CreateDatabaseUsingFileCopy loop (janitor
/// deferred-copy support): the same pg_tablespace scan and the same
/// per-row srcpath-has-content filter, yielding the copy list WITHOUT
/// copying. Kept separate from CreateDatabaseUsingFileCopy on purpose —
/// the C-shaped path keeps its exact copy-inside-the-scan interleaving.
fn enumerate_file_copy_dirs(
    mcx: Mcx<'_>,
    src_dboid: Oid,
    dst_dboid: Oid,
    src_tsid: Oid,
    dst_tsid: Oid,
) -> PgResult<Vec<FileCopyDir>> {
    let mut dirs = Vec::new();
    let rel = table::table_open(mcx, TableSpaceRelationId, AccessShareLock)?;
    let mut scan = genam::systable_beginscan(mcx, &rel, InvalidOid, false, None, &[])?;
    while let Some(tup) = genam::systable_getnext(mcx, &mut scan)? {
        let mut isnull = false;
        // SAFETY: pg_tablespace row; attno 1 is the oid column.
        let srctablespace =
            unsafe { types_tuple::heap_getattr(tup, 1, rel.descr(), &mut isnull) }.as_oid();
        if srctablespace == GLOBALTABLESPACE_OID {
            continue;
        }
        let srcpath = relpath::GetDatabasePath(mcx, src_dboid, srctablespace)?;
        match std::fs::metadata(srcpath.as_str()) {
            Ok(md) if md.is_dir() && !fd::directory_is_empty(srcpath.as_str())? => {}
            _ => continue,
        }
        let dsttablespace = if srctablespace == src_tsid { dst_tsid } else { srctablespace };
        let dstpath = relpath::GetDatabasePath(mcx, dst_dboid, dsttablespace)?;
        dirs.push(FileCopyDir {
            srcpath: srcpath.as_str().to_owned(),
            dstpath: dstpath.as_str().to_owned(),
            src_tablespace: srctablespace,
            dst_tablespace: dsttablespace,
        });
    }
    genam::systable_endscan(mcx, scan)?;
    rel.close(AccessShareLock)?;
    Ok(dirs)
}

fn CreateDatabaseUsingFileCopy(
    mcx: Mcx<'_>,
    src_dboid: Oid,
    dst_dboid: Oid,
    src_tsid: Oid,
    dst_tsid: Oid,
    request_checkpoints: bool,
) -> PgResult<()> {
    use transam_xlog::{
        CHECKPOINT_FLUSH_ALL, CHECKPOINT_FORCE, CHECKPOINT_IMMEDIATE, CHECKPOINT_WAIT,
    };

    if request_checkpoints && !init_small::globals::IsBinaryUpgrade() {
        checkpointer::RequestCheckpoint(
            CHECKPOINT_IMMEDIATE | CHECKPOINT_FORCE | CHECKPOINT_WAIT | CHECKPOINT_FLUSH_ALL,
        )?;
    }

    let rel = table::table_open(mcx, TableSpaceRelationId, AccessShareLock)?;
    let mut scan = genam::systable_beginscan(mcx, &rel, InvalidOid, false, None, &[])?;
    while let Some(tup) = genam::systable_getnext(mcx, &mut scan)? {
        let mut isnull = false;
        // SAFETY: pg_tablespace row; attno 1 is the oid column.
        let srctablespace =
            unsafe { types_tuple::heap_getattr(tup, 1, rel.descr(), &mut isnull) }.as_oid();
        if srctablespace == GLOBALTABLESPACE_OID {
            continue;
        }

        let srcpath = relpath::GetDatabasePath(mcx, src_dboid, srctablespace)?;
        match std::fs::metadata(srcpath.as_str()) {
            Ok(md) if md.is_dir() && !fd::directory_is_empty(srcpath.as_str())? => {}
            _ => continue,
        }

        let dsttablespace = if srctablespace == src_tsid { dst_tsid } else { srctablespace };
        let dstpath = relpath::GetDatabasePath(mcx, dst_dboid, dsttablespace)?;

        fd::copydir(srcpath.as_str(), dstpath.as_str(), false)?;

        log_file_copy_record(dst_dboid, dsttablespace, src_dboid, srctablespace)?;
    }
    genam::systable_endscan(mcx, scan)?;
    rel.close(AccessShareLock)?;

    // Checkpoint before commit so committed FILE_COPY creates never need
    // ordinary crash-recovery replay (dbcommands.c's #1/#2 scenarios).
    if request_checkpoints && !init_small::globals::IsBinaryUpgrade() {
        checkpointer::RequestCheckpoint(CHECKPOINT_IMMEDIATE | CHECKPOINT_FORCE | CHECKPOINT_WAIT)?;
    }
    Ok(())
}

/// The wal_log arm of createdb_failure_cleanup, callable AFTER the failed
/// transaction is gone (janitor batch-abort cleanup): a wal_log copy runs
/// through shared buffers, so an aborted-batch member's dirty dst-database
/// buffers MUST be dropped (and its pending fsync requests forgotten)
/// BEFORE its datadir is rmtree'd — a checkpointer write into a removed
/// directory otherwise hard-errors. The lock releases of the in-transaction
/// cleanup are deliberately absent: the aborting transaction released them.
pub fn forget_walog_database(dst_dboid: Oid) -> PgResult<()> {
    bufmgr::DropDatabaseBuffers(dst_dboid)?;
    smgr::ForgetDatabaseSyncRequests(dst_dboid)?;
    Ok(())
}

fn createdb_failure_cleanup(
    mcx: Mcx<'_>,
    src_dboid: Oid,
    dst_dboid: Oid,
    strategy: CreateDBStrategy,
) -> PgResult<()> {
    if strategy == CreateDBStrategy::WalLog {
        bufmgr::DropDatabaseBuffers(dst_dboid)?;
        smgr::ForgetDatabaseSyncRequests(dst_dboid)?;
        lmgr::UnlockSharedObject(DATABASE_RELATION_ID, dst_dboid, 0, AccessShareLock)?;
    }
    lmgr::UnlockSharedObject(DATABASE_RELATION_ID, src_dboid, 0, ShareLock)?;
    crate::remove_dbtablespaces(mcx, dst_dboid)
}

/// How createdb_guts runs the copy half.
#[derive(Clone, Copy, PartialEq, Eq)]
enum CreatedbCopyMode {
    /// C-shaped: copy inline, with FILE_COPY's checkpoint pair.
    Inline,
    /// Batched-mint entry: copy inline, both FILE_COPY checkpoints elided
    /// (the caller brackets its batch with one pair).
    InlineSkipCheckpoints,
    /// Deferred-copy entry (parallel batch copy): catalog work only; the
    /// caller owes the directory copies + WAL records + checkpoint pair.
    /// FILE_COPY strategy required.
    Deferred,
}

pub fn createdb<'mcx>(mcx: Mcx<'mcx>, stmt: &CreatedbStmt<'mcx>) -> PgResult<Oid> {
    Ok(createdb_guts(mcx, stmt, CreatedbCopyMode::Inline)?.0)
}

/// pgrust-only additive entry (docs/design/test-views.md, "Batched-minting
/// addendum"; the `dropdb_skip_checkpoint` precedent — the second
/// sanctioned extension of this C-parity crate): `createdb` minus BOTH of
/// FILE_COPY's per-create `RequestCheckpoint`s, for the ephemeral-db
/// janitor's batched mints. The janitor brackets a whole batch with ONE
/// checkpoint pair inside a single transaction: the FLUSH_ALL pre-checkpoint
/// before the first copy, this entry once per batch member, the plain
/// post-checkpoint after the LAST copy, THEN the commit.
///
/// Why hoisting the pair out of the loop is safe — C's rationale for each
/// request (dbcommands.c CreateDatabaseUsingFileCopy) discharges per batch:
///
/// 1. Pre-copy FLUSH_ALL ("Force a checkpoint before starting the copy of
///    the source database... including private buffers of unlogged
///    relations, so the copy sees every committed page on disk"): one
///    batch-level FLUSH_ALL covers every member because nothing dirties a
///    template's per-database files between it and the copies — the batch
///    transaction itself writes only shared catalogs (pg_database,
///    pg_shdepend, pg_db_role_setting: global tablespace, never inside the
///    directories copydir copies; the STRATEGY-PICK probe reads template
///    pg_class buffers and can dirty them via hint bits, which the janitor
///    prices separately — a probing pick invalidates the flush mark and
///    re-pays the member's FLUSH_ALL, mint.rs member_fires_pre), a batch
///    member's freshly copied database goes through the filesystem rather
///    than shared buffers, and each member's own `CountOtherDBBackends`
///    still refuses an occupied source immediately before its copy — in
///    Deferred mode (createdb_deferred_file_copy) the catalog-time check
///    no longer sits adjacent to the copy, so the janitor RE-RUNS it per
///    unique source at the top of the copy phase (run_deferred_copies),
///    restoring the adjacency at batch grain. A backend that connects to a source and
///    dirties it AFTER that check is C's own documented FILE_COPY residual
///    (the "source database must remain idle" caveat); the batch widens
///    that window from one copy to one batch, it does not add a state.
///    NOTE the check's honest limit: `CountOtherDBBackends` cannot see a
///    writer that already DISCONNECTED — a connect-dirty-disconnect
///    entirely inside the widened window passes every member's check while
///    its unflushed pages sit in shared buffers. The janitor caller
///    therefore restricts batch members to templates with
///    datallowconn = false (verified at preflight AND re-checked inside
///    the batch transaction), making the widened window unreachable by
///    ordinary connections; connectable templates mint on the serial path,
///    whose window is stock C's own one-copy-wide residual.
/// 2. Post-copy checkpoint before commit ("committed FILE_COPY creates
///    never need ordinary crash-recovery replay", the #1/#2 scenarios): the
///    invariant is per-member "commit implies a checkpoint ran after this
///    member's copy". One post-checkpoint after the last copy, INSIDE the
///    single transaction that carries every member, precedes the one commit
///    that makes any member visible — so it holds for all of them. A crash
///    before the commit aborts every member (scenario-free); a crash after
///    it finds every copy checkpointed.
/// 3. `IsBinaryUpgrade` skips both requests in stock createdb; this entry
///    is janitor-only and never runs in binary upgrade, so the flag gating
///    stays inside the guts unchanged.
///
/// Everything else — option parsing, privilege checks, template occupancy,
/// catalog insert, WAL FILE_COPY records, failure cleanup, ForceSyncCommit —
/// is the shared guts, byte-identical; the C-shaped `createdb` entry above
/// passes `request_checkpoints = true` and keeps its exact behavior. The
/// flag is a no-op under STRATEGY wal_log (that path has no checkpoint
/// sites). Callers of this entry MUST bracket their batch:
/// `RequestCheckpoint(CHECKPOINT_IMMEDIATE | CHECKPOINT_FORCE |
/// CHECKPOINT_WAIT | CHECKPOINT_FLUSH_ALL)` before the first call and
/// `RequestCheckpoint(CHECKPOINT_IMMEDIATE | CHECKPOINT_FORCE |
/// CHECKPOINT_WAIT)` after the last, before committing the transaction.
pub fn createdb_skip_checkpoints<'mcx>(
    mcx: Mcx<'mcx>,
    stmt: &CreatedbStmt<'mcx>,
) -> PgResult<Oid> {
    Ok(createdb_guts(mcx, stmt, CreatedbCopyMode::InlineSkipCheckpoints)?.0)
}

/// pgrust-only additive entry (test-views.md mint-strategy addendum; the
/// third sanctioned extension of this C-parity crate, extending the
/// `createdb_skip_checkpoints` precedent): `createdb_skip_checkpoints`
/// with the FILE_COPY directory copies and their WAL records DEFERRED to
/// the caller, so the ephemeral-db janitor can fan a whole batch's copies
/// across worker threads while every catalog mutation, WAL insertion, and
/// checkpoint stays on the backend thread that owns the transaction.
///
/// Contract (violations break FILE_COPY's crash invariant):
/// - STRATEGY must be file_copy (wal_log has nothing to defer; refused).
/// - The caller MUST re-run `CountOtherDBBackends` per unique source
///   immediately before the deferred copies (aborting the batch if
///   occupied): this entry's own catalog-time check no longer guards
///   "immediately before its copy" — a whole batch of catalog work (and
///   any checkpoint stall) sits in between, where an attaching
///   anti-wraparound autovacuum worker would survive to write-race the
///   copy (safety-discharge item 1 above records the widening and the
///   restored guard).
/// - The caller MUST, before committing the transaction: copy every
///   returned dir (srcpath -> dstpath, the fd::copydir shape: top-level
///   regular files, honoring file_copy_method, fsync settings, AND the
///   fsync/create-mode semantics — pg_fsync routing and
///   pg_file_create_mode, not stdlib defaults), then
///   insert one `log_file_copy_record` per dir ON THIS THREAD strictly
///   after that dir's copy completed, then bracket exactly like
///   `createdb_skip_checkpoints`' batch contract (FLUSH_ALL pre-checkpoint
///   before the first copy of the batch, plain post-checkpoint after the
///   last, both before the commit).
/// - On any copy failure the caller must abort the transaction and remove
///   the orphaned datadirs (the janitor's `cleanup_orphaned_datadirs`
///   shape: rmtree + XLOG_DBASE_DROP + XLogFlush).
pub fn createdb_deferred_file_copy<'mcx>(
    mcx: Mcx<'mcx>,
    stmt: &CreatedbStmt<'mcx>,
) -> PgResult<DeferredFileCopy> {
    let (oid, deferred) = createdb_guts(mcx, stmt, CreatedbCopyMode::Deferred)?;
    let _ = oid;
    Ok(deferred.expect("Deferred mode always yields the copy list"))
}

fn createdb_guts<'mcx>(
    mcx: Mcx<'mcx>,
    stmt: &CreatedbStmt<'mcx>,
    copy_mode: CreatedbCopyMode,
) -> PgResult<(Oid, Option<DeferredFileCopy>)> {
    let dbname = stmt.dbname.unwrap_or("");

    let mut tablespacename_el: Option<&DefElem> = None;
    let mut owner_el: Option<&DefElem> = None;
    let mut template_el: Option<&DefElem> = None;
    let mut encoding_el: Option<&DefElem> = None;
    let mut locale_el: Option<&DefElem> = None;
    let mut builtinlocale_el: Option<&DefElem> = None;
    let mut collate_el: Option<&DefElem> = None;
    let mut ctype_el: Option<&DefElem> = None;
    let mut iculocale_el: Option<&DefElem> = None;
    let mut icurules_el: Option<&DefElem> = None;
    let mut locprovider_el: Option<&DefElem> = None;
    let mut istemplate_el: Option<&DefElem> = None;
    let mut allowconnections_el: Option<&DefElem> = None;
    let mut connlimit_el: Option<&DefElem> = None;
    let mut collversion_el: Option<&DefElem> = None;
    let mut strategy_el: Option<&DefElem> = None;
    let mut dboid: Oid = InvalidOid;

    for opt in stmt.options.iter() {
        let defel = opt.as_def_elem().expect("createdb options are DefElems");
        let slot: &mut Option<&DefElem> = match defel.defname.unwrap_or("") {
            "tablespace" => &mut tablespacename_el,
            "owner" => &mut owner_el,
            "template" => &mut template_el,
            "encoding" => &mut encoding_el,
            "locale" => &mut locale_el,
            "builtin_locale" => &mut builtinlocale_el,
            "lc_collate" => &mut collate_el,
            "lc_ctype" => &mut ctype_el,
            "icu_locale" => &mut iculocale_el,
            "icu_rules" => &mut icurules_el,
            "locale_provider" => &mut locprovider_el,
            "is_template" => &mut istemplate_el,
            "allow_connections" => &mut allowconnections_el,
            "connection_limit" => &mut connlimit_el,
            "collation_version" => &mut collversion_el,
            "strategy" => &mut strategy_el,
            "location" => {
                ereport(WARNING)
                    .errcode(types_error::ERRCODE_FEATURE_NOT_SUPPORTED)
                    .errmsg("LOCATION is not supported anymore".to_string())
                    .errhint("Consider using tablespaces instead.".to_string())
                    .finish(loc("createdb"))?;
                continue;
            }
            "oid" => {
                dboid = defGetObjectId(defel)?;
                if dboid < FirstNormalObjectId
                    && !init_small::globals::allowSystemTableMods()
                    && !init_small::globals::IsBinaryUpgrade()
                {
                    return Err(ereport(ERROR)
                        .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
                        .errmsg(format!(
                            "OIDs less than {FirstNormalObjectId} are reserved for system objects"
                        ))
                        .into_error()
                        .into());
                }
                continue;
            }
            other => {
                return Err(ereport(ERROR)
                    .errcode(ERRCODE_SYNTAX_ERROR)
                    .errmsg(format!("option \"{other}\" not recognized"))
                    .errposition(defel.location + 1)
                    .into_error()
                    .into());
            }
        };
        if slot.is_some() {
            return Err(conflicting(defel));
        }
        *slot = Some(defel);
    }

    let arg_str = |el: Option<&DefElem<'mcx>>| -> PgResult<Option<&'mcx str>> {
        match el {
            Some(d) if d.arg.is_some() => Ok(Some(explain::defGetString(mcx, d)?)),
            _ => Ok(None),
        }
    };

    let dbowner = arg_str(owner_el)?;
    let dbtemplate = arg_str(template_el)?;
    let mut encoding: i32 = -1;
    if let Some(el) = encoding_el {
        if let Some(arg) = el.arg {
            if arg.node_tag() == NodeTag::T_Integer {
                encoding = defGetInt32(el)?;
                let name = mbutils::pg_encoding_to_char(encoding);
                if name.is_empty() || mbutils::pg_valid_server_encoding(name) < 0 {
                    return Err(ereport(ERROR)
                        .errcode(ERRCODE_UNDEFINED_OBJECT)
                        .errmsg(format!("{encoding} is not a valid encoding code"))
                        .errposition(el.location + 1)
                        .into_error()
                        .into());
                }
            } else {
                let name = explain::defGetString(mcx, el)?;
                encoding = mbutils::pg_valid_server_encoding(name);
                if encoding < 0 {
                    return Err(ereport(ERROR)
                        .errcode(ERRCODE_UNDEFINED_OBJECT)
                        .errmsg(format!("{name} is not a valid encoding name"))
                        .errposition(el.location + 1)
                        .into_error()
                        .into());
                }
            }
        }
    }
    let mut dbcollate = arg_str(collate_el)?;
    let mut dbctype = arg_str(ctype_el)?;
    let mut dblocale = arg_str(locale_el)?;
    // Explicit LC_COLLATE/LC_CTYPE override LOCALE (pg_dump emits both).
    if let Some(l) = dblocale {
        if dbcollate.is_none() {
            dbcollate = Some(l);
        }
        if dbctype.is_none() {
            dbctype = Some(l);
        }
    }
    if let Some(l) = arg_str(builtinlocale_el)? {
        dblocale = Some(l);
    }
    if let Some(l) = arg_str(iculocale_el)? {
        dblocale = Some(l);
    }
    let mut dbicurules = arg_str(icurules_el)?;
    let mut dblocprovider: u8 = 0;
    if let Some(p) = arg_str(locprovider_el)? {
        dblocprovider = if p.eq_ignore_ascii_case("builtin") {
            COLLPROVIDER_BUILTIN
        } else if p.eq_ignore_ascii_case("icu") {
            COLLPROVIDER_ICU
        } else if p.eq_ignore_ascii_case("libc") {
            COLLPROVIDER_LIBC
        } else {
            return Err(ereport(ERROR)
                .errcode(types_error::ERRCODE_INVALID_OBJECT_DEFINITION)
                .errmsg(format!("unrecognized locale provider: {p}"))
                .into_error()
                .into());
        };
    }
    let mut dbistemplate = false;
    if let Some(el) = istemplate_el {
        if el.arg.is_some() {
            dbistemplate = explain::defGetBoolean(el)?;
        }
    }
    let mut dballowconnections = true;
    if let Some(el) = allowconnections_el {
        if el.arg.is_some() {
            dballowconnections = explain::defGetBoolean(el)?;
        }
    }
    let mut dbconnlimit = DATCONNLIMIT_UNLIMITED;
    if let Some(el) = connlimit_el {
        if el.arg.is_some() {
            dbconnlimit = defGetInt32(el)?;
            if dbconnlimit < DATCONNLIMIT_UNLIMITED {
                return Err(ereport(ERROR)
                    .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
                    .errmsg(format!("invalid connection limit: {dbconnlimit}"))
                    .into_error()
                    .into());
            }
        }
    }
    // C reads collation_version without the arg guard: a bare option errors.
    let mut dbcollversion = match collversion_el {
        Some(el) => Some(explain::defGetString(mcx, el)?),
        None => None,
    };

    let datdba = match dbowner {
        Some(owner) => adt_acl::get_role_oid(owner, false)?,
        None => miscinit::GetUserId(),
    };

    if !have_createdb_privilege()? {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_INSUFFICIENT_PRIVILEGE)
            .errmsg("permission denied to create database".to_string())
            .into_error()
            .into());
    }

    // check_can_set_role(GetUserId(), datdba) (acl.c).
    if !adt_acl::member_can_set_role(miscinit::GetUserId(), datdba)? {
        let rolename = miscinit::GetUserNameFromId(mcx, datdba, false)?
            .expect("noerr=false yields a name");
        return Err(ereport(ERROR)
            .errcode(ERRCODE_INSUFFICIENT_PRIVILEGE)
            .errmsg(format!("must be able to SET ROLE \"{}\"", rolename.as_str()))
            .into_error()
            .into());
    }

    let dbtemplate = dbtemplate.unwrap_or("template1");
    let Some(src) = get_db_info(mcx, dbtemplate, ShareLock)? else {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_UNDEFINED_DATABASE)
            .errmsg(format!("template database \"{dbtemplate}\" does not exist"))
            .into_error()
            .into());
    };
    let src_dboid = src.oid;

    if database_is_invalid_form(src.datconnlimit) {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
            .errmsg(format!("cannot use invalid database \"{dbtemplate}\" as template"))
            .errhint("Use DROP DATABASE to drop invalid databases.".to_string())
            .into_error()
            .into());
    }

    if !src.datistemplate
        && !adt_acl::has_privs_of_role(miscinit::GetUserId(), src.datdba)?
    {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_INSUFFICIENT_PRIVILEGE)
            .errmsg(format!("permission denied to copy database \"{dbtemplate}\""))
            .into_error()
            .into());
    }

    let mut dbstrategy = CreateDBStrategy::WalLog;
    if let Some(strategy) = arg_str(strategy_el)? {
        if strategy.eq_ignore_ascii_case("wal_log") {
            dbstrategy = CreateDBStrategy::WalLog;
        } else if strategy.eq_ignore_ascii_case("file_copy") {
            dbstrategy = CreateDBStrategy::FileCopy;
        } else {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
                .errmsg(format!("invalid create database strategy \"{strategy}\""))
                .errhint("Valid strategies are \"wal_log\" and \"file_copy\".".to_string())
                .into_error()
                .into());
        }
    }
    if copy_mode == CreatedbCopyMode::Deferred && dbstrategy != CreateDBStrategy::FileCopy {
        // Janitor-only entry misuse, not a user-reachable state: wal_log has
        // no deferrable copy phase.
        return Err(ereport(ERROR)
            .errcode(types_error::ERRCODE_INTERNAL_ERROR)
            .errmsg(
                "createdb_deferred_file_copy requires STRATEGY file_copy".to_string(),
            )
            .into_error()
            .into());
    }

    if encoding < 0 {
        encoding = src.encoding;
    }
    let src_collate = src.datcollate.as_str();
    let src_ctype = src.datctype.as_str();
    let src_locale = src.datlocale.as_ref().map(|s| s.as_str());
    let src_icurules = src.daticurules.as_ref().map(|s| s.as_str());
    let dbcollate_v = dbcollate.unwrap_or(src_collate);
    let dbctype_v = dbctype.unwrap_or(src_ctype);
    if dblocprovider == 0 {
        dblocprovider = src.datlocprovider;
    }
    let mut dblocale_v: Option<&str> = dblocale;
    // C compares dblocale != src_locale by pointer below: true exactly when
    // this default assignment supplied the locale.
    let dblocale_from_template = dblocale_v.is_none() && dblocprovider == src.datlocprovider;
    if dblocale_v.is_none() && dblocprovider == src.datlocprovider {
        dblocale_v = src_locale;
    }
    if dbicurules.is_none() {
        dbicurules = src_icurules;
    }

    if !wchar::pg_valid_be_encoding(encoding) {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_WRONG_OBJECT_TYPE)
            .errmsg(format!("invalid server encoding {encoding}"))
            .into_error()
            .into());
    }

    // check_locale failures carry a provider-routing hint (dbcommands.c).
    let locale_hint = |b: elog::ErrorBuilder| -> elog::ErrorBuilder {
        if dblocprovider == COLLPROVIDER_BUILTIN {
            b.errhint(
                "If the locale name is specific to the builtin provider, use BUILTIN_LOCALE."
                    .to_string(),
            )
        } else if dblocprovider == COLLPROVIDER_ICU {
            b.errhint(
                "If the locale name is specific to the ICU provider, use ICU_LOCALE.".to_string(),
            )
        } else {
            b
        }
    };
    let (ok, canon) = pg_locale::check_locale(LC_COLLATE, dbcollate_v)?;
    if !ok {
        return Err(locale_hint(
            ereport(ERROR)
                .errcode(ERRCODE_WRONG_OBJECT_TYPE)
                .errmsg(format!("invalid LC_COLLATE locale name: \"{dbcollate_v}\"")),
        )
        .into_error()
        .into());
    }
    let dbcollate_s: String = canon.unwrap_or_else(|| dbcollate_v.to_owned());
    let (ok, canon) = pg_locale::check_locale(LC_CTYPE, dbctype_v)?;
    if !ok {
        return Err(locale_hint(
            ereport(ERROR)
                .errcode(ERRCODE_WRONG_OBJECT_TYPE)
                .errmsg(format!("invalid LC_CTYPE locale name: \"{dbctype_v}\"")),
        )
        .into_error()
        .into());
    }
    let dbctype_s: String = canon.unwrap_or_else(|| dbctype_v.to_owned());

    check_encoding_locale_matches(encoding, &dbcollate_s, &dbctype_s)?;

    // Validate provider-specific parameters.
    if dblocprovider != COLLPROVIDER_BUILTIN && builtinlocale_el.is_some() {
        return Err(ereport(ERROR)
            .errcode(types_error::ERRCODE_INVALID_OBJECT_DEFINITION)
            .errmsg(
                "BUILTIN_LOCALE cannot be specified unless locale provider is builtin".to_string(),
            )
            .into_error()
            .into());
    }
    if dblocprovider != COLLPROVIDER_ICU {
        if iculocale_el.is_some() {
            return Err(ereport(ERROR)
                .errcode(types_error::ERRCODE_INVALID_OBJECT_DEFINITION)
                .errmsg("ICU locale cannot be specified unless locale provider is ICU".to_string())
                .into_error()
                .into());
        }
        if dbicurules.is_some() {
            return Err(ereport(ERROR)
                .errcode(types_error::ERRCODE_INVALID_OBJECT_DEFINITION)
                .errmsg("ICU rules cannot be specified unless locale provider is ICU".to_string())
                .into_error()
                .into());
        }
    }

    // Validate and canonicalize the locale for the provider.
    let mut dblocale_s: Option<String> = dblocale_v.map(|s| s.to_owned());
    if dblocprovider == COLLPROVIDER_BUILTIN {
        // Happens if template0 uses the libc provider but the new database
        // uses builtin.
        let Some(locale) = dblocale_s.as_deref() else {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
                .errmsg("LOCALE or BUILTIN_LOCALE must be specified".to_string())
                .into_error()
                .into());
        };
        dblocale_s = Some(pg_locale::builtin_validate_locale(encoding, locale)?.to_owned());
    } else if dblocprovider == COLLPROVIDER_ICU {
        if !catalog_namespace::is_encoding_supported_by_icu(encoding) {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
                .errmsg(format!(
                    "encoding \"{}\" is not supported with ICU provider",
                    mbutils::pg_encoding_to_char(encoding)
                ))
                .into_error()
                .into());
        }
        // Happens if template0 uses the libc provider but the new database
        // uses icu.
        if dblocale_s.is_none() {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
                .errmsg("LOCALE or ICU_LOCALE must be specified".to_string())
                .into_error()
                .into());
        }
        // During binary upgrade, or when the locale came from the template
        // database, preserve the locale string; otherwise canonicalize to a
        // language tag.
        if !init_small::globals::IsBinaryUpgrade() && !dblocale_from_template {
            let locale = dblocale_s.as_deref().expect("checked non-NULL above");
            let elevel =
                types_error::ErrorLevel(guc_tables::vars::icu_validation_level.read());
            if let Some(langtag) = pg_locale::icu_language_tag(locale, elevel)? {
                if langtag != locale {
                    ereport(NOTICE)
                        .errmsg(format!(
                            "using standard form \"{langtag}\" for ICU locale \"{locale}\""
                        ))
                        .finish(loc("createdb"))?;
                    dblocale_s = Some(langtag);
                }
            }
        }
        pg_locale::icu_validate_locale(dblocale_s.as_deref().expect("checked non-NULL above"))?;
    }
    // For libc, the locale comes from datcollate and datctype.
    if dblocprovider == COLLPROVIDER_LIBC {
        dblocale_s = None;
    }
    if dbtemplate != "template0" {
        if encoding != src.encoding {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
                .errmsg(format!(
                    "new encoding ({}) is incompatible with the encoding of the template database ({})",
                    mbutils::pg_encoding_to_char(encoding),
                    mbutils::pg_encoding_to_char(src.encoding)
                ))
                .errhint(
                    "Use the same encoding as in the template database, or use template0 as template."
                        .to_string(),
                )
                .into_error()
                .into());
        }
        if dbcollate_s != src_collate {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
                .errmsg(format!(
                    "new collation ({dbcollate_s}) is incompatible with the collation of the template database ({src_collate})"
                ))
                .errhint(
                    "Use the same collation as in the template database, or use template0 as template."
                        .to_string(),
                )
                .into_error()
                .into());
        }
        if dbctype_s != src_ctype {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
                .errmsg(format!(
                    "new LC_CTYPE ({dbctype_s}) is incompatible with the LC_CTYPE of the template database ({src_ctype})"
                ))
                .errhint(
                    "Use the same LC_CTYPE as in the template database, or use template0 as template."
                        .to_string(),
                )
                .into_error()
                .into());
        }
        if dblocprovider != src.datlocprovider {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
                .errmsg(format!(
                    "new locale provider ({}) does not match locale provider of the template database ({})",
                    collprovider_name(dblocprovider),
                    collprovider_name(src.datlocprovider)
                ))
                .errhint(
                    "Use the same locale provider as in the template database, or use template0 as template."
                        .to_string(),
                )
                .into_error()
                .into());
        }
        if dblocprovider == COLLPROVIDER_ICU {
            // C Asserts both locales; providers match, so both are ICU-set.
            let val1 = dblocale_s.as_deref().expect("ICU database carries a locale");
            let val2 = src_locale.expect("ICU template carries a locale");
            if val1 != val2 {
                return Err(ereport(ERROR)
                    .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
                    .errmsg(format!(
                        "new ICU locale ({val1}) is incompatible with the ICU locale of the template database ({val2})"
                    ))
                    .errhint(
                        "Use the same ICU locale as in the template database, or use template0 as template."
                            .to_string(),
                    )
                    .into_error()
                    .into());
            }
            let val1 = dbicurules.unwrap_or("");
            let val2 = src_icurules.unwrap_or("");
            if val1 != val2 {
                return Err(ereport(ERROR)
                    .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
                    .errmsg(format!(
                        "new ICU collation rules ({val1}) are incompatible with the ICU collation rules of the template database ({val2})"
                    ))
                    .errhint(
                        "Use the same ICU collation rules as in the template database, or use template0 as template."
                            .to_string(),
                    )
                    .into_error()
                    .into());
            }
        }
    }

    // Template collversion check + fill-in. For libc the versioned locale is
    // datcollate; other providers version datlocale.
    let versioned_locale = |dbcollate_s: &str, dblocale_s: &Option<String>| -> String {
        if dblocprovider == COLLPROVIDER_LIBC {
            dbcollate_s.to_owned()
        } else {
            dblocale_s.as_deref().expect("non-libc provider carries a locale").to_owned()
        }
    };
    if let Some(src_collversion) = src.datcollversion.as_ref() {
        if collversion_el.is_none() {
            let locale = versioned_locale(&dbcollate_s, &dblocale_s);
            let actual = pg_locale::get_collation_actual_version(dblocprovider, &locale)?;
            let Some(actual) = actual else {
                return Err(ereport(ERROR)
                    .errmsg(format!(
                        "template database \"{dbtemplate}\" has a collation version, but no actual collation version could be determined"
                    ))
                    .into_error()
                    .into());
            };
            if actual != src_collversion.as_str() {
                return Err(ereport(ERROR)
                    .errmsg(format!(
                        "template database \"{dbtemplate}\" has a collation version mismatch"
                    ))
                    .errdetail(format!(
                        "The template database was created using collation version {}, but the operating system provides version {actual}.",
                        src_collversion.as_str()
                    ))
                    .errhint(format!(
                        "Rebuild all objects in the template database that use the default collation and run ALTER DATABASE {} REFRESH COLLATION VERSION, or build PostgreSQL with the right library version.",
                        format_type::quote_identifier(dbtemplate)
                    ))
                    .into_error()
                    .into());
            }
        }
    }
    let mut dbcollversion_s: Option<String> = dbcollversion.take().map(|s| s.to_owned());
    if dbcollversion_s.is_none() {
        dbcollversion_s = src.datcollversion.as_ref().map(|s| s.as_str().to_owned());
    }
    if dbcollversion_s.is_none() {
        let locale = versioned_locale(&dbcollate_s, &dblocale_s);
        dbcollversion_s = pg_locale::get_collation_actual_version(dblocprovider, &locale)?;
    }

    let src_deftablespace = src.dattablespace;
    let dst_deftablespace = match tablespacename_el {
        Some(el) if el.arg.is_some() => {
            let tablespacename = explain::defGetString(mcx, el)?;
            let dst = commands_tablespace::get_tablespace_oid(mcx, tablespacename, false)?;
            let aclresult = aclchk::object_aclcheck(
                TableSpaceRelationId,
                dst,
                miscinit::GetUserId(),
                adt_acl::ACL_CREATE,
            )?;
            if aclresult != aclchk::ACLCHECK_OK {
                aclchk::aclcheck_error(
                    aclresult,
                    types_nodes::parsenodes::ObjectType::OBJECT_TABLESPACE,
                    tablespacename,
                )?;
            }
            if dst == GLOBALTABLESPACE_OID {
                return Err(ereport(ERROR)
                    .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
                    .errmsg("pg_global cannot be used as default tablespace".to_string())
                    .into_error()
                    .into());
            }
            if dst != src_deftablespace {
                let srcpath = relpath::GetDatabasePath(mcx, src_dboid, dst)?;
                if matches!(std::fs::metadata(srcpath.as_str()), Ok(md) if md.is_dir())
                    && !fd::directory_is_empty(srcpath.as_str())?
                {
                    return Err(ereport(ERROR)
                        .errcode(types_error::ERRCODE_FEATURE_NOT_SUPPORTED)
                        .errmsg(format!(
                            "cannot assign new default tablespace \"{tablespacename}\""
                        ))
                        .errdetail(format!(
                            "There is a conflict because database \"{dbtemplate}\" already has some tables in this tablespace."
                        ))
                        .into_error()
                        .into());
                }
            }
            dst
        }
        _ => src_deftablespace,
    };

    if crate::get_database_oid(mcx, dbname, true)? != InvalidOid {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_DUPLICATE_DATABASE)
            .errmsg(format!("database \"{dbname}\" already exists"))
            .into_error()
            .into());
    }

    if let Some((notherbackends, npreparedxacts)) = procarray::CountOtherDBBackends(src_dboid)? {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_OBJECT_IN_USE)
            .errmsg(format!(
                "source database \"{dbtemplate}\" is being accessed by other users"
            ))
            .errdetail(crate::errdetail_busy_db(notherbackends, npreparedxacts))
            .into_error()
            .into());
    }

    let pg_database_rel = table::table_open(mcx, DATABASE_RELATION_ID, RowExclusiveLock)?;

    if dboid != InvalidOid {
        if let Some(existing) = crate::get_database_name(dboid)? {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
                .errmsg(format!(
                    "database OID {dboid} is already in use by database \"{existing}\""
                ))
                .into_error()
                .into());
        }
        if check_db_file_conflict(mcx, dboid)? {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
                .errmsg(format!(
                    "data directory with the specified OID {dboid} already exists"
                ))
                .into_error()
                .into());
        }
    } else {
        loop {
            dboid = catalog::GetNewOidWithIndex(
                mcx,
                &pg_database_rel,
                DatabaseOidIndexId,
                Anum_pg_database_oid as types_core::AttrNumber,
            )?;
            if !check_db_file_conflict(mcx, dboid)? {
                break;
            }
        }
    }

    let mut datname = NameData::default();
    datname.namestrcpy(dbname);
    let collate_text = varlena::cstring_to_text(mcx, dbcollate_s.as_bytes())?;
    let ctype_text = varlena::cstring_to_text(mcx, dbctype_s.as_bytes())?;
    let collversion_text = match dbcollversion_s.as_deref() {
        Some(v) => Some(varlena::cstring_to_text(mcx, v.as_bytes())?),
        None => None,
    };

    let mut values = [Datum::null(); Natts_pg_database];
    let mut nulls = [false; Natts_pg_database];
    values[0] = Datum::from_oid(dboid);
    values[1] = Datum::from_usize(datname.data.as_ptr() as usize);
    values[2] = Datum::from_oid(datdba);
    values[3] = Datum::from_i32(encoding);
    values[4] = Datum::from_char(dblocprovider as i8);
    values[5] = Datum::from_bool(dbistemplate);
    values[6] = Datum::from_bool(dballowconnections);
    values[7] = Datum::from_bool(src.dathasloginevt);
    values[8] = Datum::from_i32(dbconnlimit);
    values[9] = Datum::from_u32(src.datfrozenxid);
    values[10] = Datum::from_u32(src.datminmxid);
    values[11] = Datum::from_oid(dst_deftablespace);
    values[12] = Datum::from_usize(collate_text.as_bytes().as_ptr() as usize);
    values[13] = Datum::from_usize(ctype_text.as_bytes().as_ptr() as usize);
    // datlocale is null under the libc provider.
    let locale_text = match dblocale_s.as_deref() {
        Some(l) => Some(varlena::cstring_to_text(mcx, l.as_bytes())?),
        None => None,
    };
    match &locale_text {
        Some(v) => values[14] = Datum::from_usize(v.as_bytes().as_ptr() as usize),
        None => nulls[14] = true,
    }
    let icurules_text = match dbicurules {
        Some(r) => Some(varlena::cstring_to_text(mcx, r.as_bytes())?),
        None => None,
    };
    match &icurules_text {
        Some(v) => values[15] = Datum::from_usize(v.as_bytes().as_ptr() as usize),
        None => nulls[15] = true,
    }
    match &collversion_text {
        Some(v) => values[16] = Datum::from_usize(v.as_bytes().as_ptr() as usize),
        None => nulls[16] = true,
    }
    // datacl deliberately defaults to NULL rather than copying the template's.
    nulls[17] = true;

    let mut tuple = heaptuple::heap_form_tuple(mcx, pg_database_rel.descr(), &values, &nulls)?;
    catalog_indexing::CatalogTupleInsert(mcx, &pg_database_rel, &mut tuple)?;

    pg_depend::recordDependencyOnOwner(mcx, DATABASE_RELATION_ID, dboid, datdba)?;
    pg_shdepend::copyTemplateDependencies(mcx, src_dboid, dboid)?;

    if dbstrategy == CreateDBStrategy::WalLog {
        lmgr::LockSharedObject(DATABASE_RELATION_ID, dboid, 0, AccessShareLock)?;
    }

    let copy = match (dbstrategy, copy_mode) {
        (CreateDBStrategy::WalLog, _) => crate::walcopy::CreateDatabaseUsingWalLog(
            mcx,
            src_dboid,
            dboid,
            src_deftablespace,
            dst_deftablespace,
        )
        .map(|()| None),
        (CreateDBStrategy::FileCopy, CreatedbCopyMode::Deferred) => enumerate_file_copy_dirs(
            mcx,
            src_dboid,
            dboid,
            src_deftablespace,
            dst_deftablespace,
        )
        .map(|dirs| Some(DeferredFileCopy { db_oid: dboid, src_dboid, dirs })),
        (CreateDBStrategy::FileCopy, mode) => CreateDatabaseUsingFileCopy(
            mcx,
            src_dboid,
            dboid,
            src_deftablespace,
            dst_deftablespace,
            mode == CreatedbCopyMode::Inline,
        )
        .map(|()| None),
    };
    let deferred = match copy {
        Ok(d) => d,
        Err(e) => {
            let _ = createdb_failure_cleanup(mcx, src_dboid, dboid, dbstrategy);
            return Err(e);
        }
    };

    pg_database_rel.close(types_storage::lock::NoLock)?;

    xact::ForceSyncCommit();

    Ok((dboid, deferred))
}

#[cfg(test)]
mod tests {
    use super::*;

    // Provider-lane helpers (previously the whole non-libc lane was a 0A000
    // fence): collprovider_name spellings for the template-mismatch message,
    // and the BUILTIN lane's validation/canonicalization with C's errors.
    #[test]
    fn provider_lane_helpers_match_c() {
        assert_eq!(collprovider_name(COLLPROVIDER_BUILTIN), "builtin");
        assert_eq!(collprovider_name(COLLPROVIDER_ICU), "icu");
        assert_eq!(collprovider_name(COLLPROVIDER_LIBC), "libc");
        assert_eq!(collprovider_name(b'x'), "???");

        assert_eq!(pg_locale::builtin_validate_locale(PG_UTF8, "C.UTF8").unwrap(), "C.UTF-8");
        assert_eq!(pg_locale::builtin_validate_locale(PG_UTF8, "C.UTF-8").unwrap(), "C.UTF-8");
        // "C" works under any encoding (builtin_locale_encoding = -1).
        assert_eq!(pg_locale::builtin_validate_locale(PG_SQL_ASCII, "C").unwrap(), "C");
        let e = pg_locale::builtin_validate_locale(PG_UTF8, "en_US").unwrap_err();
        assert_eq!(e.message(), "invalid locale name \"en_US\" for builtin provider");
        // C.UTF-8 is pinned to UTF8.
        let e = pg_locale::builtin_validate_locale(PG_SQL_ASCII, "C.UTF-8").unwrap_err();
        assert_eq!(
            e.message(),
            format!(
                "encoding \"{}\" does not match locale \"C.UTF-8\"",
                mbutils::pg_encoding_to_char(PG_SQL_ASCII)
            )
        );
    }
}
