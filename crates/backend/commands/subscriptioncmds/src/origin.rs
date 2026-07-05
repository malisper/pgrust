// Minimal replication/logical/origin.c surface: catalog create/lookup/drop
// only. Shmem progress tracking has no readers here (no apply workers), so
// replorigin_get_progress is a constant InvalidXLogRecPtr.

use std::rc::Rc;

use datum::Datum;
use mcx::Mcx;
use types_core::{AttrNumber, InvalidOid, Oid};
use types_error::{
    PgError, PgResult, ERRCODE_PROGRAM_LIMIT_EXCEEDED, ERRCODE_UNDEFINED_OBJECT,
};
use types_rel::{AccessExclusiveLock, ExclusiveLock, NoLock, RowExclusiveLock};
use types_scan::scankey::{BTEqualStrategyNumber, ScanKeyData};
use types_snapshot::{SnapshotData, SNAPSHOT_DIRTY};

use cache_syscache::cacheinfo::{REPLORIGIDENT, REPLORIGNAME};
use cache_syscache::{
    ReleaseSysCache, SearchSysCache1, SearchSysCacheCopy, SysCacheGetAttr, SysCacheKey,
};
use catalog::{ReplicationOriginIdentIndex, ReplicationOriginRelationId};
use types_core::fmgr::F_OIDEQ;

const Anum_pg_replication_origin_roident: i32 = 1;
const Anum_pg_replication_origin_roname: i32 = 2;
const Natts_pg_replication_origin: usize = 2;
const MAX_RONAME_LEN: usize = 512;

pub(crate) fn ReplicationOriginNameForLogicalRep(subid: Oid, relid: Oid) -> String {
    if relid == InvalidOid {
        format!("pg_{subid}")
    } else {
        format!("pg_{subid}_{relid}")
    }
}

fn text_datum(mcx: Mcx<'_>, s: &str) -> PgResult<Datum> {
    let img = varlena::cstring_to_text(mcx, s.as_bytes())?.into_image().leak();
    Ok(Datum::from_usize(img.as_ptr() as usize))
}

pub(crate) fn replorigin_create(mcx: Mcx<'_>, roname: &str) -> PgResult<Oid> {
    if roname.len() > MAX_RONAME_LEN {
        return Err(Box::new(
            PgError::error("replication origin name is too long")
                .with_sqlstate(ERRCODE_PROGRAM_LIMIT_EXCEEDED)
                .with_detail(format!(
                    "Replication origin names must be no longer than {MAX_RONAME_LEN} bytes."
                )),
        ));
    }

    let roname_d = text_datum(mcx, roname)?;
    let rel = table::table_open(mcx, ReplicationOriginRelationId, ExclusiveLock)?;

    let mut created = InvalidOid;
    for roident in 1..0xFFFFu32 {
        let mut key = ScanKeyData::empty();
        key.sk_attno = Anum_pg_replication_origin_roident as AttrNumber;
        key.sk_strategy = BTEqualStrategyNumber;
        key.sk_collation = types_core::C_COLLATION_OID;
        key.sk_func = fmgr_seams::fmgr_info::call(F_OIDEQ)
            .unwrap_or_else(|e| panic!("fmgr_info(F_OIDEQ) failed: {e:?}"));
        key.sk_argument = Datum::from_oid(roident);

        let dirty = Rc::new(SnapshotData::sentinel(mcx, SNAPSHOT_DIRTY));
        let mut scan = genam::systable_beginscan(
            mcx,
            &rel,
            ReplicationOriginIdentIndex,
            true,
            Some(dirty),
            &[key],
        )?;
        let collides = genam::systable_getnext(mcx, &mut scan)?.is_some();
        genam::systable_endscan(mcx, scan)?;

        if !collides {
            let mut values = [Datum::null(); Natts_pg_replication_origin];
            let nulls = [false; Natts_pg_replication_origin];
            values[(Anum_pg_replication_origin_roident - 1) as usize] =
                Datum::from_oid(roident);
            values[(Anum_pg_replication_origin_roname - 1) as usize] = roname_d;
            let mut tup = heaptuple::heap_form_tuple(mcx, rel.descr(), &values, &nulls)?;
            catalog_indexing::CatalogTupleInsert(mcx, &rel, &mut tup)?;
            xact::CommandCounterIncrement()?;
            created = roident;
            break;
        }
    }

    rel.close(ExclusiveLock)?;

    if created == InvalidOid {
        return Err(Box::new(
            PgError::error("could not find free replication origin ID")
                .with_sqlstate(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
        ));
    }
    Ok(created)
}

// replorigin_check_prerequisites(check_origins=false, recoveryOK=false).
fn replorigin_check_prerequisites_create() -> PgResult<()> {
    if transam_xlog_seams::recovery_in_progress::call() {
        return Err(Box::new(
            PgError::error("cannot manipulate replication origins during recovery")
                .with_sqlstate(types_error::ERRCODE_READ_ONLY_SQL_TRANSACTION),
        ));
    }
    Ok(())
}

// IsReservedOriginName: "none" or "any" (pg_strcasecmp).
fn is_reserved_origin_name(name: &str) -> bool {
    name.eq_ignore_ascii_case("none") || name.eq_ignore_ascii_case("any")
}

pub fn fc_pg_replication_origin_create(
    _flinfo: Option<&mut types_fmgr::FmgrInfo>,
    fcinfo: &mut types_fmgr::FunctionCallInfoBaseData,
) -> PgResult<Datum> {
    replorigin_check_prerequisites_create()?;

    // SAFETY: strict fn — arg 0 is a non-null text varlena.
    let name = unsafe { fcinfo.arg_varlena_packed(0)? };
    let name = String::from_utf8_lossy(name.data()).into_owned();

    if catalog::IsReservedName(&name) || is_reserved_origin_name(&name) {
        return Err(Box::new(
            PgError::error(format!("replication origin name \"{name}\" is reserved"))
                .with_sqlstate(types_error::ERRCODE_RESERVED_NAME)
                .with_detail(
                    "Origin names \"any\", \"none\", and names starting with \"pg_\" are reserved."
                        .to_string(),
                ),
        ));
    }

    let roident = replorigin_create(fcinfo.result_mcx(), &name)?;
    Ok(Datum::from_oid(roident))
}

pub const ORIGIN_BUILTINS: &[types_fmgr::FmgrBuiltin] = &[types_fmgr::FmgrBuiltin {
    foid: 6003,
    name: "pg_replication_origin_create",
    nargs: 1,
    strict: true,
    retset: false,
    func: fc_pg_replication_origin_create,
}];

pub(crate) fn replorigin_by_name(roname: &str, missing_ok: bool) -> PgResult<Oid> {
    let mut roident = InvalidOid;
    if let Some(tup) = SearchSysCache1(REPLORIGNAME, SysCacheKey::Str(roname))? {
        roident =
            SysCacheGetAttr(REPLORIGNAME, &tup, Anum_pg_replication_origin_roident)?.0.as_oid();
        ReleaseSysCache(tup);
    } else if !missing_ok {
        return Err(Box::new(
            PgError::error(format!("replication origin \"{roname}\" does not exist"))
                .with_sqlstate(ERRCODE_UNDEFINED_OBJECT),
        ));
    }
    Ok(roident)
}

pub(crate) fn replorigin_drop_by_name(
    mcx: Mcx<'_>,
    roname: &str,
    missing_ok: bool,
) -> PgResult<()> {
    let rel = table::table_open(mcx, ReplicationOriginRelationId, RowExclusiveLock)?;
    let roident = replorigin_by_name(roname, missing_ok)?;

    lmgr::LockSharedObject(ReplicationOriginRelationId, roident, 0, AccessExclusiveLock)?;

    let Some(tup) = SearchSysCacheCopy(
        mcx,
        REPLORIGIDENT,
        SysCacheKey::Value(Datum::from_oid(roident)),
        SysCacheKey::UNUSED,
        SysCacheKey::UNUSED,
        SysCacheKey::UNUSED,
    )?
    else {
        if !missing_ok {
            return Err(Box::new(PgError::error(format!(
                "cache lookup failed for replication origin with ID {roident}"
            ))));
        }
        lmgr::UnlockSharedObject(ReplicationOriginRelationId, roident, 0, AccessExclusiveLock)?;
        return rel.close(RowExclusiveLock);
    };

    let tid = tup.as_tuple().t_self;
    catalog_indexing::CatalogTupleDelete(&rel, &tid)?;
    xact::CommandCounterIncrement()?;

    rel.close(NoLock)
}
