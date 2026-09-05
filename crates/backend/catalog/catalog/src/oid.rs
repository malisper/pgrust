use std::rc::Rc;

use datum::Datum;
use mcx::Mcx;
use types_core::{
    AttrNumber, ForkNumber, InvalidOid, Oid, ProcNumber, RelFileNumber, F_OIDEQ,
    INVALID_PROC_NUMBER,
};
use types_error::{PgError, PgResult, LOG};
use types_rel::Relation;
use types_scan::{BTEqualStrategyNumber, ScanKeyData};
use types_snapshot::{SnapshotData, SnapshotType};
use types_storage::RelFileLocator;

pub const ClassOidIndexId: Oid = 2662;
pub const Anum_pg_class_oid: i32 = 1;

const RELPERSISTENCE_PERMANENT: u8 = b'p';
const RELPERSISTENCE_UNLOGGED: u8 = b'u';
const RELPERSISTENCE_TEMP: u8 = b't';

// catalog.c:55-56
const GETNEWOID_LOG_THRESHOLD: u64 = 1_000_000;
const GETNEWOID_LOG_MAX_INTERVAL: u64 = 128_000_000;

// CHECK_FOR_INTERRUPTS at the top of each collision-retry iteration
// (catalog.c:475 GetNewOidWithIndex, catalog.c:600 GetNewRelFileNumber):
// a near-full OID space spins these loops for a long time (the huge-toast-
// table case), and C keeps them cancellable per probe.  Gated on
// InterruptPending so the common single-iteration case costs one global load.
#[inline(always)]
fn check_for_interrupts() -> PgResult<()> {
    if init_small::globals::InterruptPending() {
        return postgres_seams::check_for_interrupts::call();
    }
    Ok(())
}

// catalog.c:479 ScanKeyInit -> fmgr_info(F_OIDEQ): an fmgr failure is a
// catchable elog(ERROR), never a panic.
pub(crate) fn oid_eq_key(attno: AttrNumber, oid: Oid) -> PgResult<ScanKeyData> {
    let mut key = ScanKeyData::empty();
    key.sk_attno = attno;
    key.sk_strategy = BTEqualStrategyNumber;
    key.sk_func = fmgr_seams::fmgr_info::call(F_OIDEQ)?;
    key.sk_argument = Datum::from_oid(oid);
    Ok(key)
}

// catalog.c:511-519: the next retry count to log at -- doubling until it
// reaches GETNEWOID_LOG_MAX_INTERVAL, then growing by GETNEWOID_LOG_MAX_INTERVAL.
pub(crate) fn next_retries_before_log(retries_before_log: u64) -> u64 {
    if retries_before_log * 2 <= GETNEWOID_LOG_MAX_INTERVAL {
        retries_before_log * 2
    } else {
        retries_before_log + GETNEWOID_LOG_MAX_INTERVAL
    }
}

/// catalog.c:473-535: GetNewOidWithIndex's collision loop with the OID source
/// and the index probe injected, so the retry bookkeeping and its LOG reports
/// can be witnessed across millions of collisions without a catalog.
pub(crate) fn get_new_oid_loop(
    relname: &str,
    mut next_oid: impl FnMut() -> PgResult<Oid>,
    mut collides: impl FnMut(Oid) -> PgResult<bool>,
) -> PgResult<Oid> {
    let mut retries: u64 = 0;
    let mut retries_before_log: u64 = GETNEWOID_LOG_THRESHOLD;
    let new_oid = loop {
        check_for_interrupts()?;
        let new_oid = next_oid()?;
        let collides = collides(new_oid)?;

        // catalog.c:493-519: past GETNEWOID_LOG_THRESHOLD probes without an
        // unused OID, LOG at exponentially growing intervals (capped at
        // GETNEWOID_LOG_MAX_INTERVAL) so the server log is not flooded.
        if retries >= retries_before_log {
            let detail = if retries == 1 {
                format!("OID candidates have been checked {retries} time, but no unused OID has been found yet.")
            } else {
                format!("OID candidates have been checked {retries} times, but no unused OID has been found yet.")
            };
            elog_seams::ereport_msg::call(
                LOG,
                format!("still searching for an unused OID in relation \"{relname}\""),
                Some(detail),
            )?;
            retries_before_log = next_retries_before_log(retries_before_log);
        }

        retries += 1;
        if !collides {
            break new_oid;
        }
    };

    // catalog.c:524-534: once at least one progress LOG went out, LOG the
    // completion too.
    if retries > GETNEWOID_LOG_THRESHOLD {
        let msg = if retries == 1 {
            format!("new OID has been assigned in relation \"{relname}\" after {retries} retry")
        } else {
            format!("new OID has been assigned in relation \"{relname}\" after {retries} retries")
        };
        elog_seams::ereport_msg::call(LOG, msg, None)?;
    }

    Ok(new_oid)
}

// SnapshotAny probe: uncommitted rows must count as collisions.
pub fn GetNewOidWithIndex<'mcx>(
    mcx: Mcx<'mcx>,
    relation: &Relation<'mcx>,
    indexId: Oid,
    oidcolumn: AttrNumber,
) -> PgResult<Oid> {
    debug_assert!(crate::IsSystemRelation(relation.data_rc()));
    if miscinit_seams::is_bootstrap_processing_mode::call() {
        return varsup::GetNewObjectId();
    }
    let snapshot = Rc::new(SnapshotData::sentinel(mcx, SnapshotType::SNAPSHOT_ANY));
    get_new_oid_loop(relation.data_rc().name(), varsup::GetNewObjectId, |new_oid| {
        let key = [oid_eq_key(oidcolumn, new_oid)?];
        let mut scan =
            genam::systable_beginscan(mcx, relation, indexId, true, Some(Rc::clone(&snapshot)), &key)?;
        let collides = genam::systable_getnext(mcx, &mut scan)?.is_some();
        genam::systable_endscan(mcx, scan)?;
        Ok(collides)
    })
}

/// catalog.c:571-584: the proc number a new relfilenumber is probed under.
/// A temp relation lives under ProcNumberForTempRelations() -- the parallel
/// leader's proc number inside a worker -- and an unknown relpersistence is a
/// catchable elog(ERROR) printing the byte as a character.
pub(crate) fn relfilenumber_proc_number(relpersistence: u8) -> PgResult<ProcNumber> {
    match relpersistence {
        RELPERSISTENCE_TEMP => Ok(init_small::globals::ProcNumberForTempRelations()),
        RELPERSISTENCE_UNLOGGED | RELPERSISTENCE_PERMANENT => Ok(INVALID_PROC_NUMBER),
        _ => Err(Box::new(PgError::error(format!(
            "invalid relpersistence: {}",
            relpersistence as char
        )))),
    }
}

pub fn GetNewRelFileNumber<'mcx>(
    mcx: Mcx<'mcx>,
    reltablespace: Oid,
    pg_class: Option<&Relation<'mcx>>,
    relpersistence: u8,
) -> PgResult<RelFileNumber> {
    let proc_number = relfilenumber_proc_number(relpersistence)?;

    let spc_oid = if reltablespace != InvalidOid {
        reltablespace
    } else {
        init_small::globals::MyDatabaseTableSpace()
    };
    let db_oid = if spc_oid == relpath::GLOBALTABLESPACE_OID {
        InvalidOid
    } else {
        init_small::globals::MyDatabaseId()
    };

    loop {
        check_for_interrupts()?;
        let rel_number: RelFileNumber = match pg_class {
            Some(rel) => {
                GetNewOidWithIndex(mcx, rel, ClassOidIndexId, Anum_pg_class_oid as AttrNumber)?
            }
            None => varsup::GetNewObjectId()?,
        };
        let locator = RelFileLocator::new(spc_oid, db_oid, rel_number);
        // access(F_OK) relative to the datadir cwd, as C's relpath probe.
        let rpath = relpath::GetRelationPath(locator, proc_number, ForkNumber::MAIN_FORKNUM);
        if !std::path::Path::new(&rpath).exists() {
            return Ok(rel_number);
        }
    }
}
