//! `pg_tablespace_databases(oid)` (OID 2556) registered as an executor-frame
//! materialize-mode set-returning function.
//!
//! `misc.c`'s `pg_tablespace_databases` is a value-per-call SRF emitting one
//! `oid` row per database directory found under the given tablespace's storage
//! path. The directory-scan core (the `$PGDATA/.../<tablespace>` enumeration,
//! filtering `pg_internal.init`-less database subdirectories) is ported in
//! [`misc::pg_tablespace_databases`], which hands back a
//! `Vec<Oid>` of database OIDs.
//!
//! Here that core is driven over the executor frame in materialize mode: the
//! database-OID set is enumerated once and the whole tuplestore filled.
//! `InitMaterializedSRF` with `MAT_SRF_USE_EXPECTED_DESC` takes the executor's
//! already-resolved one-column `oid` descriptor; the OIDs are appended via
//! `materialized_srf_putvalues`. Registered from
//! [`register_pg_tablespace_databases`] (called by `init_seams`); it bypasses the
//! by-OID builtin registry whose tag-only `resultinfo` cannot carry the live
//! `ReturnSetInfo` (the WONTFIX dual-home).

use types_core::Oid;
use nodes::fmgr::FunctionCallInfoBaseData;
use nodes::funcapi::MAT_SRF_USE_EXPECTED_DESC;
use types_error::PgResult;
use types_tuple::heaptuple::Datum;

use funcapi::srf_support::{InitMaterializedSRF, materialized_srf_putvalues};

use crate::register_srf;

/// `pg_tablespace_databases(oid)` (OID 2556).
const PG_TABLESPACE_DATABASES: Oid = 2556;

/// Register `pg_tablespace_databases` in the executor-frame SRF table.
pub(crate) fn register_pg_tablespace_databases() {
    register_srf(PG_TABLESPACE_DATABASES, pg_tablespace_databases);
}

/// `pg_tablespace_databases(PG_FUNCTION_ARGS)` (misc.c) over the executor frame.
fn pg_tablespace_databases<'mcx>(
    fcinfo: &mut FunctionCallInfoBaseData<'mcx>,
) -> PgResult<Datum<'mcx>> {
    // C: tablespaceOid = PG_GETARG_OID(0). The function is `proisstrict => 't'`,
    // so a NULL argument never reaches here (the strict-arg guard yields an empty
    // set in ExecMakeTableFunctionResult before dispatch).
    let tablespace_oid = fcinfo.args[0].value.as_oid();

    // The directory-scan core (the database-OID enumeration).
    let dboids = misc::pg_tablespace_databases(tablespace_oid)?;

    // C: the SRF returns one `oid` per database directory; take the executor's
    // already-resolved one-column `oid` descriptor.
    InitMaterializedSRF(fcinfo, MAT_SRF_USE_EXPECTED_DESC)?;

    let rsinfo = fcinfo
        .resultinfo
        .as_mut()
        .expect("pg_tablespace_databases: InitMaterializedSRF establishes fcinfo->resultinfo");

    for dboid in dboids {
        let values = [Datum::from_oid(dboid)];
        let nulls = [false];
        materialized_srf_putvalues(rsinfo, &values, &nulls)?;
    }

    fcinfo.isnull = true;
    Ok(Datum::null())
}
