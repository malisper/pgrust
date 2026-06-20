//! `pg_get_catalog_foreign_keys()` (OID 6159) registered as an executor-frame
//! materialize-mode set-returning function.
//!
//! `misc.c`'s `pg_get_catalog_foreign_keys` is a value-per-call SRF emitting one
//! `(fktable regclass, fkcols text[], pktable regclass, pkcols text[], is_array
//! bool, is_opt bool)` row per `sys_fk_relationships[]` entry (the catalog
//! foreign-key metadata transcribed from `catalog/system_fk_info.h`), building
//! each `text[]` column via `array_in` over the comma-separated column list. The
//! row-render core (the `sys_fk_relationships[]` walk + the `array_in`-equivalent
//! identifier split for `fk_columns`/`pk_columns`) is ported in
//! [`backend_utils_adt_misc::pg_get_catalog_foreign_keys`], which hands back a
//! `Vec<CatalogForeignKeyRow>` whose `fkcols`/`pkcols` are already the decoded
//! `text[]` element byte strings.
//!
//! Here that core is driven over the executor frame in materialize mode (the row
//! set is fixed and known up front, so the whole tuplestore is filled once).
//! `InitMaterializedSRF` with `MAT_SRF_USE_EXPECTED_DESC` takes the executor's
//! already-resolved `(oid, text[], oid, text[], bool, bool)` descriptor (skipping
//! the catalog `get_call_result_type`), the rows are appended via
//! `materialized_srf_putvalues`, and the entry point returns SQL NULL. The
//! `text[]` columns are assembled with `construct_text_array` (C: `array_in`)
//! into header-ful on-disk array images placed on the by-reference Datum lane.
//! Registered from [`register_pg_get_catalog_foreign_keys`] (called by
//! `init_seams`); it bypasses the by-OID builtin registry whose tag-only
//! `resultinfo` cannot carry the live `ReturnSetInfo` (the WONTFIX dual-home).

use mcx::Mcx;
use types_core::Oid;
use types_nodes::fmgr::FunctionCallInfoBaseData;
use types_nodes::funcapi::MAT_SRF_USE_EXPECTED_DESC;
use types_error::PgResult;
use types_tuple::backend_access_common_heaptuple::Datum;

use backend_utils_fmgr_funcapi::srf_support::{InitMaterializedSRF, materialized_srf_putvalues};

use crate::register_srf;

/// `pg_get_catalog_foreign_keys()` (OID 6159).
const PG_GET_CATALOG_FOREIGN_KEYS: Oid = 6159;

/// Register `pg_get_catalog_foreign_keys` in the executor-frame SRF table.
pub(crate) fn register_pg_get_catalog_foreign_keys() {
    register_srf(PG_GET_CATALOG_FOREIGN_KEYS, pg_get_catalog_foreign_keys);
}

/// Copy a raw header-ful varlena array image (`text[]` on-disk bytes) into an
/// `mcx`-owned by-reference Datum — exactly the pointer C's `array_in` returned
/// into the per-query context. The image already carries a complete varlena
/// header (the array constructor emits one), so it round-trips header-for-header
/// through the tuplestore / printtup output lane.
fn byref_image<'mcx>(mcx: Mcx<'mcx>, image: &[u8]) -> PgResult<Datum<'mcx>> {
    let mut buf = mcx::PgVec::new_in(mcx);
    buf.try_reserve(image.len())
        .map_err(|_| mcx.oom(image.len()))?;
    buf.extend_from_slice(image);
    Ok(Datum::ByRef(buf))
}

/// `pg_get_catalog_foreign_keys(PG_FUNCTION_ARGS)` (misc.c) over the executor
/// frame.
fn pg_get_catalog_foreign_keys<'mcx>(
    fcinfo: &mut FunctionCallInfoBaseData<'mcx>,
) -> PgResult<Datum<'mcx>> {
    let mcx: Mcx<'mcx> = fcinfo
        .fn_mcxt
        .expect("pg_get_catalog_foreign_keys: fn_mcxt set by ExecMakeTableFunctionResult");

    // The catalog foreign-key render core (the sys_fk_relationships[] walk; pure
    // static catalog metadata). `fkcols`/`pkcols` are the decoded text[] element
    // byte strings (C: array_in result elements).
    let rows = backend_utils_adt_misc::pg_get_catalog_foreign_keys()?;

    // C: get_call_result_type → the (regclass, text[], regclass, text[], bool,
    // bool) row type. Take the executor's already-resolved descriptor.
    InitMaterializedSRF(fcinfo, MAT_SRF_USE_EXPECTED_DESC)?;

    let rsinfo = fcinfo
        .resultinfo
        .as_mut()
        .expect("pg_get_catalog_foreign_keys: InitMaterializedSRF establishes fcinfo->resultinfo");

    for row in &rows {
        // values[1]/values[3] = array_in(fk_columns/pk_columns) — build the
        // text[] on-disk image (construct_array_builtin(elems, n, TEXTOID)) and
        // place it on the by-reference Datum lane.
        let fkcols_img =
            backend_utils_adt_array_more_seams::construct_text_array::call(&row.fkcols)?;
        let pkcols_img =
            backend_utils_adt_array_more_seams::construct_text_array::call(&row.pkcols)?;

        // Catalog column order: (fktable regclass, fkcols text[], pktable
        // regclass, pkcols text[], is_array bool, is_opt bool).
        let values = [
            Datum::from_oid(row.fktable),
            byref_image(mcx, &fkcols_img)?,
            Datum::from_oid(row.pktable),
            byref_image(mcx, &pkcols_img)?,
            Datum::from_bool(row.is_array),
            Datum::from_bool(row.is_opt),
        ];
        let nulls = [false, false, false, false, false, false];
        materialized_srf_putvalues(rsinfo, &values, &nulls)?;
    }

    // C: SRF_RETURN_DONE — the whole set is in the materialize tuplestore.
    fcinfo.isnull = true;
    Ok(Datum::null())
}
