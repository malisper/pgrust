//! `pg_available_extensions()` (OID 3082) and `pg_available_extension_versions()`
//! (OID 3083) over the executor frame — the materialize-mode SRFs backing the
//! `pg_available_extensions` / `pg_available_extension_versions` system views.
//!
//! The control-directory walk + `parse_extension_control_file` (and the
//! version-update graph for the `_versions` variant) lives in
//! [`extension`]. This module is the executor-frame adapter:
//! `InitMaterializedSRF` establishes `fcinfo->resultinfo`, then each row is
//! emitted as the typed `(values, nulls)` pair C builds with
//! `tuplestore_putvalues` (the `name`/`name[]` columns via the `namein` /
//! `convert_requires_to_datum` images, the text columns via `cstring_to_text`,
//! the bools directly). They are dispatched through this crate's executor-frame
//! SRF table because the by-OID fmgr home's tag-only `resultinfo` can't carry the
//! live `ReturnSetInfo` (the WONTFIX dual-home).

use ::mcx::Mcx;
use ::types_core::Oid;
use ::types_error::PgResult;
use ::nodes::fmgr::FunctionCallInfoBaseData;
use ::nodes::funcapi::MAT_SRF_USE_EXPECTED_DESC;
use types_tuple::heaptuple::Datum;

use ::funcapi::srf_support::{materialized_srf_putvalues, InitMaterializedSRF};

use crate::register_srf;

/// `pg_available_extensions()` (OID 3082).
const PG_AVAILABLE_EXTENSIONS: Oid = 3082;
/// `pg_available_extension_versions()` (OID 3083).
const PG_AVAILABLE_EXTENSION_VERSIONS: Oid = 3083;

/// Register the available-extension view SRFs in the executor-frame SRF table.
pub(crate) fn register_pg_available_extensions() {
    register_srf(PG_AVAILABLE_EXTENSIONS, pg_available_extensions);
    register_srf(
        PG_AVAILABLE_EXTENSION_VERSIONS,
        pg_available_extension_versions,
    );
}

/// `CStringGetTextDatum(s)` over the call's per-query context.
fn text_datum<'mcx>(mcx: Mcx<'mcx>, s: &str) -> PgResult<Datum<'mcx>> {
    varlena_seams::cstring_to_text_v::call(mcx, s)
}

/// `DirectFunctionCall1(namein, CStringGetDatum(s))` — a `NAMEDATALEN`-byte
/// NUL-padded `NameData` by-reference `Datum` image.
fn name_datum<'mcx>(mcx: Mcx<'mcx>, s: &str) -> PgResult<Datum<'mcx>> {
    use ::types_core::fmgr::NAMEDATALEN;
    let n = NAMEDATALEN as usize;
    let mut img = vec![0u8; n];
    let src = s.as_bytes();
    let take = core::cmp::min(src.len(), n - 1);
    img[..take].copy_from_slice(&src[..take]);
    Datum::from_byref_bytes_in(mcx, &img)
}

/// `pg_available_extensions(PG_FUNCTION_ARGS)` (extension.c:2334) over the
/// executor frame.
fn pg_available_extensions<'mcx>(
    fcinfo: &mut FunctionCallInfoBaseData<'mcx>,
) -> PgResult<Datum<'mcx>> {
    let mcx: Mcx<'mcx> = fcinfo
        .fn_mcxt
        .expect("pg_available_extensions: fn_mcxt set by ExecMakeTableFunctionResult");

    let rows = extension::pg_available_extensions()?;

    InitMaterializedSRF(fcinfo, MAT_SRF_USE_EXPECTED_DESC)?;

    let rsinfo = fcinfo
        .resultinfo
        .as_mut()
        .expect("pg_available_extensions: InitMaterializedSRF establishes fcinfo->resultinfo");

    for row in &rows {
        let mut values: [Datum<'mcx>; 3] = core::array::from_fn(|_| Datum::null());
        let mut nulls = [false; 3];

        // [0] name — name
        values[0] = name_datum(mcx, &row.name)?;
        // [1] default_version — text / NULL
        match &row.default_version {
            Some(s) => values[1] = text_datum(mcx, s)?,
            None => nulls[1] = true,
        }
        // [2] comment — text / NULL
        match &row.comment {
            Some(s) => values[2] = text_datum(mcx, s)?,
            None => nulls[2] = true,
        }

        materialized_srf_putvalues(rsinfo, &values, &nulls)?;
    }

    fcinfo.isnull = true;
    Ok(Datum::null())
}

/// `pg_available_extension_versions(PG_FUNCTION_ARGS)` (extension.c:2432) over
/// the executor frame.
fn pg_available_extension_versions<'mcx>(
    fcinfo: &mut FunctionCallInfoBaseData<'mcx>,
) -> PgResult<Datum<'mcx>> {
    let mcx: Mcx<'mcx> = fcinfo
        .fn_mcxt
        .expect("pg_available_extension_versions: fn_mcxt set by ExecMakeTableFunctionResult");

    let rows = extension::pg_available_extension_versions()?;

    InitMaterializedSRF(fcinfo, MAT_SRF_USE_EXPECTED_DESC)?;

    let rsinfo = fcinfo.resultinfo.as_mut().expect(
        "pg_available_extension_versions: InitMaterializedSRF establishes fcinfo->resultinfo",
    );

    for row in &rows {
        let mut values: [Datum<'mcx>; 8] = core::array::from_fn(|_| Datum::null());
        let mut nulls = [false; 8];

        // [0] name — name
        values[0] = name_datum(mcx, &row.name)?;
        // [1] version — text
        values[1] = text_datum(mcx, &row.version)?;
        // [2] superuser — bool
        values[2] = Datum::from_bool(row.superuser);
        // [3] trusted — bool
        values[3] = Datum::from_bool(row.trusted);
        // [4] relocatable — bool
        values[4] = Datum::from_bool(row.relocatable);
        // [5] schema — name / NULL
        match &row.schema {
            Some(s) => values[5] = name_datum(mcx, s)?,
            None => nulls[5] = true,
        }
        // [6] requires — name[] / NULL
        if row.requires.is_empty() {
            nulls[6] = true;
        } else {
            values[6] = extension::convert_requires_to_datum(mcx, &row.requires)?;
        }
        // [7] comment — text / NULL
        match &row.comment {
            Some(s) => values[7] = text_datum(mcx, s)?,
            None => nulls[7] = true,
        }

        materialized_srf_putvalues(rsinfo, &values, &nulls)?;
    }

    fcinfo.isnull = true;
    Ok(Datum::null())
}
