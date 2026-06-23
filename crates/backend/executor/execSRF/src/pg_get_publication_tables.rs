//! `pg_get_publication_tables(VARIADIC pubnames text[])` (OID 6119) registered
//! as an executor-frame materialize-mode set-returning function — the function
//! `pg_publication_tables` / `pg_publication_rel` system views and logical
//! replication tablesync use to enumerate the published tables (with their
//! column lists and row filters) of one or more publications.
//!
//! The pure-data row builder (`gather_publication_tables` +
//! `build_publication_table_rows`, walking each publication's relation set,
//! filtering partitions for `publish_via_partition_root`, and projecting the
//! per-table column list / row filter) is ported in
//! [`pg_publication`] and exposed via the
//! `build_publication_table_rows` seam, which hands back one
//! [`PublicationTableRow`] per published table.
//!
//! Here that core is driven over the executor frame in materialize mode (the C
//! `pg_get_publication_tables` is a value-per-call SRF whose row set is fully
//! determined by the input array, so the whole tuplestore is filled once,
//! emitting the identical rows the C per-call series would). The
//! `(pubid oid, relid oid, attrs int2vector, qual pg_node_tree)` 4-column
//! descriptor is taken from the executor via `MAT_SRF_USE_EXPECTED_DESC`, the
//! rows are appended via `materialized_srf_putvalues`, and the entry point
//! returns SQL NULL. Registered from [`register_pg_get_publication_tables`]
//! (called by `init_seams`); it bypasses the by-OID builtin registry whose
//! tag-only `resultinfo` cannot carry the live `ReturnSetInfo` (the WONTFIX
//! dual-home, same as [`crate::pg_event_trigger_dropped_objects`]).
//!
//! The `text[]` argument arrives header-ful on the by-reference side channel
//! (a varlena array image; C `PG_GETARG_ARRAYTYPE_P(0)`). It is deconstructed
//! once into its per-element UTF-8 publication-name strings (C
//! `deconstruct_array_builtin(arr, TEXTOID, ...)` + `TextDatumGetCString`),
//! which then outlive the immutable `fcinfo` borrow (which must end before the
//! mutable `InitMaterializedSRF` / `putvalues` calls below).

extern crate alloc;

use mcx::Mcx;
use types_core::Oid;
use types_error::PgResult;
use ::nodes::fmgr::FunctionCallInfoBaseData;
use ::nodes::funcapi::MAT_SRF_USE_EXPECTED_DESC;
use types_tuple::heaptuple::Datum;

use ::funcapi::srf_support::{InitMaterializedSRF, materialized_srf_putvalues};

use crate::register_srf;

/// `pg_get_publication_tables(VARIADIC pubnames text[])` (OID 6119).
const PG_GET_PUBLICATION_TABLES: Oid = 6119;

/// Register `pg_get_publication_tables` in the executor-frame SRF table.
pub(crate) fn register_pg_get_publication_tables() {
    register_srf(PG_GET_PUBLICATION_TABLES, pg_get_publication_tables);
}

/// `pg_get_publication_tables(PG_FUNCTION_ARGS)` (pg_publication.c:1116) over
/// the executor frame.
fn pg_get_publication_tables<'mcx>(
    fcinfo: &mut FunctionCallInfoBaseData<'mcx>,
) -> PgResult<Datum<'mcx>> {
    const NUM_PUBLICATION_TABLES_ELEM: usize = 4;

    let mcx: Mcx<'mcx> = fcinfo
        .fn_mcxt
        .expect("pg_get_publication_tables: fn_mcxt set by ExecMakeTableFunctionResult");

    // C: arr = PG_GETARG_ARRAYTYPE_P(0); deconstruct_array_builtin(arr, TEXTOID,
    // &elems, NULL, &nelems); ... GetPublicationByName(TextDatumGetCString(...)).
    // The `text[]` arrives header-ful on the by-reference side channel. Decode
    // its elements to owned UTF-8 strings so they outlive the immutable `fcinfo`
    // borrow (released before the mutable SRF set-up below).
    let pubnames: alloc::vec::Vec<alloc::string::String> = {
        let image: &[u8] = fcinfo
            .ref_arg(0)
            .and_then(|a| a.as_varlena())
            .expect("pg_get_publication_tables: text[] argument missing from the by-ref lane");
        let strs = arrayfuncs::construct::text_array_to_strings_bytes(mcx, image)?;
        strs.iter().map(|s| s.as_str().to_string()).collect()
    };

    // gather + build: one PublicationTableRow per (published table, publication)
    // — the portable body of pg_get_publication_tables. (Crosses the owner-crate
    // seam; the publication crate has no funcapi/executor-frame dependency.)
    let pubname_refs: alloc::vec::Vec<&str> = pubnames.iter().map(|s| s.as_str()).collect();
    let rows = pg_publication_seams::build_publication_table_rows::call(
        mcx,
        &pubname_refs,
    )?;

    // C: tupdesc = CreateTemplateTupleDesc(4): (pubid oid, relid oid,
    // attrs int2vector, qual pg_node_tree). Take the executor's already-resolved
    // descriptor via MAT_SRF_USE_EXPECTED_DESC (skipping get_call_result_type).
    InitMaterializedSRF(fcinfo, MAT_SRF_USE_EXPECTED_DESC)?;

    let rsinfo = fcinfo
        .resultinfo
        .as_mut()
        .expect("pg_get_publication_tables: InitMaterializedSRF establishes fcinfo->resultinfo");

    for row in rows.iter() {
        // values[0] = ObjectIdGetDatum(pub->oid); values[1] =
        // ObjectIdGetDatum(relid). attrs (int2vector) / qual (pg_node_tree) are
        // pass-by-reference varlena images; NULL when the publication has no
        // column list / no row filter for this table.
        let mut values: [Datum<'mcx>; NUM_PUBLICATION_TABLES_ELEM] =
            core::array::from_fn(|_| Datum::null());
        let mut nulls = [false; NUM_PUBLICATION_TABLES_ELEM];

        values[0] = Datum::from_oid(row.pubid);
        values[1] = Datum::from_oid(row.relid);

        match &row.attrs {
            Some(bytes) => values[2] = Datum::from_byref_bytes_in(mcx, bytes.as_slice())?,
            None => nulls[2] = true,
        }
        match &row.qual {
            Some(bytes) => values[3] = Datum::from_byref_bytes_in(mcx, bytes.as_slice())?,
            None => nulls[3] = true,
        }

        // tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc, values, nulls)
        materialized_srf_putvalues(rsinfo, &values, &nulls)?;
    }

    // C: SRF_RETURN_DONE — the whole set is in the materialize tuplestore.
    fcinfo.isnull = true;
    Ok(Datum::null())
}
