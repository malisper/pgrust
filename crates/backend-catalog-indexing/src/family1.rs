//! F1 — the per-catalog *typed* insert/update/delete seam bodies.
//!
//! Each seam crosses a typed `*InsertRow` (or the addressed heap TID); the
//! body forms the catalog heap tuple from that row against the relation's
//! descriptor (`heap_form_tuple(RelationGetDescr(rel), values, nulls)`),
//! allocates the row OID when the catalog has an OID column
//! (`GetNewOidWithIndex`), and calls the F0 engine ([`crate::keystone`]'s
//! `CatalogTupleInsert` / `CatalogTupleUpdate` / `CatalogTupleDelete`). This is
//! the faithful split of the C `CreateXxx` callers, which build `values[]` /
//! `nulls[]` inline and call `CatalogTupleInsert(rel, tup)` directly.
//!
//! CONTRACT RECONCILE: the seams were scaffolded crossing `&RelationData<'_>`
//! with no `mcx`, but the F0 engine (and `heap_form_tuple` /
//! `GetNewOidWithIndex`) require both an owned `&Relation<'mcx>` (for the
//! relcache cell `CatalogOpenIndexes` aliases + `RelationGetIndexList`) and an
//! `Mcx<'mcx>` (there is no ambient context — mcx/lib.rs). The seams are
//! therefore re-signed to take `mcx: Mcx<'mcx>` + `rel: &Relation<'mcx>`; every
//! consumer already holds an open `Relation` and an `mcx`, so the change is
//! transparent at the call site (the old `&RelationData` was a `Deref`
//! coercion of the consumer's `&Relation`).

#![allow(non_snake_case)]

use mcx::Mcx;
use types_catalog as cat;
use types_core::Oid;
use types_error::PgResult;
use types_rel::Relation;
use types_tuple::backend_access_common_heaptuple::Datum;
use types_tuple::heaptuple::ItemPointerData;

use backend_access_common_heaptuple::heap_form_tuple;
use backend_catalog_catalog::GetNewOidWithIndex;

use crate::keystone::CatalogTupleInsert;

/// `NameGetDatum(&name)` for a 64-byte `NameData` image: a by-reference Datum
/// over the column's on-disk bytes (the `name` type is fixed-length 64, stored
/// inline). The `InsertRow` carriers already hold the NUL-padded image
/// (`namestrcpy` ran in the port), so this wraps the bytes unchanged.
fn name_datum<'mcx>(mcx: Mcx<'mcx>, image: &[u8; 64]) -> PgResult<Datum<'mcx>> {
    Ok(Datum::ByRef(mcx::slice_in(mcx, &image[..])?))
}

/// Shared tail: `heap_form_tuple(RelationGetDescr(rel), values, nulls)` +
/// `CatalogTupleInsert(rel, tup)` + `heap_freetuple(tup)` (the C
/// `CatalogTupleInsert` callers' last three lines). `values`/`nulls` are the
/// per-catalog column arrays the seam body built.
fn form_and_insert<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    values: &[Datum<'mcx>],
    nulls: &[bool],
) -> PgResult<()> {
    // tuple = heap_form_tuple(RelationGetDescr(rel), values, nulls);
    let tupdesc = rel.rd_att_clone_in(mcx)?;
    let mut tup = heap_form_tuple(mcx, &tupdesc, values, nulls)?;
    // CatalogTupleInsert(rel, tuple);
    CatalogTupleInsert(mcx, rel, &mut tup)?;
    // heap_freetuple(tuple); — `tup` drops here.
    Ok(())
}

/* ======================================================================== *
 * pg_inherits — StoreSingleInheritance (no OID column).
 * ======================================================================== */

fn insert_pg_inherits<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    row: &cat::pg_inherits::PgInheritsInsertRow,
) -> PgResult<()> {
    // values[Anum_pg_inherits_inhrelid - 1]         = ObjectIdGetDatum(relationId);
    // values[Anum_pg_inherits_inhparent - 1]        = ObjectIdGetDatum(parentOid);
    // values[Anum_pg_inherits_inhseqno - 1]         = Int32GetDatum(seqNumber);
    // values[Anum_pg_inherits_inhdetachpending - 1] = BoolGetDatum(false);
    let values = [
        Datum::from_oid(row.inhrelid),
        Datum::from_oid(row.inhparent),
        Datum::from_i32(row.inhseqno),
        Datum::from_bool(row.inhdetachpending),
    ];
    let nulls = [false; cat::pg_inherits::Natts_pg_inherits];
    form_and_insert(mcx, rel, &values, &nulls)
}

/* ======================================================================== *
 * pg_range — RangeCreate (no OID column).
 * ======================================================================== */

fn insert_pg_range<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    row: &cat::pg_range::PgRangeInsertRow,
) -> PgResult<()> {
    // values[Anum_pg_range_rngtypid - 1]     = ObjectIdGetDatum(typeOid);
    // values[Anum_pg_range_rngsubtype - 1]   = ObjectIdGetDatum(rangeSubType);
    // values[Anum_pg_range_rngmultitypid - 1]= ObjectIdGetDatum(multirangeOid);
    // values[Anum_pg_range_rngcollation - 1] = ObjectIdGetDatum(rangeCollation);
    // values[Anum_pg_range_rngsubopc - 1]    = ObjectIdGetDatum(rangeSubOpclass);
    // values[Anum_pg_range_rngcanonical - 1] = ObjectIdGetDatum(rangeCanonical);
    // values[Anum_pg_range_rngsubdiff - 1]   = ObjectIdGetDatum(rangeSubDiff);
    let values = [
        Datum::from_oid(row.rngtypid),
        Datum::from_oid(row.rngsubtype),
        Datum::from_oid(row.rngmultitypid),
        Datum::from_oid(row.rngcollation),
        Datum::from_oid(row.rngsubopc),
        Datum::from_oid(row.rngcanonical),
        Datum::from_oid(row.rngsubdiff),
    ];
    let nulls = [false; cat::pg_range::Natts_pg_range];
    form_and_insert(mcx, rel, &values, &nulls)
}

/* ======================================================================== *
 * pg_cast — CastCreate (OID column).
 * ======================================================================== */

fn insert_pg_cast<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    row: &cat::pg_cast::PgCastInsertRow,
) -> PgResult<Oid> {
    // castid = GetNewOidWithIndex(rel, CastOidIndexId, Anum_pg_cast_oid);
    let castid = GetNewOidWithIndex(rel, cat::pg_cast::CastOidIndexId, cat::pg_cast::Anum_pg_cast_oid)?;
    // values[Anum_pg_cast_oid - 1]        = ObjectIdGetDatum(castid);
    // values[Anum_pg_cast_castsource - 1] = ObjectIdGetDatum(sourcetypeid);
    // values[Anum_pg_cast_casttarget - 1] = ObjectIdGetDatum(targettypeid);
    // values[Anum_pg_cast_castfunc - 1]   = ObjectIdGetDatum(funcid);
    // values[Anum_pg_cast_castcontext - 1]= CharGetDatum(castcontext);
    // values[Anum_pg_cast_castmethod - 1] = CharGetDatum(castmethod);
    let values = [
        Datum::from_oid(castid),
        Datum::from_oid(row.castsource),
        Datum::from_oid(row.casttarget),
        Datum::from_oid(row.castfunc),
        Datum::from_char(row.castcontext),
        Datum::from_char(row.castmethod),
    ];
    let nulls = [false; cat::pg_cast::Natts_pg_cast];
    form_and_insert(mcx, rel, &values, &nulls)?;
    Ok(castid)
}

/* ======================================================================== *
 * pg_conversion — ConversionCreate (OID column + conname NameData).
 * ======================================================================== */

fn insert_pg_conversion<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    row: &cat::pg_conversion::PgConversionInsertRow,
) -> PgResult<Oid> {
    // oid = GetNewOidWithIndex(rel, ConversionOidIndexId, Anum_pg_conversion_oid);
    let oid = GetNewOidWithIndex(
        rel,
        cat::pg_conversion::ConversionOidIndexId,
        cat::pg_conversion::Anum_pg_conversion_oid,
    )?;
    // namestrcpy(&cname, conname);  values[..conname] = NameGetDatum(&cname);
    let values = [
        Datum::from_oid(oid),
        name_datum(mcx, &row.conname)?,
        Datum::from_oid(row.connamespace),
        Datum::from_oid(row.conowner),
        Datum::from_i32(row.conforencoding),
        Datum::from_i32(row.contoencoding),
        Datum::from_oid(row.conproc),
        Datum::from_bool(row.condefault),
    ];
    let nulls = [false; cat::pg_conversion::Natts_pg_conversion];
    form_and_insert(mcx, rel, &values, &nulls)?;
    Ok(oid)
}

/* ======================================================================== *
 * pg_enum — AddEnumLabel / EnumValuesCreate single-row inserts + update.
 *
 * The caller pre-allocates `row.oid` (the even/odd OID sort-order selection is
 * port logic); `get_new_oid_with_index_pg_enum` exposes the bare
 * GetNewOidWithIndex probe the port inspects before forming the tuple.
 * ======================================================================== */

fn enum_values<'mcx>(mcx: Mcx<'mcx>, row: &cat::pg_enum::PgEnumInsertRow) -> PgResult<[Datum<'mcx>; 4]> {
    Ok([
        Datum::from_oid(row.oid),
        Datum::from_oid(row.enumtypid),
        Datum::from_f32(row.enumsortorder),
        name_datum(mcx, &row.enumlabel)?,
    ])
}

fn insert_pg_enum<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    row: &cat::pg_enum::PgEnumInsertRow,
) -> PgResult<()> {
    let values = enum_values(mcx, row)?;
    let nulls = [false; cat::pg_enum::Natts_pg_enum];
    form_and_insert(mcx, rel, &values, &nulls)
}

fn get_new_oid_pg_enum<'mcx>(rel: &Relation<'mcx>) -> PgResult<Oid> {
    GetNewOidWithIndex(rel, cat::pg_enum::EnumOidIndexId, cat::pg_enum::Anum_pg_enum_oid)
}

fn update_pg_enum<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    tid: ItemPointerData,
    row: &cat::pg_enum::PgEnumInsertRow,
) -> PgResult<()> {
    // The in-place mutators (RenameEnumLabel / RenumberEnumType) re-form the
    // whole row from the supplied values and CatalogTupleUpdate at tid. The
    // caller already read the existing row and rebuilt `row` with the one
    // column changed, so a full re-form is behaviour-identical to C's
    // heap_modify_tuple over a single column.
    let values = enum_values(mcx, row)?;
    let nulls = [false; cat::pg_enum::Natts_pg_enum];
    let tupdesc = rel.rd_att_clone_in(mcx)?;
    let mut tup = heap_form_tuple(mcx, &tupdesc, &values, &nulls)?;
    crate::keystone::CatalogTupleUpdate(mcx, rel, tid, &mut tup)
}

/// Install the F1 per-catalog typed seams whose substrate is fully present
/// (pure `heap_form_tuple` + engine, plus `GetNewOidWithIndex` for the
/// OID-column catalogs). Wired from [`crate::init_seams`].
pub fn install() {
    backend_catalog_indexing_seams::catalog_tuple_insert_pg_inherits::set(insert_pg_inherits);
    backend_catalog_indexing_seams::catalog_tuple_insert_pg_range::set(insert_pg_range);
    backend_catalog_indexing_seams::catalog_tuple_insert_pg_cast::set(insert_pg_cast);
    backend_catalog_indexing_seams::catalog_tuple_insert_pg_conversion::set(insert_pg_conversion);
    backend_catalog_indexing_seams::catalog_tuple_insert_pg_enum::set(insert_pg_enum);
    backend_catalog_indexing_seams::get_new_oid_with_index_pg_enum::set(get_new_oid_pg_enum);
    backend_catalog_indexing_seams::catalog_tuple_update_pg_enum::set(update_pg_enum);
}
