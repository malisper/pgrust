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
use types_tuple::backend_access_common_heaptuple::{Datum, FormedTuple};
use types_tuple::heaptuple::ItemPointerData;

use backend_access_common_heaptuple::heap_form_tuple;
use backend_catalog_catalog::GetNewOidWithIndex;

use crate::keystone::{
    CatalogCloseIndexes, CatalogOpenIndexes, CatalogTupleInsert, CatalogTuplesMultiInsertWithInfo,
};

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

/// Shared multi-insert tail: form each row's heap tuple
/// (`heap_form_tuple(RelationGetDescr(rel), values, nulls)`), then
/// `CatalogOpenIndexes(rel)` + `CatalogTuplesMultiInsertWithInfo(rel, slots,
/// n, indstate)` + `CatalogCloseIndexes(indstate)` (the C caller's slot-prep +
/// multi-insert + index-state lifecycle, folded into one seam body per the
/// repo's typed-row design). `rows` yields each tuple's per-column
/// `values`/`nulls` arrays; an empty `rows` is the C `ntuples <= 0` fast path
/// (no indexes opened).
fn form_and_multi_insert<'mcx, F>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    n: usize,
    natts: usize,
    mut row: F,
) -> PgResult<()>
where
    F: FnMut(usize) -> PgResult<(mcx::PgVec<'mcx, Datum<'mcx>>, mcx::PgVec<'mcx, bool>)>,
{
    // /* Nothing to do */ — no rows, so no index work either.
    if n == 0 {
        return Ok(());
    }

    let tupdesc = rel.rd_att_clone_in(mcx)?;
    let mut tuples: mcx::PgVec<'mcx, FormedTuple<'mcx>> = mcx::vec_with_capacity_in(mcx, n)?;
    for i in 0..n {
        // The C caller fills slot[i]->tts_values/tts_isnull and
        // ExecStoreVirtualTuple; heap_multi_insert forms the heap tuple. The
        // typed-row design forms it here (heap_form_tuple over rd_att).
        let (values, nulls) = row(i)?;
        debug_assert_eq!(values.len(), natts);
        debug_assert_eq!(nulls.len(), natts);
        let tup = heap_form_tuple(mcx, &tupdesc, &values, &nulls)?;
        // Within the capacity reserved by vec_with_capacity_in above.
        tuples.push(tup);
    }

    // indstate = CatalogOpenIndexes(rel);
    let mut indstate = CatalogOpenIndexes(mcx, rel)?;
    // CatalogTuplesMultiInsertWithInfo(rel, slot, ntuples, indstate);
    CatalogTuplesMultiInsertWithInfo(mcx, rel, tuples, &mut indstate)?;
    // CatalogCloseIndexes(indstate);
    CatalogCloseIndexes(indstate)
}

/* ======================================================================== *
 * pg_depend — recordMultipleDependencies batch (no OID column).
 * ======================================================================== */

fn multi_insert_pg_depend<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    forms: &[cat::catalog_dependency::FormData_pg_depend],
) -> PgResult<()> {
    form_and_multi_insert(
        mcx,
        rel,
        forms.len(),
        cat::catalog_dependency::Natts_pg_depend,
        |i| {
            let f = &forms[i];
            // slot->tts_values[Anum_pg_depend_classid - 1]    = ObjectIdGetDatum(classId);
            // slot->tts_values[Anum_pg_depend_objid - 1]      = ObjectIdGetDatum(objid);
            // slot->tts_values[Anum_pg_depend_objsubid - 1]   = Int32GetDatum(objsubid);
            // slot->tts_values[Anum_pg_depend_refclassid - 1] = ObjectIdGetDatum(refclassid);
            // slot->tts_values[Anum_pg_depend_refobjid - 1]   = ObjectIdGetDatum(refobjid);
            // slot->tts_values[Anum_pg_depend_refobjsubid - 1]= Int32GetDatum(refobjsubid);
            // slot->tts_values[Anum_pg_depend_deptype - 1]    = CharGetDatum((char) deptype);
            let mut values = mcx::vec_with_capacity_in(mcx, cat::catalog_dependency::Natts_pg_depend)?;
            values.push(Datum::from_oid(f.classid));
            values.push(Datum::from_oid(f.objid));
            values.push(Datum::from_i32(f.objsubid));
            values.push(Datum::from_oid(f.refclassid));
            values.push(Datum::from_oid(f.refobjid));
            values.push(Datum::from_i32(f.refobjsubid));
            values.push(Datum::from_char(f.deptype));
            // memset(tts_isnull, false, natts);
            let mut nulls = mcx::vec_with_capacity_in(mcx, cat::catalog_dependency::Natts_pg_depend)?;
            for _ in 0..cat::catalog_dependency::Natts_pg_depend {
                nulls.push(false);
            }
            Ok((values, nulls))
        },
    )
}

/* ======================================================================== *
 * pg_shdepend — shared-dependency batch (no OID column).
 * ======================================================================== */

fn multi_insert_pg_shdepend<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    forms: &[cat::catalog_shdepend::FormData_pg_shdepend],
) -> PgResult<()> {
    form_and_multi_insert(
        mcx,
        rel,
        forms.len(),
        cat::catalog_shdepend::Natts_pg_shdepend,
        |i| {
            let f = &forms[i];
            // dbid / classid / objid / objsubid / refclassid / refobjid / deptype
            let mut values = mcx::vec_with_capacity_in(mcx, cat::catalog_shdepend::Natts_pg_shdepend)?;
            values.push(Datum::from_oid(f.dbid));
            values.push(Datum::from_oid(f.classid));
            values.push(Datum::from_oid(f.objid));
            values.push(Datum::from_i32(f.objsubid));
            values.push(Datum::from_oid(f.refclassid));
            values.push(Datum::from_oid(f.refobjid));
            values.push(Datum::from_char(f.deptype));
            let mut nulls = mcx::vec_with_capacity_in(mcx, cat::catalog_shdepend::Natts_pg_shdepend)?;
            for _ in 0..cat::catalog_shdepend::Natts_pg_shdepend {
                nulls.push(false);
            }
            Ok((values, nulls))
        },
    )
}

/* ======================================================================== *
 * pg_enum — EnumValuesCreate batch (OID pre-assigned by the caller).
 * ======================================================================== */

fn multi_insert_pg_enum<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    rows: &[cat::pg_enum::PgEnumInsertRow],
) -> PgResult<()> {
    form_and_multi_insert(mcx, rel, rows.len(), cat::pg_enum::Natts_pg_enum, |i| {
        let r = &rows[i];
        // oid / enumtypid / enumsortorder (Float4) / enumlabel (NameGetDatum)
        let mut values = mcx::vec_with_capacity_in(mcx, cat::pg_enum::Natts_pg_enum)?;
        values.push(Datum::from_oid(r.oid));
        values.push(Datum::from_oid(r.enumtypid));
        values.push(Datum::from_f32(r.enumsortorder));
        values.push(name_datum(mcx, &r.enumlabel)?);
        let mut nulls = mcx::vec_with_capacity_in(mcx, cat::pg_enum::Natts_pg_enum)?;
        for _ in 0..cat::pg_enum::Natts_pg_enum {
            nulls.push(false);
        }
        Ok((values, nulls))
    })
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
    backend_catalog_indexing_seams::catalog_tuples_multi_insert_pg_depend::set(multi_insert_pg_depend);
    backend_catalog_indexing_seams::catalog_tuples_multi_insert_pg_shdepend::set(
        multi_insert_pg_shdepend,
    );
    backend_catalog_indexing_seams::catalog_tuples_multi_insert_pg_enum::set(multi_insert_pg_enum);
}
