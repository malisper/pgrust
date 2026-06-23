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

use ::mcx::Mcx;
use types_catalog as cat;
use types_core::{InvalidOid, Oid};
use ::types_error::PgResult;
use ::rel::Relation;
use ::types_tuple::heaptuple::{Datum, FormedTuple};
use ::types_tuple::heaptuple::ItemPointerData;

use heaptuple::{heap_form_tuple, heap_modify_tuple};
use ::catalog_catalog::GetNewOidWithIndex;

use crate::keystone::{
    CatalogCloseIndexes, CatalogOpenIndexes, CatalogTupleInsert, CatalogTupleUpdate,
    CatalogTuplesMultiInsertWithInfo,
};

/// `NameGetDatum(&name)` for a 64-byte `NameData` image: a by-reference Datum
/// over the column's on-disk bytes (the `name` type is fixed-length 64, stored
/// inline). The `InsertRow` carriers already hold the NUL-padded image
/// (`namestrcpy` ran in the port), so this wraps the bytes unchanged.
fn name_datum<'mcx>(mcx: Mcx<'mcx>, image: &[u8; 64]) -> PgResult<Datum<'mcx>> {
    Ok(Datum::ByRef(::mcx::slice_in(mcx, &image[..])?))
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
    F: FnMut(usize) -> PgResult<(::mcx::PgVec<'mcx, Datum<'mcx>>, ::mcx::PgVec<'mcx, bool>)>,
{
    // /* Nothing to do */ — no rows, so no index work either.
    if n == 0 {
        return Ok(());
    }

    let tupdesc = rel.rd_att_clone_in(mcx)?;
    let mut tuples: ::mcx::PgVec<'mcx, FormedTuple<'mcx>> = ::mcx::vec_with_capacity_in(mcx, n)?;
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
            let mut values = ::mcx::vec_with_capacity_in(mcx, cat::catalog_dependency::Natts_pg_depend)?;
            values.push(Datum::from_oid(f.classid));
            values.push(Datum::from_oid(f.objid));
            values.push(Datum::from_i32(f.objsubid));
            values.push(Datum::from_oid(f.refclassid));
            values.push(Datum::from_oid(f.refobjid));
            values.push(Datum::from_i32(f.refobjsubid));
            values.push(Datum::from_char(f.deptype));
            // memset(tts_isnull, false, natts);
            let mut nulls = ::mcx::vec_with_capacity_in(mcx, cat::catalog_dependency::Natts_pg_depend)?;
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
            let mut values = ::mcx::vec_with_capacity_in(mcx, cat::catalog_shdepend::Natts_pg_shdepend)?;
            values.push(Datum::from_oid(f.dbid));
            values.push(Datum::from_oid(f.classid));
            values.push(Datum::from_oid(f.objid));
            values.push(Datum::from_i32(f.objsubid));
            values.push(Datum::from_oid(f.refclassid));
            values.push(Datum::from_oid(f.refobjid));
            values.push(Datum::from_char(f.deptype));
            let mut nulls = ::mcx::vec_with_capacity_in(mcx, cat::catalog_shdepend::Natts_pg_shdepend)?;
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
        let mut values = ::mcx::vec_with_capacity_in(mcx, cat::pg_enum::Natts_pg_enum)?;
        values.push(Datum::from_oid(r.oid));
        values.push(Datum::from_oid(r.enumtypid));
        values.push(Datum::from_f32(r.enumsortorder));
        values.push(name_datum(mcx, &r.enumlabel)?);
        let mut nulls = ::mcx::vec_with_capacity_in(mcx, cat::pg_enum::Natts_pg_enum)?;
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

/// `MarkInheritDetached`'s in-place flip of `inhdetachpending` (the C
/// `heap_copytuple` + `GETSTRUCT(newtup)->inhdetachpending = true` +
/// `CatalogTupleUpdate(rel, &inheritsTuple->t_self, newtup)`). Re-forms the
/// full pg_inherits row from the carrier (all four columns are fixed-width
/// NOT NULL, so re-forming the whole row from the scanned values with the one
/// changed column is bit-identical to the C's in-place struct set), then
/// updates at the scanned tuple's `tid`.
fn update_pg_inherits<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    tid: ItemPointerData,
    row: &cat::pg_inherits::PgInheritsUpdateRow,
) -> PgResult<()> {
    let values = [
        Datum::from_oid(row.inhrelid),
        Datum::from_oid(row.inhparent),
        Datum::from_i32(row.inhseqno),
        Datum::from_bool(row.inhdetachpending),
    ];
    let nulls = [false; cat::pg_inherits::Natts_pg_inherits];
    // newtup = heap_form_tuple(RelationGetDescr(rel), values, nulls);
    let tupdesc = rel.rd_att_clone_in(mcx)?;
    let mut tup = heap_form_tuple(mcx, &tupdesc, &values, &nulls)?;
    // CatalogTupleUpdate(rel, &inheritsTuple->t_self, newtup);
    CatalogTupleUpdate(mcx, rel, tid, &mut tup)
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
 * pg_transform — CreateTransform (OID column; insert new or update REPLACE).
 * ======================================================================== */

fn insert_pg_transform<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    row: &cat::pg_transform::PgTransformInsertRow,
    replace_oid: Oid,
    replace_tid: ItemPointerData,
) -> PgResult<Oid> {
    use cat::pg_transform as pt;

    // values[Anum_pg_transform_trftype - 1]    = ObjectIdGetDatum(typeid);
    // values[Anum_pg_transform_trflang - 1]    = ObjectIdGetDatum(langid);
    // values[Anum_pg_transform_trffromsql - 1] = ObjectIdGetDatum(fromsqlfuncid);
    // values[Anum_pg_transform_trftosql - 1]   = ObjectIdGetDatum(tosqlfuncid);
    if replace_oid != InvalidOid {
        // REPLACE: heap_modify_tuple of trffromsql/trftosql + CatalogTupleUpdate.
        // The C re-forms only those two columns; re-forming the whole row from
        // the same (trftype, trflang, trffromsql, trftosql) values is
        // behaviour-identical, with the existing oid kept.
        let values = [
            Datum::from_oid(replace_oid),
            Datum::from_oid(row.trftype),
            Datum::from_oid(row.trflang),
            Datum::from_oid(row.trffromsql),
            Datum::from_oid(row.trftosql),
        ];
        let nulls = [false; pt::Natts_pg_transform];
        let tupdesc = rel.rd_att_clone_in(mcx)?;
        let mut tup = heap_form_tuple(mcx, &tupdesc, &values, &nulls)?;
        crate::keystone::CatalogTupleUpdate(mcx, rel, replace_tid, &mut tup)?;
        return Ok(replace_oid);
    }

    // transformid = GetNewOidWithIndex(rel, TransformOidIndexId,
    //                                  Anum_pg_transform_oid);
    let transformid =
        GetNewOidWithIndex(rel, pt::TransformOidIndexId, pt::Anum_pg_transform_oid)?;
    let values = [
        Datum::from_oid(transformid),
        Datum::from_oid(row.trftype),
        Datum::from_oid(row.trflang),
        Datum::from_oid(row.trffromsql),
        Datum::from_oid(row.trftosql),
    ];
    let nulls = [false; pt::Natts_pg_transform];
    form_and_insert(mcx, rel, &values, &nulls)?;
    Ok(transformid)
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

/* ======================================================================== *
 * pg_statistic_ext — CreateStatistics (OID column + int2vector / char[] /
 * text variable-length columns).
 * ======================================================================== */

/// `buildint2vector(int2s, n)` (utils/adt/int.c): the on-disk `int2vector`
/// image — a varlena whose header (`vl_len_` via `SET_VARSIZE`, then `ndim=1`,
/// `dataoffset=0`, `elemtype=INT2OID`, `dim1=n`, `lbound1=0`) is followed by the
/// `n` `int16` values. `Int2VectorSize(n) = offsetof(int2vector, values) + n *
/// sizeof(int16) = 24 + 2n`. Returned as the verbatim `Datum::ByRef` bytes
/// (header included), exactly what `heap_form_tuple` reads via `VARSIZE_ANY`.
fn buildint2vector<'mcx>(mcx: Mcx<'mcx>, int2s: &[i16]) -> PgResult<Datum<'mcx>> {
    const INT2OID: Oid = 21;
    // offsetof(int2vector, values): vl_len_(4) + ndim(4) + dataoffset(4) +
    // elemtype(4) + dim1(4) + lbound1(4) = 24.
    const HEADER: usize = 24;
    let n = int2s.len();
    let total = HEADER + n * core::mem::size_of::<i16>();
    let mut buf: ::mcx::PgVec<'mcx, u8> = ::mcx::vec_with_capacity_in(mcx, total)?;
    buf.resize(total, 0u8);
    // SET_VARSIZE(result, Int2VectorSize(n)): va_header = (uint32) total << 2.
    let vl_len: u32 = (total as u32) << 2;
    buf[0..4].copy_from_slice(&vl_len.to_ne_bytes());
    // ndim = 1; dataoffset = 0; elemtype = INT2OID; dim1 = n; lbound1 = 0.
    buf[4..8].copy_from_slice(&1i32.to_ne_bytes());
    buf[8..12].copy_from_slice(&0i32.to_ne_bytes());
    buf[12..16].copy_from_slice(&INT2OID.to_ne_bytes());
    buf[16..20].copy_from_slice(&(n as i32).to_ne_bytes());
    buf[20..24].copy_from_slice(&0i32.to_ne_bytes());
    // memcpy(result->values, int2s, n * sizeof(int16));
    for (i, v) in int2s.iter().enumerate() {
        let off = HEADER + i * 2;
        buf[off..off + 2].copy_from_slice(&v.to_ne_bytes());
    }
    Ok(Datum::ByRef(buf))
}

/// `CStringGetTextDatum(s)` (postgres.h → `cstring_to_text`): a `text` varlena
/// with the standard 4-byte header (`SET_VARSIZE(VARHDRSZ + len)`) followed by
/// the payload bytes. Returned as the verbatim `Datum::ByRef` bytes (header
/// included) — `heap_form_tuple` reads the stored length via `VARSIZE_ANY`.
fn cstring_to_text_datum<'mcx>(mcx: Mcx<'mcx>, s: &str) -> PgResult<Datum<'mcx>> {
    const VARHDRSZ: usize = 4;
    let payload = s.as_bytes();
    let total = VARHDRSZ + payload.len();
    let mut buf: ::mcx::PgVec<'mcx, u8> = ::mcx::vec_with_capacity_in(mcx, total)?;
    buf.resize(total, 0u8);
    // SET_VARSIZE(result, total): va_header = (uint32) total << 2 (4B format).
    let vl_len: u32 = (total as u32) << 2;
    buf[0..4].copy_from_slice(&vl_len.to_ne_bytes());
    buf[VARHDRSZ..].copy_from_slice(payload);
    Ok(Datum::ByRef(buf))
}

fn insert_pg_statistic_ext<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    row: &cat::pg_statistic_ext::PgStatisticExtInsertRow,
) -> PgResult<Oid> {
    use cat::pg_statistic_ext as se;

    // statoid = GetNewOidWithIndex(statrel, StatisticExtOidIndexId,
    //                              Anum_pg_statistic_ext_oid);
    let statoid = GetNewOidWithIndex(rel, se::StatisticExtOidIndexId, se::Anum_pg_statistic_ext_oid)?;

    // stxkeys = buildint2vector(attnums, nattnums);
    let stxkeys = buildint2vector(mcx, &row.stxkeys)?;

    // stxkind = construct_array_builtin(types, ntypes, CHAROID);
    let mut kind_elems: ::mcx::PgVec<'mcx, datum::datum::Datum> =
        ::mcx::vec_with_capacity_in(mcx, row.stxkind.len())?;
    for &c in &row.stxkind {
        kind_elems.push(datum::datum::Datum::from_char(c));
    }
    // construct_array(.., CHAROID): elmlen=1, elmbyval=true, elmalign='c'.
    let stxkind_bytes = arrayfuncs::construct::construct_array(
        mcx,
        &kind_elems,
        arrayfuncs::foundation::CHAROID,
        1,
        true,
        b'c',
    )?;

    // memset(values, 0, ..); memset(nulls, false, ..);
    // values[oid] = statoid; values[stxrelid] = relid; values[stxname] = name;
    // values[stxnamespace] = nsp; values[stxowner] = owner;
    // values[stxkeys] = stxkeys; nulls[stxstattarget] = true;
    // values[stxkind] = stxkind; values[stxexprs] = exprsDatum / nulls if 0.
    let exprs_value: Datum<'mcx> = match &row.stxexprs {
        Some(s) => cstring_to_text_datum(mcx, s)?,
        // The column value is NULL; the slot holds a placeholder (nulls[] set).
        None => Datum::ByVal(0),
    };

    let values = [
        Datum::from_oid(statoid),
        Datum::from_oid(row.stxrelid),
        name_datum(mcx, &row.stxname)?,
        Datum::from_oid(row.stxnamespace),
        Datum::from_oid(row.stxowner),
        stxkeys,
        // stxstattarget — left NULL on a fresh CREATE (placeholder by-value).
        Datum::ByVal(0),
        Datum::ByRef(stxkind_bytes),
        exprs_value,
    ];
    let mut nulls = [false; se::Natts_pg_statistic_ext];
    nulls[(se::Anum_pg_statistic_ext_stxstattarget - 1) as usize] = true;
    if row.stxexprs.is_none() {
        nulls[(se::Anum_pg_statistic_ext_stxexprs - 1) as usize] = true;
    }

    form_and_insert(mcx, rel, &values, &nulls)?;
    Ok(statoid)
}

/* ======================================================================== *
 * pg_language — CreateProceduralLanguage (OID column + lanname NameData +
 * lanacl varlen, always NULL on the values we form).
 * ======================================================================== */

/// The `values[]` / `nulls[]` arrays C builds for the `pg_language` row, with
/// `oid` stamped to `langoid`. `lanacl` is column 9 and always NULL.
fn language_values<'mcx>(
    mcx: Mcx<'mcx>,
    langoid: Oid,
    row: &cat::pg_language::PgLanguageInsertRow,
) -> PgResult<([Datum<'mcx>; cat::pg_language::Natts_pg_language], [bool; cat::pg_language::Natts_pg_language])> {
    // values[Anum_pg_language_oid - 1]           = ObjectIdGetDatum(langoid);
    // namestrcpy(&langname, languageName);
    // values[Anum_pg_language_lanname - 1]        = NameGetDatum(&langname);
    // values[Anum_pg_language_lanowner - 1]       = ObjectIdGetDatum(languageOwner);
    // values[Anum_pg_language_lanispl - 1]        = BoolGetDatum(true);
    // values[Anum_pg_language_lanpltrusted - 1]   = BoolGetDatum(stmt->pltrusted);
    // values[Anum_pg_language_lanplcallfoid - 1]  = ObjectIdGetDatum(handlerOid);
    // values[Anum_pg_language_laninline - 1]      = ObjectIdGetDatum(inlineOid);
    // values[Anum_pg_language_lanvalidator - 1]   = ObjectIdGetDatum(valOid);
    // (lanacl left as the zeroed value; nulls[Anum_pg_language_lanacl - 1] = true.)
    let values = [
        Datum::from_oid(langoid),
        name_datum(mcx, &row.lanname)?,
        Datum::from_oid(row.lanowner),
        Datum::from_bool(row.lanispl),
        Datum::from_bool(row.lanpltrusted),
        Datum::from_oid(row.lanplcallfoid),
        Datum::from_oid(row.laninline),
        Datum::from_oid(row.lanvalidator),
        Datum::from_oid(InvalidOid), // lanacl placeholder; nulls[] marks it NULL
    ];
    // memset(nulls, false, ...); nulls[Anum_pg_language_lanacl - 1] = true;
    let mut nulls = [false; cat::pg_language::Natts_pg_language];
    nulls[cat::pg_language::Anum_pg_language_lanacl as usize - 1] = true;
    Ok((values, nulls))
}

fn insert_pg_language<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    row: &cat::pg_language::PgLanguageInsertRow,
) -> PgResult<Oid> {
    // langoid = GetNewOidWithIndex(rel, LanguageOidIndexId, Anum_pg_language_oid);
    let langoid = GetNewOidWithIndex(
        rel,
        cat::pg_language::LanguageOidIndexId,
        cat::pg_language::Anum_pg_language_oid,
    )?;
    let (values, nulls) = language_values(mcx, langoid, row)?;
    // tup = heap_form_tuple(RelationGetDescr(rel), values, nulls);
    // CatalogTupleInsert(rel, tup);
    form_and_insert(mcx, rel, &values, &nulls)?;
    Ok(langoid)
}

fn update_pg_language<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    oldtup: &FormedTuple<'mcx>,
    row: &cat::pg_language::PgLanguageInsertRow,
) -> PgResult<()> {
    // replaces[] starts all-true; oid / lanowner / lanacl are pinned to the old
    // tuple (proclang.c:144-146), so heap_modify_tuple takes them from `oldtup`.
    let mut replaces = [true; cat::pg_language::Natts_pg_language];
    replaces[cat::pg_language::Anum_pg_language_oid as usize - 1] = false;
    replaces[cat::pg_language::Anum_pg_language_lanowner as usize - 1] = false;
    replaces[cat::pg_language::Anum_pg_language_lanacl as usize - 1] = false;

    // The not-replaced columns are read from `oldtup`, so any OID here is unused;
    // pass InvalidOid for the oid slot (replaces[oid] = false ignores it).
    let (values, nulls) = language_values(mcx, InvalidOid, row)?;

    // tup = heap_modify_tuple(oldtup, RelationGetDescr(rel), values, nulls, replaces);
    let tupdesc = rel.rd_att_clone_in(mcx)?;
    let mut tup = heap_modify_tuple(mcx, oldtup, &tupdesc, &values, &nulls, &replaces)?;
    // CatalogTupleUpdate(rel, &tup->t_self, tup);
    CatalogTupleUpdate(mcx, rel, tup.tuple.t_self, &mut tup)
}

/* ======================================================================== *
 * pg_rewrite — InsertRule / EnableDisableRule / RenameRewriteRule
 * (rewriteDefine.c). OID column + rulename NameData + two pg_node_tree text
 * columns (ev_qual, ev_action).
 * ======================================================================== */

/// `CStringGetTextDatum(s)` — a `text` varlena Datum: the 4-byte (full) varlena
/// header followed by the string bytes (no trailing NUL). Mirrors C's
/// `cstring_to_text` storage image carried as on-disk bytes.
fn cstring_get_text_datum<'mcx>(mcx: Mcx<'mcx>, s: &str) -> PgResult<Datum<'mcx>> {
    let payload = s.as_bytes();
    let total = 4 + payload.len();
    // SET_VARSIZE(result, total): the length word is (total << 2) in native
    // byte order (4-byte varlena header, VARTAG full).
    let word = (total as u32) << 2;
    let mut buf = Vec::with_capacity(total);
    buf.extend_from_slice(&word.to_ne_bytes());
    buf.extend_from_slice(payload);
    Ok(Datum::ByRef(::mcx::slice_in(mcx, &buf)?))
}

/// `namestrcpy(&name, src)` — copy `src` into a zero-filled 64-byte `NameData`,
/// truncated to `NAMEDATALEN`, force-terminated at the last slot.
fn namestrcpy_image(src: &str) -> [u8; 64] {
    let mut name = [0u8; 64];
    for (i, &byte) in src.as_bytes().iter().take(64).enumerate() {
        name[i] = byte;
    }
    name[64 - 1] = 0;
    name
}

/// The `((Form_pg_rewrite) GETSTRUCT(tup))->oid` of a formed/held pg_rewrite
/// tuple, read out of the deformed fixed columns.
fn rewrite_tuple_oid<'mcx>(mcx: Mcx<'mcx>, rel: &Relation<'mcx>, tup: &FormedTuple<'mcx>) -> PgResult<Oid> {
    let tupdesc = rel.rd_att_clone_in(mcx)?;
    let cols = ::heaptuple::heap_deform_tuple(mcx, &tup.tuple, &tupdesc, &tup.data)?;
    let idx = cat::pg_rewrite::Anum_pg_rewrite_oid as usize - 1;
    let (value, isnull) = &cols[idx];
    if *isnull {
        return Err(::types_error::PgError::error("pg_rewrite.oid is NULL"));
    }
    Ok(value.as_oid())
}

/// Build the full `values[]`/`nulls[]` for a pg_rewrite row.
fn rewrite_values<'mcx>(
    mcx: Mcx<'mcx>,
    oid: Oid,
    rulename: &str,
    ev_class: Oid,
    ev_type: u8,
    ev_enabled: u8,
    is_instead: bool,
    ev_qual: &str,
    ev_action: &str,
) -> PgResult<([Datum<'mcx>; cat::pg_rewrite::Natts_pg_rewrite], [bool; cat::pg_rewrite::Natts_pg_rewrite])> {
    let values = [
        Datum::from_oid(oid),
        name_datum(mcx, &namestrcpy_image(rulename))?,
        Datum::from_oid(ev_class),
        Datum::from_char(ev_type as i8),
        Datum::from_char(ev_enabled as i8),
        Datum::from_bool(is_instead),
        cstring_get_text_datum(mcx, ev_qual)?,
        cstring_get_text_datum(mcx, ev_action)?,
    ];
    let nulls = [false; cat::pg_rewrite::Natts_pg_rewrite];
    Ok((values, nulls))
}

fn insert_pg_rewrite<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    rulename: &str,
    ev_class: Oid,
    ev_type: u8,
    is_instead: bool,
    ev_qual: &str,
    ev_action: &str,
) -> PgResult<Oid> {
    // rewriteObjectId = GetNewOidWithIndex(rel, RewriteOidIndexId, Anum_pg_rewrite_oid);
    let rewrite_oid = GetNewOidWithIndex(
        rel,
        cat::pg_rewrite::RewriteOidIndexId,
        cat::pg_rewrite::Anum_pg_rewrite_oid,
    )?;
    // values[Anum_pg_rewrite_ev_enabled - 1] = CharGetDatum(RULE_FIRES_ON_ORIGIN);
    let (values, nulls) = rewrite_values(
        mcx,
        rewrite_oid,
        rulename,
        ev_class,
        ev_type,
        cat::pg_rewrite::RULE_FIRES_ON_ORIGIN,
        is_instead,
        ev_qual,
        ev_action,
    )?;
    // tup = heap_form_tuple(pg_rewrite_desc->rd_att, values, nulls);
    // CatalogTupleInsert(pg_rewrite_desc, tup);
    form_and_insert(mcx, rel, &values, &nulls)?;
    Ok(rewrite_oid)
}

fn update_pg_rewrite<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    oldtup: &FormedTuple<'mcx>,
    ev_type: u8,
    is_instead: bool,
    ev_qual: &str,
    ev_action: &str,
) -> PgResult<Oid> {
    // replaces[] starts all-false; only ev_type / is_instead / ev_qual /
    // ev_action are replaced (rewriteDefine.c:110-113). All other columns
    // (oid / rulename / ev_class / ev_enabled) are taken from `oldtup`.
    let mut replaces = [false; cat::pg_rewrite::Natts_pg_rewrite];
    replaces[cat::pg_rewrite::Anum_pg_rewrite_ev_type as usize - 1] = true;
    replaces[cat::pg_rewrite::Anum_pg_rewrite_is_instead as usize - 1] = true;
    replaces[cat::pg_rewrite::Anum_pg_rewrite_ev_qual as usize - 1] = true;
    replaces[cat::pg_rewrite::Anum_pg_rewrite_ev_action as usize - 1] = true;

    // The not-replaced columns are read from `oldtup`; pass placeholders for them.
    let (values, nulls) = rewrite_values(
        mcx,
        InvalidOid,
        "",
        InvalidOid,
        ev_type,
        cat::pg_rewrite::RULE_FIRES_ON_ORIGIN,
        is_instead,
        ev_qual,
        ev_action,
    )?;

    let tupdesc = rel.rd_att_clone_in(mcx)?;
    let mut tup = heap_modify_tuple(mcx, oldtup, &tupdesc, &values, &nulls, &replaces)?;
    // CatalogTupleUpdate(pg_rewrite_desc, &tup->t_self, tup);
    CatalogTupleUpdate(mcx, rel, tup.tuple.t_self, &mut tup)?;
    // rewriteObjectId = ((Form_pg_rewrite) GETSTRUCT(tup))->oid;
    rewrite_tuple_oid(mcx, rel, &tup)
}

/// Re-form `oldtup` with a single fixed column replaced and update at
/// `oldtup->t_self` (the EnableDisableRule / RenameRewriteRule in-place mutate
/// pattern, behaviour-identical to mutating the GETSTRUCT field then
/// CatalogTupleUpdate of the same tuple).
fn modify_one_pg_rewrite_column<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    oldtup: &FormedTuple<'mcx>,
    attnum: usize,
    value: Datum<'mcx>,
) -> PgResult<()> {
    let mut replaces = [false; cat::pg_rewrite::Natts_pg_rewrite];
    replaces[attnum - 1] = true;
    let mut values = [
        Datum::from_oid(InvalidOid),
        Datum::from_oid(InvalidOid),
        Datum::from_oid(InvalidOid),
        Datum::from_char(0),
        Datum::from_char(0),
        Datum::from_bool(false),
        Datum::from_oid(InvalidOid),
        Datum::from_oid(InvalidOid),
    ];
    values[attnum - 1] = value;
    let nulls = [false; cat::pg_rewrite::Natts_pg_rewrite];
    let tupdesc = rel.rd_att_clone_in(mcx)?;
    let mut tup = heap_modify_tuple(mcx, oldtup, &tupdesc, &values, &nulls, &replaces)?;
    CatalogTupleUpdate(mcx, rel, oldtup.tuple.t_self, &mut tup)
}

fn update_pg_rewrite_enabled<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    oldtup: &FormedTuple<'mcx>,
    ev_enabled: u8,
) -> PgResult<()> {
    modify_one_pg_rewrite_column(
        mcx,
        rel,
        oldtup,
        cat::pg_rewrite::Anum_pg_rewrite_ev_enabled as usize,
        Datum::from_char(ev_enabled as i8),
    )
}

fn update_pg_rewrite_name<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    oldtup: &FormedTuple<'mcx>,
    new_name: &str,
) -> PgResult<()> {
    let image = namestrcpy_image(new_name);
    let value = name_datum(mcx, &image)?;
    modify_one_pg_rewrite_column(
        mcx,
        rel,
        oldtup,
        cat::pg_rewrite::Anum_pg_rewrite_rulename as usize,
        value,
    )
}

/* ======================================================================== *
 * pg_proc — ProcedureCreate (OID column + proname NameData + oidvector
 * proargtypes + the CATALOG_VARLEN columns: proallargtypes / proargmodes /
 * proargnames / proargdefaults / protrftypes / probin / prosqlbody /
 * proconfig / proacl). pg_proc.c:320-380, 580-609.
 * ======================================================================== */

/// `OIDOID` (`pg_type_d.h`).
const OIDOID: Oid = 26;
/// `CHAROID` (`pg_type_d.h`).
const CHAROID: Oid = 18;
/// `TEXTOID` (`pg_type_d.h`).
const TEXTOID: Oid = 25;

/// `PointerGetDatum(buildoidvector(oids, n))` (oid.c): the on-disk `oidvector`
/// image — a varlena-ish fixed-layout struct (`int2vector`-shaped) whose header
/// (`vl_len_` via `SET_VARSIZE`, then `ndim=1`, `dataoffset=0`,
/// `elemtype=OIDOID`, `dim1=n`, `lbound1=0`) is followed by the `n` `Oid`
/// values. `OidVectorSize(n) = offsetof(oidvector, values) + n * sizeof(Oid) =
/// 24 + 4n`. Returned as the verbatim `Datum::ByRef` bytes (header included).
fn buildoidvector<'mcx>(mcx: Mcx<'mcx>, oids: &[Oid]) -> PgResult<Datum<'mcx>> {
    // offsetof(oidvector, values): vl_len_(4) + ndim(4) + dataoffset(4) +
    // elemtype(4) + dim1(4) + lbound1(4) = 24.
    const HEADER: usize = 24;
    let n = oids.len();
    let total = HEADER + n * core::mem::size_of::<Oid>();
    let mut buf: ::mcx::PgVec<'mcx, u8> = ::mcx::vec_with_capacity_in(mcx, total)?;
    buf.resize(total, 0u8);
    // SET_VARSIZE(result, OidVectorSize(n)): va_header = (uint32) total << 2.
    let vl_len: u32 = (total as u32) << 2;
    buf[0..4].copy_from_slice(&vl_len.to_ne_bytes());
    // ndim = 1; dataoffset = 0; elemtype = OIDOID; dim1 = n; lbound1 = 0.
    buf[4..8].copy_from_slice(&1i32.to_ne_bytes());
    buf[8..12].copy_from_slice(&0i32.to_ne_bytes());
    buf[12..16].copy_from_slice(&OIDOID.to_ne_bytes());
    buf[16..20].copy_from_slice(&(n as i32).to_ne_bytes());
    buf[20..24].copy_from_slice(&0i32.to_ne_bytes());
    // memcpy(result->values, oids, n * sizeof(Oid));
    for (i, v) in oids.iter().enumerate() {
        let off = HEADER + i * 4;
        buf[off..off + 4].copy_from_slice(&v.to_ne_bytes());
    }
    Ok(Datum::ByRef(buf))
}

/// `construct_array_builtin(oids, n, OIDOID)` — a 1-D `Oid[]` array varlena.
/// OID is pass-by-value, so the element-`Datum` path of `construct_array`
/// resolves without the by-ref `datum_as_byte_window` detoast leg.
fn build_oid_array<'mcx>(mcx: Mcx<'mcx>, oids: &[Oid]) -> PgResult<Datum<'mcx>> {
    let mut elems: ::mcx::PgVec<'mcx, datum::datum::Datum> =
        ::mcx::vec_with_capacity_in(mcx, oids.len())?;
    for &o in oids {
        elems.push(datum::datum::Datum::from_oid(o));
    }
    // construct_array(.., OIDOID): elmlen=4, elmbyval=true, elmalign='i'.
    let buf = arrayfuncs::construct::construct_array(
        mcx, &elems, OIDOID, 4, true, b'i',
    )?;
    Ok(Datum::ByRef(buf))
}

/// `construct_array(chars, n, CHAROID, 1, true, 'c')` — a 1-D `char[]` array
/// varlena. `char` is pass-by-value.
fn build_char_array<'mcx>(mcx: Mcx<'mcx>, chars: &[i8]) -> PgResult<Datum<'mcx>> {
    let mut elems: ::mcx::PgVec<'mcx, datum::datum::Datum> =
        ::mcx::vec_with_capacity_in(mcx, chars.len())?;
    for &c in chars {
        elems.push(datum::datum::Datum::from_char(c));
    }
    let buf = arrayfuncs::construct::construct_array(
        mcx, &elems, CHAROID, 1, true, b'c',
    )?;
    Ok(Datum::ByRef(buf))
}

/// `construct_array(text_datums, n, TEXTOID, -1, false, 'i')` — a 1-D `text[]`
/// array varlena built directly from the UTF-8 strings (the by-ref text builder
/// in arrayfuncs).
fn build_text_array_datum<'mcx>(mcx: Mcx<'mcx>, strs: &[&str]) -> PgResult<Datum<'mcx>> {
    let buf = arrayfuncs::construct::build_text_array(mcx, strs)?;
    Ok(Datum::ByRef(buf))
}

/// `CStringGetTextDatum(s)` — a `text` varlena Datum: a 4-byte (full) varlena
/// header (`SET_VARSIZE`) followed by the string bytes.
fn proc_text_datum<'mcx>(mcx: Mcx<'mcx>, s: &str) -> PgResult<Datum<'mcx>> {
    let payload = s.as_bytes();
    let total = 4 + payload.len();
    let word = (total as u32) << 2;
    let mut buf: ::mcx::PgVec<'mcx, u8> = ::mcx::vec_with_capacity_in(mcx, total)?;
    buf.resize(total, 0u8);
    buf[0..4].copy_from_slice(&word.to_ne_bytes());
    buf[4..].copy_from_slice(payload);
    Ok(Datum::ByRef(buf))
}

/// Build the full `values[]` / `nulls[]` arrays for a `pg_proc` row from the
/// crossed [`PgProcInsertRow`], mirroring pg_proc.c:320-379. `oid` is taken
/// from `row.fields.oid` (the owner assigned it on the new-row path; on the
/// replace path it is unused — `replaces[oid] = false`). `proacl` is column 30:
/// present only on a fresh insert with a default ACL.
fn proc_values_nulls<'mcx>(
    mcx: Mcx<'mcx>,
    row: &cat::pg_proc::PgProcInsertRow,
) -> PgResult<(
    [Datum<'mcx>; cat::pg_proc::Natts_pg_proc],
    [bool; cat::pg_proc::Natts_pg_proc],
)> {
    use cat::pg_proc as pp;
    let f = &row.fields;

    // memset(nulls, false, ...); set per-column below.
    let mut nulls = [false; pp::Natts_pg_proc];

    // proname (NameData) — the row carries the already-truncated name string.
    let mut name_image = [0u8; 64];
    for (i, &b) in f.proname.as_bytes().iter().take(63).enumerate() {
        name_image[i] = b;
    }

    // Fixed-width columns (pg_proc.c:327-345).
    let mut values: [Datum<'mcx>; pp::Natts_pg_proc] = [
        Datum::from_oid(f.oid),                                  // oid
        name_datum(mcx, &name_image)?,                          // proname
        Datum::from_oid(f.pronamespace),                        // pronamespace
        Datum::from_oid(f.proowner),                            // proowner
        Datum::from_oid(f.prolang),                             // prolang
        Datum::from_f32(f.procost),                             // procost
        Datum::from_f32(f.prorows),                             // prorows
        Datum::from_oid(f.provariadic),                         // provariadic
        Datum::from_oid(f.prosupport),                          // prosupport
        Datum::from_char(f.prokind),                            // prokind
        Datum::from_bool(f.prosecdef),                          // prosecdef
        Datum::from_bool(f.proleakproof),                       // proleakproof
        Datum::from_bool(f.proisstrict),                        // proisstrict
        Datum::from_bool(f.proretset),                          // proretset
        Datum::from_char(f.provolatile),                        // provolatile
        Datum::from_char(f.proparallel),                        // proparallel
        Datum::from_u16(f.pronargs as u16),                     // pronargs (UInt16GetDatum)
        Datum::from_u16(f.pronargdefaults as u16),              // pronargdefaults
        Datum::from_oid(f.prorettype),                          // prorettype
        // proargtypes (oidvector) — always present.
        buildoidvector(mcx, &row.proargtypes)?,
        // proallargtypes (Oid[]) — placeholder; overwritten or nulled below.
        Datum::ByVal(0),
        Datum::ByVal(0), // proargmodes (char[])
        Datum::ByVal(0), // proargnames (text[])
        Datum::ByVal(0), // proargdefaults (pg_node_tree)
        Datum::ByVal(0), // protrftypes (Oid[])
        proc_text_datum(mcx, &row.prosrc)?, // prosrc (text) — always present.
        Datum::ByVal(0), // probin (text)
        Datum::ByVal(0), // prosqlbody (pg_node_tree)
        Datum::ByVal(0), // proconfig (text[])
        Datum::ByVal(0), // proacl (aclitem[])
    ];

    // proallargtypes (pg_proc.c:347-350).
    match &row.proallargtypes {
        Some(v) => values[(pp::Anum_pg_proc_proallargtypes - 1) as usize] = build_oid_array(mcx, v)?,
        None => nulls[(pp::Anum_pg_proc_proallargtypes - 1) as usize] = true,
    }
    // proargmodes (pg_proc.c:351-354).
    match &row.proargmodes {
        Some(v) => values[(pp::Anum_pg_proc_proargmodes - 1) as usize] = build_char_array(mcx, v)?,
        None => nulls[(pp::Anum_pg_proc_proargmodes - 1) as usize] = true,
    }
    // proargnames (pg_proc.c:355-358) — each unnamed slot is the empty string.
    match &row.proargnames {
        Some(v) => {
            let strs: Vec<&str> =
                v.iter().map(|o| o.as_deref().unwrap_or("")).collect();
            values[(pp::Anum_pg_proc_proargnames - 1) as usize] =
                build_text_array_datum(mcx, &strs)?;
        }
        None => nulls[(pp::Anum_pg_proc_proargnames - 1) as usize] = true,
    }
    // proargdefaults (pg_proc.c:359-362) — CStringGetTextDatum(nodeToString(...)).
    match &row.proargdefaults {
        Some(s) => {
            values[(pp::Anum_pg_proc_proargdefaults - 1) as usize] = proc_text_datum(mcx, s)?
        }
        None => nulls[(pp::Anum_pg_proc_proargdefaults - 1) as usize] = true,
    }
    // protrftypes (pg_proc.c:363-366).
    match &row.protrftypes {
        Some(v) => values[(pp::Anum_pg_proc_protrftypes - 1) as usize] = build_oid_array(mcx, v)?,
        None => nulls[(pp::Anum_pg_proc_protrftypes - 1) as usize] = true,
    }
    // probin (pg_proc.c:368-371).
    match &row.probin {
        Some(s) => values[(pp::Anum_pg_proc_probin - 1) as usize] = proc_text_datum(mcx, s)?,
        None => nulls[(pp::Anum_pg_proc_probin - 1) as usize] = true,
    }
    // prosqlbody (pg_proc.c:372-375) — CStringGetTextDatum(nodeToString(...)).
    match &row.prosqlbody {
        Some(s) => values[(pp::Anum_pg_proc_prosqlbody - 1) as usize] = proc_text_datum(mcx, s)?,
        None => nulls[(pp::Anum_pg_proc_prosqlbody - 1) as usize] = true,
    }
    // proconfig (pg_proc.c:376-379) — text[] of "name=value" entries.
    match &row.proconfig {
        Some(v) => {
            let strs: Vec<&str> = v.iter().map(|s| s.as_str()).collect();
            values[(pp::Anum_pg_proc_proconfig - 1) as usize] = build_text_array_datum(mcx, &strs)?;
        }
        None => nulls[(pp::Anum_pg_proc_proconfig - 1) as usize] = true,
    }
    // proacl (pg_proc.c:599-602) — set on the insert path with a default ACL
    // from `ALTER DEFAULT PRIVILEGES` (`get_user_default_acl`); carried as the
    // full on-disk `aclitem[]` varlena image, NULL otherwise.
    match &row.proacl {
        Some(image) => {
            values[(pp::Anum_pg_proc_proacl - 1) as usize] =
                Datum::ByRef(::mcx::slice_in(mcx, image)?)
        }
        None => nulls[(pp::Anum_pg_proc_proacl - 1) as usize] = true,
    }

    Ok((values, nulls))
}

fn get_new_oid_pg_proc<'mcx>(rel: &Relation<'mcx>) -> PgResult<Oid> {
    GetNewOidWithIndex(
        rel,
        cat::pg_proc::ProcedureOidIndexId,
        cat::pg_proc::Anum_pg_proc_oid,
    )
}

fn insert_pg_proc<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    row: &cat::pg_proc::PgProcInsertRow,
) -> PgResult<()> {
    let (values, nulls) = proc_values_nulls(mcx, row)?;
    // tup = heap_form_tuple(RelationGetDescr(rel), values, nulls);
    // CatalogTupleInsert(rel, tup);
    form_and_insert(mcx, rel, &values, &nulls)
}

fn update_pg_proc<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    oldtup: &FormedTuple<'mcx>,
    row: &cat::pg_proc::PgProcInsertRow,
) -> PgResult<()> {
    use cat::pg_proc as pp;
    // replaces[] starts all-true; oid / proowner / proacl are pinned to the old
    // tuple (pg_proc.c:580-585), so heap_modify_tuple takes them from `oldtup`.
    let mut replaces = [true; pp::Natts_pg_proc];
    replaces[(pp::Anum_pg_proc_oid - 1) as usize] = false;
    replaces[(pp::Anum_pg_proc_proowner - 1) as usize] = false;
    replaces[(pp::Anum_pg_proc_proacl - 1) as usize] = false;

    let (values, nulls) = proc_values_nulls(mcx, row)?;
    // tup = heap_modify_tuple(oldtup, RelationGetDescr(rel), values, nulls, replaces);
    let tupdesc = rel.rd_att_clone_in(mcx)?;
    let mut tup = heap_modify_tuple(mcx, oldtup, &tupdesc, &values, &nulls, &replaces)?;
    // CatalogTupleUpdate(rel, &tup->t_self, tup);
    CatalogTupleUpdate(mcx, rel, tup.tuple.t_self, &mut tup)
}

/* ======================================================================== *
 * pg_aggregate — AggregateCreate (no OID column; the key column aggfnoid is
 * the pre-assigned pg_proc OID). 20 fixed columns + 2 nullable text columns
 * (agginitval, aggminitval).
 * ======================================================================== */

/// The `values[]` / `nulls[]` arrays `AggregateCreate` builds for one
/// `pg_aggregate` row (pg_aggregate.c:653-687). Every fixed column is non-null;
/// `agginitval` / `aggminitval` are NULL when absent.
fn aggregate_values<'mcx>(
    mcx: Mcx<'mcx>,
    row: &cat::pg_aggregate::PgAggregateInsertRow,
) -> PgResult<(
    [Datum<'mcx>; cat::pg_aggregate::Natts_pg_aggregate],
    [bool; cat::pg_aggregate::Natts_pg_aggregate],
)> {
    let f = &row.form;
    // The column value placeholder for a NULL text column (nulls[] set below).
    let null_text = Datum::ByVal(0);
    let agginitval: Datum<'mcx> = match &row.agginitval {
        // values[Anum_pg_aggregate_agginitval - 1] = CStringGetTextDatum(agginitval);
        Some(s) => cstring_to_text_datum(mcx, s)?,
        None => null_text.clone(),
    };
    let aggminitval: Datum<'mcx> = match &row.aggminitval {
        // values[Anum_pg_aggregate_aggminitval - 1] = CStringGetTextDatum(aggminitval);
        Some(s) => cstring_to_text_datum(mcx, s)?,
        None => null_text,
    };

    let values = [
        // values[Anum_pg_aggregate_aggfnoid - 1] = ObjectIdGetDatum(procOid);
        Datum::from_oid(f.aggfnoid),
        // values[Anum_pg_aggregate_aggkind - 1] = CharGetDatum(aggKind);
        Datum::from_char(f.aggkind),
        // values[Anum_pg_aggregate_aggnumdirectargs - 1] = Int16GetDatum(numDirectArgs);
        Datum::from_i16(f.aggnumdirectargs),
        // values[Anum_pg_aggregate_aggtransfn - 1] = ObjectIdGetDatum(transfn);
        Datum::from_oid(f.aggtransfn),
        // values[Anum_pg_aggregate_aggfinalfn - 1] = ObjectIdGetDatum(finalfn);
        Datum::from_oid(f.aggfinalfn),
        // values[Anum_pg_aggregate_aggcombinefn - 1] = ObjectIdGetDatum(combinefn);
        Datum::from_oid(f.aggcombinefn),
        // values[Anum_pg_aggregate_aggserialfn - 1] = ObjectIdGetDatum(serialfn);
        Datum::from_oid(f.aggserialfn),
        // values[Anum_pg_aggregate_aggdeserialfn - 1] = ObjectIdGetDatum(deserialfn);
        Datum::from_oid(f.aggdeserialfn),
        // values[Anum_pg_aggregate_aggmtransfn - 1] = ObjectIdGetDatum(mtransfn);
        Datum::from_oid(f.aggmtransfn),
        // values[Anum_pg_aggregate_aggminvtransfn - 1] = ObjectIdGetDatum(minvtransfn);
        Datum::from_oid(f.aggminvtransfn),
        // values[Anum_pg_aggregate_aggmfinalfn - 1] = ObjectIdGetDatum(mfinalfn);
        Datum::from_oid(f.aggmfinalfn),
        // values[Anum_pg_aggregate_aggfinalextra - 1] = BoolGetDatum(finalfnExtraArgs);
        Datum::from_bool(f.aggfinalextra),
        // values[Anum_pg_aggregate_aggmfinalextra - 1] = BoolGetDatum(mfinalfnExtraArgs);
        Datum::from_bool(f.aggmfinalextra),
        // values[Anum_pg_aggregate_aggfinalmodify - 1] = CharGetDatum(finalfnModify);
        Datum::from_char(f.aggfinalmodify),
        // values[Anum_pg_aggregate_aggmfinalmodify - 1] = CharGetDatum(mfinalfnModify);
        Datum::from_char(f.aggmfinalmodify),
        // values[Anum_pg_aggregate_aggsortop - 1] = ObjectIdGetDatum(sortop);
        Datum::from_oid(f.aggsortop),
        // values[Anum_pg_aggregate_aggtranstype - 1] = ObjectIdGetDatum(aggTransType);
        Datum::from_oid(f.aggtranstype),
        // values[Anum_pg_aggregate_aggtransspace - 1] = Int32GetDatum(aggTransSpace);
        Datum::from_i32(f.aggtransspace),
        // values[Anum_pg_aggregate_aggmtranstype - 1] = ObjectIdGetDatum(aggmTransType);
        Datum::from_oid(f.aggmtranstype),
        // values[Anum_pg_aggregate_aggmtransspace - 1] = Int32GetDatum(aggmTransSpace);
        Datum::from_i32(f.aggmtransspace),
        // values[Anum_pg_aggregate_agginitval - 1] = CStringGetTextDatum(agginitval);
        agginitval,
        // values[Anum_pg_aggregate_aggminitval - 1] = CStringGetTextDatum(aggminitval);
        aggminitval,
    ];

    // for (i = 0; i < Natts_pg_aggregate; i++) nulls[i] = false;
    let mut nulls = [false; cat::pg_aggregate::Natts_pg_aggregate];
    // if (agginitval) ... else nulls[Anum_pg_aggregate_agginitval - 1] = true;
    if row.agginitval.is_none() {
        nulls[cat::pg_aggregate::Anum_pg_aggregate_agginitval as usize - 1] = true;
    }
    // if (aggminitval) ... else nulls[Anum_pg_aggregate_aggminitval - 1] = true;
    if row.aggminitval.is_none() {
        nulls[cat::pg_aggregate::Anum_pg_aggregate_aggminitval as usize - 1] = true;
    }
    Ok((values, nulls))
}

fn insert_pg_aggregate<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    row: &cat::pg_aggregate::PgAggregateInsertRow,
) -> PgResult<()> {
    let (values, nulls) = aggregate_values(mcx, row)?;
    // tup = heap_form_tuple(tupDesc, values, nulls);
    // CatalogTupleInsert(aggdesc, tup);
    form_and_insert(mcx, rel, &values, &nulls)
}

fn update_pg_aggregate<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    oldtup: &FormedTuple<'mcx>,
    row: &cat::pg_aggregate::PgAggregateInsertRow,
    replaces: cat::pg_aggregate::PgAggregateReplaces,
) -> PgResult<()> {
    // replaces[] starts all-true (pg_aggregate.c:658); aggfnoid / aggkind /
    // aggnumdirectargs are pinned to the old tuple (pg_aggregate.c:720-722), so
    // heap_modify_tuple takes them from `oldtup`.
    let mut repl = [true; cat::pg_aggregate::Natts_pg_aggregate];
    repl[cat::pg_aggregate::Anum_pg_aggregate_aggfnoid as usize - 1] = replaces.aggfnoid;
    repl[cat::pg_aggregate::Anum_pg_aggregate_aggkind as usize - 1] = replaces.aggkind;
    repl[cat::pg_aggregate::Anum_pg_aggregate_aggnumdirectargs as usize - 1] =
        replaces.aggnumdirectargs;

    let (values, nulls) = aggregate_values(mcx, row)?;

    // tup = heap_modify_tuple(oldtup, tupDesc, values, nulls, replaces);
    let tupdesc = rel.rd_att_clone_in(mcx)?;
    let mut tup = heap_modify_tuple(mcx, oldtup, &tupdesc, &values, &nulls, &repl)?;
    // CatalogTupleUpdate(aggdesc, &tup->t_self, tup);
    CatalogTupleUpdate(mcx, rel, tup.tuple.t_self, &mut tup)
}

/// Install the F1 per-catalog typed seams whose substrate is fully present
/// (pure `heap_form_tuple` + engine, plus `GetNewOidWithIndex` for the
/// OID-column catalogs). Wired from [`crate::init_seams`].
pub fn install() {
    indexing_seams::catalog_tuple_insert_pg_inherits::set(insert_pg_inherits);
    indexing_seams::catalog_tuple_update_pg_inherits::set(update_pg_inherits);
    indexing_seams::catalog_tuple_insert_pg_range::set(insert_pg_range);
    indexing_seams::catalog_tuple_insert_pg_cast::set(insert_pg_cast);
    indexing_seams::catalog_tuple_insert_pg_transform::set(insert_pg_transform);
    indexing_seams::catalog_tuple_insert_pg_conversion::set(insert_pg_conversion);
    indexing_seams::catalog_tuple_insert_pg_enum::set(insert_pg_enum);
    indexing_seams::get_new_oid_with_index_pg_enum::set(get_new_oid_pg_enum);
    indexing_seams::catalog_tuple_update_pg_enum::set(update_pg_enum);
    indexing_seams::catalog_tuple_insert_pg_language::set(insert_pg_language);
    indexing_seams::catalog_tuple_update_pg_language::set(update_pg_language);
    indexing_seams::catalog_tuples_multi_insert_pg_depend::set(multi_insert_pg_depend);
    indexing_seams::catalog_tuples_multi_insert_pg_shdepend::set(
        multi_insert_pg_shdepend,
    );
    indexing_seams::catalog_tuples_multi_insert_pg_enum::set(multi_insert_pg_enum);
    indexing_seams::catalog_tuple_insert_pg_rewrite::set(insert_pg_rewrite);
    indexing_seams::catalog_tuple_update_pg_rewrite::set(update_pg_rewrite);
    indexing_seams::catalog_tuple_update_pg_rewrite_enabled::set(
        update_pg_rewrite_enabled,
    );
    indexing_seams::catalog_tuple_update_pg_rewrite_name::set(update_pg_rewrite_name);
    indexing_seams::catalog_tuple_insert_pg_statistic_ext::set(
        insert_pg_statistic_ext,
    );
    indexing_seams::get_new_oid_with_index_pg_proc::set(get_new_oid_pg_proc);
    indexing_seams::catalog_tuple_insert_pg_proc::set(insert_pg_proc);
    indexing_seams::catalog_tuple_update_pg_proc::set(update_pg_proc);
    indexing_seams::catalog_tuple_insert_pg_aggregate::set(insert_pg_aggregate);
    indexing_seams::catalog_tuple_update_pg_aggregate::set(update_pg_aggregate);
}
