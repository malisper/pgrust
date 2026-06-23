//! `pg_opfamily` / `pg_opclass` / `pg_amop` / `pg_amproc` catalog-write value
//! layer (`commands/opclasscmds.c`'s `CreateOpFamily` / `DefineOpClass` /
//! `storeOperators` / `storeProcedures`).
//!
//! The `opclasscmds.c` orchestration (in `backend-commands-opclasscmds`) opens
//! the relation by OID and supplies the deformed `FormData_*` row; each body
//! assigns the row OID with `GetNewOidWithIndex`, forms the heap tuple against
//! the relation's descriptor, and runs `CatalogTupleInsert` (index maintenance
//! included). Mirrors the `family_authid` no-`mcx` insert precedent: the bodies
//! own a private `MemoryContext` and re-open the relation in it.

use mcx::{Mcx, MemoryContext};
use ::types_catalog::opclasscmds_catalog as oc;
use ::types_core::Oid;
use ::types_error::PgResult;
use rel::{Relation, RelationData};
use ::types_storage::lock::RowExclusiveLock;
use types_tuple::heaptuple::Datum;

use ::heaptuple::heap_form_tuple;
use ::table::table_open;
use ::catalog_catalog::GetNewOidWithIndex;

use crate::keystone::CatalogTupleInsert;

/// `namestrcpy(&name, src)` — a zero-filled 64-byte `NameData` image.
fn name_datum<'mcx>(mcx: Mcx<'mcx>, src: &str) -> PgResult<Datum<'mcx>> {
    let mut name = [0u8; 64];
    for (i, &b) in src.as_bytes().iter().take(63).enumerate() {
        name[i] = b;
    }
    Ok(Datum::ByRef(::mcx::slice_in(mcx, &name[..])?))
}

/// `table_open(rel->rd_id, RowExclusiveLock)` — re-open the catalog in `mcx`.
fn reopen<'mcx>(mcx: Mcx<'mcx>, rel: &RelationData<'_>) -> PgResult<Relation<'mcx>> {
    table_open(mcx, rel.rd_id, RowExclusiveLock)
}

/// `heap_form_tuple(RelationGetDescr(rel), values, nulls)` +
/// `CatalogTupleInsert(rel, tup)`.
fn form_and_insert<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    values: &[Datum<'mcx>],
    nulls: &[bool],
) -> PgResult<()> {
    let tupdesc = rel.rd_att_clone_in(mcx)?;
    let mut tup = heap_form_tuple(mcx, &tupdesc, values, nulls)?;
    CatalogTupleInsert(mcx, rel, &mut tup)
}

/// `CreateOpFamily`'s pg_opfamily insert (opclasscmds.c).
fn insert_pg_opfamily(rel: &RelationData<'_>, form: &oc::FormData_pg_opfamily) -> PgResult<Oid> {
    let ctx = MemoryContext::new("insert_pg_opfamily");
    let mcx = ctx.mcx();
    let r = reopen(mcx, rel)?;

    // opfamilyoid = GetNewOidWithIndex(rel, OpfamilyOidIndexId, Anum_pg_opfamily_oid);
    let opfamilyoid =
        GetNewOidWithIndex(&r, oc::OpfamilyOidIndexId, oc::Anum_pg_opfamily_oid)?;

    let mut values: Vec<Datum> = vec![Datum::null(); oc::Natts_pg_opfamily];
    let nulls: Vec<bool> = vec![false; oc::Natts_pg_opfamily];

    values[oc::Anum_pg_opfamily_oid as usize - 1] = Datum::from_oid(opfamilyoid);
    values[oc::Anum_pg_opfamily_opfmethod as usize - 1] = Datum::from_oid(form.opfmethod);
    values[oc::Anum_pg_opfamily_opfname as usize - 1] = name_datum(mcx, &form.opfname)?;
    values[oc::Anum_pg_opfamily_opfnamespace as usize - 1] = Datum::from_oid(form.opfnamespace);
    values[oc::Anum_pg_opfamily_opfowner as usize - 1] = Datum::from_oid(form.opfowner);

    form_and_insert(mcx, &r, &values, &nulls)?;
    Ok(opfamilyoid)
}

/// `DefineOpClass`'s pg_opclass insert (opclasscmds.c).
fn insert_pg_opclass(rel: &RelationData<'_>, form: &oc::FormData_pg_opclass) -> PgResult<Oid> {
    let ctx = MemoryContext::new("insert_pg_opclass");
    let mcx = ctx.mcx();
    let r = reopen(mcx, rel)?;

    // opclassoid = GetNewOidWithIndex(rel, OpclassOidIndexId, Anum_pg_opclass_oid);
    let opclassoid = GetNewOidWithIndex(&r, oc::OpclassOidIndexId, oc::Anum_pg_opclass_oid)?;

    let mut values: Vec<Datum> = vec![Datum::null(); oc::Natts_pg_opclass];
    let nulls: Vec<bool> = vec![false; oc::Natts_pg_opclass];

    values[oc::Anum_pg_opclass_oid as usize - 1] = Datum::from_oid(opclassoid);
    values[oc::Anum_pg_opclass_opcmethod as usize - 1] = Datum::from_oid(form.opcmethod);
    values[oc::Anum_pg_opclass_opcname as usize - 1] = name_datum(mcx, &form.opcname)?;
    values[oc::Anum_pg_opclass_opcnamespace as usize - 1] = Datum::from_oid(form.opcnamespace);
    values[oc::Anum_pg_opclass_opcowner as usize - 1] = Datum::from_oid(form.opcowner);
    values[oc::Anum_pg_opclass_opcfamily as usize - 1] = Datum::from_oid(form.opcfamily);
    values[oc::Anum_pg_opclass_opcintype as usize - 1] = Datum::from_oid(form.opcintype);
    values[oc::Anum_pg_opclass_opcdefault as usize - 1] = Datum::from_bool(form.opcdefault);
    // opckeytype is a plain Oid column (InvalidOid when no STORAGE type).
    values[oc::Anum_pg_opclass_opckeytype as usize - 1] = Datum::from_oid(form.opckeytype);

    form_and_insert(mcx, &r, &values, &nulls)?;
    Ok(opclassoid)
}

/// `storeOperators`' pg_amop insert (opclasscmds.c).
fn insert_pg_amop(rel: &RelationData<'_>, form: &oc::FormData_pg_amop) -> PgResult<Oid> {
    let ctx = MemoryContext::new("insert_pg_amop");
    let mcx = ctx.mcx();
    let r = reopen(mcx, rel)?;

    // entryoid = GetNewOidWithIndex(rel, AccessMethodOperatorOidIndexId, Anum_pg_amop_oid);
    let entryoid =
        GetNewOidWithIndex(&r, oc::AccessMethodOperatorOidIndexId, oc::Anum_pg_amop_oid)?;

    let mut values: Vec<Datum> = vec![Datum::null(); oc::Natts_pg_amop];
    let nulls: Vec<bool> = vec![false; oc::Natts_pg_amop];

    values[oc::Anum_pg_amop_oid as usize - 1] = Datum::from_oid(entryoid);
    values[oc::Anum_pg_amop_amopfamily as usize - 1] = Datum::from_oid(form.amopfamily);
    values[oc::Anum_pg_amop_amoplefttype as usize - 1] = Datum::from_oid(form.amoplefttype);
    values[oc::Anum_pg_amop_amoprighttype as usize - 1] = Datum::from_oid(form.amoprighttype);
    values[oc::Anum_pg_amop_amopstrategy as usize - 1] = Datum::from_i16(form.amopstrategy);
    values[oc::Anum_pg_amop_amoppurpose as usize - 1] = Datum::from_char(form.amoppurpose);
    values[oc::Anum_pg_amop_amopopr as usize - 1] = Datum::from_oid(form.amopopr);
    values[oc::Anum_pg_amop_amopmethod as usize - 1] = Datum::from_oid(form.amopmethod);
    values[oc::Anum_pg_amop_amopsortfamily as usize - 1] = Datum::from_oid(form.amopsortfamily);

    form_and_insert(mcx, &r, &values, &nulls)?;
    Ok(entryoid)
}

/// `storeProcedures`' pg_amproc insert (opclasscmds.c).
fn insert_pg_amproc(rel: &RelationData<'_>, form: &oc::FormData_pg_amproc) -> PgResult<Oid> {
    let ctx = MemoryContext::new("insert_pg_amproc");
    let mcx = ctx.mcx();
    let r = reopen(mcx, rel)?;

    // entryoid = GetNewOidWithIndex(rel, AccessMethodProcedureOidIndexId, Anum_pg_amproc_oid);
    let entryoid =
        GetNewOidWithIndex(&r, oc::AccessMethodProcedureOidIndexId, oc::Anum_pg_amproc_oid)?;

    let mut values: Vec<Datum> = vec![Datum::null(); oc::Natts_pg_amproc];
    let nulls: Vec<bool> = vec![false; oc::Natts_pg_amproc];

    values[oc::Anum_pg_amproc_oid as usize - 1] = Datum::from_oid(entryoid);
    values[oc::Anum_pg_amproc_amprocfamily as usize - 1] = Datum::from_oid(form.amprocfamily);
    values[oc::Anum_pg_amproc_amproclefttype as usize - 1] = Datum::from_oid(form.amproclefttype);
    values[oc::Anum_pg_amproc_amprocrighttype as usize - 1] = Datum::from_oid(form.amprocrighttype);
    values[oc::Anum_pg_amproc_amprocnum as usize - 1] = Datum::from_i16(form.amprocnum);
    values[oc::Anum_pg_amproc_amproc as usize - 1] = Datum::from_oid(form.amproc);

    form_and_insert(mcx, &r, &values, &nulls)?;
    Ok(entryoid)
}

/// `CreateAccessMethod`'s pg_am insert (commands/amcmds.c).
fn insert_pg_am(
    rel: &RelationData<'_>,
    amname: &str,
    amhandler: Oid,
    amtype: u8,
) -> PgResult<Oid> {
    let ctx = MemoryContext::new("insert_pg_am");
    let mcx = ctx.mcx();
    let r = reopen(mcx, rel)?;

    // amoid = GetNewOidWithIndex(rel, AmOidIndexId, Anum_pg_am_oid);
    let amoid = GetNewOidWithIndex(&r, oc::AmOidIndexId, oc::Anum_pg_am_oid)?;

    let mut values: Vec<Datum> = vec![Datum::null(); oc::Natts_pg_am];
    let nulls: Vec<bool> = vec![false; oc::Natts_pg_am];

    // values[Anum_pg_am_oid-1]      = ObjectIdGetDatum(amoid);
    // values[Anum_pg_am_amname-1]   = CStringGetDatum(namein(stmt->amname));
    // values[Anum_pg_am_amhandler-1]= ObjectIdGetDatum(amhandler);
    // values[Anum_pg_am_amtype-1]   = CharGetDatum(stmt->amtype);
    values[oc::Anum_pg_am_oid as usize - 1] = Datum::from_oid(amoid);
    values[oc::Anum_pg_am_amname as usize - 1] = name_datum(mcx, amname)?;
    values[oc::Anum_pg_am_amhandler as usize - 1] = Datum::from_oid(amhandler);
    values[oc::Anum_pg_am_amtype as usize - 1] = Datum::from_char(amtype as i8);

    form_and_insert(mcx, &r, &values, &nulls)?;
    Ok(amoid)
}

pub fn install() {
    use indexing_seams as s;
    s::catalog_tuple_insert_pg_opfamily::set(insert_pg_opfamily);
    s::catalog_tuple_insert_pg_opclass::set(insert_pg_opclass);
    s::catalog_tuple_insert_pg_amop::set(insert_pg_amop);
    s::catalog_tuple_insert_pg_amproc::set(insert_pg_amproc);
    s::catalog_tuple_insert_pg_am::set(insert_pg_am);
}
