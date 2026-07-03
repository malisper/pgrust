//! pg_namespace.c. nspacl stays NULL (no pg_default_acl rows can exist);

#![allow(non_snake_case, non_upper_case_globals)]

use datum::Datum;
use mcx::Mcx;
use types_core::{AttrNumber, InvalidOid, Oid, NAMESPACE_RELATION_ID};
use types_error::{PgError, PgResult, ERRCODE_DUPLICATE_SCHEMA, ERROR};
use types_rel::RowExclusiveLock;
use types_tuple::NameData;

pub const NamespaceOidIndexId: Oid = 2685;
pub const Anum_pg_namespace_oid: AttrNumber = 1;
pub const Natts_pg_namespace: usize = 4;

// isTemp-only skips (default ACL, extension dep, hook) are unported no-ops.
pub fn NamespaceCreate<'mcx>(
    mcx: Mcx<'mcx>,
    nspName: &str,
    ownerId: Oid,
    _isTemp: bool,
) -> PgResult<Oid> {
    if syscache_seams::lookup_pg_namespace_oid_by_name::call(nspName)? != InvalidOid {
        return Err(Box::new(
            PgError::new(ERROR, format!("schema \"{nspName}\" already exists"))
                .with_sqlstate(ERRCODE_DUPLICATE_SCHEMA),
        ));
    }

    let nspdesc = table::table_open(mcx, NAMESPACE_RELATION_ID, RowExclusiveLock)?;
    let nspoid = catalog::GetNewOidWithIndex(
        mcx,
        &nspdesc,
        NamespaceOidIndexId,
        Anum_pg_namespace_oid,
    )?;
    let mut name = NameData::default();
    name.namestrcpy(nspName);
    let values = [
        Datum::from_oid(nspoid),
        Datum::from_usize(name.data.as_ptr() as usize),
        Datum::from_oid(ownerId),
        Datum::null(),
    ];
    let nulls = [false, false, false, true];
    let mut tup = heaptuple::heap_form_tuple(mcx, nspdesc.descr(), &values, &nulls)?;
    catalog_indexing::CatalogTupleInsert(mcx, &nspdesc, &mut tup)?;
    nspdesc.close(RowExclusiveLock)?;

    pg_depend::recordDependencyOnOwner(NAMESPACE_RELATION_ID, nspoid, ownerId);
    Ok(nspoid)
}

// RemoveSchemaById (schemacmds.c), hosted here: schemacmds reaches
// catalog_dependency (cycle).
pub fn RemoveSchemaById<'mcx>(mcx: Mcx<'mcx>, schemaOid: Oid) -> PgResult<()> {
    let rel = table::table_open(mcx, NAMESPACE_RELATION_ID, RowExclusiveLock)?;
    let mut key = types_scan::scankey::ScanKeyData::empty();
    key.sk_attno = Anum_pg_namespace_oid;
    key.sk_strategy = types_scan::scankey::BTEqualStrategyNumber;
    key.sk_collation = 0;
    key.sk_func = fmgr_seams::fmgr_info::call(types_core::fmgr::F_OIDEQ)
        .unwrap_or_else(|e| panic!("fmgr_info(F_OIDEQ) failed: {e:?}"));
    key.sk_argument = Datum::from_oid(schemaOid);
    let mut scan = genam::systable_beginscan(
        mcx,
        &rel,
        NamespaceOidIndexId,
        true,
        None,
        core::slice::from_ref(&key),
    )?;
    let tup = genam::systable_getnext(mcx, &mut scan)?
        .unwrap_or_else(|| panic!("cache lookup failed for namespace {schemaOid}"));
    let tid = tup.t_self;
    catalog_indexing::CatalogTupleDelete(&rel, &tid)?;
    genam::systable_endscan(mcx, scan)?;
    rel.close(RowExclusiveLock)
}
