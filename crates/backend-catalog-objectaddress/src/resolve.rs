//! F0 keystone — the resolution model: `get_object_address[_rv]` plus the 13
//! `get_object_address_*` helpers, ownership/namespace checks, the
//! string↔objtype maps, and `get_catalog_object_by_oid[_extended]`
//! (objectaddress.c 923-2864, 2391-2727, 6186).
//!
//! Fill status (F0 keystone): `get_object_namespace`,
//! `read_objtype_from_string`, `get_relkind_objtype`, and
//! `get_catalog_object_by_oid[_extended]` are filled with faithful C logic
//! (syscache/indexing-seam scans + total maps). The resolution ENGINE —
//! `get_object_address[_rv]`, the 13 `get_object_address_*` helpers,
//! `check_object_ownership`, and `object_ownercheck` — remains a sanctioned
//! mirror-and-panic: faithfully porting it fans out to ~40 cross-crate lookup
//! callees, ~25 of which need NEW seam declarations and 6 NEW `-seams` crates
//! that do not yet exist (pg-cast/-transform/-statistic-ext/-parameter-acl/
//! rewritesupport/-amop/amproc/publication), plus several EXISTING owner seams
//! whose signatures diverge from what objectaddress.c needs. That coordinated
//! cross-crate seam-contract pass is out of scope for the F0 fill (it overlaps
//! the active seam-reconcile lane); the bodies resolve against real [`Node`],
//! [`Relation`], [`ObjectAddress`], [`ObjectType`] (no invented node-demux
//! model) and panic loudly until that contract pass lands.

use mcx::Mcx;
use types_catalog::catalog_dependency::ObjectAddress;
use types_core::Oid;
use types_error::{PgError, PgResult};
use types_nodes::parsenodes::ObjectType;
use types_nodes::parsenodes::{
    OBJECT_FOREIGN_TABLE, OBJECT_INDEX, OBJECT_MATVIEW, OBJECT_SEQUENCE, OBJECT_TABLE, OBJECT_VIEW,
};
use types_parsenodes::Node;
use types_rel::Relation;
use types_storage::lock::LOCKMODE;

use types_tuple::backend_access_common_heaptuple::{Datum as TupleDatum, FormedTuple};

use backend_catalog_objectaddress_seams::ResolvedObjectAddress;

const INVALID_OID: Oid = 0;

/// Extract an `Oid` from a tuple column value (catalog OID columns are
/// pass-by-value). The canonical `Datum` codec reads the bare word back as an
/// `Oid`; a by-reference value is not an OID column, so yield `InvalidOid`.
fn tuplevalue_oid(val: &TupleDatum<'_>) -> Oid {
    match val {
        TupleDatum::ByVal(_) => val.as_oid(),
        TupleDatum::ByRef(_) => 0,
    }
}

/* ---------------------------------------------------------------------------
 * get_object_address + get_object_address_rv (the public resolution entry)
 * ------------------------------------------------------------------------- */

/// `get_object_address(ObjectType objtype, Node *object, Relation *relp,
/// LOCKMODE lockmode, bool missing_ok)` (objectaddress.c 923): resolve the
/// parser representation `object` to an [`ObjectAddress`], taking `lockmode`
/// and returning the relation it opened (`*relp`) inside
/// [`ResolvedObjectAddress`]. This is the body the
/// `backend_catalog_objectaddress_seams::get_object_address` seam routes to.
pub fn get_object_address<'mcx>(
    _mcx: Mcx<'mcx>,
    _objtype: ObjectType,
    _object: &Node,
    _lockmode: LOCKMODE,
    _missing_ok: bool,
) -> PgResult<ResolvedObjectAddress<'mcx>> {
    panic!("decomp: get_object_address not yet filled")
}

/// `get_object_address_rv(ObjectType objtype, RangeVar *rel, List *object,
/// Relation *relp, LOCKMODE lockmode, bool missing_ok)` (objectaddress.c
/// 1225): the RangeVar-prefixed variant. `rel`/`object` cross as their real
/// [`Node`] representations.
pub fn get_object_address_rv<'mcx>(
    _mcx: Mcx<'mcx>,
    _objtype: ObjectType,
    _rel: Option<&Node>,
    _object: &Node,
    _lockmode: LOCKMODE,
    _missing_ok: bool,
) -> PgResult<ResolvedObjectAddress<'mcx>> {
    panic!("decomp: get_object_address_rv not yet filled")
}

/* ---------------------------------------------------------------------------
 * The 13 get_object_address_* helpers (objectaddress.c 1247-1963)
 * ------------------------------------------------------------------------- */

/// `get_object_address_unqualified(ObjectType objtype, String *strval, bool
/// missing_ok)` (objectaddress.c 1247).
pub fn get_object_address_unqualified(
    _objtype: ObjectType,
    _strval: &Node,
    _missing_ok: bool,
) -> PgResult<ObjectAddress> {
    panic!("decomp: get_object_address_unqualified not yet filled")
}

/// `get_relation_by_qualified_name(ObjectType objtype, List *object, Relation
/// *relp, LOCKMODE lockmode, bool missing_ok)` (objectaddress.c 1338).
pub fn get_relation_by_qualified_name<'mcx>(
    _mcx: Mcx<'mcx>,
    _objtype: ObjectType,
    _object: &Node,
    _lockmode: LOCKMODE,
    _missing_ok: bool,
) -> PgResult<ResolvedObjectAddress<'mcx>> {
    panic!("decomp: get_relation_by_qualified_name not yet filled")
}

/// `get_object_address_relobject(ObjectType objtype, List *object, Relation
/// *relp, bool missing_ok)` (objectaddress.c 1420).
pub fn get_object_address_relobject<'mcx>(
    _mcx: Mcx<'mcx>,
    _objtype: ObjectType,
    _object: &Node,
    _missing_ok: bool,
) -> PgResult<ResolvedObjectAddress<'mcx>> {
    panic!("decomp: get_object_address_relobject not yet filled")
}

/// `get_object_address_attribute(ObjectType objtype, List *object, Relation
/// *relp, LOCKMODE lockmode, bool missing_ok)` (objectaddress.c 1499).
pub fn get_object_address_attribute<'mcx>(
    _mcx: Mcx<'mcx>,
    _objtype: ObjectType,
    _object: &Node,
    _lockmode: LOCKMODE,
    _missing_ok: bool,
) -> PgResult<ResolvedObjectAddress<'mcx>> {
    panic!("decomp: get_object_address_attribute not yet filled")
}

/// `get_object_address_attrdef(ObjectType objtype, List *object, Relation
/// *relp, LOCKMODE lockmode, bool missing_ok)` (objectaddress.c 1550).
pub fn get_object_address_attrdef<'mcx>(
    _mcx: Mcx<'mcx>,
    _objtype: ObjectType,
    _object: &Node,
    _lockmode: LOCKMODE,
    _missing_ok: bool,
) -> PgResult<ResolvedObjectAddress<'mcx>> {
    panic!("decomp: get_object_address_attrdef not yet filled")
}

/// `get_object_address_type(ObjectType objtype, TypeName *typename, bool
/// missing_ok)` (objectaddress.c 1608). `typename` crosses as a real
/// [`Node`] (`Node::TypeName`).
pub fn get_object_address_type(
    _objtype: ObjectType,
    _typename: &Node,
    _missing_ok: bool,
) -> PgResult<ObjectAddress> {
    panic!("decomp: get_object_address_type not yet filled")
}

/// `get_object_address_opcf(ObjectType objtype, List *object, bool
/// missing_ok)` (objectaddress.c 1647).
pub fn get_object_address_opcf(
    _objtype: ObjectType,
    _object: &Node,
    _missing_ok: bool,
) -> PgResult<ObjectAddress> {
    panic!("decomp: get_object_address_opcf not yet filled")
}

/// `get_object_address_opf_member(ObjectType objtype, List *object, bool
/// missing_ok)` (objectaddress.c 1685).
pub fn get_object_address_opf_member(
    _objtype: ObjectType,
    _object: &Node,
    _missing_ok: bool,
) -> PgResult<ObjectAddress> {
    panic!("decomp: get_object_address_opf_member not yet filled")
}

/// `get_object_address_usermapping(List *object, bool missing_ok)`
/// (objectaddress.c 1797).
pub fn get_object_address_usermapping(
    _object: &Node,
    _missing_ok: bool,
) -> PgResult<ObjectAddress> {
    panic!("decomp: get_object_address_usermapping not yet filled")
}

/// `get_object_address_publication_rel(List *object, Relation *relp, bool
/// missing_ok)` (objectaddress.c 1868).
pub fn get_object_address_publication_rel<'mcx>(
    _mcx: Mcx<'mcx>,
    _object: &Node,
    _missing_ok: bool,
) -> PgResult<ResolvedObjectAddress<'mcx>> {
    panic!("decomp: get_object_address_publication_rel not yet filled")
}

/// `get_object_address_publication_schema(List *object, bool missing_ok)`
/// (objectaddress.c 1921).
pub fn get_object_address_publication_schema(
    _object: &Node,
    _missing_ok: bool,
) -> PgResult<ObjectAddress> {
    panic!("decomp: get_object_address_publication_schema not yet filled")
}

/// `get_object_address_defacl(List *object, bool missing_ok)`
/// (objectaddress.c 1963).
pub fn get_object_address_defacl(
    _object: &Node,
    _missing_ok: bool,
) -> PgResult<ObjectAddress> {
    panic!("decomp: get_object_address_defacl not yet filled")
}

/* ---------------------------------------------------------------------------
 * Ownership + namespace (objectaddress.c 2391-2608)
 * ------------------------------------------------------------------------- */

/// `check_object_ownership(Oid roleid, ObjectType objtype, ObjectAddress
/// address, Node *object, Relation relation)` (objectaddress.c 2391): verify
/// `roleid` may drop the object, else `ereport(ERROR)` (carried on `Err`).
/// This is the body the seam install routes to.
pub fn check_object_ownership<'mcx>(
    _roleid: Oid,
    _objtype: ObjectType,
    _address: ObjectAddress,
    _object: &Node,
    _relation: Option<&Relation<'mcx>>,
) -> PgResult<()> {
    panic!("decomp: check_object_ownership not yet filled")
}

/// `object_ownercheck(Oid classid, Oid objectid, Oid roleid)` — the
/// catalog-class ownership probe used by `check_object_ownership` (acl.c in C,
/// but reasoned over here against `ObjectProperty[]`'s owner attnum). Returns
/// whether `roleid` owns the object.
pub fn object_ownercheck(_classid: Oid, _objectid: Oid, _roleid: Oid) -> PgResult<bool> {
    panic!("decomp: object_ownercheck not yet filled")
}

/// `get_object_namespace(const ObjectAddress *address)` (objectaddress.c
/// 2573). This is the body the seam install routes to.
pub fn get_object_namespace(address: &ObjectAddress) -> PgResult<Oid> {
    use backend_utils_cache_syscache::{SearchSysCache1, SysCacheGetAttrNotNull};
    use types_cache::SysCacheKey;
    use types_datum::Datum;

    // If not owned by a namespace, just return InvalidOid.
    let property = crate::properties::get_object_property_data(address.classId)?;
    if property.attnum_namespace == crate::consts::InvalidAttrNumber {
        return Ok(INVALID_OID);
    }

    // Currently, we can only handle object types with system caches.
    let cache = property.oid_catcache_id;
    debug_assert!(cache != -1);

    // Fetch tuple from syscache and extract namespace attribute. The C reads
    // in `CurrentMemoryContext`; the result is a bare `Oid` value (nothing
    // escapes the context), so a transient local context is the faithful
    // stand-in for the seam's mcx-less signature.
    let cx = mcx::MemoryContext::new("get_object_namespace");
    let mcx = cx.mcx();
    let tuple = SearchSysCache1(
        mcx,
        cache,
        SysCacheKey::Value(Datum::from_oid(address.objectId)),
    )?;
    let Some(tuple) = tuple else {
        return Err(PgError::error(format!(
            "cache lookup failed for cache {} oid {}",
            cache, address.objectId
        )));
    };
    let val = SysCacheGetAttrNotNull(mcx, cache, &tuple, property.attnum_namespace as i32)?;
    Ok(tuplevalue_oid(&val))
}

/* ---------------------------------------------------------------------------
 * string↔objtype + relkind mapping (objectaddress.c 2609, 6186)
 * ------------------------------------------------------------------------- */

/// `read_objtype_from_string(const char *objtype)` (objectaddress.c 2609):
/// scan [`crate::tables::OBJECT_TYPE_MAP`] for the matching name, returning
/// the raw `ObjectType` value; `ereport(ERROR)` for an unrecognized string
/// (carried on `Err`). The raw `i32` preserves the C `-1` "unmapped" rows.
pub fn read_objtype_from_string(objtype: &str) -> PgResult<i32> {
    for entry in crate::tables::OBJECT_TYPE_MAP {
        if entry.tm_name == objtype {
            return Ok(entry.tm_type);
        }
    }
    Err(PgError::error(format!(
        "unrecognized object type \"{objtype}\""
    )))
}

/// `get_relkind_objtype(char relkind)` (objectaddress.c 6186): map a pg_class
/// relkind to the `ObjectType` used in error messages (unknown ⇒
/// `OBJECT_TABLE`). Total; cannot `ereport`. This is the body the seam install
/// routes to. The seam contract pins `relkind: u8`.
pub fn get_relkind_objtype(relkind: u8) -> ObjectType {
    use types_tuple::access::{
        RELKIND_FOREIGN_TABLE, RELKIND_INDEX, RELKIND_MATVIEW, RELKIND_PARTITIONED_INDEX,
        RELKIND_PARTITIONED_TABLE, RELKIND_RELATION, RELKIND_SEQUENCE, RELKIND_TOASTVALUE,
        RELKIND_VIEW,
    };
    match relkind {
        x if x == RELKIND_RELATION || x == RELKIND_PARTITIONED_TABLE => OBJECT_TABLE,
        x if x == RELKIND_INDEX || x == RELKIND_PARTITIONED_INDEX => OBJECT_INDEX,
        x if x == RELKIND_SEQUENCE => OBJECT_SEQUENCE,
        x if x == RELKIND_VIEW => OBJECT_VIEW,
        x if x == RELKIND_MATVIEW => OBJECT_MATVIEW,
        x if x == RELKIND_FOREIGN_TABLE => OBJECT_FOREIGN_TABLE,
        x if x == RELKIND_TOASTVALUE => OBJECT_TABLE,
        // Per above, don't raise an error.
        _ => OBJECT_TABLE,
    }
}

/* ---------------------------------------------------------------------------
 * get_catalog_object_by_oid[_extended] (objectaddress.c 2790-2862)
 * ------------------------------------------------------------------------- */

/// `get_catalog_object_by_oid(Relation catalog, AttrNumber oidcol, Oid
/// objectId)` (objectaddress.c 2790): `systable_beginscan` the open `catalog`
/// relation keyed on `oidcol = objectId`, returning the located heap tuple (or
/// `None` when absent). Backed by the
/// `backend_catalog_indexing_seams::get_catalog_object_by_oid` scan primitive;
/// the returned token carries the located tuple for the description/identity
/// families to deform.
pub fn get_catalog_object_by_oid<'mcx>(
    mcx: Mcx<'mcx>,
    catalog: &Relation<'mcx>,
    oidcol: i16,
    object_id: Oid,
) -> PgResult<Option<FormedTuple<'mcx>>> {
    get_catalog_object_by_oid_extended(mcx, catalog, oidcol, object_id, false)
}

/// `get_catalog_object_by_oid_extended(Relation catalog, AttrNumber oidcol,
/// Oid objectId, bool locktuple)` (objectaddress.c 2803): the `locktuple`
/// variant that takes a `LockTuple` on the located row before returning it.
pub fn get_catalog_object_by_oid_extended<'mcx>(
    mcx: Mcx<'mcx>,
    catalog: &Relation<'mcx>,
    oidcol: i16,
    object_id: Oid,
    locktuple: bool,
) -> PgResult<Option<FormedTuple<'mcx>>> {
    // The syscache-first scan + optional `LockTuple` (which itself dispatches on
    // `get_object_catcache_oid` / `get_object_oid_index`) is the indexing seam's
    // `get_catalog_object_by_oid` scan primitive.
    backend_catalog_indexing_seams::get_catalog_object_by_oid::call(
        mcx, catalog, oidcol, object_id, locktuple,
    )
}
