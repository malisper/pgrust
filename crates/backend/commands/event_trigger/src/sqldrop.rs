use datum::Datum;
use mcx::Mcx;
use pg_depend::ObjectAddress;
use types_core::{AttrNumber, Oid, OidIsValid, NAMEDATALEN};
use types_error::{PgResult, ERROR};
use types_scan::scankey::{BTEqualStrategyNumber, ScanKeyData};

use crate::{SQLDropObject, CURRENT_STATE};

const NAMESPACE_RELATION_ID: Oid = 2615;
const ATTR_DEFAULT_RELATION_ID: Oid = 2604;
const TRIGGER_RELATION_ID: Oid = 2620;
const TRIGGER_OID_INDEX_ID: Oid = 2702;
const Anum_pg_trigger_oid: AttrNumber = 1;
const Anum_pg_trigger_tgrelid: AttrNumber = 2;
const POLICY_RELATION_ID: Oid = 3256;

pub fn EventTriggerSQLDropAddObject(
    mcx: Mcx<'_>,
    object: &ObjectAddress,
    original: bool,
    normal: bool,
) -> PgResult<()> {
    if !crate::state_is_set() {
        return Ok(());
    }
    debug_assert!(crate::EventTriggerSupportsObject(object));

    let mut obj = SQLDropObject {
        address: *object,
        schemaname: None,
        objname: None,
        objidentity: None,
        objecttype: None,
        addrnames: None,
        addrargs: None,
        original,
        normal,
        istemp: false,
    };

    if object.classId == NAMESPACE_RELATION_ID {
        if catalog_namespace::isTempNamespace(object.objectId) {
            obj.istemp = true;
        } else if catalog_namespace::isAnyTempNamespace(object.objectId)? {
            return Ok(());
        }
        obj.objname = lsyscache::misc::get_namespace_name(mcx, object.objectId)?
            .map(|s| s.as_str().to_string());
    } else if object.classId == ATTR_DEFAULT_RELATION_ID {
        let (relid, attnum) = pg_attrdef::GetAttrDefaultColumnAddress(mcx, object.objectId)?;
        if OidIsValid(relid) {
            let mut colobject =
                ObjectAddress::set(types_core::RELATION_RELATION_ID, relid);
            colobject.objectSubId = attnum as i32;
            if !obtain_object_name_namespace(mcx, &colobject, &mut obj)? {
                return Ok(());
            }
        }
    } else if object.classId == TRIGGER_RELATION_ID {
        let relid = trigger_get_relid(mcx, object.objectId)?;
        if OidIsValid(relid) {
            // objectSubId 1 marks "namespace only, no objname" (C's trick).
            let mut relobject = ObjectAddress::set(types_core::RELATION_RELATION_ID, relid);
            relobject.objectSubId = 1;
            if !obtain_object_name_namespace(mcx, &relobject, &mut obj)? {
                return Ok(());
            }
        }
    } else if object.classId == POLICY_RELATION_ID {
        // C: a policy is temp if its table is temp; polrelid fetched the hard
        // way (no lsyscache support), then namespace-only via subId 1.
        let relid = policy_get_relid(mcx, object.objectId)?;
        if OidIsValid(relid) {
            let mut relobject = ObjectAddress::set(types_core::RELATION_RELATION_ID, relid);
            relobject.objectSubId = 1;
            if !obtain_object_name_namespace(mcx, &relobject, &mut obj)? {
                return Ok(());
            }
        }
    } else if !obtain_object_name_namespace(mcx, object, &mut obj)? {
        return Ok(());
    }

    let identity =
        catalog_objectaddress::getObjectIdentityParts(mcx, &obj.address, false)?
            .expect("missing_ok=false");
    obj.objidentity = Some(identity.identity);
    obj.addrnames = Some(identity.objname);
    obj.addrargs = Some(identity.objargs);
    obj.objecttype = Some(
        catalog_objectaddress::getObjectTypeDescription(mcx, &obj.address, false)?
            .expect("missing_ok=false"),
    );

    CURRENT_STATE.with(|s| {
        if let Some(st) = s.borrow_mut().last_mut() {
            // slist_push_head: reported in reverse-insertion order.
            st.sql_drop_list.insert(0, obj);
        }
    });
    Ok(())
}

// get_catalog_object_by_oid (objectaddress.c) with the caller's attribute
// reads folded into `decode`; None = no catalog row for objid.
fn catalog_object_row<T>(
    mcx: Mcx<'_>,
    class_id: Oid,
    objid: Oid,
    decode: impl FnOnce(&types_tuple::HeapTupleData<'_>, &types_tuple::TupleDescData<'_>) -> T,
) -> PgResult<Option<T>> {
    let prop = catalog_objectaddress::get_object_property_data(class_id);
    let rel = table::table_open(mcx, class_id, types_rel::AccessShareLock)?;
    let mut key = ScanKeyData::empty();
    key.sk_attno = prop.attnum_oid as AttrNumber;
    key.sk_strategy = BTEqualStrategyNumber;
    key.sk_collation = types_core::C_COLLATION_OID;
    key.sk_func = fmgr_seams::fmgr_info::call(types_core::fmgr::F_OIDEQ)
        .unwrap_or_else(|e| panic!("fmgr_info(F_OIDEQ) failed: {e:?}"));
    key.sk_argument = Datum::from_oid(objid);
    let mut scan = genam::systable_beginscan(
        mcx,
        &rel,
        prop.oid_index_oid,
        true,
        None,
        core::slice::from_ref(&key),
    )?;
    let result = genam::systable_getnext(mcx, &mut scan)?.map(|tup| decode(tup, rel.descr()));
    genam::systable_endscan(mcx, scan)?;
    rel.close(types_rel::AccessShareLock)?;
    Ok(result)
}

// NameStr + pstrdup of a name-column datum.
fn name_datum_str(d: Datum) -> String {
    // SAFETY: name-column datum points at NAMEDATALEN bytes.
    let bytes =
        unsafe { core::slice::from_raw_parts(d.as_usize() as *const u8, NAMEDATALEN as usize) };
    let end = bytes.iter().position(|&b| b == 0).unwrap_or(bytes.len());
    String::from_utf8_lossy(&bytes[..end]).into_owned()
}

// obtain_object_name_namespace (event_trigger.c): fill objname / schemaname /
// istemp; false = foreign temp object, don't report. Generic over
// ObjectProperty (objectaddress.c); classes outside the table are a no-op,
// as in C.
fn obtain_object_name_namespace(
    mcx: Mcx<'_>,
    object: &ObjectAddress,
    obj: &mut SQLDropObject,
) -> PgResult<bool> {
    if !catalog_objectaddress::is_objectclass_supported(object.classId) {
        return Ok(true);
    }
    let prop = catalog_objectaddress::get_object_property_data(object.classId);
    let row = catalog_object_row(mcx, object.classId, object.objectId, |tup, desc| {
        let nsp = (prop.attnum_namespace != 0)
            .then(|| {
                let mut isnull = false;
                // SAFETY: attnum from the ObjectProperty row for this catalog.
                let d = unsafe {
                    types_tuple::heap_getattr(tup, prop.attnum_namespace, desc, &mut isnull)
                };
                (!isnull).then(|| d.as_oid())
            })
            .flatten();
        let name = (prop.is_nsp_name_unique && object.objectSubId == 0 && prop.attnum_name != 0)
            .then(|| {
                let mut isnull = false;
                // SAFETY: attnum from the ObjectProperty row for this catalog.
                let d = unsafe {
                    types_tuple::heap_getattr(tup, prop.attnum_name, desc, &mut isnull)
                };
                (!isnull).then(|| name_datum_str(d))
            })
            .flatten();
        (nsp, name)
    })?;
    let Some((nsp, name)) = row else {
        // C: no catalog tuple -> nothing to fill, still reported.
        return Ok(true);
    };
    if let Some(namespace_id) = nsp {
        if catalog_namespace::isTempNamespace(namespace_id) {
            obj.schemaname = Some("pg_temp".to_string());
            obj.istemp = true;
        } else if catalog_namespace::isAnyTempNamespace(namespace_id)? {
            return Ok(false);
        } else {
            obj.schemaname = lsyscache::misc::get_namespace_name(mcx, namespace_id)?
                .map(|s| s.as_str().to_string());
            obj.istemp = false;
        }
    }
    if name.is_some() {
        obj.objname = name;
    }
    Ok(true)
}

// Namespace column value for a collected object (SRF schema column),
// pg_event_trigger_ddl_commands (event_trigger.c): classes without a
// namespace column (or outside ObjectProperty) report NULL; a vanished row or
// null namespace is an error, as in C.
pub(crate) fn object_namespace(mcx: Mcx<'_>, addr: &ObjectAddress) -> PgResult<Option<Oid>> {
    if !catalog_objectaddress::is_objectclass_supported(addr.classId) {
        return Ok(None);
    }
    let prop = catalog_objectaddress::get_object_property_data(addr.classId);
    if prop.attnum_namespace == 0 {
        return Ok(None);
    }
    let row = catalog_object_row(mcx, addr.classId, addr.objectId, |tup, desc| {
        let mut isnull = false;
        // SAFETY: attnum from the ObjectProperty row for this catalog.
        let d = unsafe { types_tuple::heap_getattr(tup, prop.attnum_namespace, desc, &mut isnull) };
        (d.as_oid(), isnull)
    })?;
    let Some((nsp, isnull)) = row else {
        return Err(elog::ereport(ERROR)
            .errmsg(format!(
                "cache lookup failed for object {}/{}",
                addr.classId, addr.objectId
            ))
            .into_error()
            .into());
    };
    if isnull {
        return Err(elog::ereport(ERROR)
            .errmsg(format!(
                "invalid null namespace in object {}/{}/{}",
                addr.classId, addr.objectId, addr.objectSubId
            ))
            .into_error()
            .into());
    }
    Ok(Some(nsp))
}

fn trigger_get_relid(mcx: Mcx<'_>, trigger_oid: Oid) -> PgResult<Oid> {
    let rel = table::table_open(mcx, TRIGGER_RELATION_ID, types_rel::AccessShareLock)?;
    let mut key = ScanKeyData::empty();
    key.sk_attno = Anum_pg_trigger_oid;
    key.sk_strategy = BTEqualStrategyNumber;
    key.sk_collation = types_core::C_COLLATION_OID;
    key.sk_func = fmgr_seams::fmgr_info::call(types_core::fmgr::F_OIDEQ)
        .unwrap_or_else(|e| panic!("fmgr_info(F_OIDEQ) failed: {e:?}"));
    key.sk_argument = Datum::from_oid(trigger_oid);
    let mut scan = genam::systable_beginscan(
        mcx,
        &rel,
        TRIGGER_OID_INDEX_ID,
        true,
        None,
        core::slice::from_ref(&key),
    )?;
    let mut relid = types_core::InvalidOid;
    if let Some(tup) = genam::systable_getnext(mcx, &mut scan)? {
        let mut isnull = false;
        // SAFETY: fixed NOT NULL pg_trigger.tgrelid under its descriptor.
        relid = unsafe {
            types_tuple::heap_getattr(tup, Anum_pg_trigger_tgrelid as i32, rel.descr(), &mut isnull)
        }
        .as_oid();
    }
    genam::systable_endscan(mcx, scan)?;
    rel.close(types_rel::AccessShareLock)?;
    Ok(relid)
}

const POLICY_OID_INDEX_ID: Oid = 3257;
const Anum_pg_policy_oid: AttrNumber = 1;
const Anum_pg_policy_polrelid: AttrNumber = 3;

fn policy_get_relid(mcx: Mcx<'_>, policy_oid: Oid) -> PgResult<Oid> {
    let rel = table::table_open(mcx, POLICY_RELATION_ID, types_rel::AccessShareLock)?;
    let mut key = ScanKeyData::empty();
    key.sk_attno = Anum_pg_policy_oid;
    key.sk_strategy = BTEqualStrategyNumber;
    key.sk_collation = types_core::C_COLLATION_OID;
    key.sk_func = fmgr_seams::fmgr_info::call(types_core::fmgr::F_OIDEQ)
        .unwrap_or_else(|e| panic!("fmgr_info(F_OIDEQ) failed: {e:?}"));
    key.sk_argument = Datum::from_oid(policy_oid);
    let mut scan = genam::systable_beginscan(
        mcx,
        &rel,
        POLICY_OID_INDEX_ID,
        true,
        None,
        core::slice::from_ref(&key),
    )?;
    let mut relid = types_core::InvalidOid;
    if let Some(tup) = genam::systable_getnext(mcx, &mut scan)? {
        let mut isnull = false;
        // SAFETY: fixed NOT NULL pg_policy.polrelid under its descriptor.
        relid = unsafe {
            types_tuple::heap_getattr(tup, Anum_pg_policy_polrelid as i32, rel.descr(), &mut isnull)
        }
        .as_oid();
    }
    genam::systable_endscan(mcx, scan)?;
    rel.close(types_rel::AccessShareLock)?;
    Ok(relid)
}

#[cfg(test)]
mod tests {
    use super::*;
    use mcx::MemoryContext;

    fn drop_obj(object: ObjectAddress) -> SQLDropObject {
        SQLDropObject {
            address: object,
            schemaname: None,
            objname: None,
            objidentity: None,
            objecttype: None,
            addrnames: None,
            addrargs: None,
            original: true,
            normal: false,
            istemp: false,
        }
    }

    // Classes outside ObjectProperty are a no-op that still reports the drop
    // (C event_trigger.c: "does nothing for object classes that are not in
    // ObjectProperty"); previously fenced with a panic.
    #[test]
    fn obtain_object_name_namespace_ignores_unsupported_class() {
        let ctx = MemoryContext::new("sqldrop-test");
        // pg_enum (3501) has no ObjectProperty row.
        let object = ObjectAddress::set(3501, 50020);
        let mut obj = drop_obj(object);
        assert!(obtain_object_name_namespace(ctx.mcx(), &object, &mut obj).unwrap());
        assert!(obj.schemaname.is_none());
        assert!(obj.objname.is_none());
        assert!(!obj.istemp);
    }

    // ddl_commands schema column: classes without a namespace column (or
    // outside ObjectProperty) are NULL without a catalog lookup.
    #[test]
    fn object_namespace_null_for_schema_less_classes() {
        let ctx = MemoryContext::new("sqldrop-test");
        // pg_enum (3501): not in ObjectProperty.
        assert_eq!(object_namespace(ctx.mcx(), &ObjectAddress::set(3501, 1)).unwrap(), None);
        // pg_rewrite (2618), pg_default_acl (826), pg_user_mapping (1418):
        // ObjectProperty rows with attnum_namespace = InvalidAttrNumber.
        assert_eq!(object_namespace(ctx.mcx(), &ObjectAddress::set(2618, 1)).unwrap(), None);
        assert_eq!(object_namespace(ctx.mcx(), &ObjectAddress::set(826, 1)).unwrap(), None);
        assert_eq!(object_namespace(ctx.mcx(), &ObjectAddress::set(1418, 1)).unwrap(), None);
    }
}
