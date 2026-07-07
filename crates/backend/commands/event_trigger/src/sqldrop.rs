use datum::Datum;
use mcx::Mcx;
use pg_depend::ObjectAddress;
use types_core::{AttrNumber, Oid, OidIsValid, NAMEDATALEN};
use types_error::PgResult;
use types_scan::scankey::{BTEqualStrategyNumber, ScanKeyData};

use crate::{SQLDropObject, CURRENT_STATE};

const NAMESPACE_RELATION_ID: Oid = 2615;
const ATTR_DEFAULT_RELATION_ID: Oid = 2604;
const TRIGGER_RELATION_ID: Oid = 2620;
const TRIGGER_OID_INDEX_ID: Oid = 2702;
const Anum_pg_trigger_oid: AttrNumber = 1;
const Anum_pg_trigger_tgrelid: AttrNumber = 2;
const POLICY_RELATION_ID: Oid = 3256;
const DEFAULT_ACL_RELATION_ID: Oid = 826;
const TYPE_RELATION_ID: Oid = types_core::TYPE_RELATION_ID;
const CONSTRAINT_RELATION_ID: Oid = 2606;
const PROCEDURE_RELATION_ID: Oid = 1255;
const REWRITE_RELATION_ID: Oid = 2618;
const STATISTIC_EXT_RELATION_ID: Oid = 3381;
const USER_MAPPING_RELATION_ID: Oid = types_core::USER_MAPPING_RELATION_ID;
const FOREIGN_SERVER_RELATION_ID: Oid = types_core::catalog::FOREIGN_SERVER_RELATION_ID;
const FOREIGN_DATA_WRAPPER_RELATION_ID: Oid = types_core::catalog::FOREIGN_DATA_WRAPPER_RELATION_ID;
const Anum_pg_foreign_server_srvname: i32 = 2;
const Anum_pg_foreign_data_wrapper_fdwname: i32 = 2;

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

struct ClassNaming {
    nsp_attnum: Option<i32>,
    name_attnum: i32,
    namensp_unique: bool,
    syscache_id: i32,
}

fn class_naming(class_id: Oid) -> Option<ClassNaming> {
    // (attnum_namespace, attnum_name, namensp_unique) per ObjectProperty
    // (objectaddress.c), for the classes reachable from ported drops.
    match class_id {
        TYPE_RELATION_ID => Some(ClassNaming {
            nsp_attnum: Some(3),
            name_attnum: 2,
            namensp_unique: true,
            syscache_id: cache_syscache::TYPEOID,
        }),
        CONSTRAINT_RELATION_ID => Some(ClassNaming {
            nsp_attnum: Some(3),
            name_attnum: 2,
            namensp_unique: false,
            syscache_id: cache_syscache::CONSTROID,
        }),
        PROCEDURE_RELATION_ID => Some(ClassNaming {
            nsp_attnum: Some(3),
            name_attnum: 2,
            namensp_unique: false,
            syscache_id: cache_syscache::PROCOID,
        }),
        STATISTIC_EXT_RELATION_ID => Some(ClassNaming {
            nsp_attnum: Some(4),
            name_attnum: 3,
            namensp_unique: true,
            syscache_id: cache_syscache::STATEXTOID,
        }),
        // ObjectProperty: no namespace column (srvname/fdwname are globally
        // unique, not per-schema).
        FOREIGN_SERVER_RELATION_ID => Some(ClassNaming {
            nsp_attnum: None,
            name_attnum: Anum_pg_foreign_server_srvname,
            namensp_unique: true,
            syscache_id: cache_syscache::FOREIGNSERVEROID,
        }),
        FOREIGN_DATA_WRAPPER_RELATION_ID => Some(ClassNaming {
            nsp_attnum: None,
            name_attnum: Anum_pg_foreign_data_wrapper_fdwname,
            namensp_unique: true,
            syscache_id: cache_syscache::FOREIGNDATAWRAPPEROID,
        }),
        _ => None,
    }
}

// obtain_object_name_namespace (event_trigger.c): fill objname / schemaname /
// istemp; false = foreign temp object, don't report.
fn obtain_object_name_namespace(
    mcx: Mcx<'_>,
    object: &ObjectAddress,
    obj: &mut SQLDropObject,
) -> PgResult<bool> {
    let (nsp, name): (Option<Oid>, Option<String>) = match object.classId {
        types_core::RELATION_RELATION_ID => {
            let nsp = lsyscache::relation::get_rel_namespace(object.objectId)?;
            let name = lsyscache::relation::get_rel_name(mcx, object.objectId)?
                .map(|s| s.as_str().to_string());
            (Some(nsp), name)
        }
        // ObjectProperty rows with no name/namespace attnums (objectaddress.c):
        // C's obtain_object_name_namespace is a no-op for these classes.
        REWRITE_RELATION_ID | DEFAULT_ACL_RELATION_ID | USER_MAPPING_RELATION_ID => (None, None),
        other => match class_naming(other) {
            Some(naming) => syscache_naming(object.objectId, &naming)?,
            None => panic!(
                "obtain_object_name_namespace (event_trigger.c): unported object class {other}"
            ),
        },
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

    let unique = match object.classId {
        types_core::RELATION_RELATION_ID => true,
        REWRITE_RELATION_ID => false,
        other => class_naming(other).map(|n| n.namensp_unique).unwrap_or(false),
    };
    if unique && object.objectSubId == 0 {
        obj.objname = name;
    }
    Ok(true)
}

// Namespace column value for a collected object (SRF schema column).
pub(crate) fn object_namespace(addr: &ObjectAddress) -> PgResult<Option<Oid>> {
    match class_naming(addr.classId) {
        Some(naming) => Ok(syscache_naming(addr.objectId, &naming)?.0),
        None => Ok(None),
    }
}

fn syscache_naming(oid: Oid, naming: &ClassNaming) -> PgResult<(Option<Oid>, Option<String>)> {
    let Some(tup) = cache_syscache::SearchSysCache1(
        naming.syscache_id,
        cache_syscache::SysCacheKey::Value(Datum::from_oid(oid)),
    )?
    else {
        return Ok((None, None));
    };
    let nsp = match naming.nsp_attnum {
        Some(attnum) => {
            let (nsp_d, nsp_null) =
                cache_syscache::SysCacheGetAttr(naming.syscache_id, &tup, attnum)?;
            if nsp_null { None } else { Some(nsp_d.as_oid()) }
        }
        None => None,
    };
    let (name_d, name_null) =
        cache_syscache::SysCacheGetAttr(naming.syscache_id, &tup, naming.name_attnum)?;
    let name = if name_null {
        None
    } else {
        // SAFETY: name-column datum points at NAMEDATALEN bytes.
        let bytes = unsafe {
            core::slice::from_raw_parts(name_d.as_usize() as *const u8, NAMEDATALEN as usize)
        };
        let end = bytes.iter().position(|&b| b == 0).unwrap_or(bytes.len());
        Some(String::from_utf8_lossy(&bytes[..end]).into_owned())
    };
    cache_syscache::ReleaseSysCache(tup);
    Ok((nsp, name))
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
