// getObjectDescription / getObjectIdentity arms for the live object classes;
// all other classes are named panics.
use crate::{
    unported, AttrDefaultRelationId, ConstraintRelationId, ObjectAddress, ProcedureRelationId,
    RewriteRelationId, TriggerRelationId,
};
use datum::Datum;
use format_type::quote_identifier;
use mcx::Mcx;
use types_core::primitive::OidIsValid;
use types_core::{
    AttrNumber, Oid, AUTH_ID_RELATION_ID, DATABASE_RELATION_ID,
    NAMESPACE_RELATION_ID, RELATION_RELATION_ID, TYPE_RELATION_ID,
};
use types_error::PgResult;
use types_rel::{AccessShareLock, RELKIND_COMPOSITE_TYPE, RELKIND_FOREIGN_TABLE, RELKIND_INDEX,
    RELKIND_MATVIEW, RELKIND_PARTITIONED_INDEX, RELKIND_PARTITIONED_TABLE, RELKIND_RELATION,
    RELKIND_SEQUENCE, RELKIND_TOASTVALUE, RELKIND_VIEW,
};
use types_scan::scankey::{BTEqualStrategyNumber, ScanKeyData};
use types_tuple::NameData;

fn oid_key(attno: AttrNumber, oid: Oid) -> ScanKeyData {
    let mut key = ScanKeyData::empty();
    key.sk_attno = attno;
    key.sk_strategy = BTEqualStrategyNumber;
    key.sk_collation = 0;
    key.sk_func = fmgr_seams::fmgr_info::call(types_core::fmgr::F_OIDEQ)
        .unwrap_or_else(|e| panic!("fmgr_info(F_OIDEQ) failed: {e:?}"));
    key.sk_argument = Datum::from_oid(oid);
    key
}

fn name_from_datum(d: Datum) -> String {
    let mut name = NameData::default();
    // SAFETY: a NameData column's datum points at its 64-byte in-tuple buffer.
    unsafe {
        core::ptr::copy_nonoverlapping(
            d.as_usize() as *const u8,
            name.data.as_mut_ptr(),
            name.data.len(),
        );
    }
    core::str::from_utf8(name.name_str()).expect("catalog NameData is valid UTF-8").to_string()
}

fn name_str(name: &NameData) -> &str {
    core::str::from_utf8(name.name_str()).expect("catalog NameData is valid UTF-8")
}

fn get_namespace_name(nspid: Oid) -> PgResult<Option<String>> {
    Ok(syscache_seams::pg_namespace_nspname::call(nspid)?.map(|n| name_str(&n).to_string()))
}

fn quote_qualified(nspname: Option<&str>, name: &str) -> String {
    match nspname {
        Some(nsp) => format!("{}.{}", quote_identifier(nsp), quote_identifier(name)),
        None => quote_identifier(name).into_owned(),
    }
}

// getRelationDescription (objectaddress.c).
fn getRelationDescription(relid: Oid, missing_ok: bool) -> PgResult<Option<String>> {
    let Some(relname) = syscache_seams::pg_class_relname::call(relid)? else {
        if !missing_ok {
            panic!("cache lookup failed for relation {relid}");
        }
        return Ok(None);
    };
    let shape = syscache_seams::lookup_pg_class_ls_shape::call(relid)?
        .unwrap_or_else(|| panic!("cache lookup failed for relation {relid}"));
    let nspname = if catalog_namespace::RelationIsVisible(relid)? {
        None
    } else {
        get_namespace_name(shape.relnamespace)?
    };
    let relname = quote_qualified(nspname.as_deref(), name_str(&relname));
    let kind = shape.relkind as u8;
    let noun = match kind {
        RELKIND_RELATION | RELKIND_PARTITIONED_TABLE => "table",
        RELKIND_INDEX | RELKIND_PARTITIONED_INDEX => "index",
        RELKIND_SEQUENCE => "sequence",
        RELKIND_TOASTVALUE => "toast table",
        RELKIND_VIEW => "view",
        RELKIND_MATVIEW => "materialized view",
        RELKIND_COMPOSITE_TYPE => "composite type",
        RELKIND_FOREIGN_TABLE => "foreign table",
        _ => "relation",
    };
    Ok(Some(format!("{noun} {relname}")))
}

// format_procedure_extended (regproc.c), plain signature slice.
fn format_procedure(mcx: Mcx<'_>, procid: Oid, force_qualify: bool) -> PgResult<Option<String>> {
    let Some(proname) = syscache_seams::pg_proc_proname::call(procid)? else {
        return Ok(None);
    };
    let shape = syscache_seams::lookup_pg_proc_shape::call(procid)?
        .unwrap_or_else(|| panic!("cache lookup failed for function {procid}"));
    let (_, argtypes) = syscache_seams::lookup_pg_proc_signature::call(mcx, procid)?
        .unwrap_or_else(|| panic!("cache lookup failed for function {procid}"));
    let nspname = if !force_qualify && catalog_namespace::FunctionIsVisible(mcx, procid)? {
        None
    } else {
        get_namespace_name(shape.pronamespace)?
    };
    let mut out = quote_qualified(nspname.as_deref(), name_str(&proname));
    out.push('(');
    for (i, argtype) in argtypes.iter().enumerate() {
        if i > 0 {
            out.push(',');
        }
        out.push_str(&format_type::format_type_be(*argtype)?);
    }
    out.push(')');
    Ok(Some(out))
}

fn scan_one_row<'mcx, T>(
    mcx: Mcx<'mcx>,
    reloid: Oid,
    indexoid: Oid,
    objid: Oid,
    decode: impl FnOnce(&types_tuple::HeapTupleData<'_>, &types_tuple::TupleDescData<'_>) -> T,
) -> PgResult<Option<T>> {
    let rel = table::table_open(mcx, reloid, AccessShareLock)?;
    let keys = [oid_key(1, objid)];
    let mut scan = genam::systable_beginscan(mcx, &rel, indexoid, true, None, &keys)?;
    let result = genam::systable_getnext(mcx, &mut scan)?.map(|tup| decode(tup, rel.descr()));
    genam::systable_endscan(mcx, scan)?;
    rel.close(AccessShareLock)?;
    Ok(result)
}

fn getattr(tup: &types_tuple::HeapTupleData<'_>, attnum: i32, desc: &types_tuple::TupleDescData<'_>) -> Datum {
    let mut isnull = false;
    // SAFETY: fixed NOT NULL catalog column under the relation's descriptor.
    let d = unsafe { types_tuple::heap_getattr(tup, attnum, desc, &mut isnull) };
    debug_assert!(!isnull);
    d
}

// getObjectDescription (objectaddress.c). None = undefined object under
// missing_ok (C returns NULL / empty buffer).
pub fn getObjectDescription(
    mcx: Mcx<'_>,
    object: &ObjectAddress,
    missing_ok: bool,
) -> PgResult<Option<String>> {
    match object.classId {
        RELATION_RELATION_ID => {
            if object.objectSubId == 0 {
                getRelationDescription(object.objectId, missing_ok)
            } else {
                let Some(attname) = lsyscache::get_attname(
                    mcx,
                    object.objectId,
                    object.objectSubId as AttrNumber,
                    missing_ok,
                )?
                else {
                    return Ok(None);
                };
                let rel = getRelationDescription(object.objectId, missing_ok)?
                    .expect("relation of an existing column exists");
                Ok(Some(format!("column {} of {rel}", attname.as_str())))
            }
        }
        ProcedureRelationId => {
            // FORMAT_PROC_INVALID_AS_NULL.
            let Some(proname) = format_procedure(mcx, object.objectId, false)? else {
                if !missing_ok {
                    panic!("cache lookup failed for function {}", object.objectId);
                }
                return Ok(None);
            };
            Ok(Some(format!("function {proname}")))
        }
        TYPE_RELATION_ID => {
            // FORMAT_TYPE_INVALID_AS_NULL.
            if syscache_seams::pg_type_name_namespace::call(object.objectId)?.is_none() {
                if !missing_ok {
                    panic!("cache lookup failed for type {}", object.objectId);
                }
                return Ok(None);
            }
            Ok(Some(format!(
                "type {}",
                format_type::format_type_be(object.objectId)?
            )))
        }
        ConstraintRelationId => {
            let Some(con) =
                syscache_seams::lookup_pg_constraint_desc_shape::call(object.objectId)?
            else {
                if !missing_ok {
                    panic!("cache lookup failed for constraint {}", object.objectId);
                }
                return Ok(None);
            };
            if OidIsValid(con.conrelid) {
                let rel = getRelationDescription(con.conrelid, false)?
                    .expect("constraint's relation exists");
                Ok(Some(format!("constraint {} on {rel}", name_str(&con.conname))))
            } else {
                Ok(Some(format!("constraint {}", name_str(&con.conname))))
            }
        }
        AttrDefaultRelationId => {
            let (adrelid, adnum) = pg_attrdef::GetAttrDefaultColumnAddress(mcx, object.objectId)?;
            if !OidIsValid(adrelid) {
                if !missing_ok {
                    panic!("could not find tuple for attrdef {}", object.objectId);
                }
                return Ok(None);
            }
            let colobject = ObjectAddress::sub_set(RELATION_RELATION_ID, adrelid, adnum as i32);
            let col = getObjectDescription(mcx, &colobject, false)?
                .expect("attrdef's column exists");
            Ok(Some(format!("default value for {col}")))
        }
        RewriteRelationId => {
            let row = scan_one_row(mcx, RewriteRelationId, 2692, object.objectId, |tup, desc| {
                (name_from_datum(getattr(tup, 2, desc)), getattr(tup, 3, desc).as_oid())
            })?;
            let Some((rulename, ev_class)) = row else {
                if !missing_ok {
                    panic!("could not find tuple for rule {}", object.objectId);
                }
                return Ok(None);
            };
            let rel = getRelationDescription(ev_class, false)?.expect("rule's relation exists");
            Ok(Some(format!("rule {rulename} on {rel}")))
        }
        TriggerRelationId => {
            let row = scan_one_row(mcx, TriggerRelationId, 2702, object.objectId, |tup, desc| {
                (getattr(tup, 2, desc).as_oid(), name_from_datum(getattr(tup, 4, desc)))
            })?;
            let Some((tgrelid, tgname)) = row else {
                if !missing_ok {
                    panic!("could not find tuple for trigger {}", object.objectId);
                }
                return Ok(None);
            };
            let rel = getRelationDescription(tgrelid, false)?.expect("trigger's relation exists");
            Ok(Some(format!("trigger {tgname} on {rel}")))
        }
        NAMESPACE_RELATION_ID => {
            let Some(nspname) = get_namespace_name(object.objectId)? else {
                if !missing_ok {
                    panic!("cache lookup failed for namespace {}", object.objectId);
                }
                return Ok(None);
            };
            Ok(Some(format!("schema {nspname}")))
        }
        AUTH_ID_RELATION_ID => {
            Ok(miscinit::GetUserNameFromId(mcx, object.objectId, missing_ok)?
                .map(|name| format!("role {}", name.as_str())))
        }
        DATABASE_RELATION_ID => {
            let Some(datname) = dbcommands_seams::get_database_name::call(object.objectId)? else {
                if !missing_ok {
                    panic!("cache lookup failed for database {}", object.objectId);
                }
                return Ok(None);
            };
            Ok(Some(format!("database {datname}")))
        }
        other => unported(&format!("getObjectDescription object class {other}")),
    }
}

// getObjectIdentity (objectaddress.c getObjectIdentityParts), same class
// coverage as getObjectDescription.
pub fn getObjectIdentity(
    mcx: Mcx<'_>,
    object: &ObjectAddress,
    missing_ok: bool,
) -> PgResult<Option<String>> {
    match object.classId {
        RELATION_RELATION_ID => {
            let Some(ident) = getRelationIdentity(object.objectId, missing_ok)? else {
                return Ok(None);
            };
            if object.objectSubId == 0 {
                return Ok(Some(ident));
            }
            let Some(attname) = lsyscache::get_attname(
                mcx,
                object.objectId,
                object.objectSubId as AttrNumber,
                missing_ok,
            )?
            else {
                return Ok(None);
            };
            Ok(Some(format!("{ident}.{}", quote_identifier(attname.as_str()))))
        }
        ProcedureRelationId => {
            let Some(proname) = format_procedure(mcx, object.objectId, true)? else {
                if !missing_ok {
                    panic!("cache lookup failed for function {}", object.objectId);
                }
                return Ok(None);
            };
            Ok(Some(proname))
        }
        TYPE_RELATION_ID => {
            let Some((typname, typnamespace)) =
                syscache_seams::pg_type_name_namespace::call(object.objectId)?
            else {
                if !missing_ok {
                    panic!("cache lookup failed for type {}", object.objectId);
                }
                return Ok(None);
            };
            // FORMAT_TYPE_FORCE_QUALIFY: catalog names, no special-casing.
            let nspname = get_namespace_name(typnamespace)?
                .unwrap_or_else(|| panic!("cache lookup failed for namespace {typnamespace}"));
            Ok(Some(quote_qualified(Some(&nspname), name_str(&typname))))
        }
        NAMESPACE_RELATION_ID => {
            let Some(nspname) = get_namespace_name(object.objectId)? else {
                if !missing_ok {
                    panic!("cache lookup failed for namespace {}", object.objectId);
                }
                return Ok(None);
            };
            Ok(Some(quote_identifier(&nspname).into_owned()))
        }
        AUTH_ID_RELATION_ID => {
            Ok(miscinit::GetUserNameFromId(mcx, object.objectId, missing_ok)?
                .map(|name| quote_identifier(name.as_str()).into_owned()))
        }
        DATABASE_RELATION_ID => {
            let Some(datname) = dbcommands_seams::get_database_name::call(object.objectId)? else {
                if !missing_ok {
                    panic!("cache lookup failed for database {}", object.objectId);
                }
                return Ok(None);
            };
            Ok(Some(quote_identifier(&datname).into_owned()))
        }
        other => unported(&format!("getObjectIdentity object class {other}")),
    }
}

// getRelationIdentity (objectaddress.c): always schema-qualified.
fn getRelationIdentity(relid: Oid, missing_ok: bool) -> PgResult<Option<String>> {
    let Some(relname) = syscache_seams::pg_class_relname::call(relid)? else {
        if !missing_ok {
            panic!("cache lookup failed for relation {relid}");
        }
        return Ok(None);
    };
    let shape = syscache_seams::lookup_pg_class_ls_shape::call(relid)?
        .unwrap_or_else(|| panic!("cache lookup failed for relation {relid}"));
    let nspname = get_namespace_name(shape.relnamespace)?
        .unwrap_or_else(|| panic!("cache lookup failed for namespace {}", shape.relnamespace));
    Ok(Some(quote_qualified(Some(&nspname), name_str(&relname))))
}
