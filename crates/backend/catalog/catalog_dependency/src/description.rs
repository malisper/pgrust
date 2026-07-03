// getObjectDescription/getObjectTypeDescription/getObjectIdentityParts
// (objectaddress.c), drop-lane object classes only; every other class is loud.
use datum::Datum;
use format_type::quote_identifier;
use mcx::Mcx;
use pg_depend::ObjectAddress;
use types_core::{
    AttrNumber, InvalidOid, Oid, CONSTRAINT_OID_INDEX_ID, CONSTRAINT_RELATION_ID,
    NAMESPACE_RELATION_ID, RELATION_RELATION_ID, TYPE_RELATION_ID,
};
use types_error::{PgError, PgResult};
use types_rel::pg_class::{
    RELKIND_COMPOSITE_TYPE, RELKIND_FOREIGN_TABLE, RELKIND_INDEX, RELKIND_MATVIEW,
    RELKIND_PARTITIONED_INDEX, RELKIND_PARTITIONED_TABLE, RELKIND_RELATION,
    RELKIND_SEQUENCE, RELKIND_TOASTVALUE, RELKIND_VIEW,
};
use types_rel::AccessShareLock;
use types_tuple::NameData;

const TriggerRelationId: Oid = 2620;
const TriggerOidIndexId: Oid = 2702;
const RewriteOidIndexId: Oid = 2692;
const Anum_pg_rewrite_rulename: usize = 2;
const Anum_pg_rewrite_ev_class: usize = 3;
const Anum_pg_trigger_tgrelid: usize = 2;
const Anum_pg_trigger_tgname: usize = 4;
const Anum_pg_statistic_ext_stxname: usize = 3;
const Anum_pg_statistic_ext_stxnamespace: usize = 4;
const Anum_pg_event_trigger_evtname: i32 = 2;

#[cold]
#[inline(never)]
fn cache_lookup_failed(relid: Oid) -> Box<PgError> {
    Box::new(PgError::error(format!("cache lookup failed for relation {relid}")))
}

pub fn getObjectDescription<'mcx>(
    mcx: Mcx<'mcx>,
    object: &ObjectAddress,
) -> PgResult<Option<String>> {
    match object.classId {
        RELATION_RELATION_ID => {
            if object.objectSubId == 0 {
                Ok(Some(getRelationDescription(mcx, object.objectId)?))
            } else {
                let attname = lsyscache::attribute::get_attname(
                    mcx,
                    object.objectId,
                    object.objectSubId as AttrNumber,
                    false,
                )?
                .expect("missing_ok=false");
                let rel = getRelationDescription(mcx, object.objectId)?;
                Ok(Some(format!("column {attname} of {rel}")))
            }
        }
        other => panic!("unported: objectaddress.c getObjectDescription class {other}"),
    }
}

fn getRelationDescription<'mcx>(mcx: Mcx<'mcx>, relid: Oid) -> PgResult<String> {
    let Some(relname) = lsyscache::relation::get_rel_name(mcx, relid)? else {
        return Err(cache_lookup_failed(relid));
    };
    let relkind = lsyscache::relation::get_rel_relkind(relid)? as u8;

    // RelationIsVisible: visible iff an unqualified lookup along the active
    // search path resolves to this relation.
    let nspname = if catalog_namespace::RelnameGetRelid(&relname)? == relid {
        None
    } else {
        let nsp = lsyscache::relation::get_rel_namespace(relid)?;
        lsyscache::misc::get_namespace_name(mcx, nsp)?
    };
    let qualified = match &nspname {
        Some(nsp) => format!(
            "{}.{}",
            format_type::quote_identifier(nsp),
            format_type::quote_identifier(&relname)
        ),
        None => format_type::quote_identifier(&relname).into_owned(),
    };

    let noun = match relkind {
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
    Ok(format!("{noun} {qualified}"))
}

pub struct ObjectIdentity {
    pub identity: String,
    pub objname: Vec<String>,
    pub objargs: Vec<String>,
}

#[cold]
#[inline(never)]
fn lookup_err(msg: String) -> Box<PgError> {
    Box::new(PgError::error(msg))
}

fn quote_qualified(schema: &str, name: &str) -> String {
    format!("{}.{}", quote_identifier(schema), quote_identifier(name))
}

fn name_at(d: Datum) -> String {
    // SAFETY: NameData column datums point at the 64-byte in-tuple buffer.
    let n = unsafe { *(d.as_usize() as *const NameData) };
    String::from_utf8_lossy(n.name_str()).into_owned()
}

pub fn getObjectTypeDescription<'mcx>(
    mcx: Mcx<'mcx>,
    object: &ObjectAddress,
    missing_ok: bool,
) -> PgResult<Option<String>> {
    let s: String = match object.classId {
        RELATION_RELATION_ID => {
            getRelationTypeDescription(mcx, object.objectId, object.objectSubId, missing_ok)?
        }
        TYPE_RELATION_ID => "type".into(),
        crate::ConstraintRelationId => {
            getConstraintTypeDescription(mcx, object.objectId, missing_ok)?
        }
        crate::AttrDefaultRelationId => "default value".into(),
        crate::RewriteRelationId => "rule".into(),
        TriggerRelationId => "trigger".into(),
        NAMESPACE_RELATION_ID => "schema".into(),
        x if x == statscmds::StatisticExtRelationId => "statistics object".into(),
        crate::EventTriggerRelationId => "event trigger".into(),
        types_core::PROCEDURE_RELATION_ID => {
            getProcedureTypeDescription(object.objectId, missing_ok)?
        }
        other => panic!("unported: objectaddress.c getObjectTypeDescription class {other}"),
    };
    Ok(Some(s))
}

fn getRelationTypeDescription<'mcx>(
    mcx: Mcx<'mcx>,
    relid: Oid,
    objectSubId: i32,
    missing_ok: bool,
) -> PgResult<String> {
    if lsyscache::relation::get_rel_name(mcx, relid)?.is_none() {
        if !missing_ok {
            return Err(cache_lookup_failed(relid));
        }
        return Ok("relation".into());
    }
    let relkind = lsyscache::relation::get_rel_relkind(relid)? as u8;
    let mut s: String = match relkind {
        RELKIND_RELATION | RELKIND_PARTITIONED_TABLE => "table",
        RELKIND_INDEX | RELKIND_PARTITIONED_INDEX => "index",
        RELKIND_SEQUENCE => "sequence",
        RELKIND_TOASTVALUE => "toast table",
        RELKIND_VIEW => "view",
        RELKIND_MATVIEW => "materialized view",
        RELKIND_COMPOSITE_TYPE => "composite type",
        RELKIND_FOREIGN_TABLE => "foreign table",
        _ => "relation",
    }
    .into();
    if objectSubId != 0 {
        s.push_str(" column");
    }
    Ok(s)
}

fn constraint_row<'mcx>(
    mcx: Mcx<'mcx>,
    constroid: Oid,
) -> PgResult<Option<(String, Oid, Oid)>> {
    let constr_rel = table::table_open(mcx, CONSTRAINT_RELATION_ID, AccessShareLock)?;
    let keys = [crate::oid_key(pg_constraint::Anum_pg_constraint_oid as usize, constroid)];
    let mut scan =
        genam::systable_beginscan(mcx, &constr_rel, CONSTRAINT_OID_INDEX_ID, true, None, &keys)?;
    let row = genam::systable_getnext(mcx, &mut scan)?.map(|tup| {
        let desc = constr_rel.descr();
        (
            name_at(crate::getattr(tup, pg_constraint::Anum_pg_constraint_conname as usize, desc)),
            crate::getattr(tup, pg_constraint::Anum_pg_constraint_conrelid as usize, desc)
                .as_oid(),
            crate::getattr(tup, pg_constraint::Anum_pg_constraint_contypid as usize, desc)
                .as_oid(),
        )
    });
    genam::systable_endscan(mcx, scan)?;
    constr_rel.close(AccessShareLock)?;
    Ok(row)
}

fn getConstraintTypeDescription<'mcx>(
    mcx: Mcx<'mcx>,
    constroid: Oid,
    missing_ok: bool,
) -> PgResult<String> {
    let Some((_, conrelid, contypid)) = constraint_row(mcx, constroid)? else {
        if !missing_ok {
            return Err(lookup_err(format!("cache lookup failed for constraint {constroid}")));
        }
        return Ok("constraint".into());
    };
    if conrelid != InvalidOid {
        Ok("table constraint".into())
    } else if contypid != InvalidOid {
        Ok("domain constraint".into())
    } else {
        Err(lookup_err(format!("invalid constraint {constroid}")))
    }
}

// (name, referenced relation oid) of a pg_rewrite/pg_trigger-style row.
fn name_on_relation_row<'mcx>(
    mcx: Mcx<'mcx>,
    catalog_id: Oid,
    index_id: Oid,
    name_anum: usize,
    rel_anum: usize,
    oid: Oid,
) -> PgResult<Option<(String, Oid)>> {
    let rel = table::table_open(mcx, catalog_id, AccessShareLock)?;
    let keys = [crate::oid_key(1, oid)];
    let mut scan = genam::systable_beginscan(mcx, &rel, index_id, true, None, &keys)?;
    let row = genam::systable_getnext(mcx, &mut scan)?.map(|tup| {
        let desc = rel.descr();
        (
            name_at(crate::getattr(tup, name_anum, desc)),
            crate::getattr(tup, rel_anum, desc).as_oid(),
        )
    });
    genam::systable_endscan(mcx, scan)?;
    rel.close(AccessShareLock)?;
    Ok(row)
}

#[cold]
fn identity_vanished(object: &ObjectAddress, missing_ok: bool) -> PgResult<Option<ObjectIdentity>> {
    if !missing_ok {
        return Err(lookup_err(format!(
            "requested object address for unsupported object class {}: text result \"\"",
            object.classId
        )));
    }
    Ok(None)
}

pub fn getObjectIdentityParts<'mcx>(
    mcx: Mcx<'mcx>,
    object: &ObjectAddress,
    missing_ok: bool,
) -> PgResult<Option<ObjectIdentity>> {
    match object.classId {
        RELATION_RELATION_ID => {
            let attr = if object.objectSubId != 0 {
                match lsyscache::attribute::get_attname(
                    mcx,
                    object.objectId,
                    object.objectSubId as AttrNumber,
                    missing_ok,
                )? {
                    Some(a) => Some(a.as_str().to_owned()),
                    None => return Ok(None),
                }
            } else {
                None
            };
            let Some(mut ident) = getRelationIdentity(mcx, object.objectId, missing_ok)? else {
                return Ok(None);
            };
            if let Some(attr) = attr {
                ident.identity.push('.');
                ident.identity.push_str(&quote_identifier(&attr));
                ident.objname.push(attr);
            }
            Ok(Some(ident))
        }
        TYPE_RELATION_ID => {
            let Some(typeout) = format_type::format_type_extended(
                object.objectId,
                -1,
                format_type::FORMAT_TYPE_INVALID_AS_NULL | format_type::FORMAT_TYPE_FORCE_QUALIFY,
            )?
            else {
                return identity_vanished(object, missing_ok);
            };
            Ok(Some(ObjectIdentity {
                identity: typeout.clone(),
                objname: vec![typeout],
                objargs: vec![],
            }))
        }
        NAMESPACE_RELATION_ID => {
            let Some(nspname) =
                lsyscache::misc::get_namespace_name_or_temp(mcx, object.objectId)?
            else {
                if !missing_ok {
                    return Err(lookup_err(format!(
                        "cache lookup failed for namespace {}",
                        object.objectId
                    )));
                }
                return Ok(None);
            };
            let nspname = nspname.as_str().to_owned();
            Ok(Some(ObjectIdentity {
                identity: quote_identifier(&nspname).into_owned(),
                objname: vec![nspname],
                objargs: vec![],
            }))
        }
        crate::ConstraintRelationId => {
            let Some((conname, conrelid, contypid)) = constraint_row(mcx, object.objectId)?
            else {
                if !missing_ok {
                    return Err(lookup_err(format!(
                        "cache lookup failed for constraint {}",
                        object.objectId
                    )));
                }
                return Ok(None);
            };
            if conrelid != InvalidOid {
                let rel = getRelationIdentity(mcx, conrelid, false)?.expect("missing_ok=false");
                let identity = format!("{} on {}", quote_identifier(&conname), rel.identity);
                let mut objname = rel.objname;
                objname.push(conname);
                Ok(Some(ObjectIdentity { identity, objname, objargs: vec![] }))
            } else {
                debug_assert!(contypid != InvalidOid);
                let domain = ObjectAddress::set(TYPE_RELATION_ID, contypid);
                let t =
                    getObjectIdentityParts(mcx, &domain, false)?.expect("missing_ok=false");
                Ok(Some(ObjectIdentity {
                    identity: format!("{} on {}", quote_identifier(&conname), t.identity),
                    objname: t.objname,
                    objargs: vec![conname],
                }))
            }
        }
        crate::AttrDefaultRelationId => {
            let colobject = pg_attrdef::GetAttrDefaultColumnAddress(mcx, object.objectId)?;
            if colobject.objectId == InvalidOid {
                if !missing_ok {
                    return Err(lookup_err(format!(
                        "could not find tuple for attrdef {}",
                        object.objectId
                    )));
                }
                return Ok(None);
            }
            let col = getObjectIdentityParts(mcx, &colobject, false)?.expect("missing_ok=false");
            Ok(Some(ObjectIdentity {
                identity: format!("for {}", col.identity),
                objname: col.objname,
                objargs: col.objargs,
            }))
        }
        crate::RewriteRelationId => {
            let Some((rulename, ev_class)) = name_on_relation_row(
                mcx,
                crate::RewriteRelationId,
                RewriteOidIndexId,
                Anum_pg_rewrite_rulename,
                Anum_pg_rewrite_ev_class,
                object.objectId,
            )?
            else {
                if !missing_ok {
                    return Err(lookup_err(format!(
                        "could not find tuple for rule {}",
                        object.objectId
                    )));
                }
                return Ok(None);
            };
            let rel = getRelationIdentity(mcx, ev_class, false)?.expect("missing_ok=false");
            let identity = format!("{} on {}", quote_identifier(&rulename), rel.identity);
            let mut objname = rel.objname;
            objname.push(rulename);
            Ok(Some(ObjectIdentity { identity, objname, objargs: vec![] }))
        }
        TriggerRelationId => {
            let Some((tgname, tgrelid)) = name_on_relation_row(
                mcx,
                TriggerRelationId,
                TriggerOidIndexId,
                Anum_pg_trigger_tgname,
                Anum_pg_trigger_tgrelid,
                object.objectId,
            )?
            else {
                if !missing_ok {
                    return Err(lookup_err(format!(
                        "could not find tuple for trigger {}",
                        object.objectId
                    )));
                }
                return Ok(None);
            };
            let rel = getRelationIdentity(mcx, tgrelid, false)?.expect("missing_ok=false");
            let identity = format!("{} on {}", quote_identifier(&tgname), rel.identity);
            let mut objname = rel.objname;
            objname.push(tgname);
            Ok(Some(ObjectIdentity { identity, objname, objargs: vec![] }))
        }
        x if x == statscmds::StatisticExtRelationId => {
            let Some((stxname, stxnamespace)) = name_on_relation_row(
                mcx,
                statscmds::StatisticExtRelationId,
                statscmds::StatisticExtOidIndexId,
                Anum_pg_statistic_ext_stxname,
                Anum_pg_statistic_ext_stxnamespace,
                object.objectId,
            )?
            else {
                if !missing_ok {
                    return Err(lookup_err(format!(
                        "cache lookup failed for statistics object {}",
                        object.objectId
                    )));
                }
                return Ok(None);
            };
            let schema = namespace_name_or_temp(mcx, stxnamespace)?;
            Ok(Some(ObjectIdentity {
                identity: quote_qualified(&schema, &stxname),
                objname: vec![schema, stxname],
                objargs: vec![],
            }))
        }
        crate::EventTriggerRelationId => {
            let Some(ht) = cache_syscache::SearchSysCache1(
                cache_syscache::EVENTTRIGGEROID,
                cache_syscache::SysCacheKey::Value(Datum::from_oid(object.objectId)),
            )?
            else {
                if !missing_ok {
                    return Err(lookup_err(format!(
                        "cache lookup failed for event trigger {}",
                        object.objectId
                    )));
                }
                return Ok(None);
            };
            let (d, _) = cache_syscache::SysCacheGetAttr(
                cache_syscache::EVENTTRIGGEROID,
                &ht,
                Anum_pg_event_trigger_evtname,
            )?;
            let evtname = name_at(d);
            cache_syscache::ReleaseSysCache(ht);
            Ok(Some(ObjectIdentity {
                identity: quote_identifier(&evtname).into_owned(),
                objname: vec![evtname],
                objargs: vec![],
            }))
        }
        types_core::PROCEDURE_RELATION_ID => {
            let Some(row) = proc_row(object.objectId)? else {
                if !missing_ok {
                    return Err(lookup_err(format!(
                        "cache lookup failed for procedure {}",
                        object.objectId
                    )));
                }
                return Ok(None);
            };
            let schema = namespace_name_or_temp(mcx, row.namespace)?;
            let mut args = String::new();
            let mut objargs = Vec::with_capacity(row.argtypes.len());
            for (i, &t) in row.argtypes.iter().enumerate() {
                let tn = format_type::format_type_be_qualified(t)?;
                if i > 0 {
                    args.push(',');
                }
                args.push_str(&tn);
                objargs.push(tn);
            }
            let identity = format!("{}({})", quote_qualified(&schema, &row.name), args);
            Ok(Some(ObjectIdentity {
                identity,
                objname: vec![schema, row.name],
                objargs,
            }))
        }
        other => panic!("unported: objectaddress.c getObjectIdentityParts class {other}"),
    }
}

struct ProcNaming {
    name: String,
    namespace: Oid,
    kind: i8,
    argtypes: Vec<Oid>,
}

fn proc_row(oid: Oid) -> PgResult<Option<ProcNaming>> {
    const Anum_pg_proc_proname: i32 = 2;
    const Anum_pg_proc_pronamespace: i32 = 3;
    const Anum_pg_proc_prokind: i32 = 10;
    const Anum_pg_proc_proargtypes: i32 = 20;
    let Some(ht) = cache_syscache::SearchSysCache1(
        cache_syscache::PROCOID,
        cache_syscache::SysCacheKey::Value(Datum::from_oid(oid)),
    )?
    else {
        return Ok(None);
    };
    let get = |anum: i32| cache_syscache::SysCacheGetAttr(cache_syscache::PROCOID, &ht, anum);
    let name = name_at(get(Anum_pg_proc_proname)?.0);
    let namespace = get(Anum_pg_proc_pronamespace)?.0.as_oid();
    let kind = get(Anum_pg_proc_prokind)?.0.as_i8();
    let (argd, argnull) = get(Anum_pg_proc_proargtypes)?;
    debug_assert!(!argnull);
    // oidvector image: 24B 1-D array header, then n 4-byte oids.
    let p = argd.as_usize() as *const u8;
    // SAFETY: NOT NULL pg_proc.proargtypes oidvector under its declared size.
    let argtypes = unsafe {
        let n = u32::from_ne_bytes(*(p.add(16) as *const [u8; 4])) as usize;
        (0..n)
            .map(|i| u32::from_ne_bytes(*(p.add(24 + i * 4) as *const [u8; 4])) as Oid)
            .collect::<Vec<Oid>>()
    };
    cache_syscache::ReleaseSysCache(ht);
    Ok(Some(ProcNaming { name, namespace, kind, argtypes }))
}

fn getProcedureTypeDescription(oid: Oid, missing_ok: bool) -> PgResult<String> {
    match proc_row(oid)? {
        Some(row) => Ok(match row.kind as u8 {
            b'a' => "aggregate".into(),
            b'p' => "procedure".into(),
            _ => "function".into(),
        }),
        None => {
            if !missing_ok {
                return Err(lookup_err(format!("cache lookup failed for procedure {oid}")));
            }
            Ok("routine".into())
        }
    }
}

fn namespace_name_or_temp<'mcx>(mcx: Mcx<'mcx>, nspid: Oid) -> PgResult<String> {
    // C tolerates a concurrently dropped namespace (NULL qualifier); loud here.
    Ok(lsyscache::misc::get_namespace_name_or_temp(mcx, nspid)?
        .unwrap_or_else(|| panic!("cache lookup failed for namespace {nspid}"))
        .as_str()
        .to_owned())
}

fn getRelationIdentity<'mcx>(
    mcx: Mcx<'mcx>,
    relid: Oid,
    missing_ok: bool,
) -> PgResult<Option<ObjectIdentity>> {
    let Some(relname) = lsyscache::relation::get_rel_name(mcx, relid)? else {
        if !missing_ok {
            return Err(cache_lookup_failed(relid));
        }
        return Ok(None);
    };
    let relname = relname.as_str().to_owned();
    let schema = namespace_name_or_temp(mcx, lsyscache::relation::get_rel_namespace(relid)?)?;
    Ok(Some(ObjectIdentity {
        identity: quote_qualified(&schema, &relname),
        objname: vec![schema, relname],
        objargs: vec![],
    }))
}
