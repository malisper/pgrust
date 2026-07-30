// getObjectTypeDescription / getObjectIdentityParts (objectaddress.c) for the
// sql_drop collection surface; classes outside the ported set are loud.
use crate::description::{getattr, name_from_datum, scan_one_row};
use crate::{
    AttrDefaultRelationId, ConstraintRelationId, EventTriggerRelationId, ObjectAddress,
    RewriteRelationId, TriggerRelationId,
};
use datum::Datum;
use format_type::quote_identifier;
use mcx::Mcx;
use types_core::primitive::OidIsValid;
use types_core::{
    AttrNumber, InvalidOid, Oid, CONSTRAINT_OID_INDEX_ID, CONSTRAINT_RELATION_ID,
    NAMESPACE_RELATION_ID, RELATION_RELATION_ID, TYPE_RELATION_ID,
};
use types_error::{PgError, PgResult};
use types_rel::{
    RELKIND_COMPOSITE_TYPE, RELKIND_FOREIGN_TABLE, RELKIND_INDEX, RELKIND_MATVIEW,
    RELKIND_PARTITIONED_INDEX, RELKIND_PARTITIONED_TABLE, RELKIND_RELATION, RELKIND_SEQUENCE,
    RELKIND_TOASTVALUE, RELKIND_VIEW,
};

const StatisticExtRelationId: Oid = 3381;
const StatisticExtOidIndexId: Oid = 3380;
use crate::{
    AccessMethodOperatorOidIndexId, AccessMethodOperatorRelationId,
    AccessMethodProcedureOidIndexId, AccessMethodProcedureRelationId, AuthMemOidIndexId,
    AuthMemRelationId, DefaultAclOidIndexId, DefaultAclRelationId, ParameterAclRelationId,
    TransformRelationId,
};
const PolicyOidIndexId: Oid = 3257;
const TriggerOidIndexId: Oid = 2702;
const RewriteOidIndexId: Oid = 2692;
const Anum_pg_rewrite_rulename: i32 = 2;
const Anum_pg_rewrite_ev_class: i32 = 3;
const Anum_pg_trigger_tgrelid: i32 = 2;
const Anum_pg_trigger_tgname: i32 = 4;
const Anum_pg_statistic_ext_stxname: i32 = 3;
const Anum_pg_statistic_ext_stxnamespace: i32 = 4;
const Anum_pg_constraint_conname: i32 = 2;
const Anum_pg_constraint_conrelid: i32 = 9;
const Anum_pg_constraint_contypid: i32 = 10;
const Anum_pg_event_trigger_evtname: i32 = 2;

pub struct ObjectIdentity {
    pub identity: String,
    pub objname: Vec<String>,
    pub objargs: Vec<String>,
}

#[track_caller]
#[cold]
#[inline(never)]
fn lookup_err(msg: String) -> Box<PgError> {
    Box::new(PgError::error(msg))
}

#[track_caller]
#[cold]
#[inline(never)]
fn cache_lookup_failed(relid: Oid) -> Box<PgError> {
    Box::new(PgError::error(format!("cache lookup failed for relation {relid}")))
}

fn quote_qualified(schema: &str, name: &str) -> String {
    format!("{}.{}", quote_identifier(schema), quote_identifier(name))
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
        ConstraintRelationId => getConstraintTypeDescription(mcx, object.objectId, missing_ok)?,
        AttrDefaultRelationId => "default value".into(),
        RewriteRelationId => "rule".into(),
        TriggerRelationId => "trigger".into(),
        NAMESPACE_RELATION_ID => "schema".into(),
        StatisticExtRelationId => "statistics object".into(),
        EventTriggerRelationId => "event trigger".into(),
        types_core::PROCEDURE_RELATION_ID => {
            getProcedureTypeDescription(object.objectId, missing_ok)?
        }
        crate::CastRelationId => "cast".into(),
        crate::CollationRelationId => "collation".into(),
        pg_conversion::ConversionRelationId => "conversion".into(),
        proclang::LanguageRelationId => "language".into(),
        crate::LargeObjectRelationId => "large object".into(),
        types_core::OPERATOR_RELATION_ID => "operator".into(),
        types_core::OPERATOR_CLASS_RELATION_ID => "operator class".into(),
        types_core::OPERATOR_FAMILY_RELATION_ID => "operator family".into(),
        crate::AccessMethodRelationId => "access method".into(),
        AccessMethodOperatorRelationId => "operator of access method".into(),
        AccessMethodProcedureRelationId => "function of access method".into(),
        types_core::AUTH_ID_RELATION_ID => "role".into(),
        AuthMemRelationId => "role membership".into(),
        types_core::DATABASE_RELATION_ID => "database".into(),
        types_core::TABLE_SPACE_RELATION_ID => "tablespace".into(),
        types_core::FOREIGN_DATA_WRAPPER_RELATION_ID => "foreign-data wrapper".into(),
        types_core::FOREIGN_SERVER_RELATION_ID => "server".into(),
        types_core::USER_MAPPING_RELATION_ID => "user mapping".into(),
        DefaultAclRelationId => "default acl".into(),
        types_core::EXTENSION_RELATION_ID => "extension".into(),
        ParameterAclRelationId => "parameter ACL".into(),
        crate::PolicyRelationId => "policy".into(),
        crate::PublicationRelationId => "publication".into(),
        crate::PublicationNamespaceRelationId => "publication namespace".into(),
        crate::PublicationRelRelationId => "publication relation".into(),
        crate::SubscriptionRelationId => "subscription".into(),
        TransformRelationId => "transform".into(),
        crate::TSParserRelationId => "text search parser".into(),
        crate::TSDictionaryRelationId => "text search dictionary".into(),
        crate::TSTemplateRelationId => "text search template".into(),
        crate::TSConfigRelationId => "text search configuration".into(),
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

fn constraint_row<'mcx>(mcx: Mcx<'mcx>, constroid: Oid) -> PgResult<Option<(String, Oid, Oid)>> {
    scan_one_row(mcx, CONSTRAINT_RELATION_ID, CONSTRAINT_OID_INDEX_ID, constroid, |tup, desc| {
        (
            name_from_datum(getattr(tup, Anum_pg_constraint_conname, desc)),
            getattr(tup, Anum_pg_constraint_conrelid, desc).as_oid(),
            getattr(tup, Anum_pg_constraint_contypid, desc).as_oid(),
        )
    })
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
            let Some(nspname) = lsyscache::misc::get_namespace_name_or_temp(mcx, object.objectId)?
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
        ConstraintRelationId => {
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
                let t = getObjectIdentityParts(mcx, &domain, false)?.expect("missing_ok=false");
                Ok(Some(ObjectIdentity {
                    identity: format!("{} on {}", quote_identifier(&conname), t.identity),
                    objname: t.objname,
                    objargs: vec![conname],
                }))
            }
        }
        AttrDefaultRelationId => {
            let (adrelid, adnum) = pg_attrdef::GetAttrDefaultColumnAddress(mcx, object.objectId)?;
            if adrelid == InvalidOid {
                if !missing_ok {
                    return Err(lookup_err(format!(
                        "could not find tuple for attrdef {}",
                        object.objectId
                    )));
                }
                return Ok(None);
            }
            let colobject = ObjectAddress::sub_set(RELATION_RELATION_ID, adrelid, adnum as i32);
            let col = getObjectIdentityParts(mcx, &colobject, false)?.expect("missing_ok=false");
            Ok(Some(ObjectIdentity {
                identity: format!("for {}", col.identity),
                objname: col.objname,
                objargs: col.objargs,
            }))
        }
        RewriteRelationId => {
            let row = scan_one_row(mcx, RewriteRelationId, RewriteOidIndexId, object.objectId, |tup, desc| {
                (
                    name_from_datum(getattr(tup, Anum_pg_rewrite_rulename, desc)),
                    getattr(tup, Anum_pg_rewrite_ev_class, desc).as_oid(),
                )
            })?;
            let Some((rulename, ev_class)) = row else {
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
            let row = scan_one_row(mcx, TriggerRelationId, TriggerOidIndexId, object.objectId, |tup, desc| {
                (
                    name_from_datum(getattr(tup, Anum_pg_trigger_tgname, desc)),
                    getattr(tup, Anum_pg_trigger_tgrelid, desc).as_oid(),
                )
            })?;
            let Some((tgname, tgrelid)) = row else {
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
        StatisticExtRelationId => {
            let row = scan_one_row(
                mcx,
                StatisticExtRelationId,
                StatisticExtOidIndexId,
                object.objectId,
                |tup, desc| {
                    (
                        name_from_datum(getattr(tup, Anum_pg_statistic_ext_stxname, desc)),
                        getattr(tup, Anum_pg_statistic_ext_stxnamespace, desc).as_oid(),
                    )
                },
            )?;
            let Some((stxname, stxnamespace)) = row else {
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
        EventTriggerRelationId => {
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
            let evtname = name_from_datum(d);
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
            Ok(Some(ObjectIdentity { identity, objname: vec![schema, row.name], objargs }))
        }
        crate::CastRelationId => {
            let row = crate::description::scan_one_row(
                mcx,
                crate::CastRelationId,
                2660,
                object.objectId,
                |tup, desc| {
                    (
                        crate::description::getattr(tup, 2, desc).as_oid(),
                        crate::description::getattr(tup, 3, desc).as_oid(),
                    )
                },
            )?;
            let Some((castsource, casttarget)) = row else {
                if !missing_ok {
                    return Err(lookup_err(format!(
                        "could not find tuple for cast {}",
                        object.objectId
                    )));
                }
                return Ok(None);
            };
            let src = format_type::format_type_be_qualified(castsource)?;
            let tgt = format_type::format_type_be_qualified(casttarget)?;
            Ok(Some(ObjectIdentity {
                identity: format!("({src} AS {tgt})"),
                objname: vec![src],
                objargs: vec![tgt],
            }))
        }
        crate::CollationRelationId => {
            named_nsp_identity(mcx, object, missing_ok, cache_syscache::cacheinfo::COLLOID, 2, 3, "collation")
        }
        pg_conversion::ConversionRelationId => {
            named_nsp_identity(mcx, object, missing_ok, cache_syscache::cacheinfo::CONVOID, 2, 3, "conversion")
        }
        proclang::LanguageRelationId => {
            let Some(lanname) =
                syscache_name_att(cache_syscache::cacheinfo::LANGOID, object.objectId, 2)?
            else {
                if !missing_ok {
                    return Err(lookup_err(format!(
                        "cache lookup failed for language {}",
                        object.objectId
                    )));
                }
                return Ok(None);
            };
            Ok(Some(ObjectIdentity {
                identity: quote_identifier(&lanname).into_owned(),
                objname: vec![lanname],
                objargs: vec![],
            }))
        }
        crate::LargeObjectRelationId => {
            if !pg_largeobject::LargeObjectExists(mcx, object.objectId)? {
                return Ok(None);
            }
            let s = object.objectId.to_string();
            Ok(Some(ObjectIdentity { identity: s.clone(), objname: vec![s], objargs: vec![] }))
        }
        types_core::OPERATOR_RELATION_ID => {
            // FORMAT_OPERATOR_FORCE_QUALIFY | FORMAT_OPERATOR_INVALID_AS_NULL.
            let Some(op) = operator_row(object.objectId)? else {
                if !missing_ok {
                    return Err(lookup_err(format!(
                        "cache lookup failed for operator {}",
                        object.objectId
                    )));
                }
                return Ok(None);
            };
            let schema = namespace_name_or_temp(mcx, op.namespace)?;
            let mut identity = format!("{}.{}(", quote_identifier(&schema), op.name);
            let mut objargs = Vec::with_capacity(2);
            if op.left != InvalidOid {
                let t = format_type::format_type_be_qualified(op.left)?;
                identity.push_str(&t);
                objargs.push(t);
            } else {
                identity.push_str("NONE");
            }
            identity.push(',');
            if op.right != InvalidOid {
                let t = format_type::format_type_be_qualified(op.right)?;
                identity.push_str(&t);
                objargs.push(t);
            } else {
                identity.push_str("NONE");
            }
            identity.push(')');
            Ok(Some(ObjectIdentity { identity, objname: vec![schema, op.name], objargs }))
        }
        types_core::OPERATOR_CLASS_RELATION_ID => {
            let Some((opcmethod, opcname, opcnamespace)) =
                crate::description::opclass_or_opfamily_row(
                    cache_syscache::cacheinfo::CLAOID,
                    object.objectId,
                )?
            else {
                if !missing_ok {
                    return Err(lookup_err(format!(
                        "cache lookup failed for opclass {}",
                        object.objectId
                    )));
                }
                return Ok(None);
            };
            let amname = crate::description::am_name(opcmethod)?;
            let schema = namespace_name_or_temp(mcx, opcnamespace)?;
            Ok(Some(ObjectIdentity {
                identity: format!(
                    "{} USING {}",
                    quote_qualified(&schema, &opcname),
                    quote_identifier(&amname)
                ),
                objname: vec![amname, schema, opcname],
                objargs: vec![],
            }))
        }
        types_core::OPERATOR_FAMILY_RELATION_ID => {
            getOpFamilyIdentity(mcx, object.objectId, missing_ok)
        }
        crate::AccessMethodRelationId => {
            let Some(amname) =
                syscache_name_att(cache_syscache::cacheinfo::AMOID, object.objectId, 2)?
            else {
                if !missing_ok {
                    return Err(lookup_err(format!(
                        "cache lookup failed for access method {}",
                        object.objectId
                    )));
                }
                return Ok(None);
            };
            Ok(Some(ObjectIdentity {
                identity: quote_identifier(&amname).into_owned(),
                objname: vec![amname],
                objargs: vec![],
            }))
        }
        AccessMethodOperatorRelationId => {
            let row = crate::description::scan_one_row(
                mcx,
                AccessMethodOperatorRelationId,
                AccessMethodOperatorOidIndexId,
                object.objectId,
                |tup, desc| {
                    (
                        crate::description::getattr(tup, 2, desc).as_oid(),
                        crate::description::getattr(tup, 3, desc).as_oid(),
                        crate::description::getattr(tup, 4, desc).as_oid(),
                        crate::description::getattr(tup, 5, desc).as_i16(),
                    )
                },
            )?;
            let Some((amopfamily, amoplefttype, amoprighttype, amopstrategy)) = row else {
                if !missing_ok {
                    return Err(lookup_err(format!(
                        "could not find tuple for amop entry {}",
                        object.objectId
                    )));
                }
                return Ok(None);
            };
            let fam = getOpFamilyIdentity(mcx, amopfamily, false)?.expect("missing_ok=false");
            let ltype = format_type::format_type_be_qualified(amoplefttype)?;
            let rtype = format_type::format_type_be_qualified(amoprighttype)?;
            let mut objname = fam.objname;
            objname.push(amopstrategy.to_string());
            Ok(Some(ObjectIdentity {
                identity: format!(
                    "operator {amopstrategy} ({ltype}, {rtype}) of {}",
                    fam.identity
                ),
                objname,
                objargs: vec![ltype, rtype],
            }))
        }
        AccessMethodProcedureRelationId => {
            let row = crate::description::scan_one_row(
                mcx,
                AccessMethodProcedureRelationId,
                AccessMethodProcedureOidIndexId,
                object.objectId,
                |tup, desc| {
                    (
                        crate::description::getattr(tup, 2, desc).as_oid(),
                        crate::description::getattr(tup, 3, desc).as_oid(),
                        crate::description::getattr(tup, 4, desc).as_oid(),
                        crate::description::getattr(tup, 5, desc).as_i16(),
                    )
                },
            )?;
            let Some((amprocfamily, amproclefttype, amprocrighttype, amprocnum)) = row else {
                if !missing_ok {
                    return Err(lookup_err(format!(
                        "could not find tuple for amproc entry {}",
                        object.objectId
                    )));
                }
                return Ok(None);
            };
            let fam = getOpFamilyIdentity(mcx, amprocfamily, false)?.expect("missing_ok=false");
            let ltype = format_type::format_type_be_qualified(amproclefttype)?;
            let rtype = format_type::format_type_be_qualified(amprocrighttype)?;
            let mut objname = fam.objname;
            objname.push(amprocnum.to_string());
            Ok(Some(ObjectIdentity {
                identity: format!(
                    "function {amprocnum} ({ltype}, {rtype}) of {}",
                    fam.identity
                ),
                objname,
                objargs: vec![ltype, rtype],
            }))
        }
        types_core::AUTH_ID_RELATION_ID => {
            let Some(username) = miscinit::GetUserNameFromId(mcx, object.objectId, missing_ok)?
            else {
                return Ok(None);
            };
            let username = username.as_str().to_owned();
            Ok(Some(ObjectIdentity {
                identity: quote_identifier(&username).into_owned(),
                objname: vec![username],
                objargs: vec![],
            }))
        }
        AuthMemRelationId => {
            let row = crate::description::scan_one_row(
                mcx,
                AuthMemRelationId,
                AuthMemOidIndexId,
                object.objectId,
                |tup, desc| {
                    (
                        crate::description::getattr(tup, 2, desc).as_oid(),
                        crate::description::getattr(tup, 3, desc).as_oid(),
                    )
                },
            )?;
            let Some((roleid, member)) = row else {
                if !missing_ok {
                    return Err(lookup_err(format!(
                        "could not find tuple for pg_auth_members entry {}",
                        object.objectId
                    )));
                }
                return Ok(None);
            };
            let member = miscinit::GetUserNameFromId(mcx, member, false)?
                .expect("noerr=false")
                .as_str()
                .to_owned();
            let role = miscinit::GetUserNameFromId(mcx, roleid, false)?
                .expect("noerr=false")
                .as_str()
                .to_owned();
            // C provides no objname for role memberships; identity only.
            Ok(Some(ObjectIdentity {
                identity: format!("membership of role {member} in role {role}"),
                objname: vec![],
                objargs: vec![],
            }))
        }
        types_core::DATABASE_RELATION_ID => {
            let Some(datname) = dbcommands_seams::get_database_name::call(object.objectId)? else {
                if !missing_ok {
                    return Err(lookup_err(format!(
                        "cache lookup failed for database {}",
                        object.objectId
                    )));
                }
                return Ok(None);
            };
            Ok(Some(ObjectIdentity {
                identity: quote_identifier(&datname).into_owned(),
                objname: vec![datname],
                objargs: vec![],
            }))
        }
        types_core::TABLE_SPACE_RELATION_ID => {
            let Some(tblspc) = commands_tablespace::get_tablespace_name(mcx, object.objectId)?
            else {
                if !missing_ok {
                    return Err(lookup_err(format!(
                        "cache lookup failed for tablespace {}",
                        object.objectId
                    )));
                }
                return Ok(None);
            };
            let tblspc = core::str::from_utf8(tblspc.name_str())
                .expect("catalog names are valid UTF-8")
                .to_owned();
            Ok(Some(ObjectIdentity {
                identity: quote_identifier(&tblspc).into_owned(),
                objname: vec![tblspc],
                objargs: vec![],
            }))
        }
        types_core::FOREIGN_DATA_WRAPPER_RELATION_ID => {
            let Some(fdwname) = crate::description::foreign_object_name(
                cache_syscache::cacheinfo::FOREIGNDATAWRAPPEROID,
                object.objectId,
            )?
            else {
                if !missing_ok {
                    return Err(lookup_err(format!(
                        "foreign-data wrapper with OID {} does not exist",
                        object.objectId
                    )));
                }
                return Ok(None);
            };
            Ok(Some(ObjectIdentity {
                identity: quote_identifier(&fdwname).into_owned(),
                objname: vec![fdwname],
                objargs: vec![],
            }))
        }
        types_core::FOREIGN_SERVER_RELATION_ID => {
            let Some(srvname) = crate::description::foreign_object_name(
                cache_syscache::cacheinfo::FOREIGNSERVEROID,
                object.objectId,
            )?
            else {
                if !missing_ok {
                    return Err(lookup_err(format!(
                        "foreign server with OID {} does not exist",
                        object.objectId
                    )));
                }
                return Ok(None);
            };
            Ok(Some(ObjectIdentity {
                identity: quote_identifier(&srvname).into_owned(),
                objname: vec![srvname],
                objargs: vec![],
            }))
        }
        types_core::USER_MAPPING_RELATION_ID => {
            let cacheid = cache_syscache::cacheinfo::USERMAPPINGOID;
            let Some(tup) = cache_syscache::SearchSysCache1(
                cacheid,
                cache_syscache::SysCacheKey::Value(Datum::from_oid(object.objectId)),
            )?
            else {
                if !missing_ok {
                    return Err(lookup_err(format!(
                        "cache lookup failed for user mapping {}",
                        object.objectId
                    )));
                }
                return Ok(None);
            };
            let useid = cache_syscache::SysCacheGetAttrNotNull(cacheid, &tup, 2)?.as_oid();
            let serverid = cache_syscache::SysCacheGetAttrNotNull(cacheid, &tup, 3)?.as_oid();
            cache_syscache::ReleaseSysCache(tup);
            let usename = if useid != InvalidOid {
                miscinit::GetUserNameFromId(mcx, useid, false)?
                    .expect("noerr=false")
                    .as_str()
                    .to_owned()
            } else {
                "public".to_string()
            };
            let srvname = crate::description::foreign_object_name(
                cache_syscache::cacheinfo::FOREIGNSERVEROID,
                serverid,
            )?
            .unwrap_or_else(|| panic!("cache lookup failed for foreign server {serverid}"));
            Ok(Some(ObjectIdentity {
                identity: format!("{} on server {srvname}", quote_identifier(&usename)),
                objname: vec![usename],
                objargs: vec![srvname],
            }))
        }
        DefaultAclRelationId => {
            let row = crate::description::scan_one_row(
                mcx,
                DefaultAclRelationId,
                DefaultAclOidIndexId,
                object.objectId,
                |tup, desc| {
                    (
                        crate::description::getattr(tup, 2, desc).as_oid(),
                        crate::description::getattr(tup, 3, desc).as_oid(),
                        crate::description::getattr(tup, 4, desc).as_i8() as u8,
                    )
                },
            )?;
            let Some((defaclrole, defaclnamespace, defaclobjtype)) = row else {
                if !missing_ok {
                    return Err(lookup_err(format!(
                        "could not find tuple for default ACL {}",
                        object.objectId
                    )));
                }
                return Ok(None);
            };
            let username = miscinit::GetUserNameFromId(mcx, defaclrole, false)?
                .expect("noerr=false")
                .as_str()
                .to_owned();
            let mut identity = format!("for role {}", quote_identifier(&username));
            let mut objname = vec![username];
            if OidIsValid(defaclnamespace) {
                let schema = namespace_name_or_temp(mcx, defaclnamespace)?;
                identity.push_str(&format!(" in schema {}", quote_identifier(&schema)));
                objname.push(schema);
            }
            identity.push_str(match defaclobjtype {
                b'r' => " on tables",
                b'S' => " on sequences",
                b'f' => " on functions",
                b'T' => " on types",
                b'n' => " on schemas",
                b'L' => " on large objects",
                _ => "",
            });
            Ok(Some(ObjectIdentity {
                identity,
                objname,
                objargs: vec![(defaclobjtype as char).to_string()],
            }))
        }
        types_core::EXTENSION_RELATION_ID => {
            let Some(extname) = extension::get_extension_name(mcx, object.objectId)? else {
                if !missing_ok {
                    return Err(lookup_err(format!(
                        "cache lookup failed for extension {}",
                        object.objectId
                    )));
                }
                return Ok(None);
            };
            let extname = extname.as_str().to_owned();
            Ok(Some(ObjectIdentity {
                identity: quote_identifier(&extname).into_owned(),
                objname: vec![extname],
                objargs: vec![],
            }))
        }
        ParameterAclRelationId => {
            let cacheid = cache_syscache::cacheinfo::PARAMETERACLOID;
            let Some(tup) = cache_syscache::SearchSysCache1(
                cacheid,
                cache_syscache::SysCacheKey::Value(Datum::from_oid(object.objectId)),
            )?
            else {
                if !missing_ok {
                    return Err(lookup_err(format!(
                        "cache lookup failed for parameter ACL {}",
                        object.objectId
                    )));
                }
                return Ok(None);
            };
            let d = cache_syscache::SysCacheGetAttrNotNull(cacheid, &tup, 2)?;
            let parname = text_datum_to_string(d);
            cache_syscache::ReleaseSysCache(tup);
            // C: parname is appended unquoted.
            Ok(Some(ObjectIdentity {
                identity: parname.clone(),
                objname: vec![parname],
                objargs: vec![],
            }))
        }
        crate::PolicyRelationId => {
            let row = crate::description::scan_one_row(
                mcx,
                crate::PolicyRelationId,
                PolicyOidIndexId,
                object.objectId,
                |tup, desc| {
                    (
                        name_from_datum(crate::description::getattr(tup, 2, desc)),
                        crate::description::getattr(tup, 3, desc).as_oid(),
                    )
                },
            )?;
            let Some((polname, polrelid)) = row else {
                if !missing_ok {
                    return Err(lookup_err(format!(
                        "could not find tuple for policy {}",
                        object.objectId
                    )));
                }
                return Ok(None);
            };
            let rel = getRelationIdentity(mcx, polrelid, false)?.expect("missing_ok=false");
            let identity = format!("{} on {}", quote_identifier(&polname), rel.identity);
            let mut objname = rel.objname;
            objname.push(polname);
            Ok(Some(ObjectIdentity { identity, objname, objargs: vec![] }))
        }
        crate::PublicationRelationId => {
            let Some(pubname) =
                lsyscache::get_publication_name(mcx, object.objectId, missing_ok)?
            else {
                return Ok(None);
            };
            let pubname = pubname.as_str().to_owned();
            Ok(Some(ObjectIdentity {
                identity: quote_identifier(&pubname).into_owned(),
                objname: vec![pubname],
                objargs: vec![],
            }))
        }
        crate::PublicationNamespaceRelationId => {
            let Some((pubname, nspname)) =
                crate::description::getPublicationSchemaInfo(mcx, object.objectId, missing_ok)?
            else {
                return Ok(None);
            };
            Ok(Some(ObjectIdentity {
                identity: format!("{nspname} in publication {pubname}"),
                objname: vec![nspname],
                objargs: vec![pubname],
            }))
        }
        crate::PublicationRelRelationId => {
            let cacheid = cache_syscache::cacheinfo::PUBLICATIONREL;
            let Some(tup) = cache_syscache::SearchSysCache1(
                cacheid,
                cache_syscache::SysCacheKey::Value(Datum::from_oid(object.objectId)),
            )?
            else {
                if !missing_ok {
                    return Err(lookup_err(format!(
                        "cache lookup failed for publication table {}",
                        object.objectId
                    )));
                }
                return Ok(None);
            };
            let prpubid = cache_syscache::SysCacheGetAttrNotNull(cacheid, &tup, 2)?.as_oid();
            let prrelid = cache_syscache::SysCacheGetAttrNotNull(cacheid, &tup, 3)?.as_oid();
            cache_syscache::ReleaseSysCache(tup);
            let pubname = lsyscache::get_publication_name(mcx, prpubid, false)?
                .expect("missing_ok=false")
                .as_str()
                .to_owned();
            let rel = getRelationIdentity(mcx, prrelid, false)?.expect("missing_ok=false");
            Ok(Some(ObjectIdentity {
                identity: format!("{} in publication {pubname}", rel.identity),
                objname: rel.objname,
                objargs: vec![pubname],
            }))
        }
        crate::SubscriptionRelationId => {
            let Some(subname) =
                lsyscache::get_subscription_name(mcx, object.objectId, missing_ok)?
            else {
                return Ok(None);
            };
            let subname = subname.as_str().to_owned();
            Ok(Some(ObjectIdentity {
                identity: quote_identifier(&subname).into_owned(),
                objname: vec![subname],
                objargs: vec![],
            }))
        }
        TransformRelationId => {
            let cacheid = cache_syscache::cacheinfo::TRFOID;
            let Some(tup) = cache_syscache::SearchSysCache1(
                cacheid,
                cache_syscache::SysCacheKey::Value(Datum::from_oid(object.objectId)),
            )?
            else {
                if !missing_ok {
                    return Err(lookup_err(format!(
                        "could not find tuple for transform {}",
                        object.objectId
                    )));
                }
                return Ok(None);
            };
            let trftype = cache_syscache::SysCacheGetAttrNotNull(cacheid, &tup, 2)?.as_oid();
            let trflang = cache_syscache::SysCacheGetAttrNotNull(cacheid, &tup, 3)?.as_oid();
            cache_syscache::ReleaseSysCache(tup);
            let transform_type = format_type::format_type_be_qualified(trftype)?;
            let transform_lang =
                syscache_name_att(cache_syscache::cacheinfo::LANGOID, trflang, 2)?
                    .unwrap_or_else(|| panic!("cache lookup failed for language {trflang}"));
            Ok(Some(ObjectIdentity {
                identity: format!("for {transform_type} language {transform_lang}"),
                objname: vec![transform_type],
                objargs: vec![transform_lang],
            }))
        }
        crate::TSParserRelationId => {
            named_nsp_identity(mcx, object, missing_ok, cache_syscache::cacheinfo::TSPARSEROID, 2, 3, "text search parser")
        }
        crate::TSDictionaryRelationId => {
            named_nsp_identity(mcx, object, missing_ok, cache_syscache::cacheinfo::TSDICTOID, 2, 3, "text search dictionary")
        }
        crate::TSTemplateRelationId => {
            named_nsp_identity(mcx, object, missing_ok, cache_syscache::cacheinfo::TSTEMPLATEOID, 2, 3, "text search template")
        }
        crate::TSConfigRelationId => {
            named_nsp_identity(mcx, object, missing_ok, cache_syscache::cacheinfo::TSCONFIGOID, 2, 3, "text search configuration")
        }
        other => panic!("unported: objectaddress.c getObjectIdentityParts class {other}"),
    }
}

// Shared "schema-qualified name" identity for name+namespace syscache
// catalogs (collation, conversion, TS objects).
fn named_nsp_identity<'mcx>(
    mcx: Mcx<'mcx>,
    object: &ObjectAddress,
    missing_ok: bool,
    cacheid: i32,
    name_attnum: i32,
    nsp_attnum: i32,
    noun: &str,
) -> PgResult<Option<ObjectIdentity>> {
    let Some(tup) = cache_syscache::SearchSysCache1(
        cacheid,
        cache_syscache::SysCacheKey::Value(Datum::from_oid(object.objectId)),
    )?
    else {
        if !missing_ok {
            return Err(lookup_err(format!(
                "cache lookup failed for {noun} {}",
                object.objectId
            )));
        }
        return Ok(None);
    };
    let name =
        name_from_datum(cache_syscache::SysCacheGetAttrNotNull(cacheid, &tup, name_attnum)?);
    let nsp = cache_syscache::SysCacheGetAttrNotNull(cacheid, &tup, nsp_attnum)?.as_oid();
    cache_syscache::ReleaseSysCache(tup);
    let schema = namespace_name_or_temp(mcx, nsp)?;
    Ok(Some(ObjectIdentity {
        identity: quote_qualified(&schema, &name),
        objname: vec![schema, name],
        objargs: vec![],
    }))
}

// getOpFamilyIdentity (objectaddress.c): amname is NOT quoted in the
// identity string, per C.
fn getOpFamilyIdentity<'mcx>(
    mcx: Mcx<'mcx>,
    opfid: Oid,
    missing_ok: bool,
) -> PgResult<Option<ObjectIdentity>> {
    let Some((opfmethod, opfname, opfnamespace)) = crate::description::opclass_or_opfamily_row(
        cache_syscache::cacheinfo::OPFAMILYOID,
        opfid,
    )?
    else {
        if !missing_ok {
            return Err(lookup_err(format!("cache lookup failed for opfamily {opfid}")));
        }
        return Ok(None);
    };
    let amname = crate::description::am_name(opfmethod)?;
    let schema = namespace_name_or_temp(mcx, opfnamespace)?;
    Ok(Some(ObjectIdentity {
        identity: format!("{} USING {amname}", quote_qualified(&schema, &opfname)),
        objname: vec![amname, schema, opfname],
        objargs: vec![],
    }))
}

struct OperatorRow {
    name: String,
    namespace: Oid,
    left: Oid,
    right: Oid,
}

fn operator_row(oprid: Oid) -> PgResult<Option<OperatorRow>> {
    let cacheid = cache_syscache::cacheinfo::OPEROID;
    let Some(tup) = cache_syscache::SearchSysCache1(
        cacheid,
        cache_syscache::SysCacheKey::Value(Datum::from_oid(oprid)),
    )?
    else {
        return Ok(None);
    };
    let name = name_from_datum(cache_syscache::SysCacheGetAttrNotNull(cacheid, &tup, 2)?);
    let namespace = cache_syscache::SysCacheGetAttrNotNull(cacheid, &tup, 3)?.as_oid();
    let left = cache_syscache::SysCacheGetAttrNotNull(cacheid, &tup, 8)?.as_oid();
    let right = cache_syscache::SysCacheGetAttrNotNull(cacheid, &tup, 9)?.as_oid();
    cache_syscache::ReleaseSysCache(tup);
    Ok(Some(OperatorRow { name, namespace, left, right }))
}

pub(crate) fn language_name(langid: Oid) -> PgResult<Option<String>> {
    syscache_name_att(cache_syscache::cacheinfo::LANGOID, langid, 2)
}

fn syscache_name_att(cacheid: i32, oid: Oid, attnum: i32) -> PgResult<Option<String>> {
    let Some(tup) = cache_syscache::SearchSysCache1(
        cacheid,
        cache_syscache::SysCacheKey::Value(Datum::from_oid(oid)),
    )?
    else {
        return Ok(None);
    };
    let name = name_from_datum(cache_syscache::SysCacheGetAttrNotNull(cacheid, &tup, attnum)?);
    cache_syscache::ReleaseSysCache(tup);
    Ok(Some(name))
}

pub(crate) fn text_datum_to_string(d: Datum) -> String {
    let p = d.as_usize() as *const u8;
    // SAFETY: non-null detoasted text column datum from the syscache tuple.
    unsafe {
        let (off, len) = if types_tuple::varatt::varatt_is_1b(p) {
            (
                types_tuple::varatt::VARHDRSZ_SHORT,
                types_tuple::varatt::varsize_1b(p) - types_tuple::varatt::VARHDRSZ_SHORT,
            )
        } else {
            (
                types_tuple::varatt::VARHDRSZ,
                types_tuple::varatt::varsize_4b(p) - types_tuple::varatt::VARHDRSZ,
            )
        };
        core::str::from_utf8(core::slice::from_raw_parts(p.add(off), len))
            .expect("catalog text is valid UTF-8")
            .to_string()
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
    let name = name_from_datum(get(Anum_pg_proc_proname)?.0);
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
