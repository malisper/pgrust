// getObjectDescription / getObjectIdentity arms for the live object classes;
// all other classes are named panics.
use crate::{
    unported, AttrDefaultRelationId, CastRelationId, ConstraintRelationId, ObjectAddress,
    PolicyRelationId,
    ProcedureRelationId, PublicationNamespaceRelationId, PublicationRelRelationId,
    PublicationRelationId, RewriteRelationId, SubscriptionRelationId, TriggerRelationId,
};
use datum::Datum;
use format_type::quote_identifier;
use mcx::Mcx;
use types_core::primitive::OidIsValid;
use types_core::{
    AttrNumber, Oid, AUTH_ID_RELATION_ID, DATABASE_RELATION_ID, EXTENSION_RELATION_ID,
    NAMESPACE_RELATION_ID, OPERATOR_CLASS_RELATION_ID, OPERATOR_FAMILY_RELATION_ID,
    OPERATOR_RELATION_ID, RELATION_RELATION_ID, TYPE_RELATION_ID,
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

pub(crate) fn name_from_datum(d: Datum) -> String {
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
    let nspname = if !force_qualify && catalog_namespace::FunctionIsVisible(procid)? {
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

pub(crate) fn scan_one_row<'mcx, T>(
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

pub(crate) fn getattr(tup: &types_tuple::HeapTupleData<'_>, attnum: i32, desc: &types_tuple::TupleDescData<'_>) -> Datum {
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
        CastRelationId => {
            let row = scan_one_row(mcx, CastRelationId, 2660, object.objectId, |tup, desc| {
                (getattr(tup, 2, desc).as_oid(), getattr(tup, 3, desc).as_oid())
            })?;
            let Some((castsource, casttarget)) = row else {
                if !missing_ok {
                    panic!("could not find tuple for cast {}", object.objectId);
                }
                return Ok(None);
            };
            Ok(Some(format!(
                "cast from {} to {}",
                format_type::format_type_be(castsource)?,
                format_type::format_type_be(casttarget)?
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
        PolicyRelationId => {
            let row = scan_one_row(mcx, PolicyRelationId, 3257, object.objectId, |tup, desc| {
                (name_from_datum(getattr(tup, 2, desc)), getattr(tup, 3, desc).as_oid())
            })?;
            let Some((polname, polrelid)) = row else {
                if !missing_ok {
                    panic!("could not find tuple for policy {}", object.objectId);
                }
                return Ok(None);
            };
            let rel = getRelationDescription(polrelid, false)?.expect("policy's relation exists");
            Ok(Some(format!("policy {polname} on {rel}")))
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
        EXTENSION_RELATION_ID => {
            let Some(extname) = extension::get_extension_name(mcx, object.objectId)? else {
                if !missing_ok {
                    panic!("cache lookup failed for extension {}", object.objectId);
                }
                return Ok(None);
            };
            Ok(Some(format!("extension {}", extname.as_str())))
        }
        OPERATOR_RELATION_ID => {
            // FORMAT_OPERATOR_INVALID_AS_NULL.
            if syscache_seams::pg_operator_oprname::call(object.objectId)?.is_none() {
                if !missing_ok {
                    panic!("cache lookup failed for operator {}", object.objectId);
                }
                return Ok(None);
            }
            Ok(Some(format!(
                "operator {}",
                adt_regproc::format_operator(mcx, object.objectId)?
            )))
        }
        crate::CollationRelationId => {
            named_nsp_class_description(
                mcx, object, missing_ok,
                crate::CollationRelationId, 3085, 2, 3, "collation",
                |name| catalog_namespace::get_collation_oid(&[name], true),
            )
        }
        StatisticExtRelationId_descr => {
            named_nsp_class_description(
                mcx, object, missing_ok,
                StatisticExtRelationId_descr, 3380, 3, 4, "statistics object",
                |name| statscmds::get_statistics_object_oid(&[name], true),
            )
        }
        crate::TSParserRelationId => {
            named_nsp_class_description(
                mcx, object, missing_ok,
                crate::TSParserRelationId, 3607, 2, 3, "text search parser",
                |name| catalog_namespace::get_ts_parser_oid(&[name], true),
            )
        }
        crate::TSDictionaryRelationId => {
            named_nsp_class_description(
                mcx, object, missing_ok,
                crate::TSDictionaryRelationId, 3605, 2, 3, "text search dictionary",
                |name| catalog_namespace::get_ts_dict_oid(&[name], true),
            )
        }
        crate::TSTemplateRelationId => {
            named_nsp_class_description(
                mcx, object, missing_ok,
                crate::TSTemplateRelationId, 3767, 2, 3, "text search template",
                |name| catalog_namespace::get_ts_template_oid(&[name], true),
            )
        }
        crate::TSConfigRelationId => {
            named_nsp_class_description(
                mcx, object, missing_ok,
                crate::TSConfigRelationId, 3712, 2, 3, "text search configuration",
                |name| catalog_namespace::get_ts_config_oid(&[name], true),
            )
        }
        pg_conversion::ConversionRelationId => {
            let row = scan_one_row(
                mcx,
                pg_conversion::ConversionRelationId,
                pg_conversion::ConversionOidIndexId,
                object.objectId,
                |tup, desc| {
                    (name_from_datum(getattr(tup, 2, desc)), getattr(tup, 3, desc).as_oid())
                },
            )?;
            let Some((conname, connamespace)) = row else {
                if !missing_ok {
                    panic!("cache lookup failed for conversion {}", object.objectId);
                }
                return Ok(None);
            };
            let nspname = if catalog_namespace::get_conversion_oid(&[&conname], true)?
                == object.objectId
            {
                None
            } else {
                get_namespace_name(connamespace)?
            };
            Ok(Some(format!(
                "conversion {}",
                quote_qualified(nspname.as_deref(), &conname)
            )))
        }
        proclang::LanguageRelationId => {
            let row = scan_one_row(
                mcx,
                proclang::LanguageRelationId,
                proclang::LanguageOidIndexId,
                object.objectId,
                |tup, desc| name_from_datum(getattr(tup, 2, desc)),
            )?;
            let Some(lanname) = row else {
                if !missing_ok {
                    panic!("cache lookup failed for language {}", object.objectId);
                }
                return Ok(None);
            };
            Ok(Some(format!("language {}", quote_qualified(None, &lanname))))
        }
        OPERATOR_CLASS_RELATION_ID => {
            let Some((opcmethod, opcname, opcnamespace)) = opclass_or_opfamily_row(
                cache_syscache::cacheinfo::CLAOID,
                object.objectId,
            )?
            else {
                if !missing_ok {
                    panic!("cache lookup failed for opclass {}", object.objectId);
                }
                return Ok(None);
            };
            let amname = am_name(opcmethod)?;
            let nspname =
                if catalog_namespace::OpclassnameGetOpcid(opcmethod, &opcname)? == object.objectId
                {
                    None
                } else {
                    get_namespace_name(opcnamespace)?
                };
            Ok(Some(format!(
                "operator class {} for access method {amname}",
                quote_qualified(nspname.as_deref(), &opcname)
            )))
        }
        OPERATOR_FAMILY_RELATION_ID => {
            let Some((opfmethod, opfname, opfnamespace)) = opclass_or_opfamily_row(
                cache_syscache::cacheinfo::OPFAMILYOID,
                object.objectId,
            )?
            else {
                if !missing_ok {
                    panic!("cache lookup failed for opfamily {}", object.objectId);
                }
                return Ok(None);
            };
            let amname = am_name(opfmethod)?;
            let nspname = if catalog_namespace::OpfamilynameGetOpfid(opfmethod, &opfname)?
                == object.objectId
            {
                None
            } else {
                get_namespace_name(opfnamespace)?
            };
            Ok(Some(format!(
                "operator family {} for access method {amname}",
                quote_qualified(nspname.as_deref(), &opfname)
            )))
        }
        PublicationRelationId => {
            Ok(lsyscache::get_publication_name(mcx, object.objectId, missing_ok)?
                .map(|pubname| format!("publication {}", pubname.as_str())))
        }
        PublicationNamespaceRelationId => {
            let Some((pubname, nspname)) =
                getPublicationSchemaInfo(mcx, object.objectId, missing_ok)?
            else {
                return Ok(None);
            };
            Ok(Some(format!(
                "publication of schema {nspname} in publication {pubname}"
            )))
        }
        PublicationRelRelationId => {
            let Some(tup) = cache_syscache::SearchSysCache1(
                cache_syscache::cacheinfo::PUBLICATIONREL,
                cache_syscache::SysCacheKey::Value(Datum::from_oid(object.objectId)),
            )?
            else {
                if !missing_ok {
                    panic!("cache lookup failed for publication table {}", object.objectId);
                }
                return Ok(None);
            };
            let prpubid = cache_syscache::SysCacheGetAttrNotNull(
                cache_syscache::cacheinfo::PUBLICATIONREL,
                &tup,
                2,
            )?
            .as_oid();
            let prrelid = cache_syscache::SysCacheGetAttrNotNull(
                cache_syscache::cacheinfo::PUBLICATIONREL,
                &tup,
                3,
            )?
            .as_oid();
            cache_syscache::ReleaseSysCache(tup);
            let pubname = lsyscache::get_publication_name(mcx, prpubid, false)?
                .expect("missing_ok=false yields a name");
            let rel = getRelationDescription(prrelid, false)?
                .expect("publication relation's table exists");
            Ok(Some(format!(
                "publication of {rel} in publication {}",
                pubname.as_str()
            )))
        }
        SubscriptionRelationId => {
            Ok(lsyscache::get_subscription_name(mcx, object.objectId, missing_ok)?
                .map(|subname| format!("subscription {}", subname.as_str())))
        }
        types_core::FOREIGN_DATA_WRAPPER_RELATION_ID => {
            let Some(name) = foreign_object_name(
                cache_syscache::cacheinfo::FOREIGNDATAWRAPPEROID,
                object.objectId,
            )?
            else {
                if !missing_ok {
                    panic!("cache lookup failed for foreign-data wrapper {}", object.objectId);
                }
                return Ok(None);
            };
            Ok(Some(format!("foreign-data wrapper {name}")))
        }
        types_core::FOREIGN_SERVER_RELATION_ID => {
            let Some(name) = foreign_object_name(
                cache_syscache::cacheinfo::FOREIGNSERVEROID,
                object.objectId,
            )?
            else {
                if !missing_ok {
                    panic!("cache lookup failed for foreign server {}", object.objectId);
                }
                return Ok(None);
            };
            Ok(Some(format!("server {name}")))
        }
        types_core::USER_MAPPING_RELATION_ID => {
            let Some(tup) = cache_syscache::SearchSysCache1(
                cache_syscache::cacheinfo::USERMAPPINGOID,
                cache_syscache::SysCacheKey::Value(Datum::from_oid(object.objectId)),
            )?
            else {
                if !missing_ok {
                    panic!("cache lookup failed for user mapping {}", object.objectId);
                }
                return Ok(None);
            };
            let useid = cache_syscache::SysCacheGetAttrNotNull(
                cache_syscache::cacheinfo::USERMAPPINGOID,
                &tup,
                2,
            )?
            .as_oid();
            let serverid = cache_syscache::SysCacheGetAttrNotNull(
                cache_syscache::cacheinfo::USERMAPPINGOID,
                &tup,
                3,
            )?
            .as_oid();
            cache_syscache::ReleaseSysCache(tup);
            let usename = if useid == types_core::InvalidOid {
                "public".to_string()
            } else {
                miscinit::GetUserNameFromId(mcx, useid, false)?
                    .expect("noerr=false")
                    .as_str()
                    .to_string()
            };
            let srvname = foreign_object_name(
                cache_syscache::cacheinfo::FOREIGNSERVEROID,
                serverid,
            )?
            .unwrap_or_else(|| panic!("cache lookup failed for foreign server {serverid}"));
            Ok(Some(format!("user mapping for {usename} on server {srvname}")))
        }
        crate::LargeObjectRelationId => {
            if !pg_largeobject::LargeObjectExists(mcx, object.objectId)? {
                return Ok(None);
            }
            Ok(Some(format!("large object {}", object.objectId)))
        }
        crate::AccessMethodRelationId => {
            let cacheid = cache_syscache::cacheinfo::AMOID;
            let Some(tup) = cache_syscache::SearchSysCache1(
                cacheid,
                cache_syscache::SysCacheKey::Value(Datum::from_oid(object.objectId)),
            )?
            else {
                if !missing_ok {
                    panic!("cache lookup failed for access method {}", object.objectId);
                }
                return Ok(None);
            };
            let amname =
                name_from_datum(cache_syscache::SysCacheGetAttrNotNull(cacheid, &tup, 2)?);
            cache_syscache::ReleaseSysCache(tup);
            Ok(Some(format!("access method {amname}")))
        }
        crate::AccessMethodOperatorRelationId => {
            let row = scan_one_row(
                mcx,
                crate::AccessMethodOperatorRelationId,
                crate::AccessMethodOperatorOidIndexId,
                object.objectId,
                |tup, desc| {
                    (
                        getattr(tup, 2, desc).as_oid(),
                        getattr(tup, 3, desc).as_oid(),
                        getattr(tup, 4, desc).as_oid(),
                        getattr(tup, 5, desc).as_i16(),
                        getattr(tup, 7, desc).as_oid(),
                    )
                },
            )?;
            let Some((amopfamily, lefttype, righttype, strategy, amopopr)) = row else {
                if !missing_ok {
                    panic!("could not find tuple for amop entry {}", object.objectId);
                }
                return Ok(None);
            };
            let opfam = getObjectDescription(
                mcx,
                &ObjectAddress::set(OPERATOR_FAMILY_RELATION_ID, amopfamily),
                false,
            )?
            .expect("missing_ok=false");
            let lt = format_type::format_type_extended(
                lefttype,
                -1,
                format_type::FORMAT_TYPE_ALLOW_INVALID,
            )?
            .expect("ALLOW_INVALID never yields NULL");
            let rt = format_type::format_type_extended(
                righttype,
                -1,
                format_type::FORMAT_TYPE_ALLOW_INVALID,
            )?
            .expect("ALLOW_INVALID never yields NULL");
            Ok(Some(format!(
                "operator {strategy} ({lt}, {rt}) of {opfam}: {}",
                adt_regproc::format_operator(mcx, amopopr)?
            )))
        }
        crate::AccessMethodProcedureRelationId => {
            let row = scan_one_row(
                mcx,
                crate::AccessMethodProcedureRelationId,
                crate::AccessMethodProcedureOidIndexId,
                object.objectId,
                |tup, desc| {
                    (
                        getattr(tup, 2, desc).as_oid(),
                        getattr(tup, 3, desc).as_oid(),
                        getattr(tup, 4, desc).as_oid(),
                        getattr(tup, 5, desc).as_i16(),
                        getattr(tup, 6, desc).as_oid(),
                    )
                },
            )?;
            let Some((amprocfamily, lefttype, righttype, procnum, amproc)) = row else {
                if !missing_ok {
                    panic!("could not find tuple for amproc entry {}", object.objectId);
                }
                return Ok(None);
            };
            let opfam = getObjectDescription(
                mcx,
                &ObjectAddress::set(OPERATOR_FAMILY_RELATION_ID, amprocfamily),
                false,
            )?
            .expect("missing_ok=false");
            let lt = format_type::format_type_extended(
                lefttype,
                -1,
                format_type::FORMAT_TYPE_ALLOW_INVALID,
            )?
            .expect("ALLOW_INVALID never yields NULL");
            let rt = format_type::format_type_extended(
                righttype,
                -1,
                format_type::FORMAT_TYPE_ALLOW_INVALID,
            )?
            .expect("ALLOW_INVALID never yields NULL");
            let procname = format_procedure(mcx, amproc, false)?
                .unwrap_or_else(|| panic!("cache lookup failed for function {amproc}"));
            Ok(Some(format!(
                "function {procnum} ({lt}, {rt}) of {opfam}: {procname}"
            )))
        }
        crate::AuthMemRelationId => {
            let row = scan_one_row(
                mcx,
                crate::AuthMemRelationId,
                crate::AuthMemOidIndexId,
                object.objectId,
                |tup, desc| (getattr(tup, 2, desc).as_oid(), getattr(tup, 3, desc).as_oid()),
            )?;
            let Some((roleid, member)) = row else {
                if !missing_ok {
                    panic!("could not find tuple for role membership {}", object.objectId);
                }
                return Ok(None);
            };
            let member = miscinit::GetUserNameFromId(mcx, member, false)?
                .expect("noerr=false")
                .as_str()
                .to_string();
            let role = miscinit::GetUserNameFromId(mcx, roleid, false)?
                .expect("noerr=false")
                .as_str()
                .to_string();
            Ok(Some(format!("membership of role {member} in role {role}")))
        }
        types_core::TABLE_SPACE_RELATION_ID => {
            let Some(tblspc) = commands_tablespace::get_tablespace_name(mcx, object.objectId)?
            else {
                if !missing_ok {
                    panic!("cache lookup failed for tablespace {}", object.objectId);
                }
                return Ok(None);
            };
            Ok(Some(format!("tablespace {}", name_str(&tblspc))))
        }
        crate::DefaultAclRelationId => {
            let row = scan_one_row(
                mcx,
                crate::DefaultAclRelationId,
                crate::DefaultAclOidIndexId,
                object.objectId,
                |tup, desc| {
                    (
                        getattr(tup, 2, desc).as_oid(),
                        getattr(tup, 3, desc).as_oid(),
                        getattr(tup, 4, desc).as_i8() as u8,
                    )
                },
            )?;
            let Some((defaclrole, defaclnamespace, defaclobjtype)) = row else {
                if !missing_ok {
                    panic!("could not find tuple for default ACL {}", object.objectId);
                }
                return Ok(None);
            };
            let rolename = miscinit::GetUserNameFromId(mcx, defaclrole, false)?
                .expect("noerr=false")
                .as_str()
                .to_string();
            let nspname = if OidIsValid(defaclnamespace) {
                get_namespace_name(defaclnamespace)?
            } else {
                None
            };
            let noun = match defaclobjtype {
                b'r' => "relations",
                b'S' => "sequences",
                b'f' => "functions",
                b'T' => "types",
                b'n' => "schemas",
                b'L' => "large objects",
                other => panic!("unrecognized default ACL object type {}", other as char),
            };
            Ok(Some(match nspname {
                Some(nsp) => format!(
                    "default privileges on new {noun} belonging to role {rolename} in schema {nsp}"
                ),
                None => {
                    format!("default privileges on new {noun} belonging to role {rolename}")
                }
            }))
        }
        crate::EventTriggerRelationId => {
            let cacheid = cache_syscache::cacheinfo::EVENTTRIGGEROID;
            let Some(tup) = cache_syscache::SearchSysCache1(
                cacheid,
                cache_syscache::SysCacheKey::Value(Datum::from_oid(object.objectId)),
            )?
            else {
                if !missing_ok {
                    panic!("cache lookup failed for event trigger {}", object.objectId);
                }
                return Ok(None);
            };
            let evtname =
                name_from_datum(cache_syscache::SysCacheGetAttrNotNull(cacheid, &tup, 2)?);
            cache_syscache::ReleaseSysCache(tup);
            Ok(Some(format!("event trigger {evtname}")))
        }
        crate::ParameterAclRelationId => {
            let cacheid = cache_syscache::cacheinfo::PARAMETERACLOID;
            let Some(tup) = cache_syscache::SearchSysCache1(
                cacheid,
                cache_syscache::SysCacheKey::Value(Datum::from_oid(object.objectId)),
            )?
            else {
                if !missing_ok {
                    panic!("cache lookup failed for parameter ACL {}", object.objectId);
                }
                return Ok(None);
            };
            let parname = crate::identity::text_datum_to_string(
                cache_syscache::SysCacheGetAttrNotNull(cacheid, &tup, 2)?,
            );
            cache_syscache::ReleaseSysCache(tup);
            Ok(Some(format!("parameter {parname}")))
        }
        crate::TransformRelationId => {
            let cacheid = cache_syscache::cacheinfo::TRFOID;
            let Some(tup) = cache_syscache::SearchSysCache1(
                cacheid,
                cache_syscache::SysCacheKey::Value(Datum::from_oid(object.objectId)),
            )?
            else {
                if !missing_ok {
                    panic!("could not find tuple for transform {}", object.objectId);
                }
                return Ok(None);
            };
            let trftype = cache_syscache::SysCacheGetAttrNotNull(cacheid, &tup, 2)?.as_oid();
            let trflang = cache_syscache::SysCacheGetAttrNotNull(cacheid, &tup, 3)?.as_oid();
            cache_syscache::ReleaseSysCache(tup);
            let langname = crate::identity::language_name(trflang)?
                .unwrap_or_else(|| panic!("cache lookup failed for language {trflang}"));
            Ok(Some(format!(
                "transform for {} language {langname}",
                format_type::format_type_be(trftype)?
            )))
        }
        other => {
            // C getObjectDescription's default arm (objectaddress.c REL_18_3:
            // `elog(ERROR, "unsupported object class: %u")`) for any classId
            // outside its switch — sqlsmith reaches it with arbitrary OIDs
            // (pg_describe_object(16384, 1, 0), the baseline census site).
            // Classes C DOES describe but this port has not implemented yet
            // stay loud below (a coverage gap is not an unsupported class).
            if !c_described_classes(other) {
                return Err(crate::err(
                    ::types_error::ERRCODE_INTERNAL_ERROR,
                    format!("unsupported object class: {other}"),
                ));
            }
            unported(&format!("getObjectDescription object class {other}"))
        }
    }
}

// The classId set C's getObjectDescription switch handles (objectaddress.c
// REL_18_3 case list; OIDs from the CATALOG() lines in the pg_*.h headers).
fn c_described_classes(class_id: types_core::Oid) -> bool {
    matches!(
        class_id,
        826    // pg_default_acl (DefaultAclRelationId)
        | 1213 // pg_tablespace (TableSpaceRelationId)
        | 1247 // pg_type (TypeRelationId)
        | 1255 // pg_proc (ProcedureRelationId)
        | 1259 // pg_class (RelationRelationId)
        | 1260 // pg_authid (AuthIdRelationId)
        | 1261 // pg_auth_members (AuthMemRelationId)
        | 1262 // pg_database (DatabaseRelationId)
        | 1417 // pg_foreign_server (ForeignServerRelationId)
        | 1418 // pg_user_mapping (UserMappingRelationId)
        | 2328 // pg_foreign_data_wrapper (ForeignDataWrapperRelationId)
        | 2601 // pg_am (AccessMethodRelationId)
        | 2602 // pg_amop (AccessMethodOperatorRelationId)
        | 2603 // pg_amproc (AccessMethodProcedureRelationId)
        | 2604 // pg_attrdef (AttrDefaultRelationId)
        | 2605 // pg_cast (CastRelationId)
        | 2606 // pg_constraint (ConstraintRelationId)
        | 2607 // pg_conversion (ConversionRelationId)
        | 2612 // pg_language (LanguageRelationId)
        | 2613 // pg_largeobject (LargeObjectRelationId)
        | 2615 // pg_namespace (NamespaceRelationId)
        | 2616 // pg_opclass (OperatorClassRelationId)
        | 2617 // pg_operator (OperatorRelationId)
        | 2618 // pg_rewrite (RewriteRelationId)
        | 2620 // pg_trigger (TriggerRelationId)
        | 2753 // pg_opfamily (OperatorFamilyRelationId)
        | 3079 // pg_extension (ExtensionRelationId)
        | 3256 // pg_policy (PolicyRelationId)
        | 3381 // pg_statistic_ext (StatisticExtRelationId)
        | 3456 // pg_collation (CollationRelationId)
        | 3466 // pg_event_trigger (EventTriggerRelationId)
        | 3576 // pg_transform (TransformRelationId)
        | 3600 // pg_ts_dict (TSDictionaryRelationId)
        | 3601 // pg_ts_parser (TSParserRelationId)
        | 3602 // pg_ts_config (TSConfigRelationId)
        | 3764 // pg_ts_template (TSTemplateRelationId)
        | 6100 // pg_subscription (SubscriptionRelationId)
        | 6104 // pg_publication (PublicationRelationId)
        | 6106 // pg_publication_rel (PublicationRelRelationId)
        | 6237 // pg_publication_namespace (PublicationNamespaceRelationId)
        | 6243 // pg_parameter_acl (ParameterAclRelationId)
    )
}

// getPublicationSchemaInfo (objectaddress.c): (pubname, nspname) for a
// pg_publication_namespace row.
pub(crate) fn getPublicationSchemaInfo(
    mcx: Mcx<'_>,
    psoid: types_core::Oid,
    missing_ok: bool,
) -> PgResult<Option<(String, String)>> {
    let cacheid = cache_syscache::cacheinfo::PUBLICATIONNAMESPACE;
    let Some(tup) = cache_syscache::SearchSysCache1(
        cacheid,
        cache_syscache::SysCacheKey::Value(Datum::from_oid(psoid)),
    )?
    else {
        if !missing_ok {
            panic!("cache lookup failed for publication schema {psoid}");
        }
        return Ok(None);
    };
    let pnpubid = cache_syscache::SysCacheGetAttrNotNull(cacheid, &tup, 2)?.as_oid();
    let pnnspid = cache_syscache::SysCacheGetAttrNotNull(cacheid, &tup, 3)?.as_oid();
    cache_syscache::ReleaseSysCache(tup);

    let Some(pubname) = lsyscache::get_publication_name(mcx, pnpubid, missing_ok)? else {
        return Ok(None);
    };
    let Some(nspname) = get_namespace_name(pnnspid)? else {
        if !missing_ok {
            panic!("cache lookup failed for schema {pnnspid}");
        }
        return Ok(None);
    };
    Ok(Some((pubname.as_str().to_string(), nspname)))
}

// pg_foreign_data_wrapper and pg_foreign_server carry their NAMEDATALEN name
// column at attnum 2.
pub(crate) fn foreign_object_name(cache_id: i32, oid: Oid) -> PgResult<Option<String>> {
    let Some(tup) = cache_syscache::SearchSysCache1(
        cache_id,
        cache_syscache::SysCacheKey::Value(Datum::from_oid(oid)),
    )?
    else {
        return Ok(None);
    };
    let d = cache_syscache::SysCacheGetAttrNotNull(cache_id, &tup, 2)?;
    // SAFETY: attnum 2 is the inline NAMEDATALEN name column in both catalogs.
    let name = unsafe { &*(d.as_usize() as *const NameData) };
    let s = String::from_utf8_lossy(name.name_str()).into_owned();
    cache_syscache::ReleaseSysCache(tup);
    Ok(Some(s))
}

// pg_opclass and pg_opfamily share the (method, name, namespace) column
// layout at attnums 2/3/4.
pub(crate) fn opclass_or_opfamily_row(cacheid: i32, objid: Oid) -> PgResult<Option<(Oid, String, Oid)>> {
    let Some(tup) = cache_syscache::SearchSysCache1(
        cacheid,
        cache_syscache::SysCacheKey::Value(Datum::from_oid(objid)),
    )?
    else {
        return Ok(None);
    };
    let method = cache_syscache::SysCacheGetAttrNotNull(cacheid, &tup, 2)?.as_oid();
    let name = name_from_datum(cache_syscache::SysCacheGetAttrNotNull(cacheid, &tup, 3)?);
    let nsp = cache_syscache::SysCacheGetAttrNotNull(cacheid, &tup, 4)?.as_oid();
    cache_syscache::ReleaseSysCache(tup);
    Ok(Some((method, name, nsp)))
}

pub(crate) fn am_name(amoid: Oid) -> PgResult<String> {
    let cacheid = cache_syscache::cacheinfo::AMOID;
    let Some(tup) = cache_syscache::SearchSysCache1(
        cacheid,
        cache_syscache::SysCacheKey::Value(Datum::from_oid(amoid)),
    )?
    else {
        panic!("cache lookup failed for access method {amoid}");
    };
    let name = name_from_datum(cache_syscache::SysCacheGetAttrNotNull(cacheid, &tup, 2)?);
    cache_syscache::ReleaseSysCache(tup);
    Ok(name)
}

// getObjectIdentity (objectaddress.c) == getObjectIdentityParts(obj, NULL,
// NULL, missing_ok); one body keeps the strings from diverging.
pub fn getObjectIdentity(
    mcx: Mcx<'_>,
    object: &ObjectAddress,
    missing_ok: bool,
) -> PgResult<Option<String>> {
    Ok(crate::identity::getObjectIdentityParts(mcx, object, missing_ok)?.map(|i| i.identity))
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

const StatisticExtRelationId_descr: Oid = 3381;

// Shared "noun qualified-name" description for name+namespace catalogs;
// schema-qualifies when a bare-name lookup does not resolve to the object
// (the C *IsVisible probes).
fn named_nsp_class_description<'mcx>(
    mcx: Mcx<'mcx>,
    object: &ObjectAddress,
    missing_ok: bool,
    class_id: Oid,
    oid_index: Oid,
    name_attnum: i32,
    nsp_attnum: i32,
    noun: &str,
    visible: impl Fn(&str) -> PgResult<Oid>,
) -> PgResult<Option<String>> {
    let row = scan_one_row(mcx, class_id, oid_index, object.objectId, |tup, desc| {
        (
            name_from_datum(getattr(tup, name_attnum, desc)),
            getattr(tup, nsp_attnum, desc).as_oid(),
        )
    })?;
    let Some((name, nsp)) = row else {
        if !missing_ok {
            panic!("cache lookup failed for {noun} {}", object.objectId);
        }
        return Ok(None);
    };
    let nspname = if visible(&name)? == object.objectId {
        None
    } else {
        get_namespace_name(nsp)?
    };
    Ok(Some(format!("{noun} {}", quote_qualified(nspname.as_deref(), &name))))
}
