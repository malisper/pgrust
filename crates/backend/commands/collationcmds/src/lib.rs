//! collationcmds.c, DefineCollation, full ICU leg: icu_language_tag
//! canonicalization + "using standard form" NOTICE, icu_validate_locale, and
//! collversion via ucol_getVersion (the IsBinaryUpgrade preserve leg is
//! unported; binary upgrade is unreachable repo-wide).

#![allow(non_snake_case)]

use datum::Datum;
use mcx::Mcx;
use pg_collation::CollationForm;
use pg_depend::ObjectAddress;
use pg_locale::{COLLPROVIDER_BUILTIN, COLLPROVIDER_ICU, COLLPROVIDER_LIBC};
use types_core::{InvalidOid, Oid, OidIsValid, COLLATION_RELATION_ID, NAMESPACE_RELATION_ID};
use types_error::{
    PgError, PgResult, ERRCODE_FEATURE_NOT_SUPPORTED, ERRCODE_INVALID_OBJECT_DEFINITION,
    ERRCODE_SYNTAX_ERROR, ERROR,
};
use types_nodes::parsenodes::DefElem;
use types_nodes::{DefineStmt, NodeList, NodeTag};

use parser_small1::ParseState;

const COLLPROVIDER_DEFAULT: u8 = b'd';

fn err_pos(
    pstate: &ParseState<'_, '_>,
    sqlstate: types_error::SqlState,
    msg: String,
    location: i32,
) -> Box<PgError> {
    let pos = parser_small1::parser_errposition(pstate, location, mbutils::GetDatabaseEncoding());
    Box::new(PgError::new(ERROR, msg).with_sqlstate(sqlstate).with_cursor_position(pos))
}

// errorConflictingDefElem (define.c).
fn conflicting_def_elem(pstate: &ParseState<'_, '_>, defel: &DefElem<'_>) -> Box<PgError> {
    err_pos(
        pstate,
        ERRCODE_SYNTAX_ERROR,
        "conflicting or redundant options".into(),
        defel.location,
    )
}

fn err_detail(sqlstate: types_error::SqlState, msg: &str, detail: &str) -> Box<PgError> {
    Box::new(
        PgError::new(ERROR, msg.to_string()).with_sqlstate(sqlstate).with_detail(detail.to_string()),
    )
}

fn param_required(param: &str) -> Box<PgError> {
    Box::new(
        PgError::new(ERROR, format!("parameter \"{param}\" must be specified"))
            .with_sqlstate(ERRCODE_INVALID_OBJECT_DEFINITION),
    )
}

// defGetQualifiedName (define.c) fused with the get_collation_oid call
// (NodeList is not Copy; C returns the embedded list pointer).
fn from_collation_oid<'mcx>(mcx: Mcx<'mcx>, def: &DefElem<'_>) -> PgResult<types_core::Oid> {
    let Some(arg) = def.arg else {
        return Err(Box::new(
            PgError::new(ERROR, format!("{} requires a parameter", def.defname.unwrap_or("")))
                .with_sqlstate(ERRCODE_SYNTAX_ERROR),
        ));
    };
    match arg.node_tag() {
        NodeTag::T_TypeName => catalog_namespace::get_collation_oid_list(
            &arg.as_type_name().expect("TypeName").names,
            false,
        ),
        NodeTag::T_List => {
            catalog_namespace::get_collation_oid_list(arg.as_list().expect("List"), false)
        }
        NodeTag::T_String => {
            let l = NodeList::make1(mcx, arg)?;
            catalog_namespace::get_collation_oid_list(&l, false)
        }
        t => panic!("defGetQualifiedName (define.c): unhandled arg {t:?}"),
    }
}

pub fn DefineCollation<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &mut ParseState<'_, 'mcx>,
    stmt: &DefineStmt<'mcx>,
) -> PgResult<ObjectAddress> {
    let (coll_namespace, coll_name) = creation_namespace(mcx, &stmt.defnames)?;

    let mut from_el: Option<&DefElem<'_>> = None;
    let mut locale_el: Option<&DefElem<'_>> = None;
    let mut lccollate_el: Option<&DefElem<'_>> = None;
    let mut lcctype_el: Option<&DefElem<'_>> = None;
    let mut provider_el: Option<&DefElem<'_>> = None;
    let mut deterministic_el: Option<&DefElem<'_>> = None;
    let mut rules_el: Option<&DefElem<'_>> = None;
    let mut version_el: Option<&DefElem<'_>> = None;

    for pl in stmt.definition.iter() {
        let defel = pl.as_def_elem().expect("DefElem");
        let defname = defel.defname.unwrap_or("");
        let slot: &mut Option<&DefElem<'_>> = match defname {
            "from" => &mut from_el,
            "locale" => &mut locale_el,
            "lc_collate" => &mut lccollate_el,
            "lc_ctype" => &mut lcctype_el,
            "provider" => &mut provider_el,
            "deterministic" => &mut deterministic_el,
            "rules" => &mut rules_el,
            "version" => &mut version_el,
            _ => {
                return Err(err_pos(
                    pstate,
                    ERRCODE_SYNTAX_ERROR,
                    format!("collation attribute \"{defname}\" not recognized"),
                    defel.location,
                ));
            }
        };
        if slot.is_some() {
            return Err(conflicting_def_elem(pstate, defel));
        }
        *slot = Some(defel);
    }

    if locale_el.is_some() && (lccollate_el.is_some() || lcctype_el.is_some()) {
        return Err(err_detail(
            ERRCODE_SYNTAX_ERROR,
            "conflicting or redundant options",
            "LOCALE cannot be specified together with LC_COLLATE or LC_CTYPE.",
        ));
    }
    if from_el.is_some() && stmt.definition.len() != 1 {
        return Err(err_detail(
            ERRCODE_SYNTAX_ERROR,
            "conflicting or redundant options",
            "FROM cannot be specified together with any other options.",
        ));
    }

    let collprovider: u8;
    let collisdeterministic: bool;
    let collencoding: i32;
    let collcollate: Option<String>;
    let collctype: Option<String>;
    let colllocale: Option<String>;
    let collicurules: Option<String>;
    let mut collversion: Option<String> = None;

    if let Some(from_el) = from_el {
        let collid = from_collation_oid(mcx, from_el)?;
        let ctx = mcx::MemoryContext::new("DefineCollation from");
        let row = syscache_seams::lookup_pg_collation_locale_row::call(ctx.mcx(), collid)?
            .ok_or_else(|| -> Box<PgError> {
                PgError::error(format!("cache lookup failed for collation {collid}")).into()
            })?;
        collprovider = row.collprovider;
        collisdeterministic = row.collisdeterministic;
        collencoding = row.collencoding;
        collcollate = row.collcollate.as_ref().map(|s| s.as_str().to_owned());
        collctype = row.collctype.as_ref().map(|s| s.as_str().to_owned());
        colllocale = row.colllocale.as_ref().map(|s| s.as_str().to_owned());
        collicurules = None;
        if collprovider == COLLPROVIDER_DEFAULT {
            return Err(Box::new(
                PgError::new(ERROR, "collation \"default\" cannot be copied".to_string())
                    .with_sqlstate(ERRCODE_INVALID_OBJECT_DEFINITION),
            ));
        }
    } else {
        let mut collproviderstr: Option<&str> = None;
        if let Some(el) = provider_el {
            collproviderstr = Some(explain::defGetString(mcx, el)?);
        }
        collisdeterministic = match deterministic_el {
            Some(el) => explain::defGetBoolean(el)?,
            None => true,
        };
        collicurules = match rules_el {
            Some(el) => Some(explain::defGetString(mcx, el)?.to_owned()),
            None => None,
        };
        if let Some(el) = version_el {
            collversion = Some(explain::defGetString(mcx, el)?.to_owned());
        }

        collprovider = match collproviderstr {
            Some(s) if s.eq_ignore_ascii_case("builtin") => COLLPROVIDER_BUILTIN,
            Some(s) if s.eq_ignore_ascii_case("icu") => COLLPROVIDER_ICU,
            Some(s) if s.eq_ignore_ascii_case("libc") => COLLPROVIDER_LIBC,
            Some(s) => {
                return Err(Box::new(
                    PgError::new(ERROR, format!("unrecognized collation provider: {s}"))
                        .with_sqlstate(ERRCODE_INVALID_OBJECT_DEFINITION),
                ));
            }
            None => COLLPROVIDER_LIBC,
        };

        let mut ccollate: Option<String> = None;
        let mut cctype: Option<String> = None;
        let mut clocale: Option<String> = None;
        if let Some(el) = locale_el {
            let v = explain::defGetString(mcx, el)?;
            if collprovider == COLLPROVIDER_LIBC {
                ccollate = Some(v.to_owned());
                cctype = Some(v.to_owned());
            } else {
                clocale = Some(v.to_owned());
            }
        }
        if let Some(el) = lccollate_el {
            ccollate = Some(explain::defGetString(mcx, el)?.to_owned());
        }
        if let Some(el) = lcctype_el {
            cctype = Some(explain::defGetString(mcx, el)?.to_owned());
        }

        if collprovider == COLLPROVIDER_BUILTIN {
            let loc = clocale.as_deref().ok_or_else(|| param_required("locale"))?;
            clocale = Some(
                pg_locale::builtin_validate_locale(mbutils::GetDatabaseEncoding(), loc)?
                    .to_owned(),
            );
        } else if collprovider == COLLPROVIDER_LIBC {
            if ccollate.is_none() {
                return Err(param_required("lc_collate"));
            }
            if cctype.is_none() {
                return Err(param_required("lc_ctype"));
            }
        } else if collprovider == COLLPROVIDER_ICU {
            let Some(loc) = clocale.as_deref() else {
                return Err(param_required("locale"));
            };
            let elevel =
                types_error::ErrorLevel(guc_tables::vars::icu_validation_level.read());
            if let Some(langtag) = pg_locale::icu_language_tag(loc, elevel)? {
                if langtag != loc {
                    elog::ereport(types_error::NOTICE)
                        .errmsg(format!(
                            "using standard form \"{langtag}\" for ICU locale \"{loc}\""
                        ))
                        .finish(types_error::ErrorLocation::new(
                            "src/backend/commands/collationcmds.c",
                            293,
                            "DefineCollation",
                        ))?;
                    clocale = Some(langtag);
                }
            }
            pg_locale::icu_validate_locale(clocale.as_deref().expect("checked"))?;
        }

        if !collisdeterministic && collprovider != COLLPROVIDER_ICU {
            return Err(Box::new(
                PgError::new(
                    ERROR,
                    "nondeterministic collations not supported with this provider".to_string(),
                )
                .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED),
            ));
        }
        if collicurules.is_some() && collprovider != COLLPROVIDER_ICU {
            return Err(Box::new(
                PgError::new(
                    ERROR,
                    "ICU rules cannot be specified unless locale provider is ICU".to_string(),
                )
                .with_sqlstate(ERRCODE_INVALID_OBJECT_DEFINITION),
            ));
        }

        if collprovider == COLLPROVIDER_BUILTIN {
            collencoding =
                pg_locale::builtin_locale_encoding(clocale.as_deref().expect("validated"))?;
        } else if collprovider == COLLPROVIDER_ICU {
            if !catalog_namespace::is_encoding_supported_by_icu(mbutils::GetDatabaseEncoding()) {
                return Err(Box::new(
                    PgError::new(
                        ERROR,
                        "current database's encoding is not supported with this provider"
                            .to_string(),
                    )
                    .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED),
                ));
            }
            collencoding = -1;
        } else {
            collencoding = mbutils::GetDatabaseEncoding();
            dbcommands::check_encoding_locale_matches(
                collencoding,
                ccollate.as_deref().expect("checked"),
                cctype.as_deref().expect("checked"),
            )?;
        }
        collcollate = ccollate;
        collctype = cctype;
        colllocale = clocale;
    }

    if collversion.is_none() {
        let locale = if collprovider == COLLPROVIDER_LIBC {
            collcollate.as_deref().expect("libc collcollate")
        } else {
            colllocale.as_deref().expect("non-libc colllocale")
        };
        collversion = pg_locale::get_collation_actual_version(collprovider, locale)?;
    }

    let newoid = pg_collation::CollationCreate(
        mcx,
        coll_name,
        coll_namespace,
        miscinit::GetUserId(),
        &CollationForm {
            collprovider,
            collisdeterministic,
            collencoding,
            collcollate: collcollate.as_deref(),
            collctype: collctype.as_deref(),
            colllocale: colllocale.as_deref(),
            collicurules: collicurules.as_deref(),
            collversion: collversion.as_deref(),
        },
        stmt.if_not_exists,
        false,
    )?;

    if !OidIsValid(newoid) {
        return Ok(ObjectAddress::set(InvalidOid, InvalidOid));
    }

    xact::CommandCounterIncrement()?;
    pg_locale::pg_newlocale_from_collation(newoid)?;

    Ok(ObjectAddress::set(COLLATION_RELATION_ID, newoid))
}

// QualifiedNameGetCreationNamespace + DefineCollation's CREATE aclcheck.
fn creation_namespace<'mcx, 'a>(
    mcx: Mcx<'mcx>,
    qualified: &types_nodes::NodeList<'a>,
) -> PgResult<(Oid, &'a str)> {
    let mut names: [&str; 4] = [""; 4];
    let nnames = qualified.len();
    assert!((1..=3).contains(&nnames), "improper qualified name");
    for (i, n) in qualified.iter().enumerate() {
        names[i] = n.as_string().expect("qualified name").sval;
    }
    let (schemaname, name) = catalog_namespace::DeconstructQualifiedName(&names[..nnames])?;
    let namespace = match schemaname {
        Some("pg_temp") => {
            panic!("DefineCollation (collationcmds.c): pg_temp-alias collation creation")
        }
        Some(schemaname) => catalog_namespace::get_namespace_oid(schemaname, false)?,
        None => {
            let path = catalog_namespace::fetch_search_path(mcx, false)?;
            match path.first() {
                Some(&ns) => ns,
                None => {
                    return Err(Box::new(
                        PgError::new(
                            ERROR,
                            "no schema has been selected to create in".to_string(),
                        )
                        .with_sqlstate(types_error::ERRCODE_UNDEFINED_SCHEMA),
                    ));
                }
            }
        }
    };
    if catalog_namespace::isAnyTempNamespace(namespace)? {
        panic!("DefineCollation (collationcmds.c): temp-namespace collation creation");
    }
    if aclchk::object_aclcheck(
        NAMESPACE_RELATION_ID,
        namespace,
        miscinit::GetUserId(),
        adt_acl::ACL_CREATE,
    )? != aclchk::ACLCHECK_OK
    {
        let nspname = syscache_seams::pg_namespace_nspname::call(namespace)?
            .map(|n| String::from_utf8_lossy(n.name_str()).into_owned())
            .unwrap_or_else(|| namespace.to_string());
        return Err(Box::new(
            PgError::new(ERROR, format!("permission denied for schema {nspname}"))
                .with_sqlstate(types_error::ERRCODE_INSUFFICIENT_PRIVILEGE),
        ));
    }
    Ok((namespace, name))
}

fn text_datum_to_string(d: Datum) -> String {
    let p = d.as_usize() as *const u8;
    // SAFETY: non-null in-line text column datum from a pg_collation row.
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

// ALTER COLLATION ... REFRESH VERSION (collationcmds.c AlterCollation).
pub fn AlterCollation<'mcx>(
    mcx: Mcx<'mcx>,
    stmt: &types_nodes::parsenodes::AlterCollationStmt<'mcx>,
) -> PgResult<Oid> {
    const Anum_pg_collation_collprovider: i32 = 5;
    const Anum_pg_collation_collcollate: i32 = 8;
    const Anum_pg_collation_colllocale: i32 = 10;
    const Anum_pg_collation_collversion: i32 = 12;

    let rel = table::table_open(mcx, COLLATION_RELATION_ID, types_rel::RowExclusiveLock)?;
    let coll_oid = catalog_namespace::get_collation_oid_list(&stmt.collname, false)?;

    if coll_oid == types_core::catalog::DEFAULT_COLLATION_OID {
        return Err(Box::new(
            PgError::error("cannot refresh version of default collation")
                .with_hint("Use ALTER DATABASE ... REFRESH COLLATION VERSION instead."),
        ));
    }

    if !aclchk::object_ownercheck(COLLATION_RELATION_ID, coll_oid, miscinit::GetUserId())? {
        aclchk::aclcheck_error(
            aclchk::ACLCHECK_NOT_OWNER,
            types_nodes::parsenodes::ObjectType::OBJECT_COLLATION,
            &catalog_objectaddress::NameListToString(&stmt.collname),
        )?;
    }

    let td = rel.descr();
    let mut key = types_scan::scankey::ScanKeyData::empty();
    key.sk_attno = pg_collation::Anum_pg_collation_oid;
    key.sk_strategy = types_scan::scankey::BTEqualStrategyNumber;
    key.sk_collation = 0;
    key.sk_func = fmgr_seams::fmgr_info::call(types_core::fmgr::F_OIDEQ)
        .unwrap_or_else(|e| panic!("fmgr_info(F_OIDEQ) failed: {e:?}"));
    key.sk_argument = Datum::from_oid(coll_oid);
    let mut scan = genam::systable_beginscan(
        mcx,
        &rel,
        pg_collation::CollationOidIndexId,
        true,
        None,
        core::slice::from_ref(&key),
    )?;
    let Some(tup) = genam::systable_getnext(mcx, &mut scan)? else {
        return Err(Box::new(PgError::error(format!(
            "cache lookup failed for collation {coll_oid}"
        ))));
    };

    let getattr = |attno: i32| -> (Datum, bool) {
        let mut isnull = false;
        // SAFETY: tup is a pg_collation row read under pg_collation's descriptor.
        let d = unsafe { types_tuple::heap_getattr(tup, attno, td, &mut isnull) };
        (d, isnull)
    };

    let (version_datum, version_isnull) = getattr(Anum_pg_collation_collversion);
    let oldversion =
        if version_isnull { None } else { Some(text_datum_to_string(version_datum)) };

    let collprovider = getattr(Anum_pg_collation_collprovider).0.as_u8();
    let locale_attno = if collprovider == COLLPROVIDER_LIBC {
        Anum_pg_collation_collcollate
    } else {
        Anum_pg_collation_colllocale
    };
    let (locale_datum, locale_isnull) = getattr(locale_attno);
    assert!(!locale_isnull, "unexpected null locale in pg_collation row");
    let locale = text_datum_to_string(locale_datum);

    let newversion = pg_locale::get_collation_actual_version(collprovider, &locale)?;

    let loc = |line: i32| {
        types_error::ErrorLocation::new(
            "src/backend/commands/collationcmds.c",
            line,
            "AlterCollation",
        )
    };
    let mut version_image = None;
    let mut replacements: Vec<(i32, Datum)> = Vec::new();
    match (&oldversion, &newversion) {
        (None, Some(_)) | (Some(_), None) => {
            return Err(Box::new(PgError::error("invalid collation version change")));
        }
        (Some(old), Some(new)) if new != old => {
            elog::ereport(types_error::NOTICE)
                .errmsg(format!("changing version from {old} to {new}"))
                .finish(loc(476))?;
            let img = version_image.insert(varlena::cstring_to_text(mcx, new.as_bytes())?);
            replacements.push((
                Anum_pg_collation_collversion,
                Datum::from_usize(img.as_bytes().as_ptr() as usize),
            ));
        }
        _ => {
            elog::ereport(types_error::NOTICE)
                .errmsg("version has not changed".to_string())
                .finish(loc(491))?;
        }
    }
    let _ = &version_image;

    let natts = td.natts as usize;
    let mut repl_values: mcx::PgVec<'_, Datum> = mcx::vec_with_capacity_in(mcx, natts)?;
    let mut repl_isnull: mcx::PgVec<'_, bool> = mcx::vec_with_capacity_in(mcx, natts)?;
    let mut repl: mcx::PgVec<'_, bool> = mcx::vec_with_capacity_in(mcx, natts)?;
    repl_values.resize(natts, Datum::null());
    repl_isnull.resize(natts, false);
    repl.resize(natts, false);
    for &(attnum, d) in &replacements {
        repl_values[(attnum - 1) as usize] = d;
        repl[(attnum - 1) as usize] = true;
    }
    let mut newtup = heaptuple::heap_modify_tuple(mcx, tup, td, &repl_values, &repl_isnull, &repl)?;
    let otid = tup.t_self;
    genam::systable_endscan(mcx, scan)?;
    catalog_indexing::CatalogTupleUpdate(mcx, &rel, &otid, &mut newtup)?;

    rel.close(types_rel::NoLock)?;
    Ok(coll_oid)
}

// IsThereCollationInNamespace (collationcmds.c): friendliness check ahead of
// the unique-index failure; must not match an any-encoding entry either.
pub fn IsThereCollationInNamespace(mcx: Mcx<'_>, collname: &str, nsp_oid: Oid) -> PgResult<()> {
    let dup = |msg: String| -> Box<PgError> {
        Box::new(PgError::error(msg).with_sqlstate(types_error::ERRCODE_DUPLICATE_OBJECT))
    };
    let exists = |encoding: i32| -> PgResult<bool> {
        cache_syscache::SearchSysCacheExists(
            cache_syscache::cacheinfo::COLLNAMEENCNSP,
            cache_syscache::SysCacheKey::Str(collname),
            cache_syscache::SysCacheKey::Value(datum::Datum::from_i32(encoding)),
            cache_syscache::SysCacheKey::Value(datum::Datum::from_oid(nsp_oid)),
            cache_syscache::SysCacheKey::UNUSED,
        )
    };
    let nspname = || -> PgResult<String> {
        Ok(lsyscache::get_namespace_name(mcx, nsp_oid)?
            .map(|n| n.to_string())
            .unwrap_or_default())
    };
    if exists(mbutils::GetDatabaseEncoding() as i32)? {
        return Err(dup(format!(
            "collation \"{collname}\" for encoding \"{}\" already exists in schema \"{}\"",
            mbutils::GetDatabaseEncodingName(),
            nspname()?
        )));
    }
    if exists(-1)? {
        return Err(dup(format!(
            "collation \"{collname}\" already exists in schema \"{}\"",
            nspname()?
        )));
    }
    Ok(())
}
