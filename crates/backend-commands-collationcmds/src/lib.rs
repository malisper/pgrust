// The public surface keeps the PostgreSQL-shaped function names
// (`DefineCollation`, `AlterCollation`, `pg_collation_actual_version`,
// `pg_import_system_collations`, `IsThereCollationInNamespace`) so callers map
// 1:1 onto collationcmds.c.
#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(clippy::result_large_err)]
#![allow(clippy::too_many_arguments)]

//! Port of `backend/commands/collationcmds.c` — collation command support
//! (PostgreSQL 18.3). Every collationcmds.c function is present in-crate with
//! identical control flow, branch order, constants, error codes/messages, and
//! invalidation/hook firing:
//!
//!   * [`DefineCollation`] — CREATE COLLATION (collationcmds.c:52-387);
//!   * [`IsThereCollationInNamespace`] — SET SCHEMA / RENAME dup-name check
//!     (collationcmds.c:395-418);
//!   * [`AlterCollation`] — ALTER COLLATION … REFRESH VERSION (423-503);
//!   * [`pg_collation_actual_version`] body (506-574);
//!   * `normalize_libc_locale_name` (595-621), `cmpaliases` (626-634),
//!     `create_collation_from_locale` (694-753);
//!   * [`pg_import_system_collations`] body (835-1054).
//!
//! `QualifiedNameGetCreationNamespace`, `get_collation_oid`, `NameListToString`
//! are reused directly from the ported `backend-catalog-namespace` crate.
//! `pg_is_ascii` (`common-string`) and `pg_valid_be_encoding` (`types-wchar`)
//! are pure ported computations. The ACL / identity / encoding / transaction /
//! pg_locale substrate crosses canonical seam crates; the collationcmds-owned
//! externals (CollationCreate, the COLLOID syscache read, the locale validators
//! / version helpers, the libc/ICU enumeration, comment creation, the
//! collversion update) cross [`backend_commands_collationcmds_seams`], each of
//! which panics until its owner lands. The `Datum(PG_FUNCTION_ARGS)` fmgr
//! wrappers for the two SQL functions are the accepted project Datum/fmgr
//! deferral; the bodies are ported here taking the concrete `Oid`.

use backend_catalog_namespace::{
    get_collation_oid, NameListToString, QualifiedNameGetCreationNamespace,
};
use backend_utils_error::ereport;
use mcx::Mcx;
use types_acl::{ACLCHECK_OK, ACL_CREATE};
use types_catalog::catalog::COLLATION_RELATION_ID;
use types_catalog::catalog_dependency::{InvalidObjectAddress, ObjectAddress};
use types_core::primitive::{Oid, OidIsValid};
use types_core::NAMESPACE_RELATION_ID;
use types_error::{
    ErrorLocation, PgError, PgResult, ERRCODE_DUPLICATE_OBJECT, ERRCODE_FEATURE_NOT_SUPPORTED,
    ERRCODE_INSUFFICIENT_PRIVILEGE, ERRCODE_INVALID_OBJECT_DEFINITION, ERRCODE_SYNTAX_ERROR,
    ERRCODE_UNDEFINED_OBJECT, ERRCODE_UNDEFINED_SCHEMA, ERROR, NOTICE, WARNING,
};
use types_locale::CollProvider;
use types_nodes::parsenodes::OBJECT_SCHEMA;
use types_parsenodes::{DefElem, Node};
use types_tuple::heaptuple::DEFAULT_COLLATION_OID;
use types_wchar::encoding::{pg_valid_be_encoding, PG_SQL_ASCII};

use backend_commands_collationcmds_seams as seam;
use backend_commands_collationcmds_seams::CollationCreateArgs;
use backend_commands_define_seams::DefElemArg;

use backend_access_transam_xact_seams::command_counter_increment;
use backend_catalog_aclchk_seams::{
    aclcheck_error, error_conflicting_def_elem, object_aclcheck,
};
use backend_utils_adt_pg_locale_seams::{get_collation_actual_version, pg_newlocale_from_collation};
use backend_utils_init_miscinit_seams::{get_user_id, is_binary_upgrade, superuser};
use backend_utils_mb_mbutils_seams::{get_database_encoding, get_database_encoding_name};

pub use backend_commands_collationcmds_seams::CollationRow as PgCollationRow;

/// `ACLCHECK_NOT_OWNER` mapped from the aclchk enum (collationcmds never reaches
/// this code path directly; `aclcheck_error_not_owner_collation` raises it).
/// `CollProvider` codes as the `char` (`i8`) collationcmds passes across seams.
const COLLPROVIDER_DEFAULT: i8 = CollProvider::Default as i8;
const COLLPROVIDER_BUILTIN: i8 = CollProvider::Builtin as i8;
const COLLPROVIDER_ICU: i8 = CollProvider::Icu as i8;
const COLLPROVIDER_LIBC: i8 = CollProvider::Libc as i8;

/// `CollationRelationId` — `pg_collation` (catalog/pg_collation.h, OID 3456).
const CollationRelationId: Oid = COLLATION_RELATION_ID;

/// `ErrorLocation` for `ereport(...).finish(...)` in this module.
fn here(funcname: &'static str) -> ErrorLocation {
    ErrorLocation::new("../src/backend/commands/collationcmds.c", 0, funcname)
}

/// `ObjectAddressSet(addr, class, object)` — sets `objectSubId = 0`.
fn ObjectAddressSet(class_id: Oid, object_id: Oid) -> ObjectAddress {
    ObjectAddress {
        classId: class_id,
        objectId: object_id,
        objectSubId: 0,
    }
}

/// `elog(ERROR, "cache lookup failed for collation %u", collid)`.
fn elog_cache_lookup_failed_collation(collid: Oid) -> PgError {
    ereport(ERROR)
        .errmsg_internal(format!("cache lookup failed for collation {collid}"))
        .finish(here("collationcmds.c"))
        .expect_err("ereport(ERROR) always yields an Err")
}

/// `defel->defname` of a parsed `DefElem`, as a `&str` (empty string if unset).
fn def_name(opt: &DefElem) -> &str {
    opt.defname.as_deref().unwrap_or("")
}

/// Project a parse-tree `DefElem`'s value node into the `DefElemArg` the
/// define.c value accessors (`defGetString`/`defGetBoolean`) switch on. Mirrors
/// the `nodeTag(def->arg)` dispatch; `None` for `def->arg == NULL`.
fn defel_arg(defel: &DefElem) -> Option<DefElemArg> {
    let node = defel.arg.as_deref()?;
    Some(match node {
        Node::Integer(i) => DefElemArg::Integer(i.ival as i64),
        Node::Float(f) => DefElemArg::Float(f.fval.clone().unwrap_or_default()),
        Node::Boolean(b) => DefElemArg::Boolean(b.boolval),
        Node::String(s) => DefElemArg::String(s.sval.clone().unwrap_or_default()),
        _ => DefElemArg::AStar,
    })
}

/// `defGetString(def)` (define.c) — owns its result in `mcx`.
fn def_get_string<'mcx>(mcx: Mcx<'mcx>, defel: &DefElem) -> PgResult<String> {
    let s = backend_commands_define_seams::def_get_string::call(
        mcx,
        defel.defname.clone().unwrap_or_default(),
        defel_arg(defel),
    )?;
    Ok(s.to_string())
}

/// `defGetBoolean(def)` (define.c).
fn def_get_boolean(defel: &DefElem) -> PgResult<bool> {
    backend_commands_define_seams::def_get_boolean::call(
        defel.defname.clone().unwrap_or_default(),
        defel_arg(defel),
    )
}

/* =========================================================================
 * CREATE COLLATION — DefineCollation   (collationcmds.c:52-387)
 * ========================================================================= */

/// `DefineCollation` — CREATE COLLATION (collationcmds.c:52-387).
///
/// `names` is the qualified collation name; `parameters` is the list of
/// `DefElem` value nodes.
pub fn DefineCollation<'mcx>(
    mcx: Mcx<'mcx>,
    names: &[Option<String>],
    parameters: &[Node],
    if_not_exists: bool,
) -> PgResult<ObjectAddress> {
    let mut fromEl: Option<&DefElem> = None;
    let mut localeEl: Option<&DefElem> = None;
    let mut lccollateEl: Option<&DefElem> = None;
    let mut lcctypeEl: Option<&DefElem> = None;
    let mut providerEl: Option<&DefElem> = None;
    let mut deterministicEl: Option<&DefElem> = None;
    let mut rulesEl: Option<&DefElem> = None;
    let mut versionEl: Option<&DefElem> = None;

    let collcollate: Option<String>;
    let collctype: Option<String>;
    let mut colllocale: Option<String>;
    let mut collicurules: Option<String>;
    let collisdeterministic: bool;
    let collencoding: i32;
    let collprovider: i8;
    let mut collversion: Option<String> = None;

    /* collNamespace = QualifiedNameGetCreationNamespace(names, &collName); */
    let (collNamespace, collName) = QualifiedNameGetCreationNamespace(mcx, names)?;
    let collName = collName.to_string();

    /*
     * aclresult = object_aclcheck(NamespaceRelationId, collNamespace,
     *                             GetUserId(), ACL_CREATE);
     */
    let aclresult = object_aclcheck::call(
        NAMESPACE_RELATION_ID,
        collNamespace,
        get_user_id::call(),
        ACL_CREATE,
    )?;
    if aclresult != ACLCHECK_OK {
        aclcheck_error::call(
            aclresult,
            OBJECT_SCHEMA,
            seam::get_namespace_name::call(collNamespace)?,
        )?;
    }

    /* foreach(pl, parameters) */
    for node in parameters {
        let Some(defel) = node.as_defelem() else {
            return Err(elog_error_value(
                "DefineCollation: parameter list element is not a DefElem".to_string(),
            ));
        };
        let defname = def_name(defel);

        let defelp: &mut Option<&DefElem> = if defname == "from" {
            &mut fromEl
        } else if defname == "locale" {
            &mut localeEl
        } else if defname == "lc_collate" {
            &mut lccollateEl
        } else if defname == "lc_ctype" {
            &mut lcctypeEl
        } else if defname == "provider" {
            &mut providerEl
        } else if defname == "deterministic" {
            &mut deterministicEl
        } else if defname == "rules" {
            &mut rulesEl
        } else if defname == "version" {
            &mut versionEl
        } else {
            return ereport(ERROR)
                .errcode(ERRCODE_SYNTAX_ERROR)
                .errmsg(format!("collation attribute \"{defname}\" not recognized"))
                .errposition(seam::parser_errposition::call(defel.location))
                .finish(here("DefineCollation"))
                .map(|()| unreachable!("ereport(ERROR) always yields an Err"));
        };
        if defelp.is_some() {
            error_conflicting_def_elem::call(defel.defname.clone().unwrap_or_default())?;
        }
        *defelp = Some(defel);
    }

    /* if (localeEl && (lccollateEl || lcctypeEl)) */
    if localeEl.is_some() && (lccollateEl.is_some() || lcctypeEl.is_some()) {
        return ereport(ERROR)
            .errcode(ERRCODE_SYNTAX_ERROR)
            .errmsg("conflicting or redundant options")
            .errdetail("LOCALE cannot be specified together with LC_COLLATE or LC_CTYPE.")
            .finish(here("DefineCollation"))
            .map(|()| unreachable!());
    }

    /* if (fromEl && list_length(parameters) != 1) */
    if fromEl.is_some() && parameters.len() != 1 {
        return ereport(ERROR)
            .errcode(ERRCODE_SYNTAX_ERROR)
            .errmsg("conflicting or redundant options")
            .errdetail("FROM cannot be specified together with any other options.")
            .finish(here("DefineCollation"))
            .map(|()| unreachable!());
    }

    if let Some(fromEl) = fromEl {
        /* collid = get_collation_oid(defGetQualifiedName(fromEl), false); */
        let from_name = backend_commands_define::defGetQualifiedName(fromEl)?;
        let from_name_list: Vec<Option<String>> = from_name
            .iter()
            .map(|n| match n {
                Node::String(s) => Ok(s.sval.clone()),
                _ => Err(PgError::error("collation name must be a string").with_sqlstate(
                    types_error::ERRCODE_SYNTAX_ERROR,
                )),
            })
            .collect::<PgResult<_>>()?;
        let collid = get_collation_oid(mcx, &from_name_list, false)?;

        /* tp = SearchSysCache1(COLLOID, ObjectIdGetDatum(collid)); */
        let tp = match seam::collation_row_by_oid::call(collid)? {
            Some(row) => row,
            None => return Err(elog_cache_lookup_failed_collation(collid)),
        };

        collprovider = tp.provider;
        collisdeterministic = tp.is_deterministic;
        collencoding = tp.encoding;

        collcollate = tp.collate;
        collctype = tp.ctype;
        colllocale = tp.locale;

        /*
         * When the ICU locale comes from an existing collation, do not
         * canonicalize to a language tag.
         */
        collicurules = tp.icurules;

        /*
         * Copying the "default" collation is not allowed because most code
         * checks for DEFAULT_COLLATION_OID instead of COLLPROVIDER_DEFAULT.
         */
        if collprovider == COLLPROVIDER_DEFAULT {
            return ereport(ERROR)
                .errcode(ERRCODE_INVALID_OBJECT_DEFINITION)
                .errmsg("collation \"default\" cannot be copied")
                .finish(here("DefineCollation"))
                .map(|()| unreachable!());
        }
    } else {
        let mut collproviderstr: Option<String> = None;

        let mut collcollate_v: Option<String> = None;
        let mut collctype_v: Option<String> = None;
        colllocale = None;
        collicurules = None;

        if let Some(providerEl) = providerEl {
            collproviderstr = Some(def_get_string(mcx, providerEl)?);
        }

        if let Some(deterministicEl) = deterministicEl {
            collisdeterministic = def_get_boolean(deterministicEl)?;
        } else {
            collisdeterministic = true;
        }

        if let Some(rulesEl) = rulesEl {
            collicurules = Some(def_get_string(mcx, rulesEl)?);
        }

        if let Some(versionEl) = versionEl {
            collversion = Some(def_get_string(mcx, versionEl)?);
        }

        if let Some(ref providerstr) = collproviderstr {
            if pg_strcasecmp(providerstr, "builtin") == 0 {
                collprovider = COLLPROVIDER_BUILTIN;
            } else if pg_strcasecmp(providerstr, "icu") == 0 {
                collprovider = COLLPROVIDER_ICU;
            } else if pg_strcasecmp(providerstr, "libc") == 0 {
                collprovider = COLLPROVIDER_LIBC;
            } else {
                return ereport(ERROR)
                    .errcode(ERRCODE_INVALID_OBJECT_DEFINITION)
                    .errmsg(format!("unrecognized collation provider: {providerstr}"))
                    .finish(here("DefineCollation"))
                    .map(|()| unreachable!());
            }
        } else {
            collprovider = COLLPROVIDER_LIBC;
        }

        if let Some(localeEl) = localeEl {
            if collprovider == COLLPROVIDER_LIBC {
                let s = def_get_string(mcx, localeEl)?;
                collcollate_v = Some(s.clone());
                collctype_v = Some(s);
            } else {
                colllocale = Some(def_get_string(mcx, localeEl)?);
            }
        }

        if let Some(lccollateEl) = lccollateEl {
            collcollate_v = Some(def_get_string(mcx, lccollateEl)?);
        }

        if let Some(lcctypeEl) = lcctypeEl {
            collctype_v = Some(def_get_string(mcx, lcctypeEl)?);
        }

        if collprovider == COLLPROVIDER_BUILTIN {
            if colllocale.is_none() {
                return ereport(ERROR)
                    .errcode(ERRCODE_INVALID_OBJECT_DEFINITION)
                    .errmsg(format!("parameter \"{}\" must be specified", "locale"))
                    .finish(here("DefineCollation"))
                    .map(|()| unreachable!());
            }

            colllocale = Some(
                seam::builtin_validate_locale::call(
                    mcx,
                    get_database_encoding::call(),
                    colllocale
                        .as_deref()
                        .ok_or_else(|| PgError::error("DefineCollation: colllocale is NULL"))?,
                )?
                .to_string(),
            );
        } else if collprovider == COLLPROVIDER_LIBC {
            if collcollate_v.is_none() {
                return ereport(ERROR)
                    .errcode(ERRCODE_INVALID_OBJECT_DEFINITION)
                    .errmsg(format!("parameter \"{}\" must be specified", "lc_collate"))
                    .finish(here("DefineCollation"))
                    .map(|()| unreachable!());
            }

            if collctype_v.is_none() {
                return ereport(ERROR)
                    .errcode(ERRCODE_INVALID_OBJECT_DEFINITION)
                    .errmsg(format!("parameter \"{}\" must be specified", "lc_ctype"))
                    .finish(here("DefineCollation"))
                    .map(|()| unreachable!());
            }
        } else if collprovider == COLLPROVIDER_ICU {
            if colllocale.is_none() {
                return ereport(ERROR)
                    .errcode(ERRCODE_INVALID_OBJECT_DEFINITION)
                    .errmsg(format!("parameter \"{}\" must be specified", "locale"))
                    .finish(here("DefineCollation"))
                    .map(|()| unreachable!());
            }

            /*
             * During binary upgrade, preserve the locale string.  Otherwise,
             * canonicalize to a language tag.
             */
            if !is_binary_upgrade::call() {
                let langtag = seam::icu_language_tag::call(
                    mcx,
                    colllocale
                        .as_deref()
                        .ok_or_else(|| PgError::error("DefineCollation: colllocale is NULL"))?,
                    seam::icu_validation_level::call()?,
                )?;

                if let Some(langtag) = langtag {
                    let langtag = langtag.to_string();
                    if colllocale
                        .as_deref()
                        .ok_or_else(|| PgError::error("DefineCollation: colllocale is NULL"))?
                        != langtag
                    {
                        ereport(NOTICE)
                            .errmsg(format!(
                                "using standard form \"{}\" for ICU locale \"{}\"",
                                langtag,
                                colllocale.as_deref().ok_or_else(|| PgError::error(
                                    "DefineCollation: colllocale is NULL"
                                ))?
                            ))
                            .finish(here("DefineCollation"))?;

                        colllocale = Some(langtag);
                    }
                }
            }

            seam::icu_validate_locale::call(
                colllocale
                    .as_deref()
                    .ok_or_else(|| PgError::error("DefineCollation: colllocale is NULL"))?,
            )?;
        }

        /*
         * Nondeterministic collations are currently only supported with ICU.
         */
        if !collisdeterministic && collprovider != COLLPROVIDER_ICU {
            return ereport(ERROR)
                .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
                .errmsg("nondeterministic collations not supported with this provider")
                .finish(here("DefineCollation"))
                .map(|()| unreachable!());
        }

        if collicurules.is_some() && collprovider != COLLPROVIDER_ICU {
            return ereport(ERROR)
                .errcode(ERRCODE_INVALID_OBJECT_DEFINITION)
                .errmsg("ICU rules cannot be specified unless locale provider is ICU")
                .finish(here("DefineCollation"))
                .map(|()| unreachable!());
        }

        if collprovider == COLLPROVIDER_BUILTIN {
            collencoding = seam::builtin_locale_encoding::call(
                colllocale
                    .as_deref()
                    .ok_or_else(|| PgError::error("DefineCollation: colllocale is NULL"))?,
            )?;
        } else if collprovider == COLLPROVIDER_ICU {
            /*
             * ICU collations use collencoding == -1 to match initdb, but only
             * allow creation when the database encoding is supported. Skip the
             * test when !USE_ICU (the seam reports false then; the proper error
             * is thrown later).
             */
            if !seam::is_encoding_supported_by_icu::call(get_database_encoding::call())? {
                return ereport(ERROR)
                    .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
                    .errmsg("current database's encoding is not supported with this provider")
                    .finish(here("DefineCollation"))
                    .map(|()| unreachable!());
            }
            collencoding = -1;
        } else {
            collencoding = get_database_encoding::call();
            seam::check_encoding_locale_matches::call(
                collencoding,
                collcollate_v.as_deref().unwrap_or(""),
                collctype_v.as_deref().unwrap_or(""),
            )?;
        }

        collcollate = collcollate_v;
        collctype = collctype_v;
    }

    if collversion.is_none() {
        /* locale = (collprovider == COLLPROVIDER_LIBC) ? collcollate : colllocale; */
        let locale: Option<&str> = if collprovider == COLLPROVIDER_LIBC {
            collcollate.as_deref()
        } else {
            colllocale.as_deref()
        };

        collversion =
            get_collation_actual_version::call(mcx, collprovider, locale.unwrap_or(""))?
                .map(|s| s.to_string());
    }

    /* newoid = CollationCreate(...); */
    let newoid = seam::collation_create::call(CollationCreateArgs {
        collname: collName.clone(),
        collnamespace: collNamespace,
        collowner: get_user_id::call(),
        collprovider,
        collisdeterministic,
        collencoding,
        collcollate: collcollate.clone(),
        collctype: collctype.clone(),
        colllocale: colllocale.clone(),
        collicurules: collicurules.clone(),
        collversion: collversion.clone(),
        if_not_exists,
        quiet: false,
    })?;

    if !OidIsValid(newoid) {
        return Ok(InvalidObjectAddress);
    }

    /* Check that the locales can be loaded. */
    command_counter_increment::call()?;
    /* (void) pg_newlocale_from_collation(newoid); */
    pg_newlocale_from_collation::call(mcx, newoid)?;

    let address = ObjectAddressSet(CollationRelationId, newoid);

    Ok(address)
}

/* =========================================================================
 * IsThereCollationInNamespace   (collationcmds.c:395-418)
 * ========================================================================= */

/// `IsThereCollationInNamespace` — subroutine for ALTER COLLATION SET SCHEMA and
/// RENAME (collationcmds.c:395-418).  Raises on a duplicate name.
pub fn IsThereCollationInNamespace(collname: &str, nspOid: Oid) -> PgResult<()> {
    /* make sure the name doesn't already exist in new schema */
    if seam::collation_name_enc_nsp_exists::call(
        collname.to_string(),
        get_database_encoding::call(),
        nspOid,
    )? {
        return ereport(ERROR)
            .errcode(ERRCODE_DUPLICATE_OBJECT)
            .errmsg(format!(
                "collation \"{}\" for encoding \"{}\" already exists in schema \"{}\"",
                collname,
                get_database_encoding_name::call(),
                seam::get_namespace_name::call(nspOid)?.unwrap_or_default()
            ))
            .finish(here("IsThereCollationInNamespace"))
            .map(|()| unreachable!());
    }

    /* mustn't match an any-encoding entry, either */
    if seam::collation_name_enc_nsp_exists::call(collname.to_string(), -1, nspOid)? {
        return ereport(ERROR)
            .errcode(ERRCODE_DUPLICATE_OBJECT)
            .errmsg(format!(
                "collation \"{}\" already exists in schema \"{}\"",
                collname,
                seam::get_namespace_name::call(nspOid)?.unwrap_or_default()
            ))
            .finish(here("IsThereCollationInNamespace"))
            .map(|()| unreachable!());
    }

    Ok(())
}

/* =========================================================================
 * ALTER COLLATION — AlterCollation   (collationcmds.c:423-503)
 * ========================================================================= */

/// `AlterCollation` — ALTER COLLATION … REFRESH VERSION (collationcmds.c:423-503).
///
/// `collname` is the owned `stmt->collname` (a list of `String` value nodes).
/// Opening `pg_collation` (RowExclusiveLock), the `collversion`
/// `CatalogTupleUpdate`, `InvokeObjectPostAlterHook`, and closing (NoLock) are
/// performed inside `update_collation_version`.
pub fn AlterCollation<'mcx>(
    mcx: Mcx<'mcx>,
    name_list: &[Option<String>],
) -> PgResult<ObjectAddress> {
    let collOid = get_collation_oid(mcx, name_list, false)?;

    if collOid == DEFAULT_COLLATION_OID {
        return ereport(ERROR)
            .errmsg("cannot refresh version of default collation")
            /* translator: %s is an SQL command */
            .errhint(format!(
                "Use {} instead.",
                "ALTER DATABASE ... REFRESH COLLATION VERSION"
            ))
            .finish(here("AlterCollation"))
            .map(|()| unreachable!());
    }

    if !seam::collation_ownercheck::call(collOid, get_user_id::call())? {
        seam::aclcheck_error_not_owner_collation::call(
            NameListToString(mcx, name_list)?.to_string(),
        )?;
    }

    /* tup = SearchSysCacheCopy1(COLLOID, ObjectIdGetDatum(collOid)); */
    let tup = match seam::collation_row_by_oid::call(collOid)? {
        Some(row) => row,
        None => return Err(elog_cache_lookup_failed_collation(collOid)),
    };

    let oldversion: Option<String> = tup.version.clone();

    /*
     * if (collForm->collprovider == COLLPROVIDER_LIBC)
     *     datum = SysCacheGetAttrNotNull(COLLOID, tup, Anum_pg_collation_collcollate);
     * else
     *     datum = SysCacheGetAttrNotNull(COLLOID, tup, Anum_pg_collation_colllocale);
     */
    let locale_for_version: String = if tup.provider == COLLPROVIDER_LIBC {
        sys_cache_get_attr_not_null(tup.collate.clone(), collOid, "collcollate")?
    } else {
        sys_cache_get_attr_not_null(tup.locale.clone(), collOid, "colllocale")?
    };

    let newversion = get_collation_actual_version::call(mcx, tup.provider, &locale_for_version)?
        .map(|s| s.to_string());

    /* cannot change from NULL to non-NULL or vice versa */
    if (oldversion.is_none() && newversion.is_some())
        || (oldversion.is_some() && newversion.is_none())
    {
        return Err(elog_error_value("invalid collation version change".to_string()));
    } else if oldversion.is_some()
        && newversion.is_some()
        && newversion.as_deref() != oldversion.as_deref()
    {
        ereport(NOTICE)
            .errmsg(format!(
                "changing version from {} to {}",
                oldversion
                    .as_deref()
                    .ok_or_else(|| PgError::error("AlterCollation: oldversion is NULL"))?,
                newversion
                    .as_deref()
                    .ok_or_else(|| PgError::error("AlterCollation: newversion is NULL"))?
            ))
            .finish(here("AlterCollation"))?;

        seam::update_collation_version::call(collOid, newversion.clone())?;
    } else {
        ereport(NOTICE)
            .errmsg("version has not changed")
            .finish(here("AlterCollation"))?;

        /*
         * The C no-change path still issues CatalogTupleUpdate of the unmodified
         * tuple and fires InvokeObjectPostAlterHook; update_collation_version
         * with the old version performs that no-op update plus the hook.
         */
        seam::update_collation_version::call(collOid, oldversion.clone())?;
    }

    let address = ObjectAddressSet(CollationRelationId, collOid);

    Ok(address)
}

/* =========================================================================
 * pg_collation_actual_version   (collationcmds.c:506-574)
 * ========================================================================= */

/// `pg_collation_actual_version` body (collationcmds.c:506-574).  Returns the
/// actual version of the collation `collid`, or `None` for SQL NULL.
pub fn pg_collation_actual_version<'mcx>(
    mcx: Mcx<'mcx>,
    collid: Oid,
) -> PgResult<Option<String>> {
    let provider: i8;
    let locale: String;

    if collid == DEFAULT_COLLATION_OID {
        /* retrieve from pg_database */
        match seam::database_locale_for_default_collation::call()? {
            Some((dbprovider, dblocale)) => {
                provider = dbprovider;
                locale = dblocale;
            }
            None => {
                return ereport(ERROR)
                    .errcode(ERRCODE_UNDEFINED_OBJECT)
                    .errmsg(format!(
                        "database with OID {} does not exist",
                        seam::my_database_id::call()
                    ))
                    .finish(here("pg_collation_actual_version"))
                    .map(|()| unreachable!());
            }
        }
    } else {
        /* retrieve from pg_collation */
        let colltp = match seam::collation_row_by_oid::call(collid)? {
            Some(row) => row,
            None => {
                return ereport(ERROR)
                    .errcode(ERRCODE_UNDEFINED_OBJECT)
                    .errmsg(format!("collation with OID {collid} does not exist"))
                    .finish(here("pg_collation_actual_version"))
                    .map(|()| unreachable!());
            }
        };

        provider = colltp.provider;
        debug_assert!(provider != COLLPROVIDER_DEFAULT);

        if provider == COLLPROVIDER_LIBC {
            locale = sys_cache_get_attr_not_null(colltp.collate, collid, "collcollate")?;
        } else {
            locale = sys_cache_get_attr_not_null(colltp.locale, collid, "colllocale")?;
        }
    }

    let version = get_collation_actual_version::call(mcx, provider, &locale)?.map(|s| s.to_string());
    Ok(version)
}

/* =========================================================================
 * Locale helpers used by pg_import_system_collations   (collationcmds.c:588-753)
 * ========================================================================= */

/// `normalize_libc_locale_name` (collationcmds.c:595-621).
///
/// "Normalize" a libc locale name, stripping off encoding tags such as `.utf8`
/// (e.g., `en_US.utf8` -> `en_US`, but `br_FR.iso885915@euro` -> `br_FR@euro`).
/// Returns `(new_name, changed)`. `READ_LOCALE_A_OUTPUT` (non-WIN32) only.
fn normalize_libc_locale_name(old: &str) -> (String, bool) {
    let mut new = String::with_capacity(old.len());
    let mut changed = false;
    let bytes = old.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        let c = bytes[i];
        if c == b'.' {
            /* skip over encoding tag such as ".utf8" or ".UTF-8" */
            i += 1;
            while i < bytes.len() {
                let o = bytes[i];
                if o.is_ascii_uppercase()
                    || o.is_ascii_lowercase()
                    || o.is_ascii_digit()
                    || o == b'-'
                {
                    i += 1;
                } else {
                    break;
                }
            }
            changed = true;
        } else {
            new.push(c as char);
            i += 1;
        }
    }

    (new, changed)
}

/// `cmpaliases` (collationcmds.c:626-634) — qsort comparator on `localename`.
fn cmpaliases(ca: &CollAliasData, cb: &CollAliasData) -> core::cmp::Ordering {
    ca.localename.as_bytes().cmp(cb.localename.as_bytes())
}

/// `typedef struct { char *localename; char *alias; int enc; } CollAliasData`
/// (collationcmds.c:41-46).
#[derive(Clone)]
struct CollAliasData {
    /// name of locale, as per "locale -a"
    localename: String,
    /// shortened alias for same
    alias: String,
    /// encoding
    enc: i32,
}

/// `create_collation_from_locale` (collationcmds.c:694-753).
///
/// Subroutine for `pg_import_system_collations`. `nvalidp` is incremented for a
/// valid encoding; `ncreatedp` when the collation is actually created. Returns
/// the encoding of the locale, or -1 if not valid for a collation.
fn create_collation_from_locale<'mcx>(
    mcx: Mcx<'mcx>,
    locale: &str,
    nspid: Oid,
    nvalidp: &mut i32,
    ncreatedp: &mut i32,
) -> PgResult<i32> {
    /*
     * Some systems have locale names that don't consist entirely of ASCII
     * letters.  We can't interpret those, so we filter them out.
     */
    if !common_string::pg_is_ascii(locale) {
        seam::elog_debug1::call(&format!("skipping locale with non-ASCII name: \"{locale}\""))?;
        return Ok(-1);
    }

    let enc = seam::pg_get_encoding_from_locale::call(locale)?;
    if enc < 0 {
        seam::elog_debug1::call(&format!(
            "skipping locale with unrecognized encoding: \"{locale}\""
        ))?;
        return Ok(-1);
    }
    if !pg_valid_be_encoding(enc) {
        seam::elog_debug1::call(&format!(
            "skipping locale with client-only encoding: \"{locale}\""
        ))?;
        return Ok(-1);
    }
    if enc == PG_SQL_ASCII {
        return Ok(-1); /* C/POSIX are already in the catalog */
    }

    /* count valid locales found in operating system */
    *nvalidp += 1;

    /*
     * Create a collation named the same as the locale, quietly doing nothing if
     * it already exists.
     */
    let version =
        get_collation_actual_version::call(mcx, COLLPROVIDER_LIBC, locale)?.map(|s| s.to_string());
    let collid = seam::collation_create::call(CollationCreateArgs {
        collname: locale.to_string(),
        collnamespace: nspid,
        collowner: get_user_id::call(),
        collprovider: COLLPROVIDER_LIBC,
        collisdeterministic: true,
        collencoding: enc,
        collcollate: Some(locale.to_string()),
        collctype: Some(locale.to_string()),
        colllocale: None,
        collicurules: None,
        collversion: version,
        if_not_exists: true,
        quiet: true,
    })?;
    if OidIsValid(collid) {
        *ncreatedp += 1;

        /* Must do CCI between inserts to handle duplicates correctly */
        command_counter_increment::call()?;
    }

    Ok(enc)
}

/* =========================================================================
 * pg_import_system_collations   (collationcmds.c:835-1054)
 * ========================================================================= */

/// `pg_import_system_collations` body (collationcmds.c:835-1054).  Adds known
/// system collations to `pg_collation` in namespace `nspid`; returns the count
/// created.
pub fn pg_import_system_collations<'mcx>(mcx: Mcx<'mcx>, nspid: Oid) -> PgResult<i32> {
    let mut ncreated: i32 = 0;

    if !superuser::call(mcx)? {
        return ereport(ERROR)
            .errcode(ERRCODE_INSUFFICIENT_PRIVILEGE)
            .errmsg("must be superuser to import system collations")
            .finish(here("pg_import_system_collations"))
            .map(|()| unreachable!());
    }

    if !seam::namespace_exists::call(nspid)? {
        return ereport(ERROR)
            .errcode(ERRCODE_UNDEFINED_SCHEMA)
            .errmsg(format!("schema with OID {nspid} does not exist"))
            .finish(here("pg_import_system_collations"))
            .map(|()| unreachable!());
    }

    /* Load collations known to libc, using "locale -a" to enumerate them. */
    /* #ifdef READ_LOCALE_A_OUTPUT (non-WIN32) */
    {
        let mut nvalid: i32 = 0;

        /* expansible array of aliases */
        let mut aliases: Vec<CollAliasData> = Vec::new();

        /*
         * locale_a_handle = OpenPipeStream("locale -a", "r"); the runtime
         * enumerates libc locales (lines already newline-stripped) and raises
         * errcode_for_file_access if the pipe could not be opened.
         */
        let lines = seam::enumerate_libc_locales::call()?;

        for localebuf in &lines {
            let enc =
                create_collation_from_locale(mcx, localebuf, nspid, &mut nvalid, &mut ncreated)?;
            if enc < 0 {
                continue;
            }

            /*
             * Generate aliases such as "en_US" in addition to "en_US.utf8".
             * Save them up and add after reading all output, because an alias
             * might conflict with a later "locale -a" name.
             */
            let (alias, changed) = normalize_libc_locale_name(localebuf);
            if changed {
                aliases.push(CollAliasData {
                    localename: localebuf.clone(),
                    alias,
                    enc,
                });
            }
        }

        /*
         * Sort aliases by locale name so a deterministic one is chosen among
         * duplicates with the same encoding/base name. First in ASCII sort order.
         */
        if aliases.len() > 1 {
            aliases.sort_by(cmpaliases);
        }

        /* Now add aliases, ignoring any that match pre-existing entries */
        for a in &aliases {
            let locale = &a.localename;
            let alias = &a.alias;
            let enc = a.enc;

            let version = get_collation_actual_version::call(mcx, COLLPROVIDER_LIBC, locale)?
                .map(|s| s.to_string());
            let collid = seam::collation_create::call(CollationCreateArgs {
                collname: alias.clone(),
                collnamespace: nspid,
                collowner: get_user_id::call(),
                collprovider: COLLPROVIDER_LIBC,
                collisdeterministic: true,
                collencoding: enc,
                collcollate: Some(locale.clone()),
                collctype: Some(locale.clone()),
                colllocale: None,
                collicurules: None,
                collversion: version,
                if_not_exists: true,
                quiet: true,
            })?;
            if OidIsValid(collid) {
                ncreated += 1;

                command_counter_increment::call()?;
            }
        }

        /* Give a warning if "locale -a" seems to be malfunctioning */
        if nvalid == 0 {
            ereport(WARNING)
                .errmsg("no usable system locales were found")
                .finish(here("pg_import_system_collations"))?;
        }
    }

    /*
     * Load collations known to ICU.  uloc_countAvailable()/uloc_getAvailable()
     * returns a full set of language+region combinations.
     */
    /* #ifdef USE_ICU — the seam returns an empty list when !USE_ICU. */
    {
        /*
         * The seam's enumerate_icu_locales prepends the ICU root locale ("") so
         * it is processed without code duplication (the C loop starts at -1).
         */
        let icu_names = seam::enumerate_icu_locales::call()?;
        for name in &icu_names {
            /* langtag = icu_language_tag(name, ERROR); */
            let langtag = seam::icu_language_tag_error::call(mcx, name)?.to_string();

            /* Be paranoid about not allowing any non-ASCII strings into pg_collation */
            if !common_string::pg_is_ascii(&langtag) {
                continue;
            }

            let version = get_collation_actual_version::call(mcx, COLLPROVIDER_ICU, &langtag)?
                .map(|s| s.to_string());
            let collid = seam::collation_create::call(CollationCreateArgs {
                collname: format!("{langtag}-x-icu"),
                collnamespace: nspid,
                collowner: get_user_id::call(),
                collprovider: COLLPROVIDER_ICU,
                collisdeterministic: true,
                collencoding: -1,
                collcollate: None,
                collctype: None,
                colllocale: Some(langtag.clone()),
                collicurules: None,
                collversion: version,
                if_not_exists: true,
                quiet: true,
            })?;
            if OidIsValid(collid) {
                ncreated += 1;

                command_counter_increment::call()?;

                let icucomment = seam::get_icu_locale_comment::call(name)?;
                if let Some(icucomment) = icucomment {
                    seam::create_comment::call(collid, &icucomment)?;
                }
            }
        }
    }

    /*
     * Load collations known to WIN32 — #ifdef ENUM_SYSTEM_LOCALE. Compiled out
     * on the non-WIN32 project target; the libc path above covers it.
     */

    /* PG_RETURN_INT32(ncreated); */
    Ok(ncreated)
}

/* =========================================================================
 * helpers
 * ========================================================================= */

/// Project the `String` value nodes of a `collname` (`List *`) into the
/// `NameList`-shaped `Vec<Option<String>>` the lookups expect. A non-`String`
/// node maps to `None` (the deconstruct path then errors like the C `strVal`).
fn rich_node_string_list(collname: &[types_nodes::nodes::NodePtr<'_>]) -> Vec<Option<String>> {
    collname
        .iter()
        .map(|n| match &**n {
            types_nodes::nodes::Node::String(s) => Some(s.sval.to_string()),
            _ => None,
        })
        .collect()
}

/// `case T_AlterCollationStmt: AlterCollation(stmt)` (utility.c) — the
/// ProcessUtilitySlow dispatch carries the parse tree as `&Node`; project
/// `stmt->collname` into the `NameList` and run the ported body.
fn alter_collation_arm<'mcx>(
    mcx: Mcx<'mcx>,
    stmt: &types_nodes::nodes::Node<'mcx>,
) -> PgResult<ObjectAddress> {
    let acs = match stmt.node_tag() {
        types_nodes::nodes::ntag::T_AlterCollationStmt => stmt.expect_altercollationstmt(),
        _ => panic!("alter_collation: parse tree is not an AlterCollationStmt"),
    };
    let name_list = rich_node_string_list(&acs.collname);
    AlterCollation(mcx, &name_list)
}

/// `elog(ERROR, ...)` value (no SQLSTATE; XX000), for the "can't happen"
/// not-a-DefElem and invalid-version-change failures.
fn elog_error_value(msg: String) -> PgError {
    ereport(ERROR)
        .errmsg_internal(msg)
        .finish(here("collationcmds.c"))
        .expect_err("ereport(ERROR) always yields an Err")
}

/// `SysCacheGetAttrNotNull(cacheId, tup, attr)` — extract a required text column,
/// raising the cache's "unexpected null" error when it is SQL NULL.
fn sys_cache_get_attr_not_null(
    value: Option<String>,
    collid: Oid,
    attr: &str,
) -> PgResult<String> {
    match value {
        Some(v) => Ok(v),
        None => Err(elog_error_value(format!(
            "unexpected null value in cached tuple for catalog pg_collation column {attr} (OID {collid})"
        ))),
    }
}

/// `pg_strcasecmp(s1, s2)` — case-insensitive ASCII compare, 0 on equality
/// (`port/pgstrcasecmp.c`; only the `== 0` result is used here).
fn pg_strcasecmp(s1: &str, s2: &str) -> i32 {
    let a = s1.as_bytes();
    let b = s2.as_bytes();
    let n = a.len().min(b.len());
    for i in 0..n {
        let ca = a[i].to_ascii_lowercase();
        let cb = b[i].to_ascii_lowercase();
        if ca != cb {
            return ca as i32 - cb as i32;
        }
    }
    a.len() as i32 - b.len() as i32
}

mod fmgr_builtins;

/// `pub fn init_seams()` — collationcmds owns no inward seam (no crate calls
/// into it across a cycle). Its `*-seams` crate holds only OUTWARD declarations,
/// installed by their real owners. It does register its SQL-callable fmgr
/// builtins (the two collation-management functions) into the fmgr-core builtin
/// table, so by-OID dispatch resolves them.
pub fn init_seams() {
    fmgr_builtins::register_collationcmds_builtins();

    // ProcessUtilitySlow dispatch arm (utility.c ALTER COLLATION … REFRESH
    // VERSION).
    backend_tcop_utility_out_seams::alter_collation::set(alter_collation_arm);

    // `pg_import_system_collations` static helpers — these bodies live in
    // collationcmds.c itself, so this unit installs them.
    seam::elog_debug1::set(|msg| {
        let _ = ereport(types_error::DEBUG1).errmsg(msg.to_string());
        Ok(())
    });
    // `enumerate_icu_locales` / `get_icu_locale_comment` are entirely under
    // `#ifdef USE_ICU`; in this ICU-disabled build they are empty / None.
    seam::enumerate_icu_locales::set(|| Ok(Vec::new()));
    seam::get_icu_locale_comment::set(|_localename| Ok(None));
    // `enumerate_libc_locales` — the `OpenPipeStream("locale -a")` read loop
    // (`READ_LOCALE_A_OUTPUT`, non-WIN32), yielding newline-stripped names.
    seam::enumerate_libc_locales::set(enumerate_libc_locales);
}

/// `OpenPipeStream("locale -a", "r")` + `fgets` loop (collationcmds.c:852-888),
/// returning each locale name with its trailing newline stripped. Names with no
/// trailing newline (too long for the 128-byte buffer) are skipped with a
/// `DEBUG1`. `Err(errcode_for_file_access)` if the pipe cannot be opened.
fn enumerate_libc_locales() -> PgResult<Vec<String>> {
    use backend_storage_file_fd_seams::{
        close_pipe_stream, open_pipe_stream_read, pipe_read_line, PipeReadLine,
    };

    const LOCALE_NAME_BUFLEN: i32 = 128;

    let stream = match open_pipe_stream_read::call("locale -a")? {
        Some(s) => s,
        None => {
            return Err(PgError::error("could not execute command \"locale -a\"")
                .with_sqlstate(types_error::ERRCODE_IO_ERROR));
        }
    };

    let mut names = Vec::new();
    loop {
        match pipe_read_line::call(stream, LOCALE_NAME_BUFLEN)? {
            PipeReadLine::Line(bytes) => {
                if bytes.last() != Some(&b'\n') {
                    let shown = String::from_utf8_lossy(&bytes);
                    let _ = ereport(types_error::DEBUG1)
                        .errmsg(format!("skipping locale with too-long name: \"{shown}\""));
                    continue;
                }
                let name = String::from_utf8_lossy(&bytes[..bytes.len() - 1]).into_owned();
                names.push(name);
            }
            PipeReadLine::Eof => break,
            // C doesn't check the pipe read return value (supports a missing
            // "locale" command); a read error just ends the loop.
            PipeReadLine::Error(_) => break,
        }
    }

    // C ignores ClosePipeStream's status here (missing-command tolerance).
    let _ = close_pipe_stream::call(stream);

    Ok(names)
}
