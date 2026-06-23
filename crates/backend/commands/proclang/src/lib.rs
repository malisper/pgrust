#![allow(non_snake_case)]

//! `backend/commands/proclang.c` — `CREATE [OR REPLACE] LANGUAGE`.
//!
//! Faithful port of the two public functions of proclang.c (PostgreSQL 18.3):
//!
//!   * [`CreateProceduralLanguage`] — the `CREATE LANGUAGE` command driver;
//!   * [`get_language_oid`] — name → OID, with the optional missing-error.
//!
//! The branch order, error codes / messages / SQLSTATEs, the create-vs-replace
//! catalog decision (on replace the OID / owner / ACL are left untouched, the
//! `#ifdef NOT_USED` ownership recheck deliberately omitted as in C), the
//! dependency wiring (owner / current-extension / handler + inline + validator)
//! with its exact ordering and the update-vs-create gating, and the post-create
//! object-access hook are all reproduced in order.
//!
//! The catalog tuple build + insert/update crosses the `backend-catalog-indexing`
//! typed seams (`catalog_tuple_insert_pg_language` / `catalog_tuple_update_pg_language`)
//! — the indexing keystone owns `heap_form_tuple` / `GetNewOidWithIndex` /
//! `CatalogTupleInsert`. The pre-existing-definition syscache probe crosses the
//! syscache owner (`language_tuple_by_name` / `language_oid_by_name`). Dependency
//! recording goes through the already-ported `backend-catalog-dependency` /
//! `-pg-depend` / `-pg-shdepend` crates directly.

use ::dependency::{
    add_exact_object_address, new_object_addresses, record_object_address_dependencies,
};
use ::pg_depend::{deleteDependencyRecordsFor, recordDependencyOnCurrentExtension};
use ::pg_shdepend::recordDependencyOnOwner;

use ::mcx::Mcx;
use ::types_catalog::catalog_dependency::{ObjectAddress, DEPENDENCY_NORMAL};
use ::types_catalog::pg_language::{LanguageRelationId, PgLanguageInsertRow};
use ::types_core::{InvalidOid, Oid, OidIsValid, INTERNALOID, OIDOID, PROCEDURE_RELATION_ID};
use ::types_error::{PgError, PgResult, ERRCODE_DUPLICATE_OBJECT, ERRCODE_INSUFFICIENT_PRIVILEGE,
    ERRCODE_UNDEFINED_OBJECT, ERRCODE_WRONG_OBJECT_TYPE, ERROR};
use ::nodes::ddlnodes::CreatePLangStmt;
use ::types_storage::lock::RowExclusiveLock;

/// `LANGUAGE_HANDLEROID` — OID of the `language_handler` pseudotype
/// (`pg_type.dat`, OID 2280).
const LANGUAGE_HANDLEROID: Oid = 2280;

/// `NAMEDATALEN` (`pg_config_manual.h`).
const NAMEDATALEN: usize = 64;

/// `namestrcpy(&name, src)` — copy `src` into a zero-filled `NameData`,
/// truncated to `NAMEDATALEN`, force-terminated at the last slot.
fn namestrcpy(src: &str) -> [u8; NAMEDATALEN] {
    let mut name = [0u8; NAMEDATALEN];
    for (i, &byte) in src.as_bytes().iter().take(NAMEDATALEN).enumerate() {
        name[i] = byte;
    }
    name[NAMEDATALEN - 1] = 0;
    name
}

/// Flatten a name list (`List *` of `String` value nodes) to its bare string
/// components, as `LookupFuncName` and `NameListToString` consume it.
fn name_list_strings<'mcx>(
    mcx: Mcx<'mcx>,
    names: &[::nodes::nodes::NodePtr<'_>],
) -> PgResult<::mcx::PgVec<'mcx, ::mcx::PgString<'mcx>>> {
    let mut out = ::mcx::vec_with_capacity_in(mcx, names.len())?;
    for n in names.iter() {
        let s = match n.as_string() {
            Some(s) => ::mcx::PgString::from_str_in(s.sval.as_str(), mcx)?,
            None => ::mcx::PgString::from_str_in("", mcx)?,
        };
        out.push(s);
    }
    Ok(out)
}

/// `NameListToString(names)` — join the name parts with '.' (not double-quoted),
/// for error messages. (proclang only needs the un-quoted join; this is the
/// faithful image of utils-side `NameListToString` for a list of `String`
/// nodes.)
fn name_list_to_string(names: &[::nodes::nodes::NodePtr<'_>]) -> String {
    let mut string = String::new();
    for (i, n) in names.iter().enumerate() {
        if i != 0 {
            string.push('.');
        }
        match n.as_string() {
            Some(s) => string.push_str(s.sval.as_str()),
            None => string.push('*'),
        }
    }
    string
}

/* ===========================================================================
 * CreateProceduralLanguage (proclang.c:36-217)
 * ========================================================================= */

/// `CreateProceduralLanguage(CreatePLangStmt *stmt)` — `CREATE LANGUAGE`.
pub fn CreateProceduralLanguage<'mcx>(
    mcx: Mcx<'mcx>,
    stmt: &CreatePLangStmt<'_>,
) -> PgResult<ObjectAddress> {
    // const char *languageName = stmt->plname;
    let languageName: &str = stmt.plname.as_deref().unwrap_or("");
    // Oid languageOwner = GetUserId();
    let languageOwner: Oid = miscinit_seams::get_user_id::call();
    let handlerOid: Oid;
    let inlineOid: Oid;
    let valOid: Oid;
    let funcrettype: Oid;

    /*
     * Check permission
     */
    // if (!superuser()) ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE), ...));
    if !superuser_seams::superuser::call()? {
        return Err(PgError::new(
            ERROR,
            "must be superuser to create custom procedural language",
        )
        .with_sqlstate(ERRCODE_INSUFFICIENT_PRIVILEGE));
    }

    /*
     * Lookup the PL handler function and check that it is of the expected
     * return type
     */
    // Assert(stmt->plhandler);
    debug_assert!(!stmt.plhandler.is_empty());
    // handlerOid = LookupFuncName(stmt->plhandler, 0, NULL, false);
    let plhandler_names = name_list_strings(mcx, &stmt.plhandler)?;
    handlerOid =
        parse_func_seams::lookup_func_name::call(&plhandler_names, 0, &[], false)?;
    // funcrettype = get_func_rettype(handlerOid);
    funcrettype = lsyscache_seams::get_func_rettype::call(handlerOid)?;
    if funcrettype != LANGUAGE_HANDLEROID {
        // ereport(ERROR, (errcode(ERRCODE_WRONG_OBJECT_TYPE),
        //   errmsg("function %s must return type %s",
        //          NameListToString(stmt->plhandler), "language_handler")));
        return Err(PgError::new(
            ERROR,
            format!(
                "function {} must return type {}",
                name_list_to_string(&stmt.plhandler),
                "language_handler"
            ),
        )
        .with_sqlstate(ERRCODE_WRONG_OBJECT_TYPE));
    }

    /* validate the inline function */
    if !stmt.plinline.is_empty() {
        // funcargtypes[0] = INTERNALOID;
        // inlineOid = LookupFuncName(stmt->plinline, 1, funcargtypes, false);
        let names = name_list_strings(mcx, &stmt.plinline)?;
        inlineOid = parse_func_seams::lookup_func_name::call(
            &names,
            1,
            &[INTERNALOID],
            false,
        )?;
        /* return value is ignored, so we don't check the type */
    } else {
        inlineOid = InvalidOid;
    }

    /* validate the validator function */
    if !stmt.plvalidator.is_empty() {
        // funcargtypes[0] = OIDOID;
        // valOid = LookupFuncName(stmt->plvalidator, 1, funcargtypes, false);
        let names = name_list_strings(mcx, &stmt.plvalidator)?;
        valOid =
            parse_func_seams::lookup_func_name::call(&names, 1, &[OIDOID], false)?;
        /* return value is ignored, so we don't check the type */
    } else {
        valOid = InvalidOid;
    }

    /* ok to create it */
    // rel = table_open(LanguageRelationId, RowExclusiveLock);
    let rel = table::table_open(mcx, LanguageRelationId, RowExclusiveLock)?;

    /* Prepare data to be inserted */
    // namestrcpy(&langname, languageName);
    // values[..] = ... (oid stamped inside the insert/update seam)
    let row = PgLanguageInsertRow {
        lanname: namestrcpy(languageName),
        lanowner: languageOwner,
        lanispl: true,
        lanpltrusted: stmt.pltrusted,
        lanplcallfoid: handlerOid,
        laninline: inlineOid,
        lanvalidator: valOid,
    };

    /* Check for pre-existing definition */
    // oldtup = SearchSysCache1(LANGNAME, PointerGetDatum(languageName));
    let oldtup = syscache_seams::language_tuple_by_name::call(mcx, languageName)?;

    let langoid: Oid;
    let is_update: bool;
    if let Some((oldtuple, oldform)) = oldtup {
        /* There is one; okay to replace it? */
        if !stmt.replace {
            // ereport(ERROR, (errcode(ERRCODE_DUPLICATE_OBJECT),
            //   errmsg("language \"%s\" already exists", languageName)));
            return Err(PgError::new(
                ERROR,
                format!("language \"{languageName}\" already exists"),
            )
            .with_sqlstate(ERRCODE_DUPLICATE_OBJECT));
        }

        /*
         * This is currently pointless, since we already checked superuser
         * (#ifdef NOT_USED ownership recheck — omitted exactly as in C).
         *
         * Do not change existing oid, ownership or permissions.  The
         * replaces[] mask (oid/lanowner/lanacl => false) is applied inside the
         * update seam over `oldtuple`.
         */
        // tup = heap_modify_tuple(oldtup, tupDesc, values, nulls, replaces);
        // CatalogTupleUpdate(rel, &tup->t_self, tup);
        indexing_seams::catalog_tuple_update_pg_language::call(
            mcx, &rel, &oldtuple, &row,
        )?;
        // langoid = oldform->oid;
        langoid = oldform.oid;
        // ReleaseSysCache(oldtup); — the owned tuple drops here.
        is_update = true;
    } else {
        /* Creating a new language */
        // langoid = GetNewOidWithIndex(rel, LanguageOidIndexId, Anum_pg_language_oid);
        // values[Anum_pg_language_oid - 1] = ObjectIdGetDatum(langoid);
        // tup = heap_form_tuple(tupDesc, values, nulls);
        // CatalogTupleInsert(rel, tup);
        langoid =
            indexing_seams::catalog_tuple_insert_pg_language::call(mcx, &rel, &row)?;
        is_update = false;
    }

    /*
     * Create dependencies for the new language.  If we are updating an
     * existing language, first delete any existing pg_depend entries.
     */
    // myself.classId = LanguageRelationId; myself.objectId = langoid; myself.objectSubId = 0;
    let myself = ObjectAddress {
        classId: LanguageRelationId,
        objectId: langoid,
        objectSubId: 0,
    };

    // if (is_update) deleteDependencyRecordsFor(myself.classId, myself.objectId, true);
    if is_update {
        deleteDependencyRecordsFor(myself.classId, myself.objectId, true)?;
    }

    /* dependency on owner of language */
    // if (!is_update) recordDependencyOnOwner(myself.classId, myself.objectId, languageOwner);
    if !is_update {
        recordDependencyOnOwner(myself.classId, myself.objectId, languageOwner)?;
    }

    /* dependency on extension */
    // recordDependencyOnCurrentExtension(&myself, is_update);
    recordDependencyOnCurrentExtension(mcx, &myself, is_update)?;

    // addrs = new_object_addresses();
    let mut addrs = new_object_addresses();

    /* dependency on the PL handler function */
    // ObjectAddressSet(referenced, ProcedureRelationId, handlerOid);
    // add_exact_object_address(&referenced, addrs);
    let referenced = ObjectAddress {
        classId: PROCEDURE_RELATION_ID,
        objectId: handlerOid,
        objectSubId: 0,
    };
    add_exact_object_address(&referenced, &mut addrs);

    /* dependency on the inline handler function, if any */
    if OidIsValid(inlineOid) {
        let referenced = ObjectAddress {
            classId: PROCEDURE_RELATION_ID,
            objectId: inlineOid,
            objectSubId: 0,
        };
        add_exact_object_address(&referenced, &mut addrs);
    }

    /* dependency on the validator function, if any */
    if OidIsValid(valOid) {
        let referenced = ObjectAddress {
            classId: PROCEDURE_RELATION_ID,
            objectId: valOid,
            objectSubId: 0,
        };
        add_exact_object_address(&referenced, &mut addrs);
    }

    // record_object_address_dependencies(&myself, addrs, DEPENDENCY_NORMAL);
    record_object_address_dependencies(&myself, &mut addrs, DEPENDENCY_NORMAL)?;
    // free_object_addresses(addrs); — the owned ObjectAddresses drops here.
    drop(addrs);

    /* Post creation hook for new procedural language */
    // InvokeObjectPostCreateHook(LanguageRelationId, myself.objectId, 0);
    objectaccess_seams::invoke_object_post_create_hook::call(
        LanguageRelationId,
        myself.objectId,
        0,
    )?;

    // table_close(rel, RowExclusiveLock);
    rel.close(RowExclusiveLock)?;

    Ok(myself)
}

/* ===========================================================================
 * get_language_oid (proclang.c:225-237)
 * ========================================================================= */

/// `get_language_oid(const char *langname, bool missing_ok)` — name → OID.
pub fn get_language_oid(langname: &str, missing_ok: bool) -> PgResult<Oid> {
    // oid = GetSysCacheOid1(LANGNAME, Anum_pg_language_oid, CStringGetDatum(langname));
    let oid = syscache_seams::language_oid_by_name::call(langname)?;
    if !OidIsValid(oid) && !missing_ok {
        // ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT),
        //   errmsg("language \"%s\" does not exist", langname)));
        return Err(PgError::new(ERROR, format!("language \"{langname}\" does not exist"))
            .with_sqlstate(ERRCODE_UNDEFINED_OBJECT));
    }
    Ok(oid)
}

/// Install this unit's inward seams (`commands/proclang.c`). The `get_language_oid`
/// seam is owned here; the rest of proclang's externals consume other owners'
/// seam crates.
/// `case T_CreatePLangStmt: CreateProceduralLanguage(stmt)` (utility.c).
fn create_procedural_language_arm<'mcx>(
    mcx: Mcx<'mcx>,
    stmt: &::nodes::nodes::Node<'mcx>,
) -> PgResult<ObjectAddress> {
    match stmt.node_tag() {
        ::nodes::nodes::ntag::T_CreatePLangStmt => {
            CreateProceduralLanguage(mcx, stmt.expect_createplangstmt())
        }
        _ => panic!("create_procedural_language: parse tree is not a CreatePLangStmt"),
    }
}

pub fn init_seams() {
    proclang_seams::get_language_oid::set(get_language_oid);

    // ProcessUtilitySlow dispatch arm (utility.c CREATE LANGUAGE).
    utility_out_seams::create_procedural_language::set(create_procedural_language_arm);
}
