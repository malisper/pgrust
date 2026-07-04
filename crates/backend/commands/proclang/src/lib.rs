// proclang.c: CREATE [OR REPLACE] LANGUAGE + get_language_oid.
#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

use cache_syscache::cacheinfo::LANGNAME;
use cache_syscache::{ReleaseSysCache, SysCacheGetAttrNotNull, SysCacheKey};
use datum::Datum;
use mcx::Mcx;
use pg_depend::{DependencyType, ObjectAddress};
use types_core::{InvalidOid, Oid, OidIsValid, INTERNALOID, OIDOID, PROCEDURE_RELATION_ID};
use types_error::{
    PgError, PgResult, ERRCODE_DUPLICATE_OBJECT, ERRCODE_INSUFFICIENT_PRIVILEGE,
    ERRCODE_UNDEFINED_OBJECT, ERRCODE_WRONG_OBJECT_TYPE,
};
use types_nodes::parsenodes::CreatePLangStmt;
use types_nodes::NodeList;
use types_rel::RowExclusiveLock;
use types_tuple::NameData;

pub const LanguageRelationId: Oid = 2612;
pub const LanguageOidIndexId: Oid = 2682;

const Natts_pg_language: usize = 9;
const Anum_pg_language_oid: i32 = 1;
const Anum_pg_language_oid_att: types_core::AttrNumber = 1;
const LANGUAGE_HANDLEROID: Oid = 2280;

fn name_list_to_string(names: &NodeList<'_>) -> String {
    let mut out = String::new();
    for (i, n) in names.iter().enumerate() {
        if i > 0 {
            out.push('.');
        }
        out.push_str(n.as_string().expect("qualified name component is a String node").sval);
    }
    out
}

/// C `CreateProceduralLanguage`.
pub fn CreateProceduralLanguage<'mcx>(
    mcx: Mcx<'mcx>,
    stmt: &CreatePLangStmt<'mcx>,
) -> PgResult<ObjectAddress> {
    let language_name = stmt.plname.expect("CREATE LANGUAGE without name");
    let language_owner = miscinit::GetUserId();

    if !superuser::superuser()? {
        return Err(Box::new(
            PgError::error("must be superuser to create custom procedural language")
                .with_sqlstate(ERRCODE_INSUFFICIENT_PRIVILEGE),
        ));
    }

    debug_assert!(!stmt.plhandler.is_nil());
    let handler_oid = parse_func::LookupFuncName(&stmt.plhandler, 0, &[], false)?;
    if lsyscache::get_func_rettype(handler_oid)? != LANGUAGE_HANDLEROID {
        return Err(Box::new(
            PgError::error(format!(
                "function {} must return type {}",
                name_list_to_string(&stmt.plhandler),
                "language_handler"
            ))
            .with_sqlstate(ERRCODE_WRONG_OBJECT_TYPE),
        ));
    }

    // Return types of the inline and validator functions are ignored.
    let inline_oid = if !stmt.plinline.is_nil() {
        parse_func::LookupFuncName(&stmt.plinline, 1, &[INTERNALOID], false)?
    } else {
        InvalidOid
    };
    let val_oid = if !stmt.plvalidator.is_nil() {
        parse_func::LookupFuncName(&stmt.plvalidator, 1, &[OIDOID], false)?
    } else {
        InvalidOid
    };

    let rel = table::table_open(mcx, LanguageRelationId, RowExclusiveLock)?;

    let mut langname = NameData::default();
    langname.namestrcpy(language_name);
    let mut values = [Datum::null(); Natts_pg_language];
    let mut nulls = [false; Natts_pg_language];
    let mut replaces = [true; Natts_pg_language];
    values[1] = Datum::from_usize(langname.data.as_ptr() as usize);
    values[2] = Datum::from_oid(language_owner);
    values[3] = Datum::from_bool(true); // lanispl
    values[4] = Datum::from_bool(stmt.pltrusted);
    values[5] = Datum::from_oid(handler_oid);
    values[6] = Datum::from_oid(inline_oid);
    values[7] = Datum::from_oid(val_oid);
    nulls[8] = true; // lanacl

    let oldtup = cache_syscache::SearchSysCache1(LANGNAME, SysCacheKey::Str(language_name))?;

    let (langoid, is_update) = match oldtup {
        Some(oldtup) => {
            if !stmt.replace {
                ReleaseSysCache(oldtup);
                rel.close(RowExclusiveLock)?;
                return Err(Box::new(
                    PgError::error(format!("language \"{language_name}\" already exists"))
                        .with_sqlstate(ERRCODE_DUPLICATE_OBJECT),
                ));
            }
            let langoid =
                SysCacheGetAttrNotNull(LANGNAME, &oldtup, Anum_pg_language_oid)?.as_oid();

            // Existing oid, ownership and permissions are kept; the
            // dependency update below agrees with this.
            replaces[0] = false;
            replaces[2] = false;
            replaces[8] = false;

            let old = oldtup.tuple();
            let mut tup =
                heaptuple::heap_modify_tuple(mcx, &old, rel.descr(), &values, &nulls, &replaces)?;
            let otid = old.t_self;
            catalog_indexing::CatalogTupleUpdate(mcx, &rel, &otid, &mut tup)?;
            ReleaseSysCache(oldtup);
            (langoid, true)
        }
        None => {
            let langoid =
                catalog::GetNewOidWithIndex(mcx, &rel, LanguageOidIndexId, Anum_pg_language_oid_att)?;
            values[0] = Datum::from_oid(langoid);
            let mut tup = heaptuple::heap_form_tuple(mcx, rel.descr(), &values, &nulls)?;
            catalog_indexing::CatalogTupleInsert(mcx, &rel, &mut tup)?;
            (langoid, false)
        }
    };

    let myself = ObjectAddress::set(LanguageRelationId, langoid);

    if is_update {
        pg_depend::deleteDependencyRecordsFor(mcx, LanguageRelationId, langoid, true)?;
    }
    if !is_update {
        pg_depend::recordDependencyOnOwner(mcx, LanguageRelationId, langoid, language_owner)?;
    }
    pg_depend::recordDependencyOnCurrentExtension(mcx, &myself, is_update)?;

    let mut referenced: [ObjectAddress; 3] = [myself; 3];
    let mut n = 0;
    referenced[n] = ObjectAddress::set(PROCEDURE_RELATION_ID, handler_oid);
    n += 1;
    if OidIsValid(inline_oid) {
        referenced[n] = ObjectAddress::set(PROCEDURE_RELATION_ID, inline_oid);
        n += 1;
    }
    if OidIsValid(val_oid) {
        referenced[n] = ObjectAddress::set(PROCEDURE_RELATION_ID, val_oid);
        n += 1;
    }
    pg_depend::record_object_address_dependencies(
        mcx,
        &myself,
        &mut referenced[..n],
        DependencyType::Normal,
    )?;

    // InvokeObjectPostCreateHook: object-access hooks are elided repo-wide.

    rel.close(RowExclusiveLock)?;
    Ok(myself)
}

/// C `get_language_oid`.
pub fn get_language_oid(langname: &str, missing_ok: bool) -> PgResult<Oid> {
    let oid = cache_syscache::GetSysCacheOid(
        LANGNAME,
        Anum_pg_language_oid,
        SysCacheKey::Str(langname),
        SysCacheKey::UNUSED,
        SysCacheKey::UNUSED,
        SysCacheKey::UNUSED,
    )?;
    if !OidIsValid(oid) && !missing_ok {
        return Err(Box::new(
            PgError::error(format!("language \"{langname}\" does not exist"))
                .with_sqlstate(ERRCODE_UNDEFINED_OBJECT),
        ));
    }
    Ok(oid)
}
