#![allow(non_snake_case)]
#![allow(non_camel_case_types)]
#![allow(non_upper_case_globals)]
// `check_object_ownership` faithfully takes the same parameter set as the C
// callee (roleid, objtype, address, object, relation).
#![allow(clippy::too_many_arguments)]

//! `backend/commands/seclabel.c` — SECURITY LABEL.
//!
//! Applies, replaces, or removes the `pg_seclabel` / `pg_shseclabel` tuple that
//! holds an object's security label for a given label provider, and maintains
//! the in-process list of registered label providers. The in-crate control
//! flow mirrors the C exactly: the provider-default logic, the support-check,
//! the OBJECT_COLUMN relkind whitelist, the provider relabel-hook invocation,
//! the `label == NULL` → delete branch, the found-vs-not-found upsert decision,
//! the `values`/`nulls`/`replaces` array setup, the `DeleteSecurityLabel`
//! 3-vs-2 scan-key choice, and the `IsSharedRelation` routing for shared vs.
//! database catalogs. The object-address / ownership resolution and the
//! `pg_seclabel` / `pg_shseclabel` catalog primitives cross seams to their
//! owners; `IsSharedRelation` and `errdetail_relkind_not_supported` are real
//! ported functions called directly.

use std::sync::Mutex;

use backend_catalog_catalog::IsSharedRelation;
use backend_catalog_objectaddress_seams::{
    check_object_ownership, get_object_address, ResolvedObjectAddress,
};
use backend_commands_seclabel_seams as seam;
use backend_utils_error::ereport;
use mcx::Mcx;
use types_catalog::catalog_dependency::ObjectAddress;
use types_core::primitive::Oid;
use types_tuple::backend_access_common_heaptuple::Datum;
use types_error::{
    ErrorLocation, PgError, PgResult, ERRCODE_INVALID_PARAMETER_VALUE, ERRCODE_WRONG_OBJECT_TYPE,
    ERROR,
};
use types_nodes::parsenodes::{
    ObjectType, OBJECT_ACCESS_METHOD, OBJECT_AGGREGATE, OBJECT_AMOP, OBJECT_AMPROC,
    OBJECT_ATTRIBUTE, OBJECT_CAST, OBJECT_COLLATION, OBJECT_COLUMN, OBJECT_CONVERSION,
    OBJECT_DATABASE, OBJECT_DEFACL, OBJECT_DEFAULT, OBJECT_DOMAIN, OBJECT_DOMCONSTRAINT,
    OBJECT_EVENT_TRIGGER, OBJECT_EXTENSION, OBJECT_FDW, OBJECT_FOREIGN_SERVER, OBJECT_FOREIGN_TABLE,
    OBJECT_FUNCTION, OBJECT_INDEX, OBJECT_LANGUAGE, OBJECT_LARGEOBJECT, OBJECT_MATVIEW,
    OBJECT_OPCLASS, OBJECT_OPERATOR, OBJECT_OPFAMILY, OBJECT_PARAMETER_ACL, OBJECT_POLICY,
    OBJECT_PROCEDURE, OBJECT_PUBLICATION, OBJECT_PUBLICATION_NAMESPACE, OBJECT_PUBLICATION_REL,
    OBJECT_ROLE, OBJECT_ROUTINE, OBJECT_RULE, OBJECT_SCHEMA, OBJECT_SEQUENCE, OBJECT_STATISTIC_EXT,
    OBJECT_SUBSCRIPTION, OBJECT_TABCONSTRAINT, OBJECT_TABLE, OBJECT_TABLESPACE, OBJECT_TRANSFORM,
    OBJECT_TRIGGER, OBJECT_TSCONFIGURATION, OBJECT_TSDICTIONARY, OBJECT_TSPARSER, OBJECT_TSTEMPLATE,
    OBJECT_TYPE, OBJECT_USER_MAPPING, OBJECT_VIEW,
};
use types_parsenodes::{Node, SecLabelStmt};
use types_storage::lock::{AccessShareLock, NoLock, RowExclusiveLock, ShareUpdateExclusiveLock};
use types_tuple::access::{
    RELKIND_COMPOSITE_TYPE, RELKIND_FOREIGN_TABLE, RELKIND_MATVIEW, RELKIND_PARTITIONED_TABLE,
    RELKIND_RELATION, RELKIND_VIEW,
};

/*
 * pg_seclabel / pg_shseclabel column counts and 1-based attribute numbers
 * (catalog/pg_seclabel_d.h, catalog/pg_shseclabel_d.h). seclabel.c indexes the
 * `values[]` / `nulls[]` / `replaces[]` arrays by `Anum_* - 1`; the in-crate
 * upsert reproduces that, so these are transcribed here verbatim.
 */
const NATTS_PG_SECLABEL: usize = 5;
const ANUM_PG_SECLABEL_OBJOID: usize = 1;
const ANUM_PG_SECLABEL_CLASSOID: usize = 2;
const ANUM_PG_SECLABEL_OBJSUBID: usize = 3;
const ANUM_PG_SECLABEL_PROVIDER: usize = 4;
const ANUM_PG_SECLABEL_LABEL: usize = 5;

const NATTS_PG_SHSECLABEL: usize = 4;
const ANUM_PG_SHSECLABEL_OBJOID: usize = 1;
const ANUM_PG_SHSECLABEL_CLASSOID: usize = 2;
const ANUM_PG_SHSECLABEL_PROVIDER: usize = 3;
const ANUM_PG_SHSECLABEL_LABEL: usize = 4;

/// `typedef void (*check_object_relabel_type) (const ObjectAddress *object,
/// const char *seclabel)` (commands/seclabel.h) — the provider callback that
/// gets control to veto (by `ereport(ERROR)`) a new label.
///
/// The C raw-pointer arguments become borrows: the object address by reference
/// and the (possibly-NULL) label as `Option<&str>`. The hook is a
/// non-capturing `fn` pointer installed by an extension at library load and is
/// stored verbatim in the provider list and invoked directly, as the C does.
/// It returns a [`PgResult`] so a veto surfaces through the error spine (the
/// equivalent of the C hook's `ereport(ERROR)`).
pub type check_object_relabel_type =
    fn(object: &ObjectAddress, seclabel: Option<&str>) -> PgResult<()>;

/// `struct { const char *provider_name; check_object_relabel_type hook; }`
/// (seclabel.c:28-32) — one registered label provider.
struct LabelProvider {
    provider_name: String,
    hook: check_object_relabel_type,
}

/// `static List *label_provider_list = NIL;` (seclabel.c:34).
///
/// The C list is `palloc`'d in `TopMemoryContext` and only ever appended to
/// (never freed), so process-lifetime ownership is exact; the Rust equivalent
/// is a process-global `Vec` guarded for concurrent access.
/// `register_label_provider` runs at shared-library load time (before queries
/// execute), matching the C's `TopMemoryContext` allocation.
static LABEL_PROVIDER_LIST: Mutex<Vec<LabelProvider>> = Mutex::new(Vec::new());

/// `errstart`/`errfinish` source location — seclabel.c is
/// `src/backend/commands/seclabel.c`.
fn errloc(lineno: i32, funcname: &'static str) -> ErrorLocation {
    ErrorLocation::new("../src/backend/commands/seclabel.c", lineno, funcname)
}

/// `SecLabelSupportsObjectType` — return whether security labels are supported
/// for `objtype`.
///
/// seclabel.c:36-105. The C switch lists every `ObjectType` explicitly (with no
/// `default:` so the compiler warns on a new, unhandled type); this match is
/// likewise exhaustive over the Rust `ObjectType` enum, so a newly-added
/// variant is a compile error here too. The trailing `return false` for the
/// "shouldn't get here" case is subsumed by the exhaustive match.
pub fn SecLabelSupportsObjectType(objtype: ObjectType) -> bool {
    match objtype {
        OBJECT_AGGREGATE | OBJECT_COLUMN | OBJECT_DATABASE | OBJECT_DOMAIN | OBJECT_EVENT_TRIGGER
        | OBJECT_FOREIGN_TABLE | OBJECT_FUNCTION | OBJECT_LANGUAGE | OBJECT_LARGEOBJECT
        | OBJECT_MATVIEW | OBJECT_PROCEDURE | OBJECT_PUBLICATION | OBJECT_ROLE | OBJECT_ROUTINE
        | OBJECT_SCHEMA | OBJECT_SEQUENCE | OBJECT_SUBSCRIPTION | OBJECT_TABLE | OBJECT_TABLESPACE
        | OBJECT_TYPE | OBJECT_VIEW => true,

        OBJECT_ACCESS_METHOD
        | OBJECT_AMOP
        | OBJECT_AMPROC
        | OBJECT_ATTRIBUTE
        | OBJECT_CAST
        | OBJECT_COLLATION
        | OBJECT_CONVERSION
        | OBJECT_DEFAULT
        | OBJECT_DEFACL
        | OBJECT_DOMCONSTRAINT
        | OBJECT_EXTENSION
        | OBJECT_FDW
        | OBJECT_FOREIGN_SERVER
        | OBJECT_INDEX
        | OBJECT_OPCLASS
        | OBJECT_OPERATOR
        | OBJECT_OPFAMILY
        | OBJECT_PARAMETER_ACL
        | OBJECT_POLICY
        | OBJECT_PUBLICATION_NAMESPACE
        | OBJECT_PUBLICATION_REL
        | OBJECT_RULE
        | OBJECT_STATISTIC_EXT
        | OBJECT_TABCONSTRAINT
        | OBJECT_TRANSFORM
        | OBJECT_TRIGGER
        | OBJECT_TSCONFIGURATION
        | OBJECT_TSDICTIONARY
        | OBJECT_TSPARSER
        | OBJECT_TSTEMPLATE
        | OBJECT_USER_MAPPING => false,
    }
}

/// `ExecSecLabelStmt` — apply a security label to a database object.
///
/// Returns the [`ObjectAddress`] of the object to which the label was applied.
///
/// seclabel.c:114-217.
pub fn ExecSecLabelStmt<'mcx>(mcx: Mcx<'mcx>, stmt: &SecLabelStmt) -> PgResult<ObjectAddress> {
    /*
     * Find the named label provider, or if none specified, check whether
     * there's exactly one, and if so use it (seclabel.c:122-155).
     *
     * We resolve the provider against LABEL_PROVIDER_LIST under the lock and
     * copy out the matched provider's name and hook; the list is append-only
     * for process lifetime, so the copied values stay valid exactly as the C
     * pointer would.
     */
    let provider_name: String;
    let provider_hook: check_object_relabel_type;
    {
        let list = LABEL_PROVIDER_LIST.lock().unwrap();
        match stmt.provider.as_deref() {
            None => {
                if list.is_empty() {
                    drop(list);
                    ereport(ERROR)
                        .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
                        .errmsg("no security label providers have been loaded")
                        .finish(errloc(129, "ExecSecLabelStmt"))?;
                    unreachable!();
                }
                if list.len() != 1 {
                    drop(list);
                    ereport(ERROR)
                        .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
                        .errmsg(
                            "must specify provider when multiple security label providers have been loaded",
                        )
                        .finish(errloc(133, "ExecSecLabelStmt"))?;
                    unreachable!();
                }
                let lp = &list[0];
                provider_name = lp.provider_name.clone();
                provider_hook = lp.hook;
            }
            Some(stmt_provider) => {
                match list.iter().find(|lp| lp.provider_name == stmt_provider) {
                    Some(lp) => {
                        provider_name = lp.provider_name.clone();
                        provider_hook = lp.hook;
                    }
                    None => {
                        drop(list);
                        ereport(ERROR)
                            .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
                            .errmsg(format!(
                                "security label provider \"{stmt_provider}\" is not loaded"
                            ))
                            .finish(errloc(151, "ExecSecLabelStmt"))?;
                        unreachable!();
                    }
                }
            }
        }
    }

    if !SecLabelSupportsObjectType(stmt.objtype) {
        ereport(ERROR)
            .errcode(ERRCODE_WRONG_OBJECT_TYPE)
            .errmsg("security labels are not supported for this type of object")
            .finish(errloc(158, "ExecSecLabelStmt"))?;
        unreachable!();
    }

    /*
     * Translate the parser representation which identifies this object into an
     * ObjectAddress. get_object_address() will throw an error if the object
     * does not exist, and will also acquire a lock on the target to guard
     * against concurrent modifications (seclabel.c:162-169).
     */
    let object = object_node(stmt);
    let ResolvedObjectAddress { address, relation } =
        get_object_address::call(mcx, stmt.objtype, object, ShareUpdateExclusiveLock, false)?;

    /* Require ownership of the target object (seclabel.c:171-173). */
    check_object_ownership::call(
        seam::get_user_id::call()?,
        stmt.objtype,
        address,
        object,
        relation.as_ref(),
    )?;

    /* Perform other integrity checks as needed (seclabel.c:175-199). */
    #[allow(clippy::single_match)]
    match stmt.objtype {
        OBJECT_COLUMN => {
            /*
             * Allow security labels only on columns of tables, views,
             * materialized views, composite types, and foreign tables (which
             * are the only relkinds for which pg_dump will dump labels).
             */
            let rel = relation.as_ref().ok_or_else(|| {
                PgError::error("ExecSecLabelStmt: OBJECT_COLUMN must have opened a relation")
            })?;
            let relkind = rel.rd_rel.relkind;
            if relkind != RELKIND_RELATION
                && relkind != RELKIND_VIEW
                && relkind != RELKIND_MATVIEW
                && relkind != RELKIND_COMPOSITE_TYPE
                && relkind != RELKIND_FOREIGN_TABLE
                && relkind != RELKIND_PARTITIONED_TABLE
            {
                let relname = rel.name().to_string();
                let detail = backend_catalog_pg_class::errdetail_relkind_not_supported(relkind)?;
                ereport(ERROR)
                    .errcode(ERRCODE_WRONG_OBJECT_TYPE)
                    .errmsg(format!("cannot set security label on relation \"{relname}\""))
                    .errdetail(detail)
                    .finish(errloc(191, "ExecSecLabelStmt"))?;
            }
        }
        _ => {}
    }

    /*
     * Provider gets control here, may throw ERROR to veto new label
     * (seclabel.c:201-202).
     */
    provider_hook(&address, stmt.label.as_deref())?;

    /* Apply new label (seclabel.c:204-205). */
    SetSecurityLabel(mcx, &address, &provider_name, stmt.label.as_deref())?;

    /*
     * If get_object_address() opened the relation for us, we close it to keep
     * the reference count correct - but we retain any locks acquired by
     * get_object_address() until commit time, to guard against concurrent
     * activity (seclabel.c:207-214).
     */
    if let Some(rel) = relation {
        rel.close(NoLock)?;
    }

    Ok(address)
}

/// `GetSharedSecurityLabel` — return the security label for a shared object for
/// a given provider, or `None` if there is no such label.
///
/// seclabel.c:223-265.
fn GetSharedSecurityLabel<'mcx>(
    mcx: Mcx<'mcx>,
    object: &ObjectAddress,
    provider: &str,
) -> PgResult<Option<String>> {
    let pg_shseclabel = seam::shseclabel_open::call(mcx, AccessShareLock)?;

    /* char *seclabel = NULL; (seclabel.c:232) */
    let mut seclabel: Option<String> = None;

    if let Some(col) = seam::shseclabel_get_label::call(
        &pg_shseclabel,
        object.objectId,
        object.classId,
        provider,
    )? {
        if !col.isnull {
            seclabel = Some(seam::text_datum_get_cstring::call(col.value)?);
        }
    }

    pg_shseclabel.close(AccessShareLock)?;

    Ok(seclabel)
}

/// `GetSecurityLabel` — return the security label for a shared or database
/// object for a given provider, or `None` if there is no such label.
///
/// seclabel.c:271-322.
pub fn GetSecurityLabel<'mcx>(
    mcx: Mcx<'mcx>,
    object: &ObjectAddress,
    provider: &str,
) -> PgResult<Option<String>> {
    /* Shared objects have their own security label catalog (seclabel.c:282-284). */
    if IsSharedRelation(object.classId) {
        return GetSharedSecurityLabel(mcx, object, provider);
    }

    /* Must be an unshared object, so examine pg_seclabel (seclabel.c:286-319). */
    let pg_seclabel = seam::seclabel_open::call(mcx, AccessShareLock)?;

    /* char *seclabel = NULL; (seclabel.c:280) */
    let mut seclabel: Option<String> = None;

    if let Some(col) = seam::seclabel_get_label::call(
        &pg_seclabel,
        object.objectId,
        object.classId,
        object.objectSubId,
        provider,
    )? {
        if !col.isnull {
            seclabel = Some(seam::text_datum_get_cstring::call(col.value)?);
        }
    }

    pg_seclabel.close(AccessShareLock)?;

    Ok(seclabel)
}

/// `SetSharedSecurityLabel` — helper of [`SetSecurityLabel`] for shared
/// database objects.
///
/// seclabel.c:328-396. `label == None` means "delete any existing label".
fn SetSharedSecurityLabel<'mcx>(
    mcx: Mcx<'mcx>,
    object: &ObjectAddress,
    provider: &str,
    label: Option<&str>,
) -> PgResult<()> {
    /* Prepare to form or update a tuple, if necessary (seclabel.c:341-348). */
    let mut values: [Datum; NATTS_PG_SHSECLABEL] = std::array::from_fn(|_| Datum::null());
    let nulls = [false; NATTS_PG_SHSECLABEL];
    let mut replaces = [false; NATTS_PG_SHSECLABEL];
    values[ANUM_PG_SHSECLABEL_OBJOID - 1] = Datum::from_oid(object.objectId);
    values[ANUM_PG_SHSECLABEL_CLASSOID - 1] = Datum::from_oid(object.classId);
    values[ANUM_PG_SHSECLABEL_PROVIDER - 1] = seam::cstring_get_text_datum::call(mcx, provider)?;
    if let Some(label) = label {
        values[ANUM_PG_SHSECLABEL_LABEL - 1] = seam::cstring_get_text_datum::call(mcx, label)?;
    }

    /* Use the index to search for a matching old tuple (seclabel.c:350-367). */
    let pg_shseclabel = seam::shseclabel_open::call(mcx, RowExclusiveLock)?;
    let oldtup = seam::shseclabel_find_one::call(
        &pg_shseclabel,
        object.objectId,
        object.classId,
        provider,
    )?;

    /*
     * Found: delete or update it; else, with a label, insert a new one
     * (seclabel.c:369-390). The C `newtup != NULL` guard is "we updated or
     * inserted"; here the equivalent is "we found a tuple to update" vs "no
     * match, so insert".
     */
    let found = match oldtup {
        Some(tuple) => {
            if label.is_none() {
                seam::shseclabel_delete::call(&pg_shseclabel, tuple)?;
            } else {
                replaces[ANUM_PG_SHSECLABEL_LABEL - 1] = true;
                seam::shseclabel_update::call(&pg_shseclabel, tuple, &values, &nulls, &replaces)?;
            }
            true
        }
        None => false,
    };

    /* If we didn't find an old tuple, insert a new one (seclabel.c:384-390). */
    if !found && label.is_some() {
        seam::shseclabel_insert::call(&pg_shseclabel, &values, &nulls)?;
    }

    pg_shseclabel.close(RowExclusiveLock)
}

/// `SetSecurityLabel` — set the security label for `provider` on `object` to
/// `label`. `None` (the C `NULL`) means any existing label should be deleted.
///
/// seclabel.c:403-484.
pub fn SetSecurityLabel<'mcx>(
    mcx: Mcx<'mcx>,
    object: &ObjectAddress,
    provider: &str,
    label: Option<&str>,
) -> PgResult<()> {
    /* Shared objects have their own security label catalog (seclabel.c:416-421). */
    if IsSharedRelation(object.classId) {
        return SetSharedSecurityLabel(mcx, object, provider, label);
    }

    /* Prepare to form or update a tuple, if necessary (seclabel.c:423-431). */
    let mut values: [Datum; NATTS_PG_SECLABEL] = std::array::from_fn(|_| Datum::null());
    let nulls = [false; NATTS_PG_SECLABEL];
    let mut replaces = [false; NATTS_PG_SECLABEL];
    values[ANUM_PG_SECLABEL_OBJOID - 1] = Datum::from_oid(object.objectId);
    values[ANUM_PG_SECLABEL_CLASSOID - 1] = Datum::from_oid(object.classId);
    values[ANUM_PG_SECLABEL_OBJSUBID - 1] = Datum::from_i32(object.objectSubId);
    values[ANUM_PG_SECLABEL_PROVIDER - 1] = seam::cstring_get_text_datum::call(mcx, provider)?;
    if let Some(label) = label {
        values[ANUM_PG_SECLABEL_LABEL - 1] = seam::cstring_get_text_datum::call(mcx, label)?;
    }

    /* Use the index to search for a matching old tuple (seclabel.c:433-454). */
    let pg_seclabel = seam::seclabel_open::call(mcx, RowExclusiveLock)?;
    let oldtup = seam::seclabel_find_one::call(
        &pg_seclabel,
        object.objectId,
        object.classId,
        object.objectSubId,
        provider,
    )?;

    /* Found: delete or update it; else, with a label, insert (seclabel.c:456-477). */
    let found = match oldtup {
        Some(tuple) => {
            if label.is_none() {
                seam::seclabel_delete::call(&pg_seclabel, tuple)?;
            } else {
                replaces[ANUM_PG_SECLABEL_LABEL - 1] = true;
                seam::seclabel_update::call(&pg_seclabel, tuple, &values, &nulls, &replaces)?;
            }
            true
        }
        None => false,
    };

    /* If we didn't find an old tuple, insert a new one (seclabel.c:471-477). */
    if !found && label.is_some() {
        seam::seclabel_insert::call(&pg_seclabel, &values, &nulls)?;
    }

    pg_seclabel.close(RowExclusiveLock)
}

/// `DeleteSharedSecurityLabel` — helper of [`DeleteSecurityLabel`] for shared
/// database objects: remove all pg_shseclabel labels for the object.
///
/// seclabel.c:490-516. Always two scan keys `{objoid, classoid}`.
pub fn DeleteSharedSecurityLabel<'mcx>(
    mcx: Mcx<'mcx>,
    objectId: Oid,
    classId: Oid,
) -> PgResult<()> {
    let pg_shseclabel = seam::shseclabel_open::call(mcx, RowExclusiveLock)?;
    seam::shseclabel_delete_all::call(&pg_shseclabel, objectId, classId)?;
    /* Done (seclabel.c:515) — closes holding RowExclusiveLock. */
    pg_shseclabel.close(RowExclusiveLock)
}

/// `DeleteSecurityLabel` — remove all security labels for an object (and any
/// sub-objects, if applicable).
///
/// seclabel.c:522-567.
pub fn DeleteSecurityLabel<'mcx>(mcx: Mcx<'mcx>, object: &ObjectAddress) -> PgResult<()> {
    /* Shared objects have their own security label catalog (seclabel.c:531-537). */
    if IsSharedRelation(object.classId) {
        debug_assert!(object.objectSubId == 0);
        return DeleteSharedSecurityLabel(mcx, object.objectId, object.classId);
    }

    /*
     * Build the scan keys: always {objoid, classoid}; add the objsubid key only
     * when `objectSubId != 0` (seclabel.c:547-556, where nkeys becomes 3).
     */
    let objsubid = if object.objectSubId != 0 {
        Some(object.objectSubId)
    } else {
        None
    };

    let pg_seclabel = seam::seclabel_open::call(mcx, RowExclusiveLock)?;
    seam::seclabel_delete_all::call(&pg_seclabel, object.objectId, object.classId, objsubid)?;
    /* Done (seclabel.c:566) — closes holding RowExclusiveLock. */
    pg_seclabel.close(RowExclusiveLock)
}

/// `register_label_provider` — append a provider to the in-process list.
///
/// seclabel.c:569-581. The C version `palloc`s the `LabelProvider` and
/// `pstrdup`s the name in `TopMemoryContext` and `lappend`s it; the Rust
/// equivalent pushes an owned `LabelProvider` (owned `String` name) onto the
/// process-global list.
pub fn register_label_provider(provider_name: &str, hook: check_object_relabel_type) {
    let provider = LabelProvider {
        provider_name: provider_name.to_string(),
        hook,
    };
    LABEL_PROVIDER_LIST.lock().unwrap().push(provider);
}

/// Borrow the statement's opaque parser `object` `Node` for
/// `get_object_address` / `check_object_ownership`.
///
/// seclabel.c passes `stmt->object` (a parser `Node *`). The SECURITY LABEL
/// grammar always supplies an object, so a `None` here is a malformed statement
/// and is surfaced as a hard panic rather than silently substituting a
/// sentinel (which would resolve the wrong object).
fn object_node(stmt: &SecLabelStmt) -> &Node {
    stmt.object
        .as_deref()
        .expect("SecLabelStmt::object must be a valid parser node")
}

/// This crate owns no inward-facing seams: nothing in the tree calls into
/// seclabel across a cycle yet. The outward seams it consumes are installed by
/// their owners (objectaddress, varlena, the pg_seclabel/pg_shseclabel catalog
/// owner), not here.
pub fn init_seams() {}

#[cfg(test)]
mod tests {
    use super::*;

    /// seclabel.c:36-105 — the per-`ObjectType` whitelist: true exactly for the
    /// object kinds pg_dump can dump labels for.
    #[test]
    fn supports_object_type_matches_c_whitelist() {
        assert!(SecLabelSupportsObjectType(OBJECT_TABLE));
        assert!(SecLabelSupportsObjectType(OBJECT_COLUMN));
        assert!(SecLabelSupportsObjectType(OBJECT_ROLE));
        assert!(SecLabelSupportsObjectType(OBJECT_DATABASE));
        assert!(SecLabelSupportsObjectType(OBJECT_TYPE));
        assert!(SecLabelSupportsObjectType(OBJECT_VIEW));

        assert!(!SecLabelSupportsObjectType(OBJECT_INDEX));
        assert!(!SecLabelSupportsObjectType(OBJECT_RULE));
        assert!(!SecLabelSupportsObjectType(OBJECT_TRIGGER));
        assert!(!SecLabelSupportsObjectType(OBJECT_POLICY));
        assert!(!SecLabelSupportsObjectType(OBJECT_USER_MAPPING));
    }

    /// seclabel.c:569-581 — `register_label_provider` appends an owned provider
    /// to the process-global list.
    #[test]
    fn register_label_provider_appends() {
        fn ok_hook(_o: &ObjectAddress, _l: Option<&str>) -> PgResult<()> {
            Ok(())
        }
        let before = LABEL_PROVIDER_LIST.lock().unwrap().len();
        register_label_provider("test_provider_unique_xyz", ok_hook);
        let list = LABEL_PROVIDER_LIST.lock().unwrap();
        assert_eq!(list.len(), before + 1);
        assert!(list
            .iter()
            .any(|p| p.provider_name == "test_provider_unique_xyz"));
    }
}
