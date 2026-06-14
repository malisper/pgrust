#![allow(non_snake_case)]
// `CommentObject` faithfully takes/returns the same parameter set as the C
// callee; `PgError` is a large error type shared across the whole tree, so
// boxing it would diverge from every sibling crate's Result shape.
#![allow(clippy::result_large_err)]

//! `backend/commands/comment.c` — `COMMENT ON`.
//!
//! Faithful port of comment.c: add, replace, or remove the `pg_description` /
//! `pg_shdescription` tuple holding an object's comment. Every C function is
//! ported here branch-for-branch:
//!
//!  * [`CommentObject`] — the COMMENT-ON driver: the COMMENT ON DATABASE dump
//!    work-around, `get_object_address` + ownership check, the per-`ObjectType`
//!    integrity check (only `OBJECT_COLUMN` has one), the shared-vs-local
//!    catalog dispatch, finishing with `relation_close`;
//!  * [`CreateComments`] / [`CreateSharedComments`] — upsert/delete a comment;
//!  * [`DeleteComments`] / [`DeleteSharedComments`] — remove comments for an
//!    object (used when the object itself is dropped);
//!  * [`GetComment`] — fetch an object's comment, or `None`.
//!
//! The control flow is identical to the C: the empty-string -> NULL reduction,
//! the null-comment -> delete vs upsert branch, the "scan; if a match exists
//! delete-or-update it and stop; else insert" upsert structure (the
//! found-vs-not-found decision, the `values`/`nulls`/`replaces` array setup, and
//! the scan-key values), the `DeleteComments` 3-vs-2 scan-key choice, the
//! `GetComment` `!isnull` branch, the WARNING-not-ERROR special case for a
//! missing database, the relkind whitelist for column comments, and the
//! shared-catalog routing for databases/tablespaces/roles.
//!
//! Only genuine cross-subsystem primitives cross the seams in
//! [`backend_commands_comment_seams`]: `get_object_address` /
//! `check_object_ownership` (objectaddress.c), the `strVal` accessor for the
//! opaque parser object node, the relation `relkind`/name/close reads, the
//! decomposed `pg_description`/`pg_shdescription` catalog primitives
//! (`table_open`/`table_close`, the `systable` scans, and the
//! `CatalogTupleDelete`/`heap_modify_tuple`+`CatalogTupleUpdate`/
//! `heap_form_tuple`+`CatalogTupleInsert` mutations), and the
//! `CStringGetTextDatum`/`TextDatumGetCString` varlena conversions.
//! `errdetail_relkind_not_supported` is a real ported function
//! (`backend-catalog-pg-class`) called directly. `GetUserId` is the canonical
//! miscinit seam.

use backend_commands_comment_seams as seam;
use backend_utils_error::ereport;
use backend_utils_init_miscinit_seams::get_user_id;
use types_core::{Oid, OidIsValid};
use types_datum::Datum;
use types_error::{
    ErrorLocation, PgResult, ERRCODE_UNDEFINED_DATABASE, ERRCODE_WRONG_OBJECT_TYPE, ERROR, WARNING,
};
use types_nodes::parsenodes::{OBJECT_COLUMN, OBJECT_DATABASE, OBJECT_ROLE, OBJECT_TABLESPACE};
use types_parsenodes::CommentStmt;
use types_storage::lock::{AccessShareLock, NoLock, RowExclusiveLock, ShareUpdateExclusiveLock};
use types_tuple::access::{
    RELKIND_COMPOSITE_TYPE, RELKIND_FOREIGN_TABLE, RELKIND_MATVIEW, RELKIND_PARTITIONED_TABLE,
    RELKIND_RELATION, RELKIND_VIEW,
};

/*
 * pg_description / pg_shdescription column counts and 1-based attribute numbers
 * (catalog/pg_description_d.h, catalog/pg_shdescription_d.h). comment.c indexes
 * the values[]/nulls[]/replaces[] arrays by `Anum_* - 1`; the in-crate upsert
 * reproduces that, so these are transcribed verbatim.
 */
const NATTS_PG_DESCRIPTION: usize = 4;
const ANUM_PG_DESCRIPTION_OBJOID: usize = 1;
const ANUM_PG_DESCRIPTION_CLASSOID: usize = 2;
const ANUM_PG_DESCRIPTION_OBJSUBID: usize = 3;
const ANUM_PG_DESCRIPTION_DESCRIPTION: usize = 4;

const NATTS_PG_SHDESCRIPTION: usize = 3;
const ANUM_PG_SHDESCRIPTION_OBJOID: usize = 1;
const ANUM_PG_SHDESCRIPTION_CLASSOID: usize = 2;
const ANUM_PG_SHDESCRIPTION_DESCRIPTION: usize = 3;

/// `ErrorLocation` for `ereport(...).finish(...)` in this module.
fn here(funcname: &'static str) -> ErrorLocation {
    ErrorLocation::new("../src/backend/commands/comment.c", 0, funcname)
}

/// `CommentObject` — add the comment in `stmt` into `pg_description` for the
/// object the SQL command names. Returns the resolved [`ObjectAddress`].
///
/// comment.c:39-131.
pub fn CommentObject(
    stmt: &CommentStmt,
) -> PgResult<types_catalog::catalog_dependency::ObjectAddress> {
    // ObjectAddress address = InvalidObjectAddress;
    let mut address = types_catalog::catalog_dependency::InvalidObjectAddress;

    /*
     * When loading a dump, we may see a COMMENT ON DATABASE for the old name
     * of the database. Erroring out would prevent pg_restore from completing
     * (which is really pg_restore's fault, but for now we will work around
     * the problem here). Consensus is that the best fix is to treat wrong
     * database name as a WARNING not an ERROR; hence, the following special
     * case.
     */
    if stmt.objtype == OBJECT_DATABASE {
        // char *database = strVal(stmt->object);
        let database = seam::database_name::call(stmt);

        // get_database_oid(database, true) — missing_ok, so InvalidOid (not an
        // error) when the database is gone.
        if !OidIsValid(
            backend_commands_dbcommands_seams::get_database_oid::call(&database, true)?,
        ) {
            // ereport(WARNING, errmsg("database \"%s\" does not exist", database))
            ereport(WARNING)
                .errcode(ERRCODE_UNDEFINED_DATABASE)
                .errmsg(format!("database \"{database}\" does not exist"))
                .finish(here("CommentObject"))?;
            return Ok(address);
        }
    }

    /*
     * Translate the parser representation that identifies this object into an
     * ObjectAddress. get_object_address() will throw an error if the object
     * does not exist, and will also acquire a lock on the target to guard
     * against concurrent DROP operations.
     */
    let resolved = seam::get_object_address::call(stmt, ShareUpdateExclusiveLock)?;
    address = resolved.address;
    let relation = resolved.relation;

    /* Require ownership of the target object. */
    seam::check_object_ownership::call(get_user_id::call(), stmt, address, relation)?;

    /* Perform other integrity checks as needed. */
    #[allow(clippy::single_match)]
    match stmt.objtype {
        OBJECT_COLUMN => {
            /*
             * Allow comments only on columns of tables, views, materialized
             * views, composite types, and foreign tables (which are the only
             * relkinds for which pg_dump will dump per-column comments). In
             * particular we wish to disallow comments on index columns, because
             * the naming of an index's columns may change across PG versions, so
             * dumping per-column comments could create reload failures.
             */
            // The C dereferences `relation` here unconditionally — for an
            // OBJECT_COLUMN, get_object_address always opened the table.
            let rel = relation.expect(
                "CommentObject: OBJECT_COLUMN must have opened a relation (get_object_address)",
            );
            let relkind = seam::relation_get_relkind::call(rel)?;
            if relkind != RELKIND_RELATION
                && relkind != RELKIND_VIEW
                && relkind != RELKIND_MATVIEW
                && relkind != RELKIND_COMPOSITE_TYPE
                && relkind != RELKIND_FOREIGN_TABLE
                && relkind != RELKIND_PARTITIONED_TABLE
            {
                let relname = seam::relation_get_relation_name::call(rel)?;
                let detail = backend_catalog_pg_class::errdetail_relkind_not_supported(relkind)?;
                ereport(ERROR)
                    .errcode(ERRCODE_WRONG_OBJECT_TYPE)
                    .errmsg(format!("cannot set comment on relation \"{relname}\""))
                    .errdetail(detail)
                    .finish(here("CommentObject"))?;
            }
        }
        _ => {}
    }

    /*
     * Databases, tablespaces, and roles are cluster-wide objects, so any
     * comments on those objects are recorded in the shared pg_shdescription
     * catalog. Comments on all other objects are recorded in pg_description.
     */
    if stmt.objtype == OBJECT_DATABASE
        || stmt.objtype == OBJECT_TABLESPACE
        || stmt.objtype == OBJECT_ROLE
    {
        CreateSharedComments(address.objectId, address.classId, comment_str(stmt))?;
    } else {
        CreateComments(
            address.objectId,
            address.classId,
            address.objectSubId,
            comment_str(stmt),
        )?;
    }

    /*
     * If get_object_address() opened the relation for us, we close it to keep
     * the reference count correct - but we retain any locks acquired by
     * get_object_address() until commit time, to guard against concurrent
     * activity.
     */
    if let Some(rel) = relation {
        seam::relation_close::call(rel, NoLock)?;
    }

    Ok(address)
}

/// `CreateComments` — create/replace/delete a `pg_description` comment.
///
/// Inserts a new pg_description tuple, or replaces an existing one with the
/// same key. If `comment` is `None`/empty (the C null/empty-string case),
/// instead delete any existing comment for the specified key.
///
/// comment.c:142-226.
pub fn CreateComments(oid: Oid, classoid: Oid, subid: i32, comment: Option<&str>) -> PgResult<()> {
    /* Reduce empty-string to NULL case (comment.c:156). */
    let comment = reduce_empty(comment);

    /*
     * Prepare to form or update a tuple, if necessary (comment.c:159-171).
     * When `comment` is None this stays unused — like the C, which only fills
     * the arrays in the `comment != NULL` branch.
     */
    let mut values = [Datum::null(); NATTS_PG_DESCRIPTION];
    let mut nulls = [false; NATTS_PG_DESCRIPTION];
    let mut replaces = [false; NATTS_PG_DESCRIPTION];
    if let Some(comment) = comment {
        for i in 0..NATTS_PG_DESCRIPTION {
            nulls[i] = false;
            replaces[i] = true;
        }
        values[ANUM_PG_DESCRIPTION_OBJOID - 1] = Datum::from_oid(oid);
        values[ANUM_PG_DESCRIPTION_CLASSOID - 1] = Datum::from_oid(classoid);
        values[ANUM_PG_DESCRIPTION_OBJSUBID - 1] = Datum::from_i32(subid);
        values[ANUM_PG_DESCRIPTION_DESCRIPTION - 1] = seam::cstring_get_text_datum::call(comment)?;
    }

    /*
     * Use the index to search for a matching old tuple (comment.c:173-191).
     * The scan-key values are the `{oid, classoid, subid}` key; the
     * `DescriptionObjIndexId` index scan lives behind the primitive.
     */
    let description = seam::description_open::call(RowExclusiveLock)?;
    let oldtuple = seam::description_find_one::call(description, oid, classoid, subid)?;

    /*
     * Found the old tuple, so delete or update it; else, if we have a comment,
     * insert a new one (comment.c:193-218).
     */
    match oldtuple {
        Some(tuple) => {
            if comment.is_none() {
                seam::description_delete::call(description, tuple)?;
            } else {
                seam::description_update::call(description, tuple, &values, &nulls, &replaces)?;
            }
        }
        None => {
            /* If we didn't find an old tuple, insert a new one. */
            if comment.is_some() {
                seam::description_insert::call(description, &values, &nulls)?;
            }
        }
    }

    /* Done (comment.c:225). */
    seam::description_close::call(description, NoLock)
}

/// `CreateSharedComments` — create/replace/delete a `pg_shdescription` comment.
///
/// comment.c:237-316. Same shape as [`CreateComments`] with two scan keys and
/// no objsubid column.
pub fn CreateSharedComments(oid: Oid, classoid: Oid, comment: Option<&str>) -> PgResult<()> {
    /* Reduce empty-string to NULL case (comment.c:251). */
    let comment = reduce_empty(comment);

    /* Prepare to form or update a tuple, if necessary (comment.c:254-265). */
    let mut values = [Datum::null(); NATTS_PG_SHDESCRIPTION];
    let mut nulls = [false; NATTS_PG_SHDESCRIPTION];
    let mut replaces = [false; NATTS_PG_SHDESCRIPTION];
    if let Some(comment) = comment {
        for i in 0..NATTS_PG_SHDESCRIPTION {
            nulls[i] = false;
            replaces[i] = true;
        }
        values[ANUM_PG_SHDESCRIPTION_OBJOID - 1] = Datum::from_oid(oid);
        values[ANUM_PG_SHDESCRIPTION_CLASSOID - 1] = Datum::from_oid(classoid);
        values[ANUM_PG_SHDESCRIPTION_DESCRIPTION - 1] =
            seam::cstring_get_text_datum::call(comment)?;
    }

    /* Use the index to search for a matching old tuple (comment.c:267-281). */
    let shdescription = seam::shdescription_open::call(RowExclusiveLock)?;
    let oldtuple = seam::shdescription_find_one::call(shdescription, oid, classoid)?;

    /* Found: delete or update it; else, with a comment, insert (comment.c:283-307). */
    match oldtuple {
        Some(tuple) => {
            if comment.is_none() {
                seam::shdescription_delete::call(shdescription, tuple)?;
            } else {
                seam::shdescription_update::call(shdescription, tuple, &values, &nulls, &replaces)?;
            }
        }
        None => {
            if comment.is_some() {
                seam::shdescription_insert::call(shdescription, &values, &nulls)?;
            }
        }
    }

    /* Done (comment.c:315). */
    seam::shdescription_close::call(shdescription, NoLock)
}

/// `DeleteComments` — remove `pg_description` comments for an object.
///
/// If `subid` is nonzero then only comments matching it will be removed. If
/// `subid` is zero, all comments matching the oid/classoid will be removed
/// (this corresponds to deleting a whole object).
///
/// comment.c:325-368.
pub fn DeleteComments(oid: Oid, classoid: Oid, subid: i32) -> PgResult<()> {
    /*
     * Build the scan keys: always {objoid, classoid}; add the objsubid key only
     * when `subid != 0` (comment.c:345-352, where nkeys becomes 3).
     */
    let objsubid = if subid != 0 { Some(subid) } else { None };

    let description = seam::description_open::call(RowExclusiveLock)?;
    seam::description_delete_all::call(description, oid, classoid, objsubid)?;
    /* Done (comment.c:367) — closes holding RowExclusiveLock. */
    seam::description_close::call(description, RowExclusiveLock)
}

/// `DeleteSharedComments` — remove `pg_shdescription` comments for a shared
/// object.
///
/// comment.c:373-404. Always two scan keys `{objoid, classoid}`.
pub fn DeleteSharedComments(oid: Oid, classoid: Oid) -> PgResult<()> {
    let shdescription = seam::shdescription_open::call(RowExclusiveLock)?;
    seam::shdescription_delete_all::call(shdescription, oid, classoid)?;
    /* Done (comment.c:403) — closes holding RowExclusiveLock. */
    seam::shdescription_close::call(shdescription, RowExclusiveLock)
}

/// `GetComment` — get the comment for an object, or `None` if not found.
///
/// comment.c:409-459.
pub fn GetComment(oid: Oid, classoid: Oid, subid: i32) -> PgResult<Option<String>> {
    let description = seam::description_open::call(AccessShareLock)?;

    /* comment = NULL; (comment.c:440) */
    let mut comment: Option<String> = None;

    /*
     * Found the tuple, get description field (comment.c:441-451). The scan
     * returns the description column value and its isnull flag for the one
     * match, or None when nothing matched (the C while-loop body not entered).
     */
    if let Some(col) = seam::description_get_description::call(description, oid, classoid, subid)? {
        if !col.isnull {
            comment = Some(seam::text_datum_get_cstring::call(col.value)?);
        }
    }

    /* Done (comment.c:456). */
    seam::description_close::call(description, AccessShareLock)?;

    Ok(comment)
}

/// Borrow `stmt.comment` (the C `const char *comment` argument) as
/// `Option<&str>`. The empty-string -> NULL reduction is performed inside
/// [`CreateComments`] / [`CreateSharedComments`], matching the C.
fn comment_str(stmt: &CommentStmt) -> Option<&str> {
    stmt.comment.as_deref()
}

/// `if (comment != NULL && strlen(comment) == 0) comment = NULL;`
/// (comment.c:156, 251) — fold an empty (zero-length) comment to `None`.
fn reduce_empty(comment: Option<&str>) -> Option<&str> {
    match comment {
        Some("") => None,
        other => other,
    }
}

/// Install this crate's inward seams. comment.c owns no inward seam boundary
/// (nothing calls *into* it across a dependency cycle — its callers,
/// dependency.c / ruleutils.c, are downstream), so there is nothing to install.
/// Present and wired into `seams-init::init_all()` for the registry invariant.
pub fn init_seams() {}

#[cfg(test)]
mod tests;
